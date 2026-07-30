"""Shared helpers for S2 fetchers: an HONEST ok/no_change/partial decision.

The dominant oversight finding: fetchers laundered sub-unit failures (a per-endpoint
timeout, a wholesale 404, a 200 with an unparseable/empty body) into no_change/ok, so
the orchestrator stamped last_success and the source looked fresh while actually frozen.

Tally + finalize make the returned status truthful:
  - STRUCTURAL break (a 200 that parsed 0 rows from a non-trivial body)  -> DefinitiveError
  - a large all-empty/404 window (likely a structural break, not a quiet period) -> DefinitiveError
  - any TRANSIENT sub-failure                                            -> status='partial'
        (the orchestrator does NOT stamp last_success on 'partial'; the unit re-runs next tick)
  - otherwise                                                            -> 'ok' (added>0) / 'no_change'
In every failure case the caller has already left existing data untouched (merge_and_write
only publishes good data), so this is about correct STATUS, never data loss.
"""
from __future__ import annotations
import datetime as _dt
import time

from ..base import Result
from ...errors import DefinitiveError


def sane_since(stored_max, *, max_future_days=400):
    """Guard a date-tail boundary against CORRUPT far-future obs_dates.

    PxWeb (and some SDMX) time-dimension heuristics can mis-store a sentinel like
    year 9999/6000/2584. If a fetcher then filters `period > stored_max`, it selects
    NOTHING and the series freezes forever. This returns None when stored_max is more
    than max_future_days beyond today (so the caller should fall back to a trailing
    window / full re-pull instead of a delta), else returns stored_max unchanged.
    """
    if stored_max is None:
        return None
    try:
        d = stored_max if isinstance(stored_max, _dt.date) else _dt.date.fromisoformat(str(stored_max)[:10])
    except Exception:
        return None
    if (d - _dt.date.today()).days > max_future_days:
        return None
    return stored_max


def retry_after_seconds(resp, default: int = 10) -> int:
    """Seconds to wait per the server's Retry-After header, or `default` if absent.

    Handles BOTH forms RFC 9110 allows: <delay-seconds> and <http-date>. A backoff that
    ignores this header keeps retrying INSIDE the server's cooldown, which is how you earn
    an escalating block rather than recover from a throttle — ONS publishes that it will
    block for "up to 1 hour" (developer.ons.gov.uk/bots), and statfin's crawl died on a
    429 after exhausting retries that all slept on a guess instead of the stated wait.

    Clamped to [1, 120] so a bogus or hostile value cannot park a run for hours; +1 so we
    resume just after the window rather than exactly on its edge.
    """
    raw = (resp.headers.get("Retry-After") or "").strip() if getattr(resp, "headers", None) else ""
    wait = None
    if raw.isdigit():
        wait = int(raw)
    elif raw:
        try:
            from email.utils import parsedate_to_datetime
            when = parsedate_to_datetime(raw)
            if when.tzinfo is None:
                when = when.replace(tzinfo=_dt.timezone.utc)
            wait = int((when - _dt.datetime.now(_dt.timezone.utc)).total_seconds())
        except Exception:
            wait = None
    if wait is None:
        wait = default
    return min(max(wait, 1), 120) + 1


class Deadline:
    """A wall-clock budget for ONE source, checked between sub-units.

    orchestrate.py runs sources strictly serially, so a single slow upstream stalls every
    source queued behind it and can run the daily job into its 300-minute ceiling. Retry
    budgets make that far worse than it looks: worldbank_esg reuses an ingester whose
    get_json does 6 tries x 120 s timeout plus 1+2+4+8+16+30 s of backoff — about 13
    minutes PER URL, and it walks ~71 indicators, so a flaky day is a ~15 hour source.
    Observed for real: a local run sat on worldbank_esg for 39 minutes at 0.16 GB RSS
    (hung on IO, not memory) and had to be killed.

    This does NOT interrupt an in-flight request — you cannot portably do that mid-call.
    It lets a fetcher stop starting NEW work once the budget is spent and report `partial`,
    exactly like ons_uk's MAX_PER_RUN cap: the remainder drains on the next tick and the
    unit vintage is not advanced, so nothing is silently skipped.

        dl = Deadline(minutes=20)
        for ind in indicators:
            if dl.spent():
                capped = True
                break
    """

    def __init__(self, minutes: float):
        self.budget = minutes * 60.0
        self.t0 = time.monotonic()

    def spent(self) -> bool:
        return (time.monotonic() - self.t0) >= self.budget

    def elapsed_min(self) -> float:
        return (time.monotonic() - self.t0) / 60.0


REVISION_LOOKBACK_DAYS = 30


def revision_since(last, unit=None, default_days=REVISION_LOOKBACK_DAYS):
    """The date-tail EDGE-CASE fix: start the fetch a lookback window BEHIND the
    stored frontier, not exactly at it.

    A pure `fetch from last stored obs_date` tail can never see observations a
    provider INSERTS or REVISES at dates <= that frontier (late postings after
    an outage on their side, back-corrections). Refetching a trailing window is
    free-to-cheap for date-tail sources (dedup keep-last absorbs the overlap,
    merge's never-shrink still guards), so every S2 fetcher computes its start
    as revision_since(last) instead of last. Override per source with
    `revision_lookback_days` in the registry entry's config; 0 disables.

    Honest-status nuance: rows inside the window carry no NEW dates, so a run
    that only absorbed revisions still reports `no_change` — the status tracks
    the data frontier, and revised values are corrected in the store either way.

    Returns a datetime.date (never earlier than 1900-01-01), or None if `last`
    is None/unparseable (caller falls back to its first-run origin).
    """
    if last is None:
        return None
    try:
        d = last if isinstance(last, _dt.date) else _dt.date.fromisoformat(str(last)[:10])
    except Exception:
        return None
    days = default_days
    cfg = getattr(unit, "config", None) or {}
    if "revision_lookback_days" in cfg:
        try:
            days = max(0, int(cfg["revision_lookback_days"]))
        except (TypeError, ValueError):
            pass
    return max(d - _dt.timedelta(days=days), _dt.date(1900, 1, 1))


class Tally:
    """Counts the outcome of each sub-unit (endpoint / dataset / series / day) a fetcher attempts."""

    def __init__(self):
        self.attempted = 0
        self.added = 0          # total NEW rows merged in
        self.empty = 0          # sub-units that succeeded but had no new data
        self.transient = 0      # sub-units that transient-failed (retry next run)
        self.structural = 0     # sub-units that returned 200 but were unparseable/empty-from-real-body
        # WHICH sub-units failed, not just how many. A message that says "1/20 sub-unit(s)
        # returned 200 but parsed 0 rows" names a defect you cannot act on: it takes a
        # bisect to find the one endpoint at fault, so the finding sits unfixed. Callers
        # pass a label (endpoint / file / dataset id); finalize() names the offenders.
        self.structural_ids: list = []
        self.transient_ids: list = []

    def added_unit(self, n: int, label=None):
        self.attempted += 1
        if n and n > 0:
            self.added += n
        else:
            self.empty += 1

    def empty_unit(self, label=None):
        self.attempted += 1
        self.empty += 1

    def transient_unit(self, label=None):
        self.attempted += 1
        self.transient += 1
        if label:
            self.transient_ids.append(str(label))

    def structural_unit(self, label=None):
        self.attempted += 1
        self.structural += 1
        if label:
            self.structural_ids.append(str(label))


def _named(ids, cap: int = 6) -> str:
    """Render the offending sub-unit labels for an error message, bounded.

    Bounded because a source with hundreds of sub-units would otherwise push a
    multi-KB blob into unit_state.last_error and the digest email; the count in
    the message stays authoritative, and the elision is stated rather than silent.
    """
    if not ids:
        return ""
    shown = ", ".join(ids[:cap])
    extra = f", +{len(ids) - cap} more" if len(ids) > cap else ""
    return f" [{shown}{extra}]"


def finalize(tally: Tally, total_rows, last_obs, *, source, series_cursors=None,
             empty_window_floor=10):
    """Turn a Tally into an honest Result (or raise DefinitiveError). See module docstring."""
    if tally.structural:
        raise DefinitiveError(
            f"{source}: {tally.structural}/{tally.attempted} sub-unit(s) returned 200 but parsed 0 "
            f"rows from a non-trivial body (schema/structural break); existing data kept"
            + _named(tally.structural_ids))
    if tally.added == 0 and tally.empty == tally.attempted and tally.attempted > empty_window_floor:
        raise DefinitiveError(
            f"{source}: all {tally.attempted} attempted sub-units returned empty/404 over a large "
            f"window — likely a structural break, not a quiet period; existing data kept")
    if tally.transient:
        return Result(status="partial", obs=total_rows, last_obs_date=last_obs,
                      new_vintage="date-tail", series_cursors=series_cursors,
                      error=f"{tally.transient}/{tally.attempted} sub-unit(s) transient-failed; will retry"
                            + _named(tally.transient_ids))
    status = "ok" if tally.added > 0 else "no_change"
    return Result(status=status, obs=total_rows, last_obs_date=last_obs, new_vintage="date-tail",
                  series_cursors=series_cursors,
                  error=(f"+{tally.added} new rows" if tally.added else "no new rows"))


def cursors_from_parquet(path, key_col="series_key", date_col="obs_date") -> dict:
    """{series_key: max obs_date ISO} for one published parquet.

    WHY EVERY BULK FETCHER NEEDS THIS. orchestrate._derive_changed_csvs takes the changed-series
    set from `Result.series_cursors` and nothing else. A fetcher that merges rows and reports no
    cursors is handled deliberately (§5.7): the run is demoted to `partial` and the vintage is
    NOT bumped, so it re-fetches every run forever while its CSVs stay stale. Nothing crashes and
    the parquet is published, which is exactly what makes it easy to miss.

    Bulk snapshot sources have no natural per-series cursor — they replace whole files — so the
    honest changed-set is "every series in the file we just republished", read back from that
    file. One grouped scan of two columns, not a full read.

    Returns {} on any failure: a cursor problem must never sink a good publish. The caller then
    lands in the documented no-cursors path rather than raising.
    """
    try:
        import pyarrow.parquet as pq
        import pyarrow.compute as pc
        tbl = pq.read_table(path, columns=[key_col, date_col])
        if tbl.num_rows == 0:
            return {}
        agg = tbl.group_by(key_col).aggregate([(date_col, "max")])
        keys = agg.column(key_col).to_pylist()
        maxes = agg.column(f"{date_col}_max").to_pylist()
        return {k: d.isoformat() for k, d in zip(keys, maxes) if k and d is not None}
    except Exception:                                        # noqa: BLE001
        return {}


def structural_on_zero_rows(stored_max, resp) -> bool:
    """Uniform PxWeb-family rule for a 200 body that parsed to 0 observations: is it a
    STRUCTURAL break (True) or a benign empty/quiet (False)?  Shared by the PxWeb S3
    fetchers so they classify identically (statfin / stat_estonia / hagstofa).

    A break is ONLY the loss of data we ALREADY serve: `stored_max` is the table's SANE
    on-disk boundary (callers pass sane_since(raw_max), so a corrupt far-future sentinel
    is already demoted to None), the body is a real json-stat2 envelope (declared `id`
    dimensions + a `value` array), and that array still carries at least one NON-NULL
    value — real observations are present yet none parsed, i.e. the cube's shape / time
    coding regressed. Everything else is benign:
      - stored_max is None   -> never-landed, or a corrupt-boundary table demoted to a
                                full pull: not (yet) part of the data we serve.
      - no `id` / no `value`  -> not a real time-series envelope.
      - every value is null   -> a period slot published ahead of its data, not a break.

    (Fixes the stat_estonia inversion — its old gate fired on never-landed tables and
    stayed silent when a populated table went dark, the actual break. MISTAKES R25.)
    """
    if stored_max is None or not isinstance(resp, dict):
        return False
    if not resp.get("id"):
        return False
    vals = resp.get("value")
    if isinstance(vals, dict):
        vals = list(vals.values())
    return any(v is not None for v in (vals or []))
