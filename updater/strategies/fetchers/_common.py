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
import json
import os
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
    exactly like ons_uk's MAX_PER_RUN cap: the unit vintage is not advanced, so nothing is
    silently skipped.

    THE REMAINDER DOES NOT DRAIN BY ITSELF — this docstring used to claim it did, and that
    sentence propagated the bug into abs (R190). Sub-unit lists here are overwhelmingly
    STABLE in order, so a budget over a fixed order re-walks the same PREFIX every run and
    the tail is never fetched at all: a silent outage wearing a reassuring `partial`. A
    budgeted fetcher MUST also either
      (a) skip already-fresh sub-units cheaply via a per-sub-unit sidecar (eia, zillow,
          bis, fed_board — their vintage gate makes every run start somewhere new), or
      (b) rotate its starting point with load_rotation / save_rotation / rotate_after.
    A bound without one of those is a truncation, not a budget.

        dl = Deadline(minutes=20)
        for ind in indicators:
            if dl.spent():
                capped = True
                break
    """

    def __init__(self, minutes: float):
        # GLOBAL OVERRIDE for the workstation (2026-07-30). Every BUDGET_MIN in this package
        # was sized for the SHARED 16 GB CI job, where one source must not consume the whole
        # 240-minute run. The 16 databases routed to run_location: local are the opposite
        # case: a dedicated 382 GB machine processing only them, where a 25-minute cap just
        # defers work for no reason — abs deferred 805 of its 1,222 flows in CI purely
        # because of its budget. One env var rather than editing 17 fetchers.
        #
        # OPT-IN AND LOUD: unset (CI, and any normal run) behaves exactly as before.
        # tools/run_local_heavy.ps1 sets it.
        override = os.environ.get("AQUEDUCT_BUDGET_MIN_OVERRIDE")
        if override:
            try:
                effective = float(override)
            except (TypeError, ValueError):
                pass                                         # a bad value must not un-bound a run
            else:
                # SAY SO. Fetchers print their own module BUDGET_MIN in the deferral message,
                # so with an override in force a run logs "budget of 20 min spent after
                # 3.1 min" — a flat contradiction that sent me hunting a bug that did not
                # exist. The constant stops being the effective budget the moment this is set,
                # so the override announces itself rather than leaving 38 call sites to
                # quietly misreport it.
                if effective != minutes:
                    print(f"[budget] AQUEDUCT_BUDGET_MIN_OVERRIDE={effective:g} min replaces "
                          f"this fetcher's {minutes:g} min — a later 'budget of {minutes:g} "
                          f"min spent' message means {effective:g}", flush=True)
                minutes = effective
        self.budget = minutes * 60.0
        self.budget_min = minutes      # the EFFECTIVE budget, for callers that report it
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
        # NOT ATTEMPTED this tick — the budget stopped the sweep before reaching them. Kept apart
        # from `transient` because they are not failures (R303).
        self.deferred = 0
        self.deferred_ids: list = []
        # DATASETS OUTSIDE THE TIME-SERIES MODEL — the publisher's DSD declares no SDMX
        # TimeDimension, so the export has no TIME_PERIOD column and can NEVER parse as a
        # series. Not a break: nothing was ever there. Kept apart from `structural` because
        # finalize() RAISES on any structural count — oecd carries 60 such flows (verified
        # 2026-08-30 against the publisher's own DSDs, 18/18 with controls; 0 of the 60 have
        # ever had rows vs 40/40 controls — R523), and counting them as breaks made every
        # oecd run definitive_fail and starved the whole 1,545-flow giant for weeks.
        self.no_time = 0
        self.no_time_ids: list = []
        # SUB-UNITS THAT LEGITIMATELY HELD NOTHING. Counted since the beginning; COLLECTED only
        # since 2026-09-02, because `empty_unit` and `added_unit` both took a `label` argument
        # and discarded it while transient_unit, structural_unit and no_time_unit recorded
        # theirs. Nine modules were already passing one into the void - bea, census, defillama,
        # hagstofa, stat_estonia, unsdg, wid, _imf_direct (imported by 105 fetchers) and
        # _who_gho.
        #
        # NOT rendered into Result.error. A first attempt did, and an adversarial review showed
        # why it must not: on the success path orchestrate.py writes that string to
        # unit_state.last_error and runs.note with no _clip_err, and tools/gen_runbook.py cuts
        # it at ~1,152 characters with no ellipsis. This list exists so a caller that wants the
        # names can have them, cheaply and boundedly - not so every quiet tick grows a note.
        self.empty_ids: list = []

    def added_unit(self, n: int, label=None):
        self.attempted += 1
        if n and n > 0:
            self.added += n
        else:
            # This branch increments the SAME counter as empty_unit, so its label belongs in the
            # same list. ~70 call sites already pass one; before 2026-09-02 every one was
            # dropped, which would have made `empty_ids` a subset of what `empty` counts.
            self.empty += 1
            if label:
                self.empty_ids.append(str(label))

    def empty_unit(self, label=None):
        self.attempted += 1
        self.empty += 1
        if label:
            self.empty_ids.append(str(label))

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

    def no_time_unit(self, label=None):
        """Sub-unit whose DSD declares no SDMX TimeDimension — outside the series model.

        Only for sub-units that have NEVER had rows (the caller must check the store first,
        abs.py's predicate): the same body on a flow that previously HAD rows is a genuine
        structural break and belongs in structural_unit(). Attempted counts — we did the
        work — but it is neither a failure nor a break, and finalize() reports it as a
        non-demoting note (R359's precedent: a permanent, explained residue must not redden
        every run, or real reds drown).
        """
        self.attempted += 1
        self.no_time += 1
        if label:
            self.no_time_ids.append(str(label))

    def deferred_unit(self, label=None):
        """Sub-unit the budget stopped us reaching. NOT a failure, and NOT attempted.

        Before this existed, budget deferrals went through transient_unit(), so ecb reported
            "252/540 sub-unit(s) transient-failed; will retry"
        while every named unit read "budget 35 min spent, deferred". Nothing had failed and 252
        units were never touched. Counting them as attempted AND failed makes the real failure
        rate unreadable: 252 of 540 is alarming, 0 of 288 attempted is fine, and the log showed
        the first (R303, measured on abs/ecb/ssb).

        Deliberately does not touch `attempted` — that word has to keep meaning attempted, or the
        denominator lies too. "Transient" says something went wrong and retrying may help;
        "deferred" says nothing went wrong and rotation takes it next tick. Both mean come back;
        only one means investigate.
        """
        self.deferred += 1
        if label:
            self.deferred_ids.append(str(label))


def _named(ids, cap: int = 20) -> str:
    """Render the offending sub-unit labels for an error message, bounded.

    Bounded because a source with hundreds of sub-units would otherwise push a
    multi-KB blob into unit_state.last_error and the digest email; the count in
    the message stays authoritative, and the elision is stated rather than silent.

    CAP RAISED 6 -> 20 on 2026-08-04. Six was matched to the orchestrator's old
    `str(e)[:300]` store — naming more was pointless when the row would be cut anyway. That
    clip now carries 1400 characters and announces itself (orchestrate._clip_err), so the
    binding constraint moved and the cap can follow it.

    20 is chosen against the sources that actually have this problem rather than as a round
    number: wid names 12 sub-units and hagstofa 7, so both go from a partial list plus
    "+6 more" to the COMPLETE set — which is the difference between a finding you can act on
    and one that still needs a bisect. Twenty path-shaped ids run ~900 characters, comfortably
    inside 1400 with the message prefix; beyond that the orchestrator's clip takes over and
    says so.
    """
    if not ids:
        return ""
    shown = ", ".join(ids[:cap])
    extra = f", +{len(ids) - cap} more" if len(ids) > cap else ""
    return f" [{shown}{extra}]"


def finalize(tally: Tally, total_rows, last_obs, *, source, series_cursors=None,
             empty_window_floor=10, merged_rows=None):
    """Turn a Tally into an honest Result (or raise DefinitiveError). See module docstring.

    `total_rows` is what it says: about 120 of ~123 call sites pass the STORE'S TOTAL, so the
    resulting `Result.obs` describes how big the store is, not what this run did. Do not read
    it as a merge count and do not let any message call it one.

    `merged_rows` is the honest answer to "did this run merge anything", and a fetcher passes it
    ONLY where it can prove it. None (the default) means not reported, which is what almost
    every fetcher truthfully is, and behaves exactly as before.
    """
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
                      new_vintage="date-tail", series_cursors=series_cursors, merged_rows=merged_rows,
                      error=f"{tally.transient}/{tally.attempted} sub-unit(s) transient-failed; will retry"
                            + _named(tally.transient_ids))
    if tally.deferred:
        # NOTHING FAILED — the budget simply stopped the sweep. Still `partial`, because the tick
        # did not cover everything and must not stamp a full-coverage vintage; the change is that
        # the message no longer calls a deliberate deferral a failure, and the denominator is what
        # was actually attempted (R303).
        return Result(status="partial", obs=total_rows, last_obs_date=last_obs,
                      new_vintage="date-tail", series_cursors=series_cursors, merged_rows=merged_rows,
                      error=(f"{tally.attempted} sub-unit(s) attempted, none failed; "
                             f"{tally.deferred} deferred by budget and taken next tick"
                             + _named(tally.deferred_ids)))
    status = "ok" if tally.added > 0 else "no_change"
    note = f"+{tally.added} new rows" if tally.added else "no new rows"
    if tally.no_time:
        # NON-DEMOTING by design (R359's precedent: a permanent, explained residue must not
        # redden every run). These sub-units' DSDs declare no SDMX TimeDimension — outside
        # the series model, never had rows, re-probed only when the publisher's vintage
        # changes. Named so the note stays actionable rather than a bare count.
        note += (f"; {tally.no_time} dataset(s) outside the series model — publisher DSD "
                 f"declares no SDMX TimeDimension" + _named(tally.no_time_ids))
    return Result(status=status, obs=total_rows, last_obs_date=last_obs, new_vintage="date-tail",
                  series_cursors=series_cursors, merged_rows=merged_rows, error=note)


CURSOR_CAP = 50_000
"""Most cursors one fetcher should report in a run — a DISCLOSED bound, never silent.

Why a bound exists at all. orchestrate._catalog_ids_for runs ONE SQLite query per changed key,
and StateStore.put_series_cursors writes one row per cursor into state.db (already ~306 MB,
compressed and pushed to R2 every run). Both are linear in the cursor count, so an unbounded
set is a real outage risk, not an inefficiency.

Why 50k is not arbitrary. Measured on ilostat: 1,947 indicators hold ~30.8 MILLION distinct
store series (388M rows, ~15,800 series per indicator), and its store keys already carry the
`ilostat:` prefix — so `_catalog_ids_for` builds `ilostat:ilostat:…`, nothing maps, and with 80
catalog ids (under _DERIVE_ALL_CAP=5000) the orchestrator re-derives all 80 anyway. Reporting
millions of cursors there would buy exactly nothing and cost millions of queries and rows.
Every other bulk source here is far below the cap: fed_board's largest release has 39,882
series, maddison 338, who_hwf 4,421 — but NOT fhfa, whose annual_tract cube alone holds
63,930 series (union ~89,706 — the stale "~5k" here survived 18x growth, WU-2b), so every
fhfa rebuild saturates the cap; its fetcher discloses that via Result.cursor_cap_hit.

When the cap bites, the caller LOGS the count it dropped. A truncation nobody is told about
reads as "we covered everything".
"""


def _max_by_key(tbl, key_col="series_key", date_col="obs_date") -> "dict[str, str]":
    """{key: max date as an ISO STRING} WITHOUT pyarrow's group_by.

    THE VALUES ARE STRINGS, NOT dates — the final line calls .isoformat() for you. Said in the
    signature and shouted here because EVERY ONE of the five callers got it wrong, and the
    failures were not alike:

      boc, tcmb   called .isoformat() a SECOND time -> `'str' object has no attribute
                  'isoformat'`, taking both sources to transient_fail. Fixed a1c42881.
      riksbank    filtered on `isinstance(v, dt.date)`, which no string satisfies, so it returned
                  an EMPTY cursor map every run — no crash, no log line, just permanent `partial`
                  from the §5.7 coherence check. Fixed a1c42881.
      bcrp        SAME .isoformat() crash, but 120 lines downstream of the call, in the cursor
                  seed and again in last_db. It was still crashing in production SIX HOURS AFTER
                  a1c42881 landed. Fixed 15f49f1c.
      scb         worse-shaped: _table_frontiers is annotated dict[str, dt.date] and passed these
                  strings straight through, so `stored_max.isoformat()` raised AND
                  `_parse_date(c) > stored_max` raised TypeError — and that comparison IS the
                  date-tail window deciding what gets fetched. Latent only because scb had not
                  run since before this function existed. Fixed 15f49f1c at the source.

    THIS PARAGRAPH PREVIOUSLY CLAIMED "bcrp and scb work only because ISO strings sort and compare
    exactly like dates". That is true where a string meets a string and false the moment one meets
    a real date — which is what both of them do. The claim came from reading each fetcher's CALL
    SITE, where nothing is obviously wrong; both consume the value far downstream. If you add a
    caller, trace where the VALUE ends up, not where the call is.

    Cursors are STORED as ISO strings, so returning strings is correct; the annotation is what was
    missing.

    group_by is not merely slow on a big string column, it is UNSAFE: Arrow indexes string
    data with int32 offsets, and past 2 GiB in one column the aggregate overflows and kills
    the PROCESS (0xC0000005 / SIGABRT), it does not raise. That matters here because the
    caller wraps this in `except Exception` and returns {} - which reads as "cursors are
    best-effort, nothing can go wrong" while actually being incapable of catching the failure
    mode that occurs. An updater run would simply vanish, mid-source, with no traceback.
    That is how a whole night's daily run was lost before merge._dedup was rewritten the same
    way (2026-07-31).

    So: promote to large_string when the column is big enough to be at risk, then sort and
    take the last row per key - the same shape merge.py uses, for the same reason.
    """
    import pyarrow as pa
    import pyarrow.compute as pc
    from ... import merge as _merge

    if _merge._needs_large_string(tbl):
        tbl = _merge._promote_large_string(tbl)[0]
    t = _merge._sort(tbl, (key_col, date_col))
    n = t.num_rows
    if n == 0:
        return {}
    keys = t.column(key_col).combine_chunks()
    # last row of each key run == that key's max date, because the sort put dates ascending
    # within a key.
    if n == 1:
        last = pa.array([True])
    else:
        changes = pc.invert(pc.equal(keys.slice(0, n - 1), keys.slice(1, n - 1)))
        last = pa.concat_arrays([changes.cast(pa.bool_()), pa.array([True], pa.bool_())])
    picked = t.filter(last)
    ks = picked.column(key_col).to_pylist()
    ds = picked.column(date_col).to_pylist()
    return {k: d.isoformat() for k, d in zip(ks, ds) if k and d is not None}


def cursors_from_table(tbl, cap: int = CURSOR_CAP, key_col="series_key",
                       date_col="obs_date") -> dict:
    """Cursors for rows a fetcher just merged, without re-reading the published file.

    A bulk fetcher that has the new table in hand should report the series IT changed, not
    every series in the file: over-reporting re-derives CSVs that did not move, and on a
    170-million-observation source that is the difference between a few thousand PUTs and
    millions. Returns {} on failure - a cursor problem must never sink a good publish.
    """
    try:
        if tbl is None or tbl.num_rows == 0:
            return {}
        out = _max_by_key(tbl, key_col, date_col)
        if cap and len(out) > cap:
            print(f"[cursors] {len(out):,} changed series exceeds the {cap:,} cap - "
                  f"reporting the first {cap:,} (sorted)", flush=True)
            out = {k: out[k] for k in sorted(out)[:cap]}
        return out
    except Exception:                                        # noqa: BLE001
        return {}


def api_key(name: str) -> str | None:
    """An API key from the environment, falling back to the repo's .env files.

    NOTHING ELSE LOADS .env. Not the orchestrator, not run_local_heavy.ps1, not any module
    here — a fetcher that reads only os.environ therefore cannot see a key that IS present
    on the workstation. bea is the case that exposed it: BEA_API_KEY has been sitting in
    .env while bea refused every run with "BEA_API_KEY is not set, so nothing can be
    fetched", and that was recorded as blocked on Ahmed creating a GitHub secret. The
    secret is genuinely missing; the KEY was not.

    Order is env first so CI (which injects real secrets and has no .env) is unaffected and
    a shell override still wins locally. Never logs or returns anything but the value.
    """
    v = os.environ.get(name)
    if v and v.strip():
        return v.strip()
    from ... import config as _config
    for fname in (".env", ".env.local"):
        p = os.path.join(_config.ROOT, fname)
        if not os.path.exists(p):
            continue
        try:
            with open(p, encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line.startswith(f"{name}="):
                        val = line.split("=", 1)[1].strip().strip('"').strip("'")
                        if val:
                            return val
        except Exception:                                    # noqa: BLE001
            pass
    return None


def cursors_from_parquet(path, key_col="series_key", date_col="obs_date",
                         cap: int = CURSOR_CAP, key_prefix: str = "") -> dict:
    """{series_key: max obs_date ISO} for one published parquet.

    key_prefix is prepended to every key. It exists because the cursor key must satisfy
    ONE contract: `<source>:` + key == the catalog id, since that is exactly what
    orchestrate._catalog_ids_for reconstructs. Sources whose catalog ids carry a
    dataset/release segment (fed_board:CHGDEL:<series>, fhfa:annual_cbsa:<series>) store
    only the bare series in the parquet, so a raw read-back maps to NOTHING — the source
    reports `partial` forever and its published CSVs drift from the parquet while the
    fetch itself looks healthy. Measured 2026-08-02: fed_board 2,004 unmapped keys over
    52,322 real catalog rows, fhfa the same shape. Pass the segment here.

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
        tbl = pq.read_table(path, columns=[key_col, date_col])
        if tbl.num_rows == 0:
            return {}
        out = _max_by_key(tbl, key_col, date_col)
        if cap and len(out) > cap:
            print(f"[cursors] {os.path.basename(path)}: {len(out):,} changed series exceeds the "
                  f"{cap:,} cursor cap — reporting the first {cap:,} (sorted); the orchestrator's "
                  f"derive-all path covers the rest for small catalogs", flush=True)
            out = {k: out[k] for k in sorted(out)[:cap]}
        if key_prefix:
            out = {f"{key_prefix}{k}": v for k, v in out.items()}
        return out
    except Exception:                                        # noqa: BLE001
        return {}


def merge_cursors(dst: dict, path, **kw) -> dict:
    """Accumulate one file's cursors into `dst`, respecting CURSOR_CAP ACROSS FILES.

    cursors_from_parquet caps a SINGLE file. That is not enough for a multi-file source: zillow
    republishes up to 206 cubes (~543,000 series in union), bis 375 files (LBS alone has 608,570),
    fhfa 9 cubes (~89,700), fed_board 18 releases. Capping each file and then unioning them
    reaches the same unbounded total the cap exists to prevent — the per-file bound is necessary
    and not sufficient.

    Stops adding once dst reaches the cap and returns dst unchanged thereafter. The CALLER logs
    the fact; see the CURSOR_CAP docstring for why silence would be the real bug.
    """
    if len(dst) >= CURSOR_CAP:
        return dst
    got = cursors_from_parquet(path, **kw)
    for k, v in got.items():
        if len(dst) >= CURSOR_CAP:
            break
        dst[k] = v
    return dst


def merge_cursor_map(dst: dict, src, cap: int = CURSOR_CAP) -> bool:
    """Fold an IN-MEMORY {series_key: iso_date} map into `dst`, respecting the cap.

    merge_cursors bounds a set read back from a PARQUET. Fetchers that already hold the
    keys in memory — because they parsed the rows this run — had no bounded path, so each
    grew its own unbounded dict: vdem 1,465,759 series and owid 1,048,968 (measured
    2026-07-30), against a 50,000 cap. Neither is an OOM on its own, but every cursor
    becomes a state.db row and a _catalog_ids_for query, both linear in the count.

    Returns True if the cap was reached so the caller can DISCLOSE it — a silent bound is
    the defect the CURSOR_CAP docstring warns about. Keys already present keep advancing
    after the cap is hit, so the reported set stays coherent instead of freezing mid-file.
    """
    capped = False
    items = src.items() if isinstance(src, dict) else src
    for k, v in items:
        prev = dst.get(k)
        if prev is None:
            if len(dst) >= cap:
                capped = True
                continue
            dst[k] = v
        elif v > prev:
            dst[k] = v
    return capped


ROTATION_FILE = "_rotation.json"


def load_rotation(out_dir, fname: str = ROTATION_FILE) -> str:
    """The sub-unit key the LAST run stopped after, or ''.

    WHY EVERY BUDGETED FETCHER NEEDS THIS (R190). A Deadline or MAX_PER_RUN stops work
    partway; the log then says the remainder "drains next tick". That is only true if the
    next run starts somewhere NEW. Sub-unit lists here are overwhelmingly stable in order —
    blob.list_parquets sorts, module-level CUBES tuples are literals, catalog pulls come
    back in the publisher's order — so a bound over a fixed order re-walks the same PREFIX
    forever and the tail is never fetched at all. That is a silent, self-certifying outage:
    the source reports `partial` with a reassuring reason, indefinitely.

    A per-sub-unit freshness sidecar (eia, zillow, bis, fed_board) solves it a different
    way — done units are skipped cheaply, so every run makes progress. Fetchers WITHOUT
    such a sidecar need this bookmark instead.
    """
    from ... import blob as _blob                             # local: avoid a cycle at import
    raw = _blob.read_bytes(os.path.join(out_dir, fname))
    if not raw:
        return ""
    try:
        return str((json.loads(raw.decode("utf-8")) or {}).get("after") or "")
    except (ValueError, UnicodeDecodeError, AttributeError):
        return ""


def save_rotation(out_dir, key: str, fname: str = ROTATION_FILE) -> None:
    """Record where to resume. Callers save even after a COMPLETE pass, so the bookmark is
    then the last sub-unit in order and the next run wraps to the top through the same code
    path — no branch that could silently stop rotating.

    Swallows failures: a bookmark is an optimisation, and losing it costs one run of
    re-walking a prefix, never data. It must never sink a good publish.
    """
    from ... import blob as _blob
    try:
        _blob.write_bytes_atomic(os.path.join(out_dir, fname),
                                 json.dumps({"after": key}, indent=1).encode("utf-8"))
    except Exception:                                        # noqa: BLE001
        pass


def rotate_after(items: list, bookmark: str, key=None) -> list:
    """`items` re-ordered to start just after `bookmark`, wrapping around.

    Unknown or empty bookmark -> unchanged, so a first run, a renamed sub-unit or a
    corrupt bookmark all degrade to "start at the top" rather than skipping anything.
    """
    if not bookmark or not items:
        return items
    kf = key or (lambda x: x)
    for i, it in enumerate(items):
        if kf(it) == bookmark:
            return items[i + 1:] + items[:i + 1]
    return items


CONSECUTIVE_TRANSIENT_LIMIT = 25


class TransientStreak:
    """Stop walking sub-units once the upstream is plainly refusing all of them.

    THE COST THIS AVOIDS, measured. insee_bdm walks 201 flows. On 2026-07-31 every one of
    them transient-failed and the fetcher ground through all 201 anyway, each with its own
    retry-and-backoff budget: 104.5 MINUTES to establish that the upstream was throttling,
    against 11.1 minutes for the same source's healthy run two weeks earlier. It then
    consumed 44% of the whole 240-minute daily budget to produce nothing. Probed afterwards
    the API answered 200 in 0.6s, so the block was temporary — which is exactly the case
    where spending an hour proving it is worst.

    A handful of scattered transient failures is normal and must NOT trip this: one flaky
    endpoint should never stop the other 200. But N IN A ROW is not flakiness, it is the
    upstream saying no — rate limit, outage, or revoked access — and every further request
    is both futile and, if it is a rate limit, actively counter-productive.

    Honest status is unchanged: the caller records the remaining sub-units transient, so the
    run is `partial`, the vintage is not advanced, and it retries. The only thing that
    changes is how long we take to reach that conclusion.

        streak = TransientStreak()
        for unit in units:
            if streak.tripped:
                tally.transient_unit(f"{unit} not attempted (upstream refusing)")
                continue
            try:
                ...
                streak.ok()
            except TransientError:
                tally.transient_unit(unit)
                streak.fail()
    """

    def __init__(self, limit: int = CONSECUTIVE_TRANSIENT_LIMIT):
        self.limit = max(1, int(limit))
        self.streak = 0
        self.tripped = False
        self.tripped_after = 0

    def ok(self) -> None:
        """A sub-unit succeeded — the upstream is answering, so reset."""
        self.streak = 0

    def fail(self) -> bool:
        """A sub-unit transient-failed. Returns True once the breaker has tripped."""
        self.streak += 1
        if not self.tripped and self.streak >= self.limit:
            self.tripped = True
            self.tripped_after = self.streak
        return self.tripped


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


def cancellable_pool(max_workers: int):
    """A ThreadPoolExecutor whose shutdown CANCELS queued work instead of draining it.

    WHY THIS EXISTS — the 2026-08-07 daily updater outage. `with ThreadPoolExecutor(...) as ex`
    calls `shutdown(wait=True)` on the way out, INCLUDING when an exception is propagating, and
    that waits for every future already submitted. The orchestrator's per-unit hard timeout is a
    SIGALRM that raises `UnitTimeout` in the main thread, so on a slow source the sequence is:

        10:02  owid starts, submits all 150 slugs to a 6-worker pool
        10:47  SIGALRM fires, UnitTimeout raised inside the as_completed loop
        10:47  the `with` block starts shutdown(wait=True) and drains the remaining ~100 slugs
        12:32  GitHub kills the step at its 250-minute cap

    owid printed nothing for 150 minutes and the timeout message never appeared, because the
    exception could not escape the context manager. The 45-minute cap was armed and correct; it
    simply could not take effect. Four fetchers share the pattern (boe, ksh_stadat, ons_uk, owid).

    `cancel_futures=True` drops the QUEUED futures and `wait=True` still joins the at most
    `max_workers` already running, so shutdown is bounded by one task, not by the backlog. On the
    success path nothing is queued and this is a no-op.
    """
    import contextlib
    from concurrent.futures import ThreadPoolExecutor

    @contextlib.contextmanager
    def _cm():
        ex = ThreadPoolExecutor(max_workers=max_workers)
        try:
            yield ex
        finally:
            ex.shutdown(wait=True, cancel_futures=True)
    return _cm()
