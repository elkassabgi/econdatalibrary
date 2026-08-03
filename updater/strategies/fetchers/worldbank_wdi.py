"""S2 fetcher — World Bank World Development Indicators (WDI), annual, 1960-present.

CC BY 4.0, no key. Single grouped parquet clean_full/worldbank_wdi/worldbank_wdi.parquet,
schema (series_key, obs_date, value) where series_key = 'WDI:{IndicatorCode}:{CountryCode}'
(e.g. WDI:NY.GDP.MKTP.CD:USA) and obs_date is the Dec-31 annual stamp. This MUST match the
existing on-disk format so merges EXTEND each series rather than duplicating it.

Why extend_by_date (not the bulk zip): the World Bank v2 JSON API exposes a native
server-side `date=YYYY:YYYY` filter on the per-indicator endpoint
(`/v2/country/all/indicator/{code}?date=2024:2026`). So instead of re-downloading the
whole WDI_CSV.zip and rebuilding ~8.9M rows, we read max(obs_date) from the existing
parquet, request ONLY a small trailing window (stored_max_year - LOOKBACK .. this_year+1),
and merge the new/revised cells in (dedup series_key+obs_date, new wins on revision,
never-shrink). The LOOKBACK absorbs annual back-revisions and late-arriving years.

Country-code matching (the subtle part): the bulk CSV's "Country Code" uses 3-char codes
for BOTH real countries (USA) and aggregates/income-groups (WLD, AFE, HIC, LMC). The v2
`country/all` payload returns `countryiso3code` for real countries but leaves it BLANK for
income-group/region aggregates, exposing only a 2-char `country.id` (XD, 1W, ...). We
resolve those via the WDI `/v2/country` reference list ({iso2Code: id_3char}), so every
emitted key is byte-identical to the on-disk 3-char code. Verified: 0 unresolved rows.

Honest-status contract (Tally + finalize):
  - The indicator list is a wholesale gate; if it can't be fetched, nothing else can be
    attempted -> transient_unit (status partial, retry next tick), existing data kept.
  - Each indicator is one sub-unit. A network/non-200 after retries -> transient_unit.
    A 200 with a real envelope but an empty data window (no obs in the trailing years)
    -> empty_unit (a quiet sub-unit, not a failure). Parsed rows -> added_unit.
  - finalize() raises DefinitiveError if EVERY sub-unit was empty over a large window
    (likely a structural break, not a quiet period) or if any structural break is flagged.
  - merge_and_write only publishes good data and never shrinks, so any failure path keeps
    the existing parquet untouched; this module only ever fixes STATUS, never loses data.
"""
from __future__ import annotations
import datetime as dt
import os
import time

import pyarrow as pa
import pyarrow.compute as pc
import requests

from ... import config, blob, merge
from ..base import Result
from ._common import (Deadline, Tally, finalize, load_rotation, rotate_after, save_rotation,
                      sane_since)
from ._vintage import UA

SOURCE = "worldbank_wdi"
BASE = "https://api.worldbank.org/v2"
WDI_SOURCE_ID = "2"          # source=2 is the WDI database in the v2 API
DEDUP = ("series_key", "obs_date")
PER_PAGE = 20000             # one indicator x small date window fits well within one page
RATE = 0.15                  # polite pause between indicator GETs
# Deliberately UNDER the orchestrator's 45-minute per-source cap. A hard kill at 45 min
# destroys the run (the single end-of-run merge never executes), so the fetcher must yield
# first and merge what it has. Rotation carries the remainder to the next run.
BUDGET_MIN = float(os.environ.get("WDI_BUDGET_MIN", "35"))
LOOKBACK_YEARS = 2           # re-pull this many stored years to absorb back-revisions
# Floor the window so a fresh/empty parquet still pulls a sensible recent tail rather
# than the whole 1960- history (which would be the bulk path, not a date-tail).
DEFAULT_WINDOW_YEARS = 6


def _get_json(url, params, retries=4):
    """200 -> json; 400/404 -> None (gone/empty); 429/5xx/network -> retry; give up -> 'transient'.

    Returns the parsed JSON, None (definitively empty/gone), or the sentinel string
    'transient' when retries are exhausted on a retryable fault so the caller can tally it
    as a transient sub-unit rather than silently treating it as empty.
    """
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=UA, timeout=90)
        except (requests.Timeout, requests.ConnectionError):
            time.sleep(min(30, 5 * (attempt + 1)))
            continue
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError:
                # 200 with an unparseable body = flaky/truncated body -> transient
                time.sleep(min(30, 5 * (attempt + 1)))
                continue
        if r.status_code in (400, 404):
            return None
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(60, 5 * (attempt + 1)))
            continue
        # other hard non-200 -> treat as gone for this sub-unit
        return None
    return "transient"


def _country_map() -> dict | None:
    """Build {iso2Code: id_3char} from the WDI /v2/country reference list, so blank-iso3
    aggregate rows in country/all data resolve to the 3-char code the on-disk keys use.
    Returns None on a transient failure (caller treats the whole run as transient)."""
    j = _get_json(f"{BASE}/country", {"format": "json", "per_page": 400})
    if j == "transient" or not isinstance(j, list) or len(j) < 2 or not j[1]:
        return None
    out = {}
    for c in j[1]:
        iso2 = c.get("iso2Code")
        code3 = c.get("id")
        if iso2 and code3:
            out[iso2] = code3
    return out or None


def _list_indicators() -> list | str | None:
    """All WDI (source=2) indicator codes via the paginated /v2/indicator endpoint.
    Returns a list of codes, None if the list is definitively empty, or 'transient'
    on a retryable failure (so the caller surfaces partial and keeps existing data)."""
    codes: list[str] = []
    page = 1
    while True:
        j = _get_json(f"{BASE}/indicator",
                      {"format": "json", "source": WDI_SOURCE_ID, "per_page": 1000, "page": page})
        if j == "transient":
            return "transient"
        if not isinstance(j, list) or len(j) < 2 or not j[1]:
            break
        for it in j[1]:
            cid = it.get("id")
            if cid:
                codes.append(cid)
        meta = j[0] or {}
        try:
            pages = int(meta.get("pages", 1))
        except (TypeError, ValueError):
            pages = 1
        if page >= pages:
            break
        page += 1
        time.sleep(RATE)
    return codes or None


def _stored_max_year(path: str) -> int | None:
    """Max year currently on disk (guarded against corrupt far-future obs_dates)."""
    if not blob.exists(path):
        return None
    tbl = blob.read_table(path)
    if tbl.num_rows == 0 or "obs_date" not in tbl.column_names:
        return None
    md = pc.max(tbl.column("obs_date")).as_py()
    if md is None:
        return None
    md = sane_since(md)            # None if md is corruptly far in the future
    if md is None:
        return None
    return md.year if isinstance(md, dt.date) else dt.date.fromisoformat(str(md)[:10]).year


def _window(path: str, since) -> tuple[int, int]:
    """Compute the [start_year, end_year] trailing window for the date= filter.

    start = (stored max year on disk) - LOOKBACK_YEARS, falling back to `since` then to a
    DEFAULT_WINDOW_YEARS tail. end = current calendar year + 1 (next year's early releases).
    The LOOKBACK re-pulls a couple of recent years so annual back-revisions land via merge.
    """
    this_year = dt.date.today().year
    end = this_year + 1

    smy = _stored_max_year(path)
    if smy is not None:
        start = smy - LOOKBACK_YEARS
    elif since:
        try:
            start = dt.date.fromisoformat(str(since)[:10]).year - LOOKBACK_YEARS
        except Exception:
            start = this_year - DEFAULT_WINDOW_YEARS
    else:
        start = this_year - DEFAULT_WINDOW_YEARS

    # Clamp: never below WDI's first year, never an inverted window.
    start = max(1960, min(start, end))
    return start, end


def _fetch_indicator(code: str, date_param: str, iso2to3: dict, tally: Tally,
                     keys: list, dates: list, vals: list) -> None:
    """Paginate one indicator over the date window; append parsed obs and tally the sub-unit.

    series_key = 'WDI:{code}:{code3}', code3 = countryiso3code if present else the 2->3 map.
    A row whose code can't be resolved AND whose value is null is skipped silently (those are
    null-valued aggregate placeholders the API pads windows with). A transient page failure
    flags the indicator transient; a fully parsed but data-empty window flags it empty.
    """
    page = 1
    n_added = 0
    saw_any_row = False
    while True:
        j = _get_json(f"{BASE}/country/all/indicator/{code}",
                      {"format": "json", "date": date_param, "per_page": PER_PAGE, "page": page})
        if j == "transient":
            tally.transient_unit()
            return
        if not isinstance(j, list) or len(j) < 2 or not j[1]:
            break
        rows = j[1]
        for x in rows:
            saw_any_row = True
            val = x.get("value")
            if val is None:
                continue
            code3 = x.get("countryiso3code") or iso2to3.get((x.get("country") or {}).get("id"))
            if not code3:
                continue
            try:
                v = float(val)
                yr = int(str(x.get("date")).strip())
            except (TypeError, ValueError):
                continue
            if v != v:  # NaN guard
                continue
            keys.append(f"WDI:{code}:{code3}")
            dates.append(dt.date(yr, 12, 31))
            vals.append(v)
            n_added += 1
        meta = j[0] or {}
        try:
            pages = int(meta.get("pages", 1))
        except (TypeError, ValueError):
            pages = 1
        if page >= pages:
            break
        page += 1
        time.sleep(RATE)

    if n_added > 0:
        tally.added_unit(n_added)
    else:
        # A real 200 envelope (saw rows or a clean empty list) with no usable obs in the
        # trailing window is a genuinely quiet sub-unit, not a failure.
        tally.empty_unit()


def _series_maxes(tbl):
    out = {}
    if tbl.num_rows == 0:
        return out
    for k, d in zip(tbl.column("series_key").to_pylist(), tbl.column("obs_date").to_pylist()):
        if d is None:
            continue
        if k not in out or d > out[k]:
            out[k] = d
    return {k: v.isoformat() for k, v in out.items()}


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "worldbank_wdi.parquet")
    before = blob.row_count(path)
    tally = Tally()

    # Reference map for blank-iso3 aggregates — a wholesale gate. Without it, income-group
    # rows would either be dropped or keyed wrong; treat its loss as transient (retry).
    iso2to3 = _country_map()
    if iso2to3 is None:
        tally.transient_unit()
        return finalize(tally, before, None, source=SOURCE)

    # Indicator list — also a wholesale gate.
    indicators = _list_indicators()
    if indicators == "transient":
        tally.transient_unit()
        return finalize(tally, before, None, source=SOURCE)
    if not indicators:
        # 200 but an empty indicator list from a real body -> structural break.
        tally.structural_unit()
        return finalize(tally, before, None, source=SOURCE)

    start, end = _window(path, since)
    date_param = f"{start}:{end}"

    # INSTRUMENTED, because this source consumes its ENTIRE per-source cap and nothing said
    # where the time went or whether the pass finished. Measured 2026-08-02 (CI run
    # 30738981790): worldbank_wdi ran exactly 2,700s — the 45-minute orchestrator cap — and
    # reported `ok`. Separately, NY.GDP.MKTP.CD is missing 2025 entirely (233 entities
    # upstream, 0 stored) while 189 indicators ranked LATER than it do have 2025, so the gap
    # is not prefix starvation and not the date window. Without per-indicator progress there
    # is no way to tell a completed pass from one the cap truncated — and a truncated pass
    # that reports `ok` is the same false green this codebase keeps finding (#64).
    #
    # A hard kill leaves no summary line, so progress is printed AS IT GOES; the absence of
    # the final "pass COMPLETE" line is then itself the signal that the cap fired.
    # BUDGET + ROTATION. Without these this source did 45 minutes of work and stored NOTHING,
    # on every run, forever.
    #
    # Every observation is accumulated in memory and merged ONCE after the loop. The
    # orchestrator's 45-minute hard cap therefore did not truncate the pass — it destroyed it:
    # the merge never executed, so nothing reached the store. Measured in CI run 30738981790:
    #     TIMEOUT worldbank_wdi/_all — exceeded its 45-minute hard limit and was interrupted
    #     took 2,700s, peak_rss=3,463MB, status `timeout`
    # and worldbank_wdi has NO unit_state row at all — it has never once succeeded. The data in
    # the store is what the original bulk ingest left, which is why NY.GDP.MKTP.CD sits without
    # 2025 while the World Bank publishes it (#64).
    #
    # A full pass cannot fit the cap anyway: measured locally, 1,000 of 1,498 indicators took
    # 42.5 minutes and the rate DEGRADES (0.5/s -> 0.4/s) because a single bad indicator costs
    # up to ~7 minutes in retries (4 attempts x 90s timeout plus backoff) before it gives up.
    #
    # So: stop starting new indicators at BUDGET_MIN, which is deliberately UNDER the
    # orchestrator's cap so the fetcher yields on its own terms and the merge below actually
    # runs; then resume past the bookmark next time so the tail is reached rather than
    # re-walked. A bound over a fixed order without rotation is a truncation, not a budget
    # (R190) — and this one was not even a truncation, it was a discard.
    dl = Deadline(minutes=BUDGET_MIN)
    order = rotate_after(list(indicators), load_rotation(out_dir))
    keys, dates, vals = [], [], []
    t0 = time.monotonic()
    done = 0
    capped = False
    last_code = None
    for n, code in enumerate(order, 1):
        if dl.spent():
            capped = True
            break
        _fetch_indicator(code, date_param, iso2to3, tally, keys, dates, vals)
        last_code = code
        # Written per sub-unit, not once at the end. The orchestrator's 45-minute cap
        # KILLS a source rather than breaking its loop, so an end-of-function save is
        # exactly what a kill destroys — which is why stat_estonia had never written a
        # _rotation.json at all while worldbank_wdi and hagstofa each wrote their first
        # one the moment they stopped being killed (R273). Relying on not being killed
        # to persist the state whose purpose is surviving a kill is circular.
        save_rotation(out_dir, code)
        done = n
        time.sleep(RATE)
        if n % 200 == 0:
            el = time.monotonic() - t0
            print(f"[{SOURCE}] {n:,}/{len(order):,} indicators in {el/60:.1f} min "
                  f"({n/max(el, 1):.1f}/s), {len(keys):,} obs so far", flush=True)
    if last_code:
        save_rotation(out_dir, last_code)                    # resume past here next run
    el = (time.monotonic() - t0) / 60
    if capped:
        print(f"[{SOURCE}] budget of {BUDGET_MIN} min spent after {el:.1f} min — "
              f"{done:,}/{len(order):,} indicators done, {len(order)-done:,} deferred to the "
              f"NEXT run (rotation bookmark saved); merging what was collected", flush=True)
    else:
        print(f"[{SOURCE}] pass COMPLETE: {len(order):,} indicators in {el:.1f} min",
              flush=True)

    tbl = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date":   pa.array(dates, pa.date32()),
        "value":      pa.array(vals, pa.float64()),
    })

    if tbl.num_rows == 0:
        # No new obs anywhere in the trailing window. finalize() decides: a large all-empty
        # window raises DefinitiveError (structural), a small/quiet one returns no_change.
        # Either way existing data is untouched (we never call merge with 0 rows).
        return finalize(tally, before, None, source=SOURCE)

    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    # Rebase the added count to ACTUAL new rows in the published file (merge dedups revisions
    # of already-stored cells away); keep transient/empty tallies for honest status.
    tally.added = max(0, n - before)
    return finalize(tally, n, md, source=SOURCE, series_cursors=_series_maxes(tbl))
