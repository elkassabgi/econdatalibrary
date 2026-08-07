"""S2 date-tail fetcher — World Bank ESG database (source=75). CC BY 4.0, no key.

Layout: one parquet per indicator, clean_full/worldbank_esg/<IND>.parquet (71 files, 461,719 rows),
columns (country, obs_date, value) — there is NO series_key column here, so the dedup grain is
("country", "obs_date"), scoped to the per-indicator file.

The v2 `date=YYYY:YYYY` filter is genuinely honoured server-side (verified: an indicator's
meta.total falls 17,160 -> 520 for date=2024:2026 -> 260 for 2025:2026, while an UNKNOWN control
param leaves it at 17,160 — proving the reduction is the filter working, not merely being
accepted). So instead of re-pulling every indicator's full history we read each file's stored max
year and request only [max_year - LOOKBACK .. current year + 1]; merge dedups the overlap and
never shrinks. The lookback absorbs the annual back-revisions WB publishes.

Country coding is reused from the ingester (build_code_map + the countryiso3code-else-name lookup)
so aggregate economies (ARB, WLD, SSF ...) resolve to exactly the codes already on disk — otherwise
the same economy would land under two identities and merge would not dedup it.

Store I/O via blob (R36). Serial with a small pause: the WB API is shared and this is only ~71
cheap windowed calls per tick.

HONEST-STATUS: the country reference map is a wholesale gate -> its loss is transient (partial,
retried, data kept). A per-indicator fetch failure -> transient_unit. A real 200 whose window holds
no observations -> empty_unit (a quiet year is normal for annual data). Cursors emitted (R41).
"""
from __future__ import annotations
import json
import datetime as dt
import os
import time

import pyarrow as pa
import pyarrow.compute as pc

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import (Deadline, Tally, finalize, load_rotation, rotate_after, sane_since,
                      save_rotation)
from jobs import ingest_worldbank_esg as ig   # reuse code map + json client + year parsing

# Minutes this source may spend before deferring the rest to the next tick.
# 20 leaves plenty of room inside the 300-minute job ceiling for other sources.
BUDGET_MIN = 20
SOURCE = "worldbank_esg"
DEDUP = ("country", "obs_date")        # no series_key column: identity is (country, obs_date)
LOOKBACK_YEARS = 3                     # absorb WB annual back-revisions
DEFAULT_WINDOW_YEARS = 6
RATE = 0.15


def _stored_max_year(path) -> int | None:
    if not blob.exists(path):
        return None
    t = blob.read_table(path, columns=["obs_date"])
    if t.num_rows == 0:
        return None
    md = pc.max(t.column("obs_date")).as_py()
    md = sane_since(md) if md is not None else None      # defuse corrupt far-future stamps
    if md is None:
        return None
    return md.year if isinstance(md, dt.date) else None


def _window(path, since) -> str:
    this_year = dt.date.today().year
    end = this_year + 1
    if not blob.exists(path):
        # An indicator we do not hold AT ALL wants its whole history, not a tail. Without
        # this it would fall through to the `since`/default window below and we would ingest
        # a new indicator already truncated to the last few years — silently, since a short
        # series looks the same as a short-lived one.
        return f"1960:{end}"
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
    start = max(1960, min(start, end))
    return f"{start}:{end}"


def _published_indicators():
    """The indicator ids the publisher CURRENTLY lists under source 75, or None if the
    listing itself could not be read.

    This is the authoritative answer to "does this indicator still exist" — a structured
    fact, not an error message. Returning None on failure is load-bearing: an unreadable
    listing must never be read as "everything is archived", which would retire the whole
    source in one run.
    """
    out, page = set(), 1
    while True:
        try:
            j = ig.get_json(f"{ig.API}/sources/{ig.WB_SOURCE}/indicators"
                            f"?format=json&per_page=200&page={page}")
        except Exception:                                          # noqa: BLE001
            return None
        if not isinstance(j, list) or len(j) < 2 or not isinstance(j[0], dict):
            return None
        out |= {i.get("id") for i in (j[1] or []) if isinstance(i, dict) and i.get("id")}
        try:
            pages = int(j[0].get("pages", 1))
        except (TypeError, ValueError):
            pages = 1
        if page >= pages:
            return out
        page += 1


def _retired_path(out_dir):
    """Indicators the publisher has archived. Blob-routed like every other sidecar (R36):
    a local write is scratch on a CI runner and would be re-learned every single run."""
    return os.path.join(out_dir, "_retired.json")


def _fetch_window(ind_id, date_param):
    """Windowed pull for one indicator -> list[record]. Raises TransientError on failure."""
    url = (f"{ig.API}/country/all/indicator/{ind_id}"
           f"?source={ig.WB_SOURCE}&format=json&per_page=20000&date={date_param}")
    try:
        j = ig.get_json(url)
    except Exception as e:
        # get_json raises RuntimeError when retries are exhausted AND (now) immediately on
        # a permanent 4xx. Neither was caught here before, so a single bad indicator took
        # the WHOLE source down instead of being isolated. Per the honest-status contract
        # this is one transient sub-unit: the other ~70 indicators still publish, the run
        # reports partial, and this one is retried next tick.
        raise TransientError(f"worldbank_esg: {ind_id} window fetch failed: {e}")
    if not j:
        raise TransientError(f"worldbank_esg: {ind_id} window fetch failed")
    # A message envelope is the API COMPLAINING, never data — and it is a 1-element list, so
    # the `len(j) < 2` fallthrough below used to book it as an empty window, which the caller
    # then recorded as `empty_unit` ("a quiet window is normal for annual data"). That is how
    # 13 archived indicators reported healthy for weeks.
    #
    # It is NOT decided here whether the indicator is archived. The message id for a deleted
    # indicator depends on the URL — 175 without `source=`, 120 ("The provided parameter value
    # is not valid") WITH it — and 120 is also what an ordinary bad parameter returns. Keying a
    # permanent retirement on that string would retire live indicators on a transient mistake.
    # Retirement is decided in update() against the publisher's own indicator LIST, which is a
    # structured fact rather than a formatted sentence (the R142 lesson).
    if isinstance(j, list) and j and isinstance(j[0], dict) and j[0].get("message"):
        msgs = j[0]["message"] or []
        raise TransientError(f"worldbank_esg: {ind_id} API message {str(msgs[:1])[:110]}")
    if not isinstance(j, list) or len(j) < 2:
        # a real 200 whose envelope is not [meta, rows] -> treat as empty window, not a break
        return []
    return j[1] or []


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    tally = Tally()

    # wholesale gate: without the code map, aggregate economies would be keyed by NAME and
    # would not dedup against the codes already on disk.
    try:
        code_map = ig.build_code_map()
    except Exception as e:
        raise TransientError(f"worldbank_esg: country reference map unavailable: {e}")
    if not code_map:
        raise TransientError("worldbank_esg: country reference map came back empty")

    files = blob.list_parquets(out_dir)
    if not files:
        raise DefinitiveError("worldbank_esg: no indicator parquets on the store")

    # R190. THE DEFERRAL BELOW SAYS "the rest drain next tick" AND THEY DID NOT. blob.list_parquets
    # sorts, so every run started at AG.* and stopped at the same place, and the indicators past
    # that point were deferred FOREVER while the run honestly reported `partial`. Measured on the
    # store 2026-08-03: 39 of 71 files carried recent write times and the other 32 — CC.EST,
    # EN.ATM.CO2E.PC, EN.ATM.METH.PC, EN.ATM.NOXE.PC, EN.CLC.GHGR.MT.CE, EN.POP.DNST, GE.EST,
    # IC.LGL.CRED.XQ and the rest of the alphabet — still sat at 2026-06-30, their first-pass
    # ingest date. Never updated once. The tail is contiguous and alphabetical, which is the
    # signature.
    #
    # Resuming past the last one worked on makes the deferral true. An unknown or empty bookmark
    # degrades to "start at the top", so a first run or a renamed indicator skips nothing.
    # Drop indicators the publisher has ARCHIVED before anything else looks at the list.
    # Not attempted, so they cannot be retried forever, cannot be counted as healthy-empty,
    # and — the sharp edge — cannot trip finalize()'s "all attempted sub-units returned
    # empty => likely a structural break" guard when a rotation slice happens to be all
    # archived. Disclosed every run: a silently shorter work list reads as full coverage.
    retired = set()
    _raw = blob.read_bytes(_retired_path(out_dir))
    if _raw:
        try:
            retired = set(json.loads(_raw.decode("utf-8")) or [])
        except (ValueError, UnicodeDecodeError):
            retired = set()

    # Re-derive retirement from the publisher's CURRENT listing. Additive both ways: an
    # indicator that reappears upstream is un-retired, so a publisher restoring something is
    # not permanently ignored by us. If the listing cannot be read we keep the stored set and
    # change nothing — an unreadable listing must never read as "everything is archived".
    published = _published_indicators()
    if published:
        ours = {f[:-len(".parquet")] for f in files}
        fresh = ours - published
        back = retired & published
        if fresh != (retired & ours) or back:
            retired = (retired | fresh) - back
            blob.write_bytes_atomic(
                _retired_path(out_dir),
                json.dumps(sorted(retired), separators=(",", ":")).encode("utf-8"))
        if back:
            print(f"[worldbank_esg] {len(back)} indicator(s) are published again, "
                  f"un-retired: {sorted(back)[:6]}", flush=True)
    else:
        print("[worldbank_esg] source-75 indicator listing unreadable this run — keeping the "
              "stored retired set unchanged, retiring nothing new", flush=True)

    # NEWLY PUBLISHED indicators. The work list is built from `blob.list_parquets(out_dir)`
    # — the files we ALREADY have — so an indicator the World Bank adds is never fetched, no
    # matter how long it is scheduled. Measured 2026-08-07: 22 of the publisher's 80 source-75
    # indicators had no file here, and nothing in the loop could ever create one. Adding them
    # by name gives `_window` a non-existent path, which now means "pull the full history".
    if published:
        new_inds = sorted(published - {f[:-len(".parquet")] for f in files} - retired)
        if new_inds:
            print(f"[worldbank_esg] {len(new_inds)} newly published indicator(s) not held "
                  f"yet, queued for a full-history pull: {new_inds[:8]}"
                  + (" ..." if len(new_inds) > 8 else ""), flush=True)
            files = files + [f"{i}.parquet" for i in new_inds]

    if retired:
        before = len(files)
        files = [f for f in files if f[:-len(".parquet")] not in retired]
        print(f"[worldbank_esg] {before - len(files)} indicator(s) archived upstream, skipped "
              f"(data kept, can never refresh): {sorted(retired)[:8]}"
              + (" ..." if len(retired) > 8 else ""), flush=True)

    files = rotate_after(files, load_rotation(out_dir))

    cursors: dict[str, str] = {}
    maxd = None
    total = 0

    # Wall-clock budget. The reused ingester's get_json retries 6x at a 120 s timeout plus
    # ~61 s of backoff — ~13 min per URL — and this walks ~71 indicators, so a flaky WB day
    # is a multi-hour source. Since orchestrate.py runs sources SERIALLY, that stalls every
    # source behind it and can run the job into its 300-minute ceiling. Measured: a local
    # run sat here 39 minutes at 0.16 GB RSS (hung on IO, not memory) before I killed it.
    deadline = Deadline(minutes=BUDGET_MIN)
    capped = False

    for i, fn in enumerate(files):
        if deadline.spent():
            # Stop starting NEW indicators; the rest drain next tick. Same contract as
            # ons_uk's MAX_PER_RUN cap — nothing is skipped silently, the run reports
            # partial and the unit vintage is not advanced.
            capped = True
            # Count the deferred indicators' EXISTING rows toward the total. `total` is
            # reported as the source's row count, not this run's slice, so breaking early
            # without this would look like the source had suddenly shrunk to a fraction of
            # its size — a false alarm in exactly the reporting I spent today fixing.
            deferred = files[i:]
            total += sum(blob.row_count(os.path.join(out_dir, f)) for f in deferred)
            print(f"[worldbank_esg] budget of {BUDGET_MIN} min spent after "
                  f"{deadline.elapsed_min():.1f} min; deferring {len(deferred)} "
                  f"of {len(files)} indicators to the next tick (resuming after "
                  f"{files[i - 1] if i else '(none)'})", flush=True)
            break
        # AFTER the deferral check, never before: the bookmark means "the last indicator this run
        # actually WORKED ON". Stamped above the check it would record one that was deferred, and
        # the next run — starting just past it — would skip the very indicator the deferral
        # promised to return to.
        #
        # Written per indicator rather than once at the end, because the end is not guaranteed to
        # be reached: the orchestrator's 45-minute cap KILLS a source rather than breaking its
        # loop, and a kill before an end-of-function save loses the bookmark entirely — which is
        # why twelve of fourteen rotating sources have never persisted one (R273). One small
        # write per indicator, against ~71 of them, buys immunity from that.
        save_rotation(out_dir, fn)
        ind_id = fn[:-len(".parquet")]
        path = os.path.join(out_dir, fn)
        date_param = _window(path, since)
        try:
            rows = _fetch_window(ind_id, date_param)
        except TransientError as e:
            # Name the indicator and the cause. Unlabelled, an 8-of-9 failure reads only
            # "8/9 sub-unit(s) transient-failed; will retry" — a number with nothing to act on.
            tally.transient_unit(f"{ind_id}: {str(e)[-70:]}")
            total += blob.row_count(path)
            time.sleep(RATE)
            continue

        countries, dates, vals = [], [], []
        for r in rows:
            v = r.get("value")
            if v is None:
                continue
            od = ig.year_to_date(r.get("date"))
            if od is None:
                continue
            code = r.get("countryiso3code") or ""
            if not code:
                nm = (r.get("country") or {}).get("value", "").strip()
                code = code_map.get(nm, "") or nm or "UNKNOWN"
            try:
                fv = float(v)
            except (TypeError, ValueError):
                continue
            countries.append(code); dates.append(od); vals.append(fv)

        if not vals:
            tally.empty_unit()            # quiet window is normal for annual data
            total += blob.row_count(path)
            time.sleep(RATE)
            continue

        tbl = pa.table({
            "country": pa.array(countries, pa.string()),
            "obs_date": pa.array(dates, pa.date32()),
            "value": pa.array(vals, pa.float64()),
        })
        before = blob.row_count(path)
        try:
            n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
        except DefinitiveError as e:
            # A merge guard trip (never-shrink etc.) is a DIFFERENT failure from a fetch
            # failure, and unlabelled they are indistinguishable in the state row.
            tally.transient_unit(f"{ind_id}: merge guard — {str(e)[-60:]}")
            total += before
            time.sleep(RATE)
            continue
        total += n
        tally.added_unit(max(0, n - before))
        # cursor key must match the catalog grain for this source: <indicator>:<country>
        for c, d in zip(countries, dates):
            k = f"{ind_id}:{c}"
            iso = d.isoformat()
            if k not in cursors or iso > cursors[k]:
                cursors[k] = iso
        if md and (maxd is None or str(md) > str(maxd)):
            maxd = md
        time.sleep(RATE)

    res = finalize(tally, total, maxd or (since or None), source=SOURCE,
                   series_cursors=cursors, empty_window_floor=len(files) + 1)
    if capped:
        # Indicators still owe work — do NOT stamp a "fully current" vintage or the
        # remainder would be skipped on the next tick instead of resumed.
        res.new_vintage = None
    return res
