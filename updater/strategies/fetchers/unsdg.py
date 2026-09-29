"""S1 fetcher — UN Sustainable Development Goals (SDG) database (713 series).

CC BY 4.0 (UN Statistics Division). Single grouped parquet
clean_full/unsdg/unsdg.parquet, schema (series_key, obs_date, value), annual
(Dec-31), series_key = '<seriesCode>:<geoAreaCode>[|Dim=val|...]' (up to 3 sorted
non-trivial dimensions) — exactly as built by jobs/ingest_unsdg.py.

The SDG API exposes NO global since/updatedAfter filter, and the UNSD re-estimates
+ extends history at each quarterly release, so the correct refresh is to re-fetch
the whole table per series and MERGE (dedup series_key+obs_date, revised values win,
never-shrink). Change-detection is the release tag the API stamps on every series in
Series/List (currently '2026.Q1.G.02'); a content hash of all (code,release) pairs
moves iff any series got a new release -> exactly the "history was re-estimated"
signal. One cheap ~180 KB GET, no auth.

Each series is a sub-unit: 200-with-records that parse >0 obs -> added/empty;
200/4xx that yields 0 records from a real query -> structural; 429/5xx/network ->
transient. A full pull is ~713 paginated GETs (the SDG API is slow, ~7-9 s/page),
so a per-run series budget is honored from unit.config['max_series'] /
$UNSDG_MAX_SERIES (default: all 713). A bounded run is a legitimate partial refresh:
merge unions the re-fetched series with the untouched ones and never shrinks.
"""
from __future__ import annotations
import datetime as dt
import json
import os
import sys
import time

import pyarrow as pa
import requests

from ... import config, merge, blob
from ...errors import DefinitiveError
from ..base import Result
from ._common import Tally, finalize, load_rotation, save_rotation, rotate_after, Deadline, RotationCycle
from ._vintage import content_hash, UA as _UA

SOURCE = "unsdg"
DEDUP = ("series_key", "obs_date")

BASE = "https://unstats.un.org/sdgapi/v1/sdg"
UA = {**_UA, "Accept": "application/json"}
PAGE = 1000
RATE = 0.5          # polite delay between series (matches the ingester)
PAGE_RETRIES = 4

# SELF-BOUNDING default (R243): ~200 codes x ~8.5s/code ~= 28 min, safely under the
# orchestrator's 45-min unit kill. With the rotation bookmark, 4 runs cover all 713
# and the release-tag vintage gates re-pulls between releases. Overridable via
# unit.config['max_series'] or $UNSDG_MAX_SERIES; 0/empty = all (manual backfills).
MAX_SERIES_DEFAULT = 200
# TIME self-bound (R243, measured run 31132634539): the alphabetical head codes are
# giants (AG_LND_* paginate 30+ pages at 7-9s each), so a code-count bound cannot
# guarantee staying under the orchestrator's 45-min kill. The Deadline is checked
# between codes; on expiry the current chunk flushes, the bookmark saves, and the
# remainder tallies as deferred (honest partial, R303).
TIME_BUDGET_MIN = 35


def _get_json(url, params=None, retries=PAGE_RETRIES):
    """Return (json, outcome) where outcome is 'ok' (200 parsed), 'missing'
    (400/404/other 4xx -> structural for the sub-unit), or 'transient'
    (timeout/5xx/429/network -> retry next tick)."""
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=UA, timeout=120)
        except (requests.Timeout, requests.ConnectionError):
            if attempt >= retries - 1:
                return None, "transient"
            time.sleep(3 * (attempt + 1))
            continue
        if r.status_code == 200:
            try:
                return r.json(), "ok"
            except ValueError:
                return None, "missing"  # 200 with non-JSON body -> structural
        if r.status_code in (400, 404):
            return None, "missing"
        if r.status_code in (429, 500, 502, 503, 504):
            if attempt >= retries - 1:
                return None, "transient"
            time.sleep(60 if r.status_code == 429 else 5 * (attempt + 1))
            continue
        return None, "missing"  # other 4xx
    return None, "transient"


def _series_list():
    """Return (series_list, outcome). The release-tagged catalog is also the
    vintage source, so we fetch it once and reuse it."""
    data, outcome = _get_json(f"{BASE}/Series/List")
    if outcome != "ok":
        return None, outcome
    return (data if isinstance(data, list) else []), "ok"


def current_vintage(unit):
    """Cheap probe: a content hash over the sorted (seriesCode, release) pairs from
    Series/List. Changes iff any of the 713 series got a new release tag -> the
    exact signal that the SDG data vintage moved. Returns None if the catalog can't
    be fetched cheaply (strategy then fetches anyway; merge dedups + never-shrinks)."""
    series, outcome = _series_list()
    if outcome != "ok" or not series:
        return None
    return current_token(series)


def current_token(series) -> "str | None":
    """The release token over one Series/List - ONE function for the probe and the cycle's stamp, so the
    two can never be spelled differently. None when no series carries a release tag: a token over codes
    alone would match for ever and seal the unit (the unctad R1154 guard)."""
    if not any(str(s.get("release") or "").strip() for s in series):
        return None
    pairs = sorted((str(s.get("code", "")), str(s.get("release", ""))) for s in series)
    return content_hash("".join(f"{c}={r};" for c, r in pairs).encode("utf-8"))


def _stored_codes(path, n_rows) -> set:
    """The series codes the store holds rows for: the '<code>:' prefix of every series_key. Read once per
    pass (one column of a ~14 MB file). An unreadable store gives an empty set, which falls back to the
    all-attempted-empty rule - never to silence."""
    if not n_rows:
        return set()
    try:
        import pyarrow.compute as pc                                  # noqa: PLC0415
        col = pc.unique(blob.read_table(path, columns=["series_key"]).column("series_key"))
        return {str(k).split(":", 1)[0] for k in col.to_pylist() if k} - {""}
    except Exception as e:                                           # noqa: BLE001
        print(f"[unsdg] could not read the store's series codes ({type(e).__name__}: {e}); the outage "
              f"check falls back to the all-attempted-empty rule", flush=True)
        return set()


CYCLE_TOKEN_FILE = "_cycle_token.json"
MIXED = "mixed"
CANARIES = 3                         # known-present codes tried before an outage verdict (see update)


def _mark_mixed(out_dir) -> str:
    """Mark the current cycle's token MIXED: the cycle may close but can never claim a release."""
    path = os.path.join(out_dir, CYCLE_TOKEN_FILE)
    try:
        blob.write_bytes_atomic(path, json.dumps({"token": MIXED}).encode("utf-8"))
    except Exception as e:                                           # noqa: BLE001
        print(f"[unsdg] could not save {path} ({type(e).__name__}: {e})", flush=True)
    return MIXED


def _cycle_token(out_dir, token, first_pass_of_cycle) -> "str | None":
    """The release token the current cycle was swept under: recorded at a cycle's first pass, MIXED once
    the release moves mid-cycle (see update). Blob-routed like the cycle file. A read error re-records
    nothing and returns MIXED: the close then stamps the placeholder - one extra sweep, never a skip."""
    path = os.path.join(out_dir, CYCLE_TOKEN_FILE)
    try:
        raw = blob.read_bytes(path)
        prev = json.loads(raw.decode("utf-8")).get("token") if raw else None
    except Exception:                                                # noqa: BLE001
        prev = MIXED
    if first_pass_of_cycle:
        new = token
    elif prev == token:
        return token
    else:
        new = MIXED
    try:
        blob.write_bytes_atomic(path, json.dumps({"token": new}).encode("utf-8"))
    except Exception as e:                                           # noqa: BLE001
        print(f"[unsdg] could not save {path} ({type(e).__name__}: {e}); this cycle stamps no token",
              flush=True)
        return MIXED
    return new


def _parse_records(records, code):
    """Reuse the ingester's exact key/value/date construction."""
    keys, dates, vals = [], [], []
    for rec in records:
        geo = str(rec.get("geoAreaCode", rec.get("geoAreaName", "WLD")))
        time_period = rec.get("timePeriodStart") or rec.get("timePeriod")
        val_raw = rec.get("value")
        if val_raw is None or str(val_raw).strip() in ("", "N/A", "null", "None"):
            continue
        try:
            v = float(str(val_raw).replace(",", ""))
        except (ValueError, TypeError):
            continue
        if time_period is None:
            continue
        try:
            yr = int(str(time_period).split(".")[0])
            obs_date = dt.date(yr, 12, 31)
        except (ValueError, TypeError):
            continue
        dims = rec.get("dimensions", {})
        dim_str = ""
        if isinstance(dims, dict) and dims:
            dim_parts = [f"{k}={dv}" for k, dv in sorted(dims.items())
                         if dv and dv not in ("", "_T", "ALLAREA", "G")]
            if dim_parts:
                # Carry ALL non-trivial dimensions, not just the first 3. Several SDG
                # series expose 4+ disaggregating dimensions (e.g. SE_ADT_ACTS has
                # Age|Location|Sex|Type-of-skill|Reporting-Type); truncating to [:3]
                # dropped "Type of skill" and collapsed up to 36 distinct values onto
                # one (series_key, obs_date), so the merge dedup shrank the store 11%
                # and tripped never-shrink. Full dimensions make the key unique.
                dim_str = "|" + "|".join(dim_parts)
        keys.append(f"{code}:{geo}{dim_str}")
        dates.append(obs_date)
        vals.append(v)
    return keys, dates, vals


def _fetch_series(code):
    """Fetch all observations for one series across all pages. Returns
    (keys, dates, vals, outcome) where outcome is 'ok' (>=1 page parsed),
    'missing' (200/4xx but no records from a real query -> structural), or
    'transient' (timeout/5xx/429/network on any page -> retry next tick)."""
    keys, dates, vals = [], [], []
    page = 1
    got_any = False
    while True:
        data, outcome = _get_json(f"{BASE}/Series/Data",
                                  params={"seriesCode": code, "pageSize": PAGE, "page": page})
        if outcome == "transient":
            # A mid-pagination transient means this series is incomplete; surface it
            # as transient (do not publish a truncated series as success).
            return keys, dates, vals, "transient"
        if outcome == "missing":
            break
        if isinstance(data, dict):
            records = data.get("data", [])
            total_pages = data.get("totalPages", 1)
        elif isinstance(data, list):
            records = data
            total_pages = 1
        else:
            break
        if not records:
            break
        got_any = True
        k, d, v = _parse_records(records, code)
        keys.extend(k); dates.extend(d); vals.extend(v)
        if page >= (total_pages or 1):
            break
        page += 1
    return keys, dates, vals, ("ok" if got_any else "missing")


def _series_maxes(keys, dates):
    out = {}
    for k, d in zip(keys, dates):
        if d is None:
            continue
        if k not in out or d > out[k]:
            out[k] = d
    return {k: v.isoformat() for k, v in out.items()}


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "unsdg.parquet")
    before = blob.row_count(path)
    tally = Tally()

    cfg = unit.config or {}
    budget = int(cfg.get("max_series")
                 or os.environ.get("UNSDG_MAX_SERIES", MAX_SERIES_DEFAULT) or 0)

    series, outcome = _series_list()
    if outcome != "ok":
        # Can't even list series -> whole pull is transient; existing data untouched.
        tally.transient_unit("Series/List")
        return finalize(tally, before, None, source=SOURCE)
    if not series:
        tally.structural_unit("Series/List parsed 0 series")  # real catalogue break
        return finalize(tally, before, None, source=SOURCE)

    codes = [s.get("code") for s in series if s.get("code")]
    listed = list(codes)                # the whole current Series/List (the canaries come from it)
    total = len(codes)
    # THE ROTATION CYCLE (R303; the shared RotationCycle of #66). `ok` means "every listed code was
    # visited since the last ok", never "this pass stopped somewhere" - and before this, never either:
    # every pass deferred ~550-670 of the 713 codes to its budget and read `partial`, so unsdg had
    # NEVER succeeded (runbook, 2026-09-08) and a never-succeeded unit is due only once per ~6.3 days
    # (base.is_due's PARTIAL_RETRY path). The 35-min deadline, not the 200-code budget, ends each pass
    # (46-171 codes per ~2,130 s pass in state.db), so a sweep is ~11 passes: ~70 days to the FIRST ok.
    # Now a pass skips codes visited this cycle, books the unvisited rest as deferred, and the pass
    # that completes the cycle cleanly reads ok. After that first ok the unit is on base.is_due's fast
    # branch (due every run until the cycle closes).
    cycle = RotationCycle(out_dir, codes)
    # THE CYCLE'S RELEASE TOKEN (review R1284 defect 2). The closing pass must hand the orchestrator the
    # Series/List release token, or it stores finalize's "date-tail", the probe never matches, and every
    # due tick starts a fresh ~11-pass cycle against one UNSD release a quarter. But a token read only at
    # the close would claim codes fetched passes earlier under an older release. So the token is recorded
    # when a cycle STARTS (nothing visited yet) and marked MIXED if the release moves mid-cycle; the close
    # stamps it only if it is still the current one. A mixed cycle closes with the placeholder, and the
    # next due tick re-sweeps under one release.
    token = current_token(series)
    first_pass_of_cycle = not cycle.visited
    cycle_token = _cycle_token(out_dir, token, first_pass_of_cycle)
    # ROTATION (R190): Series/List order is stable, so a budget over it re-walks the
    # same prefix forever and the tail never refreshes. Resume just after where the
    # last run stopped; the bookmark is saved after every merged CHUNK below, so a
    # kill costs at most one in-flight chunk of progress, never the rotation.
    codes = [c for c in rotate_after(codes, load_rotation(out_dir)) if not cycle.done(c)]
    if budget > 0 and len(codes) > budget:
        codes = codes[:budget]          # the rest stay unvisited: booked as deferred at the end

    # CHUNKED PUBLISH (R249): the old accumulate-then-merge made any kill a total
    # discard — fatal for a ~713x8s full pull under the 45-min cap. Merging every
    # CHUNK series turns a kill into truncation: everything merged so far survives
    # and the bookmark resumes the tail next run.
    CHUNK = 50
    all_cursors: dict = {}
    n, md = before, None
    merged_any = False
    keys, dates, vals = [], [], []
    # codes fetched since the last flush, with whether their fetch failed. A code is VISITED only once
    # its rows are MERGED: marked at fetch time, a kill before the chunk's merge would record work as
    # done that never reached the store.
    pending: list = []
    fetched: list = []                  # every code fetched this pass, with whether it failed
    empty_codes: set = set()            # codes that came back with no data this pass
    stored_codes = _stored_codes(path, before)   # codes the store holds rows for (the outage baseline)

    def _visit_pending():
        nonlocal pending
        for c, failed in pending:
            cycle.visit(c, failed=failed)
        pending = []

    def _flush(last_code):
        nonlocal n, md, merged_any, keys, dates, vals
        if not keys:
            save_rotation(out_dir, last_code)
            _visit_pending()
            return True
        tbl = pa.table({"series_key": pa.array(keys, pa.string()),
                        "obs_date": pa.array(dates, pa.date32()),
                        "value": pa.array(vals, pa.float64())})
        # Atomic merge per chunk: dedup on series_key+obs_date, revised values win,
        # never-shrink guard intact (min_ratio unchanged) — see the 2026-07
        # key-uniqueness note in git history for why the effective key is unique.
        n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
        merged_any = True
        all_cursors.update(_series_maxes(keys, dates))
        keys, dates, vals = [], [], []
        save_rotation(out_dir, last_code)
        _visit_pending()
        return True

    dl = Deadline(TIME_BUDGET_MIN)
    stopped_at = None
    for i, code in enumerate(codes):
        if dl.spent():
            stopped_at = i
            break
        try:
            k, d, v, outc = _fetch_series(code)
        except Exception as e:                                       # noqa: BLE001
            # A broken stream (ChunkedEncodingError, ContentDecodingError - not ConnectionErrors) or a
            # record our parser cannot read escaped update() before any flush: the bookmark never moved
            # and every pass re-asked the same codes and stored nothing (reviews R1284/R1286, the R1114
            # class). It is a failure of THIS code - tallied, retried, quarantined after two - EXCEPT the
            # orchestrator's own unit timeout, which must end the pass (R1114: swallowing it ran the
            # fetcher past its window).
            orch = sys.modules.get("updater.orchestrate")
            if orch is not None and (getattr(orch, "UNIT_TIMEOUT_FIRED", False) or
                                     isinstance(e, getattr(orch, "UnitTimeout", ()))):
                raise
            print(f"[unsdg] {code}: {type(e).__name__}: {str(e)[:120]}", flush=True)
            k, d, v, outc = [], [], [], "transient"
        pending.append((code, outc == "transient"))
        fetched.append((code, outc == "transient"))
        if outc == "transient":
            tally.transient_unit(code)
        elif outc == "missing" or not k:
            # A single listed code with no data is a SUB-UNIT gap, not a source break
            # (R44 — faostat's per-domain structural_unit vetoed whole sources).
            # A wholesale outage is judged after the loop, by vanished codes.
            tally.empty_unit(code)
            empty_codes.add(code)
        else:
            keys.extend(k); dates.extend(d); vals.extend(v)
            tally.added_unit(len(k), code)
        if (i + 1) % CHUNK == 0:
            try:
                _flush(code)
            except DefinitiveError as e:
                return Result(status="partial", obs=n, last_obs_date=md,
                              new_vintage=None, series_cursors=all_cursors or None,
                              error=("merge refused (existing data kept, guard "
                                     f"intact): {e}"))
        time.sleep(RATE)

    if codes:
        last_visited = codes[(stopped_at - 1) if stopped_at else -1] if (stopped_at is None or stopped_at > 0) else None
        try:
            if last_visited is not None:
                _flush(last_visited)
        except DefinitiveError as e:
            return Result(status="partial", obs=n, last_obs_date=md,
                          new_vintage=None, series_cursors=all_cursors or None,
                          error=("merge refused (existing data kept, guard "
                                 f"intact): {e}"))

    # Every code not yet visited THIS CYCLE - the deadline's tail, the budget's tail, and any whose
    # fetch failed - is deferred (-> partial); with none owed and no failure the cycle closes (-> ok).
    # Unlabelled, as before: deferral is the budget working, not a failure (R303), and naming ~500
    # codes would bury the sub-units that actually broke.
    # A code whose fetch failed THIS pass is already tallied as transient; it is not deferred as well.
    #
    # A WHOLESALE OUTAGE is judged on EVERY pass, by VANISHED codes: codes the store holds rows for that
    # came back empty. finalize's all-empty floor judged the attempted set, which skipping visited codes
    # shrinks - round 1 raised on a later pass that merely held empty codes (review R1284), and round 2's
    # first-pass-only rule let an outage that began mid-cycle close the cycle and SEAL the release token
    # while most codes were never refreshed (review R1286). A code the store holds that the publisher
    # suddenly serves empty is the outage signal; a code that was always empty is not. More than 10 such
    # codes, and every stored code this pass attempted among them, is an outage: nothing this pass did
    # counts as a visit, the cycle cannot close, and the pass raises. A store with no rows at all (a
    # first ingest) falls back to the old all-attempted-empty rule.
    stored_tried = [c for c, failed in fetched if not failed and c in stored_codes]
    vanished = [c for c in stored_tried if c in empty_codes]
    outage = ((len(vanished) > 10 and len(vanished) == len(stored_tried)) or
              (not stored_codes and tally.added == 0 and tally.revised == 0
               and tally.empty == tally.attempted and tally.attempted > 10))
    if outage and vanished:
        # A KNOWN-PRESENT CANARY before the verdict (review R1290; R316). Vanished codes are also what a
        # block of codes UNSD has BLANKED looks like - adjacent families such as SG_DMK_PARL* can fill a
        # closing pass - and calling that an outage un-visits them every pass: the cycle never closes and
        # every other code is skipped for ever (R1111's freeze). Up to CANARIES stored codes outside this
        # pass are asked: any data back means the API is serving, so these codes were blanked (visited as
        # empty, the cycle proceeds); nothing back from all of them means the outage is real.
        # CANDIDATES ARE STILL LISTED (review R1292): the store never forgets a code, so a code UNSD
        # retired (dropped from Series/List) or a blanked code sorting first would be the canary for ever
        # and bring the freeze back. Only codes in THIS Series/List are known-present candidates (R316),
        # and several are tried so one blanked candidate cannot decide it.
        seen_now = {c for c, _ in fetched}
        candidates = [c for c in sorted(stored_codes & set(listed)) if c not in seen_now][:CANARIES]
        for canary in candidates:
            try:
                ck, _cd, _cv, coutc = _fetch_series(canary)
            except Exception as e:                                   # noqa: BLE001
                orch = sys.modules.get("updater.orchestrate")
                if orch is not None and (getattr(orch, "UNIT_TIMEOUT_FIRED", False) or
                                         isinstance(e, getattr(orch, "UnitTimeout", ()))):
                    raise
                ck, coutc = [], "transient"
            if ck and coutc not in ("transient", "missing"):
                print(f"[unsdg] {len(vanished)} stored code(s) came back empty but the canary {canary} "
                      f"returned data: they were blanked by UNSD, not an outage - visited as empty",
                      flush=True)
                outage = False
                break
    if outage:
        cycle.forget([c for c, _failed in fetched])      # nothing was refreshed: they stay owed
        raise DefinitiveError(
            f"unsdg: all {len(vanished) or tally.attempted} stored series code(s) attempted this pass came back "
            f"empty ({', '.join((vanished or [c for c, _ in fetched])[:5])} ...) - a wholesale outage, not "
            f"retired series; existing data kept, the codes stay owed and no release is claimed")
    floor = 10 ** 9                  # the outage is judged above, by vanished codes - never by finalize
    # A SUSPECT PASS CANNOT HELP SEAL A RELEASE (review R1286). A pass of more than 10 codes that added
    # nothing is either legitimately empty codes or an outage over codes the store never held (a first
    # sweep) - the two look the same from here. Either way the cycle may still close, but it may not
    # claim the release: its token is marked MIXED, the close keeps the placeholder, and the next due
    # tick re-sweeps.
    if tally.attempted > 10 and tally.added == 0 and tally.revised == 0 and cycle_token != MIXED:
        cycle_token = _mark_mixed(out_dir)
    closed = cycle.close_if_complete(tally)
    if not closed:
        failed_now = {c for c, failed in fetched if failed}
        for c in cycle.unvisited():
            if c not in failed_now:
                tally.deferred_unit()

    if not merged_any:
        # Nothing merged this pass (every code attempted was empty or failed, and it was no outage).
        res = finalize(tally, before, None, source=SOURCE, empty_window_floor=floor)
    else:
        print(f"[unsdg] merged {len(all_cursors):,} refreshed keys across "
              f"{min(len(codes), total):,}/{total} series codes; store now {n:,} rows",
              flush=True)
        res = finalize(tally, n, md, source=SOURCE, series_cursors=all_cursors or None,
                       empty_window_floor=floor)
    # The closing pass hands the orchestrator the cycle's release token - only if the whole cycle was
    # swept under the release that is still current (see _cycle_token); otherwise finalize's placeholder
    # stands and the next due tick re-sweeps.
    if closed and res.status in ("ok", "no_change") and token is not None and cycle_token == token:
        res.new_vintage = token
    return res
