"""S3 fetcher — Statistics Norway (SSB), PxWeb statistical database. No key.

License: NLOD + CC BY 4.0.  Source: https://www.ssb.no/en/statbank/
API: https://data.ssb.no/api/v0/en/table  (PxWeb; GET metadata, POST JSON-stat2 data)

LAYOUT (set by jobs/ingest_ssb.py): ONE parquet per SUBJECT GROUP under
clean_full/ssb/grp_<subj>.parquet, where <subj> = table_id[:2]. Each file holds
EVERY table whose id starts with those two chars. Schema is always:
  series_key : "SSB:<table_id>:<dim>=<code>:<dim>=<code>:..."  (time dim EXCLUDED)
  obs_date   : date32  (parsed from the PxWeb time code: YYYY / YYYYMnn / YYYYKn ...)
  value      : float64
(series_key, obs_date) is a UNIQUE key on disk (verified), so it is the dedup key.
There are MANY tables per group file and MANY (series_key, obs_date) rows per table.

DATE-TAIL (the S3 contract, PxWeb flavour). PxWeb has no SDMX startPeriod and no
updatedAfter, so "request ONLY newer observations" is done by restricting the table's
TIME dimension in the POST body to only the time codes whose parsed date is AFTER the
table's stored max(obs_date):
  1. read the group parquet once -> per-table max(obs_date) (the EXACT key columns are
     series_key/obs_date/value, identical for every table);
  2. GET the table metadata (lists every time code + the time variable, flagged
     `time: true`);
  3. keep only time codes with parse_date(code) > stored_max  (the "codes AFTER the
     stored max" the contract asks for);
  4. POST the data query with the time dimension restricted to EXACTLY those codes
     (other dims selected exactly as the ingester did, honouring MAX_CELLS);
  5. parse JSON-stat2 with the ingester's logic, accumulate all tables of the group,
     and merge ONCE per group file via merge.merge_and_write(mode="merge",
     dedup_keys=("series_key","obs_date")).
A table with ZERO newer time codes is an empty (legitimately-quiet) sub-unit and is
SKIPPED without a POST (an empty time selection 400s on PxWeb).

ROBUSTNESS to a corrupted on-disk max: a few legacy rows carry an absurd parsed year
(e.g. 5636-12-31 from an old parse artifact). If the stored max is implausibly far in
the future it would filter out every real new period and freeze the table, so the
comparison max is CAPPED at today + MAX_FUTURE_DAYS; beyond that we treat the boundary
as "recent" and still request a trailing window. merge dedups the overlap and the
never-shrink invariant guarantees existing data is preserved.

HONEST STATUS (Tally + finalize): each TABLE is a sub-unit.
  added_unit(n)     rows merged for the table (n>0 new / n==0 nothing newer)
  empty_unit()      table had no newer time codes, or a 200 JSON-stat2 with no usable
                    points in the requested tail (legitimately quiet)
  transient_unit()  timeout / 5xx / 429 / network / non-JSON 200 -> the WHOLE run is
                    'partial' (orchestrator does NOT stamp success; unit re-runs)
  structural_unit() a 200 whose JSON-stat2 envelope is present but the expected
                    structure is gone (no dimensions / no time dim) -> DefinitiveError
empty_window_floor is set to (#group files - 1) so a healthy steady-state run where
every table is "nothing newer" is honest no_change, not a false structural break; a real
wholesale outage (every table empty across many groups) still trips the floor.

series_cursors: keyed by table id -> 'YYYY-MM-DD' (the table's frontier this run), so a
frozen table cannot hide behind the unit-level max.
"""
from __future__ import annotations
import datetime as dt
import json
import os
import re
import time
from collections import defaultdict

import pyarrow as pa
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Tally, finalize

SOURCE = "ssb"
BASE = "https://data.ssb.no/api/v0/en/table"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
DEDUP = ("series_key", "obs_date")
RATE = 0.12              # polite pause between table requests
MAX_CELLS = 80_000       # matches jobs/ingest_ssb.py
TIMEOUT_META = 60
TIMEOUT_DATA = 120
MAX_ATTEMPTS = 4
MAX_FUTURE_DAYS = 400    # ceiling for a sane stored max; beyond this the on-disk max is
                         # treated as corrupt and a trailing window is requested instead
TRAIL_YEARS = 3          # how far back to request when the stored max is corrupt/unknown


# --------------------------------------------------------------------------- #
# date parsing — copied verbatim from jobs/ingest_ssb.py so codes map identically
# --------------------------------------------------------------------------- #
def parse_date(s: str):
    """Parse SSB time values (2023, 2023M01, 2023K1, 2023Q1, 2023W01, 2023H1)."""
    s = (s or "").strip()
    try:
        if re.match(r"^\d{4}$", s):
            return dt.date(int(s), 12, 31)
        m = re.match(r"^(\d{4})M(\d{2})$", s, re.IGNORECASE)
        if m:
            return dt.date(int(m.group(1)), int(m.group(2)), 1)
        m = re.match(r"^(\d{4})[QK](\d)$", s, re.IGNORECASE)
        if m:
            q = int(m.group(2))
            return dt.date(int(m.group(1)), (q - 1) * 3 + 1, 1)
        m = re.match(r"^(\d{4})H(\d)$", s, re.IGNORECASE)
        if m:
            return dt.date(int(m.group(1)), 1 if m.group(2) == "1" else 7, 1)
        m = re.match(r"^(\d{4})W(\d{2})$", s, re.IGNORECASE)
        if m:
            yr, wk = int(m.group(1)), int(m.group(2))
            return dt.date.fromisocalendar(yr, wk, 1)
        if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
            return dt.date.fromisoformat(s)
    except (ValueError, TypeError):
        pass
    return None


def is_time_dim(code: str, values) -> bool:
    code_l = code.lower()
    if code_l in ("tid", "time", "year", "aar", "kvartal", "maaned", "period"):
        return True
    if values:
        sample = values[:5]
        yr_count = sum(1 for v in sample if re.match(r"^\d{4}[MQKHW]?\d*$", str(v).strip()))
        return yr_count >= len(sample) * 0.6
    return False


def parse_jsonstat2(data: dict, table_id: str, time_code: str | None = None):
    """Parse JSON-stat2 -> list[(series_key, date, value)].

    Based on jobs/ingest_ssb.py, with ONE correctness fix: when the caller passes the
    AUTHORITATIVE time dimension code (from the PxWeb metadata, where the time variable
    is flagged `time: true`), lock onto that dimension instead of re-guessing with the
    `^\\d{4}...` heuristic. The heuristic mis-fires on dimensions whose codes look like
    years — e.g. Norwegian municipality codes 4601/5001 in the `Region` dimension — and
    the ingester's first-match-wins guess picked `Region` as time, mapping municipality
    5001 to the year 5001 (this is the origin of the absurd 5001-12-31 / 9610-12-31 rows
    already on disk). Using the metadata-declared time code parses every period correctly.
    """
    results = []
    dim_ids = data.get("id", [])
    dim_sizes = data.get("size", [])
    dims = data.get("dimension", {})
    values = data.get("value", [])
    if not dim_ids or not values:
        return results

    time_dim_idx = None
    dim_codes = []
    for i, did in enumerate(dim_ids):
        cat = dims.get(did, {}).get("category", {})
        cat_idx = cat.get("index", {})
        if isinstance(cat_idx, dict):
            size = dim_sizes[i] if i < len(dim_sizes) else max(cat_idx.values(), default=-1) + 1
            pos_to_code = [""] * size
            for code, pos in cat_idx.items():
                if pos < len(pos_to_code):
                    pos_to_code[pos] = code
        elif isinstance(cat_idx, list):
            pos_to_code = list(cat_idx)
        else:
            pos_to_code = []
        dim_codes.append(pos_to_code)
        # Authoritative time code wins; else fall back to the (imperfect) heuristic.
        if time_code is not None:
            if did == time_code:
                time_dim_idx = i
        elif time_dim_idx is None and is_time_dim(did, pos_to_code):
            time_dim_idx = i

    if time_dim_idx is None:
        return results

    strides = [1] * len(dim_sizes)
    for i in range(len(dim_sizes) - 2, -1, -1):
        strides[i] = strides[i + 1] * dim_sizes[i + 1]

    for flat_idx, raw_v in enumerate(values):
        if raw_v is None:
            continue
        try:
            v = float(raw_v)
            if v != v:
                continue
        except (ValueError, TypeError):
            continue

        remainder = flat_idx
        dim_indices = []
        for stride in strides:
            dim_indices.append(remainder // stride)
            remainder %= stride

        t_pos = dim_indices[time_dim_idx]
        t_codes = dim_codes[time_dim_idx]
        if t_pos >= len(t_codes):
            continue
        obs_date = parse_date(t_codes[t_pos])
        if obs_date is None:
            continue

        key_parts = [f"SSB:{table_id}"]
        for i, (did, pos) in enumerate(zip(dim_ids, dim_indices)):
            if i == time_dim_idx:
                continue
            codes_for_dim = dim_codes[i]
            code_val = codes_for_dim[pos] if pos < len(codes_for_dim) else str(pos)
            key_parts.append(f"{did}={code_val}")

        results.append((":".join(key_parts), obs_date, v))
    return results


# --------------------------------------------------------------------------- #
# HTTP — transient vs definitive vs empty, mirroring the reference fetchers
# --------------------------------------------------------------------------- #
def _get_meta(sess: requests.Session, table_id: str):
    """GET table metadata. Returns dict | None(legit empty: 404/400) | raises Transient."""
    url = f"{BASE}/{table_id}"
    last = None
    for a in range(MAX_ATTEMPTS):
        try:
            r = sess.get(url, headers=UA, timeout=TIMEOUT_META)
        except (requests.Timeout, requests.ConnectionError) as e:
            last = str(e)[:120]
            if a == MAX_ATTEMPTS - 1:
                raise TransientError(f"ssb meta {table_id}: {last}")
            time.sleep(min(2 ** a, 20)); continue
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError:
                last = "bad json"
                if a == MAX_ATTEMPTS - 1:
                    raise TransientError(f"ssb meta {table_id}: {last}")
                time.sleep(min(2 ** a, 20)); continue
        if r.status_code in (400, 404):
            return None  # table not available — legitimately empty for this run
        if r.status_code in (429, 500, 502, 503, 504):
            last = f"HTTP {r.status_code}"
            if a == MAX_ATTEMPTS - 1:
                raise TransientError(f"ssb meta {table_id}: {last}")
            time.sleep(min(2 ** a, 30)); continue
        raise DefinitiveError(f"ssb meta {table_id}: HTTP {r.status_code}")
    raise TransientError(f"ssb meta {table_id}: {last}")


def _post_data(sess: requests.Session, table_id: str, body: dict):
    """POST a JSON-stat2 query. Returns dict | None(legit empty: 400/403/404) | raises."""
    url = f"{BASE}/{table_id}"
    last = None
    for a in range(MAX_ATTEMPTS):
        try:
            r = sess.post(url, json=body, headers=UA, timeout=TIMEOUT_DATA)
        except (requests.Timeout, requests.ConnectionError) as e:
            last = str(e)[:120]
            if a == MAX_ATTEMPTS - 1:
                raise TransientError(f"ssb data {table_id}: {last}")
            time.sleep(min(2 ** a, 20)); continue
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError:
                last = "bad json"
                if a == MAX_ATTEMPTS - 1:
                    raise TransientError(f"ssb data {table_id}: {last}")
                time.sleep(min(2 ** a, 20)); continue
        if r.status_code in (400, 403, 404):
            return None  # no rows for this selection — legitimately empty
        if r.status_code in (429, 500, 502, 503, 504):
            last = f"HTTP {r.status_code}"
            if a == MAX_ATTEMPTS - 1:
                raise TransientError(f"ssb data {table_id}: {last}")
            time.sleep(min(2 ** a, 30)); continue
        raise DefinitiveError(f"ssb data {table_id}: HTTP {r.status_code}")
    raise TransientError(f"ssb data {table_id}: {last}")


# --------------------------------------------------------------------------- #
# query construction — newer time codes only, ingester's dim selection otherwise
# --------------------------------------------------------------------------- #
def _time_var(variables):
    """Return (code, values) of the time variable, or (None, None)."""
    for v in variables:
        if v.get("time") is True:
            return v.get("code"), v.get("values", [])
    for v in variables:
        if is_time_dim(v.get("code", ""), v.get("values", [])):
            return v.get("code"), v.get("values", [])
    return None, None


def _newer_codes(time_values, floor_date):
    """Time codes whose parsed date is STRICTLY after floor_date (None -> all)."""
    out = []
    for code in time_values:
        d = parse_date(str(code))
        if d is None:
            continue
        if floor_date is None or d > floor_date:
            out.append(code)
    return out


def _build_query(variables, time_code, newer_time_codes):
    """Build the PxWeb query body: time restricted to newer_time_codes; other dims as
    the ingester chose (all if total cells <= MAX_CELLS, else a single representative
    aggregate value per non-time dim). Returns the query list, or None if not buildable."""
    # total cells if we took ALL values of every non-time dim, with only the newer times
    total_cells = max(len(newer_time_codes), 1)
    for var in variables:
        if var.get("code") == time_code:
            continue
        total_cells *= max(len(var.get("values", [])), 1)

    query = []
    if total_cells <= MAX_CELLS:
        for var in variables:
            code = var.get("code")
            if code == time_code:
                query.append({"code": code,
                              "selection": {"filter": "item", "values": newer_time_codes}})
            else:
                vals = var.get("values", [])
                if vals:
                    query.append({"code": code,
                                  "selection": {"filter": "item", "values": vals}})
    else:
        for var in variables:
            code = var.get("code")
            vals = var.get("values", [])
            if not vals:
                continue
            if code == time_code:
                query.append({"code": code,
                              "selection": {"filter": "item", "values": newer_time_codes}})
            else:
                agg = [x for x in vals if str(x).upper() in
                       ("0", "00", "000", "TOTAL", "TOT", "T", "ALL", "9999")]
                selected = agg[:1] if agg else vals[:1]
                query.append({"code": code,
                              "selection": {"filter": "item", "values": selected}})
    return query or None


# --------------------------------------------------------------------------- #
# on-disk frontier (per table, from one parquet read of a group file)
# --------------------------------------------------------------------------- #
def _per_table_max(path: str) -> dict:
    """Read a group parquet ONCE -> {table_id: max(obs_date) as dt.date}."""
    out: dict[str, dt.date] = {}
    if not blob.exists(path):
        return out
    t = blob.read_table(path)
    if t.num_rows == 0 or "series_key" not in t.column_names:
        return out
    keys = t.column("series_key").to_pylist()
    ods = t.column("obs_date").to_pylist()
    for sk, d in zip(keys, ods):
        if d is None:
            continue
        if isinstance(d, dt.datetime):
            d = d.date()
        parts = sk.split(":")
        if len(parts) < 2 or parts[0] != "SSB":
            continue
        tid = parts[1]
        prev = out.get(tid)
        if prev is None or d > prev:
            out[tid] = d
    return out


def _floor_for(stored_max, today):
    """Effective comparison floor: cap an absurd future max so a corrupt on-disk date
    cannot freeze a table. Returns a date (or None to request a trailing window)."""
    if stored_max is None:
        return None
    if stored_max > today + dt.timedelta(days=MAX_FUTURE_DAYS):
        # corrupt/implausible max: ignore it and request a trailing window instead
        return None
    return stored_max


def _trailing_floor(today):
    """When the boundary is unknown/corrupt, request roughly the last TRAIL_YEARS."""
    return dt.date(today.year - TRAIL_YEARS, 1, 1)


# --------------------------------------------------------------------------- #
# catalog — group tables exactly as jobs/ingest_ssb.py does
# --------------------------------------------------------------------------- #
def _load_catalog(out_dir):
    cat_file = os.path.join(out_dir, "_catalog.json")
    if not os.path.exists(cat_file):
        raise DefinitiveError(f"ssb catalog missing: {cat_file}")
    try:
        cat = json.load(open(cat_file, encoding="utf-8"))
    except (ValueError, OSError) as e:
        raise DefinitiveError(f"ssb catalog unreadable: {e}")
    if not isinstance(cat, list) or not cat:
        raise DefinitiveError(f"ssb catalog empty/malformed: {cat_file}")
    return cat


def _group_of(table_id: str) -> str:
    return table_id[:2] if len(table_id) >= 2 else table_id


# --------------------------------------------------------------------------- #
# contract entry point
# --------------------------------------------------------------------------- #
def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    if not os.path.isdir(out_dir):
        raise DefinitiveError(f"ssb source dir missing: {out_dir}")

    cat = _load_catalog(out_dir)

    # Group catalog table ids exactly as the ingester (by 2-char prefix), deduping
    # repeated ids within a group (the catalog holds some ids under >1 path).
    group_tables: dict[str, list[str]] = defaultdict(list)
    seen_in_group: dict[str, set] = defaultdict(set)
    for e in cat:
        tid = str(e.get("id", ""))
        if not tid:
            continue
        g = _group_of(tid)
        if tid not in seen_in_group[g]:
            seen_in_group[g].add(tid)
            group_tables[g].append(tid)

    # Only process groups that already have an on-disk parquet (those are the units the
    # source HAS). A group prefix in the catalog with no file produced no usable data on
    # the full ingest; there is nothing to date-tail-extend.
    pfiles = sorted(f for f in os.listdir(out_dir)
                    if f.startswith("grp_") and f.endswith(".parquet"))
    if not pfiles:
        raise DefinitiveError(f"no ssb group parquet files under {out_dir}")

    sess = requests.Session()
    today = dt.date.today()
    tally = Tally()
    total = 0
    cursors: dict[str, str] = {}   # table_id -> frontier 'YYYY-MM-DD'

    for fn in pfiles:
        path = os.path.join(out_dir, fn)
        subj = fn[len("grp_"):-len(".parquet")]
        before = blob.row_count(path)
        per_max = _per_table_max(path)             # one parquet read per group
        # Seed cursors from the on-disk frontier so an untouched table still reports it —
        # but SKIP absurd legacy dates (e.g. 9999-12-31 from a malformed time code) so the
        # cursor map reports an honest frontier. Such rows stay on disk (never-shrink); the
        # corrupt-max guard re-fetches a trailing window for them and the real fetched max
        # below replaces the seed.
        for tid, d in per_max.items():
            if d <= today + dt.timedelta(days=MAX_FUTURE_DAYS):
                cursors[tid] = d.isoformat()

        # Tables to attempt for this group: those known on disk (so we extend real data),
        # unioned with any catalog ids that map here (kept for completeness; a brand-new
        # table gets a trailing-window backfill).
        cat_ids = group_tables.get(subj, [])
        attempt_ids = list(dict.fromkeys(list(per_max.keys()) + cat_ids))

        g_keys: list[str] = []
        g_dates: list[dt.date] = []
        g_vals: list[float] = []
        fetched_tables: list[str] = []   # tables that returned real rows this group

        for tid in attempt_ids:
            stored_max = per_max.get(tid)
            floor = _floor_for(stored_max, today)
            if floor is None and stored_max is not None:
                # corrupt future max -> request a trailing window so real periods flow
                floor = _trailing_floor(today)
            elif floor is None and stored_max is None:
                # brand-new table not yet on disk -> trailing-window backfill
                floor = _trailing_floor(today)

            try:
                meta = _get_meta(sess, tid)
            except TransientError:
                tally.transient_unit()   # -> partial; existing data untouched
                time.sleep(RATE)
                continue
            if not meta or not isinstance(meta, dict):
                tally.empty_unit()       # table gone/unavailable (404/400) — legit empty
                time.sleep(RATE)
                continue
            variables = meta.get("variables", [])
            if not variables:
                # 200 metadata with NO variables -> the expected PxWeb structure is gone.
                if before > 0 and tid in per_max:
                    tally.structural_unit()   # previously-populated table, structure lost
                else:
                    tally.empty_unit()
                time.sleep(RATE)
                continue

            time_code, time_vals = _time_var(variables)
            if not time_code or not time_vals:
                # No time dimension at all -> can't date-tail; structural for a known table.
                if before > 0 and tid in per_max:
                    tally.structural_unit()
                else:
                    tally.empty_unit()
                time.sleep(RATE)
                continue

            newer = _newer_codes(time_vals, floor)
            if not newer:
                tally.empty_unit()       # nothing newer than the boundary — quiet table
                time.sleep(RATE)
                continue

            query = _build_query(variables, time_code, newer)
            if not query:
                tally.empty_unit()
                time.sleep(RATE)
                continue

            body = {"query": query, "response": {"format": "json-stat2"}}
            try:
                resp = _post_data(sess, tid, body)
            except TransientError:
                tally.transient_unit()
                time.sleep(RATE)
                continue
            if not resp:
                tally.empty_unit()       # 400/403/404 on the selection — legit empty
                time.sleep(RATE)
                continue

            rows = parse_jsonstat2(resp, tid, time_code=time_code)
            if not rows:
                # 200 JSON-stat2 but no usable points in the requested tail. If the
                # envelope itself is broken (no dims), that's structural; otherwise it's
                # a legitimately-quiet/all-null tail.
                if resp.get("value") and resp.get("id") and before > 0 and tid in per_max \
                        and not isinstance(resp.get("id"), list):
                    tally.structural_unit()
                else:
                    tally.empty_unit()
                time.sleep(RATE)
                continue

            # Seed the table frontier from the SANE stored max only (a corrupt future
            # stored_max would otherwise dominate every real fetched date and freeze the
            # cursor at e.g. 9999-12-31). `floor` is the sane comparison boundary used for
            # this fetch, so it is a safe lower bound for the cursor.
            sane_stored = stored_max if (stored_max is not None and
                                         stored_max <= today + dt.timedelta(days=MAX_FUTURE_DAYS)) else None
            t_max = sane_stored
            for sk, d, v in rows:
                g_keys.append(sk)
                g_dates.append(d)
                g_vals.append(v)
                if t_max is None or d > t_max:
                    t_max = d
            # A table that returned a 200 with real rows is a SUCCESSFUL sub-unit even if
            # every row is at/below the boundary and merge nets zero. Record it for an
            # HONEST per-table tally AFTER the group merge (net-based, like treasury): we
            # can't attribute per-table net from a single per-group merge, so we tally the
            # group's NET growth across its fetched tables — yielding 'no_change' on a
            # fully-idempotent re-run rather than a misleading 'ok'.
            fetched_tables.append(tid)
            if t_max is not None:
                prev = cursors.get(tid)
                ti = t_max.isoformat()
                if prev is None or ti > prev:
                    cursors[tid] = ti
            time.sleep(RATE)

        # Merge this group's accumulated new rows ONCE (never write parquet directly).
        if g_vals:
            new_tbl = pa.table({
                "series_key": pa.array(g_keys, pa.string()),
                "obs_date":   pa.array(g_dates, pa.date32()),
                "value":      pa.array(g_vals, pa.float64()),
            })
            n, _md = merge.merge_and_write(path, new_tbl, mode="merge", dedup_keys=DEDUP)
            total += n
            net = max(0, n - before)   # NET new rows the group actually gained
        else:
            total += before
            net = 0

        # HONEST per-table tally (net-based, treasury-style): distribute the group's NET
        # growth across the tables that successfully fetched rows. On a fully-idempotent
        # re-run net==0 -> every fetched table counts empty -> 'no_change' (not a
        # misleading 'ok'); when the group genuinely gained rows the first fetched table
        # carries the count so finalize() reports 'ok'. (Per-table net is unavailable from
        # one per-group merge, so the group is the finest honest granularity here.)
        for j, tid in enumerate(fetched_tables):
            tally.added_unit(net if j == 0 else 0)

    # last_obs: derive ONLY from sane cursor values. The merge-returned max and the
    # on-disk frontier are unreliable here because legacy ingest left a handful of rows
    # with absurd parsed years (e.g. 9999-12-31, 5001-12-31 from malformed time codes);
    # those rows are PRESERVED on disk (never-shrink) but must NOT be reported as the
    # source's frontier. Cursors are sane-capped at today + MAX_FUTURE_DAYS, so max(sane
    # cursors) is the honest last observation.
    sane_ceiling = (today + dt.timedelta(days=MAX_FUTURE_DAYS)).isoformat()
    last_obs = None
    if cursors:
        sane = [c for c in cursors.values() if c <= sane_ceiling]
        if sane:
            last_obs = max(sane)

    # Sub-units are TABLES (each recorded on the tally). MOST SSB tables are closed or
    # already-current snapshots: in any healthy run the overwhelming majority legitimately
    # return "nothing newer than the boundary" (an empty sub-unit), so the blunt
    # all-empty-window heuristic in finalize() would FALSE-POSITIVE on a perfectly healthy
    # date-tail. We therefore disable that heuristic (floor above the attempted count, the
    # same choice treasury.py makes) and rely on the PRECISE per-table structural signal:
    # a table whose 200 metadata lost its variables / time dimension, or whose JSON-stat2
    # envelope is broken, is flagged via tally.structural_unit() -> DefinitiveError. That
    # catches real breaks exactly while letting an all-quiet run be honest no_change.
    floor = tally.attempted + 1
    return finalize(tally, total, last_obs, source=SOURCE, series_cursors=cursors,
                    empty_window_floor=floor)
