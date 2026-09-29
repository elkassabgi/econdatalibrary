#!/usr/bin/env python3
"""Statistics Denmark (DST/Danmarks Statistik) — full StatBank ingest.

License: Creative Commons Attribution 4.0 (CC BY 4.0)
Source: https://www.dst.dk/en/Statistik/statistikbanken
No API key required.

Coverage: ~2,300 tables in the Danish StatBank:
  * National accounts (GDP, GNI, output)
  * Labour force (employment, unemployment, wages)
  * Consumer and producer prices
  * Trade in goods and services
  * Population and demography
  * Housing prices and construction
  * Financial statistics
  * Business statistics
  * Energy statistics

API: GET  /v1/tables?lang=en          → list all tables
     GET  /v1/tableinfo?id=X&lang=en  → table metadata (variables + values)
     POST /v1/data                    → data in JSON-stat format

Run: python jobs/ingest_dst.py
"""
from __future__ import annotations
import datetime as dt, json, os, re, time
from collections import defaultdict
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
import sys as _sys
# The shared value-first time-axis resolver lives in THIS module's own repo (jobs/ and
# core/ are siblings). Derive the repo root from __file__ so `from core import pxweb`
# resolves both when this file is run standalone AND when the fetcher importlib-loads it
# (same convention as updater/config.py and tools/pxweb_regression.py). The hardcoded ROOT
# above is the DATA tree, which does not carry core/pxweb.py on this branch.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in _sys.path:
    _sys.path.insert(0, _REPO_ROOT)
from core import pxweb as _pxweb
OUT  = os.path.join(ROOT, "data", "clean_full", "dst")
BASE = "https://api.statbank.dk/v1"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
RATE = 0.3
MAX_CELLS = 50_000  # DST has ~100K limit; use 50K for safety


def log(m):
    try:
        print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
    except UnicodeEncodeError:
        print(f"[{time.strftime('%H:%M:%S')}] {str(m).encode('ascii','replace').decode()}", flush=True)


def get_json(url: str, retries: int = 3) -> dict | list | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=60)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 404):
                return None
            if r.status_code == 429:
                log("  429 throttle, sleeping 30s"); time.sleep(30); continue
            log(f"  HTTP {r.status_code}: {url[-80:]}")
        except Exception as e:
            log(f"  ERR: {e}")
        time.sleep(5 * (attempt + 1))
    return None


def post_json(url: str, body: dict, retries: int = 3) -> dict | None:
    for attempt in range(retries):
        try:
            r = requests.post(url, json=body, headers=UA, timeout=120)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 403, 404):
                return None
            if r.status_code == 429:
                log("  429 throttle, sleeping 30s"); time.sleep(30); continue
            log(f"  POST HTTP {r.status_code}: {url[-60:]}")
        except Exception as e:
            log(f"  POST ERR: {e}")
        time.sleep(5 * (attempt + 1))
    return None


def parse_date(s: str) -> dt.date | None:
    """Parse DST time values: 2023, 2023M01, 2023Q1, 2023H1, 2023W01."""
    s = (s or "").strip()
    try:
        if re.match(r"^\d{4}$", s):
            return dt.date(int(s), 12, 31)
        m = re.match(r"^(\d{4})M(\d{2})$", s, re.IGNORECASE)
        if m:
            return dt.date(int(m.group(1)), int(m.group(2)), 1)
        m = re.match(r"^(\d{4})Q(\d)$", s, re.IGNORECASE)
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
        # DAILY: "2022M04D01" (DNINDEX, DNVALD - 2026-09-29). Real bodies of 1,121 non-null values parsed to 0
        # rows and were booked as legitimately EMPTY tables, advancing the manifest. Dated to the day itself,
        # the convention cso's identical grammar uses (R299); an impossible day ("M02D30") stays None.
        m = re.match(r"^(\d{4})M(\d{2})D(\d{2})$", s, re.IGNORECASE)
        if m:
            return dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except (ValueError, TypeError):
        pass
    return None


# Codes that literally name a DST time dimension. A real time axis (DST always uses
# "Tid") must win over any non-time category whose numeric codes happen to look date-ish.
TIME_CODES = ("tid", "time", "year", "aar", "år", "periode", "period", "datum",
              "maaned", "måned", "maned", "kvartal", "uge", "week", "month", "quarter")


def is_time_dim(code: str, values: list[str]) -> bool:
    """RETAINED BUT NO LONGER CALLED. Time-axis selection now goes through the
    shared value-first resolver core.pxweb.resolve_time_dim (see parse_jsonstat),
    which takes the JSON-stat `dimension.role.time` array as its authoritative
    step (DST always populates it with "Tid") and value-parses before any name
    match. Kept defined for parity with sibling ingesters and because the
    pxweb_regression superset check re-derives per-source time-name tokens from
    this file's TIME_CODES tuple. DST's over-cap branch (query_table) uses the
    tableinfo `time` flag, not this function.

    A dimension is 'time' only if its code names a time dim, OR its values parse via
    parse_date() to a SANE year (~1900..current_year+2). The old fallback matched
    ^\\d{4}[MQHW]?\\d*$ against the first code, which over-matched non-time numeric
    category/ContentsCode codes (e.g. '00000858', a category code '2584', municipality
    codes) and treated them as the time axis, writing garbage obs_dates like 2584-12-31.
    Anchoring on parse_date() + a sane year range removes that false match (out-of-range
    years are rejected; 8-digit codes don't parse to a date at all)."""
    if str(code).strip().lower() in TIME_CODES:
        return True
    if values:
        sample = [str(v).strip() for v in values[:8]]
        cur = dt.date.today().year
        sane = sum(1 for v in sample
                   if (d := parse_date(v)) is not None and 1900 <= d.year <= cur + 2)
        if sample and sane >= max(1, int(len(sample) * 0.6)):
            return True
    return False


def parse_jsonstat(data: dict, table_id: str) -> list[tuple[str, dt.date, float]]:
    """Parse JSON-stat (v1) format from DST.

    Time-axis selection goes through the shared value-first resolver
    (core/pxweb.py): authoritative JSON-stat role.time first (DST always
    populates it with "Tid"; note v1 nests `role` under `dimension`, so it is
    read from dim_obj, NOT the response root as in JSON-stat2), else highest
    date-parse-rate, else literal name as a last resort. Value-first stops a
    month axis (non-date codes) from outranking a year axis when role.time is
    absent — the old name-first fallback picked the month dim of a month+year
    cube and produced 0 rows. DST is code-coded: the dates live in the
    category.index CODES (the key/date assembly below reads dim_codes), so
    dim_codes is what the resolver scores."""
    results = []
    try:
        ds = data.get("dataset", data)  # handle both root and wrapped
        dim_obj = ds.get("dimension", {})
        dim_ids = dim_obj.get("id", [])
        dim_sizes = dim_obj.get("size", [])
        role = dim_obj.get("role", {})
        metric_dims = set(role.get("metric", []))
        values = ds.get("value", [])

        if not dim_ids or not values:
            return results

        # Build dimension code maps
        dim_codes = []
        dim_labels = []
        for i, did in enumerate(dim_ids):
            dim_info = dim_obj.get(did, {})
            cat = dim_info.get("category", {})
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
            # Some tables index the time axis POSITIONALLY ("0","1","2"…) and carry the real
            # period only in the category label. A code-only date lookup then parses nothing,
            # every observation is skipped, and a good 200 yields zero rows — which the fetcher
            # reports as a structural break. Keep the labels as a date fallback; the KEY still
            # uses codes, so no existing series_key changes.
            lab = cat.get("label", {})
            dim_labels.append([lab.get(c, "") if isinstance(lab, dict) else ""
                               for c in pos_to_code])

        # Pick the time dimension via the shared value-first resolver (core/pxweb.py):
        # authoritative role.time (v1: nested under `dimension`, hence role.get("time")
        # rather than _pxweb.role_time_of(data)), else highest date-parse-rate, else name.
        time_dim_idx = _pxweb.resolve_time_dim(dim_ids, dim_codes, meta_time_code=None, role_time=role.get("time"), parse_fn=parse_date)

        if time_dim_idx is None:
            return results

        # Compute strides
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
                # positional-index time codes: the period is in the label (see above)
                t_labels = dim_labels[time_dim_idx] if time_dim_idx < len(dim_labels) else []
                if t_pos < len(t_labels):
                    obs_date = parse_date(t_labels[t_pos])
            if obs_date is None:
                continue

            key_parts = [f"DST:{table_id}"]
            for i, (did, pos) in enumerate(zip(dim_ids, dim_indices)):
                if i == time_dim_idx:
                    continue
                if did in metric_dims:
                    continue  # ContentCode is redundant when there's only one metric
                codes_for_dim = dim_codes[i]
                code_val = codes_for_dim[pos] if pos < len(codes_for_dim) else str(pos)
                key_parts.append(f"{did}={code_val}")

            results.append((":".join(key_parts), obs_date, v))

    except Exception as e:
        log(f"  JSON-stat parse error for {table_id}: {e}")
    return results


def query_table(table_id: str, variables: list[dict]) -> list[tuple[str, dt.date, float]]:
    """Fetch data for one DST table (rows only - the bulk path's contract, unchanged)."""
    return query_table_detailed(table_id, variables)[0]


# DST answers HTTP 500 to a data POST with too many TIME values even when MAX_CELLS holds (DNVALD, 2026-09-29:
# 12,551 daily values -> 500 every time; the last 50 or 500 -> 200). A POST that fails is retried in time
# chunks of this size before the table is called failed.
TIME_CHUNK = 1000
_SPAN_PERIOD = re.compile(r"^(\d{4})\s*[:\-/]\s*(\d{4})$")


def query_table_detailed(table_id: str, variables: list[dict]) -> "tuple[list[tuple[str, dt.date, float]], str]":
    """(rows, outcome). outcome is one of
        ok            rows parsed
        no_selection  the table offers nothing to select - legitimately empty
        failed        the data POST failed (5xx/timeout after retries, 400/403/404) even in time chunks - the
                      PUBLISHER's bad hour, retried next run; NEVER a quiet table (review R1283: it was booked
                      empty and the manifest advanced, so the release was skipped until DST republished)
        all_null      a real body whose every value is missing ('..') - legitimately empty
        span_time     every time code is a multi-year window ('2007:2009') - a convention not yet chosen, like
                      cso's span_time; nothing is stored
        unparsed      a real body with values that parsed to 0 rows - OUR gap, never self-heals"""
    selection = _query_selection(variables)
    if not selection:
        return [], "no_selection"
    resp = _post_data(table_id, selection)
    if resp is None:
        tvar = next((s for s in selection if s.get("_time")), None)
        if tvar is None or len(tvar["values"]) <= TIME_CHUNK:
            return [], "failed"
        rows = []
        for i in range(0, len(tvar["values"]), TIME_CHUNK):
            part = [dict(s, values=tvar["values"][i:i + TIME_CHUNK]) if s is tvar else s for s in selection]
            chunk = _post_data(table_id, part)
            if chunk is None:
                return [], "failed"
            got = parse_jsonstat(chunk, table_id)
            if not got:
                why = _why_empty(chunk)
                if why == "all_null":
                    continue                 # an all-missing stretch (holidays, a gap) - the other chunks stand
                return [], why
            rows.extend(got)
        return (rows, "ok") if rows else ([], "all_null")
    rows = parse_jsonstat(resp, table_id)
    if rows:
        return rows, "ok"
    return [], _why_empty(resp)


def _post_data(table_id, selection):
    body = {"table": table_id, "format": "JSONSTAT", "lang": "en",
            "variables": [{"code": s["code"], "values": s["values"]} for s in selection]}
    resp = post_json(f"{BASE}/data", body)
    time.sleep(RATE)
    return resp or None


def _why_empty(resp) -> str:
    """Name a real body that yielded no rows: the publisher's empty table, an undated window, or our gap."""
    try:
        ds = resp.get("dataset", resp)
        vals = ds.get("value")
        vlist = list(vals.values()) if isinstance(vals, dict) else list(vals or [])
        if not vlist or all(v is None for v in vlist):
            return "all_null"
        dims = ds.get("dimension") or {}
        role = (dims.get("role") or ds.get("role") or {}).get("time") or []
        tdim = next(iter(role), None)
        if tdim is not None:
            idx = (dims.get(tdim) or {}).get("category", {}).get("index")
            codes = list(idx) if isinstance(idx, (dict, list)) else []
            spans = [_SPAN_PERIOD.match(str(c)) for c in codes]
            if codes and all(m and int(m.group(2)) > int(m.group(1)) + 1 for m in spans):
                return "span_time"
    except (AttributeError, TypeError, ValueError):
        pass
    return "unparsed"


def _query_selection(variables: list[dict]) -> list[dict]:
    """The select-all query (the old query_table's body logic, unchanged) as [{code, values, _time}]."""
    # Build select-all query
    total_cells = 1
    var_selection = []
    time_var_ids = []

    for var in variables:
        vid = var["id"]
        is_time = var.get("time", False)
        vals = [v["id"] for v in var.get("values", [])]
        if not vals:
            continue
        total_cells *= len(vals)
        if is_time:
            time_var_ids.append(vid)

    # If too large, restrict non-time vars to first/aggregate value
    if total_cells > MAX_CELLS:
        for var in variables:
            vid = var["id"]
            is_time = var.get("time", False)
            vals = [v["id"] for v in var.get("values", [])]
            if not vals:
                continue
            if is_time:
                var_selection.append({"code": vid, "values": vals, "_time": True})
            else:
                # Prefer aggregate/total codes
                agg = [v for v in vals if v.upper() in ("TOT", "0", "000", "TOTAL", "T", "ALL")]
                selected = agg[:1] if agg else vals[:1]
                var_selection.append({"code": vid, "values": selected, "_time": False})
    else:
        for var in variables:
            vid = var["id"]
            vals = [v["id"] for v in var.get("values", [])]
            if vals:
                var_selection.append({"code": vid, "values": vals, "_time": bool(var.get("time", False))})

    return var_selection


def main():
    os.makedirs(OUT, exist_ok=True)

    # Get all tables
    log("Fetching DST table catalog...")
    tables_raw = get_json(f"{BASE}/tables?lang=en")
    if not tables_raw:
        log("Failed to get table list"); return

    # Filter to active tables
    tables = [t for t in tables_raw if t.get("active", True)]
    log(f"Found {len(tables)} active tables")

    # Group by subject area (first 2 chars of table ID)
    by_subject: dict[str, list] = defaultdict(list)
    for t in tables:
        tid = t.get("id", "")
        subj = re.sub(r"\d+$", "", tid)[:6] or tid[:2]
        by_subject[subj].append(t)

    log(f"Found {len(by_subject)} subject groups")

    total_obs = 0
    for subj in sorted(by_subject.keys()):
        subj_tables = by_subject[subj]
        out_path = os.path.join(OUT, f"{subj}.parquet")
        if os.path.exists(out_path):
            n = pq.read_metadata(out_path).num_rows
            log(f"  Skip {subj}: {n:,} rows"); total_obs += n; continue

        log(f"  Subject '{subj}': {len(subj_tables)} tables")
        all_keys, all_dates, all_vals = [], [], []
        seen: set[tuple] = set()

        for i, t in enumerate(subj_tables):
            table_id = t["id"]
            try:
                # Get table metadata
                meta = get_json(f"{BASE}/tableinfo?id={table_id}&lang=en")
                time.sleep(RATE)
                if not meta:
                    continue
                variables = meta.get("variables", [])
                if not variables:
                    continue

                rows = query_table(table_id, variables)
                n = 0
                for key, d, v in rows:
                    tok = (key, d)
                    if tok not in seen:
                        seen.add(tok)
                        all_keys.append(key)
                        all_dates.append(d)
                        all_vals.append(v)
                        n += 1
                if n > 0:
                    log(f"    [{i+1}/{len(subj_tables)}] {table_id}: {n:,} obs")
            except Exception as e:
                log(f"    [{i+1}] {table_id} ERR: {e}")

        if all_vals:
            tbl = pa.table({
                "series_key": pa.array(all_keys,  pa.string()),
                "obs_date":   pa.array(all_dates, pa.date32()),
                "value":      pa.array(all_vals,  pa.float64()),
            })
            pq.write_table(tbl, out_path, compression="zstd")
            n = pq.read_metadata(out_path).num_rows
            log(f"  {subj}: {n:,} obs saved")
            total_obs += n

    log(f"DONE: {total_obs:,} total DST Denmark observations")


if __name__ == "__main__":
    main()
