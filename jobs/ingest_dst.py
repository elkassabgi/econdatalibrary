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

ROOT = r"D:/research/econfindatalibrary"
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
    except (ValueError, TypeError):
        pass
    return None


# Codes that literally name a DST time dimension. A real time axis (DST always uses
# "Tid") must win over any non-time category whose numeric codes happen to look date-ish.
TIME_CODES = ("tid", "time", "year", "aar", "år", "periode", "period", "datum",
              "maaned", "måned", "maned", "kvartal", "uge", "week", "month", "quarter")


def is_time_dim(code: str, values: list[str]) -> bool:
    """FALLBACK time-dim detector. The AUTHORITATIVE path is the JSON-stat
    `dimension.role.time` array (DST always populates it with "Tid") — see
    parse_jsonstat. This is used ONLY when role.time is absent.

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
    """Parse JSON-stat (v1) format from DST."""
    results = []
    try:
        ds = data.get("dataset", data)  # handle both root and wrapped
        dim_obj = ds.get("dimension", {})
        dim_ids = dim_obj.get("id", [])
        dim_sizes = dim_obj.get("size", [])
        role = dim_obj.get("role", {})
        time_dims = set(role.get("time", []))
        metric_dims = set(role.get("metric", []))
        values = ds.get("value", [])

        if not dim_ids or not values:
            return results

        # Build dimension code maps
        time_dim_idx = None
        dim_codes = []
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
            if did in time_dims and time_dim_idx is None:
                time_dim_idx = i

        # Fallback (only if JSON-stat role.time was absent). Two-pass so a real time
        # dimension can never be beaten by a non-time numeric category:
        #   pass 1 — a dimension whose CODE literally names a time axis (Tid/Time/...);
        #   pass 2 — a dimension whose VALUES parse to SANE dates via is_time_dim().
        if time_dim_idx is None:
            for i, did in enumerate(dim_ids):       # pass 1: literally-named time dim
                if str(did).strip().lower() in TIME_CODES:
                    time_dim_idx = i; break
        if time_dim_idx is None:
            for i, (did, codes) in enumerate(zip(dim_ids, dim_codes)):  # pass 2: sane dates
                if is_time_dim(did, codes):
                    time_dim_idx = i; break

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
    """Fetch data for one DST table."""
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
                var_selection.append({"code": vid, "values": vals})
            else:
                # Prefer aggregate/total codes
                agg = [v for v in vals if v.upper() in ("TOT", "0", "000", "TOTAL", "T", "ALL")]
                selected = agg[:1] if agg else vals[:1]
                var_selection.append({"code": vid, "values": selected})
    else:
        for var in variables:
            vid = var["id"]
            vals = [v["id"] for v in var.get("values", [])]
            if vals:
                var_selection.append({"code": vid, "values": vals})

    if not var_selection:
        return []

    body = {
        "table": table_id,
        "format": "JSONSTAT",
        "lang": "en",
        "variables": var_selection,
    }

    resp = post_json(f"{BASE}/data", body)
    time.sleep(RATE)
    if not resp:
        return []

    return parse_jsonstat(resp, table_id)


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
