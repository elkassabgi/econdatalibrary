#!/usr/bin/env python3
"""Statistics Sweden (SCB) — full PxWeb ingest.

License: Creative Commons Attribution 4.0 (CC BY 4.0)
Source: https://www.scb.se/en/services/open-data-api/api-for-the-statistical-database/
No API key required.

Coverage: ~10,000+ tables in the SCB statistical database:
  * National accounts (GDP, GNI, output by industry)
  * Labour force survey (employment, unemployment, wages)
  * Consumer/Producer price indices
  * Trade in goods and services
  * Population and demography
  * Industrial and business surveys
  * Financial accounts and public sector
  * Housing and construction

PxWeb API: GET {BASE}/{path}  →  catalog list
           POST {BASE}/{path}/{tableId}/  →  data (JSON-stat2)

Run: python jobs/ingest_scb.py
"""
from __future__ import annotations
import datetime as dt, io, json, os, re, time
from collections import deque
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
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
OUT  = os.path.join(ROOT, "data", "clean_full", "scb")
BASE = "https://api.scb.se/OV0104/v1/doris/en/ssd"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
RATE = 0.2        # 10 req/s limit; 0.2 = comfortable margin
MAX_CELLS = 100_000  # SCB limit is 150K; use 100K as safe ceiling
CATALOG_FILE = os.path.join(OUT, "_catalog.json")


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
                log(f"  429 throttle, sleeping 30s"); time.sleep(30); continue
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
            if r.status_code == 400:
                # Too large or bad request
                return None
            if r.status_code == 429:
                log(f"  429 throttle, sleeping 30s"); time.sleep(30); continue
            log(f"  POST HTTP {r.status_code}: {url[-60:]}")
        except Exception as e:
            log(f"  POST ERR: {e}")
        time.sleep(5 * (attempt + 1))
    return None


def crawl_catalog() -> list[dict]:
    """BFS crawl of SCB directory tree. Returns list of {path, id, text} for all tables."""
    if os.path.exists(CATALOG_FILE):
        with open(CATALOG_FILE) as f:
            tables = json.load(f)
        log(f"Loaded catalog: {len(tables)} tables")
        return tables

    log("Crawling SCB catalog tree (this may take a few minutes)...")
    tables = []
    queue = deque([""])  # start at root

    while queue:
        path = queue.popleft()
        url = f"{BASE}/{path}" if path else BASE
        data = get_json(url)
        time.sleep(RATE)
        if not isinstance(data, list):
            continue
        for item in data:
            item_id = item.get("id", "")
            item_text = item.get("text", "")
            item_type = item.get("type", "l")  # "l"=level, "t"=table
            child_path = f"{path}/{item_id}".lstrip("/")
            if item_type == "t":
                tables.append({"path": child_path, "id": item_id, "text": item_text})
            elif item_type == "l":
                queue.append(child_path)

    log(f"Catalog complete: {len(tables)} tables found")
    os.makedirs(OUT, exist_ok=True)
    with open(CATALOG_FILE, "w") as f:
        json.dump(tables, f)
    return tables


def parse_date(s: str) -> dt.date | None:
    """Parse PxWeb time value (e.g. 2023, 2023M01, 2023K1, 2023Q1, 2023W01)."""
    s = (s or "").strip()
    try:
        # Annual: 2023
        if re.match(r"^\d{4}$", s):
            return dt.date(int(s), 12, 31)
        # Monthly: 2023M01 or 2023-01
        m = re.match(r"^(\d{4})M(\d{2})$", s, re.IGNORECASE)
        if m:
            return dt.date(int(m.group(1)), int(m.group(2)), 1)
        m = re.match(r"^(\d{4})-(\d{2})$", s)
        if m:
            return dt.date(int(m.group(1)), int(m.group(2)), 1)
        # Quarterly: 2023Q1 or 2023K1 (Swedish: kvartal)
        m = re.match(r"^(\d{4})[QK](\d)$", s, re.IGNORECASE)
        if m:
            q = int(m.group(2))
            return dt.date(int(m.group(1)), (q - 1) * 3 + 1, 1)
        # Weekly: 2023W01 → first day of week
        m = re.match(r"^(\d{4})W(\d{2})$", s, re.IGNORECASE)
        if m:
            yr, wk = int(m.group(1)), int(m.group(2))
            return dt.date.fromisocalendar(yr, wk, 1)
        # Half-year: 2023H1
        m = re.match(r"^(\d{4})H(\d)$", s, re.IGNORECASE)
        if m:
            return dt.date(int(m.group(1)), 1 if m.group(2) == "1" else 7, 1)
        # Full date: 2023-01-15
        if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
            return dt.date.fromisoformat(s)
    except (ValueError, TypeError):
        pass
    return None


# Codes that literally name a PxWeb time dimension (a real "Tid" must win).
TIME_CODES = ("tid", "time", "year", "period", "datum", "ar", "år", "manad", "månad", "kvartal")


def is_time_dim(code: str, values: list[str]) -> bool:
    """A dimension is 'time' only if its code is a known time code, OR its values
    actually parse to SANE dates (year ~1900..current_year+2).

    The old heuristic matched ^\\d{4}[MQKHW]?\\d*$, which over-matched non-time
    numeric category codes — e.g. a ContentsCode value like '00000858' or a category
    code '2584' — and made the parser treat them as the time dimension, writing
    garbage obs_dates such as 2584-12-31. Anchoring on parse_date() + a sane year
    range removes that false match (out-of-range years like 2584 are rejected, and
    8-digit codes like '00000858' don't parse to a date at all)."""
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


def parse_jsonstat2(data: dict, table_path: str, meta_time_code: str | None = None) -> list[tuple[str, dt.date, float]]:
    """Parse JSON-stat2 response into (series_key, date, value) tuples.

    `meta_time_code` is the AUTHORITATIVE time dimension id from the PxWeb metadata
    (the variable flagged `time: true`). When provided the shared resolver locks onto
    that dimension instead of guessing — this prevents a numeric category dimension
    (e.g. Region municipality codes '0114'/'1280', which parse to garbage years
    114/1280) from being mistaken for the time axis. Falls back to the value-first
    selection (highest date-parse-rate, literal name only as a last resort) when the
    flag is absent."""
    results = []
    try:
        dim_ids   = data.get("id", [])
        dim_sizes = data.get("size", [])
        dims      = data.get("dimension", {})
        values    = data.get("value", [])

        if not dim_ids or not values:
            return results

        # Build index → code maps for all dimensions
        dim_codes = []  # list of [code_at_position_0, code_at_position_1, ...]
        for did in dim_ids:
            dim_info = dims.get(did, {})
            cat = dim_info.get("category", {})
            cat_idx = cat.get("index", {})
            if isinstance(cat_idx, dict):
                # {code: position} → invert to [code_at_pos_0, code_at_pos_1, ...]
                size = max(cat_idx.values()) + 1
                pos_to_code = [""] * size
                for code, pos in cat_idx.items():
                    if pos < size:
                        pos_to_code[pos] = code
            elif isinstance(cat_idx, list):
                pos_to_code = list(cat_idx)
            else:
                pos_to_code = []
            dim_codes.append(pos_to_code)

        # Pick the time dimension via the shared value-first resolver (core/pxweb.py):
        # authoritative `time: true` / role.time, else highest date-parse-rate, else name.
        # Value-first stops a category axis (Region codes '0114'/'1280') from outranking Tid.
        time_dim_idx = _pxweb.resolve_time_dim(dim_ids, dim_codes, meta_time_code=meta_time_code, role_time=_pxweb.role_time_of(data), parse_fn=parse_date)

        if time_dim_idx is None:
            return results

        # Compute strides for index arithmetic
        strides = [1] * len(dim_sizes)
        for i in range(len(dim_sizes) - 2, -1, -1):
            strides[i] = strides[i + 1] * dim_sizes[i + 1]

        prefix = table_path.replace("/", ":")

        # Iterate over all cells
        for flat_idx, raw_v in enumerate(values):
            if raw_v is None:
                continue
            try:
                v = float(raw_v)
                if v != v:
                    continue
            except (ValueError, TypeError):
                continue

            # Decompose flat index into per-dimension indices
            remainder = flat_idx
            dim_indices = []
            for i, stride in enumerate(strides):
                dim_indices.append(remainder // stride)
                remainder %= stride

            # Get time value
            t_pos = dim_indices[time_dim_idx]
            t_codes = dim_codes[time_dim_idx]
            if t_pos >= len(t_codes):
                continue
            time_str = t_codes[t_pos]
            obs_date = parse_date(time_str)
            if obs_date is None:
                continue

            # Build series key from non-time dimension codes
            key_parts = [prefix]
            for i, (did, pos) in enumerate(zip(dim_ids, dim_indices)):
                if i == time_dim_idx:
                    continue
                codes_for_dim = dim_codes[i]
                code_val = codes_for_dim[pos] if pos < len(codes_for_dim) else str(pos)
                key_parts.append(f"{did}={code_val}")

            results.append((":".join(key_parts), obs_date, v))

    except Exception as e:
        log(f"  JSON-stat2 parse error: {e}")
    return results


def query_table(table_info: dict) -> list[tuple[str, dt.date, float]]:
    """Fetch and parse one SCB table."""
    path = table_info["path"]
    url  = f"{BASE}/{path}/"

    # Get metadata
    meta = get_json(url)
    time.sleep(RATE)
    if not meta or not isinstance(meta, dict):
        return []

    variables = meta.get("variables", [])
    if not variables:
        return []

    # Authoritative time dimension: PxWeb metadata flags the time variable `time: true`.
    time_code = next((v.get("code") for v in variables if v.get("time") is True), None)

    # Compute total cells to check size
    total_cells = 1
    for var in variables:
        total_cells *= max(len(var.get("values", [])), 1)

    query_vars = []
    if total_cells <= MAX_CELLS:
        # Select all values for all variables
        for var in variables:
            vals = var.get("values", [])
            if vals:
                query_vars.append({
                    "code": var["code"],
                    "selection": {"filter": "item", "values": vals}
                })
    else:
        # Too large: select only time + key aggregates
        # Find time variable and content variable, select all time but limit others
        for var in variables:
            vals = var.get("values", [])
            code = var.get("code", "")
            if not vals:
                continue
            if is_time_dim(code, vals):
                query_vars.append({"code": code, "selection": {"filter": "item", "values": vals}})
            else:
                # Select only first value (aggregate/total) — usually "000" or "T" or index 0
                # Prefer aggregate codes
                agg_vals = [v for v in vals if v.upper() in ("000", "TOTAL", "TOT", "T", "0", "ALL")]
                selected = agg_vals[:1] if agg_vals else vals[:1]
                query_vars.append({"code": code, "selection": {"filter": "item", "values": selected}})

    if not query_vars:
        return []

    body = {
        "query": query_vars,
        "response": {"format": "json-stat2"}
    }

    resp = post_json(url, body)
    time.sleep(RATE)
    if not resp:
        return []

    return parse_jsonstat2(resp, path, time_code)


def main():
    os.makedirs(OUT, exist_ok=True)

    # Get all tables
    tables = crawl_catalog()
    log(f"Processing {len(tables)} SCB tables")

    # Group by first path component (subject area) for per-file saving
    from collections import defaultdict
    by_subject: dict[str, list] = defaultdict(list)
    for t in tables:
        parts = t["path"].split("/")
        subj = parts[0] if parts else "root"
        by_subject[subj].append(t)

    log(f"Found {len(by_subject)} subject areas")

    total_obs = 0
    for subj, subj_tables in sorted(by_subject.items()):
        out_path = os.path.join(OUT, f"{subj}.parquet")
        if os.path.exists(out_path):
            n = pq.read_metadata(out_path).num_rows
            log(f"  Skip {subj}: {n:,} rows"); total_obs += n; continue

        log(f"  Subject '{subj}': {len(subj_tables)} tables")
        all_keys, all_dates, all_vals = [], [], []
        seen: set[tuple] = set()

        for i, t in enumerate(subj_tables):
            try:
                rows = query_table(t)
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
                    log(f"    [{i+1}/{len(subj_tables)}] {t['id']}: {n:,} obs")
            except Exception as e:
                log(f"    [{i+1}] {t['id']} ERR: {e}")

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

    log(f"DONE: {total_obs:,} total SCB Sweden observations")


if __name__ == "__main__":
    main()
