#!/usr/bin/env python3
"""Statistics Iceland (Hagstofa Íslands) — PxWeb ingest.

License: Creative Commons Attribution 4.0 (CC BY 4.0)
Source: https://www.statice.is/
No API key required.

Coverage: All databases in the Statistics Iceland PxWeb system:
  * Efnahagur (Economy): GDP, trade, prices, public finance
  * Atvinnuvegir (Industry): fishing, agriculture, manufacturing
  * Ibuar (Population): demographics, births, deaths, migration
  * Samfelag (Society): education, health, housing

PxWeb multi-database: GET {BASE}/en      → list databases
                       GET {BASE}/en/{db}/{path}  → catalog or table
                       POST {BASE}/en/{db}/{path}/{id}/  → data (JSON-stat2)

Run: python jobs/ingest_hagstofa.py
"""
from __future__ import annotations
import datetime as dt, json, os, re, time
from collections import deque, defaultdict
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
OUT  = os.path.join(ROOT, "data", "clean_full", "hagstofa")
BASE = "https://px.hagstofa.is/pxen/api/v1/en"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
RATE = 0.3
MAX_CELLS = 100_000
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
            if r.status_code in (400, 403):
                return None
            if r.status_code == 429:
                log("  429 throttle, sleeping 30s"); time.sleep(30); continue
            log(f"  POST HTTP {r.status_code}: {url[-60:]}")
        except Exception as e:
            log(f"  POST ERR: {e}")
        time.sleep(5 * (attempt + 1))
    return None


def crawl_catalog() -> list[dict]:
    """Crawl all Iceland databases and return table list."""
    if os.path.exists(CATALOG_FILE):
        with open(CATALOG_FILE) as f:
            tables = json.load(f)
        log(f"Loaded catalog: {len(tables)} tables")
        return tables

    log("Fetching Iceland databases...")
    root = get_json(BASE)
    time.sleep(RATE)
    if not root or not isinstance(root, list):
        log("Failed to get root"); return []

    databases = [item.get("dbid", "") for item in root if item.get("dbid")]
    log(f"Databases: {databases}")

    tables = []
    for db in databases:
        log(f"  Crawling database '{db}'...")
        queue = deque([""])
        while queue:
            path = queue.popleft()
            url = f"{BASE}/{db}/{path}" if path else f"{BASE}/{db}"
            data = get_json(url)
            time.sleep(RATE)
            if not isinstance(data, list):
                continue
            for item in data:
                item_id = item.get("id", "")
                item_text = item.get("text", "")
                item_type = item.get("type", "l")
                child_path = f"{path}/{item_id}".lstrip("/")
                if item_type == "t":
                    tables.append({
                        "db": db, "path": child_path,
                        "id": item_id, "text": item_text
                    })
                elif item_type == "l":
                    queue.append(child_path)

    log(f"Catalog complete: {len(tables)} tables")
    os.makedirs(OUT, exist_ok=True)
    with open(CATALOG_FILE, "w") as f:
        json.dump(tables, f)
    return tables


# Statistics Iceland puts AGGREGATE PERIODS on the time axis ITSELF, coded as bare 4-digit
# numbers in the 3000s. UMH11130.px is the proven case: its `Ár` variable is flagged time=True
# AND carries role.time, and its values are ['3002','3003','3004','3001','1949','1950', ...] —
# four sentinels and real years side by side on the same axis.
#
# So this is NOT the dimension-selection defect cso had, and the distinction matters because it
# is why a synthetic test passed while production failed: the correct axis IS chosen here, the
# codes on it simply are not all periods. Parsing 3001 as the year 3001 is what put 1,120 rows
# of Umhverfi.parquet past the year 3000, and re-parsing UMH11130 live on 2026-08-03 produced
# 120 impossible rows out of 168 with the selection logic already correct.
#
# A year we cannot place is not a period, so it yields None and the caller skips the row. That
# is the right outcome: an observation whose time coordinate is fabricated is worse than no
# observation. The bound is deliberately wide — genuine long history and real projections must
# be untouched (un_wpp reaches 2101, bfs 2150 in scenarios) — and every sentinel actually
# observed sits far outside it.
_YEAR_LO, _YEAR_HI = 1500, 2100


def _year_ok(y: int) -> bool:
    return _YEAR_LO <= y <= _YEAR_HI


def parse_date(s: str) -> dt.date | None:
    # EVERY branch is bounded, not just the bare-year one. Bounding only `^\d{4}$` left
    # `3001M03` parsing to 3001-03-01 — caught by a unit test after the live re-parse already
    # looked clean, because UMH11130 happens to use bare years. A partial bound on a parser is
    # the same bug with a smaller footprint, waiting for a table with a monthly sentinel.
    s = (s or "").strip()
    try:
        if re.match(r"^\d{4}$", s):
            y = int(s)
            return dt.date(y, 12, 31) if _year_ok(y) else None
        m = re.match(r"^(\d{4})M(\d{2})$", s, re.IGNORECASE)
        if m:
            y = int(m.group(1))
            return dt.date(y, int(m.group(2)), 1) if _year_ok(y) else None
        m = re.match(r"^(\d{4})[QK](\d)$", s, re.IGNORECASE)
        if m:
            y, q = int(m.group(1)), int(m.group(2))
            return dt.date(y, (q - 1) * 3 + 1, 1) if _year_ok(y) else None
        m = re.match(r"^(\d{4})H(\d)$", s, re.IGNORECASE)
        if m:
            y = int(m.group(1))
            return dt.date(y, 1 if m.group(2) == "1" else 7, 1) if _year_ok(y) else None
        m = re.match(r"^(\d{4})W(\d{2})$", s, re.IGNORECASE)
        if m:
            yr, wk = int(m.group(1)), int(m.group(2))
            return dt.date.fromisocalendar(yr, wk, 1) if _year_ok(yr) else None
        if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
            d = dt.date.fromisoformat(s)
            return d if _year_ok(d.year) else None
    except (ValueError, TypeError):
        pass
    return None


# Codes that literally name a PxWeb time dimension. Icelandic "ár" (year) is the
# common one (sent as "Ár"/"ar"/"ár"); "tími"/"timi" = time. A literally-named
# time dim always wins over any numeric category dimension.
TIME_CODES = ("tid", "tími", "timi", "time", "year", "ar", "ár", "period",
              "quarter", "month", "week", "datum", "manudur", "mánuður",
              "arsfjordungur", "ársfjórðungur")


def is_time_dim(code: str, values: list[str]) -> bool:
    """FALLBACK time-dim detector. The authoritative path is the PxWeb metadata
    `time: true` flag (see query_table/parse_jsonstat2); this is used only when that
    flag is absent. A dimension is 'time' only if its code literally names a time dim,
    OR its values parse to SANE dates (year ~1900..current_year+2).

    The old value test matched ^\\d{4}[MQKHW]?\\d*$, which false-matched non-time PxWeb
    numeric category codes and positional indices (0,1,2,...) — and even a literally
    named but non-time 'Year' variable whose values are indices — producing obs_dates
    like 8722/9999/111/1000 (origin of the far-future / sub-1900 corruption). Anchoring
    on parse_date() + a sane year range rejects all of those."""
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


def parse_jsonstat2(data: dict, prefix: str, time_code: str | None = None) -> list[tuple[str, dt.date, float]]:
    """Parse JSON-stat2 → (series_key, date, value).

    `time_code` is the AUTHORITATIVE time dimension id from the PxWeb metadata (the
    variable flagged `time: true`). When provided we lock onto that dimension instead
    of guessing — this prevents a numeric category dimension (or a positional-index
    axis, or a non-time variable literally named 'Year') from being mistaken for the
    time axis. Falls back to a two-pass is_time_dim only when the flag is absent."""
    results = []
    try:
        dim_ids   = data.get("id", [])
        dim_sizes = data.get("size", [])
        dims      = data.get("dimension", {})
        values    = data.get("value", [])
        if not dim_ids or not values:
            return results

        time_dim_idx = None
        dim_codes = []
        dim_labels = []
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
            # POSITIONAL CODES NEED THEIR LABELS. Some Hagstofa tables publish the time axis
            # with codes that are INDEX POSITIONS ('0','1','2', ...) and carry the real
            # periods only in the labels/valueTexts:
            #     Year : values=['0','1','2', ...]  valueTexts=['2010','2011','2012', ...]
            # parse_date('0') is None, so every observation was skipped and the table produced
            # zero rows while returning a perfectly good 200 — which the fetcher then reported
            # as a schema/structural break. Keep the labels so the date lookup can fall back
            # to them; the KEY still uses codes, so no existing series_key changes.
            lab = cat.get("label", {})
            dim_labels.append([lab.get(c, "") if isinstance(lab, dict) else ""
                               for c in pos_to_code])

        # Pick the time dimension via the shared value-first resolver (core/pxweb.py):
        # authoritative `time: true` / role.time, else highest date-parse-rate, else name.
        # Value-first stops a month axis (codes '0'..'11') from outranking a year axis.
        time_dim_idx = _pxweb.resolve_time_dim(dim_ids, dim_codes, meta_time_code=time_code, role_time=_pxweb.role_time_of(data), parse_fn=parse_date)

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
                # positional-index time codes: the period is in the label (see above)
                t_labels = dim_labels[time_dim_idx] if time_dim_idx < len(dim_labels) else []
                if t_pos < len(t_labels):
                    obs_date = parse_date(t_labels[t_pos])
            if obs_date is None:
                continue

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
    db = table_info["db"]
    path = table_info["path"]
    url = f"{BASE}/{db}/{path}/"

    meta = get_json(url)
    time.sleep(RATE)
    if not meta or not isinstance(meta, dict):
        return []

    variables = meta.get("variables", [])
    if not variables:
        return []

    # Authoritative time dimension: PxWeb metadata flags the time variable `time: true`.
    time_code = next((v.get("code") for v in variables if v.get("time") is True), None)

    total_cells = 1
    for var in variables:
        total_cells *= max(len(var.get("values", [])), 1)

    query_vars = []
    if total_cells <= MAX_CELLS:
        for var in variables:
            vals = var.get("values", [])
            if vals:
                query_vars.append({"code": var["code"], "selection": {"filter": "item", "values": vals}})
    else:
        for var in variables:
            vals = var.get("values", [])
            code = var.get("code", "")
            if not vals:
                continue
            is_time = (code == time_code) if time_code is not None else is_time_dim(code, vals)
            if is_time:
                query_vars.append({"code": code, "selection": {"filter": "item", "values": vals}})
            else:
                agg_vals = [v for v in vals if v.upper() in ("0", "000", "TOTAL", "T", "ALL", "HEILD")]
                selected = agg_vals[:1] if agg_vals else vals[:1]
                query_vars.append({"code": code, "selection": {"filter": "item", "values": selected}})

    if not query_vars:
        return []

    body = {"query": query_vars, "response": {"format": "json-stat2"}}
    resp = post_json(url, body)
    time.sleep(RATE)
    if not resp:
        return []

    prefix = f"ICE:{db}:{path.replace('/', ':')}"
    return parse_jsonstat2(resp, prefix, time_code)


def main():
    os.makedirs(OUT, exist_ok=True)

    tables = crawl_catalog()
    log(f"Processing {len(tables)} Iceland tables")

    by_db: dict[str, list] = defaultdict(list)
    for t in tables:
        by_db[t["db"]].append(t)

    total_obs = 0
    for db, db_tables in sorted(by_db.items()):
        out_path = os.path.join(OUT, f"{db}.parquet")
        if os.path.exists(out_path):
            n = pq.read_metadata(out_path).num_rows
            log(f"  Skip {db}: {n:,} rows"); total_obs += n; continue

        log(f"  Database '{db}': {len(db_tables)} tables")
        all_keys, all_dates, all_vals = [], [], []
        seen: set[tuple] = set()

        for i, t in enumerate(db_tables):
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
                    log(f"    [{i+1}/{len(db_tables)}] {t['id']}: {n:,} obs")
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
            log(f"  {db}: {n:,} obs saved")
            total_obs += n

    log(f"DONE: {total_obs:,} total Statistics Iceland observations")


if __name__ == "__main__":
    main()
