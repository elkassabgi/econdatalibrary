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

ROOT = r"D:/research/econfindatalibrary"
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


def parse_date(s: str) -> dt.date | None:
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

        # Pick the time dimension:
        #   1. AUTHORITATIVE: the dimension id == metadata `time: true` code.
        #   2. else a literally-named time dim (TIME_CODES) wins.
        #   3. else fall back to value-parsing (sane-date is_time_dim).
        # A numeric category dimension can never beat a real time axis.
        if time_code is not None:
            for i, did in enumerate(dim_ids):
                if did == time_code:
                    time_dim_idx = i
                    break
        if time_dim_idx is None:
            for i, did in enumerate(dim_ids):
                if str(did).strip().lower() in TIME_CODES:
                    time_dim_idx = i
                    break
        if time_dim_idx is None:
            for i, did in enumerate(dim_ids):
                if is_time_dim(did, dim_codes[i]):
                    time_dim_idx = i
                    break

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
