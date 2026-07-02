#!/usr/bin/env python3
"""Statistics Finland (StatFin) — full PxWeb ingest.

License: Creative Commons Attribution 4.0 (CC BY 4.0)
Source: https://stat.fi/en/statistics/
No API key required.

Coverage: ~3,000+ tables in the StatFin database:
  * National accounts (GDP, GNI, output by industry)
  * Labour force survey (employment, unemployment, wages)
  * Consumer price index
  * Trade in goods and services
  * Population and vital statistics
  * Industrial production
  * Financial accounts
  * Housing prices and permits
  * Energy and environment
  * Regional statistics

PxWeb API: GET {BASE}/{path}         → catalog list
           POST {BASE}/{path}/{id}/  → data (JSON-stat2)

Run: python jobs/ingest_statfin.py
"""
from __future__ import annotations
import datetime as dt, json, os, re, time
from collections import deque, defaultdict
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "statfin")
BASE = "https://pxdata.stat.fi/PxWeb/api/v1/en/StatFin"
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
    """BFS crawl of StatFin PxWeb catalog. Returns list of table dicts."""
    if os.path.exists(CATALOG_FILE):
        with open(CATALOG_FILE) as f:
            tables = json.load(f)
        log(f"Loaded catalog: {len(tables)} tables")
        return tables

    log("Crawling StatFin catalog tree...")
    tables = []
    queue = deque([""])

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
            item_type = item.get("type", "l")
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


# Codes that literally name a PxWeb time dimension. A real time dim must win over any
# non-time numeric category whose codes happen to look date-ish. StatFin's own time dim
# is usually flagged `time: true` in the metadata (e.g. code "timeperiod_y") and is the
# authoritative signal — see query_table/parse_jsonstat2. These are the fallback names.
TIME_CODES = (
    "vuosi", "tid", "time", "year", "kuukausi", "kk", "period", "quarter",
    "neljännes", "neljannes", "kvartal", "kausi", "viikko", "week", "month",
    "maaned", "datum", "aika", "timeperiod_y", "timeperiod_m", "timeperiod_q",
)


def is_time_dim(code: str, values: list[str]) -> bool:
    """FALLBACK time-dim detector. The authoritative path is the PxWeb metadata
    `time: true` flag (see query_table/parse_jsonstat2); this is used only when that
    flag is absent. A dimension is 'time' only if its code names a time dim, OR its
    values parse via parse_date() to a SANE year (~1900..current_year+2).

    The old heuristic matched ^\\d{4}[MQKHW]?\\d*$, which false-matched non-time PxWeb
    category/ContentsCode numeric codes — e.g. sector codes, 4-digit municipality/
    industry codes like '2584'/'9610'/'9999' or an 8-digit code like '00000858' — and
    made the parser treat them as the time axis, writing garbage obs_dates such as
    2584-12-31 / 9610-12-31 / 9999-12-31. Anchoring on parse_date() + a sane year
    range removes that false match."""
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


def parse_jsonstat2(data: dict, table_path: str, time_code: str | None = None) -> list[tuple[str, dt.date, float]]:
    """Parse JSON-stat2 format → (series_key, date, value).

    `time_code` is the AUTHORITATIVE time dimension id from the PxWeb metadata (the
    variable flagged `time: true`, e.g. "timeperiod_y"). When provided we lock onto
    that dimension instead of guessing — this prevents a non-time numeric category
    (sector codes, municipality codes like 9610/9999) from being mistaken for the time
    axis. Selection is two-pass: (1) the metadata-flagged or literally-named time dim
    wins first; (2) only if none exists do we fall back to value-parsing via
    is_time_dim. A real time dim is therefore never beaten by a date-ish category."""
    results = []
    try:
        dim_ids   = data.get("id", [])
        dim_sizes = data.get("size", [])
        dims      = data.get("dimension", {})
        values    = data.get("value", [])
        if not dim_ids or not values:
            return results

        # Build index → code maps for all dimensions first.
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

        # Two-pass time-dim selection: authoritative/literal name first, value-parse last.
        time_dim_idx = None
        if time_code is not None:                       # pass 1a: metadata `time: true`
            for i, did in enumerate(dim_ids):
                if did == time_code:
                    time_dim_idx = i
                    break
        if time_dim_idx is None:                        # pass 1b: literally-named time dim
            for i, did in enumerate(dim_ids):
                if str(did).strip().lower() in TIME_CODES:
                    time_dim_idx = i
                    break
        if time_dim_idx is None:                        # pass 2: values parse as sane dates
            for i, did in enumerate(dim_ids):
                if is_time_dim(did, dim_codes[i]):
                    time_dim_idx = i
                    break

        if time_dim_idx is None:
            return results

        strides = [1] * len(dim_sizes)
        for i in range(len(dim_sizes) - 2, -1, -1):
            strides[i] = strides[i + 1] * dim_sizes[i + 1]

        prefix = table_path.replace("/", ":")

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
    path = table_info["path"]
    url  = f"{BASE}/{path}/"

    meta = get_json(url)
    time.sleep(RATE)
    if not meta or not isinstance(meta, dict):
        return []

    variables = meta.get("variables", [])
    if not variables:
        return []

    # Authoritative time dimension: PxWeb metadata flags the time variable `time: true`
    # (StatFin uses codes like "timeperiod_y"). Prefer it over any heuristic.
    time_code = next((v.get("code") for v in variables if v.get("time") is True), None)

    total_cells = 1
    for var in variables:
        total_cells *= max(len(var.get("values", [])), 1)

    query_vars = []
    if total_cells <= MAX_CELLS:
        for var in variables:
            vals = var.get("values", [])
            if vals:
                query_vars.append({
                    "code": var["code"],
                    "selection": {"filter": "item", "values": vals}
                })
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
                agg_vals = [v for v in vals if v.upper() in ("SSS", "000", "TOTAL", "TOT", "T", "0", "ALL", "1KP")]
                selected = agg_vals[:1] if agg_vals else vals[:1]
                query_vars.append({"code": code, "selection": {"filter": "item", "values": selected}})

    if not query_vars:
        return []

    body = {"query": query_vars, "response": {"format": "json-stat2"}}
    resp = post_json(url, body)
    time.sleep(RATE)
    if not resp:
        return []

    return parse_jsonstat2(resp, path, time_code)


def main():
    os.makedirs(OUT, exist_ok=True)

    tables = crawl_catalog()
    log(f"Processing {len(tables)} StatFin tables")

    # Group by subject area (first path component)
    by_subject: dict[str, list] = defaultdict(list)
    for t in tables:
        parts = t["path"].split("/")
        subj = parts[0] if parts else "root"
        by_subject[subj].append(t)

    log(f"Found {len(by_subject)} subject areas")

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

    log(f"DONE: {total_obs:,} total StatFin Finland observations")


if __name__ == "__main__":
    main()
