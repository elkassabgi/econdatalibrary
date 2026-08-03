#!/usr/bin/env python3
"""Statistics Estonia — PxWeb ingest.

License: Creative Commons Attribution 4.0 (CC BY 4.0)
Source: https://www.stat.ee/en/
No API key required.

Coverage: All tables in the Statistics Estonia database:
  * National accounts (GDP, GVA by sector)
  * Labour force (employment, wages, LFS)
  * Consumer prices and inflation
  * External trade
  * Population and demography
  * Energy statistics
  * Industry and construction

PxWeb API: GET {BASE}/{path}         → catalog
           POST {BASE}/{path}/{id}/  → data (JSON-stat2)

Run: python jobs/ingest_stat_estonia.py
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
OUT  = os.path.join(ROOT, "data", "clean_full", "stat_estonia")
BASE = "https://andmed.stat.ee/api/v1/en/stat"
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
    if os.path.exists(CATALOG_FILE):
        with open(CATALOG_FILE) as f:
            tables = json.load(f)
        log(f"Loaded catalog: {len(tables)} tables")
        return tables

    log("Crawling Statistics Estonia catalog...")
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


# Codes that literally name a Statistics Estonia PxWeb time dimension (a real
# AASTA/KUU/KVARTAL must win). Estonian: aasta=year, kuu=month, kvartal=quarter,
# periood=period, nädal=week, kuupäev=date.
TIME_CODES = ("aasta", "kuu", "kvartal", "periood", "nadal", "nädal", "kuupaev",
              "kuupäev", "tid", "time", "year", "month", "quarter", "week",
              "period", "datum", "ar", "år")


def is_time_dim(code: str, values: list[str]) -> bool:
    """FALLBACK time-dim detector. The AUTHORITATIVE path is the PxWeb metadata
    `time: true` flag (Statistics Estonia sets it on the AASTA/KUU axis — see
    query_table/parse_jsonstat2). Used only when that flag is absent.

    A dimension is 'time' only if its code literally names a time dim, OR its values
    parse to SANE dates (year ~1900..current_year+2). The old value test matched
    ^\\d{4}[MQKHW]?\\d*$, which false-matched non-time numeric PxWeb category codes
    (4-digit municipality / classification codes like 2101/4601/5001/9610/9999, and
    low codes like 1000/1881) as years — the origin of the 9999-12-31 / 1881-12-31
    obs_date corruption. Anchoring on parse_date() + a sane year range rejects all of
    those, so a category code is never fed to parse_date as a fake time value."""
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
    """Parse JSON-stat2 format -> (series_key, date, value).

    `time_code` is the AUTHORITATIVE time dimension id from the PxWeb metadata (the
    variable flagged `time: true`). When provided we lock onto that dimension instead
    of guessing — this prevents a numeric category dimension (municipality / class
    codes) from being mistaken for the time axis. Falls back to a two-pass is_time_dim
    selection (literally-named time dim wins first) only when the flag is absent."""
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
            # POSITIONAL CODES NEED THEIR LABELS. Some PxWeb tables publish the time axis with
            # codes that are INDEX POSITIONS ('0','1','2', ...) and carry the real periods only
            # in the labels/valueTexts (measured on Hagstofa SJA01101: Year values ['0','1',..]
            # vs valueTexts ['2010','2011',..]). parse_date('0') is None, so EVERY observation
            # was skipped and the table produced zero rows from a good 200 — which the fetcher
            # then reported as a schema/structural break. Keep the labels so the date lookup can
            # fall back to them; the KEY still uses codes, so no existing series_key changes.
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
    path = table_info["path"]
    url  = f"{BASE}/{path}"     # no trailing slash — POST with slash returns empty values

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
                agg_vals = [v for v in vals if v.upper() in ("0", "000", "TOTAL", "T", "ALL", "KOKKU")]
                selected = agg_vals[:1] if agg_vals else vals[:1]
                query_vars.append({"code": code, "selection": {"filter": "item", "values": selected}})

    if not query_vars:
        return []

    body = {"query": query_vars, "response": {"format": "json-stat2"}}
    resp = post_json(url, body)
    time.sleep(RATE)
    if not resp:
        return []

    prefix = f"EE:{path.replace('/', ':')}"
    return parse_jsonstat2(resp, prefix, time_code)


def main():
    os.makedirs(OUT, exist_ok=True)

    tables = crawl_catalog()
    log(f"Processing {len(tables)} Estonia tables")

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

    log(f"DONE: {total_obs:,} total Statistics Estonia observations")


if __name__ == "__main__":
    main()
