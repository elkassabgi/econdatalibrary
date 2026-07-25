#!/usr/bin/env python3
"""Swiss Federal Statistical Office (BFS/OFS) — PxWeb ingest.

License: OPEN-BY-ASK (free for non-commercial use with attribution)
Source: https://www.bfs.admin.ch/bfs/de/home/statistiken.html
No API key required.

Coverage: ~673 tables in the BFS PxWeb database (German API):
  * National accounts (GDP, GNI, output)
  * Labour market (employment, wages, unemployment)
  * Prices (CPI, producer prices)
  * Trade (imports/exports)
  * Population and demography
  * Housing and construction
  * Industry and services
  * Finance and insurance
  * Health and education

PxWeb multi-table root: GET /api/v1/en/  → list all table IDs
Data query: POST /api/v1/de/{dbid}/{table}.px/  → JSON-stat2
NOTE: English API returns 400 for individual tables; use German endpoint for data.

Run: python jobs/ingest_bfs.py
"""
from __future__ import annotations
import datetime as dt, json, os, re, time
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
OUT  = os.path.join(ROOT, "data", "clean_full", "bfs")
BASE_EN = "https://www.pxweb.bfs.admin.ch/api/v1/en"  # for table listing
BASE_DE = "https://www.pxweb.bfs.admin.ch/api/v1/de"  # for data queries
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
RATE = 0.3
MAX_CELLS = 100_000


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


def parse_date(s: str) -> dt.date | None:
    """Parse time values including plain years and PxWeb formats."""
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


def is_time_dim(code: str) -> bool:
    """Check if a variable code represents time."""
    code_l = code.lower()
    return code_l in ("jahr", "year", "time", "tid", "periode", "quarter", "month",
                       "année", "año", "año_mes", "quartal", "monat")


def parse_jsonstat2_bfs(data: dict, prefix: str, time_code: str | None = None) -> list[tuple[str, dt.date, float]]:
    """Parse BFS JSON-stat2 where time codes are numeric indices, labels are dates."""
    results = []
    try:
        dim_ids   = data.get("id", [])
        dim_sizes = data.get("size", [])
        dims      = data.get("dimension", {})
        values    = data.get("value", [])
        if not dim_ids or not values:
            return results

        time_dim_idx = None
        dim_codes = []   # pos → code (for series key)
        dim_labels = []  # pos → label (for date parsing)

        for i, did in enumerate(dim_ids):
            cat = dims.get(did, {}).get("category", {})
            cat_idx = cat.get("index", {})
            cat_label = cat.get("label", {})

            if isinstance(cat_idx, dict):
                size = dim_sizes[i] if i < len(dim_sizes) else max(cat_idx.values(), default=-1) + 1
                pos_to_code  = [""] * size
                pos_to_label = [""] * size
                for code, pos in cat_idx.items():
                    if pos < len(pos_to_code):
                        pos_to_code[pos]  = str(code)
                        pos_to_label[pos] = str(cat_label.get(code, code))
            elif isinstance(cat_idx, list):
                pos_to_code  = [str(c) for c in cat_idx]
                pos_to_label = [str(cat_label.get(c, c)) for c in cat_idx]
            else:
                pos_to_code = pos_to_label = []

            dim_codes.append(pos_to_code)
            dim_labels.append(pos_to_label)

        # Pick the time dimension via the shared value-first resolver (core/pxweb.py).
        # bfs is INDEX-CODED: the category CODES are positional indices ('0','1',...) that
        # parse to NO date; the real period strings live in the LABELS ('1994','1994M03',
        # ...). So we score dim_LABELS (exactly what parse_date reads dates from below) -- NOT
        # the codes, or the resolver would reject the real axis. Scoring labels also makes a
        # 'Hochschule'/'Fachrichtung'/'Relevante Vortrittsregelung' dim whose numeric CODES
        # coincidentally fall in a year range lose to the real year axis (whose LABELS are the
        # years). Authoritative `time: true` (threaded) / role.time still win first.
        time_dim_idx = _pxweb.resolve_time_dim(
            dim_ids, dim_labels, meta_time_code=time_code,
            role_time=_pxweb.role_time_of(data), parse_fn=parse_date)

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
            t_labels = dim_labels[time_dim_idx]
            if t_pos >= len(t_labels):
                continue
            # Try label first (actual year string), then code
            time_str = t_labels[t_pos]
            obs_date = parse_date(time_str)
            if obs_date is None:
                obs_date = parse_date(dim_codes[time_dim_idx][t_pos] if t_pos < len(dim_codes[time_dim_idx]) else "")
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


def main():
    os.makedirs(OUT, exist_ok=True)

    # Get all table IDs from the English listing
    log("Fetching BFS table catalog...")
    catalog = get_json(f"{BASE_EN}/")
    if not catalog or not isinstance(catalog, list):
        log("Failed to get catalog"); return

    log(f"Found {len(catalog)} BFS tables")

    all_keys, all_dates, all_vals = [], [], []
    seen: set[tuple] = set()

    for i, item in enumerate(catalog):
        dbid = item.get("dbid", "")
        text = item.get("text", dbid)
        if not dbid:
            continue

        # Check if already done (per-table checkpointing via seen set)
        out_path = os.path.join(OUT, "bfs.parquet")
        table_id = f"{dbid}.px"
        url = f"{BASE_DE}/{dbid}/{table_id}/"

        # Get metadata
        meta = get_json(url)
        time.sleep(RATE)
        if not meta or not isinstance(meta, dict):
            log(f"  [{i+1}/{len(catalog)}] {dbid}: no metadata")
            continue

        variables = meta.get("variables", [])
        if not variables:
            continue

        # Compute total cells
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
                val_texts = var.get("valueTexts", vals)
                code = var.get("code", "")
                if not vals:
                    continue
                if is_time_dim(code) or (val_texts and re.match(r"^\d{4}$", str(val_texts[0]).strip())):
                    query_vars.append({"code": code, "selection": {"filter": "item", "values": vals}})
                else:
                    # Select aggregate/total value (usually index "0" or code "tot"/"total")
                    agg = [v for v in vals if v.lower() in ("tot", "total", "0", "all", "t")]
                    selected = agg[:1] if agg else vals[:1]
                    query_vars.append({"code": code, "selection": {"filter": "item", "values": selected}})

        if not query_vars:
            continue

        body = {"query": query_vars, "response": {"format": "json-stat2"}}
        resp = post_json(url, body)
        time.sleep(RATE)
        if not resp:
            log(f"  [{i+1}/{len(catalog)}] {dbid}: no data")
            continue

        prefix = f"BFS:{dbid}"
        meta_time_code = next((v.get("code") for v in variables if v.get("time") is True), None)
        rows = parse_jsonstat2_bfs(resp, prefix, meta_time_code)
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
            log(f"  [{i+1}/{len(catalog)}] {dbid} ({text[:40]}): {n:,} obs")

        # Checkpoint every 50 tables
        if (i + 1) % 50 == 0 and all_vals:
            tbl = pa.table({
                "series_key": pa.array(all_keys,  pa.string()),
                "obs_date":   pa.array(all_dates, pa.date32()),
                "value":      pa.array(all_vals,  pa.float64()),
            })
            pq.write_table(tbl, out_path, compression="zstd")
            log(f"  Checkpoint: {len(all_vals):,} obs saved")

    if not all_vals:
        log("0 observations"); return

    out_path = os.path.join(OUT, "bfs.parquet")
    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"DONE: {n:,} BFS Switzerland observations ({len(set(all_keys))} series)")


if __name__ == "__main__":
    main()
