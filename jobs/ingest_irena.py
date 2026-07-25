#!/usr/bin/env python3
"""IRENA (International Renewable Energy Agency) - full bulk ingest via PxWeb API.

Source: https://pxweb.irena.org/
License: CC BY 4.0 (IRENA data)
No API key required.

Coverage:
  * 226 countries/areas, 1990s-present
  * Electricity capacity by technology (wind, solar PV, hydro, etc.)
  * Electricity generation by technology
  * Renewable energy share (%)
  * Public investment in renewables (USD millions)

Output: data/clean_full/irena/{table_id}.parquet

Series key format: IRENA:{category}:{table}:{Country}:{Technology}
Run: python jobs/ingest_irena.py
"""
from __future__ import annotations
import datetime as dt
import json
import os
import re
import time

import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT  = os.path.join(ROOT, "data", "clean_full", "irena")
BASE = "https://pxweb.irena.org/api/v1/en"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
        "Content-Type": "application/json"}
RATE = 1.0   # seconds between requests


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def get_json(url: str, retries: int = 3) -> dict | list | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=60)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 404:
                return None
            log(f"  HTTP {r.status_code}: {url[-60:]}")
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
                log(f"  400 bad request: {url[-60:]}")
                return None
            log(f"  POST HTTP {r.status_code}: {url[-60:]}")
        except Exception as e:
            log(f"  POST ERR: {e}")
        time.sleep(5 * (attempt + 1))
    return None


def crawl_catalog(db: str) -> list[dict]:
    """BFS crawl of PxWeb catalog. Returns list of {path, id, text}."""
    tables = []
    queue = [f"{BASE}/{db}/"]
    visited = set()
    while queue:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)
        items = get_json(url)
        if not items:
            continue
        for item in (items if isinstance(items, list) else [items]):
            item_type = item.get("type", "l")
            item_id   = item.get("id", item.get("dbid", ""))
            item_text = item.get("text", "")
            if item_type == "t":   # table
                tables.append({"path": url, "id": item_id, "text": item_text})
            elif item_type in ("l", "h"):  # level/heading - go deeper
                child_url = url.rstrip("/") + "/" + requests.utils.quote(item_id)
                if child_url not in visited:
                    queue.append(child_url + "/")
        time.sleep(0.3)
    return tables


def parse_jsonstat2(resp: dict, table_path: str) -> tuple[list, list, list]:
    """Parse json-stat2 response into (keys, dates, vals) long format."""
    if not resp or "dimension" not in resp:
        return [], [], []

    dims = resp["dimension"]
    size = resp.get("size", [])
    ids  = resp.get("id", [])
    vals_raw = resp.get("value", [])

    # Find the time dimension (contains years or year-like values)
    time_dim = None
    time_vals = []
    for dim_id in ids:
        dim = dims.get(dim_id, {})
        cats = dim.get("category", {}).get("label", {})
        cat_vals = list(cats.values())
        # Year dimension: all values look like 4-digit years or "YYYY H1/H2"
        if cat_vals and all(re.match(r"^\d{4}", str(v)) for v in cat_vals):
            time_dim = dim_id
            time_vals = cat_vals
            break

    if not time_dim:
        log(f"  No time dimension in {table_path}")
        return [], [], []

    # Non-time dimensions are the "series" dimensions
    series_dims = [d for d in ids if d != time_dim]
    series_sizes = [size[ids.index(d)] for d in series_dims]
    time_size = size[ids.index(time_dim)]

    # Build index mapping
    series_cats = []
    for sd in series_dims:
        dim = dims.get(sd, {})
        cats = dim.get("category", {})
        labels = cats.get("label", {})
        series_cats.append(list(labels.values()))

    # Parse dates
    def parse_year(s):
        m = re.match(r"^(\d{4})", str(s))
        return int(m.group(1)) if m else None

    time_dates = []
    for tv in time_vals:
        yr = parse_year(tv)
        if yr:
            time_dates.append(dt.date(yr, 12, 31))
        else:
            time_dates.append(None)

    # Flatten values - PxWeb json-stat2 is row-major (fastest-varying dimension is last)
    # Order of ids determines the layout
    keys, dates, vals = [], [], []
    total = len(vals_raw)
    if total == 0:
        return [], [], []

    # Compute strides
    strides = [1] * len(ids)
    for i in range(len(ids) - 2, -1, -1):
        strides[i] = strides[i + 1] * size[i + 1]

    time_idx_in_ids = ids.index(time_dim)

    for flat_idx, val in enumerate(vals_raw):
        if val is None:
            continue
        try:
            v = float(val)
            if v != v:
                continue
        except (TypeError, ValueError):
            continue

        # Decode indices
        idx = flat_idx
        dim_indices = []
        for i, s in enumerate(size):
            dim_indices.append(idx // strides[i])
            idx %= strides[i]

        t_i = dim_indices[time_idx_in_ids]
        if t_i >= len(time_dates) or time_dates[t_i] is None:
            continue

        # Build series key from non-time dimensions
        key_parts = []
        for si, sd in enumerate(series_dims):
            d_i_in_ids = ids.index(sd)
            cat_i = dim_indices[d_i_in_ids]
            if cat_i < len(series_cats[si]):
                key_parts.append(str(series_cats[si][cat_i]).replace("|", "_"))

        # Table name (last segment of path, without .px)
        tname = table_path.split("/")[-1].replace(".px", "").replace("%20", " ")
        key = f"IRENA:{tname}|{'|'.join(key_parts)}"
        keys.append(key)
        dates.append(time_dates[t_i])
        vals.append(v)

    return keys, dates, vals


def ingest_table(path: str, table_id: str) -> int:
    safe_name = re.sub(r"[^\w]", "_", table_id)[:80]
    out_path = os.path.join(OUT, f"{safe_name}.parquet")
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"  [{table_id[:40]}] already {n:,} rows")
        return n

    # Get metadata
    table_url = path.rstrip("/") + "/" + requests.utils.quote(table_id)
    meta = get_json(table_url)
    if not meta:
        log(f"  [{table_id[:40]}] metadata failed")
        return 0

    # POST to get data (select all values for all variables)
    body = {"query": [], "response": {"format": "json-stat2"}}
    resp = post_json(table_url, body)
    if not resp:
        log(f"  [{table_id[:40]}] data fetch failed")
        return 0

    k, d, v = parse_jsonstat2(resp, table_url)
    if not k:
        log(f"  [{table_id[:40]}] 0 obs parsed")
        return 0

    tbl = pa.table({
        "series_key": pa.array(k, pa.string()),
        "obs_date":   pa.array(d, pa.date32()),
        "value":      pa.array(v, pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"  [{table_id[:40]}] -> {n:,} obs saved")
    return n


def main():
    os.makedirs(OUT, exist_ok=True)
    log("=== IRENA Renewable Energy Ingest ===")
    grand = 0

    for db in ["IRENASTAT", "RE-STAT"]:
        log(f"Crawling catalog: {db}")
        tables = crawl_catalog(db)
        log(f"  Found {len(tables)} tables in {db}")
        for t in tables:
            n = ingest_table(t["path"], t["id"])
            grand += n
            time.sleep(RATE)

    log(f"=== IRENA TOTAL: {grand:,} observations ===")


if __name__ == "__main__":
    main()
