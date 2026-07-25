#!/usr/bin/env python3
"""NASA GISS Surface Temperature Analysis (GISTEMP v4) ingest.

Downloads global/hemispheric/zonal mean temperature anomaly tables.
Source: https://data.giss.nasa.gov/gistemp/  (public domain, NASA)
Output: data/clean_full/nasa_giss/giss_temp.parquet

Series key format: GISS:{table}:{period_label}
  e.g. GISS:global:annual  GISS:global:Jan  GISS:global:DJF
Run: python jobs/ingest_nasa_giss.py
"""
from __future__ import annotations
import csv
import datetime as dt
import io
import os
import time

import requests
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT  = os.path.join(ROOT, "data", "clean_full", "nasa_giss")
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
BASE = "https://data.giss.nasa.gov/gistemp/tabledata_v4"

# (filename, series_label)
TABLES = [
    ("GLB.Ts+dSST.csv",  "global"),
    ("NH.Ts+dSST.csv",   "north_hemisphere"),
    ("SH.Ts+dSST.csv",   "south_hemisphere"),
    ("ZonAnn.Ts+dSST.csv", "zonal"),
]

MONTH_MAP = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def fetch(url: str) -> bytes | None:
    try:
        r = requests.get(url, headers=UA, timeout=30)
        if r.status_code == 200:
            return r.content
        log(f"  HTTP {r.status_code}: {url}")
    except Exception as e:
        log(f"  ERR: {e}")
    return None


def parse_giss_csv(data: bytes, label: str) -> tuple[list, list, list]:
    """Parse GISS temperature table. Wide format: year in col1, months and seasons in other cols."""
    text = data.decode("utf-8", errors="replace")
    # Skip comment lines starting with '*' and blank lines
    lines = [l for l in text.splitlines() if l.strip() and not l.startswith("*")]
    if not lines:
        return [], [], []

    keys, dates, vals = [], [], []

    # Find header row (contains month names or 'Year')
    header = None
    data_start = 0
    for i, line in enumerate(lines):
        parts = [p.strip() for p in line.split(",")]
        if parts and parts[0].lower() in ("year", ""):
            header = parts
            data_start = i + 1
            break

    if header is None:
        log(f"  {label}: no header found"); return [], [], []
    log(f"  {label}: header = {header}")

    # Map column index → (period_label, month or None)
    col_map = {}  # col_idx -> (period_label, date_fn)
    for ci, col in enumerate(header):
        col = col.strip()
        if ci == 0 or col.lower() == "year":
            continue
        if col in MONTH_MAP:
            col_map[ci] = (col, MONTH_MAP[col])
        elif col in ("J-D", "J.D"):   # annual (Jan-Dec)
            col_map[ci] = ("annual", None)
        elif col in ("D-N", "D.N"):   # Dec-Nov annual
            col_map[ci] = ("dec_nov", None)
        elif col.upper() in ("DJF", "MAM", "JJA", "SON"):
            col_map[ci] = (col.upper(), None)  # seasonal
        elif col and col not in ("", "Year"):
            # Geographic zones or other labels (e.g. "24N-90N", "Glob", "NHem")
            # Map these to annual obs
            col_map[ci] = (col.replace(" ", "_"), None)

    for line in lines[data_start:]:
        parts = [p.strip() for p in line.split(",")]
        if not parts or not parts[0]:
            continue
        try:
            yr = int(parts[0])
        except ValueError:
            continue

        for ci, (period, month) in col_map.items():
            if ci >= len(parts):
                continue
            raw = parts[ci].strip()
            if raw in ("", "****", "***", "NA", "N/A"):
                continue
            try:
                v = float(raw)
            except ValueError:
                continue

            if month is not None:
                obs_d = dt.date(yr, month, 1)
            else:
                obs_d = dt.date(yr, 12, 31)

            key = f"GISS:{label}:{period}"
            keys.append(key)
            dates.append(obs_d)
            vals.append(v)

    return keys, dates, vals


def main():
    os.makedirs(OUT, exist_ok=True)
    out = os.path.join(OUT, "giss_temp.parquet")
    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows
        log(f"NASA GISS: already {n:,} rows"); return

    log("=== NASA GISS Temperature Ingest ===")
    all_keys, all_dates, all_vals = [], [], []

    for fname, label in TABLES:
        url = f"{BASE}/{fname}"
        log(f"Downloading {fname}...")
        data = fetch(url)
        if not data:
            continue
        k, d, v = parse_giss_csv(data, label)
        log(f"  {label}: {len(v):,} obs")
        all_keys.extend(k); all_dates.extend(d); all_vals.extend(v)

    if not all_vals:
        log("0 observations total"); return

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out, compression="zstd")
    n = pq.read_metadata(out).num_rows
    log(f"=== GISS total: {n:,} obs saved ===")


if __name__ == "__main__":
    main()
