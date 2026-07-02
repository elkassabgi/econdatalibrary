#!/usr/bin/env python3
"""Full-coverage BLS flat-file ingest.

Downloads ALL survey folders from https://download.bls.gov/pub/time.series/
Skips folders already with a complete .parquet file in clean_full/bls/.
Writes one grouped Parquet per survey (series_id + obs_date + value + period_type).

Run:  python jobs/ingest_bls_full.py [--dry] [--survey cu]
"""
from __future__ import annotations
import argparse
import io
import os
import re
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
OUT = os.path.join(ROOT, "data", "clean_full", "bls")
UA = "Econ-Fin Data Library admin@hfdatalibrary.com"
BASE = "https://download.bls.gov/pub/time.series"
SKIP = {"compressed", "sdmx"}  # not data folders


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def sess():
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    s = requests.Session()
    r = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers["User-Agent"] = UA
    return s


def list_surveys(s):
    r = s.get(f"{BASE}/", timeout=60)
    r.raise_for_status()
    return [f for f in re.findall(r'HREF="/pub/time\.series/([a-z][a-z0-9_]*)/"', r.text)
            if f not in SKIP]


def list_data_files(s, survey):
    """Return list of data file names in a survey folder."""
    r = s.get(f"{BASE}/{survey}/", timeout=60)
    if r.status_code == 404:
        return []
    r.raise_for_status()
    return re.findall(r'HREF="/pub/time\.series/[^/]+/([^"]+\.data\.[^"]+)"', r.text)


def parse_data(content: bytes, survey: str):
    """Parse a BLS flat .data.* file. Returns list of (series_id, obs_date, value, period)."""
    rows = []
    try:
        text = content.decode("utf-8", errors="replace")
    except Exception:
        return rows
    lines = text.splitlines()
    if not lines:
        return rows
    for line in lines[1:]:  # skip header
        parts = line.split()
        if len(parts) < 4:
            continue
        series_id = parts[0].strip()
        year_str = parts[1].strip()
        period = parts[2].strip()
        val_str = parts[3].strip()
        if val_str in ("-", ".", ""):
            continue
        try:
            val = float(val_str)
        except ValueError:
            continue
        try:
            year = int(year_str)
        except ValueError:
            continue
        # parse period -> date
        if period.startswith("M") and period[1:].isdigit():
            m = int(period[1:])
            if m == 13:    # annual average, use Dec 31
                obs_date = f"{year}-12-31"
            elif 1 <= m <= 12:
                obs_date = f"{year}-{m:02d}-01"
            else:
                continue
        elif period.startswith("Q") and period[1:].isdigit():
            q = int(period[1:])
            if 1 <= q <= 4:
                obs_date = f"{year}-{q*3-2:02d}-01"
            else:
                continue
        elif period.startswith("A"):
            obs_date = f"{year}-12-31"
        elif period.startswith("S") and period[1:].isdigit():
            half = int(period[1:])
            obs_date = f"{year}-{'01' if half == 1 else '07'}-01"
        else:
            obs_date = f"{year}-12-31"
        rows.append((series_id, obs_date, val, period))
    return rows


def ingest_survey(s, survey: str, dry: bool) -> int:
    out_path = os.path.join(OUT, f"{survey}.parquet")
    if os.path.exists(out_path) and not dry:
        sz = os.path.getsize(out_path)
        if sz > 0:
            log(f"  {survey}: skip (already {sz/1e6:.1f} MB)")
            return 0

    data_files = list_data_files(s, survey)
    if not data_files:
        log(f"  {survey}: no .data.* files found")
        return 0

    log(f"  {survey}: {len(data_files)} data files")
    all_rows = []
    for fname in data_files:
        url = f"{BASE}/{survey}/{fname}"
        try:
            r = s.get(url, timeout=120)
            if r.status_code == 404:
                continue
            r.raise_for_status()
            parsed = parse_data(r.content, survey)
            all_rows.extend(parsed)
        except Exception as e:
            log(f"    {fname}: ERR {e}")
        time.sleep(0.1)

    if not all_rows:
        log(f"  {survey}: no rows parsed")
        return 0

    if dry:
        log(f"  {survey}: DRY rows={len(all_rows):,}")
        return len(all_rows)

    tbl = pa.table({
        "series_id": pa.array([r[0] for r in all_rows], pa.string()),
        "obs_date":  pa.array([r[1] for r in all_rows], pa.string()),
        "value":     pa.array([r[2] for r in all_rows], pa.float64()),
        "period":    pa.array([r[3] for r in all_rows], pa.string()),
    })
    os.makedirs(OUT, exist_ok=True)
    pq.write_table(tbl, out_path, compression="zstd")
    log(f"  {survey}: wrote {len(all_rows):,} rows -> {out_path}")
    return len(all_rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry", action="store_true")
    ap.add_argument("--survey", default=None, help="ingest only this survey")
    args = ap.parse_args()

    s = sess()
    surveys = list_surveys(s)
    log(f"BLS surveys: {len(surveys)} -> {surveys}")

    if args.survey:
        surveys = [args.survey]

    total = 0
    for sv in surveys:
        n = ingest_survey(s, sv, args.dry)
        total += n

    log(f"DONE: {total:,} total rows across {len(surveys)} surveys")


if __name__ == "__main__":
    main()
