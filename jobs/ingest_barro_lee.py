#!/usr/bin/env python3
"""Barro-Lee Educational Attainment Dataset ingest (v3).

Source: https://barrolee.github.io/BarroLeeDataSet/
Authors: Robert J. Barro and Jong-Wha Lee
License: Free for academic use
Coverage: 146 countries, 1950-2020 (5-year intervals), by age group, sex

Variables: lu (no schooling %), lp (primary incomplete %), lpc (primary complete %),
           ls (secondary incomplete %), lsc (secondary complete %),
           lh (tertiary incomplete %), lhc (tertiary complete %),
           yr_sch (avg years schooling), yr_sch_pri/sec/ter (by level)

series_key: BARRO_LEE:{variable}:{agefrom}_{ageto}:{sex}:{iso3}
  e.g. BARRO_LEE:yr_sch:25_64:MF:DZA

Output: data/clean_full/barro_lee/barro_lee.parquet
Run: python jobs/ingest_barro_lee.py
"""
from __future__ import annotations
import csv, datetime as dt, io, os, time
import requests
import pyarrow as pa, pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT  = os.path.join(ROOT, "data", "clean_full", "barro_lee")
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

BASE = "https://barrolee.github.io/BarroLeeDataSet/BLData"

# v3 files: 5-year intervals 1950-2020, 146 countries
BL_FILES = [
    # By 5-year age group (agefrom/ageto in each row)
    f"{BASE}/BL_v3_MF.csv",    # both sexes, all age groups
    f"{BASE}/BL_v3_F.csv",     # female, all age groups
    f"{BASE}/BL_v3_M.csv",     # male, all age groups
    # Aggregated for ages 15-64
    f"{BASE}/BL_v3_MF1564.csv",
    f"{BASE}/BL_v3_F1564.csv",
    f"{BASE}/BL_v3_M1564.csv",
    # Aggregated for ages 25-64
    f"{BASE}/BL_v3_MF2564.csv",
    f"{BASE}/BL_v3_F2564.csv",
    f"{BASE}/BL_v3_M2564.csv",
    # v2.2 files (1950-2010, ages 15-99 and 25-99)
    f"{BASE}/BL2013_MF1599_v2.2.csv",
    f"{BASE}/BL2013_F1599_v2.2.csv",
    f"{BASE}/BL2013_M1599_v2.2.csv",
    f"{BASE}/BL2013_MF2599_v2.2.csv",
    f"{BASE}/BL2013_F2599_v2.2.csv",
    f"{BASE}/BL2013_M2599_v2.2.csv",
]

# Value columns to extract
VAL_COLS = ["lu", "lp", "lpc", "ls", "lsc", "lh", "lhc",
            "yr_sch", "yr_sch_pri", "yr_sch_sec", "yr_sch_ter"]


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def fetch(url: str) -> bytes | None:
    try:
        r = requests.get(url, headers=UA, timeout=60, allow_redirects=True)
        if r.status_code == 200 and len(r.content) > 100:
            return r.content
        log(f"  HTTP {r.status_code}: {url[-60:]}")
    except Exception as e:
        log(f"  ERR: {e}")
    return None


def parse_bl_csv(data: bytes, url: str) -> tuple[list, list, list]:
    """Parse a Barro-Lee CSV file from GitHub Pages."""
    text = data.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    headers = list(reader.fieldnames or [])
    log(f"  Columns: {headers[:12]}")

    # Find key columns
    iso_col = next((h for h in headers if h in ("WBcode", "iso3c", "ISO3")), None)
    if iso_col is None:
        # Fall back to country name
        iso_col = next((h for h in headers if h.lower() in ("country", "name")), None)
    year_col = next((h for h in headers if h.lower() == "year"), None)
    sex_col  = next((h for h in headers if h.lower() == "sex"), None)
    agefrom_col = next((h for h in headers
                        if h.lower() in ("agefrom", "age_from", "agefr")), None)
    ageto_col   = next((h for h in headers
                        if h.lower() in ("ageto", "age_to")), None)

    if not iso_col or not year_col:
        log(f"  Missing iso or year columns in {headers[:8]}")
        return [], [], []

    # Available value columns
    available_val_cols = [c for c in VAL_COLS if c in headers]
    if not available_val_cols:
        log(f"  No value columns found; headers={headers[:15]}")
        return [], [], []

    log(f"  Value cols: {available_val_cols}")

    keys, dates, vals = [], [], []
    n_skip = 0

    for rec in reader:
        iso3 = (rec.get(iso_col) or "").strip()
        if not iso3 or iso3.lower() in ("nan", "none", ""):
            n_skip += 1
            continue

        try:
            yr = int(float((rec.get(year_col) or "").strip()))
            obs_d = dt.date(yr, 12, 31)
        except (ValueError, TypeError):
            n_skip += 1
            continue

        # Sex
        sex = (rec.get(sex_col) or "MF").strip()
        if not sex:
            sex = "MF"

        # Age group
        try:
            af = int(float(rec.get(agefrom_col) or 0)) if agefrom_col else 0
            at = int(float(rec.get(ageto_col) or 99)) if ageto_col else 99
            age_s = f"{af}_{at}"
        except (ValueError, TypeError):
            age_s = "all"

        entity = iso3[:15]

        for col in available_val_cols:
            raw = rec.get(col, "")
            if not raw or str(raw).strip() in ("", "NA", ".", "nan"):
                continue
            try:
                v = float(str(raw).strip())
                if v != v:
                    continue
                skey = f"BARRO_LEE:{col}:{age_s}:{sex}:{entity}"
                keys.append(skey)
                dates.append(obs_d)
                vals.append(v)
            except (ValueError, TypeError):
                pass

    log(f"  Parsed {len(vals):,} obs ({n_skip} rows skipped)")
    return keys, dates, vals


def main():
    os.makedirs(OUT, exist_ok=True)
    out = os.path.join(OUT, "barro_lee.parquet")

    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows
        log(f"Barro-Lee: already {n:,} rows"); return

    log("=== Barro-Lee Educational Attainment Ingest (v3 GitHub Pages) ===")

    all_keys, all_dates, all_vals = [], [], []
    seen_keys: set[str] = set()  # deduplicate

    for url in BL_FILES:
        fname = url.split("/")[-1]
        log(f"Fetching {fname}...")
        data = fetch(url)
        if not data:
            continue

        k, d, v = parse_bl_csv(data, url)
        if not v:
            continue

        # Deduplicate across files
        n_before = len(all_vals)
        for ki, di, vi in zip(k, d, v):
            dedup_key = f"{ki}|{di.isoformat()}"
            if dedup_key not in seen_keys:
                seen_keys.add(dedup_key)
                all_keys.append(ki)
                all_dates.append(di)
                all_vals.append(vi)
        n_new = len(all_vals) - n_before
        log(f"  +{n_new:,} new obs (total {len(all_vals):,})")
        time.sleep(0.3)

    if not all_vals:
        log("0 obs — all sources failed")
        return

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out, compression="zstd")
    n = pq.read_metadata(out).num_rows
    log(f"=== Barro-Lee DONE: {n:,} obs ===")


if __name__ == "__main__":
    main()
