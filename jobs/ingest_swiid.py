#!/usr/bin/env python3
"""SWIID (Standardized World Income Inequality Database) ingest.

Source: https://github.com/fsolt/swiid
Author: Frederick Solt (University of Iowa)
License: CC BY 4.0
Coverage: 192 countries, 1960–present, Gini coefficients (disposable + market income)

Downloads the summary CSV from GitHub (no registration required).
Includes: gini_disp (post-tax/transfer), gini_mkt (pre-tax/transfer),
          their standard errors, and absolute/relative redistribution.

series_key: SWIID:{variable}:{country_iso3}  e.g. SWIID:gini_disp:USA

Output: data/clean_full/swiid/swiid.parquet
Run: python jobs/ingest_swiid.py
"""
from __future__ import annotations
import csv, datetime as dt, io, os, time
import requests
import pyarrow as pa, pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT  = os.path.join(ROOT, "data", "clean_full", "swiid")
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

# GitHub raw CSV (HEAD returns 404, GET returns 200 — use stream=True)
SUMMARY_URL = "https://raw.githubusercontent.com/fsolt/swiid/master/data/swiid_summary.csv"
SOURCE_URL  = "https://raw.githubusercontent.com/fsolt/swiid/master/data/swiid_source.csv"

# Numeric columns to extract from the summary CSV
VALUE_COLS = [
    "gini_disp", "gini_disp_se",   # disposable income Gini + SE
    "gini_mkt",  "gini_mkt_se",    # market income Gini + SE
    "abs_red",   "rel_red",         # absolute/relative redistribution
]


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def fetch(url: str) -> bytes | None:
    for attempt in range(3):
        try:
            r = requests.get(url, headers=UA, timeout=60, allow_redirects=True)
            if r.status_code == 200 and len(r.content) > 100:
                log(f"  {len(r.content)//1024:,} KB from {url[-70:]}")
                return r.content
            log(f"  HTTP {r.status_code}: {url[-70:]}")
        except Exception as e:
            log(f"  ERR attempt {attempt+1}: {e}")
        time.sleep(3 * (attempt + 1))
    return None


def parse_swiid(data: bytes, source: str = "SWIID") -> tuple[list, list, list]:
    """Parse SWIID CSV summary.

    Columns: country, year, gini_disp, gini_disp_se, gini_mkt, gini_mkt_se,
             abs_red, abs_red_se, rel_red, rel_red_se, gini_disp_05, gini_disp_95, ...
    country is the country name (not ISO3); year is integer.
    """
    text = data.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    headers = [h.strip() for h in (reader.fieldnames or [])]
    log(f"  Columns: {headers[:15]}")

    # Find country identifier (SWIID uses country name, not ISO3)
    ctry_col = next((h for h in headers if h.lower() in ("country", "iso", "iso3", "code")), None)
    year_col = next((h for h in headers if h.lower() in ("year", "yr")), None)

    if not ctry_col or not year_col:
        log(f"  Missing country/year columns. Headers: {headers}"); return [], [], []

    # Find available value columns
    val_cols = [c for c in VALUE_COLS if c in headers]
    # Also grab any extra columns not in our list but numeric-looking
    extra = [h for h in headers if h not in (ctry_col, year_col) and h not in val_cols
             and h.lower() not in ("country", "year", "iso", "iso3")]
    val_cols.extend(extra)
    log(f"  Value cols ({len(val_cols)}): {val_cols[:10]}")

    keys, dates, vals = [], [], []
    for rec in reader:
        ctry = (rec.get(ctry_col) or "").strip()
        if not ctry:
            continue

        yr_raw = rec.get(year_col, "")
        try:
            yr = int(float(str(yr_raw).strip()))
            obs_d = dt.date(yr, 12, 31)
        except (TypeError, ValueError):
            continue

        for col in val_cols:
            raw = rec.get(col, "")
            if not raw or str(raw).strip() in ("", "NA", "NaN", "nan"):
                continue
            try:
                v = float(str(raw).strip())
                if v != v:
                    continue
                # Use country name as identifier (no ISO3 in base SWIID)
                safe_ctry = ctry.replace(":", "_").replace("/", "_")[:30]
                keys.append(f"SWIID:{col}:{safe_ctry}")
                dates.append(obs_d)
                vals.append(v)
            except (TypeError, ValueError):
                pass

    log(f"  Parsed {len(vals):,} obs from {len(set(keys)):,} series")
    return keys, dates, vals


def main():
    os.makedirs(OUT, exist_ok=True)
    out = os.path.join(OUT, "swiid.parquet")

    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows
        log(f"SWIID: already {n:,} rows"); return

    log("=== SWIID Ingest ===")

    # Download summary (main product — Gini estimates for 192 countries)
    log("Downloading SWIID summary CSV...")
    data = fetch(SUMMARY_URL)
    if not data:
        log("FAILED"); return

    keys, dates, vals = parse_swiid(data)

    if not vals:
        log("0 observations parsed"); return

    tbl = pa.table({
        "series_key": pa.array(keys,  pa.string()),
        "obs_date":   pa.array(dates, pa.date32()),
        "value":      pa.array(vals,  pa.float64()),
    })
    pq.write_table(tbl, out, compression="zstd")
    n = pq.read_metadata(out).num_rows
    log(f"=== SWIID DONE: {n:,} obs ===")


if __name__ == "__main__":
    main()
