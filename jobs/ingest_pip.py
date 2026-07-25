#!/usr/bin/env python3
"""World Bank Poverty & Inequality Platform (PIP) ingest.

Source: https://pip.worldbank.org/
License: CC BY 4.0 (World Bank)
Coverage: ~100 countries, 1960s-present, poverty headcount/gap, Gini, mean.

Uses the PIP REST API at api.worldbank.org/pip/v1 (no auth required).
series_key: PIP:{indicator}:{country_code}:{pov_line}
  e.g. PIP:headcount:IND:215  (povline 215 = $2.15/day in 2017 PPP)

Output: data/clean_full/pip/pip.parquet
Run: python jobs/ingest_pip.py
"""
from __future__ import annotations
import datetime as dt, os, time
import requests, pyarrow as pa, pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT  = os.path.join(ROOT, "data", "clean_full", "pip")
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

API = "https://api.worldbank.org/pip/v1/pip"

# Poverty lines in 2017 PPP $/day (as floats)
POV_LINES = [1.00, 1.90, 2.15, 3.20, 3.65, 5.50, 6.85, 10.00, 15.00, 21.70]
POV_LABELS = {1.00:"100", 1.90:"190", 2.15:"215", 3.20:"320", 3.65:"365",
              5.50:"550", 6.85:"685", 10.00:"1000", 15.00:"1500", 21.70:"2170"}

# Distributional indicators (present regardless of poverty line)
DIST_INDICATORS = [
    "mean","median","mld","gini","polarization",
    "decile1","decile2","decile3","decile4","decile5",
    "decile6","decile7","decile8","decile9","decile10",
]
# Poverty-line-specific indicators
POV_INDICATORS = ["headcount","poverty_gap","poverty_severity","watts"]

RATE = 1.0   # seconds between poverty-line calls (API is generous but we're polite)


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def get_rows(params, retries=4):
    for attempt in range(retries):
        try:
            r = requests.get(API, params=params, headers=UA, timeout=120)
            if r.status_code == 200:
                j = r.json()
                return j if isinstance(j, list) else []
            if r.status_code == 429:
                time.sleep(30); continue
        except Exception as e:
            if attempt >= retries - 1:
                log(f"  ERR: {e}")
        time.sleep(3 * (attempt + 1))
    return []


def main():
    os.makedirs(OUT, exist_ok=True)
    out = os.path.join(OUT, "pip.parquet")
    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows
        log(f"PIP: already {n:,} rows"); return

    log("=== World Bank PIP Ingest ===")
    all_keys, all_dates, all_vals = [], [], []
    writer = None
    BATCH = 500_000

    def flush():
        nonlocal writer, all_keys, all_dates, all_vals
        if not all_keys:
            return
        tbl = pa.table({
            "series_key": pa.array(all_keys,  pa.string()),
            "obs_date":   pa.array(all_dates, pa.date32()),
            "value":      pa.array(all_vals,  pa.float64()),
        })
        if writer is None:
            writer = pq.ParquetWriter(out, tbl.schema, compression="zstd")
        writer.write_table(tbl)
        all_keys.clear(); all_dates.clear(); all_vals.clear()

    # 1. Poverty-line-specific indicators
    for pline in POV_LINES:
        label = POV_LABELS[pline]
        log(f"Poverty line ${pline:.2f}/day ({label} label)...")
        rows = get_rows({
            "country": "all", "year": "all",
            "povline": pline, "fill_gaps": "false",
            "welfare_type": "all", "reporting_level": "national",
            "format": "json",
        })
        log(f"  {len(rows)} records")
        for rec in rows:
            ctry = (rec.get("country_code") or "").strip()
            yr   = rec.get("reporting_year") or rec.get("year")
            if not ctry or not yr:
                continue
            try:
                obs_d = dt.date(int(yr), 12, 31)
            except (TypeError, ValueError):
                continue
            for ind in POV_INDICATORS + DIST_INDICATORS:
                v = rec.get(ind)
                if v is None:
                    continue
                try:
                    fv = float(v)
                    if fv != fv:
                        continue
                    all_keys.append(f"PIP:{ind}:{ctry}:{label}")
                    all_dates.append(obs_d)
                    all_vals.append(fv)
                except (TypeError, ValueError):
                    pass
        if len(all_keys) >= BATCH:
            flush()
        time.sleep(RATE)

    flush()
    if writer:
        writer.close()
        n = pq.read_metadata(out).num_rows
        log(f"=== PIP DONE: {n:,} obs ===")
    else:
        log("0 obs written")


if __name__ == "__main__":
    main()
