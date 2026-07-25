#!/usr/bin/env python3
"""IMF DataMapper — free public API, no key required.

License: IMF Terms of Use (free for non-commercial research)
Source: https://www.imf.org/external/datamapper/api/v1/

Coverage:
  * 132 WEO / IFS indicators: GDP growth, inflation, debt, current account,
    unemployment, REER, commodity prices, etc.
  * ~190 countries + country groups
  * Annual data (~1980–present per indicator)

Strategy:
  * List all indicators from /api/v1/indicators
  * For each indicator: GET /api/v1/{indicator} → country × year matrix
  * Single Parquet: all indicators merged in long format

Run: python jobs/ingest_imf_weo.py
"""
from __future__ import annotations
import datetime as dt, os, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT  = os.path.join(ROOT, "data", "clean_full", "imf_weo")
BASE = "https://www.imf.org/external/datamapper/api/v1"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
        "Accept": "application/json"}
RATE = 0.5
import sys as _sys
_enc = _sys.stdout.encoding or "utf-8"


def log(m):
    try:
        print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
    except UnicodeEncodeError:
        print(f"[{time.strftime('%H:%M:%S')}] {m}".encode(_enc, errors='replace').decode(_enc), flush=True)


def get_json(url: str, retries: int = 4) -> dict | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=60)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 404):
                return None
            if r.status_code == 429:
                time.sleep(60); continue
            log(f"  HTTP {r.status_code} attempt {attempt+1}: {url[-80:]}")
        except Exception as e:
            log(f"  ERR attempt {attempt+1}: {e}")
        time.sleep(5 * (attempt + 1))
    return None


def main():
    os.makedirs(OUT, exist_ok=True)
    out_path = os.path.join(OUT, "imf_weo.parquet")
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"Already done: {n:,} rows in {out_path}"); return

    # Get indicator list
    meta = get_json(f"{BASE}/indicators")
    if not meta or "indicators" not in meta:
        log("ERROR: could not get indicator list"); return

    indicators = meta["indicators"]
    log(f"Found {len(indicators)} indicators")

    all_keys, all_dates, all_vals = [], [], []

    for i, (code, info) in enumerate(indicators.items(), 1):
        label = (info.get("label", "") or info.get("description", "") or code)[:60]
        log(f"[{i}/{len(indicators)}] {code}: {label}")

        data = get_json(f"{BASE}/{code}")
        if not data or "values" not in data:
            time.sleep(RATE); continue

        # Structure: {"values": {"IND_CODE": {"USA": {"2020": 2.3, ...}, ...}}}
        ind_vals = data["values"].get(code, {})
        for country_code, year_map in ind_vals.items():
            for yr_str, val in year_map.items():
                if val is None:
                    continue
                try:
                    v = float(val)
                    yr = int(yr_str)
                except (TypeError, ValueError):
                    continue
                # Use Dec 31 for annual obs
                d = dt.date(yr, 12, 31)
                all_keys.append(f"{code}:{country_code}")
                all_dates.append(d)
                all_vals.append(v)

        time.sleep(RATE)
        if i % 20 == 0:
            log(f"  ... {len(all_vals):,} obs so far")

    if not all_vals:
        log("0 observations collected"); return

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"DONE: {n:,} IMF WEO/DataMapper observations → {out_path}")


if __name__ == "__main__":
    main()
