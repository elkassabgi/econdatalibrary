#!/usr/bin/env python3
"""Full-coverage ingest of Frankfurter (ECB euro foreign-exchange reference rates).

Frankfurter serves the ECB daily reference rates with EUR as base. The full
daily history runs 1999-01-04..today. We pull the ENTIRE range year-by-year
(polite, retried) and union every currency that has EVER appeared -- including
currencies the ECB has since stopped publishing (CYP, EEK, GRD, SIT, SKK, ROL,
TRL, LTL, LVL, MTL, RUB, ARS, BGN, DZD, HRK, MAD, TWD), not just the ~30 that
are currently active.

GROUPED storage (anti-bloat): ONE Parquet for the whole source --
  data/clean_full/frankfurter/frankfurter_fx_eur.parquet
with columns:
  series_key  (e.g. "EURUSD" == EUR-base reference rate for USD)
  obs_date    (date32)
  value       (units of the quoted currency per 1 EUR)

License = ecb-attrib-nomodify (reservable id from configs/sources.yaml).
Attribution: "FX data from the ECB via Frankfurter".

Usage:
  python jobs/ingest_frankfurter.py --dry   # enumerate only, no writes
  python jobs/ingest_frankfurter.py         # full run
"""
import datetime as dt
import os
import sys
import time

import requests
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
sys.path.insert(0, ROOT)

SOURCE_ID = "frankfurter"
LICENSE_ID = "ecb-attrib-nomodify"
BASE = "EUR"
OUT_DIR = os.path.join(ROOT, "data", "clean_full", SOURCE_ID)
OUT_FILE = os.path.join(OUT_DIR, "frankfurter_fx_eur.parquet")
API = "https://api.frankfurter.app"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
START_YEAR = 1999  # ECB rates begin 1999-01-04


def get_json(sess, url, tries=5):
    for attempt in range(tries):
        try:
            r = sess.get(url, timeout=180)
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001
            if attempt == tries - 1:
                raise
            wait = min(2 ** attempt, 30)
            print(f"    retry {attempt + 1}/{tries} after error: {e} (sleep {wait}s)", flush=True)
            time.sleep(wait)


def main():
    dry = "--dry" in sys.argv
    sess = requests.Session()
    sess.headers.update(UA)

    today = dt.date.today()
    end_year = today.year
    print(f"{'DRY-RUN' if dry else 'FULL'}: Frankfurter ECB FX, base={BASE}, "
          f"range {START_YEAR}-01-04..{today.isoformat()}", flush=True)

    # currency-name lookup (current set; retired ones fall back to code)
    names = get_json(sess, f"{API}/currencies")
    print(f"Currently-active currencies listed by API: {len(names)}", flush=True)

    # series_key -> {obs_date(str): value}; dedupe across overlapping years.
    series = {}
    total_cells = 0
    for yr in range(START_YEAR, end_year + 1):
        start = f"{yr}-01-01"
        end = f"{yr}-12-31"
        url = f"{API}/{start}..{end}"
        d = get_json(sess, url)
        rates = d.get("rates", {})
        ndays = len(rates)
        cells = 0
        for day, row in rates.items():
            for ccy, val in row.items():
                if val is None:
                    continue
                key = f"{BASE}{ccy}"
                series.setdefault(key, {})[day] = float(val)
                cells += 1
        total_cells += cells
        print(f"  {yr}: business_days={ndays:>4}  cells={cells:>7,}", flush=True)
        time.sleep(0.25)  # polite pacing; concurrency=1

    allcur = sorted({k[len(BASE):] for k in series})
    print(f"\nCOMPLETE currency set across full history: {len(allcur)} series", flush=True)
    print("  " + ", ".join(allcur), flush=True)
    print(f"Total non-null cells gathered (pre-dedupe sum): {total_cells:,}", flush=True)

    # Build columnar table from deduped dict.
    keys_col, date_col, val_col = [], [], []
    for key in sorted(series):
        for day in sorted(series[key]):
            keys_col.append(key)
            date_col.append(dt.date.fromisoformat(day))
            val_col.append(series[key][day])

    n_obs = len(keys_col)
    n_series = len(series)
    dmin = min(date_col).isoformat() if date_col else None
    dmax = max(date_col).isoformat() if date_col else None
    print(f"\nDeduped observations to write: {n_obs:,} across {n_series} series "
          f"({dmin}..{dmax})", flush=True)

    if dry:
        # show per-series counts
        from collections import Counter
        cnt = Counter(keys_col)
        print("Per-series obs counts:", flush=True)
        for k in sorted(cnt):
            print(f"    {k}: {cnt[k]:,}", flush=True)
        print("DRY: no files written.", flush=True)
        return

    os.makedirs(OUT_DIR, exist_ok=True)
    tbl = pa.table({
        "series_key": pa.array(keys_col, type=pa.string()),
        "obs_date": pa.array(date_col, type=pa.date32()),
        "value": pa.array(val_col, type=pa.float64()),
    })
    pq.write_table(tbl, OUT_FILE, compression="zstd")
    size_mb = os.path.getsize(OUT_FILE) / 1e6
    print(f"\nWROTE {OUT_FILE}  ({size_mb:.2f} MB, 1 file)", flush=True)
    print(f"DONE: {n_series} series / {n_obs:,} observations", flush=True)


if __name__ == "__main__":
    main()
