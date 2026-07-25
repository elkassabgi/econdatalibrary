#!/usr/bin/env python3
"""World Bank World Development Indicators (WDI) -- full bulk ingest.

Source: https://datatopics.worldbank.org/world-development-indicators/
License: CC BY 4.0
Coverage: ~1,400 indicators × 260+ countries, 1960-present

Downloads the official WDI bulk CSV ZIP from the World Bank data API,
parses the wide-format WDICSV.csv into long format, and writes one
Parquet file per indicator batch (to avoid millions of tiny files).

series_key format:  WDI:{IndicatorCode}:{CountryCode}
  e.g. WDI:NY.GDP.MKTP.KD:USA

Output: data/clean_full/worldbank_wdi/
  worldbank_wdi.parquet  (all indicators concatenated)
  OR one parquet per indicator if total > 100M rows

Run: python jobs/ingest_worldbank_wdi.py
"""
from __future__ import annotations
import csv
import datetime as dt
import io
import os
import time
import zipfile

import requests
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT  = os.path.join(ROOT, "data", "clean_full", "worldbank_wdi")
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

# World Bank bulk CSV download - the official bulk endpoint
WDI_ZIP_URL = "https://api.worldbank.org/v2/en/indicator?downloadformat=csv"
# Fallback: direct ZIP URL
WDI_ZIP_URL2 = "https://databank.worldbank.org/data/download/WDI_CSV.zip"

BATCH = 1_000_000   # rows per parquet file
LOG_EVERY = 100     # print progress every N indicators


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def fetch_zip(retries: int = 3) -> bytes | None:
    """Download WDI ZIP with retries, try multiple URLs."""
    for url in [WDI_ZIP_URL, WDI_ZIP_URL2]:
        for attempt in range(retries):
            try:
                log(f"  Downloading from {url[:70]}...")
                r = requests.get(url, headers=UA, timeout=300, allow_redirects=True,
                                 stream=True)
                if r.status_code == 200:
                    chunks = []
                    total = 0
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        if chunk:
                            chunks.append(chunk)
                            total += len(chunk)
                    data = b"".join(chunks)
                    if len(data) > 100_000:
                        log(f"  Downloaded {len(data)//1024:,} KB")
                        return data
                    log(f"  Response too small ({len(data)} bytes)")
                else:
                    log(f"  HTTP {r.status_code}")
            except Exception as e:
                log(f"  ERR attempt {attempt+1}: {e}")
            time.sleep(5 * (attempt + 1))
    return None


def main():
    os.makedirs(OUT, exist_ok=True)
    out_path = os.path.join(OUT, "worldbank_wdi.parquet")

    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"WDI: already {n:,} rows"); return

    log("=== World Bank WDI Bulk Ingest ===")
    data = fetch_zip()
    if not data:
        log("FAILED: could not download WDI ZIP"); return

    # Open the ZIP and find WDICSV.csv
    try:
        z = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile as e:
        log(f"Not a valid ZIP: {e}"); return

    members = z.namelist()
    log(f"ZIP contains: {members[:10]}")

    # Look for the main data CSV (WDICSV.csv or similar)
    data_csv = next((m for m in members if "WDICSV" in m.upper() and m.endswith(".csv")), None)
    if data_csv is None:
        data_csv = next((m for m in members if m.endswith(".csv") and "metadata" not in m.lower()), None)
    if data_csv is None:
        log(f"Could not find data CSV in ZIP. Members: {members}"); return

    log(f"Parsing {data_csv}...")
    raw = z.read(data_csv).decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(raw))
    headers = reader.fieldnames or []
    log(f"Headers (first 10): {headers[:10]}")

    # Year columns: numeric strings like '1960', '1961', ...
    year_cols = [(col, int(col)) for col in headers if col.strip().isdigit() and len(col.strip()) == 4]
    log(f"  {len(year_cols)} year columns: {year_cols[0][0]}..{year_cols[-1][0]}")

    # Country Code and Indicator Code columns
    ctry_col = next((h for h in headers if h.strip().lower() in ("country code", "countrycode")), None)
    ind_col  = next((h for h in headers if h.strip().lower() in ("indicator code", "indicatorcode")), None)

    if not ctry_col or not ind_col:
        log(f"Missing country/indicator columns. Headers: {headers}"); return

    log(f"Country col='{ctry_col}', Indicator col='{ind_col}'")

    all_keys, all_dates, all_vals = [], [], []
    n_indicators = 0
    n_skipped = 0
    n_rows = 0
    writer = None
    file_idx = 0

    def flush(final=False):
        nonlocal writer, file_idx, all_keys, all_dates, all_vals
        if not all_keys:
            return
        tbl = pa.table({
            "series_key": pa.array(all_keys,  pa.string()),
            "obs_date":   pa.array(all_dates, pa.date32()),
            "value":      pa.array(all_vals,  pa.float64()),
        })
        if writer is None:
            writer = pq.ParquetWriter(out_path, tbl.schema, compression="zstd")
        writer.write_table(tbl)
        all_keys.clear(); all_dates.clear(); all_vals.clear()

    for row in reader:
        n_rows += 1
        ctry = (row.get(ctry_col) or "").strip()
        ind  = (row.get(ind_col)  or "").strip()
        if not ctry or not ind:
            n_skipped += 1
            continue

        series_key_base = f"WDI:{ind}:{ctry}"
        has_data = False

        for year_str, yr in year_cols:
            raw_val = (row.get(year_str) or "").strip()
            if not raw_val:
                continue
            try:
                v = float(raw_val)
                if v != v:   # nan
                    continue
                all_keys.append(series_key_base)
                all_dates.append(dt.date(yr, 12, 31))
                all_vals.append(v)
                has_data = True
            except (TypeError, ValueError):
                continue

        if has_data:
            n_indicators += 1

        if len(all_keys) >= BATCH:
            flush()

        if n_rows % 10000 == 0:
            log(f"  {n_rows:,} rows processed, {len(all_keys):,} pending obs")

    flush(final=True)
    if writer is not None:
        writer.close()
    else:
        log("0 observations parsed")
        return

    n_total = pq.read_metadata(out_path).num_rows
    log(f"=== WDI DONE: {n_total:,} observations | {n_rows:,} source rows | "
        f"{n_indicators:,} series with data ===")


if __name__ == "__main__":
    main()
