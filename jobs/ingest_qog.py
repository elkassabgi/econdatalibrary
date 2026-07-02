#!/usr/bin/env python3
"""Quality of Government (QoG) Standard Dataset — Gothenburg University, Jan 2025.

License: Creative Commons Attribution 4.0 (CC BY 4.0)
Source: https://www.gu.se/en/quality-government/qog-data
No API key required (direct CSV download).

Coverage: ~2,000 variables from 100+ data sources, ~190 countries, 1946–2024.
Includes (among others):
  * Fraser EFW (Economic Freedom of the World)
  * Freedom House (political rights, civil liberties)
  * Heritage Index of Economic Freedom
  * Polity IV / Polity5 (regime type)
  * V-Dem (varieties of democracy)
  * World Bank WDI (GDP, poverty, trade)
  * IMF WEO (fiscal, monetary)
  * SIPRI (military expenditure)
  * UNDP HDI
  * Worldwide Governance Indicators
  * UN Population Division
  * Many more specialized sources

Run: python jobs/ingest_qog.py
"""
from __future__ import annotations
import csv, datetime as dt, io, os, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "qog")
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

# QoG Standard time-series dataset (country-year)
URLS = [
    "https://www.qogdata.pol.gu.se/data/qog_std_ts_jan25.csv",
    "https://www.qogdata.pol.gu.se/data/qog_std_ts_jan24.csv",
]

# Columns that identify the observation (not data variables)
ID_COLS = frozenset({
    "cname_qog", "cname", "year", "ccodecow", "ccodealp", "ccodealp_year",
    "ccode_qog", "cname_year", "ccode", "version", "date", "ccowhist",
    "ccowhist_year",
})


def log(m):
    try:
        print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
    except UnicodeEncodeError:
        print(f"[{time.strftime('%H:%M:%S')}] {str(m).encode('ascii','replace').decode()}", flush=True)


def main():
    os.makedirs(OUT, exist_ok=True)
    out_path = os.path.join(OUT, "qog.parquet")
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"Already done: {n:,} rows"); return

    content = None
    for url in URLS:
        log(f"Downloading {url}")
        try:
            r = requests.get(url, headers=UA, timeout=300)
            if r.status_code == 200 and len(r.content) > 100_000:
                log(f"  OK: {len(r.content):,} bytes")
                content = r.content
                break
            log(f"  HTTP {r.status_code}")
        except Exception as e:
            log(f"  ERR: {e}")

    if not content:
        log("All URLs failed"); return

    log("Parsing CSV (wide-to-long melt)...")
    try:
        text = content.decode("utf-8", errors="replace")
    except Exception:
        text = content.decode("latin-1", errors="replace")

    reader = csv.DictReader(io.StringIO(text))
    headers = reader.fieldnames or []
    log(f"Columns: {len(headers)} total, {len([h for h in headers if h not in ID_COLS])} data columns")

    # Identify identifier and data columns
    data_cols = [h for h in headers if h not in ID_COLS and h]
    log(f"Data columns (sample): {data_cols[:10]} ...")

    all_keys, all_dates, all_vals = [], [], []
    seen: set[tuple] = set()
    n_rows = 0
    n_skipped = 0

    for row in reader:
        n_rows += 1

        # Extract year
        yr_raw = row.get("year", "")
        if not yr_raw:
            n_skipped += 1; continue
        try:
            yr = int(float(yr_raw))
            if not (1800 <= yr <= 2030):
                n_skipped += 1; continue
        except (ValueError, TypeError):
            n_skipped += 1; continue

        obs_date = dt.date(yr, 12, 31)

        # Build country identifier
        ccode = row.get("ccodealp", "").strip()  # ISO3 preferred
        if not ccode:
            ccode = row.get("cname", "").strip()[:30]
        if not ccode:
            n_skipped += 1; continue

        # Emit one observation per non-empty data column
        for col in data_cols:
            v_raw = row.get(col, "")
            if not v_raw or v_raw in ("", "NA", "N/A", ".", ".."):
                continue
            try:
                v = float(v_raw)
                if v != v:  # NaN check
                    continue
            except (ValueError, TypeError):
                continue

            series_key = f"QOG:{col}:{ccode}"
            tok = (series_key, obs_date)
            if tok not in seen:
                seen.add(tok)
                all_keys.append(series_key)
                all_dates.append(obs_date)
                all_vals.append(v)

        if n_rows % 5000 == 0:
            log(f"  Processed {n_rows:,} rows, {len(all_vals):,} obs so far")

    log(f"  Total rows processed: {n_rows:,} ({n_skipped} skipped)")
    log(f"  Total observations: {len(all_vals):,}")

    if not all_vals:
        log("0 observations — check CSV format"); return

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"DONE: {n:,} QoG observations ({len(set(all_keys))} series)")


if __name__ == "__main__":
    main()
