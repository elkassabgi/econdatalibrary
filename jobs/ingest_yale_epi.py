#!/usr/bin/env python3
"""Yale Environmental Performance Index (EPI) ingest.

Source: https://epi.yale.edu/downloads
License: CC BY 4.0 (Yale Center for Environmental Law & Policy)
Coverage: 180 countries, 2000-present, 40+ environmental indicators.
2024 release: https://epi.yale.edu/downloads/epi2024results.csv

series_key: EPI:{variable}:{iso3}  e.g. EPI:EPI.new:USA
Output: data/clean_full/yale_epi/yale_epi.parquet
Run: python jobs/ingest_yale_epi.py
"""
from __future__ import annotations
import csv, datetime as dt, io, os, time
import requests, pyarrow as pa, pyarrow.parquet as pq

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "yale_epi")
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

# Confirmed working URLs from https://epi.yale.edu/downloads
RESULT_URLS = [
    ("https://epi.yale.edu/downloads/epi2024results.csv", 2024),
]
# Abbreviation/variable name file (to decode column names)
VARIABLES_URL = "https://epi.yale.edu/downloads/epi2024variables2024-12-11.csv"


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def fetch(url):
    try:
        r = requests.get(url, headers=UA, timeout=60, allow_redirects=True)
        if r.status_code == 200 and len(r.content) > 500:
            return r.content
        log(f"  HTTP {r.status_code}: {url}")
    except Exception as e:
        log(f"  ERR: {e}")
    return None


def parse_epi_csv(data: bytes, default_year: int):
    """Parse EPI results CSV. Wide format: iso column + many indicator columns."""
    text = data.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    headers = reader.fieldnames or []
    log(f"  Headers ({len(headers)}): {headers[:12]}")

    # EPI 2024 uses 'iso' for ISO3
    iso3_col = next((h for h in headers if h.lower() in
                     ("iso", "iso3", "iso_code", "country_iso3", "code")), None)
    year_col = next((h for h in headers if h.lower() in ("year", "yr")), None)

    if not iso3_col:
        log(f"  No ISO3 column found"); return [], [], []

    skip = {(iso3_col or "").lower(), (year_col or "").lower(),
            "country", "region", "continent", "rank", "tier", "country.name"}

    keys, dates, vals = [], [], []
    for row in reader:
        iso3 = (row.get(iso3_col) or "").strip()
        if not iso3 or len(iso3) != 3:
            continue

        if year_col and row.get(year_col):
            try:
                yr = int(float(row[year_col]))
            except (ValueError, TypeError):
                yr = default_year
        else:
            yr = default_year
        obs_d = dt.date(yr, 12, 31)

        for col, raw in row.items():
            if col.lower().strip() in skip or not col:
                continue
            if not raw or str(raw).strip() in ("", "NA", "N/A", "nan", "#N/A", "-"):
                continue
            try:
                v = float(str(raw).replace(",", ""))
                if v != v:
                    continue
                keys.append(f"EPI:{col.strip()}:{iso3}")
                dates.append(obs_d)
                vals.append(v)
            except (TypeError, ValueError):
                pass

    return keys, dates, vals


def main():
    os.makedirs(OUT, exist_ok=True)
    out = os.path.join(OUT, "yale_epi.parquet")
    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows
        log(f"Yale EPI: already {n:,} rows"); return

    log("=== Yale EPI 2024 Ingest ===")
    all_keys, all_dates, all_vals = [], [], []

    for url, yr in RESULT_URLS:
        log(f"Downloading {url}...")
        data = fetch(url)
        if data:
            k, d, v = parse_epi_csv(data, default_year=yr)
            log(f"  {yr}: {len(v):,} obs")
            all_keys.extend(k); all_dates.extend(d); all_vals.extend(v)
        time.sleep(1)

    if not all_vals:
        log("0 observations parsed"); return

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out, compression="zstd")
    n = pq.read_metadata(out).num_rows
    log(f"=== Yale EPI DONE: {n:,} obs ===")


if __name__ == "__main__":
    main()
