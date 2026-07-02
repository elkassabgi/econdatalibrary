#!/usr/bin/env python3
"""Social Progress Index (SPI) ingest.

Source: https://www.socialprogress.org/
License: CC BY 4.0 (Social Progress Imperative)
Coverage: 170 countries, 2011-present, 54 social/environmental indicators.

Downloads SPI data directly from Social Progress Imperative.
series_key: SPI:{indicator}:{iso3}  e.g. SPI:SPI:{USA}

Output: data/clean_full/spi/spi.parquet
Run: python jobs/ingest_spi.py
"""
from __future__ import annotations
import csv, datetime as dt, io, os, time, zipfile
import requests, pyarrow as pa, pyarrow.parquet as pq

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "spi")
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

# Social Progress Index data downloads
URLS = [
    # Direct download from SPI (no auth needed)
    "https://www.socialprogress.org/static/c7c50b81c43e0e2dca6a3b7e7e9a3c21/2023-social-progress-index-results.csv",
    "https://www.socialprogress.org/static/9ba06bf2ebab87e4a1a5f77fbe52db5b/2022-social-progress-index-results.csv",
    # GitHub mirrors (open data)
    "https://raw.githubusercontent.com/datasets/social-progress-index/main/data/spi.csv",
    # Kaggle-hosted open mirror
    "https://raw.githubusercontent.com/social-progress-imperative/open-data/main/spi_complete.csv",
]

# Also try the Tableau public API which SPI uses
TABLEAU_URL = "https://query.data.world/s/spidata"

def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def fetch(url, timeout=60):
    try:
        r = requests.get(url, headers=UA, timeout=timeout, allow_redirects=True)
        if r.status_code == 200 and len(r.content) > 500:
            return r.content
        log(f"  HTTP {r.status_code}: {url[-60:]}")
    except Exception as e:
        log(f"  ERR: {e}")
    return None


def parse_spi_csv(data: bytes):
    """Parse SPI CSV. May be long or wide format."""
    text = data.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    headers = reader.fieldnames or []
    log(f"  Headers: {headers[:12]}, total {len(headers)}")

    # Identify key columns
    iso3_col = next((h for h in headers if h.lower().strip() in
                     ("country_iso3", "iso3", "iso", "code", "country_code",
                      "iso3code", "iso_code")), None)
    year_col = next((h for h in headers if h.lower().strip() in
                     ("year", "edition", "yr")), None)

    if not iso3_col:
        log(f"  No ISO3 column found in {headers[:8]}"); return [], [], []

    skip_cols = {(iso3_col or "").lower(), (year_col or "").lower(),
                 "country", "region", "continent", "rank", "tier"}

    keys, dates, vals = [], [], []

    for row in reader:
        iso3 = (row.get(iso3_col) or "").strip()
        if not iso3 or len(iso3) > 4:
            continue

        yr = None
        if year_col and row.get(year_col):
            try:
                yr = int(float(row[year_col]))
            except (ValueError, TypeError):
                pass
        if yr is None:
            yr = 2023  # default to latest

        obs_d = dt.date(yr, 12, 31)

        for col, raw in row.items():
            if col.lower().strip() in skip_cols or not col:
                continue
            if not raw or str(raw).strip() in ("", "NA", "N/A", "nan", "-", "--"):
                continue
            try:
                v = float(str(raw).replace(",", ""))
                if v != v:
                    continue
                keys.append(f"SPI:{col.strip()}:{iso3}")
                dates.append(obs_d)
                vals.append(v)
            except (TypeError, ValueError):
                pass

    return keys, dates, vals


def main():
    os.makedirs(OUT, exist_ok=True)
    out = os.path.join(OUT, "spi.parquet")
    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows
        log(f"SPI: already {n:,} rows"); return

    log("=== Social Progress Index Ingest ===")
    all_keys, all_dates, all_vals = [], [], []

    for url in URLS:
        log(f"Trying {url[-70:]}...")
        data = fetch(url)
        if data:
            if data[:2] == b"PK":  # ZIP
                z = zipfile.ZipFile(io.BytesIO(data))
                for cf in [m for m in z.namelist() if m.endswith(".csv")]:
                    k, d, v = parse_spi_csv(z.read(cf))
                    all_keys.extend(k); all_dates.extend(d); all_vals.extend(v)
            else:
                k, d, v = parse_spi_csv(data)
                all_keys.extend(k); all_dates.extend(d); all_vals.extend(v)
            if all_vals:
                log(f"  Got {len(all_vals):,} obs")
                break
        time.sleep(1)

    if not all_vals:
        log("0 observations parsed — all URLs failed"); return

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out, compression="zstd")
    n = pq.read_metadata(out).num_rows
    log(f"=== SPI DONE: {n:,} obs ===")


if __name__ == "__main__":
    main()
