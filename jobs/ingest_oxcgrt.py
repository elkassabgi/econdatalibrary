#!/usr/bin/env python3
"""Oxford COVID-19 Government Response Tracker (OxCGRT) ingest.

Source: https://github.com/OxCGRT/covid-policy-tracker
License: CC BY 4.0 (University of Oxford)
Coverage: 185 countries, Jan 2020-Dec 2022, 23 policy indicators + indices.

Downloads OxCGRT v4 data from GitHub (public, no auth).
series_key: OXCGRT:{indicator}:{iso3}  e.g. OXCGRT:StringencyIndex_Average:GBR

Output: data/clean_full/oxcgrt/oxcgrt.parquet
Run: python jobs/ingest_oxcgrt.py
"""
from __future__ import annotations
import csv, datetime as dt, io, os, time
import requests, pyarrow as pa, pyarrow.parquet as pq

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "oxcgrt")
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

# OxCGRT national-level data (latest)
URLS = [
    "https://raw.githubusercontent.com/OxCGRT/covid-policy-dataset/main/data/OxCGRT_compact_national_v1.csv",
    "https://raw.githubusercontent.com/OxCGRT/covid-policy-tracker/master/data/OxCGRT_latest.csv",
    "https://raw.githubusercontent.com/OxCGRT/covid-policy-tracker/master/data/OxCGRT_nat_latest.csv",
]

BATCH = 1_000_000  # rows per parquet flush


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def fetch(url):
    try:
        r = requests.get(url, headers=UA, timeout=180, stream=True)
        if r.status_code == 200:
            chunks = []
            total = 0
            for chunk in r.iter_content(chunk_size=1 << 20):
                if chunk:
                    chunks.append(chunk)
                    total += len(chunk)
            data = b"".join(chunks)
            if len(data) > 10_000:
                log(f"  Downloaded {len(data)//1024:,} KB")
                return data
        log(f"  HTTP {r.status_code}: {url[-70:]}")
    except Exception as e:
        log(f"  ERR: {e}")
    return None


def main():
    os.makedirs(OUT, exist_ok=True)
    out = os.path.join(OUT, "oxcgrt.parquet")
    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows
        log(f"OxCGRT: already {n:,} rows"); return

    log("=== OxCGRT Ingest ===")
    data = None
    for url in URLS:
        log(f"Trying {url[-70:]}...")
        data = fetch(url)
        if data:
            break
        time.sleep(2)

    if not data:
        log("FAILED: could not download OxCGRT"); return

    text = data.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    headers = reader.fieldnames or []
    log(f"Headers ({len(headers)}): {headers[:12]}")

    iso3_col   = next((h for h in headers if h.lower() in ("countrycode", "country_code", "iso")), None)
    date_col   = next((h for h in headers if h.lower() in ("date",)), None)

    if not iso3_col or not date_col:
        log(f"Missing columns. Headers: {headers[:15]}"); return

    skip = {iso3_col.lower(), date_col.lower(),
            "countryname", "country_name", "jurisdiction", "regioncode", "regionname"}

    # Identify numeric indicator columns
    value_cols = [h for h in headers if h.lower() not in skip
                  and h.lower() not in ("", "m_flag")]

    log(f"Value columns: {value_cols[:15]}... total {len(value_cols)}")

    all_keys, all_dates, all_vals = [], [], []
    writer = None
    n_rows = 0

    for row in reader:
        n_rows += 1
        iso3 = (row.get(iso3_col) or "").strip()
        date_raw = (row.get(date_col) or "").strip()
        if not iso3 or not date_raw:
            continue

        try:
            # Date format: YYYYMMDD
            if len(date_raw) == 8 and date_raw.isdigit():
                obs_d = dt.date(int(date_raw[:4]), int(date_raw[4:6]), int(date_raw[6:8]))
            else:
                obs_d = dt.date.fromisoformat(date_raw[:10])
        except (ValueError, TypeError):
            continue

        for col in value_cols:
            raw = (row.get(col) or "").strip()
            if not raw or raw in ("NA", "N/A", ""):
                continue
            try:
                v = float(raw)
                if v != v:
                    continue
                all_keys.append(f"OXCGRT:{col}:{iso3}")
                all_dates.append(obs_d)
                all_vals.append(v)
            except (TypeError, ValueError):
                pass

        if len(all_keys) >= BATCH:
            tbl = pa.table({
                "series_key": pa.array(all_keys, pa.string()),
                "obs_date":   pa.array(all_dates, pa.date32()),
                "value":      pa.array(all_vals,  pa.float64()),
            })
            if writer is None:
                writer = pq.ParquetWriter(out, tbl.schema, compression="zstd")
            writer.write_table(tbl)
            all_keys.clear(); all_dates.clear(); all_vals.clear()

        if n_rows % 50000 == 0:
            log(f"  {n_rows:,} rows, {len(all_keys):,} pending obs")

    # Final flush
    if all_keys:
        tbl = pa.table({
            "series_key": pa.array(all_keys, pa.string()),
            "obs_date":   pa.array(all_dates, pa.date32()),
            "value":      pa.array(all_vals,  pa.float64()),
        })
        if writer is None:
            writer = pq.ParquetWriter(out, tbl.schema, compression="zstd")
        writer.write_table(tbl)

    if writer:
        writer.close()
        n = pq.read_metadata(out).num_rows
        log(f"=== OxCGRT DONE: {n:,} obs from {n_rows:,} source rows ===")
    else:
        log("0 observations written")


if __name__ == "__main__":
    main()
