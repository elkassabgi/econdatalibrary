#!/usr/bin/env python3
"""UN World Population Prospects 2024 ingest.

Source: https://population.un.org/wpp/
License: Creative Commons Attribution 3.0 IGO (CC BY 3.0 IGO)
No API key required.

Coverage:
  * 237 countries and areas, 1950-2100 (medium variant + other variants)
  * Demographic indicators: total population, births, deaths, life expectancy,
    fertility rate, net migration, natural increase, growth rate, etc.

Outputs:
  data/clean_full/un_wpp/wpp_indicators_medium.parquet
  data/clean_full/un_wpp/wpp_indicators_other.parquet

Series key format:  WPP:{Indicator}:{Variant}:{ISO3}
Run: python jobs/ingest_un_wpp.py
"""
from __future__ import annotations
import csv
import datetime as dt
import gzip
import io
import os
import time
import zipfile

import requests
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT  = os.path.join(ROOT, "data", "clean_full", "un_wpp")
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

BASE = "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES"

FILES = {
    "indicators_medium": f"{BASE}/WPP2024_Demographic_Indicators_Medium.csv.gz",
    "indicators_other":  f"{BASE}/WPP2024_Demographic_Indicators_OtherVariants.csv.gz",
}

META_COLS = {
    "sortorder", "locid", "notes", "iso3_code", "iso2_code", "sdmx_code",
    "loctypenm", "loctypeid", "parentid", "location", "variant", "time",
    "mid_period", "midperiod",
}


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def fetch(url: str, retries: int = 3) -> bytes | None:
    for attempt in range(retries):
        try:
            log(f"  GET {url[-80:]}...")
            r = requests.get(url, headers=UA, timeout=300, stream=True)
            if r.status_code == 200:
                chunks = []
                for chunk in r.iter_content(chunk_size=1 << 20):
                    if chunk:
                        chunks.append(chunk)
                data = b"".join(chunks)
                if len(data) > 1000:
                    log(f"  {len(data)//1024:,} KB")
                    return data
                log(f"  Too small: {len(data)} bytes")
            else:
                log(f"  HTTP {r.status_code}")
        except Exception as e:
            log(f"  ERR attempt {attempt+1}: {e}")
        time.sleep(10 * (attempt + 1))
    return None


def parse_wpp_csv(data: bytes, label: str) -> tuple[list, list, list]:
    # Decompress: handle both .gz and .zip
    if data[:2] == b"\x1f\x8b":  # gzip magic bytes
        raw_bytes = gzip.decompress(data)
        log(f"  {label}: gzip decompressed to {len(raw_bytes)//1024:,} KB")
    elif data[:2] == b"PK":  # ZIP
        z = zipfile.ZipFile(io.BytesIO(data))
        csv_files = [m for m in z.namelist() if m.endswith(".csv")]
        if not csv_files:
            log(f"  {label}: no CSV in ZIP"); return [], [], []
        csv_name = csv_files[0]
        log(f"  {label}: parsing {csv_name}")
        raw_bytes = z.read(csv_name)
    else:
        raw_bytes = data  # assume plain CSV

    raw = raw_bytes.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(raw))
    headers = [h.strip() for h in (reader.fieldnames or [])]
    log(f"  {label}: {len(headers)} columns, first 10: {headers[:10]}")

    iso3_col = next((h for h in headers if h.lower() == "iso3_code"), None)
    loc_col  = next((h for h in headers if h.lower() == "location"), None)
    var_col  = next((h for h in headers if h.lower() == "variant"), None)
    time_col = next((h for h in headers if h.lower() == "time"), None)

    if not time_col:
        log(f"  {label}: no Time column"); return [], [], []

    val_cols = [h for h in headers if h.lower() not in META_COLS]
    log(f"  {label}: {len(val_cols)} value cols: {val_cols[:5]}...")

    keys, dates, vals = [], [], []
    n_rows = 0

    for row in reader:
        n_rows += 1
        time_str = (row.get(time_col) or "").strip()
        if not time_str:
            continue
        try:
            if "-" in time_str:
                yr = int(time_str.split("-")[0])
            else:
                yr = int(float(time_str))
            obs_d = dt.date(yr, 7, 1)
        except (ValueError, TypeError):
            continue

        iso3 = (row.get(iso3_col) or "").strip() if iso3_col else ""
        loc  = iso3 or (row.get(loc_col) or "UNKNOWN").strip()
        variant = (row.get(var_col) or "Medium").strip().replace(" ", "")

        for vcol in val_cols:
            raw_val = (row.get(vcol) or "").strip()
            if not raw_val:
                continue
            try:
                v = float(raw_val)
                if v != v:
                    continue
                keys.append(f"WPP:{vcol}:{variant}:{loc}")
                dates.append(obs_d)
                vals.append(v)
            except (TypeError, ValueError):
                continue

        if n_rows % 50000 == 0:
            log(f"  {label}: {n_rows:,} rows, {len(keys):,} obs")

    log(f"  {label}: parsed {n_rows:,} rows -> {len(keys):,} obs")
    return keys, dates, vals


def save(keys, dates, vals, path):
    if not keys:
        log(f"  0 obs, skipping {os.path.basename(path)}"); return 0
    tbl = pa.table({
        "series_key": pa.array(keys,  pa.string()),
        "obs_date":   pa.array(dates, pa.date32()),
        "value":      pa.array(vals,  pa.float64()),
    })
    pq.write_table(tbl, path, compression="zstd")
    n = pq.read_metadata(path).num_rows
    log(f"  -> {n:,} obs saved to {os.path.basename(path)}")
    return n


def main():
    os.makedirs(OUT, exist_ok=True)
    log("=== UN World Population Prospects 2024 Ingest ===")
    total = 0

    for label, url in FILES.items():
        out_path = os.path.join(OUT, f"{label}.parquet")
        if os.path.exists(out_path):
            n = pq.read_metadata(out_path).num_rows
            log(f"{label}: already {n:,} rows")
            total += n
            continue
        data = fetch(url)
        if data:
            k, d, v = parse_wpp_csv(data, label)
            total += save(k, d, v, out_path)
        time.sleep(2)

    log(f"=== UN WPP TOTAL: {total:,} observations ===")


if __name__ == "__main__":
    main()
