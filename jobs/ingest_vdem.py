#!/usr/bin/env python3
"""V-Dem (Varieties of Democracy) ingest — downloads vdem.RData from the vdemdata
GitHub R package and converts to long-format parquet.

V-Dem v16 has ~500 country-years × ~4000 variables = ~27M obs in long format.
We download the RData directly from GitHub (no account needed).

Source: https://github.com/vdeminstitute/vdemdata  (CC BY 4.0 for most indices)
Output: data/clean_full/vdem/vdem.parquet  (series_key, obs_date, value)
Run:    python jobs/ingest_vdem.py
"""
from __future__ import annotations
import datetime as dt
import os
import time
import tempfile

import requests
import pyarrow as pa
import pyarrow.parquet as pq
import pyreadr

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT  = os.path.join(ROOT, "data", "clean_full", "vdem")
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
REPO = "https://raw.githubusercontent.com/vdeminstitute/vdemdata/master/data"

# V-Dem dataset files: (remote_name, local_label, index_columns)
DATASETS = [
    ("vdem.RData",   "vdem",   ("country_text_id", "year")),
    ("vparty.RData", "vparty", ("country_text_id", "year")),
]

# Columns to always skip (metadata, text, non-numeric)
ALWAYS_SKIP = {
    "country_id", "country_name", "country_text_id",
    "histname", "codingstart", "codingend", "codingstart_contemp",
    "codingend_contemp", "codingstart_core", "codingend_core",
    "gapstart1", "gapstart2", "gapstart3", "gapend1", "gapend2", "gapend3",
    "gap_idx", "project", "historical_date", "year", "id",
    "v2x_elecreg", "e_regiongeo",  # keep numeric ones below
}


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def fetch_bytes(url: str, retries: int = 3) -> bytes | None:
    for attempt in range(retries):
        try:
            log(f"  GET {url[-60:]}")
            r = requests.get(url, headers=UA, timeout=300, stream=True)
            if r.status_code == 200:
                chunks = []
                total = 0
                for chunk in r.iter_content(65536):
                    chunks.append(chunk)
                    total += len(chunk)
                    if total % (5 * 1024 * 1024) < 65536:
                        log(f"    ... {total/1e6:.1f} MB downloaded")
                return b"".join(chunks)
            log(f"  HTTP {r.status_code}")
            if r.status_code == 404:
                return None
        except Exception as e:
            log(f"  ERR attempt {attempt+1}: {e}")
        time.sleep(10 * (attempt + 1))
    return None


def rdata_to_df(rdata_bytes: bytes, label: str):
    """Write RData to temp file, read with pyreadr, return the main data frame."""
    tmp = tempfile.NamedTemporaryFile(suffix=".RData", delete=False)
    try:
        tmp.write(rdata_bytes)
        tmp.close()
        result = pyreadr.read_r(tmp.name)
        log(f"  {label}: objects = {list(result.keys())}")
        # Prefer the object whose name contains the label or pick the largest
        best = None
        for k, df in result.items():
            if best is None or (df is not None and len(df) > len(result[best])):
                best = k
        if best is None:
            return None
        log(f"  {label}: using '{best}', shape={result[best].shape}")
        return result[best]
    finally:
        os.unlink(tmp.name)


def df_to_long(df, label: str, country_col: str, year_col: str):
    """Melt wide country-year data frame to long format."""
    import pandas as pd
    import numpy as np

    log(f"  {label}: melting {df.shape[0]:,} rows × {df.shape[1]:,} cols...")

    # Identify numeric value columns
    skip = set(ALWAYS_SKIP)
    skip.update([c.lower() for c in skip])
    skip.add(country_col.lower())
    skip.add(year_col.lower())

    value_cols = []
    for col in df.columns:
        if col.lower() in skip:
            continue
        dtype = df[col].dtype
        if dtype in (object, str):
            continue
        if pd.api.types.is_numeric_dtype(dtype):
            value_cols.append(col)

    log(f"  {label}: {len(value_cols)} numeric value columns")
    if not value_cols:
        return [], [], []

    # Work in chunks to avoid memory explosion (4000 cols × 28k rows)
    CHUNK = 200
    all_keys, all_dates, all_vals = [], [], []

    for ci in range(0, len(value_cols), CHUNK):
        chunk_cols = value_cols[ci:ci+CHUNK]
        chunk_df = df[[country_col, year_col] + chunk_cols].copy()

        # Drop rows where year is null
        chunk_df = chunk_df.dropna(subset=[year_col])

        try:
            melted = chunk_df.melt(
                id_vars=[country_col, year_col],
                value_vars=chunk_cols,
                var_name="variable",
                value_name="value"
            )
        except Exception as e:
            log(f"  melt error chunk {ci}: {e}"); continue

        # Drop nulls
        melted = melted.dropna(subset=["value"])
        # Drop infs
        melted = melted[np.isfinite(melted["value"].values)]
        if len(melted) == 0:
            continue

        # Build series keys
        countries = melted[country_col].fillna("").astype(str)
        variables = melted["variable"].astype(str)
        series_keys = "VDEM:" + variables + ":" + countries

        # Build dates — year as int
        years = pd.to_numeric(melted[year_col], errors="coerce").dropna()
        melted = melted.loc[years.index]
        years = years.astype(int)
        obs_dates = [dt.date(yr, 12, 31) for yr in years]
        series_keys = series_keys.loc[melted.index].tolist()
        values = melted["value"].tolist()

        all_keys.extend(series_keys)
        all_dates.extend(obs_dates)
        all_vals.extend(values)

        if (ci // CHUNK) % 5 == 0:
            log(f"    chunk {ci}/{len(value_cols)}: {len(all_vals):,} obs so far")

    return all_keys, all_dates, all_vals


def ingest_dataset(remote_name: str, label: str, cols: tuple[str, str]):
    out = os.path.join(OUT, f"{label}.parquet")
    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows
        log(f"{label}: already {n:,} rows"); return n

    url = f"{REPO}/{remote_name}"
    log(f"{label}: downloading {remote_name}...")
    data = fetch_bytes(url)
    if not data:
        log(f"{label}: download failed"); return 0

    log(f"{label}: {len(data)/1e6:.1f} MB downloaded, reading RData...")
    df = rdata_to_df(data, label)
    if df is None:
        log(f"{label}: could not read RData"); return 0

    country_col, year_col = cols
    # Adjust for actual column names
    col_map = {c.lower(): c for c in df.columns}
    country_col = col_map.get(country_col.lower(), country_col)
    year_col = col_map.get(year_col.lower(), year_col)
    if year_col not in df.columns:
        log(f"{label}: year column '{year_col}' not found, available: {list(df.columns[:10])}"); return 0

    keys, dates, vals = df_to_long(df, label, country_col, year_col)
    if not vals:
        log(f"{label}: 0 obs"); return 0

    tbl = pa.table({
        "series_key": pa.array(keys,  pa.string()),
        "obs_date":   pa.array(dates, pa.date32()),
        "value":      pa.array(vals,  pa.float64()),
    })
    pq.write_table(tbl, out, compression="zstd")
    n = pq.read_metadata(out).num_rows
    log(f"{label}: {n:,} obs saved")
    return n


def main():
    os.makedirs(OUT, exist_ok=True)
    log("=== V-Dem Ingest ===")
    total = 0
    for remote_name, label, cols in DATASETS:
        try:
            n = ingest_dataset(remote_name, label, cols)
            total += n
        except Exception as e:
            log(f"ERROR {label}: {e}")
            import traceback; traceback.print_exc()
    log(f"=== V-Dem GRAND TOTAL: {total:,} observations ===")


if __name__ == "__main__":
    main()
