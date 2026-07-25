#!/usr/bin/env python3
"""OFR (Office of Financial Research) data ingest.

Datasets available at data.financialresearch.gov/v1/series/dataset:
  fnyr  - Federal Reserve Bank of NY Reference Rates (BGCR, TGCR, SOFR, EFFR, OBFR + distributions)
  repo  - OFR U.S. Repo Markets Data Release (164 series: GC repo by collateral/maturity)
  mmf   - OFR U.S. Money Market Fund Data Release (42 series: AUM, flows, composition)
  nypd  - Federal Reserve Bank of NY Primary Dealer Statistics (194 series)

License: US public domain (OFR is a US federal agency created by Dodd-Frank).
Source: Office of Financial Research, U.S. Department of the Treasury.
No API key required.

Run: python jobs/ingest_ofr.py
"""
import datetime as dt
import os
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT = os.path.join(ROOT, "data", "clean_full", "ofr")
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
BASE = "https://data.financialresearch.gov/v1/series/dataset"
DATASETS = ["fnyr", "repo", "mmf", "nypd"]

SCHEMA = pa.schema([
    ("series_id",  pa.string()),
    ("obs_date",   pa.date32()),
    ("value",      pa.float64()),
    ("dataset",    pa.string()),
])


def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def parse_date(s):
    s = (s or "").strip()
    try:
        return dt.date.fromisoformat(s)
    except (ValueError, AttributeError):
        return None


def ingest_dataset(ds_key):
    out_path = os.path.join(OUT, f"{ds_key}.parquet")
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"  {ds_key}: already {n:,} rows"); return n

    r = requests.get(BASE, params={"dataset": ds_key}, headers=UA, timeout=120)
    if r.status_code != 200:
        log(f"  {ds_key}: HTTP {r.status_code}"); return 0

    d = r.json()
    ds_name = d.get("short_name", ds_key)
    series_map = d.get("timeseries", {})
    log(f"  {ds_key} ({ds_name}): {len(series_map)} series")

    sids, dates, vals, datasets = [], [], [], []
    for sid, sdata in series_map.items():
        if not isinstance(sdata, dict):
            continue
        inner = sdata.get("timeseries", {})
        if not inner:
            continue
        # data lives in either 'aggregation' or 'value' key
        points = inner.get("aggregation") or inner.get("value") or []
        for pt in points:
            if not isinstance(pt, (list, tuple)) or len(pt) < 2:
                continue
            d_val = parse_date(str(pt[0]))
            v = pt[1]
            if d_val is None or v is None:
                continue
            try:
                fv = float(v)
            except (ValueError, TypeError):
                continue
            sids.append(sid)
            dates.append(d_val)
            vals.append(fv)
            datasets.append(ds_key)

    if not sids:
        log(f"  {ds_key}: 0 obs parsed"); return 0

    tbl = pa.table({
        "series_id":  pa.array(sids, pa.string()),
        "obs_date":   pa.array(dates, pa.date32()),
        "value":      pa.array(vals, pa.float64()),
        "dataset":    pa.array(datasets, pa.string()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"  {ds_key}: {n:,} obs written ({len(set(sids))} series)")
    return n


def main():
    os.makedirs(OUT, exist_ok=True)
    total = 0
    for ds in DATASETS:
        total += ingest_dataset(ds)
        time.sleep(0.5)
    log(f"DONE: {total:,} total OFR observations")


if __name__ == "__main__":
    main()
