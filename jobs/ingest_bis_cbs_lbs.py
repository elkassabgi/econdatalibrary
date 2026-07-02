#!/usr/bin/env python3
"""Download and ingest BIS CBS (Consolidated Banking Statistics) and LBS
(Locational Banking Statistics) from the BIS Data Portal bulk downloads.

These are NOT accessible via the BIS SDMX v1 API (returns 404) but ARE
available as full CSV zip files at data.bis.org/static/bulk/.

CBS = WS_CBS_PUB (consolidated positions of internationally-active banking groups)
LBS = WS_LBS_D_PUB (locational stats - includes both LBSR and LBSN dimensions)

License: bis-attrib-nc (non-commercial, attribution required).
Run: python jobs/ingest_bis_cbs_lbs.py
"""
from __future__ import annotations
import csv
import datetime as dt
import io
import os
import sys
import time
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
OUT = os.path.join(ROOT, "data", "clean_full", "bis")
TMP = os.path.join(ROOT, "data", "raw", "bis_bulk")
UA = "Econ-Fin Data Library admin@hfdatalibrary.com"

BULK = {
    "CBS": "https://data.bis.org/static/bulk/WS_CBS_PUB_csv_flat.zip",
    "LBS": "https://data.bis.org/static/bulk/WS_LBS_D_PUB_csv_flat.zip",
}

SCHEMA = pa.schema([
    ("series_key", pa.string()),
    ("obs_date",   pa.date32()),
    ("value",      pa.float64()),
    ("freq",       pa.string()),
])


def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def download(name, url):
    os.makedirs(TMP, exist_ok=True)
    dest = os.path.join(TMP, f"{name}.zip")
    if os.path.exists(dest) and os.path.getsize(dest) > 10000:
        log(f"{name}: zip already on disk ({os.path.getsize(dest)/1e6:.1f} MB), skipping download")
        return dest
    tmp = dest + ".part"
    # Resumable download via HTTP Range
    for attempt in range(5):
        pos = os.path.getsize(tmp) if os.path.exists(tmp) else 0
        headers = {"User-Agent": UA}
        if pos:
            headers["Range"] = f"bytes={pos}-"
            log(f"{name}: resuming at {pos/1e6:.1f} MB (attempt {attempt+1})")
        else:
            log(f"{name}: downloading from {url}")
        try:
            with requests.get(url, headers=headers, stream=True, timeout=600) as r:
                if r.status_code not in (200, 206):
                    log(f"{name}: HTTP {r.status_code}"); return None
                total = int(r.headers.get("Content-Length", 0)) + pos
                mode = "ab" if (pos and r.status_code == 206) else "wb"
                done = pos if mode == "ab" else 0
                last = time.time()
                with open(tmp, mode) as f:
                    for chunk in r.iter_content(1 << 20):
                        f.write(chunk); done += len(chunk)
                        if time.time() - last > 15:
                            log(f"  {name}: {done/1e6:.0f}/{total/1e6:.0f} MB")
                            last = time.time()
            os.replace(tmp, dest)
            log(f"{name}: downloaded {os.path.getsize(dest)/1e6:.1f} MB")
            return dest
        except Exception as e:
            log(f"{name}: attempt {attempt+1} failed: {e}")
            time.sleep(10)
    log(f"{name}: GAVE UP after retries"); return None


def parse_period(s):
    s = (s or "").strip()
    try:
        if len(s) == 4 and s.isdigit():
            return dt.date(int(s), 12, 31), "A"
        if "-Q" in s:
            y, q = s.split("-Q"); return dt.date(int(y), (int(q)-1)*3+1, 1), "Q"
        if "-" in s:
            p = s.split("-")
            if len(p) == 3: return dt.date(int(p[0]), int(p[1]), int(p[2])), "D"
            if len(p) == 2: return dt.date(int(p[0]), int(p[1]), 1), "M"
    except (ValueError, KeyError):
        return None, None
    return None, None


def ingest_zip(name, zip_path):
    out_path = os.path.join(OUT, f"{name}.parquet")
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"{name}: already ingested ({n:,} rows), skipping")
        return n

    log(f"{name}: parsing CSV from zip...")
    z = zipfile.ZipFile(zip_path)
    # find the flat CSV (not the metadata/readme)
    csvs = [m for m in z.namelist() if m.endswith(".csv") and
            not any(x in m.lower() for x in ["readme", "notes", "description", "label"])]
    log(f"  {name}: CSV members in zip: {csvs}")
    if not csvs:
        log(f"  {name}: no CSV found in zip!"); return 0

    csv_name = csvs[0]
    log(f"  {name}: parsing {csv_name}")

    writer = pq.ParquetWriter(out_path, SCHEMA, compression="zstd")
    BATCH = 300_000
    keys, dates, vals, freqs = [], [], [], []
    n_total = n_bad = 0

    def flush():
        nonlocal keys, dates, vals, freqs
        if not keys: return
        n = min(len(keys), len(dates), len(vals), len(freqs))
        batch = pa.record_batch([
            pa.array(keys[:n], pa.string()),
            pa.array(dates[:n], pa.date32()),
            pa.array(vals[:n], pa.float64()),
            pa.array(freqs[:n], pa.string()),
        ], schema=SCHEMA)
        writer.write_batch(batch)
        keys, dates, vals, freqs = [], [], [], []

    with z.open(csv_name) as raw:
        reader = csv.reader(io.TextIOWrapper(raw, encoding="utf-8-sig", newline=""))
        hdr = next(reader, None)
        if not hdr:
            log(f"  {name}: empty CSV"); writer.close(); return 0

        log(f"  {name}: columns: {hdr[:8]}...")
        # BIS flat CSV: one row = one observation
        # Columns: DATAFLOW, FREQ, [dims...], TIME_PERIOD, OBS_VALUE, [attrs...]
        try:
            ti = hdr.index("TIME_PERIOD")
            vi = hdr.index("OBS_VALUE")
        except ValueError:
            # try alternate names
            ti = next((i for i, h in enumerate(hdr) if "TIME" in h.upper()), None)
            vi = next((i for i, h in enumerate(hdr) if "OBS_VALUE" in h.upper() or h.upper() == "VALUE"), None)
            if ti is None or vi is None:
                log(f"  {name}: cannot find TIME_PERIOD/OBS_VALUE in {hdr[:10]}"); writer.close(); return 0

        freq_i = next((i for i, h in enumerate(hdr) if h.upper() in ("FREQ","FREQUENCY")), None)
        # dim columns: between DATAFLOW (col 0) and TIME_PERIOD
        dim_idx = [i for i in range(1, ti) if i != vi]

        for row in reader:
            if len(row) <= max(ti, vi): continue
            try:
                val = float(row[vi])
            except (ValueError, TypeError):
                n_bad += 1; continue
            d, finf = parse_period(row[ti])
            if d is None:
                n_bad += 1; continue
            key = ".".join(row[i] if i < len(row) else "" for i in dim_idx)
            fr = (row[freq_i] if freq_i is not None and freq_i < len(row) else "") or finf or ""
            keys.append(key); dates.append(d); vals.append(val); freqs.append(fr)
            n_total += 1
            if len(keys) >= BATCH:
                flush()

    flush()
    writer.close()
    actual = pq.read_metadata(out_path).num_rows
    log(f"{name}: DONE {n_total:,} obs written ({n_bad:,} bad), verified {actual:,} rows in Parquet")
    return actual


def main():
    os.makedirs(OUT, exist_ok=True)
    grand = 0
    for name, url in BULK.items():
        zip_path = download(name, url)
        n = ingest_zip(name, zip_path)
        grand += n
    log(f"GRAND TOTAL: {grand:,} obs across CBS + LBS")


if __name__ == "__main__":
    main()
