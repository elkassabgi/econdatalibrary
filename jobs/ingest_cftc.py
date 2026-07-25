#!/usr/bin/env python3
"""CFTC Commitments of Traders (CoT) full history ingest.

Weekly futures/options positioning data by trader class (commercials, non-commercials,
non-reportables) for all US futures markets. Full history back to 1986.

License: US public domain (CFTC is a federal agency).
Source: https://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalCompressed/index.htm

Run: python jobs/ingest_cftc.py
"""
import csv, io, os, time, zipfile
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT = os.path.join(ROOT, "data", "clean_full", "cftc")
TMP = os.path.join(ROOT, "data", "raw", "cftc")
UA = "Econ-Fin Data Library admin@hfdatalibrary.com"

# CFTC historical compressed files — full history in annual ZIPs
# Legacy combined report (futures + options): all years in one file
# Legacy COT (futures only, 1986-2016 combined + 2004-2016 combined futures+options)
# Annual COT (futures+options combined) back to 1986
LEGACY_BASE = "https://www.cftc.gov/files/dea/history"
# Full history files (verified from CFTC historical page)
HISTORY_FILES = {
    # Legacy futures-only: 1986-2016 combined
    "legacy_fut_hist":   f"{LEGACY_BASE}/deacot1986_2016.zip",
    # Legacy futures+options: 1995-2016 combined
    "legacy_fo_hist":    f"{LEGACY_BASE}/deahistfo_1995_2016.zip",
    # Disaggregated: 2006-2016 combined
    "disagg_hist":       f"{LEGACY_BASE}/fut_disagg_txt_hist_2006_2016.zip",
    # Commodity index traders: 2006-2016 combined
    "cit_hist":          f"{LEGACY_BASE}/dea_cit_txt_2006_2016.zip",
    # Financial futures: 2006-2016 combined
    "fin_hist":          f"{LEGACY_BASE}/fin_fut_txt_2006_2016.zip",
}

def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)

def fetch(url, dest):
    if os.path.exists(dest) and os.path.getsize(dest) > 1000:
        return dest
    tmp = dest + ".part"
    try:
        with requests.get(url, headers={"User-Agent": UA}, stream=True,
                         timeout=300, allow_redirects=True) as r:
            if r.status_code == 404:
                return None
            r.raise_for_status()
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(1 << 20):
                    f.write(chunk)
        os.replace(tmp, dest); return dest
    except Exception as e:
        log(f"  ERR {url}: {e}"); return None

def parse_zip_csv(zip_path, label):
    """Parse all CSVs inside a zip, return list of row dicts."""
    rows = []
    try:
        z = zipfile.ZipFile(zip_path)
        for member in z.namelist():
            if not (member.lower().endswith(".csv") or member.lower().endswith(".txt")):
                continue
            with z.open(member) as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8",
                                                          errors="replace"))
                for row in reader:
                    row["_source"] = label
                    rows.append(row)
    except Exception as e:
        log(f"  parse err {zip_path}: {e}")
    return rows

def ingest_all():
    os.makedirs(TMP, exist_ok=True); os.makedirs(OUT, exist_ok=True)
    out_path = os.path.join(OUT, "cot_all.parquet")
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"Already ingested: {n:,} rows"); return n

    all_rows = []
    # 1. Historical combined files (verified URLs from CFTC page)
    for label, url in HISTORY_FILES.items():
        dest = os.path.join(TMP, f"{label}.zip")
        log(f"Fetching {label}...")
        if fetch(url, dest):
            rows = parse_zip_csv(dest, label)
            log(f"  {label}: {len(rows):,} rows"); all_rows.extend(rows)
        time.sleep(0.5)

    # 2. Recent years (2017-current) — disaggregated futures+options
    for year in range(2017, 2027):
        url = f"https://www.cftc.gov/files/dea/history/deahistfo{year}.zip"
        dest = os.path.join(TMP, f"fo_{year}.zip")
        p = fetch(url, dest)
        if p:
            rows = parse_zip_csv(p, f"fo_{year}")
            log(f"  F+O {year}: {len(rows):,} rows"); all_rows.extend(rows)
        # Also disaggregated
        url2 = f"https://www.cftc.gov/files/dea/history/fut_disagg_txt_{year}.zip"
        dest2 = os.path.join(TMP, f"disagg_{year}.zip")
        p2 = fetch(url2, dest2)
        if p2:
            rows2 = parse_zip_csv(p2, f"disagg_{year}")
            log(f"  Disagg {year}: {len(rows2):,} rows"); all_rows.extend(rows2)
        time.sleep(0.3)

    if not all_rows:
        log("No rows parsed"); return 0

    log(f"Total rows: {len(all_rows):,}. Writing Parquet...")
    all_keys = sorted({k for r in all_rows for k in r})
    schema = pa.schema([(k, pa.string()) for k in all_keys])
    writer = pq.ParquetWriter(out_path, schema, compression="zstd")
    BATCH = 100_000
    for i in range(0, len(all_rows), BATCH):
        chunk = all_rows[i:i+BATCH]
        tbl = pa.record_batch({k: [r.get(k,"") or "" for r in chunk] for k in all_keys}, schema=schema)
        writer.write_batch(tbl)
    writer.close()
    n = pq.read_metadata(out_path).num_rows
    log(f"DONE: {n:,} CoT rows"); return n

if __name__ == "__main__":
    ingest_all()
