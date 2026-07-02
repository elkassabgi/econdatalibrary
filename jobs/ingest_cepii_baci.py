#!/usr/bin/env python3
"""CEPII BACI bilateral trade database ingest.

BACI provides harmonised bilateral trade flows (value + quantity) for ~200 countries
at the HS 6-digit product level. Multiple HS vintages available.

License: Etalab Open Licence 2.0 (commercial OK, attribution required).
Attribution: "Source: CEPII, BACI (Etalab Open Licence 2.0)"
Source: https://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele_item.asp?id=37

Pulls the HS17 (2017 revision) and HS96 vintages for maximum history.
Run: python jobs/ingest_cepii_baci.py
"""
import csv, io, os, sys, time, zipfile
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
OUT = os.path.join(ROOT, "data", "clean_full", "cepii_baci")
TMP = os.path.join(ROOT, "data", "raw", "cepii_baci")
UA = "Econ-Fin Data Library admin@hfdatalibrary.com"

# Direct download — no account needed (verified 200 OK)
FILES = {
    "HS17": "https://www.cepii.fr/DATA_DOWNLOAD/baci/data/BACI_HS17_V202401b.zip",
    "HS96": "https://www.cepii.fr/DATA_DOWNLOAD/baci/data/BACI_HS96_V202401b.zip",
}

def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)

def download(name, url):
    os.makedirs(TMP, exist_ok=True)
    dest = os.path.join(TMP, f"{name}.zip")
    if os.path.exists(dest) and os.path.getsize(dest) > 1e6:
        log(f"{name}: already on disk ({os.path.getsize(dest)/1e6:.0f} MB)")
        return dest
    log(f"{name}: downloading from {url}")
    tmp = dest + ".part"
    with requests.get(url, headers={"User-Agent": UA}, stream=True, timeout=600) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", 0))
        done = 0; last = time.time()
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk); done += len(chunk)
                if time.time() - last > 15:
                    log(f"  {name}: {done/1e6:.0f}/{total/1e6:.0f} MB")
                    last = time.time()
    os.replace(tmp, dest)
    log(f"{name}: downloaded {os.path.getsize(dest)/1e6:.0f} MB")
    return dest

def ingest_zip(name, zip_path):
    out_path = os.path.join(OUT, f"baci_{name.lower()}.parquet")
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"{name}: already ingested {n:,} rows"); return n

    log(f"{name}: parsing trade data...")
    z = zipfile.ZipFile(zip_path)
    csvs = sorted(m for m in z.namelist() if m.lower().endswith(".csv")
                  and not any(x in m.lower() for x in ["country","product","readme"]))
    log(f"{name}: {len(csvs)} annual CSV files")

    SCHEMA = pa.schema([
        ("year",    pa.int16()),
        ("exporter", pa.string()),
        ("importer", pa.string()),
        ("product",  pa.string()),
        ("value",    pa.float64()),  # 1000 USD
        ("quantity", pa.float64()),  # metric tons
    ])
    writer = pq.ParquetWriter(out_path, SCHEMA, compression="zstd")
    total = 0
    for csv_name in csvs:
        with z.open(csv_name) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8", errors="replace"))
            years, exporters, importers, products, vals, qtys = [], [], [], [], [], []
            for row in reader:
                try:
                    yr = int(row.get("t") or row.get("year",""))
                    exp = str(row.get("i") or row.get("exporter",""))
                    imp = str(row.get("j") or row.get("importer",""))
                    prod = str(row.get("k") or row.get("hs6",""))
                    v = float(row.get("v") or row.get("value","0") or "0")
                    q_raw = row.get("q") or row.get("quantity","")
                    q = float(q_raw) if q_raw and q_raw.strip() not in ("","NA") else 0.0
                except (ValueError, TypeError):
                    continue
                years.append(yr); exporters.append(exp); importers.append(imp)
                products.append(prod); vals.append(v); qtys.append(q)
                if len(years) >= 500_000:
                    batch = pa.record_batch([
                        pa.array(years, pa.int16()), pa.array(exporters, pa.string()),
                        pa.array(importers, pa.string()), pa.array(products, pa.string()),
                        pa.array(vals, pa.float64()), pa.array(qtys, pa.float64()),
                    ], schema=SCHEMA)
                    writer.write_batch(batch); total += len(years)
                    years, exporters, importers, products, vals, qtys = [], [], [], [], [], []
            if years:
                batch = pa.record_batch([
                    pa.array(years, pa.int16()), pa.array(exporters, pa.string()),
                    pa.array(importers, pa.string()), pa.array(products, pa.string()),
                    pa.array(vals, pa.float64()), pa.array(qtys, pa.float64()),
                ], schema=SCHEMA)
                writer.write_batch(batch); total += len(years)
        log(f"  {csv_name}: cumulative {total:,} trade flows")
    writer.close()
    actual = pq.read_metadata(out_path).num_rows
    log(f"{name}: DONE {actual:,} bilateral trade flow records"); return actual

def main():
    os.makedirs(OUT, exist_ok=True)
    grand = 0
    for name, url in FILES.items():
        zip_path = download(name, url)
        grand += ingest_zip(name, zip_path)
    log(f"GRAND TOTAL: {grand:,} BACI trade flow records")

if __name__ == "__main__":
    main()
