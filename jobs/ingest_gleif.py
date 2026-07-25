#!/usr/bin/env python3
"""GLEIF LEI (Legal Entity Identifier) Golden Copy ingest.

The join key for EDGAR/13F ownership chains: links LEI codes to company names,
legal addresses, parent entities, and registration status.

License: CC0 (public domain). Source: GLEIF Golden Copy.
Bulk file: https://www.gleif.org/lei-data/gleif-golden-copy/download-the-golden-copy
The "golden copy" concatenated ZIP contains all active + inactive LEI records.

Run: python jobs/ingest_gleif.py
"""
import csv, gzip, io, os, sys, time, zipfile
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT = os.path.join(ROOT, "data", "clean_full", "gleif")
TMP = os.path.join(ROOT, "data", "raw", "gleif")
UA = "Econ-Fin Data Library admin@hfdatalibrary.com"
# GLEIF public concatenated golden copy (no auth needed)
BULK_URL = "https://leidata.gleif.org/api/v1/concatenated-files/lei2/latest/zip"

def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)

def download():
    os.makedirs(TMP, exist_ok=True)
    dest = os.path.join(TMP, "gleif_golden_copy.zip")
    if os.path.exists(dest) and os.path.getsize(dest) > 1e6:
        log(f"Zip already on disk: {os.path.getsize(dest)/1e6:.0f} MB"); return dest
    log(f"Downloading GLEIF golden copy...")
    tmp = dest + ".part"
    with requests.get(BULK_URL, headers={"User-Agent": UA}, stream=True,
                      allow_redirects=True, timeout=600) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", 0))
        done = 0; last = time.time()
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk); done += len(chunk)
                if time.time() - last > 15:
                    log(f"  {done/1e6:.0f}/{total/1e6:.0f} MB"); last = time.time()
    os.replace(tmp, dest)
    log(f"Downloaded: {os.path.getsize(dest)/1e6:.0f} MB"); return dest

def ingest(zip_path):
    os.makedirs(OUT, exist_ok=True)
    out = os.path.join(OUT, "lei_records.parquet")
    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows
        log(f"Already ingested: {n:,} rows"); return n
    log("Parsing GLEIF golden copy XML->CSV mapping...")
    # GLEIF golden copy is a ZIP of CSV files
    z = zipfile.ZipFile(zip_path)
    members = z.namelist()
    log(f"Members: {members[:3]}")
    xmls = [m for m in members if m.lower().endswith(".xml")]
    csvs = [m for m in members if m.lower().endswith(".csv")]
    if xmls:
        log(f"Parsing XML (LEI2 format): {xmls[0]}")
        with z.open(xmls[0]) as f:
            return _parse_xml(f, out)
    if csvs:
        with z.open(csvs[0]) as f:
            return _parse_csv(f, out)
    log("No CSV or XML found in zip"); return 0

def _parse_xml(f, out):
    """Stream-parse LEI2 XML using iterparse (memory-bounded)."""
    import xml.etree.ElementTree as ET
    NS = "http://www.gleif.org/data/schema/leidata/2016"
    ENS = "http://www.gleif.org/data/schema/leidata/2016"
    SCHEMA = pa.schema([
        ("LEI",                  pa.string()),
        ("LegalName",            pa.string()),
        ("LegalJurisdiction",    pa.string()),
        ("EntityLegalFormCode",  pa.string()),
        ("EntityStatus",         pa.string()),
        ("RegistrationStatus",   pa.string()),
        ("ManagingLOU",          pa.string()),
    ])
    writer = pq.ParquetWriter(out, SCHEMA, compression="zstd")
    bufs = {k: [] for k in SCHEMA.names}
    n = 0; BATCH = 200_000
    def flush():
        writer.write_batch(pa.record_batch(
            {k: pa.array(bufs[k], pa.string()) for k in SCHEMA.names}, schema=SCHEMA))
        for k in bufs: bufs[k].clear()
    tag = lambda name: f"{{{NS}}}{name}"
    # Use iterparse so we never hold the whole tree in memory
    context = ET.iterparse(f, events=("end",))
    for event, elem in context:
        if elem.tag != tag("LEIRecord"):
            continue
        def txt(path):
            parts = path.split("/")
            e = elem
            for p in parts:
                e = e.find(tag(p)) if e is not None else None
            return (e.text or "") if e is not None else ""
        bufs["LEI"].append(txt("LEI"))
        bufs["LegalName"].append(txt("Entity/LegalName"))
        bufs["LegalJurisdiction"].append(txt("Entity/LegalJurisdiction"))
        bufs["EntityLegalFormCode"].append(txt("Entity/LegalForm/EntityLegalFormCode"))
        bufs["EntityStatus"].append(txt("Entity/EntityStatus"))
        bufs["RegistrationStatus"].append(txt("Registration/RegistrationStatus"))
        bufs["ManagingLOU"].append(txt("Registration/ManagingLOU"))
        n += 1
        elem.clear()  # free memory
        if n % BATCH == 0:
            flush()
            log(f"  GLEIF XML: {n:,} records parsed")
    if bufs["LEI"]:
        flush()
    writer.close()
    actual = pq.read_metadata(out).num_rows
    log(f"GLEIF XML: {actual:,} LEI records written"); return actual

def _parse_csv(f, out):
    COLS = ["LEI","LegalName","LegalJurisdiction","LegalForm.id",
            "EntityStatus","RegistrationStatus","ManagingLOU",
            "NextVersion.LEI"]  # key fields
    writer = None; n = 0; BATCH = 200_000
    bufs = {c: [] for c in COLS}
    reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig", errors="replace"))
    avail = None
    for row in reader:
        if avail is None:
            avail = list(row.keys())
            actual = [c for c in COLS if c in avail]
            log(f"  Columns available: {avail[:8]}... Using: {actual}")
            bufs = {c: [] for c in actual}
            schema = pa.schema([(c, pa.string()) for c in actual])
            writer = pq.ParquetWriter(out, schema, compression="zstd")
        for c in bufs:
            bufs[c].append(row.get(c, "") or "")
        n += 1
        if n % BATCH == 0:
            batch = pa.record_batch({c: pa.array(bufs[c], pa.string()) for c in bufs}, schema=schema)
            writer.write_batch(batch)
            for c in bufs: bufs[c].clear()
            log(f"  {n:,} records")
    if writer:
        if any(bufs[c] for c in bufs):
            batch = pa.record_batch({c: pa.array(bufs[c], pa.string()) for c in bufs}, schema=schema)
            writer.write_batch(batch)
        writer.close()
    actual_n = pq.read_metadata(out).num_rows if os.path.exists(out) else 0
    log(f"DONE: {actual_n:,} LEI records"); return actual_n

if __name__ == "__main__":
    zip_path = download()
    ingest(zip_path)
