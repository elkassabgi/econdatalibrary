"""Build the EDGAR filing-pointer catalog from the already-downloaded
submissions.zip bulk file.

For every filer (CIK) in data/raw/sec_edgar/submissions.zip we emit ONE grouped
Parquet of POINTERS -- metadata + the canonical sec.gov Archives URL for each
filing's primary document. These are pointers, NOT the documents themselves.

Output columns per row (one row per filing):
    form            str  -- filing form type (e.g. '10-K', '8-K')
    filing_date     str  -- EDGAR filingDate (ISO 'YYYY-MM-DD'), kept as string
    accession       str  -- accessionNumber with dashes (e.g. '0001193125-13-215474')
    primary_doc_url str  -- https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/{primaryDocument}

Each filer's COMPLETE history is reconstructed by merging filings.recent with
every referenced filings.files[] split file inside the same zip.

Output: data/clean_full/edgar_pointers/<shard>/CIK##########.parquet
  sharded by cik[3:6] (<=1000 buckets) so no single directory holds ~1M files,
  which would cripple NTFS / directory tooling on Windows.

license: us-public-domain (SEC EDGAR is U.S. federal government work).

This script does NOT touch catalog.db.
"""
from __future__ import annotations

import json
import os
import sys
import time
import zipfile
from multiprocessing import Process, Queue

import pyarrow as pa
import pyarrow.parquet as pq

# --- Windows D:/ paths ---------------------------------------------------
ROOT = "D:/research/econfindatalibrary"
ZIP_PATH = os.path.join(ROOT, "data", "raw", "sec_edgar", "submissions.zip")
OUT_DIR = os.path.join(ROOT, "data", "clean_full", "edgar_pointers")

SOURCE_ID = "sec_edgar"
LICENSE_ID = "us-public-domain"
ARCHIVE_BASE = "https://www.sec.gov/Archives/edgar/data"

N_WORKERS = 14

_SCHEMA = pa.schema(
    [
        ("form", pa.string()),
        ("filing_date", pa.string()),
        ("accession", pa.string()),
        ("primary_doc_url", pa.string()),
    ]
)


def _shard_dir(cik_padded: str) -> str:
    """Bucket by 3 CIK digits -> <=1000 dirs, ~1k files each on average."""
    return cik_padded[3:6] if len(cik_padded) >= 6 else "000"


def _build_rows(cik_int: int, rec: dict):
    """Turn a column-oriented filings dict into parallel python lists.

    Returns (forms, filing_dates, accessions, urls). Missing primaryDocument
    (common in pre-2001 filings) -> URL points at the filing folder index.
    """
    forms = rec.get("form") or []
    if not forms:
        return None
    filing_dates = rec.get("filingDate") or []
    accessions = rec.get("accessionNumber") or []
    pdocs = rec.get("primaryDocument") or []
    n = len(forms)
    # defensively pad shorter arrays (EDGAR is consistent, but never trust input)
    if len(filing_dates) < n:
        filing_dates = filing_dates + [None] * (n - len(filing_dates))
    if len(accessions) < n:
        accessions = accessions + [None] * (n - len(accessions))
    if len(pdocs) < n:
        pdocs = pdocs + [None] * (n - len(pdocs))

    urls = [None] * n
    for i in range(n):
        acc = accessions[i]
        if not acc:
            continue
        acc_nodash = acc.replace("-", "")
        pdoc = pdocs[i] or ""
        urls[i] = f"{ARCHIVE_BASE}/{cik_int}/{acc_nodash}/{pdoc}"
    return forms, filing_dates, accessions, urls


def _process_filer(z: zipfile.ZipFile, main_name: str):
    """Read one main CIK json + its split files; return (cik_padded, pa.Table)
    or (cik_padded, None) if the filer has zero filings."""
    d = json.loads(z.read(main_name))
    cik_padded = d.get("cik") or main_name[3:13]
    cik_padded = str(cik_padded).zfill(10)
    cik_int = int(cik_padded)

    filings = d.get("filings") or {}
    recent = filings.get("recent") or {}

    forms, fdates, accs, urls = [], [], [], []
    built = _build_rows(cik_int, recent)
    if built:
        forms += built[0]
        fdates += built[1]
        accs += built[2]
        urls += built[3]

    # merge every referenced split file for the COMPLETE history
    for fmeta in filings.get("files") or []:
        sub_name = fmeta.get("name")
        if not sub_name:
            continue
        try:
            sub = json.loads(z.read(sub_name))
        except KeyError:
            # referenced split file absent from zip -- skip, history partial
            continue
        b = _build_rows(cik_int, sub)
        if b:
            forms += b[0]
            fdates += b[1]
            accs += b[2]
            urls += b[3]

    if not forms:
        return cik_padded, None

    tbl = pa.table(
        {
            "form": pa.array(forms, pa.string()),
            "filing_date": pa.array(fdates, pa.string()),
            "accession": pa.array(accs, pa.string()),
            "primary_doc_url": pa.array(urls, pa.string()),
        },
        schema=_SCHEMA,
    )
    return cik_padded, tbl


def _worker(worker_id: int, main_names: list[str], q: Queue):
    z = zipfile.ZipFile(ZIP_PATH)
    n_files = 0
    n_rows = 0
    n_empty = 0
    n_err = 0
    last_report = time.time()
    for idx, name in enumerate(main_names):
        try:
            cik_padded, tbl = _process_filer(z, name)
            if tbl is None:
                n_empty += 1
                continue
            sd = os.path.join(OUT_DIR, _shard_dir(cik_padded))
            os.makedirs(sd, exist_ok=True)
            out_path = os.path.join(sd, f"CIK{cik_padded}.parquet")
            pq.write_table(tbl, out_path, compression="zstd")
            n_files += 1
            n_rows += tbl.num_rows
        except Exception as e:  # never let one bad filer kill the worker
            n_err += 1
            if n_err <= 5:
                print(f"[w{worker_id}] ERROR {name}: {type(e).__name__}: {e}", flush=True)
        if worker_id == 0 and time.time() - last_report > 20:
            print(
                f"[w0] {idx + 1}/{len(main_names)} filers | "
                f"{n_files} written, {n_rows} rows, {n_empty} empty",
                flush=True,
            )
            last_report = time.time()
    z.close()
    q.put((worker_id, n_files, n_rows, n_empty, n_err))


def main():
    t0 = time.time()
    os.makedirs(OUT_DIR, exist_ok=True)

    z = zipfile.ZipFile(ZIP_PATH)
    names = z.namelist()
    z.close()
    main_names = [
        n
        for n in names
        if n.startswith("CIK") and "-submissions-" not in n and n.endswith(".json")
    ]
    n_split = sum(1 for n in names if "-submissions-" in n)
    print(
        f"zip entries: {len(names):,} | main filer files: {len(main_names):,} | "
        f"split files (merged into parents): {n_split:,}",
        flush=True,
    )

    # round-robin slice so the few huge filers spread across workers
    slices = [main_names[i::N_WORKERS] for i in range(N_WORKERS)]
    q: Queue = Queue()
    procs = []
    for wid in range(N_WORKERS):
        p = Process(target=_worker, args=(wid, slices[wid], q))
        p.start()
        procs.append(p)

    results = [q.get() for _ in range(N_WORKERS)]
    for p in procs:
        p.join()

    tot_files = sum(r[1] for r in results)
    tot_rows = sum(r[2] for r in results)
    tot_empty = sum(r[3] for r in results)
    tot_err = sum(r[4] for r in results)
    dt = time.time() - t0
    print("=" * 60, flush=True)
    print(f"DONE in {dt / 60:.1f} min", flush=True)
    print(f"source_id           : {SOURCE_ID}", flush=True)
    print(f"license             : {LICENSE_ID}", flush=True)
    print(f"filer parquet files : {tot_files:,}", flush=True)
    print(f"filers w/ 0 filings : {tot_empty:,}", flush=True)
    print(f"errored filers      : {tot_err:,}", flush=True)
    print(f"TOTAL FILINGS INDEXED: {tot_rows:,}", flush=True)
    print(f"output dir          : {OUT_DIR}", flush=True)
    # machine-readable summary line for the orchestrator
    print(
        f"RESULT_JSON {json.dumps({'source_id': SOURCE_ID, 'license': LICENSE_ID, 'files_written': tot_files, 'filers_empty': tot_empty, 'filers_errored': tot_err, 'total_filings_indexed': tot_rows, 'output_dir': OUT_DIR, 'main_filer_files': len(main_names), 'split_files_merged': n_split})}",
        flush=True,
    )


if __name__ == "__main__":
    main()
