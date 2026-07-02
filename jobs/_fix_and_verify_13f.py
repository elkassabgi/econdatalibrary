r"""
Remediate + verify the edgar_13f Parquet output after an interrupted run.

1. Re-process any period whose INFOTABLE parquet has 0 rows (the subdirectory-layout
   bug) using the fixed reader in ingest_edgar_13f.py.
2. Rebuild _manifest.json from the ACTUAL row counts of every parquet on disk
   (ground truth), cross-checking INFOTABLE parquet row counts against the raw zip
   line counts for a verification sample.
"""
from __future__ import annotations
import importlib.util, io, json, os, zipfile, csv
from datetime import datetime, timezone
import pyarrow.parquet as pq

PROJ = r"D:/research/econfindatalibrary"
OUT_DIR = r"D:/research/econfindatalibrary/data/clean_full/edgar_13f"
RAW_DIR = r"D:/research/econfindatalibrary/data/raw/sec_edgar/form13f"

# load the ingest module (reuse its fixed read_tsv / coerce / write_parquet / download_zip)
spec = importlib.util.spec_from_file_location("ing", os.path.join(PROJ, "jobs", "ingest_edgar_13f.py"))
ing = importlib.util.module_from_spec(spec); spec.loader.exec_module(ing)

TABLES = ing.TABLES


def parquet_rows(path: str) -> int:
    if not os.path.exists(path):
        return -1
    return pq.ParquetFile(path).metadata.num_rows


def list_periods() -> list[str]:
    base = os.path.join(OUT_DIR, "INFOTABLE")
    out = []
    for d in os.listdir(base):
        if d.startswith("period="):
            out.append(d[len("period="):])
    return sorted(out, key=ing.key_sort_value)


def zip_line_count(key: str, member_basename: str) -> int:
    """Authoritative data-row count straight from the raw zip (lines - header)."""
    path = os.path.join(RAW_DIR, f"{key}_form13f.zip")
    if not os.path.exists(path):
        return -1
    with zipfile.ZipFile(path) as z:
        resolved = ing._resolve_member(z.namelist(), member_basename)
        if resolved is None:
            return -1
        with z.open(resolved) as f:
            n = sum(1 for _ in f)
    return n - 1  # minus header


def main():
    periods = list_periods()
    print(f"periods on disk: {len(periods)}")

    # 1) find + fix any 0-row INFOTABLE period
    fixed = []
    for key in periods:
        p = os.path.join(OUT_DIR, "INFOTABLE", f"period={key}", "INFOTABLE.parquet")
        if parquet_rows(p) == 0:
            print(f"  REPROCESSING broken period: {key}")
            raw = ing.download_zip(key)
            for table in TABLES:
                df = ing.coerce(ing.read_tsv(raw, f"{table}.tsv"), table)
                n, outp = ing.write_parquet(df, table, key)
                print(f"    {table:14s} rows={n:>10,}")
            fixed.append(key)
    print(f"fixed periods: {fixed or 'none'}")

    # 2) rebuild manifest from actual disk counts + verify INFOTABLE vs raw zips
    row_counts_by_table = {t: 0 for t in TABLES}
    row_counts_by_period = {}
    files_written = 0
    grand_total = 0
    verify = {"checked": [], "mismatches": []}

    for key in periods:
        per = {"rows": {}}
        for table in TABLES:
            p = os.path.join(OUT_DIR, table, f"period={key}", f"{table}.parquet")
            n = parquet_rows(p)
            per["rows"][table] = n
            row_counts_by_table[table] += max(n, 0)
            grand_total += max(n, 0)
            files_written += 1 if n >= 0 else 0
        # verify INFOTABLE parquet rows == raw zip data lines
        zc = zip_line_count(key, "INFOTABLE.tsv")
        pc = per["rows"]["INFOTABLE"]
        per["infotable_parquet_rows"] = pc
        per["infotable_zip_rows"] = zc
        per["infotable_match"] = (zc == pc)
        verify["checked"].append(key)
        if zc != pc:
            verify["mismatches"].append({"period": key, "zip": zc, "parquet": pc})
        row_counts_by_period[key] = per

    manifest = {
        "source_id": ing.SOURCE_ID,
        "title": "SEC Form 13F structured data sets (institutional investment manager holdings)",
        "page_url": ing.PAGE_URL,
        "license": ing.LICENSE_ID,
        "attribution": ing.ATTRIBUTION,
        "user_agent": ing.UA,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "tables": TABLES,
        "holdings_table": "INFOTABLE",
        "join_key": "ACCESSION_NUMBER",
        "partitioning": "clean_full/edgar_13f/<TABLE>/period=<datasetKey>/<TABLE>.parquet",
        "value_units_note": (
            "INFOTABLE.VALUE and SUMMARYPAGE.TABLEVALUETOTAL are stored EXACTLY as "
            "published by SEC. Per SEC guidance the reported VALUE is in whole U.S. "
            "dollars for filings on/after 2023-01-03, and in thousands of dollars for "
            "earlier filings. No scaling was applied during ingest."
        ),
        "periods": periods,
        "periods_count": len(periods),
        "row_counts_by_table": row_counts_by_table,
        "row_counts_by_period": row_counts_by_period,
        "files_written": files_written,
        "grand_total_rows": grand_total,
        "infotable_total_rows": row_counts_by_table["INFOTABLE"],
        "verification": verify,
        "raw_cache_dir": RAW_DIR,
    }
    with open(os.path.join(OUT_DIR, "_manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)

    print("\n=== VERIFICATION (INFOTABLE parquet rows vs raw zip data lines) ===")
    for key in periods:
        per = row_counts_by_period[key]
        flag = "OK " if per["infotable_match"] else "MISMATCH"
        print(f"  {flag}  {key:24s} parquet={per['infotable_parquet_rows']:>10,}  zip={per['infotable_zip_rows']:>10,}")
    print(f"\nmismatches: {len(verify['mismatches'])}")
    print(f"periods={len(periods)} files={files_written} "
          f"grand_total_rows={grand_total:,} INFOTABLE_rows={row_counts_by_table['INFOTABLE']:,}")
    # also report per-table totals
    print("per-table totals:")
    for t in TABLES:
        print(f"  {t:14s} {row_counts_by_table[t]:>12,}")


if __name__ == "__main__":
    main()
