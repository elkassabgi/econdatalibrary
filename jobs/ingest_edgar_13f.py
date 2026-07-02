r"""
Ingest SEC Form 13F structured data sets -> grouped Parquet.

Source page (verified 2026-06):
  https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets
Bulk zips:
  https://www.sec.gov/files/structureddata/data/form-13f-data-sets/<key>_form13f.zip

Each quarterly zip contains 7 tab-delimited tables linked by ACCESSION_NUMBER:
  SUBMISSION, COVERPAGE, OTHERMANAGER, OTHERMANAGER2, INFOTABLE (the holdings),
  SIGNATURE, SUMMARYPAGE  -- plus FORM13F_metadata.json and FORM13F_readme.htm.

License: us-public-domain (U.S. SEC EDGAR). reservable=true.

Output layout (Windows D:/ paths):
  D:/research/econfindatalibrary/data/clean_full/edgar_13f/
      <TABLE>/period=<key>/<TABLE>.parquet
      _manifest.json
      _metadata_schema.json
      LICENSE.txt

Raw zip cache:
  D:/research/econfindatalibrary/data/raw/sec_edgar/form13f/<key>_form13f.zip

Run:  python jobs/ingest_edgar_13f.py            # default scope = 2020 onward
      python jobs/ingest_edgar_13f.py --from 2018q1
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import sys
import time
import zipfile
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

# --- license gate (use the library's own gate so this can't publish a bad class) ---
PROJ = r"D:/research/econfindatalibrary"
if PROJ not in sys.path:
    sys.path.insert(0, PROJ)
try:
    from core.licenses import assert_reservable  # type: ignore
except Exception:  # pragma: no cover - fall back to an inline check
    _RESERVABLE = {"us-public-domain"}
    def assert_reservable(license_id, *, context=""):
        if license_id not in _RESERVABLE:
            raise PermissionError(f"License {license_id!r} not re-serveable [{context}]")

SOURCE_ID = "edgar_13f"
LICENSE_ID = "us-public-domain"
ATTRIBUTION = "Source: U.S. SEC EDGAR Form 13F structured data sets (public domain)"
PAGE_URL = "https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets"
BASE_DL = "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/"
UA = "Econ-Fin Data Library admin@hfdatalibrary.com"

OUT_DIR = r"D:/research/econfindatalibrary/data/clean_full/edgar_13f"
RAW_DIR = r"D:/research/econfindatalibrary/data/raw/sec_edgar/form13f"

TABLES = [
    "SUBMISSION", "COVERPAGE", "OTHERMANAGER", "OTHERMANAGER2",
    "INFOTABLE", "SIGNATURE", "SUMMARYPAGE",
]

# Columns to coerce to a numeric (nullable) dtype after reading everything as str.
# Everything else stays string to preserve exact text (CUSIPs, accession numbers,
# leading zeros, etc.). We intentionally do NOT scale VALUE (see note in manifest).
NUMERIC_COLS = {
    "INFOTABLE": {
        "VALUE": "Int64", "SSHPRNAMT": "Int64",
        "VOTING_AUTH_SOLE": "Int64", "VOTING_AUTH_SHARED": "Int64",
        "VOTING_AUTH_NONE": "Int64", "INFOTABLE_SK": "Int64",
    },
    "SUMMARYPAGE": {
        "OTHERINCLUDEDMANAGERSCOUNT": "Int64", "TABLEENTRYTOTAL": "Int64",
        "TABLEVALUETOTAL": "Int64",
    },
    "OTHERMANAGER": {"OTHERMANAGER_SK": "Int64"},
    "OTHERMANAGER2": {"SEQUENCENUMBER": "Int64"},
}

session = requests.Session()
session.headers.update({"User-Agent": UA, "Accept-Encoding": "gzip, deflate"})


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def list_available_keys() -> list[str]:
    """Scrape the data-sets page for every <key>_form13f.zip available."""
    r = session.get(PAGE_URL, timeout=90)
    r.raise_for_status()
    keys = re.findall(r"/form-13f-data-sets/([^/\"]+)_form13f\.zip", r.text)
    # de-dup, preserve order
    seen, out = set(), []
    for k in keys:
        if k not in seen:
            seen.add(k)
            out.append(k)
    return out


_QKEY = re.compile(r"^(\d{4})q([1-4])$")
_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}
_RANGE = re.compile(r"^\d{2}([a-z]{3})(\d{4})-\d{2}([a-z]{3})(\d{4})$")


def key_sort_value(key: str) -> tuple[int, int]:
    """Map a dataset key to (year, quarter-ish) for chronological sorting/filtering.

    Old keys: '2020q1'. New keys (2024+): '01dec2025-28feb2026' -> the report
    quarter is the END month's quarter (e.g. dec..feb window reports the quarter
    ending the prior Dec 31 -> treat by END year/quarter of filing window).
    """
    m = _QKEY.match(key)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = _RANGE.match(key)
    if m:
        end_mon = _MONTHS[m.group(3)]
        end_yr = int(m.group(4))
        q = (end_mon - 1) // 3 + 1
        return end_yr, q
    return (0, 0)


def download_zip(key: str) -> bytes:
    """Download (or load cached) a quarterly zip. Polite + resumable cache."""
    os.makedirs(RAW_DIR, exist_ok=True)
    path = os.path.join(RAW_DIR, f"{key}_form13f.zip")
    if os.path.exists(path) and os.path.getsize(path) > 1000:
        # validate it is a real zip before trusting the cache
        try:
            with zipfile.ZipFile(path) as zt:
                if zt.namelist():
                    log(f"  cache hit: {os.path.basename(path)} ({os.path.getsize(path):,} B)")
                    return open(path, "rb").read()
        except zipfile.BadZipFile:
            log(f"  cached file corrupt, re-downloading: {os.path.basename(path)}")

    url = f"{BASE_DL}{key}_form13f.zip"
    for attempt in range(1, 5):
        try:
            log(f"  downloading {url} (attempt {attempt})")
            with session.get(url, timeout=600, stream=True) as r:
                r.raise_for_status()
                tmp = path + ".part"
                total = 0
                with open(tmp, "wb") as fh:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        if chunk:
                            fh.write(chunk)
                            total += len(chunk)
                os.replace(tmp, path)
            log(f"  saved {os.path.basename(path)} ({total:,} B)")
            time.sleep(0.3)  # be polite to SEC
            return open(path, "rb").read()
        except Exception as e:  # noqa
            log(f"  download error: {e!r}")
            time.sleep(2 * attempt)
    raise RuntimeError(f"failed to download {url}")


def _resolve_member(names: list[str], member: str) -> str | None:
    """Find a member by exact name OR by basename.

    Some quarterly zips store the TSVs at the archive root (e.g. 'INFOTABLE.tsv');
    others nest them under a folder (e.g. '01JUN2025-31AUG2025_form13f/INFOTABLE.tsv').
    Match on basename so both layouts work.
    """
    if member in names:
        return member
    target = member.lower()
    cands = [n for n in names if not n.endswith("/")
             and os.path.basename(n).lower() == target]
    return cands[0] if cands else None


def read_tsv(raw: bytes, member: str) -> pd.DataFrame:
    """Read a tab-delimited member as all-string, robust to embedded quotes."""
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        names = z.namelist()
        resolved = _resolve_member(names, member)
        if resolved is None:
            return pd.DataFrame()
        member = resolved
        with z.open(member) as f:
            df = pd.read_csv(
                f,
                sep="\t",
                dtype=str,
                keep_default_na=False,   # keep empty strings as '' not NaN at read
                na_values=[],
                quoting=csv.QUOTE_NONE,  # SEC TSVs are not quoted; QUOTE_NONE avoids mis-parsing
                engine="c",
                on_bad_lines="warn",
                encoding="utf-8",
                encoding_errors="replace",
            )
    df.columns = [c.strip() for c in df.columns]
    return df


def coerce(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """Turn '' into NA everywhere, then cast known numeric columns to Int64."""
    if df.empty:
        return df
    # treat empty strings as missing
    df = df.replace("", pd.NA)
    for col, dt in NUMERIC_COLS.get(table, {}).items():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(dt)
    return df


def write_parquet(df: pd.DataFrame, table: str, key: str) -> tuple[int, str]:
    out_sub = os.path.join(OUT_DIR, table, f"period={key}")
    os.makedirs(out_sub, exist_ok=True)
    path = os.path.join(out_sub, f"{table}.parquet")
    if df.empty:
        # still record an empty file so the partition exists and row count is explicit
        tbl = pa.table({})
    else:
        # add provenance columns
        df = df.copy()
        df["DATASET_PERIOD"] = key
        df["SOURCE_ID"] = SOURCE_ID
        tbl = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(tbl, path, compression="zstd")
    return (0 if df.empty else len(df)), path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="from_key", default="2020q1",
                    help="earliest dataset key to include, e.g. 2020q1 (default)")
    ap.add_argument("--only", dest="only", default=None,
                    help="comma-separated explicit keys to process (overrides --from)")
    args = ap.parse_args()

    assert_reservable(LICENSE_ID, context="edgar_13f ingest")
    os.makedirs(OUT_DIR, exist_ok=True)

    available = list_available_keys()
    log(f"available datasets on page: {len(available)} "
        f"({available[-1]} .. {available[0]})")

    if args.only:
        want = [k.strip() for k in args.only.split(",") if k.strip()]
    else:
        floor = key_sort_value(args.from_key)
        want = [k for k in available if key_sort_value(k) >= floor]
    # process chronologically (oldest first)
    want = sorted(set(want), key=key_sort_value)
    log(f"selected {len(want)} datasets: {want[0]} .. {want[-1]}")

    manifest = {
        "source_id": SOURCE_ID,
        "title": "SEC Form 13F structured data sets (institutional investment manager holdings)",
        "page_url": PAGE_URL,
        "license": LICENSE_ID,
        "attribution": ATTRIBUTION,
        "user_agent": UA,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "tables": TABLES,
        "holdings_table": "INFOTABLE",
        "join_key": "ACCESSION_NUMBER",
        "partitioning": "clean_full/edgar_13f/<TABLE>/period=<datasetKey>/<TABLE>.parquet",
        "value_units_note": (
            "INFOTABLE.VALUE and SUMMARYPAGE.TABLEVALUETOTAL are stored EXACTLY as "
            "published by SEC. Per SEC guidance the reported VALUE is in whole U.S. "
            "dollars for filings on/after 2023-01-03 (FAQ), and in thousands of "
            "dollars for earlier filings. No scaling was applied during ingest."
        ),
        "source_keys_available": available,
        "source_keys_available_count": len(available),
        "datasets_processed": [],
        "row_counts_by_table": {t: 0 for t in TABLES},
        "row_counts_by_period": {},
        "files_written": 0,
        "raw_cache_dir": RAW_DIR,
    }

    total_rows = 0
    files_written = 0

    for key in want:
        log(f"=== {key} ===")
        raw = download_zip(key)
        sha = hashlib.sha256(raw).hexdigest()
        per_period = {"sha256_zip": sha, "zip_bytes": len(raw), "rows": {}}
        for table in TABLES:
            df = read_tsv(raw, f"{table}.tsv")
            df = coerce(df, table)
            n, path = write_parquet(df, table, key)
            per_period["rows"][table] = n
            manifest["row_counts_by_table"][table] += n
            total_rows += n
            files_written += 1
            log(f"  {table:14s} rows={n:>10,}  -> {os.path.relpath(path, OUT_DIR)}")
        # Sanity guard: a real quarter must have holdings. A 0-row INFOTABLE from a
        # ~80MB zip means we failed to locate the member (silent data loss) -> fail loud.
        if per_period["rows"].get("INFOTABLE", 0) == 0 and len(raw) > 5_000_000:
            raise RuntimeError(
                f"INFOTABLE parsed 0 rows for {key} from a {len(raw):,}B zip -- "
                f"likely an unrecognized member layout. Aborting to avoid silent loss."
            )
        manifest["datasets_processed"].append(key)
        manifest["row_counts_by_period"][key] = per_period

    manifest["files_written"] = files_written
    manifest["grand_total_rows"] = total_rows
    manifest["infotable_total_rows"] = manifest["row_counts_by_table"]["INFOTABLE"]

    with open(os.path.join(OUT_DIR, "_manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)

    # license file
    with open(os.path.join(OUT_DIR, "LICENSE.txt"), "w", encoding="utf-8") as fh:
        fh.write(
            "SEC Form 13F structured data sets\n"
            f"License class: {LICENSE_ID} (reservable, commercial OK, attribution requested)\n"
            f"{ATTRIBUTION}\n"
            "U.S. government works are not subject to copyright (17 U.S.C. 105).\n"
            f"Retrieved from: {PAGE_URL}\n"
        )

    log("")
    log(f"DONE. periods={len(want)} files={files_written} "
        f"grand_total_rows={total_rows:,} INFOTABLE_rows={manifest['infotable_total_rows']:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
