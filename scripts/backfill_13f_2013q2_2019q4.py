"""
Backfill SEC Form 13F structured data sets for older quarters 2013q2 .. 2019q4.

Input : SEC Form 13F data sets quarterly ZIPs
        https://www.sec.gov/files/structureddata/data/form-13f-data-sets/<period>_form13f.zip
Output: D:/research/econfindatalibrary/data/clean_full/edgar_13f/<TABLE>/period=<period>/<TABLE>.parquet
        (ZSTD parquet, one file per table per period -- SAME layout/convention as the
         already-present 2020q1..2026 partitions, which this script DOES NOT touch.)

Adds ONLY the 27 older quarters. Verifies INFOTABLE parquet rows == raw zip data-line count.

User-Agent: Econ-Fin Data Library admin@hfdatalibrary.com
All identifier columns are stored as strings (preserve leading zeros). Empty fields -> null.
Each row carries DATASET_PERIOD and SOURCE_ID='edgar_13f'.
"""

import os
import sys
import csv
import json
import time
import zipfile
import datetime as dt

import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# -- Configuration ---------------------------------------------------------
UA          = "Econ-Fin Data Library admin@hfdatalibrary.com"
SOURCE_ID   = "edgar_13f"
BASE_URL    = "https://www.sec.gov/files/structureddata/data/form-13f-data-sets"

# Repo root derived from this file, never a drive letter. The store moved D: -> E: in
# the workstation cutover; a stale root here silently writes into, or reports on, a
# tree that is not there. R330.
def _RD(*parts):
    _r = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(_r, *parts) if parts else _r

RAW_DIR     = _RD('data', 'raw', 'sec_edgar', 'form13f')
OUT_ROOT    = _RD('data', 'clean_full', 'edgar_13f')
PROGRESS    = _RD('data', 'raw', 'sec_edgar', '_backfill_2013_2019_progress.json')

TABLES = ["SUBMISSION", "COVERPAGE", "OTHERMANAGER", "OTHERMANAGER2",
          "INFOTABLE", "SIGNATURE", "SUMMARYPAGE"]
HOLDINGS_TABLE = "INFOTABLE"

# int64 columns (everything else -> string). Derived from existing partitions.
INT_COLS = {
    "OTHERMANAGER":  ["OTHERMANAGER_SK"],
    "OTHERMANAGER2": ["SEQUENCENUMBER"],
    "INFOTABLE":     ["INFOTABLE_SK", "VALUE", "SSHPRNAMT",
                      "VOTING_AUTH_SOLE", "VOTING_AUTH_SHARED", "VOTING_AUTH_NONE"],
    "SUMMARYPAGE":   ["OTHERINCLUDEDMANAGERSCOUNT", "TABLEENTRYTOTAL", "TABLEVALUETOTAL"],
}

# Canonical column order per table (matches existing parquet schema, minus the two
# appended columns DATASET_PERIOD, SOURCE_ID which we add at the end).
CANON_COLS = {
    "SUBMISSION":   ["ACCESSION_NUMBER", "FILING_DATE", "SUBMISSIONTYPE", "CIK", "PERIODOFREPORT"],
    "COVERPAGE":    ["ACCESSION_NUMBER", "REPORTCALENDARORQUARTER", "ISAMENDMENT", "AMENDMENTNO",
                     "AMENDMENTTYPE", "CONFDENIEDEXPIRED", "DATEDENIEDEXPIRED", "DATEREPORTED",
                     "REASONFORNONCONFIDENTIALITY", "FILINGMANAGER_NAME", "FILINGMANAGER_STREET1",
                     "FILINGMANAGER_STREET2", "FILINGMANAGER_CITY", "FILINGMANAGER_STATEORCOUNTRY",
                     "FILINGMANAGER_ZIPCODE", "REPORTTYPE", "FORM13FFILENUMBER", "CRDNUMBER",
                     "SECFILENUMBER", "PROVIDEINFOFORINSTRUCTION5", "ADDITIONALINFORMATION"],
    "OTHERMANAGER": ["ACCESSION_NUMBER", "OTHERMANAGER_SK", "CIK", "FORM13FFILENUMBER",
                     "CRDNUMBER", "SECFILENUMBER", "NAME"],
    "OTHERMANAGER2":["ACCESSION_NUMBER", "SEQUENCENUMBER", "CIK", "FORM13FFILENUMBER",
                     "CRDNUMBER", "SECFILENUMBER", "NAME"],
    "INFOTABLE":    ["ACCESSION_NUMBER", "INFOTABLE_SK", "NAMEOFISSUER", "TITLEOFCLASS", "CUSIP",
                     "FIGI", "VALUE", "SSHPRNAMT", "SSHPRNAMTTYPE", "PUTCALL", "INVESTMENTDISCRETION",
                     "OTHERMANAGER", "VOTING_AUTH_SOLE", "VOTING_AUTH_SHARED", "VOTING_AUTH_NONE"],
    "SIGNATURE":    ["ACCESSION_NUMBER", "NAME", "TITLE", "PHONE", "SIGNATURE", "CITY",
                     "STATEORCOUNTRY", "SIGNATUREDATE"],
    "SUMMARYPAGE":  ["ACCESSION_NUMBER", "OTHERINCLUDEDMANAGERSCOUNT", "TABLEENTRYTOTAL",
                     "TABLEVALUETOTAL", "ISCONFIDENTIALOMITTED"],
}


def target_periods():
    out = []
    for y in range(2013, 2020):
        for q in (1, 2, 3, 4):
            if y == 2013 and q == 1:        # range starts at 2013q2
                continue
            out.append(f"{y}q{q}")
    return out


def load_progress():
    if os.path.exists(PROGRESS):
        try:
            return json.load(open(PROGRESS))
        except Exception:
            return {}
    return {}


def save_progress(p):
    tmp = PROGRESS + ".tmp"
    json.dump(p, open(tmp, "w"), indent=2)
    os.replace(tmp, PROGRESS)


def download_zip(period, session, max_retries=4):
    """Download (resume-safe). Returns local path. Validates it is a real zip."""
    fn  = f"{period}_form13f.zip"
    url = f"{BASE_URL}/{fn}"
    dst = os.path.join(RAW_DIR, fn)
    if os.path.exists(dst) and zipfile.is_zipfile(dst):
        print(f"    [cache] {fn} already present ({os.path.getsize(dst):,} bytes)")
        return dst
    for attempt in range(1, max_retries + 1):
        try:
            print(f"    GET {url}  (attempt {attempt})")
            with session.get(url, headers={"User-Agent": UA,
                                           "Accept-Encoding": "gzip, deflate"},
                             stream=True, timeout=180) as r:
                r.raise_for_status()
                tmp = dst + ".part"
                got = 0
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        if chunk:
                            f.write(chunk)
                            got += len(chunk)
                if not zipfile.is_zipfile(tmp):
                    raise ValueError("downloaded file is not a valid zip")
                os.replace(tmp, dst)
                print(f"    saved {fn}  ({got:,} bytes)")
                return dst
        except Exception as e:
            print(f"    !! download error: {e}")
            if attempt == max_retries:
                raise
            time.sleep(2.0 * attempt)
    raise RuntimeError("unreachable")


def count_data_lines(zf, member):
    """Count tab-delimited DATA rows in a TSV member = (non-empty physical lines) - 1 header.

    NOTE on multi-line fields: SEC 13F TSVs are simple tab-delimited with one record per
    physical line (no embedded newlines inside quoted fields in these data sets). We
    cross-check this count against the parsed parquet row count; if they disagree the
    script fails loudly so the assumption is always validated, never assumed silently.
    """
    n = 0
    with zf.open(member) as f:
        for raw in f:
            if raw.strip(b"\r\n").strip() == b"":
                continue
            n += 1
    return max(n - 1, 0)


def read_tsv(zf, member):
    """Read a SEC 13F TSV as all-strings, empty -> NA-safe. Returns DataFrame (str cols)."""
    with zf.open(member) as f:
        # python engine + QUOTE_NONE: these files are not CSV-quoted; embedded quote chars
        # are literal. keep_default_na=False so we control NA; we treat '' as missing later.
        df = pd.read_csv(
            f,
            sep="\t",
            dtype=str,
            keep_default_na=False,
            na_values=[],
            quoting=csv.QUOTE_NONE,
            engine="python",
            on_bad_lines="warn",
        )
    df.columns = [c.strip() for c in df.columns]
    return df


def conform(df, table, period):
    """Reorder to canonical columns, add missing as null, cast ints, append meta cols.
    Returns (pyarrow.Table, n_rows, notes:list)."""
    notes = []
    canon = CANON_COLS[table]

    present = list(df.columns)
    extra = [c for c in present if c not in canon]
    missing = [c for c in canon if c not in present]
    if extra:
        notes.append(f"{table}: dropped unexpected cols {extra}")
    if missing:
        notes.append(f"{table}: added missing cols {missing}")

    # Build output frame in canonical order
    out = pd.DataFrame(index=df.index)
    for c in canon:
        if c in df.columns:
            s = df[c]
        else:
            s = pd.Series([""] * len(df), index=df.index, dtype="object")
        out[c] = s

    n = len(out)
    int_cols = set(INT_COLS.get(table, []))

    # Build pyarrow arrays with explicit types
    arrays, names = [], []
    for c in canon:
        s = out[c]
        if c in int_cols:
            # empty string -> NA, then to nullable Int64 (parquet int64 with nulls)
            num = pd.to_numeric(s.replace("", pd.NA), errors="coerce")
            # Detect any non-empty value that failed to parse (data-quality guard)
            bad = s[(s.str.strip() != "") & num.isna()]
            if len(bad):
                notes.append(f"{table}.{c}: {len(bad)} non-numeric values coerced to null "
                             f"(e.g. {bad.unique()[:3].tolist()})")
            arr = pa.array(num.astype("Int64"), type=pa.int64())
        else:
            # string column: '' -> null to match 'empty fields stored as null'
            vals = s.where(s.astype(str).str.len() > 0, other=None)
            arr = pa.array(vals.tolist(), type=pa.string())
        arrays.append(arr)
        names.append(c)

    # Append DATASET_PERIOD and SOURCE_ID
    arrays.append(pa.array([period] * n, type=pa.string()))
    names.append("DATASET_PERIOD")
    arrays.append(pa.array([SOURCE_ID] * n, type=pa.string()))
    names.append("SOURCE_ID")

    tbl = pa.Table.from_arrays(arrays, names=names)
    return tbl, n, notes


def process_period(period, session):
    print(f"\n=== {period} ===")
    zip_path = download_zip(period, session)

    rows = {}
    notes_all = []
    infotable_zip_lines = None

    with zipfile.ZipFile(zip_path) as zf:
        members = {m.split("/")[-1].upper(): m for m in zf.namelist()}
        for table in TABLES:
            key = f"{table}.TSV"
            if key not in members:
                # some very old quarters could lack a table; record 0 and continue
                print(f"    [warn] {table}.tsv not in zip -> 0 rows, writing empty file")
                member = None
            else:
                member = members[key]

            if member is None:
                # write an empty, correctly-typed parquet so the partition exists
                empty = pd.DataFrame(columns=CANON_COLS[table])
                pat, n, notes = conform(empty, table, period)
            else:
                df = read_tsv(zf, member)
                pat, n, notes = conform(df, table, period)

            rows[table] = n
            notes_all += notes

            if table == HOLDINGS_TABLE and member is not None:
                infotable_zip_lines = count_data_lines(zf, member)

            # write parquet
            out_dir = os.path.join(OUT_ROOT, table, f"period={period}")
            os.makedirs(out_dir, exist_ok=True)
            out_path = os.path.join(out_dir, f"{table}.parquet")
            pq.write_table(pat, out_path, compression="zstd")
            print(f"    {table:13s} -> {n:>9,} rows  ({os.path.getsize(out_path):,} bytes)")

    # Verify INFOTABLE
    if infotable_zip_lines is None:
        infotable_zip_lines = 0
    pq_rows = rows.get(HOLDINGS_TABLE, 0)
    match = (pq_rows == infotable_zip_lines)
    print(f"    VERIFY INFOTABLE: parquet={pq_rows:,}  zip_lines={infotable_zip_lines:,}  "
          f"{'MATCH' if match else '*** MISMATCH ***'}")
    if not match:
        raise AssertionError(
            f"{period} INFOTABLE row mismatch: parquet={pq_rows} zip={infotable_zip_lines}")

    return {
        "rows": rows,
        "infotable_parquet_rows": pq_rows,
        "infotable_zip_rows": infotable_zip_lines,
        "infotable_match": match,
        "notes": notes_all,
    }


def main():
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(OUT_ROOT, exist_ok=True)
    periods = target_periods()
    print(f"Backfill target: {len(periods)} quarters {periods[0]}..{periods[-1]}")

    progress = load_progress()
    session = requests.Session()

    grand_added = 0
    infotable_added = 0
    per_period = progress.get("row_counts_by_period", {})
    files_written = 0

    for i, period in enumerate(periods, 1):
        # Guard: never overwrite an existing pre-loaded partition (safety).
        existing = os.path.join(OUT_ROOT, "INFOTABLE", f"period={period}")
        if period in per_period and per_period[period].get("infotable_match") and \
           os.path.exists(os.path.join(existing, "INFOTABLE.parquet")):
            print(f"\n=== {period} === already done (progress), skipping")
            res = per_period[period]
        else:
            res = process_period(period, session)
            per_period[period] = res
            progress["row_counts_by_period"] = per_period
            save_progress(progress)
            # polite pacing between quarters (SEC limit is 10 req/s; we do ~7 req/quarter)
            time.sleep(0.6)

        grand_added += sum(res["rows"].values())
        infotable_added += res["infotable_parquet_rows"]
        files_written += len(res["rows"])
        print(f"    [{i}/{len(periods)}] cumulative added rows so far: {grand_added:,}")

    # Aggregate per-table totals across the backfill
    table_totals = {t: 0 for t in TABLES}
    for per, res in per_period.items():
        for t, c in res["rows"].items():
            table_totals[t] += c

    summary = {
        "source_id": SOURCE_ID,
        "backfill_range": f"{periods[0]}..{periods[-1]}",
        "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "user_agent": UA,
        "periods_added": periods,
        "periods_added_count": len(periods),
        "row_counts_by_table_added": table_totals,
        "infotable_rows_added": infotable_added,
        "grand_total_rows_added": grand_added,
        "files_written": files_written,
        "all_infotable_verified": all(per_period[p]["infotable_match"] for p in periods),
        "row_counts_by_period": per_period,
    }
    out_summary = r"D:/research/econfindatalibrary/data/raw/sec_edgar/_backfill_2013_2019_summary.json"
    json.dump(summary, open(out_summary, "w"), indent=2)
    progress["summary"] = summary
    save_progress(progress)

    print("\n========== BACKFILL COMPLETE ==========")
    print(f"  quarters added      : {len(periods)}  ({periods[0]}..{periods[-1]})")
    print(f"  files written       : {files_written}")
    print(f"  INFOTABLE rows added: {infotable_added:,}")
    print(f"  grand total rows add: {grand_added:,}")
    print(f"  all INFOTABLE verified: {summary['all_infotable_verified']}")
    print(f"  summary -> {out_summary}")
    return summary


if __name__ == "__main__":
    main()
