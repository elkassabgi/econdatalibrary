#!/usr/bin/env python3
"""Fama-French factor data from Kenneth French's Data Library at Dartmouth.

Includes: 3-factor (Mkt-RF, SMB, HML), 5-factor (adds RMW, CMA), Momentum,
Momentum (daily), 5-factor (daily). Standard academic finance research panels.

License: Copyright Fama & French. Use permitted for research and educational
purposes (confirmed by Ahmed: education-only institution).
Attribution: "Data source: Kenneth R. French Data Library"
URL: https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html

Run: python jobs/ingest_famafrench.py
"""
import csv, io, os, sys, time, zipfile
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
OUT = os.path.join(ROOT, "data", "clean_full", "famafrench")
TMP = os.path.join(ROOT, "data", "raw", "famafrench")
UA = "Econ-Fin Data Library admin@hfdatalibrary.com"
BASE = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp"

# Key datasets — (local_name, zip_filename)
DATASETS = {
    "factors_3_monthly": "F-F_Research_Data_Factors_CSV.zip",
    "factors_3_daily":   "F-F_Research_Data_Factors_daily_CSV.zip",
    "factors_5_monthly": "F-F_Research_Data_5_Factors_2x3_CSV.zip",
    "factors_5_daily":   "F-F_Research_Data_5_Factors_2x3_daily_CSV.zip",
    "momentum_monthly":  "F-F_Momentum_Factor_CSV.zip",
    "momentum_daily":    "F-F_Momentum_Factor_daily_CSV.zip",
    "st_reversal_monthly": "F-F_ST_Reversal_Factor_CSV.zip",
    "lt_reversal_monthly": "F-F_LT_Reversal_Factor_CSV.zip",
}


def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def download(name, fname):
    os.makedirs(TMP, exist_ok=True)
    dest = os.path.join(TMP, fname)
    if os.path.exists(dest) and os.path.getsize(dest) > 500:
        return dest
    url = f"{BASE}/{fname}"
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=60)
        if r.status_code != 200:
            log(f"  {name}: HTTP {r.status_code} from {url}"); return None
        with open(dest, "wb") as f:
            f.write(r.content)
        log(f"  {name}: downloaded {len(r.content)/1e3:.0f} KB")
        return dest
    except Exception as e:
        log(f"  {name}: download ERR {e}"); return None


def parse_csv(text, name, is_daily):
    """Parse Fama-French CSV — skip preamble, read factors, stop at annual section."""
    rows = []
    lines = text.replace("\r", "").splitlines()
    # Find the header line (first line with a comma after numeric preamble)
    hdr_i = None
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped and stripped[0].isdigit():
            # This is the start of data; header is the previous non-empty line
            for j in range(i - 1, -1, -1):
                if lines[j].strip():
                    hdr_i = j; break
            break
    if hdr_i is None:
        log(f"  {name}: cannot find header"); return []

    reader = csv.reader(lines[hdr_i:])
    hdr = [h.strip() for h in next(reader)]
    factor_cols = [h for h in hdr[1:] if h]  # skip date column
    for row in reader:
        if not row or not row[0].strip(): continue
        date_str = row[0].strip()
        if not date_str.isdigit(): break  # hit the annual section or end
        try:
            if is_daily and len(date_str) == 8:
                import datetime as dt
                d = dt.date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
            elif len(date_str) == 6:  # YYYYMM
                import datetime as dt
                d = dt.date(int(date_str[:4]), int(date_str[4:6]), 1)
            elif len(date_str) == 4:  # YYYY
                import datetime as dt
                d = dt.date(int(date_str), 12, 31)
            else:
                continue
        except (ValueError, IndexError):
            continue
        for i, col in enumerate(factor_cols, 1):
            if i >= len(row): break
            v = row[i].strip()
            if not v or v in ("", " ", "99.99"): continue
            try: fv = float(v)
            except ValueError: continue
            rows.append((f"ff:{name}:{col}", d, fv))
    return rows


def ingest_dataset(name, fname):
    out_path = os.path.join(OUT, f"{name}.parquet")
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"  {name}: already {n:,} rows"); return n

    dest = download(name, fname)
    if not dest: return 0

    try:
        z = zipfile.ZipFile(dest)
        csvs = [m for m in z.namelist() if m.lower().endswith(".csv")]
        if not csvs:
            log(f"  {name}: no CSV in zip"); return 0
        text = z.read(csvs[0]).decode("utf-8", errors="replace")
        is_daily = "daily" in name
        rows = parse_csv(text, name, is_daily)
        if not rows:
            log(f"  {name}: 0 rows parsed"); return 0

        tbl = pa.table({
            "series_key": pa.array([r[0] for r in rows], pa.string()),
            "obs_date":   pa.array([r[1] for r in rows], pa.date32()),
            "value":      pa.array([r[2] for r in rows], pa.float64()),
        })
        os.makedirs(OUT, exist_ok=True)
        pq.write_table(tbl, out_path, compression="zstd")
        n = pq.read_metadata(out_path).num_rows
        log(f"  {name}: {n:,} obs"); return n
    except Exception as e:
        log(f"  {name}: ERR {e}"); return 0


def main():
    os.makedirs(OUT, exist_ok=True)
    total = 0
    for name, fname in DATASETS.items():
        total += ingest_dataset(name, fname)
        time.sleep(0.5)
    log(f"DONE: {total:,} Fama-French factor observations")


if __name__ == "__main__":
    main()
