#!/usr/bin/env python3
"""UNU-WIDER World Income Inequality Database (WIID) ingest.

Source: https://www.wider.unu.edu/database/world-income-inequality-database-wiid
License: Free for academic/non-commercial use (CC BY-NC 4.0)
Coverage: ~120 countries, 1867-present, Gini and distribution data.

Downloads WIID5 (Apr 2025) ZIP → XLSX and parses all inequality measures.
series_key: WIID:{variable}:{iso3}  e.g. WIID:gini:FIN

Output: data/clean_full/wiid/wiid.parquet
Run: python jobs/ingest_wiid.py
"""
from __future__ import annotations
import datetime as dt, io, os, time, zipfile
import requests, openpyxl
import pyarrow as pa, pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT  = os.path.join(ROOT, "data", "clean_full", "wiid")
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

WIID_ZIP  = "https://www.wider.unu.edu/sites/default/files/WIID/WIID-29APR2025.zip"
WIID_XLSX = "https://www.wider.unu.edu/sites/default/files/WIID/WIID-29APR2025.xlsx"
COMPANION_COUNTRY = "https://www.wider.unu.edu/sites/default/files/WIID/wiidcountry_4.zip"

# Numeric variables to extract from WIID (besides Gini, which is always present)
VALUE_COLS = [
    "gini", "bottom10", "bottom20", "top20", "top10",
    "d1","d2","d3","d4","d5","d6","d7","d8","d9","d10",
    "q1","q2","q3","q4","q5",
    "mean","median","areacov","popcov",
]


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def fetch(url):
    try:
        r = requests.get(url, headers=UA, timeout=300, allow_redirects=True)
        if r.status_code == 200 and len(r.content) > 1000:
            log(f"  Downloaded {len(r.content)//1024:,} KB from {url[-60:]}")
            return r.content
        log(f"  HTTP {r.status_code}: {url[-60:]}")
    except Exception as e:
        log(f"  ERR: {e}")
    return None


def parse_xlsx(data: bytes, sheet_name: str | None = None):
    """Parse WIID XLSX. WIID uses a flat sheet with country/year rows."""
    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    log(f"  Sheets: {wb.sheetnames}")

    if sheet_name:
        ws = wb[sheet_name]
    else:
        # Use first non-readme sheet
        ws = wb[wb.sheetnames[0]]

    rows = list(ws.iter_rows(values_only=True))
    header = [str(c).lower().strip() if c else "" for c in rows[0]]
    log(f"  Columns ({len(header)}): {header[:15]}")

    # Identify key columns
    c3_idx   = next((i for i, h in enumerate(header) if h in ("c3","iso3","country_code","countrycode")), None)
    year_idx = next((i for i, h in enumerate(header) if h in ("year","yr")), None)

    if c3_idx is None or year_idx is None:
        log(f"  Missing c3/year columns. Header: {header[:10]}")
        return [], [], []

    # Map value col names → indices
    val_map = {h: i for i, h in enumerate(header) if h in VALUE_COLS}
    log(f"  Found value columns: {list(val_map.keys())}")

    keys, dates, vals = [], [], []
    for row in rows[1:]:
        if not row or row[c3_idx] is None:
            continue
        c3 = str(row[c3_idx]).strip()
        if len(c3) != 3:
            continue
        try:
            yr = int(row[year_idx])
            obs_d = dt.date(yr, 12, 31)
        except (TypeError, ValueError):
            continue

        for varname, col_i in val_map.items():
            if col_i >= len(row) or row[col_i] is None:
                continue
            try:
                v = float(row[col_i])
                if v != v or v < 0:
                    continue
                keys.append(f"WIID:{varname}:{c3}")
                dates.append(obs_d)
                vals.append(v)
            except (TypeError, ValueError):
                pass

    log(f"  Parsed {len(vals):,} obs")
    return keys, dates, vals


def main():
    os.makedirs(OUT, exist_ok=True)
    out = os.path.join(OUT, "wiid.parquet")
    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows
        log(f"WIID: already {n:,} rows"); return

    log("=== UNU-WIDER WIID Ingest ===")
    all_keys, all_dates, all_vals = [], [], []

    # Try ZIP first (contains XLSX)
    for url in [WIID_ZIP, WIID_XLSX, COMPANION_COUNTRY]:
        log(f"Trying {url[-70:]}...")
        data = fetch(url)
        if not data:
            time.sleep(2); continue

        if data[:2] == b"PK":  # ZIP
            z = zipfile.ZipFile(io.BytesIO(data))
            log(f"  ZIP members: {z.namelist()}")
            xlsx_members = [m for m in z.namelist() if m.endswith(".xlsx")]
            for member in xlsx_members:
                k, d, v = parse_xlsx(z.read(member))
                all_keys.extend(k); all_dates.extend(d); all_vals.extend(v)
        else:
            # Direct XLSX
            k, d, v = parse_xlsx(data)
            all_keys.extend(k); all_dates.extend(d); all_vals.extend(v)

        if all_vals:
            log(f"  Total so far: {len(all_vals):,}"); break
        time.sleep(2)

    if not all_vals:
        log("0 observations parsed"); return

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out, compression="zstd")
    n = pq.read_metadata(out).num_rows
    log(f"=== WIID DONE: {n:,} obs ===")


if __name__ == "__main__":
    main()
