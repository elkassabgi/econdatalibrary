#!/usr/bin/env python3
"""Fragile States Index (FSI) ingest — Fund for Peace.

Source: https://fragilestatesindex.org/data/
License: CC BY-NC 3.0 (Fund for Peace)
Coverage: 178 countries, 2006-2023, 12 social/economic/political indicators.

Downloads per-year XLSX files from fragilestatesindex.org.
series_key: FSI_FP:{indicator}:{country}

Output: data/clean_full/fsi_fundforpeace/fsi_fundforpeace.parquet
Run: python jobs/ingest_fsi_fundforpeace.py
"""
from __future__ import annotations
import datetime as dt, io, os, time
import requests, openpyxl
import pyarrow as pa, pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT  = os.path.join(ROOT, "data", "clean_full", "fsi_fundforpeace")
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

# Annual XLSX files - pattern varies by year; try each plausible URL
YEAR_URLS = [
    (2023, "https://fragilestatesindex.org/wp-content/uploads/2023/06/FSI-2023-DOWNLOAD.xlsx"),
    (2022, "https://fragilestatesindex.org/wp-content/uploads/2022/07/fsi-2022-download.xlsx"),
    (2021, "https://fragilestatesindex.org/wp-content/uploads/2021/05/fsi-2021.xlsx"),
    (2021, "https://fragilestatesindex.org/wp-content/uploads/2021/05/FSI-2021-DOWNLOAD.xlsx"),
    (2020, "https://fragilestatesindex.org/wp-content/uploads/2020/05/fsi-2020-download.xlsx"),
    (2019, "https://fragilestatesindex.org/wp-content/uploads/2019/04/FSI-2019-DOWNLOAD.xlsx"),
    (2018, "https://fragilestatesindex.org/wp-content/uploads/2018/04/FSI-2018-DOWNLOAD.xlsx"),
    (2017, "https://fragilestatesindex.org/wp-content/uploads/2017/05/FSI-2017-DOWNLOAD.xlsx"),
    (2016, "https://fragilestatesindex.org/wp-content/uploads/2016/07/FSI-2016-DOWNLOAD.xlsx"),
    (2015, "https://fragilestatesindex.org/wp-content/uploads/2015/06/FSI-2015-DOWNLOAD.xlsx"),
    (2014, "https://fragilestatesindex.org/wp-content/uploads/2014/06/FSI-2014-DOWNLOAD.xlsx"),
    (2013, "https://fragilestatesindex.org/wp-content/uploads/2013/06/FSI-2013-DOWNLOAD.xlsx"),
    (2012, "https://fragilestatesindex.org/wp-content/uploads/2012/06/FSI-2012-DOWNLOAD.xlsx"),
    (2011, "https://fragilestatesindex.org/wp-content/uploads/2011/06/FSI-2011-DOWNLOAD.xlsx"),
    (2010, "https://fragilestatesindex.org/wp-content/uploads/2010/06/FSI-2010-DOWNLOAD.xlsx"),
    (2009, "https://fragilestatesindex.org/wp-content/uploads/2009/06/FSI-2009-DOWNLOAD.xlsx"),
    (2008, "https://fragilestatesindex.org/wp-content/uploads/2008/06/FSI-2008-DOWNLOAD.xlsx"),
    (2007, "https://fragilestatesindex.org/wp-content/uploads/2007/06/FSI-2007-DOWNLOAD.xlsx"),
    (2006, "https://fragilestatesindex.org/wp-content/uploads/2006/06/FSI-2006-DOWNLOAD.xlsx"),
    # CSV mirror
    (None, "https://raw.githubusercontent.com/ksreyes/tidy-fragile-states-index/main/data/fsi.csv"),
]


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def fetch(url, timeout=30):
    try:
        r = requests.get(url, headers=UA, timeout=timeout, allow_redirects=True)
        if r.status_code == 200 and len(r.content) > 5000:
            return r.content
        return None
    except Exception:
        return None


def parse_xlsx(data: bytes, year: int):
    """Parse FSI annual XLSX. Typically has Country, Rank, Total, and 12 sub-indicators."""
    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    ws = wb[wb.sheetnames[0]]
    rows = list(ws.iter_rows(values_only=True))
    header = [str(c).strip() if c else "" for c in rows[0]]
    log(f"  {year}: sheet cols={header[:10]}")

    # Find country name column (FSI uses Country or Country Name)
    ctry_idx = next((i for i, h in enumerate(header) if h.lower() in
                     ("country", "country name", "countryname", "name")), None)
    if ctry_idx is None:
        return [], [], []

    skip_idx = {ctry_idx}
    for i, h in enumerate(header):
        if h.lower() in ("rank", "change", "trend", "year"):
            skip_idx.add(i)

    obs_d = dt.date(year, 12, 31)
    keys, dates, vals = [], [], []
    for row in rows[1:]:
        if not row or row[ctry_idx] is None:
            continue
        ctry = str(row[ctry_idx]).strip()
        if not ctry or ctry.lower() in ("country", "nan"):
            continue
        for ci, (col, cell) in enumerate(zip(header, row)):
            if ci in skip_idx or not col or cell is None:
                continue
            try:
                v = float(cell)
                if v != v or v < 0:
                    continue
                keys.append(f"FSI_FP:{col}:{ctry}")
                dates.append(obs_d)
                vals.append(v)
            except (TypeError, ValueError):
                pass
    return keys, dates, vals


def main():
    os.makedirs(OUT, exist_ok=True)
    out = os.path.join(OUT, "fsi_fundforpeace.parquet")
    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows
        log(f"FSI FundForPeace: already {n:,} rows"); return

    log("=== Fragile States Index (Fund for Peace) Ingest ===")
    all_keys, all_dates, all_vals = [], [], []

    for yr, url in YEAR_URLS:
        data = fetch(url)
        if not data:
            continue
        try:
            if url.endswith(".csv"):
                import csv, io as _io
                text = data.decode("utf-8-sig", errors="replace")
                reader = csv.DictReader(_io.StringIO(text))
                headers = reader.fieldnames or []
                log(f"  CSV: {headers[:10]}")
                year_col = next((h for h in headers if h.lower() == "year"), None)
                ctry_col = next((h for h in headers if h.lower() in ("country","name")), None)
                skip = {(year_col or "").lower(), (ctry_col or "").lower(), "rank"}
                for row in reader:
                    ctry = (row.get(ctry_col) or "").strip()
                    try: row_yr = int(row.get(year_col) or "")
                    except: continue
                    obs_d = dt.date(row_yr, 12, 31)
                    for col, raw in row.items():
                        if col.lower() in skip or not raw:
                            continue
                        try:
                            v = float(raw.replace(",",""))
                            if v != v: continue
                            all_keys.append(f"FSI_FP:{col}:{ctry}")
                            all_dates.append(obs_d)
                            all_vals.append(v)
                        except: pass
                if all_keys:
                    log(f"  CSV total: {len(all_keys):,} obs")
                    break
            else:
                k, d, v = parse_xlsx(data, yr)
                if v:
                    log(f"  {yr}: {len(v):,} obs")
                    all_keys.extend(k); all_dates.extend(d); all_vals.extend(v)
        except Exception as e:
            log(f"  {yr} error: {e}")
        time.sleep(0.5)

    if not all_vals:
        log("0 observations parsed"); return

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out, compression="zstd")
    n = pq.read_metadata(out).num_rows
    log(f"=== FSI DONE: {n:,} obs ===")


if __name__ == "__main__":
    main()
