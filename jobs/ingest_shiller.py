#!/usr/bin/env python3
"""Robert Shiller's US Stock Market & CAPE Data — monthly S&P 500, 1871–present.

License: Public domain (provided by Prof. Robert J. Shiller, Yale University)
Source: http://www.econ.yale.edu/~shiller/data.htm
No API key required.

Coverage:
  * Monthly data from January 1871 to present
  * S&P 500 composite price, dividends, earnings
  * CPI (Consumer Price Index)
  * Long-term bond yield
  * Real price, real dividends, real earnings
  * CAPE (Cyclically Adjusted Price-Earnings ratio, aka PE10)
  * Earnings yield (inverse CAPE)
  * Bond equity earnings yield ratio

Run: python jobs/ingest_shiller.py
"""
from __future__ import annotations
import datetime as dt, io, os, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "shiller")
URL  = "http://www.econ.yale.edu/~shiller/data/ie_data.xls"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

COLUMNS = [
    # (col_index_in_excel, series_name, description)
    # Based on Shiller's ie_data.xls structure
    # Sheet "Data" rows start after header
    # Cols: Date, Price, Dividend, Earnings, CPI, Date, Long Rate, Real Price, Real Div, Real Earn, CAPE
    (1,  "sp500_price",     "S&P 500 Composite Stock Price Index"),
    (2,  "sp500_dividend",  "S&P 500 Dividends Per Share"),
    (3,  "sp500_earnings",  "S&P 500 Earnings Per Share"),
    (4,  "cpi",             "Consumer Price Index (CPI-U)"),
    (6,  "long_rate",       "Long-Term Interest Rate (10Y Treasury)"),
    (7,  "real_price",      "Real S&P 500 Price (inflation adjusted)"),
    (8,  "real_dividend",   "Real Dividends (inflation adjusted)"),
    (9,  "real_earnings",   "Real Earnings (inflation adjusted)"),
    (10, "cape",            "CAPE (Shiller P/E10 Cyclically Adjusted P/E)"),
]


def log(m):
    try:
        print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
    except UnicodeEncodeError:
        print(f"[{time.strftime('%H:%M:%S')}] {str(m).encode('ascii', 'replace').decode()}", flush=True)


def parse_shiller_date(val) -> dt.date | None:
    """Parse Shiller date format: 1871.01 (year.month_fraction)"""
    if val is None:
        return None
    try:
        s = str(val).strip()
        if "." in s:
            parts = s.split(".")
            yr = int(parts[0])
            # month fraction: .01=Jan, .02=Feb, ... .1=Oct, .11=Nov, .12=Dec
            mon_frac = parts[1] if len(parts) > 1 else "01"
            # Handle both "1" and "01" format
            mon = int(mon_frac[:2] if len(mon_frac) >= 2 else mon_frac.ljust(2, "0"))
            if mon == 0:
                mon = 1
            return dt.date(yr, min(mon, 12), 1)
        elif len(s) == 7 and "-" in s:
            return dt.date.fromisoformat(s + "-01")
        elif s.isdigit() and len(s) == 6:
            return dt.date(int(s[:4]), int(s[4:6]), 1)
    except (ValueError, TypeError, IndexError):
        pass
    return None


def main():
    os.makedirs(OUT, exist_ok=True)
    out_path = os.path.join(OUT, "shiller.parquet")
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"Already done: {n:,} rows"); return

    log(f"Downloading Shiller IE data from {URL}")
    try:
        r = requests.get(URL, headers=UA, timeout=120)
        r.raise_for_status()
        content = r.content
        log(f"Downloaded {len(content):,} bytes")
    except Exception as e:
        log(f"ERROR: {e}"); return

    try:
        import xlrd
        wb = xlrd.open_workbook(file_contents=content)
        log(f"Sheets: {wb.sheet_names()}")

        # Find the data sheet
        ws = None
        for name in wb.sheet_names():
            if "data" in name.lower() or "ie" in name.lower():
                ws = wb.sheet_by_name(name)
                break
        if ws is None:
            ws = wb.sheet_by_index(0)

        log(f"Total rows: {ws.nrows}, cols: {ws.ncols}")

        # Find header row and data start
        data_start = 0
        for i in range(ws.nrows):
            row_vals = ws.row_values(i)
            if row_vals and row_vals[0] is not None:
                try:
                    val = str(row_vals[0]).strip()
                    f = float(val) if val else 0
                    if 1870 <= f <= 1880:
                        data_start = i
                        break
                except (ValueError, TypeError):
                    pass

        log(f"Data starts at row {data_start} (0-indexed)")

        # Parse data rows
        all_keys, all_dates, all_vals = [], [], []

        for i in range(data_start, ws.nrows):
            row = ws.row_values(i)
            if not row or row[0] is None or row[0] == "":
                break
            obs_date = parse_shiller_date(row[0])
            if obs_date is None:
                continue

            for col_idx, name, desc in COLUMNS:
                if col_idx >= len(row):
                    continue
                v_raw = row[col_idx]
                if v_raw is None or v_raw == "":
                    continue
                try:
                    v = float(v_raw)
                    if v != v:   # NaN check
                        continue
                except (ValueError, TypeError):
                    continue
                all_keys.append(name)
                all_dates.append(obs_date)
                all_vals.append(v)

    except Exception as e:
        log(f"Parse error: {e}")
        import traceback
        traceback.print_exc()
        return

    if not all_vals:
        log("0 observations"); return

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"DONE: {n:,} Shiller observations ({len(set(all_keys))} series)")


if __name__ == "__main__":
    main()
