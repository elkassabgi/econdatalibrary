#!/usr/bin/env python3
"""Energy Institute Statistical Review of World Energy ingest.

Source: https://www.energyinst.org/statistical-review
License: CC BY 4.0 (Energy Institute, formerly BP Statistical Review)
Coverage: 80+ countries, 1965-present, energy production/consumption by fuel type.

Downloads the official Excel workbook from the Energy Institute.
series_key: EISR:{variable}:{country}  e.g. EISR:coalcons_ej:USA

Output: data/clean_full/ei_statreview/ei_statreview.parquet
Run: python jobs/ingest_ei_statreview.py
"""
from __future__ import annotations
import datetime as dt, io, os, time, zipfile
import requests, openpyxl
import pyarrow as pa, pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT  = os.path.join(ROOT, "data", "clean_full", "ei_statreview")
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

# Energy Institute Statistical Review download URLs
URLS = [
    # Official Energy Institute (2024 data)
    "https://www.energyinst.org/__data/assets/excel_doc/0020/1540154/EI-Stats-Review-All-Data.xlsx",
    "https://www.energyinst.org/__data/assets/excel_doc/0007/1055545/Statistical-Review-of-World-Energy-Consolidated-Dataset-panel-format.csv",
    # GitHub mirror maintained by OWID
    "https://raw.githubusercontent.com/owid/energy-data/master/owid-energy-data.csv",
    # Previous BP download (2022, now public)
    "https://www.bp.com/content/dam/bp/business-sites/en/global/corporate/xlsx/energy-economics/statistical-review/bp-stats-review-2022-all-data.xlsx",
]

# Expected sheet names in the EI workbook
PANEL_SHEETS = ["Panel data"]  # 'Panel data' sheet has long-format data


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def fetch(url, timeout=180):
    try:
        r = requests.get(url, headers=UA, timeout=timeout, allow_redirects=True)
        if r.status_code == 200 and len(r.content) > 10_000:
            log(f"  Downloaded {len(r.content)//1024:,} KB from {url[-60:]}")
            return r.content
        log(f"  HTTP {r.status_code}: {url[-70:]}")
    except Exception as e:
        log(f"  ERR: {e}")
    return None


def parse_panel_xlsx(data: bytes):
    """Parse EI Statistical Review panel format XLSX."""
    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    log(f"  Sheets: {wb.sheetnames}")

    # Try 'Panel data' sheet first
    sheet_name = None
    for candidate in ["Panel data", "Panel Data", "panel_data", "Data"]:
        if candidate in wb.sheetnames:
            sheet_name = candidate
            break
    if sheet_name is None:
        sheet_name = wb.sheetnames[0]

    ws = wb[sheet_name]
    rows = list(ws.iter_rows(values_only=True))
    header = [str(c).strip() if c else "" for c in rows[0]]
    log(f"  Sheet '{sheet_name}': {len(rows):,} rows, columns: {header[:12]}")

    # Key columns: country, year, + numeric variables
    ctry_idx = next((i for i, h in enumerate(header) if h.lower() in
                     ("country", "iso3", "iso_code", "country_name")), None)
    year_idx = next((i for i, h in enumerate(header) if h.lower() in ("year", "yr")), None)

    if ctry_idx is None or year_idx is None:
        log(f"  Missing country/year columns in {header[:10]}"); return [], [], []

    skip_idx = {ctry_idx, year_idx}
    for i, h in enumerate(header):
        if h.lower() in ("region", "continent", "sub_region"):
            skip_idx.add(i)

    keys, dates, vals = [], [], []
    for row in rows[1:]:
        if not row or row[ctry_idx] is None:
            continue
        ctry = str(row[ctry_idx]).strip()
        if not ctry:
            continue
        try:
            yr = int(row[year_idx])
            obs_d = dt.date(yr, 12, 31)
        except (TypeError, ValueError):
            continue
        for ci, (col, cell) in enumerate(zip(header, row)):
            if ci in skip_idx or not col or cell is None:
                continue
            try:
                v = float(cell)
                if v != v:
                    continue
                keys.append(f"EISR:{col}:{ctry}")
                dates.append(obs_d)
                vals.append(v)
            except (TypeError, ValueError):
                pass

    return keys, dates, vals


def parse_csv(data: bytes):
    """Parse CSV version (OWID energy data or panel CSV)."""
    import csv
    text = data.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    headers = reader.fieldnames or []
    log(f"  CSV columns ({len(headers)}): {headers[:12]}")

    iso3_col = next((h for h in headers if h.lower() in
                     ("iso_code", "iso3", "country_code", "code")), None)
    ctry_col = next((h for h in headers if h.lower() in ("country", "entity")), None)
    year_col  = next((h for h in headers if h.lower() in ("year", "yr")), None)

    if not (iso3_col or ctry_col) or not year_col:
        log(f"  Missing columns"); return [], [], []

    skip = {(iso3_col or "").lower(), (ctry_col or "").lower(),
            (year_col or "").lower(), "country", "entity"}

    keys, dates, vals = [], [], []
    for row in reader:
        if iso3_col:
            ctry = (row.get(iso3_col) or "").strip()
        else:
            ctry = (row.get(ctry_col) or "").strip().replace(" ", "_")[:30]
        if not ctry:
            continue
        try:
            yr = int(float(row.get(year_col) or ""))
            obs_d = dt.date(yr, 12, 31)
        except (ValueError, TypeError):
            continue
        for col, raw in row.items():
            if col.lower() in skip or not col:
                continue
            if not raw or str(raw).strip() in ("", "NA", "N/A", "nan", "#N/A"):
                continue
            try:
                v = float(str(raw).replace(",", ""))
                if v != v:
                    continue
                keys.append(f"EISR:{col}:{ctry}")
                dates.append(obs_d)
                vals.append(v)
            except (TypeError, ValueError):
                pass
    return keys, dates, vals


def main():
    os.makedirs(OUT, exist_ok=True)
    out = os.path.join(OUT, "ei_statreview.parquet")
    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows
        log(f"EI StatReview: already {n:,} rows"); return

    log("=== Energy Institute Statistical Review Ingest ===")
    all_keys, all_dates, all_vals = [], [], []

    for url in URLS:
        log(f"Trying {url[-70:]}...")
        data = fetch(url)
        if not data:
            time.sleep(2); continue

        try:
            if url.endswith(".csv"):
                k, d, v = parse_csv(data)
            else:
                k, d, v = parse_panel_xlsx(data)

            if v:
                log(f"  Got {len(v):,} obs")
                all_keys.extend(k); all_dates.extend(d); all_vals.extend(v)
                break
        except Exception as e:
            log(f"  Parse error: {e}")
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
    log(f"=== EI StatReview DONE: {n:,} obs ===")


if __name__ == "__main__":
    main()
