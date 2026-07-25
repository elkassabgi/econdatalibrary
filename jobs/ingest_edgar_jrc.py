#!/usr/bin/env python3
"""EU JRC EDGAR greenhouse gas and air pollutant emissions ingest.

Source: https://edgar.jrc.ec.europa.eu/
License: GHG totals CC BY 4.0; IEA_EDGAR_CO2 also CC BY-NC-ND 4.0 (non-commercial OK)
Coverage: 1970-2024 (GHG), 1970-2022 (air pollutants), all countries

Downloads country-total annual emissions ZIP files from JRC EDGAR 2025 release.
Gases: CO2 (fossil+IEA), CO2bio, CH4, N2O, F-gases, total GHG (AR5), NOx, PM2.5
Air pollutants (v8.1): SO2, BC, CO, NH3, NMVOC, OC, PM10, PM2.5, NOx

series_key: EDGAR:{gas}:{iso3}   e.g. EDGAR:CO2:USA  EDGAR:CH4:DEU

Output: data/clean_full/edgar_jrc/edgar_jrc.parquet
Run: python jobs/ingest_edgar_jrc.py
"""
from __future__ import annotations
import datetime as dt, io, os, time, zipfile
import requests, openpyxl
import pyarrow as pa, pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT  = os.path.join(ROOT, "data", "clean_full", "edgar_jrc")
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

GHG_BASE = "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/EDGAR/datasets/EDGAR_2025_GHG/"
AP_BASE  = "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/EDGAR/datasets/v81_FT2022_AP_new/"

# (gas_label, filename, base_url)
DATASETS = [
    # GHG 2025 release
    ("CO2",    "IEA_EDGAR_CO2_1970_2024.zip",   GHG_BASE),
    ("CO2bio", "EDGAR_CO2bio_1970_2024.zip",     GHG_BASE),
    ("CH4",    "EDGAR_CH4_1970_2024.zip",        GHG_BASE),
    ("N2O",    "EDGAR_N2O_1970_2024.zip",        GHG_BASE),
    ("Fgas",   "EDGAR_F-gases_1990_2024.zip",    GHG_BASE),
    ("GHG",    "EDGAR_AR5_GHG_1970_2024.zip",    GHG_BASE),
    ("NOx_ghg","EDGAR_NOx_1970_2024.zip",        GHG_BASE),
    ("PM25_ghg","EDGAR_PM2.5_1970_2024.zip",     GHG_BASE),
    # Air pollutants v8.1
    ("SO2",    "EDGAR_SO2_1970_2022_v2.zip",     AP_BASE),
    ("BC",     "EDGAR_BC_1970_2022.zip",          AP_BASE),
    ("CO",     "EDGAR_CO_1970_2022.zip",          AP_BASE),
    ("NH3",    "EDGAR_NH3_1970_2022.zip",         AP_BASE),
    ("NMVOC",  "EDGAR_NMVOC_1970_2022.zip",       AP_BASE),
    ("OC",     "EDGAR_OC_1970_2022.zip",          AP_BASE),
    ("PM10",   "EDGAR_PM10_1970_2022.zip",        AP_BASE),
    ("PM25_ap","EDGAR_PM25_1970_2022.zip",        AP_BASE),
    ("NOx_ap", "EDGAR_NOx_1970_2022.zip",         AP_BASE),
]


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def fetch(url: str, retries: int = 3) -> bytes | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=300, allow_redirects=True)
            if r.status_code == 200 and len(r.content) > 1000:
                log(f"  {len(r.content)//1024:,} KB from ...{url[-60:]}")
                return r.content
            log(f"  HTTP {r.status_code}: {url[-60:]}")
            if r.status_code in (403, 404):
                return None
        except Exception as e:
            log(f"  ERR attempt {attempt+1}: {e}")
        time.sleep(5 * (attempt + 1))
    return None


def parse_edgar_xlsx(data: bytes, gas: str) -> tuple[list, list, list]:
    """Parse EDGAR country-total XLSX.

    EDGAR format: rows=countries, cols=years prefixed with 'Y_'
    Key col: 'Country_code_A3' (ISO3) or variants.
    Units: kt (kilotonnes) for GHG, kt for air pollutants.
    """
    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    log(f"  Sheets: {wb.sheetnames}")

    keys, dates, vals = [], [], []

    # Try each sheet — EDGAR often has a "TOTALS BY COUNTRY" or similar sheet
    for sheet_name in wb.sheetnames:
        if any(skip in sheet_name.upper() for skip in ["README", "SOURCES", "INFO", "NOTES", "LEGEND"]):
            continue

        ws = wb[sheet_name]
        rows = list(ws.iter_rows(values_only=True))
        if len(rows) < 3:
            continue

        # Find header row (look for 'Country_code_A3' or 'country' and year columns)
        header_row_idx = None
        header = None
        for ri, row in enumerate(rows[:10]):
            row_strs = [str(c).strip() if c is not None else "" for c in row]
            # Year columns: look for Y_YYYY pattern or plain YYYY
            year_count = sum(1 for s in row_strs if
                             (s.startswith("Y_") and s[2:].isdigit() and len(s[2:]) == 4) or
                             (s.isdigit() and len(s) == 4 and 1960 <= int(s) <= 2030))
            if year_count >= 5:
                header_row_idx = ri
                header = row_strs
                break

        if header is None:
            log(f"  Sheet '{sheet_name}': no year-column header found")
            continue

        log(f"  Sheet '{sheet_name}': header at row {header_row_idx}, {len(header)} cols")

        # Identify country code column
        c3_idx = None
        for cand in ["country_code_a3", "countrycode", "iso3", "iso_3", "country_code",
                     "alpha-3 code", "country code a3", "code", "country"]:
            for i, h in enumerate(header):
                if h.lower().strip() == cand:
                    c3_idx = i
                    break
            if c3_idx is not None:
                break

        if c3_idx is None:
            # Try finding any col with 3-letter country code pattern
            log(f"  No country code col found. Header: {header[:8]}")
            continue

        # Identify year columns
        year_cols = []
        for i, h in enumerate(header):
            if h.startswith("Y_") and h[2:].isdigit() and len(h[2:]) == 4:
                year_cols.append((i, int(h[2:])))
            elif h.isdigit() and len(h) == 4 and 1960 <= int(h) <= 2030:
                year_cols.append((i, int(h)))

        log(f"  Country col idx={c3_idx}, year cols: {len(year_cols)} ({year_cols[0][1] if year_cols else '?'}-{year_cols[-1][1] if year_cols else '?'})")

        n_before = len(vals)
        for row in rows[header_row_idx + 1:]:
            if not row or row[c3_idx] is None:
                continue
            c3 = str(row[c3_idx]).strip()
            if len(c3) != 3 or not c3.isalpha():
                continue

            for col_i, yr in year_cols:
                if col_i >= len(row) or row[col_i] is None:
                    continue
                try:
                    v = float(row[col_i])
                    if v != v:  # NaN
                        continue
                    keys.append(f"EDGAR:{gas}:{c3.upper()}")
                    dates.append(dt.date(yr, 12, 31))
                    vals.append(v)
                except (TypeError, ValueError):
                    pass

        log(f"  -> {len(vals) - n_before:,} obs from sheet '{sheet_name}'")
        if vals:
            break  # Use first successful sheet (usually "TOTALS BY COUNTRY")

    return keys, dates, vals


def parse_edgar_csv(data: bytes, gas: str) -> tuple[list, list, list]:
    """Parse EDGAR CSV format (fallback if no XLSX in ZIP)."""
    import csv
    text = data.decode("utf-8-sig", errors="replace")
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        return [], [], []

    # Find header row
    header = None
    header_idx = 0
    for i, row in enumerate(rows[:10]):
        year_count = sum(1 for c in row if
                         (c.strip().startswith("Y_") and c.strip()[2:].isdigit()) or
                         (c.strip().isdigit() and len(c.strip()) == 4 and 1960 <= int(c.strip()) <= 2030))
        if year_count >= 5:
            header = [c.strip().lower() for c in row]
            header_idx = i
            break

    if header is None:
        return [], [], []

    c3_idx = None
    for cand in ["country_code_a3", "iso3", "country_code", "code", "country"]:
        if cand in header:
            c3_idx = header.index(cand)
            break

    if c3_idx is None:
        return [], [], []

    year_cols = []
    for i, h in enumerate(header):
        if h.startswith("y_") and h[2:].isdigit() and len(h[2:]) == 4:
            year_cols.append((i, int(h[2:])))
        elif h.isdigit() and len(h) == 4 and 1960 <= int(h) <= 2030:
            year_cols.append((i, int(h)))

    keys, dates, vals = [], [], []
    for row in rows[header_idx + 1:]:
        if not row or len(row) <= c3_idx:
            continue
        c3 = row[c3_idx].strip()
        if len(c3) != 3 or not c3.isalpha():
            continue
        for col_i, yr in year_cols:
            if col_i >= len(row):
                continue
            try:
                v = float(row[col_i])
                if v != v:
                    continue
                keys.append(f"EDGAR:{gas}:{c3.upper()}")
                dates.append(dt.date(yr, 12, 31))
                vals.append(v)
            except (TypeError, ValueError):
                pass

    return keys, dates, vals


def ingest_dataset(gas: str, filename: str, base_url: str) -> int:
    url = base_url + filename
    log(f"\n--- {gas} ({filename}) ---")

    data = fetch(url)
    if not data:
        return 0

    if data[:2] != b"PK":
        log("  Not a ZIP file"); return 0

    z = zipfile.ZipFile(io.BytesIO(data))
    members = z.namelist()
    log(f"  ZIP members: {members}")

    # Try XLSX first
    xlsx_members = [m for m in members if m.lower().endswith(".xlsx") and
                    not any(s in m.lower() for s in ["readme", "info", "legend"])]
    csv_members  = [m for m in members if m.lower().endswith(".csv") and
                    not any(s in m.lower() for s in ["readme", "info", "legend"])]

    keys, dates, vals = [], [], []

    if xlsx_members:
        for member in xlsx_members:
            log(f"  Parsing XLSX: {member}")
            k, d, v = parse_edgar_xlsx(z.read(member), gas)
            keys.extend(k); dates.extend(d); vals.extend(v)
            if vals:
                break
    if not vals and csv_members:
        for member in csv_members:
            log(f"  Parsing CSV: {member}")
            k, d, v = parse_edgar_csv(z.read(member), gas)
            keys.extend(k); dates.extend(d); vals.extend(v)
            if vals:
                break

    log(f"  Total: {len(vals):,} obs")
    return len(vals), keys, dates, vals


def main():
    os.makedirs(OUT, exist_ok=True)
    out = os.path.join(OUT, "edgar_jrc.parquet")

    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows
        log(f"EDGAR JRC: already {n:,} rows"); return

    log("=== EU JRC EDGAR Ingest ===")
    all_keys, all_dates, all_vals = [], [], []

    for gas, filename, base_url in DATASETS:
        result = ingest_dataset(gas, filename, base_url)
        if isinstance(result, tuple):
            n, k, d, v = result
            all_keys.extend(k); all_dates.extend(d); all_vals.extend(v)
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
    log(f"=== EDGAR JRC DONE: {n:,} obs ===")


if __name__ == "__main__":
    main()
