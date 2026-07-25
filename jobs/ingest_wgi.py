#!/usr/bin/env python3
"""World Bank World Governance Indicators (WGI) ingest.

Source: https://info.worldbank.org/governance/wgi/
License: CC BY 4.0
Coverage: ~214 economies, 1996-2023, 6 governance dimensions + sub-indicators

Dimensions:
  VA: Voice and Accountability
  PV: Political Stability and Absence of Violence
  GE: Government Effectiveness
  RQ: Regulatory Quality
  RL: Rule of Law
  CC: Control of Corruption

Downloads the official WGI Excel file (AllCountries) from the World Bank,
which includes Estimate, StdErr, NumSrc, Rank, Lower, Upper for each indicator.

series_key: WGI:{indicator}:{iso3}   e.g. WGI:GE.EST:USA

Output: data/clean_full/wgi/wgi.parquet
Run: python jobs/ingest_wgi.py
"""
from __future__ import annotations
import datetime as dt, io, os, time
import requests, openpyxl
import pyarrow as pa, pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT  = os.path.join(ROOT, "data", "clean_full", "wgi")
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

# WGI download URLs — World Bank hosts all-countries Excel
WGI_URLS = [
    "https://www.worldbank.org/content/dam/sites/govindicators/doc/WGIData.xlsx",
    "https://databank.worldbank.org/data/download/WGI_EXCEL.zip",
    "https://info.worldbank.org/governance/wgi/Home/downLoadFile?fileName=wgidataset.xlsx",
    # Direct archive link
    "https://www.worldbank.org/content/dam/sites/govindicators/doc/wgidataset.xlsx",
]


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def fetch(url: str, retries: int = 3) -> bytes | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=120, allow_redirects=True, stream=True)
            if r.status_code == 200:
                chunks = []
                for chunk in r.iter_content(chunk_size=1 << 20):
                    if chunk:
                        chunks.append(chunk)
                data = b"".join(chunks)
                if len(data) > 10_000:
                    log(f"  {len(data)//1024:,} KB from {url[-70:]}")
                    return data
                log(f"  Too small: {len(data)} bytes")
            else:
                log(f"  HTTP {r.status_code}")
                if r.status_code in (403, 404):
                    break
        except Exception as e:
            log(f"  ERR attempt {attempt+1}: {e}")
        time.sleep(5 * (attempt + 1))
    return None


def parse_wgi_xlsx(data: bytes) -> tuple[list, list, list]:
    """Parse WGI Excel. Multiple sheets, each a governance indicator.

    Sheet format: rows=countries × years, with Estimate, StdErr, NumSrc columns.
    Or: wide format with years as columns.
    """
    import zipfile
    # Handle ZIP
    if data[:2] == b"PK":
        z = zipfile.ZipFile(io.BytesIO(data))
        xlsx_files = [m for m in z.namelist() if m.lower().endswith(".xlsx")]
        if not xlsx_files:
            log("  No XLSX in ZIP"); return [], [], []
        data = z.read(xlsx_files[0])
        log(f"  Extracted {xlsx_files[0]}")

    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    log(f"  Sheets: {wb.sheetnames}")
    keys, dates, vals = [], [], []

    for sheet_name in wb.sheetnames:
        if any(s in sheet_name.lower() for s in ["cover", "readme", "metadata", "note", "info", "about"]):
            continue
        ws = wb[sheet_name]
        rows = list(ws.iter_rows(values_only=True))
        if len(rows) < 5:
            continue

        # Find header row
        header_idx = None
        for ri, row in enumerate(rows[:15]):
            row_strs = [str(c).strip().lower() if c else "" for c in row]
            if any(v in row_strs for v in ["country/territory", "country code", "code", "countrycode"]):
                header_idx = ri; break

        if header_idx is None:
            continue

        header = [str(c).strip() if c else "" for c in rows[header_idx]]
        log(f"  Sheet '{sheet_name}': header at row {header_idx}: {header[:8]}")

        # WGI "Data" sheet format:
        # Columns: Country Name, Country Code, Indicator Name, Indicator Code, 1996, 1998, 2000, ...
        # Each row = one country × one indicator; year columns = values

        ctry_ci = next((i for i, h in enumerate(header)
                        if h.lower() in ("country code", "countrycode", "iso3", "code")), None)
        ind_ci  = next((i for i, h in enumerate(header)
                        if h.lower() in ("indicator code", "indicatorcode", "series_code", "seriescode")), None)
        year_ci = next((i for i, h in enumerate(header) if h.lower() == "year"), None)

        if ctry_ci is None:
            log(f"  No country code col in '{sheet_name}'")
            continue

        # Year columns (wide format): header cells that are 4-digit years
        year_cols = [(i, int(h)) for i, h in enumerate(header)
                     if h.isdigit() and 1990 <= int(h) <= 2030]

        # For non-wide format (long format with Year column): numeric value columns
        non_year_val_cols = []
        if not year_cols and year_ci is not None:
            for i, h in enumerate(header):
                if i in {ctry_ci, ind_ci, year_ci, None} or not h:
                    continue
                hl = h.lower()
                if any(v in hl for v in ["estimate", "stderr", "numsrc", "rank", "lower", "upper", "pct"]):
                    non_year_val_cols.append((i, h.replace(" ","_")[:20]))

        if not year_cols and not non_year_val_cols:
            log(f"  No value cols in '{sheet_name}'")
            continue

        n_before = len(vals)
        for row in rows[header_idx + 1:]:
            if not row or len(row) <= ctry_ci or row[ctry_ci] is None:
                continue
            c = str(row[ctry_ci]).strip()
            if not c or len(c) > 5 or c.lower() in ("nan", "none", ""):
                continue

            # Indicator code
            if ind_ci is not None and ind_ci < len(row) and row[ind_ci] is not None:
                ind = str(row[ind_ci]).strip().replace(" ", "_")[:20]
            else:
                ind = sheet_name.strip().replace(" ", "_")[:15]

            if year_cols:
                # Wide format: one obs per (country, indicator, year)
                for col_i, yr in year_cols:
                    if col_i >= len(row) or row[col_i] is None:
                        continue
                    try:
                        v = float(row[col_i])
                        if v != v:
                            continue
                        keys.append(f"WGI:{ind}:{c}")
                        dates.append(dt.date(yr, 12, 31))
                        vals.append(v)
                    except (TypeError, ValueError):
                        pass
            else:
                # Long format
                yr_raw = row[year_ci] if year_ci is not None else None
                if yr_raw is None:
                    continue
                try:
                    yr = int(float(str(yr_raw).strip()))
                    obs_d = dt.date(yr, 12, 31)
                except (TypeError, ValueError):
                    continue
                for col_i, col_label in non_year_val_cols:
                    if col_i >= len(row) or row[col_i] is None:
                        continue
                    try:
                        v = float(row[col_i])
                        if v != v:
                            continue
                        keys.append(f"WGI:{ind}:{col_label}:{c}")
                        dates.append(obs_d)
                        vals.append(v)
                    except (TypeError, ValueError):
                        pass

        n_new = len(vals) - n_before
        log(f"  Sheet '{sheet_name}': {n_new:,} obs")

    return keys, dates, vals


def main():
    os.makedirs(OUT, exist_ok=True)
    out = os.path.join(OUT, "wgi.parquet")

    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows
        log(f"WGI: already {n:,} rows"); return

    log("=== World Governance Indicators Ingest ===")

    for url in WGI_URLS:
        log(f"Trying {url[-70:]}...")
        data = fetch(url)
        if not data:
            continue
        k, d, v = parse_wgi_xlsx(data)
        if v:
            tbl = pa.table({
                "series_key": pa.array(k,  pa.string()),
                "obs_date":   pa.array(d,  pa.date32()),
                "value":      pa.array(v,  pa.float64()),
            })
            pq.write_table(tbl, out, compression="zstd")
            n = pq.read_metadata(out).num_rows
            log(f"=== WGI DONE: {n:,} obs ===")
            return
        log("  No obs from this URL")

    log("All WGI URLs failed")


if __name__ == "__main__":
    main()
