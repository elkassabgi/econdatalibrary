#!/usr/bin/env python3
"""KOF Globalisation Index ingest.

Source: https://kof.ethz.ch/en/forecasts-and-indicators/indicators/kof-globalisation-index.html
Authors: Gygli, Haelg, Potrafke & Sturm (ETH Zurich)
License: Free for academic use
Coverage: 203 countries, 1970-2022, 24 globalisation sub-indices

Indices:
  KOF Globalisation Index (overall)
  KOF Economic Globalisation (trade, finance)
  KOF Social Globalisation (personal contact, info flows, cultural proximity)
  KOF Political Globalisation (embassies, treaties, IGO memberships)
  KOF De Facto vs De Jure sub-indices

series_key: KOF:{index}:{iso3}   e.g. KOF:KOFGI:CHE

Output: data/clean_full/kof_globalization/kof_globalization.parquet
Run: python jobs/ingest_kof_globalization.py
"""
from __future__ import annotations
import csv, datetime as dt, io, os, time
import requests, openpyxl
import pyarrow as pa, pyarrow.parquet as pq

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "kof_globalization")
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

# KOF Zurich provides direct download of the full dataset
KOF_URLS = [
    # 2025 edition (203 countries, 1970-2022) — correct path uses /dual/kof-dam/
    "https://ethz.ch/content/dam/ethz/special-interest/dual/kof-dam/documents/Globalization/2025/KOFGI_2025_public.xlsx",
    # KOF time series API (XLSX format)
    "https://datenservice.kof.ethz.ch/api/v1/public/collections/globidx_v2020?mime=xlsx",
    # 2024 fallback
    "https://ethz.ch/content/dam/ethz/special-interest/dual/kof-dam/documents/Globalization/2024/KOFGI_2024_public.xlsx",
]


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def fetch(url: str) -> bytes | None:
    for attempt in range(2):
        try:
            r = requests.get(url, headers=UA, timeout=60, allow_redirects=True)
            if r.status_code == 200 and len(r.content) > 5000:
                log(f"  {len(r.content)//1024:,} KB from {url[-70:]}")
                return r.content
            if r.status_code in (403, 404):
                return None
            log(f"  HTTP {r.status_code}: {url[-60:]}")
        except Exception as e:
            log(f"  ERR attempt {attempt+1}: {e}")
        time.sleep(3)
    return None


def parse_kof_xlsx(data: bytes) -> tuple[list, list, list]:
    """Parse KOF XLSX. Format: rows=country-year, cols=index components."""
    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    log(f"  Sheets: {wb.sheetnames}")
    keys, dates, vals = [], [], []

    for sheet_name in wb.sheetnames:
        if any(s in sheet_name.lower() for s in ["readme", "note", "info", "about", "source"]):
            continue
        ws = wb[sheet_name]
        rows = list(ws.iter_rows(values_only=True))
        if len(rows) < 5:
            continue

        # Find header row — look for 'code' or 'year' or 'country'
        header_idx = None
        for ri, row in enumerate(rows[:10]):
            row_lower = [str(c).strip().lower() if c else "" for c in row]
            if any(v in row_lower for v in ["code", "country", "iso", "year"]):
                header_idx = ri; break

        if header_idx is None:
            continue

        header = [str(c).strip().lower() if c else "" for c in rows[header_idx]]
        log(f"  Sheet '{sheet_name}': header at row {header_idx}, cols: {header[:10]}")

        # Identify key columns
        iso_ci   = next((i for i, h in enumerate(header) if h in ("code", "iso3", "iso", "iso3c", "country_code")), None)
        ctry_ci  = next((i for i, h in enumerate(header) if h in ("country", "name", "country_name")), None)
        year_ci  = next((i for i, h in enumerate(header) if h in ("year",)), None)

        id_ci = iso_ci if iso_ci is not None else ctry_ci
        if id_ci is None or year_ci is None:
            log(f"  No id/year col. Skipping.")
            continue

        # Value columns: anything numeric-looking, not the id/year columns
        skip_ci = {iso_ci, ctry_ci, year_ci, None}
        # Detect numeric columns by checking first few data rows
        val_col_names = []
        for i, h in enumerate(header):
            if i in skip_ci or not h:
                continue
            # KOF column names are like KOFGI, KOFEC, KOFSO, KOFPO, etc.
            # or longer descriptive names
            val_col_names.append((i, h))

        if not val_col_names:
            continue

        n_before = len(vals)
        for row in rows[header_idx + 1:]:
            if not row or row[id_ci] is None or row[year_ci] is None:
                continue
            try:
                yr = int(float(str(row[year_ci]).strip()))
                obs_d = dt.date(yr, 12, 31)
            except (TypeError, ValueError):
                continue

            id_val = str(row[id_ci]).strip()
            if not id_val or id_val.lower() in ("nan", "none", ""):
                continue

            for col_i, col_name in val_col_names:
                if col_i >= len(row) or row[col_i] is None:
                    continue
                try:
                    v = float(row[col_i])
                    if v != v:
                        continue
                    safe_col = col_name.replace(" ", "_")[:20].upper()
                    keys.append(f"KOF:{safe_col}:{id_val.upper()}")
                    dates.append(obs_d)
                    vals.append(v)
                except (TypeError, ValueError):
                    pass

        n_new = len(vals) - n_before
        log(f"  Sheet '{sheet_name}': {n_new:,} obs")
        if n_new > 0:
            break  # use first successful sheet

    return keys, dates, vals


def main():
    os.makedirs(OUT, exist_ok=True)
    out = os.path.join(OUT, "kof_globalization.parquet")

    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows
        log(f"KOF: already {n:,} rows"); return

    log("=== KOF Globalisation Index Ingest ===")

    for url in KOF_URLS:
        log(f"Trying {url[-70:]}...")
        data = fetch(url)
        if not data:
            continue
        k, d, v = parse_kof_xlsx(data)
        if v:
            tbl = pa.table({
                "series_key": pa.array(k,  pa.string()),
                "obs_date":   pa.array(d, pa.date32()),
                "value":      pa.array(v,  pa.float64()),
            })
            pq.write_table(tbl, out, compression="zstd")
            n = pq.read_metadata(out).num_rows
            log(f"=== KOF DONE: {n:,} obs ===")
            return
        log("  No obs from this URL")
        time.sleep(2)

    log("All KOF URLs failed")


if __name__ == "__main__":
    main()
