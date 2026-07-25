#!/usr/bin/env python3
"""Global Peace Index (GPI) ingest.

Source: https://www.visionofhumanity.org/resources/global-peace-index/
Publisher: Institute for Economics and Peace (IEP)
License: Free for research/non-commercial use
Coverage: 163 countries, 2008-2024, overall peace score + 23 indicators

series_key: GPI:{indicator}:{iso3}   e.g. GPI:Overall_Score:ISL

Output: data/clean_full/gpi/gpi.parquet
Run: python jobs/ingest_gpi.py
"""
from __future__ import annotations
import datetime as dt, io, os, re, time
import requests, openpyxl
import pyarrow as pa, pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT  = os.path.join(ROOT, "data", "clean_full", "gpi")
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

# IEP publishes annual GPI reports with downloadable Excel
# URL patterns to try (names change each year)
GPI_URLS = [
    # 2024 edition
    "https://www.visionofhumanity.org/wp-content/uploads/2024/06/GPI-2024-web.xlsx",
    "https://www.visionofhumanity.org/wp-content/uploads/2024/07/GPI-2024-full-report-data.xlsx",
    "https://www.visionofhumanity.org/wp-content/uploads/2024/06/GPI-2024-download.xlsx",
    # 2023 edition
    "https://www.visionofhumanity.org/wp-content/uploads/2023/06/GPI-2023-web.xlsx",
    "https://www.visionofhumanity.org/wp-content/uploads/2023/06/GPI-2023-Results-Overall-Scores-and-Domains.xlsx",
    # 2022
    "https://www.visionofhumanity.org/wp-content/uploads/2022/06/GPI-2022-web.xlsx",
    # GitHub mirrors (if IEP blocks direct download)
    "https://raw.githubusercontent.com/datasets/global-peace-index/master/data/global-peace-index.csv",
    # OWID GPI data
    "https://ourworldindata.org/grapher/global-peace-index.csv",
]


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def fetch(url: str) -> bytes | None:
    for attempt in range(2):
        try:
            r = requests.get(url, headers=UA, timeout=60, allow_redirects=True)
            if r.status_code == 200 and len(r.content) > 1000:
                log(f"  {len(r.content)//1024:,} KB from {url[-70:]}")
                return r.content
            log(f"  HTTP {r.status_code}: {url[-60:]}")
            if r.status_code in (403, 404):
                return None
        except Exception as e:
            log(f"  ERR: {e}")
        time.sleep(2)
    return None


def parse_owid_csv(data: bytes) -> tuple[list, list, list]:
    """Parse OWID/GitHub CSV with columns: Entity, Code, Year, ..."""
    import csv
    text = data.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    headers = [h.strip() for h in (reader.fieldnames or [])]
    log(f"  CSV columns: {headers[:10]}")

    entity_col = next((h for h in headers if h.lower() in ("entity", "country", "name")), None)
    code_col   = next((h for h in headers if h.lower() in ("code", "iso3", "iso")), None)
    year_col   = next((h for h in headers if h.lower() in ("year",)), None)
    id_col     = code_col or entity_col

    if not id_col or not year_col:
        return [], [], []

    val_cols = [h for h in headers if h not in (entity_col, code_col, year_col) and h]

    keys, dates, vals = [], [], []
    import csv as csv_mod
    for rec in reader:
        cid = (rec.get(id_col) or "").strip()
        if not cid:
            continue
        try:
            yr = int(float((rec.get(year_col) or "").strip()))
            obs_d = dt.date(yr, 12, 31)
        except (TypeError, ValueError):
            continue
        for col in val_cols:
            raw = rec.get(col, "")
            if not raw or str(raw).strip() in ("", "NA"):
                continue
            try:
                v = float(str(raw).strip())
                if v != v:
                    continue
                safe = re.sub(r"[^a-zA-Z0-9_]", "_", col)[:30]
                keys.append(f"GPI:{safe}:{cid}")
                dates.append(obs_d)
                vals.append(v)
            except (TypeError, ValueError):
                pass
    return keys, dates, vals


def parse_gpi_xlsx(data: bytes) -> tuple[list, list, list]:
    """Parse IEP GPI Excel file."""
    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    log(f"  Sheets: {wb.sheetnames}")
    keys, dates, vals = [], [], []

    for sheet_name in wb.sheetnames:
        if any(s in sheet_name.lower() for s in ["cover", "about", "note", "method", "source"]):
            continue
        ws = wb[sheet_name]
        rows = list(ws.iter_rows(values_only=True))
        if len(rows) < 5:
            continue

        # Find header row
        header_idx = None
        for ri, row in enumerate(rows[:10]):
            row_lower = [str(c).strip().lower() if c else "" for c in row]
            if "country" in row_lower or "iso" in row_lower or "code" in row_lower:
                header_idx = ri; break

        if header_idx is None:
            continue

        header = [str(c).strip() if c else "" for c in rows[header_idx]]
        log(f"  Sheet '{sheet_name}': header at row {header_idx}: {header[:8]}")

        # Find country and year columns
        ctry_ci = next((i for i, h in enumerate(header) if h.lower() in ("country", "nation", "name")), None)
        iso_ci  = next((i for i, h in enumerate(header) if h.lower() in ("iso", "iso3", "code", "iso_code")), None)
        year_ci = next((i for i, h in enumerate(header) if h.lower() in ("year",)), None)
        id_ci   = iso_ci if iso_ci is not None else ctry_ci

        if id_ci is None:
            continue

        # Value columns
        skip_ci = {id_ci, ctry_ci, iso_ci, year_ci, None}
        val_cols = [(i, h) for i, h in enumerate(header) if i not in skip_ci and h]

        if not val_cols:
            continue

        # Determine year from sheet name if no year column
        sheet_yr = None
        if year_ci is None:
            m = re.search(r"\b(20\d{2})\b", sheet_name)
            if m:
                sheet_yr = int(m.group(0))

        n_before = len(vals)
        for row in rows[header_idx + 1:]:
            if not row or row[id_ci] is None:
                continue
            cid = str(row[id_ci]).strip()
            if not cid or cid.lower() in ("nan", "none"):
                continue

            if year_ci is not None and row[year_ci] is not None:
                try:
                    yr = int(float(str(row[year_ci]).strip()))
                    obs_d = dt.date(yr, 12, 31)
                except (TypeError, ValueError):
                    continue
            elif sheet_yr:
                obs_d = dt.date(sheet_yr, 12, 31)
            else:
                continue

            for col_i, col_name in val_cols:
                if col_i >= len(row) or row[col_i] is None:
                    continue
                try:
                    v = float(row[col_i])
                    if v != v:
                        continue
                    safe = re.sub(r"[^a-zA-Z0-9_]", "_", col_name)[:30]
                    keys.append(f"GPI:{safe}:{cid}")
                    dates.append(obs_d)
                    vals.append(v)
                except (TypeError, ValueError):
                    pass

        n_new = len(vals) - n_before
        log(f"  Sheet '{sheet_name}': {n_new:,} obs")
        if n_new > 0:
            break

    return keys, dates, vals


def main():
    os.makedirs(OUT, exist_ok=True)
    out = os.path.join(OUT, "gpi.parquet")

    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows
        log(f"GPI: already {n:,} rows"); return

    log("=== Global Peace Index Ingest ===")

    for url in GPI_URLS:
        log(f"Trying {url[-70:]}...")
        data = fetch(url)
        if not data:
            continue

        if url.endswith(".csv"):
            k, d, v = parse_owid_csv(data)
        else:
            k, d, v = parse_gpi_xlsx(data)

        if v:
            tbl = pa.table({
                "series_key": pa.array(k,  pa.string()),
                "obs_date":   pa.array(d, pa.date32()),
                "value":      pa.array(v,  pa.float64()),
            })
            pq.write_table(tbl, out, compression="zstd")
            n = pq.read_metadata(out).num_rows
            log(f"=== GPI DONE: {n:,} obs ===")
            return
        log("  No obs from this URL")

    log("All GPI URLs failed")


if __name__ == "__main__":
    main()
