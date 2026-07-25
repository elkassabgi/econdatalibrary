#!/usr/bin/env python3
"""Economic Policy Uncertainty (EPU) Index ingest.

Source: https://www.policyuncertainty.com/
Authors: Baker, Bloom & Davis
License: Free for academic use
Coverage: 25+ countries, monthly news-based economic policy uncertainty indices,
          some going back to 1985 or earlier.

Downloads country-level EPU indices from policyuncertainty.com.
Also includes global EPU aggregates and equity-market-specific VIX variants.

series_key: EPU:{country_code}   e.g. EPU:USA

Output: data/clean_full/epu/epu.parquet
Run: python jobs/ingest_epu.py
"""
from __future__ import annotations
import csv, datetime as dt, io, os, re, time
import requests, openpyxl
import pyarrow as pa, pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT  = os.path.join(ROOT, "data", "clean_full", "epu")
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

BASE = "https://www.policyuncertainty.com"

# (series_label, url, country_code_for_key)
EPU_SOURCES = [
    # Global
    ("EPU_global", f"{BASE}/media/All_Country_Data.xlsx", "GLOBAL"),
    # US (longest history)
    ("EPU_US",     f"{BASE}/media/US_Historical_EPU_Data.xlsx", "USA"),
    # Europe
    ("EPU_UK",     f"{BASE}/media/UK_Policy_Uncertainty_Data.xlsx", "GBR"),
    ("EPU_EUR",    f"{BASE}/media/Western_Europe_Policy_Uncertainty_Data.xlsx", "EUR"),
    # Asia
    ("EPU_CHN",    f"{BASE}/media/China_Policy_Uncertainty_Data.xlsx", "CHN"),
    ("EPU_JPN",    f"{BASE}/media/Japan_Policy_Uncertainty_Data.xlsx", "JPN"),
    ("EPU_KOR",    f"{BASE}/media/Korea_Policy_Uncertainty_Data.xlsx", "KOR"),
    ("EPU_IND",    f"{BASE}/media/India_Policy_Uncertainty_Data.xlsx", "IND"),
    # Americas
    ("EPU_BRA",    f"{BASE}/media/Brazil_Policy_Uncertainty_Data.xlsx", "BRA"),
    ("EPU_MEX",    f"{BASE}/media/Mexico_Policy_Uncertainty_Data.xlsx", "MEX"),
    ("EPU_CAN",    f"{BASE}/media/Canada_Policy_Uncertainty_Data.xlsx", "CAN"),
    # Europe (more)
    ("EPU_DEU",    f"{BASE}/media/Germany_Policy_Uncertainty_Data.xlsx", "DEU"),
    ("EPU_FRA",    f"{BASE}/media/France_Policy_Uncertainty_Data.xlsx", "FRA"),
    ("EPU_ITA",    f"{BASE}/media/Italy_Policy_Uncertainty_Data.xlsx", "ITA"),
    ("EPU_ESP",    f"{BASE}/media/Spain_Policy_Uncertainty_Data.xlsx", "ESP"),
    ("EPU_RUS",    f"{BASE}/media/Russia_Policy_Uncertainty_Data.xlsx", "RUS"),
    # Other major economies
    ("EPU_AUS",    f"{BASE}/media/Australia_Policy_Uncertainty_Data.xlsx", "AUS"),
    ("EPU_ZAF",    f"{BASE}/media/South_Africa_Policy_Uncertainty_Data.xlsx", "ZAF"),
    ("EPU_SWE",    f"{BASE}/media/Sweden_Policy_Uncertainty_Data.xlsx", "SWE"),
    ("EPU_NLD",    f"{BASE}/media/Netherlands_Policy_Uncertainty_Data.xlsx", "NLD"),
    ("EPU_CHE",    f"{BASE}/media/Switzerland_Policy_Uncertainty_Data.xlsx", "CHE"),
    # Subindex / thematic
    ("EPU_EMERG",  f"{BASE}/media/Emerging_Markets_Policy_Uncertainty_Data.xlsx", "EMERG"),
    ("EPU_G7",     f"{BASE}/media/Global_EPU_Data.xlsx", "G7"),
]


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def fetch(url: str) -> bytes | None:
    for attempt in range(3):
        try:
            r = requests.get(url, headers=UA, timeout=60, allow_redirects=True)
            if r.status_code == 200 and len(r.content) > 1000:
                return r.content
            if r.status_code in (403, 404):
                return None
            log(f"  HTTP {r.status_code}: {url[-60:]}")
        except Exception as e:
            log(f"  ERR attempt {attempt+1}: {e}")
        time.sleep(3 * (attempt + 1))
    return None


def parse_epu_xlsx(data: bytes, code: str) -> tuple[list, list, list]:
    """Parse EPU XLSX. Format varies but usually has Year, Month, EPU columns."""
    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    keys, dates, vals = [], [], []

    for sheet_name in wb.sheetnames:
        if any(s in sheet_name.lower() for s in ["readme", "note", "info", "source"]):
            continue
        ws = wb[sheet_name]
        rows = list(ws.iter_rows(values_only=True))
        if len(rows) < 3:
            continue

        # Find header row — look for Year/Month columns
        header_idx = None
        for ri, row in enumerate(rows[:10]):
            row_lower = [str(c).strip().lower() if c else "" for c in row]
            if "year" in row_lower or "month" in row_lower:
                header_idx = ri
                break

        if header_idx is None:
            # Try first row as header
            header_idx = 0

        header = [str(c).strip().lower() if c else "" for c in rows[header_idx]]
        year_ci  = next((i for i, h in enumerate(header) if h in ("year",)), None)
        month_ci = next((i for i, h in enumerate(header) if h in ("month",)), None)

        # EPU value columns — anything numeric that's not year/month
        skip_ci = {year_ci, month_ci, None}
        val_ci_list = []
        for i, h in enumerate(header):
            if i in skip_ci or not h:
                continue
            val_ci_list.append((i, h))

        if year_ci is None or not val_ci_list:
            continue

        n_before = len(vals)
        for row in rows[header_idx + 1:]:
            if not row or row[year_ci] is None:
                continue
            try:
                yr = int(row[year_ci])
                mo = int(row[month_ci]) if month_ci is not None and row[month_ci] else 12
                obs_d = dt.date(yr, max(1, min(12, mo)), 1)
            except (TypeError, ValueError):
                continue

            for col_i, col_name in val_ci_list:
                if col_i >= len(row) or row[col_i] is None:
                    continue
                try:
                    v = float(row[col_i])
                    if v != v or v <= 0:
                        continue
                    # Build clean series key
                    clean_name = re.sub(r"[^a-zA-Z0-9_]", "_", col_name)[:30]
                    keys.append(f"EPU:{clean_name}:{code}")
                    dates.append(obs_d)
                    vals.append(v)
                except (TypeError, ValueError):
                    pass

        if len(vals) > n_before:
            break  # Use first successful sheet

    return keys, dates, vals


def parse_all_country_xlsx(data: bytes) -> tuple[list, list, list]:
    """Parse the All_Country_Data.xlsx which has multiple country columns."""
    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    keys, dates, vals = [], [], []

    for sheet_name in wb.sheetnames:
        if any(s in sheet_name.lower() for s in ["readme", "note", "info"]):
            continue
        ws = wb[sheet_name]
        rows = list(ws.iter_rows(values_only=True))
        if len(rows) < 3:
            continue

        # Find header
        header_idx = 0
        for ri, row in enumerate(rows[:10]):
            row_lower = [str(c).strip().lower() if c else "" for c in row]
            if "year" in row_lower:
                header_idx = ri; break

        header = [str(c).strip() if c else "" for c in rows[header_idx]]
        year_ci  = next((i for i, h in enumerate(header) if h.lower() == "year"), None)
        month_ci = next((i for i, h in enumerate(header) if h.lower() == "month"), None)
        if year_ci is None:
            continue

        # Country-named columns
        val_cols = [(i, h) for i, h in enumerate(header)
                    if i not in {year_ci, month_ci, None} and h and h.lower() not in ("year","month")]

        n_before = len(vals)
        for row in rows[header_idx + 1:]:
            if not row or row[year_ci] is None:
                continue
            try:
                yr = int(row[year_ci])
                mo = int(row[month_ci]) if month_ci is not None and row[month_ci] else 12
                obs_d = dt.date(yr, max(1, min(12, mo)), 1)
            except (TypeError, ValueError):
                continue

            for col_i, ctry_name in val_cols:
                if col_i >= len(row) or row[col_i] is None:
                    continue
                try:
                    v = float(row[col_i])
                    if v != v or v <= 0:
                        continue
                    clean = re.sub(r"[^a-zA-Z0-9_]", "_", ctry_name)[:20]
                    keys.append(f"EPU:epu:{clean}")
                    dates.append(obs_d)
                    vals.append(v)
                except (TypeError, ValueError):
                    pass

        if len(vals) > n_before:
            break

    return keys, dates, vals


def main():
    os.makedirs(OUT, exist_ok=True)
    out = os.path.join(OUT, "epu.parquet")

    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows
        log(f"EPU: already {n:,} rows"); return

    log("=== Economic Policy Uncertainty (EPU) Ingest ===")
    all_keys, all_dates, all_vals = [], [], []
    seen = set()  # deduplicate (country/global file may overlap)

    for label, url, code in EPU_SOURCES:
        log(f"  {label} ({code})...")
        data = fetch(url)
        if not data:
            log(f"    -> not found")
            continue

        if label == "EPU_global":
            k, d, v = parse_all_country_xlsx(data)
        else:
            k, d, v = parse_epu_xlsx(data, code)

        new = 0
        for ki, di, vi in zip(k, d, v):
            token = (ki, di)
            if token not in seen:
                seen.add(token)
                all_keys.append(ki); all_dates.append(di); all_vals.append(vi)
                new += 1
        log(f"    -> {new:,} obs (total: {len(all_vals):,})")
        time.sleep(0.5)

    if not all_vals:
        log("0 observations — all EPU URLs failed"); return

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out, compression="zstd")
    n = pq.read_metadata(out).num_rows
    log(f"=== EPU DONE: {n:,} obs ===")


if __name__ == "__main__":
    main()
