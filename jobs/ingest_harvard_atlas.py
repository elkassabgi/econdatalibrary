#!/usr/bin/env python3
"""Harvard Growth Lab – Atlas of Economic Complexity ingest.

Downloads from Harvard Dataverse:
  1. Growth Projections and Complexity Rankings (ECI, growth forecasts)
     doi:10.7910/DVN/XTAQMC
  2. International Trade Data – Services (country-year services trade)
     doi:10.7910/DVN/NDDMSN
  3. International Trade Data – HS12 (country-product and country-year)
     doi:10.7910/DVN/YAVJDF

Converts to long-format parquet: {series_key, obs_date, value}.
Run: python jobs/ingest_harvard_atlas.py
"""
from __future__ import annotations
import csv
import datetime as dt
import io
import os
import sys
import time

import requests
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "harvard_atlas")
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
DV   = "https://dataverse.harvard.edu/api/access/datafile"  # /access/datafile/{id}
RATE = 0.3

# File IDs from Harvard Dataverse API:
# doi:10.7910/DVN/XTAQMC  — ECI / growth projections
ECI_ID  = 13439575   # growth_proj_eci_rankings.csv  (~200 KB)

# doi:10.7910/DVN/NDDMSN  — Services trade
SVC_CY_ID  = 13685142  # services_unilateral_country_year.csv (~280 KB)
SVC_CP1_ID = 13685141  # services_unilateral_country_product_year_1.csv (~470 KB)
SVC_CP2_ID = 13685137  # services_unilateral_country_product_year_2.csv (~2.2 MB)
SVC_CP4_ID = 13685139  # services_unilateral_country_product_year_4.csv (~2.2 MB)
SVC_CP6_ID = 13685135  # services_unilateral_country_product_year_6.csv (~2.3 MB)

# doi:10.7910/DVN/YAVJDF  — HS12 goods trade
HS12_CY_ID  = 13685174  # hs12_country_country_year.csv (~14.5 MB) bilateral total
HS12_CP1_ID = 13685182  # hs12_country_product_year_1.csv (~1.8 MB) country-product HS1
HS12_CP2_ID = 13685186  # hs12_country_product_year_2.csv (~17 MB) country-product HS2


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def fetch(file_id: int, retries: int = 4) -> bytes | None:
    url = f"{DV}/{file_id}"
    for attempt in range(retries):
        try:
            log(f"  GET datafile/{file_id}")
            r = requests.get(url, headers=UA, timeout=180)
            if r.status_code == 200:
                return r.content
            if r.status_code == 404:
                log(f"  404"); return None
            log(f"  HTTP {r.status_code} attempt {attempt+1}")
        except Exception as e:
            log(f"  ERR attempt {attempt+1}: {e}")
        time.sleep(10 * (attempt + 1))
    return None


def read_csv(data: bytes) -> tuple[list[str], list[list[str]]]:
    text = data.decode("utf-8-sig", errors="replace")
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        return [], []
    return rows[0], rows[1:]


def save(keys, dates, vals, out_path):
    tbl = pa.table({
        "series_key": pa.array(keys,  pa.string()),
        "obs_date":   pa.array(dates, pa.date32()),
        "value":      pa.array(vals,  pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"  -> {n:,} obs saved to {os.path.basename(out_path)}")
    return n


def col_idx(headers: list[str], candidates: list[str]) -> int | None:
    lh = [h.lower().strip() for h in headers]
    for c in candidates:
        if c.lower() in lh:
            return lh.index(c.lower())
    return None


# ---------------------------------------------------------------------------

def ingest_eci():
    """ECI rankings and growth projections."""
    out = os.path.join(OUT, "eci_rankings.parquet")
    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows; log(f"ECI: already {n:,} rows"); return n
    data = fetch(ECI_ID)
    if not data: return 0

    headers, rows = read_csv(data)
    log(f"  ECI headers: {headers[:12]}")
    # Expected: country, country_id, year, eci, rank, projected_growth, ...
    year_i   = col_idx(headers, ["year"])
    ctry_i   = col_idx(headers, ["country", "country_id", "iso3"])
    if year_i is None:
        log("  ECI: no year column"); return 0

    skip = {"year", "country", "country_id", "iso3", "region", "income_group", ""}
    num_idx = [(h.strip(), i) for i, h in enumerate(headers)
               if h.lower().strip() not in skip and i != ctry_i]

    keys, dates, vals = [], [], []
    for row in rows:
        try:
            yr = int(float(row[year_i]))
            obs_d = dt.date(yr, 12, 31)
        except (ValueError, TypeError, IndexError):
            continue
        ctry = row[ctry_i].strip() if ctry_i is not None and ctry_i < len(row) else ""
        for col, ci in num_idx:
            if ci >= len(row): continue
            raw = row[ci].strip()
            if raw in ("", ".", "NA", "N/A", "#N/A"): continue
            try:
                v = float(raw)
                key = f"ATLAS:ECI:{col}:{ctry}" if ctry else f"ATLAS:ECI:{col}"
                keys.append(key); dates.append(obs_d); vals.append(v)
            except (ValueError, TypeError):
                pass
    return save(keys, dates, vals, out)


def ingest_services_cy():
    """Services trade — country-year level."""
    out = os.path.join(OUT, "services_country_year.parquet")
    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows; log(f"SVC CY: already {n:,} rows"); return n
    data = fetch(SVC_CY_ID)
    if not data: return 0

    headers, rows = read_csv(data)
    log(f"  SVC_CY headers: {headers[:12]}")
    year_i  = col_idx(headers, ["year"])
    ctry_i  = col_idx(headers, ["country_id", "country", "iso3"])
    skip = {"year", "country_id", "country", "iso3", "country_name", ""}
    num_idx = [(h.strip(), i) for i, h in enumerate(headers)
               if h.lower().strip() not in skip and i not in [year_i, ctry_i]]

    keys, dates, vals = [], [], []
    for row in rows:
        try:
            yr = int(float(row[year_i]))
            obs_d = dt.date(yr, 12, 31)
        except (ValueError, TypeError, IndexError):
            continue
        ctry = row[ctry_i].strip() if ctry_i is not None and ctry_i < len(row) else ""
        for col, ci in num_idx:
            if ci >= len(row): continue
            raw = row[ci].strip()
            if raw in ("", ".", "NA", "N/A", "#N/A"): continue
            try:
                v = float(raw)
                key = f"ATLAS:SVC:{col}:{ctry}" if ctry else f"ATLAS:SVC:{col}"
                keys.append(key); dates.append(obs_d); vals.append(v)
            except (ValueError, TypeError):
                pass
    return save(keys, dates, vals, out)


def ingest_services_cp(file_id: int, label: str):
    """Services trade — country-product-year level."""
    out = os.path.join(OUT, f"services_cp_{label}.parquet")
    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows; log(f"SVC CP {label}: already {n:,} rows"); return n
    data = fetch(file_id)
    if not data: return 0

    headers, rows = read_csv(data)
    year_i    = col_idx(headers, ["year"])
    ctry_i    = col_idx(headers, ["country_id", "country", "iso3"])
    product_i = col_idx(headers, ["service_id", "product_id", "product", "sitc"])
    if year_i is None:
        log(f"  SVC CP {label}: no year column"); return 0

    skip = {"year", "country_id", "country", "iso3", "country_name",
            "service_id", "product_id", "product", "sitc", ""}
    num_idx = [(h.strip(), i) for i, h in enumerate(headers)
               if h.lower().strip() not in skip and i not in [year_i, ctry_i, product_i]]

    keys, dates, vals = [], [], []
    for row in rows:
        try:
            yr = int(float(row[year_i]))
            obs_d = dt.date(yr, 12, 31)
        except (ValueError, TypeError, IndexError):
            continue
        ctry = row[ctry_i].strip() if ctry_i is not None and ctry_i < len(row) else ""
        prod = row[product_i].strip() if product_i is not None and product_i < len(row) else ""
        for col, ci in num_idx:
            if ci >= len(row): continue
            raw = row[ci].strip()
            if raw in ("", ".", "NA", "N/A", "#N/A"): continue
            try:
                v = float(raw)
                parts = [x for x in [col, ctry, prod] if x]
                key = "ATLAS:SVC:" + ":".join(parts)
                keys.append(key); dates.append(obs_d); vals.append(v)
            except (ValueError, TypeError):
                pass
    return save(keys, dates, vals, out)


def ingest_hs12_cy():
    """HS12 goods trade — country-country-year bilateral totals (14.5 MB)."""
    out = os.path.join(OUT, "hs12_country_year.parquet")
    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows; log(f"HS12 CY: already {n:,} rows"); return n
    data = fetch(HS12_CY_ID)
    if not data: return 0

    headers, rows = read_csv(data)
    log(f"  HS12_CY headers: {headers[:12]}")
    # Probably: year, exporter, importer, export_val, import_val
    year_i    = col_idx(headers, ["year"])
    exp_i     = col_idx(headers, ["exporter", "exporter_id", "exp_country"])
    imp_i     = col_idx(headers, ["importer", "importer_id", "imp_country"])
    if year_i is None:
        log("  HS12_CY: no year col"); return 0

    skip = {"year", "exporter", "exporter_id", "exp_country", "importer", "importer_id",
            "imp_country", "exporter_name", "importer_name", ""}
    num_idx = [(h.strip(), i) for i, h in enumerate(headers)
               if h.lower().strip() not in skip and i not in [year_i, exp_i, imp_i]]

    keys, dates, vals = [], [], []
    for row in rows:
        try:
            yr = int(float(row[year_i]))
            obs_d = dt.date(yr, 12, 31)
        except (ValueError, TypeError, IndexError):
            continue
        exp = row[exp_i].strip() if exp_i is not None and exp_i < len(row) else ""
        imp = row[imp_i].strip() if imp_i is not None and imp_i < len(row) else ""
        for col, ci in num_idx:
            if ci >= len(row): continue
            raw = row[ci].strip()
            if raw in ("", ".", "NA", "N/A", "#N/A"): continue
            try:
                v = float(raw)
                parts = [x for x in [col, exp, imp] if x]
                key = "ATLAS:HS12:" + ":".join(parts)
                keys.append(key); dates.append(obs_d); vals.append(v)
            except (ValueError, TypeError):
                pass
    return save(keys, dates, vals, out)


def ingest_hs12_cp(file_id: int, label: str):
    """HS12 goods trade — country-product-year."""
    out = os.path.join(OUT, f"hs12_cp_{label}.parquet")
    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows; log(f"HS12 CP {label}: already {n:,} rows"); return n
    data = fetch(file_id)
    if not data: return 0

    headers, rows = read_csv(data)
    log(f"  HS12_CP {label} headers: {headers[:12]}")
    year_i    = col_idx(headers, ["year"])
    ctry_i    = col_idx(headers, ["country_id", "country", "iso3"])
    product_i = col_idx(headers, ["hs_product_code", "product_id", "hs"])
    if year_i is None:
        log(f"  HS12_CP {label}: no year col"); return 0

    skip = {"year", "country_id", "country", "iso3", "country_name",
            "hs_product_code", "product_id", "hs", "hs_product_name", ""}
    num_idx = [(h.strip(), i) for i, h in enumerate(headers)
               if h.lower().strip() not in skip and i not in [year_i, ctry_i, product_i]]

    keys, dates, vals = [], [], []
    for row in rows:
        try:
            yr = int(float(row[year_i]))
            obs_d = dt.date(yr, 12, 31)
        except (ValueError, TypeError, IndexError):
            continue
        ctry = row[ctry_i].strip() if ctry_i is not None and ctry_i < len(row) else ""
        prod = row[product_i].strip() if product_i is not None and product_i < len(row) else ""
        for col, ci in num_idx:
            if ci >= len(row): continue
            raw = row[ci].strip()
            if raw in ("", ".", "NA", "N/A", "#N/A"): continue
            try:
                v = float(raw)
                parts = [x for x in [col, ctry, prod] if x]
                key = "ATLAS:HS12:" + ":".join(parts)
                keys.append(key); dates.append(obs_d); vals.append(v)
            except (ValueError, TypeError):
                pass
    return save(keys, dates, vals, out)


def main():
    os.makedirs(OUT, exist_ok=True)
    log("=== Harvard Growth Lab Atlas Ingest ===")
    total = 0
    jobs = [
        (ingest_eci,        "ECI Rankings"),
        (ingest_services_cy,"Services Country-Year"),
        (lambda: ingest_services_cp(SVC_CP1_ID, "1"), "Services Country-Product HS1"),
        (lambda: ingest_services_cp(SVC_CP2_ID, "2"), "Services Country-Product HS2"),
        (lambda: ingest_services_cp(SVC_CP4_ID, "4"), "Services Country-Product HS4"),
        (lambda: ingest_services_cp(SVC_CP6_ID, "6"), "Services Country-Product HS6"),
        (ingest_hs12_cy,    "HS12 Country-Year Bilateral"),
        (lambda: ingest_hs12_cp(HS12_CP1_ID, "hs1"), "HS12 Country-Product HS1"),
        (lambda: ingest_hs12_cp(HS12_CP2_ID, "hs2"), "HS12 Country-Product HS2"),
    ]
    for fn, name in jobs:
        log(f"--- {name} ---")
        try:
            n = fn()
            total += n
            time.sleep(RATE)
        except Exception as e:
            log(f"  ERROR: {e}")
            import traceback; traceback.print_exc()
    log(f"=== GRAND TOTAL: {total:,} Atlas observations ===")


if __name__ == "__main__":
    main()
