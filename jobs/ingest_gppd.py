#!/usr/bin/env python3
"""Global Power Plant Database (GPPD) ingest.

Source: https://datasets.wri.org/dataset/globalpowerplantdatabase
License: CC BY 4.0 (World Resources Institute)
Coverage: ~35,000 power plants, ~165 countries, capacity + generation data.

Downloads GPPD CSV from WRI/GitHub and converts to long format.
Each plant × variable × year = one observation.
series_key: GPPD:{variable}:{gppd_idnr}  e.g. GPPD:capacity_mw:WRI1000001

Output: data/clean_full/gppd/gppd.parquet
Run: python jobs/ingest_gppd.py
"""
from __future__ import annotations
import csv, datetime as dt, io, os, time, zipfile
import requests, pyarrow as pa, pyarrow.parquet as pq

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "gppd")
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

URLS = [
    "https://datasets.wri.org/dataset/540dcf46-f287-47ac-985d-269b04bea4c6/resource/c240ed2e-1190-4d7e-b1da-c66b72e08858/download/globalpowerplantdatabasev130.zip",
    "https://github.com/wri/global-power-plant-database/raw/master/output_database/global_power_plant_database.csv",
    "https://raw.githubusercontent.com/wri/global-power-plant-database/master/output_database/global_power_plant_database.csv",
]

# Numeric columns in GPPD CSV (besides ID and string fields)
STATIC_COLS = ["capacity_mw"]
# Generation columns per year
GEN_PATTERN = "generation_gwh_"  # e.g. generation_gwh_2013 .. generation_gwh_2020
ESTIMATED_GEN = "estimated_generation_gwh"  # no year suffix in some versions


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def fetch(url):
    try:
        r = requests.get(url, headers=UA, timeout=120, allow_redirects=True)
        if r.status_code == 200 and len(r.content) > 1000:
            return r.content
        log(f"  HTTP {r.status_code}: {url[-60:]}")
    except Exception as e:
        log(f"  ERR: {e}")
    return None


def parse_gppd_csv(data: bytes):
    text = data.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    headers = reader.fieldnames or []
    log(f"  Headers: {headers[:15]}... total {len(headers)}")

    id_col = next((h for h in headers if h.lower() in ("gppd_idnr", "id", "plant_id")), None)
    if not id_col:
        log(f"  No ID column"); return [], [], []

    # Find generation-by-year columns
    gen_cols = [(h, int(h[len(GEN_PATTERN):]))
                for h in headers if h.startswith(GEN_PATTERN) and h[len(GEN_PATTERN):].isdigit()]
    log(f"  Generation columns: {[g[0] for g in gen_cols]}")

    # Estimated generation (some versions have a single year column)
    est_col = next((h for h in headers if h.lower().startswith("estimated_generation_gwh")), None)
    if est_col and not gen_cols:
        log(f"  Using estimated column: {est_col}")

    keys, dates, vals = [], [], []
    n_plants = 0

    for row in reader:
        plant_id = (row.get(id_col) or "").strip()
        if not plant_id:
            continue
        n_plants += 1

        # Static: capacity_mw (date = unknown/baseline, use 2020-12-31)
        cap = row.get("capacity_mw") or row.get("Capacity (MW)") or ""
        if cap:
            try:
                v = float(cap.replace(",", ""))
                if v > 0:
                    keys.append(f"GPPD:capacity_mw:{plant_id}")
                    dates.append(dt.date(2020, 12, 31))
                    vals.append(v)
            except (ValueError, TypeError):
                pass

        # Generation by year
        for col, yr in gen_cols:
            raw = (row.get(col) or "").strip()
            if not raw or raw in ("", "NA", "N/A"):
                continue
            try:
                v = float(raw)
                if v > 0:
                    keys.append(f"GPPD:generation_gwh:{plant_id}")
                    dates.append(dt.date(yr, 12, 31))
                    vals.append(v)
            except (ValueError, TypeError):
                pass

        # Estimated generation (single year)
        if est_col and not gen_cols:
            raw = (row.get(est_col) or "").strip()
            if raw and raw not in ("", "NA"):
                try:
                    v = float(raw)
                    if v > 0:
                        keys.append(f"GPPD:estimated_generation_gwh:{plant_id}")
                        dates.append(dt.date(2017, 12, 31))  # GPPD v1.3 uses 2017 est
                        vals.append(v)
                except (ValueError, TypeError):
                    pass

    log(f"  Parsed {n_plants:,} plants, {len(vals):,} observations")
    return keys, dates, vals


def main():
    os.makedirs(OUT, exist_ok=True)
    out = os.path.join(OUT, "gppd.parquet")
    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows
        log(f"GPPD: already {n:,} rows"); return

    log("=== Global Power Plant Database Ingest ===")
    keys, dates, vals = [], [], []

    for url in URLS:
        log(f"Trying {url[-70:]}...")
        data = fetch(url)
        if not data:
            time.sleep(2); continue

        if data[:2] == b"PK":  # ZIP
            z = zipfile.ZipFile(io.BytesIO(data))
            log(f"  ZIP: {z.namelist()}")
            csv_files = [m for m in z.namelist() if m.endswith(".csv") and "metadata" not in m.lower()]
            for cf in csv_files:
                k, d, v = parse_gppd_csv(z.read(cf))
                keys.extend(k); dates.extend(d); vals.extend(v)
        else:
            keys, dates, vals = parse_gppd_csv(data)

        if vals:
            break
        time.sleep(2)

    if not vals:
        log("0 observations"); return

    tbl = pa.table({
        "series_key": pa.array(keys,  pa.string()),
        "obs_date":   pa.array(dates, pa.date32()),
        "value":      pa.array(vals,  pa.float64()),
    })
    pq.write_table(tbl, out, compression="zstd")
    n = pq.read_metadata(out).num_rows
    log(f"=== GPPD DONE: {n:,} obs ===")


if __name__ == "__main__":
    main()
