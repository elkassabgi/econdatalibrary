#!/usr/bin/env python3
"""UCDP (Uppsala Conflict Data Program) ingest.

Source: https://ucdp.uu.se/
License: Creative Commons Attribution 4.0 (CC BY 4.0)
No API key required (direct download from UCDP website).

Coverage:
  UCDP/PRIO Armed Conflict Dataset v24.1 (Pettersson & Oberg 2020):
    * 1946-2023, conflict-year level
    * 2,686 conflict-year observations
    * Intensity level (1=minor, 2=war), cumulative intensity, type of conflict
    * Country-level aggregates: n_conflicts, max_intensity, is_war, is_intrastate

Output: data/clean_full/ucdp/acd.parquet

Series key format: UCDP:ACD:{variable}:{gwno_loc}
  e.g. UCDP:ACD:intensity_level:750 (India)

Run: python jobs/ingest_ucdp.py
"""
from __future__ import annotations
import csv
import datetime as dt
import io
import os
import time
import zipfile
from collections import defaultdict

import requests
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT  = os.path.join(ROOT, "data", "clean_full", "ucdp")
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

ACD_URL = "https://ucdp.uu.se/downloads/ucdpprio/ucdp-prio-acd-241-csv.zip"

# Numeric variables to extract (conflict-year level, by conflict_id)
CONFLICT_VARS = [
    "intensity_level",       # 1=minor, 2=war
    "cumulative_intensity",  # 0/1 - has intensity ever reached war level
    "type_of_conflict",      # 1=extrastate, 2=interstate, 3=intrastate, 4=internationalized
]


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def fetch(url: str) -> bytes | None:
    for attempt in range(3):
        try:
            r = requests.get(url, headers=UA, timeout=120)
            if r.status_code == 200 and len(r.content) > 1000:
                log(f"  {len(r.content)//1024:,} KB")
                return r.content
            log(f"  HTTP {r.status_code}")
        except Exception as e:
            log(f"  ERR attempt {attempt+1}: {e}")
        time.sleep(5 * (attempt + 1))
    return None


def parse_acd(data: bytes) -> tuple[list, list, list]:
    z = zipfile.ZipFile(io.BytesIO(data))
    csv_name = next((n for n in z.namelist() if n.endswith(".csv")), None)
    if not csv_name:
        log("  No CSV in ZIP"); return [], [], []

    raw = z.read(csv_name).decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(raw))
    headers = [h.strip().strip('"') for h in (reader.fieldnames or [])]
    log(f"  Columns ({len(headers)}): {headers[:12]}")

    keys, dates, vals = [], [], []

    # Country-year aggregates: by gwno_loc (which can be multi-coded like "750,750")
    country_year: dict[tuple, dict] = defaultdict(lambda: {
        "n_conflicts": 0,
        "max_intensity": 0,
        "is_war": 0,
        "is_intrastate": 0,
        "is_interstate": 0,
        "is_extrastate": 0,
        "is_internationalized": 0,
    })

    n_rows = 0
    for row in reader:
        n_rows += 1
        year_str = (row.get("year") or "").strip().strip('"')
        if not year_str:
            continue
        try:
            yr = int(float(year_str))
            obs_d = dt.date(yr, 12, 31)
        except (ValueError, TypeError):
            continue

        conflict_id = (row.get("conflict_id") or "").strip().strip('"')
        gwno_loc = (row.get("gwno_loc") or row.get("gwno_loc_a") or "").strip().strip('"')
        # gwno_loc can be multi-value like "750,750" - take first
        gwno = gwno_loc.split(",")[0].strip() if gwno_loc else "NA"

        # Per-conflict-year records (raw variables)
        for var in CONFLICT_VARS:
            raw_val = (row.get(var) or "").strip().strip('"')
            if not raw_val:
                continue
            try:
                v = float(raw_val)
                if v != v:
                    continue
                keys.append(f"UCDP:ACD:{var}:{conflict_id}")
                dates.append(obs_d)
                vals.append(v)
            except (ValueError, TypeError):
                continue

        # Country-year aggregates
        try:
            intensity = int(row.get("intensity_level", 0) or 0)
            toc = int(row.get("type_of_conflict", 0) or 0)
        except (ValueError, TypeError):
            intensity = 0
            toc = 0

        ck = (gwno, obs_d)
        country_year[ck]["n_conflicts"] += 1
        country_year[ck]["max_intensity"] = max(country_year[ck]["max_intensity"], intensity)
        if intensity >= 2:
            country_year[ck]["is_war"] = 1
        if toc == 3:
            country_year[ck]["is_intrastate"] = 1
        elif toc == 2:
            country_year[ck]["is_interstate"] = 1
        elif toc == 1:
            country_year[ck]["is_extrastate"] = 1
        elif toc == 4:
            country_year[ck]["is_internationalized"] = 1

    # Add country-year aggregates
    for (gwno, obs_d), agg_dict in country_year.items():
        for var, v in agg_dict.items():
            keys.append(f"UCDP:ACD:{var}:{gwno}")
            dates.append(obs_d)
            vals.append(float(v))

    log(f"  Parsed {n_rows:,} conflict-year rows -> {len(keys):,} observations")
    return keys, dates, vals


def main():
    os.makedirs(OUT, exist_ok=True)
    out_path = os.path.join(OUT, "acd.parquet")

    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"UCDP ACD: already {n:,} rows"); return

    log("=== UCDP Armed Conflict Dataset v24.1 Ingest ===")
    log(f"Downloading {ACD_URL[-60:]}...")
    data = fetch(ACD_URL)
    if not data:
        log("FAILED"); return

    k, d, v = parse_acd(data)
    if not k:
        log("0 observations"); return

    tbl = pa.table({
        "series_key": pa.array(k, pa.string()),
        "obs_date":   pa.array(d, pa.date32()),
        "value":      pa.array(v, pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"=== UCDP DONE: {n:,} observations ===")


if __name__ == "__main__":
    main()
