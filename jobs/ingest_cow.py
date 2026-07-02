#!/usr/bin/env python3
"""Correlates of War (COW) ingest — downloads key datasets and converts to long-format parquet.

Datasets covered:
  NMCv7            National Material Capabilities (country-year numeric indicators)
  COW_Trade_4.0    Bilateral Trade (dyad-year; stored as-is, also agg to country-year)
  WRP_national     World Religion Project national (country-year religion shares)
  formal-alliances Alliance treaty obligations (country-year membership count)
  MID-5            Militarized Interstate Disputes (country-year dispute indicators)
  Territorial      Territorial Changes (country-year)

Source: https://correlatesofwar.org  (free, academic)
SSL:    verify=False (COW has an expired/mis-issued cert)
Output: data/clean_full/cow/{dataset}.parquet  (series_key, obs_date, value)
Run:    python jobs/ingest_cow.py
"""
from __future__ import annotations
import datetime as dt
import io
import os
import re
import sys
import time
import zipfile

import requests
import pyarrow as pa
import pyarrow.parquet as pq
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "cow")
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
BASE = "https://correlatesofwar.org/wp-content/uploads"
RATE = 0.5


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def fetch(url: str, retries: int = 4) -> bytes | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=UA, verify=False, timeout=120)
            if r.status_code == 200:
                return r.content
            if r.status_code == 404:
                log(f"  404: {url}")
                return None
            log(f"  HTTP {r.status_code} attempt {attempt+1}: {url[-70:]}")
        except Exception as e:
            log(f"  ERR attempt {attempt+1}: {e}")
        time.sleep(5 * (attempt + 1))
    return None


def read_csv_from_zip(data: bytes, prefer: str | None = None) -> tuple[list[str], list[list]]:
    """Extract first (or preferred) CSV from a ZIP, return (headers, rows)."""
    import csv
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        csvs = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if not csvs:
            return [], []
        # pick preferred file or the one with most content
        chosen = csvs[0]
        if prefer:
            for c in csvs:
                if prefer.lower() in c.lower():
                    chosen = c
                    break
        log(f"    Reading {chosen} from zip ({len(csvs)} CSVs)")
        raw = z.read(chosen)
        # Detect encoding
        text = raw.decode("utf-8-sig", errors="replace")
        reader = csv.reader(io.StringIO(text))
        rows = list(reader)
        if not rows:
            return [], []
        return rows[0], rows[1:]


def read_csv_bytes(data: bytes) -> tuple[list[str], list[list]]:
    import csv
    text = data.decode("utf-8-sig", errors="replace")
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        return [], []
    return rows[0], rows[1:]


def col_idx(headers: list[str], candidates: list[str]) -> int | None:
    lh = [h.lower().strip() for h in headers]
    for c in candidates:
        if c.lower() in lh:
            return lh.index(c.lower())
    return None


def save(keys, dates, vals, out_path):
    tbl = pa.table({
        "series_key": pa.array(keys,  pa.string()),
        "obs_date":   pa.array(dates, pa.date32()),
        "value":      pa.array(vals,  pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    log(f"  -> {len(vals):,} obs saved to {os.path.basename(out_path)}")
    return len(vals)


# ---------------------------------------------------------------------------
# Dataset-specific ingesters
# ---------------------------------------------------------------------------

def read_csv_from_nested_zip(data: bytes, outer_name: str, inner_prefer: str | None = None) -> tuple[list[str], list[list]]:
    """Read CSV from a zip-inside-a-zip (outer_name is the inner zip's path)."""
    import csv
    with zipfile.ZipFile(io.BytesIO(data)) as z_outer:
        inner_data = z_outer.read(outer_name)
    with zipfile.ZipFile(io.BytesIO(inner_data)) as z_inner:
        csvs = [n for n in z_inner.namelist() if n.lower().endswith(".csv")]
        if not csvs:
            return [], []
        chosen = csvs[0]
        if inner_prefer:
            for c in csvs:
                if inner_prefer.lower() in c.lower():
                    chosen = c; break
        log(f"    Reading {chosen} from nested zip")
        raw = z_inner.read(chosen)
        text = raw.decode("utf-8-sig", errors="replace")
        reader = csv.reader(io.StringIO(text))
        rows = list(reader)
        if not rows:
            return [], []
        return rows[0], rows[1:]


def ingest_nmc():
    """National Material Capabilities v7 — country-year, many numeric columns.
    NMCv7.zip contains a nested NMC-v7-abridged.zip with the actual CSV.
    """
    out = os.path.join(OUT, "nmc.parquet")
    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows
        log(f"NMC: already {n:,} rows"); return n

    log("NMC: downloading NMCv7.zip...")
    data = fetch(f"{BASE}/NMCv7.zip")
    if not data:
        return 0
    # NMCv7.zip contains NMCv7/NMC-v7-abridged.zip (nested)
    headers, rows = read_csv_from_nested_zip(data, "NMCv7/NMC-v7-abridged.zip")

    # Expected columns: stateabb, ccode, year, irst, milex, milper, pec, tpop, upop, cinc
    year_i = col_idx(headers, ["year"])
    ccode_i = col_idx(headers, ["stateabb", "ccode"])
    if year_i is None:
        log("  NMC: no year column"); return 0

    # Numeric value columns
    numeric_cols = ["irst", "milex", "milper", "pec", "tpop", "upop", "cinc"]
    num_idx = []
    for nc in numeric_cols:
        i = col_idx(headers, [nc])
        if i is not None:
            num_idx.append((nc, i))
    log(f"  NMC: {len(rows)} rows, numeric cols: {[n for n,_ in num_idx]}")

    keys, dates, vals = [], [], []
    for row in rows:
        if len(row) <= max(year_i, *[i for _,i in num_idx]):
            continue
        try:
            yr = int(row[year_i])
            obs_d = dt.date(yr, 12, 31)
        except (ValueError, TypeError):
            continue
        ctry = row[ccode_i].strip() if ccode_i is not None else ""
        for col, ci in num_idx:
            raw = row[ci].strip()
            if raw in ("-9", "-9.0", "", ".", "NA", "N/A"):
                continue
            try:
                v = float(raw)
                key = f"COW:NMC:{col}:{ctry}" if ctry else f"COW:NMC:{col}"
                keys.append(key); dates.append(obs_d); vals.append(v)
            except (ValueError, TypeError):
                pass
    return save(keys, dates, vals, out)


def ingest_trade():
    """COW Bilateral Trade v4.0 — dyad-year exports/imports."""
    out = os.path.join(OUT, "trade.parquet")
    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows
        log(f"Trade: already {n:,} rows"); return n

    log("Trade: downloading COW_Trade_4.0.zip...")
    data = fetch(f"{BASE}/COW_Trade_4.0.zip")
    if not data:
        return 0
    # Has Dyadic_COW_4.0.csv and National_COW_4.0.csv
    # Use national-level totals (importer/exporter aggregated)
    headers, rows = read_csv_from_zip(data, prefer="National")
    if not headers:
        log("  Trade: no CSV found, trying dyadic")
        headers, rows = read_csv_from_zip(data, prefer="Dyadic")
    if not headers:
        return 0

    year_i = col_idx(headers, ["year"])
    ccode_i = col_idx(headers, ["ccode1", "importer1", "ccode"])
    # Value columns: imports, exports (in millions current USD)
    num_cols = ["imports", "exports", "flow1", "flow2"]
    num_idx = [(nc, col_idx(headers, [nc])) for nc in num_cols if col_idx(headers, [nc]) is not None]
    log(f"  Trade: {len(rows)} rows, cols: {[n for n,_ in num_idx]}")

    if year_i is None or not num_idx:
        log("  Trade: missing required columns"); return 0

    keys, dates, vals = [], [], []
    for row in rows:
        try:
            yr = int(row[year_i])
            obs_d = dt.date(yr, 12, 31)
        except (ValueError, TypeError, IndexError):
            continue
        ctry = row[ccode_i].strip() if ccode_i is not None and ccode_i < len(row) else ""
        for col, ci in num_idx:
            if ci >= len(row):
                continue
            raw = row[ci].strip()
            if raw in ("-9", "-9.0", "", ".", "NA", "N/A"):
                continue
            try:
                v = float(raw)
                key = f"COW:Trade:{col}:{ctry}" if ctry else f"COW:Trade:{col}"
                keys.append(key); dates.append(obs_d); vals.append(v)
            except (ValueError, TypeError):
                pass
    return save(keys, dates, vals, out)


def ingest_wrp():
    """World Religion Project — national-level religion shares, country-year."""
    out = os.path.join(OUT, "wrp.parquet")
    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows
        log(f"WRP: already {n:,} rows"); return n

    log("WRP: downloading WRP_national.csv...")
    data = fetch(f"{BASE}/WRP_national.csv")
    if not data:
        return 0
    headers, rows = read_csv_bytes(data)

    year_i = col_idx(headers, ["year"])
    ccode_i = col_idx(headers, ["state", "statename", "country"])
    if year_i is None:
        log("  WRP: no year column"); return 0

    # Skip metadata/description columns; take all numeric-ish ones
    skip = {"year", "state", "statename", "country", "ccode", "name", ""}
    num_idx = []
    for i, h in enumerate(headers):
        if h.lower().strip() in skip or i in [year_i, ccode_i]:
            continue
        num_idx.append((h.strip(), i))
    log(f"  WRP: {len(rows)} rows, {len(num_idx)} value columns")

    keys, dates, vals = [], [], []
    for row in rows:
        try:
            yr = int(float(row[year_i]))
            obs_d = dt.date(yr, 12, 31)
        except (ValueError, TypeError, IndexError):
            continue
        ctry = row[ccode_i].strip() if ccode_i is not None and ccode_i < len(row) else ""
        for col, ci in num_idx:
            if ci >= len(row):
                continue
            raw = row[ci].strip()
            if raw in ("-9", "-9.0", "", ".", "NA", "N/A"):
                continue
            try:
                v = float(raw)
                key = f"COW:WRP:{col}:{ctry}" if ctry else f"COW:WRP:{col}"
                keys.append(key); dates.append(obs_d); vals.append(v)
            except (ValueError, TypeError):
                pass
    return save(keys, dates, vals, out)


def ingest_alliances():
    """COW Formal Alliances v4.1 — country-year alliance participation."""
    out = os.path.join(OUT, "alliances.parquet")
    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows
        log(f"Alliances: already {n:,} rows"); return n

    log("Alliances: downloading version4.1_csv.zip...")
    data = fetch(f"{BASE}/version4.1_csv.zip")
    if not data:
        return 0
    # Use by_member.csv — has mem_st_year / mem_end_year, expand to one row per year
    headers, rows = read_csv_from_zip(data, prefer="member")
    if not headers:
        return 0

    ccode_i   = col_idx(headers, ["ccode"])
    st_yr_i   = col_idx(headers, ["mem_st_year", "all_st_year"])
    end_yr_i  = col_idx(headers, ["mem_end_year", "all_end_year"])
    type_i    = col_idx(headers, ["ss_type", "type"])
    if st_yr_i is None:
        log(f"  Alliances: missing start-year col in {headers[:10]}"); return 0

    from collections import defaultdict
    counts = defaultdict(int)
    types_count = defaultdict(lambda: defaultdict(int))

    # ss_type text: "Type I: Defense Pact", "Type II: Neutrality Pact", etc.
    def parse_type(s):
        if not s:
            return "other"
        s = s.lower()
        if "defense" in s:  return "defense"
        if "neutral" in s:  return "neutrality"
        if "nonagg" in s or "non-agg" in s: return "nonaggression"
        if "entente" in s:  return "entente"
        return "other"

    for row in rows:
        try:
            st = int(row[st_yr_i])
        except (ValueError, TypeError, IndexError):
            continue
        try:
            en = int(row[end_yr_i]) if end_yr_i is not None and row[end_yr_i].strip() else 2016
        except (ValueError, TypeError):
            en = 2016
        ctry = row[ccode_i].strip() if ccode_i is not None and ccode_i < len(row) else ""
        tname = parse_type(row[type_i].strip() if type_i is not None and type_i < len(row) else "")
        for yr in range(st, min(en + 1, 2025)):
            counts[(yr, ctry)] += 1
            types_count[tname][(yr, ctry)] += 1

    keys, dates, vals = [], [], []
    for (yr, ctry), cnt in counts.items():
        key = f"COW:Alliance:total:{ctry}" if ctry else "COW:Alliance:total"
        keys.append(key); dates.append(dt.date(yr, 12, 31)); vals.append(float(cnt))
    for tname, tc in types_count.items():
        for (yr, ctry), cnt in tc.items():
            key = f"COW:Alliance:{tname}:{ctry}" if ctry else f"COW:Alliance:{tname}"
            keys.append(key); dates.append(dt.date(yr, 12, 31)); vals.append(float(cnt))

    return save(keys, dates, vals, out)


def ingest_mids():
    """MID v5 — Militarized Interstate Disputes, incident-level → country-year counts."""
    out = os.path.join(OUT, "mids.parquet")
    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows
        log(f"MIDs: already {n:,} rows"); return n

    log("MIDs: downloading MID-5-Data-and-Supporting-Materials.zip...")
    data = fetch(f"{BASE}/MID-5-Data-and-Supporting-Materials.zip")
    if not data:
        return 0
    headers, rows = read_csv_from_zip(data, prefer="MIDB")  # MIDB = incident-level
    if not headers:
        headers, rows = read_csv_from_zip(data)
    if not headers:
        return 0

    year_i = col_idx(headers, ["styear", "styr", "year"])
    ccode_i = col_idx(headers, ["ccode", "stateabb"])
    # Numeric outcome columns
    num_cols = ["fatalpre", "hiact", "hostlev", "orig", "revisionist"]
    num_idx = [(nc, col_idx(headers, [nc])) for nc in num_cols if col_idx(headers, [nc]) is not None]
    log(f"  MIDs: {len(rows)} rows, cols={[n for n,_ in num_idx]}")

    if year_i is None:
        log("  MIDs: no year column"); return 0

    # Also compute count of disputes per country-year
    from collections import defaultdict
    dispute_counts = defaultdict(int)
    keys, dates, vals = [], [], []

    for row in rows:
        try:
            yr = int(row[year_i])
            obs_d = dt.date(yr, 12, 31)
        except (ValueError, TypeError, IndexError):
            continue
        ctry = row[ccode_i].strip() if ccode_i is not None and ccode_i < len(row) else ""
        dispute_counts[(yr, ctry)] += 1
        for col, ci in num_idx:
            if ci >= len(row):
                continue
            raw = row[ci].strip()
            if raw in ("-9", "-9.0", "", ".", "NA", "N/A"):
                continue
            try:
                v = float(raw)
                key = f"COW:MID:{col}:{ctry}" if ctry else f"COW:MID:{col}"
                keys.append(key); dates.append(obs_d); vals.append(v)
            except (ValueError, TypeError):
                pass

    # Add dispute counts
    for (yr, ctry), cnt in dispute_counts.items():
        key = f"COW:MID:count:{ctry}" if ctry else "COW:MID:count"
        keys.append(key); dates.append(dt.date(yr, 12, 31)); vals.append(float(cnt))

    return save(keys, dates, vals, out)


def ingest_states():
    """State System Membership — country entry/exit encoded as existence flag."""
    out = os.path.join(OUT, "states.parquet")
    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows
        log(f"States: already {n:,} rows"); return n

    log("States: downloading States2024.zip...")
    data = fetch(f"{BASE}/States2024.zip")
    if not data:
        return 0
    headers, rows = read_csv_from_zip(data)
    if not headers:
        return 0

    styear_i = col_idx(headers, ["styear", "start"])
    endyear_i = col_idx(headers, ["endyear", "end"])
    ccode_i = col_idx(headers, ["stateabb", "ccode"])
    if styear_i is None or endyear_i is None:
        log("  States: missing year columns"); return 0

    keys, dates, vals = [], [], []
    for row in rows:
        try:
            st = int(row[styear_i])
            en = int(row[endyear_i])
        except (ValueError, TypeError, IndexError):
            continue
        ctry = row[ccode_i].strip() if ccode_i is not None and ccode_i < len(row) else ""
        key = f"COW:State:member:{ctry}" if ctry else "COW:State:member"
        for yr in range(st, min(en + 1, 2025)):
            keys.append(key)
            dates.append(dt.date(yr, 12, 31))
            vals.append(1.0)

    return save(keys, dates, vals, out)


def ingest_igo():
    """IGO Membership — state-year membership count."""
    out = os.path.join(OUT, "igo.parquet")
    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows
        log(f"IGO: already {n:,} rows"); return n

    log("IGO: downloading state_year_formatv3.zip...")
    data = fetch(f"{BASE}/state_year_formatv3.zip")
    if not data:
        return 0
    headers, rows = read_csv_from_zip(data)
    if not headers:
        return 0

    year_i = col_idx(headers, ["year"])
    ccode_i = col_idx(headers, ["ccode", "state"])
    if year_i is None:
        log("  IGO: no year column"); return 0

    # Total full members and partial members
    # membership codes: 1=full member, 2=associate, 3=observer, 0=non-member, -9=missing
    skip = {"year", "ccode", "state", "version"}
    num_idx = [(h.strip(), i) for i, h in enumerate(headers)
               if h.lower().strip() not in skip and i not in [year_i, ccode_i]]

    log(f"  IGO: {len(rows)} rows, {len(num_idx)} org columns")

    # Build full-member count and partial-member count per country-year
    from collections import defaultdict
    full_counts = defaultdict(int)
    partial_counts = defaultdict(int)

    for row in rows:
        try:
            yr = int(row[year_i])
        except (ValueError, TypeError, IndexError):
            continue
        ctry = row[ccode_i].strip() if ccode_i is not None and ccode_i < len(row) else ""
        for col, ci in num_idx:
            if ci >= len(row):
                continue
            raw = row[ci].strip()
            try:
                v = int(float(raw))
            except (ValueError, TypeError):
                continue
            if v == 1:
                full_counts[(yr, ctry)] += 1
            elif v in (2, 3):
                partial_counts[(yr, ctry)] += 1

    keys, dates, vals = [], [], []
    for (yr, ctry), cnt in full_counts.items():
        key = f"COW:IGO:full_member:{ctry}" if ctry else "COW:IGO:full_member"
        keys.append(key); dates.append(dt.date(yr, 12, 31)); vals.append(float(cnt))
    for (yr, ctry), cnt in partial_counts.items():
        key = f"COW:IGO:partial_member:{ctry}" if ctry else "COW:IGO:partial_member"
        keys.append(key); dates.append(dt.date(yr, 12, 31)); vals.append(float(cnt))

    return save(keys, dates, vals, out)


def main():
    os.makedirs(OUT, exist_ok=True)
    total = 0
    log("=== COW Ingest ===")

    for fn, name in [
        (ingest_nmc,      "National Material Capabilities"),
        (ingest_trade,    "Bilateral Trade"),
        (ingest_wrp,      "World Religion Project"),
        (ingest_alliances,"Formal Alliances"),
        (ingest_mids,     "MIDs"),
        (ingest_states,   "State System Membership"),
        (ingest_igo,      "IGO Membership"),
    ]:
        log(f"--- {name} ---")
        try:
            n = fn()
            total += n
            time.sleep(RATE)
        except Exception as e:
            log(f"  ERROR: {e}")
            import traceback; traceback.print_exc()

    log(f"=== GRAND TOTAL: {total:,} COW observations ===")


if __name__ == "__main__":
    main()
