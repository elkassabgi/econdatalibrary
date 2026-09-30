#!/usr/bin/env python3
"""Yale Environmental Performance Index (EPI) ingest.

Source: https://epi.yale.edu/downloads
License: CC BY 4.0 (Yale Center for Environmental Law & Policy)
Coverage: 180 countries, 2000-present, 40+ environmental indicators.
2024 release: https://epi.yale.edu/downloads/epi2024results.csv

series_key: EPI:{variable}:{iso3}  e.g. EPI:EPI.new:USA
Output: data/clean_full/yale_epi/yale_epi.parquet
Run: python jobs/ingest_yale_epi.py
"""
from __future__ import annotations
import csv, datetime as dt, hashlib, io, os, time
import requests, pyarrow as pa, pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT  = os.path.join(ROOT, "data", "clean_full", "yale_epi")
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

# The 2024 edition from the publisher's archive (Yale's own archive page links its Dataverse; epi.yale.edu no
# longer serves epi2024results.csv). doi:10.7910/DVN/ZLAHG0 v2.0 - the 2025-03-16 revision; see the fetcher's
# KNOWN_URLS for the full note (review R1299).
RESULT_URLS = [
    ("https://dataverse.harvard.edu/api/access/datafile/14094607?format=original", 2024),
]
RESULT_MD5 = {RESULT_URLS[0][0]: "688e7ee38f02d7698e3f299a40ef3fc0"}      # as the dataset's file listing gives it
# Abbreviation/variable name file (to decode column names; not read by this script) - the same dataset's copy
VARIABLES_URL = "https://dataverse.harvard.edu/api/access/datafile/14118343?format=original"


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def fetch(url):
    try:
        r = requests.get(url, headers=UA, timeout=60, allow_redirects=True)
        if r.status_code == 200 and len(r.content) > 500:
            return r.content
        log(f"  HTTP {r.status_code}: {url}")
    except Exception as e:
        log(f"  ERR: {e}")
    return None


def parse_epi_csv(data: bytes, default_year: int):
    """Parse EPI results CSV. Wide format: iso column + many indicator columns."""
    text = data.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    headers = reader.fieldnames or []
    log(f"  Headers ({len(headers)}): {headers[:12]}")

    # EPI 2024 uses 'iso' for ISO3
    iso3_col = next((h for h in headers if h.lower() in
                     ("iso", "iso3", "iso_code", "country_iso3", "code")), None)
    year_col = next((h for h in headers if h.lower() in ("year", "yr")), None)

    if not iso3_col:
        log(f"  No ISO3 column found"); return [], [], []

    skip = {(iso3_col or "").lower(), (year_col or "").lower(),
            "country", "region", "continent", "rank", "tier", "country.name"}

    keys, dates, vals = [], [], []
    for row in reader:
        iso3 = (row.get(iso3_col) or "").strip()
        # The numeric `code` vocabulary is unpadded (Afghanistan = 4): the old len==3 test, meant for
        # alpha-3, dropped every country coded under 100 - the served 2024 edition holds 157 countries, not
        # EPI 2024's 180 (review R1289). Numeric codes pass on isdigit; alpha-3 still needs 3 letters.
        if not iso3 or not (iso3.isdigit() if iso3_col.lower() == "code" else len(iso3) == 3):
            continue

        if year_col and row.get(year_col):
            try:
                yr = int(float(row[year_col]))
            except (ValueError, TypeError):
                yr = default_year
        else:
            yr = default_year
        obs_d = dt.date(yr, 12, 31)

        for col, raw in row.items():
            if col.lower().strip() in skip or not col:
                continue
            if not raw or str(raw).strip() in ("", "NA", "N/A", "nan", "#N/A", "-"):
                continue
            try:
                v = float(str(raw).replace(",", ""))
                if v != v:
                    continue
                keys.append(f"EPI:{col.strip()}:{iso3}")
                dates.append(obs_d)
                vals.append(v)
            except (TypeError, ValueError):
                pass

    return keys, dates, vals


def main():
    os.makedirs(OUT, exist_ok=True)
    out = os.path.join(OUT, "yale_epi.parquet")
    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows
        log(f"Yale EPI: already {n:,} rows"); return

    log("=== Yale EPI 2024 Ingest ===")
    all_keys, all_dates, all_vals = [], [], []

    for url, yr in RESULT_URLS:
        log(f"Downloading {url}...")
        data = fetch(url)
        want = RESULT_MD5.get(url)
        if data and want and hashlib.md5(data).hexdigest() != want:
            log(f"  {yr}: the pinned archive file answered with other bytes (md5 {hashlib.md5(data).hexdigest()}, "
                f"the publisher lists {want}) - not parsed")
            data = None
        if data:
            k, d, v = parse_epi_csv(data, default_year=yr)
            log(f"  {yr}: {len(v):,} obs")
            all_keys.extend(k); all_dates.extend(d); all_vals.extend(v)
        time.sleep(1)

    if not all_vals:
        log("0 observations parsed"); return

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out, compression="zstd")
    n = pq.read_metadata(out).num_rows
    log(f"=== Yale EPI DONE: {n:,} obs ===")


if __name__ == "__main__":
    main()
