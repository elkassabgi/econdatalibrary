#!/usr/bin/env python3
"""CEPII Gravity Dataset ingest.

Source: http://www.cepii.fr/CEPII/en/bdd_modele/presentation.asp?id=8
Authors: Head & Mayer (CEPII)
License: Free for research use
Coverage: All country pairs, 1948-2019, bilateral trade gravity variables

Key variables:
  dist (distance), comlang_off (common language), colony, contig (contiguity),
  gdp_o, gdp_d (origin/dest GDP), pop_o, pop_d, tradeflow_comtrade_o/d (trade)
  fta_wto, gatt_o, gatt_d, wto_o, wto_d (trade agreements)
  lang_ethno, col_dep, col_dep_ever, etc.

Gravity data is bilateral (country pairs), so series_key includes both countries.
series_key: GRAVITY:{variable}:{iso3_o}:{iso3_d}   e.g. GRAVITY:dist:USA:CHN

Note: The full gravity dataset is large (1.4GB CSV). We fetch a compressed version
or summary statistics to keep file size manageable.

Output: data/clean_full/cepii_gravity/cepii_gravity.parquet
Run: python jobs/ingest_cepii_gravity.py
"""
from __future__ import annotations
import csv, datetime as dt, gzip, io, os, time, zipfile
import requests
import pyarrow as pa, pyarrow.parquet as pq

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "cepii_gravity")
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

# CEPII Gravity dataset download URLs (try multiple formats)
GRAVITY_URLS = [
    # Main Gravity 3.1 dataset (CSV, ~200MB compressed)
    "http://www.cepii.fr/DATA_DOWNLOAD/gravity/data/Gravity_csv_V202211.zip",
    "https://www.cepii.fr/DATA_DOWNLOAD/gravity/data/Gravity_csv_V202211.zip",
    # Older version fallback
    "http://www.cepii.fr/DATA_DOWNLOAD/gravity/data/Gravity_V202211.zip",
    # GeoDist (much smaller — just bilateral distances and geographic vars)
    "http://www.cepii.fr/DATA_DOWNLOAD/geo_cepii/data/GeoDist.zip",
    "https://www.cepii.fr/DATA_DOWNLOAD/geo_cepii/data/GeoDist.zip",
]

# Variables to extract from the full gravity dataset (subset to keep reasonable size)
# Country-pair time-invariant variables (replicated per year but constant)
STATIC_VARS = {
    "distw", "distwces", "dist",          # weighted/unweighted distance
    "comlang_off", "comlang_ethno",        # common official/ethnic language
    "colony", "col_dep", "col_dep_ever",   # colonial ties
    "contig",                               # contiguity (shared border)
    "comleg_posit", "comleg_negat",        # common legal origin
    "comcur",                              # common currency
}
# Time-varying variables (GDP, trade flows, WTO membership)
TIME_VARS = {
    "gdp_o", "gdp_d", "gdp_ppp_pwt_o", "gdp_ppp_pwt_d",
    "pop_o", "pop_d",
    "tradeflow_comtrade_o", "tradeflow_comtrade_d",
    "fta_wto", "gatt_o", "gatt_d", "wto_o", "wto_d",
    "gsp", "eu_o", "eu_d",
}
KEEP_VARS = STATIC_VARS | TIME_VARS

# For GeoDist (geographic distances only, much smaller)
GEODIST_VARS = {
    "dist", "distcap", "distw", "distwces",
    "comlang_off", "comlang_ethno", "colony", "col_dep_ever",
    "contig", "comleg_posit", "smctry",
}

BATCH = 1_000_000   # rows per write batch


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def fetch_stream(url: str) -> tuple[bytes | None, str]:
    """Download URL, return (bytes, format) where format is 'zip' or 'csv'."""
    for attempt in range(3):
        try:
            log(f"  GET {url[-80:]}...")
            r = requests.get(url, headers=UA, timeout=300, stream=True, allow_redirects=True)
            if r.status_code == 200:
                chunks = []
                total = 0
                for chunk in r.iter_content(chunk_size=1 << 20):
                    if chunk:
                        chunks.append(chunk)
                        total += len(chunk)
                        if total > 500_000_000:  # 500 MB limit
                            log(f"  File too large (>{total//1024//1024} MB), stopping download")
                            return None, ""
                data = b"".join(chunks)
                log(f"  {len(data)//1024:,} KB downloaded")
                return data, "zip" if data[:2] == b"PK" else "csv"
            log(f"  HTTP {r.status_code}")
            if r.status_code in (403, 404):
                return None, ""
        except Exception as e:
            log(f"  ERR attempt {attempt+1}: {e}")
        time.sleep(5 * (attempt + 1))
    return None, ""


def parse_gravity_csv(raw_text: str, keep_vars: set) -> tuple[list, list, list]:
    """Parse gravity CSV to long format. Returns keys, dates, vals."""
    reader = csv.DictReader(io.StringIO(raw_text))
    headers = [h.strip() for h in (reader.fieldnames or [])]
    log(f"  Columns ({len(headers)}): {headers[:12]}")

    # Find id columns
    iso_o  = next((h for h in headers if h.lower() in ("iso3_o", "iso3num_o")), None) or \
             next((h for h in headers if "iso3" in h.lower() and "_o" in h.lower()), None)
    iso_d  = next((h for h in headers if h.lower() in ("iso3_d", "iso3num_d")), None) or \
             next((h for h in headers if "iso3" in h.lower() and "_d" in h.lower()), None)
    year_c = next((h for h in headers if h.lower() in ("year", "yr")), None)

    # GeoDist has iso_o, iso_d (2-letter) or iso3_o
    if iso_o is None:
        iso_o = next((h for h in headers if h.lower() in ("iso_o", "iso2_o")), None)
        iso_d = next((h for h in headers if h.lower() in ("iso_d", "iso2_d")), None)

    if not iso_o or not iso_d:
        log(f"  Cannot find origin/destination ISO cols in: {headers[:10]}")
        return [], [], []

    has_year = year_c is not None
    log(f"  iso_o={iso_o}, iso_d={iso_d}, year={year_c}")

    # Value columns
    val_cols = [h for h in headers
                if h.lower() in {v.lower() for v in keep_vars}
                and h not in (iso_o, iso_d, year_c)]
    log(f"  Value cols ({len(val_cols)}): {val_cols}")

    if not val_cols:
        log("  No matching value columns found")
        return [], [], []

    keys, dates, vals = [], [], []
    n_rows = 0
    seen = set()  # for static vars: only keep one obs per pair

    for rec in reader:
        n_rows += 1
        o = (rec.get(iso_o) or "").strip().upper()
        d = (rec.get(iso_d) or "").strip().upper()
        if not o or not d or len(o) > 5 or len(d) > 5:
            continue

        # Year / date
        if has_year:
            yr_raw = rec.get(year_c, "")
            try:
                yr = int(float(str(yr_raw).strip()))
                obs_d = dt.date(yr, 12, 31)
            except (TypeError, ValueError):
                continue
        else:
            obs_d = dt.date(2000, 12, 31)  # GeoDist is cross-sectional

        for col in val_cols:
            raw = rec.get(col, "")
            if not raw or str(raw).strip() in ("", "NA", "N/A", "."):
                continue
            try:
                v = float(str(raw).strip())
                if v != v:
                    continue
                skey = f"GRAVITY:{col}:{o}:{d}"
                token = (skey, obs_d)
                if col in STATIC_VARS and token in seen:
                    continue
                seen.add(token)
                keys.append(skey)
                dates.append(obs_d)
                vals.append(v)
            except (TypeError, ValueError):
                pass

        if n_rows % 500_000 == 0:
            log(f"  {n_rows:,} rows, {len(keys):,} obs so far")

    log(f"  Parsed {n_rows:,} rows -> {len(keys):,} obs")
    return keys, dates, vals


def main():
    os.makedirs(OUT, exist_ok=True)
    out = os.path.join(OUT, "cepii_gravity.parquet")

    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows
        log(f"CEPII Gravity: already {n:,} rows"); return

    log("=== CEPII Gravity Dataset Ingest ===")

    keys, dates, vals = [], [], []

    for url in GRAVITY_URLS:
        data, fmt = fetch_stream(url)
        if not data:
            continue

        keep = GEODIST_VARS if "GeoDist" in url or "geo_cepii" in url else KEEP_VARS

        if fmt == "zip":
            z = zipfile.ZipFile(io.BytesIO(data))
            log(f"  ZIP members: {z.namelist()}")
            csv_members = [m for m in z.namelist() if m.lower().endswith(".csv")]
            if not csv_members:
                log("  No CSV in ZIP"); continue
            for member in csv_members:
                log(f"  Parsing {member}...")
                raw = z.read(member)
                # Handle gzip inside zip
                if raw[:2] == b"\x1f\x8b":
                    raw = gzip.decompress(raw)
                text = raw.decode("utf-8-sig", errors="replace")
                k, d, v = parse_gravity_csv(text, keep)
                keys.extend(k); dates.extend(d); vals.extend(v)
                if vals:
                    break
        else:
            text = data.decode("utf-8-sig", errors="replace")
            k, d, v = parse_gravity_csv(text, keep)
            keys.extend(k); dates.extend(d); vals.extend(v)

        if vals:
            log(f"  Total obs: {len(vals):,}")
            break

    if not vals:
        log("0 observations — all CEPII Gravity URLs failed"); return

    tbl = pa.table({
        "series_key": pa.array(keys,  pa.string()),
        "obs_date":   pa.array(dates, pa.date32()),
        "value":      pa.array(vals,  pa.float64()),
    })
    pq.write_table(tbl, out, compression="zstd")
    n = pq.read_metadata(out).num_rows
    log(f"=== CEPII Gravity DONE: {n:,} obs ===")


if __name__ == "__main__":
    main()
