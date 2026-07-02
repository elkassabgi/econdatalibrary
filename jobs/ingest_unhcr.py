#!/usr/bin/env python3
"""UNHCR Refugee Population Statistics.

License: CC BY 4.0 (UN Refugee Agency open data)
Source: https://api.unhcr.org/population/v1/
No API key required.

Coverage:
  * Refugees, asylum seekers, IDPs, stateless persons, returned refugees
  * Bilateral (country-of-origin × country-of-asylum) flows
  * 1951–present, updated annually

Strategy:
  * Iterate over all available years
  * For each year: paginate through population/idmc/solutions endpoints
  * Long format: series_key = {indicator}:{coo_iso}:{coa_iso}

Run: python jobs/ingest_unhcr.py
"""
from __future__ import annotations
import datetime as dt, os, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "unhcr")
BASE = "https://api.unhcr.org/population/v1"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
        "Accept": "application/json"}
RATE = 0.3
PAGE = 100


def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def get_json(url: str, retries: int = 4) -> dict | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=60)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 404):
                return None
            if r.status_code == 429:
                time.sleep(30); continue
            log(f"  HTTP {r.status_code} attempt {attempt+1}: {url[-80:]}")
        except Exception as e:
            log(f"  ERR attempt {attempt+1}: {e}")
        time.sleep(5 * (attempt + 1))
    return None


NUMERIC_FIELDS = [
    "refugees", "asylum_seekers", "returned_refugees",
    "idps", "returned_idps", "stateless", "ooc", "oip", "hst",
]


def fetch_endpoint_year(endpoint: str, year: int) -> list[tuple]:
    """Fetch all pages for one endpoint+year. Returns list of (key, date, value)."""
    rows = []
    page = 1
    while True:
        url = (f"{BASE}/{endpoint}/?limit={PAGE}&page={page}"
               f"&coo_all=true&coa_all=true&year={year}")
        data = get_json(url)
        if not data or not data.get("items"):
            break
        for item in data["items"]:
            yr  = item.get("year", year)
            coo = (item.get("coo_iso") or item.get("coo") or "WLD")[:10]
            coa = (item.get("coa_iso") or item.get("coa") or "WLD")[:10]
            d   = dt.date(int(yr), 12, 31)
            # Emit one row per numeric indicator
            for field in NUMERIC_FIELDS:
                raw = item.get(field)
                if raw is None or raw in ("", "-", "0", 0):
                    continue
                try:
                    v = float(str(raw).replace(",", ""))
                except (ValueError, TypeError):
                    continue
                if v == 0:
                    continue
                rows.append((f"{field}:{coo}:{coa}", d, v))
        max_pages = data.get("maxPages", page)
        if page >= max_pages:
            break
        page += 1
        time.sleep(RATE)
    return rows


def ingest_endpoint(endpoint: str, years: list[int],
                    all_keys: list, all_dates: list, all_vals: list) -> int:
    """Ingest one UNHCR endpoint across all years."""
    before = len(all_vals)
    for yr in years:
        rows = fetch_endpoint_year(endpoint, yr)
        for key, d, v in rows:
            all_keys.append(f"{endpoint}:{key}")
            all_dates.append(d)
            all_vals.append(v)
        if yr % 10 == 0:
            log(f"  {endpoint} {yr}: {len(rows)} rows, total {len(all_vals):,}")
        time.sleep(RATE)
    return len(all_vals) - before


def main():
    os.makedirs(OUT, exist_ok=True)
    out_path = os.path.join(OUT, "unhcr.parquet")
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"Already done: {n:,} rows"); return

    # Years: 1951 through current year
    current_year = 2026
    years = list(range(1951, current_year + 1))
    log(f"UNHCR: fetching {len(years)} years × 3 endpoints")

    all_keys, all_dates, all_vals = [], [], []

    for endpoint in ("population", "idmc", "solutions"):
        log(f"=== Endpoint: {endpoint} ===")
        n = ingest_endpoint(endpoint, years, all_keys, all_dates, all_vals)
        log(f"  {endpoint}: {n:,} obs added")

    if not all_vals:
        log("0 observations collected"); return

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"DONE: {n:,} total UNHCR observations → {out_path}")


if __name__ == "__main__":
    main()
