#!/usr/bin/env python3
"""TCMB (Central Bank of Turkey) — daily exchange rates, 1996–present.

License: Open data (public domain, TCMB terms)
Source: https://tcmb.gov.tr/
No API key required.

Coverage:
  * ~30 currency pairs vs Turkish Lira (TRY), daily, 1996–present
  * Forex buying/selling, banknote buying/selling, cross rates
  * ~7,500 trading days × 30+ currencies × 4 rate types

Strategy:
  * Enumerate all weekday dates from 1996-01-01 to today
  * GET https://tcmb.gov.tr/kurlar/{YYYYMM}/{DDMMYYYY}.xml
  * Parse XML: each Currency node has ForexBuying, ForexSelling, etc.
  * Skip holidays/weekends (404 responses)

Run: python jobs/ingest_tcmb.py
"""
from __future__ import annotations
import datetime as dt, os, sys, time
import xml.etree.ElementTree as ET
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT  = os.path.join(ROOT, "data", "clean_full", "tcmb")
BASE = "https://tcmb.gov.tr/kurlar"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
RATE = 0.05   # 20 requests/second max
START_DATE = dt.date(1996, 1, 2)   # first available

RATE_FIELDS = ["ForexBuying", "ForexSelling", "BanknoteBuying", "BanknoteSelling"]
SUFFIX_MAP  = {"ForexBuying": "fb", "ForexSelling": "fs",
               "BanknoteBuying": "nb", "BanknoteSelling": "ns"}


def log(m):
    try:
        print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
    except UnicodeEncodeError:
        print(f"[{time.strftime('%H:%M:%S')}] {str(m).encode('ascii', 'replace').decode()}", flush=True)


def fetch_day(d: dt.date) -> dict[str, float]:
    """Fetch exchange rates for one date. Returns {currency_field: value}."""
    yyyymm = d.strftime("%Y%m")
    ddmmyyyy = d.strftime("%d%m%Y")
    url = f"{BASE}/{yyyymm}/{ddmmyyyy}.xml"
    try:
        r = requests.get(url, headers=UA, timeout=30)
        if r.status_code == 404:
            return {}
        if r.status_code != 200:
            return {}
        root = ET.fromstring(r.content)
        result = {}
        for cur in root.findall("Currency"):
            code = cur.get("CurrencyCode", cur.get("Kod", ""))
            if not code:
                continue
            unit_el = cur.find("Unit")
            unit = 1.0
            if unit_el is not None and unit_el.text:
                try:
                    unit = float(unit_el.text)
                except ValueError:
                    pass

            for field in RATE_FIELDS:
                el = cur.find(field)
                if el is not None and el.text and el.text.strip():
                    try:
                        val = float(el.text.replace(",", ".")) / unit
                        key = f"{code}_{SUFFIX_MAP[field]}"
                        result[key] = val
                    except (ValueError, TypeError):
                        pass
        return result
    except Exception:
        return {}


def main():
    os.makedirs(OUT, exist_ok=True)
    out_path = os.path.join(OUT, "tcmb.parquet")

    today = dt.date.today()

    # Track done dates
    done_dates: set[dt.date] = set()
    all_keys, all_dates, all_vals = [], [], []

    if os.path.exists(out_path):
        tbl = pq.read_table(out_path)
        for od in tbl.column("obs_date").to_pylist():
            done_dates.add(od)
        all_keys  = tbl.column("series_key").to_pylist()
        all_dates = tbl.column("obs_date").to_pylist()
        all_vals  = tbl.column("value").to_pylist()
        log(f"Resuming: {len(done_dates)} dates done, {len(all_vals):,} obs")

    # Check --start and --end args
    start_date = START_DATE
    end_date = today
    for a in sys.argv[1:]:
        if a.startswith("--start="):
            try:
                start_date = dt.date.fromisoformat(a.split("=", 1)[1])
            except ValueError:
                pass
        elif a.startswith("--end="):
            try:
                end_date = dt.date.fromisoformat(a.split("=", 1)[1])
            except ValueError:
                pass

    # Build list of weekdays to fetch
    all_weekdays = []
    d = start_date
    while d <= end_date:
        if d.weekday() < 5 and d not in done_dates:   # Mon–Fri only
            all_weekdays.append(d)
        d += dt.timedelta(days=1)

    log(f"TCMB: {len(all_weekdays)} weekdays to fetch (up to {end_date})")

    n_fetched = 0
    for i, day in enumerate(all_weekdays):
        rates = fetch_day(day)
        for key, val in rates.items():
            all_keys.append(key)
            all_dates.append(day)
            all_vals.append(val)
        if rates:
            n_fetched += 1

        if i % 500 == 0 and i > 0:
            log(f"  [{i}/{len(all_weekdays)}] {day}: {len(rates)} rates, total {len(all_vals):,} obs")
            if i % 500 == 0 and all_vals:
                tbl = pa.table({
                    "series_key": pa.array(all_keys,  pa.string()),
                    "obs_date":   pa.array(all_dates, pa.date32()),
                    "value":      pa.array(all_vals,  pa.float64()),
                })
                pq.write_table(tbl, out_path, compression="zstd")
                log(f"  Checkpoint: {len(all_vals):,} obs saved")

        time.sleep(RATE)

    if not all_vals:
        log("0 observations"); return

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"DONE: {n:,} TCMB Turkey observations ({n_fetched} trading days)")


if __name__ == "__main__":
    main()
