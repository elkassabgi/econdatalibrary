#!/usr/bin/env python3
"""NBP — National Bank of Poland exchange rates, 1984–present.

License: Open data (nbp.pl)
Source: https://api.nbp.pl/
No API key required.

Coverage:
  * Table A: ~32 currencies vs PLN, mid rates, business days, 2002–present
  * Table B: ~100+ exotic currencies vs PLN, weekly, 2002–present
  * Each request covers max 367 days; fetched in annual chunks

Run: python jobs/ingest_nbp.py
"""
from __future__ import annotations
import datetime as dt, os, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "nbp")
BASE = "https://api.nbp.pl/api/exchangerates"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
RATE = 0.5
START_YEAR = 2002  # Data available from 2002


def log(m):
    try:
        print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
    except UnicodeEncodeError:
        print(f"[{time.strftime('%H:%M:%S')}] {str(m).encode('ascii','replace').decode()}", flush=True)


def get_json(url: str, retries: int = 3) -> dict | list | None:
    for attempt in range(retries):
        try:
            r = requests.get(url + "?format=json", headers=UA, timeout=30)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 404):
                return None
            if r.status_code == 429:
                time.sleep(30); continue
            log(f"  HTTP {r.status_code}")
        except Exception as e:
            log(f"  ERR: {e}")
        time.sleep(3 * (attempt + 1))
    return None


def fetch_currency_history(table: str, code: str) -> list[tuple[dt.date, float]]:
    """Fetch full history for one currency in 1-year chunks."""
    results = []
    today = dt.date.today()
    start = dt.date(START_YEAR, 1, 1)

    while start <= today:
        end = min(dt.date(start.year + 1, 1, 1) - dt.timedelta(days=1), today)
        url = f"{BASE}/rates/{table}/{code}/{start.isoformat()}/{end.isoformat()}"
        data = get_json(url)
        if data and isinstance(data, dict) and "rates" in data:
            for item in data["rates"]:
                date_str = item.get("effectiveDate", "")
                mid = item.get("mid") or item.get("bid")
                if not date_str or mid is None:
                    continue
                try:
                    d = dt.date.fromisoformat(date_str[:10])
                    results.append((d, float(mid)))
                except (ValueError, TypeError):
                    pass
        start = end + dt.timedelta(days=1)
        time.sleep(0.2)
    return results


def main():
    os.makedirs(OUT, exist_ok=True)
    out_path = os.path.join(OUT, "nbp.parquet")

    done: set[str] = set()
    all_keys, all_dates, all_vals = [], [], []

    if os.path.exists(out_path):
        tbl = pq.read_table(out_path)
        done = set(tbl.column("series_key").to_pylist())
        all_keys  = tbl.column("series_key").to_pylist()
        all_dates = tbl.column("obs_date").to_pylist()
        all_vals  = tbl.column("value").to_pylist()
        log(f"Resuming: {len(done)} series done, {len(all_vals):,} obs")

    # Get current currency lists
    tables = {"A": [], "B": []}
    for table in ("A", "B"):
        data = get_json(f"{BASE}/tables/{table}")
        if data and isinstance(data, list) and data:
            rates = data[0].get("rates", [])
            tables[table] = [r["code"] for r in rates if r.get("code")]
            log(f"Table {table}: {len(tables[table])} currencies: {tables[table][:10]}")

    total_series = sum(len(v) for v in tables.values())
    log(f"NBP: {total_series} currency series to download")

    done_count = 0
    for table, codes in tables.items():
        for code in codes:
            series_key = f"PLNFX_{table}_{code}"
            if series_key in done:
                done_count += 1; continue

            log(f"  Fetching {series_key}...")
            obs = fetch_currency_history(table, code)
            for d, v in obs:
                all_keys.append(series_key)
                all_dates.append(d)
                all_vals.append(v)
            log(f"  {series_key}: {len(obs):,} obs")
            done.add(series_key)
            done_count += 1
            time.sleep(RATE)

            # Checkpoint every 5 series
            if done_count % 5 == 0 and all_vals:
                tbl = pa.table({
                    "series_key": pa.array(all_keys,  pa.string()),
                    "obs_date":   pa.array(all_dates, pa.date32()),
                    "value":      pa.array(all_vals,  pa.float64()),
                })
                pq.write_table(tbl, out_path, compression="zstd")
                log(f"  Checkpoint: {len(all_vals):,} obs saved ({done_count}/{total_series} series)")

    if not all_vals:
        log("0 observations"); return

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"DONE: {n:,} NBP Poland observations ({len(set(all_keys))} series)")


if __name__ == "__main__":
    main()
