#!/usr/bin/env python3
"""Sveriges Riksbank (Sweden) — SWEA exchange rates and interest rates, 1994–present.

License: Open data (Riksbank public data)
Source: https://api.riksbank.se/swea/v1/
No API key required.

Coverage:
  * ~117 series: SEK FX rates, policy rate, repo rate, government bond yields,
    inflation expectations, mortgage rates, interbank rates
  * Daily/weekly/monthly observations

Strategy:
  * GET /swea/v1/Series → catalog of all series IDs
  * For each: GET /swea/v1/Observations/{id}/{from}/{to}
  * Rate limit: ~30 req/min; script backs off on 429

Run: python jobs/ingest_riksbank.py
"""
from __future__ import annotations
import datetime as dt, json, os, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "riksbank")
BASE = "https://api.riksbank.se/swea/v1"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
        "Accept": "application/json"}
CATALOG_FILE = os.path.join(OUT, "_series_catalog.json")
RATE = 3.0  # 3s between requests (aggressive rate limiting on Riksbank)


def log(m):
    try:
        print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
    except UnicodeEncodeError:
        print(f"[{time.strftime('%H:%M:%S')}] {str(m).encode('ascii','replace').decode()}", flush=True)


def get_json(url: str, retries: int = 8) -> dict | list | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=60)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 404):
                return None
            if r.status_code == 429:
                # Parse retry-after header or extract from message
                wait = 65
                try:
                    body = r.json()
                    msg = body.get("message", "")
                    import re
                    m = re.search(r"(\d+)\s+second", msg)
                    if m:
                        wait = int(m.group(1)) + 5
                except Exception:
                    pass
                log(f"  429 rate limit, sleeping {wait}s")
                time.sleep(wait)
                continue
            log(f"  HTTP {r.status_code}: {url[-80:]}")
        except Exception as e:
            log(f"  ERR attempt {attempt+1}: {e}")
        time.sleep(10 * (attempt + 1))
    return None


def get_catalog() -> list[dict]:
    """Get all Riksbank series from the catalog."""
    if os.path.exists(CATALOG_FILE):
        with open(CATALOG_FILE, encoding='utf-8') as f:
            cat = json.load(f)
        log(f"Loaded catalog: {len(cat)} series")
        return cat

    log("Fetching Riksbank series catalog...")
    data = get_json(f"{BASE}/Series")
    if not data or not isinstance(data, list):
        log("Failed to get catalog")
        return []

    os.makedirs(OUT, exist_ok=True)
    with open(CATALOG_FILE, "w", encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)
    log(f"Saved catalog: {len(data)} series")
    return data


def parse_date(s: str) -> dt.date | None:
    if not s:
        return None
    try:
        return dt.date.fromisoformat(str(s)[:10])
    except (ValueError, TypeError):
        return None


def fetch_series(series_id: str, min_date: str, max_date: str) -> list[tuple[str, dt.date, float]]:
    """Fetch all observations for one Riksbank series."""
    results = []
    url = f"{BASE}/Observations/{series_id}/{min_date}/{max_date}"
    data = get_json(url)
    time.sleep(RATE)
    if not data:
        return results

    if isinstance(data, dict):
        obs_list = data.get("observations", data.get("value", []))
    elif isinstance(data, list):
        obs_list = data
    else:
        return results

    for obs in obs_list:
        date_str = obs.get("date") or obs.get("Date") or ""
        val = obs.get("value") or obs.get("Value") or obs.get("average")
        if val is None:
            continue
        d = parse_date(date_str)
        if d is None:
            continue
        try:
            v = float(str(val).replace(",", "."))
            if v != v:
                continue
            results.append((f"RIKSBANK:{series_id}", d, v))
        except (ValueError, TypeError):
            continue
    return results


def main():
    os.makedirs(OUT, exist_ok=True)
    out_path = os.path.join(OUT, "riksbank.parquet")

    # Resume from existing parquet
    done_series: set[str] = set()
    all_keys, all_dates, all_vals = [], [], []
    if os.path.exists(out_path):
        tbl = pq.read_table(out_path)
        all_keys  = tbl.column("series_key").to_pylist()
        all_dates = tbl.column("obs_date").to_pylist()
        all_vals  = tbl.column("value").to_pylist()
        for sk in set(all_keys):
            code = sk.split(":")[-1] if ":" in sk else sk
            done_series.add(code)
        log(f"Resuming: {len(done_series)} series done, {len(all_vals):,} obs")

    catalog = get_catalog()
    if not catalog:
        return

    to_do = [s for s in catalog if s.get("seriesId") and s["seriesId"] not in done_series]
    log(f"{len(to_do)} series to download")

    total_new = 0
    for i, series in enumerate(to_do, 1):
        sid = series["seriesId"]
        min_date = series.get("observationMinDate", "1990-01-01")
        max_date = series.get("observationMaxDate", dt.date.today().isoformat())
        desc = series.get("shortDescription", "")

        log(f"  [{i}/{len(to_do)}] {sid}: {desc[:50]}")
        rows = fetch_series(sid, min_date, max_date)
        for sk, d, v in rows:
            all_keys.append(sk)
            all_dates.append(d)
            all_vals.append(v)
        total_new += len(rows)

        # Checkpoint every 10 series
        if i % 10 == 0 and all_vals:
            tbl = pa.table({
                "series_key": pa.array(all_keys,  pa.string()),
                "obs_date":   pa.array(all_dates, pa.date32()),
                "value":      pa.array(all_vals,  pa.float64()),
            })
            pq.write_table(tbl, out_path, compression="zstd")
            log(f"  Checkpoint: {len(all_vals):,} obs ({i}/{len(to_do)} series)")

    if not all_vals:
        log("0 observations")
        return

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"DONE: {n:,} Riksbank Sweden observations ({len(catalog)} series)")


if __name__ == "__main__":
    main()
