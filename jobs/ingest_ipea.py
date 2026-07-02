#!/usr/bin/env python3
"""IPEA Data — Instituto de Pesquisa Econômica Aplicada, Brazil.

License: Open data (dadosabertos.ipea.gov.br)
Source: http://www.ipeadata.gov.br/
No API key required.

Coverage (~2,900 series):
  * Macroeconomic: GDP, inflation, interest rates, exchange rates, trade, fiscal
  * Regional: state-level data (Brazil's 27 states)
  * Social: poverty, inequality, education, health, demographics

API (OData v4):
  GET /api/odata4/Metadados                    → all 2,899 series metadata
  GET /api/odata4/ValoresSerie(SERCODIGO='{code}') → values for one series

Run: python jobs/ingest_ipea.py
"""
from __future__ import annotations
import datetime as dt, os, sys, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "ipea")
BASE = "http://www.ipeadata.gov.br/api/odata4"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
RATE = 0.3


def log(m):
    try:
        print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
    except UnicodeEncodeError:
        print(f"[{time.strftime('%H:%M:%S')}] {str(m).encode('ascii','replace').decode()}", flush=True)


def get_json(url: str, retries: int = 3, timeout: int = 120) -> dict | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 404):
                return None
            if r.status_code == 429:
                time.sleep(60); continue
            log(f"  HTTP {r.status_code}: {url[-60:]}")
        except Exception as e:
            log(f"  ERR: {e}")
        time.sleep(5 * (attempt + 1))
    return None


def parse_ipea_date(s: str) -> dt.date | None:
    """Parse IPEA date: '1958-01-01T00:00:00-02:00' or '1990-01-01T00:00:00Z'"""
    if not s:
        return None
    try:
        # Take first 10 characters: YYYY-MM-DD
        date_part = str(s)[:10]
        return dt.date.fromisoformat(date_part)
    except (ValueError, TypeError):
        return None


def main():
    os.makedirs(OUT, exist_ok=True)
    out_path = os.path.join(OUT, "ipea.parquet")

    done: set[str] = set()
    all_keys, all_dates, all_vals = [], [], []

    if os.path.exists(out_path):
        tbl = pq.read_table(out_path)
        done = set(tbl.column("series_key").to_pylist())
        all_keys  = tbl.column("series_key").to_pylist()
        all_dates = tbl.column("obs_date").to_pylist()
        all_vals  = tbl.column("value").to_pylist()
        log(f"Resuming: {len(done)} series done, {len(all_vals):,} obs")

    # Fetch all metadata in one call
    log("Fetching IPEA series catalog...")
    meta_data = get_json(f"{BASE}/Metadados")
    if not meta_data:
        log("ERROR: Could not fetch metadata"); return

    series_list = meta_data.get("value", [])
    log(f"Total series: {len(series_list)}")

    # Filter to active series only (SERSTATUS == 'A') and numeric
    active = [s for s in series_list
              if s.get("SERSTATUS") == "A" and s.get("SERNUMERICA", True)]
    log(f"Active numeric series: {len(active)}")

    # Group by BASNOME for logging
    from collections import Counter
    bases = Counter(s.get("BASNOME", "?") for s in active)
    for base, cnt in sorted(bases.items()):
        log(f"  {base}: {cnt} series")

    to_do = [s for s in active if s.get("SERCODIGO") and s["SERCODIGO"] not in done]
    log(f"To download: {len(to_do)} series")

    checkpoint_every = 100
    for i, meta in enumerate(to_do, 1):
        code = meta["SERCODIGO"]
        name = meta.get("SERNOME", code)[:40]

        url = f"{BASE}/ValoresSerie(SERCODIGO='{code}')"
        data = get_json(url)
        if data is None:
            time.sleep(RATE); continue

        obs_list = data.get("value", [])
        n = 0
        for item in obs_list:
            d_raw = item.get("VALDATA", "")
            v_raw = item.get("VALVALOR")
            if v_raw is None:
                continue
            obs_date = parse_ipea_date(d_raw)
            if obs_date is None:
                continue
            try:
                v = float(v_raw)
                if v != v:  # NaN
                    continue
            except (ValueError, TypeError):
                continue

            # Include territorial code if non-empty (regional series)
            ter = str(item.get("TERCODIGO", "") or "").strip()
            series_key = f"{code}:{ter}" if ter else code

            all_keys.append(series_key)
            all_dates.append(obs_date)
            all_vals.append(v)
            n += 1

        if n > 0 or i % 100 == 0:
            log(f"  [{i}/{len(to_do)}] {code} ({name}): {n:,} obs")

        if i % checkpoint_every == 0 and all_vals:
            tbl = pa.table({
                "series_key": pa.array(all_keys, pa.string()),
                "obs_date":   pa.array(all_dates, pa.date32()),
                "value":      pa.array(all_vals, pa.float64()),
            })
            pq.write_table(tbl, out_path, compression="zstd")
            log(f"  Checkpoint: {len(all_vals):,} obs saved")

        time.sleep(RATE)

    if not all_vals:
        log("0 observations"); return

    tbl = pa.table({
        "series_key": pa.array(all_keys, pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals, pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"DONE: {n:,} IPEA Brazil observations ({len(set(all_keys))} series)")


if __name__ == "__main__":
    main()
