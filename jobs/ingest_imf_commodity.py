#!/usr/bin/env python3
"""IMF Primary Commodity Prices ingest via DBnomics API.

Source: IMF PCPS (Primary Commodity Price System) via DBnomics
        https://db.nomics.world/IMF/PCPS
License: IMF data Terms of Use (free for research)
Coverage: 1,236 series; energy, metals, food, beverages, agricultural raw materials;
          monthly/quarterly/annual, 1980-present (~70 commodities)

series_key: IMF_COMMODITY:{series_code}
  e.g. IMF_COMMODITY:M.W00.POILBRE.USD  (monthly, Brent crude oil, USD)

Output: data/clean_full/imf_commodity/imf_commodity.parquet
Run: python jobs/ingest_imf_commodity.py
"""
from __future__ import annotations
import datetime as dt, os, time
import requests
import pyarrow as pa, pyarrow.parquet as pq

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "imf_commodity")
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

DBNOMICS = "https://api.db.nomics.world/v22"
PAGE_SIZE = 1000  # Max series per request


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def fetch_pcps_page(offset: int) -> dict | None:
    """Fetch a page of IMF PCPS series from DBnomics with observations."""
    url = (f"{DBNOMICS}/series/IMF/PCPS"
           f"?observations=1&limit={PAGE_SIZE}&offset={offset}")
    for attempt in range(3):
        try:
            r = requests.get(url, headers=UA, timeout=180)
            if r.status_code == 200:
                return r.json()
            log(f"  HTTP {r.status_code} (offset={offset})")
            if r.status_code in (400, 404):
                return None
        except Exception as e:
            log(f"  ERR attempt {attempt+1}: {e}")
        time.sleep(5 * (attempt + 1))
    return None


def main():
    os.makedirs(OUT, exist_ok=True)
    out = os.path.join(OUT, "imf_commodity.parquet")

    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows
        log(f"IMF Commodity: already {n:,} rows"); return

    log("=== IMF Primary Commodity Prices Ingest (DBnomics/PCPS) ===")

    all_keys, all_dates, all_vals = [], [], []
    offset = 0
    total  = None

    while True:
        log(f"Fetching PCPS page offset={offset}...")
        data = fetch_pcps_page(offset)
        if not data:
            log("  Fetch failed, stopping")
            break

        series_obj = data.get("series", {})
        docs  = series_obj.get("docs", [])
        total = series_obj.get("num_found", total or 0)

        if not docs:
            log("  No docs returned")
            break

        n_obs_batch = 0
        for series in docs:
            series_code = series.get("series_code", "")
            periods     = series.get("period_start_day", [])
            values      = series.get("value", [])

            if not series_code or not periods:
                continue

            skey = f"IMF_COMMODITY:{series_code}"

            for period_str, v in zip(periods, values):
                if v is None:
                    continue
                try:
                    obs_d = dt.date.fromisoformat(period_str)
                    fv = float(v)
                    if fv != fv:   # NaN
                        continue
                    all_keys.append(skey)
                    all_dates.append(obs_d)
                    all_vals.append(fv)
                    n_obs_batch += 1
                except (ValueError, TypeError):
                    pass

        offset += len(docs)
        log(f"  [{offset}/{total}] series, +{n_obs_batch:,} obs (total {len(all_vals):,})")

        if offset >= (total or 0):
            break

        time.sleep(1.0)   # polite rate limit

    if not all_vals:
        log("0 obs — all sources failed")
        return

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out, compression="zstd")
    n = pq.read_metadata(out).num_rows
    log(f"=== IMF Commodity DONE: {n:,} obs ===")


if __name__ == "__main__":
    main()
