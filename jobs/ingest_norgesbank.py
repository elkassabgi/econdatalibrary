#!/usr/bin/env python3
"""Norges Bank — Norwegian Central Bank SDMX-JSON API.

License: Open data (Norges Bank terms)
Source: https://data.norges-bank.no/
No API key required.

Coverage (23 dataflows):
  * Exchange rates (USD/NOK, EUR/NOK, 40+ currencies, daily 1994–present)
  * Policy interest rate
  * Financial indicators (NIBOR, NOWA, T-bills)
  * Government securities (yields, zero-coupon)
  * Money market rates
  * Short-term interest rates

API format: GET /api/data/{DATAFLOW}?format=sdmx-json
SDMX-JSON v1.0 format — series dimensions encoded as "0:1:2:0" indices.

Run: python jobs/ingest_norgesbank.py
"""
from __future__ import annotations
import datetime as dt, os, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "norgesbank")
BASE = "https://data.norges-bank.no/api"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
RATE = 1.0

# Dataflows to fetch (skip announcement/operational feeds)
DATAFLOWS = [
    "EXR",
    "FINANCIAL_INDICATORS",
    "GOVT_GENERIC_RATES",
    "GOVT_KEYFIGURES",
    "GOVT_ZEROCOUPON",
    "IR",
    "MONEY_MARKET",
    "SHORT_RATES",
    "REGNET",
    "LIQUIDITY_STATISTICS",
]


def log(m):
    try:
        print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
    except UnicodeEncodeError:
        print(f"[{time.strftime('%H:%M:%S')}] {str(m).encode('ascii','replace').decode()}", flush=True)


def get_json(url: str, retries: int = 3) -> dict | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=120)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 404):
                return None
            log(f"  HTTP {r.status_code}: {url[-60:]}")
        except Exception as e:
            log(f"  ERR: {e}")
        time.sleep(5 * (attempt + 1))
    return None


def parse_sdmx_json(data: dict, flow_id: str) -> list[tuple[str, dt.date, float]]:
    """Parse SDMX-JSON response into (series_key, date, value) tuples."""
    results = []
    try:
        structure = data.get("data", {}).get("structure", {})
        dims = structure.get("dimensions", {})
        attrs = structure.get("attributes", {})

        # Series-level dimensions (define the series identifier)
        series_dims = dims.get("series", [])
        # Observation-level dimension (time)
        obs_dims = dims.get("observation", [])

        time_dim = obs_dims[0] if obs_dims else None
        time_values = [v.get("id", "") for v in (time_dim.get("values", []) if time_dim else [])]

        datasets = data.get("data", {}).get("dataSets", [])
        if not datasets:
            return results

        ds = datasets[0]
        series_dict = ds.get("series", {})

        for series_key_str, series_data in series_dict.items():
            # Build series label from dimension indices
            indices = [int(x) for x in series_key_str.split(":")]
            label_parts = []
            for dim_idx, pos in enumerate(indices):
                if dim_idx < len(series_dims):
                    dim = series_dims[dim_idx]
                    vals = dim.get("values", [])
                    if pos < len(vals):
                        label_parts.append(vals[pos].get("id", str(pos)))
            series_label = f"{flow_id}:" + ".".join(label_parts) if label_parts else f"{flow_id}:{series_key_str}"

            obs = series_data.get("observations", {})
            for obs_idx_str, obs_val in obs.items():
                obs_idx = int(obs_idx_str)
                if obs_idx >= len(time_values):
                    continue
                time_str = time_values[obs_idx]
                v_raw = obs_val[0] if isinstance(obs_val, list) and obs_val else obs_val

                if v_raw is None:
                    continue
                try:
                    v = float(v_raw)
                    if v != v:
                        continue
                except (ValueError, TypeError):
                    continue

                # Parse date
                d = None
                try:
                    if len(time_str) == 10:
                        d = dt.date.fromisoformat(time_str)
                    elif len(time_str) == 7:
                        d = dt.date(int(time_str[:4]), int(time_str[5:7]), 1)
                    elif len(time_str) == 4:
                        d = dt.date(int(time_str), 12, 31)
                    elif len(time_str) > 10:
                        d = dt.date.fromisoformat(time_str[:10])
                except (ValueError, TypeError):
                    pass

                if d is None:
                    continue
                results.append((series_label, d, v))

    except Exception as e:
        log(f"  Parse error for {flow_id}: {e}")
        import traceback; traceback.print_exc()
    return results


def main():
    os.makedirs(OUT, exist_ok=True)
    out_path = os.path.join(OUT, "norgesbank.parquet")

    done: set[str] = set()
    all_keys, all_dates, all_vals = [], [], []

    if os.path.exists(out_path):
        tbl = pq.read_table(out_path)
        done_keys = set(tbl.column("series_key").to_pylist())
        # Mark done flows
        for k in done_keys:
            flow = k.split(":")[0]
            done.add(flow)
        all_keys  = tbl.column("series_key").to_pylist()
        all_dates = tbl.column("obs_date").to_pylist()
        all_vals  = tbl.column("value").to_pylist()
        log(f"Resuming: {len(done)} flows done, {len(all_vals):,} obs")

    to_do = [f for f in DATAFLOWS if f not in done]
    log(f"Norges Bank: {len(to_do)} dataflows to download")

    for flow_id in to_do:
        url = f"{BASE}/data/{flow_id}?format=sdmx-json"
        log(f"  Fetching {flow_id}...")
        data = get_json(url)
        if data is None:
            log(f"  {flow_id}: no data"); time.sleep(RATE); continue

        results = parse_sdmx_json(data, flow_id)
        log(f"  {flow_id}: {len(results):,} observations")

        for key, d, v in results:
            all_keys.append(key)
            all_dates.append(d)
            all_vals.append(v)

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
    log(f"DONE: {n:,} Norges Bank observations ({len(set(all_keys))} series)")


if __name__ == "__main__":
    main()
