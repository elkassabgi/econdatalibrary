#!/usr/bin/env python3
"""WHO Global Health Observatory (GHO) — 3,059 health indicators, all countries.

License: CC BY-NC-SA 3.0 IGO
Source: https://ghoapi.azureedge.net/api/
No API key required.

Coverage:
  * Life expectancy, mortality, disease burden, nutrition, health systems
  * ~3,059 indicators × 194 countries × 1990–present

Strategy:
  * GET /api/Indicator → list all indicator codes
  * For each indicator: GET /api/{indicator}?$filter=... → all observations
  * Long format Parquet; fully resumable (skip existing parquets)

Run: python jobs/ingest_who_gho.py
"""
from __future__ import annotations
import datetime as dt, os, sys, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT  = os.path.join(ROOT, "data", "clean_full", "who_gho")
BASE = "https://ghoapi.azureedge.net/api"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
        "Accept": "application/json"}
RATE = 0.5
PAGE = 1000


def log(m):
    try:
        print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
    except UnicodeEncodeError:
        print(f"[{time.strftime('%H:%M:%S')}] {str(m).encode('ascii','replace').decode()}", flush=True)


def get_json(url: str, retries: int = 4) -> dict | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=120)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 404):
                return None
            if r.status_code == 429:
                time.sleep(60); continue
            log(f"  HTTP {r.status_code} attempt {attempt+1}: {url[-80:]}")
        except Exception as e:
            log(f"  ERR attempt {attempt+1}: {e}")
        time.sleep(5 * (attempt + 1))
    return None


def get_all_indicators() -> list[str]:
    """Return list of all GHO indicator codes."""
    # Do NOT add $top — the API max is 1000 but /Indicator returns all ~3059 by default
    url = f"{BASE}/Indicator"
    data = get_json(url)
    if not data:
        return []
    return [x["IndicatorCode"] for x in data.get("value", []) if x.get("IndicatorCode")]


def fetch_indicator(code: str) -> tuple[list, list, list]:
    """Fetch all observations for one indicator. Returns (keys, dates, vals)."""
    keys, dates, vals = [], [], []
    skip = 0
    while True:
        url = (f"{BASE}/{code}"
               f"?$top={PAGE}&$skip={skip}"
               f"&$select=SpatialDim,TimeDim,NumericValue,TimeDimensionValue")
        data = get_json(url)
        if not data:
            break
        items = data.get("value", [])
        if not items:
            break
        for item in items:
            num_v = item.get("NumericValue")
            if num_v is None:
                continue
            try:
                v = float(num_v)
            except (TypeError, ValueError):
                continue
            yr_raw = item.get("TimeDim") or item.get("TimeDimensionValue")
            if yr_raw is None:
                continue
            try:
                yr = int(str(yr_raw)[:4])
                d  = dt.date(yr, 12, 31)
            except (ValueError, TypeError):
                continue
            geo = str(item.get("SpatialDim") or "GLOBAL")[:20]
            keys.append(f"{code}:{geo}")
            dates.append(d)
            vals.append(v)
        skip += len(items)
        if len(items) < PAGE:
            break
        time.sleep(0.1)
    return keys, dates, vals


def main():
    os.makedirs(OUT, exist_ok=True)
    out_path = os.path.join(OUT, "who_gho.parquet")

    # Check which indicators already done
    done: set[str] = set()
    all_keys: list = []
    all_dates: list = []
    all_vals: list = []
    if os.path.exists(out_path):
        tbl = pq.read_table(out_path)
        # Parse done indicator codes from series_key prefix
        for sk in tbl.column("series_key").to_pylist():
            code = sk.split(":")[0] if ":" in sk else sk
            done.add(code)
        all_keys  = tbl.column("series_key").to_pylist()
        all_dates = tbl.column("obs_date").to_pylist()
        all_vals  = tbl.column("value").to_pylist()
        log(f"Resuming: {len(done)} indicators done, {len(all_vals):,} obs")

    only_ids: set[str] = set()
    for a in sys.argv[1:]:
        if a.startswith("--only"):
            raw = a.split("=", 1)[-1] if "=" in a else ""
            only_ids = set(raw.split(","))
        elif not a.startswith("-"):
            only_ids.add(a)

    log("Fetching WHO GHO indicator list...")
    codes = get_all_indicators()
    log(f"Found {len(codes)} indicators")

    to_do = [c for c in codes if c not in done]
    if only_ids:
        to_do = [c for c in to_do if c in only_ids]
    log(f"{len(to_do)} indicators to download")

    for i, code in enumerate(to_do, 1):
        k, d, v = fetch_indicator(code)
        all_keys.extend(k)
        all_dates.extend(d)
        all_vals.extend(v)
        if i % 50 == 0:
            log(f"  [{i}/{len(to_do)}] {code}: {len(v):,} obs, total {len(all_vals):,}")
            # Incremental save every 200 indicators
            if i % 200 == 0:
                tbl = pa.table({
                    "series_key": pa.array(all_keys,  pa.string()),
                    "obs_date":   pa.array(all_dates, pa.date32()),
                    "value":      pa.array(all_vals,  pa.float64()),
                })
                pq.write_table(tbl, out_path, compression="zstd")
                log(f"  Checkpoint: {len(all_vals):,} obs saved")
        time.sleep(RATE)

    if not all_vals:
        log("0 observations collected"); return

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"DONE: {n:,} WHO GHO observations")


if __name__ == "__main__":
    main()
