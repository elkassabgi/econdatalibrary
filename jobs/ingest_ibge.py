#!/usr/bin/env python3
"""IBGE Brazil SIDRA aggregate data ingest — full catalog.

License: CC0 (Creative Commons Zero) — public domain dedication.
Source: IBGE SIDRA (Sistema IBGE de Recuperação Automática)
API: https://servicodados.ibge.gov.br/api/v3/

Strategy:
  * Enumerate all ~700+ aggregates from the IBGE catalog
  * For each aggregate: fetch national (N1) + state (N3) level for all periods
  * One Parquet per aggregate; fully resumable
  * Series key: "AGG={agg_id}:VAR={var_id}:LOC={geo_level}/{geo_id}"

Run: python jobs/ingest_ibge.py
     python jobs/ingest_ibge.py --only 1419,1420  # specific aggregate IDs
"""
from __future__ import annotations
import datetime as dt, json, os, sys, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "ibge")
BASE = "https://servicodados.ibge.gov.br/api/v3/agregados"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

# Geographic levels to fetch (from coarsest to finest)
# N1=Brazil, N2=Large region, N3=State, N7=Metro region
# Skip N6 (municipalities) by default — 5570 locations × many variables = massive
# Set INCLUDE_MUNI=True to add N6 for census aggregates
INCLUDE_MUNI = False


def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def get(url: str, retries: int = 4) -> dict | list | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=120)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 404):
                return None
            log(f"  HTTP {r.status_code} attempt {attempt+1}: {url[-80:]}")
        except Exception as e:
            log(f"  ERR attempt {attempt+1}: {e}")
        time.sleep(5 * (attempt + 1))
    return None


def parse_ibge_period(s: str) -> dt.date | None:
    """Parse IBGE period strings: YYYY, YYYYMM, YYYY0000 (annual), etc."""
    s = (s or "").strip()
    try:
        if len(s) == 4 and s.isdigit():
            return dt.date(int(s), 12, 31)
        if len(s) == 6 and s.isdigit():
            y, m = int(s[:4]), int(s[4:])
            if 1 <= m <= 12:
                return dt.date(y, m, 1)
            if m == 0:
                return dt.date(y, 12, 31)
        if len(s) == 8 and s.isdigit():
            y, q = int(s[:4]), int(s[4:6])
            if q in range(1, 13):
                return dt.date(y, q, 1)
            if s[4:8] in ("0000", "9999"):
                return dt.date(y, 12, 31)
    except Exception:
        pass
    return None


def get_aggregates() -> list[dict]:
    """Return list of all aggregates from IBGE catalog."""
    data = get(f"{BASE}")
    if not data or not isinstance(data, list):
        return []
    result = []
    for item in data:
        # Catalog returns list of groups; each group has aggregates
        if isinstance(item, dict):
            if "agregados" in item:
                for agg in item["agregados"]:
                    result.append(agg)
            elif "id" in item:
                result.append(item)
    return result


def get_aggregate_meta(agg_id: int) -> dict:
    """Get aggregate metadata: variables, classification, periodicidade."""
    meta = get(f"{BASE}/{agg_id}/metadados")
    if not meta or not isinstance(meta, dict):
        return {}
    return meta


def get_periods(agg_id: int) -> list[str]:
    """Get all available periods for an aggregate."""
    data = get(f"{BASE}/{agg_id}/periodos")
    if not data or not isinstance(data, list):
        return []
    return [str(p.get("id", "")) for p in data if p.get("id")]


def fetch_data(agg_id: int, var_ids: list[str],
               period_str: str, geo_level: str) -> list[tuple]:
    """Fetch data for one aggregate / variable set / period / geo level.
    Returns list of (series_key, date, value) tuples.
    """
    var_param = "|".join(var_ids[:50])  # API limit ~50 vars per call
    url = (f"{BASE}/{agg_id}/periodos/{period_str}"
           f"/variaveis/{var_param}"
           f"?localidades={geo_level}[all]")
    data = get(url)
    if not data or not isinstance(data, list):
        return []

    rows = []
    for loc_item in data:
        loc_id   = str(loc_item.get("id", ""))
        loc_name = loc_item.get("nome", "")
        # loc_item["resultados"] → list of classification combos
        for resultado in loc_item.get("resultados", []):
            for serie_item in resultado.get("series", []):
                var_info = serie_item.get("variavel", {})
                var_id   = str(var_info.get("id", ""))
                serie    = serie_item.get("serie", {})
                series_key = (f"AGG={agg_id}:VAR={var_id}"
                              f":LOC={geo_level}/{loc_id}")
                for period_code, raw_v in serie.items():
                    if raw_v in (None, "", "-", "...", "X"):
                        continue
                    try:
                        v = float(str(raw_v).replace(",", "."))
                    except (TypeError, ValueError):
                        continue
                    d = parse_ibge_period(str(period_code))
                    if d is None:
                        continue
                    rows.append((series_key, d, v))
    return rows


def ingest_aggregate(agg_id: int, agg_name: str,
                     out_dir: str) -> int:
    """Download all data for one IBGE aggregate. Returns obs count."""
    out_path = os.path.join(out_dir, f"{agg_id}.parquet")
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"  skip agg {agg_id} ({n:,} rows)")
        return n

    # Get metadata
    meta = get_aggregate_meta(agg_id)
    variables = meta.get("variaveis", [])
    if not variables:
        return 0
    var_ids = [str(v["id"]) for v in variables if v.get("id")]
    if not var_ids:
        return 0

    # Get periods
    periods = get_periods(agg_id)
    if not periods:
        return 0
    # Chunk periods to avoid URL length limits (max ~50 periods per request)
    CHUNK = 50

    geo_levels = ["N1", "N3"]
    if INCLUDE_MUNI:
        geo_levels.append("N6")

    all_keys, all_dates, all_vals = [], [], []

    for geo in geo_levels:
        for i in range(0, len(periods), CHUNK):
            period_str = "|".join(periods[i:i+CHUNK])
            rows = fetch_data(agg_id, var_ids, period_str, geo)
            for k, d, v in rows:
                all_keys.append(k); all_dates.append(d); all_vals.append(v)
            time.sleep(0.5)

    if not all_vals:
        log(f"  agg {agg_id} ({agg_name[:40]}): 0 obs")
        return 0

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"  agg {agg_id} ({agg_name[:40]}): {n:,} obs")
    return n


def main():
    os.makedirs(OUT, exist_ok=True)

    only_ids: set[int] = set()
    for a in sys.argv[1:]:
        if a.startswith("--only"):
            raw = a.split("=", 1)[-1] if "=" in a else ""
            only_ids = {int(x) for x in raw.split(",") if x.isdigit()}
        elif a.isdigit():
            only_ids.add(int(a))

    log("Fetching IBGE aggregate catalog...")
    aggregates = get_aggregates()
    log(f"Found {len(aggregates)} aggregates")

    if only_ids:
        aggregates = [a for a in aggregates if int(a.get("id", 0)) in only_ids]
        log(f"Filtered to {len(aggregates)} aggregates")

    total = 0
    for i, agg in enumerate(aggregates, 1):
        agg_id   = int(agg.get("id", 0))
        agg_name = agg.get("nome", str(agg_id))
        if not agg_id:
            continue
        log(f"[{i}/{len(aggregates)}] agg {agg_id}: {agg_name[:60]}")
        total += ingest_aggregate(agg_id, agg_name, OUT)

    log(f"DONE: {total:,} total IBGE observations")


if __name__ == "__main__":
    main()
