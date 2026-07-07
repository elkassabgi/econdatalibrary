#!/usr/bin/env python3
"""UN Sustainable Development Goals (SDG) Database — 713 series, 200+ countries.

License: CC BY 4.0
Source: https://unstats.un.org/sdgs/
No API key required.

Coverage:
  * All 17 SDG goals, 169 targets, 713 unique series
  * Countries, world regions, income groups
  * ~2000–present (varies by indicator)

API: https://unstats.un.org/sdgapi/v1/sdg/

Run: python jobs/ingest_unsdg.py
"""
from __future__ import annotations
import datetime as dt, os, sys, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "unsdg")
BASE = "https://unstats.un.org/sdgapi/v1/sdg"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
        "Accept": "application/json"}
RATE = 0.5
PAGE = 1000


def log(m):
    try:
        print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
    except UnicodeEncodeError:
        print(f"[{time.strftime('%H:%M:%S')}] {str(m).encode('ascii', 'replace').decode()}", flush=True)


def get_json(url: str, params: dict | None = None,
             retries: int = 4) -> dict | list | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=UA, timeout=120)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 404):
                return None
            if r.status_code == 429:
                time.sleep(60); continue
            log(f"  HTTP {r.status_code} attempt {attempt+1}: {url[-70:]}")
        except Exception as e:
            log(f"  ERR attempt {attempt+1}: {e}")
        time.sleep(5 * (attempt + 1))
    return None


def get_series_list() -> list[dict]:
    """Return all SDG series metadata."""
    data = get_json(f"{BASE}/Series/List")
    return data if isinstance(data, list) else []


def get_geoareas() -> list[dict]:
    """Return list of all geo areas."""
    data = get_json(f"{BASE}/GeoArea/List")
    return data if isinstance(data, list) else []


def fetch_series_data(series_code: str) -> tuple[list, list, list]:
    """Fetch all observations for one SDG series (all geographies, all years).

    Correct endpoint: GET /v1/sdg/Series/Data?seriesCode=CODE&page=N&pageSize=N
    (NOT /v1/sdg/Series/{code}/Data which returns 404)
    """
    keys, dates, vals = [], [], []
    page = 1

    while True:
        data = get_json(f"{BASE}/Series/Data",
                        params={"seriesCode": series_code,
                                "pageSize": PAGE, "page": page})
        if not data:
            break

        # Response: {"totalElements":N, "totalPages":M, "pageNumber":P, "data":[...]}
        if isinstance(data, dict):
            records = data.get("data", [])
            total_pages = data.get("totalPages", 1)
        elif isinstance(data, list):
            records = data
            total_pages = 1
        else:
            break

        if not records:
            break

        for rec in records:
            geo = str(rec.get("geoAreaCode", rec.get("geoAreaName", "WLD")))
            time_period = rec.get("timePeriodStart") or rec.get("timePeriod")
            val_raw = rec.get("value")

            if val_raw is None or str(val_raw).strip() in ("", "N/A", "null", "None"):
                continue
            try:
                v = float(str(val_raw).replace(",", ""))
            except (ValueError, TypeError):
                continue

            if time_period is None:
                continue
            try:
                yr = int(str(time_period).split(".")[0])
                obs_date = dt.date(yr, 12, 31)
            except (ValueError, TypeError):
                continue

            # Dimensions is a dict e.g. {"Reporting Type": "G", "Sex": "BOTHSEX"}
            dims = rec.get("dimensions", {})
            dim_str = ""
            if isinstance(dims, dict) and dims:
                dim_parts = [f"{k}={dv}" for k, dv in sorted(dims.items())
                             if dv and dv not in ("", "_T", "ALLAREA", "G")]
                if dim_parts:
                    # Carry ALL non-trivial dimensions, not just the first 3 — mirrors
                    # the S1 fetcher (strategies/fetchers/unsdg.py). Truncating to [:3]
                    # dropped a 4th+ disaggregating dimension (e.g. SE_ADT_ACTS's
                    # "Type of skill", 36 values) and collapsed distinct observations
                    # onto one (series_key, obs_date), inflating the store with dupes.
                    dim_str = "|" + "|".join(dim_parts)

            keys.append(f"{series_code}:{geo}{dim_str}")
            dates.append(obs_date)
            vals.append(v)

        if page >= total_pages:
            break
        page += 1

    return keys, dates, vals


def main():
    os.makedirs(OUT, exist_ok=True)
    out_path = os.path.join(OUT, "unsdg.parquet")

    done: set[str] = set()
    all_keys, all_dates, all_vals = [], [], []

    if os.path.exists(out_path):
        tbl = pq.read_table(out_path)
        for sk in tbl.column("series_key").to_pylist():
            code = sk.split(":")[0] if ":" in sk else sk
            done.add(code)
        all_keys  = tbl.column("series_key").to_pylist()
        all_dates = tbl.column("obs_date").to_pylist()
        all_vals  = tbl.column("value").to_pylist()
        log(f"Resuming: {len(done)} series done, {len(all_vals):,} obs")

    only_codes: set[str] = set()
    for a in sys.argv[1:]:
        if a.startswith("--only"):
            raw = a.split("=", 1)[-1] if "=" in a else ""
            only_codes = set(raw.split(","))
        elif not a.startswith("-"):
            only_codes.add(a)

    log("Fetching UN SDG series list...")
    series_list = get_series_list()
    log(f"Found {len(series_list)} SDG series")

    to_do = [s for s in series_list if s.get("code") not in done]
    if only_codes:
        to_do = [s for s in to_do if s.get("code") in only_codes]
    log(f"{len(to_do)} series to download")

    for i, series_meta in enumerate(to_do, 1):
        code = series_meta.get("code", "")
        if not code:
            continue
        k, d, v = fetch_series_data(code)
        all_keys.extend(k)
        all_dates.extend(d)
        all_vals.extend(v)

        if i % 50 == 0 or v:
            log(f"  [{i}/{len(to_do)}] {code}: {len(v):,} obs, total {len(all_vals):,}")

        # Save checkpoint every 100 series
        if i % 100 == 0 and all_vals:
            tbl = pa.table({
                "series_key": pa.array(all_keys,  pa.string()),
                "obs_date":   pa.array(all_dates, pa.date32()),
                "value":      pa.array(all_vals,  pa.float64()),
            })
            pq.write_table(tbl, out_path, compression="zstd")
            log(f"  Checkpoint saved: {len(all_vals):,} obs")

        time.sleep(RATE)

    if not all_vals:
        log("0 observations collected"); return

    # Dedup exact (series_key, obs_date) duplicates the UN SDG API returns: many
    # series report the SAME observation multiple times (verified: every collision
    # carries an identical value — zero differing values, so this is lossless). Left
    # raw, ~286k dup rows would make the effective key non-unique and a downstream
    # dedup-on-merge shrink the store below the 0.97 never-shrink floor.
    items = {}
    for k, d, v in zip(all_keys, all_dates, all_vals):
        items[(k, d)] = v  # exact dupes share a value; last-wins is harmless
    if len(items) != len(all_vals):
        log(f"deduped exact (key,obs_date) dupes: {len(all_vals):,} -> {len(items):,}")
        all_keys  = [k for (k, _d) in items]
        all_dates = [d for (_k, d) in items]
        all_vals  = list(items.values())

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"DONE: {n:,} UN SDG observations")


if __name__ == "__main__":
    main()
