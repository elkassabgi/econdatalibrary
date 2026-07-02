#!/usr/bin/env python3
"""GUS Poland — Bank Danych Lokalnych (BDL), national-level, 1999–present.

License: Open data (CC BY 4.0, GUS public data policy)
Source: https://bdl.stat.gov.pl/
No API key required.

Coverage:
  * 172,563 variables across 33 subject domains
  * National-level (Poland total) annual data, 1999–present
  * Prices, employment, wages, GDP, trade, demographics, housing, etc.

Strategy:
  * GET /api/v1/subjects → crawl K→G→P hierarchy
  * For each P-level subject (hasVariables=true): GET /variables?subject-id={P}
  * For each variable: GET /data/by-variable/{id}?unit-id=000000000000&level=0
  * Checkpoint every 500 variables per K-subject
  * One Parquet per K-level subject (top 33)

Run: python jobs/ingest_gus_bdl.py
"""
from __future__ import annotations
import datetime as dt, json, os, re, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "gus")
BASE = "https://bdl.stat.gov.pl/api/v1"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
RATE = 0.12
POLAND_ID = "000000000000"  # Poland national aggregate unit
CATALOG_FILE = os.path.join(OUT, "_catalog.json")


def log(m):
    try:
        print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
    except UnicodeEncodeError:
        print(f"[{time.strftime('%H:%M:%S')}] {str(m).encode('ascii','replace').decode()}", flush=True)


def get_json(url: str, retries: int = 3) -> dict | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=60)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 404):
                return None
            if r.status_code == 429:
                log("  429, sleeping 60s"); time.sleep(60); continue
            log(f"  HTTP {r.status_code}: {url[-80:]}")
        except Exception as e:
            log(f"  ERR: {e}")
        time.sleep(5 * (attempt + 1))
    return None


def get_all_pages(url_base: str) -> list[dict]:
    """Paginate through all pages of an endpoint."""
    results = []
    page = 0
    while True:
        sep = "&" if "?" in url_base else "?"
        url = f"{url_base}{sep}page={page}&page-size=100"
        data = get_json(url)
        if not data:
            break
        batch = data.get("results", [])
        if not batch:
            break
        results.extend(batch)
        if len(results) >= data.get("totalRecords", len(results)):
            break
        page += 1
        time.sleep(RATE)
    return results


def crawl_subjects() -> dict[str, list[str]]:
    """
    Returns dict: k_subject_id → list of P-level subject IDs (hasVariables=true)
    """
    if os.path.exists(CATALOG_FILE):
        with open(CATALOG_FILE) as f:
            cat = json.load(f)
        log(f"Loaded catalog: {sum(len(v) for v in cat.values())} P-subjects across {len(cat)} K-subjects")
        return cat

    log("Crawling GUS BDL subject hierarchy...")
    catalog: dict[str, list[str]] = {}

    # Get K-level subjects
    k_subjects = get_all_pages(f"{BASE}/subjects?lang=en&format=json")
    log(f"  {len(k_subjects)} K-level subjects")

    for k in k_subjects:
        kid = k["id"]
        p_subjects = []

        # BFS through G-level → P-level
        queue = [child for child in k.get("children", [])]
        visited = set()

        while queue:
            gid = queue.pop(0)
            if gid in visited:
                continue
            visited.add(gid)

            data = get_json(f"{BASE}/subjects/{gid}?lang=en&format=json")
            time.sleep(RATE)
            if not data:
                continue

            if data.get("hasVariables"):
                p_subjects.append(gid)
            else:
                # Add children to queue
                for child in data.get("children", []):
                    if child not in visited:
                        queue.append(child)

        catalog[kid] = p_subjects
        log(f"  K={kid} ({k['name'][:40]}): {len(p_subjects)} P-subjects")
        time.sleep(RATE)

    os.makedirs(OUT, exist_ok=True)
    with open(CATALOG_FILE, "w") as f:
        json.dump(catalog, f)
    log(f"Catalog saved: {sum(len(v) for v in catalog.values())} P-subjects total")
    return catalog


def get_variable_ids(p_subject_id: str) -> list[int]:
    """Get all variable IDs for a P-level subject."""
    vars_data = get_all_pages(f"{BASE}/variables?lang=en&format=json&subject-id={p_subject_id}")
    time.sleep(RATE)
    return [v["id"] for v in vars_data if isinstance(v.get("id"), int)]


def fetch_variable_data(var_id: int, p_id: str) -> list[tuple[str, dt.date, float]]:
    """Fetch national-level annual data for one variable."""
    results = []
    url = f"{BASE}/data/by-variable/{var_id}?lang=en&format=json&unit-id={POLAND_ID}&level=0&page-size=100"
    data = get_json(url)
    if not data:
        return results

    series_key = f"GUS:{p_id}:{var_id}"

    for unit in data.get("results", []):
        if unit.get("id") != POLAND_ID:
            continue
        for obs in unit.get("values", []):
            year_str = str(obs.get("year", ""))
            val = obs.get("val")
            attr_id = obs.get("attrId", 1)
            # attrId=2 means data not available/preliminary; skip flags
            if val is None or attr_id in (2, 3, 9):
                continue
            if not year_str.isdigit():
                continue
            try:
                yr = int(year_str)
                if 1990 <= yr <= 2030:
                    results.append((series_key, dt.date(yr, 12, 31), float(val)))
            except (ValueError, TypeError):
                continue
    return results


def main():
    os.makedirs(OUT, exist_ok=True)

    catalog = crawl_subjects()
    total_p = sum(len(v) for v in catalog.values())
    log(f"Processing {total_p} P-subjects across {len(catalog)} K-subjects")

    grand_total = 0

    for kid, p_ids in sorted(catalog.items()):
        out_path = os.path.join(OUT, f"{kid}.parquet")
        if os.path.exists(out_path):
            n = pq.read_metadata(out_path).num_rows
            log(f"  Skip {kid}: {n:,} rows")
            grand_total += n
            continue

        log(f"  K-subject {kid}: {len(p_ids)} P-subjects")
        all_keys, all_dates, all_vals = [], [], []
        seen: set[tuple] = set()
        done_vars = 0

        for p_id in p_ids:
            var_ids = get_variable_ids(p_id)
            log(f"    P={p_id}: {len(var_ids)} variables")

            for var_id in var_ids:
                try:
                    rows = fetch_variable_data(var_id, p_id)
                    for sk, d, v in rows:
                        tok = (sk, d)
                        if tok not in seen:
                            seen.add(tok)
                            all_keys.append(sk)
                            all_dates.append(d)
                            all_vals.append(v)
                except Exception as e:
                    log(f"    ERR var {var_id}: {e}")
                time.sleep(RATE)
                done_vars += 1

                # Checkpoint every 500 variables
                if done_vars % 500 == 0 and all_vals:
                    tbl = pa.table({
                        "series_key": pa.array(all_keys,  pa.string()),
                        "obs_date":   pa.array(all_dates, pa.date32()),
                        "value":      pa.array(all_vals,  pa.float64()),
                    })
                    pq.write_table(tbl, out_path, compression="zstd")
                    log(f"    Checkpoint: {len(all_vals):,} obs ({done_vars} vars)")

        if all_vals:
            tbl = pa.table({
                "series_key": pa.array(all_keys,  pa.string()),
                "obs_date":   pa.array(all_dates, pa.date32()),
                "value":      pa.array(all_vals,  pa.float64()),
            })
            pq.write_table(tbl, out_path, compression="zstd")
            n = pq.read_metadata(out_path).num_rows
            log(f"  {kid}: DONE {n:,} obs")
            grand_total += n
        else:
            log(f"  {kid}: 0 obs")

    log(f"DONE: {grand_total:,} total GUS Poland observations")


if __name__ == "__main__":
    main()
