#!/usr/bin/env python3
"""IDB (Inter-American Development Bank) CKAN open-data ingest.

Source: https://data.iadb.org  (CKAN portal, no API key required)
License: CC BY (IDB open data)

Fetches all 217+ datasets from the IDB CKAN portal via the datastore API.
Converts each resource to long-format parquet: {series_key, obs_date, value}.

For datasets with clear indicator+country+date+value structure, these are used
directly. For other shapes, numeric columns become separate series.

Run: python jobs/ingest_idb.py
     python jobs/ingest_idb.py --list    # list packages, no download
"""
from __future__ import annotations
import datetime as dt, json, os, sys, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "idb")
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
BASE = "https://data.iadb.org/api/3/action"
RATE = 0.3       # seconds between requests
PAGE = 5000      # rows per page
MAX_RESOURCE_ROWS = 500_000   # skip panel/micro datasets that won't parse to long format


def log(m):
    try:
        print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
    except UnicodeEncodeError:
        print(f"[{time.strftime('%H:%M:%S')}] {str(m).encode('ascii','replace').decode()}", flush=True)


def get_json(url: str, params: dict | None = None, retries: int = 4,
             timeout: int = 60) -> dict | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=UA, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 404):
                return None
            if r.status_code == 429:
                log("  429 rate-limit, sleeping 60s"); time.sleep(60); continue
            log(f"  HTTP {r.status_code} attempt {attempt+1}: {url[-80:]}")
        except Exception as e:
            log(f"  ERR attempt {attempt+1}: {e}")
        time.sleep(5 * (attempt + 1))
    return None


def parse_date(s: str | int | None) -> dt.date | None:
    """Best-effort date parse from heterogeneous IDB date strings."""
    if s is None:
        return None
    s = str(s).strip()
    try:
        if len(s) == 4 and s.isdigit():
            return dt.date(int(s), 12, 31)
        if len(s) == 7 and s[4] == "-":
            return dt.date(int(s[:4]), int(s[5:7]), 1)
        if len(s) == 10 and s[4] == "-":
            return dt.date.fromisoformat(s[:10])
        if len(s) == 19 and s[4] == "-":     # datetime string
            return dt.date.fromisoformat(s[:10])
        if s.isdigit() and 1900 <= int(s) <= 2100:
            return dt.date(int(s), 12, 31)
    except (ValueError, TypeError):
        pass
    return None


def detect_columns(fields: list[dict]) -> dict:
    """Detect date, country, indicator, value columns by name heuristics."""
    names = {f["id"].lower(): f["id"] for f in fields if f["id"] != "_id"}
    out = {}

    # Date column candidates (in priority order)
    for cand in ["dt", "date", "year", "periodo", "periodo_end",
                 "anio", "ano", "fecha", "time", "period"]:
        if cand in names:
            out["date"] = names[cand]; break

    # Country column
    for cand in ["isoalpha3", "iso3", "iso_alpha3", "country_iso",
                 "countryiso3code", "country", "pais", "isoalpha2", "iso"]:
        if cand in names:
            out["country"] = names[cand]; break

    # Indicator column
    for cand in ["indicator", "indicador", "variable", "series",
                 "indicator_code", "ind_code", "varname"]:
        if cand in names:
            out["indicator"] = names[cand]; break

    # Value column
    for cand in ["value", "valor", "val", "obs_value", "data", "amount"]:
        if cand in names:
            out["value"] = names[cand]; break

    return out


def fetch_resource_all_rows(rid: str, total: int) -> list[dict]:
    """Paginate through all rows of a datastore resource."""
    rows = []
    offset = 0
    while offset < total:
        j = get_json(f"{BASE}/datastore_search",
                     params={"resource_id": rid, "limit": PAGE, "offset": offset})
        if not j:
            break
        batch = j.get("result", {}).get("records", [])
        if not batch:
            break
        rows.extend(batch)
        offset += len(batch)
        time.sleep(RATE)
    return rows


def rows_to_long(rows: list[dict], pkg_slug: str, res_name: str,
                 fields: list[dict]) -> tuple[list, list, list]:
    """Convert raw CKAN rows to long format (series_key, obs_date, value) lists."""
    col = detect_columns(fields)
    all_keys, all_dates, all_vals = [], [], []

    # Get numeric field names (excluding id, known non-numeric cols)
    skip_fields = {"_id"}
    if "date" in col:
        skip_fields.add(col["date"].lower())
    if "country" in col:
        skip_fields.add(col["country"].lower())
    if "indicator" in col:
        skip_fields.add(col["indicator"].lower())

    numeric_fields = []
    for f in fields:
        fname = f["id"]
        if fname.lower() in skip_fields:
            continue
        ftype = f.get("type", "")
        if ftype in ("int", "float", "numeric", "number", "double precision",
                     "integer", "bigint", "real"):
            numeric_fields.append(fname)

    for rec in rows:
        # Get date
        d = None
        if "date" in col:
            d = parse_date(rec.get(col["date"]))
        elif "year" in {f["id"].lower() for f in fields}:
            yr = rec.get("year")
            if yr:
                d = parse_date(yr)
        if d is None:
            continue

        # Build country suffix
        ctry = ""
        if "country" in col:
            ctry = str(rec.get(col["country"], "") or "")

        # Build indicator prefix
        ind = ""
        if "indicator" in col:
            ind = str(rec.get(col["indicator"], "") or "")

        if "value" in col:
            # Already narrow format: one value column
            raw_v = rec.get(col["value"])
            if raw_v not in (None, "", "N/A", "NA"):
                try:
                    v = float(raw_v)
                    parts = [x for x in [pkg_slug, ind, ctry] if x]
                    key = "IDB:" + ":".join(parts)
                    all_keys.append(key); all_dates.append(d); all_vals.append(v)
                except (TypeError, ValueError):
                    pass
        else:
            # Wide format: each numeric column is a series
            for fname in numeric_fields:
                raw_v = rec.get(fname)
                if raw_v not in (None, "", "N/A", "NA"):
                    try:
                        v = float(raw_v)
                        parts = [x for x in [pkg_slug, fname, ctry] if x]
                        key = "IDB:" + ":".join(parts)
                        all_keys.append(key); all_dates.append(d); all_vals.append(v)
                    except (TypeError, ValueError):
                        pass

    return all_keys, all_dates, all_vals


def ingest_package(pkg: dict) -> int:
    slug = pkg.get("name", "")
    title = str(pkg.get("title") or "")[:60]
    resources = pkg.get("resources", [])

    pkg_rows = 0
    for res in resources:
        if not res.get("datastore_active"):
            continue
        rid = res.get("id", "")
        rname = (res.get("name") or res.get("description") or slug)[:40]
        out_path = os.path.join(OUT, f"{slug}__{rid[:8]}.parquet")
        if os.path.exists(out_path):
            n = pq.read_metadata(out_path).num_rows
            log(f"    skip {rname}: {n:,} rows")
            pkg_rows += n
            continue

        # Get total row count and field definitions
        j0 = get_json(f"{BASE}/datastore_search",
                      params={"resource_id": rid, "limit": 0})
        if not j0:
            continue
        result0 = j0.get("result", {})
        total = result0.get("total", 0)
        fields = result0.get("fields", [])
        if total == 0:
            continue
        if total > MAX_RESOURCE_ROWS:
            log(f"    {rname}: {total:,} rows -> SKIP (>{MAX_RESOURCE_ROWS:,} row limit)")
            continue

        log(f"    {rname}: {total:,} rows, {len(fields)} fields -> fetching...")
        rows = fetch_resource_all_rows(rid, total)
        if not rows:
            continue

        keys, dates, vals = rows_to_long(rows, slug, rname, fields)
        if not vals:
            log(f"      -> 0 convertible obs (no date+value pattern)")
            continue

        tbl = pa.table({
            "series_key": pa.array(keys,  pa.string()),
            "obs_date":   pa.array(dates, pa.date32()),
            "value":      pa.array(vals,  pa.float64()),
        })
        pq.write_table(tbl, out_path, compression="zstd")
        n = pq.read_metadata(out_path).num_rows
        log(f"      -> {n:,} obs saved")
        pkg_rows += n

    return pkg_rows


def main():
    list_only = "--list" in sys.argv
    os.makedirs(OUT, exist_ok=True)

    log("Fetching IDB CKAN package list...")
    j = get_json(f"{BASE}/package_list")
    if not j:
        log("ERROR: could not fetch package list"); return
    slugs = j.get("result", [])
    log(f"Found {len(slugs)} packages")

    if list_only:
        for s in slugs:
            print(f"  {s}")
        return

    total_obs = 0
    for i, slug in enumerate(slugs, 1):
        # Check if all resources for this package already exist
        j2 = get_json(f"{BASE}/package_show", params={"id": slug})
        if not j2:
            continue
        pkg = j2.get("result", {})
        if not isinstance(pkg, dict):
            continue
        title = str(pkg.get("title") or slug)[:60]
        active = [r for r in pkg.get("resources", []) if r.get("datastore_active")]
        if not active:
            time.sleep(RATE)
            continue

        log(f"[{i}/{len(slugs)}] {slug[:50]} ({len(active)} active resources)")
        n = ingest_package(pkg)
        total_obs += n
        time.sleep(RATE)

    log(f"GRAND TOTAL: {total_obs:,} IDB observations")


if __name__ == "__main__":
    main()
