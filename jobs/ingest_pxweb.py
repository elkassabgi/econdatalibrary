#!/usr/bin/env python3
"""Generic PxWeb ingest -- covers SCB (Sweden CC0), SSB (Norway CC BY 4.0),
Statistics Finland (CC BY 4.0), Statistics Denmark StatBank (CC BY 4.0),
CSO Ireland (Open Gov), FSO Switzerland (Swiss OGD).

PxWeb is an open-source API used by Nordic and other NSOs. All return
JSON-stat or custom JSON with the same query model.

Run: python jobs/ingest_pxweb.py <provider>
  providers: scb ssb statfin dst cso bfs

SCB (Sweden):     https://api.scb.se/OV0104/v1/doris/en/ssd/
SSB (Norway):     https://data.ssb.no/api/v0/en/
StatFin (Finland):https://pxdata.stat.fi/PXWeb/api/v1/en/
DST (Denmark):    https://api.statbank.dk/v1/
CSO (Ireland):    https://data.cso.ie/api/v1.0/
BFS (Switzerland):https://www.pxweb.bfs.admin.ch/api/v1/en/

License: CC0 (SCB), CC BY 4.0 (SSB, StatFin, DST), Open Gov (CSO, BFS)
"""
from __future__ import annotations
import os, sys, time, json, datetime as dt
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
sys.path.insert(0, ROOT)

UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
      "Accept": "application/json"}

PROVIDERS = {
    "scb": {
        "base": "https://api.scb.se/OV0104/v1/doris/sv/ssd/",  # Swedish API has full coverage; EN subset causes 400s
        "name": "Statistics Sweden",
        "license": "CC0",
        "rate": 0.5,   # 10 req/10s
    },
    "ssb": {
        "base": "https://data.ssb.no/api/v0/en/table/",  # root returns {dbid} format; real PxWeb tree starts at /table/
        "name": "Statistics Norway",
        "license": "CC BY 4.0",
        "rate": 1.0,
    },
    "statfin": {
        "base": "https://pxdata.stat.fi/PXWeb/api/v1/en/",
        "name": "Statistics Finland",
        "license": "CC BY 4.0",
        "rate": 1.0,
    },
    "dst": {
        "base": "https://api.statbank.dk/v1/",
        "name": "Statistics Denmark",
        "license": "CC BY 4.0",
        "rate": 0.5,
        "mode": "statbank",  # different API format
    },
    "cso": {
        "base": "https://data.cso.ie/api/v1.0/",
        "name": "CSO Ireland",
        "license": "Open Government",
        "rate": 1.0,
    },
    "bfs": {
        "base": "https://www.pxweb.bfs.admin.ch/api/v1/en/",
        "name": "Swiss Federal Statistics",
        "license": "Swiss OGD",
        "rate": 2.0,
    },
}

SCHEMA = pa.schema([
    ("provider", pa.string()), ("table_id", pa.string()),
    ("series_key", pa.string()), ("obs_date", pa.date32()),
    ("value", pa.float64()),
])


def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def get(url, timeout=60):
    for attempt in range(4):
        try:
            r = requests.get(url, headers=UA, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(30 * (attempt + 1)); continue
            log(f"  HTTP {r.status_code}: {url[-70:]}")
            return None
        except Exception as e:
            log(f"  ERR {e} attempt {attempt+1}")
            time.sleep(5 * (attempt + 1))
    return None


def post_json(url, payload, timeout=120):
    for attempt in range(4):
        try:
            r = requests.post(url, json=payload, headers=UA, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(30 * (attempt + 1)); continue
            log(f"  POST HTTP {r.status_code}: {url[-70:]}")
            return None
        except Exception as e:
            log(f"  POST ERR {e} attempt {attempt+1}")
            time.sleep(5 * (attempt + 1))
    return None


import re

# Codes that literally name a PxWeb time dimension (a real "Tid"/"Time" must win).
TIME_CODES = ("tid", "time", "year", "ar", "år", "period", "datum", "date",
              "manad", "månad", "maaned", "måned", "month", "kvartal", "kvartaal",
              "quarter", "uke", "vecka", "week", "leto", "mesec", "kuukausi",
              "vuosi", "neljannes")


def parse_period(s):
    """Parse PxWeb period codes: 2023, 2023M01, 2023Q1/2023K1, 2023W01, 2023H1,
    2023-01, 2023-01-15, YYYYMM. Returns None for anything that is not a real,
    SANE-range time code — non-time numeric category codes (municipality codes,
    8-digit ContentsCode values, out-of-range years like 2584/9999) must NOT parse,
    because that is exactly how garbage obs_dates were written."""
    s = (s or "").strip()
    try:
        # Annual: 2023
        if re.match(r"^\d{4}$", s):
            return dt.date(int(s), 12, 31)
        # Monthly: 2023M01
        m = re.match(r"^(\d{4})M(\d{2})$", s, re.IGNORECASE)
        if m:
            return dt.date(int(m.group(1)), int(m.group(2)), 1)
        # Monthly: 2023-01
        m = re.match(r"^(\d{4})-(\d{2})$", s)
        if m:
            return dt.date(int(m.group(1)), int(m.group(2)), 1)
        # Quarterly: 2023Q1 or 2023K1 (Nordic kvartal)
        m = re.match(r"^(\d{4})[QK](\d)$", s, re.IGNORECASE)
        if m:
            q = int(m.group(2))
            return dt.date(int(m.group(1)), (q - 1) * 3 + 1, 1)
        # Half-year: 2023H1
        m = re.match(r"^(\d{4})H(\d)$", s, re.IGNORECASE)
        if m:
            return dt.date(int(m.group(1)), 1 if m.group(2) == "1" else 7, 1)
        # Weekly: 2023W01
        m = re.match(r"^(\d{4})W(\d{2})$", s, re.IGNORECASE)
        if m:
            return dt.date.fromisocalendar(int(m.group(1)), int(m.group(2)), 1)
        # Full date: 2023-01-15
        if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
            return dt.date.fromisoformat(s)
        # YYYYMM (6 digits) — but reject impossible months so e.g. '095001' (a code)
        # is not read as 0950-01.
        m = re.match(r"^(\d{4})(\d{2})$", s)
        if m and 1 <= int(m.group(2)) <= 12:
            return dt.date(int(m.group(1)), int(m.group(2)), 1)
    except Exception:
        pass
    return None


def is_time_dim(code, values):
    """A dimension is 'time' ONLY if its code is a known time code, OR its values
    parse via parse_period() to a SANE year (~1900..current_year+2).

    The previous ingester had NO value-level guard at all: it matched a dimension by
    name and otherwise fell back to dims[-1], and parse_period() accepted any bare
    4-digit / 6-digit number. That made non-time numeric category codes (a ContentsCode
    like '00000858', a category code '2584', a municipality code) get read as the time
    axis, writing garbage obs_dates such as 2584-12-31. Anchoring on parse_period() plus
    a sane year range removes the false match."""
    if str(code).strip().lower() in TIME_CODES:
        return True
    if values:
        sample = [str(v).strip() for v in values[:8]]
        cur = dt.date.today().year
        sane = sum(1 for v in sample
                   if (d := parse_period(v)) is not None and 1900 <= d.year <= cur + 2)
        if sample and sane >= max(1, int(len(sample) * 0.6)):
            return True
    return False


def crawl_pxweb_tables(base_url, path="", depth=0, max_depth=8):
    """Recursively walk PxWeb directory tree, return list of table URLs."""
    if depth > max_depth:
        return []
    url = base_url + path
    data = get(url)
    if not data:
        return []
    tables = []
    if isinstance(data, list):
        for item in data:
            item_id = item.get("id", "")
            item_type = item.get("type", "")
            if item_type == "t":  # table
                tables.append(path + "/" + item_id if path else item_id)
            elif item_type == "l":  # level (directory) only — "h" is heading/label, not a subfolder
                sub = path + "/" + item_id if path else item_id
                tables.extend(crawl_pxweb_tables(base_url, sub, depth+1, max_depth))
    return tables


def ingest_pxweb_table(base_url, table_path, provider_key, out_dir, rate):
    """Download one PxWeb table and save as Parquet."""
    safe_name = table_path.replace("/", "__").strip("_") + ".parquet"
    out_path = os.path.join(out_dir, safe_name)
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"  skip {table_path} ({n:,} rows)")
        return n

    # Get table metadata
    meta_url = base_url + table_path
    meta = get(meta_url)
    if not meta:
        return 0

    # Build query: select all values for all variables
    variables = meta.get("variables", [])
    if not variables:
        return 0

    # Estimate total cells (cross-product) — cap to avoid huge requests
    total_cells = 1
    for var in variables:
        total_cells *= max(1, len(var.get("values", [])))

    # Authoritative time dimension: PxWeb metadata flags the time variable `time: true`.
    # Prefer it; only fall back to the name/value heuristic when the flag is absent.
    meta_time_code = next(
        (v.get("code") for v in variables if v.get("time") is True), None)

    # If too large, limit time dimension to last 200 periods
    MAX_CELLS = 50000
    time_var = meta_time_code
    query = {"query": [], "response": {"format": "json-stat2"}}
    for var in variables:
        code = var.get("code", "")
        values = var.get("values", [])
        if not values:
            continue
        # Detect time variable only if metadata didn't already give us one.
        if time_var is None and is_time_dim(code, values):
            time_var = code
        # Use explicit "item" filter (not "all"/"*" wildcard — many PxWeb servers reject it)
        sel_values = values[-200:] if (time_var == code and total_cells > MAX_CELLS) else values
        query["query"].append({
            "code": code,
            "selection": {"filter": "item", "values": sel_values}
        })

    # POST query to get data
    data_url = base_url + table_path.rstrip("/")
    result = post_json(data_url, query)
    if not result:
        return 0

    # Parse JSON-stat2
    dims = result.get("id", [])
    sizes = result.get("size", [])
    values_map = result.get("dimension", {})
    obs_values = result.get("value", [])

    if not obs_values or not dims:
        return 0

    # Build index iterators (code maps) for all dimensions first — needed for
    # value-based time detection below.
    dim_labels = {}
    dim_codes = {}   # dim -> {position(str): code}
    for d in dims:
        dim_info = values_map.get(d, {})
        cats = dim_info.get("category", {})
        index = cats.get("index", {})
        labels = cats.get("label", {})
        if isinstance(index, list):
            dim_labels[d] = {str(i): k for i, k in enumerate(index)}
            dim_codes[d] = {str(i): c for i, c in enumerate(index)}
        elif isinstance(index, dict):
            dim_labels[d] = {v: k for k, v in index.items()}
            dim_codes[d] = {str(pos): code for code, pos in index.items()}
        else:
            dim_labels[d] = {str(i): k for i, k in enumerate(labels.keys())}
            dim_codes[d] = {str(i): c for i, c in enumerate(labels.keys())}

    # Identify the time dimension.
    # 1) AUTHORITATIVE: the PxWeb metadata `time: true` flag (passed in via meta_time_code).
    # 2) pass 1 fallback: a dimension whose CODE literally names a time dim.
    # 3) pass 2 fallback: a dimension whose VALUES parse as SANE dates.
    # A literally-named/flagged time dim ALWAYS wins over a non-time numeric category
    # whose codes merely look date-ish.
    def _codes_for(d):
        cm = dim_codes.get(d, {})
        return [cm[str(i)] for i in range(len(cm)) if str(i) in cm]

    time_dim = None
    if meta_time_code and meta_time_code in dims:
        time_dim = meta_time_code
    if time_dim is None:
        for d in dims:                      # pass 1: literally-named time dim
            if str(d).strip().lower() in TIME_CODES:
                time_dim = d
                break
    if time_dim is None:
        for d in dims:                      # pass 2: values parse as sane dates
            if is_time_dim(d, _codes_for(d)):
                time_dim = d
                break
    if time_dim is None:
        return 0   # never guess dims[-1]: that wrote garbage obs_dates

    # Flatten observations
    rows_prov, rows_table, rows_key, rows_date, rows_val = [], [], [], [], []
    n_dims = len(dims)
    n_obs = len(obs_values)

    # Compute strides
    strides = [1] * n_dims
    for i in range(n_dims - 2, -1, -1):
        strides[i] = strides[i+1] * sizes[i+1]

    time_idx = dims.index(time_dim) if time_dim in dims else -1

    for flat_idx, val in enumerate(obs_values):
        if val is None:
            continue
        try:
            v = float(val)
        except (TypeError, ValueError):
            continue

        # Decompose flat index into per-dimension indices
        coords = {}
        remainder = flat_idx
        for di, d in enumerate(dims):
            ci = remainder // strides[di]
            remainder = remainder % strides[di]
            coords[d] = ci

        # Get time value. Parse the canonical PxWeb period CODE first (e.g. 2023M01);
        # fall back to the (possibly localized) label only if the code does not parse.
        if time_idx >= 0:
            t_cat_idx = coords[dims[time_idx]]
            t_code = dim_codes[dims[time_idx]].get(str(t_cat_idx), "")
            obs_date = parse_period(t_code)
            if obs_date is None:
                t_label = dim_labels[dims[time_idx]].get(str(t_cat_idx), "")
                obs_date = parse_period(t_label)
            if obs_date is None:
                continue
            # Final safety net: even on the correctly-selected time dim, refuse any
            # obs_date outside a sane range. Legitimate Nordic/Baltic population
            # projections run to ~2100, so allow up to 2100; reject sentinels
            # (9999) and sub-1900 codes that should never appear on a real time axis.
            if not (1900 <= obs_date.year <= 2100):
                continue
        else:
            continue

        # Build series key from non-time dims
        key_parts = []
        for di, d in enumerate(dims):
            if d == time_dim:
                continue
            ci = coords[d]
            label = dim_labels[d].get(str(ci), str(ci))
            key_parts.append(f"{d}={label}")
        series_key = ":".join(key_parts)

        rows_prov.append(provider_key)
        rows_table.append(table_path)
        rows_key.append(series_key)
        rows_date.append(obs_date)
        rows_val.append(v)

    if not rows_val:
        return 0

    tbl = pa.table({
        "provider": pa.array(rows_prov, pa.string()),
        "table_id": pa.array(rows_table, pa.string()),
        "series_key": pa.array(rows_key, pa.string()),
        "obs_date": pa.array(rows_date, pa.date32()),
        "value": pa.array(rows_val, pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"  {table_path}: {n:,} obs")
    return n


def ingest_dst(out_dir, rate):
    """Statistics Denmark StatBank — different API format."""
    provider_key = "dst"
    base = PROVIDERS["dst"]["base"]

    # Get table list
    tables_meta = get(base + "tables?lang=en&format=JSON")
    if not tables_meta:
        log("DST: failed to get table list"); return 0

    log(f"DST: {len(tables_meta)} tables")
    total = 0
    for i, tbl in enumerate(tables_meta, 1):
        tid = tbl.get("id", "")
        if not tid:
            continue
        safe = f"DST__{tid}.parquet"
        out_path = os.path.join(out_dir, safe)
        if os.path.exists(out_path):
            n = pq.read_metadata(out_path).num_rows
            log(f"  [{i}/{len(tables_meta)}] {tid}: skip ({n:,})")
            total += n
            continue

        # Get table metadata
        meta = get(base + f"tableinfo?id={tid}&lang=en&format=JSON")
        if not meta:
            continue

        variables = meta.get("variables", [])
        # Find time variable
        time_var = next((v for v in variables if v.get("time") is True), None)
        if not time_var:
            time_var = variables[-1] if variables else None

        # Build query selecting all values
        query_vars = []
        for var in variables:
            vals = [v["id"] for v in var.get("values", [])]
            if vals:
                query_vars.append({"code": var["id"], "values": vals})

        payload = {
            "table": tid, "format": "SDMX-Compact-2.0",
            "valuePresentation": "Value", "variables": query_vars
        }
        # DST returns CSV for bulk
        try:
            r = requests.post(base + "data", json=payload,
                              headers=UA, timeout=120)
            if r.status_code != 200:
                log(f"  [{i}] {tid}: HTTP {r.status_code}")
                time.sleep(rate); continue
        except Exception as e:
            log(f"  [{i}] {tid}: ERR {e}")
            time.sleep(rate); continue

        # Parse SDMX-Compact CSV — simpler: request CSV instead
        payload2 = {**payload, "format": "CSV"}
        try:
            r2 = requests.post(base + "data", json=payload2,
                               headers=UA, timeout=120)
            if r2.status_code != 200:
                time.sleep(rate); continue
            lines = r2.text.split("\n")
            if len(lines) < 2:
                time.sleep(rate); continue
            headers = lines[0].strip().split(";")
        except Exception:
            time.sleep(rate); continue

        # Find TID column
        tid_col = next((j for j, h in enumerate(headers)
                        if h.upper() in ("TID", "TIME PERIOD", "TIME")), -1)
        val_col = next((j for j, h in enumerate(headers)
                        if h.upper() in ("INDHOLD", "VALUE", "OBS_VALUE")), -1)
        if tid_col < 0 or val_col < 0:
            time.sleep(rate); continue

        key_cols = [j for j in range(len(headers))
                    if j != tid_col and j != val_col]

        rows_prov, rows_table, rows_key, rows_date, rows_val = [], [], [], [], []
        for line in lines[1:]:
            if not line.strip():
                continue
            parts = line.strip().split(";")
            if len(parts) <= max(tid_col, val_col):
                continue
            d = parse_period(parts[tid_col].strip('"'))
            if d is None:
                continue
            raw_v = parts[val_col].strip().strip('"').replace(",", ".")
            try:
                v = float(raw_v)
            except ValueError:
                continue
            key = ":".join(parts[j].strip('"') for j in key_cols
                           if j < len(parts))
            rows_prov.append(provider_key)
            rows_table.append(tid)
            rows_key.append(key)
            rows_date.append(d)
            rows_val.append(v)

        if not rows_val:
            log(f"  [{i}/{len(tables_meta)}] {tid}: 0 obs")
            time.sleep(rate); continue

        tbl_pa = pa.table({
            "provider": pa.array(rows_prov, pa.string()),
            "table_id": pa.array(rows_table, pa.string()),
            "series_key": pa.array(rows_key, pa.string()),
            "obs_date": pa.array(rows_date, pa.date32()),
            "value": pa.array(rows_val, pa.float64()),
        })
        pq.write_table(tbl_pa, out_path, compression="zstd")
        n = pq.read_metadata(out_path).num_rows
        log(f"  [{i}/{len(tables_meta)}] {tid}: {n:,} obs")
        total += n
        time.sleep(rate)

    return total


def main():
    if len(sys.argv) < 2:
        print("Usage: python ingest_pxweb.py <provider>")
        print("Providers:", ", ".join(PROVIDERS))
        sys.exit(1)

    provider_key = sys.argv[1].lower()
    if provider_key not in PROVIDERS:
        print(f"Unknown provider: {provider_key}")
        sys.exit(1)

    cfg = PROVIDERS[provider_key]
    out_dir = os.path.join(ROOT, "data", "clean_full", provider_key)
    os.makedirs(out_dir, exist_ok=True)
    log(f"Provider: {cfg['name']} | License: {cfg['license']}")

    if cfg.get("mode") == "statbank":
        total = ingest_dst(out_dir, cfg["rate"])
        log(f"DONE: {total:,} obs total")
        return

    log("Crawling table directory...")
    tables = crawl_pxweb_tables(cfg["base"])
    log(f"Found {len(tables)} tables")

    total = 0
    for i, table_path in enumerate(tables, 1):
        log(f"[{i}/{len(tables)}] {table_path}")
        total += ingest_pxweb_table(
            cfg["base"], table_path, provider_key, out_dir, cfg["rate"])
        time.sleep(cfg["rate"])

    log(f"DONE: {total:,} total observations from {cfg['name']}")


if __name__ == "__main__":
    main()
