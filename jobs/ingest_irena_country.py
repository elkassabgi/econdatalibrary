#!/usr/bin/env python3
"""IRENA country-level electricity data via batched PxWeb requests.
Extends ingest_irena.py for the large tables that reject full queries.
Run: python jobs/ingest_irena_country.py
"""
from __future__ import annotations
import datetime as dt, json, os, re, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "irena")
BASE = "https://pxweb.irena.org/api/v1/en/IRENASTAT"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
        "Content-Type": "application/json"}
BATCH = 20   # countries per request

LARGE_TABLES = {
    "Power Capacity and Generation": [
        "Country_ELECCAP_2026_H1_v-PX 1.px",
        "Country_ELECGEN_2025_H2_v-PX 1.px",
    ],
    "Finance": [
        "PUBFIN_2025_H2_PX.px",
    ],
}

def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)

def get_json(url):
    for attempt in range(3):
        try:
            r = requests.get(url, headers=UA, timeout=60)
            if r.status_code == 200: return r.json()
        except Exception as e:
            log(f"  ERR: {e}")
        time.sleep(5 * (attempt + 1))
    return None

def post_json(url, body):
    for attempt in range(3):
        try:
            r = requests.post(url, json=body, headers=UA, timeout=120)
            if r.status_code == 200: return r.json()
            if r.status_code == 400:
                log(f"  400 bad request"); return None
            log(f"  POST {r.status_code}")
        except Exception as e:
            log(f"  POST ERR: {e}")
        time.sleep(5 * (attempt + 1))
    return None

def parse_jsonstat2(resp, prefix):
    if not resp or "dimension" not in resp: return [], [], []
    dims = resp["dimension"]; size = resp["size"]; ids = resp["id"]
    vals_raw = resp.get("value", [])
    
    time_dim = None
    for dim_id in ids:
        cats = list(dims[dim_id]["category"]["label"].values())
        if cats and all(re.match(r"^\d{4}", str(v)) for v in cats):
            time_dim = dim_id; break
    if not time_dim: return [], [], []
    
    time_cats = list(dims[time_dim]["category"]["label"].values())
    series_dims = [d for d in ids if d != time_dim]
    
    strides = [1] * len(ids)
    for i in range(len(ids) - 2, -1, -1):
        strides[i] = strides[i+1] * size[i+1]
    time_i = ids.index(time_dim)
    
    def parse_yr(s):
        m = re.match(r"^(\d{4})", str(s))
        return int(m.group(1)) if m else None

    time_dates = [dt.date(y, 12, 31) if (y:=parse_yr(c)) else None for c in time_cats]
    
    series_cats = []
    for sd in series_dims:
        series_cats.append(list(dims[sd]["category"]["label"].values()))
    
    keys, dates, vals = [], [], []
    for flat_idx, val in enumerate(vals_raw):
        if val is None: continue
        try: v = float(val)
        except: continue
        if v != v: continue
        
        idx = flat_idx
        dim_indices = []
        for i, s in enumerate(size):
            dim_indices.append(idx // strides[i])
            idx %= strides[i]
        
        ti = dim_indices[time_i]
        if ti >= len(time_dates) or not time_dates[ti]: continue
        
        parts = []
        for si, sd in enumerate(series_dims):
            di = ids.index(sd); ci = dim_indices[di]
            if ci < len(series_cats[si]):
                parts.append(str(series_cats[si][ci]).replace("|","_"))
        
        keys.append(f"{prefix}|{'|'.join(parts)}")
        dates.append(time_dates[ti])
        vals.append(v)
    
    return keys, dates, vals

def ingest_large_table(category, table_id):
    safe = re.sub(r"[^\w]", "_", table_id)[:80]
    out_path = os.path.join(OUT, f"ctry_{safe}.parquet")
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"  [{table_id[:40]}] already {n:,} rows"); return n
    
    cat_enc = requests.utils.quote(category)
    tbl_enc = requests.utils.quote(table_id)
    url = f"{BASE}/{cat_enc}/{tbl_enc}"
    
    meta = get_json(url)
    if not meta: return 0
    
    # Find country-like dimension
    ctry_var = None
    for v in meta["variables"]:
        if v["code"].lower() in ("country/area", "country", "countries"):
            ctry_var = v; break
    if not ctry_var:
        ctry_var = meta["variables"][0]  # assume first dim is geographic
    
    all_ctry = ctry_var["values"]
    other_vars = [v for v in meta["variables"] if v["code"] != ctry_var["code"]]
    
    log(f"  [{table_id[:40]}] {len(all_ctry)} countries, batching {BATCH} at a time...")
    
    all_keys, all_dates, all_vals = [], [], []
    
    for batch_start in range(0, len(all_ctry), BATCH):
        batch = all_ctry[batch_start:batch_start + BATCH]
        query = [
            {"code": ctry_var["code"], "selection": {"filter": "item", "values": batch}}
        ] + [
            {"code": v["code"], "selection": {"filter": "all", "values": ["*"]}}
            for v in other_vars
        ]
        body = {"query": query, "response": {"format": "json-stat2"}}
        resp = post_json(url, body)
        if not resp: continue
        
        k, d, v = parse_jsonstat2(resp, f"IRENA:{table_id}")
        all_keys.extend(k); all_dates.extend(d); all_vals.extend(v)
        time.sleep(0.5)
    
    if not all_keys:
        log(f"  [{table_id[:40]}] 0 obs"); return 0
    
    tbl = pa.table({
        "series_key": pa.array(all_keys, pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"  [{table_id[:40]}] -> {n:,} obs saved")
    return n

def main():
    os.makedirs(OUT, exist_ok=True)
    log("=== IRENA Country-Level Tables ===")
    grand = 0
    for cat, tables in LARGE_TABLES.items():
        for tbl_id in tables:
            grand += ingest_large_table(cat, tbl_id)
            time.sleep(1)
    log(f"=== IRENA Country TOTAL: {grand:,} obs ===")

if __name__ == "__main__":
    main()
