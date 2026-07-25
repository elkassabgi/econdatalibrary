#!/usr/bin/env python3
"""INSEE Melodi API ingest -- all 109 dataflows, keyless.

Melodi is INSEE's new unified data catalog API. No authentication needed
('libre' / keyless plan). 30 req/min rate limit.

109 dataflows covering: national accounts (CNA), employment, prices,
foreign trade, industry, construction, services, demographics, and more.
Each dataflow's data is fetched from /data/{FLOW_CODE} as JSON.

License: Licence Ouverte / Open Licence 2.0 — commercial redistribution OK.
Attribution: Source: INSEE Melodi (Licence Ouverte 2.0) www.insee.fr
API: https://api.insee.fr/melodi (keyless, 30 req/min)

Run: python jobs/ingest_insee_melodi.py [--dry]
"""
from __future__ import annotations
import datetime as dt, json, os, re, sys, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
sys.path.insert(0, ROOT)

OUT = os.path.join(ROOT, "data", "clean_full", "insee_melodi")
BASE = "https://api.insee.fr/melodi"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
      "Accept": "application/json"}
RATE_DELAY = 2.1   # 30 req/min → 2s between calls

SCHEMA = pa.schema([
    ("flow", pa.string()), ("series_key", pa.string()),
    ("obs_date", pa.date32()), ("value", pa.float64()),
])


def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def get_flows():
    r = requests.get(f"{BASE}/dataflow/all", headers=UA, timeout=30)
    r.raise_for_status()
    return r.json()


def parse_date(s):
    s = (s or "").strip()
    try:
        if re.match(r'\d{4}-Q\d', s):
            y, q = s.split("-Q"); return dt.date(int(y), (int(q)-1)*3+1, 1)
        if re.match(r'\d{4}-\d{2}$', s): return dt.date(int(s[:4]), int(s[5:7]), 1)
        if re.match(r'\d{4}$', s): return dt.date(int(s), 12, 31)
        if re.match(r'\d{4}-\d{2}-\d{2}', s): return dt.date.fromisoformat(s[:10])
    except (ValueError, KeyError): pass
    return None


def ingest_flow(flow_code, flow_name, dry):
    out_path = os.path.join(OUT, f"{flow_code}.parquet")
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"  {flow_code}: skip ({n:,} rows)"); return n

    # Melodi paginates via ?page=N (10k per page). Follow all pages.
    obs_list = []
    page = 1
    while True:
        url = f"{BASE}/data/{flow_code}?page={page}"
        for attempt in range(4):
            try:
                r = requests.get(url, headers=UA, timeout=120)
                if r.status_code == 429:
                    log(f"  rate limit, sleeping 60s"); time.sleep(60); continue
                if r.status_code != 200:
                    log(f"  {flow_code}: HTTP {r.status_code}"); return 0
                data = r.json(); break
            except Exception as e:
                log(f"  {flow_code}: ERR {e} (attempt {attempt+1})"); time.sleep(10)
        else:
            break
        page_obs = data.get("observations", [])
        obs_list.extend(page_obs)
        paging = data.get("paging", {})
        if "next" not in paging or len(page_obs) == 0:
            break
        page += 1
        time.sleep(RATE_DELAY)

    if not obs_list:
        log(f"  {flow_code}: 0 observations"); return 0
    if dry:
        log(f"  {flow_code}: DRY {len(obs_list)} obs"); return len(obs_list)

    # Structure: each obs has dimensions{TIME_PERIOD,...}, attributes{}, measures{OBS_VALUE_*:{value:N}}
    rows_flow, rows_key, rows_date, rows_val = [], [], [], []
    for obs in obs_list:
        dims = obs.get("dimensions", {})
        tp = dims.get("TIME_PERIOD", "")
        d = parse_date(tp)
        if d is None:
            continue
        # Value is in the first measures entry -> .value
        measures = obs.get("measures", {})
        fv = None
        for mv in measures.values():
            if isinstance(mv, dict) and mv.get("value") is not None:
                try:
                    fv = float(mv["value"]); break
                except (ValueError, TypeError):
                    pass
        if fv is None:
            continue
        key = ":".join(f"{k}={v}" for k, v in sorted(dims.items()) if k != "TIME_PERIOD")
        rows_flow.append(flow_code)
        rows_key.append(key)
        rows_date.append(d)
        rows_val.append(fv)

    if not rows_flow:
        log(f"  {flow_code}: 0 valid obs parsed"); return 0

    tbl = pa.table({"flow": pa.array(rows_flow, pa.string()),
                    "series_key": pa.array(rows_key, pa.string()),
                    "obs_date": pa.array(rows_date, pa.date32()),
                    "value": pa.array(rows_val, pa.float64())})
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"  {flow_code}: {n:,} obs ({flow_name[:35]})"); return n


def main():
    dry = "--dry" in sys.argv
    os.makedirs(OUT, exist_ok=True)
    log("Fetching Melodi dataflow catalog...")
    flows = get_flows()
    log(f"Found {len(flows)} Melodi dataflows")
    total = 0
    for i, flow in enumerate(flows, 1):
        code = flow.get("code", "")
        name = flow.get("label", {}).get("en", flow.get("label", {}).get("fr", ""))
        if not code:
            continue
        log(f"[{i}/{len(flows)}] {code}")
        total += ingest_flow(code, name, dry)
        time.sleep(RATE_DELAY)
    log(f"DONE: {total:,} Melodi observations across {len(flows)} dataflows")


if __name__ == "__main__":
    main()
