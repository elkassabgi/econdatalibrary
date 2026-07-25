#!/usr/bin/env python3
"""INSEE BDM (Banque de Donnees Macroeconomiques) full ingest.

150,000+ French macroeconomic time series. No API key required.
API: https://api.insee.fr/series/BDM/V1 (SDMX 2.1 StructureSpecific format)

Strategy:
1. Get all categorisations -> extract distinct Dataflow IDs (388 entries -> ~100 unique flows)
2. For each dataflow: fetch all observations using data/SERIES_BDM/{FLOW_ID}
3. Write one Parquet per dataflow to data/clean_full/insee_bdm/

License: Licence Ouverte / Open Licence 2.0 — commercial redistribution OK.
Attribution: Source: INSEE BDM (Licence Ouverte 2.0) www.insee.fr

Run: python jobs/ingest_insee_bdm.py [--dry]
"""
from __future__ import annotations
import datetime as dt, os, re, sys, time
import xml.etree.ElementTree as ET
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
sys.path.insert(0, ROOT)

OUT = os.path.join(ROOT, "data", "clean_full", "insee_bdm")
BASE = "https://api.insee.fr/series/BDM/V1"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com", "Accept": "application/xml"}

SCHEMA = pa.schema([
    ("idbank", pa.string()), ("obs_date", pa.date32()),
    ("value", pa.float64()), ("dataflow", pa.string()),
])


def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def get_xml(url, timeout=120):
    for attempt in range(4):
        try:
            r = requests.get(url, headers=UA, timeout=timeout)
            if r.status_code == 200: return ET.fromstring(r.content)
            if r.status_code == 429: time.sleep(30*(attempt+1)); continue
            log(f"  HTTP {r.status_code} {url[-60:]}"); return None
        except Exception as e:
            log(f"  ERR {e}"); time.sleep(5*(attempt+1))
    return None


def parse_period(s):
    s = (s or "").strip()
    try:
        if re.match(r'\d{4}-Q\d', s):
            y, q = s.split("-Q"); return dt.date(int(y), (int(q)-1)*3+1, 1)
        if re.match(r'\d{4}-\d{2}$', s): return dt.date(int(s[:4]), int(s[5:7]), 1)
        if re.match(r'\d{4}$', s): return dt.date(int(s), 12, 31)
        if re.match(r'\d{4}-\d{2}-\d{2}', s): return dt.date.fromisoformat(s[:10])
    except (ValueError, KeyError): pass
    return None


def get_dataflows():
    """Return list of (flow_id, flow_name) from the BDM dataflow endpoint.
    Correct URL: /dataflow/all/all/latest returns all 243 BDM dataflows.
    """
    root = get_xml(f"{BASE}/dataflow/all/all/latest", timeout=60)
    if root is None: return []
    flows = []
    seen = set()
    for e in root.iter():
        if e.tag.split("}")[-1] == "Dataflow":
            fid = e.get("id", "")
            if not fid or fid == "SERIES_BDM" or fid in seen:
                continue
            seen.add(fid)
            nm = ""
            for ch in e:
                local = ch.tag.split("}")[-1]
                lang = ch.get("{http://www.w3.org/XML/1998/namespace}lang", "")
                if local == "Name" and lang == "fr":
                    nm = ch.text or ""
                    break
            flows.append((fid, nm))
    return flows


def ingest_flow(flow_id, dry):
    out_path = os.path.join(OUT, f"{flow_id}.parquet")
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"  {flow_id}: skip ({n:,} rows)"); return n

    # BDM dataflows are accessed via /data/{FLOW_ID} (not /data/SERIES_BDM/{FLOW_ID})
    root = get_xml(f"{BASE}/data/{flow_id}")
    if root is None: return 0

    idbanks, obs_dates, values, flows = [], [], [], []
    n_series = 0
    for elem in root.iter():
        local = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
        if local == "Series":
            idbank = elem.get("IDBANK", "")
            if not idbank: continue
            n_series += 1
            for obs in elem:
                ol = obs.tag.split("}")[-1] if "}" in obs.tag else obs.tag
                if ol == "Obs":
                    d = parse_period(obs.get("TIME_PERIOD", ""))
                    ov = obs.get("OBS_VALUE", "")
                    if d and ov:
                        try:
                            idbanks.append(idbank); obs_dates.append(d)
                            values.append(float(ov)); flows.append(flow_id)
                        except (ValueError, TypeError): pass

    if not idbanks:
        log(f"  {flow_id}: 0 obs"); return 0
    if dry:
        log(f"  {flow_id}: DRY {len(idbanks):,} obs / {n_series} series"); return len(idbanks)

    tbl = pa.table({"idbank": pa.array(idbanks, pa.string()),
                    "obs_date": pa.array(obs_dates, pa.date32()),
                    "value": pa.array(values, pa.float64()),
                    "dataflow": pa.array(flows, pa.string())})
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"  {flow_id}: {n:,} obs / {n_series} series"); return n


def main():
    dry = "--dry" in sys.argv
    os.makedirs(OUT, exist_ok=True)
    log("Enumerating BDM dataflows via categorisation...")
    flows = get_dataflows()
    log(f"Found {len(flows)} unique dataflows")
    total = 0
    for i, (fid, fname) in enumerate(flows, 1):
        log(f"[{i}/{len(flows)}] {fid}: {fname[:40]}")
        total += ingest_flow(fid, dry)
        time.sleep(0.5)
    log(f"DONE: {total:,} BDM observations across {len(flows)} dataflows")


if __name__ == "__main__":
    main()
