#!/usr/bin/env python3
"""IMF Financial Soundness Indicators (FSI) ingest.

Source: https://data.imf.org/?sk=51B096FA-2CD2-40C2-8D09-0699CC1764DA
License: IMF open data (CC BY-NC 4.0)
Coverage: ~150 countries, 2000-present, 40+ banking sector health indicators
  (NPL ratio, capital adequacy, return on equity, liquidity, etc.)

Uses the IMF SDMX REST API.
series_key: FSI:{indicator}:{country_code}  e.g. FSI:FSANL:USA

Output: data/clean_full/imf_fsi/imf_fsi.parquet
Run: python jobs/ingest_imf_fsi.py
"""
from __future__ import annotations
import datetime as dt, io, os, time
import requests, pyarrow as pa, pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT  = os.path.join(ROOT, "data", "clean_full", "imf_fsi")
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

# IMF SDMX API
IMF_API  = "https://data.imf.org/api/SDMX/BI"
DATAFLOW = "FSI"
RATE = 0.5


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def get_json(url, params=None, retries=4, timeout=120):
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=UA, timeout=timeout,
                             allow_redirects=True)
            if r.status_code == 200:
                ct = r.headers.get("content-type", "")
                if "json" in ct or r.content[:1] == b"{":
                    return r.json()
                return None
            if r.status_code in (400, 404, 422):
                return None
            if r.status_code == 429:
                log(f"  429 rate-limit, sleeping 60s"); time.sleep(60); continue
        except Exception as e:
            if attempt >= retries - 1:
                log(f"  ERR: {e}")
        time.sleep(4 * (attempt + 1))
    return None


def get_xml(url, retries=4, timeout=120):
    """Fetch SDMX XML response."""
    for attempt in range(retries):
        try:
            r = requests.get(url, headers={**UA, "Accept": "application/xml"},
                             timeout=timeout)
            if r.status_code == 200:
                return r.content
            if r.status_code in (400, 404):
                return None
        except Exception as e:
            if attempt >= retries - 1:
                log(f"  ERR: {e}")
        time.sleep(4 * (attempt + 1))
    return None


def parse_sdmx_xml(xml_data: bytes):
    """Parse SDMX-ML 2.1 compact format."""
    import xml.etree.ElementTree as ET
    ns = {
        "message":  "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
        "generic":  "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic",
        "compact":  "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/structurespecific",
    }
    keys, dates, vals = [], [], []
    try:
        root = ET.fromstring(xml_data)
        # Try to find observations generically
        # SDMX 2.1 Generic format
        for series in root.iter("{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic}Series"):
            # Get series key
            sk_elem = series.find("{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic}SeriesKey")
            if sk_elem is None:
                continue
            parts = {}
            for v_elem in sk_elem.findall("{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic}Value"):
                parts[v_elem.get("id", "")] = v_elem.get("value", "")

            indicator = parts.get("INDICATOR", parts.get("SERIES", ""))
            country   = parts.get("REF_AREA", parts.get("COUNTRY", ""))
            if not indicator or not country:
                continue

            series_key = f"FSI:{indicator}:{country}"

            # Get observations
            for obs in series.findall("{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic}Obs"):
                dim = obs.find("{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic}ObsDimension")
                val_elem = obs.find("{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic}ObsValue")
                if dim is None or val_elem is None:
                    continue
                period = dim.get("value", "")
                obs_val = val_elem.get("value", "")
                if not obs_val or obs_val in ("", "nan", "N/A"):
                    continue
                try:
                    v = float(obs_val)
                    if v != v:
                        continue
                    # Parse period: YYYY, YYYY-Qn, YYYY-Mm
                    if len(period) == 4 and period.isdigit():
                        obs_d = dt.date(int(period), 12, 31)
                    elif "Q" in period:
                        y, q = period.split("Q")
                        obs_d = dt.date(int(y), {"1":3,"2":6,"3":9,"4":12}[q.strip()], 31)
                    elif "-" in period and len(period) == 7:
                        y, m = period.split("-")
                        obs_d = dt.date(int(y), int(m), 1)
                    else:
                        continue
                    keys.append(series_key)
                    dates.append(obs_d)
                    vals.append(v)
                except (TypeError, ValueError, KeyError):
                    pass
    except Exception as e:
        log(f"  XML parse error: {e}")
    return keys, dates, vals


def main():
    os.makedirs(OUT, exist_ok=True)
    out = os.path.join(OUT, "imf_fsi.parquet")
    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows
        log(f"IMF FSI: already {n:,} rows"); return

    log("=== IMF Financial Soundness Indicators Ingest ===")

    # Try bulk download via SDMX API: FSI dataflow, all countries+indicators+years
    url = f"{IMF_API}/GetData/{DATAFLOW}/Q...?startPeriod=2000&endPeriod=2025&detail=dataonly"
    log(f"Fetching bulk: {url}")
    xml_data = get_xml(url)

    all_keys, all_dates, all_vals = [], [], []

    if xml_data and len(xml_data) > 1000:
        log(f"  Downloaded {len(xml_data)//1024:,} KB XML")
        k, d, v = parse_sdmx_xml(xml_data)
        log(f"  Parsed {len(v):,} obs from XML")
        all_keys.extend(k); all_dates.extend(d); all_vals.extend(v)
    else:
        log(f"  Bulk XML failed, trying annual frequency...")
        url_a = f"{IMF_API}/GetData/{DATAFLOW}/A...?startPeriod=2000&endPeriod=2025&detail=dataonly"
        xml_data = get_xml(url_a)
        if xml_data and len(xml_data) > 1000:
            k, d, v = parse_sdmx_xml(xml_data)
            log(f"  Annual: {len(v):,} obs")
            all_keys.extend(k); all_dates.extend(d); all_vals.extend(v)

    if not all_vals:
        # Fallback 1: try data.imf.org JSON API (newer endpoint)
        log("  Trying data.imf.org API v1...")
        try:
            url_j = "https://data.imf.org/api/v1/en/DataSets/FSI/Dimensions"
            rj = get_json(url_j, timeout=60)
            if rj:
                log(f"  Got dimensions: {list(rj.keys())[:5]}")
        except Exception as e:
            log(f"  data.imf.org API err: {e}")

    if not all_vals:
        # Fallback 2: try DBnomics (mirrors IMF FSI)
        log("  Trying DBnomics IMF/FSI...")
        import json
        dbnomics_base = "https://api.db.nomics.world/v22"
        offset = 0
        limit  = 1000
        while True:
            try:
                url_db = f"{dbnomics_base}/series/IMF/FSI?observations=1&limit={limit}&offset={offset}"
                r = get_json(url_db, timeout=180)
                if not r:
                    break
                series_obj = r.get("series", {})
                docs  = series_obj.get("docs", [])
                total = series_obj.get("num_found", 0)
                if not docs:
                    break
                for s in docs:
                    sc    = s.get("series_code", "")
                    perds = s.get("period_start_day", [])
                    vvals = s.get("value", [])
                    if not sc:
                        continue
                    sk = f"FSI:{sc}"
                    for pd_str, vv in zip(perds, vvals):
                        if vv is None:
                            continue
                        try:
                            fv = float(vv)
                            if fv != fv:
                                continue
                            obs_d = dt.date.fromisoformat(pd_str)
                            all_keys.append(sk)
                            all_dates.append(obs_d)
                            all_vals.append(fv)
                        except (ValueError, TypeError):
                            pass
                offset += len(docs)
                log(f"    [{offset}/{total}] series, {len(all_vals):,} obs")
                if offset >= total:
                    break
                import time as _t; _t.sleep(1)
            except Exception as e:
                log(f"  DBnomics err: {e}")
                break

    if not all_vals:
        log("0 observations obtained"); return

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out, compression="zstd")
    n = pq.read_metadata(out).num_rows
    log(f"=== IMF FSI DONE: {n:,} obs ===")


if __name__ == "__main__":
    main()
