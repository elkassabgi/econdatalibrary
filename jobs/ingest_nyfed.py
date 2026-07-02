#!/usr/bin/env python3
"""NY Federal Reserve reference rates ingest via FRED public API.

Pulls SOFR, BGCR, TGCR, OBFR, SOFR averages — NY Fed-published rates that are
NOT in the Fed Board H.15 release. These are US public domain (NY Fed is federal).

Data is accessed via FRED's public JSON API (same data NY Fed publishes there).
No API key required for these individual series at FRED's public rate.

Run: python jobs/ingest_nyfed.py
"""
import datetime as dt, os, sys, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
OUT = os.path.join(ROOT, "data", "clean_full", "nyfed")
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

# NY Fed-exclusive rates (not in Fed Board H.15)
SERIES = {
    "sofr":    "SOFR",         # Secured Overnight Financing Rate, 2018+
    "bgcr":    "BGCR",         # Broad General Collateral Rate, 2018+
    "tgcr":    "TGCR",         # Tri-Party General Collateral Rate, 2018+
    "obfr":    "OBFR",         # Overnight Bank Funding Rate, 2016+
    "sofr30d": "SOFR30DAYAVG",  # 30-day SOFR average
    "sofr90d": "SOFR90DAYAVG",  # 90-day SOFR average
    "sofr180d":"SOFR180DAYAVG", # 180-day SOFR average
    "sofridx": "SOFRINDEX",     # SOFR Index (compounded)
    # TGCR (Tri-Party General Collateral Rate) — in FRED as TGCRRATE
    "tgcr":    "TGCRRATE",      # TGCR rate, 2018+
    "tgcrvol": "TGCRVOLUME",    # TGCR volume (billions USD), 2018+
    # BGCR (Broad General Collateral Rate) — confirmed NOT in FRED as of 2026-06-05
    # Not available via FRED or NY Fed API; genuinely inaccessible programmatically.
}


def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def parse_date(s):
    s = (s or "").strip()
    for fmt in ["%Y-%m-%d", "%m/%d/%Y"]:
        try:
            return dt.datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None


def fetch_series(name, fred_id):
    out_path = os.path.join(OUT, f"{name}.parquet")
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"  {name}: already {n:,} rows"); return n
    import os as _os, sys as _sys
    if ROOT not in _sys.path: _sys.path.insert(0, ROOT)
    from core.config import load_env as _le; _le()
    api_key = _os.environ.get("FRED_API_KEY","")
    if not api_key: raise SystemExit("FRED_API_KEY not set in .env")
    params = {"series_id": fred_id, "api_key": api_key, "file_type": "json",
              "observation_start": "2000-01-01",
              "sort_order": "asc"}
    try:
        r = requests.get(FRED_BASE, params=params, headers=UA, timeout=60)
        if r.status_code != 200:
            log(f"  {name} ({fred_id}): HTTP {r.status_code}"); return 0
        obs = r.json().get("observations", [])
        if not obs:
            log(f"  {name}: 0 observations returned"); return 0
        dates, keys, vals = [], [], []
        for o in obs:
            d = parse_date(o.get("date", ""))
            v = (o.get("value") or "").strip()
            if d is None or v in ("", ".", "NA"): continue
            try: fv = float(v)
            except ValueError: continue
            dates.append(d); keys.append(f"nyfed:{name}"); vals.append(fv)
        if not dates:
            log(f"  {name}: 0 valid obs"); return 0
        tbl = pa.table({"series_key": pa.array(keys, pa.string()),
                        "obs_date":   pa.array(dates, pa.date32()),
                        "value":      pa.array(vals, pa.float64())})
        pq.write_table(tbl, out_path, compression="zstd")
        n = len(dates)
        log(f"  {name} ({fred_id}): {n:,} obs written"); return n
    except Exception as e:
        log(f"  {name}: ERR {type(e).__name__}: {e}"); return 0


def main():
    os.makedirs(OUT, exist_ok=True)
    total = 0
    for name, fred_id in SERIES.items():
        total += fetch_series(name, fred_id)
        time.sleep(1)  # polite rate limiting to FRED
    log(f"DONE: {total:,} total NY Fed rate observations")


if __name__ == "__main__":
    main()
