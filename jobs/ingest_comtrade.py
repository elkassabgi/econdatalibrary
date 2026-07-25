#!/usr/bin/env python3
"""UN Comtrade — annual total merchandise trade (imports/exports) for 200+ countries.

License: CC BY 3.0 IGO
Source: https://comtradeapi.un.org/
No API key required (public preview endpoint, free tier).

Coverage:
  * Annual total merchandise trade (imports CIF, exports FOB)
  * ~200 reporting countries × 2014–present
  * cmdCode=TOTAL (all goods aggregate)
  * Additional: total services trade, bilateral totals for major partners

Rate limits: ~100 requests/hour on free tier; handled with 2s rate limiting.

Run: python jobs/ingest_comtrade.py
"""
from __future__ import annotations
import datetime as dt, os, sys, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT  = os.path.join(ROOT, "data", "clean_full", "comtrade")
BASE = "https://comtradeapi.un.org/public/v1/preview"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
        "Accept": "application/json"}
RATE = 2.0   # respect free tier limits
BATCH = 20   # reporters per request

# UN M49 numeric codes for all reporting countries
REPORTERS = [
    4,   8,  12,  20,  24,  28,  32,  36,  40,  44,
   48,  50,  51,  52,  56,  60,  64,  68,  72,  76,
   84,  90,  96, 100, 104, 108, 112, 116, 120, 124,
  132, 140, 144, 148, 152, 156, 170, 174, 178, 180,
  188, 191, 192, 196, 203, 204, 208, 214, 218, 222,
  226, 230, 232, 233, 242, 246, 250, 266, 268, 276,
  288, 296, 300, 308, 320, 324, 328, 332, 340, 344,
  348, 356, 360, 364, 368, 372, 376, 380, 384, 388,
  392, 398, 400, 404, 408, 410, 414, 417, 418, 422,
  426, 430, 434, 440, 442, 446, 450, 454, 458, 462,
  466, 484, 492, 496, 498, 504, 508, 516, 524, 528,
  540, 554, 558, 562, 566, 578, 586, 591, 598, 600,
  604, 608, 616, 620, 624, 626, 630, 634, 642, 643,
  646, 659, 662, 670, 682, 686, 694, 702, 703, 706,
  710, 716, 724, 728, 729, 740, 752, 756, 762, 764,
  768, 776, 780, 784, 788, 800, 804, 826, 834, 840,
  858, 860, 862, 882, 887, 894,
]


def log(m):
    try:
        print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
    except UnicodeEncodeError:
        print(f"[{time.strftime('%H:%M:%S')}] {str(m).encode('ascii', 'replace').decode()}", flush=True)


def fetch_totals(reporters: list[int], flow: str, retries: int = 4) -> list[dict]:
    """Fetch total merchandise trade for a batch of reporters and one flow."""
    codes = ",".join(str(r) for r in reporters)
    url = (f"{BASE}/C/A/HS"
           f"?reporterCode={codes}"
           f"&flowCode={flow}"
           f"&partnerCode=0"
           f"&cmdCode=TOTAL")
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=120)
            if r.status_code == 200:
                d = r.json()
                return d.get("data", [])
            if r.status_code == 429:
                wait = 60 * (attempt + 1)
                log(f"  429 rate limit, sleeping {wait}s")
                time.sleep(wait); continue
            if r.status_code in (400, 404):
                return []
            log(f"  HTTP {r.status_code} attempt {attempt+1}: flow={flow}")
        except Exception as e:
            log(f"  ERR attempt {attempt+1}: {e}")
        time.sleep(10 * (attempt + 1))
    return []


def fetch_bilateral_totals(reporter: int, partners: list[int], flow: str) -> list[dict]:
    """Fetch bilateral total trade between one reporter and multiple partners."""
    pcodes = ",".join(str(p) for p in partners)
    url = (f"{BASE}/C/A/HS"
           f"?reporterCode={reporter}"
           f"&flowCode={flow}"
           f"&partnerCode={pcodes}"
           f"&cmdCode=TOTAL")
    try:
        r = requests.get(url, headers=UA, timeout=120)
        if r.status_code == 200:
            return r.json().get("data", [])
    except Exception:
        pass
    return []


def parse_record(rec: dict, key_prefix: str) -> tuple[str, dt.date, float] | None:
    period = str(rec.get("period", ""))[:4]
    val = rec.get("primaryValue") or rec.get("cifvalue") or rec.get("fobvalue")
    if not period or val is None:
        return None
    try:
        yr = int(period)
        v = float(val)
        d = dt.date(yr, 12, 31)
        return key_prefix, d, v
    except (ValueError, TypeError):
        return None


def main():
    os.makedirs(OUT, exist_ok=True)
    out_path = os.path.join(OUT, "comtrade.parquet")

    # Check what's already done
    done_combos: set[str] = set()
    all_keys, all_dates, all_vals = [], [], []
    if os.path.exists(out_path):
        tbl = pq.read_table(out_path)
        # Keys like "import_total:842" or "export_total:842"
        for sk in set(tbl.column("series_key").to_pylist()):
            parts = sk.split(":")
            if len(parts) >= 2:
                done_combos.add(sk)
        all_keys  = tbl.column("series_key").to_pylist()
        all_dates = tbl.column("obs_date").to_pylist()
        all_vals  = tbl.column("value").to_pylist()
        log(f"Resuming: {len(done_combos)} series done, {len(all_vals):,} obs")

    # ── Phase 1: Aggregate imports/exports for all countries ─────────────────
    log("Phase 1: Total merchandise trade for all reporters...")
    flow_map = {"M": "import_total", "X": "export_total"}

    for flow, flow_label in flow_map.items():
        # Batch reporters
        todo_reporters = [r for r in REPORTERS
                          if f"{flow_label}:{r}" not in done_combos]
        log(f"  Flow {flow} ({flow_label}): {len(todo_reporters)} reporters to fetch")

        batches = [todo_reporters[i:i+BATCH] for i in range(0, len(todo_reporters), BATCH)]
        for bi, batch in enumerate(batches, 1):
            recs = fetch_totals(batch, flow)
            for rec in recs:
                reporter = rec.get("reporterCode")
                key = f"{flow_label}:{reporter}"
                r = parse_record(rec, key)
                if r:
                    all_keys.append(r[0])
                    all_dates.append(r[1])
                    all_vals.append(r[2])
            log(f"  [{bi}/{len(batches)}] flow={flow} batch: {len(recs)} records")
            time.sleep(RATE)

    # ── Phase 2: Bilateral trade (major reporters × major partners) ─────────
    log("Phase 2: Bilateral total trade for major economies...")
    MAJOR = [
        # G20 + EU major economies
        124,  # Canada
        156,  # China
        276,  # Germany
        356,  # India
        392,  # Japan
        410,  # South Korea
        484,  # Mexico
        643,  # Russia
        682,  # Saudi Arabia
        710,  # South Africa
        792,  # Turkey
        826,  # UK
        840,  # USA
        76,   # Brazil
        36,   # Australia
        250,  # France
        380,  # Italy
        528,  # Netherlands
        724,  # Spain
        756,  # Switzerland
        804,  # Ukraine
        702,  # Singapore
        344,  # Hong Kong
        764,  # Thailand
        458,  # Malaysia
    ]
    # Major trading partners (world = 0, plus key partners)
    MAJOR_PARTNERS = [0, 124, 156, 276, 356, 392, 410, 484, 643, 826, 840, 76, 250, 380, 528]

    for flow, flow_label in {"M": "import_bilateral", "X": "export_bilateral"}.items():
        for reporter in MAJOR:
            if all(f"{flow_label}:{reporter}:{p}" in done_combos for p in MAJOR_PARTNERS):
                continue
            todo_partners = [p for p in MAJOR_PARTNERS
                             if f"{flow_label}:{reporter}:{p}" not in done_combos]
            recs = fetch_bilateral_totals(reporter, todo_partners, flow)
            for rec in recs:
                partner = rec.get("partnerCode", 0)
                key = f"{flow_label}:{reporter}:{partner}"
                r = parse_record(rec, key)
                if r:
                    all_keys.append(r[0])
                    all_dates.append(r[1])
                    all_vals.append(r[2])
            if recs:
                log(f"  bilateral {flow} reporter={reporter}: {len(recs)} records")
            time.sleep(RATE)

    # ── Save ─────────────────────────────────────────────────────────────────
    if not all_vals:
        log("0 observations collected"); return

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"DONE: {n:,} Comtrade observations")


if __name__ == "__main__":
    main()
