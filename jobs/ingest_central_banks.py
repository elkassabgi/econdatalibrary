#!/usr/bin/env python3
"""Free central bank data connectors — Bank of Canada, SNB Switzerland, Riksbank Sweden.

All keyless, open data.

Providers:
  boc      — Bank of Canada Valet API  https://www.bankofcanada.ca/valet/
  snb      — Swiss National Bank       https://data.snb.ch/api/
  riksbank — Riksbank Sweden           https://api.riksbank.se/swea/v1/

Run: python jobs/ingest_central_banks.py <provider>
     python jobs/ingest_central_banks.py all    # run all
"""
from __future__ import annotations
import datetime as dt, os, sys, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
        "Accept": "application/json"}


def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def get_json(url: str, retries: int = 4, timeout: int = 120) -> dict | list | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 404):
                return None
            if r.status_code == 429:
                time.sleep(60); continue
            log(f"  HTTP {r.status_code} attempt {attempt+1}: {url[-80:]}")
        except Exception as e:
            log(f"  ERR attempt {attempt+1}: {e}")
        time.sleep(5 * (attempt + 1))
    return None


def parse_date(s: str) -> dt.date | None:
    s = (s or "").strip()
    try:
        if len(s) == 4: return dt.date(int(s), 12, 31)
        if len(s) == 7: return dt.date(int(s[:4]), int(s[5:7]), 1)
        if len(s) == 10 and s[4] == "-": return dt.date.fromisoformat(s[:10])
    except Exception:
        pass
    return None


# ─────────────────────────── Bank of Canada ──────────────────────────────

def ingest_boc(out_dir: str) -> int:
    """Bank of Canada Valet API — all groups / all series."""
    out_path = os.path.join(out_dir, "boc.parquet")
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"BoC: already {n:,} rows"); return n

    BASE_BOC = "https://www.bankofcanada.ca/valet"
    # Get all series via the lists endpoint
    series_data = get_json(f"{BASE_BOC}/lists/series/json")
    if not series_data:
        log("BoC: failed to get series list"); return 0
    series_dict = series_data.get("series", {})
    log(f"BoC: {len(series_dict)} series")

    # Batch series into groups of 20 for the observations endpoint
    series_keys = list(series_dict.keys())
    BATCH = 20
    all_keys, all_dates, all_vals = [], [], []
    for batch_start in range(0, len(series_keys), BATCH):
        batch = series_keys[batch_start:batch_start+BATCH]
        batch_str = ",".join(batch)
        url = f"{BASE_BOC}/observations/{batch_str}/json"
        data = get_json(url)
        if not data:
            time.sleep(0.5); continue
        obs_list = data.get("observations", [])
        for obs in obs_list:
            d_str = obs.get("d", "")
            d = parse_date(d_str)
            if d is None:
                continue
            for skey, sval in obs.items():
                if skey == "d":
                    continue
                if isinstance(sval, dict):
                    raw_v = sval.get("v")
                else:
                    raw_v = sval
                if raw_v in (None, "", "nan", "null"):
                    continue
                try:
                    v = float(raw_v)
                except (TypeError, ValueError):
                    continue
                all_keys.append(skey)
                all_dates.append(d)
                all_vals.append(v)
        if batch_start % 200 == 0:
            log(f"  BoC: {batch_start}/{len(series_keys)} series done, {len(all_vals):,} obs")
        time.sleep(0.3)

    if not all_vals:
        log("BoC: 0 obs"); return 0

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"BoC: DONE {n:,} obs"); return n


# ─────────────────────────── Swiss National Bank ─────────────────────────
# New API (2025+): cube-based.  No catalog endpoint — cube IDs hard-coded.
# Data endpoint: GET /api/cube/{cube_id}/data/csv/en  → UTF-8-BOM semicolon CSV
# CSV header row:  "Date";"D0";"D1";...;"Value"
# Dimensions endpoint: GET /api/cube/{cube_id}/dimensions/en  → JSON

SNB_CUBES = [
    # Exchange rates
    "devkum",        # Foreign exchange rates — monthly averages & end-of-month
    "devkur",        # Foreign exchange rates — daily
    "devwki",        # Nominal / real exchange rate indices
    "devwkilandga",  # Country weights for exchange rate index
    # Monetary aggregates
    "snbmonagg",     # M1, M2, M3 aggregates
    # Banking statistics
    "bastrbwa",      # Key figures all bank categories — annual
    "bawebesecja",   # Custody account holdings — annual
    "bsta",          # Banks' balance sheets
    "bstaq",         # Banks' balance sheets — quarterly
    # Bond yields / interest rates
    "rendoblid",     # Bond yields on bond issues — daily
    "rendoblim",     # Bond yields on bond issues — monthly
    "snbgwdzid",     # SNB policy rates and SARON — daily
    "snbgwdmigirow", # SNB interest rate and minimum reserve requirement
    # Capital market
    "capchstocki",   # Swiss stock market indices
    "capcollvf",     # Foreign claims on Switzerland and Swiss claims abroad
    "capcollu",      # Capital imports/exports
    # SNB conditional inflation forecast
    "snbiprogq",     # Conditional inflation forecast — quarterly
    # National accounts / prices
    "statbip",       # GDP by expenditure approach
    "statpreismik",  # Micro price indices (CPI components)
    "statpreisindex",# Price indices overview
    "statpreiskpii", # CPI by type of expenditure
    # Balance of payments
    "statzbz",       # Balance of payments
    "statzfhza",     # Financial accounts
    "statzfhzf",     # Financial accounts — financial instruments
    # Reserve assets
    "devisreserven", # Foreign exchange reserves
]

BASE_SNB = "https://data.snb.ch/api"
RATE_SNB = 1.0   # 1 request/s — SNB is a government server, be polite


def parse_snb_date(s: str) -> dt.date | None:
    """Parse SNB date strings: YYYY-MM (monthly), YYYY-MM-DD (daily), YYYY (annual)."""
    s = (s or "").strip().strip('"')
    try:
        if len(s) == 4 and s.isdigit():                     # Annual
            return dt.date(int(s), 12, 31)
        if len(s) == 7 and s[4] == "-" and s[5:].isdigit():# Monthly YYYY-MM
            return dt.date(int(s[:4]), int(s[5:7]), 1)
        if len(s) == 10 and s[4] == "-" and s[7] == "-":   # Daily YYYY-MM-DD
            return dt.date.fromisoformat(s)
        if len(s) == 7 and s[4] == "-" and s[5] == "Q":    # Quarterly YYYY-Q1
            yr, q = int(s[:4]), int(s[6])
            return dt.date(yr, (q - 1) * 3 + 1, 1)
    except (ValueError, IndexError):
        pass
    return None


def download_snb_cube(cube_id: str) -> list[tuple[str, dt.date, float]]:
    """Download and parse one SNB cube CSV. Returns list of (series_key, date, value)."""
    url = f"{BASE_SNB}/cube/{cube_id}/data/csv/en"
    results: list[tuple[str, dt.date, float]] = []
    try:
        r = requests.get(url, headers=UA, timeout=120)
        if r.status_code == 404:
            log(f"  SNB {cube_id}: 404 (cube not found)")
            return []
        if r.status_code != 200:
            log(f"  SNB {cube_id}: HTTP {r.status_code}")
            return []
        # Decode BOM-prefixed UTF-8
        text = r.content.decode("utf-8-sig")
        lines = text.splitlines()

        # Find the header row (starts with "Date")
        header_idx = None
        for i, line in enumerate(lines):
            stripped = line.strip().strip('"')
            if stripped.startswith("Date") or stripped.startswith('"Date"'):
                header_idx = i
                break
        if header_idx is None:
            log(f"  SNB {cube_id}: could not find header row")
            return []

        # Parse header to get column names (semicolon-delimited)
        headers = [h.strip().strip('"') for h in lines[header_idx].split(";")]
        if "Value" not in headers or "Date" not in headers:
            log(f"  SNB {cube_id}: unexpected columns {headers[:6]}")
            return []

        date_idx  = headers.index("Date")
        val_idx   = headers.index("Value")
        dim_idxes = [i for i, h in enumerate(headers) if h not in ("Date", "Value")]

        for line in lines[header_idx + 1:]:
            if not line.strip():
                continue
            parts = [p.strip().strip('"') for p in line.split(";")]
            if len(parts) < val_idx + 1:
                continue
            raw_v = parts[val_idx]
            if not raw_v:
                continue
            try:
                v = float(raw_v)
            except (TypeError, ValueError):
                continue
            d = parse_snb_date(parts[date_idx])
            if d is None:
                continue
            dim_vals = ":".join(parts[i] for i in dim_idxes if i < len(parts))
            key = f"SNB:{cube_id}:{dim_vals}"
            results.append((key, d, v))
    except Exception as e:
        log(f"  SNB {cube_id}: ERR {e}")
    return results


def ingest_snb(out_dir: str) -> int:
    """Swiss National Bank — cube-based REST API (2025 portal)."""
    total = 0
    for i, cube_id in enumerate(SNB_CUBES, 1):
        out_path = os.path.join(out_dir, f"{cube_id}.parquet")
        if os.path.exists(out_path):
            n = pq.read_metadata(out_path).num_rows
            log(f"  SNB [{i}/{len(SNB_CUBES)}] {cube_id}: skip ({n:,} rows)")
            total += n
            continue
        log(f"  SNB [{i}/{len(SNB_CUBES)}] {cube_id} ...")
        rows = download_snb_cube(cube_id)
        if rows:
            keys  = [r[0] for r in rows]
            dates = [r[1] for r in rows]
            vals  = [r[2] for r in rows]
            tbl = pa.table({
                "series_key": pa.array(keys,  pa.string()),
                "obs_date":   pa.array(dates, pa.date32()),
                "value":      pa.array(vals,  pa.float64()),
            })
            pq.write_table(tbl, out_path, compression="zstd")
            n = pq.read_metadata(out_path).num_rows
            log(f"    {cube_id}: saved {n:,} obs")
            total += n
        time.sleep(RATE_SNB)
    log(f"SNB: DONE {total:,} total obs")
    return total


# ─────────────────────────── Riksbank Sweden ─────────────────────────────

def get_json_riksbank(url: str, retries: int = 6) -> dict | list | None:
    """Riksbank-specific getter with aggressive rate-limit handling (429 → retry)."""
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=60)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 404):
                return None
            if r.status_code == 429:
                wait = 10 * (attempt + 1)
                log(f"  Riksbank 429 rate-limit, sleeping {wait}s")
                time.sleep(wait); continue
            log(f"  Riksbank HTTP {r.status_code} attempt {attempt+1}")
        except Exception as e:
            log(f"  Riksbank ERR attempt {attempt+1}: {e}")
        time.sleep(5)
    return None


def ingest_riksbank(out_dir: str) -> int:
    """Riksbank Sweden SWEA API — 117 series: exchange rates, rates, macro."""
    out_path = os.path.join(out_dir, "riksbank.parquet")
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"Riksbank: already {n:,} rows"); return n

    BASE_RB = "https://api.riksbank.se/swea/v1"

    # GET /swea/v1/Series → full catalog (117 series, no pagination needed)
    series_list = get_json_riksbank(f"{BASE_RB}/Series")
    if not series_list or not isinstance(series_list, list):
        log("Riksbank: could not get series list"); return 0
    log(f"Riksbank: {len(series_list)} series")

    all_keys, all_dates, all_vals = [], [], []
    for i, s in enumerate(series_list):
        sid = s.get("seriesId") or s.get("seriesid") or s.get("id", "")
        if not sid:
            continue

        # Observations endpoint: GET /Observations/{seriesId}/d (daily native)
        # Falls back to /m (monthly) if daily returns empty
        obs_list = None
        for freq in ("d", "m", "q", "y"):
            data = get_json_riksbank(f"{BASE_RB}/Observations/{sid}/{freq}")
            if data and isinstance(data, list) and len(data) > 0:
                obs_list = data; break
            time.sleep(2)   # Riksbank rate limit: be conservative

        if not obs_list:
            time.sleep(2); continue

        for obs in obs_list:
            d_str = obs.get("date") or obs.get("Date") or obs.get("period", "")
            v_raw  = obs.get("value") or obs.get("Value")
            if not d_str or v_raw in (None, "", "null"):
                continue
            d = parse_date(str(d_str))
            if d is None:
                continue
            try:
                v = float(v_raw)
            except (TypeError, ValueError):
                continue
            all_keys.append(str(sid))
            all_dates.append(d)
            all_vals.append(v)

        log(f"  Riksbank [{i+1}/{len(series_list)}] {sid}: {len(obs_list)} obs")
        time.sleep(2)  # conservative rate: ~0.5 req/s

    if not all_vals:
        log("Riksbank: 0 obs"); return 0

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"Riksbank: DONE {n:,} obs"); return n


# ─────────────────────────── Main ────────────────────────────────────────

PROVIDERS = {
    "boc":      (ingest_boc,      "Bank of Canada"),
    "snb":      (ingest_snb,      "Swiss National Bank"),
    "riksbank": (ingest_riksbank, "Riksbank Sweden"),
}


def main():
    args = sys.argv[1:]
    which = args[0].lower() if args else "all"

    if which == "all":
        to_run = list(PROVIDERS.keys())
    elif which in PROVIDERS:
        to_run = [which]
    else:
        print(f"Unknown provider: {which}. Options: all, {', '.join(PROVIDERS)}")
        sys.exit(1)

    total = 0
    for key in to_run:
        fn, name = PROVIDERS[key]
        out_dir = os.path.join(ROOT, "data", "clean_full", key)
        os.makedirs(out_dir, exist_ok=True)
        log(f"=== {name} ===")
        total += fn(out_dir)

    log(f"GRAND TOTAL: {total:,} central bank observations")


if __name__ == "__main__":
    main()
