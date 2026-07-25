#!/usr/bin/env python3
"""Daily update orchestrator for econdatalibrary.

This script runs every day (GitHub Actions cron: 0 8 * * 2-6, Tue-Sat)
and updates each source based on its declared schedule. It mirrors the
pattern used by hfdatalibrary's daily pipeline.

Each connector's fetch(since=last_run_date) ensures only new/changed data
is pulled -- no full re-downloads. For bulk sources (EDGAR, Eurostat, etc.)
the delta scripts are called separately on their own schedule.

Cadences:
  daily:   EIA, ECB/Frankfurter, Fed H.4.1, OFR, NY Fed, GLEIF (delta), DeFiLlama
  weekly:  BLS, CFTC, World Bank, Treasury, StatCan, Zillow, FRED releases
  monthly: BEA, USDA, ILOSTAT, FAOSTAT, IMF, OECD, NOAA, Ember
  annual:  Penn World Table, Fama-French, CEPII BACI, EDGAR full rebuild

Run:  python jobs/daily_update.py [--dry] [--source eia]
"""
import datetime as dt
import json
import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
RUNNER = os.path.join(ROOT, "jobs", "run_connector.py")
STATE_FILE = os.path.join(ROOT, "data", "_last_run.json")

# Connector -> (cadence, run_on_days_of_week [0=Mon, 6=Sun])
# Cadences: daily=every run, weekly=Tue only, monthly=1st of month, annual=Jan only
SCHEDULE = {
    # --- DAILY (prices, rates, flows) ---
    "eia":         "daily",
    "ecb":         "daily",
    "frankfurter": "daily",
    "fed_board":   "daily",    # H.4.1, H.8, H.15 (daily series)
    "defillama":   "daily",
    # --- WEEKLY ---
    "bls":         "weekly",
    "cftc":        "weekly",   # via ingest_cftc.py
    "worldbank":   "weekly",
    "worldbank_esg":"weekly",
    "worldbank_pink":"weekly",
    "treasury":    "weekly",
    "statcan":     "weekly",
    "zillow":      "weekly",
    "fred":        "weekly",   # via ingest_fred_releases.py
    "ofr":         "weekly",
    "nyfed":       "weekly",
    # --- MONTHLY ---
    "bea":         "monthly",
    "usda":        "monthly",
    "ilostat":     "monthly",
    "faostat":     "monthly",
    "imf":         "monthly",
    "oecd":        "monthly",
    "noaa":        "monthly",
    "ember":       "monthly",
    "owid":        "monthly",
    "fhfa":        "monthly",
    "abs":         "monthly",
    "boe":         "monthly",
    "census":      "monthly",
    "bis":         "monthly",
    "dbnomics":    "monthly",
    "wikidata":    "monthly",
    # --- QUARTERLY / INFREQUENT ---
    "penn_world_table": "annual",
    "famafrench":       "monthly",  # factors updated monthly
    "gleif":            "weekly",   # delta file published daily; pull weekly
    # Bulk sources with their own ingest scripts (not run_connector.py)
    # These run via separate workflow jobs on their own cadence:
    #   sec_edgar:   weekly (new filings daily, but batched weekly)
    #   eurostat:    monthly (check last_updated in TOC per dataset)
    #   cepii_baci:  annual (new vintage once/year)
}

BULK_SCRIPTS = {
    "sec_edgar_daily":  ("weekly",  "jobs/ingest_sec_edgar.py --incremental"),
    "gleif_delta":      ("weekly",  "jobs/ingest_gleif.py --delta"),
    "eurostat_delta":   ("monthly", "jobs/ingest_eurostat.py --delta"),
    "fred_releases":    ("weekly",  "jobs/ingest_fred_releases.py"),
    "cftc":             ("weekly",  "jobs/ingest_cftc.py"),
    "ofr":              ("daily",   "jobs/ingest_ofr.py"),
    "nyfed":            ("daily",   "jobs/ingest_nyfed.py"),
    "famafrench":       ("monthly", "jobs/ingest_famafrench.py"),
}


def load_state():
    if os.path.exists(STATE_FILE):
        try:
            return json.load(open(STATE_FILE))
        except Exception:
            pass
    return {}


def save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2, default=str)


def should_run(source, cadence, today, state):
    last = state.get(source)
    if last is None:
        return True
    last_date = dt.date.fromisoformat(last)
    days_since = (today - last_date).days
    if cadence == "daily":
        return days_since >= 1
    elif cadence == "weekly":
        return days_since >= 7
    elif cadence == "monthly":
        return days_since >= 28
    elif cadence == "annual":
        return days_since >= 365
    return True


def run_connector(source, since_date, dry):
    since_str = since_date.isoformat() if since_date else "2000-01-01"
    cmd = [sys.executable, RUNNER, source, "--since", since_str]
    print(f"  [{source}] running fetch(since={since_str})", flush=True)
    if dry:
        return True
    t0 = time.time()
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
    dur = time.time() - t0
    if result.returncode == 0:
        print(f"  [{source}] OK ({dur:.0f}s): {result.stdout.strip()[-120:]}", flush=True)
        return True
    else:
        print(f"  [{source}] FAIL: {result.stderr.strip()[-200:]}", flush=True)
        return False


def main():
    dry = "--dry" in sys.argv
    only = None
    if "--source" in sys.argv:
        only = sys.argv[sys.argv.index("--source") + 1]

    today = dt.date.today()
    state = load_state()
    print(f"=== econdatalibrary daily update {today} (dry={dry}) ===", flush=True)

    ran = 0; ok = 0; skipped = 0
    for source, cadence in SCHEDULE.items():
        if only and source != only:
            continue
        if not should_run(source, cadence, today, state):
            skipped += 1
            continue
        last = dt.date.fromisoformat(state[source]) if source in state else None
        ran += 1
        success = run_connector(source, last, dry)
        if success:
            ok += 1
            if not dry:
                state[source] = today.isoformat()
                save_state(state)

    print(f"\nDone: {ran} ran ({ok} ok, {ran-ok} failed), {skipped} skipped by schedule")


if __name__ == "__main__":
    main()
