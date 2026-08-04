#!/usr/bin/env python3
"""Generic DBnomics dataset ingest (any provider).

Usage: python jobs/ingest_imf_dbnomics.py <DATASET_CODE> [<PROVIDER>]
  Provider defaults to IMF if not specified.
  e.g. python jobs/ingest_imf_dbnomics.py IFS
       python jobs/ingest_imf_dbnomics.py DOT
       python jobs/ingest_imf_dbnomics.py BOP
       python jobs/ingest_imf_dbnomics.py HDR UNDP
       python jobs/ingest_imf_dbnomics.py EDU UNESCO
       python jobs/ingest_imf_dbnomics.py ITS_MTV_AM WTO

Datasets:
  IFS  - International Financial Statistics (193K series, exchange rates, money,
          interest rates, national accounts, BOP, 200 countries)
  DOT  - Direction of Trade Statistics (472K series, 70M+ obs, bilateral
          merchandise trade, all IMF members, 1947-present)
  BOP  - Balance of Payments and IIP (547K series, current/financial/capital
          accounts, international investment positions)
  AFRREO - Sub-Saharan Africa Regional Economic Outlook (1,654 series)
  APDREO - Asia and Pacific Regional Economic Outlook (265 series)
  FSI  - Financial Soundness Indicators (73K series, banking sector health)

series_key: IMF_{code}:{series_code}
  e.g. IMF_IFS:M.US.ENDA_XDC_USD_RATE  (monthly USD/USD exchange rate for US)
       IMF_DOT:A.US.W00.TMG_CIF_USD    (annual imports, US from World)

Output: data/clean_full/imf_{code_lower}/imf_{code_lower}.parquet
Run: python jobs/ingest_imf_dbnomics.py IFS
"""

# DEFUSED 2026-08-04: the guard below is the enforcement, the CI test tests/test_dbnomics_ban.py
# is the proof, and the PreToolUse hook is the session-level backstop. Three layers on purpose.
raise SystemExit(
    "RETIRED: this script fetched from DBnomics, which is BANNED (CLAUDE.md \u00a70, ledger R251) - "
    "no fetching, no probing, no relays or mirrors. The data it ingested is maintained by "
    "publisher-direct paths now (see updater/strategies/fetchers/ and jobs/ingest_imf_direct.py). "
    "Kept for history; running it is refused.")

from __future__ import annotations
import datetime as dt, os, sys, time
import requests
import pyarrow as pa, pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

DBNOMICS  = "https://api.db.nomics.world/v22"
PAGE_SIZE = 1000


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def fetch_page(provider: str, dataset: str, offset: int) -> dict | None:
    url = (f"{DBNOMICS}/series/{provider}/{dataset}"
           f"?observations=1&limit={PAGE_SIZE}&offset={offset}")
    for attempt in range(4):
        try:
            r = requests.get(url, headers=UA, timeout=300)
            if r.status_code == 200:
                return r.json()
            log(f"  HTTP {r.status_code} (offset={offset}, attempt={attempt+1})")
            if r.status_code in (400, 404):
                return None
        except Exception as e:
            log(f"  ERR attempt {attempt+1}: {e}")
        time.sleep(10 * (attempt + 1))
    return None


def main():
    if len(sys.argv) < 2:
        print("Usage: python ingest_imf_dbnomics.py <DATASET_CODE> [PROVIDER]")
        print("  e.g. python ingest_imf_dbnomics.py IFS")
        print("  e.g. python ingest_imf_dbnomics.py HDR UNDP")
        sys.exit(1)

    dataset   = sys.argv[1].strip()          # preserve case (some codes are mixed, e.g. WoRLD)
    provider  = sys.argv[2].upper().strip() if len(sys.argv) > 2 else "IMF"
    code_low  = dataset.lower()
    prov_low  = provider.lower()
    # Output dir uses provider prefix for non-IMF datasets
    dir_name  = f"imf_{code_low}" if provider == "IMF" else f"{prov_low}_{code_low}"
    out_dir   = os.path.join(ROOT, "data", "clean_full", dir_name)
    out_path  = os.path.join(out_dir, f"{dir_name}.parquet")
    prefix    = f"{provider}_{dataset}"

    os.makedirs(out_dir, exist_ok=True)

    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"IMF {dataset}: already {n:,} rows"); return

    log(f"=== {provider}/{dataset} Ingest via DBnomics ===")

    all_keys:  list[str]      = []
    all_dates: list[dt.date]  = []
    all_vals:  list[float]    = []
    offset = 0
    total  = None

    while True:
        data = fetch_page(provider, dataset, offset)
        if not data:
            log("  Page fetch failed, stopping")
            break

        series_obj = data.get("series", {})
        docs  = series_obj.get("docs", [])
        total = series_obj.get("num_found", total or 0)

        if not docs:
            log("  No docs, done")
            break

        n_obs = 0
        for s in docs:
            sc     = s.get("series_code", "")
            perds  = s.get("period_start_day", [])
            vvals  = s.get("value", [])
            if not sc or not perds:
                continue
            skey = f"{prefix}:{sc}"
            for pd_str, vv in zip(perds, vvals):
                if vv is None:
                    continue
                try:
                    obs_d = dt.date.fromisoformat(pd_str)
                    fv    = float(vv)
                    if fv != fv:
                        continue
                    all_keys.append(skey)
                    all_dates.append(obs_d)
                    all_vals.append(fv)
                    n_obs += 1
                except (ValueError, TypeError):
                    pass

        offset += len(docs)
        log(f"  [{offset}/{total}] series | +{n_obs:,} obs (total {len(all_vals):,})")

        if offset >= (total or 0):
            break

        # Checkpoint every 50K series to avoid losing progress
        if offset % 50000 == 0 and all_vals:
            _checkpoint(out_path, all_keys, all_dates, all_vals)

        time.sleep(1.5)   # polite rate limit

    if not all_vals:
        log(f"0 obs — {provider}/{dataset} failed")
        return

    _write(out_path, all_keys, all_dates, all_vals)
    n = pq.read_metadata(out_path).num_rows
    log(f"=== {provider}/{dataset} DONE: {n:,} obs ===")


def _write(path, keys, dates, vals):
    tbl = pa.table({
        "series_key": pa.array(keys,  pa.string()),
        "obs_date":   pa.array(dates, pa.date32()),
        "value":      pa.array(vals,  pa.float64()),
    })
    pq.write_table(tbl, path, compression="zstd")
    log(f"  Written {len(vals):,} obs to {path}")


def _checkpoint(path, keys, dates, vals):
    """Write intermediate checkpoint parquet."""
    cp_path = path + ".checkpoint"
    _write(cp_path, keys, dates, vals)
    log(f"  Checkpoint: {len(vals):,} obs saved to {cp_path}")


if __name__ == "__main__":
    main()
