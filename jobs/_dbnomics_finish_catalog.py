#!/usr/bin/env python3
"""Finish the catalog: enumerate the last providers (Eurostat, ISTAT) gently
(sequential, generous timeouts, long backoff), write their checkpoints, then
emit _catalog_summary.json + _classification.json from ALL checkpoints.
"""

# DEFUSED 2026-08-04: the guard below is the enforcement, the CI test tests/test_dbnomics_ban.py
# is the proof, and the PreToolUse hook is the session-level backstop. Three layers on purpose.
raise SystemExit(
    "RETIRED: this script fetched from DBnomics, which is BANNED (CLAUDE.md \u00a70, ledger R251) - "
    "no fetching, no probing, no relays or mirrors. The data it ingested is maintained by "
    "publisher-direct paths now (see updater/strategies/fetchers/ and jobs/ingest_imf_direct.py). "
    "Kept for history; running it is refused.")

import json
import os
import sys
import time

import requests
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
RAW = os.path.join(ROOT, "data", "raw", "dbnomics")
CK = os.path.join(RAW, "_ckpt_datasets")
API = "https://api.db.nomics.world/v22"
UA = "Econ-Fin Data Library admin@hfdatalibrary.com"
S = requests.Session(); S.headers.update({"User-Agent": UA})
sys.path.insert(0, os.path.join(ROOT, "jobs"))
from _dbnomics_classify import DUPLICATE_OF, PROVIDER_LICENSE  # noqa: E402


def get(url, tries=8):
    last = None
    for a in range(tries):
        try:
            r = S.get(url, timeout=120)
        except requests.RequestException as e:
            last = e; time.sleep(3 * (a + 1)); continue
        if r.status_code == 429 or 500 <= r.status_code < 600:
            last = RuntimeError(f"HTTP {r.status_code}"); time.sleep(4 * (a + 1)); continue
        if r.status_code == 404:
            return None
        r.raise_for_status(); return r.json()
    raise last or RuntimeError("unreachable")


def enum_provider(code):
    ckp = os.path.join(CK, f"{code}.json")
    if os.path.exists(ckp):
        print(f"{code}: already have checkpoint", flush=True)
        return
    rows, offset = [], 0
    while True:
        j = get(f"{API}/datasets/{code}?limit=100&offset={offset}")
        if not j:
            break
        d = j.get("datasets", {}); docs = d.get("docs", [])
        if not docs:
            break
        rows.extend({"dataset_code": x.get("code"), "nb_series": x.get("nb_series")} for x in docs)
        offset += len(docs)
        nf = d.get("num_found") or 0
        if offset % 1000 == 0 or offset >= nf:
            print(f"  {code}: {offset}/{nf}", flush=True)
        if offset >= nf:
            break
        time.sleep(0.1)
    json.dump(rows, open(ckp, "w", encoding="utf-8"))
    print(f"{code}: DONE {len(rows)} datasets, {sum(r['nb_series'] or 0 for r in rows):,} series", flush=True)


def build_outputs():
    import glob
    provs = json.load(open(os.path.join(RAW, "_catalog_providers.json"), encoding="utf-8"))
    glob_series = provs["global_nb_series"]; glob_ds = provs["global_nb_datasets"]
    tou = {p["code"]: p.get("terms_of_use") for p in provs["providers"]}
    name = {p["code"]: p.get("name") for p in provs["providers"]}

    all_rows = []
    catalog_rows = []  # for the dataset-level parquet
    for f in glob.glob(os.path.join(CK, "*.json")):
        code = os.path.basename(f)[:-5]
        ds = json.load(open(f, encoding="utf-8"))
        ns = sum(d.get("nb_series") or 0 for d in ds)
        all_rows.append({
            "provider": code, "name": name.get(code), "datasets": len(ds), "series": ns,
            "duplicate": code in DUPLICATE_OF, "duplicate_of": DUPLICATE_OF.get(code),
            "license_id": PROVIDER_LICENSE.get(code, "provider-terms"),
            "terms_of_use": tou.get(code),
        })
        for d in ds:
            catalog_rows.append({"provider_code": code, "dataset_code": d.get("dataset_code"),
                                 "nb_series": d.get("nb_series")})
    all_rows.sort(key=lambda r: -r["series"])

    dup_s = sum(r["series"] for r in all_rows if r["duplicate"])
    uniq_s = sum(r["series"] for r in all_rows if not r["duplicate"])
    dup_d = sum(r["datasets"] for r in all_rows if r["duplicate"])
    uniq_d = sum(r["datasets"] for r in all_rows if not r["duplicate"])
    tot_s = dup_s + uniq_s; tot_d = dup_d + uniq_d

    json.dump({"rows": all_rows, "dup_series": dup_s, "uniq_series": uniq_s,
               "dup_datasets": dup_d, "uniq_datasets": uniq_d,
               "total_series": tot_s, "total_datasets": tot_d,
               "global_meta_series": glob_series, "global_meta_datasets": glob_ds,
               "providers_enumerated": len(all_rows)},
              open(os.path.join(RAW, "_classification.json"), "w", encoding="utf-8"), indent=2)
    json.dump({"by_provider": {r["provider"]: {"datasets": r["datasets"], "series": r["series"]} for r in all_rows},
               "enumerated_datasets": tot_d, "enumerated_series": tot_s,
               "meta_datasets": glob_ds, "meta_series": glob_series},
              open(os.path.join(RAW, "_catalog_summary.json"), "w", encoding="utf-8"), indent=2)
    # dataset-level catalog parquet
    if catalog_rows:
        pq.write_table(pa.table({k: [r.get(k) for r in catalog_rows] for k in
                                 ("provider_code", "dataset_code", "nb_series")}),
                       os.path.join(RAW, "_catalog_datasets.parquet"), compression="zstd")

    print("\n=== FINAL CATALOG CENSUS ===", flush=True)
    print(f"providers enumerated: {len(all_rows)} / 93", flush=True)
    print(f"datasets enumerated: {tot_d:,} (global meta: {glob_ds:,})", flush=True)
    print(f"series   enumerated: {tot_s:,} (global meta: {glob_series:,})", flush=True)
    print(f"DUPLICATE series: {dup_s:,} ({100*dup_s/tot_s:.1f}% of enumerated)", flush=True)
    print(f"UNIQUE    series: {uniq_s:,} ({100*uniq_s/tot_s:.1f}% of enumerated)", flush=True)


def main():
    for code in ["ISTAT", "Eurostat"]:
        try:
            enum_provider(code)
        except Exception as e:  # noqa: BLE001
            print(f"{code}: FAILED {e}", flush=True)
    build_outputs()


if __name__ == "__main__":
    main()
