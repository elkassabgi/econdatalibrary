#!/usr/bin/env python3
"""DBnomics FULL catalog enumeration (the authoritative census).

Walks /v22/providers (all 93) then /v22/datasets/<provider> (paginated, limit=100)
for EVERY provider, recording per-dataset: provider, dataset code, name, nb_series,
dimension order, indexed_at. Writes:
  data/raw/dbnomics/_catalog_datasets.parquet   (one row per dataset, 44k rows)
  data/raw/dbnomics/_catalog_providers.json     (93 providers + terms_of_use)
Prints the global census so we can report TOTAL series/datasets honestly.

Concurrency <=6, polite UA, retry/backoff. Resumable: skips providers already
present in a per-provider checkpoint JSON.
"""

# DEFUSED 2026-08-04: the guard below is the enforcement, the CI test tests/test_dbnomics_ban.py
# is the proof, and the PreToolUse hook is the session-level backstop. Three layers on purpose.
raise SystemExit(
    "RETIRED: this script fetched from DBnomics, which is BANNED (CLAUDE.md \u00a70, ledger R251) - "
    "no fetching, no probing, no relays or mirrors. The data it ingested is maintained by "
    "publisher-direct paths now (see updater/strategies/fetchers/ and jobs/ingest_imf_direct.py). "
    "Kept for history; running it is refused.")

from __future__ import annotations
import concurrent.futures as cf
import json
import os
import sys
import time

import requests
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT = os.path.join(ROOT, "data", "raw", "dbnomics")
CKPT = os.path.join(OUT, "_ckpt_datasets")
API = "https://api.db.nomics.world/v22"
UA = "Econ-Fin Data Library admin@hfdatalibrary.com"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": UA})


def get(url, tries=5):
    last = None
    for a in range(tries):
        try:
            r = SESSION.get(url, timeout=90)
        except requests.RequestException as e:
            last = e
            time.sleep(1.5 * (a + 1))
            continue
        if r.status_code == 429 or 500 <= r.status_code < 600:
            last = RuntimeError(f"HTTP {r.status_code}")
            time.sleep(2.0 * (a + 1))
            continue
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
    raise last or RuntimeError("unreachable")


def list_providers():
    j = get(f"{API}/providers?limit=1000")
    meta_total_series = j.get("nb_series")
    meta_total_ds = j.get("nb_datasets")
    docs = j["providers"]["docs"]
    return docs, meta_total_series, meta_total_ds


def enumerate_provider(code):
    """Return list of dataset dicts for one provider (paginated)."""
    ckpt_path = os.path.join(CKPT, f"{code}.json")
    if os.path.exists(ckpt_path):
        with open(ckpt_path, encoding="utf-8") as f:
            return code, json.load(f)
    rows = []
    offset = 0
    num_found = None
    while True:
        j = get(f"{API}/datasets/{code}?limit=100&offset={offset}")
        if not j:
            break
        ds = j.get("datasets", {})
        num_found = ds.get("num_found", 0)
        docs = ds.get("docs", [])
        if not docs:
            break
        for d in docs:
            rows.append({
                "provider_code": code,
                "dataset_code": d.get("code"),
                "dataset_name": d.get("name"),
                "nb_series": d.get("nb_series"),
                "indexed_at": d.get("indexed_at"),
                "dims": ",".join(d.get("dimensions_codes_order") or []),
            })
        offset += len(docs)
        if offset >= (num_found or 0):
            break
        time.sleep(0.05)
    os.makedirs(CKPT, exist_ok=True)
    with open(ckpt_path, "w", encoding="utf-8") as f:
        json.dump(rows, f)
    return code, rows


def main():
    os.makedirs(OUT, exist_ok=True)
    os.makedirs(CKPT, exist_ok=True)
    providers, meta_series, meta_ds = list_providers()
    codes = [p["code"] for p in providers]
    print(f"PROVIDERS: {len(codes)}", flush=True)
    print(f"GLOBAL nb_datasets (meta): {meta_ds:,}", flush=True)
    print(f"GLOBAL nb_series   (meta): {meta_series:,}", flush=True)

    # save providers + terms_of_use
    with open(os.path.join(OUT, "_catalog_providers.json"), "w", encoding="utf-8") as f:
        json.dump({"global_nb_datasets": meta_ds, "global_nb_series": meta_series,
                   "providers": providers}, f, indent=2)

    all_rows = []
    done = 0
    with cf.ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(enumerate_provider, c): c for c in codes}
        for fut in cf.as_completed(futs):
            c = futs[fut]
            try:
                _, rows = fut.result()
            except Exception as e:  # noqa: BLE001
                print(f"  PROVIDER {c} FAILED: {e}", flush=True)
                continue
            all_rows.extend(rows)
            done += 1
            tot = sum(r["nb_series"] or 0 for r in rows)
            print(f"  [{done}/{len(codes)}] {c:12} datasets={len(rows):>6,} series={tot:>14,}", flush=True)

    # write the dataset catalog parquet
    if all_rows:
        cols = {k: [r.get(k) for r in all_rows] for k in
                ("provider_code", "dataset_code", "dataset_name", "nb_series", "indexed_at", "dims")}
        tbl = pa.table(cols)
        pq.write_table(tbl, os.path.join(OUT, "_catalog_datasets.parquet"))

    # summary by provider
    byp = {}
    for r in all_rows:
        p = r["provider_code"]
        byp.setdefault(p, [0, 0])
        byp[p][0] += 1
        byp[p][1] += (r["nb_series"] or 0)
    summ = sorted(byp.items(), key=lambda kv: -kv[1][1])
    enum_ds = sum(v[0] for v in byp.values())
    enum_series = sum(v[1] for v in byp.values())
    print("\n=== ENUMERATED CATALOG SUMMARY (by series desc) ===", flush=True)
    for p, (nd, ns) in summ:
        print(f"  {p:14} datasets={nd:>6,}  series={ns:>15,}", flush=True)
    print(f"\nENUMERATED datasets total: {enum_ds:,} (meta said {meta_ds:,})", flush=True)
    print(f"ENUMERATED series   total: {enum_series:,} (meta said {meta_series:,})", flush=True)

    with open(os.path.join(OUT, "_catalog_summary.json"), "w", encoding="utf-8") as f:
        json.dump({"by_provider": {p: {"datasets": nd, "series": ns} for p, (nd, ns) in summ},
                   "enumerated_datasets": enum_ds, "enumerated_series": enum_series,
                   "meta_datasets": meta_ds, "meta_series": meta_series}, f, indent=2)
    print("CATALOG DONE", flush=True)


if __name__ == "__main__":
    main()
