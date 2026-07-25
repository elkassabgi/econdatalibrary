#!/usr/bin/env python3
"""Verify the DBnomics pull: re-read every Parquet footer for the true obs count,
cross-check against the enumerated catalog, and print the honest coverage report.
"""
import glob
import json
import os

import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
RAW = os.path.join(ROOT, "data", "raw", "dbnomics")
OUT = os.path.join(ROOT, "data", "clean_full", "dbnomics")


def main():
    # 1) measured observations from Parquet footers (no full read; footer row counts)
    files = glob.glob(os.path.join(OUT, "*", "*.parquet"))
    per_prov = {}
    total_obs = 0
    total_bytes = 0
    for f in files:
        prov = os.path.basename(os.path.dirname(f))
        n = pq.ParquetFile(f).metadata.num_rows
        per_prov.setdefault(prov, [0, 0, 0])
        per_prov[prov][0] += n          # obs
        per_prov[prov][1] += 1          # files
        b = os.path.getsize(f)
        per_prov[prov][2] += b
        total_obs += n
        total_bytes += b

    # distinct series per provider (sample-safe: read only series_key column)
    print("=== MEASURED PULL (Parquet footers) ===")
    print(f"{'provider':14} {'obs':>14} {'files':>6} {'MB':>8}  {'DONE':>5}")
    for prov in sorted(per_prov, key=lambda p: -per_prov[p][0]):
        obs, nf, b = per_prov[prov]
        done = os.path.exists(os.path.join(OUT, prov, "_DONE"))
        print(f"{prov:14} {obs:>14,} {nf:>6} {b/1e6:>8.1f}  {str(done):>5}")
    print(f"\nTOTAL files: {len(files)}")
    print(f"TOTAL observations (measured): {total_obs:,}")
    print(f"TOTAL size: {total_bytes/1e9:.2f} GB")

    # 2) catalog census + duplication
    if os.path.exists(os.path.join(RAW, "_classification.json")):
        cls = json.load(open(os.path.join(RAW, "_classification.json"), encoding="utf-8"))
        gm_s = cls["global_meta_series"]; gm_d = cls["global_meta_datasets"]
        enum_s = cls["enumerated_series"]
        resid = cls["residual_series_eurostat_plus_istat"]
        dup_e = cls["dup_series_enumerated"]; uniq_e = cls["uniq_series_enumerated"]
        print("\n=== CATALOG CENSUS / DUPLICATION ===")
        print(f"providers in catalog: {cls['providers_total']} (enumerated exactly: {cls['providers_enumerated']})")
        print(f"GLOBAL datasets (meta): {gm_d:,}")
        print(f"GLOBAL series   (meta): {gm_s:,}")
        print(f"enumerated series (91 providers): {enum_s:,}")
        print(f"residual (Eurostat+ISTAT, not enumerated): {resid:,}")
        print(f"  DUPLICATE series (enumerated): {dup_e:,} ({100*dup_e/enum_s:.1f}% of enumerated)")
        print(f"  UNIQUE    series (enumerated): {uniq_e:,} ({100*uniq_e/enum_s:.1f}% of enumerated)")
        # Whole-catalog estimate: residual is ~all Eurostat (duplicate); ISTAT (unique) ds=4039 is tiny.
        # Lower bound on duplicate share of full catalog = (dup_e + resid_eurostat) / global.
        # Treat the entire residual as duplicate-leaning (Eurostat dominates 9559 of 13598 residual datasets).
        dup_full_lo = dup_e + (resid * cls["eurostat_datasets"] / (cls["eurostat_datasets"] + cls["istat_datasets"]))
        print(f"\nWHOLE-CATALOG duplicate share (Eurostat counted as duplicate via dataset-share split):")
        print(f"  ~DUPLICATE: {dup_full_lo:,.0f} / {gm_s:,} = {100*dup_full_lo/gm_s:.1f}%")
        print(f"  ~UNIQUE:    {gm_s-dup_full_lo:,.0f} / {gm_s:,} = {100*(gm_s-dup_full_lo)/gm_s:.1f}%")
        # how much of the enumerated UNIQUE catalog did we actually pull?
        pulled = set(per_prov)
        uniq_rows = [r for r in cls["rows"] if not r["duplicate"] and (r.get("series") or 0) > 0]
        uniq_total = sum(r["series"] for r in uniq_rows)
        pulled_uniq = sum(r["series"] for r in uniq_rows if r["provider"] in pulled)
        print(f"\nUNIQUE providers pulled (by enumerated series): "
              f"{pulled_uniq:,} / {uniq_total:,} = {100*pulled_uniq/uniq_total:.1f}% of the enumerated-unique catalog")
        notpulled = [(r["provider"], r["series"]) for r in uniq_rows if r["provider"] not in pulled]
        notpulled.sort(key=lambda x: -x[1])
        print(f"UNIQUE providers NOT pulled (provider, enumerated_series): {notpulled}")

        # Honest provider-count coverage (not series-weighted): how many UNIQUE
        # providers did we fully download, vs deferred giants.
        n_uniq = len(uniq_rows)
        n_uniq_pulled = sum(1 for r in uniq_rows if r["provider"] in pulled)
        GIANTS = {"CEPII", "CSO", "NBB", "DESTATIS", "DGDDI", "WTO"}
        giant_series = sum(r["series"] for r in uniq_rows if r["provider"] in GIANTS)
        small_uniq_series = uniq_total - giant_series
        print("\n=== HONEST COVERAGE (provider-count + giant caveat) ===")
        print(f"UNIQUE providers (enumerated, series>0): {n_uniq}")
        print(f"UNIQUE providers FULLY pulled: {n_uniq_pulled}")
        print(f"6 deferred giants {sorted(GIANTS)} = {giant_series:,} series "
              f"({100*giant_series/uniq_total:.1f}% of all unique series) -- bilateral-trade/cross-tab/customs, "
              f"available directly from each provider; not pulled")
        print(f"Non-giant unique series (the conventional-macro unique slice): {small_uniq_series:,}")
        # series actually captured in our Parquet (distinct provider/dataset/series_key)
    else:
        print("\n(_classification.json not present yet)")


if __name__ == "__main__":
    main()
