#!/usr/bin/env python3
"""Independent verification: re-read EVERY Zillow obs Parquet from disk, sum rows,
count unique series, validate schema, and reconcile against zillow.meta.json."""
import glob
import json
import os

import pyarrow.parquet as pq


# Repo root derived from this file, never a drive letter: the store moved D: -> E: in the
# workstation cutover, and a verify script pointed at an absent tree reports "0 files,
# nothing wrong" instead of failing. R330.
def _RD(*parts):
    _r = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(_r, *parts) if parts else _r

OUT = _RD('data', 'clean_full', 'zillow')
EXPECTED_SCHEMA = ["dataset", "series_key", "obs_date", "value"]


def main():
    obs_files = sorted(f for f in glob.glob(os.path.join(OUT, "*.parquet"))
                       if not f.endswith("__series.parquet"))
    series_files = sorted(glob.glob(os.path.join(OUT, "*__series.parquet")))

    total_obs = 0
    all_series = set()
    bad_schema = []
    null_dates = 0
    null_vals = 0
    min_d = None
    max_d = None
    geo_counts = {}
    metric_counts = {}

    for f in obs_files:
        t = pq.read_table(f)
        names = t.schema.names
        if names != EXPECTED_SCHEMA:
            bad_schema.append((os.path.basename(f), names))
        total_obs += t.num_rows
        sk = t.column("series_key").to_pylist()
        all_series.update(sk)
        d = t.column("obs_date").to_pandas()
        null_dates += int(d.isna().sum())
        v = t.column("value").to_pandas()
        null_vals += int(v.isna().sum())
        dd = d.dropna()
        if len(dd):
            lo, hi = dd.min(), dd.max()
            min_d = lo if min_d is None or lo < min_d else min_d
            max_d = hi if max_d is None or hi > max_d else max_d
        # geo/metric from filename + first series key
        if sk:
            parts = sk[0].split(":")
            if len(parts) >= 3:
                metric_counts[parts[1]] = metric_counts.get(parts[1], 0) + t.num_rows
                geo_counts[parts[2]] = geo_counts.get(parts[2], 0) + t.num_rows

    print(f"obs parquet files     : {len(obs_files)}")
    print(f"series sidecar files  : {len(series_files)}")
    print(f"TOTAL observations    : {total_obs:,}")
    print(f"UNIQUE series keys     : {len(all_series):,}")
    print(f"date range            : {min_d} .. {max_d}")
    print(f"null obs_date cells   : {null_dates}")
    print(f"null value cells      : {null_vals}")
    print(f"schema-mismatched files: {len(bad_schema)}")
    for name, sc in bad_schema[:10]:
        print("   ", name, sc)

    print("\nobs by geography level:")
    for k, v in sorted(geo_counts.items(), key=lambda x: -x[1]):
        print(f"   {k:14} {v:>13,}")
    print("\ntop metrics by obs:")
    for k, v in sorted(metric_counts.items(), key=lambda x: -x[1])[:15]:
        print(f"   {k:32} {v:>13,}")

    meta_path = os.path.join(OUT, "zillow.meta.json")
    if os.path.exists(meta_path):
        m = json.load(open(meta_path, encoding="utf-8"))
        print("\n--- reconcile vs zillow.meta.json ---")
        print(f"meta total_obs={m['total_obs']:,}  disk total_obs={total_obs:,}  "
              f"match={m['total_obs'] == total_obs}")
        print(f"meta datasets={m['n_datasets_written']}  disk obs files={len(obs_files)}  "
              f"match={m['n_datasets_written'] == len(obs_files)}")
        print(f"meta dead={m['n_dead_urls']}  catalog urls={m['n_catalog_urls']}")
        n_verify_ok = sum(1 for d in m["datasets"] if d.get("verify_ok"))
        print(f"per-dataset verify_ok: {n_verify_ok}/{len(m['datasets'])}")

    # sanity: a sample series sidecar
    if series_files:
        s = pq.read_table(series_files[0])
        print(f"\nsample sidecar {os.path.basename(series_files[0])}: "
              f"{s.num_rows} rows, cols={s.schema.names[:8]}...")


if __name__ == "__main__":
    main()
