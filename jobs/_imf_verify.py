#!/usr/bin/env python3
"""Verify the IMF clean_full output: count series/obs across all parquet files,
check integrity, and reconcile against the manifest."""
import glob
import json
import os

import pyarrow.parquet as pq

OUT = r"D:/research/econfindatalibrary/data/clean_full/imf"


def main():
    files = sorted(glob.glob(os.path.join(OUT, "*.parquet")))
    print(f"Parquet files: {len(files)}")
    tot_obs = 0
    tot_series = 0
    tot_bytes = 0
    bad = []
    rows = []
    for f in files:
        name = os.path.basename(f)[:-8]
        try:
            pf = pq.ParquetFile(f)
            nrows = pf.metadata.num_rows
            # count distinct series_key cheaply via column read
            tbl = pq.read_table(f, columns=["series_key"])
            nser = len(set(tbl.column("series_key").to_pylist()))
        except Exception as e:  # noqa: BLE001
            bad.append((name, str(e)))
            continue
        sz = os.path.getsize(f)
        tot_obs += nrows
        tot_series += nser
        tot_bytes += sz
        rows.append((name, nser, nrows, sz))

    rows.sort(key=lambda r: -r[2])
    print(f"{'DATAFLOW':36} {'SERIES':>10} {'OBS':>14} {'MB':>8}")
    for name, nser, nrows, sz in rows:
        print(f"{name:36} {nser:>10,} {nrows:>14,} {sz/1e6:>8.1f}")
    print("=" * 72)
    print(f"TOTAL datasets={len(rows)} series={tot_series:,} obs={tot_obs:,} size={tot_bytes/1e6:.1f}MB")
    if bad:
        print(f"CORRUPT/UNREADABLE: {len(bad)}")
        for n, e in bad:
            print(f"  {n}: {e}")

    mf = os.path.join(OUT, "_manifest.json")
    if os.path.exists(mf):
        m = json.load(open(mf, encoding="utf-8"))
        print("--- manifest ---")
        print(f"published_total={m.get('published_total')} base_total={m.get('base_total')} "
              f"vintage_total={m.get('vintage_total')}")
        print(f"attempted={m.get('attempted')} ok={m.get('ok')} empty={m.get('empty')} "
              f"error={m.get('error')} skipped={m.get('skipped')}")
        print(f"manifest total_observations={m.get('total_observations'):,} "
              f"total_series={m.get('total_series'):,}")
        # list any non-ok results
        for r in m.get("results", []):
            if r.get("status") not in ("ok",):
                print(f"  NON-OK {r.get('id')}: {r.get('status')} - {r.get('note')}")


if __name__ == "__main__":
    main()
