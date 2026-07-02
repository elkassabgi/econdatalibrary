#!/usr/bin/env python3
"""Independent verification of the BLS grouped-Parquet full ingest.

Re-reads EVERY data/clean_full/bls/<survey>.parquet from disk (does NOT trust the
run log or sidecars) and reports:
  * per-survey: parquet row count (= observations written), distinct series_id,
    date span, and the survey's published-series count from <survey>.series;
  * grand totals: observations written, distinct series with data, and the
    authoritative published-series total (sum of .series line counts).

This is the honesty check feeding coverage_pct = obs_written is complete iff every
survey parsed without error and the per-survey series-with-data matches .series.
"""
from __future__ import annotations
import glob
import json
import os

import pyarrow.parquet as pq
import pyarrow.compute as pc

ROOT = r"D:/research/econfindatalibrary"
OUT = os.path.join(ROOT, "data", "clean_full", "bls")
RAW = os.path.join(ROOT, "data", "raw", "bls")


def pub_series_count(survey: str):
    p = os.path.join(RAW, survey, survey + ".series")
    if not os.path.exists(p):
        return None
    with open(p, encoding="utf-8", errors="replace") as fh:
        return sum(1 for _ in fh) - 1  # minus header


def main():
    files = sorted(glob.glob(os.path.join(OUT, "*.parquet")))
    print(f"verifying {len(files)} parquet files in {OUT}", flush=True)
    grand_obs = 0
    grand_series = 0
    grand_pub = 0
    rows = []
    for path in files:
        survey = os.path.basename(path)[:-8]  # strip .parquet
        pf = pq.ParquetFile(path)
        n_obs = pf.metadata.num_rows
        # distinct series_id: read only that column, in batches, count transitions
        # (file is written globally sorted by series_id) -> exact, low memory.
        last = None
        n_series = 0
        mn = mx = None
        for batch in pf.iter_batches(columns=["series_id", "obs_date"], batch_size=1_000_000):
            sids = batch.column("series_id")
            dates = batch.column("obs_date")
            # distinct via run-length on sorted column
            for s in sids.to_pylist():
                if s != last:
                    n_series += 1
                    last = s
            dmin = pc.min(dates).as_py()
            dmax = pc.max(dates).as_py()
            if dmin is not None and (mn is None or dmin < mn):
                mn = dmin
            if dmax is not None and (mx is None or dmax > mx):
                mx = dmax
        pub = pub_series_count(survey)
        grand_obs += n_obs
        grand_series += n_series
        if pub:
            grand_pub += pub
        match = "OK" if (pub is not None and n_series == pub) else (
            "n/a" if pub is None else f"DIFF({n_series}vs{pub})")
        rows.append((survey, n_obs, n_series, pub, str(mn), str(mx), match))
        print(f"{survey:6} obs={n_obs:>12,} series={n_series:>10,} "
              f"pub={pub if pub is not None else '?':>10} {mn}..{mx} {match}", flush=True)

    print("=" * 80, flush=True)
    print(f"TOTAL parquet files          : {len(files)}", flush=True)
    print(f"TOTAL observations written   : {grand_obs:,}", flush=True)
    print(f"TOTAL distinct series w/ data : {grand_series:,}", flush=True)
    print(f"TOTAL published series (.series): {grand_pub:,}", flush=True)
    summary = {
        "n_parquet_files": len(files),
        "obs_written": grand_obs,
        "series_with_data": grand_series,
        "pub_series_total": grand_pub,
        "per_survey": [
            {"survey": s, "obs": o, "series": se, "pub": p, "start": a, "end": b, "match": m}
            for (s, o, se, p, a, b, m) in rows
        ],
    }
    json.dump(summary, open(os.path.join(OUT, "_verify.json"), "w"), indent=2)
    print(f"wrote {os.path.join(OUT, '_verify.json')}", flush=True)


if __name__ == "__main__":
    main()
