#!/usr/bin/env python3
"""Independent verification of the OWID grouped Parquet output.

Re-reads EVERY Parquet in data/clean_full/owid/ to count actual observations and
distinct series (does NOT trust the ingest summary). Cross-checks against the
sitemap slug catalog and the per-status breakdown in _ingest_summary.json so the
final coverage number is grounded in what is actually on disk.
"""
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

OUT = _RD('data', 'clean_full', 'owid')
RAW = _RD('data', 'raw', 'owid')
def main():
    files = sorted(glob.glob(os.path.join(OUT, "*.parquet")))
    total_rows = 0
    total_bytes = 0
    empty_files = 0
    bad = []
    series = set()
    sample_series_cap = 5_000_000  # bound the distinct-series set memory
    cap_hit = False
    min_date = None
    max_date = None
    for f in files:
        try:
            md = pq.read_metadata(f)
        except Exception as e:  # corrupt file
            bad.append((os.path.basename(f), str(e)[:80]))
            continue
        total_rows += md.num_rows
        total_bytes += os.path.getsize(f)
        if md.num_rows == 0:
            empty_files += 1

    # distinct series + date range from a full scan of the key column (chunked)
    for f in files:
        try:
            t = pq.read_table(f, columns=["series_key", "obs_date"])
        except Exception:
            continue
        if not cap_hit:
            for k in t.column("series_key").to_pylist():
                series.add(k)
                if len(series) >= sample_series_cap:
                    cap_hit = True
                    break
        d = t.column("obs_date")
        if t.num_rows:
            lo = min(x for x in d.to_pylist() if x is not None)
            hi = max(x for x in d.to_pylist() if x is not None)
            min_date = lo if min_date is None or lo < min_date else min_date
            max_date = hi if max_date is None or hi > max_date else max_date

    slugs = []
    sp = os.path.join(RAW, "owid_slugs.txt")
    if os.path.exists(sp):
        slugs = [s for s in open(sp, encoding="utf-8").read().split("\n") if s]

    summ = {}
    spath = os.path.join(OUT, "_ingest_summary.json")
    if os.path.exists(spath):
        summ = json.load(open(spath, encoding="utf-8"))

    print("=== OWID PARQUET VERIFICATION (read back from disk) ===")
    print(f"parquet files on disk      : {len(files):,}")
    print(f"  of which empty (0 rows)  : {empty_files:,}")
    print(f"corrupt/unreadable files   : {len(bad)}")
    for b in bad[:10]:
        print(f"    BAD {b}")
    print(f"TOTAL observations (rows)  : {total_rows:,}")
    print(f"distinct series_key        : {len(series):,}"
          + ("  (CAPPED)" if cap_hit else ""))
    print(f"date range                 : {min_date} .. {max_date}")
    print(f"total size on disk         : {total_bytes/1e6:.1f} MB "
          f"({total_bytes/1e9:.2f} GB)")
    if files:
        print(f"avg bytes/file             : {total_bytes//len(files):,}")
    print()
    print(f"catalog slugs (sitemap)    : {len(slugs):,}")
    if summ:
        print(f"summary.status_breakdown   : {summ.get('status_breakdown')}")
        print(f"summary.charts_attempted   : {summ.get('charts_attempted')}")
        print(f"summary.observations_written: {summ.get('observations_written'):,}")
        sb = summ.get("status_breakdown", {})
        ok = sb.get("ok", 0) + sb.get("cached", 0)
        nonredist = sb.get("non_redistributable", 0)
        empt = sb.get("empty", 0)
        ndc = sb.get("no_data_cols", 0)
        miss = sb.get("missing", 0)
        err = sb.get("error", 0)
        denom_total = len(slugs)
        # downloadable-numeric denominator = total minus the legitimately-excluded
        # non-redistributable (license carve-out) charts
        denom_redist = denom_total - nonredist
        print()
        print("=== COVERAGE ===")
        print(f"charts with numeric data written : {ok}")
        print(f"non_redistributable (excluded)   : {nonredist}")
        print(f"empty (categorical, no numbers)  : {empt}")
        print(f"no_data_cols / missing / error   : {ndc} / {miss} / {err}")
        print(f"coverage vs FULL catalog         : {ok}/{denom_total} = "
              f"{ok/denom_total*100:.1f}%")
        print(f"coverage vs REDISTRIBUTABLE      : {ok}/{denom_redist} = "
              f"{ok/denom_redist*100:.1f}%")


if __name__ == "__main__":
    main()
