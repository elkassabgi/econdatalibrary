"""Measure rows-per-prefix at depth 1/2/3 for giant #8 before choosing a catalog grain.

D1 sits at 93.3% of its 10 GB ceiling, so series grain (8,589,597 ids) is not an option.
The collapse point is whichever depth yields thousands of ids, not millions - the same
measurement that put nonplastic at depth-2 (22,079 prefixes, p50 2,676 rows).
"""
import collections, os, statistics, sys
import pyarrow.compute as pc, pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = "unctad_criticalmineralstradebypart"
pf = pq.ParquetFile(os.path.join(ROOT, "data", "clean_full", SRC, SRC + ".parquet"))
d = {1: collections.Counter(), 2: collections.Counter(), 3: collections.Counter()}
for i, b in enumerate(pf.iter_batches(columns=["series_key"], batch_size=1_000_000)):
    for it in pc.value_counts(b.column(0)).to_pylist():
        s = it["values"]
        if not s:
            continue
        p = s.split(".")
        for k in (1, 2, 3):
            if len(p) > k:
                d[k][".".join(p[:k])] += it["counts"]
    if i % 25 == 0:
        print(f"  scanned {i+1}M rows", flush=True)
for k in (1, 2, 3):
    c = d[k]
    if not c:
        print(f"depth{k}: none"); continue
    v = sorted(c.values())
    print(f"depth{k}: {len(c):,} prefixes  p50={statistics.median(v):,.0f}  "
          f"max={max(v):,}  min={min(v):,}", flush=True)
