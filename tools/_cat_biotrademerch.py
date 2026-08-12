"""One-shot: catalogue unctad_biotrademerch at depth-2 dot grain (giant #13).

1,063,192,830 obs / 112,266,300 series (M4023 measure; M0100's campaign is in
flight and lands under the SAME depth-2 tables — keys are
Product.Flow.Economy.Partner.Measure, so the table set below is measure-stable).

GRAIN MEASURED 2026-08-12 over the FULL store (tools/_grain_biotrademerch.py,
duckdb, no sampling):
    depth-1      2,222 ids   <- mega-tables (~478k obs each)
    depth-2      6,666 ids   <- chosen: Product.Flow, ~159k obs/table
    depth-3  1,362,065 ids   <- 172% of D1's ENTIRE remaining headroom
Series grain (112,266,300) is 141x D1's headroom — the R364-at-141x hazard is
THIS source.

D1 arithmetic (measured 2026-08-10): 9.35 GB of 10 GB, ~794k rows headroom.
Depth-2 costs 6,666 rows = 0.8% of headroom; depth-3 cannot fit at all.

Titles are the bare code prefix, the shipped precedent for critmin/oceantrade/
tradeservcatbypartner; the title_unctad_* enrichment family decodes codes into
names as a separate step — not a reason to hold up serving.
"""
import collections
import os
import sqlite3
import sys

import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = "unctad_biotrademerch"
DS = "US.BiotradeMerch"

con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=7200)
con.execute("PRAGMA busy_timeout=7200000")  # local heavy runs hold long write txns (R400)
con.execute(
    "INSERT OR IGNORE INTO source (source_id, name, homepage, license_id, attribution, terms_url) "
    "VALUES (?,?,?,?,?,?)",
    (SRC,
     "United Nations Conference on Trade and Development (UNCTAD) - UNCTAD Data Hub (UNCTADstat)",
     f"https://unctadstat.unctad.org/datacentre/dataviewer/{DS}",
     "cc-by-3.0-igo",
     "Source: United Nations Trade and Development Data Hub (UNCTADstat)",
     "https://unctadstat.unctad.org/EN/FAQ.html"))

counts = collections.Counter()
pf = pq.ParquetFile(os.path.join(ROOT, "data", "clean_full", SRC, SRC + ".parquet"))
for i, b in enumerate(pf.iter_batches(columns=["series_key"], batch_size=1_000_000)):
    for it in pc.value_counts(b.column(0)).to_pylist():
        s = it["values"]
        if s:
            p = s.split(".")
            if len(p) > 2:
                counts[".".join(p[:2])] += it["counts"]
    if (i + 1) % 50 == 0:
        print(f"  scanned ~{(i + 1)}M rows, {len(counts):,} prefixes", flush=True)

print(f"scan done: {len(counts):,} depth-2 prefixes", flush=True)
if len(counts) != 6666:
    print(f"REFUSING: prefix count {len(counts):,} != measured depth-2 6,666 "
          f"(grain measurement and catalogue scan disagree)", file=sys.stderr)
    sys.exit(1)

existing = {r[0] for r in con.execute(
    "SELECT series_id FROM series WHERE source_id=?", (SRC,))}
rows = [(f"{SRC}:{p}", SRC, p, None, None, None, None, "cc-by-3.0-igo",
         None, None, None, "{}")
        for p in sorted(counts) if f"{SRC}:{p}" not in existing]
con.executemany(
    "INSERT OR IGNORE INTO series (series_id, source_id, title, frequency, unit, geography, "
    "category, license_id, start_date, end_date, last_updated, metadata) "
    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows)
con.commit()
total = con.execute("SELECT COUNT(*) FROM series WHERE source_id=?", (SRC,)).fetchone()[0]
print(f"prefixes={len(counts):,} inserted={len(rows):,} total={total:,}")
if total != len(counts):
    print(f"WARNING: catalogue total {total:,} != measured prefixes {len(counts):,}",
          file=sys.stderr)
    sys.exit(1)
