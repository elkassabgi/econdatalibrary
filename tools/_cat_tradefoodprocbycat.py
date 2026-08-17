"""One-shot: catalogue unctad_tradefoodprocbycat at depth-2 dot grain (giant #15).

336,738,347 obs / 22,682,414 series (4 measures: M4023 growth 85,570,215,
M0100 value 100,446,471, M5066 %food 73,820,788, M5058 %merch 76,900,873 —
sum matches the parquet EXACTLY, ingest 2026-08-17 07:57 local; parquet counts
re-verified with duckdb the same hour).

GRAIN MEASURED 2026-08-17 over the FULL store (duckdb, no sampling):
    depth-2     20,578 ids   <- chosen: ProcessFoodCategory.Economy, ~16.4k obs/table
Series grain (22,682,414) cannot fit D1 (R364 class); the depth-2 shape and
density match sibling #14 (23,906 ids), which serves cleanly at this grain.

The EXPECT gate below is the two-instrument agreement check: this scan
(pyarrow value_counts) must reproduce the duckdb measurement or the tool
refuses — grain drift between measurement and cataloguing is a served-surface
defect, not a warning.

Titles are the bare code prefix (critmin/oceantrade/biotrademerch/#14
precedent); the title_unctad_* enrichment family decodes codes separately.
"""
import collections
import os
import sqlite3
import sys

import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = "unctad_tradefoodprocbycat"
DS = "US.TradeFoodProcByCat"
EXPECT_DEPTH2 = 20_578

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
if len(counts) != EXPECT_DEPTH2:
    print(f"REFUSING: prefix count {len(counts):,} != measured depth-2 {EXPECT_DEPTH2:,} "
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
