"""One-shot: catalogue unctad_tradefoodcatbyproc at depth-2 dot grain (giant #14).

394,118,603 obs / 26,579,759 series (4 measures: M4023 growth, M0100 value,
M5066 %food, M5058 %merch — sum matches the parquet EXACTLY, ingest 2026-08-16).

GRAIN MEASURED 2026-08-16 over the FULL store (tools/_grain_tradefoodcatbyproc.py,
duckdb, no sampling):
    depth-1         88 ids   <- mega-tables (~4.5M obs each)
    depth-2     23,906 ids   <- chosen: Category.Economy, ~16.5k obs/table
    depth-3  3,766,088 ids   <- does not fit D1 headroom
Series grain (26,579,759) cannot fit D1 (R364 class).

D1 arithmetic: primary at 6.69 GB of 10 GB post-noaa-shard; 23,906 rows is
under 1% of remaining headroom.

Titles are the bare code prefix (critmin/oceantrade/biotrademerch precedent);
the title_unctad_* enrichment family decodes codes separately — not a reason
to hold up serving.
"""
import collections
import os
import sqlite3
import sys

import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = "unctad_tradefoodcatbyproc"
DS = "US.TradeFoodCatByProc"
EXPECT_DEPTH2 = 23_906

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
