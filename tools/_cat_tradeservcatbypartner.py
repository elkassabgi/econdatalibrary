"""One-shot: catalogue unctad_tradeservcatbypartner at depth-2 dot grain (giant #9).

14,337,747 obs / 1,304,958 series. Keys are Economy.Partner.Flow.Category.Measure.

GRAIN MEASURED 2026-08-10 over the FULL store (all 14 row groups, no sampling):
    depth-1    208 ids | p50 6,396 rows | max 581,450   <- unusable mega-tables
    depth-2  9,243 ids | p50    82 rows | max  12,675   <- chosen
    depth-3 18,419 ids | p50    42 rows | max   6,426   <- 2x the ids for half the table
    depth-4 496,132 ids | p50   29 rows | max      83   <- 62% of D1's whole headroom
Depth-2 is the collapse point, the same shape as giant #8 (critmin: depth1=64 p50 1,463,126,
depth2=16,103 p50 3,460, depth3=29,459 p50 2,187).

WHY GRAIN IS NOT A STYLE CHOICE HERE. D1 measured 2026-08-10 at 9.35 GB of its 10 GB ceiling —
819 bytes/row, ~793,990 rows of headroom left. Cataloguing this source at SERIES grain would
need 1,304,958 rows, exceeding the entire remaining headroom by 64%. At depth-2 it costs 9,243
rows, 1.2% of headroom, so it does not depend on the pending NOAA shard (task #45).

Titles are the bare code prefix, matching the shipped precedent for critmin and oceantrade.
Decoding Economy/Partner into names would be better, but reportMetadata for this dataset
returns only name/title/version/lastUpdated (2,650 bytes) with no codelists, so enrichment is
a separate step via the pipeline/title_unctad_*.py family — not a reason to hold up serving.
"""
import collections
import os
import sqlite3
import sys

import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = "unctad_tradeservcatbypartner"
DS = "US.TradeServCatByPartner"

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
    print(f"  scanned ~{(i + 1)}M rows, {len(counts):,} prefixes", flush=True)

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
