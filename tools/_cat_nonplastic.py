"""One-shot: catalogue unctad_nonplasticsubststradebypartner at depth-2 dot grain (giant #7).

100,233,168 obs / 9,298,529 series; measured grain: depth1=84 tables (p50 1,091,315),
depth2=22,079 (p50 2,676), depth3=2,892,210 (p50 29) -> depth-2 is the collapse point.
"""
import collections
import os
import sqlite3
import sys

import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = "unctad_nonplasticsubststradebypartner"

con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=180)
con.execute(
    "INSERT OR IGNORE INTO source (source_id, name, homepage, license_id, attribution, terms_url) "
    "VALUES (?,?,?,?,?,?)",
    (SRC,
     "United Nations Conference on Trade and Development (UNCTAD) - UNCTAD Data Hub (UNCTADstat)",
     "https://unctadstat.unctad.org/datacentre/dataviewer/US.NonPlasticSubstsTradeByPartner",
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
    if i % 20 == 0:
        print(f"  scanned {(i + 1)}M rows, {len(counts):,} prefixes", flush=True)

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
print(f"prefixes={len(counts):,} inserted={len(rows):,} "
      f"total={con.execute('SELECT COUNT(*) FROM series WHERE source_id=?', (SRC,)).fetchone()[0]:,}")
