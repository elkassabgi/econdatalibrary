"""One-shot: catalogue bea's full tree at SERIES grain (task #65).

MEASURED 2026-08-10 (full-tree census, _bea_census.log): 67,696,973 obs /
913,230 distinct series across 592 parquets — bea.parquet plus 12 dataset
subdirectories (NIPA, Regional, GDPbyIndustry, ...). Only 240 were catalogued;
the rest were dark solely on D1 headroom, which task #45's NOAA shard freed
(primary 6.34 GB, headroom ~3.66 GB; these rows cost ~0.75 GB).

GRAIN: series. The resolver (_resolve_bea) opens the WHOLE tree as one dataset
and exact-matches series_key, with dedup_on for the byte-identical replication
of the same key across tables (its documented design; #82 adjudicated the
under-keyed class as no-live-loss). Catalogue id = bea:<series_key>, exactly
the 240 existing ids' shape.

TITLES: the ingest discards LineDescription (jobs/ingest_bea_full.py keeps only
SeriesCode/TimePeriod/DataValue), so titles are NOT minable from the store.
Precedent (critmin, oceantrade, tradeservcatbypartner): ship code-titles now,
enrich later via a pipeline/title_bea.py against BEA's metadata endpoints.
The 240 existing rows keep their real titles — INSERT OR IGNORE never
overwrites.

frequency is parsed from the ':A'/':Q'/':M' key suffix where present.
"""
import collections
import glob
import os
import sqlite3
import sys

import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = "bea"
STORE = os.path.join(ROOT, "data", "clean_full", "bea")

con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=7200)
con.execute("PRAGMA busy_timeout=7200000")  # local heavy runs hold long write txns (R400)

files = sorted(glob.glob(os.path.join(STORE, "**", "*.parquet"), recursive=True))
print(f"{len(files)} parquet files")
keys: set[str] = set()
for i, f in enumerate(files):
    pf = pq.ParquetFile(f)
    if "series_key" not in pf.schema_arrow.names:
        print(f"  SKIP (no series_key): {os.path.relpath(f, STORE)}")
        continue
    for b in pf.iter_batches(columns=["series_key"], batch_size=500_000):
        keys.update(b.column(0).to_pylist())
    if i % 100 == 0:
        print(f"  {i}/{len(files)} files, {len(keys):,} keys", flush=True)
print(f"distinct keys: {len(keys):,}")

existing = {r[0] for r in con.execute(
    "SELECT series_id FROM series WHERE source_id=?", (SRC,))}
print(f"already catalogued: {len(existing):,}")

def freq_of(k: str) -> str | None:
    tail = k.rsplit(":", 1)[-1] if ":" in k else ""
    return tail if tail in ("A", "Q", "M") else None

rows = [(f"{SRC}:{k}", SRC, k, freq_of(k), None, None, None, "us-public-domain",
         None, None, None, "{}")
        for k in sorted(keys) if f"{SRC}:{k}" not in existing]
print(f"to insert: {len(rows):,}")
con.executemany(
    "INSERT OR IGNORE INTO series (series_id, source_id, title, frequency, unit, geography, "
    "category, license_id, start_date, end_date, last_updated, metadata) "
    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows)
con.commit()
total = con.execute("SELECT COUNT(*) FROM series WHERE source_id=?", (SRC,)).fetchone()[0]
print(f"inserted={len(rows):,} total={total:,} (expected {len(keys):,})")
if total != len(keys):
    print(f"WARNING: total {total:,} != distinct keys {len(keys):,} "
          f"(the {len(existing)} pre-existing ids must all be within the key set)",
          file=sys.stderr)
    sys.exit(1)
