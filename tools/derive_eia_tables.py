"""Bulk per-table CSV derive for eia — one streaming pass per dataset file.

Same design as tools/derive_usda_bulk.py (proven byte-exact 30/30 before its full run):
`_resolve.native_to_tidy` sorts by (series_id, obs_date), so `ORDER BY sid, obs_date`
reproduces the served bytes exactly; verify-before-write refuses to PUT unless a random
sample byte-matches `core.derive_csv._series_csv_bytes` — the path users' downloads
actually come from.

eia specifics (all measured, see tools/catalog_eia_tables.py):
  * one parquet per dataset; the table id is a DOT-PREFIX of the native series_id at a
    per-dataset depth (DEPTH map imported from the catalogue tool — one source of truth);
  * IEO.parquet is EXCLUDED: it is a redundant union of the four IEO.<year>.parquet
    files (identical id sets), whose tables are derived from the vintage files;
  * datasets are prefix-disjoint (AEO.2025 vs AEO.IEO2 vs COAL ...), so per-file
    streaming cannot split a table across passes;
  * the 7 legacy series-grain CSVs already exist in R2 and are untouched — this pass
    emits table ids only.
  * NULL value/obs_date rows are dropped, matching the resolver's predicate.

    python tools/derive_eia_tables.py --verify 40 --dry-run
    python tools/derive_eia_tables.py --verify 40 --bucket econ-data
"""
from __future__ import annotations

import argparse
import csv
import io
import os
import random
import sqlite3
import sys
import urllib.parse

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))

from tools.catalog_eia_tables import DEPTH, SKIP, STORE  # noqa: E402  (one source of truth)

SRC = "eia"


def _catalogued() -> set:
    con = sqlite3.connect(f"file:{os.path.join(ROOT, 'data', 'catalog.db')}?mode=ro", uri=True)
    con.execute("PRAGMA busy_timeout = 180000")
    return {r[0] for r in con.execute(
        "select series_id from series where source_id=?", (SRC,))}


def _csv_bytes(rows) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["series_id", "obs_date", "value"])
    for sid, d, v in rows:
        w.writerow([sid, d, v])
    return buf.getvalue().encode("utf-8")


def _prefix_sql(depth: int) -> str:
    # first `depth` dot-segments of series_id (DuckDB lists are 1-based, slice inclusive)
    return f"array_to_string(list_slice(string_split(series_id, '.'), 1, {depth}), '.')"


def _stream(q, path, depth):
    """Yield (catalog_id, [(sid, obs_date, value)...]) one table at a time from one file."""
    q.execute(f"""
        select {_prefix_sql(depth)} as tbl, series_id, obs_date, value
        from read_parquet(?)
        where value is not null and obs_date is not null
        order by tbl, series_id, obs_date""", [path])
    cur, acc = None, []
    while True:
        batch = q.fetchmany(50_000)
        if not batch:
            break
        for tbl, sid, d, v in batch:
            if tbl != cur:
                if cur is not None:
                    yield f"{SRC}:{cur}", acc
                cur, acc = tbl, []
            acc.append((sid, d, v))
    if cur is not None:
        yield f"{SRC}:{cur}", acc


def _dataset_files():
    files = sorted(f for f in os.listdir(STORE) if f.endswith(".parquet"))
    out = []
    for fn in files:
        name = fn[:-len(".parquet")]
        if name in SKIP:
            continue
        out.append((name, os.path.join(STORE, fn).replace(os.sep, "/"), DEPTH[name]))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket")
    ap.add_argument("--prefix", default="series")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--verify", type=int, default=40,
                    help="byte-compare this many RANDOM table ids against the resolver "
                         "FIRST; refuse to write unless every one matches")
    a = ap.parse_args()

    import duckdb
    sets = _dataset_files()
    cat = _catalogued()
    table_ids = {c for c in cat if c.count(":") == 1}  # includes the 7 legacy series ids too
    print(f"{len(sets)} dataset file(s); {len(cat):,} catalogued eia id(s)", flush=True)

    if a.verify:
        from core.derive_csv import _series_csv_bytes
        q = duckdb.connect(); q.execute("PRAGMA memory_limit='6GB'")
        # Sample TABLE ids only (exclude the 7 legacy full-series ids: this pass never
        # writes them; they stay served from their original derive).
        legacy = {c for c in cat if len(c.split(":", 1)[1].split(".")) >
                  DEPTH.get(c.split(":", 1)[1].split(".")[0], 99)}
        pool = sorted(cat - legacy)
        sample = random.Random(20260808).sample(pool, min(a.verify, len(pool)))
        bad = 0
        for i, sid in enumerate(sample, 1):
            native = sid.split(":", 1)[1]
            # route to the dataset file exactly like the resolver: longest filename prefix
            cand = [(n, p, d) for (n, p, d) in sets if native == n or native.startswith(n + ".")]
            n, p, d = max(cand, key=lambda t: len(t[0]))
            rows = q.execute(f"""
                select series_id, obs_date, value from read_parquet(?)
                where ({_prefix_sql(d)}) = ? and value is not null and obs_date is not null
                order by series_id, obs_date""", [p, native]).fetchall()
            if _csv_bytes(rows) != _series_csv_bytes(sid):
                bad += 1
                print(f"   MISMATCH {sid}")
            if i % 10 == 0:
                print(f"   verified {i}/{len(sample)}", flush=True)
        print(f"verify: {len(sample) - bad}/{len(sample)} byte-identical to the resolver")
        if bad:
            print("REFUSING to write — this path does not reproduce what users download.")
            return 1

    if a.dry_run:
        print("dry run — nothing written")
        return 0
    if not a.bucket:
        print("pass --bucket to write")
        return 2

    from core import r2_util
    from core.derive_csv import _put_with_backoff
    s3 = r2_util.client()
    q = duckdb.connect(); q.execute("PRAGMA memory_limit='6GB'")
    put = skipped = 0
    for name, path, depth in sets:
        print(f" {name} (depth {depth})", flush=True)
        for cid, rows in _stream(q, path, depth):
            if cid not in table_ids:
                skipped += 1
                continue
            key = f"{a.prefix}/" + urllib.parse.quote(cid, safe="") + ".csv"
            _put_with_backoff(s3, a.bucket, key, _csv_bytes(rows))
            put += 1
            if put % 5000 == 0:
                print(f"   put {put:,}", flush=True)
    print(f"done: put {put:,} table CSVs; {skipped:,} store tables have no catalogue row")
    return 0


if __name__ == "__main__":
    sys.exit(main())
