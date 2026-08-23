"""Bulk per-table CSV derive for usda — one streaming pass instead of 69,704 x 60-file scans.

WHY usda NEEDS ITS OWN TOOL. `core/derive_csv.py` resolves each catalogue id independently, and
`_resolve_usda` deliberately scans ALL 60 parquets every time: 61,644 of 72,046 tables (86%) have
rows in more than one file, so narrowing to one file would silently return a fragment. Measured
2026-08-07: 337 objects in 90 minutes = 3.7/min, i.e. ~314 hours for the source. And
`tools/derive_csv_bulk.py` cannot help either — it groups by the parquet's `series_key`, of which
usda has 15,537,982, against 69,704 catalogued TABLE ids.

WHY A BULK PASS TURNED OUT TO BE POSSIBLE AFTER ALL. I first recorded that byte-parity was
unreachable because the served row order followed an unsorted dataset scan, which a streaming
group-by (which must ORDER BY to find its boundaries) could never reproduce. That was wrong, and
checking rather than believing it is what found the tool: `_resolve.native_to_tidy` ends with

    return out.sort_values(["series_id", "obs_date"]).reset_index(drop=True)

so the served bytes ARE sorted, and `ORDER BY sid, obs_date` reproduces them exactly. Verified
byte-for-byte on a real table before this file was written.

THE TWO DETAILS THAT MAKE THE BYTES MATCH, both found by diffing against the resolver rather than
by reading code:
  * the store's `series_key` ALREADY carries the `usda:` prefix — do not add it again;
  * the served id appends the row_id_from pair as `|REFERENCE_PERIOD_DESC=<value>`, NOT the bare
    value. Maryland winter wheat 2020 carries six figures under one key (MAY/JUN/JUL/AUG
    FORECAST, JUN ACREAGE, final YEAR), so that suffix is what separates a forecast from the
    final estimate.
  * NULL value/obs_date rows are dropped, matching the resolver's predicate — without it a
    suppressed cell would appear as `nan` in front of a reader.

    python tools/derive_usda_bulk.py --verify 40 --dry-run
    python tools/derive_usda_bulk.py --verify 40 --bucket econ-data
"""
from __future__ import annotations

import argparse
import csv
import glob
import io
import os
import random
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))

SRC = "usda"


def _files():
    return sorted(f.replace(os.sep, "/")
                  for f in glob.glob(os.path.join(ROOT, "data", "clean_full", SRC,
                                                  "**", "*.parquet"), recursive=True))


def _catalogued() -> set:
    con = sqlite3.connect(f"file:{os.path.join(ROOT, 'data', 'catalog.db')}?mode=ro", uri=True)
    con.execute("PRAGMA busy_timeout = 180000")
    return {r[0] for r in con.execute(
        "select series_id from series where source_id=?", (SRC,))}


def _csv_bytes(rows) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")      # byte-for-byte with the dev shim / Worker
    w.writerow(["series_id", "obs_date", "value"])
    for sid, d, v in rows:
        w.writerow([sid, d, v])
    return buf.getvalue().encode("utf-8")


def _stream(q, files):
    """Yield (catalog_id, [(sid, obs_date, value)...]) one table at a time, in one pass."""
    q.execute(f"""
        select SOURCE_DESC, AGG_LEVEL_DESC, SHORT_DESC,
               series_key || '|REFERENCE_PERIOD_DESC=' || REFERENCE_PERIOD_DESC as sid,
               obs_date, value
        from read_parquet({files})
        where value is not null and obs_date is not null
        order by SOURCE_DESC, AGG_LEVEL_DESC, SHORT_DESC, sid, obs_date""")
    cur_key, acc = None, []
    while True:
        batch = q.fetchmany(50_000)
        if not batch:
            break
        for s, a, sh, sid, d, v in batch:
            k = (s, a, sh)
            if k != cur_key:
                if cur_key is not None:
                    yield f"{SRC}:{cur_key[0]}|{cur_key[1]}|{cur_key[2]}", acc
                cur_key, acc = k, []
            acc.append((sid, d, v))
    if cur_key is not None:
        yield f"{SRC}:{cur_key[0]}|{cur_key[1]}|{cur_key[2]}", acc


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket")
    ap.add_argument("--prefix", default="series")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--verify", type=int, default=40,
                    help="byte-compare this many RANDOM tables against the resolver FIRST; "
                         "refuse to write unless every one matches")
    a = ap.parse_args()

    import duckdb
    files = _files()
    if not files:
        print("no usda parquet under data/clean_full/usda")
        return 2
    cat = _catalogued()
    print(f"{len(files)} parquet file(s); {len(cat):,} catalogued table id(s)", flush=True)

    # VERIFY BEFORE WRITING, not after. The whole risk of a second derive path is that it agrees
    # with itself and disagrees with the resolver users' downloads come from.
    if a.verify:
        from core.derive_csv import _series_csv_bytes
        q = duckdb.connect(); q.execute("PRAGMA memory_limit='6GB'")
        sample = random.Random(20260808).sample(sorted(cat), min(a.verify, len(cat)))
        bad = 0
        for i, sid in enumerate(sample, 1):
            src_d, agg, short = sid.split(":", 1)[1].split("|", 2)
            rows = q.execute(f"""
                select series_key || '|REFERENCE_PERIOD_DESC=' || REFERENCE_PERIOD_DESC as sid,
                       obs_date, value
                from read_parquet({files})
                where SOURCE_DESC=? and AGG_LEVEL_DESC=? and SHORT_DESC=?
                  and value is not null and obs_date is not null
                order by sid, obs_date""", [src_d, agg, short]).fetchall()
            if _csv_bytes(rows) != _series_csv_bytes(sid):
                bad += 1
                print(f"   MISMATCH {sid[:100]}")
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
    import urllib.parse
    s3 = r2_util.client()
    q = duckdb.connect(); q.execute("PRAGMA memory_limit='6GB'")
    put = skipped = 0
    for cid, rows in _stream(q, files):
        if cid not in cat:
            skipped += 1                      # a table the catalogue does not offer
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
