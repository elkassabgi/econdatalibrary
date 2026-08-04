"""Derive the noaa series that are IN THE STORE but have no CSV in R2.

WHY A TARGETED TOOL. noaa's store grew after its bulk derive: 1,998 series (measured
2026-08-04) exist in the shard parquets and their `__series.parquet` sidecars, but have no
`series/<id>.csv` object, so they are neither downloadable nor catalogued. The general tool,
`derive_csv_bulk.py --source noaa --skip-existing`, cannot be used to fix that cheaply: its
skip-existing pass LISTS the source's objects, and a noaa listing passed 400,000 objects on a
PARTIAL run before being abandoned. This derives a named set instead, so cost is O(missing).

ORDER MATTERS, AND IT IS DERIVE-THEN-CATALOGUE. `tools/catalog_noaa.py` projects the catalogue
from the sidecars, so running it today would add 1,998 rows whose downloads 404 - "listed and
undownloadable", which that tool's own docstring calls worse than being invisible. Its `stale`
check cannot catch this: it tests the key FORMAT (`substr(series_id,1,10)`), not membership.
So: derive first, verify every object exists, and only then catalogue.

FORMAT IS COPIED FROM WHAT IS ALREADY SERVED, not re-derived from the spec. An existing object
(series/noaa%3Agsom%3AACW00011604%3APRCP.csv) reads:

    series_id,obs_date,value\\n
    gsom:ACW00011604:PRCP,1949-01-01,46.2\\n

i.e. the SOURCE PREFIX IS STRIPPED from the id column while the R2 key keeps it, LF line
endings, ContentType text/csv.

    python tools/derive_noaa_missing.py                 # report only
    python tools/derive_noaa_missing.py --apply
"""
from __future__ import annotations

import argparse
import collections
import concurrent.futures as cf
import csv
import glob
import io
import os
import sqlite3
import sys
import urllib.parse

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import duckdb                                                   # noqa: E402
import pyarrow.parquet as pq                                    # noqa: E402

from core import r2_util                                        # noqa: E402

SOURCE = "noaa"
BUCKET = "econ-data"
HEADER = ["series_id", "obs_date", "value"]
STORE = os.path.join(ROOT, "data", "clean_full", SOURCE)
CAT = os.path.join(ROOT, "data", "catalog.db")


def _csv_bytes(short_id: str, rows) -> bytes:
    """Mirrors tools/derive_csv_bulk._csv_bytes and core.derive_csv._series_csv_bytes."""
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(HEADER)
    for d, v in rows:
        w.writerow([short_id, d, v])
    return buf.getvalue().encode("utf-8")


def _r2_key(store_key: str) -> str:
    return f"series/{urllib.parse.quote(f'{SOURCE}:{store_key}', safe='')}.csv"


def store_keys() -> set[str]:
    out = set()
    for p in sorted(glob.glob(os.path.join(STORE, "*__series.parquet"))):
        t = pq.read_table(p, columns=["series_key"])
        out.update(t.column("series_key").to_pylist())
    return out


def catalogued() -> set[str]:
    con = sqlite3.connect(f"file:{CAT}?mode=ro", uri=True, timeout=300)
    try:
        return {r[0][len(SOURCE) + 1:] for r in
                con.execute("SELECT series_id FROM series WHERE source_id=?", (SOURCE,))}
    finally:
        con.close()


def shard_for(store_key: str) -> str:
    """'gsom:AYM00089504:DSND' -> 'gsom__AY.parquet'. Verified against the real store layout;
    any key whose shard is absent is REPORTED, never silently dropped."""
    ds, station = store_key.split(":")[0], store_key.split(":")[1]
    return f"{ds}__{station[:2]}.parquet"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--workers", type=int, default=16)
    a = ap.parse_args()

    s3 = r2_util.client(write=True) if a.apply else r2_util.client()

    keys = store_keys()
    print(f"store series (sidecars): {len(keys):,}")

    def absent(k):
        try:
            s3.head_object(Bucket=BUCKET, Key=_r2_key(k))
            return False
        except Exception:                                        # noqa: BLE001
            return True

    todo = sorted(keys - catalogued())
    print(f"uncatalogued: {len(todo):,}  — checking which of those also lack a CSV")
    with cf.ThreadPoolExecutor(max_workers=a.workers) as ex:
        flags = list(ex.map(absent, todo))
    missing = [k for k, f in zip(todo, flags) if f]
    print(f"MISSING a CSV in R2: {len(missing):,}")
    if not missing:
        print("nothing to derive.")
        return 0

    by_shard = collections.defaultdict(list)
    for k in missing:
        by_shard[shard_for(k)].append(k)
    absent_shards = [s for s in by_shard if not os.path.exists(os.path.join(STORE, s))]
    if absent_shards:
        n = sum(len(by_shard[s]) for s in absent_shards)
        print(f"  !! {len(absent_shards)} shard file(s) absent, covering {n:,} series: "
              f"{absent_shards[:5]}")
    print(f"shards to read: {len(by_shard)}")

    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='4GB'")
    written = 0
    for shard in sorted(by_shard):
        path = os.path.join(STORE, shard)
        if not os.path.exists(path):
            continue
        want = set(by_shard[shard])
        rows = con.execute(
            "SELECT series_key, obs_date, value FROM read_parquet(?) "
            "WHERE series_key IN (SELECT UNNEST(?)) ORDER BY series_key, obs_date",
            [path.replace("\\", "/"), list(want)]).fetchall()
        grouped = collections.OrderedDict()
        for k, d, v in rows:
            grouped.setdefault(k, []).append((d, v))
        got = set(grouped)
        if got != want:
            print(f"  {shard}: {len(want)-len(got)} key(s) had NO rows in the shard "
                  f"(not written): {sorted(want-got)[:3]}")
        for k, rr in grouped.items():
            body = _csv_bytes(k, rr)
            if a.apply:
                s3.put_object(Bucket=BUCKET, Key=_r2_key(k), Body=body,
                              ContentType="text/csv")
            written += 1
        print(f"  {shard:<24} {len(grouped):>5} series {'written' if a.apply else 'ready'}",
              flush=True)

    print(f"\n{'WROTE' if a.apply else 'WOULD WRITE'} {written:,} CSV object(s)")
    if not a.apply:
        print("(dry run — pass --apply)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
