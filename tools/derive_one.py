#!/usr/bin/env python3
"""Derive ONE series to R2, in its own process. Isolation is the whole point.

WHY THIS EXISTS. The sorted-streaming path (DuckDB COPY + external sort) works perfectly on
a single table in a fresh process — cbs_nl:83999NED, 39.5M rows, 124,325,147 bytes,
byte-identical, twice. Inside core/derive_csv.py's loop it SEGFAULTS: bash reports
"Segmentation fault" and the process dies with no traceback, taking the rest of that shard's
queue with it. Twelve shards died producing nothing; three shards died the same way; the
crash is not memory (3GB and 12GB limits both), not the S3 client (tested loaded first), and
not the table (derives fine alone).

An unexplained native crash inside a long-lived process is not something to keep guessing at
while 86 tables stay unserved. One process per table turns a fatal crash into a single
failed table that the next run retries, and --skip-existing means retries cost nothing.

Prints one line per outcome so a wrapper loop can count successes without parsing tracebacks.
"""
from __future__ import annotations

import os
import sys
import tempfile
import urllib.parse

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: derive_one.py <series_id> [--bucket econ-data]")
        return 2
    sid = sys.argv[1]
    bucket = "econ-data"
    if "--bucket" in sys.argv:
        bucket = sys.argv[sys.argv.index("--bucket") + 1]

    from core import derive_csv as d
    from updater.blob import R2Blob

    key = "series/" + urllib.parse.quote(sid, safe="") + ".csv"
    r2 = R2Blob()
    try:
        r2.client.head_object(Bucket=bucket, Key=key)
        print(f"SKIP {sid} (already present)")
        return 0
    except Exception:
        pass

    fd, tmp = tempfile.mkstemp(suffix=".csv.gz", prefix="one_")
    os.close(fd)
    try:
        n = d._series_csv_to_file_sorted(sid, tmp)
        d._put_gzip_file_with_backoff(r2.client, bucket, key, tmp)
        print(f"OK {sid} {n}")
        return 0
    except Exception as e:                                   # noqa: BLE001
        print(f"FAIL {sid} {type(e).__name__}: {str(e)[:120]}")
        return 1
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass


if __name__ == "__main__":
    sys.exit(main())
