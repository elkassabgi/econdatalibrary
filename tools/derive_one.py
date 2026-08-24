#!/usr/bin/env python3
"""Derive ONE series to R2, in its own process.

WHY THIS EXISTS. This was written to contain a crash I had misdiagnosed: the sorted-streaming
derive kept dying with no traceback, and I concluded the fault was unexplainable and could only
be isolated. It was not. Every DuckDB connection was spilling its external sort to the same
shared temp path, so concurrent sorts collided on one filename - "Access is denied" between
processes, a silent native crash between threads. That is fixed at the source in
core/derive_csv.py (`_duck_spill_dir`, one spill directory per process). See ledger R467.

The per-process model is kept anyway because it earns its place independently: --skip-existing
makes retries free, one bad table cannot take a queue with it, and shard count is a shell loop
rather than a threading argument. It is no longer a workaround for a mystery.

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


# Largest PROVEN-servable table: gus_dbw:area_46, 529,322,150 rows -> a 3.11 GB gzipped
# object (area_16, 358,524,120 rows -> 1.34 GB). The ceiling sits above those and far below
# the two cbs_nl monsters (1,886,692,500 and 1,056,918,900 rows), which at the measured
# ~190 bytes/row are ~360 GB and ~200 GB of plain CSV - more than the staging drive holds.
# Set from what has actually been derived, not from a round number: my first guess of
# 500,000,000 would have refused area_46, which serves fine.
MAX_ROWS = 750_000_000


def _row_count(sid: str):
    """Rows in this series' store file, from parquet footer metadata only (no data read).

    Returns None when the count cannot be established — an unknown size must not silently
    pass a ceiling check, but neither should it block a derive that would otherwise work,
    so the caller treats None as "no evidence" and proceeds.
    """
    try:
        import pyarrow.parquet as pq
        from clients.python.econdl import _resolve
        r = _resolve.resolve(sid)
        path = getattr(r, "parquet_path", None)
        if not path or not os.path.exists(str(path)):
            return None
        return pq.read_metadata(str(path)).num_rows
    except Exception:                                        # noqa: BLE001
        return None


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

    # SIZE CEILING LIVES HERE, NOT IN THE QUEUE FILE (R469). Two cbs_nl tables are ~1.9B and
    # ~1.1B rows; at the measured ~190 bytes/row they are ~360 GB and ~200 GB as a single CSV -
    # unservable, and bigger than the free space on the drive they stage to. I excluded them
    # from one queue file, then rebuilt that file from a fresh reconciliation, which of course
    # listed them again, and a 4-hour-timeout pass started writing both. A ceiling the caller
    # can forget is not a ceiling.
    n_rows = _row_count(sid)
    if n_rows and n_rows > MAX_ROWS:
        print(f"REFUSE {sid} {n_rows:,} rows exceeds the {MAX_ROWS:,}-row single-CSV ceiling; "
              f"needs the #part split convention")
        return 0

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
        # This process's private DuckDB spill dir; pids get reused, so don't leave it.
        try:
            import shutil
            shutil.rmtree(d._duck_spill_dir().replace("/", os.sep), ignore_errors=True)
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
