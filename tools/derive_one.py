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
# A new object may be this much smaller in BYTES than the one it replaces without comment.
# gzip over nearly-identical CSV is stable to well under a percent; the losses actually
# observed are whole series, not rounding. Row counts, when both sides have them, are compared
# exactly and this tolerance is not consulted.
SHRINK_TOLERANCE = 0.01


def _existing_rows(head):
    """Row count recorded on the object already in R2, or None if it predates the metadata."""
    md = (head or {}).get("Metadata") or {}
    for k in ("rows", "x-amz-meta-rows"):
        if k in md:
            try:
                return int(md[k])
            except (TypeError, ValueError):
                return None
    return None


def shrink_verdict(new_rows, new_bytes, head, tolerance=SHRINK_TOLERANCE):
    """(ok, why) for replacing the object `head` describes with the one just built.

    NEVER SHRINK SILENTLY. A reviewer measured gus_dbw:area_8 as 20,015 bytes smaller locally
    than its served copy, and this tool would have published that and printed OK. Exact when
    both sides know their row count, bytes with a tolerance when the served object predates
    that metadata, and no opinion at all when there is nothing to compare against - a first
    upload cannot shrink anything.
    """
    if not head:
        return True, "no existing object"
    old_rows = _existing_rows(head)
    if old_rows is not None and new_rows is not None:
        if new_rows < old_rows:
            return False, (f"the new CSV has {new_rows:,} rows and the served one has "
                           f"{old_rows:,} - {old_rows - new_rows:,} fewer")
        return True, f"rows {old_rows:,} -> {new_rows:,}"
    old_bytes = head.get("ContentLength")
    if not old_bytes or new_bytes is None:
        return True, "nothing comparable on the served object"
    if new_bytes < old_bytes * (1.0 - tolerance):
        pct = 100.0 * (old_bytes - new_bytes) / old_bytes
        return False, (f"the new object is {new_bytes:,} bytes against {old_bytes:,} served, "
                       f"{pct:.2f}% smaller, and the served copy records no row count")
    return True, f"bytes {old_bytes:,} -> {new_bytes:,}"


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
        print("usage: derive_one.py <series_id> [--bucket econ-data] [--force] "
              "[--allow-shrink]")
        return 2
    sid = sys.argv[1]
    bucket = "econ-data"
    if "--bucket" in sys.argv:
        bucket = sys.argv[sys.argv.index("--bucket") + 1]
    # CREATE-ONLY UNTIL NOW. An existing object short-circuited to SKIP unconditionally, so
    # this tool could not REFRESH a series - which is why the 16 gus_dbw areas serving a
    # nine-day-old vintage could not be repaired with it. Default unchanged; --force opts in.
    force = "--force" in sys.argv
    allow_shrink = "--allow-shrink" in sys.argv

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
    head = None
    try:
        head = r2.client.head_object(Bucket=bucket, Key=key)
    except Exception:                                        # noqa: BLE001
        head = None
    if head and not force:
        print(f"SKIP {sid} (already present; --force to refresh)")
        return 0

    fd, tmp = tempfile.mkstemp(suffix=".csv.gz", prefix="one_")
    os.close(fd)
    try:
        n = d._series_csv_to_file_sorted(sid, tmp)
        ok, why = shrink_verdict(n, os.path.getsize(tmp), head)
        if not ok and not allow_shrink:
            print(f"REFUSE {sid} would publish a REGRESSION: {why}. Nothing uploaded. Pass "
                  f"--allow-shrink if the publisher genuinely withdrew those rows.")
            return 0
        if not ok:
            print(f"SHRINK {sid} allowed by --allow-shrink: {why}")
        d._put_gzip_file_with_backoff(r2.client, bucket, key, tmp, metadata={"rows": str(n)})
        print(f"OK {sid} {n} ({why})")
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
