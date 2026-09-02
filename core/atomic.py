"""One atomic file replacement that survives a reader holding the target open.

WHY THIS EXISTS. On Windows `os.replace(tmp, dest)` raises `PermissionError [WinError 5]` while
another process holds `dest` open - and every tool in this repository that reads a parquet
footer does exactly that. A reviewer demonstrated the collision in both directions on
2026-09-02: a plain read handle and a `pyarrow.ParquetFile` handle each make the writer's
replace fail, while a DuckDB `read_parquet` cursor does not.

The consequence is not theoretical. `catalog_table_grain._date_range` and `derive_one._row_count`
both open footers, and both would be pointed at stores whose crawlers are writing - so
measuring the catalogue could kill the crawl. Today that fear is why the gus_dbw repair cannot
be run at all: its crawler writes continuously, and the 16 areas serving a nine-day-old vintage
stay stale because touching their files might stop the process that maintains them.

A bounded retry removes the whole class. A footer read holds the file for milliseconds, so a
handful of short sleeps clears every real collision, and a genuinely stuck handle still surfaces
as an error rather than being swallowed - which is the half that matters, because a replace that
silently did nothing is how a run reports success having written nothing (R641).

POSIX renames regardless of open handles, so this is a no-op there by construction rather than
by accident.
"""
from __future__ import annotations

import os
import time

# A footer read holds the file for milliseconds. These waits sum to about 3.1 seconds, which is
# three orders of magnitude more than the collision needs and still short enough that a real
# lock - another writer, an antivirus scan that never ends - is reported rather than waited on.
BACKOFF_S = (0.05, 0.1, 0.2, 0.4, 0.8, 1.6)


def atomic_replace(tmp: str, dest: str, retries: "tuple | None" = None,
                   on_retry=None) -> None:
    """`os.replace(tmp, dest)`, retried while a reader holds `dest` open.

    Raises the last `PermissionError` if every attempt fails, because a replace that quietly
    did not happen is worse than one that failed loudly: the caller goes on to report the new
    data as written. Every other OSError is raised immediately - a missing directory or a
    cross-device link is not going to resolve itself in 3.1 seconds.
    """
    waits = BACKOFF_S if retries is None else tuple(retries)
    last = None
    for i, wait in enumerate((*waits, None)):
        try:
            os.replace(tmp, dest)
            return
        except PermissionError as e:                 # a reader holds dest (Windows)
            last = e
            if wait is None:
                break
            if on_retry is not None:
                on_retry(i + 1, wait, e)
            time.sleep(wait)
    raise last


def replaced_after(tmp: str, dest: str) -> int:
    """`atomic_replace`, returning how many retries it took. For callers that log."""
    n = 0

    def count(_attempt, _wait, _e):
        nonlocal n
        n += 1

    atomic_replace(tmp, dest, on_retry=count)
    return n
