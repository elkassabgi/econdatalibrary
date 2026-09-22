"""One DuckDB spill directory PER CONNECTION, created and removed around the work.

WHY THIS EXISTS. DuckDB names its spill files by block size - `duckdb_temp_storage_DEFAULT-3.tmp`,
`duckdb_temp_storage_S128K-2.tmp` - with NO instance id in the name. Two DuckDB instances pointed
at the same `temp_directory` therefore open each other's files, and the process segfaults with no
traceback. R612 measured it at N = 2, 8 and 16 connections: all exit 139.

`tools/mirror_sync.py` has had the fix since R612 - a per-process, per-connection subdirectory. Four
other tools kept pointing `temp_directory` at the shared root, so any two of them running together
are exactly the configuration that was measured crashing. This module is that fix, extracted once so
the five call sites cannot drift apart again (R1061: a fix applied to one call site and not its
twins is a fraction of a fix).

IT ALSO STOPS THE ORPHANING. The spill is removed in a `finally`, so a normal exit and an exception
both clean up. Measured 2026-09-22: 32 orphaned spill files had accumulated in `logs/`, 73.37 GB -
97.8% of that directory - left by processes that died on 2026-09-09 without removing their temp.
A SIGKILL still orphans, which is why `sweep_orphans()` exists for the caller that wants it.

    from core.duck_spill import connection_spill

    with connection_spill("catalog_census") as spill:
        q.execute(f"SET temp_directory='{spill}'")
        ...
"""
from __future__ import annotations

import contextlib
import os
import shutil
import time
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SPILL_ROOT = os.path.join(ROOT, "logs", "_duckspill")


def spill_path(tag: str) -> str:
    """A path unique to this process AND this call, in DuckDB's posix-slash form.

    The pid alone is not enough: one process opens several connections, and two of them sharing a
    directory is the same crash.
    """
    p = os.path.join(SPILL_ROOT, f"{tag}_{os.getpid()}_{uuid.uuid4().hex[:8]}")
    return p.replace(os.sep, "/")


@contextlib.contextmanager
def connection_spill(tag: str):
    """Yield a private spill directory and remove it afterwards, on success or on an exception."""
    p = spill_path(tag)
    os.makedirs(p, exist_ok=True)
    try:
        yield p
    finally:
        shutil.rmtree(p, ignore_errors=True)


def sweep_orphans(older_than_hours: float = 24.0) -> tuple:
    """Remove spill directories no live run can still be using. Returns (removed, bytes).

    Age is the guard rather than a pid check, because a pid is reused and a directory whose owner
    was SIGKILLed has no owner left to ask. Nothing younger than `older_than_hours` is touched, so a
    long-running query cannot lose its temp under it.
    """
    if not os.path.isdir(SPILL_ROOT):
        return (0, 0)
    cutoff = time.time() - older_than_hours * 3600
    removed = freed = 0
    for name in os.listdir(SPILL_ROOT):
        d = os.path.join(SPILL_ROOT, name)
        if not os.path.isdir(d):
            continue
        try:
            if os.path.getmtime(d) > cutoff:
                continue
            size = sum(os.path.getsize(os.path.join(dp, f))
                       for dp, _dn, fs in os.walk(d) for f in fs)
        except OSError:
            continue
        shutil.rmtree(d, ignore_errors=True)
        if not os.path.isdir(d):
            removed += 1
            freed += size
    return (removed, freed)
