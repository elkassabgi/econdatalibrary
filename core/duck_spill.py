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

WHERE THE SPILL LIVES (`spill_root()`, read at every call, not at import). With ECONDL_DUCKDB_TMP
unset, empty or not absolute it is `<repo>/logs/_duckspill`, as before (a non-absolute value also
prints one warning line on stderr). With it set to an absolute path, it is
`<ECONDL_DUCKDB_TMP>/_duckspill` - a dedicated SUBFOLDER, never the variable's folder itself.
That folder is also the base of insee's persistent staging (`<base>/insee_melodi_staging`,
updater/strategies/fetchers/_melodi_state.staging_dir), derive_csv's pid folders
(core/derive_csv._duck_spill_dir) and the bounded-merge spill folders
(updater/merge._bounded_spill_dir). `sweep_orphans()` removes every directory older than 24 h in
the spill root, so a root equal to the base would delete insee's staging (T0 plan B30, review
condition C-1).

    from core.duck_spill import connection_spill

    with connection_spill("catalog_census") as spill:
        q.execute(f"SET temp_directory='{spill}'")
        ...
"""
from __future__ import annotations

import contextlib
import os
import shutil
import sys
import time
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# The DEFAULT spill root, used when ECONDL_DUCKDB_TMP is unset or empty. A module constant so a test
# can point it at a temporary folder.
SPILL_ROOT = os.path.join(ROOT, "logs", "_duckspill")
ENV_VAR = "ECONDL_DUCKDB_TMP"
ENV_SUBDIR = "_duckspill"     # the subfolder of ECONDL_DUCKDB_TMP; never the variable's folder itself
_WARNED: set = set()          # the non-absolute values already warned about in this process


def spill_root() -> str:
    """The folder the spill directories are made in, and the ONLY folder sweep_orphans() reaps.

    Read at every call, so a variable set after import is honoured.
    - ECONDL_DUCKDB_TMP set to an ABSOLUTE path -> `<ECONDL_DUCKDB_TMP>/_duckspill` (see the module
      note on why it is a subfolder).
    - Unset or empty -> SPILL_ROOT ("empty means unset", the rule of the other three readers:
      core/derive_csv.py, updater/merge.py, _melodi_state.py).
    - Set but NOT absolute (`relspill`, a bare drive `F:`, `F:relspill`, `" "`) -> SPILL_ROOT, and ONE
      warning line on stderr per value per process. Such a value resolves against the cwd, which
      for the T0 runs is the E: checkout. It does not raise: the spill only moves back to where it
      was before this variable was read. stderr, because a tool's stdout can carry its JSON.
    """
    base = os.environ.get(ENV_VAR)
    if base:
        if os.path.isabs(base):
            return os.path.join(base, ENV_SUBDIR)
        if base not in _WARNED:
            _WARNED.add(base)
            print(f"[duck_spill] WARNING: {ENV_VAR}={base!r} is not an absolute path; it is ignored "
                  f"and the spill goes to {SPILL_ROOT}", file=sys.stderr, flush=True)
    return SPILL_ROOT


def spill_path(tag: str) -> str:
    """A path unique to this process AND this call, in DuckDB's posix-slash form.

    The pid alone is not enough: one process opens several connections, and two of them sharing a
    directory is the same crash.
    """
    p = os.path.join(spill_root(), f"{tag}_{os.getpid()}_{uuid.uuid4().hex[:8]}")
    return p.replace(os.sep, "/")


_SWEPT = False


def ensure_spill(tag: str, sweep: bool = True) -> str:
    """Create this run's private spill directory, and reap what a SIGKILL left behind.

    The atexit cleanup in the call sites covers a normal exit and an unhandled exception. It does
    NOT cover SIGKILL, and SIGKILL is how the 83.73 GB found on 2026-09-22 got there - 19 files
    across three directories, the oldest 20.42 days, from processes that ran no handler at all.

    The sweep runs ONCE per process and only on directories older than the guard, so a long query
    in a sibling process cannot lose its temp. Returns the path, already created.
    """
    global _SWEPT
    p = spill_path(tag)
    os.makedirs(p, exist_ok=True)
    if sweep and not _SWEPT:
        _SWEPT = True
        try:
            sweep_orphans()
        except OSError:
            pass          # reaping is best-effort; never let it stop the real work
    return p


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

    Only the children of spill_root() are candidates. With ECONDL_DUCKDB_TMP set that is its
    `_duckspill` subfolder, so the folders beside it (insee_melodi_staging, derive_csv's pid
    folders, merge spill folders) are never listed here, whatever their age.
    """
    root = spill_root()
    if not os.path.isdir(root):
        return (0, 0)
    cutoff = time.time() - older_than_hours * 3600
    removed = freed = 0
    for name in os.listdir(root):
        d = os.path.join(root, name)
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
