"""THE catalogue resolver of the econ self-hosting move (docs/ECON_SELF_HOSTING_PLAN.md, section 3, change
4a; review R1167 A).

Every catalogue open is meant to go through connect() here. tests/test_catalog_path.py fails when a NEW
file names catalog.db any other way; the files that do so today are listed in
tests/catalog_db_legacy.txt and move here one by one (that list may only shrink).

Rules:
  * Before T0 the catalogue is the checkout's data/catalog.db, as today.
  * After T0 (core/cutover.py) it is ONE fixed machine-wide build, BUILD_PATH, whichever worktree the
    process runs from - so a run from any of the dozens of worktrees cannot become a second catalogue.
  * The file is opened with a mode=ro or mode=rw URI, which NEVER creates it: a missing catalogue is an
    error, never a new empty one that a later step would publish.
  * After T0 a WRITE connection also needs the single-writer lock (writer_lock()), held by this process:
    two writers on one build is how a half-written catalogue gets served.
The paths are module constants with no environment override (the same reason as core/cutover.py: an
override is a way around the rule); tests monkeypatch them.
"""
from __future__ import annotations

import contextlib
import os
import pathlib
import sqlite3

from core.cutover import CutoverRefused, is_cut_over

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHECKOUT_PATH = os.path.join(ROOT, "data", "catalog.db")
BUILD_PATH = r"E:\econ_live\catalog\catalog.db"
LOCK_PATH = r"E:\econ_live\state\writer.lock"
# The other fixed machine-wide places a post-T0 writer must use (plan change 4; updater/run.py refuses a
# run whose configuration points anywhere else, so a worktree can never become a second writer).
LIVE_STATE_DIR = r"E:\econ_live\state"
LIVE_STORE_ROOT = r"E:\research\econfindatalibrary"

_held: object | None = None           # the open lock file while this process holds the writer lock


def catalog_path() -> str:
    """The catalogue this process must use: the fixed build after T0, the checkout's before."""
    return BUILD_PATH if is_cut_over() else CHECKOUT_PATH


def connect(*, write: bool = False, timeout: float = 60.0) -> sqlite3.Connection:
    """Open the catalogue. Never creates it. After T0 a write needs writer_lock() held by this process."""
    path = catalog_path()
    if write and is_cut_over() and _held is None:
        raise CutoverRefused(f"refused: a write to the catalogue build {path} needs the single-writer lock "
                             f"({LOCK_PATH}); take it with core.catalog_path.writer_lock()")
    if not os.path.isfile(path):
        raise FileNotFoundError(f"no catalogue at {path} (it is never created implicitly)")
    uri = pathlib.Path(path).resolve().as_uri() + ("?mode=rw" if write else "?mode=ro")
    return sqlite3.connect(uri, uri=True, timeout=timeout)


@contextlib.contextmanager
def writer_lock():
    """Hold the machine-wide single-writer lock for the duration. Fails at once (never waits) when another
    process holds it: two writers are refused, not queued behind each other unseen."""
    global _held
    if _held is not None:
        raise RuntimeError("writer_lock() is already held by this process")
    os.makedirs(os.path.dirname(LOCK_PATH), exist_ok=True)
    fh = open(LOCK_PATH, "a+b")
    try:
        _lock(fh)
    except OSError:
        fh.close()
        raise CutoverRefused(f"refused: another process holds the catalogue writer lock {LOCK_PATH}") from None
    _held = fh
    try:
        fh.seek(0)
        fh.truncate()
        fh.write(f"pid {os.getpid()}\n".encode())
        fh.flush()
        yield
    finally:
        _held = None
        try:
            _unlock(fh)
        finally:
            fh.close()


if os.name == "nt":
    import msvcrt

    def _lock(fh):
        fh.seek(0)
        msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)

    def _unlock(fh):
        fh.seek(0)
        msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
else:
    import fcntl

    def _lock(fh):
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    def _unlock(fh):
        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
