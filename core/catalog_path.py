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
# THE post-T0 places (plan change 4). The production checkout's own files ARE the build (review R1176: a
# separate E:\econ_live\catalog build left the updater - and ~150 modules that open <root>/data/catalog.db -
# writing a different file from the one the resolver named). updater/run.py pins the updater to this root
# and refuses every path override that would point elsewhere; only the lock lives outside the checkout.
LIVE_STORE_ROOT = r"E:\research\econfindatalibrary"
BUILD_PATH = os.path.join(LIVE_STORE_ROOT, "data", "catalog.db")
LIVE_STATE_DIR = os.path.join(LIVE_STORE_ROOT, "data", "_aqueduct")
LOCK_PATH = r"E:\econ_live\state\writer.lock"

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


def connect_path(path: str | os.PathLike, *, write: bool, timeout: float = 60.0, **kw) -> sqlite3.Connection:
    """The one-line replacement for a tool's own `sqlite3.connect(<catalogue path>)` (plan step 1): the tool
    keeps its --db argument and its tests keep their temporary catalogues.

    Before T0 it opens `path` as the tool did (mode=ro for a read, which a reader never needed to write;
    mode=rw for a write - neither creates a missing file). After T0 it opens only THE build: any other path
    is refused (a copy, a worktree's data/catalog.db, a stale path in a script), and a write needs
    writer_lock() held by this process. `kw` goes to sqlite3.connect (detect_types, check_same_thread...)."""
    p = os.fspath(path)
    if is_cut_over():
        if os.path.normcase(os.path.realpath(p)) != os.path.normcase(os.path.realpath(BUILD_PATH)):
            raise CutoverRefused(f"refused: after T0 the catalogue is {BUILD_PATH}, not {p}")
        if write and _held is None:
            raise CutoverRefused(f"refused: a write to the catalogue build {p} needs the single-writer lock "
                                 f"({LOCK_PATH}); take it with core.catalog_path.writer_lock()")
    if not os.path.isfile(p):
        raise FileNotFoundError(f"no catalogue at {p} (it is never created implicitly)")
    uri = pathlib.Path(p).resolve().as_uri() + ("?mode=rw" if write else "?mode=ro")
    return sqlite3.connect(uri, uri=True, timeout=timeout, **kw)


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
        _recover_hot_journal()
        yield
    finally:
        _held = None
        try:
            _unlock(fh)
        finally:
            fh.close()


def _recover_hot_journal() -> None:
    """A writer killed mid-transaction leaves a HOT rollback journal (catalog.db is in rollback-journal
    mode). Until a read-write open rolls it back, every mode=ro open fails, and a copy of the file is torn
    (review R1176, measured). The new lock holder is the only writer, so it opens the catalogue read-write
    once - SQLite rolls a hot journal back on the first read - and refuses if a journal is still there."""
    path = catalog_path()
    if not os.path.isfile(path):
        return
    con = sqlite3.connect(pathlib.Path(path).resolve().as_uri() + "?mode=rw", uri=True, timeout=60)
    try:
        con.execute("SELECT count(*) FROM sqlite_master").fetchone()
    except sqlite3.OperationalError as e:
        raise RuntimeError(f"the catalogue at {path} could not be opened for recovery ({e}): another process is "
                           "writing it WITHOUT the writer lock - a legacy opener (tests/catalog_db_legacy.txt)? "
                           "Find and stop it; do not copy or serve the catalogue meanwhile") from None
    finally:
        con.close()
    if journal_is_hot(path + "-journal"):
        raise RuntimeError(f"the catalogue at {path} still has a live rollback journal after recovery: another "
                           "process is mid-write WITHOUT the writer lock (a legacy opener?), or the journal is "
                           "damaged. Do not copy or serve it - inspect it first")


# The first 8 bytes of a rollback journal that still holds a transaction. journal_mode=PERSIST leaves the
# file in place after a commit with its header ZEROED, and TRUNCATE leaves it empty - neither is hot, and
# refusing on "exists and not empty" alone refused every PERSIST-mode catalogue (AR-153).
JOURNAL_MAGIC = bytes.fromhex("d9d505f920a163d7")


def journal_is_hot(journal: str) -> bool:
    try:
        with open(journal, "rb") as fh:
            return fh.read(8) == JOURNAL_MAGIC
    except FileNotFoundError:
        return False


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
