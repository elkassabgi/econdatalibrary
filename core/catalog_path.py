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
import sys
import threading

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


# ---- THE RUNTIME GUARD (reviews R1209, R1214) ----------------------------------------------------------------
# The static rules (tests/test_catalogue_writers_*) see only the spellings they were written for. The first
# runtime guard (R1209) matched the PATH given to sqlite3.connect - and a path has too many spellings: an
# ATTACH of the build, a \\?\ prefix, an admin share, a hard link, a URI with a '#' all wrote the build with no
# lock after T0 (R1214). So the rule is enforced where SQLite itself decides what a statement touches:
#   * this module wraps sqlite3.connect (and sqlite3.dbapi2.connect). After T0 every connection that passes
#     through it gets an AUTHORIZER, which SQLite consults for every statement it prepares: a write to the
#     main database when that database IS the build (os.path.samefile: the same file, however it was named),
#     and ANY write to an attached database (SQLite does not say which file a parameter-bound ATTACH named),
#     are refused unless this process holds the writer lock (_authorizer has the exact rule);
#   * an audit hook on the "sqlite3.connect/handle" event refuses, after T0, a connection that did NOT pass
#     through the wrapper (a `from sqlite3 import connect` bound before this module was imported) - so no
#     connection in this process escapes the authorizer.
# Before T0 nothing is installed; the cost is one flag check per connect. Not covered (no SQLite hook sees
# them): the backup API writing INTO a build connection, VACUUM (no authorizer code), a connection opened
# before T0 or one whose authorizer the caller removed (set_authorizer(None) is a deliberate bypass), a
# file-level replace of the build, and processes that never import this module (the legacy list and t0_ready
# cover those).
_WRITE_ACTIONS = frozenset(getattr(sqlite3, n) for n in (
    "SQLITE_INSERT", "SQLITE_UPDATE", "SQLITE_DELETE", "SQLITE_CREATE_TABLE", "SQLITE_CREATE_INDEX",
    "SQLITE_CREATE_TRIGGER", "SQLITE_CREATE_VIEW", "SQLITE_CREATE_VTABLE", "SQLITE_DROP_TABLE",
    "SQLITE_DROP_INDEX", "SQLITE_DROP_TRIGGER", "SQLITE_DROP_VIEW", "SQLITE_DROP_VTABLE", "SQLITE_ALTER_TABLE",
    "SQLITE_REINDEX", "SQLITE_ANALYZE") if hasattr(sqlite3, n))
_real_connect = getattr(sys, "_econ_catalog_real_connect", None) or sqlite3.connect
sys._econ_catalog_real_connect = _real_connect
_passing = threading.local()


def _is_build(path) -> bool:
    """Whether `path` names THE build file - by identity, so a \\\\?\\ prefix, a share, a hard link or a junction
    is still the build. A missing path is not."""
    if not path:
        return False
    try:
        return os.path.samefile(path, BUILD_PATH)
    except (OSError, ValueError, TypeError):
        return False


# pragmas that change the FILE (its header or its format) when given a value, and two that write whatever they
# are given; everything else (busy_timeout, table_info, cache_size, ...) is a reader's business and is allowed
_FILE_PRAGMAS = frozenset({"journal_mode", "user_version", "application_id", "schema_version", "writable_schema",
                           "page_size", "auto_vacuum"})
_WRITING_PRAGMAS = frozenset({"incremental_vacuum", "optimize"})


def _authorizer(main_is_build: bool):
    """The rule, per connection, after T0 and without the writer lock: a statement may write its MAIN database
    when that is not the build, and TEMP - nothing else. An ATTACHed database is never written: SQLite names the
    attached file to the authorizer only when the ATTACH is a string literal (a bound parameter or an expression
    arrives as None, measured on 3.50.4), so which file a schema is cannot be known here, and the schema is
    refused whatever it is. Attaching - to read - is allowed. A pragma that changes the file is refused on the
    same terms; an unqualified one (SQLite passes no schema) counts as touching every attached database."""
    attached = [False]

    def check(action, arg1, arg2, dbname, source):
        if _held is not None:
            return sqlite3.SQLITE_OK
        if action == sqlite3.SQLITE_ATTACH:
            attached[0] = True
            return sqlite3.SQLITE_OK
        if action in _WRITE_ACTIONS:
            if dbname == "temp" or (dbname == "main" and not main_is_build):
                return sqlite3.SQLITE_OK
            return sqlite3.SQLITE_DENY
        if action == sqlite3.SQLITE_PRAGMA and arg1:
            name = arg1.lower()
            if name in _WRITING_PRAGMAS or (name in _FILE_PRAGMAS and arg2 is not None):
                if dbname == "temp" or (dbname == "main" and not main_is_build):
                    return sqlite3.SQLITE_OK
                if dbname is None and not main_is_build and not attached[0]:
                    return sqlite3.SQLITE_OK
                return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK
    return check


def _guarded_connect(*args, **kwargs):
    _passing.on = True
    try:
        conn = _real_connect(*args, **kwargs)
    finally:
        _passing.on = False
    if is_cut_over():
        main = next((r[2] for r in conn.execute("PRAGMA database_list") if r[1] == "main"), "")
        conn.set_authorizer(_authorizer(_is_build(main)))
    return conn


def _audit(event: str, args) -> None:
    if event != "sqlite3.connect/handle" or getattr(_passing, "on", False) or not is_cut_over():
        return
    raise CutoverRefused("refused: after T0 every sqlite3 connection goes through core.catalog_path's guard, and "
                         "this one did not (a `from sqlite3 import connect` bound before core.catalog_path was "
                         "imported?) - call sqlite3.connect after importing it (R1214)")


# once per process: a hook cannot be removed, and a second copy of this module (a test that loads it by path)
# must not wrap twice - the first copy's functions read that copy's globals, which the tests monkeypatch
if not getattr(sys, "_econ_catalog_audit_installed", False):
    sqlite3.connect = _guarded_connect
    sqlite3.dbapi2.connect = _guarded_connect
    sys.addaudithook(_audit)
    sys._econ_catalog_audit_installed = True


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


def under(root: str | os.PathLike) -> str:
    """<root>/data/catalog.db: the catalogue of the checkout at `root`, for a tool that keeps its own ROOT (a
    test points it at a temporary folder). Open it with connect_path(), which after T0 accepts only the
    build - so a production run is unchanged and a worktree's copy is refused."""
    return os.path.join(os.fspath(root), "data", "catalog.db")


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
def write_session():
    """What a catalogue WRITER wraps its writes in (plan step 1): after T0 the single-writer lock - taken
    here, or already held by this process (the updater holds it for its whole run and may call a
    cataloguer in-process) - and before T0 nothing, so CI and the pre-T0 desktop write as today (and CI's
    Linux runner never creates the Windows lock folder)."""
    if not is_cut_over() or _held is not None:
        yield
        return
    with writer_lock():
        yield


def write_session_for_process() -> None:
    """write_session() for a top-level cataloguing SCRIPT (module code, no main() to wrap): entered now,
    left at interpreter exit (atexit). The lock is an OS file lock, so even a killed script releases it."""
    import atexit                                                           # noqa: PLC0415
    session = write_session()
    session.__enter__()
    atexit.register(session.__exit__, None, None, None)


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
