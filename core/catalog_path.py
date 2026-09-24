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


# ---- THE RUNTIME GUARD (reviews R1209, R1214, R1215, R1219) --------------------------------------------------
# The static rules (tests/test_catalogue_writers_*) see only the spellings they were written for. The first
# runtime guard (R1209) matched the PATH given to sqlite3.connect - and a path has too many spellings: an
# ATTACH of the build, a \\?\ prefix, an admin share, a hard link, a URI with a '#' all wrote the build with no
# lock after T0 (R1214). So the rule is enforced where SQLite itself decides what a statement touches, and the
# guard is the connection's CLASS (R1215: Blob I/O, a caller's authorizer, a connection factory and the
# statement cache all went around an authorizer added afterwards):
#   * after T0 this module's sqlite3.connect (and sqlite3.dbapi2.connect) makes every connection a
#     _GuardedConnection, whose __init__ installs an AUTHORIZER that SQLite consults for every statement it
#     prepares: a write to the main database when that database IS the build (os.path.samefile: the same
#     file, however it was named), and ANY write to an attached database (SQLite does not say which file a
#     parameter-bound ATTACH named), are refused unless this process holds the writer lock (_authorizer has
#     the exact rule). The statement cache is off, so every execute is authorized against the lock as it is.
#   * an audit hook on the "sqlite3.connect/handle" event refuses, after T0, every connection that is not
#     exactly a _GuardedConnection - one opened through a `connect` bound before this module was imported, a
#     direct sqlite3.Connection(...), a subclass, or one opened by code that runs INSIDE a connect (a path's
#     __fspath__, a timeout's __float__: R1219 finding 3 - a thread-local "inside the wrapper" flag let those
#     through). The hook checks the object's type, not where the call came from.
# Before T0 nothing changes; the cost is one flag check per connect (after T0 about 0.5 ms per connect,
# measured by review R1215, and a statement re-prepare per execute: ~6-11 us instead of ~3 us).
# VACUUM is REFUSED after T0 without the lock, on every database (SQLite authorizes it as an internal ATTACH
# plus writes - measured by review R1219), so a tool that VACUUMs after T0 (tools/prune_series_cursors.py)
# must hold the lock. Not covered (no SQLite hook sees them): the backup API writing INTO a build connection,
# a connection opened before T0, an explicit call of the BASE class (sqlite3.Connection.set_authorizer(conn,
# None), .blobopen(conn, ...)) or of a private attribute (conn._main_build, a guarded blob's _blob), and the
# interpreter's own introspection (sys._getframe reaching a connection inside its __init__, gc.get_referents
# reaching a guarded blob's raw blob) - deliberate bypasses, not slips (the base class's __init__ on a guarded
# connection IS refused: R1219); a program that changes the FILESYSTEM under a connection while it opens (a
# junction removed or re-pointed between the open and the identity check - R1223): the identity is read from
# the file's name, which such a program controls, so this guard cannot see it - an OS permission on the build
# file is the only complete guard against that; a statement still stepping when the lock is let go; a
# file-level replace of the build; a second copy of this module loaded by path (its lock is invisible to the
# first copy's guard, so its writes are REFUSED, not let through); and processes that never import this module
# (the legacy list and t0_ready cover those).
_WRITE_ACTIONS = frozenset(getattr(sqlite3, n) for n in (
    "SQLITE_INSERT", "SQLITE_UPDATE", "SQLITE_DELETE", "SQLITE_CREATE_TABLE", "SQLITE_CREATE_INDEX",
    "SQLITE_CREATE_TRIGGER", "SQLITE_CREATE_VIEW", "SQLITE_CREATE_VTABLE", "SQLITE_DROP_TABLE",
    "SQLITE_DROP_INDEX", "SQLITE_DROP_TRIGGER", "SQLITE_DROP_VIEW", "SQLITE_DROP_VTABLE", "SQLITE_ALTER_TABLE",
    "SQLITE_REINDEX", "SQLITE_ANALYZE") if hasattr(sqlite3, n))
_real_connect = getattr(sys, "_econ_catalog_real_connect", None) or sqlite3.connect
sys._econ_catalog_real_connect = _real_connect


def _is_build(path) -> bool:
    """Whether `path` names THE build file - by identity, so a \\\\?\\ prefix, a share, a hard link or a junction
    is still the build. When identity cannot be read (a file not there yet) the spelled paths are compared."""
    if not path:
        return False
    try:
        return os.path.samefile(path, BUILD_PATH)
    except (OSError, ValueError, TypeError):
        try:
            if not os.path.exists(path):
                # SQLite has just opened this file, so it existed a moment ago: gone now means the name was
                # changed under the connection (a junction removed - R1223). Cannot tell: the build.
                return True
            return os.path.normcase(os.path.realpath(path)) == os.path.normcase(os.path.realpath(BUILD_PATH))
        except (OSError, ValueError, TypeError):
            return True                     # cannot tell: the build (fail closed - R1215 finding 4)


def _main_is_build(conn) -> bool:
    """Whether a connection's MAIN database is the build. Anything that stops the answer being read - no main
    row, a text_factory that returns bytes, an error - counts as the build (R1215 finding 4: the first version
    fell open to "not the build")."""
    try:
        rows = sqlite3.Connection.execute(conn, "PRAGMA database_list").fetchall()
    except sqlite3.Error:
        return True
    for r in rows:
        name, path = r[1], r[2]
        if isinstance(name, bytes):
            name = name.decode("utf-8", "replace")
        if isinstance(path, bytes):
            path = path.decode("utf-8", "replace")
        if name == "main":
            return _is_build(path)
    return True


# pragmas that change the FILE (its header or its format) when given a value, and two that write whatever they
# are given; everything else (busy_timeout, table_info, cache_size, ...) is a reader's business and is allowed
_FILE_PRAGMAS = frozenset({"journal_mode", "user_version", "application_id", "schema_version", "writable_schema",
                           "page_size", "auto_vacuum"})
_WRITING_PRAGMAS = frozenset({"incremental_vacuum", "optimize"})


def _may_write(schema, main_is_build: bool) -> bool:
    return schema == "temp" or (schema == "main" and not main_is_build)


def _authorizer(main_is_build: bool, attached: list):
    """The rule, per connection, after T0 and without the writer lock: a statement may write its MAIN database
    when that is not the build, and TEMP - nothing else. An ATTACHed database is never written: SQLite names the
    attached file to the authorizer only when the ATTACH is a string literal (a bound parameter or an expression
    arrives as None, measured on 3.50.4), so which file a schema is cannot be known here, and the schema is
    refused whatever it is. Attaching - to read - is allowed. A pragma that changes the file is refused on the
    same terms; an unqualified one (SQLite passes no schema) counts as touching every attached database.
    `attached` is the CONNECTION's own record (R1219 finding 1: kept in this closure it was reset by
    set_authorizer(), and an ATTACH made under the lock was never recorded at all)."""

    def check(action, arg1, arg2, dbname, source):
        if action == sqlite3.SQLITE_ATTACH:
            # before the lock test: an ATTACH made under the lock outlives it. Not VACUUM's own ATTACH of ''
            # (a private temporary database): recording it made every later file pragma on the connection a
            # refusal, state.db included (R1223 side effect). VACUUM's writes are refused without the lock anyway.
            if arg1 != "":
                attached[0] = True
            return sqlite3.SQLITE_OK
        if _held is not None:
            return sqlite3.SQLITE_OK
        if action == sqlite3.SQLITE_ALTER_TABLE:
            # SQLite passes ALTER's schema as arg1 and no dbname (R1215: `dbname == "main"` never matched)
            return sqlite3.SQLITE_OK if _may_write(arg1, main_is_build) else sqlite3.SQLITE_DENY
        if action in _WRITE_ACTIONS:
            return sqlite3.SQLITE_OK if _may_write(dbname, main_is_build) else sqlite3.SQLITE_DENY
        if action == sqlite3.SQLITE_PRAGMA and arg1:
            name = arg1.lower()
            if name in _WRITING_PRAGMAS or (name in _FILE_PRAGMAS and arg2 is not None):
                if _may_write(dbname, main_is_build):
                    return sqlite3.SQLITE_OK
                if dbname is None and not main_is_build and not attached[0]:
                    return sqlite3.SQLITE_OK
                return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK
    return check


class _GuardedBlob:
    """A writable blob on the build (or an attached schema), which only the lock holder may open: every write
    asks for the lock AGAIN, so a blob kept open after the lock is let go cannot write (R1219 finding 2 - the
    check at open time alone was the statement-cache hole of R1215 in another form). Reads pass through."""
    __slots__ = ("_blob",)

    def __init__(self, blob):
        self._blob = blob

    def _check(self):
        if _held is None:
            raise CutoverRefused("refused: this writable blob was opened under the catalogue writer lock, which "
                                 "has been let go (R1219)")

    def write(self, data, /):
        self._check()
        return self._blob.write(data)

    def __setitem__(self, key, value):
        self._check()
        self._blob[key] = value

    def __getitem__(self, key):
        return self._blob[key]

    def __len__(self):
        return len(self._blob)

    def __enter__(self):
        self._blob.__enter__()
        return self

    def __exit__(self, *exc):
        return self._blob.__exit__(*exc)

    def __getattr__(self, name):
        return getattr(self._blob, name)


_CONNECT_PARAMS = ("database", "timeout", "detect_types", "isolation_level", "check_same_thread", "factory",
                   "cached_statements", "uri")


class _GuardedConnection(sqlite3.Connection):
    """What every connection is after T0. The guard is installed by __init__ itself, so nothing runs on the
    connection before it does:
      * the authorizer (see _authorizer), with the statement cache off - every execute is prepared, and
        authorized, against the lock as it is NOW (R1215 finding 5);
      * a caller's set_authorizer() is COMBINED with it (the guard decides first) - set_authorizer(f) or
        (None) no longer replaces it, and the connection's ATTACH record survives it (R1219);
      * blobopen(readonly=False) on the build or an attached schema - Blob I/O prepares no SQL, so the
        authorizer never sees it - needs the lock, and the blob keeps asking for it (_GuardedBlob)."""
    _main_build = True
    _user_authorizer = None
    _attached = None
    _guarded = False                        # the guard is installed only on a connection made after T0

    def __init__(self, *args, **kwargs):
        cut = is_cut_over()
        if cut:
            if len(args) > len(_CONNECT_PARAMS):
                raise TypeError("sqlite3 connection: too many positional arguments")
            kwargs.update(zip(_CONNECT_PARAMS, args))
            args = ()
            kwargs["cached_statements"] = 0
        _initialising.add(id(self))         # the audit hook lets THIS object's handle through, nothing else
        try:
            super().__init__(*args, **kwargs)
        finally:
            _initialising.discard(id(self))
        self._attached = [False]
        if cut:
            self._main_build = _main_is_build(self)
            self._guarded = True
            self._install_guard()

    def _install_guard(self) -> None:
        rule = _authorizer(self._main_build, self._attached)
        user = self._user_authorizer
        if user is None:
            fn = rule
        else:
            def fn(*a):
                verdict = rule(*a)
                return verdict if verdict != sqlite3.SQLITE_OK else user(*a)
        sqlite3.Connection.set_authorizer(self, fn)

    def set_authorizer(self, *args, **kwargs):
        (callback,) = args or tuple(kwargs.values())
        if not self._guarded:
            return super().set_authorizer(callback)
        self._user_authorizer = callback
        self._install_guard()

    def blobopen(self, table, column, row, /, *, readonly=False, name="main"):
        if self._guarded:
            # the arguments are checked HERE and converted again in C: a str subclass that compared equal to
            # "temp", or a readonly that was true once and false the next time, passed the check and opened a
            # writable blob on the build (R1223). After T0 only the plain types are taken.
            if type(readonly) is not bool or type(name) is not str:
                raise TypeError("after T0 blobopen takes readonly as a bool and name as a str (R1223)")
        if readonly or not self._guarded or _may_write(name, self._main_build):
            return super().blobopen(table, column, row, readonly=readonly, name=name)
        if _held is None:
            raise CutoverRefused(f"refused: a writable blob on schema {name!r} after T0 needs the catalogue "
                                 f"writer lock ({LOCK_PATH}) - Blob I/O goes around the SQL guard (R1215)")
        return _GuardedBlob(super().blobopen(table, column, row, readonly=readonly, name=name))


def _guarded_connect(*args, **kwargs):
    if is_cut_over():
        factory = kwargs.get("factory", args[5] if len(args) > 5 else None)
        if factory not in (None, sqlite3.Connection, _GuardedConnection):
            # a factory's own __init__ runs on the connection before any guard, and a connection it opens
            # inside it is not seen at all (R1215 findings 2 and 3); nothing in the tree passes one
            raise CutoverRefused(f"refused: sqlite3.connect(factory={factory!r}) after T0 - a connection "
                                 f"factory runs before core.catalog_path's guard can (R1215)")
        if len(args) > 5:
            args = args[:5] + (_GuardedConnection,) + args[6:]
        else:
            kwargs["factory"] = _GuardedConnection
    return _real_connect(*args, **kwargs)


_initialising: set = set()             # ids of _GuardedConnections inside their own __init__


def _audit(event: str, args) -> None:
    # a handle is let through only for a _GuardedConnection inside ITS OWN __init__ - so the base class's
    # __init__ called on a guarded connection (re-pointing it at the build with no guard) is refused too
    if event != "sqlite3.connect/handle" or (args and type(args[0]) is _GuardedConnection
                                             and id(args[0]) in _initialising) or not is_cut_over():
        return
    raise CutoverRefused("refused: after T0 every sqlite3 connection is made by core.catalog_path's connect, "
                         "and this one was not (a `connect` bound before core.catalog_path was imported, a "
                         "direct sqlite3.Connection(...), or a connection opened while another was being made) "
                         "- call sqlite3.connect after importing it (R1214, R1219)")


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


def write_session_for_process():
    """write_session() for a top-level cataloguing SCRIPT (module code, no main() to wrap): entered now,
    left at interpreter exit (atexit). The lock is an OS file lock, so even a killed script releases it.
    Returns the entered session, so a caller that must let go earlier (a test) can __exit__ it."""
    import atexit                                                           # noqa: PLC0415
    session = write_session()
    session.__enter__()
    atexit.register(session.__exit__, None, None, None)
    return session


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
