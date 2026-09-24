"""core/catalog_path.py - the one catalogue resolver (plan 4a). The real E:/ProgramData paths are never
touched: the module constants are monkeypatched to temporary paths."""
import os
import re
import sqlite3
import subprocess
import sys
import time

import pytest

from core import catalog_path as cp
from core import cutover

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture
def paths(tmp_path, monkeypatch):
    checkout, build = tmp_path / "checkout" / "catalog.db", tmp_path / "live" / "catalog" / "catalog.db"
    for p, name in ((checkout, "checkout"), (build, "build")):
        p.parent.mkdir(parents=True)
        with sqlite3.connect(p) as c:
            c.execute("CREATE TABLE which (name TEXT)")
            c.execute("INSERT INTO which VALUES (?)", (name,))
    monkeypatch.setattr(cp, "CHECKOUT_PATH", str(checkout))
    monkeypatch.setattr(cp, "BUILD_PATH", str(build))
    monkeypatch.setattr(cp, "LOCK_PATH", str(tmp_path / "live" / "state" / "writer.lock"))
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    return tmp_path


def _which(conn):
    return conn.execute("SELECT name FROM which").fetchone()[0]


def test_before_t0_the_checkout_after_t0_the_build(paths):
    assert _which(cp.connect()) == "checkout"
    (paths / "CUTOVER").write_text("")
    assert _which(cp.connect()) == "build"


def test_read_connections_cannot_write(paths):
    with pytest.raises(sqlite3.OperationalError, match="readonly"):
        cp.connect().execute("INSERT INTO which VALUES ('x')")


def test_a_missing_catalogue_is_an_error_never_created(paths, monkeypatch):
    monkeypatch.setattr(cp, "CHECKOUT_PATH", str(paths / "nowhere" / "catalog.db"))
    for write in (False, True):
        with pytest.raises(FileNotFoundError):
            cp.connect(write=write)
    assert not (paths / "nowhere").exists()


def test_the_uri_never_creates_the_file(paths, monkeypatch):
    """Even past the isfile() check (a file removed in between), the mode=ro / mode=rw URI must not create a
    database (finding 7: 'rw' changed to 'rwc' survived because isfile() answered first)."""
    missing = paths / "gone" / "catalog.db"
    missing.parent.mkdir()
    monkeypatch.setattr(cp, "CHECKOUT_PATH", str(missing))
    monkeypatch.setattr(cp.os.path, "isfile", lambda p: True)
    for write in (False, True):
        with pytest.raises(sqlite3.OperationalError):
            cp.connect(write=write).execute("SELECT 1").fetchone()
    assert not missing.exists()


def test_before_t0_a_write_needs_no_lock(paths):
    c = cp.connect(write=True)
    c.execute("INSERT INTO which VALUES ('w')")
    c.commit()


def test_after_t0_a_write_needs_the_lock(paths):
    (paths / "CUTOVER").write_text("")
    with pytest.raises(cutover.CutoverRefused, match="single-writer lock"):
        cp.connect(write=True)
    with cp.writer_lock():
        c = cp.connect(write=True)
        c.execute("INSERT INTO which VALUES ('w')")
        c.commit()
        c.close()
    with pytest.raises(cutover.CutoverRefused):
        cp.connect(write=True)                           # released again


HOLDER = r"""
import sys, time
sys.path.insert(0, sys.argv[1])
from core import catalog_path as cp
cp.LOCK_PATH = sys.argv[2]
with cp.writer_lock():
    print("held", flush=True)
    time.sleep(30)
"""


def test_a_second_process_is_refused_not_queued(paths):
    lock = str(paths / "live" / "state" / "writer.lock")
    p = subprocess.Popen([sys.executable, "-B", "-c", HOLDER, ROOT, lock], stdout=subprocess.PIPE, text=True)
    try:
        assert p.stdout.readline().strip() == "held"
        t0 = time.monotonic()
        with pytest.raises(cutover.CutoverRefused, match="another process"):
            with cp.writer_lock():
                pass
        assert time.monotonic() - t0 < 5, "refused at once, not after waiting"
    finally:
        p.kill()
        p.wait()
    with cp.writer_lock():                               # free again once the holder is gone
        pass


TORN = r"""
import sqlite3, sys, time
c = sqlite3.connect(sys.argv[1], isolation_level=None)
c.execute("PRAGMA cache_size=10")                   # tiny cache: changed pages spill into the file
c.execute("BEGIN IMMEDIATE")
c.executemany("INSERT INTO which VALUES (?)", [("torn" + "x" * 900,) for _ in range(3000)])
print("mid-transaction", flush=True)
time.sleep(60)                                      # killed here: no COMMIT, no ROLLBACK
"""


def test_taking_the_lock_recovers_a_writer_killed_mid_transaction(paths):
    """R1176 finding 4: a hot journal made every read-only open fail and any copy torn, while the lock was
    free. The next lock holder must roll it back before anyone reads or copies the catalogue."""
    (paths / "CUTOVER").write_text("")
    build = cp.BUILD_PATH
    p = subprocess.Popen([sys.executable, "-B", "-c", TORN, build], stdout=subprocess.PIPE, text=True)
    try:
        assert p.stdout.readline().strip() == "mid-transaction"
    finally:
        p.kill()
        p.wait()
    assert os.path.exists(build + "-journal"), "the crash left a hot journal (the test's own precondition)"
    with pytest.raises(sqlite3.OperationalError):
        cp.connect().execute("SELECT count(*) FROM which").fetchone()
    with cp.writer_lock():
        pass
    assert not os.path.exists(build + "-journal")
    assert cp.connect().execute("SELECT name FROM which").fetchall() == [("build",)], "the torn rows are gone"


def test_a_persist_mode_journal_is_not_hot(paths):
    """AR-153: journal_mode=PERSIST keeps the journal after a commit with a zeroed header - not hot. The lock
    refused every such catalogue when it tested only "exists and is not empty"."""
    (paths / "CUTOVER").write_text("")
    build = cp.BUILD_PATH
    c = sqlite3.connect(build)
    c.execute("PRAGMA journal_mode=PERSIST")
    c.execute("INSERT INTO which VALUES ('committed')")
    c.commit()
    c.close()
    j = build + "-journal"
    assert os.path.getsize(j) > 0 and not cp.journal_is_hot(j), "precondition: a kept, zeroed journal"
    with cp.writer_lock():
        pass
    assert sorted(r[0] for r in cp.connect().execute("SELECT name FROM which")) == ["build", "committed"]


class _NoRollback:
    """A connection that opens fine but leaves the journal where it is (as a live writer's journal stays)."""
    def execute(self, *_a):
        return self

    def fetchone(self):
        return (1,)

    def close(self):
        pass


def test_a_live_journal_is_hot_and_refused(paths, monkeypatch):
    """Positive control for the magic check, and the message names the likely cause."""
    (paths / "CUTOVER").write_text("")
    j = cp.BUILD_PATH + "-journal"
    assert not cp.journal_is_hot(j), "no journal: not hot"
    with open(j, "wb") as fh:
        fh.write(cp.JOURNAL_MAGIC + bytes(504))
    assert cp.journal_is_hot(j)
    monkeypatch.setattr(cp.sqlite3, "connect", lambda *a, **k: _NoRollback())
    with pytest.raises(RuntimeError, match="WITHOUT the writer lock"):
        with cp.writer_lock():
            pass


def test_a_busy_catalogue_names_the_unlocked_writer(paths, monkeypatch):
    (paths / "CUTOVER").write_text("")

    class Busy(_NoRollback):
        def execute(self, *_a):
            raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(cp.sqlite3, "connect", lambda *a, **k: Busy())
    with pytest.raises(RuntimeError, match="WITHOUT the writer lock"):
        with cp.writer_lock():
            pass


# ---- connect_path: the one-line replacement for a tool's own sqlite3.connect(<catalogue>) ---------------
def test_connect_path_before_t0_opens_the_tool_s_own_path(paths, tmp_path):
    other = tmp_path / "a_test_catalogue.db"
    with sqlite3.connect(other) as c:
        c.execute("CREATE TABLE which (name TEXT)")
        c.execute("INSERT INTO which VALUES ('other')")
    assert _which(cp.connect_path(other, write=False)) == "other", "a --db argument or a test's copy still works"
    with pytest.raises(sqlite3.OperationalError):
        cp.connect_path(other, write=False).execute("INSERT INTO which VALUES ('x')")
    w = cp.connect_path(str(other), write=True)
    w.execute("INSERT INTO which VALUES ('x')")
    w.commit()
    with pytest.raises(FileNotFoundError):
        cp.connect_path(tmp_path / "missing.db", write=True)
    assert not (tmp_path / "missing.db").exists(), "never created"


def test_connect_path_after_t0_opens_only_the_build(paths, tmp_path):
    (paths / "CUTOVER").write_text("")
    with pytest.raises(cutover.CutoverRefused, match="not"):
        cp.connect_path(cp.CHECKOUT_PATH, write=False)               # a worktree's data/catalog.db
    assert _which(cp.connect_path(cp.BUILD_PATH, write=False)) == "build"
    spelled = os.path.join(os.path.dirname(cp.BUILD_PATH), ".", os.path.basename(cp.BUILD_PATH))
    assert _which(cp.connect_path(spelled.upper() if os.name == "nt" else spelled, write=False)) == "build"
    with pytest.raises(cutover.CutoverRefused, match="lock"):
        cp.connect_path(cp.BUILD_PATH, write=True)
    with cp.writer_lock():
        cp.connect_path(cp.BUILD_PATH, write=True).execute("INSERT INTO which VALUES ('y')").connection.commit()


def test_under_names_the_checkout_s_catalogue():
    """The real (unpatched) constants: a tool's own ROOT names the checkout's file, production's the build."""
    assert cp.under(cp.ROOT) == cp.CHECKOUT_PATH
    assert cp.under(cp.LIVE_STORE_ROOT) == cp.BUILD_PATH, "production's own ROOT names the build"


def test_connect_path_passes_sqlite_options(paths, tmp_path):
    c = cp.connect_path(cp.CHECKOUT_PATH, write=False, detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=False)
    assert _which(c) == "checkout"


def test_the_lock_is_not_reentrant(paths):
    with cp.writer_lock():
        with pytest.raises(RuntimeError):
            with cp.writer_lock():
                pass


def test_the_resolver_has_no_override():
    src = open(cp.__file__, encoding="utf-8").read()
    code = re.sub(r'"""[\s\S]*?"""', "", src)
    code = "\n".join(l for l in code.splitlines() if not l.lstrip().startswith("#"))
    for banned in ("environ", "getenv", "argv"):
        assert banned not in code, f"core/catalog_path.py reads {banned!r}: the paths must not be overridable"
    assert cp.LIVE_STORE_ROOT == r"E:\research\econfindatalibrary" and cp.LOCK_PATH == r"E:\econ_live\state\writer.lock"
    assert cp.BUILD_PATH == os.path.join(cp.LIVE_STORE_ROOT, "data", "catalog.db"), "the production checkout IS the build"
    assert cp.LIVE_STATE_DIR == os.path.join(cp.LIVE_STORE_ROOT, "data", "_aqueduct")


# ---- the ratchet: files that name catalog.db outside the resolver may only become fewer ---------------
LEGACY = os.path.join(ROOT, "tests", "catalog_db_legacy.txt")
# clients/ IS scanned (R1176: the updater's own catalogue open lives in clients/python/econdl/_catalog.py).
# Code that names the catalogue: the literal, the variable that overrides it, the name split in two, or
# the module constant several files import instead of naming the file (AR-153).
NAMES_CATALOGUE = re.compile(r"catalog\.db|ECONDL_CATALOG|\bCATALOG_DB\b|[\"']catalog[\"']\s*[+,]\s*[\"']\.db[\"']")
# A narrow exemption: only the named function of the named file is left out of the scan.
EXEMPT_FUNCTIONS = {"updater/run.py": frozenset({"_selfhost_preflight"})}   # names ECONDL_CATALOG to REFUSE it


def _code_only(src: str, ext: str, rel: str = "") -> str:
    """Only CODE counts, so a migrated file that still MENTIONS catalog.db in a comment or docstring leaves
    the list (R1176). Python goes through the parser (tests/_repo_walk.code_text, AR-153): other strings,
    triple-quoted ones included, are kept and implicit concatenation is joined. Shell: '#' lines dropped."""
    import _repo_walk
    if ext == ".py":
        return _repo_walk.code_text(src, EXEMPT_FUNCTIONS.get(rel, frozenset()))
    return "\n".join(l for l in src.splitlines() if not l.lstrip().startswith("#"))


def _naming_files():
    found = set()
    import _repo_walk                                  # the one shared walk (R1178)
    for rel, p in _repo_walk.code_files((".py", ".ps1", ".sh"), ROOT):
        with open(p, encoding="utf-8", errors="replace") as fh:
            if NAMES_CATALOGUE.search(_code_only(fh.read(), os.path.splitext(p)[1], rel)):
                found.add(rel)
    found.discard("core/catalog_path.py")
    return found


def test_the_catalogue_ratchet_can_fail():
    for code in ('p = os.path.join(ROOT, "data", "catalog.db")', 'os.environ.get("ECONDL_CATALOG")',
                 'name = "catalog" + ".db"', 'os.path.join(d, "catalog", ".db")'):
        assert NAMES_CATALOGUE.search(_code_only(code, ".py")), code
    assert not NAMES_CATALOGUE.search(_code_only('# the old catalog.db road\nx = 1\n"""uses catalog.db"""', ".py"))
    # AR-153: what the regex-based reader missed or hid
    for code in ('p = "catalog" ".db"',                                   # implicit concatenation
                 "SQL = '''ATTACH \"data/catalog.db\" AS c'''\nx = 1",   # a triple-quoted string that is CODE
                 'from core.paths import CATALOG_DB',                      # the module constant
                 'def f():\n    """doc"""\n    return "catalog.db"'):    # code after a docstring
        assert NAMES_CATALOGUE.search(_code_only(code, ".py")), code
    assert not NAMES_CATALOGUE.search(_code_only('def f():\n    """uses catalog.db"""\n    return 1', ".py"))
    run = 'def _selfhost_preflight(a):\n    os.environ.get("ECONDL_CATALOG")\n\ndef other():\n    return "catalog.db"'
    assert NAMES_CATALOGUE.search(_code_only(run, ".py", "updater/run.py")), "the rest of run.py is scanned"
    assert not NAMES_CATALOGUE.search(_code_only(run.split("\n\n")[0], ".py", "updater/run.py"))
    assert NAMES_CATALOGUE.search(_code_only("x = (", ".py") + "catalog.db"), "unparsable: scanned raw"


def test_no_new_file_names_catalog_db_outside_the_resolver():
    legacy = {l.strip() for l in open(LEGACY, encoding="utf-8") if l.strip() and not l.startswith("#")}
    found = _naming_files()
    assert found - legacy == set(), "new files name catalog.db: open it through core.catalog_path.connect"
    gone = legacy - found
    assert not gone, f"these no longer name catalog.db - remove them from {LEGACY}: {sorted(gone)}"
