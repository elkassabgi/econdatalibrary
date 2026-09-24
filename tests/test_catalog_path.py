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
# Code that names the catalogue: the literal, the variable that overrides it, or the name split in two.
NAMES_CATALOGUE = re.compile(r"catalog\.db|ECONDL_CATALOG|[\"']catalog[\"']\s*[+,]\s*[\"']\.db[\"']")


def _code_only(src: str, ext: str) -> str:
    """The text with comments and docstrings removed, so a migrated file that still MENTIONS catalog.db
    in a comment leaves the list (R1176) - only code that names it counts."""
    if ext == ".py":
        src = re.sub(r'"""[\s\S]*?"""|\'\'\'[\s\S]*?\'\'\'', "", src)
    return "\n".join(l for l in src.splitlines() if not l.lstrip().startswith("#"))


def _naming_files():
    found = set()
    import _repo_walk                                  # the one shared walk (R1178)
    for rel, p in _repo_walk.code_files((".py", ".ps1", ".sh"), ROOT):
        with open(p, encoding="utf-8", errors="replace") as fh:
            if NAMES_CATALOGUE.search(_code_only(fh.read(), os.path.splitext(p)[1])):
                found.add(rel)
    found.discard("core/catalog_path.py")
    found.discard("updater/run.py")      # names ECONDL_CATALOG only to REFUSE an override (plan change 4)
    return found


def test_the_catalogue_ratchet_can_fail():
    for code in ('p = os.path.join(ROOT, "data", "catalog.db")', 'os.environ.get("ECONDL_CATALOG")',
                 'name = "catalog" + ".db"', 'os.path.join(d, "catalog", ".db")'):
        assert NAMES_CATALOGUE.search(_code_only(code, ".py")), code
    assert not NAMES_CATALOGUE.search(_code_only('# the old catalog.db road\nx = 1\n"""uses catalog.db"""', ".py"))


def test_no_new_file_names_catalog_db_outside_the_resolver():
    legacy = {l.strip() for l in open(LEGACY, encoding="utf-8") if l.strip() and not l.startswith("#")}
    found = _naming_files()
    assert found - legacy == set(), "new files name catalog.db: open it through core.catalog_path.connect"
    gone = legacy - found
    assert not gone, f"these no longer name catalog.db - remove them from {LEGACY}: {sorted(gone)}"
