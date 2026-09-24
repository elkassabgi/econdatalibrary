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
    assert cp.BUILD_PATH == r"E:\econ_live\catalog\catalog.db" and cp.LOCK_PATH == r"E:\econ_live\state\writer.lock"


# ---- the ratchet: files that name catalog.db outside the resolver may only become fewer ---------------
LEGACY = os.path.join(ROOT, "tests", "catalog_db_legacy.txt")
SKIP_DIRS = {".git", "node_modules", "data", "dist", "tests", "docs", "scratchpad", ".wrangler", "__pycache__",
             ".claude", "logs", "state", "clients"}      # clients/ ships its own bundled registry to users


def _naming_files():
    found = set()
    for dirpath, dirs, files in os.walk(ROOT):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        for f in files:
            if f.endswith((".py", ".ps1", ".sh")):
                p = os.path.join(dirpath, f)
                with open(p, encoding="utf-8", errors="replace") as fh:
                    if "catalog.db" in fh.read():
                        found.add(os.path.relpath(p, ROOT).replace(os.sep, "/"))
    found.discard("core/catalog_path.py")
    return found


def test_no_new_file_names_catalog_db_outside_the_resolver():
    legacy = {l.strip() for l in open(LEGACY, encoding="utf-8") if l.strip() and not l.startswith("#")}
    found = _naming_files()
    assert found - legacy == set(), "new files name catalog.db: open it through core.catalog_path.connect"
    gone = legacy - found
    assert not gone, f"these no longer name catalog.db - remove them from {LEGACY}: {sorted(gone)}"
