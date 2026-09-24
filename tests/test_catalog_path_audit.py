"""core.catalog_path's RUNTIME guard (review R1209): after T0 a read-write sqlite3 open of the build needs this
process to hold the writer lock - WHATEVER the spelling. The static rules missed an alias of the module,
`from core.catalog_path import BUILD_PATH`, a helper in another module, sqlite3.dbapi2.connect and a file with a
byte-order mark; each of those wrote the build with no lock after T0. The audit hook on sqlite3.connect sees
them all."""
import os
import pathlib
import sqlite3
import sqlite3.dbapi2

import pytest

from core import catalog_path as cp
from core import cutover


@pytest.fixture
def build(tmp_path, monkeypatch):
    b = tmp_path / "live" / "catalog.db"
    b.parent.mkdir(parents=True)
    with sqlite3.connect(b) as c:                          # before T0: a plain open is as it always was
        c.execute("CREATE TABLE which (name TEXT)")
    monkeypatch.setattr(cp, "BUILD_PATH", str(b))
    monkeypatch.setattr(cp, "CHECKOUT_PATH", str(tmp_path / "checkout" / "catalog.db"))
    monkeypatch.setattr(cp, "LOCK_PATH", str(tmp_path / "state" / "writer.lock"))
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    (tmp_path / "CUTOVER").write_text("")
    assert cutover.is_cut_over()
    return b


def _uri(p, mode=None):
    u = pathlib.Path(p).resolve().as_uri()
    return u + (f"?mode={mode}" if mode else "")


SHAPES = {
    "plain str": lambda b: sqlite3.connect(str(b)),
    "a Path": lambda b: sqlite3.connect(b),
    "bytes": lambda b: sqlite3.connect(str(b).encode()),
    "dbapi2": lambda b: sqlite3.dbapi2.connect(str(b)),
    "a file: URI, rw": lambda b: sqlite3.connect(_uri(b, "rw"), uri=True),
    "a file: URI, no mode": lambda b: sqlite3.connect(_uri(b), uri=True),
    "an alias of the module": lambda b: __import__("core.catalog_path", fromlist=["x"]).BUILD_PATH and
    sqlite3.connect(__import__("core.catalog_path", fromlist=["x"]).BUILD_PATH),
}


@pytest.mark.parametrize("shape", sorted(SHAPES))
def test_every_spelling_of_a_plain_rw_open_is_refused_after_t0(build, shape):
    with pytest.raises(cutover.CutoverRefused, match="R1209"):
        SHAPES[shape](build)


def test_a_relative_spelling_is_refused_too(build, monkeypatch):
    monkeypatch.chdir(build.parent)
    with pytest.raises(cutover.CutoverRefused, match="R1209"):
        sqlite3.connect("catalog.db")


def test_a_helper_in_another_module_is_refused_too(build, tmp_path, monkeypatch):
    (tmp_path / "helper_mod.py").write_text("import sqlite3\ndef open_it(p):\n    return sqlite3.connect(p)\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    import helper_mod
    with pytest.raises(cutover.CutoverRefused, match="R1209"):
        helper_mod.open_it(str(build))


def test_read_only_opens_and_other_files_and_the_lock_are_allowed(build, tmp_path):
    sqlite3.connect(_uri(build, "ro"), uri=True).close()                       # a read
    sqlite3.connect(_uri(build, "ro") + "&immutable=1", uri=True).close()
    sqlite3.connect(":memory:").close()
    sqlite3.connect(str(tmp_path / "other.db")).close()                        # not the build
    with cp.writer_lock():                                                     # the writer, holding the lock
        c = sqlite3.connect(str(build))
        c.execute("INSERT INTO which VALUES ('locked')")
        c.commit()
        c.close()
    with cp.write_session():
        cp.connect(write=True).close()                                         # the resolver's own path


def test_before_t0_nothing_changes(build, tmp_path):
    os.remove(tmp_path / "CUTOVER")
    assert not cutover.is_cut_over()
    sqlite3.connect(str(build)).close()


def test_the_hook_is_installed_once():
    import sys
    assert getattr(sys, "_econ_catalog_audit_installed", False) is True
