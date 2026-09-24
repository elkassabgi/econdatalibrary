"""core.catalog_path's RUNTIME guard (reviews R1209, R1214): after T0 no sqlite3 connection in a process that
imports it can WRITE the build without the writer lock - whatever the build is called. SQLite's own authorizer
decides, on every statement, against the file's identity (os.path.samefile), so an alias of the module, bytes,
a Path, dbapi2, file: URIs (with rw, without a mode, with '#', %-encoded), a relative spelling, a helper in
another module, a \\\\?\\ prefix, an admin share, a hard link and an ATTACH of the build are all refused; reads
are not; the lock holder and the resolver's own write path are allowed; before T0 nothing changes. A
connection that did not pass through the guard (a connect bound before the module) is refused outright."""
import os
import pathlib
import sqlite3
import sqlite3.dbapi2
import urllib.parse

import pytest

from core import catalog_path as cp
from core import cutover

NOT_AUTHORIZED = (sqlite3.DatabaseError,)


@pytest.fixture
def build(tmp_path, monkeypatch):
    b = tmp_path / "live" / "catalog.db"
    b.parent.mkdir(parents=True)
    with sqlite3.connect(b) as c:                          # before T0: a plain open is as it always was
        c.execute("CREATE TABLE which (name TEXT)")
        c.execute("INSERT INTO which VALUES ('before')")
    monkeypatch.setattr(cp, "BUILD_PATH", str(b))
    monkeypatch.setattr(cp, "CHECKOUT_PATH", str(tmp_path / "checkout" / "catalog.db"))
    monkeypatch.setattr(cp, "LOCK_PATH", str(tmp_path / "state" / "writer.lock"))
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    (tmp_path / "CUTOVER").write_text("")
    assert cutover.is_cut_over()
    return b


def _uri(p, query=""):
    return pathlib.Path(p).resolve().as_uri() + query


def _write(conn):
    conn.execute("INSERT INTO which VALUES ('written')")
    conn.commit()


def _rows(b):
    c = sqlite3.connect(_uri(b, "?mode=ro"), uri=True)
    try:
        return [r[0] for r in c.execute("SELECT name FROM which")]
    finally:
        c.close()


SHAPES = {
    "plain str": lambda b: sqlite3.connect(str(b)),
    "a Path": lambda b: sqlite3.connect(b),
    "bytes": lambda b: sqlite3.connect(str(b).encode()),
    "dbapi2": lambda b: sqlite3.dbapi2.connect(str(b)),
    "a file: URI, rw": lambda b: sqlite3.connect(_uri(b, "?mode=rw"), uri=True),
    "a file: URI, no mode": lambda b: sqlite3.connect(_uri(b), uri=True),
    "a file: URI ending in #": lambda b: sqlite3.connect(_uri(b) + "#", uri=True),
    "a file: URI with #?mode=ro after the file": lambda b: sqlite3.connect(_uri(b) + "#?mode=ro", uri=True),
    "a %-encoded file: URI": lambda b: sqlite3.connect(
        "file:" + urllib.parse.quote(str(pathlib.Path(b).resolve()).replace("\\", "/"), safe=":/"), uri=True),
    "a \\\\?\\ prefix": lambda b: sqlite3.connect("\\\\?\\" + str(pathlib.Path(b).resolve())),
    "an alias of the module": lambda b: sqlite3.connect(__import__("core.catalog_path", fromlist=["x"]).BUILD_PATH),
}


@pytest.mark.parametrize("shape", sorted(SHAPES))
def test_every_spelling_of_a_write_is_refused_after_t0(build, shape):
    conn = SHAPES[shape](build)
    try:
        with pytest.raises(NOT_AUTHORIZED, match="not authorized"):
            _write(conn)
        assert conn.execute("SELECT count(*) FROM which").fetchone()[0] == 1, "a read is allowed"
    finally:
        conn.close()
    assert _rows(build) == ["before"]


def test_a_relative_spelling_and_a_helper_module_are_refused(build, tmp_path, monkeypatch):
    monkeypatch.chdir(build.parent)
    with sqlite3.connect("catalog.db") as c, pytest.raises(NOT_AUTHORIZED):
        _write(c)
    (tmp_path / "helper_mod.py").write_text("import sqlite3\ndef open_it(p):\n    return sqlite3.connect(p)\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    import helper_mod
    c = helper_mod.open_it(str(build))
    with pytest.raises(NOT_AUTHORIZED):
        _write(c)
    c.close()


def test_a_hard_link_to_the_build_is_the_build(build, tmp_path):
    link = tmp_path / "link.db"
    try:
        os.link(build, link)
    except OSError as e:
        pytest.skip(f"no hard link here: {e}")
    c = sqlite3.connect(str(link))
    with pytest.raises(NOT_AUTHORIZED):
        _write(c)
    c.close()


@pytest.mark.skipif(os.name != "nt", reason="a Windows admin share")
def test_an_admin_share_name_is_the_build(build):
    full = str(pathlib.Path(build).resolve())
    share = "\\\\localhost\\" + full[0] + "$" + full[2:]
    if not os.path.exists(share):
        pytest.skip("the admin share is not reachable here")
    c = sqlite3.connect(share)
    with pytest.raises(NOT_AUTHORIZED):
        _write(c)
    c.close()


def _main(tmp_path, from_mode):
    if from_mode == "another file":
        return sqlite3.connect(str(tmp_path / "staging.db"), uri=True)
    if from_mode == ":memory:":
        return sqlite3.connect("file::memory:", uri=True)
    other = tmp_path / "ro.db"
    sqlite3.connect(str(other)).close()
    return sqlite3.connect(_uri(other, "?mode=ro"), uri=True)


ATTACH_FORMS = {
    "a bound path": lambda b: ("ATTACH ? AS cat", (str(b),)),
    "a bound rw URI": lambda b: ("ATTACH ? AS cat", (_uri(b, "?mode=rw"),)),
    "a bound URI with #": lambda b: ("ATTACH ? AS cat", (_uri(b) + "#",)),
    "a literal path": lambda b: ("ATTACH '%s' AS cat" % str(b).replace("'", "''"), ()),
    "an expression": lambda b: ("ATTACH (? || '') AS cat", (str(b),)),
}


@pytest.mark.parametrize("from_mode", ["another file", ":memory:", "read-only main"])
@pytest.mark.parametrize("form", sorted(ATTACH_FORMS))
def test_a_write_through_an_attach_of_the_build_is_refused(build, tmp_path, from_mode, form):
    """R1214 finding 1: ATTACH never calls sqlite3.connect - and SQLite hands the authorizer the attached file's
    name only for a literal, so every attached schema is write-refused. The attach itself must SUCCEED here (a
    read proves it), or 'refused' would be an open error passing for the guard (R890)."""
    c = _main(tmp_path, from_mode)
    sql, params = ATTACH_FORMS[form](build)
    try:
        c.execute(sql, params)
        assert c.execute("SELECT name FROM cat.which").fetchall() == [("before",)], "the attach opened the build"
        with pytest.raises(NOT_AUTHORIZED, match="not authorized"):
            c.execute("INSERT INTO cat.which VALUES ('attached')")
        with pytest.raises(NOT_AUTHORIZED, match="not authorized"):
            c.execute("DROP TABLE cat.which")
    finally:
        c.close()
    assert _rows(build) == ["before"]


def test_an_attached_other_file_is_read_only_too_and_main_and_temp_stay_writable(build, tmp_path):
    c = sqlite3.connect(str(tmp_path / "staging.db"))
    c.execute("ATTACH ? AS side", (str(tmp_path / "side.db"),))
    with pytest.raises(NOT_AUTHORIZED, match="not authorized"):
        c.execute("CREATE TABLE side.t (x)")
    c.execute("CREATE TABLE t (x)")                                        # main is not the build
    c.execute("INSERT INTO main.t VALUES (1)")
    c.execute("CREATE TEMP TABLE tt (x)")
    c.execute("INSERT INTO tt VALUES (1)")
    c.commit()
    c.close()
    with cp.writer_lock():                                                  # the lock holder may write it
        c = sqlite3.connect(":memory:")
        c.execute("ATTACH ? AS side", (str(tmp_path / "side.db"),))
        c.execute("CREATE TABLE side.t (x)")
        c.close()


def test_pragmas_that_change_the_file_are_refused_and_reader_pragmas_are_not(build, tmp_path):
    c = sqlite3.connect(str(build))
    try:
        for p in ("PRAGMA user_version=7", "PRAGMA main.user_version=7", "PRAGMA journal_mode=DELETE",
                  "PRAGMA application_id=1", "PRAGMA writable_schema=ON", "PRAGMA optimize",
                  "PRAGMA incremental_vacuum"):
            with pytest.raises(NOT_AUTHORIZED, match="not authorized"):
                c.execute(p).fetchall()
        for p in ("PRAGMA busy_timeout=5000", "PRAGMA table_info(which)", "PRAGMA cache_size=-20000",
                  "PRAGMA user_version", "PRAGMA journal_mode", "PRAGMA query_only=1", "PRAGMA quick_check"):
            c.execute(p).fetchall()
    finally:
        c.close()
    assert sqlite3.connect(_uri(build, "?mode=ro"), uri=True).execute("PRAGMA user_version").fetchone() == (0,)
    other = sqlite3.connect(str(tmp_path / "staging.db"))
    other.execute("PRAGMA user_version=7")                                 # not the build, nothing attached
    other.execute("ATTACH ? AS cat", (str(build),))
    with pytest.raises(NOT_AUTHORIZED, match="not authorized"):
        other.execute("PRAGMA user_version=8")                             # unqualified: reaches cat as well
    with pytest.raises(NOT_AUTHORIZED, match="not authorized"):
        other.execute("PRAGMA cat.user_version=8")
    other.execute("PRAGMA main.user_version=9")
    other.close()


def test_the_lock_holder_and_the_resolver_may_write(build):
    with cp.writer_lock():
        c = sqlite3.connect(str(build))
        _write(c)
        c.close()
    with cp.write_session():
        c = cp.connect(write=True)
        _write(c)
        c.close()
    assert _rows(build) == ["before", "written", "written"]


def test_a_connection_that_bypassed_the_guard_is_refused_after_t0(build, tmp_path):
    """R1214: `from sqlite3 import connect` bound before the module - the raw C connect - gets no authorizer,
    so the audit hook refuses it outright after T0."""
    import sys
    raw = sys._econ_catalog_real_connect
    with pytest.raises(cutover.CutoverRefused, match="R1214"):
        raw(str(tmp_path / "anything.db"))


def test_before_t0_nothing_changes(build, tmp_path):
    os.remove(tmp_path / "CUTOVER")
    assert not cutover.is_cut_over()
    with sqlite3.connect(str(build)) as c:
        _write(c)
    import sys
    sys._econ_catalog_real_connect(str(tmp_path / "raw.db")).close()      # no refusal before T0


# ---- R1215: the routes around the first authorizer ------------------------------------------------------------

def test_a_writable_blob_on_the_build_is_refused(build, tmp_path):
    c = sqlite3.connect(str(build))
    try:
        with pytest.raises(cutover.CutoverRefused, match="R1215"):
            c.blobopen("which", "name", 1, readonly=False)
        with c.blobopen("which", "name", 1, readonly=True) as b:            # reading is allowed
            assert b.read() == b"before"
    finally:
        c.close()
    other = sqlite3.connect(str(tmp_path / "staging.db"))
    other.execute("ATTACH ? AS x", (str(build),))
    with pytest.raises(cutover.CutoverRefused, match="R1215"):
        other.blobopen("which", "name", 1, readonly=False, name="x")
    other.execute("CREATE TABLE t (b BLOB)")
    other.execute("INSERT INTO t VALUES (zeroblob(4))")
    with other.blobopen("t", "b", 1) as b:                                  # main is not the build
        b.write(b"ok!!")
    other.close()
    with cp.writer_lock():
        c = sqlite3.connect(str(build))
        with c.blobopen("which", "name", 1) as b:
            b.write(b"BEFORE")
        c.close()
    assert _rows(build) == ["BEFORE"]


class _MyConnection(sqlite3.Connection):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.execute("PRAGMA journal_mode=WAL")


@pytest.mark.parametrize("how", ["keyword", "positional"])
def test_a_connection_factory_is_refused_after_t0(build, how):
    """R1215 findings 2-4: a factory's __init__ ran on the connection before the guard existed."""
    with pytest.raises(cutover.CutoverRefused, match="factory"):
        if how == "keyword":
            sqlite3.connect(str(build), factory=_MyConnection)
        else:
            sqlite3.connect(str(build), 5.0, 0, "DEFERRED", True, _MyConnection)
    assert sqlite3.connect(_uri(build, "?mode=ro"), uri=True).execute("PRAGMA journal_mode").fetchone() == ("delete",)


def test_a_statement_prepared_under_the_lock_is_not_reused_after_it(build):
    """R1215 finding 5: the statement cache kept an INSERT prepared while the lock was held."""
    c = sqlite3.connect(str(build))
    try:
        with cp.writer_lock():
            c.execute("INSERT INTO which VALUES ('under-lock')")
            c.commit()
        with pytest.raises(NOT_AUTHORIZED, match="not authorized"):
            c.execute("INSERT INTO which VALUES ('under-lock')")
        cur = c.cursor()
        with pytest.raises(NOT_AUTHORIZED, match="not authorized"):
            cur.execute("INSERT INTO which VALUES ('under-lock')")
    finally:
        c.close()
    assert _rows(build) == ["before", "under-lock"]


@pytest.mark.parametrize("theirs", ["allow everything", "None"])
def test_a_callers_authorizer_is_combined_not_a_replacement(build, theirs):
    c = sqlite3.connect(str(build))
    c.set_authorizer((lambda *a: sqlite3.SQLITE_OK) if theirs != "None" else None)
    with pytest.raises(NOT_AUTHORIZED, match="not authorized"):
        _write(c)
    c.close()
    other = sqlite3.connect(":memory:")                          # and theirs still applies where ours allows
    other.execute("CREATE TABLE t (x)")
    other.set_authorizer(lambda a, *r: sqlite3.SQLITE_DENY if a == sqlite3.SQLITE_INSERT else sqlite3.SQLITE_OK)
    with pytest.raises(NOT_AUTHORIZED):
        other.execute("INSERT INTO t VALUES (1)")
    other.close()


def test_alter_is_judged_by_its_real_schema(build, tmp_path):
    """R1215: SQLite passes ALTER's schema as arg1, so the first rule refused ALTER everywhere."""
    other = sqlite3.connect(str(tmp_path / "staging.db"))
    other.execute("CREATE TABLE t (x)")
    other.execute("ALTER TABLE t ADD COLUMN y")                  # not the build: allowed
    other.close()
    c = sqlite3.connect(str(build))
    with pytest.raises(NOT_AUTHORIZED, match="not authorized"):
        c.execute("ALTER TABLE which ADD COLUMN z")
    c.close()


def test_an_unreadable_main_counts_as_the_build(build, monkeypatch):
    """R1215 finding 4: an identity that cannot be read fell open to 'not the build'."""
    c = sqlite3.connect(str(build))
    c.text_factory = bytes                                        # the shape that fell open
    assert cp._main_is_build(c) is True
    c.close()
    assert cp._main_is_build(c) is True, "a closed connection: the answer cannot be read"
    m = sqlite3.connect(":memory:")
    assert cp._main_is_build(m) is False, "a control: memory is not the build"
    m.close()
    monkeypatch.setattr(cp.os.path, "samefile", lambda a, b: (_ for _ in ()).throw(OSError("no")))
    monkeypatch.setattr(cp.os.path, "realpath", lambda p: (_ for _ in ()).throw(OSError("no")))
    assert cp._is_build("anything") is True


def test_the_guard_is_installed_once():
    import sys
    assert getattr(sys, "_econ_catalog_audit_installed", False) is True
    assert sqlite3.connect is sqlite3.dbapi2.connect and sqlite3.connect.__name__ == "_guarded_connect"
