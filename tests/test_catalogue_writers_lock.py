"""Catalogue WRITERS after T0: every read-write open holds the single-writer lock (plan step 1).

Review R1192: the three core writers moved in 2d49d8a40 were never RUN by any test - seven mutants in them
(a plain read-write sqlite3.connect of the resolver's path; write_session removed from __main__) passed all
78 cited tests. So:
  - the three core writers are run after a simulated T0: without the lock their write is refused, with it
    it lands and every read-write open held the lock; a dry run opens read-only and takes no lock;
  - a static ratchet over the repo: a file that opens the catalogue for WRITE through the resolver also
    takes the lock (write_session / writer_lock / write_session_for_process) - the resolver refuses such a
    write after T0 without it, so a missing lock is a writer that stops working at T0;
  - a static ratchet: no plain sqlite3.connect of a path the resolver hands out (it bypasses both the
    read-only mode and the lock check)."""
import ast
import os
import runpy
import sqlite3
import sys
import types

import pytest

from core import catalog_path as cp
from core import cutover

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tests"))
import _repo_walk  # noqa: E402

SCHEMA = """
CREATE TABLE license (license_id TEXT PRIMARY KEY, name TEXT, url TEXT, reservable INTEGER, commercial_ok INTEGER,
  attribution_required INTEGER, no_modify INTEGER);
CREATE TABLE source (source_id TEXT PRIMARY KEY, name TEXT, homepage TEXT, license_id TEXT);
CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, frequency TEXT, unit TEXT,
  geography TEXT, category TEXT, license_id TEXT, start_date TEXT, end_date TEXT, last_updated TEXT, metadata TEXT);
CREATE VIRTUAL TABLE series_fts USING fts5(series_id UNINDEXED, title, geography);
INSERT INTO license (license_id, reservable) VALUES ('L1', 1);
INSERT INTO source VALUES ('revsrc', 'Rev Source', 'https://example.org', 'L1');
INSERT INTO series (series_id, source_id, title, metadata) VALUES ('revsrc:A', 'revsrc', 'old title', '{}');
INSERT INTO series_fts VALUES ('revsrc:A', 'old title', NULL);
"""


@pytest.fixture
def t0(tmp_path, monkeypatch):
    """A catalogue that is BOTH the checkout's and the build (so either resolution finds it), the flag SET,
    and every sqlite3.connect recorded as (mode, lock held)."""
    db = tmp_path / "catalog.db"
    c = sqlite3.connect(db)
    c.executescript(SCHEMA)
    c.close()
    monkeypatch.setattr(cp, "CHECKOUT_PATH", str(db))
    monkeypatch.setattr(cp, "BUILD_PATH", str(db))
    monkeypatch.setattr(cp, "LOCK_PATH", str(tmp_path / "state" / "writer.lock"))
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    (tmp_path / "CUTOVER").write_text("")
    opens = []
    real = sqlite3.connect

    def spy(database, *a, **kw):
        s = str(database)
        opens.append(("ro" if "mode=ro" in s else "rw" if "mode=rw" in s else "PLAIN", cp._held is not None))
        return real(database, *a, **kw)
    monkeypatch.setattr(sqlite3, "connect", spy)
    return types.SimpleNamespace(db=str(db), opens=opens, tmp=tmp_path, real=real)


def _read(env, sql):
    c = env.real(env.db)
    try:
        return c.execute(sql).fetchone()[0]
    finally:
        c.close()


def test_apply_title_wave_needs_the_lock_after_t0(t0):
    from core import apply_title_wave as w
    with pytest.raises(cutover.CutoverRefused, match="single-writer lock"):
        w.apply_local({"revsrc:A": "new title"})
    with cp.write_session():
        assert w.apply_local({"revsrc:A": "new title"}) == [("revsrc:A", "new title")]
    assert _read(t0, "SELECT title FROM series WHERE series_id='revsrc:A'") == "new title"
    assert t0.opens[-1] == ("rw", True) and ("PLAIN", False) not in t0.opens and ("PLAIN", True) not in t0.opens


def test_build_series_metadata_runs_as_main_with_the_lock_after_t0(t0, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["core/build_series_metadata.py"])
    runpy.run_path(os.path.join(ROOT, "core", "build_series_metadata.py"), run_name="__main__")
    assert '"citation_short"' in _read(t0, "SELECT metadata FROM series WHERE series_id='revsrc:A'")
    assert t0.opens and all(o == ("rw", True) for o in t0.opens), t0.opens
    assert cp._held is None, "released at the end"


def test_build_series_metadata_s_main_alone_is_refused_after_t0(t0):
    from core import build_series_metadata as b
    with pytest.raises(cutover.CutoverRefused, match="single-writer lock"):
        b.main()


@pytest.fixture
def broaden(t0, monkeypatch):
    from core import broaden_catalog as bc
    store = t0.tmp / "store"
    store.mkdir()
    monkeypatch.setattr(bc, "STORE", str(store))
    monkeypatch.setattr(bc, "OUTDIR", str(t0.tmp / "out"))
    monkeypatch.setattr(bc, "_served_ids", lambda: set())
    return bc


def test_broaden_dry_run_reads_only_and_takes_no_lock(t0, broaden, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["broaden_catalog.py", "--dry-run"])
    broaden.main()
    assert t0.opens == [("ro", False)], t0.opens


def test_broaden_writes_only_with_the_lock_after_t0(t0, broaden, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["broaden_catalog.py"])
    with pytest.raises(cutover.CutoverRefused, match="single-writer lock"):
        broaden.main()
    with cp.write_session():
        broaden.main()
    assert t0.opens[-1] == ("rw", True)
    assert _read(t0, "SELECT COUNT(*) FROM series_fts") == 1, "the FTS rebuild ran on the catalogue"


# ---- static ratchets over the whole repo ---------------------------------------------------------------------
LOCKERS = {"write_session", "writer_lock", "write_session_for_process"}


def _calls(tree):
    for n in ast.walk(tree):
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute):
            yield n


def _resolver_write_opens(tree):
    """catalog_path.connect(...) / connect_path(...) whose write= is anything but the literal False."""
    for n in _calls(tree):
        if n.func.attr in ("connect", "connect_path") and ast.unparse(n.func.value).endswith("catalog_path"):
            w = next((k.value for k in n.keywords if k.arg == "write"), None)
            if w is not None and not (isinstance(w, ast.Constant) and w.value is False):
                yield n


def _python_files():
    for rel, p in _repo_walk.code_files((".py",)):
        if rel == "core/catalog_path.py":
            continue
        try:
            yield rel, ast.parse(open(p, encoding="utf-8").read())
        except SyntaxError:
            continue


def test_every_resolver_writer_takes_the_lock():
    bad = []
    for rel, tree in _python_files():
        if any(True for _ in _resolver_write_opens(tree)):
            if not any(n.func.attr in LOCKERS for n in _calls(tree)):
                bad.append(rel)
    assert not bad, ("these open the catalogue for WRITE through core.catalog_path but never take the lock - "
                     "after T0 the resolver refuses their write: wrap the entry in catalog_path.write_session() "
                     "(or write_session_for_process() for a top-level script):\n  " + "\n  ".join(bad))


def test_the_lock_ratchet_can_fail(tmp_path):
    tree = ast.parse("from core import catalog_path\ncon = catalog_path.connect(write=True)\n")
    assert any(True for _ in _resolver_write_opens(tree)) and not any(n.func.attr in LOCKERS for n in _calls(tree))
    ro = ast.parse("con = catalog_path.connect_path(p, write=False)\n")
    assert not any(True for _ in _resolver_write_opens(ro))


def _plain_opens_of_resolver_paths(tree):
    for n in _calls(tree):
        if n.func.attr == "connect" and ast.unparse(n.func.value) == "sqlite3" and n.args:
            if "catalog_path." in ast.unparse(n.args[0]):
                yield n


def test_no_plain_sqlite3_open_of_a_resolver_path():
    bad = [f"{rel}:{n.lineno}" for rel, tree in _python_files() for n in _plain_opens_of_resolver_paths(tree)]
    assert not bad, ("plain sqlite3.connect of a path from core.catalog_path bypasses its read-only mode and its "
                     "lock check - use catalog_path.connect() / connect_path():\n  " + "\n  ".join(bad))


def test_the_plain_open_ratchet_can_fail():
    tree = ast.parse("import sqlite3\nc = sqlite3.connect(catalog_path.catalog_path())\n"
                     "d = sqlite3.connect(catalog_path.under(ROOT), timeout=5)\n")
    assert len(list(_plain_opens_of_resolver_paths(tree))) == 2
