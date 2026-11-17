"""Four more files open the catalogue through core.catalog_path (t0_ready legacy-catalogue, 2026-11-17):
tools/flowgrain_insee_melodi.py and tools/flowgrain_ons_uk.py (writers: --catalog), tools/delist_timeless_tables.py
(a writer: --apply) and tools/make_servable.py (its pre-T0 catalogue read).

* flowgrain: $ECONDL_CATALOG when set (asked of econdl's default_db(), which owns the override and refuses one
  that is not the build after T0), else <ROOT>/data's catalogue - the old constant, now resolved at the open.
  The licence read is read-only (5 s, sqlite's own, as before); --catalog writes read-write (insee 5 s, ons
  180 s, the old arguments; no busy_timeout PRAGMA follows, so the argument is pinned) under the lock.
* delist_timeless_tables: its check read is read-only (30 s) and a missing catalogue is still only "not
  readable"; its local delete is read-write (60 s, PRAGMA follows) under the lock; a missing catalogue
  fails the step (return 1) and is never created. It still refuses after T0 first (test_retired_at_t0_tools).
* make_servable before T0: the checkout's catalogue, read-only, 5 s.
A missing catalogue is never created (an EXISTING data/ folder in every case). Hermetic: temporary roots, the
store, D1, R2 and the live surface stubbed, no network."""
import datetime as dt
import importlib.util
import io
import os
import sqlite3
import sys
import types

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from core import catalog_path, cutover  # noqa: E402
from updater import blob  # noqa: E402


def _load(rel):
    name = "_t0wb_" + rel.replace("/", "_").replace(".", "_")
    spec = importlib.util.spec_from_file_location(name, os.path.join(ROOT, rel))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def _norm(p):
    return os.path.normcase(os.path.realpath(os.fspath(p)))


@pytest.fixture
def spy(monkeypatch):
    seen = []
    real = catalog_path.connect_path

    def wrapped(path, *, write, **kw):
        seen.append((_norm(path), write, kw.get("timeout")))
        return real(path, write=write, **kw)
    monkeypatch.setattr(catalog_path, "connect_path", wrapped)
    return seen


@pytest.fixture
def pre_t0(tmp_path, monkeypatch):
    monkeypatch.setenv("AQUEDUCT_BACKEND", "x")
    monkeypatch.delenv("AQUEDUCT_BACKEND")
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "no_flag" / "CUTOVER"))
    return tmp_path


def _cat(path, rows=(), source=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(path)
    c.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, frequency TEXT, "
              "unit TEXT, geography TEXT, category TEXT, license_id TEXT, start_date TEXT, end_date TEXT, "
              "last_updated TEXT, metadata TEXT)")
    c.execute("CREATE TABLE source (source_id TEXT PRIMARY KEY, license_id TEXT)")
    c.execute("CREATE VIRTUAL TABLE series_fts USING fts5(series_id UNINDEXED, title, geography)")
    if source:
        c.execute("INSERT INTO source VALUES (?, 'lic-1')", (source,))
    for sid, title in rows:
        c.execute("INSERT INTO series (series_id, source_id, title) VALUES (?,?,?)", (sid, sid.split(":")[0], title))
        c.execute("INSERT INTO series_fts (series_id, title) VALUES (?,?)", (sid, title))
    c.commit()
    c.close()
    return path


def _q(p, sql):
    c = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
    try:
        return c.execute(sql).fetchall()
    finally:
        c.close()


# ---- flowgrain_insee_melodi / flowgrain_ons_uk ------------------------------------------------------------------
FLOW = {"tools/flowgrain_insee_melodi.py": ("insee_melodi", "flow_titles", 5.0),
        "tools/flowgrain_ons_uk.py": ("ons_uk", "ons_titles", 180)}


class _Store:
    bucket, root, store = "econ-data", "<recording store>", "<opened>"

    def __init__(self, objs):
        self.objs = objs

    def list_keys(self, prefix):
        return sorted(k for k in self.objs if k.startswith(prefix))

    def get(self, key):
        return self.objs.get(key)


def _pq(keys):
    buf = io.BytesIO()
    pq.write_table(pa.table({"series_key": keys, "obs_date": pa.array([dt.date(2020, 1, 1)] * len(keys), pa.date32()),
                             "value": [1.0] * len(keys)}), buf)
    return buf.getvalue()


def _flow(rel, tmp_path, monkeypatch):
    m = _load(rel)
    src, titles_fn, _t = FLOW[rel]
    cat = _cat(tmp_path / "data" / "catalog.db", [(f"{src}:OLD", "old row"), ("zz:keep", "other source")], source=src)
    monkeypatch.setattr(m, "CATALOG", str(cat))
    monkeypatch.setattr(m, titles_fn, lambda: {"DS_A": "Real title A"})
    store = _Store({f"clean_full/{src}/DS_A.parquet": _pq(["A=1"]), f"clean_full/{src}/DS_B.parquet": _pq(["B=1"])})
    monkeypatch.setattr(blob, "R2Blob", lambda *a, **k: store)
    return m, src, cat


@pytest.mark.parametrize("rel", sorted(FLOW))
def test_flowgrain_dry_run_reads_only(pre_t0, monkeypatch, spy, rel):
    m, src, cat = _flow(rel, pre_t0, monkeypatch)
    monkeypatch.setattr(sys, "argv", ["x", "--dry-run", "--threads", "1"])
    m.main()
    assert spy == [(_norm(cat), False, 5.0)]
    assert _q(cat, "SELECT series_id FROM series ORDER BY 1") == [(f"{src}:OLD",), ("zz:keep",)]


@pytest.mark.parametrize("rel", sorted(FLOW))
def test_flowgrain_catalog_rewrites_the_source_s_rows_and_fts(pre_t0, monkeypatch, spy, rel):
    m, src, cat = _flow(rel, pre_t0, monkeypatch)
    monkeypatch.setattr(sys, "argv", ["x", "--catalog", "--threads", "1"])
    m.main()
    assert spy == [(_norm(cat), False, 5.0), (_norm(cat), True, FLOW[rel][2])]
    assert _q(cat, "SELECT series_id, title, license_id FROM series ORDER BY 1") == [
        (f"{src}:DS_A", "Real title A", "lic-1"), (f"{src}:DS_B", "DS_B", "lic-1"), ("zz:keep", "other source", None)]
    assert _q(cat, "SELECT series_id, title FROM series_fts ORDER BY 1") == [
        (f"{src}:DS_A", "Real title A"), (f"{src}:DS_B", "DS_B"), ("zz:keep", "other source")]


@pytest.mark.parametrize("rel", sorted(FLOW))
def test_flowgrain_missing_catalogue_is_an_error_and_never_created(pre_t0, monkeypatch, rel):
    m, src, cat = _flow(rel, pre_t0, monkeypatch)
    os.remove(cat)
    assert cat.parent.is_dir()
    for mode in ("--dry-run", "--catalog"):
        monkeypatch.setattr(sys, "argv", ["x", mode, "--threads", "1"])
        with pytest.raises((FileNotFoundError, sqlite3.OperationalError)):
            m.main()
        assert not cat.exists()


@pytest.mark.parametrize("rel", sorted(FLOW))
def test_flowgrain_resolves_the_override_at_the_open(pre_t0, monkeypatch, rel):
    """$ECONDL_CATALOG when set, else this checkout's data/ catalogue - the old constant's two answers."""
    m = _load(rel)
    assert m.CATALOG is None
    other = pre_t0 / "elsewhere" / "named.db"
    monkeypatch.setenv("ECONDL_CATALOG", str(other))
    assert m._catalogue() == str(other)
    monkeypatch.delenv("ECONDL_CATALOG")
    assert _norm(m._catalogue()) == _norm(os.path.join(m.ROOT, "data", "catalog" + ".db"))


@pytest.mark.parametrize("rel", sorted(FLOW))
def test_flowgrain_after_t0_writes_only_the_build_under_the_lock(tmp_path, monkeypatch, rel):
    m, src, cat = _flow(rel, tmp_path, monkeypatch)
    from updater import config
    (tmp_path / "CUTOVER").write_text("")
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    monkeypatch.setattr(catalog_path, "LOCK_PATH", str(tmp_path / "state" / "writer.lock"))
    data_root = tmp_path / "clean_full"
    (data_root / src).mkdir(parents=True)
    (data_root / src / "DS_L.parquet").write_bytes(_pq(["L=1"]))
    monkeypatch.setattr(config, "DATA_ROOT", str(data_root))
    monkeypatch.setattr(sys, "argv", ["x", "--catalog", "--threads", "1"])
    monkeypatch.setattr(catalog_path, "BUILD_PATH", str(tmp_path / "the_build.db"))
    with pytest.raises(cutover.CutoverRefused, match="not"):
        m.main()                                           # a catalogue that is not the build
    monkeypatch.setattr(catalog_path, "BUILD_PATH", str(cat))
    with pytest.raises(cutover.CutoverRefused, match="single-writer lock"):
        m.main()
    with catalog_path.write_session():
        m.main()
    assert _q(cat, "SELECT series_id FROM series ORDER BY 1") == [(f"{src}:DS_L",), ("zz:keep",)]


# ---- delist_timeless_tables -------------------------------------------------------------------------------------
class _NotFound(Exception):
    response = {"Error": {"Code": "404"}}


def _delist(tmp_path, monkeypatch):
    m = _load("tools/delist_timeless_tables.py")
    root = tmp_path / "root"
    cat = _cat(root / "data" / "catalog.db", [(m.IDS[0], "Slovenia table one"), (m.IDS[-1], "CBS table"),
                                             ("zz:keep", "other")])
    monkeypatch.setattr(m, "ROOT", str(root))
    monkeypatch.setattr(m, "STORE", str(root / "data" / "clean_full"))

    def d1(sql):
        if sql.startswith("SELECT COUNT(*)"):
            return {"results": [{"n": 0}], "meta": {}}
        return {"results": [], "meta": {}}
    monkeypatch.setattr(m, "d1", d1)

    class C:
        def head_object(self, Bucket, Key):
            if Key != m.CONTROL_KEY:
                raise _NotFound()
    monkeypatch.setattr(m, "r2", lambda: C())
    monkeypatch.setattr(m, "live_status", lambda sid: 404)
    monkeypatch.setattr(m, "subprocess", types.SimpleNamespace(
        run=lambda *a, **k: types.SimpleNamespace(returncode=0, stdout="ok", stderr="")))
    monkeypatch.setattr(m.time, "sleep", lambda s: None)
    return m, cat


def test_delist_dry_run_reads_only_and_apply_deletes_the_local_rows(pre_t0, monkeypatch, spy):
    m, cat = _delist(pre_t0, monkeypatch)
    monkeypatch.setattr(sys, "argv", ["x"])
    assert m.main() == 0
    assert spy == [(_norm(cat), False, 30)]
    assert len(_q(cat, "SELECT * FROM series")) == 3
    spy.clear()
    monkeypatch.setattr(sys, "argv", ["x", "--apply"])
    assert m.main() == 0
    assert spy == [(_norm(cat), False, 30), (_norm(cat), True, 60.0)]
    assert _q(cat, "SELECT series_id FROM series") == [("zz:keep",)]
    assert _q(cat, "SELECT series_id FROM series_fts") == [("zz:keep",)]


def test_delist_missing_catalogue_fails_the_step_and_is_never_created(pre_t0, monkeypatch, capsys):
    m, cat = _delist(pre_t0, monkeypatch)
    os.remove(cat)
    monkeypatch.setattr(sys, "argv", ["x"])
    assert m.main() == 0 and "not readable" in capsys.readouterr().out     # the check: as the old mode=ro open
    monkeypatch.setattr(sys, "argv", ["x", "--apply"])
    assert m.main() == 1 and "4 local catalogue FAILED" in capsys.readouterr().out
    assert not cat.exists() and cat.parent.is_dir()


def test_delist_refuses_a_flag_prefix(pre_t0, monkeypatch):
    m, _cat_ = _delist(pre_t0, monkeypatch)
    monkeypatch.setattr(sys, "argv", ["x", "--app"])
    with pytest.raises(SystemExit):
        m.main()


# ---- make_servable before T0 -------------------------------------------------------------------------------------
def test_make_servable_before_t0_reads_the_checkout_s_catalogue_read_only(pre_t0, monkeypatch, spy):
    m = _load("tools/make_servable.py")
    root = pre_t0 / "root"
    cat = _cat(root / "data" / "catalog.db", [("zz:a", "A"), ("zz:b", "B"), ("zy:c", "C")])
    monkeypatch.setattr(m, "ROOT", str(root))
    monkeypatch.setattr(m, "_prepare_process", lambda: None)
    monkeypatch.setattr(m.r2_util, "client", lambda *a, **k: object())
    monkeypatch.setattr(m.bm, "from_env", lambda *a, **k: object())
    monkeypatch.setattr(m, "sync_parquet", lambda client, src: 1)
    monkeypatch.setattr(m, "subprocess", types.SimpleNamespace(
        run=lambda *a, **k: types.SimpleNamespace(returncode=0, stdout="", stderr="")))
    monkeypatch.setattr(m, "parquet_mtime", lambda client, src: None)
    monkeypatch.setattr(m, "r2_stamps", lambda client, src: {})
    derived = []
    monkeypatch.setattr(m.derive, "derive_and_put", lambda ids, b: derived.append(sorted(ids)) or
                        {"put": len(ids), "failed": []})
    m.main(["zz"])
    assert spy == [(_norm(cat), False, 5.0)]
    assert derived == [["zz:a", "zz:b"]]
    os.remove(cat)
    with pytest.raises((FileNotFoundError, sqlite3.OperationalError)):
        m.main(["zz"])
    assert not cat.exists() and cat.parent.is_dir()
