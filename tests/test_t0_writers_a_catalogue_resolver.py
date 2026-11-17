"""Four catalogue WRITERS open the catalogue through core.catalog_path (t0_ready legacy-catalogue, 2026-11-17):
core/backfill_coverage.py, pipeline/title_bea.py, tools/apply_series_names.py and tools/refresh_flowgrain_dates.py.

Each one is now on the writer road: connect_path(write=...) on the same file as before (backfill_coverage: its
fixed REPO's catalogue; the others: <ROOT>/data's), a read-only open for a dry run, a read-write open for a write,
and the single-writer lock around the write entry (tests/test_catalogue_writers_main.py runs the entries). The
timeouts are the old ones: sqlite's own 5 s where no busy_timeout PRAGMA follows the open (pinned here, because
then the argument IS the busy timeout), the old argument where a PRAGMA follows. A missing catalogue is an error
and is never created - the old plain opens made an empty file (the tests use an EXISTING data/ folder, so a
creating fallback would be seen). After T0 a write without the lock is refused, and refresh_flowgrain_dates
refuses before any read (it writes local AND D1; D1 is frozen after T0). Hermetic: temporary roots, no network,
the store and D1 stubbed."""
import importlib.util
import os
import sqlite3
import sys
import types

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from core import catalog_path, cutover  # noqa: E402


def _load(rel):
    name = "_t0wa_" + rel.replace("/", "_").replace(".", "_")
    spec = importlib.util.spec_from_file_location(name, os.path.join(ROOT, rel))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


@pytest.fixture(scope="module")
def mods():
    return {r: _load(r) for r in ("core/backfill_coverage.py", "pipeline/title_bea.py",
                                  "tools/apply_series_names.py", "tools/refresh_flowgrain_dates.py")}


def _cat(root, rows=(), fts=True):
    p = root / "data" / "catalog.db"
    p.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(p)
    c.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, frequency TEXT, "
              "unit TEXT, geography TEXT, category TEXT, license_id TEXT, start_date TEXT, end_date TEXT, "
              "last_updated TEXT, metadata TEXT)")
    if fts:
        c.execute("CREATE VIRTUAL TABLE series_fts USING fts5(series_id UNINDEXED, title, geography)")
    for sid, title, sd, ed in rows:
        c.execute("INSERT INTO series (series_id, source_id, title, start_date, end_date) VALUES (?,?,?,?,?)",
                  (sid, sid.split(":")[0], title, sd, ed))
    c.commit()
    c.close()
    return p


def _rows(p, sql):
    c = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
    try:
        return c.execute(sql).fetchall()
    finally:
        c.close()


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
def cut(tmp_path, monkeypatch):
    """After T0, with the build at `build` and the lock file in the temporary folder."""
    def go(build):
        monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
        (tmp_path / "CUTOVER").write_text("")
        monkeypatch.setattr(catalog_path, "BUILD_PATH", str(build))
        monkeypatch.setattr(catalog_path, "LOCK_PATH", str(tmp_path / "state" / "writer.lock"))
    return go


# ---- core/backfill_coverage.py ---------------------------------------------------------------------------------
def _backfill_root(tmp_path, monkeypatch, m):
    import pyarrow as pa
    import pyarrow.parquet as pq
    import datetime as dt
    root = tmp_path / "repo"
    d = root / "data" / "clean_full" / "zz"
    d.mkdir(parents=True)
    pq.write_table(pa.table({"series_key": ["a", "a", "c"],
                             "obs_date": [dt.date(2001, 1, 1), dt.date(2005, 6, 30), dt.date(1999, 1, 1)],
                             "value": [1.0, 2.0, 3.0]}), d / "x.parquet")
    monkeypatch.setattr(m, "REPO", str(root))
    return root


def test_backfill_dry_run_reads_only_and_apply_fills_only_dateless_rows(mods, tmp_path, monkeypatch, spy):
    m = mods["core/backfill_coverage.py"]
    root = _backfill_root(tmp_path, monkeypatch, m)
    db = _cat(root, [("zz:a", "A", None, None), ("zz:b", "B", None, None), ("zz:c", "C", "1990-01-01", None)])
    monkeypatch.setattr(sys, "argv", ["backfill_coverage.py", "zz"])
    assert m.main() == 0
    assert spy == [(_norm(db), False, 5.0)]
    assert _rows(db, "SELECT start_date, end_date FROM series WHERE series_id='zz:a'") == [(None, None)]
    spy.clear()
    monkeypatch.setattr(sys, "argv", ["backfill_coverage.py", "zz", "--apply"])
    assert m.main() == 0
    assert spy == [(_norm(db), True, 5.0)]
    assert _rows(db, "SELECT series_id, start_date, end_date FROM series ORDER BY series_id") == [
        ("zz:a", "2001-01-01", "2005-06-30"), ("zz:b", None, None), ("zz:c", "1990-01-01", None)]


def test_backfill_missing_catalogue_is_an_error_and_never_created(mods, tmp_path, monkeypatch):
    m = mods["core/backfill_coverage.py"]
    root = _backfill_root(tmp_path, monkeypatch, m)
    assert (root / "data").is_dir()
    for argv in (["x", "zz"], ["x", "zz", "--apply"]):
        monkeypatch.setattr(sys, "argv", argv)
        with pytest.raises((FileNotFoundError, sqlite3.OperationalError)):
            m.main()
        assert not (root / "data" / "catalog.db").exists()


def test_backfill_after_t0_writes_only_under_the_lock(mods, tmp_path, monkeypatch, cut):
    m = mods["core/backfill_coverage.py"]
    root = _backfill_root(tmp_path, monkeypatch, m)
    db = _cat(root, [("zz:a", "A", None, None)])
    cut(db)
    monkeypatch.setattr(sys, "argv", ["x", "zz", "--apply"])
    with pytest.raises(cutover.CutoverRefused, match="single-writer lock"):
        m.main()
    assert _rows(db, "SELECT start_date FROM series") == [(None,)]
    with catalog_path.write_session():
        assert m.main() == 0
    assert _rows(db, "SELECT start_date FROM series") == [("2001-01-01",)]


# ---- pipeline/title_bea.py -------------------------------------------------------------------------------------
def _bea(tmp_path, monkeypatch, m):
    root = tmp_path / "bea_root"
    db = _cat(root, [("bea:T1", "T1", None, None), ("bea:T2", "A real title", None, None),
                     ("bea:R1:G", "R1:G", None, None), ("zz:x", "other", None, None)])
    monkeypatch.setattr(m, "ROOT", str(root))
    monkeypatch.setattr(m, "build", lambda: {"NIPA": {"T1": "Gross output", "T2": "never applied"}})
    # the whole ingest module is replaced: it is imported at first use and its import requires the BEA key
    monkeypatch.setattr(m, "ing", types.SimpleNamespace(load_manifest=lambda: {"param_values": {}}))
    monkeypatch.setattr(m, "build_regional", lambda pv: {"R1:G": "Somewhere - table line R1"})
    return root, db


def test_title_bea_both_modes_write_the_root_s_catalogue_and_its_fts(mods, tmp_path, monkeypatch, spy):
    m = mods["pipeline/title_bea.py"]
    root, db = _bea(tmp_path, monkeypatch, m)
    assert m.main() == 0
    assert spy == [(_norm(db), True, 7200)]
    assert _rows(db, "SELECT series_id, title FROM series ORDER BY series_id") == [
        ("bea:R1:G", "R1:G"), ("bea:T1", "Gross output"), ("bea:T2", "A real title"), ("zz:x", "other")]
    assert ("bea:T1", "Gross output") in _rows(db, "SELECT series_id, title FROM series_fts")
    spy.clear()
    assert m.rerun_regional() == 0
    assert spy == [(_norm(db), True, 7200)]
    assert _rows(db, "SELECT title FROM series WHERE series_id='bea:R1:G'") == [("Somewhere - table line R1",)]


def test_title_bea_missing_catalogue_is_an_error_and_never_created(mods, tmp_path, monkeypatch):
    m = mods["pipeline/title_bea.py"]
    root, db = _bea(tmp_path, monkeypatch, m)
    os.remove(db)
    for fn in (m.main, m.rerun_regional):
        with pytest.raises((FileNotFoundError, sqlite3.OperationalError)):
            fn()
        assert not db.exists() and (root / "data").is_dir()


def test_title_bea_after_t0_needs_the_lock(mods, tmp_path, monkeypatch, cut):
    m = mods["pipeline/title_bea.py"]
    root, db = _bea(tmp_path, monkeypatch, m)
    cut(db)
    for fn in (m.main, m.rerun_regional):
        with pytest.raises(cutover.CutoverRefused, match="single-writer lock"):
            fn()
    assert _rows(db, "SELECT title FROM series WHERE series_id='bea:T1'") == [("T1",)]


@pytest.mark.parametrize("argv,called", [([], "main"), (["--only-regional"], "rerun_regional")])
def test_title_bea_entry_holds_the_lock_in_both_modes(mods, tmp_path, monkeypatch, cut, argv, called):
    """Review AR-279 C1: the __main__ dispatch itself, run after T0 - each mode's function is called with the
    lock held (tests/test_catalogue_writers_main.py runs the entry with default arguments only, so a
    --only-regional branch moved out of write_session() passed every test there)."""
    import ast
    m = mods["pipeline/title_bea.py"]
    cut(tmp_path / "build.db")
    tree = ast.parse(open(m.__file__, encoding="utf-8").read())
    block = next(n for n in tree.body if isinstance(n, ast.If) and "__main__" in ast.unparse(n.test))
    seen = []

    def probe(name):
        def f(*a, **k):
            seen.append((name, catalog_path._held is not None))
            return 0
        return f
    g = dict(vars(m))
    g.update(main=probe("main"), rerun_regional=probe("rerun_regional"))
    monkeypatch.setattr(sys, "argv", ["title_bea.py"] + argv)
    with pytest.raises(SystemExit):
        exec(compile(ast.Module(body=block.body, type_ignores=[]), m.__file__, "exec"), g)  # noqa: S102
    assert seen == [(called, True)], f"{argv}: called with (function, lock held) = {seen}"
    assert catalog_path._held is None, "the lock is let go when the entry returns"


# ---- tools/apply_series_names.py -------------------------------------------------------------------------------
def _names(tmp_path, monkeypatch, m):
    root = tmp_path / "names_root"
    db = _cat(root, [("zz:a", "old a", None, None), ("zz:b", "b", None, None)])
    sweep = tmp_path / "sweep"
    (sweep / "csv").mkdir(parents=True)
    (sweep / "csv" / "zz.csv").write_text("series_id,title,description,geography,unit\n"
                                         "zz:a,New A,,France,\nzz:b,b,,,\n", encoding="utf-8")
    monkeypatch.setattr(m, "ROOT", str(root))
    monkeypatch.setattr(m, "SWEEP", str(sweep))
    monkeypatch.setattr(m, "PENDING", str(tmp_path / "state" / "pending.txt"))
    return root, db


def test_apply_series_names_measure_reads_only_and_apply_writes(mods, tmp_path, monkeypatch, spy):
    m = mods["tools/apply_series_names.py"]
    root, db = _names(tmp_path, monkeypatch, m)
    monkeypatch.setattr(sys, "argv", ["x", "--measure"])
    m.main()
    assert spy == [(_norm(db), False, 7200)]
    assert _rows(db, "SELECT title, geography FROM series WHERE series_id='zz:a'") == [("old a", None)]
    assert not os.path.exists(m.PENDING)
    spy.clear()
    monkeypatch.setattr(sys, "argv", ["x", "--apply"])
    m.main()
    assert spy == [(_norm(db), True, 7200)]
    assert _rows(db, "SELECT title, geography FROM series WHERE series_id='zz:a'") == [("New A", "France")]
    assert open(m.PENDING, encoding="utf-8").read() == "zz:a\n"


def test_apply_series_names_refuses_a_flag_prefix(mods, tmp_path, monkeypatch):
    """__main__ takes the lock on the exact '--apply'; argparse must not read '--app' as it."""
    m = mods["tools/apply_series_names.py"]
    _names(tmp_path, monkeypatch, m)
    monkeypatch.setattr(sys, "argv", ["x", "--app"])
    with pytest.raises(SystemExit):
        m.main()


def test_apply_series_names_missing_catalogue_is_an_error_and_never_created(mods, tmp_path, monkeypatch):
    m = mods["tools/apply_series_names.py"]
    root, db = _names(tmp_path, monkeypatch, m)
    os.remove(db)
    for argv in (["x", "--measure"], ["x", "--apply"]):
        monkeypatch.setattr(sys, "argv", argv)
        with pytest.raises((FileNotFoundError, sqlite3.OperationalError)):
            m.main()
        assert not db.exists() and (root / "data").is_dir()


def test_apply_series_names_after_t0_writes_only_under_the_lock(mods, tmp_path, monkeypatch, cut):
    m = mods["tools/apply_series_names.py"]
    root, db = _names(tmp_path, monkeypatch, m)
    cut(db)
    monkeypatch.setattr(sys, "argv", ["x", "--apply"])
    with pytest.raises(cutover.CutoverRefused, match="single-writer lock"):
        m.main()
    with catalog_path.write_session():
        m.main()
    assert _rows(db, "SELECT title FROM series WHERE series_id='zz:a'") == [("New A",)]


# ---- tools/refresh_flowgrain_dates.py --------------------------------------------------------------------------
def _flow(tmp_path, monkeypatch, m):
    root = tmp_path / "flow_root"
    db = _cat(root, [("zz:T1", "t1", "1990-01-01", "2010-12-31"), ("zz:T2", "t2", "2000-01-01", "2020-12-31"),
                     ("zy:T1", "other source", None, None)])
    monkeypatch.setattr(m, "ROOT", str(root))
    monkeypatch.setattr(m, "RECEIPT_DIR", str(tmp_path))
    monkeypatch.setattr(m, "_load_cataloguer", lambda: types.SimpleNamespace(SOURCES=["zz"]))
    monkeypatch.setattr(m.config, "BACKEND", "r2")
    monkeypatch.setattr(m, "store_truth", lambda src: ({"T1": ("2000-01-01", "2020-12-31", 9),
                                                        "T2": ("2000-01-01", "2020-12-31", 9)}, ["f.parquet"]))
    d1 = {"zz:T1": ("1990-01-01", "2010-12-31"), "zz:T2": ("2000-01-01", "2020-12-31")}
    calls = []

    def d1_rows(ids):
        calls.append(("read", list(ids)))
        return {i: d1[i] for i in ids if i in d1}, 1

    def wrangler(args):
        calls.append(("write", args[0]))
        d1["zz:T1"] = ("2000-01-01", "2020-12-31")
        return [{"meta": {"changes": 1}, "results": []}]
    monkeypatch.setattr(m, "d1_rows", d1_rows)
    monkeypatch.setattr(m, "_wrangler_json", wrangler)
    monkeypatch.setattr(m, "live_meta", lambda sid: (200,) + d1[sid])
    return root, db, calls


def test_flowgrain_dates_local_rows_read_only_by_the_pk_range(mods, tmp_path, monkeypatch, spy):
    m = mods["tools/refresh_flowgrain_dates.py"]
    root, db, _c = _flow(tmp_path, monkeypatch, m)
    assert m.local_rows("zz") == {"zz:T1": ("1990-01-01", "2010-12-31"), "zz:T2": ("2000-01-01", "2020-12-31")}
    assert spy == [(_norm(db), False, 5.0)]


def test_flowgrain_dates_dry_run_writes_nothing_and_apply_writes_local(mods, tmp_path, monkeypatch, spy):
    m = mods["tools/refresh_flowgrain_dates.py"]
    root, db, calls = _flow(tmp_path, monkeypatch, m)
    monkeypatch.setattr(sys, "argv", ["x", "--source", "zz", "--dry-run"])
    assert m.main() == 0
    assert spy == [(_norm(db), False, 5.0)] and all(c[0] == "read" for c in calls)
    assert _rows(db, "SELECT start_date FROM series WHERE series_id='zz:T1'") == [("1990-01-01",)]
    spy.clear()
    monkeypatch.setattr(sys, "argv", ["x", "--source", "zz", "--apply"])
    assert m.main() == 0
    assert spy == [(_norm(db), False, 5.0), (_norm(db), True, 180)]
    assert _rows(db, "SELECT series_id, start_date, end_date FROM series ORDER BY series_id") == [
        ("zy:T1", None, None), ("zz:T1", "2000-01-01", "2020-12-31"), ("zz:T2", "2000-01-01", "2020-12-31")]


def test_flowgrain_dates_missing_catalogue_is_an_error_and_never_created(mods, tmp_path, monkeypatch):
    m = mods["tools/refresh_flowgrain_dates.py"]
    root, db, _c = _flow(tmp_path, monkeypatch, m)
    os.remove(db)
    with pytest.raises((FileNotFoundError, sqlite3.OperationalError)):
        m.local_rows("zz")
    assert not db.exists() and (root / "data").is_dir()


def test_flowgrain_dates_refuses_after_t0_before_any_read(mods, tmp_path, monkeypatch, spy, cut):
    m = mods["tools/refresh_flowgrain_dates.py"]
    root, db, calls = _flow(tmp_path, monkeypatch, m)
    cut(db)
    for mode in ("--dry-run", "--apply"):
        monkeypatch.setattr(sys, "argv", ["x", "--source", "zz", mode])
        with pytest.raises(cutover.CutoverRefused):
            m.main()
    assert spy == [] and calls == []
