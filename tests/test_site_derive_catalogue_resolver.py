"""catalog/gen_site.py, tools/derive_unsdg_flows.py and tools/derive_csv_bulk.py open the catalogue through
core.catalog_path (t0_ready legacy-catalogue, 2026-11-16). Before T0: the same file, read-only, the same
timeout, the same exception class on a missing file. After T0: the build only."""
import importlib
import os
import sqlite3
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "catalog"))
sys.path.insert(0, os.path.join(ROOT, "tools"))
sys.path.insert(0, os.path.join(ROOT, "tests"))
from core import catalog_path, cutover  # noqa: E402

import gen_site as G  # noqa: E402


def _cat(root):
    p = root / "data" / "catalog.db"
    p.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(p) as c:
        c.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, start_date TEXT)")
        c.execute("INSERT INTO series VALUES ('maddison:x', 'maddison', '0001-01-01')")
    c.close()
    return p


@pytest.fixture
def spy(monkeypatch):
    seen = []
    real = catalog_path.connect_path

    def wrapped(path, *, write, **kw):
        seen.append((os.path.normcase(os.path.realpath(os.fspath(path))), write, kw.get("timeout")))
        return real(path, write=write, **kw)
    monkeypatch.setattr(catalog_path, "connect_path", wrapped)
    return seen


def _cut(tmp_path, monkeypatch):
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    (tmp_path / "CUTOVER").write_text("")


# ---- gen_site -------------------------------------------------------------------------------------------
def test_gen_site_reads_its_own_root_s_catalogue_read_only(tmp_path, monkeypatch, spy):
    p = _cat(tmp_path)
    monkeypatch.setattr(G, "ROOT", str(tmp_path))
    con = G._catalogue_ro(timeout=120)
    with pytest.raises(sqlite3.OperationalError, match="readonly"):
        con.execute("INSERT INTO series VALUES ('a:b', 'a', NULL)")
    con.close()
    assert spy == [(os.path.normcase(os.path.realpath(str(p))), False, 120)]


def test_gen_site_years_of_history_still_reads(tmp_path, monkeypatch):
    _cat(tmp_path)
    monkeypatch.setattr(G, "ROOT", str(tmp_path))
    assert G._earliest_data_year() >= 2000           # the maddison row starts in year 1


def test_gen_site_a_missing_catalogue_raises_sqlite_s_error(tmp_path, monkeypatch):
    monkeypatch.setattr(G, "ROOT", str(tmp_path / "nowhere"))
    with pytest.raises(sqlite3.OperationalError):
        G._catalogue_ro()
    assert not (tmp_path / "nowhere").exists()


def test_gen_site_after_t0_a_worktree_catalogue_is_refused(tmp_path, monkeypatch):
    _cat(tmp_path)
    monkeypatch.setattr(G, "ROOT", str(tmp_path))
    _cut(tmp_path, monkeypatch)
    with pytest.raises(cutover.CutoverRefused):
        G._catalogue_ro()


# ---- derive_unsdg_flows ---------------------------------------------------------------------------------
def test_unsdg_opens_the_catalogue_read_only(tmp_path, monkeypatch, spy):
    monkeypatch.setenv("AQUEDUCT_BACKEND", "x")
    monkeypatch.delenv("AQUEDUCT_BACKEND")
    import test_derive_batch3_blob_route as B
    m = importlib.import_module("derive_unsdg_flows")
    main = tmp_path / "main"
    (main / "data" / "clean_full" / "unsdg").mkdir(parents=True)
    store_file = main / "data" / "clean_full" / "unsdg" / "unsdg.parquet"
    store_file.write_bytes(B._parquet_bytes(["AG_A:AFG"]))
    B._catalog(str(main / "data" / "catalog.db"), [("unsdg:AG_A", "unsdg")])
    monkeypatch.setattr(m, "MAIN", str(main))
    monkeypatch.setattr(m, "STORE", str(store_file))
    B._route(monkeypatch, B.Store())
    monkeypatch.setattr(sys, "argv", ["x", "--bucket", "econ-data", "--threads", "1"])
    assert m.main() == 0
    assert spy == [(os.path.normcase(os.path.realpath(str(main / "data" / "catalog.db"))), False, 120)]


# ---- derive_csv_bulk ------------------------------------------------------------------------------------
def test_bulk_only_catalogued_opens_read_only():
    """PR4 mutant M4 (write=True) survived every running test: the --only-catalogued open sits deep in main().
    Pin it structurally: every catalog_path.connect_path call in the file passes write=False (a write=True open
    after T0 without the writer lock would be REFUSED and break the bulk derive)."""
    import ast
    path = os.path.join(ROOT, "tools", "derive_csv_bulk.py")
    tree = ast.parse(open(path, encoding="utf-8").read())
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
             and n.func.attr == "connect_path"]
    assert len(calls) == 1, f"expected one catalogue open, found {len(calls)}"
    w = next((k.value for k in calls[0].keywords if k.arg == "write"), None)
    assert isinstance(w, ast.Constant) and w.value is False, ast.unparse(calls[0])
