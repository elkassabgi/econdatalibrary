"""The D1 exporters and loaders open the catalogue through core.catalog_path (t0_ready legacy-catalogue,
2026-11-17): core/export_d1.py, export_d1_i18n_delta.py, export_d1_new_series.py, export_d1_sources.py,
load_d1_chunked.py and load_d1_rest.py.

Before T0: <ROOT>/data's catalogue, READ-ONLY, with sqlite's own 5 s busy timeout (the plain opens had no
timeout argument and no busy_timeout PRAGMA after them, so 5 s is the effective value and it is pinned here).
A missing catalogue raises sqlite3.OperationalError (the class the old opens raised) and creates nothing (a
plain sqlite3.connect made an empty file).
After T0: the build only; a worktree's own catalogue is refused. Hermetic: temporary roots, no network."""
import json
import os
import sqlite3
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from core import catalog_path, cutover  # noqa: E402
from core import export_d1, export_d1_i18n_delta, export_d1_new_series, export_d1_sources  # noqa: E402
from core import load_d1_chunked, load_d1_rest  # noqa: E402

MODULES = [export_d1, export_d1_i18n_delta, export_d1_new_series, export_d1_sources, load_d1_chunked,
           load_d1_rest]
SCOLS = ["series_id", "source_id", "title", "frequency", "unit", "geography", "category", "license_id",
         "start_date", "end_date", "last_updated", "metadata"]


def _cat(root):
    p = root / "data" / "catalog.db"
    p.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(p)
    c.execute("CREATE TABLE license (license_id TEXT PRIMARY KEY, name TEXT)")
    c.execute("CREATE TABLE source (source_id TEXT PRIMARY KEY, name TEXT, license_id TEXT)")
    c.execute(f"CREATE TABLE series ({', '.join(c_ + ' TEXT' for c_ in SCOLS)}, PRIMARY KEY(series_id))")
    c.execute("CREATE VIRTUAL TABLE series_fts USING fts5(series_id UNINDEXED, title, geography)")
    c.execute("INSERT INTO license VALUES ('cc-by', 'CC BY 4.0')")
    c.execute("INSERT INTO source VALUES ('src', 'A source', 'cc-by')")
    md = json.dumps({"titles": {"fr": "Une serie"}})
    c.execute(f"INSERT INTO series ({', '.join(SCOLS)}) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
              ("src:a", "src", "A series", "A", "u", "W", "c", "cc-by", "2000", "2020", "x", md))
    c.commit()
    c.close()
    return p


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


def _cut(tmp_path, monkeypatch, build=None):
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    (tmp_path / "CUTOVER").write_text("")
    if build is not None:
        monkeypatch.setattr(catalog_path, "BUILD_PATH", str(build))


# ---- the one helper, in each of the six files -----------------------------------------------------------
@pytest.mark.parametrize("mod", MODULES, ids=lambda m: m.__name__)
def test_the_open_is_read_only_with_sqlite_s_own_timeout(mod, tmp_path, monkeypatch, spy):
    p = _cat(tmp_path)
    monkeypatch.setattr(mod, "ROOT", str(tmp_path))
    con = mod._catalogue_ro()
    try:
        assert con.execute("SELECT COUNT(*) FROM series").fetchone()[0] == 1
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            con.execute("INSERT INTO license VALUES ('x', 'y')")
    finally:
        con.close()
    assert spy == [(_norm(p), False, 5.0)]


@pytest.mark.parametrize("mod", MODULES, ids=lambda m: m.__name__)
def test_a_missing_catalogue_is_an_error_and_nothing_is_created(mod, tmp_path, monkeypatch):
    monkeypatch.setattr(mod, "ROOT", str(tmp_path / "nowhere"))
    with pytest.raises(sqlite3.OperationalError):
        mod._catalogue_ro()
    assert not (tmp_path / "nowhere").exists()


@pytest.mark.parametrize("mod", MODULES, ids=lambda m: m.__name__)
def test_after_t0_a_worktree_catalogue_is_refused(mod, tmp_path, monkeypatch):
    _cat(tmp_path)
    monkeypatch.setattr(mod, "ROOT", str(tmp_path))
    _cut(tmp_path, monkeypatch)
    with pytest.raises(cutover.CutoverRefused):
        mod._catalogue_ro()


@pytest.mark.parametrize("mod", MODULES, ids=lambda m: m.__name__)
def test_after_t0_the_build_opens_read_only(mod, tmp_path, monkeypatch):
    p = _cat(tmp_path)
    monkeypatch.setattr(mod, "ROOT", str(tmp_path))
    _cut(tmp_path, monkeypatch, build=p)
    con = mod._catalogue_ro()
    try:
        with pytest.raises(sqlite3.Error):
            con.execute("INSERT INTO license VALUES ('x', 'y')")
    finally:
        con.close()


# ---- each exporter's main() goes through it (run end to end on a temporary root) ------------------------
def _point(mod, monkeypatch, tmp_path, **extra):
    monkeypatch.setattr(mod, "ROOT", str(tmp_path))
    out = tmp_path / "dist"
    out.mkdir(exist_ok=True)
    monkeypatch.setattr(mod, "OUT_DIR", str(out))
    for k, v in extra.items():
        monkeypatch.setattr(mod, k, v)
    return out


def test_export_d1_main_dumps_through_the_read_only_open(tmp_path, monkeypatch, spy):
    p = _cat(tmp_path)
    out = _point(export_d1, monkeypatch, tmp_path, OUT=str(tmp_path / "dist" / "econ_catalog.sql"),
                 STATE=str(tmp_path / "no_state.db"))
    export_d1.main()
    sql = (out / "econ_catalog.sql").read_text(encoding="utf-8")
    assert "INSERT INTO series" in sql and "'src:a'" in sql
    assert spy == [(_norm(p), False, 5.0)]


def test_export_d1_on_a_missing_catalogue_writes_no_dump(tmp_path, monkeypatch):
    """Before: the plain open CREATED an empty catalogue and an empty dump - one that DROPs every D1 table."""
    out = _point(export_d1, monkeypatch, tmp_path, OUT=str(tmp_path / "dist" / "econ_catalog.sql"),
                 STATE=str(tmp_path / "no_state.db"))
    with pytest.raises(sqlite3.OperationalError):
        export_d1.main()
    assert not (out / "econ_catalog.sql").exists()
    assert not (tmp_path / "data").exists()


def test_export_d1_i18n_delta_main_reads_through_the_read_only_open(tmp_path, monkeypatch, spy):
    p = _cat(tmp_path)
    out = _point(export_d1_i18n_delta, monkeypatch, tmp_path)
    export_d1_i18n_delta.main()
    assert any(f.startswith("part_") for f in os.listdir(out))
    assert spy == [(_norm(p), False, 5.0)]


def test_export_d1_new_series_main_reads_through_the_read_only_open(tmp_path, monkeypatch, spy):
    p = _cat(tmp_path)
    titles = tmp_path / "titles"
    titles.mkdir()
    (titles / "src.json").write_text(json.dumps({"src:a": "A series"}), encoding="utf-8")
    out = _point(export_d1_new_series, monkeypatch, tmp_path, TITLES_DIR=str(titles))
    export_d1_new_series.main(["src"])
    assert "'src:a'" in (out / "part_000.sql").read_text(encoding="utf-8")
    assert spy == [(_norm(p), False, 5.0)]


def test_export_d1_sources_main_reads_through_the_read_only_open(tmp_path, monkeypatch, spy):
    p = _cat(tmp_path)
    out = _point(export_d1_sources, monkeypatch, tmp_path)
    with pytest.raises(SystemExit) as ex:
        export_d1_sources.main(["src"])
    assert ex.value.code == 0
    assert "'src:a'" in (out / "part_000.sql").read_text(encoding="utf-8")
    assert spy == [(_norm(p), False, 5.0)]


@pytest.mark.parametrize("mod", [load_d1_chunked, load_d1_rest], ids=lambda m: m.__name__)
def test_the_loaders_verify_step_uses_the_helper(mod):
    """main() talks to D1, so it is not run here; its one catalogue open is the helper (the catalog_path
    ratchet already refuses any other spelling of the file in these modules)."""
    import inspect
    src = inspect.getsource(mod.main)
    assert "local = _catalogue_ro()" in src and "sqlite3.connect" not in src
