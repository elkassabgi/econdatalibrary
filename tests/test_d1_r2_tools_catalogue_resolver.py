"""Five D1 / R2 tools open the catalogue through core.catalog_path (t0_ready legacy-catalogue, 2026-11-17):
tools/migrate_noaa_shard.py, rebuild_series_fts.py, sync_titles_to_d1.py, repair_stale_csvs.py and
resync_and_repair.py.

Before T0: the checkout's catalogue (rebuild_series_fts: $AQUEDUCT_PLAN_CATALOG when set, as before), READ-ONLY,
with sqlite's own 5 s busy timeout (no timeout argument and no busy_timeout PRAGMA after the old opens, so 5 s is
the effective value; pinned here). A missing catalogue raises sqlite3.OperationalError, the class the old opens
raised, and creates nothing (migrate_noaa_shard's plain open used to create an empty file). After T0: the build
only. Hermetic: temporary roots, no network, D1 and R2 stubbed."""
import importlib.util
import inspect
import os
import sqlite3
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from core import catalog_path, cutover  # noqa: E402

NAMES = ["migrate_noaa_shard", "rebuild_series_fts", "sync_titles_to_d1", "repair_stale_csvs", "resync_and_repair"]


def _load(name):
    spec = importlib.util.spec_from_file_location(f"{name}_resolver_undertest", os.path.join(ROOT, "tools", name + ".py"))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


@pytest.fixture(scope="module")
def mods():
    return {n: _load(n) for n in NAMES}


def _cat(root):
    p = root / "data" / "catalog.db"
    p.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(p)
    c.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, frequency TEXT, "
              "unit TEXT, geography TEXT, category TEXT, license_id TEXT, start_date TEXT, end_date TEXT, "
              "last_updated TEXT, metadata TEXT)")
    c.execute("CREATE TABLE source (source_id TEXT PRIMARY KEY, name TEXT, homepage TEXT, license_id TEXT, "
              "attribution TEXT, terms_url TEXT)")
    c.execute("CREATE TABLE license (license_id TEXT PRIMARY KEY, name TEXT, url TEXT, reservable INTEGER, "
              "commercial_ok INTEGER, attribution_required INTEGER, no_modify INTEGER)")
    c.execute("INSERT INTO source VALUES ('noaa', 'NOAA', NULL, 'pd', NULL, NULL)")
    c.execute("INSERT INTO license VALUES ('pd', 'Public domain', NULL, 1, 1, 0, 0)")
    for i in range(3):
        c.execute("INSERT INTO series (series_id, source_id, title) VALUES (?, 'noaa', ?)", (f"noaa:s{i}", f"T{i}"))
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


# ---- the one helper, in each file -----------------------------------------------------------------------
@pytest.mark.parametrize("name", NAMES)
def test_the_open_is_read_only_with_sqlite_s_own_timeout(mods, name, tmp_path, monkeypatch, spy):
    m = mods[name]
    p = _cat(tmp_path)
    monkeypatch.setattr(m, "ROOT", str(tmp_path))
    con = m._catalogue_ro()
    try:
        assert con.execute("SELECT COUNT(*) FROM series").fetchone()[0] == 3
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            con.execute("DELETE FROM series")
    finally:
        con.close()
    assert spy == [(_norm(p), False, 5.0)]


@pytest.mark.parametrize("name", NAMES)
def test_a_missing_catalogue_raises_sqlite_s_error_and_creates_nothing(mods, name, tmp_path, monkeypatch):
    m = mods[name]
    monkeypatch.setattr(m, "ROOT", str(tmp_path / "nowhere"))
    with pytest.raises(sqlite3.OperationalError):
        m._catalogue_ro()
    assert not (tmp_path / "nowhere").exists()


@pytest.mark.parametrize("name", NAMES)
def test_a_missing_file_in_an_existing_data_dir_raises_and_no_file_is_created(mods, name, tmp_path, monkeypatch):
    # data/ exists, so a creating open WOULD succeed here and leave an empty catalog.db behind
    m = mods[name]
    (tmp_path / "data").mkdir()
    monkeypatch.setattr(m, "ROOT", str(tmp_path))
    with pytest.raises(sqlite3.OperationalError):
        m._catalogue_ro()
    assert list((tmp_path / "data").iterdir()) == []


@pytest.mark.parametrize("name", NAMES)
def test_after_t0_a_worktree_catalogue_is_refused(mods, name, tmp_path, monkeypatch):
    m = mods[name]
    _cat(tmp_path)
    monkeypatch.setattr(m, "ROOT", str(tmp_path))
    _cut(tmp_path, monkeypatch)
    with pytest.raises(cutover.CutoverRefused):
        m._catalogue_ro()


@pytest.mark.parametrize("name", NAMES)
def test_after_t0_the_build_opens_read_only(mods, name, tmp_path, monkeypatch):
    m = mods[name]
    p = _cat(tmp_path)
    monkeypatch.setattr(m, "ROOT", str(tmp_path))
    _cut(tmp_path, monkeypatch, build=p)
    con = m._catalogue_ro()
    try:
        with pytest.raises(sqlite3.Error):
            con.execute("DELETE FROM series")
    finally:
        con.close()


# ---- the call sites ------------------------------------------------------------------------------------
def test_noaa_verify_counts_through_the_read_only_open(mods, tmp_path, monkeypatch, spy):
    m = mods["migrate_noaa_shard"]
    p = _cat(tmp_path)
    monkeypatch.setattr(m, "ROOT", str(tmp_path))

    def d1(db, sql, **kw):
        if "COUNT(*)" in sql:
            return {"results": [{"n": 3}]}
        sid = sql.split("series_id='", 1)[1].split("'", 1)[0]
        return {"results": [{"title": "T" + sid[-1]}]}
    monkeypatch.setattr(m, "_d1", d1)
    assert m.verify() == 0
    assert spy == [(_norm(p), False, 5.0)]


def test_noaa_emit_reads_through_the_read_only_open(mods, tmp_path, monkeypatch, spy):
    m = mods["migrate_noaa_shard"]
    p = _cat(tmp_path)
    monkeypatch.setattr(m, "ROOT", str(tmp_path))
    monkeypatch.setattr(m, "OUT_DIR", str(tmp_path / "out"))
    m.emit()
    body = "".join(open(os.path.join(tmp_path / "out", f), encoding="utf-8").read()
                   for f in os.listdir(tmp_path / "out"))
    assert "noaa:s2" in body
    assert spy == [(_norm(p), False, 5.0)]


def test_rebuild_fts_plan_reads_the_checkout_or_the_planning_copy(mods, tmp_path, monkeypatch, spy):
    m = mods["rebuild_series_fts"]
    p = _cat(tmp_path / "a")
    monkeypatch.setattr(m, "ROOT", str(tmp_path / "a"))
    monkeypatch.setattr(m, "CATALOG", None)                          # no $AQUEDUCT_PLAN_CATALOG
    assert [r[0] for r in m.chunk_plan()] == ["noaa"]
    plan = _cat(tmp_path / "plan")
    monkeypatch.setattr(m, "CATALOG", str(plan))                     # $AQUEDUCT_PLAN_CATALOG set
    monkeypatch.setattr(m, "ROOT", str(tmp_path / "nowhere"))
    assert [r[0] for r in m.chunk_plan()] == ["noaa"]
    assert spy == [(_norm(p), False, 5.0), (_norm(plan), False, 5.0)]


def test_rebuild_fts_reads_the_override_at_import_as_before(monkeypatch, tmp_path):
    monkeypatch.setenv("AQUEDUCT_PLAN_CATALOG", str(tmp_path / "copy.db"))
    assert _load("rebuild_series_fts").CATALOG == str(tmp_path / "copy.db")
    monkeypatch.delenv("AQUEDUCT_PLAN_CATALOG")
    assert _load("rebuild_series_fts").CATALOG is None


def test_repair_sample_ids_reads_through_the_read_only_open(mods, tmp_path, monkeypatch, spy):
    m = mods["repair_stale_csvs"]
    p = _cat(tmp_path)
    monkeypatch.setattr(m, "ROOT", str(tmp_path))
    assert sorted(m._sample_ids("noaa", 5, 1)) == ["noaa:s0", "noaa:s1", "noaa:s2"]
    assert spy == [(_norm(p), False, 5.0)]


def test_resync_stale_sample_reads_through_the_read_only_open(mods, tmp_path, monkeypatch, spy):
    m = mods["resync_and_repair"]
    p = _cat(tmp_path)
    monkeypatch.setattr(m, "ROOT", str(tmp_path))
    from core import r2_util
    monkeypatch.setattr(r2_util, "client", lambda *a, **k: object())
    assert m.stale_sample("absent_source", 5, 1) == (0, 0, "")
    assert spy == [(_norm(p), False, 5.0)]


@pytest.mark.parametrize("name,fn,line", [("sync_titles_to_d1", "main", "con = _catalogue_ro()"),
                                          ("repair_stale_csvs", "main", "cat = _catalogue_ro()")])
def test_the_main_reads_use_the_helper(mods, name, fn, line):
    """These main()s reach D1 or R2 first, so they are not run here; their catalogue open is the helper (the
    catalog_path ratchet refuses any other spelling of the file in these modules)."""
    src = inspect.getsource(getattr(mods[name], fn))
    assert line in src
    assert all("plain-open:" in ln for ln in src.splitlines() if "sqlite3.connect(" in ln)
