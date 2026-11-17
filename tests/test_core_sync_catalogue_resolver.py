"""core/sync_catalog_d1.py, core/sync_state_d1.py and core/gen_denylist.py open the catalogue through core.catalog_path
(t0_ready legacy-catalogue, 2026-11-17).

Before T0: the same file as before - sync_catalog_d1 and sync_state_d1 follow $ECONDL_CATALOG (now asked of
econdl._catalog.default_db(), the exempt client that owns the override), else the checkout's catalogue - opened
READ-ONLY with the old timeouts: sync_catalog_d1 300 s (and PRAGMA busy_timeout = 300000 after it), sync_state_d1
and gen_denylist sqlite's own 5 s (no PRAGMA after them, so 5 s is pinned here). After T0: the build only.
Hermetic: temporary catalogues, D1 never reached."""
import os
import sqlite3
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from core import catalog_path, cutover, gen_denylist, sync_catalog_d1, sync_state_d1  # noqa: E402


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


def _cut(tmp_path, monkeypatch):
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    (tmp_path / "CUTOVER").write_text("")


def _catalogue(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(path)
    c.execute("CREATE TABLE license (license_id TEXT PRIMARY KEY, reservable INTEGER)")
    c.execute("CREATE TABLE source (source_id TEXT PRIMARY KEY, license_id TEXT)")
    c.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, start_date TEXT, "
              "end_date TEXT)")
    c.execute("INSERT INTO license VALUES ('open', 1), ('closed', 0)")
    c.execute("INSERT INTO source VALUES ('ecb', 'open'), ('shut', 'closed')")
    c.execute("INSERT INTO series VALUES ('ecb:a', 'ecb', 't', '2000-01-01', '2026-06-30')")
    c.commit()
    c.close()
    return path


def _state(path):
    s = sqlite3.connect(path)
    s.executescript("CREATE TABLE source_state (source_id TEXT PRIMARY KEY, cadence TEXT, status TEXT, "
                    "last_success_utc TEXT);"
                    "CREATE TABLE unit_state (source_id TEXT, unit_id TEXT, status TEXT, last_success_utc TEXT, "
                    "upstream_vintage TEXT, last_obs_date TEXT, obs_count INTEGER, PRIMARY KEY (source_id, unit_id));")
    s.execute("INSERT INTO source_state VALUES ('ecb', 'daily', 'ok', '2026-09-01T00:00:00+00:00')")
    s.execute("INSERT INTO unit_state VALUES ('ecb', '_all', 'ok', '2026-09-01T00:00:00+00:00', NULL, NULL, 1)")
    s.commit()
    s.close()
    return path


# ---- the override, resolved by the exempt client --------------------------------------------------------
def test_no_override_reads_the_root_s_catalogue(monkeypatch, tmp_path):
    monkeypatch.delenv("ECONDL_CATALOG", raising=False)
    assert sync_state_d1._default_catalogue(str(tmp_path)) == catalog_path.under(str(tmp_path))


def test_the_override_is_followed_before_t0(monkeypatch, tmp_path):
    monkeypatch.setenv("ECONDL_CATALOG", str(tmp_path / "copy.db"))
    assert sync_state_d1._default_catalogue(str(tmp_path / "x")) == str(tmp_path / "copy.db")


def test_after_t0_a_foreign_override_is_refused(monkeypatch, tmp_path):
    monkeypatch.setenv("ECONDL_CATALOG", str(tmp_path / "copy.db"))
    _cut(tmp_path, monkeypatch)
    from econdl import _catalog
    monkeypatch.setattr(_catalog, "_cut_over", lambda: True)
    with pytest.raises(RuntimeError, match="refused"):
        sync_state_d1._default_catalogue(str(tmp_path))


# ---- sync_state_d1.emit_sql ------------------------------------------------------------------------------
def test_emit_sql_reads_the_named_catalogue_read_only_with_5s(tmp_path, spy):
    cat = _catalogue(tmp_path / "c" / "catalog.db")
    st = _state(tmp_path / "state.db")
    (tmp_path / "out").mkdir()
    _f, counts = sync_state_d1.emit_sql(str(st), str(tmp_path / "out"), gated=set(), catalogue=str(cat))
    assert counts.get("source_data_through", 0) == 1
    assert spy == [(_norm(cat), False, 5.0)]


def test_emit_sql_default_follows_the_override(tmp_path, monkeypatch, spy):
    cat = _catalogue(tmp_path / "c" / "catalog.db")
    st = _state(tmp_path / "state.db")
    (tmp_path / "out").mkdir()
    monkeypatch.setenv("ECONDL_CATALOG", str(cat))
    _f, counts = sync_state_d1.emit_sql(str(st), str(tmp_path / "out"), gated=set())
    assert counts.get("source_data_through", 0) == 1 and spy == [(_norm(cat), False, 5.0)]


def test_emit_sql_a_missing_catalogue_is_still_skipped(tmp_path, monkeypatch, spy, capsys):
    st = _state(tmp_path / "state.db")
    (tmp_path / "out").mkdir()
    monkeypatch.setenv("ECONDL_CATALOG", str(tmp_path / "none.db"))
    _f, counts = sync_state_d1.emit_sql(str(st), str(tmp_path / "out"), gated=set())
    assert "source_data_through" not in counts and spy == []
    assert "data_through SKIPPED" in capsys.readouterr().out
    assert not (tmp_path / "none.db").exists()


def test_emit_sql_a_directory_catalogue_raises_sqlite_s_error(tmp_path):
    """C277-2: a path that exists but is not a file passes the exists check; the open must fail with
    sqlite3.OperationalError (the class the old mode=ro open raised), not FileNotFoundError."""
    st = _state(tmp_path / "state.db")
    (tmp_path / "out").mkdir()
    (tmp_path / "dir.db").mkdir()
    with pytest.raises(sqlite3.OperationalError, match="unable to open database file"):
        sync_state_d1.emit_sql(str(st), str(tmp_path / "out"), gated=set(), catalogue=str(tmp_path / "dir.db"))


def test_emit_sql_without_data_through_never_resolves_the_catalogue(tmp_path, monkeypatch, spy):
    """The self-hosted origin calls emit_sql(data_through=False) after T0: no override rule may stop it."""
    st = _state(tmp_path / "state.db")
    (tmp_path / "out").mkdir()
    monkeypatch.setattr(sync_state_d1, "_default_catalogue", lambda root: pytest.fail("catalogue resolved"))
    sync_state_d1.emit_sql(str(st), str(tmp_path / "out"), gated=set(), data_through=False)
    assert spy == []


def test_emit_sql_after_t0_a_worktree_catalogue_is_refused(tmp_path, monkeypatch):
    cat = _catalogue(tmp_path / "c" / "catalog.db")
    st = _state(tmp_path / "state.db")
    (tmp_path / "out").mkdir()
    _cut(tmp_path, monkeypatch)
    with pytest.raises(cutover.CutoverRefused):
        sync_state_d1.emit_sql(str(st), str(tmp_path / "out"), gated=set(), catalogue=str(cat))


# ---- sync_catalog_d1.main --------------------------------------------------------------------------------
@pytest.fixture
def sync(tmp_path, monkeypatch):
    pending = tmp_path / "pending.txt"
    pending.write_text("", encoding="utf-8")
    monkeypatch.setattr(sync_catalog_d1, "PENDING", str(pending))
    monkeypatch.setattr(sync_catalog_d1, "_manifest_path", lambda root: str(tmp_path / "sent.db"))
    monkeypatch.setattr(sync_catalog_d1, "_gated_ids", lambda: set())
    monkeypatch.setattr(sync_catalog_d1, "execute_remote", lambda *a, **k: pytest.fail("D1 reached"))
    return tmp_path


def test_sync_main_reads_the_attribute_read_only_with_300s(sync, monkeypatch, spy):
    cat = _catalogue(sync / "c" / "catalog.db")
    monkeypatch.setattr(sync_catalog_d1, "CATALOGUE", str(cat))
    sync_catalog_d1.main(["--dry-run"])
    assert spy == [(_norm(cat), False, 300.0)]


def test_sync_main_default_follows_the_override(sync, monkeypatch, spy):
    cat = _catalogue(sync / "c" / "catalog.db")
    monkeypatch.setattr(sync_catalog_d1, "CATALOGUE", None)
    monkeypatch.setenv("ECONDL_CATALOG", str(cat))
    sync_catalog_d1.main(["--dry-run"])
    assert spy == [(_norm(cat), False, 300.0)]


def test_sync_main_default_without_override_is_root_s(sync, monkeypatch, spy):
    _catalogue(sync / "data" / "catalog.db")
    monkeypatch.setattr(sync_catalog_d1, "CATALOGUE", None)
    monkeypatch.delenv("ECONDL_CATALOG", raising=False)
    monkeypatch.setattr(sync_catalog_d1, "ROOT", str(sync))
    sync_catalog_d1.main(["--dry-run"])
    assert spy == [(_norm(sync / "data" / "catalog.db"), False, 300.0)]


def test_sync_main_a_missing_catalogue_is_sqlite_s_error(sync, monkeypatch):
    monkeypatch.setattr(sync_catalog_d1, "CATALOGUE", str(sync / "none.db"))
    with pytest.raises(sqlite3.OperationalError):
        sync_catalog_d1.main(["--dry-run"])
    assert not (sync / "none.db").exists()


def test_importing_sync_catalog_d1_resolves_nothing():
    """It was read at import; now the module default is None and the override is asked for at the open."""
    import inspect
    src = inspect.getsource(sync_catalog_d1)
    assert 'CATALOGUE: str | None = None' in src and src.count('_default_catalogue(ROOT)') == 1


# ---- gen_denylist ------------------------------------------------------------------------------------------
def test_gen_denylist_reads_db_read_only_with_5s(tmp_path, monkeypatch, spy):
    cat = _catalogue(tmp_path / "catalog.db")
    monkeypatch.setattr(gen_denylist, "DB", str(cat))
    con = gen_denylist._catalogue_ro()
    try:
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            con.execute("DELETE FROM source")
    finally:
        con.close()
    assert spy == [(_norm(cat), False, 5.0)]


def test_gen_denylist_default_is_the_root_s_catalogue(tmp_path, monkeypatch, spy):
    cat = _catalogue(tmp_path / "data" / "catalog.db")
    monkeypatch.setattr(gen_denylist, "DB", None)
    monkeypatch.setattr(gen_denylist, "ROOT", str(tmp_path))
    gen_denylist._catalogue_ro().close()
    assert spy == [(_norm(cat), False, 5.0)]


def test_gen_denylist_a_missing_catalogue_is_sqlite_s_error_and_nothing_is_created(tmp_path, monkeypatch):
    monkeypatch.setattr(gen_denylist, "DB", str(tmp_path / "none.db"))
    with pytest.raises(sqlite3.OperationalError):
        gen_denylist._catalogue_ro()
    assert not (tmp_path / "none.db").exists()


def test_gen_denylist_after_t0_a_worktree_catalogue_is_refused(tmp_path, monkeypatch):
    cat = _catalogue(tmp_path / "catalog.db")
    monkeypatch.setattr(gen_denylist, "DB", str(cat))
    _cut(tmp_path, monkeypatch)
    with pytest.raises(cutover.CutoverRefused):
        gen_denylist._catalogue_ro()


def test_gen_denylist_main_goes_through_the_helper():
    import inspect
    src = inspect.getsource(gen_denylist.main)
    assert "c = _catalogue_ro()" in src and "sqlite3.connect" not in src
