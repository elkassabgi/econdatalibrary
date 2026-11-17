"""The updater's catalogue opens go through core.catalog_path (t0_ready legacy-catalogue, 2026-11-16):
updater/orchestrate.py (six read-only opens), updater/strategies/fetchers/worldbank.py and core/derive_csv.py.

Before T0 each reads the same file as before - $ECONDL_CATALOG when set, else <config.ROOT>/data/catalog.db
(orchestrate, worldbank) or the checkout's CATALOG (derive_csv) - with the same exception class on a missing
file. After T0 they read the build only: a worktree's own catalogue is refused, never opened."""
import os
import sqlite3
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import core.derive_csv as dc  # noqa: E402 - puts clients/python on sys.path
from core import catalog_path, cutover  # noqa: E402
from econdl import _catalog as econdl_catalog  # noqa: E402
from updater import config, orchestrate as orch  # noqa: E402
from updater.errors import TransientError  # noqa: E402
from updater.strategies.fetchers import worldbank as wb  # noqa: E402


def _cat(path, ids):
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as c:
        c.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
        c.executemany("INSERT INTO series VALUES (?, ?)", [(i, i.split(":")[0]) for i in ids])
    c.close()
    return str(path)


@pytest.fixture
def world(tmp_path, monkeypatch):
    """config.ROOT relocated to a worktree with its own catalogue; the build is another file."""
    wt = tmp_path / "worktree"
    _cat(wt / "data" / "catalog.db", ["worldbank:NY.GDP", "ecb:worktree"])
    build = _cat(tmp_path / "live" / "data" / "catalog.db", ["worldbank:BUILD", "ecb:build"])
    monkeypatch.setattr(config, "ROOT", str(wt))
    monkeypatch.setattr(catalog_path, "BUILD_PATH", build)
    monkeypatch.setattr(econdl_catalog, "_BUILD_DB", build)
    monkeypatch.delenv("ECONDL_CATALOG", raising=False)
    flag = tmp_path / "CUTOVER"
    monkeypatch.setattr(cutover, "FLAG_PATH", str(flag))
    monkeypatch.setattr(econdl_catalog, "_CUTOVER_FLAG", str(flag))

    def cut():
        flag.write_text("")
    world_ = {"wt": wt, "build": build, "cut": cut, "tmp": tmp_path}
    return world_


def _ids(con):
    try:
        return sorted(r[0] for r in con.execute("SELECT series_id FROM series"))
    finally:
        con.close()


# ---- orchestrate ----------------------------------------------------------------------------------------
def test_orchestrate_before_t0_reads_config_root_s_catalogue(world):
    assert orch._catalogue_file() == catalog_path.under(config.ROOT)
    assert _ids(orch._catalogue_ro()) == ["ecb:worktree", "worldbank:NY.GDP"]


def test_orchestrate_before_t0_follows_the_override(world, monkeypatch):
    copy = _cat(world["tmp"] / "copy.db", ["ecb:copy"])
    monkeypatch.setenv("ECONDL_CATALOG", copy)
    assert _ids(orch._catalogue_ro()) == ["ecb:copy"]


def test_orchestrate_connections_are_read_only(world):
    con = orch._catalogue_ro()
    with pytest.raises(sqlite3.OperationalError, match="readonly"):
        con.execute("INSERT INTO series VALUES ('x:y', 'x')")
    con.close()


def test_orchestrate_a_missing_catalogue_raises_sqlite_s_error_as_before(world, monkeypatch):
    monkeypatch.setattr(config, "ROOT", str(world["tmp"] / "nowhere"))
    with pytest.raises(sqlite3.OperationalError):
        orch._catalogue_ro()
    assert not (world["tmp"] / "nowhere").exists(), "never created"
    # the callers that answered "unreadable" still do (-1 = cannot tell, fail closed)
    assert orch._catalog_series_count("ecb") == -1


def test_orchestrate_after_t0_reads_the_build_not_the_worktree(world):
    world["cut"]()
    assert _ids(orch._catalogue_ro()) == ["ecb:build", "worldbank:BUILD"]


def test_orchestrate_after_t0_refuses_an_override_naming_another_file(world, monkeypatch):
    monkeypatch.setenv("ECONDL_CATALOG", _cat(world["tmp"] / "copy.db", ["ecb:copy"]))
    world["cut"]()
    with pytest.raises(RuntimeError, match="refused"):
        orch._catalogue_ro()


def test_orchestrate_mapping_reads_through_the_resolver(world):
    """_catalog_ids_for, the derive mapping: exact ids come from the file the resolver names."""
    ids, _unmapped = orch._catalog_ids_for("ecb", ["worktree"])
    assert "ecb:worktree" in ids
    world["cut"]()
    ids, _unmapped = orch._catalog_ids_for("ecb", ["build"])
    assert "ecb:build" in ids


def _spy(monkeypatch):
    seen = []
    real = catalog_path.connect_path

    def spy(path, *, write, **kw):
        seen.append((write, kw.get("timeout")))
        return real(path, write=write, **kw)
    monkeypatch.setattr(catalog_path, "connect_path", spy)
    return seen


def test_the_opens_are_read_only_with_sqlite_s_own_timeout(world, monkeypatch):
    """Review of PR3 findings 1-2: write=True or another busy timeout survived every other test. Both opens
    are read-only with sqlite's 5 s default, as the plain opens were (a write=True open after T0 without the
    writer lock would be REFUSED and break the worldbank fetch)."""
    seen = _spy(monkeypatch)
    orch._catalogue_ro().close()
    wb._published_keys()
    # review C271-1: derive_csv._catalog_ids has no PRAGMA after its open, so 5 s IS its busy timeout
    monkeypatch.setattr(dc, "CATALOG", str(world["wt"] / "data" / "catalog.db"))
    dc._catalog_ids(None, ["ecb"])
    assert seen == [(False, 5.0), (False, 5.0), (False, 5.0)], seen


# ---- worldbank fetcher ---------------------------------------------------------------------------------
def test_worldbank_before_t0_reads_config_root_s_catalogue(world):
    assert wb._published_keys() == {"NY.GDP"}


def test_worldbank_a_missing_catalogue_is_transient(world, monkeypatch):
    monkeypatch.setattr(config, "ROOT", str(world["tmp"] / "nowhere"))
    with pytest.raises(TransientError, match="not present"):
        wb._published_keys()


def test_worldbank_after_t0_a_worktree_catalogue_is_refused(world):
    world["cut"]()
    with pytest.raises(cutover.CutoverRefused):
        wb._published_keys()


# ---- derive_csv ------------------------------------------------------------------------------------------
def test_derive_after_t0_reads_the_build_not_the_checkout_s_copy(world, monkeypatch):
    checkout = _cat(world["tmp"] / "checkout" / "data" / "catalog.db", [("ecb:checkout")])
    monkeypatch.setattr(dc, "CATALOG", checkout)
    assert [i for i, _s in dc._catalog_ids(None, ["ecb"])] == ["ecb:checkout"], "before T0: CATALOG"
    world["cut"]()
    assert [i for i, _s in dc._catalog_ids(None, ["ecb"])] == ["ecb:build"]


def test_derive_catalog_constant_is_the_resolver_s_name_for_its_root():
    assert dc.CATALOG == catalog_path.under(dc.ROOT) == os.path.join(dc.ROOT, "data", "catalog.db")
