"""The four updater fetchers that read the catalogue (_faostat, _imf_mapped, unesco_dem, _who_base) read
<config.ROOT>/data/catalog.db through core.catalog_path, READ-ONLY (plan step 1).

Pinned here because the fetchers' own tests never reach these lines (review of the catalogue branch,
R1189): a wrong folder or a read-write open passed all 507 of them. The fetchers whose self-check compares
against the catalogue (_faostat, _imf_mapped) must REFUSE when it cannot be read - an empty set there
skips the check and merges unchecked."""
import sqlite3
import types

import pytest

from core import catalog_path as cp
from core import cutover
from updater import config
from updater.errors import TransientError
from updater.strategies.fetchers import _faostat, _imf_mapped, _who_base, unesco_dem

ROWS = [
    ("imf_x:IMFX:A.B", "imf_x"),
    ("imf_x:IMFX:C.D", "imf_x"),
    ("fao_qcl:FAO_QCL:5111.1.1016", "fao_qcl"),
    ("unesco_dem:DEM:CR.1.AFG.2020", "unesco_dem"),
    ("unesco_dem:DEM:ROFST.2.BRA.2019", "unesco_dem"),
    ("who_gho:GHO:WHOSIS_1.AFG", "who_gho"),
    ("who_gho:GHO:MDG_2.BRA.F", "who_gho"),
    ("who_gho:OTHER:NOPE.X", "who_gho"),
    ("elsewhere:E:Z.Z", "elsewhere"),
]


@pytest.fixture
def root(tmp_path, monkeypatch):
    """A checkout at tmp_path with its own data/catalog.db; before T0; every open recorded."""
    (tmp_path / "data").mkdir()
    with sqlite3.connect(tmp_path / "data" / "catalog.db") as c:
        c.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
        c.executemany("INSERT INTO series VALUES (?, ?)", ROWS)
    c.close()
    monkeypatch.setattr(config, "ROOT", str(tmp_path))
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    monkeypatch.setattr(cp, "BUILD_PATH", str(tmp_path / "live" / "catalog.db"))
    opened = []
    real = cp.connect_path

    def spy(path, *, write, **kw):
        opened.append((str(path), write))
        return real(path, write=write, **kw)

    monkeypatch.setattr(cp, "connect_path", spy)
    return types.SimpleNamespace(path=tmp_path, opened=opened)


READERS = [
    ("_imf_mapped", lambda: _imf_mapped._catalog_ids("imf_x"), {"IMFX:A.B", "IMFX:C.D"}),
    ("_faostat", lambda: _faostat._catalog_ids("fao_qcl"), {"FAO_QCL:5111.1.1016"}),
    ("unesco_dem ids", lambda: unesco_dem._catalog_ids("unesco_dem"), {"DEM:CR.1.AFG.2020", "DEM:ROFST.2.BRA.2019"}),
    ("unesco_dem indicators", lambda: unesco_dem._published_indicators("unesco_dem"), {"CR.1", "ROFST.2"}),
    ("_who_base", lambda: set(_who_base._published_indicators("who_gho", "GHO")), {"WHOSIS_1", "MDG_2"}),
]


@pytest.mark.parametrize("name,read,want", READERS, ids=[r[0] for r in READERS])
def test_each_fetcher_reads_its_own_checkout_s_catalogue_read_only(root, name, read, want):
    assert read() == want
    assert root.opened == [(cp.under(root.path), False)], f"{name}: opened {root.opened}"


@pytest.mark.parametrize("name,read,want", READERS, ids=[r[0] for r in READERS])
def test_after_t0_a_checkout_s_catalogue_is_refused(root, name, read, want):
    (root.path / "CUTOVER").write_text("")
    with pytest.raises((cutover.CutoverRefused, TransientError)):
        read()


def test_a_readable_catalogue_without_the_source_is_an_empty_set(root):
    """A new source: nothing published yet, so the self-check has nothing to compare - not an error."""
    assert _imf_mapped._catalog_ids("imf_new") == set()
    assert _faostat._catalog_ids("fao_new") == set()


@pytest.mark.parametrize("mod,src", [(_imf_mapped, "imf_x"), (_faostat, "fao_qcl")])
def test_an_unreadable_catalogue_refuses_never_an_empty_set(root, mod, src):
    (root.path / "data" / "catalog.db").write_bytes(b"not a database" * 100)
    with pytest.raises(TransientError, match="catalogue unreadable"):
        mod._catalog_ids(src)
    (root.path / "data" / "catalog.db").unlink()
    with pytest.raises(TransientError, match="catalogue unreadable"):
        mod._catalog_ids(src)


@pytest.mark.parametrize("mod,src", [(_imf_mapped, "imf_x"), (_faostat, "fao_qcl")])
def test_a_hot_journal_refuses(root, monkeypatch, mod, src):
    """The reviewer's case: a crashed writer's journal. A read-only open cannot roll it back, so the read
    fails - and the fetcher must refuse rather than skip its self-check."""
    def hot(path, *, write, **kw):
        raise sqlite3.OperationalError("attempt to write a readonly database (hot journal)")

    monkeypatch.setattr(cp, "connect_path", hot)
    with pytest.raises(TransientError, match="catalogue unreadable"):
        mod._catalog_ids(src)
