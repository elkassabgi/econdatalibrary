"""The four updater fetchers that read the catalogue (_faostat, _imf_mapped, unesco_dem, _who_base) read
<config.ROOT>/data/catalog.db through core.catalog_path, READ-ONLY (plan step 1).

Pinned here because the fetchers' own tests never reach these lines (review of the catalogue branch,
R1189): a wrong folder or a read-write open passed all 507 of them. An unreadable catalogue is a
TransientError in all four - never an empty or partial answer - and the two fetchers whose self-check
compares against it (_faostat, _imf_mapped) must never reach the merge without it (R1190: the first
version of these tests faked the failure at connect, where a real hot journal does not fail, and never
ran run())."""
import io
import os
import shutil
import sqlite3
import types
import zipfile

import pytest

from core import catalog_path as cp
from core import cutover
from updater import config
from updater.errors import TransientError
from updater.strategies.fetchers import _faostat, _imf_mapped, _who_base, unesco_dem

ROWS = [
    ("imf_x:IMFX:A.US", "imf_x"),
    ("imf_x:IMFX:A.FR", "imf_x"),
    ("fao_qcl:FAO_QCL:4.15", "fao_qcl"),
    ("unesco_dem:DEM:CR.1.AFG.2020", "unesco_dem"),
    ("unesco_dem:DEM:ROFST.2.BRA.2019", "unesco_dem"),
    ("who_gho:GHO:WHOSIS_1.AFG", "who_gho"),
    ("who_gho:GHO:MDG_2.BRA.F", "who_gho"),
    ("who_gho:OTHER:NOPE.X", "who_gho"),
    ("elsewhere:E:Z.Z", "elsewhere"),
]


def _write_catalogue(path, rows=ROWS):
    with sqlite3.connect(path) as c:
        c.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
        c.executemany("INSERT INTO series VALUES (?, ?)", rows)
    c.close()


def _hot_catalogue(dest_dir):
    """A REAL crashed-writer state: the catalogue and its rollback journal copied mid-transaction (valid
    magic, pages dirtied). A read-only open succeeds; the first read fails, because it cannot roll back."""
    work = os.path.join(dest_dir, "_hot_src")
    os.makedirs(work)
    src = os.path.join(work, "src.db")
    c = sqlite3.connect(src, isolation_level=None)
    c.execute("PRAGMA journal_mode=DELETE")
    c.execute("PRAGMA cache_size=5")
    c.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    c.execute("BEGIN")
    c.executemany("INSERT INTO series VALUES (?, ?)", ROWS + [(f"pad:P:{i}", "pad") for i in range(20000)])
    c.execute("COMMIT")
    c.execute("BEGIN")
    c.execute("UPDATE series SET source_id = source_id || ''")
    c.executemany("INSERT INTO series VALUES (?, ?)", [(f"more:M:{i}", "more") for i in range(20000)])
    dst = os.path.join(dest_dir, "data", "catalog.db")
    shutil.copyfile(src, dst)
    shutil.copyfile(src + "-journal", dst + "-journal")
    c.execute("ROLLBACK")
    c.close()
    with open(dst + "-journal", "rb") as f:
        assert f.read(8) == cp.JOURNAL_MAGIC, "the planted journal is not hot - the test would prove nothing"
    return dst


@pytest.fixture
def root(tmp_path, monkeypatch):
    """A checkout at tmp_path with its own data/catalog.db; before T0; EVERY sqlite3.connect recorded."""
    (tmp_path / "data").mkdir()
    _write_catalogue(tmp_path / "data" / "catalog.db")
    monkeypatch.setattr(config, "ROOT", str(tmp_path))
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    monkeypatch.setattr(cp, "BUILD_PATH", str(tmp_path / "live" / "catalog.db"))
    opened = []
    real = sqlite3.connect

    def spy(database, *a, **kw):
        opened.append(str(database))
        return real(database, *a, **kw)

    monkeypatch.setattr(sqlite3, "connect", spy)          # the module object catalog_path uses too
    return types.SimpleNamespace(path=tmp_path, opened=opened)


READERS = [
    ("_imf_mapped", lambda: _imf_mapped._catalog_ids("imf_x"), {"IMFX:A.US", "IMFX:A.FR"}),
    ("_faostat", lambda: _faostat._catalog_ids("fao_qcl"), {"FAO_QCL:4.15"}),
    ("unesco_dem ids", lambda: unesco_dem._catalog_ids("unesco_dem"), {"DEM:CR.1.AFG.2020", "DEM:ROFST.2.BRA.2019"}),
    ("unesco_dem indicators", lambda: unesco_dem._published_indicators("unesco_dem"), {"CR.1", "ROFST.2"}),
    ("_who_base", lambda: set(_who_base._published_indicators("who_gho", "GHO")), {"WHOSIS_1", "MDG_2"}),
]
IDS = [r[0] for r in READERS]


@pytest.mark.parametrize("name,read,want", READERS, ids=IDS)
def test_each_fetcher_reads_its_own_checkout_s_catalogue_once_read_only(root, name, read, want):
    assert read() == want
    uri = cp.pathlib.Path(cp.under(root.path)).resolve().as_uri() + "?mode=ro"
    assert root.opened == [uri], f"{name}: exactly one open, read-only, of <config.ROOT>/data/catalog.db: {root.opened}"


@pytest.mark.parametrize("name,read,want", READERS, ids=IDS)
def test_after_t0_a_checkout_s_catalogue_is_refused(root, name, read, want):
    (root.path / "CUTOVER").write_text("")
    with pytest.raises(cutover.CutoverRefused):
        read()


def test_a_readable_catalogue_without_the_source_is_an_empty_set(root):
    """A new source: nothing published yet, so the self-check has nothing to compare - not an error."""
    assert _imf_mapped._catalog_ids("imf_new") == set()
    assert _faostat._catalog_ids("fao_new") == set()


@pytest.mark.parametrize("name,read,want", READERS, ids=IDS)
def test_a_garbage_or_missing_catalogue_is_transient(root, name, read, want):
    (root.path / "data" / "catalog.db").write_bytes(b"not a database" * 100)
    with pytest.raises(TransientError, match="catalogue unreadable|no catalogue"):
        read()
    (root.path / "data" / "catalog.db").unlink()
    with pytest.raises(TransientError, match="catalogue unreadable|no catalogue"):
        read()


@pytest.mark.parametrize("name,read,want", READERS, ids=IDS)
def test_a_real_hot_journal_is_transient(root, name, read, want):
    (root.path / "data" / "catalog.db").unlink()
    _hot_catalogue(str(root.path))
    with pytest.raises(TransientError, match="catalogue unreadable"):
        read()


# ---- run(): an unreadable catalogue never reaches the merge ---------------------------------------------------
class _Merged(Exception):
    """Raised by the stub merge: the fetcher got past its self-check and tried to write."""


def _stub_common(mod, monkeypatch, tmp):
    monkeypatch.setattr(mod.config, "source_dir", lambda s: str(tmp / "store" / s))
    monkeypatch.setattr(mod.blob, "exists", lambda p: False)

    def merge_and_write(*a, **kw):
        raise _Merged()
    monkeypatch.setattr(mod.merge, "merge_and_write", merge_and_write)


def _stub_imf(monkeypatch, tmp):
    _stub_common(_imf_mapped, monkeypatch, tmp)
    monkeypatch.setattr(_imf_mapped, "load", lambda s: {
        "flow": "F", "agency": "IMF", "slots": {"FREQ": 0, "REF_AREA": 1}, "code_maps": {},
        "date_convention": "start", "arity": 2, "key_prefix": "IMFX"})
    xml = (b'<M><Series FREQ="A" REF_AREA="US"><Obs TIME_PERIOD="2020" OBS_VALUE="1.5"/></Series>'
           b'<Series FREQ="A" REF_AREA="FR"><Obs TIME_PERIOD="2020" OBS_VALUE="2.5"/></Series></M>')
    monkeypatch.setattr(_imf_mapped.ing, "http_get", lambda url: xml)
    return lambda: _imf_mapped.run("imf_x")


def _stub_fao(monkeypatch, tmp):
    _stub_common(_faostat, monkeypatch, tmp)
    monkeypatch.setattr(_faostat, "load", lambda s: {"code": "QCL", "key_columns": ["Area Code", "Item Code"],
                                                     "key_prefix": "FAO_QCL"})
    monkeypatch.setattr(_faostat, "_entry", lambda code: {"FileLocation": "stub"})
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("QCL_E_All_Data_(Normalized).csv", "Area Code,Item Code,Year Code,Value\n4,15,2020,7.0\n")
    monkeypatch.setattr(_faostat, "_get", lambda url, timeout=900: buf.getvalue())
    return lambda: _faostat.run("fao_qcl")


def _stub_unesco(monkeypatch, tmp, spoil=None):
    """unesco_dem.update() reads the catalogue TWICE: the indicator list first, then its id self-check
    after the fetch. `spoil(cat)` runs inside the fetch, so the self-check meets a catalogue that the
    first read found healthy (R1192 finding 3)."""
    _stub_common(unesco_dem, monkeypatch, tmp)

    def get(url, timeout=300):
        if spoil:
            spoil(tmp / "data" / "catalog.db")
        return {"records": [{"value": 1.0, "year": 2020, "geoUnit": "AFG", "indicatorId": "CR.1"}]}
    monkeypatch.setattr(unesco_dem, "_get", get)
    return lambda: unesco_dem.update(None, None)


def _garbage(cat):
    cat.write_bytes(b"not a database" * 100)


RUNS = [("_imf_mapped", _stub_imf), ("_faostat", _stub_fao)]


def test_unesco_update_passes_its_self_check_on_a_readable_catalogue(root, monkeypatch):
    """Positive control for the test below: the same stubs get PAST the self-check read."""
    seen = []
    real = unesco_dem._catalog_ids
    monkeypatch.setattr(unesco_dem, "_catalog_ids", lambda s: seen.append(s) or real(s))
    run = _stub_unesco(monkeypatch, root.path)
    from updater.errors import DefinitiveError
    try:
        run()
    except (_Merged, DefinitiveError):     # merged, or the self-check refused these test ids: both got past the read
        pass
    assert seen == ["unesco_dem"]


def test_unesco_update_refuses_when_its_self_check_cannot_read(root, monkeypatch):
    run = _stub_unesco(monkeypatch, root.path, spoil=_garbage)
    with pytest.raises(TransientError, match="catalogue unreadable"):
        run()


@pytest.mark.parametrize("name,stub", RUNS, ids=[r[0] for r in RUNS])
def test_run_reaches_the_merge_when_the_catalogue_is_readable(root, monkeypatch, name, stub):
    """The positive control: with these stubs a readable catalogue DOES reach the merge, so the refusals
    below are the self-check's doing and not a stub failing earlier."""
    run = stub(monkeypatch, root.path)
    with pytest.raises(_Merged):
        run()


@pytest.mark.parametrize("name,stub", RUNS, ids=[r[0] for r in RUNS])
@pytest.mark.parametrize("state", ["garbage", "missing", "hot"])
def test_run_never_merges_without_the_self_check(root, monkeypatch, name, stub, state):
    run = stub(monkeypatch, root.path)
    cat = root.path / "data" / "catalog.db"
    if state == "garbage":
        cat.write_bytes(b"not a database" * 100)
    else:
        cat.unlink()
        if state == "hot":
            _hot_catalogue(str(root.path))
    with pytest.raises(TransientError, match="catalogue unreadable"):
        run()
