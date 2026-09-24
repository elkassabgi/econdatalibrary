"""tools/verify_source_served.py after T0 (plan step 6d): the store is the self-hosted one (R2 is a frozen copy and is
not asked), the third leg is what the EDGE serves (/v1/catalog total - D1 is frozen), there is no mirror to check,
and only the live checkout may answer. The two network probes are stubbed; the rest runs for real."""
import os
import sqlite3
import sys

import pytest

from core import catalog_path, cutover, r2_util
from updater import blob
from updater import config as updater_config

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools", "selfhost"))
from tools import verify_source_served as V  # noqa: E402
from blobstore import BlobStore  # noqa: E402


def _csv(sid):
    return f"series_id,obs_date,value\n{sid},2024-01-01,1\n".encode()


@pytest.fixture
def live(tmp_path, monkeypatch):
    root = tmp_path / "live"
    (root / "data").mkdir(parents=True)
    build = root / "data" / "catalog.db"
    with sqlite3.connect(build) as c:
        c.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
        c.executemany("INSERT INTO series VALUES (?, 'zz')", [("zz:a",), ("zz:b",)])
    c.close()
    BlobStore(str(tmp_path / "blobs"), create=True)
    monkeypatch.setattr(blob, "SELFHOST_BLOB_ROOT", str(tmp_path / "blobs"))
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    monkeypatch.setattr(catalog_path, "LIVE_STORE_ROOT", str(root))
    monkeypatch.setattr(catalog_path, "BUILD_PATH", str(build))
    monkeypatch.setattr(catalog_path, "LOCK_PATH", str(tmp_path / "lock" / "writer.lock"))
    monkeypatch.setattr(blob, "_code_root", lambda: str(root))
    monkeypatch.setattr(updater_config, "ROOT", str(root))
    monkeypatch.setattr(updater_config, "DATA_ROOT", str(root / "data" / "clean_full"))
    monkeypatch.delenv("ECONDL_DATA", raising=False)
    monkeypatch.delenv("ECONDL_CATALOG", raising=False)
    monkeypatch.setattr(r2_util, "client", lambda *a, **k: pytest.fail("R2 asked after T0"))
    monkeypatch.setattr(V, "_d1_count", lambda s: pytest.fail("D1 asked after T0"))
    served = {"n": 2}
    monkeypatch.setattr(V, "_served_count", lambda s: (served["n"], None))
    monkeypatch.setattr(V, "_listed_live", lambda s: True)
    import core.derive_csv as dc
    monkeypatch.setattr(dc, "_series_csv_bytes", _csv)
    monkeypatch.setattr(dc, "_mirror_behind_store", lambda *a, **k: pytest.fail("mirror checked after T0"))
    store = blob.SelfhostBlob()
    with catalog_path.writer_lock():
        for sid in ("zz:a", "zz:b"):
            store.put_atomic("series/" + sid.replace(":", "%3A") + ".csv", _csv(sid))
    (tmp_path / "CUTOVER").write_text("")
    return tmp_path, store, served


def _run(monkeypatch, *extra):
    monkeypatch.setattr(sys, "argv", ["verify_source_served.py", "--source", "zz", *extra])
    return V.main()


def test_after_t0_a_coherent_source_is_served(live, monkeypatch, capsys):
    assert _run(monkeypatch) == 0
    out = capsys.readouterr().out
    assert "store objects  : 2" in out and "R2 objects" not in out
    assert "SERVED (edge) : 2 row(s)" in out and "2/2 identical" in out
    assert "SERVED — MISSING 0" in out and "the served catalogue in step" in out


def test_after_t0_a_missing_object_is_not_clean(live, monkeypatch, capsys):
    tmp, store, served = live
    with catalog_path.writer_lock():
        store.delete("series/zz%3Ab.csv")
    assert _run(monkeypatch) == 1
    out = capsys.readouterr().out
    assert "MISSING  (catalogued, no object): 1" in out and "NOT CLEAN" in out


def test_after_t0_a_served_catalogue_behind_names_the_swap(live, monkeypatch, capsys):
    tmp, store, served = live
    served["n"] = 1
    assert _run(monkeypatch) == 1
    out = capsys.readouterr().out
    assert "CATALOGUED BUT NOT SERVED" in out and "the next swap publishes it" in out


def test_after_t0_a_changed_byte_is_a_mismatch(live, monkeypatch, capsys):
    tmp, store, served = live
    with catalog_path.writer_lock():
        store.put_atomic("series/zz%3Aa.csv", b"series_id,obs_date,value\nzz:a,2024-01-01,999\n")
    assert _run(monkeypatch) == 1
    assert "MISMATCH zz:a" in capsys.readouterr().out


def test_after_t0_another_checkout_is_refused(live, monkeypatch, tmp_path):
    monkeypatch.setattr(blob, "_code_root", lambda: str(tmp_path / "a_worktree"))
    import core.catalog_path as cpm
    monkeypatch.setattr(cpm, "connect", lambda *a, **k: pytest.fail("the catalogue was opened before refusing"))
    with pytest.raises(cutover.CutoverRefused, match="R1203"):
        _run(monkeypatch)


def test_after_t0_a_listed_object_that_vanishes_is_not_served(live, monkeypatch, capsys):
    """R1242 mutant M10 (the VANISHED branch removed) survived."""
    tmp, store, served = live
    real = blob.SelfhostBlob.get
    monkeypatch.setattr(blob.SelfhostBlob, "get", lambda self, k: None if k == "series/zz%3Ab.csv" else real(self, k))
    assert _run(monkeypatch, "--sample", "2") == 1
    assert "VANISHED zz:b" in capsys.readouterr().out


def test_after_t0_a_sibling_sources_objects_are_not_this_sources(live, monkeypatch, capsys):
    """R1242 mutant M12 (the listing without the colon-anchored prefix) survived."""
    tmp, store, served = live
    with catalog_path.writer_lock():
        store.put_atomic("series/zzz%3Aother.csv", _csv("zzz:other"))
    assert _run(monkeypatch) == 0
    out = capsys.readouterr().out
    assert "store objects  : 2" in out and "ORPHANED (object, no catalogue row): 0" in out


def test_after_t0_carved_series_are_not_expected_to_be_served(live, monkeypatch, capsys):
    """R1242: the edge's total leaves carve-outs out; the expectation must too, or the source reads 'not served'
    for ever."""
    tmp, store, served = live
    from core import gen_denylist
    monkeypatch.setattr(gen_denylist, "committed_carveouts", lambda *a, **k: {"zz": ["b"]})
    served["n"] = 1                                           # zz:b is carved: the edge serves 1
    assert _run(monkeypatch) == 0
    out = capsys.readouterr().out
    assert "carved out     : 1" in out and "SERVED (edge) : 1 row(s)  — matches the catalogue" in out


class _Resp:
    def __init__(self, body):
        self.body = body

    def __enter__(self):
        import io
        return io.BytesIO(self.body)

    def __exit__(self, *a):
        return False


def test_the_served_count_reads_total_for_this_source_fresh_every_time(monkeypatch):
    """The REAL leg (R1242 M06 read `count`, M07 dropped ?source=, M08 turned a failure into 0 rows - all survived
    a test that stubbed the whole function)."""
    import urllib.request
    seen = []

    def urlopen(req, timeout=None):
        seen.append(req.full_url)
        return _Resp(b'{"total": 7, "count": 99}')
    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    assert V._served_count("zz") == (7, None)
    V._served_count("zz")
    assert "source=zz" in seen[0] and "&_fresh=" in seen[0] and seen[0] != seen[1], seen


@pytest.mark.parametrize("body,why", [(b'{"count": 7}', "no integer `total`"), (b"[]", "no integer `total`")])
def test_an_answer_without_a_total_is_unchecked_not_zero(monkeypatch, body, why):
    import urllib.request
    monkeypatch.setattr(urllib.request, "urlopen", lambda req, timeout=None: _Resp(body))
    n, err = V._served_count("zz")
    assert n == 0 and why in err


def test_a_network_failure_is_unchecked_not_zero(monkeypatch):
    import urllib.request
    monkeypatch.setattr(urllib.request, "urlopen", lambda *a, **k: (_ for _ in ()).throw(OSError("down")))
    n, err = V._served_count("zz")
    assert n == 0 and err and "OSError" in err
