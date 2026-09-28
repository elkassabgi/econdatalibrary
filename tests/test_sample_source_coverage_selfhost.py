"""tools/sample_source_coverage.py after T0 (plan step 6d): the sample is checked against the SELF-HOSTED store (R2 is a
frozen copy and is not asked), from the live checkout only."""
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
from tools import sample_source_coverage as C  # noqa: E402
from blobstore import BlobStore  # noqa: E402


@pytest.fixture
def live(tmp_path, monkeypatch):
    root = tmp_path / "live"
    (root / "data").mkdir(parents=True)
    build = root / "data" / "catalog.db"
    with sqlite3.connect(build) as c:
        c.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
        c.executemany("INSERT INTO series VALUES (?, 'zz')", [("zz:a",), ("zz:b",), ("zz:c",), ("zz:d",)])
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
    store = blob.SelfhostBlob()
    with catalog_path.writer_lock():
        for k in ("series/zz%3Aa.csv", "series/zz%3Ab.csv", "series/zz%3Ac.csv"):   # zz:d has no CSV
            store.put_atomic(k, b"series_id,obs_date,value\n")
    (tmp_path / "CUTOVER").write_text("")
    return tmp_path


def test_after_t0_the_sample_is_checked_against_the_self_hosted_store(live, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["sample_source_coverage.py", "--source", "zz", "--sample", "4", "--workers", "2"])
    assert C.main() == 0
    out = capsys.readouterr().out
    assert "present in the self-hosted store: 3" in out and "coverage          : 75.0%" in out
    assert "zz:d" in out


def test_after_t0_another_checkout_is_refused(live, monkeypatch, tmp_path):
    monkeypatch.setattr(blob, "_code_root", lambda: str(tmp_path / "a_worktree"))
    monkeypatch.setattr(sys, "argv", ["sample_source_coverage.py", "--source", "zz"])
    with pytest.raises(cutover.CutoverRefused, match="R1203"):
        C.main()


def _trace_series_reads(monkeypatch):
    """Every SQL statement the tool sends through the catalogue resolver, judged by the ONE shared predicate
    (review round 5: four looser copies passed a 10**12 chunk and a whole-table `series_id >= ''`)."""
    import _bounded_reads                                  # noqa: PLC0415
    return _bounded_reads.trace_series_reads(monkeypatch)


def test_after_t0_the_sample_is_read_by_primary_key_range(live, monkeypatch, capsys):
    """R1249 G8: `source_id=?` has no index - a full scan of the live build, twice, one with ORDER BY RANDOM()."""
    check = _trace_series_reads(monkeypatch)
    monkeypatch.setattr(sys, "argv", ["sample_source_coverage.py", "--source", "zz", "--sample", "4", "--workers", "2"])
    C.main()
    reads, bad = check()
    assert len(reads) == 2 and not bad and all("series_id >=" in q for q in reads), reads

def _add_neighbour_source(monkeypatch):
    """A source whose id sorts right after `zz:` keys under a WIDER upper bound: `zz_x:1`. The primary-key range is
    [`zz:`, `zz;`) - `;` is the byte after `:` - and must exclude it; `~` as the bound would include it (R1253)."""
    with catalog_path.writer_lock():
        c = catalog_path.connect(write=True)
        with c:
            c.execute("INSERT INTO series (series_id, source_id) VALUES ('zz_x:1', 'zz_x')")
        c.close()


def test_after_t0_a_neighbouring_source_is_not_in_the_key_range(live, monkeypatch, capsys):
    _add_neighbour_source(monkeypatch)
    monkeypatch.setattr(sys, "argv", ["sample_source_coverage.py", "--source", "zz", "--sample", "4", "--workers", "2"])
    C.main()
    out = capsys.readouterr().out
    assert "catalogued        : 4" in out and "zz_x" not in out, out
