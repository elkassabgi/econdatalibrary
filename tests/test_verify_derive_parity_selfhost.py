"""tools/verify_derive_parity.py after T0 (plan step 6d): parity of the catalogue against the SELF-HOSTED store (R2 is
a frozen copy and is not asked), both directions, from the live checkout only; the colon-anchored prefix keeps a
sibling source's objects out."""
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
from tools import verify_derive_parity as P  # noqa: E402
from blobstore import BlobStore  # noqa: E402


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
    store = blob.SelfhostBlob()
    with catalog_path.writer_lock():
        for k in ("series/zz%3Aa.csv", "series/zz%3Ab.csv", "series/zzz%3Asibling.csv"):
            store.put_atomic(k, b"series_id,obs_date,value\n")
    (tmp_path / "CUTOVER").write_text("")
    return tmp_path, build, store


def _run(monkeypatch, build):
    monkeypatch.setattr(sys, "argv", ["verify_derive_parity.py", "--source", "zz", "--catalog", str(build)])
    return P.main()


def test_after_t0_parity_is_judged_on_the_self_hosted_store(live, monkeypatch, capsys):
    tmp, build, store = live
    assert _run(monkeypatch, build) == 0
    out = capsys.readouterr().out
    assert "self-hosted store: 2 derived CSV(s)" in out and "PARITY: OK" in out and "r2://" not in out


def test_after_t0_a_missing_csv_fails_parity(live, monkeypatch, capsys):
    tmp, build, store = live
    with catalog_path.writer_lock():
        store.delete("series/zz%3Ab.csv")
    assert _run(monkeypatch, build) == 1
    assert "MISSING  (catalogued, no CSV): 1" in capsys.readouterr().out


def test_after_t0_another_checkout_is_refused(live, monkeypatch, tmp_path):
    tmp, build, store = live
    monkeypatch.setattr(blob, "_code_root", lambda: str(tmp_path / "a_worktree"))
    with pytest.raises(cutover.CutoverRefused, match="R1203"):
        _run(monkeypatch, build)
