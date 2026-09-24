"""tools/audit_csv_staleness.py after T0 (plan step 6d): the newest parquet is the live local store's (recursive) and
the CSV write times are the self-hosted store's stored_utc - R2, a frozen copy, is not asked; live checkout only."""
import datetime as dt
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
from tools import audit_csv_staleness as S  # noqa: E402
from blobstore import BlobStore  # noqa: E402


@pytest.fixture
def live(tmp_path, monkeypatch):
    root = tmp_path / "live"
    nested = root / "data" / "clean_full" / "zz" / "part"
    nested.mkdir(parents=True)
    (nested / "p.parquet").write_bytes(b"x")                       # nested: the listing must recurse
    t = dt.datetime(2030, 1, 1, tzinfo=dt.timezone.utc).timestamp()
    os.utime(nested / "p.parquet", (t, t))
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
    monkeypatch.setattr(S, "ROOT", str(root))
    monkeypatch.setattr(updater_config, "ROOT", str(root))
    monkeypatch.setattr(updater_config, "DATA_ROOT", str(root / "data" / "clean_full"))
    monkeypatch.delenv("ECONDL_DATA", raising=False)
    monkeypatch.delenv("ECONDL_CATALOG", raising=False)
    monkeypatch.setattr(r2_util, "client", lambda *a, **k: pytest.fail("R2 asked after T0"))
    store = blob.SelfhostBlob()
    with catalog_path.writer_lock():
        for k in ("series/zz%3Aa.csv", "series/zz%3Ab.csv"):
            store.put_atomic(k, b"series_id,obs_date,value\n")
    s = store.store
    s._w.execute("UPDATE blobs SET stored_utc=? WHERE key='series/zz%3Aa.csv'", ("2029-06-01T00:00:00+00:00",))
    s._w.execute("UPDATE blobs SET stored_utc=? WHERE key='series/zz%3Ab.csv'", ("2031-06-01T00:00:00+00:00",))
    s._w.commit()
    (tmp_path / "CUTOVER").write_text("")
    return tmp_path


def test_after_t0_a_csv_older_than_the_newest_parquet_is_counted(live, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["audit_csv_staleness.py", "--source", "zz"])
    S.main()
    out = capsys.readouterr().out
    assert "zz" in out and "1 of        2 CSVs predate the newest parquet (2030-01-01)" in out, out


def test_after_t0_another_checkout_is_refused(live, monkeypatch, tmp_path):
    monkeypatch.setattr(blob, "_code_root", lambda: str(tmp_path / "a_worktree"))
    monkeypatch.setattr(sys, "argv", ["audit_csv_staleness.py", "--source", "zz"])
    with pytest.raises(cutover.CutoverRefused, match="R1203"):
        S.main()
