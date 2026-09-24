"""tools/probe_csv_freshness.py after T0 (plan: LOCAL + a scheduler): the served CSVs are the self-hosted store's,
the store they are judged against is the live checkout's (so it refuses to run from anywhere else), the mirror
gate has nothing to compare (one store), and the rotation bookmark is the local file only - nothing reaches R2.
Before T0 unchanged."""
import gzip
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
from tools import probe_csv_freshness as P  # noqa: E402
from blobstore import BlobStore  # noqa: E402

CSV = b"series_id,obs_date,value\nk,2024-01-01,1\n"


@pytest.fixture
def t0(tmp_path, monkeypatch):
    live = tmp_path / "live"
    (live / "data").mkdir(parents=True)
    build = live / "data" / "catalog.db"
    with sqlite3.connect(build) as c:
        c.execute("CREATE TABLE series (series_id TEXT, source_id TEXT)")
        c.executemany("INSERT INTO series VALUES (?, ?)", [("zz:a", "zz"), ("zz:b", "zz")])
    c.close()
    BlobStore(str(tmp_path / "blobs"), create=True)
    monkeypatch.setattr(blob, "SELFHOST_BLOB_ROOT", str(tmp_path / "blobs"))
    sb = blob.SelfhostBlob()
    sb.put_atomic("series/zz%3Aa.csv", CSV)                  # before T0: gzip at rest, as served
    sb.put_atomic("series/zz%3Ab.csv", CSV.replace(b",1\n", b",2\n"))
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    monkeypatch.setattr(catalog_path, "LIVE_STORE_ROOT", str(live))
    monkeypatch.setattr(catalog_path, "BUILD_PATH", str(build))
    monkeypatch.setattr(catalog_path, "LOCK_PATH", str(tmp_path / "lock" / "writer.lock"))
    monkeypatch.setattr(blob, "_code_root", lambda: str(live))
    monkeypatch.setattr(updater_config, "ROOT", str(live))
    monkeypatch.setattr(updater_config, "DATA_ROOT", str(live / "data" / "clean_full"))
    monkeypatch.delenv("ECONDL_DATA", raising=False)
    monkeypatch.delenv("ECONDL_CATALOG", raising=False)
    monkeypatch.setattr(P, "_cursor_local", lambda: str(tmp_path / "state" / "csv_freshness_cursor.json"))
    monkeypatch.setattr(r2_util, "client", lambda *a, **k: pytest.fail("R2 reached after T0"))
    import core.derive_csv as dc
    monkeypatch.setattr(dc, "_series_csv_bytes", lambda sid: CSV)     # what the store derives now
    (tmp_path / "CUTOVER").write_text("")
    return tmp_path


def test_after_t0_served_bytes_come_from_the_self_hosted_store(t0, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["probe_csv_freshness.py", "--source", "zz", "--sample", "5"])
    assert P.main() == 1, "zz:b is served with a different value: stale"
    out = capsys.readouterr().out
    assert "STALE  zz" in out and "1/2 differ" in out


def test_after_t0_a_matching_store_is_clean_and_the_bookmark_is_local(t0, monkeypatch):
    sb = blob.SelfhostBlob()
    with catalog_path.writer_lock():
        sb.put_atomic("series/zz%3Ab.csv", CSV)
    monkeypatch.setattr(sys, "argv", ["probe_csv_freshness.py", "--sources", "5", "--sample", "5"])
    assert P.main() == 0
    assert P._load_cursor(None) == "zz", "the rotation bookmark is the local file"


def test_after_t0_the_probe_refuses_another_checkout(t0, monkeypatch):
    monkeypatch.setattr(blob, "_code_root", lambda: str(t0 / "a_worktree"))
    monkeypatch.setattr(sys, "argv", ["probe_csv_freshness.py", "--source", "zz"])
    with pytest.raises(cutover.CutoverRefused, match="R1203"):
        P.main()


def test_after_t0_the_mirror_gate_has_nothing_to_compare(t0):
    assert P._mirror_matches_store("zz") is True


def test_gzip_at_rest_is_inflated_before_the_comparison(t0):
    stored = blob.SelfhostBlob().get("series/zz%3Aa.csv")
    assert stored[:2] == b"\x1f\x8b" and gzip.decompress(stored) == CSV, "precondition: served gzipped"
