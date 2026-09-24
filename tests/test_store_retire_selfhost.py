"""Retiring a store file (tools/repull_file.py, cso_repull_matrix.py, cso_repull_subject.py) through
updater.blob.backup_store_object / delete_store_object: before T0 a server-side R2 copy and delete, exactly as the
tools did; after T0 the store is local, so the backup is a local copy beside it (<data>/_backup/...), proved byte
for byte, never overwritten - from the live checkout only, with the writer lock held."""
import datetime as dt
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from core import catalog_path, cutover
from updater import blob
from updater import config as updater_config

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools"))
import repull_file  # noqa: E402


@pytest.fixture
def live(tmp_path, monkeypatch):
    root = tmp_path / "live"
    full = root / "data" / "clean_full"
    (full / "cso").mkdir(parents=True)
    pq.write_table(pa.table({"series_key": ["a", "b"], "obs_date": [dt.date(2020, 1, 1), dt.date(9999, 1, 1)],
                             "value": [1.0, 2.0]}), full / "cso" / "X.parquet")
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    monkeypatch.setattr(catalog_path, "LIVE_STORE_ROOT", str(root))
    monkeypatch.setattr(catalog_path, "BUILD_PATH", str(tmp_path / "no_build" / "catalog.db"))
    monkeypatch.setattr(catalog_path, "LOCK_PATH", str(tmp_path / "lock" / "writer.lock"))
    monkeypatch.setattr(blob, "_code_root", lambda: str(root))
    monkeypatch.setattr(updater_config, "ROOT", str(root))
    monkeypatch.setattr(updater_config, "DATA_ROOT", str(full))
    monkeypatch.setattr(updater_config, "source_dir", lambda s: str(full / s))
    monkeypatch.delenv("ECONDL_DATA", raising=False)
    monkeypatch.delenv("ECONDL_CATALOG", raising=False)
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(repull_file, "runs_in_flight", lambda: [])
    monkeypatch.setattr(blob, "R2Blob", lambda *a, **k: pytest.fail("R2 used after T0"))
    (tmp_path / "CUTOVER").write_text("")
    return root, full


def test_after_t0_repull_backs_up_locally_then_deletes_under_the_lock(live, monkeypatch):
    root, full = live
    held = []
    real_delete = blob.delete_store_object
    monkeypatch.setattr(blob, "delete_store_object",
                        lambda p: held.append(catalog_path._held is not None) or real_delete(p))
    original = (full / "cso" / "X.parquet").read_bytes()
    monkeypatch.setattr(sys, "argv", ["repull_file.py", "cso", "X.parquet", "--apply", "--cursor-cleared"])
    assert repull_file.main() == 0
    assert not (full / "cso" / "X.parquet").exists()
    backups = list((root / "data" / "_backup" / "repull" / "cso").rglob("X.parquet"))
    assert len(backups) == 1 and backups[0].read_bytes() == original, "a byte-exact local backup"
    assert held == [True], "the delete ran under the writer lock"
    assert catalog_path._held is None, "and let it go"


def test_after_t0_repull_from_another_checkout_changes_nothing(live, monkeypatch, tmp_path):
    root, full = live
    monkeypatch.setattr(blob, "_code_root", lambda: str(tmp_path / "a_worktree"))
    monkeypatch.setattr(sys, "argv", ["repull_file.py", "cso", "X.parquet", "--apply", "--cursor-cleared"])
    with pytest.raises(cutover.CutoverRefused, match="R1203"):
        repull_file.main()
    assert (full / "cso" / "X.parquet").exists()
    assert not (root / "data" / "_backup").exists()


def test_after_t0_the_helpers_refuse_without_the_lock(live):
    root, full = live
    with pytest.raises(cutover.CutoverRefused, match="writer lock"):
        blob.backup_store_object(str(full / "cso" / "X.parquet"), "_backup/t/X.parquet")
    with pytest.raises(cutover.CutoverRefused, match="writer lock"):
        blob.delete_store_object(str(full / "cso" / "X.parquet"))
    assert (full / "cso" / "X.parquet").exists()


def test_a_backup_is_never_overwritten_and_keys_stay_in_the_backup_tree(live):
    root, full = live
    p = str(full / "cso" / "X.parquet")
    with catalog_path.writer_lock():
        where = blob.backup_store_object(p, "_backup/t/X.parquet")
        assert os.path.exists(where)
        with pytest.raises(FileExistsError):
            blob.backup_store_object(p, "_backup/t/X.parquet")
        for bad in ("clean_full/cso/X.parquet", "_backup/../clean_full/x", "_backup\\x", "C:_backup/x"):
            with pytest.raises(ValueError):
                blob.backup_store_object(p, bad)


def test_after_t0_a_delete_that_did_nothing_is_an_abort(live, monkeypatch, capsys):
    """R1228: repull_file's check after the delete was removable."""
    root, full = live
    monkeypatch.setattr(blob, "delete_store_object", lambda p: None)
    monkeypatch.setattr(sys, "argv", ["repull_file.py", "cso", "X.parquet", "--apply", "--cursor-cleared"])
    assert repull_file.main() == 1
    assert "still present after delete" in capsys.readouterr().out


def test_a_copy_that_differs_is_not_a_backup(live, monkeypatch):
    """R1228: the byte check of the local backup was removable."""
    root, full = live
    import shutil
    monkeypatch.setattr(shutil, "copy2", lambda s, d: open(d, "wb").write(b"not the same bytes"))
    with catalog_path.writer_lock():
        with pytest.raises(RuntimeError, match="does not match"):
            blob.backup_store_object(str(full / "cso" / "X.parquet"), "_backup/t/X.parquet")


def test_before_t0_an_unreadable_r2_backup_is_not_a_backup(tmp_path, monkeypatch):
    """R1228: the pre-T0 exists check was removable."""
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "no_flag"))

    class R2:
        bucket = "econ-data"
        client = type("C", (), {"copy_object": lambda self, **kw: None})()

        def exists(self, key):
            return False
    monkeypatch.setattr(blob, "R2Blob", lambda *a, **k: R2())
    with pytest.raises(RuntimeError, match="not readable"):
        blob.backup_store_object(os.path.join("x", "data", "clean_full", "cso", "X.parquet"), "_backup/r/X.parquet")


def test_before_t0_the_helpers_use_r2_as_the_tools_did(tmp_path, monkeypatch):
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "no_flag"))
    calls = []

    class C:
        def copy_object(self, **kw):
            calls.append(("copy", kw["Key"], kw["CopySource"]["Key"]))

        def delete_object(self, **kw):
            calls.append(("delete", kw["Key"]))

    class R2:
        bucket = "econ-data"
        client = C()

        def exists(self, key):
            return True
    monkeypatch.setattr(blob, "R2Blob", lambda *a, **k: R2())
    path = os.path.join("x", "data", "clean_full", "cso", "X.parquet")
    assert blob.backup_store_object(path, "_backup/r/X.parquet") == "r2://_backup/r/X.parquet"
    blob.delete_store_object(path)
    assert calls == [("copy", "_backup/r/X.parquet", "clean_full/cso/X.parquet"),
                     ("delete", "clean_full/cso/X.parquet")]
