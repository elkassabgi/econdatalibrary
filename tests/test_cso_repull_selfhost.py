"""tools/cso_repull_subject.py and tools/cso_repull_matrix.py after T0 (plan: LOCAL): the whole --apply (cursor
first, then the parquet) runs from the live checkout, under the writer lock, with local byte-verified backups; a
refusal comes BEFORE the live cursor is touched (R1228: from another checkout the cursor was emptied and only the
parquet backup refused), and a refusal is raised, never reported as a failed backup."""
import datetime as dt
import json
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from core import catalog_path, cutover
from updater import blob
from updater import config as updater_config

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from tools import cso_repull_matrix as MAT  # noqa: E402
from tools import cso_repull_subject as SUB  # noqa: E402

CURSOR = {"AAA01": "2026-01-01", "BBB02": "2026-01-01", "CCC03": "2026-01-01"}


@pytest.fixture
def live(tmp_path, monkeypatch):
    root = tmp_path / "live"
    out = root / "data" / "clean_full" / "cso"
    out.mkdir(parents=True)
    pq.write_table(pa.table({"series_key": ["CSO:AAA01:x", "CSO:BBB02:y"],
                             "obs_date": [dt.date(2020, 1, 1)] * 2, "value": [1.0, 2.0]}), out / "S1.parquet")
    pq.write_table(pa.table({"series_key": ["CSO:AAA01:TLIST(A1)=2020:x", "CSO:CCC03:z"],
                             "obs_date": [dt.date(2020, 1, 1)] * 2, "value": [3.0, 4.0]}), out / "S2.parquet")
    (out / "_collupd.json").write_text(json.dumps(CURSOR), encoding="utf-8")
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    monkeypatch.setattr(catalog_path, "LIVE_STORE_ROOT", str(root))
    monkeypatch.setattr(catalog_path, "BUILD_PATH", str(tmp_path / "no_build" / "catalog.db"))
    monkeypatch.setattr(catalog_path, "LOCK_PATH", str(tmp_path / "lock" / "writer.lock"))
    monkeypatch.setattr(blob, "_code_root", lambda: str(root))
    monkeypatch.setattr(updater_config, "ROOT", str(root))
    monkeypatch.setattr(updater_config, "DATA_ROOT", str(root / "data" / "clean_full"))
    monkeypatch.setattr(updater_config, "source_dir", lambda s: str(root / "data" / "clean_full" / s))
    monkeypatch.delenv("ECONDL_DATA", raising=False)
    monkeypatch.delenv("ECONDL_CATALOG", raising=False)
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(blob, "R2Blob", lambda *a, **k: pytest.fail("R2 used after T0"))
    for m in (SUB, MAT):
        monkeypatch.setattr(m, "runs_in_flight", lambda: [])
        monkeypatch.setattr(m, "banner", lambda *a, **k: None)
    monkeypatch.setattr(SUB.C, "_matrix_subject_map", lambda: {"AAA01": "S1", "BBB02": "S1", "CCC03": "S2"})
    monkeypatch.setattr(SUB.C, "_out_dir", lambda: str(out))
    monkeypatch.setattr(SUB.C, "_cursor_path", lambda: str(out / "_collupd.json"))
    (tmp_path / "CUTOVER").write_text("")
    return root, out


def _cursor(out):
    return json.loads((out / "_collupd.json").read_text(encoding="utf-8"))


def _spy_lock(monkeypatch):
    seen = []
    real = blob.backup_store_object
    monkeypatch.setattr(blob, "backup_store_object",
                        lambda p, k: seen.append(catalog_path._held is not None) or real(p, k))
    return seen


def test_subject_after_t0_runs_under_the_lock_with_a_local_backup(live, monkeypatch):
    root, out = live
    seen = _spy_lock(monkeypatch)
    original = (out / "S1.parquet").read_bytes()
    monkeypatch.setattr(sys, "argv", ["cso_repull_subject.py", "S1", "--apply"])
    assert SUB.main() == 0
    assert _cursor(out) == {"CCC03": "2026-01-01"}, "S1's matrices dropped from the cursor"
    assert not (out / "S1.parquet").exists()
    backups = list((root / "data" / "_backup" / "cso_repull").rglob("S1.parquet"))
    assert len(backups) == 1 and backups[0].read_bytes() == original
    assert seen == [True] and catalog_path._held is None


def test_matrix_after_t0_runs_under_the_lock_with_a_local_backup(live, monkeypatch):
    root, out = live
    seen = _spy_lock(monkeypatch)
    monkeypatch.setattr(sys, "argv", ["cso_repull_matrix.py", "--apply"])
    assert MAT.main() == 0
    assert "AAA01" not in _cursor(out)
    assert pq.read_table(out / "S2.parquet").column("series_key").to_pylist() == ["CSO:CCC03:z"]
    assert len(list((root / "data" / "_backup" / "cso_repull_matrix").rglob("S2.parquet"))) == 1
    assert seen == [True] and catalog_path._held is None


def test_before_t0_subject_reports_the_r2_key_it_deleted(live, monkeypatch, tmp_path, capsys):
    """R1230: the pre-T0 success line named the local path as "deleted" when the R2 key was deleted."""
    (tmp_path / "CUTOVER").unlink()
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
    monkeypatch.setattr(sys, "argv", ["cso_repull_subject.py", "S1", "--apply"])
    assert SUB.main() == 0
    out = capsys.readouterr().out
    assert "deleted r2://clean_full/cso/S1.parquet" in out, out[-600:]
    assert [c[0] for c in calls] == ["copy", "delete"] and calls[1][1] == "clean_full/cso/S1.parquet"


@pytest.mark.parametrize("tool,argv", [(SUB, ["S1", "--apply"]), (MAT, ["--apply"])])
def test_after_t0_another_checkout_touches_nothing(live, monkeypatch, tmp_path, tool, argv):
    """R1228: the cursor was emptied and a _collupd.backup file written in the live store before the parquet
    backup refused. The checkout is now checked first."""
    root, out = live
    monkeypatch.setattr(blob, "_code_root", lambda: str(tmp_path / "a_worktree"))
    before = {p.name: p.read_bytes() for p in out.iterdir()}
    monkeypatch.setattr(sys, "argv", [tool.__name__, *argv])
    with pytest.raises(cutover.CutoverRefused, match="R1203"):
        tool.main()
    assert {p.name: p.read_bytes() for p in out.iterdir()} == before, "nothing in the store changed"
    assert catalog_path._held is None


@pytest.mark.parametrize("tool,argv", [(SUB, ["S1", "--apply"]), (MAT, ["--apply"])])
def test_a_refusal_at_the_backup_is_raised_not_reported_as_a_failed_backup(live, monkeypatch, capsys, tool, argv):
    def refuse(p, k):
        raise cutover.CutoverRefused("refused: some rule")
    monkeypatch.setattr(blob, "backup_store_object", refuse)
    monkeypatch.setattr(sys, "argv", [tool.__name__, *argv])
    with pytest.raises(cutover.CutoverRefused, match="some rule"):
        tool.main()
    assert "backup not proved" not in capsys.readouterr().out


@pytest.mark.parametrize("tool,argv", [(SUB, ["S1", "--apply"]), (MAT, ["--apply"])])
def test_the_whole_apply_holds_the_lock(live, monkeypatch, tool, argv):
    """R1228: removing write_session from either tool passed. Every store write in --apply sees the lock."""
    seen = []
    real = blob.write_bytes_atomic
    monkeypatch.setattr(blob, "write_bytes_atomic",
                        lambda p, d: seen.append(catalog_path._held is not None) or real(p, d))
    monkeypatch.setattr(sys, "argv", [tool.__name__, *argv])
    assert tool.main() == 0
    assert seen and all(seen), "every cursor write ran under the lock"
