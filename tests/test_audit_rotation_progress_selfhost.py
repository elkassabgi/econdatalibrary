"""tools/audit_rotation_progress.py after T0 (plan step 6d): the write times are the LIVE local store's (recursive, every
.parquet - the same files the R2 listing counted), R2 - a frozen copy - is not asked; live checkout only."""
import datetime as dt
import os
import sys

import pytest

from core import catalog_path, cutover, r2_util
from updater import blob
from updater import config as updater_config

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from tools import audit_rotation_progress as R  # noqa: E402


@pytest.fixture
def live(tmp_path, monkeypatch):
    root = tmp_path / "live"
    full = root / "data" / "clean_full"
    (full / "zz" / "deep").mkdir(parents=True)
    now = dt.datetime.now(dt.timezone.utc).timestamp()
    for i in range(21):                              # >= MIN_FILES; 15 long unwritten, 6 fresh
        p = full / "zz" / ("deep" if i % 2 else "") / f"f{i}.parquet"
        p.write_bytes(b"x")
        age = 400 if i < 15 else 1
        os.utime(p, (now - age * 86400, now - age * 86400))
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    monkeypatch.setattr(catalog_path, "LIVE_STORE_ROOT", str(root))
    monkeypatch.setattr(blob, "_code_root", lambda: str(root))
    monkeypatch.setattr(updater_config, "ROOT", str(root))
    monkeypatch.setattr(updater_config, "DATA_ROOT", str(full))
    monkeypatch.setattr(updater_config, "source_dir", lambda s: str(full / s))
    monkeypatch.delenv("ECONDL_DATA", raising=False)
    monkeypatch.delenv("ECONDL_CATALOG", raising=False)
    from updater import registry
    monkeypatch.setattr(registry, "load", lambda *a, **k: {"sources": [
        {"source_id": "zz", "live": True, "cadence": "monthly"}]})
    monkeypatch.setattr(R, "budgeted_sources", lambda: {"zz"})
    monkeypatch.setattr(r2_util, "client", lambda *a, **k: pytest.fail("R2 asked after T0"))
    (tmp_path / "CUTOVER").write_text("")
    return tmp_path


def test_after_t0_a_stuck_rotation_is_found_in_the_live_store(live, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["audit_rotation_progress.py", "--verbose"])
    R.main()
    out = capsys.readouterr().out
    assert "STUCK zz" in out and "files=   21" in out, out


def test_after_t0_only_parquet_files_are_counted(live, monkeypatch, capsys):
    """R1243 R3: a store dir also holds sidecars (_incr_state.json, _manifest.jsonl, .done markers); counted,
    their fresh write times would hide a stuck rotation - the R2 listing counted .parquet only."""
    zz = live / "live" / "data" / "clean_full" / "zz"
    for name in ("_incr_state.json", "_manifest.jsonl", "a.done", "b.done"):
        (zz / name).write_text("x")
    monkeypatch.setattr(sys, "argv", ["audit_rotation_progress.py", "--verbose"])
    R.main()
    out = capsys.readouterr().out
    assert "STUCK zz" in out and "files=   21" in out, out


def test_after_t0_another_checkout_is_refused(live, monkeypatch, tmp_path):
    monkeypatch.setattr(blob, "_code_root", lambda: str(tmp_path / "a_worktree"))
    monkeypatch.setattr(sys, "argv", ["audit_rotation_progress.py"])
    with pytest.raises(cutover.CutoverRefused, match="R1203"):
        R.main()
