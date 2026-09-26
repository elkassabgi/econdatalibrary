"""tools/audit_unwritten_store_regions.py after T0 (plan step 6d): the write times are the LIVE local store's (recursive),
R2 - a frozen copy - is not asked and the process is not switched onto the R2 backend; live checkout only."""
import datetime as dt
import os
import sys

import pytest

from core import catalog_path, cutover, r2_util
from updater import blob
from updater import config as updater_config

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from tools import audit_unwritten_store_regions as W  # noqa: E402


@pytest.fixture
def live(tmp_path, monkeypatch):
    root = tmp_path / "live"
    full = root / "data" / "clean_full"
    (full / "zz" / "tree").mkdir(parents=True)
    # an hour back, on a whole even second: exFAT (D:\temp here) rounds a time UP to the next even second, which
    # put the "fresh" file in the future and read -1d (R1243)
    now = (int(dt.datetime.now(dt.timezone.utc).timestamp()) - 3600) // 2 * 2
    for rel, days in (("a.parquet", 0), ("tree/b.parquet", 120)):
        p = full / "zz" / rel
        p.write_bytes(b"x")
        os.utime(p, (now - days * 86400, now - days * 86400))
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    monkeypatch.setattr(catalog_path, "LIVE_STORE_ROOT", str(root))
    monkeypatch.setattr(blob, "_code_root", lambda: str(root))
    monkeypatch.setattr(updater_config, "ROOT", str(root))
    monkeypatch.setattr(updater_config, "DATA_ROOT", str(full))
    monkeypatch.setattr(updater_config, "source_dir", lambda s: str(full / s))
    monkeypatch.setattr(updater_config, "STATE_DB", str(tmp_path / "no_state.db"))
    monkeypatch.delenv("ECONDL_DATA", raising=False)
    monkeypatch.delenv("ECONDL_CATALOG", raising=False)
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    from updater import registry
    monkeypatch.setattr(registry, "load", lambda *a, **k: {"sources": [{"source_id": "zz", "live": True}]})
    monkeypatch.setattr(r2_util, "client", lambda *a, **k: pytest.fail("R2 asked after T0"))
    (tmp_path / "CUTOVER").write_text("")
    return tmp_path


def test_after_t0_the_live_store_is_screened(live, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["audit_unwritten_store_regions.py", "--source", "zz"])
    W.main()
    out = capsys.readouterr().out
    assert "the live local store" in out and "TWO or more parquets      : 1" in out, out
    row = next(ln for ln in out.splitlines() if ln.startswith("zz "))
    assert row.split()[1:4] == ["2", "0d", "120d"], row           # the nested file's age was read (recursive walk)
    assert os.environ.get("AQUEDUCT_BACKEND") is None, "after T0 the process is not put on the R2 backend"


def test_after_t0_another_checkout_is_refused(live, monkeypatch, tmp_path):
    monkeypatch.setattr(blob, "_code_root", lambda: str(tmp_path / "a_worktree"))
    monkeypatch.setattr(sys, "argv", ["audit_unwritten_store_regions.py", "--source", "zz"])
    with pytest.raises(cutover.CutoverRefused, match="R1203"):
        W.main()
