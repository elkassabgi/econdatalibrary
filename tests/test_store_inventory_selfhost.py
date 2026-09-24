"""tools/store_inventory.py after T0 (plan step 6d): the live checkout's local store IS the store, R2 is a frozen copy
and is not asked; outside the live checkout it refuses (a worktree's tree is scratch). Before T0 unchanged: R2 is
the store and an unreachable R2 is a refusal, never a local answer."""
import os
import sqlite3
import sys

import pytest

from core import catalog_path, cutover, r2_util
from updater import blob
from updater import config as updater_config

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from tools import store_inventory as S  # noqa: E402


@pytest.fixture
def live(tmp_path, monkeypatch):
    root = tmp_path / "live"
    store = root / "data" / "clean_full" / "zz"
    store.mkdir(parents=True)
    for stem in ("A01", "B02"):
        (store / f"{stem}.parquet").write_bytes(b"x")
    build = root / "data" / "catalog.db"
    with sqlite3.connect(build) as c:
        c.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
        c.executemany("INSERT INTO series VALUES (?, 'zz')", [("zz:A01",), ("zz:C03",)])
    c.close()
    monkeypatch.setattr(S, "ROOT", str(root))
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    monkeypatch.setattr(catalog_path, "LIVE_STORE_ROOT", str(root))
    monkeypatch.setattr(catalog_path, "BUILD_PATH", str(build))
    monkeypatch.setattr(blob, "_code_root", lambda: str(root))
    monkeypatch.setattr(updater_config, "ROOT", str(root))
    monkeypatch.setattr(updater_config, "DATA_ROOT", str(root / "data" / "clean_full"))
    monkeypatch.delenv("ECONDL_DATA", raising=False)
    monkeypatch.delenv("ECONDL_CATALOG", raising=False)
    monkeypatch.setattr(r2_util, "client", lambda *a, **k: pytest.fail("R2 asked after T0"))
    (tmp_path / "CUTOVER").write_text("")
    return tmp_path


def test_after_t0_the_local_store_is_the_store(live, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["store_inventory.py", "zz"])
    assert S.main() == 0
    out = capsys.readouterr().out
    assert "local store files :       2   <- THE STORE" in out
    assert "catalogued ids with NO store file: 1" in out and "C03" in out
    assert "R2 store files" not in out


def test_after_t0_a_nested_store_is_counted_whole(live, monkeypatch, capsys):
    """R1242: a top-level listing counted bea as 1 of 592 and edgar_13f as 0 of 371 and called it THE STORE."""
    deep = live / "live" / "data" / "clean_full" / "zz" / "tree" / "deeper"
    deep.mkdir(parents=True)
    (deep / "C03.parquet").write_bytes(b"x")
    monkeypatch.setattr(sys, "argv", ["store_inventory.py", "zz"])
    assert S.main() == 0
    out = capsys.readouterr().out
    assert "local store files :       3   <- THE STORE" in out
    assert "catalogued ids with NO store file: 0" in out, "the nested file is the catalogued C03"


def test_after_t0_another_checkout_is_refused(live, monkeypatch, tmp_path):
    monkeypatch.setattr(blob, "_code_root", lambda: str(tmp_path / "a_worktree"))
    monkeypatch.setattr(sys, "argv", ["store_inventory.py", "zz"])
    with pytest.raises(cutover.CutoverRefused, match="R1203"):
        S.main()


def test_before_t0_r2_is_the_store_and_unreachable_is_a_refusal(live, monkeypatch, tmp_path, capsys):
    (tmp_path / "CUTOVER").unlink()
    monkeypatch.setattr(r2_util, "client", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("no credentials")))
    monkeypatch.setattr(sys, "argv", ["store_inventory.py", "zz"])
    assert S.main() == 2
    assert "REFUSING to answer from the local disk" in capsys.readouterr().out
