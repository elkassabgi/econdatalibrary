"""tools/audit_untouched_files.py after T0 (plan step 6d): the write times are the LIVE local store's files (recursive),
R2 - a frozen copy - is not asked, and outside the live checkout it refuses once, up front (a refusal inside the
per-source loop would otherwise read as 'cannot list' and the run as clean)."""
import datetime as dt
import os
import sys

import pytest

from core import catalog_path, cutover, r2_util
from updater import blob
from updater import config as updater_config

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from tools import audit_untouched_files as U  # noqa: E402


@pytest.fixture
def live(tmp_path, monkeypatch):
    root = tmp_path / "live"
    full = root / "data" / "clean_full"
    (full / "zz" / "sub").mkdir(parents=True)
    new = dt.datetime(2030, 6, 1, tzinfo=dt.timezone.utc).timestamp()
    old = dt.datetime(2030, 1, 1, tzinfo=dt.timezone.utc).timestamp()
    for rel, t in (("a.parquet", new), ("sub/b.parquet", old)):
        p = full / "zz" / rel
        p.write_bytes(b"x")
        os.utime(p, (t, t))
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    monkeypatch.setattr(catalog_path, "LIVE_STORE_ROOT", str(root))
    monkeypatch.setattr(blob, "_code_root", lambda: str(root))
    monkeypatch.setattr(updater_config, "ROOT", str(root))
    monkeypatch.setattr(updater_config, "DATA_ROOT", str(full))
    monkeypatch.setattr(updater_config, "source_dir", lambda s: str(full / s))
    monkeypatch.delenv("ECONDL_DATA", raising=False)
    monkeypatch.delenv("ECONDL_CATALOG", raising=False)
    monkeypatch.setattr(r2_util, "client", lambda *a, **k: pytest.fail("R2 asked after T0"))
    (tmp_path / "CUTOVER").write_text("")
    return tmp_path


def test_after_t0_the_live_store_files_are_judged(live, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["audit_untouched_files.py", "zz", "--days", "14"])
    U.main()
    out = capsys.readouterr().out
    assert "1 of 2 file(s)" in out and "sub/b.parquet" in out, out


def test_after_t0_another_checkout_is_refused_not_listed_as_unlistable(live, monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(blob, "_code_root", lambda: str(tmp_path / "a_worktree"))
    monkeypatch.setattr(sys, "argv", ["audit_untouched_files.py", "zz"])
    with pytest.raises(cutover.CutoverRefused, match="R1203"):
        U.main()
    assert "cannot list" not in capsys.readouterr().out
