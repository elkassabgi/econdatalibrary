"""tools/refresh_sec_edgar.py reaches D1 only through core.d1_remote and opens the catalogue only through
core.catalog_path (t0_ready legacy-remote-d1 and legacy-catalogue, 2026-11-16).

Before T0 nothing a run does changes: the same `wrangler d1 execute econ-catalog --remote --json ...` from the
version-pinned local wrangler, the same environment (and its switch to the OAuth login on error 7403), the
same three tries on error 10000, the same catalogue file. After T0 the D1 road refuses before wrangler runs,
and the catalogue opens only the build. No network: subprocess.run is faked."""
import os
import sqlite3
import subprocess
import sys

import pytest

from core import catalog_path, cutover, d1_remote

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from tools import refresh_sec_edgar as R  # noqa: E402

OK_JSON = '[\n  {"results": [{"n": 1}], "success": true, "meta": {"rows_read": 1}}\n]\n'


@pytest.fixture
def runs(monkeypatch, tmp_path):
    """Every wrangler run, recorded; answers come from `runs.answers` (rc, stdout), one per run."""
    calls = []
    wrangler = tmp_path / "wrangler.js"
    wrangler.write_text("")
    monkeypatch.setattr(d1_remote, "WRANGLER_JS", str(wrangler))
    monkeypatch.setattr(R.time, "sleep", lambda s: None)          # the 10000 retry waits 10 s

    def fake_run(argv, **kw):
        calls.append({"argv": list(argv), "env": dict(kw.get("env") or {}), "cwd": kw.get("cwd"),
                      "timeout": kw.get("timeout")})
        rc, out = fake_run.answers.pop(0) if fake_run.answers else (0, OK_JSON)
        return subprocess.CompletedProcess(argv, rc, stdout=out, stderr="")
    fake_run.answers = []
    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setitem(R._WRANGLER_ENV, "env", None)
    monkeypatch.setitem(R._WRANGLER_ENV, "mode", "inherited")
    fake_run.calls = calls
    fake_run.wrangler = str(wrangler)
    return fake_run


def test_before_t0_the_same_wrangler_command(runs, monkeypatch):
    monkeypatch.setenv("CLOUDFLARE_API_TOKEN", "ci-token")
    out = R._d1_json(["--command", "SELECT 1"], timeout=77)
    assert out == [{"results": [{"n": 1}], "success": True, "meta": {"rows_read": 1}}]
    (c,) = runs.calls
    assert c["argv"] == ["node", runs.wrangler, "d1", "execute", "econ-catalog", "--remote", "--json",
                         "--command", "SELECT 1"]
    assert c["cwd"] == d1_remote.WORKER_DIR and c["timeout"] == 77
    assert c["env"]["CLOUDFLARE_API_TOKEN"] == "ci-token", "the inherited environment (CI's token)"


def test_error_7403_switches_once_to_the_oauth_login(runs, monkeypatch):
    monkeypatch.setenv("CLOUDFLARE_API_TOKEN", "r2-only-token")
    monkeypatch.setenv("CLOUDFLARE_ACCOUNT_ID", "acct")
    runs.answers = [(1, "not authorized [code: 7403]"), (0, OK_JSON)]
    R._d1_json(["--file", "x.sql"])
    first, second = runs.calls
    assert first["env"].get("CLOUDFLARE_API_TOKEN") == "r2-only-token"
    assert not any(k.startswith("CLOUDFLARE_") for k in second["env"]), "the retry runs without CLOUDFLARE_*"
    assert second["argv"][-2:] == ["--file", "x.sql"]


def test_error_10000_is_tried_three_times_then_raised(runs):
    runs.answers = [(1, "Authentication error [code: 10000]")] * 3
    with pytest.raises(RuntimeError, match="rc=1"):
        R._d1_json(["--command", "SELECT 1"])
    assert len(runs.calls) == 3


def test_after_t0_the_d1_road_refuses_before_wrangler_runs(runs, tmp_path, monkeypatch):
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    (tmp_path / "CUTOVER").write_text("")
    with pytest.raises(cutover.CutoverRefused, match="frozen"):
        R._d1_json(["--command", "SELECT 1"])
    with pytest.raises(cutover.CutoverRefused):
        d1_remote.wrangler_execute("econ-catalog", ["--json", "--command", "SELECT 1"])
    assert runs.calls == []


def test_wrangler_execute_takes_only_the_econ_databases(runs):
    with pytest.raises(ValueError):
        d1_remote.wrangler_execute("hfdatalibrary-db", ["--command", "SELECT 1"])
    assert runs.calls == []


# ---- the catalogue opens -----------------------------------------------------------------------------------
def _catalogue(root):
    p = root / "data" / "catalog.db"
    p.parent.mkdir(parents=True)
    with sqlite3.connect(p) as c:
        c.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, frequency TEXT,"
                  " unit TEXT, geography TEXT, category TEXT, license_id TEXT, start_date TEXT, end_date TEXT,"
                  " metadata TEXT, last_updated TEXT)")
        c.execute("CREATE TABLE series_fts (series_id TEXT, title TEXT, geography TEXT)")
    c.close()
    return p


def test_before_t0_the_tool_s_own_catalogue_is_written(tmp_path, monkeypatch):
    p = _catalogue(tmp_path)
    monkeypatch.setattr(R, "ROOT", str(tmp_path))
    R.update_catalog([("ACME", "2020-01-01", "2026-06-30", "Acme Corp", 123)], apply_d1=False)
    c = sqlite3.connect(p)
    assert c.execute("SELECT series_id, end_date FROM series").fetchall() == [("sec_edgar:ACME", "2026-06-30")]
    c.close()


def test_after_t0_a_worktree_s_catalogue_is_refused_not_written(tmp_path, monkeypatch):
    """A plain sqlite3.connect of a worktree's own data/catalog.db is NOT refused after T0 (the runtime guard
    lets a connection write a main database that is not the build); the resolver refuses it."""
    p = _catalogue(tmp_path / "worktree")
    monkeypatch.setattr(R, "ROOT", str(tmp_path / "worktree"))
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    (tmp_path / "CUTOVER").write_text("")
    with pytest.raises(cutover.CutoverRefused, match="not"):
        R.update_catalog([("ACME", "2020-01-01", "2026-06-30", "Acme Corp", 123)], apply_d1=False)
    c = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
    assert c.execute("SELECT count(*) FROM series").fetchone() == (0,)
    c.close()
