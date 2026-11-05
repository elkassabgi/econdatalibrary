"""tools/prune_series_cursors.runs_in_flight: BOTH cloud workflows that pull and push the state block a prune
(AR-430, R1804 - a heavy run queued at 19:15Z on 2026-11-05 did not), and a gh call that fails blocks too."""
import json
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools"))
import prune_series_cursors as psc  # noqa: E402


def _fake_gh(monkeypatch, tmp_path, by_workflow, rc=0):
    monkeypatch.setattr(psc, "ROOT", str(tmp_path))               # no logs/local_heavy.lock here
    seen = []

    def run(cmd, **kw):
        wf = next(a.split("=", 1)[1] for a in cmd if a.startswith("--workflow="))
        seen.append(wf)
        return subprocess.CompletedProcess(cmd, rc, stdout=json.dumps(by_workflow.get(wf, [])), stderr="")
    monkeypatch.setattr(psc.subprocess, "run", run)
    return seen


def test_idle_on_both_workflows_is_clear(monkeypatch, tmp_path):
    seen = _fake_gh(monkeypatch, tmp_path, {})
    assert psc.runs_in_flight() == []
    assert seen == ["updater-daily.yml", "updater-heavy.yml"]


def test_a_queued_heavy_run_blocks(monkeypatch, tmp_path):
    _fake_gh(monkeypatch, tmp_path, {"updater-heavy.yml": [
        {"status": "completed", "databaseId": 1}, {"status": "queued", "databaseId": 42092121529}]})
    why = psc.runs_in_flight()
    assert why == ["CI updater-heavy 42092121529 is queued"]


def test_a_running_daily_run_blocks(monkeypatch, tmp_path):
    _fake_gh(monkeypatch, tmp_path, {"updater-daily.yml": [{"status": "in_progress", "databaseId": 7}]})
    assert psc.runs_in_flight() == ["CI updater-daily 7 is in_progress"]


def test_a_failed_gh_call_blocks_instead_of_reading_idle(monkeypatch, tmp_path):
    _fake_gh(monkeypatch, tmp_path, {}, rc=1)
    why = psc.runs_in_flight()
    assert len(why) == 2 and all("could not check CI" in w for w in why)


def test_pending_counts_as_in_flight(monkeypatch, tmp_path):
    _fake_gh(monkeypatch, tmp_path, {"updater-heavy.yml": [{"status": "pending", "databaseId": 9}]})
    assert psc.runs_in_flight() == ["CI updater-heavy 9 is pending"]


def test_no_tool_keeps_its_own_copy():
    """AR-431: cso_repull_subject.py and repull_file.py had private copies that read updater-daily only."""
    own = []
    for d in ("tools", "jobs", "updater", "core"):
        for dirpath, _, files in os.walk(os.path.join(ROOT, d)):
            for fn in files:
                if fn.endswith(".py") and fn != "prune_series_cursors.py":
                    p = os.path.join(dirpath, fn)
                    if "def runs_in_flight" in open(p, encoding="utf-8", errors="replace").read():
                        own.append(os.path.relpath(p, ROOT))
    assert own == [], f"import runs_in_flight from tools.prune_series_cursors instead: {own}"
    for rel in ("tools/cso_repull_subject.py", "tools/repull_file.py"):
        assert "from tools.prune_series_cursors import runs_in_flight" in open(
            os.path.join(ROOT, rel), encoding="utf-8").read()


def test_the_workstation_lock_blocks(monkeypatch, tmp_path):
    _fake_gh(monkeypatch, tmp_path, {})
    (tmp_path / "logs").mkdir()
    (tmp_path / "logs" / "local_heavy.lock").write_text("1,2")
    assert any("workstation lock held" in w for w in psc.runs_in_flight())
