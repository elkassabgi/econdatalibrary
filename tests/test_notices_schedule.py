"""The daily publisher-notice run: tools/selfhost/run_notices.ps1, started by the guard loop with -IfDue.

Static checks run everywhere. The -IfDue rules are exercised on Windows only (Windows PowerShell 5.1 is what
the guard runs), against a COPY of the script in a scratch root, so logs/ of the checkout is never touched."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PS1 = os.path.join(ROOT, "tools", "selfhost", "run_notices.ps1")
GUARD_COPY = os.path.join(ROOT, "tools", "machine", "RELAUNCH_GUARD.ps1.workstation-copy")
POWERSHELL = shutil.which("powershell.exe") if os.name == "nt" else None


def test_the_runner_is_ascii_and_runs_only_the_watcher():
    raw = open(PS1, "rb").read()
    assert all(b < 128 for b in raw), "PS 5.1 reads a BOM-less .ps1 as ANSI: keep it ASCII"
    src = raw.decode("ascii")
    assert "[string] $Job    = 'jobs\\watch_notices.py'" in src
    assert "PriorityClass = 'BelowNormal'" in src
    assert "CUTOVER" not in src, "the watcher never touches the updater state: no T0 gate"


def test_the_guard_copy_starts_the_runner_with_if_due_and_no_redirect():
    lines = [l.strip() for l in open(GUARD_COPY, encoding="utf-8-sig")]
    code = [l for l in lines if l and not l.startswith("#")]
    i = next(n for n, l in enumerate(code) if l.startswith("$notices = ") and "run_notices.ps1" in l)
    block = " ".join(code[i:i + 6])
    assert "-File',$notices,'-IfDue')" in block, "without -IfDue it would run every 5 minutes"
    assert "Redirect" not in block, "the runner writes its own log; a redirect leaves empty files every tick"


def _scratch(tmp_path, job_rc):
    root = tmp_path / "root"
    (root / "tools" / "selfhost").mkdir(parents=True)
    shutil.copy(PS1, root / "tools" / "selfhost" / "run_notices.ps1")
    (root / "job.py").write_text(f"print('job ran')\nraise SystemExit({job_rc})\n", encoding="ascii")
    return root


def _run(root, *args):
    r = subprocess.run([POWERSHELL, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File",
                        str(root / "tools" / "selfhost" / "run_notices.ps1"), *args,
                        "-Python", sys.executable, "-Job", str(root / "job.py")],
                       capture_output=True, text=True, timeout=120)
    st = root / "logs" / "notices.last.json"
    return r.returncode, (json.loads(st.read_text(encoding="ascii")) if st.exists() else None)


@pytest.mark.skipif(not POWERSHELL, reason="Windows PowerShell only")
def test_if_due_rules(tmp_path):
    root = _scratch(tmp_path, 0)
    assert _run(root, "-IfDue", "-NowUtc", "2026-11-06T08:59:00Z") == (0, None), "before 09:00 UTC: nothing"
    rc, st = _run(root, "-IfDue", "-NowUtc", "2026-11-06T09:00:00Z")
    assert rc == 0 and st["rc"] == 0 and st["last_ok_started"] == "2026-11-06T09:00:00Z"
    logs = sorted(p.name for p in (root / "logs").glob("notices_*.log"))
    assert logs == ["notices_20261106T090000Z.log"]
    assert "job ran" in (root / "logs" / logs[0]).read_text(encoding="utf-8")
    _run(root, "-IfDue", "-NowUtc", "2026-11-06T15:00:00Z")
    assert len(list((root / "logs").glob("notices_*.log"))) == 1, "today's run already succeeded"
    rc, st = _run(root, "-IfDue", "-NowUtc", "2026-11-07T09:05:00Z")
    assert st["started"] == "2026-11-07T09:05:00Z", "a new UTC day is due again"
    assert not (root / "logs" / "notices.lock").exists(), "the lock is released"


@pytest.mark.skipif(not POWERSHELL, reason="Windows PowerShell only")
def test_a_failed_run_is_retried_after_two_hours_not_every_tick(tmp_path):
    root = _scratch(tmp_path, 1)
    rc, st = _run(root, "-IfDue", "-NowUtc", "2026-11-06T09:00:00Z")
    assert rc == 1 and st["rc"] == 1 and st["last_ok_started"] is None
    _run(root, "-IfDue", "-NowUtc", "2026-11-06T10:59:00Z")
    assert len(list((root / "logs").glob("notices_*.log"))) == 1, "within 2 h of the attempt: nothing"
    _run(root, "-IfDue", "-NowUtc", "2026-11-06T11:00:00Z")
    assert len(list((root / "logs").glob("notices_*.log"))) == 2, "2 h later: retried"
