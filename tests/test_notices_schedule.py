"""The daily publisher-notice run: tools/selfhost/run_notices.ps1, started by the guard loop with -IfDue.

Static checks run everywhere. The -IfDue rules are exercised on Windows only (Windows PowerShell 5.1 is what
the guard runs), against a COPY of the script in a scratch root, so logs/ of the checkout is never touched."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time

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


def _scratch(tmp_path, job_rc, sleep_s=0):
    root = tmp_path / "root"
    (root / "tools" / "selfhost").mkdir(parents=True)
    shutil.copy(PS1, root / "tools" / "selfhost" / "run_notices.ps1")
    (root / "job.py").write_text(f"import os, time\nprint('job ran', os.getpid(), flush=True)\n"
                                 f"time.sleep({sleep_s})\nraise SystemExit({job_rc})\n", encoding="ascii")
    return root


def _cmd(root, *args):
    return [POWERSHELL, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File",
            str(root / "tools" / "selfhost" / "run_notices.ps1"), *args,
            "-Python", sys.executable, "-Job", str(root / "job.py")]


def _run(root, *args):
    r = subprocess.run(_cmd(root, *args), capture_output=True, text=True, timeout=120)
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


def _pid_alive(pid):
    out = subprocess.run(["tasklist", "/FI", f"PID eq {pid}", "/NH"], capture_output=True, text=True).stdout
    return str(pid) in out


@pytest.mark.skipif(not POWERSHELL, reason="Windows PowerShell only")
def test_a_hung_run_is_killed_with_its_tree_at_the_limit(tmp_path):
    """AR-435: no limit meant a hung watcher held the lock until a reboot, unreported."""
    root = _scratch(tmp_path, 0, sleep_s=120)
    rc, st = _run(root, "-IfDue", "-NowUtc", "2026-11-06T09:00:00Z", "-LimitMin", "0.1")
    assert rc == 96 and st["rc"] == 96 and st["last_ok_started"] is None
    assert "killed after" in st["note"], "the runner's word is in the status record (AR-438)"
    log = next((root / "logs").glob("notices_*.log")).read_text(encoding="utf-8")
    child = int(log.split("job ran", 1)[1].split()[0])
    assert not _pid_alive(child), "the python child was killed too"
    assert not (root / "logs" / "notices.lock").exists()


@pytest.mark.skipif(not POWERSHELL, reason="Windows PowerShell only")
def test_an_empty_lock_is_held_while_new_and_stale_when_old(tmp_path):
    """AR-435 r2: Set-Content truncates then writes, so a reader can see an empty line; that must not read as free."""
    root = _scratch(tmp_path, 0)
    (root / "logs").mkdir()
    lock = root / "logs" / "notices.lock"
    lock.write_text("", encoding="ascii")
    rc, st = _run(root, "-NowUtc", "2026-11-06T09:00:00Z")
    assert rc == 3 and st is None, "a lock written seconds ago is another run starting"
    old = time.time() - 60
    os.utime(lock, (old, old))
    rc, st = _run(root, "-NowUtc", "2026-11-06T09:00:00Z")
    assert rc == 0 and st["rc"] == 0 and not lock.exists(), "an empty lock a minute old is stale"


@pytest.mark.skipif(not POWERSHELL, reason="Windows PowerShell only")
def test_a_python_that_survives_the_kill_keeps_the_lock_and_says_why(tmp_path):
    """AR-438: run the real rc 95 path (kill skipped with the test-only -NoKill), not a grep of the source."""
    root = _scratch(tmp_path, 0, sleep_s=60)
    try:
        # no captured output: the surviving python inherits the runner's pipes, and a capture would wait for it
        rc = subprocess.run(_cmd(root, "-IfDue", "-NowUtc", "2026-11-06T09:00:00Z", "-LimitMin", "0.05", "-NoKill",
                                 "-KillWaitS", "1"), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                            timeout=60).returncode
        st = json.loads((root / "logs" / "notices.last.json").read_text(encoding="ascii"))
        assert rc == 95 and st["rc"] == 95 and "could NOT kill python" in st["note"]
        lock = root / "logs" / "notices.lock"
        assert lock.exists() and len(lock.read_text(encoding="ascii").strip().split(",")) == 4, "lock kept"
        r = subprocess.run(_cmd(root, "-NowUtc", "2026-11-06T09:30:00Z"), stdout=subprocess.PIPE,
                           stderr=subprocess.DEVNULL, text=True, timeout=60)
        assert r.returncode == 3 and "refused" in r.stdout, "the surviving python still holds the lock"
    finally:
        log = next((root / "logs").glob("notices_*.log")).read_text(encoding="utf-8")
        child = int(log.split("job ran", 1)[1].split()[0])
        subprocess.run(["taskkill", "/F", "/PID", str(child)], capture_output=True)


@pytest.mark.skipif(not POWERSHELL, reason="Windows PowerShell only")
def test_a_live_run_holds_the_lock(tmp_path):
    root = _scratch(tmp_path, 0, sleep_s=25)
    first = subprocess.Popen(_cmd(root, "-NowUtc", "2026-11-06T09:00:00Z"),
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        lock = root / "logs" / "notices.lock"
        for _ in range(100):                                      # wait until the python child is in the lock
            if lock.exists() and len(lock.read_text(encoding="ascii").strip().split(",")) == 4:
                break
            time.sleep(1)
        assert len(lock.read_text(encoding="ascii").strip().split(",")) == 4, "runner and child are recorded"
        r = subprocess.run(_cmd(root, "-NowUtc", "2026-11-06T09:01:00Z"), capture_output=True, text=True,
                           timeout=60)
        assert r.returncode == 3 and "refused" in r.stdout
        r = subprocess.run(_cmd(root, "-IfDue", "-NowUtc", "2026-11-06T12:00:00Z"), capture_output=True,
                           text=True, timeout=60)
        assert r.returncode == 0 and len(list((root / "logs").glob("notices_*.log"))) == 1, "-IfDue: silent"
    finally:
        first.wait(timeout=120)
    assert first.returncode == 0
