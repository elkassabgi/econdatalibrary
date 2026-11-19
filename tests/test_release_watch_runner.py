"""The daily release-watch runner: tools/selfhost/run_release_watch.ps1, started by the guard loop with -IfDue.

Release-watch design Revision 5, B3 (a copy of run_notices.ps1's shape) and C1 (the runner's status file is what
the digest's LATE rule reads). Static checks run everywhere; the -IfDue rules run on Windows only, against a COPY
of the script in a scratch root with a fake job, so logs/ of the checkout is never touched and nothing probes."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PS1 = os.path.join(ROOT, "tools", "selfhost", "run_release_watch.ps1")
NOTICES_PS1 = os.path.join(ROOT, "tools", "selfhost", "run_notices.ps1")
GUARD_COPY = os.path.join(ROOT, "tools", "machine", "RELAUNCH_GUARD.ps1.workstation-copy")
POWERSHELL = shutil.which("powershell.exe") if os.name == "nt" else None


def test_the_runner_is_ascii_and_runs_only_the_release_watch():
    raw = open(PS1, "rb").read()
    assert all(b < 128 for b in raw), "PS 5.1 reads a BOM-less .ps1 as ANSI: keep it ASCII"
    src = raw.decode("ascii")
    assert "@('-B', '-u', '-m', 'updater.release_watch', '--desktop')" in src
    assert "PriorityClass = 'BelowNormal'" in src
    assert "[double] $LimitMin = 15" in src and "$rc = 96" in src
    assert "'release_watch.last.json'" in src and "'release_watch.lock'" in src
    assert "CUTOVER" not in src, "the watch never touches the updater state: no T0 gate"


def test_the_runner_is_run_notices_shape_line_for_line():
    """Only the header, the names, the job and the limit differ from run_notices.ps1 (B3)."""
    mine = open(PS1, encoding="ascii").read().split("#>\n", 1)[1].splitlines()
    theirs = open(NOTICES_PS1, encoding="ascii").read().split("#>\n", 1)[1].splitlines()
    import difflib
    d = [l for l in difflib.unified_diff(theirs, mine, lineterm="", n=0) if not l.startswith(("---", "+++", "@@"))]
    removed, added = [l for l in d if l.startswith("-")], [l for l in d if l.startswith("+")]
    assert len(removed) == 11 and len(added) == 12, d


def test_the_guard_copy_starts_the_runner_with_if_due_and_no_redirect():
    lines = [l.strip() for l in open(GUARD_COPY, encoding="utf-8-sig")]
    code = [l for l in lines if l and not l.startswith("#")]
    i = next(n for n, l in enumerate(code) if l.startswith("$releaseWatch = ") and "run_release_watch.ps1" in l)
    block = " ".join(code[i:i + 6])
    assert "-File',$releaseWatch,'-IfDue')" in block, "without -IfDue it would run every 5 minutes"
    assert "Redirect" not in block, "the runner writes its own log; a redirect leaves empty files every tick"
    assert "CUTOVER" not in block, "no T0 gate"


def _scratch(tmp_path, job_rc, sleep_s=0):
    root = tmp_path / "root"
    (root / "tools" / "selfhost").mkdir(parents=True)
    shutil.copy(PS1, root / "tools" / "selfhost" / "run_release_watch.ps1")
    (root / "job.py").write_text(f"import os, time\nprint('job ran', os.getpid(), flush=True)\n"
                                 f"time.sleep({sleep_s})\nraise SystemExit({job_rc})\n", encoding="ascii")
    return root


def _run(root, *args):
    cmd = [POWERSHELL, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File",
           str(root / "tools" / "selfhost" / "run_release_watch.ps1"), *args,
           "-Python", sys.executable, "-Job", str(root / "job.py")]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    st = root / "logs" / "release_watch.last.json"
    return r.returncode, (json.loads(st.read_text(encoding="ascii")) if st.exists() else None)


@pytest.mark.skipif(not POWERSHELL, reason="Windows PowerShell only")
def test_if_due_rules_and_the_status_the_digest_reads(tmp_path):
    root = _scratch(tmp_path, 0)
    assert _run(root, "-IfDue", "-NowUtc", "2026-11-20T08:59:00Z") == (0, None), "before 09:00 UTC: nothing"
    rc, st = _run(root, "-IfDue", "-NowUtc", "2026-11-20T09:00:00Z")
    assert rc == 0 and st["rc"] == 0 and st["started"] == st["last_ok_started"] == "2026-11-20T09:00:00Z"
    logs = sorted(p.name for p in (root / "logs").glob("release_watch_*.log"))
    assert logs == ["release_watch_20261120T090000Z.log"]
    _run(root, "-IfDue", "-NowUtc", "2026-11-20T15:00:00Z")
    assert len(list((root / "logs").glob("release_watch_*.log"))) == 1, "today's run already succeeded"
    assert not (root / "logs" / "release_watch.lock").exists(), "the lock is released"


@pytest.mark.skipif(not POWERSHELL, reason="Windows PowerShell only")
def test_a_hung_run_is_killed_and_recorded_rc_96_with_the_old_ok_kept(tmp_path):
    root = _scratch(tmp_path, 0, sleep_s=120)
    rc, st = _run(root, "-IfDue", "-NowUtc", "2026-11-20T09:00:00Z", "-LimitMin", "0.1")
    assert rc == 96 and st["rc"] == 96 and st["last_ok_started"] is None and "killed after" in st["note"]
    assert not (root / "logs" / "release_watch.lock").exists()


@pytest.mark.skipif(not POWERSHELL, reason="Windows PowerShell only")
def test_started_is_written_before_python_runs(tmp_path):
    """C1: the status names the run (started, ended null) before the first probe, so a run that dies at once is
    still on record. The fake job reads the status file while it runs."""
    root = _scratch(tmp_path, 0)
    (root / "job.py").write_text("import os\nprint('STATUS', open(os.path.join('logs', 'release_watch.last.json'), "
                                 "encoding='ascii').read().strip(), flush=True)\n", encoding="ascii")
    rc, st = _run(root, "-IfDue", "-NowUtc", "2026-11-20T09:00:00Z")
    log = next((root / "logs").glob("release_watch_*.log")).read_text(encoding="utf-8")
    during = json.loads(log.split("STATUS", 1)[1].strip().splitlines()[0])
    assert during["started"] == "2026-11-20T09:00:00Z" and during["ended"] is None and during["rc"] is None
    assert rc == 0 and st["ended"] is not None


@pytest.mark.skipif(not POWERSHELL, reason="Windows PowerShell only")
def test_a_failed_run_is_retried_after_two_hours_and_keeps_the_old_ok(tmp_path):
    root = _scratch(tmp_path, 3)
    rc, st = _run(root, "-IfDue", "-NowUtc", "2026-11-20T09:00:00Z")
    assert rc == 3 and st["rc"] == 3 and st["last_ok_started"] is None
    _run(root, "-IfDue", "-NowUtc", "2026-11-20T10:59:00Z")
    assert len(list((root / "logs").glob("release_watch_*.log"))) == 1, "within 2 h of the attempt: nothing"
    _run(root, "-IfDue", "-NowUtc", "2026-11-20T11:00:00Z")
    assert len(list((root / "logs").glob("release_watch_*.log"))) == 2, "2 h later: retried"
