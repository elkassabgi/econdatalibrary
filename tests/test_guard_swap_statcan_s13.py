"""tools/machine/swap_guard_statcan_s13.ps1 - the S13 install of the post-T0 guard (T0 item B3), run in REAL Windows
PowerShell 5.1 on a fake tree. Every refusal leaves the live file untouched; the happy path backs up, swaps and
verifies; -DryRun prints and writes nothing. Windows only (the script is the workstation's)."""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPT = os.path.join(ROOT, "tools", "machine", "swap_guard_statcan_s13.ps1")
PRE = os.path.join(ROOT, "tools", "machine", "RELAUNCH_GUARD.ps1.workstation-copy")
POST = os.path.join(ROOT, "tools", "machine", "RELAUNCH_GUARD.ps1.post-t0-copy")

pytestmark = pytest.mark.skipif(os.name != "nt" or shutil.which("powershell") is None,
                                reason="Windows PowerShell runs the S13 swap")


def _sha(p) -> str:
    return hashlib.sha256(open(p, "rb").read()).hexdigest().upper()


@pytest.fixture
def tree(tmp_path):
    root = tmp_path / "econ"
    (root / "tools" / "machine").mkdir(parents=True)
    shutil.copyfile(PRE, root / "RELAUNCH_GUARD.ps1")                       # the live guard before S13
    shutil.copyfile(POST, root / "tools" / "machine" / "RELAUNCH_GUARD.ps1.post-t0-copy")
    (tmp_path / "CUTOVER").write_text("")
    (tmp_path / "procs.json").write_text("[]")
    return tmp_path


def _run(t, live_sha=None, copy_sha=None, dry=False, procs=None, flag=True):
    root = t / "econ"
    if procs is not None:
        (t / "procs.json").write_text(json.dumps(procs))
    args = ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", SCRIPT,
            "-ExpectLiveSha", live_sha or _sha(PRE), "-ExpectCopySha", copy_sha or _sha(POST),
            "-Root", str(root), "-FlagPath", str(t / ("CUTOVER" if flag else "NO_FLAG")),
            "-BackupDir", str(t / "backups"), "-ProcessListJson", str(t / "procs.json")]
    if dry:
        args.append("-DryRun")
    return subprocess.run(args, capture_output=True, text=True, timeout=300)


def test_the_script_is_ascii():
    assert all(b < 128 for b in open(SCRIPT, "rb").read()), "Windows PowerShell 5.1 reads a BOM-less .ps1 as ANSI"


def test_the_happy_path_backs_up_swaps_and_verifies(tree):
    live = tree / "econ" / "RELAUNCH_GUARD.ps1"
    r = _run(tree)
    assert r.returncode == 0, (r.stdout, r.stderr)
    assert _sha(live) == _sha(POST) and "SWAPPED" in r.stdout
    baks = list((tree / "backups").glob("RELAUNCH_GUARD.ps1.bak-*"))
    assert len(baks) == 1 and _sha(baks[0]) == _sha(PRE)
    assert "ROLLBACK if needed" in r.stdout
    assert not list((tree / "econ").glob("RELAUNCH_GUARD.tmp-*")), "no temp file left"
    assert (tree / "backups" / "swap_guard_statcan_s13.log").exists()


def test_the_dry_run_prints_every_check_and_writes_nothing(tree):
    live = tree / "econ" / "RELAUNCH_GUARD.ps1"
    r = _run(tree, dry=True)
    assert r.returncode == 0, (r.stdout, r.stderr)
    for n in (1, 2, 3, 4, 5, 6):
        assert f"check {n} ok" in r.stdout, r.stdout
    assert "DRY RUN: would back up" in r.stdout and "nothing was written" in r.stdout
    assert _sha(live) == _sha(PRE) and not (tree / "backups").exists()


@pytest.mark.parametrize("case", ["no_flag", "guard_loop", "old_lane", "live_sha", "copy_sha", "bad_sha_arg",
                                  "copy_launches_old", "copy_unparseable", "live_differs_elsewhere", "done_flag",
                                  "live_is_not_pre_t0"])
def test_every_premise_refuses_and_leaves_the_live_guard_untouched(tree, case):
    live = tree / "econ" / "RELAUNCH_GUARD.ps1"
    copy = tree / "econ" / "tools" / "machine" / "RELAUNCH_GUARD.ps1.post-t0-copy"
    kw, want_rc, want = {}, 1, None
    if case == "no_flag":
        kw["flag"], want = False, "does not exist"
    elif case == "guard_loop":
        kw["procs"] = [{"ProcessId": 4242, "Name": "powershell.exe",
                        "CommandLine": 'powershell.exe -File "E:\\research\\econfindatalibrary\\RELAUNCH_GUARD_LOOP.ps1"'}]
        want = "the guard loop is running"
    elif case == "old_lane":
        kw["procs"] = [{"ProcessId": 94008, "Name": "python.exe", "CommandLine": "python.exe -u jobs/statcan_lane.py"}]
        want = "the pre-T0 statcan lane is running"
    elif case == "live_sha":
        kw["live_sha"], want_rc, want = "0" * 64, 5, "NOT SWAPPED"
    elif case == "copy_sha":
        kw["copy_sha"], want = "A" * 64, "post-T0 copy's sha256"
    elif case == "bad_sha_arg":
        kw["live_sha"], want = "xyz", "not 64 hex"
    elif case == "copy_launches_old":
        text = open(POST, encoding="utf-8-sig").read().replace("'jobs/statcan_selfhost_lane.py'", "'jobs/statcan_lane.py'")
        copy.write_bytes(b"\xef\xbb\xbf" + text.encode("utf-8"))
        kw["copy_sha"], want = _sha(copy), "does not launch jobs/statcan_selfhost_lane.py"
    elif case == "copy_unparseable":
        copy.write_bytes(open(POST, "rb").read() + b"\r\nif ( {\r\n")
        kw["copy_sha"], want = _sha(copy), "does not parse"
    elif case == "live_differs_elsewhere":
        # a live-only edit outside the statcan entry: the swap would revert it silently (design review C10)
        live.write_bytes(open(PRE, "rb").read().replace(b"$STALL_HOURS = 3", b"$STALL_HOURS = 4"))
        kw["live_sha"], want = _sha(live), "differ OUTSIDE the statcan entry"
    elif case == "done_flag":
        (tree / "econ" / "logs").mkdir()
        (tree / "econ" / "logs" / "statcan_selfhost.DONE").write_text("x")
        want = "statcan_selfhost.DONE exists"
    elif case == "live_is_not_pre_t0":
        live.write_bytes(open(PRE, "rb").read().replace(b"'jobs/statcan_lane.py') }", b"'jobs/other_lane.py') }"))
        kw["live_sha"], want = _sha(live), "is not the pre-T0 guard"
    before = _sha(live)
    r = _run(tree, **kw)
    assert r.returncode == want_rc, (case, r.returncode, r.stdout, r.stderr)
    assert want in r.stdout, (case, r.stdout)
    assert _sha(live) == before, "the live guard is untouched"
    assert not list((tree / "backups").glob("RELAUNCH_GUARD.ps1.bak-*")) if (tree / "backups").exists() else True


def test_a_selfhost_lane_process_is_not_the_old_lane(tree):
    """'*statcan_lane.py*' must not match the post-T0 lane's command line (the names were chosen for that)."""
    r = _run(tree, dry=True, procs=[{"ProcessId": 1, "Name": "python.exe",
                                     "CommandLine": "python.exe -u jobs/statcan_selfhost_lane.py"}])
    assert r.returncode == 0, (r.stdout, r.stderr)
