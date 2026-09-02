"""tools/proc_match must never match the process that asks, the shells above it, or the shell
WRAPPERS of sibling tool calls (R574, R576), and must not claim a kill it did not wait for."""
import dataclasses
import os
import subprocess
import sys
import time

import psutil

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools import proc_match  # noqa: E402


def _spawn(marker, seconds=30):
    return subprocess.Popen([sys.executable, "-c", f"import time; x='{marker}'; time.sleep({seconds})"])


def _wait_for(marker, deadline_s=10):
    deadline = time.time() + deadline_s
    hits = []
    while time.time() < deadline and not hits:
        hits = proc_match.find(marker)
        time.sleep(0.2)
    return hits


def test_ancestry_includes_self_and_parent():
    a = proc_match.ancestry_pids()
    assert os.getpid() in a


def test_a_token_our_own_command_line_carries_does_not_self_match():
    token = "test_proc_match"
    assert all(p.pid != os.getpid() for p in proc_match.find(token))


def test_finds_a_child_by_literal_token_and_not_itself():
    marker = f"PROCMATCH_MARKER_{os.getpid()}"
    child = _spawn(marker)
    try:
        assert [h.pid for h in _wait_for(marker)] == [child.pid]
    finally:
        child.kill()
        child.wait()


def test_shell_wrappers_carrying_the_token_are_never_matched():
    """R576: `bash -c "<whole command>"` wrappers of sibling tool calls carry every token."""
    marker = f"PROCMATCH_WRAP_{os.getpid()}"
    assert proc_match.is_wrapper("bash.exe", f"bash.exe -c 'echo {marker}'")
    assert proc_match.is_wrapper("python.exe", f"python.exe -c 'source C:/x/shell-snapshots/snapshot-bash-1.sh; {marker}'")
    assert not proc_match.is_wrapper("python.exe", f"python.exe -u worker.py {marker}")
    if os.name != "nt":
        # THE LIVE HALF IS WINDOWS-SHAPED, and pretending otherwise is how a green CI run
        # would stop meaning anything. The wrapper this guards is the harness's own
        # `bash.exe -c "<whole command>"` on this workstation; a Linux runner spawns a
        # different process shape, and proc_match's docstring calls itself "the only process
        # instrument to use on THIS machine". The pure is_wrapper assertions above - which are
        # the actual rule - still run everywhere.
        return
    child = subprocess.Popen(["bash.exe", "-c", f"x='{marker}'; sleep 20"])
    try:
        time.sleep(1.0)
        assert all(p.pid != child.pid for p in proc_match.find(marker))          # excluded
        assert any(p.pid == child.pid for p in proc_match.find(marker, include_wrappers=True))  # visible when asked
    finally:
        child.kill()
        child.wait()


def test_short_or_empty_token_is_refused():
    for bad in ("", "abc"):
        try:
            proc_match.find(bad)
        except ValueError:
            continue
        raise AssertionError(f"{bad!r} accepted")


def test_kill_verified_refuses_a_recycled_pid_a_wrapper_and_itself_and_waits_for_the_real_child():
    marker = f"PROCMATCH_KILL_{os.getpid()}"
    child = _spawn(marker)
    try:
        hits = _wait_for(marker)
        assert [h.pid for h in hits] == [child.pid]
        stale = dataclasses.replace(hits[0], create_time=hits[0].create_time - 100.0)
        assert proc_match.kill_verified(stale)[0] is False
        assert child.poll() is None
        me = proc_match.Proc(os.getpid(), psutil.Process().name(), " ".join(psutil.Process().cmdline()),
                             psutil.Process().create_time())
        assert proc_match.kill_verified(me)[0] is False
        wrapper = dataclasses.replace(hits[0], name="bash.exe")
        assert proc_match.kill_verified(wrapper)[0] is False                    # name check happens live: real name is python
        ok, msg = proc_match.kill_verified(hits[0])
        assert ok and msg.startswith("killed") and "waited" in msg
        assert child.poll() is not None                                          # really dead
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()


def test_cli_kill_requires_expect_and_exact_count():
    marker = f"PROCMATCH_CLI_{os.getpid()}"
    a, b = _spawn(marker), _spawn(marker)
    try:
        _wait_for(marker)
        tool = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tools", "proc_match.py")
        r = subprocess.run([sys.executable, tool, marker, "--kill"], capture_output=True, text=True)
        assert r.returncode == 2 and "REFUSING --kill without --expect" in r.stdout
        r = subprocess.run([sys.executable, tool, marker, "--kill", "--expect", "1"], capture_output=True, text=True)
        assert r.returncode == 2 and "match now, --expect 1" in r.stdout
        assert a.poll() is None and b.poll() is None
        r = subprocess.run([sys.executable, tool, marker, "--kill", "--expect", "2"], capture_output=True, text=True)
        assert r.returncode == 0, r.stdout + r.stderr
        a.wait(timeout=10); b.wait(timeout=10)
    finally:
        for c in (a, b):
            if c.poll() is None:
                c.kill(); c.wait()


def test_cli_expect_is_enforced_even_when_nothing_matches():
    """R578 minor: 'expected 2, found 0' is a refusal (exit 2), not a not-found (exit 1)."""
    tool = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tools", "proc_match.py")
    r = subprocess.run([sys.executable, tool, f"PROCMATCH_NONE_{os.getpid()}", "--kill", "--expect", "2"],
                       capture_output=True, text=True)
    assert r.returncode == 2 and "0 worker process(es) match now, --expect 2" in r.stdout


def test_cli_expect_zero_is_the_nothing_running_assertion():
    """R579 minor: '--expect 0' with 0 matches must succeed (exit 0), not report not-found."""
    tool = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tools", "proc_match.py")
    r = subprocess.run([sys.executable, tool, f"PROCMATCH_ZERO_{os.getpid()}", "--expect", "0"],
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stdout
