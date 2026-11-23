"""The guard loop leaves a line when it ends, and its launcher starts it without a Windows Terminal window.

WHY. The econ guard loop (the live E:\\research\\econfindatalibrary\\RELAUNCH_GUARD_LOOP.ps1, versioned as
tools/machine/RELAUNCH_GUARD_LOOP.ps1.workstation-copy) died twice with no line anywhere: on 2026-08-02
(ledger R260) and on 2026-11-21 between 21:22 and 21:29Z, so neither cause could be measured. The loop now
writes "loop starting", "loop trap" and "loop exiting" lines to logs/_guard_loop.log. Its launcher
(tools/machine/EconGuard.cmd.startup-copy) used `start "" powershell ...`, which hands the console to
Windows Terminal (R2031); it now starts the loop through Win32_Process.Create with a hidden window.

WHAT RUNS HERE. The SHIPPED loop copy, under the PowerShell engine this machine has (Windows PowerShell 5.1
on the workstation; pwsh 7 on the ubuntu CI runner - a different engine, so CI proves the shape, the
workstation proves 5.1), with only its constants swapped, each swap matched exactly once:
  - the root, to a temporary folder;
  - the pinned python, to a stub script, so no test ever publishes a heartbeat (a fake beat is R260);
  - the 300 s sleep, to a hook that plants three terminating errors on ticks 1-3 (a .NET method exception,
    a `throw`, an error inside a catch block; GetEncoding is used because its message carries the planted
    name on both .NET runtimes) and ends the run with `exit 0` on tick 5;
  - the trap's 60 s wait, to 50 ms.
The plan review (F:/work/maintenance/20261123_guard_exit_trace/review/VERDICT_plan.md) measured that a trap
at SCRIPT scope around the try/finally ENDS the loop on the first error; this test fails on that shape.
On CI a missing engine is a FAILURE, not a skip: a skip is not a pass.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOOP_COPY = os.path.join(ROOT, "tools", "machine", "RELAUNCH_GUARD_LOOP.ps1.workstation-copy")
LAUNCHER_COPY = os.path.join(ROOT, "tools", "machine", "EconGuard.cmd.startup-copy")
LOOP_FILE_LITERAL = '-File "E:\\research\\econfindatalibrary\\RELAUNCH_GUARD_LOOP.ps1"'
LOOP_CMD = "powershell -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden " + LOOP_FILE_LITERAL

HOOK = """Start-Sleep -Milliseconds 100
  $script:tick++
  Add-Content -Path $loopLog -Value ("tick $($script:tick) done") -Encoding utf8
  if ($script:tick -eq 1) { [System.Text.Encoding]::GetEncoding('planted-dotnet') | Out-Null }
  if ($script:tick -eq 2) { throw 'planted-throw' }
  if ($script:tick -eq 3) { try { throw 'inner' } catch { [System.Text.Encoding]::GetEncoding('planted-in-catch') | Out-Null } }
  if ($script:tick -ge 5) { exit 0 }"""


def engine():
    return shutil.which("powershell") or shutil.which("pwsh")


def swap(text: str, old: str, new: str) -> str:
    n = text.count(old)
    assert n == 1, (f"{old!r} occurs {n} times in the loop copy; the test would not run what it claims "
                    f"(a missed swap could run the real 300 s sleep, the real root or the real python)")
    return text.replace(old, new)


def loop_under_test(text: str, root: str, hook: str = HOOK, trap_wait_ms: int = 50) -> str:
    """The shipped loop text with only its constants swapped (each exactly once)."""
    t = swap(text, "$root  = 'E:\\research\\econfindatalibrary'", "$root  = '" + root.replace("'", "''") + "'")
    t = swap(t, '$python = "C:\\Users\\aelkassabgi\\AppData\\Local\\Programs\\Python\\Python314\\python.exe"',
             "$python = Join-Path $root 'publish_stub.ps1'")
    t = swap(t, "Start-Sleep -Seconds 300", hook)
    t = swap(t, "[System.Threading.Thread]::Sleep(60000)", f"[System.Threading.Thread]::Sleep({trap_wait_ms})")
    return t


def prepare(tmp: str, text: str, hook: str = HOOK) -> str:
    """Writes the stubs and the loop under test into tmp; returns the loop file's path."""
    os.makedirs(os.path.join(tmp, "logs"), exist_ok=True)
    with open(os.path.join(tmp, "RELAUNCH_GUARD.ps1"), "w", encoding="ascii", newline="\r\n") as fh:
        fh.write("Add-Content -Path (Join-Path $PSScriptRoot 'guard_ran.txt') -Value 'guard' -Encoding utf8\n")
    with open(os.path.join(tmp, "publish_stub.ps1"), "w", encoding="ascii", newline="\r\n") as fh:
        fh.write("Add-Content -Path (Join-Path $PSScriptRoot 'publish_ran.txt') -Value ($args -join ' ') -Encoding utf8\n")
    p = os.path.join(tmp, "loop_under_test.ps1")
    with open(p, "w", encoding="ascii", newline="\r\n") as fh:
        fh.write(loop_under_test(text, tmp, hook))
    return p


def find_log(tmp: str) -> str:
    """logs\\_guard_loop.log; on pwsh/Linux a backslash may be kept in the name, so look for both."""
    for cand in (os.path.join(tmp, "logs", "_guard_loop.log"), os.path.join(tmp, "logs\\_guard_loop.log")):
        if os.path.exists(cand):
            return cand
    raise AssertionError(f"no _guard_loop.log under {tmp}: {sorted(os.listdir(tmp))}")


def run_loop(tmp: str, text: str, hook: str = HOOK, timeout: int = 240):
    exe = engine()
    if exe is None:
        if os.environ.get("GITHUB_ACTIONS"):
            pytest.fail("no PowerShell engine on the CI runner: this test must run there, a skip is not a pass")
        pytest.skip("no PowerShell engine (powershell or pwsh) on this machine")
    p = prepare(tmp, text, hook)
    r = subprocess.run([exe, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", p],
                       capture_output=True, timeout=timeout, cwd=tmp)
    with open(find_log(tmp), encoding="utf-8-sig", errors="replace") as fh:
        lines = fh.read().splitlines()
    return r, lines, exe


def _count(path: str) -> int:
    if not os.path.exists(path):
        return 0
    with open(path, encoding="utf-8-sig", errors="replace") as fh:
        return len([ln for ln in fh.read().splitlines() if ln.strip()])


def _index(lines, needle):
    for i, ln in enumerate(lines):
        if needle in ln:
            return i
    raise AssertionError(f"no line contains {needle!r}:\n" + "\n".join(lines))


def test_trapped_errors_keep_the_loop_and_the_exit_leaves_a_line(tmp_path):
    with open(LOOP_COPY, encoding="ascii") as fh:
        text = fh.read()
    tmp = str(tmp_path)
    r, lines, exe = run_loop(tmp, text)
    out = (r.stdout or b"").decode("utf-8", "replace") + (r.stderr or b"").decode("utf-8", "replace")
    detail = f"engine {exe}, rc {r.returncode}\nlog:\n" + "\n".join(lines) + "\noutput:\n" + out[-3000:]

    assert r.returncode == 0, detail
    start = _index(lines, "loop starting pid ")
    assert start == 0, detail
    # Each planted error is trapped, named, and followed by a completed tick of the SAME loop.
    t1, t2, t3, t4, t5 = (_index(lines, f"tick {n} done") for n in range(1, 6))
    e1 = _index(lines, "planted-dotnet")
    e2 = _index(lines, "planted-throw")
    e3 = _index(lines, "planted-in-catch")
    assert t1 < e1 < t2 < e2 < t3 < e3 < t4 < t5, detail
    for e in (e1, e2, e3):
        assert re.search(r"loop trap pid \d+: \S+: ", lines[e]), detail
        assert "the loop goes on in 60 s" in lines[e], detail
    assert len([ln for ln in lines if "loop trap pid" in ln]) == 3, detail
    # The exit leaves the last line, and it names the last trapped error.
    last = lines[-1]
    assert re.search(r"loop exiting pid \d+ \(started \S+; last trapped error: \S+ .*planted-in-catch", last), detail
    pid_start = re.search(r"loop starting pid (\d+)", lines[start]).group(1)
    assert f"loop exiting pid {pid_start} " in last, detail
    # Five whole ticks ran: the guard job, the beat and the publish job on every one.
    assert _count(os.path.join(tmp, "guard_ran.txt")) == 5, detail
    assert _count(os.path.join(tmp, "publish_ran.txt")) == 5, detail
    with open(os.path.join(tmp, "publish_ran.txt"), encoding="utf-8-sig") as fh:
        assert fh.read().split("\n")[0].strip() == "tools/guard_heartbeat.py --publish", detail


def test_the_loop_body_still_has_the_bounded_tick():
    """What the exit trace must NOT change: the 300 s tick, the 120 s guard bound, the beat, the 90 s publish."""
    with open(LOOP_COPY, encoding="ascii") as fh:
        text = fh.read()
    for must in ("Start-Sleep -Seconds 300", "Wait-Job $j -Timeout 120", "Set-Content -Path $beat -Value $ts",
                 "& $p 'tools/guard_heartbeat.py' '--publish'", "Wait-Job $hb -Timeout 90"):
        assert text.count(must) == 1, must
    # The trap is the first statement INSIDE the loop, not at script scope (a script-scope trap ends the loop).
    assert re.search(r"try \{\r?\nwhile \(\$true\) \{\r?\n  trap \{", text), "trap is not the first statement in the loop"
    assert text.count("trap {") == 1
    assert re.search(r"\r?\n\} finally \{\r?\n", text), "no outer finally"


def _launcher():
    with open(LAUNCHER_COPY, "rb") as fh:
        raw = fh.read()
    return raw, raw.decode("ascii")


def _command_body(text: str) -> str:
    ps = [ln for ln in text.splitlines() if ln.startswith("powershell ")]
    assert len(ps) == 1, ps
    m = re.fullmatch(r'powershell -NoProfile -ExecutionPolicy Bypass -Command "(.*)"', ps[0])
    assert m, ps[0]
    return m.group(1)


# Functions win over cmdlets in PowerShell's command lookup, so these shadow the two CIM cmdlets: the
# launcher's own -Command body runs unchanged, with no WMI call, on any engine (CI included).
SHADOW = """function New-CimInstance { param([string]$ClassName, [switch]$ClientOnly, $Property)
  [pscustomobject]@{ ClassName = $ClassName; ShowWindow = $Property.ShowWindow } }
function Invoke-CimMethod { param([string]$ClassName, [string]$MethodName, $Arguments)
  Set-Content -Path $env:TEST_CALL_FILE -Encoding utf8 -Value ($ClassName + '|' + $MethodName + '|' + $Arguments.CommandLine + '|' + $Arguments.CurrentDirectory + '|' + $Arguments.ProcessStartupInformation.ShowWindow)
  switch ($env:TEST_CIM_MODE) {
    'ok' { [pscustomobject]@{ ReturnValue = 0; ProcessId = 4242 } }
    'rv8' { [pscustomobject]@{ ReturnValue = 8; ProcessId = $null } }
    'none' { $null }
    'throw' { throw 'planted-wmi-failure' }
  } }
"""


@pytest.mark.parametrize("mode, rc, text", [
    ("ok", 0, "launcher EconGuard.cmd: Win32_Process.Create returned 0, loop pid 4242"),
    ("rv8", 2, "launcher EconGuard.cmd: Win32_Process.Create returned 8, falling back to start (a Windows Terminal window)"),
    ("none", 2, "launcher EconGuard.cmd: Win32_Process.Create gave no result, falling back to start (a Windows Terminal window)"),
    ("throw", 2, "launcher EconGuard.cmd: Win32_Process.Create threw planted-wmi-failure, falling back to start (a Windows Terminal window)"),
])
def test_the_launcher_exit_code_decides_the_fallback(tmp_path, mode, rc, text):
    """The .cmd falls back to `start ""` on `if errorlevel 1`: only a Create that returned 0 may exit 0, and
    every other outcome must exit non-zero (a 0 there means no loop at all), each with ONE log line."""
    exe = engine()
    if exe is None:
        if os.environ.get("GITHUB_ACTIONS"):
            pytest.fail("no PowerShell engine on the CI runner: this test must run there, a skip is not a pass")
        pytest.skip("no PowerShell engine (powershell or pwsh) on this machine")
    _raw, launcher = _launcher()
    log = str(tmp_path / "launcher.log")
    body = swap(_command_body(launcher), "'E:\\research\\econfindatalibrary\\logs\\_guard_loop.log'",
                "'" + log.replace("'", "''") + "'")
    env = dict(os.environ, ECON_GUARD_LOOP_CMD=LOOP_CMD, TEST_CIM_MODE=mode,
               TEST_CALL_FILE=str(tmp_path / "call.txt"))
    r = subprocess.run([exe, "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", SHADOW + body],
                       capture_output=True, timeout=120, env=env, cwd=str(tmp_path))
    out = (r.stdout or b"").decode("utf-8", "replace") + (r.stderr or b"").decode("utf-8", "replace")
    with open(log, encoding="utf-8-sig", errors="replace") as fh:
        lines = [ln for ln in fh.read().splitlines() if ln.strip()]
    assert r.returncode == rc, f"engine {exe}: rc {r.returncode}, want {rc}\n{lines}\n{out[-2000:]}"
    assert len(lines) == 1 and re.fullmatch(r"\S+Z  " + re.escape(text), lines[0]), (lines, out[-2000:])
    with open(tmp_path / "call.txt", encoding="utf-8-sig") as fh:
        call = fh.read().strip()
    assert call == "Win32_Process|Create|" + LOOP_CMD + "|E:\\research\\econfindatalibrary|0", call


def test_the_launcher_starts_the_loop_hidden_through_wmi():
    raw, text = _launcher()
    assert all(b < 128 for b in raw), "the launcher must be ASCII"
    lines = [ln for ln in text.splitlines() if ln.strip() and not ln.lower().startswith("rem")]
    # The loop's command line is defined once, on the set line, with the -File literal unchanged.
    sets = [ln for ln in lines if ln.startswith('set "ECON_GUARD_LOOP_CMD=')]
    assert sets == ['set "ECON_GUARD_LOOP_CMD=' + LOOP_CMD + '"'], sets
    carriers = [ln for ln in lines if "RELAUNCH_GUARD_LOOP" in ln]
    assert carriers == sets, ("the loop path must sit on the set line only, so the launcher's own command "
                              f"line never matches a -File literal check (R260): {carriers}")
    ps = [ln for ln in lines if ln.startswith("powershell ")]
    assert len(ps) == 1, ps
    m = re.fullmatch(r'powershell -NoProfile -ExecutionPolicy Bypass -Command "(.*)"', ps[0])
    assert m, ps[0]
    body = m.group(1)
    assert '"' not in body and "%" not in body, "a quote or a percent sign inside -Command breaks cmd's parsing"
    for must in ("Invoke-CimMethod -ClassName Win32_Process -MethodName Create",
                 "CommandLine = $env:ECON_GUARD_LOOP_CMD", "ShowWindow = [uint16]0",
                 "ProcessStartupInformation = $su }",
                 "CurrentDirectory = 'E:\\research\\econfindatalibrary'",
                 "$r.ReturnValue -eq 0) { $rc = 0;", "'E:\\research\\econfindatalibrary\\logs\\_guard_loop.log'"):
        assert must in body, must
    assert body.rstrip().endswith("exit $rc"), "the -Command must end with an explicit exit code"
    # start "" survives only as the fallback for a failed Create.
    starts = [ln for ln in lines if re.search(r"\bstart \"", ln)]
    assert starts == ['if errorlevel 1 start "" %ECON_GUARD_LOOP_CMD%'], starts
    assert lines[-1] == "exit /b 0", lines[-1]
