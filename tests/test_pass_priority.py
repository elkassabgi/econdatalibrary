"""tools/run_local_heavy.ps1 -Priority (owner, 2026-11-04): the desktop pass yields the CPU to the owner's own work.

The runner lowers its OWN priority class only around the lines that start a heavy child (pull-state, the progress
probe, the updater), so the child inherits the class at creation and so does everything the child starts; then it
returns to its own class for the hard-stop watchdog. The gate, the killed-unit recorder and push-state are not
lowered: they are the R5 safety tail (tools/pass_priority.ps1 says why).

What is checked here:
  - everywhere (no PowerShell): the default is BelowNormal, the switch accepts exactly Normal|BelowNormal|Idle, the
    runner dot-sources the helper and logs the class at the start of a pass, the file keeps its BOM and is ASCII;
  - under any PowerShell (pwsh 7 on the Linux CI runner, 5.1 here): the parameter as PowerShell itself binds it -
    default, ValidateSet, a bogus value refused before the script runs - and, by the parser, which child calls sit
    inside a lowered try and which do not; with a copy that moves the updater out, so the probe can fail;
  - under Windows PowerShell 5.1 only (the engine production runs, see tests/test_replay_run_deadline_214.py): the
    REAL runner, copied into a sandbox with fake children, run self-hosted (no state pull or push is ever started -
    R1204), and the priority class each child and the updater's own child actually ran at, read by the child itself
    with GetPriorityClass. -Priority Normal is the negative control.
"""
import json
import os
import re
import shutil
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUNNER = os.path.join(ROOT, "tools", "run_local_heavy.ps1")
HELPER = os.path.join(ROOT, "tools", "pass_priority.ps1")
CADENCE = os.path.join(ROOT, "tools", "cadence_decision.ps1")
POWERSHELL = shutil.which("powershell") or shutil.which("pwsh")

# GetPriorityClass values (winbase.h)
NORMAL, IDLE, BELOW_NORMAL = 0x20, 0x40, 0x4000
CLASS = {"Normal": NORMAL, "BelowNormal": BELOW_NORMAL, "Idle": IDLE}


def _runner_text():
    return open(RUNNER, encoding="utf-8-sig").read().replace("\r\n", "\n")


# ------------------------------------------------------------------ everywhere

def test_the_default_is_belownormal_and_the_switch_takes_three_values():
    src = _runner_text()
    param = src[src.index("param("):src.index("\n)\n")]
    m = re.search(r"\[ValidateSet\(([^)]*)\)\]\s*\[string\]\s+\$Priority\s*=\s*'(\w+)'", param)
    assert m, "the -Priority parameter (ValidateSet + default) is not in the param block"
    values = [v.strip().strip("'") for v in m.group(1).split(",")]
    assert values == ["Normal", "BelowNormal", "Idle"] and m.group(2) == "BelowNormal"
    helper = open(HELPER, encoding="ascii").read()
    for v in values:
        assert f"'{v}'" in helper.split("function Enter-PassPriority")[0], f"Get-PassPriorityClass misses {v}"


def test_the_runner_dot_sources_the_helper_and_logs_the_class_at_the_start():
    src = _runner_text()
    assert ". (Join-Path $PSScriptRoot 'pass_priority.ps1')" in src
    start = src.index('Say "local heavy updater starting; log -> $log"')
    log = src.index('Say ("CPU priority: " + $Priority')
    lister = src.index("$routed = (& $pythonExe $lister")
    assert start < log < lister, "the class is logged at the start of every pass, before any child runs"
    assert 'Say ("updater CPU time: " + $cpuS + " s over " + $wallS' in src


def test_the_runner_keeps_its_bom_and_both_files_are_ascii():
    raw = open(RUNNER, "rb").read()
    assert raw.startswith(b"\xef\xbb\xbf"), "run_local_heavy.ps1 lost its UTF-8 BOM"
    assert all(b < 128 for b in raw[3:])
    assert all(b < 128 for b in open(HELPER, "rb").read()), "pass_priority.ps1 must be ASCII (PS 5.1, no BOM)"


# ------------------------------------------------------------------ any PowerShell

PARAM_PROBE = r"""
param([string]$Path, [string]$Helper)
$tokens = $null; $errors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile($Path, [ref]$tokens, [ref]$errors)
"PARSE_ERRORS=" + $errors.Count
$p = $ast.ParamBlock.Parameters | Where-Object { $_.Name.VariablePath.UserPath -eq 'Priority' }
"DEFAULT=" + $p.DefaultValue.Value
$vs = $p.Attributes | Where-Object { $_.TypeName.Name -eq 'ValidateSet' }
"SET=" + (($vs.PositionalArguments | ForEach-Object { $_.Value }) -join ',')
. $Helper
foreach ($n in @('Normal', 'BelowNormal', 'Idle', 'idle')) { "MAP|" + $n + "|" + [int](Get-PassPriorityClass $n) }
try { Get-PassPriorityClass 'High' | Out-Null; "HIGH=accepted" } catch { "HIGH=refused" }
$needles = @('--pull-state', '--push-state', "unit_progress_probe.py') --snapshot",
             "unit_progress_probe.py') --diff", 'Start-Process -FilePath $pythonExe', "record_killed_unit.py",
             '$gate 2>&1', '$gate --until-block', '$lister 2>&1', 'taskkill')
$cmds = $ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.PipelineAst] }, $true)
foreach ($c in $cmds) {
    $t = $c.Extent.Text
    $hit = $null
    foreach ($n in $needles) { if ($t.Contains($n)) { $hit = $n } }
    if (-not $hit) { continue }
    $lowered = $false
    $q = $c.Parent
    while ($q) {
        if ($q -is [System.Management.Automation.Language.TryStatementAst] -and $q.Finally -and
            $q.Finally.Extent.Text.Contains('Exit-PassPriority $prio')) {
            # the statement just before the try must be the Enter that $prio comes from
            $sib = $q.Parent.Statements
            $i = [array]::IndexOf(@($sib), $q)
            if ($i -gt 0 -and $sib[$i - 1].Extent.Text -eq '$prio = Enter-PassPriority $Priority') { $lowered = $true }
        }
        $q = $q.Parent
    }
    "CALL|" + $hit + "|" + $lowered
}
"""

LOWERED = {"--pull-state", "unit_progress_probe.py') --snapshot", "unit_progress_probe.py') --diff",
           "Start-Process -FilePath $pythonExe"}
NOT_LOWERED = {"--push-state", "record_killed_unit.py", "$gate 2>&1", "$gate --until-block", "$lister 2>&1",
               "taskkill"}


def _param_probe(tmp_path, path=RUNNER):
    probe = tmp_path / "probe.ps1"
    probe.write_text(PARAM_PROBE, encoding="utf-8")
    r = subprocess.run([POWERSHELL, "-NoProfile", "-NonInteractive", "-File", str(probe), "-Path", path,
                        "-Helper", HELPER], capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, (r.stdout + r.stderr)[-1500:]
    return r.stdout.splitlines()


@pytest.mark.skipif(POWERSHELL is None, reason="needs PowerShell (the workstation runs this script under it)")
def test_powershell_binds_the_switch_as_pinned(tmp_path):
    lines = _param_probe(tmp_path)
    assert "PARSE_ERRORS=0" in lines
    assert "DEFAULT=BelowNormal" in lines and "SET=Normal,BelowNormal,Idle" in lines
    maps = {ln.split("|")[1]: int(ln.split("|")[2]) for ln in lines if ln.startswith("MAP|")}
    assert maps == {"Normal": NORMAL, "BelowNormal": BELOW_NORMAL, "Idle": IDLE, "idle": IDLE}
    assert "HIGH=refused" in lines, "only the three classes that yield or keep normal may be asked for"


@pytest.mark.skipif(POWERSHELL is None, reason="needs PowerShell")
def test_a_bogus_priority_is_refused_before_the_script_runs(tmp_path):
    """Binding fails before line one of the body, so nothing is listed, gated, locked or logged."""
    sandbox = tmp_path / "repo"
    (sandbox / "tools").mkdir(parents=True)
    for f in (RUNNER, CADENCE, HELPER):
        shutil.copy(f, sandbox / "tools")
    r = subprocess.run([POWERSHELL, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File",
                        str(sandbox / "tools" / "run_local_heavy.ps1"), "-Priority", "High", "-WhatIf"],
                       capture_output=True, text=True, timeout=120)
    text = r.stdout + r.stderr
    assert r.returncode != 0, text[-1500:]
    assert "'Priority'" in text and "does not belong to the set" in text, text[-1500:]
    assert not (sandbox / "logs").exists(), "the body ran: it made logs/"


@pytest.mark.skipif(POWERSHELL is None, reason="needs PowerShell")
def test_exactly_the_heavy_children_are_lowered(tmp_path):
    lines = _param_probe(tmp_path)
    calls = [ln.split("|")[1:] for ln in lines if ln.startswith("CALL|")]
    seen = {c[0] for c in calls}
    assert seen == LOWERED | NOT_LOWERED, calls
    wrong = [c for c in calls if (c[1] == "True") != (c[0] in LOWERED)]
    assert not wrong, f"lowered/not lowered differs from the design: {wrong}"


@pytest.mark.skipif(POWERSHELL is None, reason="needs PowerShell")
def test_the_lowered_probe_can_fail(tmp_path):
    """A copy whose updater is started OUTSIDE the lowered try, and one whose push is moved INSIDE one."""
    src = open(RUNNER, encoding="utf-8-sig").read().replace("\r\n", "\n")
    old = "$prio = Enter-PassPriority $Priority\ntry {\n    $proc = Start-Process"
    assert old in src, "precondition: the updater's lowered start was found"
    bad = src.replace(old, "$prio = $null\ntry {\n    $proc = Start-Process")
    p = tmp_path / "bad.ps1"
    p.write_text(bad, encoding="utf-8-sig")
    calls = [ln.split("|")[1:] for ln in _param_probe(tmp_path, str(p)) if ln.startswith("CALL|")]
    assert ["Start-Process -FilePath $pythonExe", "False"] in calls, calls
    push = "    & $pythonExe -m updater.run --push-state\n"
    assert push in src
    bad2 = src.replace(push, "    $prio = Enter-PassPriority $Priority\n    try { & $pythonExe -m updater.run "
                             "--push-state } finally { Exit-PassPriority $prio }\n")
    p2 = tmp_path / "bad2.ps1"
    p2.write_text(bad2, encoding="utf-8-sig")
    calls = [ln.split("|")[1:] for ln in _param_probe(tmp_path, str(p2)) if ln.startswith("CALL|")]
    assert ["--push-state", "True"] in calls, calls


# ------------------------------------------------------------------ Windows PowerShell 5.1: the real runner

def _production_engine():
    exe = shutil.which("powershell")
    if exe is None or os.name != "nt":
        return None, "no Windows PowerShell 5.1 (the engine production runs run_local_heavy.ps1 under) on this host"
    r = subprocess.run([exe, "-NoProfile", "-Command",
                        "'' + $PSVersionTable.PSEdition + ' ' + $PSVersionTable.PSVersion.ToString()"],
                       capture_output=True, text=True, timeout=120)
    ed = (r.stdout or "").strip()
    if r.returncode != 0 or not ed.startswith("Desktop 5.1"):
        return None, f"powershell on this host is {ed or 'unreadable'!r}, not Windows PowerShell 5.1 Desktop"
    return exe, ed


# Every fake child appends {"who": ..., "class": GetPriorityClass(own process)} to $PRIO_OUT.
_RECORD = r'''
import ctypes, json, os, sys
def record(who):
    k = ctypes.windll.kernel32
    k.GetCurrentProcess.restype = ctypes.c_void_p
    k.GetPriorityClass.argtypes = [ctypes.c_void_p]
    with open(os.environ["PRIO_OUT"], "a", encoding="ascii") as f:
        f.write(json.dumps({"who": who, "pid": os.getpid(), "class": k.GetPriorityClass(k.GetCurrentProcess())}) + "\n")
'''

_FAKES = {
    "_rec.py": _RECORD,
    "tools/_list_local_sources.py": "import sys, os\nsys.path.insert(0, os.path.dirname(os.path.dirname("
                                    "os.path.abspath(__file__))))\nfrom _rec import record\nrecord('lister')\n"
                                    "print('abs')\n",
    "core/__init__.py": "",
    # self-hosted (after T0): the runner then starts no gate and NO state pull or push (R1204: no test may)
    "core/cutover.py": "from _rec import record\ndef is_cut_over():\n    record('cutover_probe')\n    return True\n",
    "tools/unit_progress_probe.py": "import sys, os\nsys.path.insert(0, os.path.dirname(os.path.dirname("
                                    "os.path.abspath(__file__))))\nfrom _rec import record\n"
                                    "record('probe ' + sys.argv[1])\nprint('3' if sys.argv[1] == '--snapshot' "
                                    "else '1')\n",
    "updater/__init__.py": "",
    "updater/run.py": "import os, subprocess, sys\nsys.path.insert(0, os.getcwd())\nfrom _rec import record\n"
                      "assert not any(a in sys.argv for a in ('--pull-state', '--push-state'))\n"
                      "record('updater')\n"
                      "subprocess.run([sys.executable, os.path.join(os.getcwd(), 'grandchild.py')], check=True)\n",
    "grandchild.py": "import os, sys\nsys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))\n"
                     "from _rec import record\nrecord('updater child')\n",
}


def _sandbox_pass(tmp_path, engine, extra):
    repo = tmp_path / "repo"
    for rel, body in _FAKES.items():
        f = repo / rel
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(body, encoding="ascii")
    for src in (RUNNER, CADENCE, HELPER):
        shutil.copy(src, repo / "tools")              # the REAL runner and its helpers, byte for byte
    out = tmp_path / "prio.jsonl"
    env = {k: v for k, v in os.environ.items() if not k.startswith(("AQUEDUCT_", "PYTHON"))}
    env["PRIO_OUT"] = str(out)
    # The launcher pins ITS class to Normal first, so the runner starts at Normal however this test was started
    # (a suite run below normal would otherwise make every child below normal and the controls meaningless).
    ps1 = str(repo / "tools" / "run_local_heavy.ps1").replace("'", "''")
    cmd = ("[System.Diagnostics.Process]::GetCurrentProcess().PriorityClass = 'Normal'; & '" + ps1 + "' "
           + " ".join(extra) + "; exit $LASTEXITCODE")
    r = subprocess.run([engine, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", cmd],
                       capture_output=True, text=True, timeout=300, env=env, cwd=str(tmp_path))
    rows = [json.loads(ln) for ln in out.read_text(encoding="ascii").splitlines()] if out.exists() else []
    return r, rows


@pytest.mark.parametrize("extra, want", [
    ([], "BelowNormal"),                      # the default
    (["-Priority", "Idle"], "Idle"),
    (["-Priority", "Normal"], "Normal"),      # negative control: the low class comes from the switch alone
])
def test_the_real_runner_starts_its_heavy_children_at_the_class_asked(tmp_path, extra, want):
    engine, why = _production_engine()
    if engine is None:
        pytest.skip(why)
    r, rows = _sandbox_pass(tmp_path, engine, extra)
    assert r.returncode == 0, (r.stdout + r.stderr)[-2500:]
    got = {}
    for row in rows:
        got.setdefault(row["who"], set()).add(row["class"])
    # every child ran exactly once and each one says what class it ran at
    assert set(got) == {"lister", "cutover_probe", "probe --snapshot", "updater", "updater child",
                        "probe --diff"}, (rows, r.stdout[-2000:])
    lowered = {"probe --snapshot", "updater", "updater child", "probe --diff"}
    for who, classes in got.items():
        expect = CLASS[want] if who in lowered else NORMAL
        assert classes == {expect}, f"{who} ran at {classes}, expected {hex(expect)} (rows {rows})"
    # the updater's own child inherited it with no help: nothing in the fake sets a class
    assert "CPU priority: " + want + " for pull-state" in r.stdout
    assert "own class (Normal)" in r.stdout, "the runner itself must stay at Normal (watchdog, push-state)"
    assert re.search(r"updater CPU time: [0-9.]+ s over [0-9.]+ s wall at priority " + want, r.stdout), r.stdout


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
