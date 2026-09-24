"""tools/run_local_heavy.ps1 after T0 (plan step 1: a self-hosted path; before, it asked for --pull-state and the R2
backend and failed closed). A real pass cannot run in a test - a child process reads the machine's own flag - so
this reads the script with PowerShell's own parser: it parses, and every state pull, state push and CI-gate call
sits in the ELSE branch of an `if ($selfHosted)`, whose value comes from core.cutover (one rule). The T0 probe
the script runs is executed here too: it must answer 0 or 1."""
import os
import shutil
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPT = os.path.join(ROOT, "tools", "run_local_heavy.ps1")
POWERSHELL = shutil.which("powershell") or shutil.which("pwsh")

AST_PROBE = r"""
param([string]$Path)
$tokens = $null; $errors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile($Path, [ref]$tokens, [ref]$errors)
"PARSE_ERRORS=" + $errors.Count
$needles = @('--pull-state', '--push-state', '--until-block', '$gate 2>&1')
$cmds = $ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.PipelineAst] }, $true)
foreach ($c in $cmds) {
    $t = $c.Extent.Text
    $hit = $null
    foreach ($n in $needles) { if ($t.Contains($n)) { $hit = $n } }
    if (-not $hit) { continue }
    $p = $c.Parent; $guarded = $false
    while ($p) {
        if ($p -is [System.Management.Automation.Language.IfStatementAst]) {
            $cond = $p.Clauses[0].Item1.Extent.Text.Trim()
            if ($cond -eq '$selfHosted' -and $p.ElseClause -and
                $c.Extent.StartOffset -ge $p.ElseClause.Extent.StartOffset -and
                $c.Extent.EndOffset -le $p.ElseClause.Extent.EndOffset) { $guarded = $true }
            foreach ($cl in $p.Clauses | Select-Object -Skip 1) {
                if ($p.Clauses[0].Item1.Extent.Text.Trim() -eq '$selfHosted' -and
                    $c.Extent.StartOffset -ge $cl.Item2.Extent.StartOffset -and
                    $c.Extent.EndOffset -le $cl.Item2.Extent.EndOffset) { $guarded = $true }
            }
        }
        $p = $p.Parent
    }
    "CALL|" + $hit + "|" + $guarded + "|" + $c.Extent.StartLineNumber
}
"""


def _probe(tmp_path, path=SCRIPT):
    probe = tmp_path / "probe.ps1"
    probe.write_text(AST_PROBE, encoding="utf-8")
    r = subprocess.run([POWERSHELL, "-NoProfile", "-NonInteractive", "-File", str(probe), "-Path", path],
                       capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stderr[-800:]
    lines = r.stdout.splitlines()
    errors = int(next(ln for ln in lines if ln.startswith("PARSE_ERRORS=")).split("=")[1])
    calls = [ln.split("|")[1:] for ln in lines if ln.startswith("CALL|")]
    return errors, calls


@pytest.mark.skipif(POWERSHELL is None, reason="needs PowerShell (the workstation runs this script under it)")
def test_every_cloud_state_step_is_skipped_after_t0(tmp_path):
    errors, calls = _probe(tmp_path)
    assert errors == 0, "the script must parse"
    found = {c[0] for c in calls}
    assert found == {"--pull-state", "--push-state", "--until-block", "$gate 2>&1"}, calls
    unguarded = [c for c in calls if c[1] != "True"]
    assert not unguarded, f"cloud state steps reachable after T0: {unguarded}"


@pytest.mark.skipif(POWERSHELL is None, reason="needs PowerShell")
def test_the_probe_can_fail(tmp_path):
    """The same AST check over a copy whose push is NOT under the guard must report it."""
    src = open(SCRIPT, encoding="utf-8-sig").read()
    bad = src.replace("if ($selfHosted) {\n    # the updater wrote the local state.db itself",
                      "if ($false) {\n    # the updater wrote the local state.db itself")
    assert bad != src, "precondition: the push guard was found to break"
    p = tmp_path / "bad.ps1"
    p.write_text(bad, encoding="utf-8-sig")
    errors, calls = _probe(tmp_path, str(p))
    assert errors == 0 and any(c[0] == "--push-state" and c[1] != "True" for c in calls), calls


def test_the_script_is_ascii_after_its_bom():
    raw = open(SCRIPT, "rb").read()
    body = raw[3:] if raw.startswith(b"\xef\xbb\xbf") else raw
    assert all(b < 128 for b in body), "PowerShell 5.1 mis-decodes non-ASCII in this file (its header, R-note)"


def test_an_answer_other_than_0_or_1_stops_the_pass_and_t0_stamps_the_cadence():
    """R1241 mutants H2 (the 'not 0 or 1' abort removed) and H3 (after T0 the pass never counts as committed, so
    the cadence is never stamped and every guard tick starts another pass) survived."""
    src = open(SCRIPT, encoding="utf-8-sig").read().replace("\r\n", "\n")
    guard = "if ($cutRc -ne 0 -or ($cutOut -ne '0' -and $cutOut -ne '1')) {\n"
    assert guard in src
    body = src.split(guard, 1)[1].split("\n}\n", 1)[0]
    assert "exit 2" in body, "a T0 answer that is not 0 or 1 stops the pass"
    push = src.split("if ($selfHosted) {\n    # the updater wrote the local state.db itself", 1)
    assert len(push) == 2 and "$pushRc = 0" in push[1].split("} else {", 1)[0], \
        "after T0 the state is committed as the run goes: pushRc 0, so the cadence can be stamped"
    assert "Test-CadenceShouldStamp -PushRc $pushRc" in src


def test_the_t0_probe_the_script_runs_answers_0_or_1():
    """The exact command the script runs (read from it), executed: before T0 on this machine or on CI it prints 0,
    after T0 1 - never anything else, which the script would treat as 'cannot tell' and stop."""
    src = open(SCRIPT, encoding="utf-8-sig").read()
    line = next(ln for ln in src.splitlines() if "from core import cutover; print(int(cutover.is_cut_over()))" in ln)
    code = line.split('-c "', 1)[1].split('" 2>&1', 1)[0].replace("$repo", ROOT)
    r = subprocess.run([sys.executable, "-B", "-c", code], capture_output=True, text=True, timeout=60)
    assert r.returncode == 0 and r.stdout.strip() in ("0", "1"), (r.stdout, r.stderr[-400:])
    assert "$selfHosted = ($cutOut -eq '1')" in src
    assert "if ($selfHosted) { $env:AQUEDUCT_BACKEND = 'selfhost' } else { $env:AQUEDUCT_BACKEND = 'r2' }" in src