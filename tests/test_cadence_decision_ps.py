"""Run the PowerShell cadence-decision checks as part of the suite.

Two of the three defects this guard shipped were PowerShell LANGUAGE facts - `[int]$null` is 0
and does not throw, so a sentinel only the callee can print never arrives when the callee never
runs - and reading the script cannot catch those. The checks therefore execute real PowerShell;
this file only carries them into pytest and reports what they said.

Skipped where powershell.exe is absent (CI runs on Linux), which is honest: the checks are not
running there, and the file says so rather than passing silently.
"""
import os
import shutil
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPT = os.path.join(ROOT, "tools", "test_cadence_decision.ps1")
POWERSHELL = shutil.which("powershell") or shutil.which("pwsh")


@pytest.mark.skipif(POWERSHELL is None, reason="no PowerShell on this host")
def test_the_cadence_decision_holds_in_real_powershell():
    r = subprocess.run(
        [POWERSHELL, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", SCRIPT],
        capture_output=True, text=True, cwd=ROOT)
    out = (r.stdout or "") + (r.stderr or "")
    assert r.returncode == 0, out[-3000:]
    assert "all cadence-decision checks passed" in out, out[-3000:]
    # The fact that bit twice must actually be exercised, not merely described in a comment.
    assert "the raw cast would have said 0" in out, out[-3000:]


@pytest.mark.skipif(POWERSHELL is None, reason="no PowerShell on this host")
def test_the_runner_uses_the_function_that_was_tested():
    """A tested decision that the runner does not call is decoration."""
    runner = os.path.join(ROOT, "tools", "run_local_heavy.ps1")
    body = open(runner, encoding="utf-8", errors="replace").read()
    assert "cadence_decision.ps1" in body, "the runner does not dot-source the decision"
    assert "Test-CadenceShouldStamp" in body, "the runner does not call the tested function"
    assert "Read-ProbeNumber" in body, "the runner does not use the tested parser"


def test_the_budget_is_rederived_after_pull_state_and_before_the_hard_stop():
    """AR-405 G2. Runs everywhere (no PowerShell needed): the ORDER is what the fix is.

    The budget clamp used to run only before pull-state, while the orchestrator's clock starts at
    updater launch, so a slow pull (36.1 min on 2026-10-31) moved push-state past the cloud writer's
    block time. The recompute must sit after the pull and before the hard deadline is derived, read
    the instant taken before the gate call, and its skip branch must leave without pushing.
    """
    runner = os.path.join(ROOT, "tools", "run_local_heavy.ps1")
    body = open(runner, encoding="utf-8", errors="replace").read()
    read_at = body.index("$untilReadAt = [DateTime]::UtcNow")
    gate_call = body.index("$gate --until-block")
    pull = body.index("-m updater.run --pull-state")
    recompute = body.index("$post = Get-PostPullBudget")
    hard = body.index("if ($null -ne $postHardStopUtc) { $hardDeadlineUtc = $postHardStopUtc }")
    launch = body.index("Start-Process -FilePath $pythonExe")
    loop = body.index("if ([DateTime]::UtcNow -gt $hardDeadlineUtc)")
    push = body.index("-m updater.run --push-state")
    assert read_at < gate_call < pull < recompute < hard < launch < loop < push
    call = body[recompute:body.index("\n    if (-not $post.Run)", recompute)]
    assert "-UntilReadAtUtc $untilReadAt" in call and "-UntilBlockMin $untilBlock" in call
    assert "-NowUtc ([DateTime]::UtcNow)" in call
    skip = body[recompute:hard]
    assert "SKIPPING UPDATER AFTER PULL-STATE" in skip
    assert "Release-LocalHeavyLock" in skip and "exit 0" in skip
    assert "--push-state" not in skip
    assert "$env:AQUEDUCT_RUN_BUDGET_MIN = \"$($post.BudgetMin)\"" in skip
    assert "$postHardStopUtc = $post.HardStopUtc" in skip
    # The pre-pull check uses the same function, before the lock and the pull, and exits without either.
    pre = body.index("$pre = Get-PostPullBudget")
    lock = body.index("Set-Content -Path $lockFile")
    assert gate_call < pre < lock < pull
    pre_block = body[pre:body.index("WAIT FOR A WINDOW WORTH USING", pre)]
    assert "exit 0" in pre_block and "--pull-state" not in pre_block
    # No local-clock arithmetic left in the stop path (DST): the old forms are gone.
    assert "(Get-Date) -gt $hardDeadline" not in body
    assert "$updaterStart = Get-Date" not in body
    helper = open(os.path.join(ROOT, "tools", "cadence_decision.ps1"), encoding="utf-8").read()
    assert "function Get-PostPullBudget" in helper


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
