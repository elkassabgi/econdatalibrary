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


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
