"""D3, the no-progress back-off in tools/run_local_heavy.ps1 (starvation plan rev 10, PR 1).

Three things are pinned here; the hand-worked planted cases live in tools/test_cadence_decision.ps1 and run through
tests/test_cadence_decision_ps.py.

1. PARITY WITH THE REPLAY. The 8-hour value was chosen on tools/measure_ci_lag.py `replay61`, whose hold rule is
   Replay._gate_attempt. A back-off whose production predicate drifts from the modelled one is a back-off nobody
   measured. So the SAME grid of inputs goes through the replay's own method and through the ps1's
   Test-WindowShouldHold in real PowerShell, and every answer must agree.
2. THE PARITY CHECK CAN FAIL. The same grid against a copy of cadence_decision.ps1 with the D3 term removed must
   disagree - on exactly the cases where the back-off is the only reason to hold.
3. THE STAMP IS WRITTEN IN ONE PLACE, UNDER THE TESTED DECISION. Read with PowerShell's own parser: the only
   command that writes local_heavy.last_noprogress sits inside `if ($noProg.Stamp)`, and $noProg comes from
   Test-NoProgressShouldStamp.

Skipped where no PowerShell exists; GitHub's ubuntu runners carry pwsh, so CI runs them.
"""
import datetime as dt
import itertools
import os
import shutil
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools"))

import measure_ci_lag as m  # noqa: E402

DECISION = os.path.join(ROOT, "tools", "cadence_decision.ps1")
RUNNER = os.path.join(ROOT, "tools", "run_local_heavy.ps1")
POWERSHELL = shutil.which("powershell") or shutil.which("pwsh")
needs_ps = pytest.mark.skipif(POWERSHELL is None, reason="no PowerShell on this host")

BACKOFF_H = 8.0
T0 = dt.datetime(2026, 10, 28, 12, 0, tzinfo=dt.timezone.utc)

# usable minutes (>= 20: below that both sides ABORT before the hold is asked), hours since the last pass that
# advanced something (>= MinHours 20: below that both sides stop earlier), age of the no-progress stamp (None = no
# stamp; the edges 8.0 and 7.99 included because the rule is a strict <).
UNTIL = [20, 60, 119, 149, 150, 151, 200, 275]
SINCE = [20.5, 30, 35.99, 36, 40, 200]
AGE = [None, 0.0, 2.0, 7.99, 8.0, 9.0, 30.0]
GRID = list(itertools.product(UNTIL, SINCE, AGE))


def _replay_hold(until_cron, since_h, age_h, backoff_h=BACKOFF_H):
    """Replay._gate_attempt on one input, with the cloud side neutralised (nothing in flight, no cron pending) and
    --until-block answering exactly until_cron + the ps1 margin. Returns True for "hold", False for "pass"."""
    r = object.__new__(m.Replay)
    r.cfg = m.ReplayConfig(policy="today", backoff_h=backoff_h)
    r.last_noprog = None if age_h is None else T0 - dt.timedelta(hours=age_h)
    r._inflight = lambda: False
    r._cron_pending = lambda t: False
    started = []
    r._pass = lambda t, budget, mode: started.append(budget)
    orig = m.until_block_fast
    m.until_block_fast = lambda t: until_cron + m.PS1_MARGIN_MIN
    try:
        verdict = r._gate_attempt(T0, since_h)
    finally:
        m.until_block_fast = orig
    assert verdict in ("hold", "pass"), (until_cron, since_h, age_h, verdict)
    assert (verdict == "pass") == bool(started)
    return verdict == "hold"


PS_DRIVER = r"""
param([string]$Decision, [string]$Cases)
$ErrorActionPreference = 'Stop'
. $Decision
foreach ($line in (Get-Content $Cases)) {
    if (-not $line.Trim()) { continue }
    $p = $line.Split(',')
    $age = $null
    if ($p[3] -ne 'none') { $age = [double]::Parse($p[3], [Globalization.CultureInfo]::InvariantCulture) }
    $h = Test-WindowShouldHold -UntilCronMin ([double]::Parse($p[1], [Globalization.CultureInfo]::InvariantCulture)) `
            -WantBlockMin 150 `
            -SinceLastHours ([double]::Parse($p[2], [Globalization.CultureInfo]::InvariantCulture)) -MaxHours 36 `
            -NoProgressAgeHours $age -NoProgressBackoffH 8
    "CASE|" + $p[0] + "|" + $h.Hold
}
"""


def _ps_holds(tmp_path, decision_path):
    cases = tmp_path / "cases.csv"
    cases.write_text("\n".join(f"{i},{u},{s},{'none' if a is None else a}" for i, (u, s, a) in enumerate(GRID)),
                     encoding="ascii")
    drv = tmp_path / "driver.ps1"
    drv.write_text(PS_DRIVER, encoding="ascii")
    r = subprocess.run([POWERSHELL, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", str(drv),
                        "-Decision", str(decision_path), "-Cases", str(cases)],
                       capture_output=True, text=True, timeout=180)
    assert r.returncode == 0, (r.stdout + r.stderr)[-2000:]
    got = {}
    for ln in r.stdout.splitlines():
        if ln.startswith("CASE|"):
            _, i, hold = ln.strip().split("|")
            got[int(i)] = (hold == "True")
    assert len(got) == len(GRID), f"PowerShell answered {len(got)} of {len(GRID)} cases"
    return got


def test_the_constants_the_parity_rests_on_match_the_runner():
    """The replay must model the runner's current values, or parity on the grid is parity with something else."""
    body = open(RUNNER, encoding="utf-8-sig").read()
    assert "[int]      $MaxHours = 36" in body and m.PS1_MAX_HOURS == 36
    assert "[int]      $WantBlockMin = 150" in body and m.PS1_WANT_BLOCK == 150
    assert "$marginMin = 25" in body and m.PS1_MARGIN_MIN == 25
    assert "if ($untilCron -lt 20)" in body and m.PS1_MIN_USABLE == 20
    assert "[double]   $NoProgressBackoffH = 8\n" in body.replace("\r\n", "\n"), "the back-off must stay 8 h (AR-393 r10)"


def test_the_replay_side_of_the_grid_has_both_answers():
    """A grid on which the model only ever says one thing cannot show agreement about anything."""
    holds = [_replay_hold(*c) for c in GRID]
    assert any(holds) and not all(holds)
    # and the D3 term is exercised: holds that exist ONLY because of the stamp
    d3_only = [c for c, h in zip(GRID, holds) if h and not _replay_hold(c[0], c[1], None)]
    assert d3_only, "no case where the back-off alone holds"
    assert all(u < 150 and s >= 36 and a is not None and a < BACKOFF_H for u, s, a in d3_only), d3_only


@needs_ps
def test_ps1_hold_decision_matches_the_replay_on_every_case(tmp_path):
    want = {i: _replay_hold(*c) for i, c in enumerate(GRID)}
    got = _ps_holds(tmp_path, DECISION)
    diff = [(GRID[i], want[i], got[i]) for i in want if want[i] != got[i]]
    assert not diff, f"{len(diff)} of {len(GRID)} cases disagree (case, replay, ps1): {diff[:10]}"


@needs_ps
def test_parity_check_fails_when_the_d3_term_is_removed(tmp_path):
    """Negative control: drop the back-off from the ps1 and the same comparison must catch it."""
    src = open(DECISION, encoding="utf-8").read()
    bad = src.replace("    if ($young) {\n", "    if ($false) {\n").replace("    if ($young) {\r\n",
                                                                          "    if ($false) {\r\n")
    assert bad != src, "precondition: the D3 branch was found"
    mutant = tmp_path / "cadence_decision_mutant.ps1"
    mutant.write_text(bad, encoding="ascii")
    want = {i: _replay_hold(*c) for i, c in enumerate(GRID)}
    got = _ps_holds(tmp_path, mutant)
    diff = [GRID[i] for i in want if want[i] != got[i]]
    assert diff, "the parity check could not see the back-off removed"
    assert all(u < 150 and s >= 36 and a is not None and a < BACKOFF_H for u, s, a in diff), diff


AST_PROBE = r"""
param([string]$Path)
$tokens = $null; $errors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile($Path, [ref]$tokens, [ref]$errors)
"PARSE_ERRORS=" + $errors.Count
$cmds = $ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.CommandAst] }, $true)
foreach ($c in $cmds) {
    $t = $c.Extent.Text
    if ($t -notmatch '\$noProgFile') { continue }
    $name = $c.GetCommandName()
    $conds = @()
    $p = $c.Parent
    while ($p) {
        if ($p -is [System.Management.Automation.Language.IfStatementAst]) {
            $first = $p.Clauses[0]
            if ($c.Extent.StartOffset -ge $first.Item2.Extent.StartOffset -and
                $c.Extent.EndOffset -le $first.Item2.Extent.EndOffset) {
                $conds += $first.Item1.Extent.Text.Trim()
            }
        }
        $p = $p.Parent
    }
    "CMD|" + $name + "|" + ($conds -join ' && ') + "|" + $c.Extent.StartLineNumber
}
$assigns = $ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.AssignmentStatementAst] }, $true)
foreach ($a in $assigns) {
    if ($a.Left.Extent.Text -eq '$noProg') { "ASSIGN|" + ($a.Right.Extent.Text -replace '\s+', ' ') }
}
"""


def _runner_probe(tmp_path, path=RUNNER):
    probe = tmp_path / "probe.ps1"
    probe.write_text(AST_PROBE, encoding="ascii")
    r = subprocess.run([POWERSHELL, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", str(probe),
                        "-Path", str(path)], capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stderr[-800:]
    lines = r.stdout.splitlines()
    errors = int(next(ln for ln in lines if ln.startswith("PARSE_ERRORS=")).split("=")[1])
    cmds = [ln.split("|")[1:] for ln in lines if ln.startswith("CMD|")]
    assigns = [ln.split("|", 1)[1] for ln in lines if ln.startswith("ASSIGN|")]
    return errors, cmds, assigns


@needs_ps
def test_the_stamp_is_written_only_under_the_tested_decision(tmp_path):
    errors, cmds, assigns = _runner_probe(tmp_path)
    assert errors == 0
    writers = [c for c in cmds if c[0] in ("Set-Content", "Add-Content", "Out-File", "New-Item", "Copy-Item",
                                            "Move-Item")]
    assert len(writers) == 1, writers
    assert writers[0][1] == "$noProg.Stamp", writers
    assert len(assigns) == 1 and assigns[0].startswith("Test-NoProgressShouldStamp "), assigns
    # every input the binding conditions name is passed, and the manual/pull flags come from the right places
    a = assigns[0]
    for needle in ("-PushRc $pushRc", "-Crashed $crashed", "-HardStopped ($hardStopped -or $rc -eq 124)",
                   "-Manual ([bool]$Only -or [bool]$Force)", "-PulledState (-not $selfHosted)",
                   "-Advanced $advanced"):
        assert needle in a, (needle, a)
    readers = [c for c in cmds if c[0] == "Get-StampAgeHours"]
    assert len(readers) == 1 and readers[0][1] == "$IfDue", readers


@needs_ps
def test_the_writer_probe_can_fail(tmp_path):
    """Negative control: an unconditional write of the stamp must be reported."""
    src = open(RUNNER, encoding="utf-8-sig").read()
    bad = src.replace("if ($noProg.Stamp) {", "if ($true) {", 1)
    assert bad != src
    p = tmp_path / "bad.ps1"
    p.write_text(bad, encoding="utf-8-sig")
    _, cmds, _ = _runner_probe(tmp_path, p)
    writers = [c for c in cmds if c[0] == "Set-Content"]
    assert writers and writers[0][1] != "$noProg.Stamp"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
