"""D3, the no-progress back-off in tools/run_local_heavy.ps1 (starvation plan rev 10, PR 1).

Three things are pinned here; the hand-worked planted cases live in tools/test_cadence_decision.ps1 and run through
tests/test_cadence_decision_ps.py.

1. PARITY WITH THE REPLAY - OF THE HOLD RULE ONLY. The 8-hour value was chosen on tools/measure_ci_lag.py
   `replay61`, whose hold rule is Replay._gate_attempt. So the SAME grid of inputs goes through the replay's own
   method and through the ps1's Test-WindowShouldHold in real PowerShell, and every answer must agree.
   The WHEN-TO-STAMP rule is NOT claimed to be the replay's: Replay._pass stamps last_noprog on every pass that
   advanced nothing, including a hard-stopped one, while production deliberately excludes rc 124. AR-398's patched
   replay (32 today rows, with and without the hard-stop stamp) gave identical results, so the difference changes
   nothing the replay measured; it is a deliberate difference, not parity.
2. THE PARITY CHECK CAN FAIL. The same grid against a copy of cadence_decision.ps1 with the D3 term removed must
   disagree - on exactly the cases where the back-off is the only reason to hold.
3. THE STAMP IS WRITTEN IN ONE PLACE, UNDER THE TESTED DECISION, TO ITS OWN FILE. Read with PowerShell's own
   parser: the only command that writes $noProgFile sits inside `if ($noProg.Stamp)`, $noProg comes from
   Test-NoProgressShouldStamp, $noProgFile is exactly 'local_heavy.last_noprogress' and is not the success stamp.
4. THE RUNNER DECIDES BY THE TESTED HOLD (review AR-398 D1). The HOLDING branch's condition is exactly
   `$hold.Hold`, `$hold` comes from Test-WindowShouldHold with every argument pinned, and the stamp age it is given
   is read once, under -IfDue, from $noProgFile. Each pin has a mutant that must break it (AR-398 G, H, I, P, Q).

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
function Conds($node) {
    # the conditions of every enclosing `if` whose FIRST clause body holds $node, innermost last
    $conds = @()
    $p = $node.Parent
    while ($p) {
        if ($p -is [System.Management.Automation.Language.IfStatementAst]) {
            $first = $p.Clauses[0]
            if ($node.Extent.StartOffset -ge $first.Item2.Extent.StartOffset -and
                $node.Extent.EndOffset -le $first.Item2.Extent.EndOffset) {
                $conds = @($first.Item1.Extent.Text.Trim()) + $conds
            }
        }
        $p = $p.Parent
    }
    return ($conds -join ' && ')
}
function Flat($s) { return ("$s" -replace '`\s*\r?\n', ' ' -replace '\s+', ' ').Trim() }
$cmds = $ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.CommandAst] }, $true)
foreach ($c in $cmds) {
    $name = $c.GetCommandName()
    $t = $c.Extent.Text
    if ($t -match '\$noProgFile' -or $t -match '\$stampFile') {
        "CMD|" + $name + "|" + (Conds $c) + "|" + (Flat $t)
    }
    if ($name -in @('Test-WindowShouldHold', 'Test-NoProgressShouldStamp', 'Get-StampAgeHours')) {
        $els = $c.CommandElements
        $pairs = @()
        for ($i = 1; $i -lt $els.Count; $i++) {
            $e = $els[$i]
            if ($e -is [System.Management.Automation.Language.CommandParameterAst]) {
                if ($null -ne $e.Argument) { $arg = $e.Argument.Extent.Text }
                elseif ($i + 1 -lt $els.Count -and
                        -not ($els[$i + 1] -is [System.Management.Automation.Language.CommandParameterAst])) {
                    $i++; $arg = $els[$i].Extent.Text
                } else { $arg = '' }
                $pairs += ($e.ParameterName + '=' + (Flat $arg))
            } else {
                $pairs += ('?=' + (Flat $e.Extent.Text))
            }
        }
        "CALL|" + $name + "|" + ($pairs -join ';;')
    }
}
$ifs = $ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.IfStatementAst] }, $true)
foreach ($f in $ifs) {
    $body = $f.Clauses[0].Item2.Extent.Text
    if ($body.Contains('HOLDING: ')) { "HOLDIF|" + (Flat $f.Clauses[0].Item1.Extent.Text) }
}
$assigns = $ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.AssignmentStatementAst] }, $true)
foreach ($a in $assigns) {
    $l = $a.Left.Extent.Text
    if ($l -in @('$noProg', '$hold', '$noProgFile', '$stampFile', '$script:NoProgressAgeHours')) {
        "ASSIGN|" + $l + "|" + (Conds $a) + "|" + (Flat $a.Right.Extent.Text)
    }
}
"""

WRITERS = ("Set-Content", "Add-Content", "Out-File", "New-Item", "Copy-Item", "Move-Item")
HOLD_ARGS = {"UntilCronMin": "$untilCron", "WantBlockMin": "$WantBlockMin", "SinceLastHours": "$script:SinceLastHours",
             "MaxHours": "$MaxHours", "NoProgressAgeHours": "$script:NoProgressAgeHours",
             "NoProgressBackoffH": "$NoProgressBackoffH"}
NOPROG_ARGS = {"PushRc": "$pushRc", "Crashed": "$crashed", "HardStopped": "($hardStopped -or $rc -eq 124)",
               "Manual": "([bool]$Only -or [bool]$Force)", "PulledState": "(-not $selfHosted)",
               "Advanced": "$advanced"}


def _runner_probe(tmp_path, path=RUNNER):
    probe = tmp_path / "probe.ps1"
    probe.write_text(AST_PROBE, encoding="ascii")
    r = subprocess.run([POWERSHELL, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", str(probe),
                        "-Path", str(path)], capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stderr[-800:]
    lines = r.stdout.splitlines()
    errors = int(next(ln for ln in lines if ln.startswith("PARSE_ERRORS=")).split("=")[1])
    cmds = [ln.split("|", 3)[1:] for ln in lines if ln.startswith("CMD|")]            # [name, conds, text]
    calls = [ln.split("|", 2)[1:] for ln in lines if ln.startswith("CALL|")]          # [name, k=v;;k=v]
    holdifs = [ln.split("|", 1)[1] for ln in lines if ln.startswith("HOLDIF|")]
    assigns = [ln.split("|", 3)[1:] for ln in lines if ln.startswith("ASSIGN|")]      # [left, conds, right]
    return errors, cmds, calls, holdifs, assigns


def _args(call):
    return dict(kv.split("=", 1) for kv in call[1].split(";;")) if call[1] else {}


def _runner_violations(tmp_path, path=RUNNER):
    """Every way the runner can fail to use the tested D3 decisions, as a list (empty = the wiring holds)."""
    errors, cmds, calls, holdifs, assigns = _runner_probe(tmp_path, path)
    v = []
    if errors:
        v.append(f"parse errors: {errors}")

    def one(left):
        a = [x for x in assigns if x[0] == left]
        if len(a) != 1:
            v.append(f"{left} assigned {len(a)} time(s), want 1: {a}")
            return None
        return a[0]

    def one_call(name):
        c = [x for x in calls if x[0] == name]
        if len(c) != 1:
            v.append(f"{name} called {len(c)} time(s), want 1: {c}")
            return {}
        return _args(c[0])

    # --- the hold (AR-398 D1: mutants G, H, I) ---
    if holdifs != ["$hold.Hold"]:
        v.append(f"the HOLDING branch must be decided by exactly `$hold.Hold`, found {holdifs}")
    h = one("$hold")
    if h and not h[2].startswith("Test-WindowShouldHold "):
        v.append(f"$hold must come from Test-WindowShouldHold: {h}")
    got = one_call("Test-WindowShouldHold")
    if got != HOLD_ARGS:
        v.append(f"Test-WindowShouldHold arguments {got} != {HOLD_ARGS}")
    # --- the stamp age the hold is given ---
    age = one("$script:NoProgressAgeHours")
    if age and (age[2] != "Get-StampAgeHours -Path $noProgFile" or age[1] != "$IfDue"):
        v.append(f"$script:NoProgressAgeHours must be `Get-StampAgeHours -Path $noProgFile` under $IfDue: {age}")
    if one_call("Get-StampAgeHours") != {"Path": "$noProgFile"}:
        v.append("Get-StampAgeHours must be called once, on $noProgFile")
    # --- the stamp's identity (AR-398 D2: mutant P) ---
    npf, sf = one("$noProgFile"), one("$stampFile")
    if npf and npf[2] != "Join-Path $logDir 'local_heavy.last_noprogress'":
        v.append(f"$noProgFile must be logs/local_heavy.last_noprogress: {npf}")
    if sf and sf[2] != "Join-Path $logDir 'local_heavy.last_success'":
        v.append(f"$stampFile changed: {sf}")
    if npf and sf and npf[2] == sf[2]:
        v.append("$noProgFile is the success stamp")
    # --- the writer: one, under the tested decision, UTC round-trip (and mutant Q) ---
    nwriters = [c for c in cmds if c[0] in WRITERS and "$noProgFile" in c[2]]
    if len(nwriters) != 1 or nwriters[0][1].split(" && ")[-1:] != ["$noProg.Stamp"]:
        v.append(f"exactly one write of $noProgFile, directly under `if ($noProg.Stamp)`: {nwriters}")
    elif "-Value ((Get-Date).ToUniversalTime().ToString('o'))" not in nwriters[0][2]:
        v.append(f"the no-progress stamp must be written as UTC round-trip ('o'): {nwriters[0]}")
    swriters = [c for c in cmds if c[0] in WRITERS and "$stampFile" in c[2]]
    if any("$noProg.Stamp" in c[1] for c in swriters):
        v.append(f"the success stamp is written under the no-progress decision: {swriters}")
    n = one("$noProg")
    if n and not n[2].startswith("Test-NoProgressShouldStamp "):
        v.append(f"$noProg must come from Test-NoProgressShouldStamp: {n}")
    got = one_call("Test-NoProgressShouldStamp")
    if got != NOPROG_ARGS:
        v.append(f"Test-NoProgressShouldStamp arguments {got} != {NOPROG_ARGS}")
    return v


@needs_ps
def test_the_runner_uses_the_tested_d3_decisions(tmp_path):
    v = _runner_violations(tmp_path)
    assert not v, "\n".join(v)


# Each mutant is one edit to the runner that the suite must reject (AR-398's G, H, I, M, P, Q, plus an unconditional
# write). (old, new, the word the violation must name).
MUTANTS = {
    "G no stamp age to the hold": ("-NoProgressAgeHours $script:NoProgressAgeHours", "-NoProgressAgeHours $null",
                                   "Test-WindowShouldHold arguments"),
    "H back-off 0 at the call": ("-NoProgressBackoffH $NoProgressBackoffH", "-NoProgressBackoffH 0",
                                 "Test-WindowShouldHold arguments"),
    "I old inline rule": ("    if ($hold.Hold) {",
                          "    if ($untilCron -lt $WantBlockMin -and $script:SinceLastHours -lt $MaxHours) {",
                          "HOLDING branch"),
    "M stamp also read outside IfDue": ('Say "local heavy updater starting; log -> $log"',
                                        'if (-not $IfDue) { $script:NoProgressAgeHours = Get-StampAgeHours -Path '
                                        '$noProgFile }\nSay "local heavy updater starting; log -> $log"',
                                        "$script:NoProgressAgeHours assigned 2"),
    "P stamp file is the success stamp": ("$noProgFile = Join-Path $logDir 'local_heavy.last_noprogress'",
                                          "$noProgFile = Join-Path $logDir 'local_heavy.last_success'",
                                          "local_heavy.last_noprogress"),
    "Q stamp in local time": ("Set-Content -Path $noProgFile -Value ((Get-Date).ToUniversalTime().ToString('o'))",
                              "Set-Content -Path $noProgFile -Value ((Get-Date).ToString('yyyy-MM-dd HH:mm:ss'))",
                              "UTC round-trip"),
    "W unconditional write": ("if ($noProg.Stamp) {", "if ($true) {", "directly under"),
}


@needs_ps
@pytest.mark.parametrize("name", sorted(MUTANTS))
def test_each_wiring_mutant_is_caught(tmp_path, name):
    old, new, word = MUTANTS[name]
    src = open(RUNNER, encoding="utf-8-sig").read()
    assert src.count(old) == 1, f"precondition: the mutated text occurs once ({src.count(old)})"
    p = tmp_path / "mutant.ps1"
    p.write_text(src.replace(old, new), encoding="utf-8-sig")
    v = _runner_violations(tmp_path, p)
    assert v, f"mutant {name} was not caught"
    assert any(word in x for x in v), (name, v)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
