"""The nightly backup window: a desktop pass that comes due between 02:00 and 06:00 America/Chicago does not start.

Owner's decision, 2026-11-12. The workstation's nightly backup starts at 02:00 Chicago and reads the disks heavily,
so run_local_heavy.ps1 -IfDue holds a DUE pass until 06:00 Chicago. A pass already running is never stopped.

The rule is Get-BackupWindowHold in tools/cadence_decision.ps1, a pure function of the instant. The behaviour tests
run it in real PowerShell (one process for every case, then one named test per case) because the DST handling is
a .NET TimeZoneInfo fact that reading the script cannot check. The order tests read the runner: they run anywhere.
"""
import json
import os
import shutil
import subprocess

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DECISION = os.path.join(ROOT, "tools", "cadence_decision.ps1")
RUNNER = os.path.join(ROOT, "tools", "run_local_heavy.ps1")
POWERSHELL = shutil.which("powershell") or shutil.which("pwsh")

# name -> UTC instant. Chicago is UTC-6 (CST) in winter and UTC-5 (CDT) in summer.
CASES = {
    "winter_0159": "2026-11-12T07:59:00Z",   # 01:59 CST
    "winter_0200": "2026-11-12T08:00:00Z",   # 02:00 CST
    "winter_0559": "2026-11-12T11:59:00Z",   # 05:59 CST
    "winter_0600": "2026-11-12T12:00:00Z",   # 06:00 CST
    "summer_0159": "2026-07-15T06:59:00Z",   # 01:59 CDT
    "summer_0200": "2026-07-15T07:00:00Z",   # 02:00 CDT
    "summer_0559": "2026-07-15T10:59:00Z",   # 05:59 CDT
    "summer_0600": "2026-07-15T11:00:00Z",   # 06:00 CDT
    # Spring forward, 2027-03-14: 01:59:59 CST is followed by 03:00 CDT; 02:00-02:59 local never happens.
    "spring_0159": "2027-03-14T07:59:00Z",   # 01:59 CST
    "spring_jump": "2027-03-14T08:00:00Z",   # 03:00 CDT, the first instant after 01:59:59 CST
    "spring_0559": "2027-03-14T10:59:00Z",   # 05:59 CDT
    "spring_0600": "2027-03-14T11:00:00Z",   # 06:00 CDT
    # Fall back, 2026-11-01: 01:00-01:59 local happens twice (CDT, then CST); 02:00 CST comes after both.
    "fall_0159_cdt": "2026-11-01T06:59:00Z",  # 01:59 CDT, first pass through the hour
    "fall_0130_cst": "2026-11-01T07:30:00Z",  # 01:30 CST, the repeated hour
    "fall_0159_cst": "2026-11-01T07:59:00Z",  # 01:59 CST
    "fall_0200": "2026-11-01T08:00:00Z",      # 02:00 CST
    "fall_0559": "2026-11-01T11:59:00Z",      # 05:59 CST
    "fall_0600": "2026-11-01T12:00:00Z",      # 06:00 CST
}

_PS = r"""
$ErrorActionPreference = 'Stop'
. '{decision}'
$cases = ConvertFrom-Json @'
{cases}
'@
$out = @{{}}
foreach ($p in $cases.PSObject.Properties) {{
    $t = [DateTime]::Parse($p.Value, [Globalization.CultureInfo]::InvariantCulture,
                           [Globalization.DateTimeStyles]::AdjustToUniversal -bor [Globalization.DateTimeStyles]::AssumeUniversal)
    $r = Get-BackupWindowHold -NowUtc $t
    $resume = ''
    if ($null -ne $r.ResumeUtc) {{ $resume = $r.ResumeUtc.ToString('yyyy-MM-ddTHH:mm:ssZ') }}
    $out[$p.Name] = @{{ hold = [bool]$r.Hold; resume = $resume; key = $r.WindowKey; why = $r.Why;
                       local = $r.LocalNow.ToString('yyyy-MM-dd HH:mm') }}
}}
# A Kind=Unspecified instant is read as UTC, never as this machine's local time.
$u = Get-BackupWindowHold -NowUtc ([DateTime]::SpecifyKind([DateTime]'2026-11-12T08:00:00', [DateTimeKind]::Unspecified))
$out['unspecified_kind_0800'] = @{{ hold = [bool]$u.Hold; resume = ''; key = $u.WindowKey; why = $u.Why; local = '' }}
# An unknown zone fails OPEN, with a reason (holding on an unknown clock would hold for ever).
$script:BackupWindowTimeZoneIds = @('No/Such_Zone_For_This_Test')
$n = Get-BackupWindowHold -NowUtc ([DateTime]::SpecifyKind([DateTime]'2026-11-12T08:00:00', [DateTimeKind]::Utc))
$out['no_zone'] = @{{ hold = [bool]$n.Hold; resume = ''; key = ''; why = $n.Why; local = '' }}
Write-Output 'JSON-BEGIN'
Write-Output ($out | ConvertTo-Json -Depth 4 -Compress)
"""


@pytest.fixture(scope="module")
def results():
    if POWERSHELL is None:
        pytest.skip("no PowerShell on this host")
    script = _PS.format(decision=DECISION.replace("'", "''"), cases=json.dumps(CASES))
    r = subprocess.run([POWERSHELL, "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", script],
                       capture_output=True, text=True, cwd=ROOT)
    out = (r.stdout or "") + (r.stderr or "")
    assert r.returncode == 0 and "JSON-BEGIN" in r.stdout, out[-3000:]
    return json.loads(r.stdout.split("JSON-BEGIN", 1)[1].strip())


def _starts(res, name):
    assert res[name]["hold"] is False, res[name]


def _holds(res, name, resume):
    assert res[name]["hold"] is True, res[name]
    assert res[name]["resume"] == resume, res[name]
    assert "backup window" in res[name]["why"], res[name]


def test_due_at_0159_starts(results):
    _starts(results, "winter_0159")
    _starts(results, "summer_0159")


def test_due_at_0200_is_held_until_0600(results):
    _holds(results, "winter_0200", "2026-11-12T12:00:00Z")
    _holds(results, "summer_0200", "2026-07-15T11:00:00Z")


def test_due_at_0559_is_held_until_0600(results):
    _holds(results, "winter_0559", "2026-11-12T12:00:00Z")
    _holds(results, "summer_0559", "2026-07-15T11:00:00Z")


def test_due_at_0600_starts(results):
    _starts(results, "winter_0600")
    _starts(results, "summer_0600")


def test_spring_forward_night_holds_from_the_jump_to_0600_cdt(results):
    _starts(results, "spring_0159")
    _holds(results, "spring_jump", "2027-03-14T11:00:00Z")      # 03:00 CDT is inside the window
    assert results["spring_jump"]["local"] == "2027-03-14 03:00", results["spring_jump"]
    _holds(results, "spring_0559", "2027-03-14T11:00:00Z")
    _starts(results, "spring_0600")


def test_fall_back_night_repeated_hour_starts_and_window_is_0200_to_0600_cst(results):
    _starts(results, "fall_0159_cdt")
    _starts(results, "fall_0130_cst")
    _starts(results, "fall_0159_cst")
    _holds(results, "fall_0200", "2026-11-01T12:00:00Z")
    _holds(results, "fall_0559", "2026-11-01T12:00:00Z")
    _starts(results, "fall_0600")


def test_one_window_one_key(results):
    """The once-per-window log line keys on the Chicago date: every held tick of one night shares it."""
    assert results["winter_0200"]["key"] == results["winter_0559"]["key"] == "2026-11-12"
    assert results["fall_0200"]["key"] == results["fall_0559"]["key"] == "2026-11-01"


def test_an_unspecified_instant_is_read_as_utc(results):
    _holds(results, "unspecified_kind_0800", "")


def test_an_unknown_zone_fails_open_and_says_so(results):
    assert results["no_zone"]["hold"] is False
    assert "NOT applied" in results["no_zone"]["why"], results["no_zone"]


# --- where the runner asks it (runs everywhere) -------------------------------------------------------------------

def _runner():
    return open(RUNNER, encoding="utf-8-sig", errors="replace").read()


def test_a_running_pass_is_untouched():
    """A tick that finds a live lock leaves BEFORE the window is asked, and a pass that has started never asks it
    again: the one call sits inside -IfDue, after the live-lock exit and the cadence floor, before the start line
    and the atomic lock. The held branch only exits; it releases, kills and pulls nothing."""
    body = _runner()
    assert body.count("Get-BackupWindowHold -NowUtc") == 1, "the runner must ask the window exactly once"
    ifdue = body.index("if ($IfDue) {")
    live_lock_exit = body.index("exit 0", body.index("WEDGED: local_heavy lock held by pid"))
    floor = body.index("$script:SinceLastHours -lt $MinHours")
    call = body.index("Get-BackupWindowHold -NowUtc")
    start = body.index('Say "local heavy updater starting')
    take_lock = body.index("[System.IO.FileMode]::CreateNew")
    assert ifdue < live_lock_exit < floor < call < start < take_lock
    held = body[call:start]
    for forbidden in ("Stop-Process", "Release-LocalHeavyLock", "--pull-state", "--push-state", "Remove-Item"):
        assert forbidden not in held, forbidden
    assert "exit 0" in held


def test_the_held_tick_logs_one_line_per_window():
    body = _runner()
    held = body[body.index("Get-BackupWindowHold -NowUtc"):body.index('Say "local heavy updater starting')]
    assert "BACKUP WINDOW: a pass is" in held
    assert "local_heavy.backup_hold" in held and "$bw.WindowKey" in held


def _code_lines(text):
    """The runner's STATEMENT lines, stripped: block comments and whole-line comments removed. A statement that
    a mutation turns into a comment, or edits in any way, is no longer in this list (review AR-483)."""
    import re
    text = re.sub(r"<#.*?#>", "", text, flags=re.S)
    return [ln.strip() for ln in text.splitlines() if ln.strip() and not ln.strip().startswith("#")]


def _ifdue_block(lines):
    """The statement lines of the top-level `if ($IfDue) {` block, by brace depth."""
    start = lines.index("if ($IfDue) {")
    depth = 0
    for i in range(start, len(lines)):
        depth += lines[i].count("{") - lines[i].count("}")
        if depth == 0:
            return lines[start:i + 1]
    raise AssertionError("the -IfDue block never closes")


# The held branch, statement by statement, exactly as it must read. Runs on every host (CI is Ubuntu and may have
# no PowerShell): a mutant that inverts the hold, never logs, never writes the marker, asks the window only in a
# comment or passes anything but [DateTime]::UtcNow changes one of these lines and fails here.
_HELD_BRANCH = [
    "$bw = Get-BackupWindowHold -NowUtc ([DateTime]::UtcNow)",
    "if ($bw.Hold) {",
    "$bwMark = Join-Path $logDir 'local_heavy.backup_hold'",
    "$bwSeen = $null",
    "try { if (Test-Path $bwMark) { $bwSeen = (Get-Content $bwMark -First 1) } } catch { }",
    "if ($bwSeen -ne $bw.WindowKey) {",
]
_HELD_TAIL = [
    "try { Set-Content -Path $bwMark -Value $bw.WindowKey -Encoding ascii } catch { }",
    "}",
    "exit 0",
    "}",
]


def test_the_held_branch_statements_are_pinned():
    lines = _code_lines(_runner())
    block = _ifdue_block(lines)
    calls = [ln for ln in lines if "Get-BackupWindowHold" in ln]
    assert calls == [_HELD_BRANCH[0]], calls          # asked once, as a statement, with the UTC clock
    i = block.index(_HELD_BRANCH[0])                  # ... and inside -IfDue
    assert block[i:i + len(_HELD_BRANCH)] == _HELD_BRANCH, block[i:i + len(_HELD_BRANCH) + 8]
    j = block.index(_HELD_TAIL[0], i)
    assert block[j:j + len(_HELD_TAIL)] == _HELD_TAIL, block[j:j + len(_HELD_TAIL)]
    log_lines = block[i + len(_HELD_BRANCH):j]        # only the one Write-Output sits between the test and the mark
    assert log_lines and log_lines[0].startswith("Write-Output (") and "BACKUP WINDOW: a pass is" in log_lines[0]
    assert not any(k in ln for ln in log_lines for k in ("exit", "Set-Content", "$bw =", "$bwSeen ="))


# --- the real held block, run in a temp copy of the layout (Windows PowerShell or pwsh) --------------------------

# Appended to the temp COPY of cadence_decision.ps1 only. It records the instant the runner passed (as the real
# function reads it: Local converted, Unspecified taken as UTC) and its distance from the real clock, then answers
# for the fixed instant in BW_TEST_NOW. The runner itself is copied byte for byte.
_WRAPPER = r"""
${function:Get-BackupWindowHoldReal} = ${function:Get-BackupWindowHold}
function Get-BackupWindowHold {
    param([Parameter(Mandatory = $true)][DateTime] $NowUtc)
    $g = $NowUtc
    if ($g.Kind -eq [DateTimeKind]::Local) { $g = $g.ToUniversalTime() }
    elseif ($g.Kind -eq [DateTimeKind]::Unspecified) { $g = [DateTime]::SpecifyKind($g, [DateTimeKind]::Utc) }
    $skew = [math]::Abs(($g - [DateTime]::UtcNow).TotalMinutes)
    Add-Content -Path $env:BW_TEST_PROBE -Value ('skew_min=' + [math]::Round($skew, 3)) -Encoding ascii
    $fixed = [DateTime]::Parse($env:BW_TEST_NOW, [Globalization.CultureInfo]::InvariantCulture,
        [Globalization.DateTimeStyles]::AdjustToUniversal -bor [Globalization.DateTimeStyles]::AssumeUniversal)
    return (Get-BackupWindowHoldReal -NowUtc $fixed)
}
"""


def _layout(base):
    tools = base / "tools"
    tools.mkdir(parents=True)
    shutil.copyfile(RUNNER, tools / "run_local_heavy.ps1")
    shutil.copyfile(os.path.join(ROOT, "tools", "pass_priority.ps1"), tools / "pass_priority.ps1")
    with open(DECISION, "rb") as f:
        decision = f.read()
    (tools / "cadence_decision.ps1").write_bytes(decision + b"\r\n" + _WRAPPER.encode("ascii"))
    # A lister that routes nothing: a tick that proceeds reaches "starting", then "nothing to do", exit 0.
    (tools / "_list_local_sources.py").write_text("", encoding="ascii")
    return base


def _tick(base, now_utc):
    probe = base / "probe.txt"
    env = dict(os.environ, BW_TEST_NOW=now_utc, BW_TEST_PROBE=str(probe))
    r = subprocess.run([POWERSHELL, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File",
                        str(base / "tools" / "run_local_heavy.ps1"), "-IfDue"],
                       capture_output=True, text=True, cwd=str(base), env=env, timeout=300)
    calls = probe.read_text(encoding="ascii").splitlines() if probe.exists() else []
    if probe.exists():
        probe.unlink()
    return r, calls


def _asked_once_with_utc_now(calls):
    assert len(calls) == 1, ("the runner must ask the window exactly once per tick", calls)
    assert float(calls[0].split("=", 1)[1]) < 5.0, ("the runner must pass the real UTC instant", calls)


def _logs(base):
    d = base / "logs"
    return sorted(p.name for p in d.iterdir()) if d.exists() else []


@pytest.fixture(scope="module")
def held_ticks(tmp_path_factory):
    if POWERSHELL is None:
        pytest.skip("no PowerShell on this host")
    base = _layout(tmp_path_factory.mktemp("bw_held"))
    out = {}
    # 02:30 CST, then 05:00 CST the same night, then 02:30 CST the next night.
    for name, now in (("first", "2026-11-12T08:30:00Z"), ("second", "2026-11-12T11:00:00Z"),
                      ("next_night", "2026-11-13T08:30:00Z")):
        r, calls = _tick(base, now)
        marker = base / "logs" / "local_heavy.backup_hold"
        out[name] = dict(r=r, calls=calls, logs=_logs(base),
                         marker=marker.read_text(encoding="ascii").strip() if marker.exists() else None)
    return out


def _bw_lines(r):
    return [ln for ln in r.stdout.splitlines() if "BACKUP WINDOW" in ln]


def test_held_first_tick_logs_one_line_writes_the_marker_and_nothing_else(held_ticks):
    t = held_ticks["first"]
    assert t["r"].returncode == 0, t["r"].stdout + t["r"].stderr
    _asked_once_with_utc_now(t["calls"])
    assert len(_bw_lines(t["r"])) == 1, t["r"].stdout
    assert "2026-11-12T12:00Z" in _bw_lines(t["r"])[0], t["r"].stdout
    assert "local heavy updater starting" not in t["r"].stdout, t["r"].stdout
    assert t["marker"] == "2026-11-12", t
    assert t["logs"] == ["local_heavy.backup_hold"], t["logs"]     # no pass log, no lock, no stamp


def test_held_second_tick_same_night_is_silent(held_ticks):
    t = held_ticks["second"]
    assert t["r"].returncode == 0, t["r"].stdout + t["r"].stderr
    _asked_once_with_utc_now(t["calls"])
    assert t["r"].stdout.strip() == "", t["r"].stdout
    assert t["marker"] == "2026-11-12", t
    assert t["logs"] == ["local_heavy.backup_hold"], t["logs"]


def test_held_next_night_logs_again(held_ticks):
    t = held_ticks["next_night"]
    assert t["r"].returncode == 0, t["r"].stdout + t["r"].stderr
    assert len(_bw_lines(t["r"])) == 1, t["r"].stdout
    assert t["marker"] == "2026-11-13", t
    assert t["logs"] == ["local_heavy.backup_hold"], t["logs"]


@pytest.mark.parametrize("now", ["2026-11-12T07:59:00Z", "2026-11-12T13:00:00Z"])  # 01:59 and 07:00 CST
def test_a_due_tick_outside_the_window_proceeds(tmp_path, now):
    if POWERSHELL is None:
        pytest.skip("no PowerShell on this host")
    base = _layout(tmp_path)
    r, calls = _tick(base, now)
    _asked_once_with_utc_now(calls)
    assert "BACKUP WINDOW" not in r.stdout, r.stdout
    assert "local heavy updater starting" in r.stdout, r.stdout + r.stderr
    assert "local_heavy.backup_hold" not in _logs(base)


def test_the_window_is_named_constants():
    text = open(DECISION, encoding="utf-8", errors="replace").read()
    assert "$script:BackupWindowStartHour = 2" in text
    assert "$script:BackupWindowEndHour   = 6" in text
    assert "'America/Chicago'" in text
