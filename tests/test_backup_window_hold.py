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


def test_the_window_is_named_constants():
    text = open(DECISION, encoding="utf-8", errors="replace").read()
    assert "$script:BackupWindowStartHour = 2" in text
    assert "$script:BackupWindowEndHour   = 6" in text
    assert "'America/Chicago'" in text
