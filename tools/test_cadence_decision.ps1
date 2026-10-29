# Exercise the cadence decision against real PowerShell. Exits 0 on success, 1 on any mismatch.
#
# The two defects this pins are LANGUAGE facts that reading the script cannot catch:
#   * `[int]$null` is 0 and does not throw, so empty stdout used to become a confident 0;
#   * "unknown is not permission" applied only under a hard stop, not on every exit code.
# Both shipped, twice, in a guard whose whole job is to refuse a 20-hour stand-down (R630/R634).

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'cadence_decision.ps1')

$fail = 0
function Check($what, $got, $want) {
    if ("$got" -ne "$want") {
        Write-Output ("FAIL  {0}: got '{1}', want '{2}'" -f $what, $got, $want)
        $script:fail++
    } else {
        Write-Output ("ok    {0} -> {1}" -f $what, $got)
    }
}

# --- Read-ProbeNumber: what the caller can and cannot believe --------------------------------
Check 'null stdout is unknown'            (Read-ProbeNumber $null)             ''
Check 'empty string is unknown'           (Read-ProbeNumber '')                ''
Check 'whitespace is unknown'             (Read-ProbeNumber "  `t ")           ''
Check 'a warning line is unknown'         (Read-ProbeNumber 'warning: blah')   ''
Check 'a sentinel is unknown by default'  (Read-ProbeNumber '-1')              ''
Check 'a negative is read when allowed'   (Read-ProbeNumber '-1' -AllowNegative) '-1'
Check 'zero reads as zero'                (Read-ProbeNumber '0')               '0'
Check 'a count reads as itself'           (Read-ProbeNumber '3')               '3'
Check 'surrounding space is tolerated'    (Read-ProbeNumber ' 12 ')            '12'

# THE FACT THAT BIT TWICE, asserted rather than described.
Check 'the raw cast would have said 0'    ([int]$null)                         '0'

# --- Test-CadenceShouldStamp: the composition -----------------------------------------------
Check 'advanced 3, clean push'   (Test-CadenceShouldStamp -PushRc 0 -Crashed $false -Advanced 3).Stamp   'True'
Check 'advanced 0'               (Test-CadenceShouldStamp -PushRc 0 -Crashed $false -Advanced 0).Stamp   'False'
Check 'unknown on a clean exit'  (Test-CadenceShouldStamp -PushRc 0 -Crashed $false -Advanced $null).Stamp 'False'
Check 'unknown after a crash'    (Test-CadenceShouldStamp -PushRc 0 -Crashed $true  -Advanced $null).Stamp 'False'
Check 'push failed, work done'   (Test-CadenceShouldStamp -PushRc 2 -Crashed $false -Advanced 5).Stamp   'False'
Check 'crash, work done'         (Test-CadenceShouldStamp -PushRc 0 -Crashed $true  -Advanced 5).Stamp   'False'

# The end-to-end shape the runner uses: probe output -> decision. A native command that writes
# to stderr becomes a TERMINATING error under `$ErrorActionPreference = 'Stop'` in PS 5.1, so
# the preference is relaxed for exactly this block - which is also how the runner behaves,
# since it never sets Stop around its probe calls.
$prev = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
$raw = ( & python (Join-Path $PSScriptRoot 'no_such_probe_script.py') 2>$null ) | Select-Object -Last 1
$ErrorActionPreference = $prev
$n = Read-ProbeNumber $raw
Check 'a probe that cannot run is unknown' ($null -eq $n) 'True'
Check 'and therefore does not stamp' (Test-CadenceShouldStamp -PushRc 0 -Crashed $false -Advanced $n).Stamp 'False'

# --- D3: the no-progress back-off --------------------------------------------------------------
# "Advanced nothing" is ONE predicate, shared with the cadence verdict above.
Check 'advanced 0 is nothing'              (Test-PassAdvancedNothing 0)     'True'
Check 'advanced 3 is something'            (Test-PassAdvancedNothing 3)     'False'
Check 'unknown is not nothing'             (Test-PassAdvancedNothing $null) 'False'

# The hold decision. Planted cases, each with the answer worked out by hand; MaxHours 36, WantBlockMin 150,
# back-off 8 h. "Past 36 h" = 40 h since the last pass that advanced something.
function Hold($until, $since, $age) {
    (Test-WindowShouldHold -UntilCronMin $until -WantBlockMin 150 -SinceLastHours $since -MaxHours 36 `
        -NoProgressAgeHours $age -NoProgressBackoffH 8).Hold
}
Check 'D3: stamp 2 h old, 60-min window, past 36 h -> HOLD'      (Hold 60 40 2)      'True'
Check 'D3: stamp 2 h old, 200-min window, past 36 h -> START'    (Hold 200 40 2)     'False'
Check 'D3: stamp 9 h old, 60-min window, past 36 h -> START'     (Hold 60 40 9)      'False'
Check 'D3: stamp exactly 8 h old -> START (strict <, as the replay)' (Hold 60 40 8)  'False'
Check 'D3: stamp 2 h old, exactly 150 min -> START'              (Hold 150 40 2)     'False'
# No stamp: today's behaviour, both sides of MaxHours.
Check 'no stamp, 60-min window, past 36 h -> START (today)'      (Hold 60 40 $null)  'False'
Check 'no stamp, 60-min window, 10 h since -> HOLD (today)'      (Hold 60 10 $null)  'True'
Check 'no stamp, 200-min window, 10 h since -> START (today)'    (Hold 200 10 $null) 'False'
# The old hold is unchanged whatever the stamp says.
Check 'stamp 9 h old, 60-min window, 10 h since -> HOLD (old rule)' (Hold 60 10 9)   'True'

# Which passes may write the stamp. Only a pulled, pushed, scheduled, orderly pass that advanced nothing.
function NoProg($push, $crash, $hard, $manual, $pulled, $adv) {
    (Test-NoProgressShouldStamp -PushRc $push -Crashed $crash -HardStopped $hard -Manual $manual `
        -PulledState $pulled -Advanced $adv).Stamp
}
Check 'pushed, advanced 0 -> write'                    (NoProg 0 $false $false $false $true 0)     'True'
Check 'pushed, ADVANCED 3 -> no stamp'                 (NoProg 0 $false $false $false $true 3)     'False'
Check 'pushed, advanced unknown -> no stamp'           (NoProg 0 $false $false $false $true $null) 'False'
Check 'push FAILED, advanced 0 -> no stamp'            (NoProg 2 $false $false $false $true 0)     'False'
Check 'CRASHED, advanced 0 -> no stamp'                (NoProg 0 $true  $false $false $true 0)     'False'
Check 'HARD STOP rc 124, advanced 0 -> no stamp'       (NoProg 0 $false $true  $false $true 0)     'False'
Check 'manual -Only/-Force, advanced 0 -> no stamp'    (NoProg 0 $false $false $true  $true 0)     'False'
Check 'no state pull (after T0), advanced 0 -> no stamp' (NoProg 0 $false $false $false $false 0) 'False'

# Reading the stamp. Planted files in a scratch directory; $Now is fixed so the ages are exact.
$tmpDir = Join-Path ([IO.Path]::GetTempPath()) ('d3_' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $tmpDir | Out-Null
try {
    $now = [DateTime]::Parse('2026-10-28T12:00:00.0000000Z', [Globalization.CultureInfo]::InvariantCulture,
                             [Globalization.DateTimeStyles]::RoundtripKind).ToUniversalTime()
    $f = Join-Path $tmpDir 'local_heavy.last_noprogress'
    Check 'missing stamp file -> no age'   ($null -eq (Get-StampAgeHours -Path $f -Now $now)) 'True'
    Set-Content -Path $f -Value $now.AddHours(-2).ToString('o') -Encoding ascii
    Check 'stamp written 2 h ago -> 2 h'   ([math]::Round((Get-StampAgeHours -Path $f -Now $now), 6)) '2'
    Set-Content -Path $f -Value $now.AddHours(-9).ToString('o') -Encoding ascii
    Check 'stamp written 9 h ago -> 9 h'   ([math]::Round((Get-StampAgeHours -Path $f -Now $now), 6)) '9'
    Set-Content -Path $f -Value 'not a date' -Encoding ascii
    Check 'garbage stamp -> no age'        ($null -eq (Get-StampAgeHours -Path $f -Now $now)) 'True'
    Set-Content -Path $f -Value '' -Encoding ascii
    Check 'empty stamp -> no age'          ($null -eq (Get-StampAgeHours -Path $f -Now $now)) 'True'
    Set-Content -Path $f -Value $now.AddHours(5).ToString('o') -Encoding ascii
    Check 'FUTURE stamp -> no age (would hold forever)' ($null -eq (Get-StampAgeHours -Path $f -Now $now)) 'True'
    # End to end from a planted file: 2 h old stamp, 60-min window, past 36 h -> HOLD; 9 h -> START.
    Set-Content -Path $f -Value $now.AddHours(-2).ToString('o') -Encoding ascii
    Check 'planted 2 h stamp -> HOLD'  (Hold 60 40 (Get-StampAgeHours -Path $f -Now $now)) 'True'
    Set-Content -Path $f -Value $now.AddHours(-9).ToString('o') -Encoding ascii
    Check 'planted 9 h stamp -> START' (Hold 60 40 (Get-StampAgeHours -Path $f -Now $now)) 'False'
} finally {
    Remove-Item -Recurse -Force $tmpDir -ErrorAction SilentlyContinue
}

if ($fail -gt 0) { Write-Output "$fail check(s) FAILED"; exit 1 }
Write-Output 'all cadence-decision checks passed'
exit 0
