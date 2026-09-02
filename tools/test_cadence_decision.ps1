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

if ($fail -gt 0) { Write-Output "$fail check(s) FAILED"; exit 1 }
Write-Output 'all cadence-decision checks passed'
exit 0
