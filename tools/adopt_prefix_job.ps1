# Write the .DONE sentinel for a guarded job whose SUPERVISOR predates the .Handle fix.
#
# WHY THIS EXISTS. run_guarded_job.ps1 gained `$null = $proc.Handle` on 2026-08-29T14:30
# (commit e9587cb8f). Without it PowerShell 5.1 returns a NULL ExitCode, `if ($rc -eq 0)` is
# false on every clean completion, and no job is ever marked done -- which is how RELAUNCH_GUARD
# resurrected a finished `derive_noaa` 975 times, ~168,000 R2 LIST/day for zero work.
#
# PowerShell reads a script into memory at launch, so a supervisor STARTED BEFORE that commit
# is still executing the old code no matter what is on disk now. Measured 2026-08-30:
# `derive_statcan`'s supervisor (pid 24344) started 2026-08-22T21:07:39, seven days early, while
# its job sat at 8,173 of 8,207 tables. When it finishes, that supervisor will fail to write the
# sentinel and the guard will relaunch a SEVEN-DAY job that re-lists ~3M R2 keys -- the noaa
# failure again, on the most expensive job in the fleet.
#
# Restarting the job under the fixed script was never an option at 99.6% complete. So this
# adopts the RUNNING process from outside: cache the handle FIRST (the whole trick), wait, read
# the real exit code, and write the sentinel only on a clean exit.
#
#   powershell -NoProfile -ExecutionPolicy Bypass -File tools\adopt_prefix_job.ps1 `
#       -Pid 24636 -Name derive_statcan -MustMatch derive_statcan_tables.py
#
# SAFE BY CONSTRUCTION:
#   * -MustMatch is verified against the live command line before adopting, so a recycled PID
#     cannot make us certify the wrong process (R49/R260: never act on a loose process match).
#   * The sentinel is written ONLY on exit code 0. A crash, a kill or an unreadable code leaves
#     no sentinel, so the guard still relaunches -- the failure direction stays the safe one.
#   * It never kills, signals or touches the job. Read-only until the moment it writes one file.
param(
  [Parameter(Mandatory=$true)][int]$TargetPid,
  [Parameter(Mandatory=$true)][string]$Name,
  [Parameter(Mandatory=$true)][string]$MustMatch,
  [string]$Root = 'E:\research\econfindatalibrary'
)
$ErrorActionPreference = 'Continue'
$guardLog = Join-Path $Root 'logs\_guard.log'
$done     = Join-Path $Root ("logs\{0}.DONE" -f $Name)

function Write-GuardLog([string]$line) {
  for ($i = 0; $i -lt 8; $i++) {
    try { Add-Content -Path $guardLog -Value $line -ErrorAction Stop; return }
    catch { Start-Sleep -Milliseconds (60 * ($i + 1)) }
  }
}

# IDENTITY BEFORE ACTION. A PID alone is not an identity on a machine that has been up for days.
$ci = Get-CimInstance Win32_Process -Filter "ProcessId=$TargetPid" -ErrorAction SilentlyContinue
if (-not $ci) {
  Write-GuardLog ("{0}  adopt {1}: pid {2} is GONE -- cannot certify, no sentinel" -f (Get-Date -Format s), $Name, $TargetPid)
  exit 2
}
if ($ci.CommandLine -notlike "*$MustMatch*") {
  Write-GuardLog ("{0}  adopt {1}: pid {2} does NOT match '{3}' -- refusing (PID reuse)" -f (Get-Date -Format s), $Name, $TargetPid, $MustMatch)
  exit 3
}

try { $proc = Get-Process -Id $TargetPid -ErrorAction Stop } catch {
  Write-GuardLog ("{0}  adopt {1}: pid {2} vanished between checks, no sentinel" -f (Get-Date -Format s), $Name, $TargetPid)
  exit 2
}
# THE WHOLE POINT: dereference .Handle BEFORE WaitForExit, or ExitCode comes back $null and we
# reproduce the exact bug we are here to work around.
$null = $proc.Handle
Write-GuardLog ("{0}  adopt {1}: watching pid {2} (supervisor predates the .Handle fix); will write {3} on exit 0" -f (Get-Date -Format s), $Name, $TargetPid, $done)
$proc.WaitForExit()
$rc = $proc.ExitCode

if ($null -eq $rc) {
  Write-GuardLog ("{0}  adopt {1}: exit code STILL unreadable -- no sentinel; investigate before assuming the job finished" -f (Get-Date -Format s), $Name)
  exit 4
}
if ($rc -eq 0) {
  Set-Content -Path $done -Value ("completed (exit 0) via adopt_prefix_job.ps1 at {0}; supervisor pid predated the .Handle fix" -f ((Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ'))) -Encoding utf8
  Write-GuardLog ("{0}  adopt {1}: COMPLETED (exit 0) -- sentinel written, guard will not relaunch" -f (Get-Date -Format s), $Name)
  exit 0
}
Write-GuardLog ("{0}  adopt {1}: exited {2} -- NO sentinel, guard will relaunch (correct)" -f (Get-Date -Format s), $Name, $rc)
exit $rc
