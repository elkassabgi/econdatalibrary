# Run one long job under the guard, and write its .DONE sentinel ONLY on clean completion.
#
# WHY THIS EXISTS. RELAUNCH_GUARD.ps1 relaunches any tracked job that is not currently running,
# which is exactly right for the crawlers (they never "finish" — they resume forever). The
# derive/measurement jobs DO finish, and without a sentinel the guard would restart a completed
# job every five minutes for ever: statcan would re-list ~3M R2 keys each time, noaa would redo a
# 14-hour pass, and the eurostat dry run would never stop re-measuring.
#
# The guard already has the convention — logs/<name>.DONE means "do not relaunch" — but nothing
# wrote it automatically, so it depended on an operator remembering. This writes it from the job's
# own exit code, which is the only thing that actually knows.
#
# EXIT 0 ONLY. A crash, a kill, or a reboot mid-run leaves NO sentinel, so the guard picks the job
# back up on its next tick. That is the whole point: the sentinel must mean "finished", never
# "started" or "died" (the R273 shape — state that only means something if the run survives).
param(
  [Parameter(Mandatory=$true)][string]$Name,      # sentinel/log stem, e.g. 'derive_noaa'
  [Parameter(Mandatory=$true)][string]$Exe,       # interpreter
  # PIPE-DELIMITED, not [string[]]. PowerShell's parameter binder treats any array element that
  # starts with '-' as a PARAMETER NAME, so `-JobArgs -u tools/x.py --dry-run` fails to bind and
  # the script dies before its first line — silently, from the guard's point of view, which had
  # already logged "relaunched". Measured 2026-08-03: the guard logged the relaunch, no process
  # appeared, and no log was written, because nothing in this file ever executed.
  [Parameter(Mandatory=$true)][string]$JobArgsJoined,
  [string]$Root = 'E:\research\econfindatalibrary',
  # k=v strings, NOT a hashtable: this script is invoked via `powershell -File`, which passes
  # everything as strings, so a [hashtable] parameter would arrive as the literal text
  # "System.Collections.Hashtable" and the env would silently not be set — the exact class of
  # failure where the job runs, looks fine, and addresses the wrong store (R296).
  [string[]]$EnvPairs = @()
)
$ErrorActionPreference = 'Continue'
$logsDir = Join-Path $Root 'logs'
$stamp   = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ')
$log     = Join-Path $logsDir ("{0}_{1}.log" -f $Name, $stamp)
$done    = Join-Path $logsDir ("{0}.DONE" -f $Name)
$guardLog = Join-Path $logsDir '_guard.log'

# Per-job env, set in THIS process only. Deliberately not set globally in the guard: the crawlers
# write locally and then upload, so flipping AQUEDUCT_BACKEND for everything would change what
# they address (R36/R296 — a tool pointed at the wrong store still looks like it works).
foreach ($pair in $EnvPairs) {
  $i = $pair.IndexOf('=')
  if ($i -gt 0) {
    $k = $pair.Substring(0, $i); $v = $pair.Substring($i + 1)
    Set-Item -Path ("env:{0}" -f $k) -Value $v
    Add-Content -Path (Join-Path $logsDir '_guard.log') -Value ("{0}  {1}: env {2}={3}" -f (Get-Date -Format s), $Name, $k, $v)
  }
}

$JobArgs = $JobArgsJoined -split '\|'
Add-Content -Path $guardLog -Value ("{0}  starting {1}: {2} {3}" -f (Get-Date -Format s), $Name, $Exe, ($JobArgs -join ' '))
# -u on every job: Python block-buffers stdout when redirected, so a long job emits nothing for
# hours and its log reads as a hang. Cost me a false progress report once already (R290).
# `*> $log` would write UTF-16 (PowerShell 5.1's redirect default), which makes the log awkward
# to grep or tail from bash — and these logs are the only window into a 14-hour job. Route through
# Out-File with an explicit encoding instead.
& $Exe @JobArgs 2>&1 | Out-File -FilePath $log -Encoding utf8
$rc = $LASTEXITCODE

if ($rc -eq 0) {
  Set-Content -Path $done -Value ("completed {0} (exit 0), log {1}" -f $stamp, $log) -Encoding utf8
  Add-Content -Path $guardLog -Value ("{0}  {1} COMPLETED (exit 0) - sentinel written, guard will not relaunch" -f (Get-Date -Format s), $Name)
} else {
  Add-Content -Path $guardLog -Value ("{0}  {1} exited {2} - NO sentinel, guard will relaunch" -f (Get-Date -Format s), $Name, $rc)
}
exit $rc
