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

function Write-GuardLog([string]$line) {
  # DEFINED BEFORE ITS FIRST USE. PowerShell resolves functions in execution order, so this sitting
  # below the env loop that calls it meant the very first call failed silently.
  #
  # RETRIED, because these runners start SIMULTANEOUSLY and all append to one file. After the
  # 2026-08-03 reboot the guard launched three at 17:40:26; two wrote their "starting" line and
  # derive_noaa's vanished — Add-Content had lost a lock race and failed silently. I then read the
  # missing line as "the runner never ran" and went hunting a binding bug that was not there.
  # A log that drops lines under contention is worse than no log: it invents absences.
  for ($i = 0; $i -lt 8; $i++) {
    try { Add-Content -Path $guardLog -Value $line -ErrorAction Stop; return }
    catch { Start-Sleep -Milliseconds (60 * ($i + 1)) }
  }
}

# Per-job env, set in THIS process only. Deliberately not set globally in the guard: the crawlers
# write locally and then upload, so flipping AQUEDUCT_BACKEND for everything would change what
# they address (R36/R296 — a tool pointed at the wrong store still looks like it works).
foreach ($pair in $EnvPairs) {
  $i = $pair.IndexOf('=')
  if ($i -gt 0) {
    $k = $pair.Substring(0, $i); $v = $pair.Substring($i + 1)
    Set-Item -Path ("env:{0}" -f $k) -Value $v
    Write-GuardLog ("{0}  {1}: env {2}={3}" -f (Get-Date -Format s), $Name, $k, $v)
  }
}

$JobArgs = $JobArgsJoined -split '\|'

Write-GuardLog ("{0}  starting {1}: {2} {3}" -f (Get-Date -Format s), $Name, $Exe, ($JobArgs -join ' '))
# -u on every job: Python block-buffers stdout when redirected, so a long job emits nothing for
# hours and its log reads as a hang. Cost me a false progress report once already (R290).
# USE THE PATTERN THAT ALREADY WORKS IN THIS REPO, not a third one.
#
# Two attempts failed before this. `*> $log` writes UTF-16 in PS 5.1, so the log could not be
# grepped or tailed from bash. Replacing it with `2>&1 | Out-File -Encoding utf8` fixed the
# encoding and introduced PIPELINE BUFFERING: measured after the 2026-08-03 reboot, statcan and
# eurostat wrote fine while derive_noaa sat at 0 BYTES for eight minutes with the process
# demonstrably alive and running `-u`. A log that stays empty is indistinguishable from a job that
# never started — the exact ambiguity this runner exists to remove (R290).
#
# Start-Process -RedirectStandard* is what RELAUNCH_GUARD.ps1 already uses for the three crawlers,
# and their logs have always been readable and live. The child writes the file with its own
# handle: no pipeline between it and disk, so nothing to buffer and no re-encoding.
$errLog = Join-Path $logsDir ("{0}_{1}.err.log" -f $Name, $stamp)
$proc = Start-Process $Exe -ArgumentList $JobArgs -WorkingDirectory $Root -WindowStyle Hidden `
          -RedirectStandardOutput $log -RedirectStandardError $errLog -PassThru
# CACHE THE HANDLE BEFORE WaitForExit, OR ExitCode COMES BACK $null AND NO JOB IS EVER "DONE".
# Measured on this machine 2026-08-29, PowerShell 5.1.26100.9168, the exact shape used above:
#     Start-Process ... -PassThru; $p.WaitForExit(); $p.ExitCode   -> [] , $null -eq it = True
#     $null = $p.Handle; $p.WaitForExit(); $p.ExitCode             -> 0
# `$null -eq 0` is False, so the `if ($rc -eq 0)` below took the "NO sentinel" branch on EVERY
# clean completion. Consequence, measured: `derive_noaa` finished its campaign around 2026-08-03
# and RELAUNCH_GUARD has resurrected it ever since — 975 launches, 43-61/day, each paging the
# whole `series/noaa%3A` prefix (3,138,169 objects = 3,139 ListObjectsV2 requests) to discover
# `to derive: 0`. That is ~150,700 LIST/day. STATE THE DENOMINATOR (R502/R505): 99.5% of the
# account's LIST operations, but 59.3% of ALL Class A (264,454 on 2026-08-28) — an earlier
# "95-96%" figure named no denominator at all. Relaunch rate measured 43-61/day, not a flat 48.
# ~$20/month for zero work, and R2 had begun answering ServiceUnavailable ("Reduce your
# concurrent request rate"). `derive_statcan` is ~9 hours from finishing and would have joined
# it. run_guarded_job.ps1's own header predicted this failure in words — the mechanism to
# prevent it was written, and then disabled by a null.
#
# .NET releases the process handle once the object is disposed unless it has been dereferenced,
# and PowerShell's Start-Process does not dereference it for you; touching .Handle caches it so
# ExitCode survives the exit. This is the whole fix.
$null = $proc.Handle
$proc.WaitForExit()
$rc = $proc.ExitCode
if ($null -eq $rc) {
  # Never let an unreadable exit code masquerade as failure again: say so loudly rather than
  # silently taking the relaunch branch (R503 — a guard's failure path IS the guard).
  Write-GuardLog ("{0}  {1} EXIT CODE UNREADABLE - treating as failure, NO sentinel; if this " +
                  "recurs the .Handle cache above has regressed" -f (Get-Date -Format s), $Name)
}

if ($rc -eq 0) {
  Set-Content -Path $done -Value ("completed {0} (exit 0), log {1}" -f $stamp, $log) -Encoding utf8
  Write-GuardLog ("{0}  {1} COMPLETED (exit 0) - sentinel written, guard will not relaunch" -f (Get-Date -Format s), $Name)
} else {
  Write-GuardLog ("{0}  {1} exited {2} - NO sentinel, guard will relaunch" -f (Get-Date -Format s), $Name, $rc)
}
exit $rc
