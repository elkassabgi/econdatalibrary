<#
    run_sec_edgar_local.ps1 - the daily refresh of sec_edgar on this workstation, after T0.

    ASCII ONLY (see tools/run_local_heavy.ps1: Windows PowerShell 5.1 reads a BOM-less .ps1 as ANSI).

    WHO STARTS IT. The guard loop, every 5-minute tick, with -IfDue (tools/machine/RELAUNCH_GUARD.ps1
    .workstation-copy). Scheduled Tasks are blocked by policy on this machine (tools/machine/README.md), so the
    Startup guard loop is the only reboot-surviving trigger - the way run_local_heavy.ps1 runs (plan: "joins
    THAT loop ... -IfDue").

    -IfDue: exit 0 at once, writing nothing, unless a run is due:
      * the T0 flag exists (C:\ProgramData\econ\CUTOVER). Before T0 the CI job sec-edgar-daily refreshes
        sec_edgar and a second writer would race it. Test-Path says "absent" also when it cannot tell; that
        only means "do nothing", and refresh_sec_edgar.py --local-only checks the flag again itself.
      * it is 08:00 UTC or later, and no run that STARTED today (UTC) ended with exit 0. 08:00 UTC is the
        hour sec-edgar-daily was scheduled before T0 (SEC has posted the previous day's index by then), and
        the run is short (CI: 1-3 min), so it does not hold the writer lock against the local heavy pass or
        the catalogue swap for long. A day it misses is not lost: the scan window reaches back to the mark.
      * no attempt started in the last 2 h (a failed day is retried every 2 h, not every 5 minutes).
      * no other run holds logs\sec_edgar_local.lock (a PID we wrote; a dead PID is a stale lock).

    WHAT IT RUNS. `refresh_sec_edgar.py --local-only --apply` and nothing else: the daily run, the one run that
    may move the scan mark. Repairs (--ciks) and test runs are manual. The FIRST run after T0 is manual too,
    with --days wide enough to reach the last CI scan (T0 step 6); until a mark exists the refresher refuses
    and this run's rc says so.

    WHERE TO LOOK. logs\sec_edgar_local_<UTC stamp>.log holds the run's output. logs\sec_edgar_local.last.json
    holds {started, ended, rc, pid, last_ok_started}; "ended": null while a run is going - or after it was
    killed. tools/guard_heartbeat.py reads it into the beat and judges it (after T0: rc not 0, no ok run in
    26 h, or a run started more than 4 h ago that never ended).
#>
param(
    [switch] $IfDue,
    # Overrides FOR TESTS ONLY (tests/test_sec_edgar_local_task.py). The guard passes none of them.
    [string] $Python   = 'C:\Users\aelkassabgi\AppData\Local\Programs\Python\Python314\python.exe',
    [string] $FlagPath = 'C:\ProgramData\econ\CUTOVER',
    [string] $NowUtc   = ''
)
$ErrorActionPreference = 'Continue'
$root    = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$logsDir = Join-Path $root 'logs'
$status  = Join-Path $logsDir 'sec_edgar_local.last.json'
$lock    = Join-Path $logsDir 'sec_edgar_local.lock'

function Utc([string]$s) {
    if (-not $s) { return $null }
    try { return [DateTimeOffset]::Parse($s, [Globalization.CultureInfo]::InvariantCulture).UtcDateTime } catch { return $null }
}
function Iso($d) { if ($null -eq $d) { return $null } return $d.ToString('yyyy-MM-ddTHH:mm:ssZ') }

$now = if ($NowUtc) { Utc $NowUtc } else { (Get-Date).ToUniversalTime() }
if ($null -eq $now) { Write-Output "refused: -NowUtc is not a time: $NowUtc"; exit 2 }

if ($IfDue -and -not (Test-Path -LiteralPath $FlagPath)) { exit 0 }      # before T0: nothing, not even a log

$prev = $null
if (Test-Path -LiteralPath $status) {
    try { $prev = Get-Content -LiteralPath $status -Raw | ConvertFrom-Json } catch { $prev = $null }
}
$prevStarted = if ($prev) { Utc $prev.started } else { $null }
$prevOk      = if ($prev) { Utc $prev.last_ok_started } else { $null }

if ($IfDue) {
    if ($now.Hour -lt 8) { exit 0 }
    if ($prevOk -and $prevOk.Date -eq $now.Date) { exit 0 }               # today's run already succeeded
    if ($prevStarted -and ($now - $prevStarted).TotalHours -lt 2) { exit 0 }
}

New-Item -ItemType Directory -Force -Path $logsDir | Out-Null
if (Test-Path -LiteralPath $lock) {
    $other = 0
    try { $other = [int]((Get-Content -LiteralPath $lock -Raw).Trim()) } catch { $other = 0 }
    if ($other -gt 0 -and $other -ne $PID -and (Get-Process -Id $other -ErrorAction SilentlyContinue)) {
        if (-not $IfDue) { Write-Output "refused: another sec_edgar local run holds $lock (pid $other)" ; exit 3 }
        exit 0
    }
}
Set-Content -LiteralPath $lock -Value $PID -Encoding ascii

$stamp = $now.ToString('yyyyMMddTHHmmssZ')
$log   = Join-Path $logsDir ("sec_edgar_local_{0}.log" -f $stamp)
$err   = $log + '.stderr'
# "started" FIRST, with no end: a run that is killed leaves exactly that, never the previous run's rc.
$rec = [ordered]@{ started = (Iso $now); ended = $null; rc = $null; pid = $PID; last_ok_started = (Iso $prevOk) }
Set-Content -LiteralPath $status -Encoding ascii -Value ($rec | ConvertTo-Json -Compress)

$env:PYTHONIOENCODING = 'utf-8'
$env:PYTHONDONTWRITEBYTECODE = '1'
$rc = 99
Push-Location $root
try {
    # Start-Process with redirected streams: the exit code is the child's own, and its stderr is not turned
    # into PowerShell error records (Windows PowerShell 5.1 does that to `2>&1` on a native exe).
    $p = Start-Process -FilePath $Python -NoNewWindow -Wait -PassThru `
        -ArgumentList @('-B', '-u', 'tools\refresh_sec_edgar.py', '--local-only', '--apply') `
        -RedirectStandardOutput $log -RedirectStandardError $err
    $rc = $p.ExitCode
    if ($null -eq $rc) { $rc = 97 }
} catch {
    Add-Content -LiteralPath $log -Value ("runner error: {0}" -f $_.Exception.Message)
    $rc = 98
} finally {
    if (Test-Path -LiteralPath $err) {
        if ((Get-Item -LiteralPath $err).Length -gt 0) {
            Add-Content -LiteralPath $log -Value '---- stderr ----'
            Get-Content -LiteralPath $err | Add-Content -LiteralPath $log
        }
        Remove-Item -LiteralPath $err -Force -ErrorAction SilentlyContinue
    }
    Pop-Location
}
$end = if ($NowUtc) { $now } else { (Get-Date).ToUniversalTime() }
$rec.ended = (Iso $end)
$rec.rc = $rc
if ($rc -eq 0) { $rec.last_ok_started = (Iso $now) }
Set-Content -LiteralPath $status -Encoding ascii -Value ($rec | ConvertTo-Json -Compress)
Remove-Item -LiteralPath $lock -Force -ErrorAction SilentlyContinue

Get-ChildItem -Path $logsDir -Filter 'sec_edgar_local_*.log' -File -ErrorAction SilentlyContinue |
    Sort-Object Name -Descending | Select-Object -Skip 60 | Remove-Item -Force -ErrorAction SilentlyContinue
exit $rc
