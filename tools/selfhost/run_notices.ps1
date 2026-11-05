<#
    run_notices.ps1 - the daily publisher-notice watch on this workstation (jobs/watch_notices.py).

    ASCII ONLY (see tools/run_local_heavy.ps1: Windows PowerShell 5.1 reads a BOM-less .ps1 as ANSI).

    WHO STARTS IT. The guard loop, every 5-minute tick, with -IfDue (tools/machine/RELAUNCH_GUARD.ps1
    .workstation-copy), as run_sec_edgar_local.ps1 runs: Scheduled Tasks are blocked by policy here.

    NO T0 GATE. The watcher reads publishers' feeds and writes only data\_notices\state.json and
    data\_maintenance\queue.jsonl. It never opens the updater's state or the catalogue, so it cannot race
    a data run, before or after T0.

    -IfDue: exit 0 at once, writing nothing, unless a run is due:
      * it is 09:00 UTC or later (03:00 on this machine's clock: off-peak), and no run that STARTED today
        (UTC) ended with exit 0. Each feed has its own poll_days on top, so a daily run fetches only the
        feeds that are due.
      * no attempt started in the last 2 h (a failed day is retried every 2 h, not every 5 minutes).
      * no other run holds logs\notices.lock (PID plus start time; a dead or reused PID is a stale lock).

    WHAT IT RUNS. `python jobs\watch_notices.py` at BelowNormal priority (the owner's own work comes first)
    and nothing else. It publishes nothing: queue items are private until triaged (tools/maintenance_queue.py).

    WHERE TO LOOK. logs\notices_<UTC stamp>.log holds the run's output; logs\notices.last.json holds
    {started, ended, rc, pid, last_ok_started} ("ended": null while running, or after a kill).
#>
param(
    [switch] $IfDue,
    # Overrides FOR TESTS ONLY (tests/test_notices_schedule.py). The guard passes none of them.
    [string] $Python = 'C:\Users\aelkassabgi\AppData\Local\Programs\Python\Python314\python.exe',
    [string] $NowUtc = '',
    [string] $Job    = 'jobs\watch_notices.py'
)
$ErrorActionPreference = 'Continue'
$root    = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$logsDir = Join-Path $root 'logs'
$status  = Join-Path $logsDir 'notices.last.json'
$lock    = Join-Path $logsDir 'notices.lock'

function Utc([string]$s) {
    if (-not $s) { return $null }
    try { return [DateTimeOffset]::Parse($s, [Globalization.CultureInfo]::InvariantCulture).UtcDateTime } catch { return $null }
}
function Iso($d) { if ($null -eq $d) { return $null } return $d.ToString('yyyy-MM-ddTHH:mm:ssZ') }

$now = if ($NowUtc) { Utc $NowUtc } else { (Get-Date).ToUniversalTime() }
if ($null -eq $now) { Write-Output "refused: -NowUtc is not a time: $NowUtc"; exit 2 }

$prev = $null
if (Test-Path -LiteralPath $status) {
    try { $prev = Get-Content -LiteralPath $status -Raw | ConvertFrom-Json } catch { $prev = $null }
}
$prevStarted = if ($prev) { Utc $prev.started } else { $null }
$prevOk      = if ($prev) { Utc $prev.last_ok_started } else { $null }

if ($IfDue) {
    if ($now.Hour -lt 9) { exit 0 }
    if ($prevOk -and $prevOk.Date -eq $now.Date) { exit 0 }               # today's run already succeeded
    if ($prevStarted -and ($now - $prevStarted).TotalHours -lt 2) { exit 0 }
}

function Write-Status($r) {
    $tmp = $status + '.tmp'
    Set-Content -LiteralPath $tmp -Encoding ascii -Value ($r | ConvertTo-Json -Compress)
    for ($i = 1; $i -le 10; $i++) {
        try { Move-Item -LiteralPath $tmp -Destination $status -Force -ErrorAction Stop; return }
        catch { Start-Sleep -Milliseconds (100 * $i) }
    }
    Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
    if ($log) { Add-Content -LiteralPath $log -Value "runner: the status file could not be replaced (held open); it is stale" }
}

New-Item -ItemType Directory -Force -Path $logsDir | Out-Null
if (Test-Path -LiteralPath $lock) {
    $held = $false
    try {
        $parts = ((Get-Content -LiteralPath $lock -First 1).Trim()) -split ','
        $other = [int]$parts[0]
        $proc = Get-Process -Id $other -ErrorAction SilentlyContinue
        if ($proc -and $other -ne $PID -and $proc.ProcessName -eq 'powershell' -and $parts.Count -ge 2 -and
            $proc.StartTime.ToUniversalTime().Ticks.ToString() -eq $parts[1]) { $held = $true }
    } catch { $held = $false }
    if ($held) {
        if (-not $IfDue) { Write-Output "refused: another notices run holds $lock (pid $other)"; exit 3 }
        exit 0
    }
}
$me = Get-Process -Id $PID
Set-Content -LiteralPath $lock -Encoding ascii -Value ("{0},{1}" -f $PID, $me.StartTime.ToUniversalTime().Ticks)

$stamp = $now.ToString('yyyyMMddTHHmmssZ')
$log   = Join-Path $logsDir ("notices_{0}.log" -f $stamp)
$err   = $log + '.stderr'
$rec = [ordered]@{ started = (Iso $now); ended = $null; rc = $null; pid = $PID; last_ok_started = (Iso $prevOk) }
Write-Status $rec

$env:PYTHONIOENCODING = 'utf-8'
$env:PYTHONDONTWRITEBYTECODE = '1'
$rc = 99
Push-Location $root
try {
    $p = Start-Process -FilePath $Python -NoNewWindow -PassThru `
        -ArgumentList @('-B', '-u', $Job) -RedirectStandardOutput $log -RedirectStandardError $err
    $null = $p.Handle            # PS 5.1: without a handle read now, ExitCode is $null after WaitForExit
    try { $p.PriorityClass = 'BelowNormal' } catch { }                    # a run that ended at once has no handle
    $p.WaitForExit()
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
Write-Status $rec
Remove-Item -LiteralPath $lock -Force -ErrorAction SilentlyContinue

Get-ChildItem -Path $logsDir -Filter 'notices_*.log' -File -ErrorAction SilentlyContinue |
    Sort-Object Name -Descending | Select-Object -Skip 60 | Remove-Item -Force -ErrorAction SilentlyContinue
exit $rc
