<#
    run_sec_edgar_local.ps1 - the scheduled daily refresh of sec_edgar on this workstation.

    ASCII ONLY (see tools/run_local_heavy.ps1: Windows PowerShell 5.1 reads a BOM-less .ps1 as ANSI).

    WHAT IT RUNS. `refresh_sec_edgar.py --local-only --apply`, nothing else:
      * before T0 (no C:\ProgramData\econ\CUTOVER) it does nothing and exits 0 - the CI job sec-edgar-daily
        refreshes sec_edgar until then, and a second writer would race it. So the task can be registered
        early and starts working at the flag, with nothing else to switch on.
      * after T0 it is the daily run: the scan window reaches back to the mark (the last OK daily scan), and
        only this run may move the mark. Repairs (--ciks) and test runs (--limit, --days) are manual and
        never moved the mark (refresh_sec_edgar.may_advance).
      * the FIRST run after T0 is manual, with --days wide enough to reach the last CI scan (T0 step 6):
        until a mark exists this task refuses (exit non-zero) instead of guessing a window.

    WHERE TO LOOK. logs\sec_edgar_local_<UTC stamp>.log is the run's whole output; logs\sec_edgar_local.last
    holds one line "<UTC start> <UTC end> rc=<exit code>" for a monitor to read. The newest 60 logs are kept.

    Registered by tools\selfhost\register_sec_edgar_task.ps1 (Ahmed runs that once).
#>
$ErrorActionPreference = 'Continue'
$root    = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$logsDir = Join-Path $root 'logs'
# Pinned interpreter, as RELAUNCH_GUARD.ps1 and run_local_heavy.ps1 do: a bare "python" resolves through PATH.
$python  = 'C:\Users\aelkassabgi\AppData\Local\Programs\Python\Python314\python.exe'
New-Item -ItemType Directory -Force -Path $logsDir | Out-Null
$start = (Get-Date).ToUniversalTime()
$stamp = $start.ToString('yyyyMMddTHHmmssZ')
$log   = Join-Path $logsDir ("sec_edgar_local_{0}.log" -f $stamp)
$last  = Join-Path $logsDir 'sec_edgar_local.last'

$env:PYTHONIOENCODING = 'utf-8'
$env:PYTHONDONTWRITEBYTECODE = '1'
$rc = 99
Push-Location $root
try {
    # Start-Process with redirected streams: the exit code is the child's own, and nothing it prints to stderr
    # is turned into a PowerShell error record (Windows PowerShell 5.1 does that to `2>&1` on a native exe).
    $err = $log + '.stderr'
    $p = Start-Process -FilePath $python -NoNewWindow -Wait -PassThru `
        -ArgumentList @('-B', '-u', 'tools\refresh_sec_edgar.py', '--local-only', '--apply') `
        -RedirectStandardOutput $log -RedirectStandardError $err
    $rc = $p.ExitCode
    if (Test-Path $err) {
        if ((Get-Item $err).Length -gt 0) { Add-Content -Path $log -Value "---- stderr ----"; Get-Content $err | Add-Content -Path $log }
        Remove-Item $err -Force -ErrorAction SilentlyContinue
    }
} catch {
    Add-Content -Path $log -Value ("runner error: {0}" -f $_.Exception.Message)
    $rc = 98
} finally {
    Pop-Location
}
$end = (Get-Date).ToUniversalTime()
Set-Content -Path $last -Encoding ascii -Value ("{0} {1} rc={2}" -f $start.ToString('yyyy-MM-ddTHH:mm:ssZ'), $end.ToString('yyyy-MM-ddTHH:mm:ssZ'), $rc)

Get-ChildItem -Path $logsDir -Filter 'sec_edgar_local_*.log' -File -ErrorAction SilentlyContinue |
    Sort-Object Name -Descending | Select-Object -Skip 60 | Remove-Item -Force -ErrorAction SilentlyContinue
exit $rc
