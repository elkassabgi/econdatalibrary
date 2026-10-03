<#
    register_sec_edgar_task.ps1 - register (once) the Windows scheduled task that runs the daily sec_edgar refresh
    on this workstation: tools\selfhost\run_sec_edgar_local.ps1.

    ASCII ONLY (see tools/run_local_heavy.ps1: Windows PowerShell 5.1 reads a BOM-less .ps1 as ANSI).

    FOR AHMED TO RUN (a scheduled task is a system setting; Claude prepares it and does not run it):

        powershell -NoProfile -ExecutionPolicy Bypass -File E:\research\econfindatalibrary\tools\selfhost\register_sec_edgar_task.ps1 -WhatIf
        powershell -NoProfile -ExecutionPolicy Bypass -File E:\research\econfindatalibrary\tools\selfhost\register_sec_edgar_task.ps1
        powershell -NoProfile -ExecutionPolicy Bypass -File E:\research\econfindatalibrary\tools\selfhost\register_sec_edgar_task.ps1 -Unregister

    It is safe to register BEFORE T0: until C:\ProgramData\econ\CUTOVER exists the task runs, writes one log line
    ("not cut over - nothing to do") and exits 0. It needs no administrator rights and no password: it runs as
    the signed-in user, like the other workstation jobs (the logon launcher), and a run missed while the machine
    was off or signed out starts at the next sign-in (-StartWhenAvailable). Missed days are not lost: the scan
    window reaches back to the last OK daily scan.

    WHEN. Daily at 03:00 local time (08:00 UTC in summer, 09:00 UTC in winter) - the hour the CI job
    sec-edgar-daily runs now (08:00 UTC), after SEC has posted the previous day's index.
#>
param(
    [switch] $WhatIf,
    [switch] $Unregister,
    [string] $At = '03:00'
)
$ErrorActionPreference = 'Stop'
$taskName = 'EconSecEdgarLocal'
$root     = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$runner   = Join-Path $root 'tools\selfhost\run_sec_edgar_local.ps1'
if (-not (Test-Path $runner)) { throw "runner not found: $runner" }

$existing = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($Unregister) {
    if (-not $existing) { Write-Output "no task named $taskName - nothing to remove"; exit 0 }
    if ($WhatIf) { Write-Output "WHATIF: would unregister $taskName"; exit 0 }
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
    Write-Output "unregistered $taskName"
    exit 0
}

$user = "$env:USERDOMAIN\$env:USERNAME"
$action = New-ScheduledTaskAction -Execute 'powershell.exe' -WorkingDirectory $root `
    -Argument ('-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File "{0}"' -f $runner)
$trigger = New-ScheduledTaskTrigger -Daily -At $At
# 4 h is far above a normal day (about 150 companies at 0.12 s each, plus the merge); a hung run is stopped
# and the next day's window covers it. IgnoreNew: never two at once (they would share the writer lock anyway).
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Hours 4) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
$principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited

Write-Output "task     : $taskName"
Write-Output "runs     : powershell.exe -File `"$runner`""
Write-Output "when     : daily at $At local time; a missed run starts at the next sign-in"
Write-Output "as       : $user (signed in, no password stored, not elevated)"
Write-Output "existing : $(if ($existing) { 'yes - it will be replaced' } else { 'no' })"
if ($WhatIf) { Write-Output "WHATIF: nothing registered"; exit 0 }

Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings `
    -Principal $principal -Description 'Daily sec_edgar refresh on the self-hosted econ store (does nothing before T0). tools\selfhost\run_sec_edgar_local.ps1' `
    -Force | Out-Null
$t = Get-ScheduledTask -TaskName $taskName
Write-Output ("registered: state {0}, next run {1}" -f $t.State, (Get-ScheduledTaskInfo -TaskName $taskName).NextRunTime)
