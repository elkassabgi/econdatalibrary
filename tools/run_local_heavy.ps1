<#
    run_local_heavy.ps1 - update the databases the cloud cannot process, on this workstation.

    ASCII ONLY, DELIBERATELY. Windows PowerShell 5.1 reads a .ps1 that has no BOM using the
    system ANSI codepage, not UTF-8. A single em-dash or smart quote is then mis-decoded and
    can corrupt the token stream MID-FILE - the first version of this script silently skipped
    lines 67-120 and exited 0 with no error at all. Keep every character in this file ASCII.

    WHY THIS EXISTS. A 16 GB GitHub runner cannot update every database here.
    updater.merge.merge_and_write reads the WHOLE existing parquet on every call, so the peak
    for one merge is set by the biggest single FILE a source holds. Measured by
    tools/audit_cloud_capacity.py:

        oecd        1,792,000,000 rows in one file   ~125 GB decoded
        statcan       962,150,400                     ~67 GB
        gus_dbw       358,524,120                     ~25 GB
        noaa          262,514,152                     ~18 GB
        cepii_baci    242,914,764                     ~17 GB
        ... 16 sources in total, down to abs at ~2.1 GB

    Five of those need more memory than an entire runner HAS, so no amount of cloud isolation
    fixes them: a matrix job gives a source its own runner, not a bigger one. Owner's standing
    rule - a database too big for the cloud is updated here instead.

    THE SOURCE LIST IS NOT HARDCODED. It is read from updater/registry.yaml
    (run_location: local), so routing a new database is a registry edit and this picks it up.
    Assess new databases with tools/audit_cloud_capacity.py.

    Usage (this workstation has Windows PowerShell 5.1, not pwsh 7):
        powershell -File tools\run_local_heavy.ps1
        powershell -File tools\run_local_heavy.ps1 -Only abs
        powershell -File tools\run_local_heavy.ps1 -WhatIf
#>
param(
    [string[]] $Only,
    [switch]   $WhatIf,
    [switch]   $SkipCiCheck
)

$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
Set-Location $repo

$stamp  = (Get-Date).ToUniversalTime().ToString('yyyyMMdd-HHmmss')
$logDir = Join-Path $repo 'logs'
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }
$log = Join-Path $logDir "local_heavy_$stamp.log"

function Say($msg) {
    $line = '[' + (Get-Date).ToUniversalTime().ToString('HH:mm:ss') + '] ' + $msg
    Write-Output $line
    Add-Content -Path $log -Value $line -Encoding utf8
}

Say "local heavy updater starting; log -> $log"

# --- which sources are routed here? read the registry, never a second copy of the list ---
$lister = Join-Path $repo 'tools\_list_local_sources.py'
$routed = (& python $lister | Out-String).Trim()
if (-not $routed) {
    Say "no sources carry run_location: local - nothing to do"
    exit 0
}
$all = @($routed -split ',' | Where-Object { $_ })
Say ("registry routes " + $all.Count + " source(s) to this machine: " + ($all -join ', '))

$targets = $all
if ($Only) {
    $bad = @($Only | Where-Object { $all -notcontains $_ })
    if ($bad.Count -gt 0) {
        Say ("REFUSING: not routed local -> " + ($bad -join ', '))
        exit 1
    }
    $targets = @($Only)
}
Say ("this run will process " + $targets.Count + " source(s): " + ($targets -join ', '))

if ($WhatIf) {
    Say "-WhatIf given: stopping before any work"
    exit 0
}

# --- DO NOT RACE CI. Both writers compare-and-swap on the state ETag, so an overlap makes
# --- one of them lose its entire run (push_state exits 2, "another writer won"). Ledger R5.
if (-not $SkipCiCheck) {
    $inflight = -1
    try {
        $runs = gh run list --workflow=updater-daily.yml --limit 5 --json status | ConvertFrom-Json
        $inflight = @($runs | Where-Object { $_.status -ne 'completed' }).Count
    } catch {
        Say "WARNING: could not query CI. Re-run with -SkipCiCheck if you know it is idle."
        exit 2
    }
    if ($inflight -gt 0) {
        Say ("ABORT: " + $inflight + " updater-daily run(s) still in flight.")
        Say "       Both writers compare-and-swap on the state ETag, so overlapping means one"
        Say "       run's state is thrown away. Wait for CI, or pass -SkipCiCheck."
        exit 2
    }
    Say "CI idle - safe to proceed"
}

$env:AQUEDUCT_BACKEND = 'r2'

# This machine is not a 16 GB shared runner. Every per-source BUDGET_MIN in the fetcher
# package was sized so one source could not eat the 240-minute CI job; here we are processing
# ONLY the routed sources on 382 GB, so those caps just defer work - abs deferred 805 of its
# 1,222 flows in CI purely because of its 35-minute budget. Raise both, loudly.
if (-not $env:AQUEDUCT_BUDGET_MIN_OVERRIDE) { $env:AQUEDUCT_BUDGET_MIN_OVERRIDE = '360' }
if (-not $env:AQUEDUCT_RUN_BUDGET_MIN)      { $env:AQUEDUCT_RUN_BUDGET_MIN      = '2880' }
Say ("per-source budget override: " + $env:AQUEDUCT_BUDGET_MIN_OVERRIDE +
     " min; whole-run budget: " + $env:AQUEDUCT_RUN_BUDGET_MIN + " min")

Say "pull-state ..."
& python -m updater.run --pull-state
if ($LASTEXITCODE -ne 0) {
    Say ("pull-state FAILED (" + $LASTEXITCODE + ") - aborting before any write")
    exit 1
}

# NOT $args - that is a reserved automatic variable and assigning to it breaks arg passing.
$srcArgs = @()
foreach ($t in $targets) { $srcArgs += '--source'; $srcArgs += $t }
Say ("running updater for " + $targets.Count + " source(s) ...")
& python -m updater.run @srcArgs
$rc = $LASTEXITCODE
Say ("updater exit code: " + $rc)

# Push state even on a non-zero exit: the updater is built to fail one source while having
# honestly refreshed the others, and discarding that is the opposite of the honest-status
# contract. push_state's compare-and-swap is what makes this safe - it cannot overwrite a
# newer remote state, it refuses with exit 2.
Say "push-state ..."
& python -m updater.run --push-state
if ($LASTEXITCODE -ne 0) {
    Say ("push-state FAILED (" + $LASTEXITCODE + ") - state NOT committed")
}

Say ("done (updater rc=" + $rc + "). Full log: " + $log)
exit $rc
