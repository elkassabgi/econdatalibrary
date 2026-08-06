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
    [switch]   $SkipCiCheck,
    # -IfDue makes this safe to call on a tight loop (the 5-minute reboot guard calls it that
    # way). It exits 0 QUIETLY unless a run is actually due, so the guard needs no cadence
    # logic of its own and there is exactly one place that decides when this runs.
    [switch]   $IfDue,
    # -Force passes --force to the updater so a MANUAL PROOF of a not-due source actually
    # exercises the fetcher (2026-08-06: `-Only census` on a monthly cadence printed
    # "NOT DUE ... 0 unit(s) processed" and exited green having proven nothing — the exact
    # R35/R50 false-green this repo already documents for the CLOUD dispatch path, which
    # is why updater-daily.yml grew its own force input). Only meaningful with -Only.
    [switch]   $Force,
    [int]      $MinHours = 20
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

# --- CADENCE + MUTEX (only consulted for -IfDue) --------------------------------------
# WHY THIS EXISTS AT ALL. run_location: local is now ENFORCED (updater/orchestrate.py), so
# these sources are excluded from the cloud run. That fixed the runner-killing, and created a
# worse failure in its place: excluded from the cloud and invoked by nothing here, they would
# simply never update, silently. Scheduled Tasks are blocked by policy on this machine, so the
# only durable, reboot-surviving mechanism is the Startup guard loop - which is built to keep
# long-running jobs ALIVE, not to run a job on a cadence. -IfDue bridges that.
$stampFile = Join-Path $logDir 'local_heavy.last_success'

$lockFile = Join-Path $logDir 'local_heavy.lock'

if ($IfDue) {
    # Already running? The guard fires every 5 minutes and a real pass takes hours.
    #
    # A PID LOCKFILE, NOT A COMMAND-LINE SCAN. The first version of this searched for another
    # powershell.exe whose command line contained 'run_local_heavy', excluding $PID. That
    # matches the LAUNCHER as well as the job: invoke it from any shell whose own command
    # line names this script and the child sees a phantom sibling, so it stands down every
    # single time. Measured: the gate exited 0 in all three test cases, including the two that
    # were genuinely due. Ledger R49 is the same rule - a process query matches your own shell.
    # A lock keyed on a PID we WROTE cannot be confused by how the job was invoked.
    if (Test-Path $lockFile) {
        $stale = $true
        try {
            $lockParts = (Get-Content $lockFile -First 1) -split ','
            $lockPid   = [int]$lockParts[0]
            $proc = Get-Process -Id $lockPid -ErrorAction SilentlyContinue
            # PID ALONE IS NOT AN IDENTITY. Windows reuses process ids, so a lock left behind by
            # a run that aborted could later name a live, unrelated powershell.exe - and this
            # job would then stand down for ever, silently, which is exactly the failure this
            # whole cadence exists to prevent. Match the recorded start time as well.
            if ($proc -and $proc.ProcessName -eq 'powershell' -and $lockParts.Count -ge 2 -and
                $proc.StartTime.ToUniversalTime().Ticks.ToString() -eq $lockParts[1]) {
                $stale = $false
            }
        } catch { }
        if (-not $stale) { exit 0 }
        # A crashed or rebooted run leaves the lock behind; a stale lock must never wedge this
        # permanently, so fall through and take it over.
    }

    # Ran recently enough? Stamp is written ONLY after a pass that actually executed, so an
    # abort on a busy CI does not push the next attempt 20 hours out.
    if (Test-Path $stampFile) {
        try {
            $last = [DateTime]::Parse((Get-Content $stampFile -First 1),
                                      [Globalization.CultureInfo]::InvariantCulture,
                                      [Globalization.DateTimeStyles]::RoundtripKind)
            if (((Get-Date).ToUniversalTime() - $last.ToUniversalTime()).TotalHours -lt $MinHours) {
                exit 0
            }
        } catch { }   # unreadable stamp: treat as due rather than never running again
    }
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
# CI sets this; without it a multi-hour local run shows almost nothing until a buffer fills,
# and "silent" is indistinguishable from "hung" on a job that legitimately takes hours.
$env:PYTHONUNBUFFERED = '1'

# This machine is not a 16 GB shared runner. Every per-source BUDGET_MIN in the fetcher
# package was sized so one source could not eat the 240-minute CI job; here we are processing
# ONLY the routed sources on 382 GB, so those caps just defer work - abs deferred 805 of its
# 1,222 flows in CI purely because of its 35-minute budget. Raise both, loudly.
if (-not $env:AQUEDUCT_BUDGET_MIN_OVERRIDE) { $env:AQUEDUCT_BUDGET_MIN_OVERRIDE = '360' }
if (-not $env:AQUEDUCT_RUN_BUDGET_MIN)      { $env:AQUEDUCT_RUN_BUDGET_MIN      = '2880' }

# THE CI CHECK ABOVE IS A CHECK AT ONE INSTANT; THE BUDGET IS 48 HOURS. Those two facts
# together are the bug this block fixes. "CI idle - safe to proceed" is true when it is
# printed and says nothing about 06:00 UTC, when updater-daily starts on its cron. A local
# pass that is still running then puts TWO writers on one state store: both pull, both push,
# and the ETag compare-and-swap means the second push is rejected outright. The loser does not
# lose a little - it loses the whole run's bookkeeping (cursors, last_success, vintages),
# while its DATA sits in R2 unrecorded, so the next gate reads healthy sources as never-run.
# That is R5 exactly: never run a local updater job concurrently with CI.
#
# So the run budget is clamped to the time remaining before the cron window opens, minus a
# margin for the push-state and D1 sync that follow the updater. A pass that cannot finish
# simply stops early and resumes tomorrow - every fetcher here rotates (R190), so stopping
# early defers work rather than discarding it, and the sources left over are the stalest
# tomorrow so they go first.
$cronOpensUtc = [DateTime]::UtcNow.Date.AddHours(5).AddMinutes(40)   # 05:40Z, cron is 06:00Z
if ([DateTime]::UtcNow -ge $cronOpensUtc) { $cronOpensUtc = $cronOpensUtc.AddDays(1) }
$marginMin = 25
$untilCron = [int](($cronOpensUtc - [DateTime]::UtcNow).TotalMinutes) - $marginMin
if ($untilCron -lt 20) {
    Say ("ABORT: only " + $untilCron + " usable min before the 05:40Z CI window - " +
         "too little to be worth a state pull/push cycle. Next tick will pick this up.")
    exit 0
}
if ([int]$env:AQUEDUCT_RUN_BUDGET_MIN -gt $untilCron) {
    Say ("whole-run budget clamped " + $env:AQUEDUCT_RUN_BUDGET_MIN + " -> " + $untilCron +
         " min so this pass ENDS before the 05:40Z CI window (R5: one writer on the state store)")
    $env:AQUEDUCT_RUN_BUDGET_MIN = "$untilCron"
}
Say ("per-source budget override: " + $env:AQUEDUCT_BUDGET_MIN_OVERRIDE +
     " min; whole-run budget: " + $env:AQUEDUCT_RUN_BUDGET_MIN + " min")

# Take the lock only now. Everything above can exit early (not due, no routed sources, CI in
# flight) and those paths must leave no lock behind - the first version held one from the very
# start, so every CI-collision abort dropped a stale lock for the next tick to clean up.
if (-not $lockFile) { $lockFile = Join-Path $logDir 'local_heavy.lock' }
Set-Content -Path $lockFile -Value (
    $PID.ToString() + ',' + (Get-Process -Id $PID).StartTime.ToUniversalTime().Ticks.ToString()
) -Encoding ascii

Say "pull-state ..."
& python -m updater.run --pull-state
if ($LASTEXITCODE -ne 0) {
    Say ("pull-state FAILED (" + $LASTEXITCODE + ") - aborting before any write")
    Remove-Item $lockFile -ErrorAction SilentlyContinue
    exit 1
}

# NOT $args - that is a reserved automatic variable and assigning to it breaks arg passing.
$srcArgs = @()
foreach ($t in $targets) { $srcArgs += '--source'; $srcArgs += $t }
if ($Force) { $srcArgs += '--force'; Say 'FORCE: cadence gate overridden (manual proof)' }
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
$pushRc = $LASTEXITCODE
if ($pushRc -ne 0) {
    Say ("push-state FAILED (" + $pushRc + ") - state NOT committed")
}

# Stamp the cadence clock ONLY IF THE RUN'S WORK WAS RECORDED. "Genuinely ran" is not the
# bar - "genuinely committed" is. The 2026-08-01 pass crashed inside ons_uk with
# 0xC0000005 after 8h56m and its push-state then lost the compare-and-swap, yet this line
# stamped success anyway and the guard stood down for 20 hours over a run whose entire
# record had been lost. That is stamping success on a failure, the exact dishonesty the
# updater's own status contract forbids everywhere else.
#
# A non-zero updater rc alone is NOT disqualifying: the design is to fail one source while
# honestly refreshing the others, and those runs deserve their cadence. A failed push IS
# disqualifying, because nothing durable came of the pass - the next tick must redo it.
# Round-trip format ('o') so it parses back as UTC regardless of locale; a bare local-time
# string re-parsed as UTC is a 5-hour error and hid a healthy run once already (R198).
$crashed = ($rc -lt 0) -or ($rc -eq 134) -or ($rc -eq 137) -or ($rc -eq 139)
if ($pushRc -eq 0 -and -not $crashed) {
    Set-Content -Path $stampFile -Value ((Get-Date).ToUniversalTime().ToString('o')) -Encoding ascii
    Say "cadence stamped - state committed"
} else {
    $why = if ($crashed) { "updater CRASHED (rc=" + $rc + ")" } else { "push-state failed (" + $pushRc + ")" }
    Say ("cadence NOT stamped: " + $why + " - this pass is due again on the next guard tick")
}
Remove-Item $lockFile -ErrorAction SilentlyContinue

Say ("done (updater rc=" + $rc + "). Full log: " + $log)
exit $rc
