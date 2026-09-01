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
    # "NOT DUE ... 0 unit(s) processed" and exited green having proven nothing - the exact
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
        if (-not $stale) {
            # A LIVE LOCK IS NOT AUTOMATICALLY A HEALTHY ONE. A pass clamps itself to the
            # next CI window (<= ~274 min) and the per-source cap is 360 min, so NO
            # legitimate run can hold this lock for many hours. On 2026-08-23 a manual
            # `-Only <giant> -Force` run livelocked and held it for NINETEEN HOURS: the guard
            # stood down every 5 minutes, silently, and the whole local route - statcan,
            # census, oecd, bea, eia - did not run at all. Nothing anywhere reported it,
            # because updater/health.py's route_silence needs THREE DAYS and its own
            # docstring says a short outage on a ~20h cadence is invisible by construction.
            # So say it here, loudly, where the lock actually is (ledger R446).
            $lockAgeH = ((Get-Date) - (Get-Item $lockFile).LastWriteTime).TotalHours
            if ($lockAgeH -gt 8) {
                Say ('WEDGED: local_heavy lock held by pid ' + $lockPid + ' for ' +
                     [math]::Round($lockAgeH,1) + ' h - longer than any legitimate pass ' +
                     '(run budget <= 274 min, per-source cap 360 min). The local route is ' +
                     'NOT updating. Investigate that pid, then delete ' + $lockFile)
            }
            exit 0
        }
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
# PINNED INTERPRETER, same value and reason as RELAUNCH_GUARD.ps1: bare "python"
# resolves through PATH and can land on a 3.11 with no pyyaml (measured 2026-08-23),
# or on the WindowsApps store shim.
$pythonExe = "C:\Users\aelkassabgi\AppData\Local\Programs\Python\Python314\python.exe"
if (-not (Test-Path $pythonExe)) { $pythonExe = 'python' }
$routed = (& $pythonExe $lister 2>&1 | Out-String).Trim()
$listerRc = $LASTEXITCODE
# A CRASHING LISTER MUST NOT LOOK LIKE AN EMPTY REGISTRY. Before this gate an
# ImportError printed a traceback, produced no stdout, and fell straight into the
# branch below - so the guard announced "nothing to do" and exited 0 while all 21
# local-routed sources silently stopped updating, reporting success for ever.
# Ledger R261's class: a listing that returns [] instead of failing.
if ($listerRc -ne 0) {
    Say "FATAL: _list_local_sources.py exited $listerRc - refusing to treat that as an empty registry"
    Say ('  output: ' + ($routed -replace '\s+', ' '))
    exit 1
}
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
# FOUR CI state writers, not two. Every one of them runs --pull-state ... --push-state, so a
# local pass must not let its own pull->push interval overlap ANY of them:
#     updater-heavy.yml  cron 03:00Z and 15:00Z
#     updater-daily.yml  cron 06:00Z and 18:00Z
# The previous model listed only the two DAILY crons and stored them ALREADY GUARD-ADJUSTED
# (05:40 standing for a 06:00 cron). Two defects followed from that. It was blind to the heavy
# workflow entirely, and because the stored value was the guard rather than the cron, crossing
# 17:40 made it conclude the window had PASSED while the 18:00Z run had not yet started: at
# 17:41:37 on 2026-08-23 it announced "CI idle - safe to proceed" and launched an 11.5 h pass
# against a run that fired at 18:17. Two writers on one state store, one of them guaranteed to
# lose its entire bookkeeping to the compare-and-swap - R5, the very thing this clamp exists to
# prevent. That pass was also budgeted to 05:15Z, straight through the 03:00Z heavy window, so
# no scenario existed in which it could have pushed successfully.
#
# A window is an INTERVAL, not an instant. Ends MEASURED from the last six runs of each
# workflow (gh run list, 2026-08-23) and expressed as minutes AFTER THE CRON so the model
# absorbs GitHub's start lag, which is routinely 13-55 min:
#     heavy  03:55->05:05, 15:13->16:30, 03:47->04:51, 15:12->16:11   worst 125 min after cron
#     daily  06:21->10:07, 18:17->21:09, 06:19->10:10, 06:25->09:55   worst 250 min after cron
# Rounded up to 150 and 270 for headroom. This leaves two usable slots a day, ~10:30-14:40Z and
# ~22:30-02:40Z, each ~250 min. That is genuinely less runway than the old model believed it
# had, and it is the honest number: the giants resume part-by-part (R190), so a pass that stops
# on budget defers work rather than discarding it.
$LEAD_MIN = 20      # never START this close to a cron - CI pulls state almost immediately
$blackouts = @()
foreach ($d in 0, 1) {
    $base = [DateTime]::UtcNow.Date.AddDays($d)
    $blackouts += , @($base.AddHours(3).AddMinutes(-$LEAD_MIN),  $base.AddHours(3).AddMinutes(150),  '03:00Z heavy')
    $blackouts += , @($base.AddHours(6).AddMinutes(-$LEAD_MIN),  $base.AddHours(6).AddMinutes(270),  '06:00Z daily')
    $blackouts += , @($base.AddHours(15).AddMinutes(-$LEAD_MIN), $base.AddHours(15).AddMinutes(150), '15:00Z heavy')
    $blackouts += , @($base.AddHours(18).AddMinutes(-$LEAD_MIN), $base.AddHours(18).AddMinutes(270), '18:00Z daily')
}
$nowUtc = [DateTime]::UtcNow
$inside = @($blackouts | Where-Object { $nowUtc -ge $_[0] -and $nowUtc -lt $_[1] })[0]
if ($inside) {
    Say ("ABORT: inside the " + $inside[2] + " CI window, which holds the state store until ~" +
         $inside[1].ToString("HH:mm") + "Z. A pass started now would lose its whole run's " +
         "bookkeeping to the compare-and-swap (R5). Next tick will pick this up.")
    exit 0
}
$next = @($blackouts | Where-Object { $_[0] -gt $nowUtc } | Sort-Object { $_[0] })[0]
$cronOpensUtc = $next[0]
$cronLabel    = $next[2]
$marginMin = 25
$untilCron = [int](($cronOpensUtc - $nowUtc).TotalMinutes) - $marginMin
if ($untilCron -lt 20) {
    Say ("ABORT: only " + $untilCron + " usable min before the " + $cronLabel + " CI window - " +
         "too little to be worth a state pull/push cycle. Next tick will pick this up.")
    exit 0
}
if ([int]$env:AQUEDUCT_RUN_BUDGET_MIN -gt $untilCron) {
    Say ("whole-run budget clamped " + $env:AQUEDUCT_RUN_BUDGET_MIN + " -> " + $untilCron +
         " min so this pass ENDS before the " + $cronLabel + " CI window (R5: one writer on the state store)")
    $env:AQUEDUCT_RUN_BUDGET_MIN = "$untilCron"
}
Say ("per-source budget override: " + $env:AQUEDUCT_BUDGET_MIN_OVERRIDE +
     " min; whole-run budget: " + $env:AQUEDUCT_RUN_BUDGET_MIN + " min")

# Take the lock only now. Everything above can exit early (not due, no routed sources, CI in
# flight) and those paths must leave no lock behind - the first version held one from the very
# start, so every CI-collision abort dropped a stale lock for the next tick to clean up.
if (-not $lockFile) { $lockFile = Join-Path $logDir 'local_heavy.lock' }

# NEVER STEAL A LIVE LOCK, AND NEVER RELEASE SOMEONE ELSE'S.
#
# The lock was written here unconditionally and removed unconditionally on both exit paths,
# so it protected only the -IfDue caller. Observed 2026-09-01: a pass launched WITHOUT -IfDue
# skipped the gate above, reached this line and OVERWROTE the live lock held by the legitimate
# 10:30 pass, stamping its own pid; when that intruder then aborted, the unconditional
# `Remove-Item` deleted the lock outright. The legitimate pass was still running with no lock
# to show for it, so every subsequent 5-minute guard tick started ANOTHER full pass  -  each one
# pulling the 11.28 GB state store, and each intending to push it back (R5: one writer).
#
# The gate at the top is the CADENCE decision and stays -IfDue-only. Ownership is not a
# cadence question: it applies to every invocation, however the job was started.
$lockHeldByOther = $false
if (Test-Path $lockFile) {
    try {
        $lp = (Get-Content $lockFile -First 1) -split ','
        $op = Get-Process -Id ([int]$lp[0]) -ErrorAction SilentlyContinue
        if ($op -and $op.ProcessName -eq 'powershell' -and [int]$lp[0] -ne $PID -and
            $lp.Count -ge 2 -and
            $op.StartTime.ToUniversalTime().Ticks.ToString() -eq $lp[1]) {
            $lockHeldByOther = $true
        }
    } catch { }
}
if ($lockHeldByOther) {
    Say ('REFUSING TO START: local_heavy.lock is held by a LIVE pass (pid ' + $lp[0] +
         '). Two passes would each pull and push the state store and one run would be ' +
         'thrown away on the ETag compare-and-swap. If you meant to run anyway, stop that ' +
         'pass first.')
    exit 0
}
Set-Content -Path $lockFile -Value (
    $PID.ToString() + ',' + (Get-Process -Id $PID).StartTime.ToUniversalTime().Ticks.ToString()
) -Encoding ascii

function Release-LocalHeavyLock {
    # Only if it is still OURS. An aborting run must not free the lock of whoever owns it now.
    if (-not (Test-Path $script:lockFile)) { return }
    try {
        $p = (Get-Content $script:lockFile -First 1) -split ','
        if ([int]$p[0] -ne $PID) { return }
    } catch { return }
    Remove-Item $script:lockFile -ErrorAction SilentlyContinue
}

Say "pull-state ..."
& $pythonExe -m updater.run --pull-state
if ($LASTEXITCODE -ne 0) {
    Say ("pull-state FAILED (" + $LASTEXITCODE + ") - aborting before any write")
    Release-LocalHeavyLock
    exit 1
}

# NOT $args - that is a reserved automatic variable and assigning to it breaks arg passing.
$srcArgs = @()
foreach ($t in $targets) { $srcArgs += '--source'; $srcArgs += $t }
if ($Force) { $srcArgs += '--force'; Say 'FORCE: cadence gate overridden (manual proof)' }
Say ("running updater for " + $targets.Count + " source(s) ...")

# HARD WALL CLOCK. The budget above is a REQUEST, not a limit: the orchestrator checks it
# BETWEEN units, and its per-unit SIGALRM is a documented no-op on Windows ("POSIX only ...
# where this is a no-op by design"). So one long unit runs as long as it likes and the pass
# sails past the window the clamp was calculated to respect. Measured 2026-08-24: a pass
# clamped to 220 min was still running 45 minutes past its deadline, mid-unit, with the
# 03:00Z heavy cron about to fire - the exact collision R448 exists to prevent, defeated by
# the budget being unenforceable rather than by the arithmetic being wrong.
#
# Killing mid-unit is safe for DATA: merge_and_write publishes through write_table_atomic,
# so a half-written store is unreachable. And stopping here is strictly better than running
# on, because push-state still executes afterwards - the pass records what it actually
# finished instead of losing the whole run's bookkeeping to a compare-and-swap it was always
# going to lose.
$graceMin = 10
$hardDeadline = (Get-Date).AddMinutes([int]$env:AQUEDUCT_RUN_BUDGET_MIN + $graceMin)
# Captured for the killed-unit recorder below: the elapsed it attributes is measured from the
# moment the updater is launched, matching the log the parser reads.
$updaterStart = Get-Date
$hardStopped = $false
# CAPTURE THE UPDATER'S OUTPUT. -NoNewWindow with no redirection sends it to the parent's
# console, and this script runs unattended from the guard, so the console is nowhere: every
# line the updater printed was discarded.
#
# Measured 2026-08-30, and it is why an alarm could not be answered. The 06:00Z health gate
# reported "ROUTE 'local' SILENT - 18 live source(s) run there and NOT ONE has succeeded
# within 3d (newest success: 6.0d ago)". The corresponding local log is 1,776 bytes and holds
# NOTHING between "running updater for 29 source(s)" at 00:44:19 and "HARD STOP" at 02:29:23
# -- 105 minutes of work with no record of which sources were attempted, which finished, or
# where the budget went. The one question the gate asked was unanswerable from our own logs.
#
# `-u` because Python block-buffers stdout the moment it is redirected, so a long job writes
# nothing for hours and the log reads as a hang (R290 -- that has already cost one false
# progress report). Separate stderr, and the same Start-Process redirection pattern
# run_guarded_job.ps1 already uses, whose logs have always been live and readable: the child
# writes the file with its own handle, so there is no pipeline to buffer and no re-encoding.
$updaterLog = Join-Path $logDir ("local_heavy_updater_{0}.log" -f $stamp)
$updaterErr = Join-Path $logDir ("local_heavy_updater_{0}.err.log" -f $stamp)
Say ("updater output -> " + $updaterLog)
$proc = Start-Process -FilePath $pythonExe -ArgumentList (@('-u','-m','updater.run') + $srcArgs) `
        -PassThru -NoNewWindow `
        -RedirectStandardOutput $updaterLog -RedirectStandardError $updaterErr
# Cache the handle BEFORE any wait, or PS 5.1 leaves ExitCode $null on a clean exit and the
# `if ($null -eq $rc) { $rc = 124 }` guard below silently relabels every SUCCESSFUL run as a
# timeout. That is the same null-ExitCode defect that had RELAUNCH_GUARD resurrecting a
# finished derive_noaa 975 times (e9587cb8f).
$null = $proc.Handle
while (-not $proc.HasExited) {
    if ((Get-Date) -gt $hardDeadline) {
        Say ("HARD STOP: the updater passed its " + $env:AQUEDUCT_RUN_BUDGET_MIN +
             "-min budget by " + $graceMin + " min and is still running. Terminating so " +
             "push-state happens BEFORE the CI window (R448). Data is safe - stores are " +
             "written atomically - and every fetcher resumes.")
        & taskkill /PID $proc.Id /T /F 2>&1 | Out-Null
        Start-Sleep -Seconds 5
        $hardStopped = $true
        break
    }
    Start-Sleep -Seconds 15
}
# Set 124 EXPLICITLY on a hard stop. A process terminated by taskkill reports HasExited
# true but leaves ExitCode unpopulated, so reading it yields $null - the run would then
# print "updater exit code: " with nothing after it and hand $null to every downstream
# comparison. Caught by testing the kill path rather than only the timer.
if ($hardStopped) { $rc = 124 }
elseif ($proc.HasExited) { $proc.Refresh(); $rc = $proc.ExitCode }
else { $rc = 124 }
if ($null -eq $rc) { $rc = 124 }
Say ("updater exit code: " + $rc)

# RECORD THE UNIT THE KILL UN-RECORDED, before push-state carries the state away. A unit
# killed by the taskkill above dies without writing its `runs` row, so run_cost_estimate
# never learns its true cost: the giant keeps a stale cheap estimate, re-enters the cheap
# band, and eats the NEXT night's whole budget too. Measured 2026-08-30/31 -
# unctad_tradefoodcatbyproc consumed ~154 min of a 153-min pass, left no row, and 18 live
# local sources went a 7th day unattempted while it led the queue again. The tool attributes
# the pass's unaccounted elapsed to the in-flight unit as status=killed_external, which
# MAX(dur_s) then sees. It must run HERE - between the run and push-state - because
# pull-state replaces local state wholesale (R340), so a row written any other time is lost.
#
# Output goes through Say and a nonzero exit is announced LOUDLY: the adversarial review's
# core point was that a recorder failing silently in this path re-opens the starvation while
# everyone believes it fixed. It never aborts the pass - push-state must still run.
if ($hardStopped) {
    $elapsedS = [int]((Get-Date) - $updaterStart).TotalSeconds
    Say ("recording externally-killed unit (elapsed " + $elapsedS + "s) ...")
    $recOut = & $pythonExe (Join-Path $PSScriptRoot 'record_killed_unit.py') `
                $updaterLog $elapsedS --apply 2>&1
    foreach ($ln in @($recOut)) { Say ("  recorder: " + $ln) }
    if ($LASTEXITCODE -ne 0) {
        Say ("RECORDER FAILED (exit " + $LASTEXITCODE + ") - the killed unit keeps its " +
             "stale cheap cost estimate and may starve the fleet again; investigate " +
             "record_killed_unit.py against " + $updaterLog)
    }
}

# Push state even on a non-zero exit: the updater is built to fail one source while having
# honestly refreshed the others, and discarding that is the opposite of the honest-status
# contract. push_state's compare-and-swap is what makes this safe - it cannot overwrite a
# newer remote state, it refuses with exit 2.
Say "push-state ..."
& $pythonExe -m updater.run --push-state
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
Release-LocalHeavyLock

Say ("done (updater rc=" + $rc + "). Full log: " + $log)
exit $rc
