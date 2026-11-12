# The cadence decision, as a pure function so it can be tested without running a pass.
#
# ASCII ONLY (same reason as run_local_heavy.ps1: PowerShell 5.1 reads a BOM-less .ps1 in the
# system ANSI codepage and a single non-ASCII character can corrupt the token stream mid-file).
#
# WHY THIS IS A SEPARATE FILE. Three rounds of this guard shipped broken, and two of the three
# defects were PowerShell language facts that reading the script cannot catch - `[int]$null` is
# 0 and does not throw, so a sentinel only the callee can print never arrives when the callee
# never runs. A test has to execute real PowerShell. Dot-sourcing run_local_heavy.ps1 would run
# a pass, so the decision lives here and both the runner and the test dot-source this.

function Read-ProbeNumber {
    <#
      .SYNOPSIS
      Parse one line of a probe's stdout into an integer, or $null for "unknown".

      EMPTY STDOUT IS NOT ZERO. `[int]$null` is 0 in PowerShell 5.1 and does NOT throw, so a
      missing script or an interpreter that fails to start - both of which print nothing - used
      to yield a confident 0 and let the caller treat unknown as permission (R630). The PATTERN
      is the guard: it rejects an empty string, a warning line and a sentinel alike.
    #>
    param([AllowNull()][object] $Raw, [switch] $AllowNegative)
    if ($null -eq $Raw) { return $null }
    $t = "$Raw".Trim()
    if ($t -eq '') { return $null }
    $pattern = if ($AllowNegative) { '^-?\d+$' } else { '^\d+$' }
    if ($t -notmatch $pattern) { return $null }
    return [int]$t
}

function Test-PassAdvancedNothing {
    <#
      .SYNOPSIS
      Did this pass advance NOTHING? The one predicate behind the "NOT ONE unit advanced" verdict.

      $true only for a KNOWN count of zero or less. $null (the probe could not be read) is
      UNKNOWN, not "nothing": it is refused a cadence stamp for that reason, and it must not be
      read as evidence of an empty pass either. Test-CadenceShouldStamp and the no-progress
      back-off (D3) both call this, so the two can never disagree about what "advanced nothing"
      means.
    #>
    param([AllowNull()][object] $Advanced)
    return ($null -ne $Advanced -and [int]$Advanced -le 0)
}

function Test-NoProgressShouldStamp {
    <#
      .SYNOPSIS
      Should this pass write local_heavy.last_noprogress (D3, the no-progress back-off)?

      ONLY a scheduled whole-route pass that PULLED the state, PUSHED it successfully and
      advanced nothing - the rule AR-393 round 10 made binding. It is NOT exactly the replay's:
      tools/measure_ci_lag.py Replay._pass stamps at push end of EVERY pass that advanced nothing,
      a hard-stopped one included. Excluding rc 124 here is a DELIBERATE difference, and AR-398
      measured it to change nothing in the replay (a patched replay over 32 today rows, with and
      without the hard-stop stamp, gave identical empty passes, passes, used minutes, owed end and
      per-unit attempts). The replay models no push failure, crash or manual pass at all.
      Every ending below writes nothing:
        * push-state failed     - the pass's outcome is not in the shared state at all;
        * the updater crashed   - a signal kill, not an orderly empty pass;
        * hard stop (rc 124)    - a unit was killed mid-run; it was busy, not idle;
        * a manual proof pass   - -Only / -Force covers what the operator chose, not the route
                                  (the 10-27 census proof advanced nothing and would otherwise
                                  hold every short window for 8 h);
        * no state pull         - after T0 there is no cloud writer and no hold to back off;
        * advanced unknown      - an unreadable probe is not evidence of anything.
    #>
    param(
        [int]  $PushRc,
        [bool] $Crashed,
        [bool] $HardStopped,
        [bool] $Manual,
        [bool] $PulledState,
        [AllowNull()][object] $Advanced
    )
    if (-not $PulledState) { return [pscustomobject]@{ Stamp = $false; Why = "no state pull this pass (after T0)" } }
    if ($Manual)           { return [pscustomobject]@{ Stamp = $false; Why = "a manual -Only/-Force pass" } }
    if ($PushRc -ne 0)     { return [pscustomobject]@{ Stamp = $false; Why = "push-state failed ($PushRc)" } }
    if ($Crashed)          { return [pscustomobject]@{ Stamp = $false; Why = "the updater crashed" } }
    if ($HardStopped)      { return [pscustomobject]@{ Stamp = $false; Why = "hard stop (rc 124): a unit was killed mid-run" } }
    if (-not (Test-PassAdvancedNothing $Advanced)) {
        if ($null -eq $Advanced) { return [pscustomobject]@{ Stamp = $false; Why = "the progress probe could not be read" } }
        return [pscustomobject]@{ Stamp = $false; Why = "$Advanced unit(s) advanced" }
    }
    return [pscustomobject]@{ Stamp = $true; Why = "a pushed pass that advanced nothing" }
}

function Get-StampAgeHours {
    <#
      .SYNOPSIS
      Hours since the ISO-8601 instant on the first line of $Path, or $null.

      $null means "no usable stamp": the file is missing, unreadable, unparseable, or dated in
      the FUTURE (a clock-skewed or hand-edited stamp would otherwise read as "younger than the
      back-off" for as long as it stays ahead, and hold short windows indefinitely). Every one of
      those falls back to today's behaviour, i.e. no back-off - the cost of that is an empty pass,
      never a lost one.
    #>
    param([string] $Path, [AllowNull()][object] $Now)
    if (-not $Path -or -not (Test-Path $Path)) { return $null }
    if ($null -eq $Now) { $Now = (Get-Date).ToUniversalTime() }
    try {
        $raw = Get-Content $Path -First 1 -ErrorAction Stop
        if ($null -eq $raw -or "$raw".Trim() -eq '') { return $null }
        $t = [DateTime]::Parse("$raw".Trim(), [Globalization.CultureInfo]::InvariantCulture,
                               [Globalization.DateTimeStyles]::RoundtripKind)
        $h = ($Now.ToUniversalTime() - $t.ToUniversalTime()).TotalHours
        if ($h -lt 0) { return $null }
        return [double]$h
    } catch {
        return $null
    }
}

function Test-WindowShouldHold {
    <#
      .SYNOPSIS
      Should a pass HOLD for a longer window instead of starting in this one?

      A window of $WantBlockMin usable minutes or more ALWAYS starts. A shorter one is held when
        * it is less than $MaxHours since the last pass that advanced something (the original
          150-min preference, unchanged), OR
        * D3: the last pushed pass advanced NOTHING less than $NoProgressBackoffH hours ago. Past
          $MaxHours the preference above is dropped, and before D3 a pass that advanced nothing
          wrote no stamp, so every window of 20+ usable minutes started another empty pass.
      This is the replay's rule (tools/measure_ci_lag.py Replay._gate_attempt) term for term, and
      a test drives both on the same inputs. $NoProgressAgeHours = $null means no usable stamp.
    #>
    param(
        [double] $UntilCronMin,
        [double] $WantBlockMin,
        [double] $SinceLastHours,
        [double] $MaxHours,
        [AllowNull()][object] $NoProgressAgeHours,
        [double] $NoProgressBackoffH
    )
    $young = ($null -ne $NoProgressAgeHours -and [double]$NoProgressAgeHours -lt $NoProgressBackoffH)
    if ($UntilCronMin -ge $WantBlockMin) {
        return [pscustomobject]@{ Hold = $false; Young = $young; Why = "the window is at least $WantBlockMin min" }
    }
    if ($SinceLastHours -lt $MaxHours) {
        return [pscustomobject]@{ Hold = $true; Young = $young; Why = "only $([int]$SinceLastHours) h since the last pass (under $MaxHours h)" }
    }
    if ($young) {
        return [pscustomobject]@{ Hold = $true; Young = $young; Why = ("the last pushed pass advanced nothing " +
            [math]::Round([double]$NoProgressAgeHours, 1) + " h ago (back-off " + $NoProgressBackoffH + " h)") }
    }
    return [pscustomobject]@{ Hold = $false; Young = $young; Why = "past $MaxHours h and no recent empty pass" }
}

function Get-PostPullBudget {
    <#
      .SYNOPSIS
      After pull-state: the updater's run budget and its HARD STOP, both from the cloud writer's block INSTANT.

      WHY (AR-405 G2). The runner clamped AQUEDUCT_RUN_BUDGET_MIN to (untilBlock - 25) BEFORE pull-state, but
      the orchestrator starts its clock (orchestrate._RUN_DEADLINE_TS = updater start + budget) only when the
      updater launches, and the hard stop was "budget + 10 min" from then. So the pull's duration, the post-stop
      overrun and push-state all landed after the window. Measured 2026-10-31 01:57Z: untilBlock 65, pull 36.1
      min, budget 40 - a pass that used its budget would have pushed ~29 min after the block (R5).

      THE ARITHMETIC (all UTC, minutes):
        BlockAt    = UntilReadAt + UntilBlock - Granularity   minutes_until_block walks in 5-min steps and can
                                                              overstate by up to one step (ci_writer_gate.py)
        HardStop   = BlockAt - PushAllowance - KillLag        push-state must still fit after the kill
        RunDeadline= HardStop - Overrun                       the orchestrator's soft ceiling; a unit may
                                                              overrun it (merge/finalize/CSV tail) up to the stop
        Budget     = floor(RunDeadline - Now)                 Now = when the updater is about to launch
      Worst case push end = HardStop + KillLag + PushAllowance = BlockAt <= the true block instant.
      Below $MinBudgetMin the updater is not started (Run = false).
    #>
    param(
        [DateTime] $UntilReadAtUtc,
        [double]   $UntilBlockMin,
        [DateTime] $NowUtc,
        [double]   $CurrentBudgetMin,
        [double]   $GranularityMin = 5,
        [double]   $PushAllowanceMin = 19,
        [double]   $KillLagMin = 1,
        [double]   $OverrunMin = 10,
        [double]   $MinBudgetMin = 20
    )
    $blockAt  = $UntilReadAtUtc.AddMinutes($UntilBlockMin - $GranularityMin)
    $hardStop = $blockAt.AddMinutes(-($PushAllowanceMin + $KillLagMin))
    $runEnd   = $hardStop.AddMinutes(-$OverrunMin)
    $remaining = [int][math]::Floor(($runEnd - $NowUtc).TotalMinutes)
    $budget = [int][math]::Min([math]::Floor($CurrentBudgetMin), $remaining)
    # A budget a caller set LOWER keeps its own, earlier, hard stop (budget + overrun from now): never later
    # than the block-derived one.
    $own = $NowUtc.AddMinutes($budget + $OverrunMin)
    if ($budget -lt $remaining -and $own -lt $hardStop) { $hardStop = $own }
    $elapsed = ($NowUtc - $UntilReadAtUtc).TotalMinutes
    $tail = ("untilBlock " + $UntilBlockMin + " - granularity " + $GranularityMin + " - push allowance " +
             $PushAllowanceMin + " - kill lag " + $KillLagMin + " - overrun " + $OverrunMin + " - " +
             [math]::Round($elapsed, 1) + " min elapsed since the gate was read")
    $o = [pscustomobject]@{ Run = ($budget -ge $MinBudgetMin); BudgetMin = $budget; RemainingMin = $remaining
                            HardStopUtc = $hardStop; BlockAtUtc = $blockAt; Why = '' }
    if ($o.Run) { $o.Why = ("" + $budget + " min (" + $tail + "); hard stop " + $hardStop.ToString('HH:mm:ss') + "Z") }
    else { $o.Why = ("only " + $budget + " min (" + $tail + "), minimum " + $MinBudgetMin) }
    return $o
}

function Test-CadenceShouldStamp {
    <#
      .SYNOPSIS
      Should this pass stamp the 20-hour cadence clock?

      Three ways to answer no, and one of them is "I do not know":
        * push-state failed        - nothing durable came of the pass;
        * the updater crashed      - a signal kill, not an orderly stop;
        * nothing advanced         - no unit moved last_success_utc, upstream_vintage or
                                     last_obs_date, so the next tick has nothing to wait for;
        * the probe is unreadable  - unknown is not permission, WHATEVER the exit code. An
                                     earlier version applied that rule only under a hard stop,
                                     which was narrower than its own comment (R634).
    #>
    param(
        [int] $PushRc,
        [bool] $Crashed,
        [AllowNull()][object] $Advanced      # $null = unknown
    )
    if ($PushRc -ne 0) { return [pscustomobject]@{ Stamp = $false; Why = "push-state failed ($PushRc)" } }
    if ($Crashed)      { return [pscustomobject]@{ Stamp = $false; Why = "the updater CRASHED" } }
    if ($null -eq $Advanced) {
        return [pscustomobject]@{ Stamp = $false; Why = "the progress probe could not be read - refusing to assume anything was advanced" }
    }
    if (Test-PassAdvancedNothing $Advanced) {
        # NOT "nothing was committed": the state was SAVED to reach this line (a failed push returns above
        # with its own reason), so the pass's attempt rows, run rows and any killed-unit row WERE committed.
        # What did not happen is progress: no unit moved one of the three fields the probe reads.
        return [pscustomobject]@{ Stamp = $false; Why = "state saved, but NOT ONE unit advanced last_success_utc, upstream_vintage or last_obs_date - no progress field moved (a push failure reads 'push-state failed')" }
    }
    return [pscustomobject]@{ Stamp = $true; Why = "$Advanced unit(s) advanced" }
}

# --- THE NIGHTLY BACKUP WINDOW (owner's decision, 2026-11-12) --------------------------------------------------
# The workstation's nightly backup starts at 02:00 America/Chicago and reads the disks heavily. A desktop pass
# that comes due inside this window does not START until it ends; a pass already running is never stopped (the
# runner asks this once, under -IfDue, after the live-lock check and before it takes the lock). The window is on
# the Chicago WALL CLOCK, whatever time zone this machine is set to, so it follows DST: 08:00-12:00Z in winter
# (CST), 07:00-11:00Z in summer (CDT). On the spring-forward night 02:00-02:59 local does not exist and the
# window is 08:00-11:00Z; on the fall-back night it is 08:00-12:00Z (02:00 CST is after the repeated hour).
# Start inclusive, end exclusive: due at 01:59 starts, 02:00 and 05:59 hold, 06:00 starts.
$script:BackupWindowStartHour = 2
$script:BackupWindowEndHour   = 6
# IANA first (pwsh 7 on Linux/CI, .NET 6+), then the Windows id for the same zone (Windows PowerShell 5.1 on
# .NET Framework knows only Windows ids). Both are America/Chicago with its DST rules.
$script:BackupWindowTimeZoneIds = @('America/Chicago', 'Central Standard Time')

function Get-BackupWindowTimeZone {
    foreach ($id in $script:BackupWindowTimeZoneIds) {
        try { return [TimeZoneInfo]::FindSystemTimeZoneById($id) } catch { }
    }
    return $null
}

function Get-BackupWindowHold {
    <#
      .SYNOPSIS
      Is $NowUtc inside the nightly backup window? A pure function of the instant: no file, no clock read.

      Returns Hold, LocalNow (Chicago wall clock), ResumeUtc (06:00 Chicago of that night, when held),
      WindowKey (the Chicago date of the window, for the once-per-window log line) and Why.

      THE TIME ZONE CANNOT BE FOUND -> Hold = $false, with a Why that says so. Fail OPEN, deliberately: holding
      on an unknown clock would hold every tick for ever, the silent starvation R1032 is about, while a pass that
      overlaps the backup only runs slower.
    #>
    param(
        [Parameter(Mandatory = $true)][DateTime] $NowUtc,
        [int] $StartHour = $script:BackupWindowStartHour,
        [int] $EndHour   = $script:BackupWindowEndHour
    )
    if ($NowUtc.Kind -eq [DateTimeKind]::Local) { $NowUtc = $NowUtc.ToUniversalTime() }
    elseif ($NowUtc.Kind -eq [DateTimeKind]::Unspecified) { $NowUtc = [DateTime]::SpecifyKind($NowUtc, [DateTimeKind]::Utc) }
    $o = [pscustomobject]@{ Hold = $false; LocalNow = $null; ResumeUtc = $null; WindowKey = ''; Why = '' }
    $tz = Get-BackupWindowTimeZone
    if ($null -eq $tz) {
        $o.Why = ("time zone not found (tried " + ($script:BackupWindowTimeZoneIds -join ', ') +
                  "); the backup window is NOT applied")
        return $o
    }
    $local = [TimeZoneInfo]::ConvertTimeFromUtc($NowUtc, $tz)
    $o.LocalNow = $local
    $tod = $local.TimeOfDay
    if ($tod -ge [TimeSpan]::FromHours($StartHour) -and $tod -lt [TimeSpan]::FromHours($EndHour)) {
        # The end hour is never inside a DST gap or overlap on this zone (both are at 01:00-03:00 local).
        $endLocal = [DateTime]::SpecifyKind($local.Date.AddHours($EndHour), [DateTimeKind]::Unspecified)
        $o.Hold = $true
        $o.ResumeUtc = [TimeZoneInfo]::ConvertTimeToUtc($endLocal, $tz)
        $o.WindowKey = $local.ToString('yyyy-MM-dd', [Globalization.CultureInfo]::InvariantCulture)
        $o.Why = ("{0:D2}:00-{1:D2}:00 {2} is the nightly backup window; it is {3} there" -f $StartHour, $EndHour,
                  $tz.Id, $local.ToString('HH:mm', [Globalization.CultureInfo]::InvariantCulture))
    }
    return $o
}
