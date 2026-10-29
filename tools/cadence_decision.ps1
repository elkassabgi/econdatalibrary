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
      advanced nothing. That is the predicate the replay modelled (tools/measure_ci_lag.py,
      Replay._pass: the stamp is set at push end of a pass that advanced nothing) and the one
      AR-393 round 10 made binding. Every other ending is NOT an empty pass and writes nothing:
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
        return [pscustomobject]@{ Stamp = $false; Why = "NOT ONE unit advanced last_success_utc, upstream_vintage or last_obs_date - nothing was committed" }
    }
    return [pscustomobject]@{ Stamp = $true; Why = "$Advanced unit(s) advanced" }
}
