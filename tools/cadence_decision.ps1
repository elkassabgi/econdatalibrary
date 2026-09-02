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
    if ([int]$Advanced -le 0) {
        return [pscustomobject]@{ Stamp = $false; Why = "NOT ONE unit advanced last_success_utc, upstream_vintage or last_obs_date - nothing was committed" }
    }
    return [pscustomobject]@{ Stamp = $true; Why = "$Advanced unit(s) advanced" }
}
