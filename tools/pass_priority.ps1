# CPU priority for the desktop pass (tools/run_local_heavy.ps1). Dot-sourced by the runner and by its tests.
#
# ASCII ONLY (same reason as run_local_heavy.ps1: PowerShell 5.1 reads a BOM-less .ps1 in the system ANSI
# codepage and a single non-ASCII character can corrupt the token stream mid-file).
#
# WHY (2026-11-04). The desktop pass shares its host with other interactive or batch work. Its heavy children
# run below normal priority so that they yield the CPU to that work instead of competing with it for every
# core.
#
# HOW: BY INHERITANCE, FROM THE MOMENT A CHILD IS CREATED. Windows gives a new process the creator's priority
# class when the creator is BelowNormal or Idle and no class flag is passed to CreateProcess (Start-Process,
# the & call operator and Python's subprocess pass none). So the runner lowers ITS OWN class just around the
# line that starts a child, then puts its own class back. The child, and everything the child starts, is
# below normal from its first instruction; there is no window in which a grandchild can start at Normal, as
# there would be if the class were set on the child after Start-Process returned.
# tests/test_pass_priority.py proves it on Windows PowerShell 5.1 by running the real runner against fakes.
#
# WHAT STAYS AT THE RUNNER'S OWN CLASS, ON PURPOSE: the hard-stop watchdog loop (the runner itself), the CI
# writer gate, the progress snapshot (cheap, and it runs after the budget is computed), the killed-unit
# recorder and push-state. Those form the tail between the hard stop and the next
# cloud state writer's block time, and that tail is a SAFETY budget (R5): its 19-min push allowance was
# measured at normal priority. A starved push or a late kill would put two writers on one state store.

function Get-PassPriorityClass {
    <#
      .SYNOPSIS
      The [System.Diagnostics.ProcessPriorityClass] for a -Priority value. Throws on anything else.
    #>
    param([Parameter(Mandatory = $true)][string] $Name)
    switch ($Name) {
        'Normal'      { return [System.Diagnostics.ProcessPriorityClass]::Normal }
        'BelowNormal' { return [System.Diagnostics.ProcessPriorityClass]::BelowNormal }
        'Idle'        { return [System.Diagnostics.ProcessPriorityClass]::Idle }
    }
    throw ("unknown -Priority '" + $Name + "' (expected Normal, BelowNormal or Idle)")
}

function Enter-PassPriority {
    <#
      .SYNOPSIS
      Set THIS process to the -Priority class and return the class it had, for Exit-PassPriority.

      Returns $null when the class could not be set. FAILS OPEN: priority is a courtesy to other work on the
      host, and a pass that refused to run over it would leave the local route silently not updating.
    #>
    param([Parameter(Mandatory = $true)][string] $Name)
    try {
        $want = Get-PassPriorityClass $Name
        $me = [System.Diagnostics.Process]::GetCurrentProcess()
        $prev = $me.PriorityClass
        $me.PriorityClass = $want
        return $prev
    } catch {
        return $null
    }
}

function Exit-PassPriority {
    <#
      .SYNOPSIS
      Put THIS process back to the class Enter-PassPriority returned. $null (Enter failed) does nothing.

      A FAILED RESTORE IS SAID, NOT SWALLOWED: it leaves the runner low for the R5 tail, so it goes to the
      pass log (through the runner's Say when it is defined, otherwise as a warning). It never throws: the
      runner reads its class back before the tail and restores it there (run_local_heavy.ps1).
    #>
    param([AllowNull()][object] $Previous)
    if ($null -eq $Previous) { return }
    try {
        [System.Diagnostics.Process]::GetCurrentProcess().PriorityClass = $Previous
    } catch {
        $msg = ("WARNING: CPU priority restore to '" + $Previous + "' FAILED (" + $_.Exception.Message +
                "); this process stays at " + [System.Diagnostics.Process]::GetCurrentProcess().PriorityClass)
        if (Get-Command Say -CommandType Function -ErrorAction SilentlyContinue) { Say $msg }
        else { Write-Warning $msg }
    }
}
