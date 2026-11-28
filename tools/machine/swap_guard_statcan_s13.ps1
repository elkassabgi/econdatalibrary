<#
    swap_guard_statcan_s13.ps1 - T0 step S13 (T0 item B3): install the POST-T0 guard.

    ASCII ONLY (Windows PowerShell 5.1 reads a BOM-less .ps1 as ANSI; R196).

    WHAT. Copies tools\machine\RELAUNCH_GUARD.ps1.post-t0-copy over the live RELAUNCH_GUARD.ps1, with a backup, a
    compare-and-swap on the live file's sha256 and a rollback in the same process. After it the guard launches the
    post-T0 statcan lane (jobs/statcan_selfhost_lane.py) and no longer the pre-T0 one (jobs/statcan_lane.py, which
    refuses after T0). Precedent: the 2026-11-21 Step C swap (F:\work\econ_finish\relaunch_guard_update).

    WHEN. S13 only: after the CUTOVER flag (T0), while EconGuard is STOPPED (S8a), with the pre-T0 lane stopped
    (S4), and BEFORE EconGuard restarts. The script refuses otherwise.

    ONE PASTED LINE (the coordinator fills both sha256 values from measurements made at S13 prep: the live file's,
    and main's merged copy's). First the dry run - it prints every check and what it would do, and writes NOTHING:
      powershell -NoProfile -ExecutionPolicy Bypass -File E:\research\econfindatalibrary\tools\machine\swap_guard_statcan_s13.ps1 -ExpectLiveSha <LIVE> -ExpectCopySha <COPY> -DryRun
    then the same line without -DryRun.

    AFTER IT (separate S13 steps, not done here): restart EconGuard; then the receipts -
      python tools\selfhost\t0_ready.py --s13          (statcan-runner and sec-edgar-runner PASS on the live guard)
      logs\_guard.log shows "relaunched statcan_selfhost" and no "relaunched statcan_lane"
      python tools\guard_heartbeat.py --check --local  (the beat carries the post-T0 lane's marker)

    Exit codes: 0 done (or, with -DryRun, every check passed); 1 refused before any change (a premise is not met);
    5 not swapped (the live file is not the expected one - measure again); 4 swapped but the result did not verify,
    ROLLED BACK to the backup; 6 ROLLBACK FAILED (the manual restore line is printed and logged).
    Writes (without -DryRun) only: the backup, one temp file beside the live guard, the live guard itself (atomic
    replace), and its log.
#>
param(
    [Parameter(Mandatory = $true)] [string] $ExpectLiveSha,
    [Parameter(Mandatory = $true)] [string] $ExpectCopySha,
    [switch] $DryRun,
    # Overrides FOR TESTS ONLY (tests/test_guard_swap_statcan_s13.py). The S13 line passes none of them.
    [string] $Root = 'E:\research\econfindatalibrary',
    [string] $FlagPath = 'C:\ProgramData\econ\CUTOVER',
    [string] $BackupDir = 'F:\launcher_watchdog\backups',
    [string] $LogPath = '',
    [string] $ProcessListJson = '',
    # TEST HOOKS (code review F5): a .ps1 run with the live guard's path as its argument just before the second
    # compare-and-swap, or just after the replace, so a test can change the live file at exactly those moments.
    # Never passed at S13; with neither set the swap runs exactly as without them.
    [string] $TestHookBeforeReplace = '',
    [string] $TestHookAfterReplace = ''
)
$ErrorActionPreference = 'Stop'
$live = Join-Path $Root 'RELAUNCH_GUARD.ps1'
$src = Join-Path (Join-Path $Root 'tools\machine') 'RELAUNCH_GUARD.ps1.post-t0-copy'
if (-not $LogPath) { $LogPath = Join-Path $BackupDir 'swap_guard_statcan_s13.log' }
$stamp = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ')
$bak = Join-Path $BackupDir ("RELAUNCH_GUARD.ps1.bak-" + $stamp)
$tmp = Join-Path $Root ("RELAUNCH_GUARD.tmp-" + $stamp + ".ps1")
$NEW_LANE = 'jobs/statcan_selfhost_lane.py'
$OLD_LANE = 'jobs/statcan_lane.py'

# Function names are Verb-Noun and checked against PowerShell's built-in aliases (R1991: `h` is Get-History).
function Write-SwapLine([string]$m) {
    $t = (Get-Date).ToUniversalTime().ToString('o')
    Write-Host "$t $m"
    if (-not $DryRun) {
        try { Add-Content -LiteralPath $LogPath -Value "$t $m" } catch { Write-Host "$t (log not written: $($_.Exception.Message))" }
    }
}
function Get-Sha256Hex([string]$p) { (Get-FileHash -LiteralPath $p -Algorithm SHA256).Hash.ToUpperInvariant() }
function Test-CutOverFlag([string]$p) {
    # core.cutover.is_cut_over's ONE rule: absent file or folder = not cut over; ANY other failure = cut over
    try { $null = [System.IO.File]::GetAttributes($p); return $true }
    catch [System.IO.FileNotFoundException] { return $false }
    catch [System.IO.DirectoryNotFoundException] { return $false }
    catch { return $true }
}
function Get-GuardCode([string]$p) {
    $text = [System.IO.File]::ReadAllText($p)
    $text = [regex]::Replace($text, '(?s)<#.*?#>', '')
    $keep = @()
    foreach ($line in ($text -split "`r?`n")) { if (-not $line.TrimStart().StartsWith('#')) { $keep += $line } }
    return ($keep -join "`n")
}
function Test-LaunchesScript([string]$code, [string]$script) {
    return [regex]::IsMatch($code, "args\s*=\s*@\([^)]*'" + [regex]::Escape($script) + "'")
}
function Split-StatcanBlock([string]$p) {
    # the statcan block = from the line '  # statcan LANE...' through the first "  @{ n = 'statcan..." entry line
    $lines = [System.IO.File]::ReadAllText($p) -split "`r?`n"
    $start = -1; $end = -1
    for ($i = 0; $i -lt $lines.Count; $i++) {
        if ($start -lt 0 -and $lines[$i] -match '^  # statcan LANE') { $start = $i }
        if ($start -ge 0 -and $lines[$i].StartsWith("  @{ n = 'statcan")) { $end = $i; break }
    }
    if ($start -lt 0 -or $end -lt 0) { return $null }
    $rest = New-Object System.Collections.Generic.List[string]
    for ($i = 0; $i -lt $lines.Count; $i++) { if ($i -lt $start -or $i -gt $end) { $rest.Add($lines[$i]) } }
    $block = New-Object System.Collections.Generic.List[string]
    for ($i = $start; $i -le $end; $i++) { $block.Add($lines[$i]) }
    return @{ rest = ($rest -join "`n"); block = ($block -join "`n") }
}
function Get-ProcessRows {
    if ($ProcessListJson) {
        $j = [System.IO.File]::ReadAllText($ProcessListJson) | ConvertFrom-Json
        return @($j)
    }
    return @(Get-CimInstance Win32_Process | Select-Object ProcessId, Name, CommandLine)
}
function Stop-Refused([int]$code, [string]$why) {
    Write-SwapLine ("REFUSED (exit " + $code + "): " + $why)
    exit $code
}

if (-not $DryRun) { try { New-Item -ItemType Directory -Force -Path $BackupDir | Out-Null } catch { } }
Write-SwapLine ("=== S13 guard swap " + $(if ($DryRun) { 'DRY RUN (writes nothing)' } else { 'START' }) + " (stamp " + $stamp + ")")
try {
    foreach ($s in @($ExpectLiveSha, $ExpectCopySha)) {
        if ($s -notmatch '^[0-9A-Fa-f]{64}$') { Stop-Refused 1 "a sha256 argument is not 64 hex characters: '$s'" }
    }
    $ExpectLiveSha = $ExpectLiveSha.ToUpperInvariant(); $ExpectCopySha = $ExpectCopySha.ToUpperInvariant()

    # 1. T0 has happened
    if (-not (Test-CutOverFlag $FlagPath)) { Stop-Refused 1 "the CUTOVER flag $FlagPath does not exist: before T0 the pre-T0 guard stays" }
    Write-SwapLine "check 1 ok: the CUTOVER flag is present (T0 has happened)"

    # 2. EconGuard is stopped and the pre-T0 lane is not running
    $rows = Get-ProcessRows
    $loops = @($rows | Where-Object { $_.CommandLine -and $_.CommandLine -like '*RELAUNCH_GUARD_LOOP.ps1*' -and $_.ProcessId -ne $PID })
    if ($loops.Count -gt 0) { Stop-Refused 1 ("the guard loop is running (pid " + (($loops | ForEach-Object { $_.ProcessId }) -join ', ') + "): stop EconGuard first (S8a), then run this again") }
    $old = @($rows | Where-Object { $_.CommandLine -and $_.CommandLine -like '*statcan_lane.py*' })
    if ($old.Count -gt 0) { Stop-Refused 1 ("the pre-T0 statcan lane is running (pid " + (($old | ForEach-Object { $_.ProcessId }) -join ', ') + "): it must be stopped (S4) - only when logs\statcan_writer.lock is absent") }
    Write-SwapLine "check 2 ok: no guard loop and no jobs/statcan_lane.py process"

    # 3. the post-T0 copy is the reviewed one, parses, and launches the post-T0 lane only
    if (-not (Test-Path -LiteralPath $src -PathType Leaf)) { Stop-Refused 1 "the post-T0 copy $src is missing (is B3 pulled into this checkout?)" }
    $srcSha = Get-Sha256Hex $src
    if ($srcSha -ne $ExpectCopySha) { Stop-Refused 1 "the post-T0 copy's sha256 is $srcSha, expected $ExpectCopySha" }
    $head = [System.IO.File]::ReadAllBytes($src)
    if ($head.Length -lt 3 -or $head[0] -ne 0xEF -or $head[1] -ne 0xBB -or $head[2] -ne 0xBF) { Stop-Refused 1 "the post-T0 copy has no UTF-8 BOM (PowerShell 5.1 would read it as ANSI)" }
    $tokens = $null; $perr = $null
    $null = [System.Management.Automation.Language.Parser]::ParseFile($src, [ref]$tokens, [ref]$perr)
    if ($perr.Count -gt 0) { Stop-Refused 1 ("the post-T0 copy does not parse: " + $perr[0].Message) }
    $code = Get-GuardCode $src
    if (-not (Test-LaunchesScript $code $NEW_LANE)) { Stop-Refused 1 "the post-T0 copy does not launch $NEW_LANE in its code" }
    if (Test-LaunchesScript $code $OLD_LANE) { Stop-Refused 1 "the post-T0 copy still launches $OLD_LANE" }
    if ($code -notmatch 'run_sec_edgar_local\.ps1') { Stop-Refused 1 "the post-T0 copy lost the sec_edgar runner call" }
    Write-SwapLine "check 3 ok: post-T0 copy sha $srcSha, BOM, 0 parse errors, launches $NEW_LANE and not $OLD_LANE"

    # 4. compare-and-swap premise: the live file is the one measured at S13 prep
    if (-not (Test-Path -LiteralPath $live -PathType Leaf)) { Stop-Refused 1 "the live guard $live is missing - restore it from a backup first" }
    $liveSha = Get-Sha256Hex $live
    if ($liveSha -ne $ExpectLiveSha) { Stop-Refused 5 "NOT SWAPPED: the live guard's sha256 is $liveSha, expected $ExpectLiveSha - measure again" }
    if ($liveSha -eq $srcSha) { Stop-Refused 1 "the live guard already IS the post-T0 copy - nothing to do" }
    Write-SwapLine "check 4 ok: live guard sha $liveSha (as expected)"

    # 5. THE SWAP CHANGES ONLY THE STATCAN ENTRY (design review C10): a live-only edit, or a copy change that was
    #    never installed, would otherwise be reverted or installed silently by this swap
    $l = Split-StatcanBlock $live
    $n = Split-StatcanBlock $src
    if ($null -eq $l -or $null -eq $n) { Stop-Refused 1 "the statcan block was not found in the live guard or in the post-T0 copy" }
    if ($l.rest -cne $n.rest) { Stop-Refused 1 "the live guard and the post-T0 copy differ OUTSIDE the statcan entry - run python tools\check_guard_copies.py --diff, install or revert that difference first (a reviewed step), then measure the shas again" }
    if (-not (Test-LaunchesScript (Get-GuardCode $live) $OLD_LANE)) { Stop-Refused 1 "the live guard's statcan entry does not launch $OLD_LANE - it is not the pre-T0 guard this swap replaces" }
    Write-SwapLine "check 5 ok: the live guard and the post-T0 copy differ only in the statcan entry (pre-T0 -> post-T0)"

    # 6. the guard skips a job with a .DONE file SILENTLY (RELAUNCH_GUARD: `if (Test-Path $doneFlag) { continue }`)
    $done = Join-Path (Join-Path $Root 'logs') 'statcan_selfhost.DONE'
    if (Test-Path -LiteralPath $done) { Stop-Refused 1 "$done exists: the guard would never launch the post-T0 lane - find out who wrote it and remove it first" }
    Write-SwapLine "check 6 ok: no logs\statcan_selfhost.DONE"

    if ($DryRun) {
        Write-SwapLine ("DRY RUN: would back up $live to $bak, write $tmp, replace $live with it (sha " + $srcSha + "), and log to $LogPath")
        Write-SwapLine "DRY RUN: every check passed; nothing was written"
        exit 0
    }

    # 7. backup, proved
    New-Item -ItemType Directory -Force -Path $BackupDir | Out-Null
    Copy-Item -LiteralPath $live -Destination $bak
    if ((Get-Sha256Hex $bak) -ne $liveSha) { Stop-Refused 1 "the backup $bak does not match the live guard - nothing changed" }
    Write-SwapLine "backup $bak sha $liveSha"
} catch {
    Write-SwapLine ("ERROR before any change to the live guard: " + $_.Exception.Message)
    exit 1
}

$swapped = $false
try {
    [System.IO.File]::WriteAllBytes($tmp, [System.IO.File]::ReadAllBytes($src))
    if ((Get-Sha256Hex $tmp) -ne $ExpectCopySha) { throw "the temp copy $tmp does not match the post-T0 copy" }
    if ($TestHookBeforeReplace) { & $TestHookBeforeReplace $live }
    if ((Get-Sha256Hex $live) -ne $ExpectLiveSha) {
        Remove-Item -LiteralPath $tmp -Force
        Write-SwapLine "NOT SWAPPED: the live guard changed during the swap - measure again"
        exit 5
    }
    [System.IO.File]::Replace($tmp, $live, [NullString]::Value)
    $swapped = $true
    if ($TestHookAfterReplace) { & $TestHookAfterReplace $live }
    $now = Get-Sha256Hex $live
    if ($now -ne $ExpectCopySha) { throw "after the replace the live guard's sha256 is $now, expected $ExpectCopySha" }
    Write-SwapLine ("SWAPPED: live guard sha256 now " + $now + ". Next (S13): restart EconGuard, then python tools\selfhost\t0_ready.py --s13")
    Write-SwapLine ("ROLLBACK if needed: Copy-Item -LiteralPath '" + $bak + "' -Destination '" + $live + "' -Force")
    exit 0
} catch {
    $why = $_.Exception.Message
    if (Test-Path -LiteralPath $tmp) { try { Remove-Item -LiteralPath $tmp -Force } catch { } }
    if (-not $swapped -and (Test-Path -LiteralPath $live) -and ((Get-Sha256Hex $live) -eq $ExpectLiveSha)) {
        Write-SwapLine ("NOT SWAPPED (" + $why + "): the live guard is unchanged")
        exit 1
    }
    try {
        Copy-Item -LiteralPath $bak -Destination $live -Force
        if ((Get-Sha256Hex $live) -ne $ExpectLiveSha) { throw "the restored live guard does not match the backup" }
        Write-SwapLine ("ROLLED BACK after: " + $why + " - the live guard is the backup again (sha " + $ExpectLiveSha + ")")
        exit 4
    } catch {
        Write-SwapLine ("ROLLBACK FAILED (" + $_.Exception.Message + ") after: " + $why)
        Write-SwapLine ("MANUAL RESTORE: Copy-Item -LiteralPath '" + $bak + "' -Destination '" + $live + "' -Force ; then check (Get-FileHash '" + $live + "').Hash -eq '" + $ExpectLiveSha + "'")
        exit 6
    }
}
