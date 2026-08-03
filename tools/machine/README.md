# Reboot-survival scripts — versioned copies of machine-local files

Everything that keeps this fleet running across a reboot lives in three files, and until
2026-08-03 **none of them was in version control**. `.gitignore:41` excludes `RELAUNCH_*.ps1`,
correctly — they pin absolute `E:\` paths and this box's `python.exe`, so committing them as
live files would hand a wrong-machine copy to anyone who checked the repo out. The consequence
was that the only copy of the fleet definition sat on one disk, unversioned and unreviewable.

These are `.copy` files, deliberately not the live ones:

| copy | live location |
|---|---|
| `EconGuard.cmd.startup-copy` | `%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\EconGuard.cmd` |
| `RELAUNCH_GUARD_LOOP.ps1.workstation-copy` | `E:\research\econfindatalibrary\RELAUNCH_GUARD_LOOP.ps1` |
| `RELAUNCH_GUARD.ps1.workstation-copy` | `E:\research\econfindatalibrary\RELAUNCH_GUARD.ps1` |

## How the chain works

1. **Startup folder** runs `EconGuard.cmd` at logon. Scheduled Tasks are blocked by policy on
   this machine, so the Startup folder is the only reboot-surviving trigger available.
2. It launches `RELAUNCH_GUARD_LOOP.ps1` hidden — a `while ($true)` watchdog that ticks every
   300 s, stamps `logs/guard_loop.heartbeat`, and publishes the beat to R2 so CI can see a dead
   workstation (the daily run fails on a stale beat).
3. Each tick runs `RELAUNCH_GUARD.ps1` once, bounded to 120 s. That script relaunches any tracked
   job that is not currently running, skipping any with a `logs/<name>.DONE` sentinel.

## Restoring after a machine rebuild

Copy each file back to its live location, then check the two absolute paths at the top of
`RELAUNCH_GUARD.ps1` — `$root` and `$python` — and the `-File` path inside `EconGuard.cmd`.
Nothing else in them is machine-specific.

## Keeping these honest

A copy drifts. `tools/check_guard_copies.py` diffs each copy against its live file and exits
non-zero when they disagree, so drift is visible rather than discovered during a rebuild.
Re-copy after any change to the live scripts.
