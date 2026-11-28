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

One more file here is a DRAFT, not a copy of anything live: `EconSlotDispatch.cmd.draft` is the
log-on launcher for the desktop slot dispatcher (`tools/econ_dispatch.py loop`, PLAN V2). It is
NOT installed. The owner installs it only after the dispatcher PRs are merged, E: is pulled, and a
post-merge receipt on a real cloud run has passed; its header holds the stop switch, heartbeat,
log and remove steps. `tools/check_guard_copies.py` does not compare it (there is no live file).

## How the chain works

1. **Startup folder** runs `EconGuard.cmd` at logon. Scheduled Tasks are blocked by policy on
   this machine, so the Startup folder is the only reboot-surviving trigger available.
2. It starts `RELAUNCH_GUARD_LOOP.ps1` through `Win32_Process.Create` with `ShowWindow 0`
   (since 2026-11-23; see "The launcher" below) — a `while ($true)` watchdog that ticks every
   300 s, stamps `logs/guard_loop.heartbeat`, and publishes the beat to R2 so CI can see a dead
   workstation (the daily run fails on a stale beat).
3. Each tick runs `RELAUNCH_GUARD.ps1` once, bounded to 120 s. That script relaunches any tracked
   job that is not currently running, skipping any with a `logs/<name>.DONE` sentinel.

## The launcher (EconGuard.cmd, changed 2026-11-23)

Until 2026-11-23 the launcher ran `start "" powershell -WindowStyle Hidden ... -File <loop>`.
That form hands the new console to Windows Terminal (ledger R2031): the loop owned a VISIBLE
Windows Terminal window inside Windows Terminal's job, and a click on its X, Ctrl+C in it, or a
Windows Terminal restart (an update is staged) can end the loop. A closed console leaves no log line
(measured on a conhost console, below; the Windows Terminal path itself was not tested). That is the
leading, unproven candidate for the death of 2026-11-21 (report
`F:\work\guard_death_20261121\REPORT.md`).

Now:

- The loop's command line is defined ONCE, on the `set "ECON_GUARD_LOOP_CMD=..."` line. Its
  `-File "E:\research\econfindatalibrary\RELAUNCH_GUARD_LOOP.ps1"` part is byte-identical to
  before, so every check that matches that literal still finds the loop. The full command line
  has one space less: `start ""` gave `powershell  -NoProfile ...` (two spaces), Create gives one.
  No check in this repo or in the known machine-local tools compares the full command line.
- A short-lived `powershell -Command` reads that line from the environment (so its own command
  line does not carry the literal, R260) and calls `Win32_Process.Create` with `ShowWindow 0`
  and `CurrentDirectory E:\research\econfindatalibrary`.
- It writes ONE line to `logs\_guard_loop.log`:
  `<utc>  launcher EconGuard.cmd: Win32_Process.Create returned 0, loop pid <P>`.
- If Create does not return 0 (or throws, or PowerShell cannot run), the old `start ""` line runs
  instead, and the log line says `falling back to start (a Windows Terminal window)`. A loop in
  a WT window is better than no loop.
- **There is NO refusal.** Running `EconGuard.cmd` while a loop runs starts a SECOND loop, and two
  loops mean double crawler launches on every tick. Check first (step 1 of "Starting the loop").

What was measured on a harmless dummy (`F:\work\maintenance\20261123_guard_exit_trace\receipts\`,
2026-11-23), each test `.cmd` made from the shipped bytes with only the paths swapped:

| form | console host | WT windows | job | parent |
|---|---|---|---|---|
| old `start ""` (control) | conhost + `OpenConsole.exe -Embedding` +0.06 s | 11 -> 12 -> 11 | caller's job | the cmd |
| new, run by `cmd /c` from an agent tool shell | conhost child only | 11 -> 11 -> 11 | not in a job | WmiPrvSE.exe |
| new, opened by explorer (the log-on path, but NOT a real log-on: timing and WMI readiness at log-on are not measured) | conhost child only | 11 -> 11 -> 11 (the cmd's own window 0.5 s) | not in a job | WmiPrvSE.exe |
| new, Create made to fail (missing folder, ReturnValue 8) | fallback: as the old form | 11 -> 12 -> 11 | caller's job | the cmd |
| `start "" conhost.exe --headless ...` | the dummy never started | - | - | - |
| hidden `Start-Process` from a short-lived powershell | conhost child only | 11 -> 11 | caller's job | the short powershell |

Only the Create form keeps the loop out of BOTH the Windows Terminal host and the caller's job.
The running loop since 2026-11-22T21:50:48Z (pid 30880) was started the same way by an agent
(`F:\work\maintenance\2026-11-22_1124_guard_review\wmi_start.ps1`). Since then its heartbeat is
fresh, a local-heavy pass under it succeeded (`logs\local_heavy.last_success` 2026-11-23T02:40:41Z),
and `logs\_guard_loop.log` has no line at all after 2026-11-21T21:10:59Z, so no publish failure
(read 2026-11-23T09:03Z). So a WMI start from an agent shell is proven for the real loop; a start at a
real log-on is not measured.

## What a dead loop's log tells you (the loop's exit trace, 2026-11-23)

The loop writes three kinds of line to `logs\_guard_loop.log`:

- `<utc>  loop starting pid <P>` — the script started.
- `<utc>  loop trap pid <P>: <type>: <message> (line <L>) - the loop goes on in 60 s` — a
  terminating error reached the while body. The loop waits 60 s and goes on with the next
  statement. The trap is the first statement INSIDE the loop: at script scope it ends the loop.
- `<utc>  loop exiting pid <P> (started <utc>; last trapped error: <...>)` — from the outer
  `finally`.

Measured on Windows PowerShell 5.1, 2026-11-23, on dummies built from the shipped loop bytes with
only the constants swapped and started the way the new launcher starts the loop (Create,
`ShowWindow 0`). Five runs per row (`F:\work\maintenance\20261123_guard_exit_trace\receipts\exit_matrix.out`):

| how the loop ends | "loop exiting" line | what happened |
|---|---|---|
| the script ends by itself (`exit`) | 5 of 5 | line, rc 0 |
| Ctrl+C on its console (`GenerateConsoleCtrlEvent`) | 5 of 5 | ends at once, line, rc 0 |
| console window closed (`WM_CLOSE`, as a click on X) | 0 of 5 | ends at once, NO line, rc 0xC000013A |
| `TerminateProcess` (and, by their documentation, Task Manager End task, `taskkill /F`, `Stop-Process`) | 0 of 5 | ends at once, NO line, rc 1 |
| its conhost killed | - | the loop does NOT end: it ticks on (5 of 5) |
| a terminating error in the loop body (.NET, `throw`, inside a catch) | - | a trap line, the loop goes on (the test, twice) |
| log-off, shutdown, a crash | not measured | |

So: a dead loop WITH a `loop exiting` line ended inside PowerShell (the script ended, or a Ctrl+C).
A dead loop WITHOUT one was killed from outside (TerminateProcess), lost its console window (a close),
crashed, or went down with a log-off or shutdown (unmeasured). With the new launcher the console has no
window to close and nobody can type Ctrl+C into it, so in practice "no line" means killed or crashed,
or a log-off/shutdown. A Ctrl+C test from an agent's tool shell is void: a child of that shell inherits
a Ctrl+C-disabled flag and ignores the event (first smoke run, 08:47Z).

## Starting the loop (after a death, or to install a new loop script)

Never while a loop runs. Never from inside a local-heavy pass (`logs\local_heavy.lock` present).

1. Prove there is no loop, in ONE PowerShell command. The path is split in two strings on purpose,
   so the checking command's own command line can never match it (R260):
   `Get-CimInstance Win32_Process -Filter "Name='powershell.exe'" | Where-Object { $_.ProcessId -ne $PID -and $_.CommandLine -like ('*-File "' + 'E:\research\econfindatalibrary\RELAUNCH_GUARD_LOOP' + '.ps1"*') }`
   must return nothing, AND `logs\guard_loop.heartbeat` must be more than 10 minutes old. A
   process the reader cannot open shows a null command line, so never trust the count alone.
2. Start it: double-click `EconGuard.cmd` in the Startup folder, or run it from any shell. When
   Create returns 0 the loop is outside that shell's job (R799), because WmiPrvSE creates it. On the
   FALLBACK path it is NOT: the old `start ""` line inherits the caller's job (measured IN A JOB). If
   the launcher line says `falling back` and the file was run from an agent's tool shell, the loop
   lives in that shell's job and dies with it (R799/R2032): treat it as NOT started.
3. Receipts (each one can fail; report section 8 plus the 2026-11-23 plan review, C7):
   1. `logs\_guard_loop.log` ends with the launcher line `Create returned 0, loop pid <P>`. After
      INSTALL step 2 (the new loop script) it is followed by `loop starting pid <P>`, the same `<P>`;
      the OLD loop script never writes that line, so before step 2 only the launcher line can be
      checked. A `falling back to start` line means the loop is in a Windows Terminal window: do not
      close that window; find out why Create failed (and see step 2 about the job).
   2. The step-1 filter now returns exactly ONE process, pid `<P>`, parent `WmiPrvSE.exe`, and its
      `SessionId` equals explorer.exe's.
   3. `python -I -B F:\work\guard_death_20261121\job_membership.py <P>` prints `not in a job`.
   4. `<P>` has a `conhost.exe` child, and NO `OpenConsole.exe -Embedding` was created within
      0.3 s AFTER `<P>`'s creation time (the launcher's own cmd window may have one 1-2 s EARLIER;
      that one is not the loop's).
   5. The Windows Terminal window count is the same before and after
      (`python -I -B F:\work\guard_death_20261121\review\rv_wt_windows.py`, read after the launcher
      has exited).
   6. Two heartbeats with different stamps in `logs\guard_loop.heartbeat` (the first comes up to
      about 2.5 min after the start), `F:\launcher_watchdog\hook.status` advances, and no new
      `heartbeat publish threw` or `EXCEEDED 90s` line in `logs\_guard_loop.log`.
   7. The next local-heavy pass succeeds (`logs\local_heavy.last_success` advances).

## INSTALL of the 2026-11-23 change (owner, or a later one-time approved step)

Two files, two steps. Do NOT do either during a log-on (cmd reads a batch file while it runs it).
Keep backups OUTSIDE the Startup folder: explorer opens EVERY file there at log-on, so a backup
named `*.cmd` or `*.bat` there would start a second loop. Copy bytes with `Copy-Item`, never
`Get-Content | Set-Content` (Windows PowerShell 5.1 writes a BOM).

0. After the PR is merged: `git -C E:\research\econfindatalibrary pull --ff-only` (E: on `main`).
   This pull deploys EVERY change merged since E: was last pulled, not only this one (R803): E: was
   on `main` at b2ed2ebe7 at 2026-11-23T09:21Z, 19 commits behind 899ffc3f5 (`git rev-list --count`). Then prove E: holds this change,
   because the step-3 receipt cannot see a stale E: (it compares E:'s copy with a live file copied
   from that same copy):
   `git -C E:\research\econfindatalibrary merge-base --is-ancestor <merge sha of this PR> HEAD`
   must exit 0. E: has `core.autocrlf=true`, so the copies there are CRLF; the live files today are
   LF. Both work; `tools/check_guard_copies.py` compares normalised lines, not bytes.
1. **The launcher, any time after step 0** (it is not running between log-ons):
   ```powershell
   $st = Join-Path $env:APPDATA 'Microsoft\Windows\Start Menu\Programs\Startup'
   New-Item -ItemType Directory -Force 'F:\econ_guard_backups' | Out-Null
   Copy-Item -LiteralPath (Join-Path $st 'EconGuard.cmd') -Destination ('F:\econ_guard_backups\EconGuard.cmd.bak-' + (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ'))
   Copy-Item -LiteralPath 'E:\research\econfindatalibrary\tools\machine\EconGuard.cmd.startup-copy' -Destination (Join-Path $st 'EconGuard.cmd')
   ```
2. **The loop script, ONLY when no loop runs** — the next time the loop is found dead, or when the
   owner chooses to stop it at a quiet moment (no `logs\local_heavy.lock`). Never edit the running
   script. Then "Starting the loop" above, steps 1-3:
   ```powershell
   Copy-Item -LiteralPath 'E:\research\econfindatalibrary\RELAUNCH_GUARD_LOOP.ps1' -Destination ('F:\econ_guard_backups\RELAUNCH_GUARD_LOOP.ps1.bak-' + (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ'))
   Copy-Item -LiteralPath 'E:\research\econfindatalibrary\tools\machine\RELAUNCH_GUARD_LOOP.ps1.workstation-copy' -Destination 'E:\research\econfindatalibrary\RELAUNCH_GUARD_LOOP.ps1'
   ```
   The extra receipt for this step: the new loop's FIRST line in `logs\_guard_loop.log` is
   `loop starting pid <P>` — the old script never writes it.
3. **Receipt for both:** `python tools\check_guard_copies.py` run in `E:\research\econfindatalibrary`
   prints `all reboot-survival scripts match their versioned copies`.

Until step 1 is done, `tests/test_guard_copies_in_sync.py` FAILS in every clone on this
workstation (it compares the clone's launcher copy with the live Startup file). Until step 2 is
done it also fails in `E:` (the loop pair). CI runs on ubuntu, where the test skips.

## Restoring after a machine rebuild

Copy each file back to its live location, then check the absolute paths: `$root` and `$python` at
the top of `RELAUNCH_GUARD.ps1` and `RELAUNCH_GUARD_LOOP.ps1`; in `EconGuard.cmd`, the loop path on
the `set "ECON_GUARD_LOOP_CMD=..."` line and the log path and `CurrentDirectory` inside the
`powershell -Command` line. Nothing else in them is machine-specific.

WHICH RELAUNCH_GUARD COPY (T0 item B3). There are two: `RELAUNCH_GUARD.ps1.workstation-copy` is the guard BEFORE
T0 (its statcan entry launches `jobs/statcan_lane.py`), and `RELAUNCH_GUARD.ps1.post-t0-copy` is the guard AFTER
T0 (its statcan entry launches `jobs/statcan_selfhost_lane.py`; nothing else differs, and
tests/test_statcan_selfhost_lane.py fails if anything else does - so a PR that changes the guard changes BOTH).
Restore from the one that matches the machine: no `C:\ProgramData\econ\CUTOVER` file = the workstation copy; the
file exists = the post-t0 copy. `tools/check_guard_copies.py` follows the same rule. A small PR after T0 makes the
post-t0 copy the only one.

## The S13 swap of the post-T0 guard (T0 item B3)

`swap_guard_statcan_s13.ps1` installs `RELAUNCH_GUARD.ps1.post-t0-copy` as the live guard at T0 step S13: after the
flag, with EconGuard STOPPED (S8a) and the pre-T0 lane stopped (S4), BEFORE EconGuard restarts. First
`python tools/check_guard_copies.py` on the stopped machine must show the live guard in sync with the WORKSTATION
copy (before the flag; after it, the tool compares with the post-T0 copy, so run it at S4-S8 or read its diff); then
the dry run, then the real run - one pasted line each, the two sha256 values measured at S13 prep:

    powershell -NoProfile -ExecutionPolicy Bypass -File E:\research\econfindatalibrary\tools\machine\swap_guard_statcan_s13.ps1 -ExpectLiveSha <LIVE> -ExpectCopySha <COPY> -DryRun

The script refuses unless the live guard and the post-T0 copy differ ONLY in the statcan entry, and while
`logs\statcan_selfhost.DONE` exists (the guard skips a .DONE'd job silently). What to do on each exit code:

- 0 - swapped: restart EconGuard, then the receipts: `python tools\selfhost\t0_ready.py --s13` (PASS),
  `logs\_guard.log` shows `relaunched statcan_selfhost` and no `relaunched statcan_lane`, and
  `python tools\guard_heartbeat.py --check --local` reads the post-T0 lane's beat.
- 1 or 5 - NOT swapped, the live guard is unchanged. Restarting EconGuard with the OLD guard is a NAMED DECISION:
  statcan then has no working lane after T0 and reads red. Otherwise fix the premise (or measure the shas again)
  and run the line again.
- 4 - swapped, did not verify, ROLLED BACK to the backup: as for 1/5.
- 6 - ROLLBACK FAILED: do NOT restart EconGuard until the printed manual-restore line has run and its hash matches.

GOING BACK after T0 (the flag removed - the owner's road): restore the pre-S13 guard from the backup the swap printed
(`F:\launcher_watchdog\backups\RELAUNCH_GUARD.ps1.bak-<stamp>`); with the flag gone the heartbeat judges the pre-T0
lane again, and that lane is only guarded by the pre-S13 guard.

## Keeping these honest

A copy drifts. `tools/check_guard_copies.py` diffs each copy against its live file and exits
non-zero when they disagree, so drift is visible rather than discovered during a rebuild.
Re-copy after any change to the live scripts.
