"""The desktop slot dispatcher: starts each cloud writer's cron slot at the cron's own time (PLAN V2, AR-470).

WHY. GitHub creates the scheduled runs of updater-heavy.yml (03:00Z, 12:00Z) and updater-daily.yml (06:00Z) hours
late (21-day median 388-402 min to 2026-11-11), and the writer gate (tools/ci_writer_gate.py) blocks the desktop
pass from each cron until its run is created: ~707 min/day of cron-not-created blocking. This program dispatches
each slot at its time, as `gh workflow run <wf> -f slot=<tag>`, so the gate can credit the cron at once. The crons
are NOT removed: they are the fallback when this machine or this program is off, and a late cron whose slot is
settled is a no-op (tools/slot_check.py ends it before any state pull).

ONE LOOK (the loop looks once a minute; `--once` is one look):
  - a STOP file (logs/STOP_DISPATCH, or STOP_DISPATCH at the repo root): nothing.
  - the OPEN slots: crons of the gate's WRITERS whose own window (ci_writer_gate.cron_windows: heavy 03Z 9 h,
    daily 06Z 24 h, heavy 12Z 15 h) holds `now` and that the gate does not credit (slot_credited, with
    run_settles on the jobs of finished slot dispatches) - exactly the crons the gate is blocking on, so every
    dispatch instant is one the gate already blocks (AR-470 H1). A slot whose window has ended is given up.
  - an open slot is dispatchable only if no slot dispatch of it exists yet, or every one that exists concluded
    `cancelled` with ZERO jobs (evicted while pending, R291 shape) and fewer than MAX_DISPATCHES were made
    (AR-470 H2: a timed-out, failed or human-cancelled run is left to the cron); and a dispatch this program sent
    that is not listed is NEVER sent again (AR-470 L2).
  - THE ORDER, under the critical-section lock shared with `manual` (one dispatcher at a time):
      1. write the marker (logs/econ_dispatch.marker; the gate blocks for 10 min after it);
      2. logs/local_heavy.lock absent;            (run_local_heavy.ps1 takes that lock atomically, THEN re-asks
      3. no run_local_heavy.ps1 process;           the gate: of the two orders, one side always backs off)
      4. both writers' listings, read through the gate's read_unfiltered / read_sched (an empty listing is
         re-read and, still empty, means "do not dispatch", R1660), show NO unfinished run (queued, pending,
         in progress, waiting): R81/R291 - never dispatch while a run is pending;
      5. no dispatch sent in the last 10 min;
      6. refresh the marker, `gh workflow run` (60 s timeout), record it, and wait for the run to be listed.
    A failed check after 1 removes the marker (no dispatch was sent).

RULE M - MANUAL DISPATCHES. Every human or agent dispatch of either writer goes through
    python tools/econ_dispatch.py manual --workflow updater-daily.yml [--source S] [--force] [--dry-run]
which takes the same critical section, waits (up to --wait-min) for no STOP file, an empty group, a free lock, no
local pass and no young marker, then dispatches with those inputs and NO slot (a manual run never settles a slot).

WHAT IT CANNOT KNOW: a cron run GitHub creates between step 4 and step 6 lands behind the dispatch (both are then
real or one is a no-op by the guard's rule; workflow-level concurrency serialises them); a dispatch from the
GitHub web page or a bare `gh workflow run` bypasses rule M; lane tools that pull and push state without
local_heavy.lock (derive_csv_bulk --clear-owed-only, clear_csv_desktop_owed, clear_fed_board_z1_retries,
prune_series_cursors) are NOT seen here at all. Some of them ask the gate (which blocks on the marker) before they
start; others ask nothing. A dispatch can therefore land inside such a lane's pull..push - the state CAS (R5) is
then the only guard, as for any cloud run today. Run those lanes only while this dispatcher is stopped (STOP file).

FILES (under --root/logs): econ_dispatch.log, econ_dispatch.state.json, econ_dispatch.heartbeat,
econ_dispatch.marker, econ_dispatch.cs.lock (critical section), econ_dispatch.loop.lock (one loop per root).

INSTALL HARDENING (AR-470 round 2, low findings L-a, L-b, L-c):
  L-a  the dispatch count of a slot is max(the count in the state file, the slot dispatches GitHub lists for that
       slot). A lost or reset state file therefore cannot reset the limit of MAX_DISPATCHES.
  L-b  with the REAL gh this program refuses to start unless its marker and lock paths are the ones the desktop
       pass uses: the marker is the file the gate of this checkout reads, tools/run_local_heavy.ps1 of this
       checkout still derives its lock and its gate from its own checkout, and the live guard script
       (RELAUNCH_GUARD.ps1, machine-local, not in git) starts run_local_heavy.ps1 from THIS checkout. In
       production that is E:\\research\\econfindatalibrary; a clone anywhere else is refused (root_problems()).
  L-c  a powershell whose command line cannot be read still blocks every dispatch (fail safe: it may be a
       run_local_heavy.ps1). The block is no longer silent: a LOUD log line at most once per LOUD_EVERY, the
       state field "process_check", and the same field on line 2 of the heartbeat, which the slot alarm's
       shadow (tools/scheduler_stall.py dispatch_shadow) reads and flags.
  The loop also holds a named mutex (Local\\EconSlotDispatchLoop_<root>) besides the loop lock: one copy per
  root in a log-on session. Launcher draft (NOT installed): tools/machine/EconSlotDispatch.cmd.draft.
TEST GUARDS (copied from the hfdatalibrary daily dispatch starter): without --gh the REAL gh is used, so
--now, --looks and --sleep are refused, --root is taken only with --once --dry-run, and --dry-run only with --once;
--root must be a folder that exists; --gh must be a path with no double quote and no " -" in it.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ci_writer_gate as g  # noqa: E402

REAL_ROOT = g.ROOT
REAL_GH = "gh"
DISPATCH_SPACING = dt.timedelta(minutes=10)
MAX_DISPATCHES = 2                  # the first dispatch + at most one re-dispatch after an eviction
LIST_WAIT_S = 300                   # wait this long for a sent dispatch to be listed
LIST_POLL_S = 15
GH_TIMEOUT_S = 60
LOCAL_PASS_SCRIPT = "run_local_heavy.ps1"
_IS_WINDOWS = os.name == "nt"              # the process check is a CIM query (tests patch this, not os.name)
LOUD_EVERY = dt.timedelta(minutes=60)     # L-c: the "process check unreadable" line repeats at most this often
GUARD_SCRIPT = "RELAUNCH_GUARD.ps1"         # L-b: the machine-local script that starts run_local_heavy.ps1
MUTEX_PREFIX = "Local\\EconSlotDispatchLoop_"
# L-b: how tools/run_local_heavy.ps1 derives its lock and its gate from its OWN checkout. If one of these lines
# changes, root_problems() refuses until this list is checked against the new runner (fail closed).
RUNNER_PATH_LITERALS = (
    "$repo = Split-Path -Parent $PSScriptRoot",
    "$logDir = Join-Path $repo 'logs'",
    "Join-Path $logDir 'local_heavy.lock'",
    "Join-Path $repo 'tools\\ci_writer_gate.py'",
)
_GUARD_ROOT_RE = re.compile(r"""(?m)^\s*\$root\s*=\s*["']([^"']+)["']""")


def _iso(t: dt.datetime) -> str:
    return t.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _norm(p: str) -> str:
    return os.path.normcase(os.path.realpath(p))


def root_paths(root: str) -> dict:
    """The two shared files, as THIS program derives them from its root: the marker the gate reads and the lock the
    desktop pass takes. Dispatcher and root_problems() both use this one definition."""
    logs = os.path.join(root, "logs")
    return {"marker": os.path.join(logs, os.path.basename(g.MARKER_PATH)),
            "lock": os.path.join(logs, "local_heavy.lock")}


def _read_text(path: str) -> str | None:
    try:
        with open(path, encoding="utf-8-sig", errors="replace") as f:
            return f.read()
    except OSError:
        return None


def root_problems(root: str, gate_root: str | None = None, gate_marker: str | None = None) -> list:
    """L-b: why `root` is NOT the checkout the desktop pass runs from ([] = it is). Fail closed: a file that cannot be
    read is a problem, never a pass."""
    gate_root = g.ROOT if gate_root is None else gate_root
    gate_marker = g.MARKER_PATH if gate_marker is None else gate_marker
    out = []
    r = _norm(root)
    if r != _norm(gate_root):
        out.append(f"the root {root} is not the checkout this program and its gate run from ({gate_root})")
    p = root_paths(root)
    if _norm(p["marker"]) != _norm(gate_marker):
        out.append(f"the marker this program writes ({p['marker']}) is not the file the gate reads ({gate_marker})")
    runner = os.path.join(root, "tools", LOCAL_PASS_SCRIPT)
    src = _read_text(runner)
    if src is None:
        out.append(f"{runner} cannot be read: this root has no desktop pass")
    else:
        missing = [lit for lit in RUNNER_PATH_LITERALS if lit not in src]
        if missing:
            out.append(f"{runner} no longer derives its lock and gate from its own checkout the way this check "
                       f"reads it (missing {missing}); its lock may not be {p['lock']} - check RUNNER_PATH_LITERALS")
    guard = os.path.join(root, GUARD_SCRIPT)
    gsrc = _read_text(guard)
    if gsrc is None:
        out.append(f"{guard} cannot be read: the guard loop does not start the desktop pass from this checkout")
    else:
        m = _GUARD_ROOT_RE.search(gsrc)
        if m is None:
            out.append(f"{guard} has no $root = \"...\" line, so the checkout of the desktop pass is unknown")
        elif _norm(m.group(1)) != r:
            out.append(f"{guard} starts the desktop pass from {m.group(1)}, not from {root}")
        if LOCAL_PASS_SCRIPT not in gsrc:
            out.append(f"{guard} does not start {LOCAL_PASS_SCRIPT}")
    return out


def mutex_name_for(root: str) -> str:
    return MUTEX_PREFIX + re.sub(r"[^a-z0-9]", "_", _norm(root).lower().rstrip("\\/"))[-200:]


class NamedMutex:
    """A Windows named mutex (session-local name), freed by the OS when the process dies. Elsewhere a no-op that
    always acquires: the dispatcher runs on the workstation only (the process check refuses other systems)."""

    def __init__(self, name: str):
        self.name, self.handle = name, None

    def acquire(self) -> bool:
        if os.name != "nt":
            return True
        import ctypes  # noqa: PLC0415
        k = ctypes.WinDLL("kernel32", use_last_error=True)
        k.CreateMutexW.restype = ctypes.c_void_p
        k.CreateMutexW.argtypes = [ctypes.c_void_p, ctypes.c_bool, ctypes.c_wchar_p]
        h = k.CreateMutexW(None, True, self.name)
        err = ctypes.get_last_error()
        if not h:
            raise OSError(f"CreateMutexW failed ({err})")
        if err == 183:                       # ERROR_ALREADY_EXISTS: another copy holds it
            k.CloseHandle(ctypes.c_void_p(h))
            return False
        self.handle = h
        return True

    def release(self):
        if self.handle is None or os.name != "nt":
            return
        import ctypes  # noqa: PLC0415
        k = ctypes.WinDLL("kernel32", use_last_error=True)
        k.ReleaseMutex(ctypes.c_void_p(self.handle))
        k.CloseHandle(ctypes.c_void_p(self.handle))
        self.handle = None


# ------------------------------------------------------------------ OS helpers (a file lock that dies with us)
class FileLock:
    """A non-blocking exclusive OS lock on a file, released by the OS when the process dies (msvcrt on Windows,
    fcntl elsewhere). Shared by every copy and every logon session that uses the same --root."""

    def __init__(self, path: str):
        self.path, self.fh = path, None

    def acquire(self) -> bool:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        fh = open(self.path, "a+b")
        try:
            if os.name == "nt":
                import msvcrt  # noqa: PLC0415
                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl  # noqa: PLC0415
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            fh.close()
            return False
        self.fh = fh
        return True

    def release(self):
        if self.fh is None:
            return
        try:
            if os.name == "nt":
                import msvcrt  # noqa: PLC0415
                self.fh.seek(0)
                msvcrt.locking(self.fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl  # noqa: PLC0415
                fcntl.flock(self.fh.fileno(), fcntl.LOCK_UN)
        finally:
            self.fh.close()
            self.fh = None


# The process check. A run_local_heavy.ps1 between its first gate read and taking the lock is visible ONLY as a
# process (the runner reads the gate long before it takes the lock), so this check is what covers that gap. The
# checker is itself a powershell whose command line contains the pattern, so it excludes its own $PID (R1859).
# It prints TWO counts, "<matched> <unreadable>": powershells whose command line names the pattern, and
# powershells whose CommandLine reads NULL (elevated, or another session). The dispatcher treats an unreadable one
# as PRESENT (AR-470 round 1: it may be the pass) and says so loudly (L-c), instead of folding both into one count.
_PS_CHECK = (
    "$all = @(Get-CimInstance Win32_Process -Filter \"Name='powershell.exe' OR Name='pwsh.exe'\" | "
    "Where-Object { $_.ProcessId -ne $PID }); "
    "$u = @($all | Where-Object { $null -eq $_.CommandLine }).Count; "
    "$m = @($all | Where-Object { $null -ne $_.CommandLine -and "
    "$_.CommandLine -match [regex]::Escape($env:ECON_DISPATCH_PATTERN) }).Count; "
    "Write-Output \"$m $u\""
)


def local_pass_probe(pattern: str = LOCAL_PASS_SCRIPT, timeout: int = 60) -> tuple:
    """(matched, unreadable): powershells running `pattern`, and powershells whose command line cannot be read.
    Anything that cannot be measured raises (fail closed: the dispatcher then sends nothing)."""
    if not _IS_WINDOWS:
        raise RuntimeError("the process check needs Windows")
    env = dict(os.environ, ECON_DISPATCH_PATTERN=pattern)
    out = subprocess.run(["powershell", "-NoProfile", "-NonInteractive", "-Command", _PS_CHECK], capture_output=True,
                         text=True, timeout=timeout, env=env)
    txt = out.stdout.strip().splitlines()
    parts = txt[-1].split() if txt else []
    if out.returncode != 0 or len(parts) != 2 or not all(x.isdigit() for x in parts):
        raise RuntimeError(f"process check failed (exit {out.returncode}): {out.stderr.strip()[:160]}")
    return int(parts[0]), int(parts[1])


def local_pass_running(pattern: str = LOCAL_PASS_SCRIPT, timeout: int = 60) -> bool:
    """True when a run_local_heavy.ps1 or an unreadable powershell runs (both block a dispatch)."""
    m, u = local_pass_probe(pattern, timeout)
    return m + u > 0


# ------------------------------------------------------------------ gh
class Gh:
    """The gh calls, bounded. `exe` may be a .py fake (run with this Python) for the tests."""

    def __init__(self, exe: str = REAL_GH):
        self.exe = exe

    def _argv(self, args):
        if self.exe.lower().endswith(".py"):
            return [sys.executable, self.exe, *args]
        return [self.exe, *args]

    def _json(self, args):
        out = subprocess.run(self._argv(args), capture_output=True, text=True, timeout=GH_TIMEOUT_S)
        if out.returncode != 0:
            raise RuntimeError(f"gh {' '.join(args[:3])} failed ({out.returncode}): {out.stderr.strip()[:160]}")
        data = json.loads(out.stdout)
        if not isinstance(data, list):
            raise RuntimeError(f"gh {' '.join(args[:3])}: not a list")
        return data

    def runs(self, wf):
        return self._json(["run", "list", "--repo", g.REPO, "--workflow", wf, "--limit", str(g.RUN_LIMIT),
                           "--json", g.RUN_FIELDS])

    def sched(self, wf):
        return self._json(["run", "list", "--repo", g.REPO, "--workflow", wf, "--event", "schedule", "--limit",
                           str(g.SCHED_LIMIT), "--json", g.RUN_FIELDS])

    def jobs(self, run_id):
        return self._json(["api", f"repos/{g.REPO}/actions/runs/{int(run_id)}/jobs?per_page=100", "--jq",
                           "[.jobs[] | {name, status, conclusion, steps: [.steps[]? | {name, conclusion}]}]"])

    def dispatch(self, wf, inputs: dict) -> tuple[bool, str]:
        args = ["workflow", "run", wf, "--repo", g.REPO, "--ref", "main"]
        for k, v in inputs.items():
            args += ["-f", f"{k}={v}"]
        try:
            out = subprocess.run(self._argv(args), capture_output=True, text=True, timeout=GH_TIMEOUT_S)
        except Exception as e:  # noqa: BLE001
            return False, f"{type(e).__name__}: {e}"
        return out.returncode == 0, (out.stdout + out.stderr).strip()[:200]


# ------------------------------------------------------------------ the decision
class Dispatcher:
    """One dispatcher over one root. Every outside effect is a method or an injected callable, so the tests can
    replace gh, the clock, the lock and the process check, and record the ORDER of the checks."""

    def __init__(self, root: str, gh: Gh | None = None, now=None, sleep=time.sleep, dry_run=False, echo=False,
                 lock_present=None, pass_running=None, pass_probe=None, mutex_name=None):
        self.root = root
        self.logs = os.path.join(root, "logs")
        self.gh = gh or Gh()
        self._now = now
        self.sleep = sleep
        self.dry_run = dry_run
        self.echo = echo
        self.paths = {k: os.path.join(self.logs, f"econ_dispatch.{k}") for k in ("log", "heartbeat")}
        # ONE marker name, the gate's (F4): the gate reads <its repo>/logs/<this name>; one definition (root_paths)
        shared = root_paths(root)
        self.paths["marker"] = shared["marker"]
        self.paths["state"] = os.path.join(self.logs, "econ_dispatch.state.json")
        self.paths["cs"] = os.path.join(self.logs, "econ_dispatch.cs.lock")
        self.paths["loop"] = os.path.join(self.logs, "econ_dispatch.loop.lock")
        self.lock_file = shared["lock"]
        self.stop_files = [os.path.join(self.logs, "STOP_DISPATCH"), os.path.join(root, "STOP_DISPATCH")]
        self._lock_present = lock_present or (lambda: os.path.exists(self.lock_file))
        if pass_probe is not None:
            self._pass_probe = pass_probe
        elif pass_running is not None:      # the older bool form (tests): a hit is a matched process
            self._pass_probe = lambda: (1 if pass_running() else 0, 0)
        else:
            self._pass_probe = local_pass_probe
        self.mutex_name = mutex_name or mutex_name_for(root)
        self.trace = []                   # the order of the checks, for the tests (AR-470 H1 mutants)

    # -- small effects
    def now(self) -> dt.datetime:
        return self._now() if self._now else dt.datetime.now(dt.timezone.utc)

    def log(self, text: str):
        line = f"{_iso(self.now())} {text}"
        os.makedirs(self.logs, exist_ok=True)
        for _ in range(5):              # shared-read append (a viewer holding the log open must not lose lines)
            try:
                with open(self.paths["log"], "a", encoding="utf-8") as f:
                    f.write(line + "\n")
                break
            except OSError:
                time.sleep(0.2)
        if self.echo:
            print(line)

    def stopped(self) -> str | None:
        return next((p for p in self.stop_files if os.path.exists(p)), None)

    def load_state(self) -> dict:
        try:
            with open(self.paths["state"], encoding="utf-8") as f:
                s = json.load(f)
            return s if isinstance(s, dict) else {}
        except FileNotFoundError:
            return {}
        except Exception as e:  # noqa: BLE001
            # an unreadable state must not read as "never dispatched": stop dispatching until a person looks
            return {"_unreadable": f"{type(e).__name__}: {e}"}

    def save_state(self, s: dict):
        os.makedirs(self.logs, exist_ok=True)
        tmp = self.paths["state"] + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(s, f, indent=1, sort_keys=True)
        os.replace(tmp, self.paths["state"])

    def write_marker(self, what: str):
        self.trace.append("marker")
        if self.dry_run:
            return
        os.makedirs(self.logs, exist_ok=True)
        with open(self.paths["marker"], "w", encoding="utf-8") as f:
            f.write(f"{_iso(self.now())}\n{what}\n")

    def remove_marker(self):
        if self.dry_run:
            return
        try:
            os.remove(self.paths["marker"])
        except FileNotFoundError:
            pass

    def heartbeat_field(self) -> dict:
        """Line 2 of the heartbeat (L-c): the process-check state, read by scheduler_stall.dispatch_shadow."""
        s = self.load_state()
        if "_unreadable" in s:
            return {"process_check": "unknown", "state_file": "unreadable"}
        pc = s.get("process_check")
        if not isinstance(pc, dict) or not pc.get("state"):
            return {"process_check": "not_checked_yet"}
        return {"process_check": pc["state"], **{k: v for k, v in pc.items() if k != "state"}}

    def heartbeat(self):
        os.makedirs(self.logs, exist_ok=True)
        field = self.heartbeat_field()
        with open(self.paths["heartbeat"], "w", encoding="utf-8") as f:
            f.write(_iso(self.now()) + "\n" + json.dumps(field, sort_keys=True) + "\n")

    def _set_process_check(self, rec: dict | None):
        """Write state["process_check"] (None removes it). Not in a dry run; never over an unreadable state."""
        if self.dry_run:
            return
        s = self.load_state()
        if "_unreadable" in s:
            return
        if rec is None:
            s.pop("process_check", None)
        else:
            s["process_check"] = rec
        self.save_state(s)

    def process_unreadable(self, key: str, n: int):
        """L-c: n powershell command lines cannot be read. Fail safe (no dispatch), but loud and recorded."""
        now = self.now()
        s = self.load_state()
        pc = s.get("process_check") if isinstance(s.get("process_check"), dict) else {}
        was = pc.get("state") == "unreadable"
        since = pc.get("unreadable_since") if was else _iso(now)
        looks = (int(pc.get("blocked_looks", 0)) if was else 0) + 1
        last_loud = g._parse(pc["last_loud_utc"]) if was and pc.get("last_loud_utc") else None
        loud = last_loud is None or now - last_loud >= LOUD_EVERY
        self._set_process_check({"state": "unreadable", "unreadable_since": since, "last_seen_utc": _iso(now),
                                 "unreadable_procs": int(n), "blocked_looks": looks,
                                 "last_loud_utc": _iso(now) if loud else pc.get("last_loud_utc")})
        if loud:
            self.log(f"LOUD: {key}: NO DISPATCH - the process check cannot read the command line of {n} powershell "
                     f"process(es) (elevated, or another log-on session), so it cannot rule out a "
                     f"{LOCAL_PASS_SCRIPT}. Fail safe: nothing is dispatched while this lasts and the crons serve the "
                     f"slots. V2 IS STALLED since {since} ({looks} blocked look(s)). Find them with: Get-CimInstance "
                     f"Win32_Process -Filter \"Name='powershell.exe' OR Name='pwsh.exe'\" | Where-Object "
                     f"{{ $null -eq $_.CommandLine }}. This line repeats at most once per "
                     f"{int(LOUD_EVERY.total_seconds() // 60)} min; the heartbeat carries the state on line 2.")
        elif self.echo:
            self.log(f"{key}: no dispatch: {n} unreadable powershell command line(s) (blocked since {since})")

    def process_readable(self):
        """L-c: the process check read every command line. Clear a recorded block, and say it ended."""
        s = self.load_state()
        pc = s.get("process_check") if isinstance(s.get("process_check"), dict) else {}
        if pc.get("state") == "unreadable":
            self.log(f"process check readable again (blocked since {pc.get('unreadable_since')}, "
                     f"{pc.get('blocked_looks')} blocked look(s)); dispatching can resume")
        self._set_process_check({"state": "clear", "checked_utc": _iso(self.now())})

    # -- readings
    def read_listings(self):
        """({wf: unfiltered}, {wf: sched}, notes, why_not) through the gate's own readers; why_not is set when the
        group cannot be ruled empty (R1660: a still-empty listing, or a read that failed)."""
        runs, sched, notes = {}, {}, []
        try:
            for wf in g.WRITERS:
                runs[wf], n = g.read_unfiltered(wf, self.gh.runs, sleep=self.sleep)
                notes += n
            for wf in g.WRITERS:
                sched[wf], n = g.read_sched(wf, self.gh.sched, runs[wf], sleep=self.sleep)
                notes += n
        except Exception as e:  # noqa: BLE001
            return runs, sched, notes, f"a listing could not be read ({type(e).__name__}: {str(e)[:120]})"
        empty = [wf for wf in g.WRITERS if not runs[wf] or not sched[wf]]
        if empty:
            return runs, sched, notes, f"listing still empty for {', '.join(empty)} (R1660: not 'no runs')"
        # A KNOWN-ROW POSITIVE CONTROL (review AR-470 round 2, F6): the newest run id seen by an earlier look must
        # still be in this unfiltered listing (newest-first, RUN_LIMIT rows). A non-empty but partial or stale
        # answer that lacks it is not evidence that the group is empty.
        state = self.load_state()
        seen = state.get("seen_max") if isinstance(state.get("seen_max"), dict) else {}
        for wf in g.WRITERS:
            ids = {r.get("databaseId") for r in runs[wf]}
            top = max((i for i in ids if isinstance(i, int)), default=None)
            known = seen.get(wf)
            if isinstance(known, int) and known not in ids and (top is None or top < known):
                return runs, sched, notes, (f"{wf} listing lacks run {known} seen by an earlier look (newest listed "
                                            f"{top}): a partial or stale answer")
        if not self.dry_run and "_unreadable" not in state:
            new = dict(seen)
            for wf in g.WRITERS:
                got = [r["databaseId"] for r in runs[wf] if isinstance(r.get("databaseId"), int)]
                if got:
                    new[wf] = max(got + ([seen[wf]] if isinstance(seen.get(wf), int) else []))
            if new != seen:
                state["seen_max"] = new
                self.save_state(state)
        return runs, sched, notes, None

    @staticmethod
    def unfinished(runs: dict, sched: dict):
        for wf in g.WRITERS:
            for r in list(runs.get(wf, [])) + list(sched.get(wf, [])):
                if r.get("status") != "completed":
                    return wf, r
        return None

    def zero_jobs(self, run_id) -> bool | None:
        try:
            return len(self.gh.jobs(run_id)) == 0
        except Exception:  # noqa: BLE001
            return None

    def open_slots(self, runs: dict, sched: dict, now: dt.datetime) -> list:
        """[(cron, wf, hour)] the gate still blocks on, oldest first (inside their own window, not credited)."""
        settled, _notes = g.settled_runs(runs, sched, now, self.gh.jobs)
        windows = g.cron_windows()
        out = []
        for wf, hours in g.WRITERS.items():
            both = list(runs.get(wf, [])) + list(sched.get(wf, []))
            for back in (1, 0):
                day = (now - dt.timedelta(days=back)).date()
                for h in hours:
                    cron = dt.datetime(day.year, day.month, day.day, h, tzinfo=dt.timezone.utc)
                    if not cron <= now < cron + windows[wf][h]:
                        continue
                    if not g.slot_credited(wf, h, cron, both, sched.get(wf, []), settled, windows):
                        out.append((cron, wf, h))
        return sorted(out)

    @staticmethod
    def listed_dispatches(wf: str, cron: dt.datetime, runs: dict) -> dict:
        """{databaseId: row} of the slot dispatches of this cron that GitHub lists."""
        mine = {}
        for r in runs.get(wf, []):
            s = g.run_slot(wf, r)
            if s and s[1] == "slot" and s[0] == cron and r.get("databaseId") is not None:
                mine[r["databaseId"]] = r
        return mine

    @staticmethod
    def _sent(state: dict, tag: str) -> int:
        try:
            return int((state.get("slots") or {}).get(tag, {}).get("sent", 0))
        except (AttributeError, TypeError, ValueError):
            return MAX_DISPATCHES          # an unreadable count must not read as "none sent"

    def dispatchable(self, wf: str, cron: dt.datetime, runs: dict, state: dict) -> tuple[bool, str]:
        tag = g.slot_tag(wf, cron)
        sent = self._sent(state, tag)
        mine = self.listed_dispatches(wf, cron, runs)
        if len(mine) < sent:
            return False, (f"{tag}: {sent} dispatch(es) sent, {len(mine)} listed - an unlisted dispatch is never "
                           f"sent again (AR-470 L2); the cron serves the slot")
        # L-a: the count is max(state, listing), so a lost or reset state file cannot reset the limit
        count = max(sent, len(mine))
        if count >= MAX_DISPATCHES:
            return False, (f"{tag}: {count} dispatches already (state {sent}, listed {len(mine)}; the limit is "
                           f"{MAX_DISPATCHES}); the cron serves the slot")
        for rid, r in mine.items():
            if r.get("conclusion") != "cancelled":
                return False, (f"{tag}: dispatch {rid} is {r.get('status')}/{r.get('conclusion')} and did not settle "
                               f"the slot - left to the cron (only an eviction is re-dispatched)")
            z = self.zero_jobs(rid)
            if z is not True:
                return False, (f"{tag}: dispatch {rid} was cancelled "
                               + ("WITH jobs (not an eviction)" if z is False else "and its jobs could not be read")
                               + " - left to the cron")
        return True, tag

    # -- one look
    def look(self) -> int:
        """0 = nothing wrong (dispatched or nothing to do), 1 = a reading failed."""
        self.trace = []
        stop = self.stopped()
        if stop:
            if self.echo:
                self.log(f"a STOP file is present ({stop}): nothing")
            return 0
        state = self.load_state()
        if "_unreadable" in state:
            self.log(f"the state file is unreadable ({state['_unreadable']}): no dispatch until a person fixes it")
            return 1
        now = self.now()
        runs, sched, notes, why_not = self.read_listings()
        if why_not:
            self.log(f"no dispatch: {why_not}")
            return 1
        busy = self.unfinished(runs, sched)
        if busy:
            if self.echo:
                self.log(f"no dispatch: {busy[0]} run {busy[1].get('databaseId')} is {busy[1].get('status')}")
            return 0
        slots = self.open_slots(runs, sched, now)
        target = None
        for cron, wf, _h in slots:
            ok, why = self.dispatchable(wf, cron, runs, state)
            if ok:
                target = (cron, wf)
                break
            if self.echo:
                self.log(why)
        if target is None:
            if self.echo:
                self.log(f"nothing to dispatch ({len(slots)} open slot(s))")
            return 0
        cron, wf = target
        return self._dispatch(wf, {"slot": g.slot_tag(wf, cron)}, g.slot_tag(wf, cron), state)

    def _dispatch(self, wf: str, inputs: dict, key: str, state: dict) -> int:
        """Steps 1-6 of the docstring, in that order, under the critical-section lock."""
        cs = FileLock(self.paths["cs"])
        if not cs.acquire():
            self.log(f"{key}: another dispatcher holds the critical section; not now")
            return 0
        sent_call = False
        try:
            self.write_marker(f"{wf} {key}")                                   # 1
            self.trace.append("lock")
            if self._lock_present():                                          # 2
                self.log(f"{key}: logs/local_heavy.lock is present - a desktop pass holds the state; not now")
                return 0
            self.trace.append("process")
            try:
                matched, unreadable = self._pass_probe()                      # 3
            except Exception as e:  # noqa: BLE001
                self.log(f"{key}: the local-pass process check failed ({e}); not now")
                return 1
            if matched:
                self.log(f"{key}: a run_local_heavy.ps1 process is running; not now")
                return 0
            if unreadable:                       # L-c: fail safe, but loud, recorded and in the heartbeat
                self.process_unreadable(key, unreadable)
                return 0
            self.process_readable()
            self.trace.append("listings")
            runs, sched, _notes, why_not = self.read_listings()                # 4
            if why_not:
                self.log(f"{key}: no dispatch: {why_not}")
                return 1
            busy = self.unfinished(runs, sched)
            if busy:
                self.log(f"{key}: no dispatch: {busy[0]} run {busy[1].get('databaseId')} is {busy[1].get('status')}")
                return 0
            last = g._parse(state["last_dispatch_utc"]) if state.get("last_dispatch_utc") else None
            if last is not None and self.now() - last < DISPATCH_SPACING:   # 5
                self.log(f"{key}: a dispatch was sent at {state['last_dispatch_utc']}; not within 10 min")
                return 0
            if "slot" in inputs:   # the slot may have been served while we checked
                cron = g.parse_slot_tag(inputs["slot"], wf)
                if (cron, wf, cron.hour) not in self.open_slots(runs, sched, self.now()):
                    self.log(f"{key}: no longer open; nothing sent")
                    return 0
                ok, why = self.dispatchable(wf, cron, runs, state)
                if not ok:
                    self.log(why)
                    return 0
            if self.dry_run:
                self.log(f"DRY RUN: would dispatch {wf} {inputs} now (nothing sent, no state written)")
                return 0
            self.write_marker(f"{wf} {key}")                                   # 6: refresh, then send
            self.trace.append("dispatch")
            sent_call = True
            ok, said = self.gh.dispatch(wf, inputs)
            fresh = self.load_state()                  # keep what read_listings wrote (seen_max)
            state = state if "_unreadable" in fresh else {**state, **{k: v for k, v in fresh.items() if k != "slots"}}
            slots = state.setdefault("slots", {})
            rec = slots.setdefault(key, {"sent": 0})
            listed = (len(self.listed_dispatches(wf, g.parse_slot_tag(inputs["slot"], wf), runs))
                      if "slot" in inputs else 0)
            rec["sent"] = max(self._sent(state, key), listed) + 1          # L-a: heal a reset count too
            rec["last_utc"] = _iso(self.now())
            rec["gh_ok"] = ok
            state["last_dispatch_utc"] = _iso(self.now())
            self.save_state(state)
            if not ok:
                self.log(f"DISPATCH CALL FAILED for {wf} {inputs}: {said}. Not sent again until a listing shows "
                         f"what happened; the cron is the fallback")
                return 1
            self.log(f"DISPATCHED {wf} {inputs} (gh returned 0; both listings showed no unfinished run)")
            self._wait_listed(wf, inputs)
            return 0
        finally:
            if not sent_call:
                self.remove_marker()
            cs.release()

    def _wait_listed(self, wf, inputs):
        want = f"slot {inputs['slot']}" if "slot" in inputs else None
        t_end = time.monotonic() + LIST_WAIT_S
        while time.monotonic() < t_end:
            self.sleep(LIST_POLL_S)
            try:
                rows = self.gh.runs(wf)
            except Exception:  # noqa: BLE001
                continue
            if want is None or any((r.get("displayTitle") or "") == want for r in rows):
                self.log(f"listed: {wf} {inputs}")
                return True
        self.log(f"NOT LISTED after {LIST_WAIT_S} s: {wf} {inputs} (the marker still blocks the desktop for 10 min "
                 f"from the dispatch; the slot's cron stays uncredited)")
        return False

    # -- rule M
    def manual(self, wf: str, inputs: dict, wait_min: float) -> int:
        t_end = time.monotonic() + wait_min * 60
        while True:
            self.trace = []
            stop = self.stopped()
            marker = g.read_marker(self.paths["marker"], self.now())
            young = marker is not None and self.now() - marker < g.MARKER_BLOCK
            if not stop and not young:
                runs, sched, _n, why_not = self.read_listings()
                busy = None if why_not else self.unfinished(runs, sched)
                if not why_not and not busy and not self._lock_present():
                    rc = self._dispatch(wf, inputs, f"manual {wf} {inputs}", self.load_state())
                    if "dispatch" in self.trace or self.dry_run:
                        return rc
            if time.monotonic() > t_end:
                self.log(f"manual {wf} {inputs}: gave up after {wait_min:g} min (group busy, lock, marker or STOP)")
                return 3
            self.sleep(60)

    # -- the loop
    def loop(self, looks: int = 0, sleep_s: int = 60) -> int:
        mx = NamedMutex(self.mutex_name)
        try:
            got = mx.acquire()
        except OSError as e:
            self.log(f"the loop mutex {self.mutex_name} could not be created ({e}); this copy leaves")
            return 1
        if not got:
            self.log(f"another copy holds the loop mutex {self.mutex_name}; this copy leaves")
            return 0
        lk = FileLock(self.paths["loop"])
        if not lk.acquire():
            mx.release()
            self.log("a dispatch loop is already running for this root; this copy leaves")
            return 0
        try:
            self.log(f"loop started (pid {os.getpid()})")
            n = 0
            while True:
                try:
                    self.look()
                except Exception as e:  # noqa: BLE001
                    self.log(f"a look ended with an error: {type(e).__name__}: {e}")
                try:
                    self.heartbeat()
                except OSError:
                    pass
                n += 1
                if looks and n >= looks:
                    self.log(f"loop ended after {n} look(s)")
                    return 0
                self.sleep(sleep_s)
        finally:
            lk.release()
            mx.release()


# ------------------------------------------------------------------ CLI with the hf starter's guards
def _refuse(msg: str) -> int:
    print(f"econ_dispatch: {msg} Nothing was done.", file=sys.stderr)
    return 2


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
                                 allow_abbrev=False)
    ap.add_argument("command", choices=["loop", "look", "manual"], nargs="?", default="loop")
    ap.add_argument("--once", action="store_true", help="one look (same as the `look` command)")
    ap.add_argument("--dry-run", action="store_true", help="say what would be dispatched; send nothing")
    ap.add_argument("--root", default=None)
    ap.add_argument("--gh", default=None)
    ap.add_argument("--now", default=None, help="test only: the clock (ISO UTC)")
    ap.add_argument("--looks", type=int, default=None, help="test only: end the loop after N looks")
    ap.add_argument("--sleep", type=int, default=None, help="test only: seconds between looks")
    ap.add_argument("--workflow", choices=sorted(g.WRITERS), help="manual: the writer")
    ap.add_argument("--source", default="", help="manual: one source")
    ap.add_argument("--force", action="store_true", help="manual: force")
    ap.add_argument("--manual-dry-run", action="store_true", help="manual: the workflow's own dry_run input (daily)")
    ap.add_argument("--wait-min", type=float, default=360.0, help="manual: give up after this long")
    a = ap.parse_args(argv)
    once = a.once or a.command == "look"
    if a.root is not None and not os.path.isdir(a.root):
        return _refuse(f"--root {a.root!r} is not a folder that is there.")
    if a.gh is not None and (not a.gh or '"' in a.gh or " -" in a.gh):
        return _refuse(f"--gh {a.gh!r} is not the name of a program.")
    if a.gh is None:
        test_only = [n for n in ("now", "looks", "sleep") if getattr(a, n) is not None]
        if a.root is not None and not (once and a.dry_run):
            test_only.append("root (but with --once --dry-run)")
        if a.dry_run and not once:
            test_only.append("dry-run without --once")
        if test_only:
            return _refuse(f"without --gh the REAL gh is used, and this is for the test only: --{', --'.join(test_only)}.")
        # L-b: with the real gh, refuse to start unless the marker and the lock are the desktop pass's own files
        problems = root_problems(a.root or REAL_ROOT)
        if problems:
            lines = "\n  - ".join(problems)
            if not a.dry_run:
                return _refuse(f"this is not the checkout the desktop pass runs from, so its marker and lock would "
                               f"guard nothing:\n  - {lines}\n")
            print(f"econ_dispatch: WARNING (dry run, nothing is sent): not the desktop pass's checkout:\n  - {lines}",
                  file=sys.stderr)
    now = None
    if a.now:
        try:
            fixed = g._parse(a.now)
        except ValueError:
            return _refuse(f"--now {a.now!r} is not a time.")
        if not once and a.command != "manual" and not a.looks:
            return _refuse("--now without --once needs --looks.")
        now = (lambda: fixed)
    d = Dispatcher(a.root or REAL_ROOT, Gh(a.gh or REAL_GH), now=now, dry_run=a.dry_run, echo=once or a.dry_run)
    if a.command == "manual":
        if not a.workflow:
            return _refuse("manual needs --workflow.")
        inputs = {}
        if a.source:
            inputs["source"] = a.source
        if a.force:
            inputs["force"] = "true"
        if a.manual_dry_run:
            if a.workflow != "updater-daily.yml":
                return _refuse("--manual-dry-run exists only for updater-daily.yml.")
            inputs["dry_run"] = "true"
        return d.manual(a.workflow, inputs, a.wait_min)
    if once:
        return d.look()
    return d.loop(looks=a.looks or 0, sleep_s=60 if a.sleep is None else a.sleep)


if __name__ == "__main__":
    sys.exit(main())
