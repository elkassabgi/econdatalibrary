r"""Find processes by a LITERAL command-line token, excluding this process, every shell above it,
and every shell WRAPPER (the harness runs each tool call as `bash.exe -c "<whole command>"`, so
a token used in ANY concurrent command of the session sits on a live sibling shell's command
line). The only process instrument to use on this machine.

Why this exists (R562, R564, R574, R576 - four self/sibling matches in one day): `pgrep`/`ps -W`
under Git Bash cannot see command lines at all; a psutil loop that excluded only os.getpid()
killed its parent shell; and one that excluded ancestry killed SEVEN sibling shells of other
tool calls, with exit 0. Matching is therefore restricted to WORKER processes (python, node,
wrangler, ...) and never to shells, `nohup`, `timeout` or the harness snapshot wrappers.

    from tools.proc_match import find
    find("core.derive_csv")                   # -> [Proc(pid, name, cmdline, create_time), ...]
    python tools/proc_match.py core.derive_csv                 # list
    python tools/proc_match.py core.derive_csv --kill --expect 1   # kill exactly one, verified

Rules encoded here: (1) exclude os.getpid(), psutil.Process().parents(), and every shell-like
wrapper; (2) match a literal token, never a regex; (3) a kill requires --expect N and refuses
when the live match count differs; (4) `killed` is claimed only after wait(); (5) exit code is
non-zero when any kill was refused or failed; (6) a kill is a separate invocation from a launch.
"""
from __future__ import annotations

import argparse
import dataclasses
import os
import sys
import time

import psutil

# Wrappers whose command line is someone else's command text, never a worker of their own.
_WRAPPER_STEMS = ("bash", "sh", "dash", "zsh", "cmd", "powershell", "pwsh", "nohup", "timeout",
                  "conhost", "mintty", "winpty", "winpty-agent", "env", "xargs", "tee")
WRAPPER_NAMES = set(_WRAPPER_STEMS) | {s + ".exe" for s in _WRAPPER_STEMS}
# The harness's per-call shell snapshot: any process carrying it is a session wrapper.
WRAPPER_MARKERS = ("shell-snapshots/snapshot-", "shell-snapshots\\snapshot-")


@dataclasses.dataclass
class Proc:
    pid: int
    name: str
    cmdline: str
    create_time: float

    @property
    def age_s(self) -> float:
        return time.time() - self.create_time


def ancestry_pids() -> set:
    """This process and every ancestor: the shells that carry our own command line."""
    out = {os.getpid()}
    try:
        for p in psutil.Process().parents():
            out.add(p.pid)
    except psutil.Error:
        pass
    return out


def is_wrapper(name: str, cmdline: str) -> bool:
    n = (name or "").lower()
    return n in WRAPPER_NAMES or any(m in cmdline for m in WRAPPER_MARKERS)


def find(token: str, exclude: set | None = None, include_wrappers: bool = False) -> list:
    """WORKER processes whose command line contains the literal `token`, minus our ancestry and
    minus every shell wrapper (unless include_wrappers, which is for listing only)."""
    if not token or len(token) < 4:
        raise ValueError("token must be a literal of at least 4 characters")
    skip = ancestry_pids() | (exclude or set())
    out = []
    for p in psutil.process_iter(["pid", "name", "cmdline", "create_time"]):
        if p.info["pid"] in skip:
            continue
        try:
            cl = " ".join(p.info["cmdline"] or [])
        except (psutil.Error, TypeError):
            continue
        if token not in cl:
            continue
        if not include_wrappers and is_wrapper(p.info["name"] or "", cl):
            continue
        out.append(Proc(p.info["pid"], p.info["name"] or "", cl, p.info["create_time"] or 0.0))
    return out


def kill_verified(proc: Proc, wait_s: float = 15.0) -> tuple:
    """(ok, message). Kill `proc` only if the pid still belongs to the SAME process (create_time
    and command line unchanged), is not a wrapper, and is not in our ancestry; then WAIT for it
    to die before claiming it did."""
    try:
        p = psutil.Process(proc.pid)
        if abs(p.create_time() - proc.create_time) > 1.0:
            return False, f"NOT killed {proc.pid}: pid recycled (create_time differs)"
        cl = " ".join(p.cmdline() or [])
        if cl != proc.cmdline:
            return False, f"NOT killed {proc.pid}: command line changed"
        if is_wrapper(p.name(), cl) or is_wrapper(proc.name, proc.cmdline):
            return False, f"NOT killed {proc.pid}: it is a shell wrapper ({p.name()} / {proc.name})"
        if proc.pid in ancestry_pids():
            return False, f"NOT killed {proc.pid}: it is this process or an ancestor"
        p.kill()
        try:
            p.wait(timeout=wait_s)
        except psutil.TimeoutExpired:
            return False, f"kill sent to {proc.pid} but it is still alive after {wait_s:g}s"
        return True, f"killed {proc.pid} (waited)"
    except psutil.NoSuchProcess:
        return False, f"NOT killed {proc.pid}: already gone"
    except psutil.Error as e:
        return False, f"could not kill {proc.pid}: {e}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("token")
    ap.add_argument("--kill", action="store_true")
    ap.add_argument("--expect", type=int, default=None,
                    help="with --kill: refuse unless exactly this many WORKER processes match now")
    ap.add_argument("--show-wrappers", action="store_true", help="list shell wrappers too (never killed)")
    a = ap.parse_args()
    found = find(a.token, include_wrappers=a.show_wrappers)
    for f in found:
        tag = "WRAPPER " if is_wrapper(f.name, f.cmdline) else ""
        print(f"{f.pid}\t{f.age_s/60:.1f} min\t{tag}{f.name}\t{f.cmdline[:150]}")
    workers = [f for f in found if not is_wrapper(f.name, f.cmdline)]
    if a.expect is not None and len(workers) != a.expect:
        # checked BEFORE the not-found path: 'expected 2, found 0' is a refusal, and
        # '--expect 0' with 0 matches is the natural "nothing is running" assertion (exit 0).
        print(f"REFUSING: {len(workers)} worker process(es) match now, --expect {a.expect}")
        return 2
    if a.kill and a.expect is None:
        print("REFUSING --kill without --expect N (R576: a kill on an unmeasured population)")
        return 2
    if not found:
        print(f"no worker process carries {a.token!r} (own ancestry {sorted(ancestry_pids())} and "
              f"shell wrappers excluded)")
        return 0 if a.expect == 0 else 1
    if not a.kill:
        return 0
    rc = 0
    for f in workers:
        ok, msg = kill_verified(f)
        print(msg)
        if not ok:
            rc = 3
    return rc


if __name__ == "__main__":
    sys.exit(main())
