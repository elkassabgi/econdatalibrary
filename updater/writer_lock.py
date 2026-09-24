"""A single-writer lock for one store region, held by a PROCESS (pid + its start time).

WHY (2026-09-23, statcan lane design review). statcan's cubes and served CSVs had nine possible
writers - the fetcher's merge, the orchestrator's CSV phase and retry drain, full runs of
tools/derive_statcan_tables.py, core.derive_csv --source statcan, tools/upload_statcan_store.py,
the bulk ingester, and more - and nothing stopped two of them at once. The continuous lane
(jobs/statcan_lane.py) now owns the region; it holds this lock for its whole run and every other
writer asks `refuse_if_held` before it writes.

A PID ALONE IS NOT AN OWNER (R600): Windows recycles pids, so a lock naming a dead lane's pid could
name an unrelated process an hour later. The lock records the owner's CREATION TIME too, and an
entry whose pid is gone or whose process started at another instant is stale.

FAILS CLOSED where it guards: when the owner cannot be judged (psutil missing or erroring, an
unreadable lock file) `owner()` reports the lock as HELD, so a writer that asks is refused rather
than let through. The cost is a refusal an operator clears by hand (delete the file after checking
the pid); the other direction is two writers on one store.
"""
from __future__ import annotations

import json
import os
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOCK_DIR = os.path.join(ROOT, "logs")

# Two processes' start times read through psutil agree to well under this (it is the same float,
# rounded by the OS); a recycled pid starts seconds to days later.
_START_TOLERANCE_S = 2.0


def lock_path(name: str) -> str:
    return os.path.join(LOCK_DIR, f"{name}.lock")


def _start_time(pid: int) -> float | None:
    """The process's creation time, None when there is no such process. Raises when it cannot
    tell (no psutil, access denied) - the caller decides which way that fails."""
    import psutil                                                    # noqa: PLC0415
    try:
        return float(psutil.Process(int(pid)).create_time())
    except psutil.NoSuchProcess:
        return None


def owner(name: str) -> dict | None:
    """The LIVE owner's record, or None when the lock is free or stale. Fails closed: an entry
    that cannot be judged is returned as live, with `unverified` set."""
    p = lock_path(name)
    try:
        with open(p, encoding="utf-8") as fh:
            rec = json.load(fh)
        pid, started = int(rec["pid"]), float(rec["started"])
    except FileNotFoundError:
        return None
    except Exception as e:                                           # noqa: BLE001
        return {"pid": None, "unverified": f"unreadable lock file {p}: {type(e).__name__}: {e}"}
    try:
        st = _start_time(pid)
    except Exception as e:                                           # noqa: BLE001
        return {**rec, "unverified": f"cannot inspect pid {pid}: {type(e).__name__}: {e}"}
    if st is None or abs(st - started) > _START_TOLERANCE_S:
        return None                                  # the owner is gone; the file is stale
    return rec


def acquire(name: str, what: str = "") -> bool:
    """Take the lock for THIS process. False when a live owner (or an unjudgeable one) holds it.
    A stale file is replaced. Atomic against a racing acquirer: O_EXCL on create, and a stale
    file is removed only after it was judged stale, then the create is retried once."""
    os.makedirs(LOCK_DIR, exist_ok=True)
    me = {"pid": os.getpid(), "started": _start_time(os.getpid()), "what": what,
          "since_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    body = json.dumps(me).encode("utf-8")
    try:
        fd = os.open(lock_path(name), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        if owner(name) is not None:
            return False
        # STALE: take it over by REPLACING, never delete-then-create - between a delete and a
        # create a second process that also judged it stale could delete the fresh lock the first
        # just made (lane review, advisory). Two racers may both replace; the last write wins, so
        # each re-reads after a pause and only the one named in the file proceeds.
        tmp = f"{lock_path(name)}.{os.getpid()}.tmp"
        with open(tmp, "wb") as fh:
            fh.write(body)
        try:
            os.replace(tmp, lock_path(name))
        except OSError:
            try:
                os.remove(tmp)
            except OSError:
                pass
            return False
        time.sleep(0.2)
        rec = owner(name)
        return rec is not None and rec.get("pid") == os.getpid()
    with os.fdopen(fd, "wb") as fh:
        fh.write(body)
    return True


def release(name: str) -> None:
    """Remove the lock if THIS process holds it (never another's)."""
    rec = owner(name)
    if rec is not None and rec.get("pid") == os.getpid():
        try:
            os.remove(lock_path(name))
        except FileNotFoundError:
            pass


def hold_or_refuse(name: str, what: str) -> None:
    """For every writer that is NOT the lane: TAKE the lock for the rest of this process, or exit
    loudly. Checking alone was one-way (lane review round 2, P6): a writer that passed the check
    wrote while holding nothing, and the lane - which releases between iterations - acquired in
    the gap and served over it. Released at exit (atexit); a hard kill leaves a file whose owner is
    gone, which the next acquirer judges stale."""
    import atexit                                                    # noqa: PLC0415
    if acquire(name, what=what):
        atexit.register(release, name)
        return
    refuse_if_held(name, what)
    # owner() found nobody alive, yet the takeover lost a race: refuse rather than write unlocked
    raise SystemExit(f"REFUSING {what}: could not take {lock_path(name)} (another writer took it first)")


def refuse_if_held(name: str, what: str) -> None:
    """For every OTHER writer: exit loudly when the region's owner is alive (or unjudgeable)."""
    rec = owner(name)
    if rec is None or rec.get("pid") == os.getpid():
        return
    raise SystemExit(
        f"REFUSING {what}: {lock_path(name)} is held by pid {rec.get('pid')} "
        f"({rec.get('what') or 'unnamed'}, since {rec.get('since_utc', '?')})"
        + (f" - and it cannot be judged: {rec['unverified']}" if rec.get("unverified") else "")
        + ". That process owns this store region; stop it first, or if it is gone and this "
          "message persists, delete the lock file after checking the pid.")
