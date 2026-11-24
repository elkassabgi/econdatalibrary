"""Child process for tests/test_istat_pacer_windows.py: one REAL participant of jobs/_istat_pacer.

Not a test file (pytest collects only test_*.py). It takes the state folder, the participant and a SCALED gap
as ARGUMENTS: that is a test seam, not a production knob - production code never passes a gap. Every pacer
time constant is scaled by gap / 16 (so the 16/60 ratio of gap to window holds: gap 0.26 s, window 1.0 s).

It sends nothing anywhere. A "send" is the clock read through pacer.started() right before the place a real
request would go; it is written, with the token's slot and mode, as one JSON line per token to --out.

Modes:
  default       take --tokens tokens as --participant, "working" --work-min..--work-max s after each send
  --hold-lock S take the raw pacer lock, print LOCKED, hold it S s (or until killed, or until the file named
                by --release-on exists), release, exit
Seams for the tests (all scaled, all optional):
  --rmw-delay S      sleep S inside every locked read (a slow disk), so a missing lock shows as a race
  --stall-on N --stall-s S --stall-flag F   on the N-th record write, write F and hold the lock S more seconds
  --wait-for F       start the first acquire only once file F exists
  --start-at T       start the first acquire at epoch T (synchronised start)
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.environ.pop("ISTAT_QPM", None)          # a workstation ISTAT_QPM=4 would give the crawler a 15-s gap here

from jobs import _istat_pacer as pacer     # noqa: E402


def scale(gap: float, lock_wait: float, slip_max: float) -> None:
    k = gap / 16.0
    pacer.GAP_S = gap
    pacer.FAILSAFE_GAP_S = pacer.N_MAX * gap
    pacer.LOW_TURN_GAP_S = 61.0 * k
    pacer.PENALTY_MIN_S = 60.0 * k
    pacer.FUTURE_SLACK_S = 5.0 * k
    pacer.WAIT_LOG_EVERY_S = 1800.0 * k
    pacer.FAILSAFE_BANNER_EVERY_S = 1800.0 * k
    pacer.YIELD_POLL_S = 60.0 * k
    pacer.LOCK_WAIT_MAX_S = lock_wait
    pacer.LOCK_POLL_S = 0.002
    pacer.SLIP_MAX_S = slip_max


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--state-dir", required=True)
    ap.add_argument("--participant", default="updater")
    ap.add_argument("--gap", type=float, required=True)
    ap.add_argument("--lock-wait", type=float, required=True)
    ap.add_argument("--slip-max", type=float, required=True)
    ap.add_argument("--tokens", type=int, default=10)
    ap.add_argument("--out", default=None)
    ap.add_argument("--work-min", type=float, default=0.0)
    ap.add_argument("--work-max", type=float, default=0.0)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--hold-lock", type=float, default=None)
    ap.add_argument("--release-on", default=None)
    ap.add_argument("--rmw-delay", type=float, default=0.0)
    ap.add_argument("--stall-on", type=int, default=0)
    ap.add_argument("--stall-s", type=float, default=0.0)
    ap.add_argument("--stall-flag", default=None)
    ap.add_argument("--wait-for", default=None)
    ap.add_argument("--start-at", type=float, default=None)
    a = ap.parse_args()

    pacer.STATE_DIR = a.state_dir
    scale(a.gap, a.lock_wait, a.slip_max)

    if a.hold_lock is not None:
        fd = pacer._open_and_lock()
        print("LOCKED", flush=True)
        t_end = time.time() + a.hold_lock
        while time.time() < t_end and not (a.release_on and os.path.exists(a.release_on)):
            time.sleep(0.01)
        pacer._unlock_close(fd)
        print("RELEASED", flush=True)
        return 0

    if a.rmw_delay > 0:
        real_read = pacer._read_record

        def slow_read(fd):
            data = real_read(fd)
            time.sleep(a.rmw_delay)
            return data
        pacer._read_record = slow_read

    if a.stall_on > 0:
        real_write = pacer._write_record
        writes = [0]

        def stalling_write(fd, rec):
            real_write(fd, rec)
            writes[0] += 1
            if writes[0] == a.stall_on:
                if a.stall_flag:
                    with open(a.stall_flag, "w") as fh:
                        fh.write(repr(time.time()))
                print(f"STALL {time.time():.6f} holding the lock {a.stall_s} s", flush=True)
                time.sleep(a.stall_s)
        pacer._write_record = stalling_write

    if a.wait_for:
        while not os.path.exists(a.wait_for):
            time.sleep(0.001)
    if a.start_at is not None:
        while time.time() < a.start_at:
            time.sleep(min(0.01, max(0.0, a.start_at - time.time())))

    rnd = random.Random(a.seed)
    out = open(a.out, "a", encoding="ascii") if a.out else None
    t_begin = time.time()
    print(f"BEGIN {t_begin:.6f}", flush=True)
    for i in range(a.tokens):
        tok = pacer.acquire(a.participant, "esploradati.istat.it", "http", "data", flow=f"F{i}")
        t_send = pacer.started(tok)  # the fake send starts here
        row = {"pid": os.getpid(), "part": a.participant, "id": tok.id, "slot": tok.slot, "start": t_send,
               "mode": tok.mode, "waited": tok.waited, "reason": tok.reason, "t_begin": t_begin, "i": i}
        if out:
            out.write(json.dumps(row) + "\n")
            out.flush()
        work = rnd.uniform(a.work_min, a.work_max) if a.work_max > 0 else 0.0
        if work > 0:
            time.sleep(work)
        pacer.outcome(tok, "200", work)
    if out:
        out.close()
    print("STATS " + json.dumps(pacer.stats()), flush=True)
    # V7 covers ONE process; the parent test reads this line to bound what a child slept inside the pacer
    print(f"REAL_SLEPT {pacer._REAL_SLEPT_S:.3f}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
