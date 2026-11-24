"""jobs/_istat_pacer on Windows: the real byte-range lock, real subprocesses, a real kill (DESIGN 12 TB1-TB6,
verdict V1-V3 and TB7).

CI (ubuntu) cannot run any of this: there is no msvcrt. The workstation runs it and its log is the receipt
(verdict V13: commit sha, Python version, "N passed, 0 skipped", and the M12/M13 kill results). Every test uses
a folder under pytest's basetemp, never the machine's E: folder (DESIGN 6.8).

Scaled time (the children's constants are scaled by gap / 16): gap 0.26 s, closed window 1.0 s (16/60), so
4 x gap = 1.04 > 1.0 and no closed window may hold more than 4 starts. The slip bound is 0.025 s (+5 ms between
acquire() returning and the child's own clock read), so 4 x gap - slip > window still holds. The lock wait is
0.25 s (production 5 s): long against a lock held for milliseconds, short enough to keep the suite quick;
TB1 and TB2, which slow every locked read by 10 ms, use 1.0 s (BUSY_LW).

The bound is asserted on ACTUAL starts (the child's clock right before its fake send), never on reserved slots
(V2), and each start must lie in [slot, slot + slip]."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jobs import _istat_pacer as pacer                                  # noqa: E402

pytestmark = [
    pytest.mark.skipif(sys.platform != "win32", reason="Windows byte-range locks (msvcrt): the workstation runs "
                       "these and its log is the receipt (verdict V13); CI has no msvcrt"),
    pytest.mark.istat_pacer_real,
]

GAP, WINDOW, LW, SLIP, EPS = 0.26, 1.0, 0.25, 0.025, 0.005
BUSY_LW = 1.0                      # TB1/TB2: the lock wait under a slowed read (see TB1's docstring)
FS = 4 * GAP
CHILD = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_istat_pacer_child.py")
CHECKER = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tools", "check_istat_rate.py")
T0 = 1_800_000_000.0


class FakeClock:
    def __init__(self, t=T0):
        self.t = float(t)
        self.sleeps = []

    def time(self):
        return self.t

    def sleep(self, s):
        self.sleeps.append(s)
        self.t += max(0.0, s)


@pytest.fixture
def state(tmp_path, monkeypatch):
    """The real module on a tmp folder (conftest already redirected STATE_DIR; this makes it this test's)."""
    d = tmp_path / "pacer"
    monkeypatch.setattr(pacer, "STATE_DIR", str(d))
    monkeypatch.delenv("ISTAT_QPM", raising=False)
    return d


def _raw_record(d):
    with open(os.path.join(str(d), pacer.LOCK_NAME), "rb") as fh:
        return fh.read()


def _record(d):
    return pacer.parse_record(_raw_record(d)[pacer.RECORD_OFFSET:])


def _put_record(d, payload: bytes):
    os.makedirs(str(d), exist_ok=True)
    with open(os.path.join(str(d), pacer.LOCK_NAME), "wb") as fh:
        fh.write(pacer.HEADER + payload)


def _seed(d, **kw):
    """A valid record whose next slot is in the past (a missing one means "a request was just sent")."""
    rec = pacer.Record(next=kw.pop("next", T0 - 100.0), seq=kw.pop("seq", 1), **kw)
    _put_record(d, pacer.format_record(rec))


def _log_lines(d, kind=None):
    out = []
    for name in sorted(os.listdir(str(d))):
        if name.startswith("requests_"):
            with open(os.path.join(str(d), name), encoding="ascii") as fh:
                out += [ln.rstrip("\n") for ln in fh if ln.strip()]
    return [ln for ln in out if kind is None or ln.startswith(kind + " ")]


# ─────────────────────────────── in-process: the lock mechanics (fake clock) ───────────────────────────────
def test_tokens_are_shared_exactly_gap_apart_and_error_free(state, capsys):
    """TA1 in shared mode, and V3: normal tokens report mode=shared with ZERO pacer errors. The mutant "no seek
    before unlock" makes every unlock fail (msvcrt.locking counts from the current position) and fails here."""
    _seed(state)
    clock = FakeClock()
    toks = [pacer.acquire("updater", "esploradati.istat.it", "http", "data", flow="F", clock=clock)
            for _ in range(20)]
    slots = [t.slot for t in toks]
    assert [t.mode for t in toks] == ["shared"] * 20
    assert slots[0] == T0
    assert all(abs((b - a) - pacer.GAP_S) < 1e-6 for a, b in zip(slots, slots[1:]))
    assert max(sum(1 for s in slots if a <= s <= a + 60.0) for a in slots) == 4
    st = pacer.stats()
    assert st["errors"] == 0 and st["unlock_errors"] == 0 and st["log_errors"] == 0, st
    assert "ERROR" not in capsys.readouterr().out
    rec = _record(state)
    assert rec.next == slots[-1] + pacer.GAP_S and rec.upd == slots[-1]
    assert len(_log_lines(state, "R")) == 20
    # the lock is free for another handle right after each token (it was really released)
    import msvcrt
    fd = os.open(os.path.join(str(state), pacer.LOCK_NAME), os.O_RDWR | os.O_BINARY)
    try:
        msvcrt.locking(fd, msvcrt.LK_NBLCK, pacer.LOCK_REGION)
        os.lseek(fd, 0, 0)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, pacer.LOCK_REGION)
    finally:
        os.close(fd)


class LockProbeClock(FakeClock):
    """While the pacer waits for its slot, try the lock from ANOTHER handle: it must be free (M13)."""

    def __init__(self, lock_path):
        super().__init__()
        self.lock_path = lock_path
        self.probes = []

    def sleep(self, s):
        import msvcrt
        fd = os.open(self.lock_path, os.O_RDWR | os.O_BINARY)
        try:
            msvcrt.locking(fd, msvcrt.LK_NBLCK, pacer.LOCK_REGION)
            os.lseek(fd, 0, 0)
            msvcrt.locking(fd, msvcrt.LK_UNLCK, pacer.LOCK_REGION)
            self.probes.append(True)
        except OSError:
            self.probes.append(False)
        finally:
            os.close(fd)
        super().sleep(s)


def test_the_lock_is_never_held_while_waiting_for_the_slot(state):
    """M13, deterministically: the lock is held only for the read-modify-write, never across the sleep."""
    _seed(state, next=T0 + 30.0)
    clock = LockProbeClock(os.path.join(str(state), pacer.LOCK_NAME))
    tok = pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=clock)
    assert tok.slot == T0 + 30.0 and clock.probes and all(clock.probes), clock.probes


def test_log_append_failure_after_the_commit_sends_once_at_the_committed_slot(state, monkeypatch, capsys):
    """V3: the record write is the commit point. A request-log failure AFTER it is logged, and the token uses
    its committed slot exactly once: no second (fail-safe) token, no 64-s wait, seq advanced by one."""
    clock = FakeClock()
    first = pacer.acquire("updater", "sdmx.istat.it", "http", "data", clock=clock)
    seq0 = _record(state).seq
    real_append = pacer._append_lines

    def broken_append(lines):
        if any(text.startswith("R ") for _, text in lines):
            raise OSError("injected: disk full")
        return real_append(lines)
    monkeypatch.setattr(pacer, "_append_lines", broken_append)
    capsys.readouterr()
    tok = pacer.acquire("updater", "sdmx.istat.it", "http", "data", clock=clock)
    out = capsys.readouterr().out
    rec = _record(state)
    assert tok.mode == "shared"
    assert tok.slot == first.slot + pacer.GAP_S == rec.upd
    assert rec.seq == seq0 + 1                       # exactly one reservation
    assert tok.waited <= pacer.GAP_S + 1e-6          # no fail-safe wait
    assert "ERROR request log append failed" in out
    assert "[istat-pacer] UNLOGGED R " in out and f"tok={tok.id}" in out
    assert "FAIL-SAFE" not in out and "FAILSAFE-TOKEN" not in out
    assert pacer.stats()["failsafe"] == 0 and pacer.stats()["log_errors"] == 1


def test_a_failure_before_the_commit_is_failsafe_and_never_starts_at_once(state, monkeypatch, capsys):
    """V1(a): a token whose record write fails waits FAILSAFE_GAP_S, tries the lock again, and only then
    sends as fail-safe; nothing was committed. The record is VALID (seeded), so the 64-s floor on the first
    shared slot afterwards is the pacer's own rule, not the "missing record means just sent" rule."""
    _seed(state)
    clock = FakeClock()

    def broken_write(fd, rec):
        raise OSError("injected: write failed")
    monkeypatch.setattr(pacer, "_write_record", broken_write)
    tok = pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=clock)
    out = capsys.readouterr().out
    assert tok.mode == "failsafe"
    assert tok.start >= T0 + pacer.FAILSAFE_GAP_S
    assert "FAIL-SAFE:" in out and "FAILSAFE-TOKEN R " in out
    monkeypatch.undo()
    monkeypatch.setattr(pacer, "STATE_DIR", str(state))
    nxt = pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=clock)
    # the first shared slot after a fail-safe token waits the FAIL-SAFE gap (the record never saw that token)
    assert nxt.mode == "shared" and nxt.slot >= tok.start + pacer.FAILSAFE_GAP_S
    assert "shared mode restored" in capsys.readouterr().out


class OvershootClock(FakeClock):
    """A process woken late (below-normal priority, a stalled machine): the first long sleep overshoots."""

    def __init__(self, overshoot):
        super().__init__()
        self.overshoot = overshoot

    def sleep(self, s):
        if self.overshoot and s > 1.0:
            s, self.overshoot = s + self.overshoot, 0.0
        super().sleep(s)


def test_a_late_wake_abandons_the_token_and_takes_a_new_one(state, capsys):
    """V2(b): a token is valid only in [slot, slot + SLIP_MAX_S]. A process that wakes 10 s late does not
    send: it logs an X line and takes a NEW token, which is the one it returns."""
    _seed(state, next=T0 + 30.0)
    clock = OvershootClock(10.0)
    tok = pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=clock)
    out = capsys.readouterr().out
    import re
    m = re.search(r"abandoned: woke (\d+\.\d+) s after its slot", out)
    assert m and float(m.group(1)) > 9.0, out           # the overshoot lands on the D3 pre-check sleep (-0.5 s)
    assert pacer.stats()["abandoned"] == 1 and tok.slot >= T0 + 39.0
    assert tok.start - tok.slot <= pacer.SLIP_MAX_S
    x = _log_lines(state, "X")
    assert len(x) == 1 and "reason=late slip=9." in x[0]
    assert len(_log_lines(state, "R")) == 2


def test_the_outcome_line_carries_the_actual_start(state):
    """V2(a): the receipt is on the time the request was REALLY sent, read just before it."""
    _seed(state)
    clock = FakeClock()
    tok = pacer.acquire("updater", "esploradati.istat.it", "http", "data", flow="22_289", clock=clock)
    clock.t += 1.25                                    # work between acquire() and the send
    t_send = pacer.started(tok, clock=clock)
    pacer.outcome(tok, "200", 0.4)
    o = _log_lines(state, "O")
    assert len(o) == 1
    assert f"start={t_send:.3f}" in o[0] and "slip=1.250" in o[0] and "outcome=200" in o[0]


# ───────────── the coordinator's D1b, D2, D3, D6, D7 (review_A/PHASE1.md sections 5 and 7) ─────────────
class MonoClock(FakeClock):
    """Wall time AND a system-wide monotonic clock (as time.monotonic is on this machine). step() moves the
    WALL clock only; step_on_sleep makes the first long sleep carry a wall step."""

    def __init__(self, step_on_sleep=0.0):
        super().__init__()
        self.m = 1000.0
        self.step_on_sleep = step_on_sleep

    def monotonic(self):
        return self.m

    def step(self, dt):
        self.t += dt

    def sleep(self, s):
        self.sleeps.append(s)
        s = max(0.0, s)
        self.t += s
        self.m += s
        if self.step_on_sleep and s > 1.0:
            self.t += self.step_on_sleep
            self.step_on_sleep = 0.0


def test_d2_a_forward_wall_step_does_not_shorten_the_gap(state):
    """D2: a FORWARD wall step of 1 h right after a start. With the wall clock alone the next token starts at
    once (true time 0 s later); the monotonic copy of next keeps it one gap later in TRUE time."""
    _seed(state)
    clock = MonoClock()
    pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=clock)
    m_first = clock.m
    clock.step(3600.0)
    pacer.acquire("crawler", "esploradati.istat.it", "http", "data", clock=clock)
    assert clock.m - m_first >= pacer.GAP_S - 1e-6, clock.m - m_first
    rec = _record(state)
    assert rec.mw > 0 and rec.mnext > rec.mw                             # the copy is written


def test_d2_a_backward_wall_step_waits_at_most_the_failsafe_gap(state):
    """D2: a BACKWARD wall step of 1 h makes the stored wall next look 1 h ahead; the clamp bounds the wait
    to 64 s of true time, and the monotonic copy keeps it at least one gap."""
    _seed(state)
    clock = MonoClock()
    pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=clock)
    m_first = clock.m
    clock.step(-3600.0)
    pacer.acquire("crawler", "esploradati.istat.it", "http", "data", clock=clock)
    assert pacer.GAP_S - 1e-6 <= clock.m - m_first <= pacer.FAILSAFE_GAP_S + 1e-6, clock.m - m_first


@pytest.mark.parametrize("mnext,mw", [(50_000.0, 49_990.0),            # another boot, far away
                                      (1_050.0, 1_040.0)])              # another boot, INSIDE the 133-s distance
def test_d2_a_monotonic_copy_from_another_boot_never_blocks(state, mnext, mw):
    """A copy written in another boot (its mw is later than this boot's monotonic now) is ignored, also when
    its distance would look plausible (50 s here): only the mw check can tell that one apart."""
    _put_record(state, pacer.format_record(pacer.Record(next=T0 - 100.0, seq=1, mnext=mnext, mw=mw)))
    clock = MonoClock()                                                  # monotonic 1000 < mw
    tok = pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=clock)
    assert tok.slot == T0 and clock.m == 1000.0


def test_d6_a_backward_wall_step_during_the_sleep_adds_nothing(state):
    """D6: the slot wait is by duration on the monotonic clock. A 1-h backward wall step during a 30-s wait
    must not add an hour (a 'sleep until the wall clock reaches the slot' loop does)."""
    _seed(state, next=T0 + 30.0)
    clock = MonoClock(step_on_sleep=-3600.0)
    tok = pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=clock)
    assert tok.slot == T0 + 30.0
    assert 30.0 - 1e-6 <= clock.m - 1000.0 <= 30.0 + 1e-6, clock.m - 1000.0
    assert tok.mslip is not None and abs(tok.mslip) < 1e-6


class LateAcrossStep(MonoClock):
    """The first long sleep carries a 1-h BACKWARD wall step AND wakes 10 s late in true time."""

    def sleep(self, s):
        if self.step_on_sleep and s > 1.0:
            s += 10.0
        super().sleep(s)


def test_d6_a_late_wake_across_a_backward_step_is_still_late(state, capsys):
    """The slip check is on the monotonic clock: on the wall clock a 10-s-late wake after a 1-h backward
    step reads as 3,590 s EARLY, and the token would be sent late."""
    _seed(state, next=T0 + 30.0)
    clock = LateAcrossStep(step_on_sleep=-3600.0)
    tok = pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=clock)
    assert pacer.stats()["abandoned"] == 1, capsys.readouterr().out
    assert tok.mslip is not None and tok.mslip <= pacer.SLIP_MAX_S


class StallAfterRead(MonoClock):
    """A thread stall of `stall_s` right after the k-th clock read (wall or monotonic) of one acquire: both
    clocks run on while the thread does nothing (review_A condition 1, the oracle's P3 shape)."""

    def __init__(self, stall_at, stall_s=30.0):
        super().__init__()
        self.n, self.stall_at, self.stall_s = 0, stall_at, stall_s

    def _tick(self):
        self.n += 1
        if self.n == self.stall_at:
            self.t += self.stall_s
            self.m += self.stall_s

    def time(self):
        v = self.t
        self._tick()
        return v

    def monotonic(self):
        v = self.m
        self._tick()
        return v


@pytest.mark.parametrize("stall_s", [30.0, 5.0, 1.5])
def test_condition1_a_stall_after_any_clock_read_never_returns_a_late_token(state, stall_s):
    """V2(b), review_A condition 1: wherever a stall falls among the clock reads of one acquire, the token it
    returns was inside [slot, slot + SLIP_MAX_S] at its last read, on the wall AND the monotonic clock, and
    never EARLY. A stall between the wall and the monotonic read under the lock made only the monotonic slot
    late; the wall half of the slip check is what refuses that token.

    The stalls SHORTER than the 30-s wait (5 s, and 1.5 s, which is under SLIP_MAX_S) pin the READ ORDER
    (re-review condition 1, ledger R2153): read monotonic first and wall second, and such a stall puts the
    monotonic slot BEFORE the wall slot, so the token starts 5 s / 1.5 s early. A 30-s stall cannot show it:
    it is as long as the wait, so the slot becomes `now`. (A stall AFTER the last read is the caller's
    residual, NOTES item 27: no pacer can see it.)"""
    _seed(state, next=T0 + 30.0)
    probe = StallAfterRead(stall_at=0)
    pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=probe)
    n_reads = probe.n
    assert n_reads >= 4
    for k in range(1, n_reads + 1):
        pacer._reset_process_state()
        _seed(state, next=T0 + 30.0)
        clock = StallAfterRead(stall_at=k, stall_s=stall_s)
        tok = pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=clock)
        assert tok.start - tok.slot <= pacer.SLIP_MAX_S + 1e-9, (k, n_reads, tok.start - tok.slot)
        assert tok.mslip is not None and tok.mslip <= pacer.SLIP_MAX_S + 1e-9, (k, tok.mslip)
        assert tok.start >= tok.slot - 1e-9, (k, "early")


def test_condition2_own_last_after_a_backward_wall_step(state):
    """review_A condition 2: the SAME process after a 1-h backward wall step waits at most the fail-safe gap of
    true time (its own last start, 1 h 'in the future', is clamped to now), and at least one gap."""
    _seed(state)
    clock = MonoClock()
    pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=clock)
    m0 = clock.m
    clock.step(-3600.0)
    pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=clock)
    assert pacer.GAP_S - 1e-6 <= clock.m - m0 <= pacer.FAILSAFE_GAP_S + 1e-6, clock.m - m0


def test_condition3_a_pen_beyond_the_cap_is_written_back_and_does_not_slide(state):
    """review_A condition 3: a crc-valid pen far beyond the 48-h cap. A crawler (which takes no slot while it
    waits) writes the clamped pen back, so its token comes 48 h after it first looked, not never."""
    _put_record(state, pacer.format_record(pacer.Record(next=T0 - 100.0, pen=9_999_999_999.0, seq=1)))
    clock = FakeClock()
    tok = pacer.acquire("crawler", "esploradati.istat.it", "http", "data", deadline_ts=T0 + 72 * 3600.0,
                        clock=clock)
    assert tok.slot <= T0 + pacer.PENALTY_MAX_S + 1e-6, (tok.slot - T0) / 3600.0


class OtherProcessPenalty(FakeClock):
    """ANOTHER process records a 429 (writes pen into the shared record) while this one waits for its slot.
    This process's own _LOCAL_PEN never hears of it."""

    def __init__(self, state_dir):
        super().__init__()
        self.state_dir = state_dir
        self.fired = False

    def sleep(self, s):
        if not self.fired and s > 1.0:
            self.fired = True
            rec = _record(self.state_dir)
            _put_record(self.state_dir, pacer.format_record(rec._replace(pen=self.t + 300.0)))
        super().sleep(s)


def test_d3_a_penalty_set_by_another_process_holds_a_reserved_slot(state, capsys):
    """D3: a slot reserved before ANOTHER process's 429 is not sent into the penalty: the pre-check before the
    last 0.5 s of the wait drops it, and the new token starts after the penalty."""
    _seed(state, next=T0 + 30.0)
    clock = OtherProcessPenalty(state)
    tok = pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=clock)
    assert clock.fired and pacer._LOCAL_PEN == 0.0
    assert tok.slot >= T0 + 300.0, tok
    assert any("reason=penalty" in x for x in _log_lines(state, "X"))
    assert "covers its slot" in capsys.readouterr().out


def test_d1b_a_crawler_behind_residue_writes_the_clamp_back(state):
    """D1b: a low-priority reader that cannot take a slot writes the clamped record back, so it is not held
    in 64-s steps until the wall clock reaches a far-future residue."""
    _seed(state, next=T0 + 36000.0)
    clock = FakeClock()
    tok = pacer.acquire("crawler", "esploradati.istat.it", "http", "data", clock=clock)
    assert tok.slot == T0 + pacer.FAILSAFE_GAP_S, tok.slot - T0


@pytest.mark.parametrize("payload", [b"", b"ekd-istat-pacer/1 torn\n"])
@pytest.mark.parametrize("writer", ["begin_turn", "end_turn", "penalize"])
def test_d7_side_writers_keep_empty_or_torn_as_just_sent(state, payload, writer):
    """D7 / G8: begin_turn, end_turn and penalize rebuild an empty or torn record as 'a request was just
    sent' (next >= now + 64 s), never as a free record (next = 0)."""
    _put_record(state, payload)
    clock = FakeClock()
    if writer == "begin_turn":
        assert pacer.begin_turn(T0 + 600.0, clock=clock)
    elif writer == "end_turn":
        assert pacer.end_turn(clock=clock)
    else:
        pacer.penalize(30, "crawler", clock=clock)
    assert _record(state).next >= T0 + pacer.FAILSAFE_GAP_S
    tok = pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=clock)
    assert tok.slot >= T0 + pacer.FAILSAFE_GAP_S


def test_window_end_leaves_the_record_unchanged(state):
    """TA7 (M16): a slot later than deadline - 1 s raises WindowEnd and writes NOTHING."""
    clock = FakeClock()
    _put_record(state, pacer.format_record(pacer.Record(next=T0 + 30.0, upd=T0, seq=5)))
    before = _raw_record(state)
    n_lines = len(_log_lines(state))
    with pytest.raises(pacer.WindowEnd):
        pacer.acquire("updater", "esploradati.istat.it", "http", "data", deadline_ts=T0 + 20.0, clock=clock)
    assert _raw_record(state) == before
    assert len(_log_lines(state)) == n_lines
    tok = pacer.acquire("updater", "esploradati.istat.it", "http", "data", deadline_ts=T0 + 31.0 + 0.5,
                        clock=clock)
    assert tok.slot == T0 + 30.0


@pytest.mark.parametrize("payload", [b"", b"\0" * 40, b"ekd-istat-pacer/1 next=123 garbage\n",
                                     b"ekd-istat-pacer/0 next=0000000000.000 crc=00000000\n", "badcrc"])
def test_bad_records_read_as_just_sent_and_are_repaired(state, payload):
    """TA4 on the real file (M10): empty, torn, bad crc and unknown OLD version mean "a request was just sent":
    the slot is now + 64 s, never now, and a valid record is written."""
    if payload == "badcrc":
        good = pacer.format_record(pacer.Record(next=T0 - 100.0, seq=3))
        payload = good.replace(b"next=1799999900", b"next=1799999800")
    _put_record(state, payload)
    clock = FakeClock()
    tok = pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=clock)
    assert tok.slot == T0 + pacer.FAILSAFE_GAP_S and tok.mode == "shared"
    assert _record(state).next == tok.slot + pacer.GAP_S


def test_a_newer_record_version_is_never_overwritten(state, capsys):
    """An unknown NEWER version is not overwritten: this process goes fail-safe (>= now + 64 s)."""
    newer = b"ekd-istat-pacer/2 next=0000000000.000 whatever crc=00000000\n"
    _put_record(state, newer)
    clock = FakeClock()
    tok = pacer.acquire("crawler", "esploradati.istat.it", "http", "data", clock=clock)
    assert tok.mode == "failsafe" and tok.start >= T0 + pacer.FAILSAFE_GAP_S
    assert _raw_record(state) == pacer.HEADER + newer
    assert "NEWER pacer" in capsys.readouterr().out


def test_a_future_next_is_clamped_on_the_real_file(state):
    """TA5 (M11): next = now + 10 h waits at most 64 s, never 10 h and never 0."""
    _put_record(state, pacer.format_record(pacer.Record(next=T0 + 36000.0, seq=1)))
    clock = FakeClock()
    tok = pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=clock)
    assert tok.slot == T0 + pacer.N_MAX * pacer.GAP_S


def test_a_penalty_is_shared_by_every_participant(state):
    """TA6 / 6.5 on the real file: penalize(30) gives 60 s for EVERYONE; penalize(1e9) gives 48 h (V6)."""
    _seed(state)
    clock = FakeClock()
    until = pacer.penalize(30, "crawler", clock=clock)
    assert until == T0 + 60.0 and _record(state).pen == T0 + 60.0
    assert pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=clock).slot == T0 + 60.0
    c = pacer.acquire("crawler", "esploradati.istat.it", "http", "data", clock=clock)
    assert c.slot >= T0 + 60.0 + pacer.GAP_S
    big = pacer.penalize(10 ** 9, "updater", clock=clock)
    assert big == clock.t + 48 * 3600.0 and _record(state).pen == big
    assert len(_log_lines(state, "P")) == 2


def test_turn_marker_gives_priority_and_yield_waits_for_it(state):
    """TA3 on the real file, and the marker functions: while the marker is live the crawler takes at most one
    token per 61 s and the updater never waits more than one gap; yield_to_turn waits for the marker's end,
    logs at the start and every 30 min, and end_turn frees it."""
    _seed(state)
    clock = FakeClock()
    assert pacer.begin_turn(T0 + 3600.0, clock=clock) is True
    assert _record(state).turn == T0 + 3600.0
    crawl = [pacer.acquire("crawler", "esploradati.istat.it", "http", "data", clock=clock) for _ in range(3)]
    gaps = [b.slot - a.slot for a, b in zip(crawl, crawl[1:])]
    assert all(g >= pacer.LOW_TURN_GAP_S - 1e-6 for g in gaps), gaps
    u = pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=clock)
    assert u.waited <= pacer.GAP_S + 1e-6
    lines = []
    waited = pacer.yield_to_turn(lines.append, clock=clock)
    assert abs(clock.t - (T0 + 3600.0)) < 1e-6 and waited > 0
    assert sum("yielding to the istat updater turn until" in x for x in lines) == 2     # start + 30 min
    assert pacer.end_turn(clock=clock) is True
    assert pacer.yield_to_turn(lines.append, clock=clock) == 0.0
    assert pacer.begin_turn(clock.t + 10 * 3600.0, clock=clock) and _record(state).turn == clock.t + 6 * 3600.0
    assert pacer.begin_turn(None, clock=clock) and _record(state).turn == clock.t + 1800.0
    clock.t += 1200.0
    pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=clock)
    assert _record(state).turn >= clock.t + 1800.0 - pacer.GAP_S                    # renewed by the token
    assert len(_log_lines(state, "T")) == 4                                         # 3 begins and 1 end


def test_describe_names_the_folder_and_the_mode(state, tmp_path, monkeypatch):
    d = pacer.describe()
    assert d.startswith(f"[istat-pacer] state {os.path.join(str(state), pacer.LOCK_NAME)}; mode shared;")
    assert d.endswith("gap 16 s; budget 4/min")
    blocker = tmp_path / "a_file"
    blocker.write_text("x")
    monkeypatch.setattr(pacer, "STATE_DIR", str(blocker / "pacer"))
    assert "mode FAIL-SAFE (state folder unusable" in pacer.describe()


def test_the_production_folder_is_refused_under_pytest_and_never_created(tmp_path, monkeypatch, capsys):
    """6.8: a test that forgot the redirect gets the fail-safe and nothing is touched. "Production" here is a
    tmp stand-in, so even a broken refusal cannot reach E:."""
    fake_prod = tmp_path / "prod" / "istat_pacer"
    monkeypatch.setattr(pacer, "PRODUCTION_STATE_DIR", str(fake_prod))
    monkeypatch.setattr(pacer, "STATE_DIR", str(fake_prod))
    clock = FakeClock()
    tok = pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=clock)
    assert tok.mode == "failsafe" and "refused under pytest" in tok.reason
    assert not (tmp_path / "prod").exists()
    assert any(str(fake_prod) in p for p in pacer._OPENED_PATHS)        # the attempt is recorded for the guard
    del pacer._OPENED_PATHS[:]          # a stand-in, not the machine's folder: keep the teardown guard quiet


def test_a_busy_lock_gives_the_failsafe_after_the_bounded_wait(state, monkeypatch, capsys):
    """TB4 in-process: another handle holds the lock longer than LOCK_WAIT_MAX_S (scaled to 0.2 s real): this
    token waits 64 s (fake), retries, still finds it busy, and sends as fail-safe; once released, shared."""
    import msvcrt
    monkeypatch.setattr(pacer, "LOCK_WAIT_MAX_S", 0.2)
    monkeypatch.setattr(pacer, "LOCK_POLL_S", 0.01)
    clock = FakeClock()
    pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=clock)
    fd = os.open(os.path.join(str(state), pacer.LOCK_NAME), os.O_RDWR | os.O_BINARY)
    msvcrt.locking(fd, msvcrt.LK_NBLCK, pacer.LOCK_REGION)
    try:
        t_ask = clock.t
        tok = pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=clock)
    finally:
        os.lseek(fd, 0, 0)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, pacer.LOCK_REGION)
        os.close(fd)
    out = capsys.readouterr().out
    assert tok.mode == "failsafe" and tok.start >= t_ask + pacer.FAILSAFE_GAP_S
    assert "FAIL-SAFE: lock busy for more than 0.2 s" in out
    nxt = pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=clock)
    assert nxt.mode == "shared" and nxt.slot >= tok.start + pacer.FAILSAFE_GAP_S


def test_a_penalty_that_could_not_be_shared_still_holds_and_is_shared_later(state, monkeypatch, capsys):
    """Review finding 1: penalize() whose lock attempt fails must still hold this process in SHARED mode, and
    its next good lock writes the penalty for everyone."""
    _seed(state)
    clock = FakeClock()
    real_open = pacer._open_and_lock

    def busy():
        raise pacer._PacerFailure("injected: lock busy")
    monkeypatch.setattr(pacer, "_open_and_lock", busy)
    until = pacer.penalize(3600, "updater", clock=clock)
    assert "penalty NOT shared yet" in capsys.readouterr().out
    monkeypatch.setattr(pacer, "_open_and_lock", real_open)
    tok = pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=clock)
    assert tok.mode == "shared" and tok.slot >= until == T0 + 3600.0
    assert _record(state).pen >= until                               # now shared


def test_a_torn_record_keeps_this_process_penalty(state):
    _seed(state)
    clock = FakeClock()
    until = pacer.penalize(7200, "crawler", clock=clock)
    _put_record(state, b"torn")
    tok = pacer.acquire("crawler", "esploradati.istat.it", "http", "data", clock=clock)
    assert tok.start >= until


class PenaltyDuringSleep(FakeClock):
    """A 429 seen by this process while its reserved token sleeps, and its lock attempt FAILS: the penalty
    is in this process only (not in the shared record), so the D3 pre-check of the record cannot see it and
    the wake check of the local penalty is what holds the token."""

    def __init__(self):
        super().__init__()
        self.fired = False

    def sleep(self, s):
        if not self.fired and s > 1.0:
            self.fired = True
            real = pacer._open_and_lock

            def busy(wait=None):
                raise pacer._PacerFailure("injected: lock busy")
            pacer._open_and_lock = busy
            try:
                pacer.penalize(600, "updater", clock=self)
            finally:
                pacer._open_and_lock = real
        super().sleep(s)


def test_a_penalty_after_the_reservation_abandons_it(state, capsys):
    """Review finding 4: a slot reserved before a 429 this process saw (but could not share) is not sent
    into the penalty."""
    _seed(state, next=T0 + 30.0)
    clock = PenaltyDuringSleep()
    tok = pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=clock)
    assert clock.fired and tok.slot >= T0 + 600.0
    assert pacer.stats()["abandoned"] == 1
    assert any("reason=penalty" in x for x in _log_lines(state, "X"))


# ─────────────────────────────── real subprocesses (real clock, scaled) ───────────────────────────────
def _env():
    env = dict(os.environ)
    env.pop("ISTAT_QPM", None)
    return env


def _cmd(state_dir, out, participant="updater", tokens=10, **kw):
    cmd = [sys.executable, "-B", CHILD, "--state-dir", str(state_dir), "--participant", participant,
           "--gap", str(GAP), "--lock-wait", str(kw.pop("lock_wait", LW)), "--slip-max", str(SLIP),
           "--tokens", str(tokens), "--out", str(out)]
    for k, v in kw.items():
        cmd += ["--" + k.replace("_", "-"), str(v)]
    return cmd


def _run(cmds, timeout=180):
    procs = [subprocess.Popen(c, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=_env())
             for c in cmds]
    outs = []
    try:
        for p in procs:
            outs.append(p.communicate(timeout=timeout)[0])
    finally:
        for p in procs:
            if p.poll() is None:
                p.kill()
    for p, o in zip(procs, outs):
        assert p.returncode == 0, o
        _child_real_sleep_ok(o)
    return outs


# V7's 2-s real-sleep guard (tests/conftest.py) counts the PARENT process only (review_A condition 7). These
# children sleep for real BY DESIGN (scaled real-time runs, skipped on Linux CI), so each one reports what it
# slept inside the pacer and the parent bounds it: a runaway child wait fails the test instead of passing.
CHILD_REAL_SLEEP_MAX_S = 90.0


def _child_real_sleep_ok(out):
    lines = [ln for ln in (out or "").splitlines() if ln.startswith("REAL_SLEPT ")]
    assert lines, "a child did not report REAL_SLEPT (it must, see tests/_istat_pacer_child.py)"
    slept = float(lines[-1].split()[1])
    assert slept <= CHILD_REAL_SLEEP_MAX_S, f"a child slept {slept:.1f} s inside the pacer (> {CHILD_REAL_SLEEP_MAX_S:g})"


def _rows(paths):
    rows = []
    for p in paths:
        if os.path.exists(str(p)):
            with open(str(p), encoding="ascii") as fh:
                rows += [json.loads(x) for x in fh if x.strip()]
    return rows


def _max_in_window(starts, window=WINDOW):
    s = sorted(starts)
    best, j = 0, 0
    for i in range(len(s)):
        j = max(j, i)
        while j + 1 < len(s) and s[j + 1] <= s[i] + window:
            j += 1
        best = max(best, j - i + 1)
    return best


def _assert_bound(rows, require_shared=True):
    """The V2 bound: every ACTUAL start in [slot, slot + slip], merged shared slots >= gap apart, and at most
    4 actual starts in any closed window."""
    assert rows, "no token was taken"
    late = [r for r in rows if not (r["slot"] - 1e-6 <= r["start"] <= r["slot"] + SLIP + EPS)]
    assert not late, f"{len(late)} start(s) outside [slot, slot + {SLIP + EPS}]: {late[:3]}"
    if require_shared:
        fs = [(r["part"], r["reason"]) for r in rows if r["mode"] != "shared"]
        assert not fs, f"{len(fs)} token(s) not in shared mode: {fs[:3]}"
    shared = sorted(r["slot"] for r in rows if r["mode"] == "shared")
    close = [(a, b) for a, b in zip(shared, shared[1:]) if b - a < GAP - 1e-6]
    assert not close, f"{len(close)} pair(s) of reserved slots closer than the gap: {close[:3]}"
    mx = _max_in_window([r["start"] for r in rows])
    assert mx <= 4, f"{mx} actual starts in one closed {WINDOW} s window"
    return mx


def _unexpected_errors(logs):
    """Pacer ERROR lines other than the DESIGNED fallback: an outcome() that cannot take the lock within its
    wait prints `ERROR outcome not logged` and then the line as `[istat-pacer] UNLOGGED O ...` (re-review
    condition 3: TB2 hit it 1 run in 4 under load). Every other ERROR still fails the test."""
    return [ln for o in logs for ln in o.splitlines()
            if "ERROR" in ln and "ERROR outcome not logged" not in ln]


def _process_logs(tmp_path, logs):
    """Each child's output as a file, for the checker's --process-log (it counts UNLOGGED lines)."""
    extra = []
    for i, o in enumerate(logs):
        p = tmp_path / f"child_{i}.out"
        p.write_text(o, encoding="utf-8")
        extra += ["--process-log", str(p)]
    return extra


def _checker(log_dir, rows, window=WINDOW, extra=()):
    lo, hi = min(r["start"] for r in rows) - 1, max(r["start"] for r in rows) + 1
    logs = [os.path.join(str(log_dir), n) for n in os.listdir(str(log_dir)) if n.startswith("requests_")]
    assert logs, f"no request log in {log_dir}"   # without --log the checker would read the MACHINE's folder
    cmd = [sys.executable, "-B", CHECKER, "--from", str(lo), "--to", str(hi), "--window", str(window),
           "--slip-max", str(SLIP + EPS)] + [x for p in logs for x in ("--log", p)] + list(extra)
    r = subprocess.run(cmd, capture_output=True, text=True, env=_env(), timeout=120)
    return r.returncode, r.stdout


def test_tb1_two_processes_updater_and_crawler(tmp_path):
    """TB1 (M12; V2, V3): updater + crawler, 60 tokens each, simulated request time 0-0.5 s, a 10-ms slow read
    inside the lock. The bound holds on actual starts, every token is shared, and the checker agrees. The lock
    wait is 1.0 s here (BUSY_LW): polling a lock is not fair, and with a slowed read and the D3 pre-check a
    0.25-s wait starved a waiter now and then (a flake, not a breach: the fail-safe is safe). M13 has its own
    deterministic test (the lock probe)."""
    d = tmp_path / "pacer"
    outs = [tmp_path / "u.jsonl", tmp_path / "c.jsonl"]
    logs = _run([_cmd(d, outs[0], "updater", 60, work_min=0.0, work_max=0.5, rmw_delay=0.01, seed=1,
                      lock_wait=BUSY_LW),
                 _cmd(d, outs[1], "crawler", 60, work_min=0.0, work_max=0.1, rmw_delay=0.01, seed=2,
                      lock_wait=BUSY_LW)])
    rows = _rows(outs)
    assert len(rows) == 120
    mx = _assert_bound(rows)
    assert {r["part"] for r in rows} == {"updater", "crawler"}
    assert not _unexpected_errors(logs), _unexpected_errors(logs)
    rc, out = _checker(d, rows, extra=_process_logs(tmp_path, logs))
    assert rc == 0, out
    assert f"overall: {mx}" in out


def test_tb2_three_processes_updater_crawler_tool(tmp_path):
    """TB2 (M12): updater + crawler + tool. The crawler and the tool are both low priority and wake at the SAME
    retry time (the record's next), so a read-modify-write without the lock gives two tokens at one instant.
    Lock wait 1.0 s, as TB1."""
    d = tmp_path / "pacer"
    outs = [tmp_path / f"{n}.jsonl" for n in ("u", "c", "t")]
    logs = _run([_cmd(d, outs[0], "updater", 30, work_min=0.2, work_max=0.6, rmw_delay=0.01, seed=3,
                      lock_wait=BUSY_LW),
                 _cmd(d, outs[1], "crawler", 30, rmw_delay=0.01, seed=4, lock_wait=BUSY_LW),
                 _cmd(d, outs[2], "tool:catalog", 30, rmw_delay=0.01, seed=5, lock_wait=BUSY_LW)])
    rows = _rows(outs)
    assert len(rows) == 90
    _assert_bound(rows)
    assert not _unexpected_errors(logs), _unexpected_errors(logs)
    rc, out = _checker(d, rows, extra=_process_logs(tmp_path, logs))
    assert rc == 0, out


def test_tb3_a_holder_killed_inside_the_lock(tmp_path):
    """TB3: a child takes the lock and blocks; the survivor starts; the holder is killed (TerminateProcess).
    During the incident the survivor never starts faster than the fail-safe rate (its first start is at least
    lock-wait + 64 s-scaled after it asked, V1) and it returns to shared mode after the kill, its first shared
    slot no earlier than its last fail-safe token + 64 s-scaled. The record is seeded VALID (next in the past),
    so that floor is the pacer's rule and not the bad-record rule."""
    d = tmp_path / "pacer"
    _seed(d, next=time.time() - 100.0)
    holder = subprocess.Popen(_cmd(d, tmp_path / "h.jsonl", hold_lock=60), stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, text=True, env=_env())
    try:
        assert holder.stdout.readline().strip() == "LOCKED"
        survivor = subprocess.Popen(_cmd(d, tmp_path / "s.jsonl", "updater", 6), stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, text=True, env=_env())
        deadline = time.time() + 30.0                 # kill once the survivor has sent 2 fail-safe tokens
        while len(_rows([tmp_path / "s.jsonl"])) < 2 and time.time() < deadline and survivor.poll() is None:
            time.sleep(0.05)
        holder.kill()
        holder.wait(10)
        out = survivor.communicate(timeout=60)[0]
        assert survivor.returncode == 0, out
    finally:
        for p in (holder,):
            if p.poll() is None:
                p.kill()
    assert holder.returncode != 0                     # it was killed, it did not exit by itself
    rows = sorted(_rows([tmp_path / "s.jsonl"]), key=lambda r: r["start"])
    fs = [r for r in rows if r["mode"] == "failsafe"]
    assert len(fs) >= 2, rows
    assert fs[0]["start"] - fs[0]["t_begin"] >= LW + FS - EPS
    assert all(b["start"] - a["start"] >= FS - EPS for a, b in zip(fs, fs[1:]))
    after = [r for r in rows if r["start"] > fs[-1]["start"]]
    assert after and {r["mode"] for r in after} == {"shared"}
    assert after[0]["start"] - fs[-1]["start"] >= FS - EPS           # back to shared only after the fail-safe gap
    assert "FAIL-SAFE: lock busy" in out and out.count("FAILSAFE-TOKEN R ") == len(fs)
    _assert_bound(rows, require_shared=False)


def test_tb4_lock_busy_longer_than_the_wait(tmp_path):
    """TB4 (M13): the lock is held far past the 0.25-s wait, until the other process has sent its first token
    (so start-up time under load cannot shorten the incident). The other process logs the FAIL-SAFE line, sends
    no earlier than lock-wait + 64 s-scaled after it asked, then returns to shared mode."""
    d = tmp_path / "pacer"
    release = tmp_path / "release"
    holder = subprocess.Popen(_cmd(d, tmp_path / "h.jsonl", hold_lock=30.0, release_on=release),
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=_env())
    try:
        assert holder.stdout.readline().strip() == "LOCKED"
        other = subprocess.Popen(_cmd(d, tmp_path / "o.jsonl", "updater", 4), stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT, text=True, env=_env())
        deadline = time.time() + 30.0
        while not _rows([tmp_path / "o.jsonl"]) and time.time() < deadline and other.poll() is None:
            time.sleep(0.05)
        release.write_text("x")
        out = other.communicate(timeout=60)[0]
        assert other.returncode == 0, out
        holder.wait(10)
    finally:
        for p in (holder,):
            if p.poll() is None:
                p.kill()
    rows = sorted(_rows([tmp_path / "o.jsonl"]), key=lambda r: r["start"])
    assert rows[0]["mode"] == "failsafe"
    assert rows[0]["start"] - rows[0]["t_begin"] >= LW + FS - EPS
    assert rows[-1]["mode"] == "shared"
    assert "FAIL-SAFE: lock busy for more than 0.25 s" in out
    _assert_bound(rows, require_shared=False)


def test_tb5_both_processes_in_failsafe(tmp_path):
    """TB5 (M8, M9; V2): an unusable folder (its parent is a FILE) puts both processes in fail-safe. Each paces
    alone at one start per 64 s-scaled, never at once after the failure, and the merged bound holds."""
    blocker = tmp_path / "not_a_folder"
    blocker.write_text("x")
    d = blocker / "pacer"
    outs = [tmp_path / "a.jsonl", tmp_path / "b.jsonl"]
    logs = _run([_cmd(d, outs[0], "updater", 4), _cmd(d, outs[1], "crawler", 4)])
    rows = _rows(outs)
    assert len(rows) == 8 and {r["mode"] for r in rows} == {"failsafe"}
    for part in ("updater", "crawler"):
        mine = sorted((r for r in rows if r["part"] == part), key=lambda r: r["start"])
        assert mine[0]["start"] - mine[0]["t_begin"] >= FS - EPS
        assert all(b["start"] - a["start"] >= FS - EPS for a, b in zip(mine, mine[1:]))
    _assert_bound(rows, require_shared=False)
    assert all("FAIL-SAFE: state folder unusable" in o for o in logs)


def test_tb6_negative_control_different_folders_break_the_bound(tmp_path):
    """TB6: two children on DIFFERENT folders do not see each other, so the merged starts break the bound.
    Proves the bound check and the checker can fail."""
    start_at = time.time() + 1.5
    outs = [tmp_path / "a.jsonl", tmp_path / "b.jsonl"]
    _run([_cmd(tmp_path / "pa", outs[0], "updater", 12, start_at=start_at),
          _cmd(tmp_path / "pb", outs[1], "updater", 12, start_at=start_at)])
    rows = _rows(outs)
    assert _max_in_window([r["start"] for r in rows]) >= 6
    with pytest.raises(AssertionError):
        _assert_bound(rows)
    lo, hi = min(r["start"] for r in rows) - 1, max(r["start"] for r in rows) + 1
    logs = [str(p / n) for p in (tmp_path / "pa", tmp_path / "pb") for n in os.listdir(str(p))
            if n.startswith("requests_")]
    cmd = [sys.executable, "-B", CHECKER, "--from", str(lo), "--to", str(hi), "--window", str(WINDOW),
           "--slip-max", str(SLIP + EPS)] + [x for p in logs for x in ("--log", p)]
    r = subprocess.run(cmd, capture_output=True, text=True, env=_env(), timeout=120)
    assert r.returncode == 1, r.stdout


def test_tb7_a_stalled_holder_and_a_timed_out_third_process(tmp_path):
    """TB7 (verdict V1(c)): two shared updaters saturate the budget; one stalls INSIDE the lock for 3 x the
    lock wait; a third process (crawler) asks during the stall and times out. With the V1 rule it waits
    64 s-scaled and retries the lock, so the merged ACTUAL starts stay <= 4 in every closed window. The mutant
    "fail-safe start = now" sends at once and fails here."""
    d = tmp_path / "pacer"
    flag = tmp_path / "stalling"
    outs = [tmp_path / "a.jsonl", tmp_path / "b.jsonl", tmp_path / "c.jsonl"]
    start_at = time.time() + 1.5
    logs = _run([_cmd(d, outs[0], "updater", 16, stall_on=6, stall_s=3 * LW, stall_flag=flag, start_at=start_at),
                 _cmd(d, outs[1], "updater", 16, start_at=start_at),
                 _cmd(d, outs[2], "crawler", 3, wait_for=flag)])
    rows = _rows(outs)
    assert "STALL " in logs[0]
    c = sorted((r for r in rows if r["part"] == "crawler"), key=lambda r: r["start"])
    assert c[0]["start"] - c[0]["t_begin"] >= LW + FS - EPS, c[0]      # V1: never at once after the timeout
    _assert_bound(rows, require_shared=False)
