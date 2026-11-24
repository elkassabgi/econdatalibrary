"""jobs/_istat_pacer on Windows: the real byte-range lock, real subprocesses, a real kill (DESIGN 12 TB1-TB6,
verdict V1-V3 and TB7).

CI (ubuntu) cannot run any of this: there is no msvcrt. The workstation runs it and its log is the receipt
(verdict V13: commit sha, Python version, "N passed, 0 skipped", and the M12/M13 kill results). Every test uses
a folder under pytest's basetemp, never the machine's E: folder (DESIGN 6.8).

Scaled time (the children's constants are scaled by gap / 16): gap 0.26 s, closed window 1.0 s (16/60), so
4 x gap = 1.04 > 1.0 and no closed window may hold more than 4 starts. The slip bound is 0.025 s (+5 ms between
acquire() returning and the child's own clock read), so 4 x gap - slip > window still holds. The lock wait is
0.25 s (production 5 s): long against a lock held for milliseconds, short enough to keep the suite quick.

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
    assert "abandoned: woke 10.000 s after its slot" in out
    assert pacer.stats()["abandoned"] == 1 and tok.slot >= T0 + 40.0
    assert tok.start - tok.slot <= pacer.SLIP_MAX_S
    x = _log_lines(state, "X")
    assert len(x) == 1 and "reason=late slip=10.000" in x[0]
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
    """A 429 seen by this process while its reserved token sleeps."""

    def __init__(self):
        super().__init__()
        self.fired = False

    def sleep(self, s):
        if not self.fired and s > 1.0:
            self.fired = True
            pacer.penalize(600, "updater", clock=self)
        super().sleep(s)


def test_a_penalty_after_the_reservation_abandons_it(state, capsys):
    """Review finding 4: a slot reserved before a 429 is not sent into the penalty."""
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
    return outs


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
        assert {r["mode"] for r in rows} == {"shared"}, sorted({r["mode"] for r in rows})
    shared = sorted(r["slot"] for r in rows if r["mode"] == "shared")
    close = [(a, b) for a, b in zip(shared, shared[1:]) if b - a < GAP - 1e-6]
    assert not close, f"{len(close)} pair(s) of reserved slots closer than the gap: {close[:3]}"
    mx = _max_in_window([r["start"] for r in rows])
    assert mx <= 4, f"{mx} actual starts in one closed {WINDOW} s window"
    return mx


def _checker(log_dir, rows, window=WINDOW, extra=()):
    lo, hi = min(r["start"] for r in rows) - 1, max(r["start"] for r in rows) + 1
    logs = [os.path.join(str(log_dir), n) for n in os.listdir(str(log_dir)) if n.startswith("requests_")]
    assert logs, f"no request log in {log_dir}"   # without --log the checker would read the MACHINE's folder
    cmd = [sys.executable, "-B", CHECKER, "--from", str(lo), "--to", str(hi), "--window", str(window),
           "--slip-max", str(SLIP + EPS)] + [x for p in logs for x in ("--log", p)] + list(extra)
    r = subprocess.run(cmd, capture_output=True, text=True, env=_env(), timeout=120)
    return r.returncode, r.stdout


def test_tb1_two_processes_updater_and_crawler(tmp_path):
    """TB1 (M12, M13; V2, V3): updater + crawler, 60 tokens each, simulated request time 0-0.5 s, a 10-ms slow
    read inside the lock. The bound holds on actual starts, every token is shared, and the checker agrees."""
    d = tmp_path / "pacer"
    outs = [tmp_path / "u.jsonl", tmp_path / "c.jsonl"]
    logs = _run([_cmd(d, outs[0], "updater", 60, work_min=0.0, work_max=0.5, rmw_delay=0.01, seed=1),
                 _cmd(d, outs[1], "crawler", 60, work_min=0.0, work_max=0.1, rmw_delay=0.01, seed=2)])
    rows = _rows(outs)
    assert len(rows) == 120
    mx = _assert_bound(rows)
    assert {r["part"] for r in rows} == {"updater", "crawler"}
    assert not any("ERROR" in o for o in logs), logs
    rc, out = _checker(d, rows)
    assert rc == 0, out
    assert f"overall: {mx}" in out


def test_tb2_three_processes_updater_crawler_tool(tmp_path):
    """TB2 (M12): updater + crawler + tool. The crawler and the tool are both low priority and wake at the SAME
    retry time (the record's next), so a read-modify-write without the lock gives two tokens at one instant."""
    d = tmp_path / "pacer"
    outs = [tmp_path / f"{n}.jsonl" for n in ("u", "c", "t")]
    logs = _run([_cmd(d, outs[0], "updater", 30, work_min=0.2, work_max=0.6, rmw_delay=0.01, seed=3),
                 _cmd(d, outs[1], "crawler", 30, rmw_delay=0.01, seed=4),
                 _cmd(d, outs[2], "tool:catalog", 30, rmw_delay=0.01, seed=5)])
    rows = _rows(outs)
    assert len(rows) == 90
    _assert_bound(rows)
    assert not any("ERROR" in o for o in logs), logs
    rc, out = _checker(d, rows)
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
    """TB4 (M13): the lock is held 3.0 s, far past the 0.25-s wait. The other process logs the FAIL-SAFE line,
    sends no earlier than lock-wait + 64 s-scaled after it asked, then returns to shared mode."""
    d = tmp_path / "pacer"
    holder = subprocess.Popen(_cmd(d, tmp_path / "h.jsonl", hold_lock=3.0), stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, text=True, env=_env())
    try:
        assert holder.stdout.readline().strip() == "LOCKED"
        out = _run([_cmd(d, tmp_path / "o.jsonl", "updater", 4)])[0]
        holder.wait(10)
    finally:
        if holder.poll() is None:
            holder.kill()
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
