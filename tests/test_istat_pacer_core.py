"""jobs/_istat_pacer: the pure core and every path that runs on ALL platforms (DESIGN 12 TA1-TA9; verdict V1,
V4, V6, V7; the 6.4/6.5 helpers; the 6.8 production-path guard).

THE LIMIT. ISTAT, https://www.istat.it/en/classifications-and-tools/sdmx-web-services/ (read 2026-11-24):
"a limit of 5 queries per minute has been set for each IP", a block of 1 to 2 days when exceeded. ondata
(https://ondata.github.io/guida-api-istat/) is the secondary source. The budget is 4 per minute as a 16-s gap.

On Linux CI there is no msvcrt, so acquire() is ALWAYS fail-safe there (one token per 64 s); these tests force
that path on Windows too (msvcrt = None) so both machines run the same assertions. The Windows lock itself is
tests/test_istat_pacer_windows.py. Time is a fake clock everywhere: no test sleeps for real (V7), and none
sends a request (127.0.0.1 only, for the urllib no-redirect opener)."""
from __future__ import annotations

import http.server
import os
import sys
import threading
import urllib.error

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jobs import _istat_pacer as pacer                                  # noqa: E402

T0 = 1_800_000_000.0
real = pytest.mark.istat_pacer_real


class FakeClock:
    def __init__(self, t=T0):
        self.t = float(t)
        self.sleeps = []

    def time(self):
        return self.t

    def sleep(self, s):
        self.sleeps.append(s)
        self.t += max(0.0, s)


def max_in_closed_window(starts, window=60.0):
    s = sorted(starts)
    best, j = 0, 0
    for i in range(len(s)):
        j = max(j, i)
        while j + 1 < len(s) and s[j + 1] <= s[i] + window:
            j += 1
        best = max(best, j - i + 1)
    return best


@pytest.fixture
def no_qpm(monkeypatch):
    monkeypatch.delenv("ISTAT_QPM", raising=False)


@pytest.fixture
def linux_like(monkeypatch, tmp_path, no_qpm):
    """The production path of Linux CI: no msvcrt, not WSL. The folder is a tmp one regardless."""
    monkeypatch.setattr(pacer, "msvcrt", None)
    monkeypatch.setattr(pacer, "_uname_release", lambda: "6.8.0-1017-azure")
    monkeypatch.setattr(pacer, "STATE_DIR", str(tmp_path / "pacer"))


def _rec(**kw):
    return pacer.Record(**kw)


# ───────────────────────────────────────── constants (6.1) ─────────────────────────────────────────
def test_constants_are_the_stated_rule():
    assert pacer.BUDGET_PER_MIN == 4
    assert pacer.GAP_S == 60.0 / pacer.BUDGET_PER_MIN + 1.0 == 16.0
    assert 4 * pacer.GAP_S > 60.0 and 3 * pacer.GAP_S < 60.0         # at most 4 starts per closed minute
    assert pacer.N_MAX == 4 and pacer.FAILSAFE_GAP_S == pacer.N_MAX * pacer.GAP_S == 64.0
    assert pacer.LOW_TURN_GAP_S == 61.0
    assert pacer.PENALTY_MIN_S == 60.0 and pacer.PENALTY_MAX_S == 48 * 3600.0        # V6
    assert pacer.TURN_MARKER_MAX_S == 6 * 3600.0
    assert pacer.LOCK_WAIT_MAX_S == 5.0
    assert pacer.SLIP_MAX_S < 4 * pacer.GAP_S - 60.0                                  # V2: < 4 s
    assert pacer.PRODUCTION_STATE_DIR == r"E:\research\econfindatalibrary\logs\istat_pacer"
    assert not pacer.PRODUCTION_STATE_DIR.startswith(("D:", "d:"))


@pytest.mark.parametrize("host,ok", [("esploradati.istat.it", True), ("sdmx.istat.it", True),
                                     ("www.istat.it", True), ("avvisi.istat.it", True), ("istat.it", True),
                                     ("https://esploradati.istat.it/SDMXWS/rest/data/X", True),
                                     ("ESPLORADATI.ISTAT.IT.", True), ("sdmx.istat.it:443", True),
                                     ("notistat.it", False), ("istat.it.example.com", False),
                                     ("127.0.0.1", False), ("pxweb.stat.si", False), ("", False)])
def test_istat_host(host, ok):
    assert pacer.is_istat_host(host) is ok
    assert pacer.ISTAT_HOST(host) is ok


# ───────────────────────────────────────── the record ─────────────────────────────────────────
def test_record_round_trip_is_fixed_width_and_fits_the_locked_region():
    a = pacer.format_record(_rec(next=T0 + 16.25, upd=T0, low=T0 - 3, turn=T0 + 600, pen=T0 + 60, seq=42))
    b = pacer.format_record(_rec(seq=1))
    assert len(a) == len(b) and a.endswith(b"\n") and a.startswith(b"ekd-istat-pacer/1 next=")
    assert pacer.RECORD_OFFSET + len(a) <= pacer.LOCK_REGION
    assert len(pacer.HEADER) == pacer.RECORD_OFFSET
    r = pacer.parse_record(a + b"trailing garbage after the line end is ignored")
    assert r == _rec(next=T0 + 16.25, upd=T0, low=T0 - 3, turn=T0 + 600, pen=T0 + 60, seq=42)
    assert r["next"] == r.next and r.get("seq") == 42


@pytest.mark.parametrize("raw,cls", [(None, pacer.RecordError), (b"", pacer.RecordError),
                                     (b"\0" * 200, pacer.RecordError),
                                     (b"ekd-istat-pacer/1 next=1 crc=00000000", pacer.RecordError),   # no \n
                                     (b"ekd-istat-pacer/0 next=1 crc=00000000\n", pacer.RecordError),
                                     (b"ekd-istat-pacer/2 next=1 crc=00000000\n", pacer.RecordNewer),
                                     (b"ekd-istat-pacer/9 anything at all\n", pacer.RecordNewer),
                                     (b"hello\n", pacer.RecordError)])
def test_bad_records_are_refused_with_their_reason(raw, cls):
    with pytest.raises(cls):
        pacer.parse_record(raw)


def test_a_flipped_digit_fails_the_crc():
    good = pacer.format_record(_rec(next=T0 + 16.0, seq=3))
    bad = good.replace(b"seq=000000000003", b"seq=000000000004")
    with pytest.raises(pacer.RecordError, match="crc"):
        pacer.parse_record(bad)


# ───────────────────────────────────────── TA1-TA8: the pure core ─────────────────────────────────────────
def test_ta1_updater_fifo_tokens_are_exactly_one_gap_apart(no_qpm):
    rec, now, slots = _rec(next=T0 - 1000.0), T0, []
    for _ in range(20):
        slot, retry_at, rec = pacer.next_slot(rec, now, "updater", slots[-1] if slots else 0.0, False)
        assert retry_at == slot
        slots.append(slot)
        now = slot                                     # asks again the moment the last one started
    assert slots[0] == T0
    assert all(abs((b - a) - pacer.GAP_S) < 1e-9 for a, b in zip(slots, slots[1:]))
    assert max_in_closed_window(slots) == 4
    assert rec.seq == 20 and rec.upd == slots[-1] and rec.next == slots[-1] + pacer.GAP_S


@pytest.mark.parametrize("part", ["crawler", "tool:catalog", "tool:probe"])
def test_ta2_low_priority_never_reserves_the_future(part, no_qpm):
    """M14: for every record and clock, a low-priority slot is None or exactly now."""
    for nxt in (T0 - 100, T0, T0 + 1, T0 + 15, T0 + 63):
        for pen in (0.0, T0 + 30):
            for turn in (0.0, T0 + 600):
                for low in (0.0, T0 - 30, T0 - 70):
                    rec = _rec(next=nxt, pen=pen, turn=turn, low=low, seq=1)
                    slot, retry_at, new = pacer.next_slot(rec, T0, part, 0.0)
                    assert slot is None or slot == T0, (nxt, pen, turn, low, slot)
                    if slot is None:
                        assert new is None and retry_at > T0
                    else:
                        assert new.next == T0 + pacer.GAP_S and new.low == T0


def test_ta2_an_updater_token_right_after_a_low_token_waits_at_most_one_gap(no_qpm):
    slot, _, rec = pacer.next_slot(_rec(next=T0 - 5), T0, "crawler", 0.0, False)
    assert slot == T0
    u, _, _ = pacer.next_slot(rec, T0 + 0.001, "updater", 0.0, False)
    assert 0 <= u - (T0 + 0.001) <= pacer.GAP_S


def _simulate(turn_live, minutes=60, updater_work=(20.0, 40.0)):
    """Discrete events on the pure core: an updater whose GETs take 20-40 s and a crawler that always wants a
    token. Each reservation is written at the moment it is made (as under the lock)."""
    import random
    rnd = random.Random(7)
    rec = _rec(next=T0 - 1.0, turn=(T0 + 10 * 3600.0) if turn_live else 0.0)
    ready = {"updater": T0, "crawler": T0 + 0.5}
    own = {"updater": 0.0, "crawler": 0.0}
    starts = {"updater": [], "crawler": []}
    waits = []
    end = T0 + minutes * 60.0
    while min(ready.values()) < end:
        who = min(ready, key=ready.get)
        now = ready[who]
        slot, retry_at, new = pacer.next_slot(rec, now, who, own[who])
        if slot is None:
            ready[who] = retry_at
            continue
        rec = new
        own[who] = slot
        starts[who].append(slot)
        if who == "updater":
            waits.append(slot - now)
            ready[who] = slot + rnd.uniform(*updater_work)
        else:
            ready[who] = slot
    return starts, waits


def test_ta3_a_live_marker_holds_the_crawler_to_one_per_61_s_and_the_updater_waits_one_gap(no_qpm):
    """M15: with the marker live the crawler takes at most one token per 61 s; the updater never waits more
    than one gap; the combined bound holds."""
    starts, waits = _simulate(turn_live=True)
    c = starts["crawler"]
    assert len(c) >= 10                                # it does get tokens (the updater's GETs leave gaps)
    assert all(b - a >= pacer.LOW_TURN_GAP_S - 1e-9 for a, b in zip(c, c[1:]))
    assert max(waits) <= pacer.GAP_S + 1e-9
    assert max_in_closed_window(starts["updater"] + c) <= 4


def test_ta3_control_without_the_marker_the_crawler_takes_more(no_qpm):
    """The negative control of TA3: outside a turn the crawler is not held to one per 61 s."""
    starts, _ = _simulate(turn_live=False)
    c = starts["crawler"]
    assert any(b - a < pacer.LOW_TURN_GAP_S for a, b in zip(c, c[1:]))
    assert max_in_closed_window(starts["updater"] + c) <= 4


@pytest.mark.parametrize("part", ["updater", "crawler"])
@pytest.mark.parametrize("record", [None, b"", b"\0" * 50, b"ekd-istat-pacer/1 next=12 bad\n",
                                    b"ekd-istat-pacer/0 x crc=00000000\n", b"ekd-istat-pacer/3 x crc=1\n",
                                    "torn-crc", {"next": float("nan")}])
def test_ta4_a_missing_empty_torn_or_unknown_record_means_just_sent(part, record, no_qpm):
    """M10: next slot = now + 64 s, never now."""
    if record == "torn-crc":
        record = pacer.format_record(_rec(next=T0 - 500)).replace(b"next=17999995", b"next=17999994")
    slot, retry_at, new = pacer.next_slot(record, T0, part, 0.0)
    if part == "updater":
        assert slot == T0 + pacer.FAILSAFE_GAP_S and new.next == slot + pacer.GAP_S
    else:
        assert slot is None and retry_at == T0 + pacer.FAILSAFE_GAP_S


@pytest.mark.parametrize("part", ["updater", "crawler"])
def test_ta5_a_future_next_is_clamped_to_now_plus_64_never_to_now(part, no_qpm):
    """M11: next = now + 10 h (clock moved back, test residue) waits <= 64 s, never 10 h and never 0."""
    slot, retry_at, _ = pacer.next_slot(_rec(next=T0 + 36000.0, seq=1), T0, part, 0.0)
    assert (slot if part == "updater" else retry_at) == T0 + pacer.N_MAX * pacer.GAP_S
    s2, r2, _ = pacer.next_slot(_rec(next=T0 + 64.0 + 4.0, seq=1), T0, part, 0.0)       # inside the slack
    assert (s2 if part == "updater" else r2) == T0 + 68.0


def test_a_just_sent_reservation_is_honoured_by_every_later_reader(no_qpm):
    """Regression (build A): after a missing record the first reader reserves now + 64 s and leaves
    next = now + 80 s. The DESIGN's clamp threshold (now + 69 s) cut that back to now + 64 s, so the second
    reader got the SAME slot. N_MAX FIFO readers in a row must get N_MAX distinct slots one gap apart."""
    rec, slots = None, []
    for i in range(pacer.N_MAX):
        slot, _, rec = pacer.next_slot(rec, T0 + 0.01 * i, "updater", 0.0)
        slots.append(slot)
    assert slots[0] == T0 + pacer.FAILSAFE_GAP_S
    assert all(abs((b - a) - pacer.GAP_S) < 1e-9 for a, b in zip(slots, slots[1:])), slots
    s, retry, _ = pacer.next_slot(rec, T0 + 0.05, "crawler", 0.0)
    assert s is None and retry == slots[-1] + pacer.GAP_S


def test_a_penalty_reservation_is_not_clamped_into_a_collision(no_qpm):
    """Regression (build A): a reservation AT the penalty end leaves next = pen + 16 s, far beyond now + 69 s.
    A second reader must queue behind it, not land on it."""
    rec = _rec(next=T0 - 1, pen=T0 + 3600.0)
    a, _, rec = pacer.next_slot(rec, T0, "updater", 0.0)
    b, _, rec = pacer.next_slot(rec, T0 + 1.0, "updater", 0.0)
    assert a == T0 + 3600.0 and b == a + pacer.GAP_S


def test_residue_beyond_any_legitimate_queue_is_clamped(no_qpm):
    lim = 2 * pacer.N_MAX * pacer.GAP_S + pacer.FUTURE_SLACK_S                 # 133 s
    assert pacer.next_slot(_rec(next=T0 + lim - 1), T0, "updater", 0.0)[0] == T0 + lim - 1
    assert pacer.next_slot(_rec(next=T0 + lim + 1), T0, "updater", 0.0)[0] == T0 + pacer.FAILSAFE_GAP_S


def test_ta5_turn_and_penalty_are_clamped(no_qpm):
    rec = _rec(next=T0 - 1, turn=T0 + 100 * 3600.0, pen=T0 + 100 * 3600.0, low=T0 + 9999.0)
    s, _, new = pacer.next_slot(rec, T0, "updater", 0.0)
    assert s == T0 + pacer.PENALTY_MAX_S and new.turn == T0 + pacer.TURN_MARKER_MAX_S
    _, retry, _ = pacer.next_slot(rec, T0, "crawler", 0.0)
    assert retry == T0 + pacer.PENALTY_MAX_S              # low was clamped to now, so 61 s is the low rule


def test_ta6_a_penalty_holds_both_participants(no_qpm):
    rec = _rec(next=T0 - 1, pen=T0 + 60.0)
    assert pacer.next_slot(rec, T0, "updater", 0.0)[0] == T0 + 60.0
    s, retry, _ = pacer.next_slot(rec, T0, "crawler", 0.0)
    assert s is None and retry == T0 + 60.0


@real
def test_ta6_penalize_clamps_to_60_s_and_48_h(linux_like):
    clock = FakeClock()
    assert pacer.penalize(30, "crawler", clock=clock) == T0 + 60.0
    assert pacer.penalize(10 ** 9, "updater", clock=clock) == T0 + 48 * 3600.0             # V6 (not 6 h)
    assert pacer.penalize(float("nan"), "updater", clock=clock) == T0 + 60.0
    assert pacer.penalize("garbage", "updater", clock=clock) == T0 + 60.0


@real
def test_ta6_a_penalty_holds_even_a_failsafe_token(linux_like):
    clock = FakeClock()
    until = pacer.penalize(600, "updater", clock=clock)
    tok = pacer.acquire("crawler", "esploradati.istat.it", "http", "data", clock=clock)
    assert tok.mode == "failsafe" and tok.start >= until


@real
def test_ta7_deadline_raises_window_end_without_sending(linux_like, capsys):
    """M17-shaped at the pacer: a token that cannot start by deadline - 1 s raises WindowEnd and sends nothing
    (the shared-file half, "the record is UNCHANGED", is tests/test_istat_pacer_windows.py)."""
    clock = FakeClock()
    with pytest.raises(pacer.WindowEnd) as e:
        pacer.acquire("updater", "esploradati.istat.it", "http", "data", deadline_ts=T0 + 30.0, clock=clock)
    assert e.value.deadline_ts == T0 + 30.0 and e.value.slot >= T0 + pacer.FAILSAFE_GAP_S
    assert "FAILSAFE-TOKEN" not in capsys.readouterr().out and pacer.stats()["tokens"] == 0
    assert clock.t == T0                                 # it did not even wait


def test_ta8_istat_qpm_may_only_lower_the_crawler_rate(capsys):
    assert pacer.own_gap_s("crawler", {"ISTAT_QPM": "2"}) == 30.0
    assert pacer.own_gap_s("crawler", {"ISTAT_QPM": "4"}) == pacer.GAP_S                  # 15 s < 16 s
    assert pacer.own_gap_s("crawler", {}) == pacer.GAP_S
    capsys.readouterr()
    assert pacer.own_gap_s("crawler", {"ISTAT_QPM": "60"}) == pacer.GAP_S                 # M25
    assert pacer.own_gap_s("crawler", {"ISTAT_QPM": "60"}) == pacer.GAP_S
    assert capsys.readouterr().out.count("ISTAT_QPM='60' is above the shared budget") == 1
    for bad in ("0", "-3", "abc", "nan"):
        assert pacer.own_gap_s("crawler", {"ISTAT_QPM": bad}) == pacer.GAP_S
    assert pacer.own_gap_s("updater", {"ISTAT_QPM": "1"}) == pacer.GAP_S                  # crawler only


def test_ta8_the_lowered_gap_reaches_next_slot(monkeypatch):
    monkeypatch.setenv("ISTAT_QPM", "2")
    rec = _rec(next=T0 - 100)
    s, retry, _ = pacer.next_slot(rec, T0, "crawler", T0 - 20.0)
    assert s is None and retry == T0 + 10.0              # own_last + 30 s
    assert pacer.next_slot(rec, T0 + 10.0, "crawler", T0 - 20.0)[0] == T0 + 10.0


# ───────────────────────────────────────── TA9 / V1: the fail-safe path ─────────────────────────────────────────
@real
def test_ta9_failsafe_tokens_are_64_s_apart_and_never_start_at_once(linux_like, capsys):
    """M8 (gap = GAP_S), M9 (gap = 0), and the V1 mutant "fail-safe start = now": the production path on Linux.
    The first token starts at least 64 s after it was asked for; consecutive tokens are >= 64 s apart; the
    FAIL-SAFE line prints once and again after 30 min; one FAILSAFE-TOKEN line per token."""
    clock = FakeClock()
    toks = [pacer.acquire("updater", "esploradati.istat.it", "http", "data", flow=f"F{i}", clock=clock)
            for i in range(4)]
    out = capsys.readouterr().out
    assert [t.mode for t in toks] == ["failsafe"] * 4
    assert toks[0].start >= T0 + pacer.FAILSAFE_GAP_S
    starts = [t.start for t in toks]
    assert all(b - a >= pacer.FAILSAFE_GAP_S - 1e-9 for a, b in zip(starts, starts[1:]))
    assert out.count("FAIL-SAFE: no msvcrt on this platform") == 1
    assert out.count("[istat-pacer] FAILSAFE-TOKEN R ") == 4
    assert "this process paces ALONE at 1 request per 64 s" in out
    clock.t += 31 * 60.0
    pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=clock)
    assert capsys.readouterr().out.count("FAIL-SAFE: no msvcrt on this platform") == 1      # every 30 min
    assert pacer.stats()["failsafe"] == 5 and pacer.stats()["shared"] == 0


@real
def test_ta9_failsafe_tokens_from_two_participants_of_one_process_share_the_spacing(linux_like):
    clock = FakeClock()
    a = pacer.acquire("updater", "esploradati.istat.it", "tcp", "tcp", clock=clock)
    b = pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=clock)
    assert b.start - a.start >= pacer.FAILSAFE_GAP_S


@real
def test_failsafe_token_line_parses_as_a_reservation_line(linux_like, capsys):
    clock = FakeClock()
    tok = pacer.acquire("tool:probe", "https://www.istat.it/", "tcp", "tcp", clock=clock)
    pacer.outcome(tok, "conn", 0.2)
    out = capsys.readouterr().out
    line = [x for x in out.splitlines() if "FAILSAFE-TOKEN" in x][0]
    r = line.split("[istat-pacer] FAILSAFE-TOKEN ", 1)[1]
    assert r.startswith("R ") and f" {tok.slot:.3f} " in r and f"tok={tok.id}" in r
    for k in ("pid=", "part=tool:probe", "host=www.istat.it", "kind=tcp", "class=tcp", "flow=-",
              "mode=failsafe", "waited="):
        assert k in r
    assert f"[istat-pacer] UNLOGGED O " in out and f"tok={tok.id} outcome=conn" in out


# ───────────────────────────────────────── V4: WSL sends nothing ─────────────────────────────────────────
@real
def test_v4_wsl_sends_no_istat_request(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(pacer, "msvcrt", None)
    monkeypatch.setattr(pacer, "_uname_release", lambda: "5.15.153.1-microsoft-standard-WSL2")
    monkeypatch.setattr(pacer, "STATE_DIR", str(tmp_path / "pacer"))
    clock = FakeClock()
    for _ in range(2):
        with pytest.raises(pacer.NoSendPlatform):
            pacer.acquire("crawler", "esploradati.istat.it", "http", "data", clock=clock)
    assert issubclass(pacer.NoSendPlatform, pacer.WindowEnd)
    assert capsys.readouterr().out.count("NO-SEND: WSL") == 1
    assert clock.t == T0 and pacer.stats()["tokens"] == 0
    assert "mode NO-SEND (WSL" in pacer.describe()
    assert not (tmp_path / "pacer").exists()


@real
def test_v4_control_linux_ci_is_failsafe_not_no_send(linux_like):
    tok = pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=FakeClock())
    assert tok.mode == "failsafe"
    assert "mode FAIL-SAFE (no msvcrt on this platform" in pacer.describe()


# ───────────────────────────────────────── arguments fail closed ─────────────────────────────────────────
@real
@pytest.mark.parametrize("args", [("crawler", "pxweb.stat.si", "http", "data"),
                                  ("crawler", "127.0.0.1", "tcp", "tcp"),
                                  ("nobody", "esploradati.istat.it", "http", "data"),
                                  ("updater", "esploradati.istat.it", "udp", "data")])
def test_a_wrong_argument_raises_and_sends_nothing(args, linux_like):
    clock = FakeClock()
    with pytest.raises(ValueError):
        pacer.acquire(*args, clock=clock)
    assert clock.t == T0 and pacer.stats()["tokens"] == 0


# ───────────────────────────────────────── V6: Retry-After ─────────────────────────────────────────
@pytest.mark.parametrize("value,expect", [("120", 120.0), ("0", 60.0), ("59", 60.0), ("3600", 3600.0),
                                          ("999999999999", 48 * 3600.0), (None, 60.0), ("", 60.0),
                                          ("Fri, 15 Jan 2027 08:05:00 GMT", 300.0),
                                          ("Fri, 15 Jan 2027 07:00:00 GMT", 60.0),            # in the past
                                          ("Mon, 18 Jan 2027 08:00:00 GMT", 48 * 3600.0),     # 3 days ahead
                                          ("Friday, 15-Jan-27 08:05:00 GMT", 300.0),          # RFC 850
                                          ("Fri Jan 15 08:05:00 2027", 300.0)])               # asctime
def test_v6_retry_after_delta_seconds_and_http_date(value, expect):
    now = 1800000000.0 - 1800000000.0 % 86400                    # any fixed now
    import datetime as dt
    now = dt.datetime(2027, 1, 15, 8, 0, 0, tzinfo=dt.timezone.utc).timestamp()
    lines = []
    assert pacer.retry_after_seconds(value, now=now, log=lines.append) == expect
    assert lines == []


@pytest.mark.parametrize("digits", [13, 400, 5000])
def test_v6_a_huge_delta_seconds_is_capped_not_an_exception(digits):
    """A valid delta-seconds of any length: 400 digits overflowed float() and 5,000 raised in int() (review)."""
    lines = []
    assert pacer.retry_after_seconds("9" * digits, now=T0, log=lines.append) == 48 * 3600.0
    assert lines == []


@real
def test_v2_started_counts_and_logs_a_late_send(linux_like, capsys):
    """started() does not refuse (the caller is about to send) but a late send is counted, logged, and the
    outcome line carries the slip for the receipt."""
    clock = FakeClock()
    tok = pacer.acquire("crawler", "esploradati.istat.it", "http", "data", clock=clock)
    clock.t += 5.0
    pacer.started(tok, clock=clock)
    pacer.outcome(tok, "200", 0.1)
    out = capsys.readouterr().out
    assert f"LATE SEND tok={tok.id}: 5.000 s after its slot" in out and "slip=5.000" in out
    assert pacer.stats()["late_sends"] == 1


@pytest.mark.parametrize("value", ["soon", "12.5", "-5", "1e3", "Fri, 99 Foo 2027", "  later  "])
def test_v6_an_unparseable_retry_after_is_60_s_and_one_log_line(value):
    lines = []
    assert pacer.retry_after_seconds(value, now=T0, log=lines.append) == 60.0
    assert len(lines) == 1 and "unparseable" in lines[0]


# ───────────────────────────────────────── 6.4 / V5: redirect helpers ─────────────────────────────────────────
def test_v5_redirect_decisions():
    base = "https://esploradati.istat.it/SDMXWS/rest/data/IT1,22_289/?startPeriod=2024"
    d = pacer.redirect_decision
    assert d(base, 200, None) == ("not-redirect", "")
    assert d(base, 302, "/SDMXWS/rest/data/IT1,22_289/A?startPeriod=2024") == \
        ("follow", "https://esploradati.istat.it/SDMXWS/rest/data/IT1,22_289/A?startPeriod=2024")
    assert d(base, 301, base)[0] == "dead"                                        # self
    assert d(base, 302, "?startPeriod=2023")[0] == "dead"                          # same path: self
    assert d(base, 302, "https://avvisi.istat.it/")[0] == "dead"                    # cross host
    assert d(base, 302, "http://esploradati.istat.it/SDMXWS/x")[0] == "dead"        # scheme change
    assert d(base, 307, "/SDMXWS/other", followed=1)[0] == "dead"                   # a second redirect
    assert d(base, 302, "")[0] == "dead" and d(base, 302, None)[0] == "dead"        # no Location
    assert d("https://sdmx.istat.it/SDMXWS/rest/data/X", 302, "https://sdmx.istat.it/") == \
        ("follow", "https://sdmx.istat.it/")
    assert d("https://sdmx.istat.it/", 302, "https://sdmx.istat.it/")[0] == "dead"  # the measured loop


def test_v5_no_redirect_opener_sends_exactly_one_request():
    """urllib's default follows up to 10 redirects; the pacer's opener follows none (a 3xx is an HTTPError).
    127.0.0.1 only."""
    hits = []

    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):                                # noqa: N802
            hits.append(self.path)
            self.send_response(302)
            self.send_header("Location", "/loop")
            self.end_headers()

        def log_message(self, *a):
            pass
    srv = http.server.HTTPServer(("127.0.0.1", 0), H)
    th = threading.Thread(target=srv.serve_forever, daemon=True)
    th.start()
    try:
        url = f"http://127.0.0.1:{srv.server_address[1]}/start"
        with pytest.raises(urllib.error.HTTPError) as e:
            pacer.no_redirect_opener().open(url, timeout=5)
        assert e.value.code == 302 and e.value.headers.get("Location") == "/loop"
        assert hits == ["/start"]
    finally:
        srv.shutdown()
        srv.server_close()


# ───────────────────────────────────────── V7 and 6.8: the test isolation ─────────────────────────────────────────
def test_v7_every_test_gets_the_zero_wait_stub_by_default():
    """Not marked istat_pacer_real: the conftest stub answers at once and records the call."""
    stub = pacer._TEST_OVERRIDE
    assert stub is not None and type(stub).__name__ == "ZeroWaitIstatPacer"
    clock = FakeClock()
    tok = pacer.acquire("updater", "esploradati.istat.it", "http", "data", flow="X", clock=clock)
    assert tok.mode == "stub" and tok.slot == T0 and clock.t == T0
    assert stub.calls[-1][:6] == ("acquire", "updater", "esploradati.istat.it", "http", "data", "X")
    with pytest.raises(pacer.WindowEnd):
        pacer.acquire("updater", "esploradati.istat.it", "http", "data", deadline_ts=T0 + 0.5, clock=clock)
    assert pacer.describe().startswith("[istat-pacer] state (test stub)")
    assert pacer.yield_to_turn(clock=clock) == 0.0 and pacer.begin_turn(T0 + 60, clock=clock) is True


@real
def test_v7_an_opted_in_test_gets_the_real_module():
    assert pacer._TEST_OVERRIDE is None


_INNER = '''
import time
import pytest
from jobs import _istat_pacer as pacer

@pytest.mark.istat_pacer_real
def test_sleeps_too_long_inside_the_pacer():
    pacer._sleep(time, 2.2)

@pytest.mark.istat_pacer_real
def test_sleeps_briefly_inside_the_pacer():
    pacer._sleep(time, 0.05)

def test_stub_answers_at_once():
    t0 = time.perf_counter()
    tok = pacer.acquire("crawler", "esploradati.istat.it", "http", "data")
    assert tok.mode == "stub" and time.perf_counter() - t0 < 0.5
'''


def test_v7_the_real_sleep_guard_fails_a_test_that_sleeps_over_2_s(pytester):
    """The guard can FAIL: an inner run with this repo's conftest, one test sleeping 2.2 s of real time inside
    the pacer (fails), one sleeping 0.05 s (passes), one using the stub (passes)."""
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, "conftest.py"), encoding="utf-8") as fh:
        pytester.makeconftest(fh.read())
    pytester.makepyfile(test_inner=_INNER)
    res = pytester.runpytest_inprocess("-p", "no:cacheprovider")
    # the guard fails at TEARDOWN: the long test's call passes, its teardown is the one error
    res.assert_outcomes(passed=3, errors=1)
    out = res.stdout.str()
    assert "ERROR at teardown of test_sleeps_too_long_inside_the_pacer" in out
    assert "slept 2.2" in out and "REAL time inside jobs/_istat_pacer" in out


def test_6_8_no_test_reaches_the_machine_pacer_folder(tmp_path_factory):
    """M24: tests/conftest.py's _no_production_paths points STATE_DIR at the test's own folder. Without that
    redirect this fails (and the pacer itself refuses the production folder under pytest)."""
    base = os.path.normcase(os.path.abspath(str(tmp_path_factory.getbasetemp())))
    sd = os.path.normcase(os.path.abspath(pacer.STATE_DIR))
    assert not pacer._is_production_dir(pacer.STATE_DIR)
    assert os.path.commonpath([sd, base]) == base, (sd, base)


_INNER_PROD = '''
import pytest
from jobs import _istat_pacer as pacer

class Clock:
    t = 1800000000.0
    def time(self):
        return self.t
    def sleep(self, s):
        self.t += s

@pytest.mark.istat_pacer_real
def test_forgets_the_redirect(monkeypatch):
    monkeypatch.setattr(pacer, "STATE_DIR", pacer.PRODUCTION_STATE_DIR)
    if pacer.msvcrt is None:
        monkeypatch.setattr(pacer, "msvcrt", object())
    tok = pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=Clock())
    assert tok.mode == "failsafe" and "refused under pytest" in tok.reason
'''


def test_6_8_a_test_that_points_the_pacer_at_the_machine_folder_fails(pytester, monkeypatch, tmp_path):
    """The teardown guard can FAIL: an inner test that points STATE_DIR at the production folder is refused by
    the pacer (nothing created) AND failed by the conftest guard. "Production" is a tmp stand-in for the whole
    inner run (set before its fixtures read it), so even a broken refusal cannot reach E:."""
    stand_in = tmp_path / "machine" / "istat_pacer"
    monkeypatch.setattr(pacer, "PRODUCTION_STATE_DIR", str(stand_in))
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, "conftest.py"), encoding="utf-8") as fh:
        pytester.makeconftest(fh.read())
    pytester.makepyfile(test_inner=_INNER_PROD)
    res = pytester.runpytest_inprocess("-p", "no:cacheprovider")
    res.assert_outcomes(passed=1, errors=1)
    assert "try the MACHINE's state folder" in res.stdout.str()
    assert not (tmp_path / "machine").exists()


@real
def test_6_8_the_pacer_refuses_the_production_folder_under_pytest(monkeypatch, tmp_path):
    """Belt and braces: a test that forgot the redirect gets the fail-safe and nothing is created. "Production"
    is a tmp stand-in here, so a broken refusal still cannot reach E:."""
    stand_in = tmp_path / "prod" / "istat_pacer"
    monkeypatch.setattr(pacer, "PRODUCTION_STATE_DIR", str(stand_in))
    monkeypatch.setattr(pacer, "STATE_DIR", str(stand_in))
    monkeypatch.setattr(pacer, "msvcrt", pacer.msvcrt if pacer.msvcrt is not None else object())
    tok = pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=FakeClock())
    assert tok.mode == "failsafe" and "refused under pytest" in tok.reason
    assert not (tmp_path / "prod").exists()


def test_the_module_does_no_io_at_import():
    """Harmless to import (the conftest imports it for every test): no folder, no file."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("_pacer_import_probe", pacer.__file__)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert mod._OPENED_PATHS == [] and mod.STATE_DIR == mod.PRODUCTION_STATE_DIR
