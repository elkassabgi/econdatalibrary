"""The istat fetcher takes its tokens from the ONE shared ISTAT budget (DESIGN 12, TC1-TC8 and TG; verdict V5, V8).

THE LIMIT. ISTAT, "SDMX web services" (https://www.istat.it/en/classifications-and-tools/sdmx-web-services/, read
2026-11-24): "a limit of 5 queries per minute has been set for each IP", and a block of 1 to 2 days above it. Until
2026-11-25 this fetcher had no limiter (by omission); on 2026-11-22 at least 17 of its GETs started in one minute.

Everything runs through the REAL istat.update() with a fake session (or a real requests.Session on a fake
adapter), a fake socket.create_connection and a fake clock: no request leaves this machine (tests/conftest.py
also refuses any non-loopback connect and any ISTAT DNS lookup). The pacer is either a recording stand-in
installed through jobs/_istat_pacer._TEST_OVERRIDE (so the ORDER of tokens and sends is visible) or the REAL
module on the test's own folder (marked istat_pacer_real); the real one runs FAIL-SAFE here (msvcrt hidden, one
token per 64 s) so CI and the workstation assert the same timeline, and in shared mode only on Windows.
  TC1  every request takes a token BEFORE it is sent: first attempts, retries, the XML fallback, both TCP probes.
  TC2  a real requests.Session that answers 302 thirty times: ONE request is sent (two when the redirect goes to
       another path of the same host: V5(a)), and the host is dead for the run.
  TC3  a 429's Retry-After is shared: the next token of ANOTHER participant waits for it.
  TC4  C8c: admitted exactly at the 22.0-min reserve a flow starts and nothing is sent after req_end_ts; a slot that
       would pass req_end_ts defers the flow AT ONCE (deferred, not transient).
  TC5  C7: a pacer that waits 15 s per token marks no host dead, counts no transport failure, and the turn reaches
       the cap.
  TC6  C7: a pacer whose lock raises gives fail-safe tokens; update() returns a Result.
  TC7  the turn marker goes up before the first token and down after the loop, also when a flow raises.
  TC8  the progress line and the run's last line carry tokens, wait and mode.
  TG   the moved pins 960 / 21.0 / 22.0, asserted from the rule (V8: on #327's head a4150289f).
"""
from __future__ import annotations

import datetime as dt
import io
import os
import sys
import time as _real_time

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests  # noqa: E402

from jobs import _istat_pacer  # noqa: E402
from updater import orchestrate  # noqa: E402
from updater.strategies.fetchers import _common, istat  # noqa: E402

SDMX = "https://sdmx.istat.it/SDMXWS/rest/"
ESPLORA = "https://esploradati.istat.it/SDMXWS/rest/"
EPOCH = 1_800_000_000.0
MARGIN_S = 300.0
real = pytest.mark.istat_pacer_real
CSV_ROW = b"DATAFLOW,FREQ,REF_AREA,TIME_PERIOD,OBS_VALUE\nIT1:X(1.0),A,IT,2026,1.5\n"
CSV_EMPTY = b"DATAFLOW,FREQ,REF_AREA,TIME_PERIOD,OBS_VALUE\n"           # parses 0 rows, < 256 bytes


class _Clock:
    def __init__(self):
        self.t = 0.0

    def time(self):
        return EPOCH + self.t

    def monotonic(self):
        return self.t

    def sleep(self, s):
        self.t += max(0.0, float(s))

    def __getattr__(self, name):
        return getattr(_real_time, name)


class _Recorder:
    """A recording pacer (installed through _TEST_OVERRIDE): every call goes into `events`, which the fake network
    shares, so a token and the send it pays for are in one ordered list. acquire() waits `wait_s` fake seconds and
    refuses (WindowEnd) a slot later than deadline - 1 s, the real rule."""

    def __init__(self, clock, events, wait_s=0.0):
        self.clock, self.events, self.wait_s, self.n = clock, events, float(wait_s), 0

    def acquire(self, participant, host, kind, path_class, *, deadline_ts=None, flow=None, clock=None):
        slot = self.clock.time() + self.wait_s
        if deadline_ts is not None and slot > float(deadline_ts) - 1.0:
            self.events.append(("refused", participant, _istat_pacer.host_of(host), kind))
            raise _istat_pacer.WindowEnd("recorder: slot after deadline - 1 s", slot=slot, deadline_ts=deadline_ts)
        self.clock.sleep(self.wait_s)
        self.n += 1
        h = _istat_pacer.host_of(host)
        self.events.append(("token", participant, h, kind, path_class, flow, self.clock.time()))
        return _istat_pacer.Token(f"rec-{self.n}", slot, self.wait_s, "recorder", participant, h, kind, path_class,
                                  flow, start=self.clock.time())

    def outcome(self, token, text, elapsed_s):
        self.events.append(("outcome", getattr(token, "id", None), text))

    def penalize(self, seconds, participant, clock=None):
        self.events.append(("penalize", float(seconds), participant))
        return self.clock.time() + float(seconds)

    def begin_turn(self, end_ts=None, clock=None):
        self.events.append(("begin_turn", end_ts))
        return True

    def end_turn(self, clock=None):
        self.events.append(("end_turn",))
        return True

    def yield_to_turn(self, log=None, clock=None):
        self.events.append(("yield_to_turn",))
        return 0.0

    def describe(self):
        return "[istat-pacer] state (recorder); mode recorder; gap 16 s; budget 4/min"


class _Resp:
    def __init__(self, status, content=b"", headers=None):
        self.status_code, self.content, self.headers = status, content, dict(headers or {})


class _Net:
    """A fake session. `answer(url, n)` -> _Resp or an exception to raise; n counts this URL's sends."""

    def __init__(self, clock, events, answer, delay=1.0):
        self.clock, self.events, self.answer, self.delay = clock, events, answer, float(delay)
        self.sent = []

    def get(self, url, headers=None, timeout=None, allow_redirects=True, **_kw):
        self.events.append(("send", _istat_pacer.host_of(url), url, allow_redirects, self.clock.time()))
        self.sent.append((url, timeout, allow_redirects, self.clock.time()))
        n = sum(1 for u, *_ in self.sent if u == url)
        self.clock.t += self.delay
        a = self.answer(url, n)
        if isinstance(a, BaseException):
            raise a
        return a


def _flow_of(url):
    return url.split(f"{istat.AGENCY},", 1)[1].split("/", 1)[0] if f"{istat.AGENCY}," in url else ""


@pytest.fixture
def rig(tmp_path, monkeypatch):
    out = tmp_path / "data" / "clean_full" / "istat"
    out.mkdir(parents=True)
    clock = _Clock()
    for k in ("AQUEDUCT_BACKEND", "ISTAT_MAX_FLOWS", "ISTAT_BUDGET_MIN", "AQUEDUCT_ISTAT_TURN_MAX_MIN", "ISTAT_QPM"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("AQUEDUCT_BUDGET_MIN_OVERRIDE", "360")
    monkeypatch.setenv("AQUEDUCT_ISTAT_MARGIN_MIN", "5")
    monkeypatch.setenv("AQUEDUCT_ISTAT_CSV_S_PER_FLOW", "0")
    monkeypatch.setenv("AQUEDUCT_ISTAT_TURN_MAX_MIN", "off")
    monkeypatch.setattr(istat.config, "source_dir", lambda sid: str(out))
    monkeypatch.setattr(istat, "time", clock)
    monkeypatch.setattr(_common, "time", clock)
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", None)
    events = []
    connects = []

    class _Sock:
        def close(self):
            pass

    def fake_connect(addr, timeout=None):
        events.append(("connect", addr[0], addr[1], timeout, clock.time()))
        connects.append((addr, timeout))
        return _Sock()

    import socket
    monkeypatch.setattr(socket, "create_connection", fake_connect)
    for c in (istat._DEAD_HOSTS, istat._TRANSPORT_FAILS, istat._TCP_PROBED):
        c.clear()

    class Rig:
        pass

    r = Rig()
    r.out, r.clock, r.mp, r.events, r.connects = out, clock, monkeypatch, events, connects

    def flows(n):
        for i in range(1, n + 1):
            pq.write_table(pa.table({
                "series_key": pa.array(["FREQ=A:REF_AREA=IT"], pa.string()),
                "obs_date": pa.array([dt.date(2020, 12, 31)], pa.date32()),
                "value": pa.array([1.0], pa.float64()),
            }), str(out / f"F{i:02d}.parquet"))
        return [f"F{i:02d}" for i in range(1, n + 1)]

    def recorder(wait_s=0.0):
        rec = _Recorder(clock, events, wait_s)
        monkeypatch.setattr(_istat_pacer, "_TEST_OVERRIDE", rec)
        return rec

    def session(answer, delay=1.0):
        net = _Net(clock, events, answer, delay)
        monkeypatch.setattr(istat.requests, "Session", lambda: net)
        return net

    def ceiling(seconds):
        monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", None if seconds is None else clock.time() + seconds)

    r.flows, r.recorder, r.session, r.ceiling = flows, recorder, session, ceiling
    yield r
    for c in (istat._DEAD_HOSTS, istat._TRANSPORT_FAILS, istat._TCP_PROBED):
        c.clear()


def _sends_are_paced(events):
    """Every send / connect is IMMEDIATELY preceded by a token for the same host and kind (the token BEFORE the
    request: M2 puts it after). Returns the list of offences."""
    bad = []
    for i, e in enumerate(events):
        if e[0] not in ("send", "connect"):
            continue
        kind = "http" if e[0] == "send" else "tcp"
        prev = [p for p in events[:i] if p[0] in ("token", "send", "connect")]
        last = prev[-1] if prev else None
        if not (last and last[0] == "token" and last[2] == e[1] and last[3] == kind):
            bad.append((i, e[:3], last[:4] if last else None))
    return bad


def _sdmx_self_302(url):
    return _Resp(302, b"", {"Location": url})


# ------------------------------------------------------------------ TC1

def test_tc1_every_request_and_both_tcp_probes_take_a_token_before_they_are_sent(rig):
    """sdmx answers a self-redirect 302 (dead after ONE paced GET); on esploradati F01's CSV body parses 0 rows, so
    its XML fallback is sent (and answers 404 NoRecordsFound); F02 answers 503 on the first pass and 200 on the
    retry. Every send and both real TCP probes take a token first, and nothing is sent without one."""
    rig.flows(2)
    rig.recorder()

    def answer(url, n):
        if url.startswith(SDMX):
            return _sdmx_self_302(url)
        flow = _flow_of(url)
        if flow == "F01":
            return _Resp(200, CSV_EMPTY) if n == 1 else _Resp(404, b"NoRecordsFound")
        return _Resp(503, b"") if n == 1 else _Resp(200, CSV_ROW)

    net = rig.session(answer)
    res = istat.update(None, None)
    assert res.status in ("ok", "partial"), res.error
    tokens = [e for e in rig.events if e[0] == "token"]
    sends = [e for e in rig.events if e[0] == "send"]
    conns = [e for e in rig.events if e[0] == "connect"]
    assert len(conns) == 2 and {c[1] for c in conns} == {"sdmx.istat.it", "esploradati.istat.it"}, conns
    # sdmx: 1 (302, dead) | esploradati: F01 CSV + F01 XML + F02 first pass + F02 retry = 4
    assert [s[1] for s in sends].count("sdmx.istat.it") == 1, sends
    assert [s[1] for s in sends].count("esploradati.istat.it") == 4, sends
    assert len(net.sent) == 5
    assert len(tokens) == len(sends) + len(conns) == 7, (len(tokens), len(sends), len(conns))
    assert _sends_are_paced(rig.events) == [], _sends_are_paced(rig.events)
    assert all(s[3] is False for s in sends), "a GET was sent with redirects followed"
    assert all(t[1] == "updater" for t in tokens)
    assert {t[3] for t in tokens if t[2] != "x"} == {"http", "tcp"}
    assert [t[5] for t in tokens if t[3] == "http"].count("F01") >= 2, "the XML fallback's token names its flow"


# ------------------------------------------------------------------ TC2 (a real requests.Session)

class _Adapter(requests.adapters.BaseAdapter):
    """A transport for a REAL requests.Session: it answers from `answer(url)` and sends nothing anywhere."""

    def __init__(self, answer, limit=100):
        super().__init__()
        self.answer, self.limit, self.sent = answer, limit, []

    def send(self, request, stream=False, timeout=None, verify=True, cert=None, proxies=None):
        self.sent.append(request.url)
        if len(self.sent) > self.limit:
            raise AssertionError(f"more than {self.limit} requests: a redirect loop is being followed")
        status, body, headers = self.answer(request.url)
        r = requests.Response()
        r.status_code, r._content, r._content_consumed = status, body, True
        r.headers = requests.structures.CaseInsensitiveDict(headers or {})
        r.url, r.request, r.reason, r.encoding, r.raw = request.url, request, "planted", "utf-8", None
        return r

    def close(self):
        pass


@pytest.mark.parametrize("shape, sdmx_sends", [("self", 1), ("other_path", 2), ("chain", 2)])
def test_tc2_a_real_session_answering_302_thirty_times_sends_one_request_and_kills_the_host(rig, shape, sdmx_sends):
    """M6/M7: with redirects followed (requests' default) this session would send 31 requests to sdmx.istat.it on
    ONE GET (TooManyRedirects after 30 hops); with a 3xx retried, three. allow_redirects=False sends ONE; a
    same-host redirect to ANOTHER path is followed ONCE with its own token (V5(a)), and its second 302 ends it."""
    names = rig.flows(3)
    rig.recorder()
    rig.mp.setattr(istat, "_tcp_reachable", lambda base: True)

    def answer(url):
        if url.startswith(SDMX):
            if shape == "chain":
                loc = f"{SDMX}hop{len(adapter.sent)}/"        # always ANOTHER path of the same host
            else:
                loc = url if shape == "self" or "/elsewhere/" in url else SDMX + "elsewhere/" + _flow_of(url)
            return 302, b"", {"Location": loc}
        return 200, CSV_ROW, {}

    adapter = _Adapter(answer, limit=40)
    sess = requests.Session()
    sess.trust_env = False                    # no proxy lookups (they may resolve the host name)
    sess.mount("https://", adapter)
    rig.mp.setattr(istat.requests, "Session", lambda: sess)
    res = istat.update(None, None)
    to_sdmx = [u for u in adapter.sent if u.startswith("https://sdmx.istat.it/")]
    assert len(to_sdmx) == sdmx_sends, to_sdmx
    assert SDMX in istat._DEAD_HOSTS
    assert sorted(res.series_cursors) == names and res.status == "ok", res.error
    assert sum(1 for u in adapter.sent if u.startswith(ESPLORA)) == len(names)
    tokens = [e for e in rig.events if e[0] == "token" and e[2] == "sdmx.istat.it"]
    assert len(tokens) == sdmx_sends, tokens
    outs = [e[2] for e in rig.events if e[0] == "outcome" and str(e[2]).startswith("302")]
    assert outs and outs[-1].endswith(":dead") and "->https://sdmx.istat.it/" in outs[-1], outs


# ------------------------------------------------------------------ TC3 (the real pacer)

@pytest.fixture
def failsafe(monkeypatch):
    """The REAL pacer on the test's folder, FAIL-SAFE on every platform (as on Linux CI): msvcrt hidden."""
    monkeypatch.setattr(_istat_pacer, "msvcrt", None)
    monkeypatch.setattr(_istat_pacer, "_is_wsl", lambda: False)
    _istat_pacer._reset_process_state()


@real
def test_tc3_a_429_retry_after_is_shared_the_other_participant_waits_for_it(rig, failsafe):
    """esploradati answers F01 with 429 Retry-After: 300 (sdmx 302s and dies). The fetcher calls penalize, so
    the crawler's NEXT token (another participant, this process's pacer) starts no earlier than the penalty end -
    300 s, more than the fail-safe's own 64-s gap, so only the shared penalty explains it (M20: local only)."""
    rig.flows(1)
    rig.mp.setattr(istat, "_tcp_reachable", lambda base: True)

    def answer(url, n):
        if url.startswith(SDMX):
            return _sdmx_self_302(url)
        return _Resp(429, b"", {"Retry-After": "300"})

    rig.session(answer)
    rig.mp.setattr(istat, "RETRIES", 1)                      # one pass: the 429 is the flow's last answer
    t429 = {}
    real_pen = _istat_pacer.penalize

    def pen(seconds, participant, clock=_real_time):
        t429["t"] = rig.clock.time()
        return real_pen(seconds, participant, clock=clock)

    rig.mp.setattr(_istat_pacer, "penalize", pen)
    istat.update(None, None)
    assert "t" in t429, "the 429 called no penalize: the penalty is not shared"
    tok = _istat_pacer.acquire("crawler", "esploradati.istat.it", "http", "data", clock=rig.clock)
    assert tok.start >= t429["t"] + 300.0 - 1e-6, (tok.start - t429["t"])


# ------------------------------------------------------------------ TC4 (C8c)

def _admitted_at_the_reserve(rig, extra_s=0.5):
    """The run ceiling for a unit admitted with exactly the reserve's minutes left, whose setup took the whole
    1-min allowance: update() starts with margin + FLOW_FLOOR_S + extra_s left (the C3 shape)."""
    left = (istat.window_admission_reserve_min() - istat.ADMISSION_SETUP_MIN) * 60.0 + extra_s
    rig.ceiling(left)
    return rig.clock.time() + left - MARGIN_S             # req_end_ts


@real
@pytest.mark.parametrize("mode", ["failsafe", pytest.param("shared", marks=pytest.mark.skipif(
    sys.platform != "win32", reason="the shared lock needs msvcrt"))])
def test_tc4_admitted_at_the_reserve_with_a_queued_slot_a_flow_starts_and_nothing_passes_req_end(rig, mode,
                                                                                                  monkeypatch):
    """C8c: the record's `next` is 16 s ahead (shared) or the fail-safe's 64 s applies: the first flow's token waits,
    the flow starts, and no GET is sent after req_end_ts."""
    monkeypatch.setattr(_istat_pacer, "_is_wsl", lambda: False)
    if mode == "failsafe":
        monkeypatch.setattr(_istat_pacer, "msvcrt", None)
    _istat_pacer._reset_process_state()
    rig.flows(3)
    rig.mp.setattr(istat, "_tcp_reachable", lambda base: True)
    if mode == "shared":
        # a token another participant took just now: the record's next start is now + 16 s
        _istat_pacer.acquire("crawler", "esploradati.istat.it", "http", "data", clock=rig.clock)
    net = rig.session(lambda url, n: _Resp(200, CSV_ROW), delay=60.0)
    req_end = _admitted_at_the_reserve(rig)
    res = istat.update(None, None)
    assert net.sent, "no flow started at the reserve"
    assert all(t <= req_end for _u, _to, _a, t in net.sent), [(t - req_end) for *_x, t in net.sent]
    first_wait = net.sent[0][3] - (req_end - (istat.FLOW_FLOOR_S + 0.5))
    assert first_wait >= (16.0 if mode == "shared" else 64.0) - 1e-6, first_wait
    assert res.window_stop == "progress" and "transient" not in (res.error or ""), res.error


@real
def test_tc4_a_slot_past_req_end_defers_the_flow_at_once_and_sends_nothing(rig, failsafe):
    """A 429 penalty (from an earlier request) runs past req_end_ts: the first token's slot cannot start by
    req_end_ts - 1 s, so the pacer refuses it and the flow is DEFERRED at once - no request, no wait for the
    penalty, not transient (M17: without the deadline the pacer would sleep to the penalty's end)."""
    rig.flows(3)
    rig.mp.setattr(istat, "_tcp_reachable", lambda base: True)
    net = rig.session(lambda url, n: _Resp(200, CSV_ROW), delay=60.0)
    req_end = _admitted_at_the_reserve(rig, extra_s=200.0)
    _istat_pacer.penalize(3000.0, "crawler", clock=rig.clock)
    t0 = rig.clock.time()
    res = istat.update(None, None)
    assert net.sent == [], net.sent
    assert rig.clock.time() - t0 < 5.0, "update() waited for a slot it could never use"
    assert res.status == "partial" and "deferred" in res.error and "transient" not in res.error, res.error
    assert res.window_stop == "progress", res.window_stop
    assert rig.clock.time() < req_end


# ------------------------------------------------------------------ TC5, TC6 (C7)

def test_tc5_a_pacer_that_waits_15_s_per_token_marks_no_host_dead_and_the_turn_reaches_the_cap(rig, capsys):
    """30+ flows, every token waits 15 s: no host is marked dead, _note_transport_failure is never called (M19),
    and the turn ends at its 60-min cap with the deferral - not at an "every host unusable" break."""
    names = rig.flows(40)
    rig.recorder(wait_s=15.0)
    rig.mp.setenv("AQUEDUCT_ISTAT_TURN_MAX_MIN", "60")
    rig.mp.setattr(istat, "_tcp_reachable", lambda base: True)
    noted = []
    real_note = istat._note_transport_failure
    rig.mp.setattr(istat, "_note_transport_failure", lambda base: (noted.append(base), real_note(base))[1])
    net = rig.session(lambda url, n: _Resp(200, CSV_ROW), delay=60.0)
    res = istat.update(None, None)
    assert noted == [] and not istat._DEAD_HOSTS, (noted, istat._DEAD_HOSTS)
    flows_started = len({_flow_of(u) for u, *_ in net.sent})
    assert 30 <= flows_started < len(names), flows_started
    assert res.status == "partial" and "per-turn cap" in res.error and "transient" not in res.error, res.error
    assert res.window_stop == "progress"
    assert "every host unusable" not in capsys.readouterr().out


@real
def test_tc6_a_pacer_whose_lock_raises_gives_failsafe_tokens_and_update_returns_a_result(rig, monkeypatch):
    """The lock itself raises (an unexpected error inside the pacer): every token goes fail-safe (64 s apart), no
    exception leaves update() (M18), and the turn walks its flows."""
    monkeypatch.setattr(_istat_pacer, "_is_wsl", lambda: False)
    _istat_pacer._reset_process_state()

    def broken(wait=None):
        raise RuntimeError("planted: the lock raises")

    monkeypatch.setattr(_istat_pacer, "_open_and_lock", broken)
    names = rig.flows(3)
    rig.mp.setattr(istat, "_tcp_reachable", lambda base: True)
    # (update() clears _DEAD_HOSTS: sdmx serves here too)
    net = rig.session(lambda url, n: _Resp(200, CSV_ROW))
    res = istat.update(None, None)
    assert res.status == "ok" and sorted(res.series_cursors) == names, res.error
    starts = [t for *_x, t in net.sent]
    assert len(starts) == 3 and all(b - a >= 64.0 - 1e-6 for a, b in zip(starts, starts[1:])), starts
    assert _istat_pacer.stats()["failsafe"] == 3


# ------------------------------------------------------------------ TC7, TC8

def test_tc7_the_marker_goes_up_before_the_first_token_and_down_even_when_a_flow_raises(rig):
    rig.flows(3)
    rig.recorder()
    rig.mp.setattr(istat, "_tcp_reachable", lambda base: True)

    def answer(url, n):
        if _flow_of(url) == "F02":
            return RuntimeError("planted: a flow raises")
        return _Resp(200, CSV_ROW)

    rig.session(answer)
    with pytest.raises(RuntimeError, match="planted"):
        istat.update(None, None)
    kinds = [e[0] for e in rig.events]
    assert kinds.count("begin_turn") == 1 and kinds.count("end_turn") == 1, kinds
    assert kinds.index("begin_turn") < kinds.index("token"), kinds
    last_token = max(i for i, k in enumerate(kinds) if k == "token")
    assert kinds.index("end_turn") > last_token, kinds
    assert istat._RUN_REQ_END[0] is None


@real
@pytest.mark.skipif(sys.platform != "win32", reason="the shared turn marker needs msvcrt")
def test_tc7_the_real_marker_is_live_during_the_turn_and_over_after_it_raises(rig, monkeypatch):
    monkeypatch.setattr(_istat_pacer, "_is_wsl", lambda: False)
    _istat_pacer._reset_process_state()
    rig.flows(3)
    rig.mp.setattr(istat, "_tcp_reachable", lambda base: True)
    # (update() clears _DEAD_HOSTS: sdmx serves here too)
    seen = []

    def answer(url, n):
        seen.append(_istat_pacer._read_turn(rig.clock) - rig.clock.time())
        if _flow_of(url) == "F02":
            return RuntimeError("planted: a flow raises")
        return _Resp(200, CSV_ROW)

    rig.session(answer)
    with pytest.raises(RuntimeError, match="planted"):
        istat.update(None, None)
    assert seen and all(s > 0 for s in seen), seen                     # live while the turn ran
    assert _istat_pacer._read_turn(rig.clock) <= rig.clock.time()      # over after it


def test_tc8_the_progress_line_and_the_runs_last_line_carry_tokens_wait_and_mode(rig, capsys):
    rig.flows(4)
    rig.recorder(wait_s=2.5)
    rig.mp.setattr(istat, "_tcp_reachable", lambda base: True)
    rig.mp.setattr(istat, "PROGRESS_EVERY_FLOWS", 2)
    # (update() clears _DEAD_HOSTS: sdmx serves here too)
    rig.session(lambda url, n: _Resp(200, CSV_ROW))
    istat.update(None, None)
    out = capsys.readouterr().out
    prog = [ln for ln in out.splitlines() if ln.startswith("[istat] progress: 2/4")]
    assert prog and "pacer: 2 token(s), waited 5.0 s, mode recorder" in prog[0], out[-2000:]
    assert "[istat] this run's pacer: 4 token(s), waited 10.0 s, mode recorder" in out, out[-2000:]
    assert "[istat-pacer] state (recorder)" in out


# ------------------------------------------------------------------ TG (V8)

def test_tg_the_moved_pins_follow_from_the_rule(monkeypatch):
    """FLOW_FLOOR_S = RETRIES x (the longest host timeout + ONE pacer gap per GET) + the backoffs between the retry
    passes; the tail adds the 5-min margin, the reserve the 1-min setup. On #327's head a4150289f: 960 s, 21.0 and
    22.0 min (912 s, 20.2 and 21.2 before the pacer). M23 drops the gap term."""
    monkeypatch.delenv("AQUEDUCT_ISTAT_MARGIN_MIN", raising=False)
    gap = 60.0 / _istat_pacer.BUDGET_PER_MIN + 1.0
    assert _istat_pacer.BUDGET_PER_MIN == 4 < 5 and _istat_pacer.GAP_S == gap == 16.0
    longest = max([istat.TIMEOUT, *istat.HOST_TIMEOUT.values()])
    backoffs = sum(min(4 * a, 30) for a in range(1, istat.RETRIES))
    rule = istat.RETRIES * (longest + gap) + backoffs
    assert (istat.RETRIES, longest, backoffs) == (3, 300, 12)
    assert istat.FLOW_FLOOR_S == rule == 960.0
    assert istat.window_stop_tail_min() == pytest.approx(5.0 + rule / 60.0) and 5.0 + rule / 60.0 == 21.0
    assert istat.window_admission_reserve_min() == pytest.approx(5.0 + rule / 60.0 + istat.ADMISSION_SETUP_MIN)
    assert 5.0 + rule / 60.0 + istat.ADMISSION_SETUP_MIN == 22.0
