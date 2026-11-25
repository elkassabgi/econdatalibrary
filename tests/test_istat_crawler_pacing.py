"""The ISTAT crawler (jobs/ingest_istat_sliced.py) takes its tokens from the ONE shared ISTAT budget (DESIGN 12 TD;
verdict V5; review B phase 1, items 2 and 3).

THE LIMIT. ISTAT, "SDMX web services" (https://www.istat.it/en/classifications-and-tools/sdmx-web-services/, read
2026-11-24): "a limit of 5 queries per minute has been set for each IP", and a block of 1 to 2 days above it. The
crawler's old limiter was per PROCESS (a guard relaunch or a manual `--only` run was a second bucket), and
`requests` followed redirects: with 5 retries one http_get could send 5 x 31 = 155 requests.

Through the REAL http_get, _preflight, _reachable and main(), with requests.get, socket.create_connection and the
clock faked and the pacer replaced by a recorder (tests/_istat_pacing_fakes.py) or the real module on the test's
own folder. Nothing leaves this machine (tests/conftest.py refuses any non-loopback connect and ISTAT DNS).
  TD1  one token per ATTEMPT (the attempt after a 429 included, 5xx retries included); allow_redirects=False on
       every request; a 429 is a SHARED penalty (penalize), not a local 60-s sleep.
  TD2  a 3xx retires the host after ONE request (it used to be retried 5 times); a same-host redirect to another
       path is followed ONCE with its own token; a cross-host one (sdmx -> http://avvisi.istat.it:80) is not.
  V5c  when every host of the run is retired mid-run the crawler exits 3 and writes NO verdict.
  TD3  yield_to_turn before the preflight and before each flow (order), and the real wait on Windows: it logs at
       the start and every 30 min, and never passes the marker's end.
  TD4  a token's wait extends the flow's budget: a flow whose only delay is pacer wait is not "BUDGET exhausted".
  ARGS the arguments are parsed BEFORE any request: --help and an unknown argument send nothing; --list sends only
       paced requests.
"""
from __future__ import annotations

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jobs import _istat_pacer  # noqa: E402
from jobs import ingest_istat_sliced as ingest  # noqa: E402
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _istat_pacing_fakes import Clock, Recorder, Resp, sends_are_paced  # noqa: E402

ESPLORA = "https://esploradati.istat.it/SDMXWS/rest/"
SDMX = "https://sdmx.istat.it/SDMXWS/rest/"
CSV_ROW = b"DATAFLOW,FREQ,REF_AREA,TIME_PERIOD,OBS_VALUE\nIT1:X(1.0),A,IT,2026,1.5\n"
real = pytest.mark.istat_pacer_real


@pytest.fixture
def rig(tmp_path, monkeypatch):
    clock = Clock()
    events = []
    monkeypatch.setattr(ingest, "time", clock)
    monkeypatch.setattr(ingest, "_FLOW_DEADLINE", None)
    monkeypatch.setattr(ingest, "_BUDGET_HIT", False)
    monkeypatch.setattr(ingest, "HOSTS", [("esplora", ESPLORA), ("sdmx", SDMX)])
    monkeypatch.setattr(ingest, "OUT_DIR", str(tmp_path / "istat"))
    monkeypatch.setattr(ingest, "DONE_PATH", str(tmp_path / "istat" / "_sliced_done.json"))
    monkeypatch.setattr(ingest, "UNREC_PATH", str(tmp_path / "istat" / "_sliced_unrecoverable.json"))
    monkeypatch.delenv("ISTAT_QPM", raising=False)
    ingest._DEAD_HOSTS.clear()
    ingest._DIMS_CACHE.clear()
    ingest._CL_CACHE.clear()
    ingest._conn_streak = 0
    ingest._blocks_seen = 0

    class _Sock:
        def close(self):
            pass

    def fake_connect(addr, timeout=None):
        events.append(("connect", addr[0], addr[1], timeout, clock.time()))
        return _Sock()

    import socket
    monkeypatch.setattr(socket, "create_connection", fake_connect)

    class Rig:
        pass

    r = Rig()
    r.clock, r.events, r.mp, r.tmp = clock, events, monkeypatch, tmp_path
    r.sent = []

    def recorder(wait_s=0.0):
        rec = Recorder(clock, events, wait_s)
        monkeypatch.setattr(_istat_pacer, "_TEST_OVERRIDE", rec)
        return rec

    def net(answer, delay=1.0):
        """requests.get faked: answer(url, n) -> Resp or an exception; n counts this URL's sends."""
        def get(url, headers=None, timeout=None, allow_redirects=True, **_kw):
            events.append(("send", _istat_pacer.host_of(url), url, allow_redirects, clock.time()))
            r.sent.append((url, allow_redirects))
            if len(r.sent) > 60:
                raise AssertionError("more than 60 requests: a redirect chain is being followed")
            n = sum(1 for u, _a in r.sent if u == url)
            clock.t += delay
            a = answer(url, n)
            if isinstance(a, BaseException):
                raise a
            return a
        monkeypatch.setattr(ingest.requests, "get", get)

    r.recorder, r.net = recorder, net
    yield r
    ingest._DEAD_HOSTS.clear()
    ingest._DIMS_CACHE.clear()
    ingest._CL_CACHE.clear()


# ------------------------------------------------------------------ TD1

def test_td1_a_token_per_attempt_the_one_after_a_429_included_and_the_429_is_shared(rig):
    rig.recorder()
    rig.net(lambda url, n: Resp(429, b"", {"Retry-After": "120"}) if n == 1 else Resp(200, CSV_ROW))
    res = ingest.http_get(ESPLORA + "data/IT1,F1/", ingest.CSV_ACCEPT)
    assert res.ok
    kinds = [e[0] for e in rig.events if e[0] in ("token", "send", "penalize")]
    assert kinds == ["token", "send", "penalize", "token", "send"], kinds
    assert [e for e in rig.events if e[0] == "penalize"] == [("penalize", 120.0, "crawler")]
    assert all(a is False for _u, a in rig.sent), rig.sent
    assert 60.0 not in rig.clock.slept, "the local 60-s sleep is back: the penalty is not shared"
    assert sends_are_paced(rig.events) == []
    assert {e[1] for e in rig.events if e[0] == "token"} == {"crawler"}


def test_td1_every_5xx_retry_takes_its_own_token(rig):
    rig.recorder()
    rig.net(lambda url, n: Resp(503, b"") if n < 3 else Resp(200, CSV_ROW))
    res = ingest.http_get(ESPLORA + "data/IT1,F1/", ingest.CSV_ACCEPT)
    assert res.ok and len(rig.sent) == 3
    assert sum(1 for e in rig.events if e[0] == "token") == 3
    assert sends_are_paced(rig.events) == []


# ------------------------------------------------------------------ TD2 (V5)

@pytest.mark.parametrize("shape, n_sends", [("self", 1), ("cross_host", 1), ("other_path", 2), ("chain", 2)])
def test_td2_a_3xx_retires_the_host_after_one_request(rig, capsys, shape, n_sends):
    rig.recorder()
    url = ESPLORA + "data/IT1,F1/"

    def answer(u, n):
        if shape == "chain":                                   # always ANOTHER path of the same host
            return Resp(302, b"", {"Location": u.rstrip("/") + "/x/"})
        if shape == "self" or "/elsewhere/" in u:
            return Resp(302, b"", {"Location": u})
        if shape == "cross_host":
            return Resp(302, b"", {"Location": "http://avvisi.istat.it/"})
        return Resp(302, b"", {"Location": ESPLORA + "elsewhere/F1"})

    rig.net(answer)
    res = ingest.http_get(url, ingest.CSV_ACCEPT)
    assert res.kind == "conn" and len(rig.sent) == n_sends, rig.sent
    assert "esploradati.istat.it" in ingest._DEAD_HOSTS
    assert sum(1 for e in rig.events if e[0] == "token") == n_sends
    assert sends_are_paced(rig.events) == []
    out = capsys.readouterr().out
    assert "HOST esploradati.istat.it answers 302 ->" in out and "RETIRED for this run" in out, out
    outs = [e[2] for e in rig.events if e[0] == "outcome"]
    assert outs[-1].startswith("302->") and outs[-1].endswith(":dead"), outs        # V5(b): status + Location
    # the next call on a retired host sends nothing
    ingest.http_get(ESPLORA + "data/IT1,F2/", ingest.CSV_ACCEPT)
    assert len(rig.sent) == n_sends


def test_v5c_every_host_retired_mid_run_exits_3_and_writes_no_verdict(rig, monkeypatch):
    """The last host answers a data URL with a 302 that is not followed: the run stops with exit 3 (as preflight
    does) and F1 gets NO verdict; F2 is never asked."""
    rig.recorder()
    monkeypatch.setattr(ingest, "HOSTS", [("esplora", ESPLORA)])
    monkeypatch.setattr(ingest, "_preflight", lambda: True)
    monkeypatch.setattr(ingest, "get_dataflows", lambda: [{"id": "F1", "name": "one"}, {"id": "F2", "name": "two"}])
    monkeypatch.setattr(sys, "argv", ["ingest_istat_sliced.py"])
    rig.net(lambda u, n: Resp(302, b"", {"Location": u}))
    with pytest.raises(SystemExit) as ex:
        ingest.main()
    assert ex.value.code == 3
    assert [u for u, _a in rig.sent] == [ESPLORA + "data/IT1,F1/"], rig.sent
    unrec = rig.tmp / "istat" / "_sliced_unrecoverable.json"
    assert not unrec.exists() or "F1" not in json.loads(unrec.read_text(encoding="utf-8")), unrec.read_text()


# ------------------------------------------------------------------ TD3

def test_td3_yield_to_turn_comes_before_the_preflight_and_before_each_flow(rig, monkeypatch):
    rig.recorder()
    monkeypatch.setattr(ingest, "get_dataflows", lambda: [{"id": "F1", "name": "one"}, {"id": "F2", "name": "two"}])
    monkeypatch.setattr(sys, "argv", ["ingest_istat_sliced.py"])
    rig.net(lambda u, n: Resp(200, CSV_ROW))
    ingest.main()
    kinds = [e[0] if e[0] != "token" else f"token:{e[3]}:{e[4]}" for e in rig.events
             if e[0] in ("yield_to_turn", "token")]
    # preflight: yield, then per host a TCP token and a preflight GET token; then a yield before each flow
    assert kinds[0] == "yield_to_turn" and kinds[1] == "token:tcp:tcp", kinds
    assert kinds.count("yield_to_turn") == 3, kinds
    data_first = [i for i, k in enumerate(kinds) if k == "token:http:data"]
    yields = [i for i, k in enumerate(kinds) if k == "yield_to_turn"]
    assert len(data_first) == 2 and yields[1] < data_first[0] < yields[2] < data_first[1], kinds
    assert sends_are_paced(rig.events) == [], sends_are_paced(rig.events)
    assert {e[1] for e in rig.events if e[0] == "token"} == {"crawler"}


@real
@pytest.mark.skipif(sys.platform != "win32", reason="the shared turn marker needs msvcrt")
def test_td3_the_real_yield_logs_at_the_start_and_every_30_min_and_ends_at_the_marker(rig, monkeypatch, capsys):
    monkeypatch.setattr(_istat_pacer, "_is_wsl", lambda: False)
    _istat_pacer._reset_process_state()
    end = rig.clock.time() + 45 * 60.0
    assert _istat_pacer.begin_turn(end, clock=rig.clock)
    rig.net(lambda u, n: Resp(200, b"<x/>"))
    ingest._preflight()
    out = capsys.readouterr().out
    lines = [ln for ln in out.splitlines() if "yielding to the istat updater turn until" in ln]
    assert len(lines) == 2, out[-2000:]                                # at the start, and 30 min later
    first_token = min(e[4] for e in rig.events if e[0] == "connect")
    assert end - 1e-6 <= first_token <= end + 1.0, (first_token - end)


# ------------------------------------------------------------------ TD4

def test_td4_pacer_wait_extends_the_flow_budget(rig, monkeypatch, capsys):
    """One flow, too big whole on both hosts (a 200 over the size ceiling), then nine decade slices, each with its
    own observation: eleven requests, each token waiting 100 s - 1,100 s of pacer wait against the 900-s flow
    budget. The waits extend the budget, so the WHOLE flow is written - 9 observations, not "BUDGET exhausted" and
    not the decades reached before a budget that counted the waits (M21)."""
    import re
    rig.recorder(wait_s=100.0)
    monkeypatch.setattr(ingest, "_preflight", lambda: True)
    monkeypatch.setattr(ingest, "get_dataflows", lambda: [{"id": "F1", "name": "one"}])
    monkeypatch.setattr(sys, "argv", ["ingest_istat_sliced.py", "--max-size-mb", "0.0001"])
    big = CSV_ROW + b"x" * 200

    def answer(u, n):
        m = re.search(r"endPeriod=(\d{4})", u)
        if not m:
            return Resp(200, big)
        return Resp(200, b"DATAFLOW,FREQ,REF_AREA,TIME_PERIOD,OBS_VALUE\n"
                         + f"IT1:X(1.0),A,IT,{m.group(1)},1.5\n".encode())

    rig.net(answer, delay=0.0)
    ingest.main()
    out = capsys.readouterr().out
    assert len(rig.sent) == 11, rig.sent
    assert sum(1 for e in rig.events if e[0] == "token") == 11
    assert "BUDGET" not in out and "-> WROTE F1: 9 obs" in out, out[-2500:]


# ------------------------------------------------------------------ ARGS (review B item 3)

@pytest.mark.parametrize("argv, code", [(["--help"], None), (["--bogus"], 2), (["--list", "--max-size-mb", "x"], 2),
                                        (["--only"], 2)])
def test_arguments_are_parsed_before_any_request(rig, monkeypatch, argv, code):
    rec = rig.recorder()
    monkeypatch.setattr(sys, "argv", ["ingest_istat_sliced.py"] + argv)
    rig.net(lambda u, n: Resp(200, CSV_ROW))
    if code is None:
        ingest.main()
    else:
        with pytest.raises(SystemExit) as ex:
            ingest.main()
        assert ex.value.code == code
    assert rig.sent == [] and rec.n == 0 and not [e for e in rig.events if e[0] == "connect"], rig.events


def test_list_sends_only_paced_requests(rig, monkeypatch, capsys):
    """--list runs the preflight and downloads the catalogue (it cannot list without it): every one of those
    requests and connects is paid for by the token right before it."""
    rig.recorder()
    monkeypatch.setattr(sys, "argv", ["ingest_istat_sliced.py", "--list"])
    flows = (b'<m:Structure xmlns:m="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message" '
             b'xmlns:s="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure" '
             b'xmlns:c="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common"><m:Structures><s:Dataflows>'
             b'<s:Dataflow id="F1" agencyID="IT1" version="1.0"><c:Name>one</c:Name></s:Dataflow>'
             b'</s:Dataflows></m:Structures></m:Structure>')
    rig.net(lambda u, n: Resp(200, flows))
    ingest.main()
    sends = [e for e in rig.events if e[0] in ("send", "connect")]
    assert sends and sends_are_paced(rig.events) == [], sends_are_paced(rig.events)
    assert all(a is False for _u, a in rig.sent)


# ------------------------------------------------------------------ V9 receipt (review B condition 1)

import re  # noqa: E402

_FLOWS_XML = (b'<m:Structure xmlns:m="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message" '
              b'xmlns:s="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure" '
              b'xmlns:c="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common"><m:Structures><s:Dataflows>'
              b'<s:Dataflow id="F1" agencyID="IT1" version="1.0"><c:Name>one</c:Name></s:Dataflow>'
              b'</s:Dataflows></m:Structures></m:Structure>')
_STATE_RX = re.compile(r"^\[istat-pacer\] state .+; mode .+; gap 16 s; budget \d+/min$")


def _loud_launch(rig, monkeypatch, capsys, argv):
    """Run main() as the guard (no argument) or as --list; every connect and GET prints a marker line, so the
    output shows whether the pacer's state line came before the first request."""
    import socket
    quiet_connect = socket.create_connection

    def loud_connect(addr, timeout=None):
        print(f"REQ connect {addr[0]}", flush=True)
        return quiet_connect(addr, timeout)

    monkeypatch.setattr(socket, "create_connection", loud_connect)

    def answer(u, n):
        print(f"REQ get {u}", flush=True)
        return Resp(200, _FLOWS_XML) if "/dataflow/" in u else Resp(404, b"NoRecordsFound")

    rig.net(answer)
    monkeypatch.setattr(sys, "argv", ["ingest_istat_sliced.py"] + argv)
    try:
        ingest.main()
    except SystemExit:
        pass
    return capsys.readouterr().out.splitlines()


@pytest.mark.parametrize("argv", [[], ["--list"]])
def test_v9_a_launch_prints_the_pacer_state_line_once_before_any_request(rig, monkeypatch, capsys, argv):
    """V9: a guard_istat_sliced_*.log written by new code carries the pacer's state line, printed once per launch
    BEFORE the first connect or GET (the fetcher's update() does the same). Old code never prints it."""
    rig.recorder()
    lines = _loud_launch(rig, monkeypatch, capsys, argv)
    states = [i for i, ln in enumerate(lines) if ln.startswith("[istat-pacer] state ")]
    reqs = [i for i, ln in enumerate(lines) if ln.startswith("REQ ")]
    assert reqs, "premise: the launch sent nothing\n" + "\n".join(lines[-40:])
    assert len(states) == 1, "\n".join(lines[-40:])
    assert states[0] < reqs[0], "\n".join(lines[:40])
    assert _STATE_RX.match(lines[states[0]]), lines[states[0]]


def test_v9_help_prints_no_pacer_line_and_sends_nothing(rig, monkeypatch, capsys):
    rig.recorder()
    lines = _loud_launch(rig, monkeypatch, capsys, ["--help"])
    assert not [ln for ln in lines if ln.startswith("[istat-pacer]") or ln.startswith("REQ ")], lines[-20:]
    assert rig.sent == []


@real
@pytest.mark.skipif(sys.platform != "win32", reason="the shared record needs msvcrt")
def test_v9_on_a_valid_production_record_the_real_line_says_mode_shared(rig, monkeypatch, capsys):
    """The production shape (review B 3d): the machine's record exists and its last token is 1,000 s old. The old
    crawler printed NO [istat-pacer] line on this record; the new one prints the state line, mode shared."""
    monkeypatch.setattr(_istat_pacer, "_is_wsl", lambda: False)
    _istat_pacer._reset_process_state()
    early = Clock()
    early.t = rig.clock.t - 1000.0
    _istat_pacer.acquire("updater", "esploradati.istat.it", "http", "data", clock=early)
    capsys.readouterr()
    lines = _loud_launch(rig, monkeypatch, capsys, ["--list"])
    states = [ln for ln in lines if ln.startswith("[istat-pacer] state ")]
    assert len(states) == 1 and "; mode shared;" in states[0], "\n".join(lines[-40:])
    assert _STATE_RX.match(states[0]), states[0]
