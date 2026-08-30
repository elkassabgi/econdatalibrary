"""istat: obey the publisher's 5 req/min, and stop poking once blocked.

WHY. ISTAT documents "5 query al minuto per ogni IP" and blocks offenders for
"compresa tra 1 e 2 giorni" (ondata.github.io/guida-api-istat, read 2026-08-30). The
crawler had NO limiter and bursts to 11-12 requests/minute. Measured the same day:
esploradati.istat.it resolves fine but a TCP connect to :443 is SILENTLY DROPPED after
21.0 s — blackholed, not refused — while an unrelated host answered HTTP 200 in 0.4 s.
The crawler had managed 275 of 2,483 flows in three days, with 743 timeouts and zero
completions in its last 2,000 log lines.

The vicious part, and the reason a plain retry loop cannot recover: the block lasts 1-2
days and EVERY retry sent while blocked renews it. Pacing alone is not enough; the crawler
has to notice and stop.

These tests are offline. Time and sleep are injected.
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jobs import ingest_istat_sliced as istat                       # noqa: E402


@pytest.fixture(autouse=True)
def _reset():
    istat._last_request_at = 0.0
    istat._conn_streak = 0
    istat._blocks_seen = 0
    yield
    istat._last_request_at = 0.0
    istat._conn_streak = 0
    istat._blocks_seen = 0


def _clock(monkeypatch):
    """A fake clock so pacing is asserted exactly, with no real sleeping."""
    state = {"t": 1000.0, "slept": []}
    monkeypatch.setattr(istat.time, "time", lambda: state["t"])

    def sleep(sec):
        state["slept"].append(sec)
        state["t"] += sec

    monkeypatch.setattr(istat.time, "sleep", sleep)
    return state


def test_requests_are_paced_under_the_published_ceiling(monkeypatch):
    st = _clock(monkeypatch)
    for _ in range(6):
        istat._throttle()
    # 6 requests, 5 gaps. Every gap must be >= 60/ISTAT_QPM seconds.
    gap = 60.0 / istat.ISTAT_QPM
    assert st["slept"] == pytest.approx([gap] * 5), st["slept"]
    assert istat.ISTAT_QPM < 5, (
        "the publisher's documented ceiling is 5/min; the default must sit BELOW it, not at "
        "it, or clock skew and their counting window put us over")


def test_pacing_does_not_sleep_when_calls_are_already_slow(monkeypatch):
    """A real data pull takes minutes. The limiter must not add delay on top of that — it is
    a floor on the gap, not a fixed tax per request."""
    st = _clock(monkeypatch)
    istat._throttle()
    st["t"] += 600                       # ten minutes of actual downloading
    st["slept"].clear()
    istat._throttle()
    assert st["slept"] == [], "already past the gap; must not sleep again"


def test_a_reply_of_any_kind_clears_the_streak():
    """A 500 means the server is TALKING to us. Counting it toward a block would make the
    crawler cool off for hours over ordinary server errors."""
    for _ in range(istat.BLOCK_STREAK - 1):
        assert istat._note_no_reply() is False
    istat._note_reply()
    assert istat._conn_streak == 0
    assert istat._note_no_reply() is False, "streak must restart after a reply"


def test_the_streak_trips_only_at_the_threshold():
    for i in range(istat.BLOCK_STREAK - 1):
        assert istat._note_no_reply() is False, f"tripped early at {i+1}"
    assert istat._note_no_reply() is True, "must trip exactly at BLOCK_STREAK"


def test_cool_off_escalates_and_is_capped(monkeypatch):
    st = _clock(monkeypatch)
    for _ in range(6):
        istat._cool_off()
    naps = st["slept"]
    assert naps[0] == istat.BLOCK_COOLDOWN_S
    assert naps[1] == 2 * istat.BLOCK_COOLDOWN_S, "must escalate, or a 2-day block is poked hourly"
    assert all(n <= istat.BLOCK_COOLDOWN_MAX_S for n in naps), naps
    assert naps[-1] == istat.BLOCK_COOLDOWN_MAX_S, "must reach and hold the cap"
    assert istat._conn_streak == 0, "streak resets after cooling off, so we re-probe once"


def test_http_get_stops_poking_once_blocked(monkeypatch):
    """THE REGRESSION, through the real http_get: a connect-timeout storm must terminate in
    a cool-off, not keep issuing requests. Before this, every retry renewed the block."""
    st = _clock(monkeypatch)
    calls = {"n": 0}

    def boom(*_a, **_kw):
        calls["n"] += 1
        raise istat.requests.exceptions.Timeout("connect timed out")

    monkeypatch.setattr(istat.requests, "get", boom)
    monkeypatch.setattr(istat, "_FLOW_DEADLINE", None)
    istat._DEAD_HOSTS.clear()

    # Drive enough calls to cross the threshold.
    for _ in range(6):
        istat.http_get("https://esploradati.istat.it/SDMXWS/rest/data/X", istat.CSV_ACCEPT)
        if istat._blocks_seen:
            break
    assert istat._blocks_seen >= 1, (
        f"{calls['n']} requests issued without ever declaring a block — the crawler would "
        f"keep renewing a 1-2 day ban")
    assert any(s >= istat.BLOCK_COOLDOWN_S for s in st["slept"]), (
        "a detected block must produce a LONG sleep, not another per-request backoff")


def test_a_healthy_response_never_triggers_a_block(monkeypatch):
    """The control. Without it, a limiter that blocked on everything would pass the test
    above and stop the crawler dead."""
    _clock(monkeypatch)

    class R:
        status_code = 200
        content = b"ok"

    monkeypatch.setattr(istat.requests, "get", lambda *_a, **_kw: R())
    monkeypatch.setattr(istat, "_FLOW_DEADLINE", None)
    istat._DEAD_HOSTS.clear()
    for _ in range(istat.BLOCK_STREAK + 5):
        res = istat.http_get("https://esploradati.istat.it/x", istat.CSV_ACCEPT)
        assert res.ok
    assert istat._blocks_seen == 0
    assert istat._conn_streak == 0
