"""istat: obey the publisher's 5 req/min, and stop poking once blocked.

WHY. ISTAT documents "5 query al minuto per ogni IP" and blocks offenders for
"compresa tra 1 e 2 giorni" (ondata.github.io/guida-api-istat, read 2026-08-30). The
crawler had NO limiter: RATE=1.5s between slices is 40 requests/minute, and the logs peak
at 11-12/min (30/min on 2026-07-27). That breach is real and this is its fix.

WHAT IS *NOT* CLAIMED, because I claimed it and was wrong (R512). The current outage is NOT
established as a ban on us. `esploradati.istat.it` (193.204.90.13) times out from here AND
returns ECONNREFUSED from an unrelated egress, while `sdmx.istat.it` (193.204.90.1) and
`www.istat.it` (.61) — the SAME /24 — answer our address in 0.13 s. The host is down for
everyone. I had reported "IP-blocked" on three observations that were all one machine
failing to reach one host.

So the block detector below is a SAFEGUARD against a ban we could earn, not a diagnosis of
one we have. That matters for its tuning: it must be hard to trip. A first version counted
every post-reply failure as a no-reply and, replayed over a real 80%-success log (321 flows,
256 written, 18.2 h), would have slept 85 HOURS.

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

# ── driven through the REAL http_get ────────────────────────────────────────
# Everything above calls the helpers directly, and a reviewer showed that is not enough:
# deleting the `_throttle()` CALL SITE from http_get -- the one line that makes the limiter
# exist in production -- left every test green. Same shape as R511. These drive http_get.

def _drive(monkeypatch, raiser=None, status=200):
    """Run http_get with the network replaced, returning the fake clock's state."""
    st = _clock(monkeypatch)
    calls = {"n": 0}

    class R:
        status_code = status
        content = b"ok"

    def fake(*_a, **_kw):
        calls["n"] += 1
        if raiser is not None:
            raise raiser()
        return R()

    monkeypatch.setattr(istat.requests, "get", fake)
    monkeypatch.setattr(istat, "_FLOW_DEADLINE", None)
    istat._DEAD_HOSTS.clear()
    st["calls"] = calls
    return st


def test_http_get_itself_paces_requests(monkeypatch):
    """THE MUTATION THAT SURVIVED: deleting `_throttle()` from http_get must fail HERE."""
    st = _drive(monkeypatch)
    for _ in range(4):
        istat.http_get("https://esploradati.istat.it/x", istat.CSV_ACCEPT)
    gap = 60.0 / istat.ISTAT_QPM
    paced = [x for x in st["slept"] if abs(x - gap) < 1e-9]
    assert len(paced) >= 3, (
        f"http_get issued 4 requests and slept {st['slept']} — the limiter is not on the "
        f"production path, only in the helper")


def test_a_successful_reply_clears_a_real_streak(monkeypatch):
    """THE VACUOUS ASSERTION, fixed: the old healthy-path test checked _conn_streak == 0 on a
    fixture that started at 0, so removing the _note_reply() call could not fail it."""
    istat._conn_streak = istat.BLOCK_STREAK - 1        # genuinely non-zero first
    _drive(monkeypatch)
    istat.http_get("https://esploradati.istat.it/x", istat.CSV_ACCEPT)
    assert istat._conn_streak == 0, (
        "a 200 must clear the streak; without it a source that fails 11 times and then "
        "recovers is one failure from a 6-hour nap")


def test_a_redirect_loop_is_not_a_block(monkeypatch):
    """sdmx.istat.it self-redirects 302, and one log carries 2,443 TooManyRedirects. That is
    a server TALKING to us. Counting it as no-reply is what would have slept 85 hours across
    an 18-hour run that wrote 256 of 321 flows."""
    istat._conn_streak = istat.BLOCK_STREAK - 1
    _drive(monkeypatch, raiser=istat.requests.exceptions.TooManyRedirects)
    istat.http_get("https://sdmx.istat.it/x", istat.CSV_ACCEPT)
    assert istat._blocks_seen == 0, "a redirect loop must never declare a rate-limit block"
    assert istat._conn_streak == 0, "the server replied, so the streak resets"


def test_a_read_timeout_is_not_a_block(monkeypatch):
    """ReadTimeout means headers arrived and the body stalled — a slow response, not a
    closed door. It is a Timeout subclass, so it must be caught before the connect branch."""
    istat._conn_streak = istat.BLOCK_STREAK - 1
    _drive(monkeypatch, raiser=istat.requests.exceptions.ReadTimeout)
    istat.http_get("https://esploradati.istat.it/x", istat.CSV_ACCEPT)
    assert istat._blocks_seen == 0 and istat._conn_streak == 0


def test_a_connect_level_error_still_counts(monkeypatch):
    """The control for the four tests above: if replied-vs-no-reply were mis-split the other
    way, the detector would never fire at all."""
    _drive(monkeypatch, raiser=istat.requests.exceptions.ConnectionError)
    for _ in range(istat.BLOCK_STREAK + 2):
        istat.http_get("https://esploradati.istat.it/x", istat.CSV_ACCEPT)
        if istat._blocks_seen:
            break
    assert istat._blocks_seen >= 1, "a genuine connect failure storm must still cool off"


def test_the_escalation_decays_on_recovery(monkeypatch):
    """Without decay a single rough patch pins the nap at the 6h cap for a process that runs
    68-78 hours, so one bad hour costs a day."""
    istat._blocks_seen = 4
    _drive(monkeypatch)
    istat.http_get("https://esploradati.istat.it/x", istat.CSV_ACCEPT)
    assert istat._blocks_seen == 0
