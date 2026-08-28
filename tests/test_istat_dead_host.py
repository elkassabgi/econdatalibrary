"""istat: an UNREACHABLE host must die fast, a SLOW one must never be called dead.

WHY THIS EXISTS (measured 2026-08-28, with a control). `esploradati.istat.it` TCP 443
times out while `sdmx.istat.it` and `www.istat.it` open in 0.2s — ISTAT's outage, not our
egress. Until this fix only `TooManyRedirects` marked a base dead, so a base dead AT THE
SOCKET returned a bare `transient` and cost its full timeout on EVERY flow: esploradati
carries HOST_TIMEOUT 300s with RETRIES 3, i.e. ~900s per flow across 2,483 flows. The
workstation pass has a ~221-minute budget, so istat consumed the WHOLE nightly local-heavy
budget and was hard-killed (rc=124) before recording one run row — starving the other 28
`run_location: local` sources. Measured consequence: bls/eia/bea/statcan/fhfa were last
ATTEMPTED 2026-08-18..22 and sat RED-DATA on the gate for it.

The opposite error is equally recorded in the module: HOST_TIMEOUT exists because
esploradati was SLOW BUT WORKING on 2026-08-24 (76.0s / 102.6s / 123.7s full-body 200s),
and calling that dead would be "a working host reported as failing". So the discriminating
pair below is the whole point — TCP-connect separates unreachable from slow-to-serve, and
any completed reply (even a 404) clears a failure streak.
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.strategies.fetchers import istat  # noqa: E402


@pytest.fixture(autouse=True)
def _clean_run_state():
    """Every test starts from a fresh RUN (these caches are per-run by design)."""
    istat._DEAD_HOSTS.clear()
    istat._TRANSPORT_FAILS.clear()
    istat._TCP_PROBED.clear()
    yield
    istat._DEAD_HOSTS.clear()
    istat._TRANSPORT_FAILS.clear()
    istat._TCP_PROBED.clear()


SDMX = "https://sdmx.istat.it/SDMXWS/rest/"
ESPLORA = "https://esploradati.istat.it/SDMXWS/rest/"


def test_base_of_maps_urls_and_rejects_strangers():
    assert istat._base_of(ESPLORA + "data/IT1,FLOW/") == ESPLORA
    assert istat._base_of(SDMX + "dataflow/IT1") == SDMX
    assert istat._base_of("https://example.org/x") is None


def test_consecutive_transport_failures_mark_the_base_dead():
    for _ in range(istat._HOST_DEAD_AFTER - 1):
        istat._note_transport_failure(ESPLORA)
    assert ESPLORA not in istat._DEAD_HOSTS, "must not die before the threshold"
    istat._note_transport_failure(ESPLORA)
    assert ESPLORA in istat._DEAD_HOSTS


def test_a_success_resets_the_streak_so_a_SLOW_host_survives():
    # The discriminating half: esploradati's real 2026-08-24 behaviour was
    # timeout, timeout, then a working 123.7s 200 — that host must stay usable.
    istat._note_transport_failure(ESPLORA)
    istat._note_transport_failure(ESPLORA)
    istat._note_success(ESPLORA)
    istat._note_transport_failure(ESPLORA)
    istat._note_transport_failure(ESPLORA)
    assert ESPLORA not in istat._DEAD_HOSTS, \
        "two failures after a success is below the threshold — a slow host is not a dead one"


def test_unknown_base_is_ignored_not_counted():
    istat._note_transport_failure(None)
    assert istat._TRANSPORT_FAILS == {} and not istat._DEAD_HOSTS


def test_tcp_probe_is_cached_and_marks_only_the_unreachable(monkeypatch):
    calls = []

    class _Sock:
        def close(self):
            pass

    def fake_connect(addr, timeout=None):
        calls.append(addr)
        if addr[0] == "esploradati.istat.it":
            raise OSError("timed out")
        return _Sock()

    import socket
    monkeypatch.setattr(socket, "create_connection", fake_connect)
    assert istat._tcp_reachable(SDMX) is True
    assert istat._tcp_reachable(ESPLORA) is False
    # cached: a second ask costs no new connection
    assert istat._tcp_reachable(ESPLORA) is False
    assert len(calls) == 2, f"probe must be cached per base per run, got {calls}"
    assert calls[0] == ("sdmx.istat.it", 443) and calls[1] == ("esploradati.istat.it", 443)


def test_mark_dead_is_idempotent_and_announces_once(capsys):
    istat._mark_dead(ESPLORA, "is unreachable")
    istat._mark_dead(ESPLORA, "is unreachable")
    out = capsys.readouterr().out
    assert out.count("skipping it for the rest of this run") == 1
    assert istat._DEAD_HOSTS == {ESPLORA}


def test_one_slow_flow_through_the_REAL_retry_loop_does_not_kill_a_live_host(monkeypatch):
    """The test the first cut lacked (review 2026-08-28).

    `_fetch_flow`'s own backoff loop calls `_try_host` once per attempt on the same base,
    so with `_HOST_DEAD_AFTER == RETRIES` a SINGLE slow flow marked a working host dead.
    Drive the production path — not the counter — with sdmx redirect-looping (its real
    state today) and esploradati reachable but timing out on this one flow.
    """
    import requests
    monkeypatch.setattr(istat.time, "sleep", lambda *_a, **_k: None)
    monkeypatch.setattr(istat, "_tcp_reachable", lambda base: base == ESPLORA)

    def fake_get(url, headers=None, timeout=None):
        if url.startswith(SDMX):
            raise requests.TooManyRedirects("Exceeded 30 redirects.")
        raise requests.Timeout("slow body")

    sess = type("S", (), {"get": staticmethod(fake_get)})()
    _k, _d, _v, kind = istat._fetch_flow(sess, "FLOW_X", "2020-01-01", had_prior=True)

    assert kind == "transient"
    assert ESPLORA not in istat._DEAD_HOSTS, (
        f"one slow flow must NOT kill a live host — streak "
        f"{istat._TRANSPORT_FAILS.get(ESPLORA)} vs threshold {istat._HOST_DEAD_AFTER}")
    assert istat._HOST_DEAD_AFTER > istat.RETRIES, "threshold must exceed one flow's retries"


def test_any_reply_clears_the_streak_even_a_404(monkeypatch):
    """A 404 NoRecordsFound proves transport; it must not accrue toward death."""
    import requests

    class _R:
        status_code = 404
        content = b"NoRecordsFound"

    calls = {"n": 0}

    def fake_get(url, headers=None, timeout=None):
        calls["n"] += 1
        if calls["n"] <= 2:
            raise requests.Timeout("blip")
        return _R()

    sess = type("S", (), {"get": staticmethod(fake_get)})()
    istat._get(sess, ESPLORA + "data/X", istat.CSV_ACCEPT)     # timeout -> streak 1
    istat._get(sess, ESPLORA + "data/X", istat.CSV_ACCEPT)     # timeout -> streak 2
    assert istat._TRANSPORT_FAILS[ESPLORA] == 2
    istat._get(sess, ESPLORA + "data/X", istat.CSV_ACCEPT)     # 404 -> proves transport
    assert istat._TRANSPORT_FAILS[ESPLORA] == 0
    assert ESPLORA not in istat._DEAD_HOSTS


def test_all_hosts_dead_is_detectable_by_the_loop_guard():
    # The early-abort predicate the per-flow loop uses before paying RATE per flow.
    assert not all(b in istat._DEAD_HOSTS for _l, b in istat.HOSTS)
    for _l, b in istat.HOSTS:
        istat._mark_dead(b, "test")
    assert all(b in istat._DEAD_HOSTS for _l, b in istat.HOSTS)
