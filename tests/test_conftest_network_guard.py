"""The suite-wide network guard (tests/conftest.py _no_network_beyond_loopback) can FAIL a test, and it refuses
BEFORE anything leaves this machine.

WHY (review B of the ISTAT pacer, phase 1, item 1; 2026-11-25). The ISTAT tests replace `requests.get`, a Session
factory or socket.create_connection with a fake, and the ISTAT pacer is a zero-wait stub in every test. A test
whose fake is bypassed would send a real, unpaced request to ISTAT from the workstation's IP; ISTAT blocks an IP
for 1 to 2 days. These planted tests run in an inner pytest with this repo's conftest:
  1. the crawler-test shape (requests.get faked) whose code uses a Session instead: the DNS lookup of the ISTAT
     name is refused, and the test FAILS at teardown although it caught the error;
  2. a raw connect to ISTAT's IP literal (no DNS at all): refused at connect, fails at teardown;
  3. CONTROL: a connect to a 127.0.0.1 listener passes (the guard does not refuse everything).
The outer test runs under the same guard, so even a broken inner guard could not reach ISTAT.
"""
from __future__ import annotations

import os
import sys

_INNER = '''
import socket

import requests


def test_a_bypassed_fake_reaches_for_istat(monkeypatch):
    monkeypatch.setattr(requests, "get", lambda *a, **k: None)      # the crawler tests' fake...
    try:                                                            # ...and code that uses a Session instead
        requests.Session().get("https://esploradati.istat.it/SDMXWS/rest/dataflow/IT1", timeout=3)
    except Exception as e:                                          # the code under test swallows it
        chain, x = [], e
        while x is not None:
            chain.append(repr(x))
            x = x.__cause__ or x.__context__
        assert any("test network guard" in c for c in chain), chain


def test_a_raw_connect_to_an_istat_ip_literal():
    try:
        socket.create_connection(("193.204.90.13", 443), timeout=1)
    except OSError as e:
        assert "test network guard" in str(e), e
    else:
        raise AssertionError("connected")


def test_control_a_loopback_connect_passes():
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    try:
        c = socket.create_connection(srv.getsockname(), timeout=5)
        c.close()
    finally:
        srv.close()
'''


def _outer_safety(monkeypatch):
    """A second refuser UNDER the inner run, on top of this test's own conftest guard: whatever the inner conftest
    does, a non-loopback connect or an ISTAT DNS lookup stops here and is recorded. It is what makes the control
    below (an inner run WITHOUT the guard) safe to run."""
    import socket
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from conftest import _is_istat_name, _is_local_address
    hits = []
    cur_connect, cur_gai = socket.socket.connect, socket.getaddrinfo

    def connect(self, address):
        host = address[0] if isinstance(address, (tuple, list)) else address
        if not (isinstance(host, (str, bytes)) and _is_local_address(host)):
            hits.append(("connect", address))
            raise ConnectionRefusedError(f"OUTER-SAFETY refused connect {address!r}")
        return cur_connect(self, address)

    def getaddrinfo(host, *a, **k):
        if _is_istat_name(host):
            hits.append(("getaddrinfo", host))
            raise ConnectionRefusedError(f"OUTER-SAFETY refused DNS {host!r}")
        return cur_gai(host, *a, **k)

    monkeypatch.setattr(socket.socket, "connect", connect)
    monkeypatch.setattr(socket, "getaddrinfo", getaddrinfo)
    return hits


def test_the_network_guard_refuses_istat_and_fails_the_test_and_lets_loopback_through(pytester, monkeypatch):
    hits = _outer_safety(monkeypatch)
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, "conftest.py"), encoding="utf-8") as fh:
        pytester.makeconftest(fh.read())
    pytester.makepyfile(test_inner=_INNER)
    res = pytester.runpytest_inprocess("-p", "no:cacheprovider")
    # the two planted calls pass their own assertions (the refusal reached them) and FAIL at teardown
    res.assert_outcomes(passed=3, errors=2)
    out = res.stdout.str()
    assert "ERROR at teardown of test_a_bypassed_fake_reaches_for_istat" in out, out[-3000:]
    assert "ERROR at teardown of test_a_raw_connect_to_an_istat_ip_literal" in out, out[-3000:]
    assert "tried the network beyond loopback" in out
    assert "getaddrinfo 'esploradati.istat.it'" in out and "193.204.90.13" in out, out[-3000:]
    assert hits == [], f"the inner guard let a call through to the outer safety: {hits}"


def test_control_without_the_guard_the_planted_calls_are_not_failed_by_it(pytester, monkeypatch):
    """NEGATIVE CONTROL: the same inner run with the guard's fixture switched off (its decorator removed). The two
    planted calls now meet only the outer safety (so nothing leaves this machine), their own assertions on the
    guard's message fail, and no teardown error is raised: the teardown errors above are the guard's work."""
    hits = _outer_safety(monkeypatch)
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, "conftest.py"), encoding="utf-8") as fh:
        text = fh.read()
    marker = "@pytest.fixture(autouse=True)\ndef _no_network_beyond_loopback(request):"
    assert text.replace("\r\n", "\n").count(marker) == 1, "premise: the guard fixture is where this test looks"
    pytester.makeconftest(text.replace("\r\n", "\n").replace(marker, "def _no_network_beyond_loopback(request):"))
    pytester.makepyfile(test_inner=_INNER)
    res = pytester.runpytest_inprocess("-p", "no:cacheprovider")
    res.assert_outcomes(passed=1, failed=2, errors=0)
    assert ("getaddrinfo", "esploradati.istat.it") in hits and ("connect", ("193.204.90.13", 443)) in hits, hits
