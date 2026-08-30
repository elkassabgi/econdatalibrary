"""ecb `_http_get` must RETRY a truncated transfer, not die on it.

WHY THIS EXISTS. `_http_get`'s retry handler caught
`(urllib.error.URLError, TimeoutError, ConnectionError, OSError)`, which reads like "every
transport failure" and is not. Measured on this interpreter:

    http.client.IncompleteRead  < HTTPException < Exception   NOT an OSError
    http.client.LineTooLong     < HTTPException < Exception   NOT an OSError
    zlib.error                  < Exception                   NOT an OSError
    http.client.RemoteDisconnected < ConnectionResetError < OSError    (was caught)
    gzip.BadGzipFile / ssl.SSLError / ssl.SSLEOFError  < OSError       (were caught)

So the single most transient failure ECB's large SDMX bodies produce — a body that arrives
short — escaped the loop written to absorb it and killed the unit. The gzip branch happened
to be covered (BadGzipFile IS an OSError) while the deflate branch was not, so the server's
choice of Content-Encoding decided whether a truncation retried or crashed.

An inheritance fact is exactly the kind of thing that is invisible when you read an except
clause and obvious when you raise the exception at it, so these tests raise each one.
"""
from __future__ import annotations

import gzip
import http.client
import os
import ssl
import sys
import urllib.error
import zlib

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.strategies.fetchers import ecb                      # noqa: E402
from updater.errors import TransientError                        # noqa: E402


# Every one of these must be absorbed by the retry loop and end as a TransientError.
TRANSIENT = [
    pytest.param(http.client.IncompleteRead(b"short"), id="IncompleteRead"),
    pytest.param(http.client.LineTooLong("header"), id="LineTooLong"),
    pytest.param(zlib.error("incorrect data check"), id="zlib.error"),
    pytest.param(gzip.BadGzipFile("not a gzipped file"), id="BadGzipFile"),
    pytest.param(http.client.RemoteDisconnected("closed"), id="RemoteDisconnected"),
    pytest.param(ssl.SSLEOFError("eof"), id="SSLEOFError"),
    pytest.param(TimeoutError("timed out"), id="TimeoutError"),
    pytest.param(urllib.error.URLError("unreachable"), id="URLError"),
]


@pytest.mark.parametrize("exc", TRANSIENT)
def test_transport_failures_are_retried_then_raised_as_transient(monkeypatch, exc):
    calls = {"n": 0}

    def boom(*_a, **_kw):
        calls["n"] += 1
        raise exc

    monkeypatch.setattr(ecb.urllib.request, "urlopen", boom)
    monkeypatch.setattr(ecb.time, "sleep", lambda *_a: None)     # no real backoff in tests

    with pytest.raises(TransientError) as ei:
        ecb._http_get("https://example.invalid/x")

    assert calls["n"] == ecb.CONNECT_RETRIES, (
        f"{type(exc).__name__} must be retried {ecb.CONNECT_RETRIES}x, got {calls['n']} — "
        f"if this is 1, the exception escaped the handler on the first attempt")
    assert type(exc).__name__ in str(ei.value), (
        "the raised TransientError must name what actually failed, or a truncation and a "
        "DNS failure are indistinguishable in the log")


def test_a_404_is_structural_and_not_retried(monkeypatch):
    """The control. If everything retried, the test above would pass for the wrong reason and
    a genuine 'no data for this selection' would cost 3 round trips and be misreported."""
    calls = {"n": 0}

    def boom(*_a, **_kw):
        calls["n"] += 1
        raise urllib.error.HTTPError("u", 404, "Not Found", {}, None)

    monkeypatch.setattr(ecb.urllib.request, "urlopen", boom)
    monkeypatch.setattr(ecb.time, "sleep", lambda *_a: None)

    with pytest.raises(urllib.error.HTTPError):
        ecb._http_get("https://example.invalid/x")
    assert calls["n"] == 1, "a 404 must not be retried"


def test_a_500_is_retried(monkeypatch):
    """The other side of the control: server errors DO retry."""
    calls = {"n": 0}

    def boom(*_a, **_kw):
        calls["n"] += 1
        raise urllib.error.HTTPError("u", 503, "Unavailable", {}, None)

    monkeypatch.setattr(ecb.urllib.request, "urlopen", boom)
    monkeypatch.setattr(ecb.time, "sleep", lambda *_a: None)

    with pytest.raises(TransientError):
        ecb._http_get("https://example.invalid/x")
    assert calls["n"] == ecb.CONNECT_RETRIES


def test_the_handler_covers_the_documented_family():
    """Pins the inheritance FACT the bug turned on, so a future edit that drops
    http.client.HTTPException from the tuple fails here with the reason attached."""
    assert not issubclass(http.client.IncompleteRead, OSError), (
        "if this ever becomes true the comment in _http_get is stale")
    assert not issubclass(zlib.error, OSError)
    assert issubclass(http.client.IncompleteRead, http.client.HTTPException)
