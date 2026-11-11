"""The Retry-After class sweep (follow-up to AR-471/AR-473, ledger R1870-R1872).

#256 gave the fetchers one strict parser, `_common.parse_retry_after_s`, which never raises. Five
other call sites still parsed the header by hand. Each one is tested HERE through its real call
site, with the sleep recorded instead of slept:

  insee_melodi._request   `int(ra) if ra.isdigit()` raised on "²" outside the retry loop's except
                          (the R1870 crash, in a live fetcher). Cap 120 s, default 60 s, unchanged.

For every site the rule is the same: an ASCII delta-seconds header keeps the wait it had before
(the base-vs-head pins), a long or absurd one is capped, and a garbage one falls back to the
site's own backoff. Nothing in the header can raise.
"""
from __future__ import annotations

import datetime as dt
import email.utils
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.strategies.fetchers import insee_melodi as IM  # noqa: E402

# ASCII delta-seconds headers and the wait each site gave BEFORE this sweep (read from the base
# code, c5840d0e8, and re-checked by running the base module against the same cases).
GARBAGE = ["²", "³", "١٢", "-5", "+5", "12.5", "1e3", "inf", "nan", "abc", "Mon, 99 Foo 2026",
           "0x10", "1_000", "", "   "]
LONG = ["1000000", "9" * 30, "1" * 5000]


class _Resp:
    def __init__(self, status, headers=None, payload=None):
        self.status_code = status
        self.headers = headers or {}
        self._payload = payload

    def json(self):
        return self._payload


def _http_date(seconds_from_now):
    when = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=seconds_from_now)
    return email.utils.format_datetime(when, usegmt=True)


@pytest.fixture
def slept(monkeypatch):
    waits = []
    monkeypatch.setattr("time.sleep", lambda s: waits.append(s))
    return waits


# --------------------------------------------------------------------------------- insee_melodi
class _Clock:
    def fits(self, _s):
        return True


class _Sess:
    def __init__(self, responses):
        self._r = list(responses)

    def get(self, *_a, **_k):
        return self._r.pop(0)


def _insee_wait(header, slept):
    headers = {} if header is None else {"Retry-After": header}
    sess = _Sess([_Resp(429, headers), _Resp(200, payload={"ok": 1})])
    assert IM._request(sess, "https://example.invalid/x", _Clock()) == ("ok", {"ok": 1})
    assert len(slept) == 1, slept
    return slept[0]


@pytest.mark.parametrize("header,want", [
    ("0", 0), ("1", 1), ("7", 7), ("30", 30), ("60", 60), ("119", 119), ("120", 120),
    ("121", 120), ("3600", 120), ("000060", 60), ("0000061", 61), ("00000000", 0),
    ("123456", 120), (None, 60),
])
def test_insee_valid_headers_keep_the_old_wait(header, want, slept):
    assert _insee_wait(header, slept) == want


@pytest.mark.parametrize("header", LONG)
def test_insee_long_delays_are_capped_and_never_raise(header, slept):
    assert _insee_wait(header, slept) == IM.RETRY_AFTER_CAP_S == 120


@pytest.mark.parametrize("header", GARBAGE)
def test_insee_garbage_falls_back_to_the_default_and_never_raises(header, slept):
    assert _insee_wait(header, slept) == IM.RETRY_AFTER_DEFAULT_S == 60


def test_insee_superscript_two_no_longer_ends_the_pass(slept):
    # the R1870 crash itself: base raised ValueError out of _request on this header
    assert _insee_wait("²", slept) == 60


def test_insee_http_date_is_honoured_within_the_cap(slept):
    assert 40 <= _insee_wait(_http_date(45), slept) <= 45
    slept.clear()
    assert _insee_wait(_http_date(3600), slept) == 120
    slept.clear()
    assert _insee_wait(_http_date(-3600), slept) == 0       # a past date: retry now
