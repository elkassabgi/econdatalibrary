"""Retry-After parsing must never raise, and must not change what valid headers mean (AR-471, R1870).

The old parsers used `str.isdigit()` + `int()`. `isdigit()` is True for "²" and other Unicode digits,
and int() then raises ValueError on "²"; int() also refuses a string of more than 4,300 digits. The
ValueError left the per-request retry loop, and a fetcher that catches only TransientError around its
requests could lose the whole pass to one bad header.

Covered here: `_common.parse_retry_after_s` (the one parser), `_common.retry_after_seconds` (its
clamped form, used by hagstofa, ssb, statfin and imf_imts_direct), `hagstofa._raw_retry_after_s` and
`ssb._raw_retry_after_s` (now delegates), and `jobs/ingest_ons_uk.retry_after_seconds` (an inline
copy, because that file also runs as a plain script).

THE PINNED OUTPUTS for valid headers are the outputs of the code BEFORE this change (main at
388e5d0d5), recorded with the same frozen clock this file uses. A changed number below is a
behaviour change for a valid header, not a test to update.
"""
from __future__ import annotations

import datetime as dt
import email.message
import email.utils
import os
import sys
import types
import urllib.error

import pytest

pytest.importorskip("pyarrow")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "jobs"))

from updater.strategies.fetchers import _common as C  # noqa: E402
from updater.strategies.fetchers import hagstofa as H  # noqa: E402
from updater.strategies.fetchers import ssb as S  # noqa: E402
import ingest_ons_uk as O  # noqa: E402

NOW = dt.datetime(2026, 11, 11, 12, 0, 0, tzinfo=dt.timezone.utc)


class _Frozen(dt.datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW if tz is not None else NOW.replace(tzinfo=None)


@pytest.fixture(autouse=True)
def frozen_clock(monkeypatch):
    """One fixed 'now' for every parser, so a date case has one exact answer."""
    shim = types.ModuleType("datetime")
    shim.__dict__.update({k: getattr(dt, k) for k in dir(dt) if not k.startswith("__")})
    shim.datetime = _Frozen
    monkeypatch.setattr(C, "_dt", shim)                    # the shared parser's clock
    for mod in (H, S):                                     # and any inline copy's (an old or a
        if getattr(mod, "dt", None) is dt:                 # re-introduced one reads `dt`)
            monkeypatch.setattr(mod, "dt", shim)
    monkeypatch.setitem(sys.modules, "datetime", shim)     # ingest_ons_uk imports it inside the call


def _fmt(d):
    return email.utils.format_datetime(d, usegmt=True)


def _resp(value):
    return types.SimpleNamespace(headers={} if value is None else {"Retry-After": value})


# name: (header, unclamped seconds, clamped with default=10, clamped with default=61)
VALID = {
    "zero":        ("0", 0, 2, 2),
    "one":         ("1", 1, 2, 2),
    "seven":       ("7", 7, 8, 8),
    "thirty":      ("30", 30, 31, 31),
    "sixty":       ("60", 60, 61, 61),
    "sixty-one":   ("61", 61, 62, 62),
    "119":         ("119", 119, 120, 120),
    "120":         ("120", 120, 121, 121),
    "121":         ("121", 121, 121, 121),
    "hour":        ("3600", 3600, 121, 121),
    "six-digits":  ("999999", 999999, 121, 121),
    "123456":      ("123456", 123456, 121, 121),
    "leading-0":   ("000060", 60, 61, 61),
    "7char-61":    ("0000061", 61, 62, 62),       # legal 1*DIGIT; AR-473 found the first version misread it
    "zeros":       ("00000000", 0, 2, 2),
    "long-1":      ("0000000000001", 1, 2, 2),
    "padded":      ("  30  ", 30, 31, 31),
    "tabs":        ("\t45\t", 45, 46, 46),
    "date+90":     (_fmt(NOW + dt.timedelta(seconds=90)), 90, 91, 91),
    "date+3600":   (_fmt(NOW + dt.timedelta(seconds=3600)), 3600, 121, 121),
    "date+30d":    (_fmt(NOW + dt.timedelta(days=30)), 2592000, 121, 121),
    "date-past":   (_fmt(NOW - dt.timedelta(seconds=3600)), -3600, 2, 2),
    "date-1999":   ("Fri, 31 Dec 1999 23:59:59 GMT", -847713601, 2, 2),
    "date-naive":  ((NOW + dt.timedelta(seconds=90)).strftime("%a, %d %b %Y %H:%M:%S"), 90, 91, 91),
    "date-offset": (email.utils.format_datetime(
        (NOW + dt.timedelta(seconds=300)).astimezone(dt.timezone(dt.timedelta(hours=2)))), 300, 121, 121),
}

# More than 6 significant digits SATURATE to 999,999 s, so every stop threshold still fires (AR-473,
# R1872: treating "1000000" as no header made hagstofa and ssb retry inside an 11.6-day lockout, the
# R1634 shape). Base read the first three as their exact value (unclamped 1,000,000 and up, clamped
# 121) and RAISED ValueError on the last two; int() is never called on them now.
SATURATED = {
    "seven-digits":  "1000000",
    "86400000":      "86400000",
    "30-digits":     "1" * 30,
    "4301-digits":   "9" * 4301,
    "5000-digits":   "1" * 5000,
}

# Not a usable header: unclamped None, clamped = the caller's default + 1. The first two RAISED
# ValueError before this change; Unicode digits were read as a number (12) and now are not.
UNUSABLE = {
    "superscript-2": "²",
    "digit+sup":     "3²",
    "arabic-12":     "١٢",
    "minus-5":       "-5",
    "12.5":          "12.5",
    "plus-5":        "+5",
    "exponent":      "1e3",
    "empty":         "",
    "whitespace":    "   ",
    "word":          "soon",
    "hex":           "0x10",
    "inner-space":   "1 2",
}

RAW_PARSERS = {
    "common.parse_retry_after_s": lambda r: C.parse_retry_after_s(r),
    "hagstofa._raw_retry_after_s": lambda r: H._raw_retry_after_s(r),
    "ssb._raw_retry_after_s": lambda r: S._raw_retry_after_s(r),
}
CLAMPED = {
    "common.retry_after_seconds": lambda r, d: C.retry_after_seconds(r, default=d),
    "ons.retry_after_seconds": lambda r, d: O.retry_after_seconds(r, default=d),
}


@pytest.mark.parametrize("parser", sorted(RAW_PARSERS))
@pytest.mark.parametrize("case", sorted(VALID))
def test_raw_parsers_keep_the_old_value_for_valid_headers(parser, case):
    header, raw, _, _ = VALID[case]
    assert RAW_PARSERS[parser](_resp(header)) == raw


@pytest.mark.parametrize("parser", sorted(CLAMPED))
@pytest.mark.parametrize("case", sorted(VALID))
def test_clamped_parsers_keep_the_old_wait_for_valid_headers(parser, case):
    header, _, d10, d61 = VALID[case]
    assert CLAMPED[parser](_resp(header), 10) == d10
    assert CLAMPED[parser](_resp(header), 61) == d61


@pytest.mark.parametrize("parser", sorted(RAW_PARSERS))
@pytest.mark.parametrize("case", sorted(SATURATED))
def test_raw_parsers_saturate_long_delays_without_raising(parser, case):
    assert RAW_PARSERS[parser](_resp(SATURATED[case])) == 999_999 == C.RETRY_AFTER_SATURATED_S


@pytest.mark.parametrize("parser", sorted(CLAMPED))
@pytest.mark.parametrize("case", sorted(SATURATED))
def test_clamped_parsers_give_121_for_long_delays(parser, case):
    assert CLAMPED[parser](_resp(SATURATED[case]), 10) == 121
    assert CLAMPED[parser](_resp(SATURATED[case]), 61) == 121


@pytest.mark.parametrize("parser", sorted(RAW_PARSERS))
@pytest.mark.parametrize("case", sorted(UNUSABLE) + ["absent"])
def test_raw_parsers_return_none_and_never_raise(parser, case):
    assert RAW_PARSERS[parser](_resp(UNUSABLE.get(case))) is None


@pytest.mark.parametrize("parser", sorted(CLAMPED))
@pytest.mark.parametrize("case", sorted(UNUSABLE) + ["absent"])
def test_clamped_parsers_fall_back_to_the_default(parser, case):
    assert CLAMPED[parser](_resp(UNUSABLE.get(case)), 10) == 11
    assert CLAMPED[parser](_resp(UNUSABLE.get(case)), 61) == 62


def test_ssb_throttle_wait_falls_back_to_its_own_backoff():
    for header in ("²", "-5", "12.5", ""):
        assert S._throttle_wait(_resp(header), 0) == 61
        assert S._throttle_wait(_resp(header), 1) == 121
    assert S._throttle_wait(_resp("7"), 0) == 8, "positive control: a valid header still wins"
    assert S._throttle_wait(_resp("1" * 5000), 0) == 121, "a saturated delay is the clamp, not the backoff"


class _BadHeaders:
    def get(self, key, default=None):
        raise RuntimeError("unreadable header block")


@pytest.mark.parametrize("resp", [
    types.SimpleNamespace(headers=_BadHeaders()),
    types.SimpleNamespace(headers=None),
    types.SimpleNamespace(),
    object(),
], ids=["get-raises", "headers-None", "no-headers", "bare-object"])
def test_an_unreadable_header_block_is_no_header(resp):
    for name, f in RAW_PARSERS.items():
        assert f(resp) is None, name
    assert C.retry_after_seconds(resp, default=10) == 11
    assert O.retry_after_seconds(resp, default=10) == 11


def test_a_non_string_header_value_is_read_as_text():
    assert C.parse_retry_after_s(_resp(30)) == 30
    assert O.retry_after_seconds(_resp(30), default=10) == 31


def test_urllib_http_error_headers_work_for_imf_imts_direct():
    """imf_imts_direct hands retry_after_seconds an HTTPError, whose headers are an email Message."""
    for header, want in (("7", 8), ("²", 11), ("1" * 5000, 121), ("0000061", 62)):
        msg = email.message.Message()
        msg["Retry-After"] = header
        err = urllib.error.HTTPError("u", 429, "Too Many Requests", msg, None)
        assert C.retry_after_seconds(err, default=10) == want, header


# ---- the callers that crashed: one bad header must not raise out of the 429 handler -------------

def test_hagstofa_429_handler_waits_the_window_on_a_garbage_header(monkeypatch):
    waited = []
    monkeypatch.setattr(H, "_stat", lambda sess, key: None)
    monkeypatch.setattr(H, "_wait", lambda sess, secs, what: waited.append(secs))
    for header in ("²", "-5"):
        H._wait_429(object(), _resp(header), "t")
    assert waited == [H.RATE_WINDOW_S + 1] * 2, waited
    H._wait_429(object(), _resp("7"), "t")
    assert waited[-1] == 8, "positive control: a valid header is still honoured"
    H._wait_429(object(), _resp("000060"), "t")
    assert waited[-1] == 61, "60 s (with leading zeros) is still waited, not a stop"


@pytest.mark.parametrize("header", ["3600", "1000000", "0000061", "1" * 5000])
def test_hagstofa_429_handler_stops_the_pass_on_a_long_delay(monkeypatch, header):
    """AR-473 / R1872 (the R1634 shape): a declared lockout over RETRY_AFTER_STOP_S stops the pass,
    however many digits it is written with - never 'no header, retry in 11 s'."""
    waited = []
    monkeypatch.setattr(H, "_stat", lambda sess, key: None)
    monkeypatch.setattr(H, "_wait", lambda sess, secs, what: waited.append(secs))
    with pytest.raises(H.Throttled) as ei:
        H._wait_429(object(), _resp(header), "t")
    assert ei.value.stop_now and waited == [], (header[:12], waited)


class _SsbSess:
    def __init__(self):
        self.slept = []

    def budget_left_s(self):
        return None

    def wait(self, s):
        self.slept.append(s)


def test_ssb_429_handler_backs_off_on_a_garbage_header():
    sess = _SsbSess()
    for header in ("²", "-5"):
        S._on_429(sess, "t", _resp(header), 0)
    assert sess.slept == [61, 61], sess.slept
    S._on_429(sess, "t", _resp("7"), 0)
    assert sess.slept[-1] == 8, "positive control: a valid header is still honoured"
    S._on_429(sess, "t", _resp("000060"), 0)
    assert sess.slept[-1] == 61, "60 s (with leading zeros) is still waited, not a stop"


@pytest.mark.parametrize("header", ["3600", "1000000", "0000061", "1" * 5000])
def test_ssb_429_handler_stops_the_sweep_on_a_long_delay(header):
    """AR-473 / R1872 (the R1634 shape): base stopped on these; the first #256 head retried inside."""
    sess = _SsbSess()
    with pytest.raises(S.Throttled) as ei:
        S._on_429(sess, "t", _resp(header), 0)
    assert ei.value.stop_now and sess.slept == [], (header[:12], sess.slept)
