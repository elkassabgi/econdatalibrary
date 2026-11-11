"""bfs honours Retry-After on HTTP 429/503, with a cap, and keeps its old back-off otherwise.

WHY. bfs._get_json and bfs._post_json slept min(2 ** attempt, 30) on a 429 and never read the
Retry-After header, so a throttled request was retried after 1, 2 and 4 s - inside the window
the server had just asked us to wait. The pattern is hagstofa's (#178): read both RFC 9110 forms,
wait what the server asks up to a cap, give the request up at once when it asks for longer.

No request leaves the machine: the session is a fake that replays canned responses, and
time.sleep is recorded instead of slept.
"""
from __future__ import annotations

import datetime as dt
import email.utils

import pytest

from updater.errors import TransientError
from updater.strategies.fetchers import bfs as B


class _Resp:
    def __init__(self, status, payload=None, headers=None):
        self.status_code = status
        self._payload = payload
        self.headers = headers or {}

    def json(self):
        if self._payload is None:
            raise ValueError("no JSON")
        return self._payload


class _Sess:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    def _next(self):
        self.calls += 1
        return self.responses.pop(0)

    def get(self, url, headers=None, timeout=None):
        return self._next()

    def post(self, url, json=None, headers=None, timeout=None):
        return self._next()


@pytest.fixture
def slept(monkeypatch):
    out = []
    monkeypatch.setattr(B.time, "sleep", lambda s: out.append(s))
    return out


def _call(kind, sess):
    return B._get_json(sess, "https://x/u") if kind == "get" else B._post_json(sess, "https://x/u", {})


BOTH = pytest.mark.parametrize("kind", ["get", "post"])


@BOTH
def test_retry_after_seconds_is_waited(slept, kind):
    s = _Sess([_Resp(429, headers={"Retry-After": "7"}), _Resp(200, {"ok": 1})])
    assert _call(kind, s) == {"ok": 1}
    assert slept == [8], slept                    # the shared helper resumes 1 s after the window


@BOTH
def test_retry_after_http_date_is_waited(slept, kind):
    when = email.utils.format_datetime(dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=30),
                                       usegmt=True)
    s = _Sess([_Resp(429, headers={"Retry-After": when}), _Resp(200, {"ok": 1})])
    assert _call(kind, s) == {"ok": 1}
    assert len(slept) == 1 and 25 <= slept[0] <= 32, slept


@BOTH
def test_503_retry_after_is_waited_too(slept, kind):
    s = _Sess([_Resp(503, headers={"Retry-After": "5"}), _Resp(200, {"ok": 1})])
    assert _call(kind, s) == {"ok": 1}
    assert slept == [6], slept


@BOTH
def test_missing_header_keeps_the_old_back_off(slept, kind):
    s = _Sess([_Resp(429), _Resp(429), _Resp(429), _Resp(200, {"ok": 1})])
    assert _call(kind, s) == {"ok": 1}
    assert slept == [1, 2, 4], slept


@BOTH
@pytest.mark.parametrize("garbage", ["soon", "-5", "12.5", "Thu, 99 Foo 2026"])
def test_garbage_header_keeps_the_old_back_off(slept, kind, garbage):
    s = _Sess([_Resp(429, headers={"Retry-After": garbage}), _Resp(200, {"ok": 1})])
    assert _call(kind, s) == {"ok": 1}
    assert slept == [1], (garbage, slept)


@BOTH
def test_retry_after_on_another_status_is_ignored(slept, kind):
    """Retry-After is read only on 429/503; a 502 keeps the old back-off whatever it carries."""
    s = _Sess([_Resp(502, headers={"Retry-After": "40"}), _Resp(200, {"ok": 1})])
    assert _call(kind, s) == {"ok": 1}
    assert slept == [1], slept


@BOTH
def test_over_the_cap_gives_up_at_once(slept, kind):
    s = _Sess([_Resp(429, headers={"Retry-After": "3600"}), _Resp(200, {"ok": 1})])
    with pytest.raises(TransientError) as e:
        _call(kind, s)
    assert slept == [] and s.calls == 1, (slept, s.calls)
    assert "Retry-After 3600 s" in str(e.value)


@BOTH
def test_negative_control_at_the_cap_is_waited(slept, kind):
    s = _Sess([_Resp(429, headers={"Retry-After": str(B.RETRY_AFTER_CAP_S)}), _Resp(200, {"ok": 1})])
    assert _call(kind, s) == {"ok": 1}
    assert slept == [B.RETRY_AFTER_CAP_S + 1], slept


@BOTH
def test_retry_budget_is_unchanged(slept, kind):
    """Four attempts, three waits, then TransientError - as before."""
    s = _Sess([_Resp(429, headers={"Retry-After": "2"}) for _ in range(4)])
    with pytest.raises(TransientError):
        _call(kind, s)
    assert s.calls == B.MAX_ATTEMPTS and slept == [3, 3, 3], (s.calls, slept)
