"""ssb: request pacing and HTTP 429 handling.

Before (2026-09-25 .. 10-16): every weekly cloud run had 198-260 tables fail on HTTP 429 (260 of 338
on 10-16). The fetcher slept 0.12 s per TABLE (a table is a GET and often a POST), retried a 429 after
1, 2 and 4 s - inside SSB's 60-second window - ignored Retry-After, and kept sweeping after a streak.
SSB publishes "30 queries within 60 seconds", per IP.

These tests pin: every request (retries included) is paced; Retry-After is honoured in both forms;
the back-off without it is capped at one window; no wait is slept past the budget; and a 429 streak
stops the sweep with the rest DEFERRED and the cut group released, not failed.
No network: the HTTP session, the clock and the sleeps are fakes.
"""
from __future__ import annotations

import datetime as dt
import email.utils
import os
import sys
import time

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.strategies.fetchers import ssb  # noqa: E402


# --------------------------------------------------------------------------- fakes
class _Clock:
    def __init__(self):
        self.t = 1000.0
        self.slept = []

    def now(self):
        return self.t

    def sleep(self, s):
        self.slept.append(s)
        self.t += s


class _Resp:
    def __init__(self, status, headers=None, body=None):
        self.status_code = status
        self.headers = headers or {}
        self._body = body if body is not None else {"variables": []}

    def json(self):
        return self._body


class _Http:
    """Answers from a script of responses; records each call's clock time."""
    def __init__(self, clock, script):
        self.clock, self.script, self.calls = clock, list(script), []

    def _next(self, *a, **kw):
        self.calls.append(self.clock.now())
        return self.script.pop(0) if len(self.script) > 1 else self.script[0]

    get = post = _next


def _paced(script, interval=2.5, deadline=None):
    clk = _Clock()
    http = _Http(clk, script)
    s = ssb._PacedSession(interval, deadline=deadline, session=http, clock=clk.now, sleep=clk.sleep)
    return s, http, clk


# --------------------------------------------------------------------------- the pacer
def test_every_request_start_is_spaced_by_the_interval_and_the_first_is_not_delayed():
    s, http, clk = _paced([_Resp(200)])
    for _ in range(4):
        s.get("u")
    assert clk.slept == [2.5, 2.5, 2.5], clk.slept
    gaps = [b - a for a, b in zip(http.calls, http.calls[1:])]
    assert all(g >= 2.5 for g in gaps), gaps
    assert s.requests == 4


def test_time_already_spent_counts_toward_the_interval():
    s, http, clk = _paced([_Resp(200)])
    s.post("u")
    clk.t += 1.0                         # e.g. the response took a second to arrive
    s.post("u")
    assert clk.slept == [1.5]


def test_retries_go_through_the_pacer_too():
    """A 429 retried with no pause was the core of the defect: the retry is a request."""
    s, http, clk = _paced([_Resp(429, {"Retry-After": "0"}), _Resp(200, body={"ok": 1})], interval=2.5)
    assert ssb._get_meta(s, "T1") == {"ok": 1}
    assert s.requests == 2
    assert http.calls[1] - http.calls[0] >= 2.5


def test_the_interval_default_floor_and_ceiling():
    assert ssb._min_interval(None) == 2.5
    assert ssb._min_interval("") == 2.5
    assert ssb._min_interval("5") == 5.0
    assert ssb._min_interval("0.1") == 2.0, "an override may never go faster than 30 per minute"
    assert ssb._min_interval("abc") == 2.5
    assert ssb._min_interval("nan") == 2.5
    assert ssb._min_interval("inf") == 60.0
    assert 60 / ssb.MIN_INTERVAL_S <= 30, "the default must stay under 30 requests per minute"
    assert 60 / ssb.MIN_INTERVAL_S <= 0.8 * 30, "and keep a 20% margin under the documented limit"
    assert 60 / ssb.MIN_INTERVAL_FLOOR_S <= 30, "even the floor stays within the documented limit"


def test_update_builds_a_paced_session_at_min_interval(monkeypatch, tmp_path):
    seen = []
    _wire(monkeypatch, tmp_path, meta=lambda sess, tid: seen.append(sess) or None)
    ssb.update(None, None)
    assert seen and all(isinstance(s, ssb._PacedSession) for s in seen)
    assert seen[0].interval == ssb.MIN_INTERVAL_S


# --------------------------------------------------------------------------- 429 handling
def test_retry_after_seconds_is_honoured_not_the_backoff():
    s, http, clk = _paced([_Resp(429, {"Retry-After": "7"}), _Resp(200, body={"ok": 1})], interval=0.0)
    assert ssb._get_meta(s, "T1") == {"ok": 1}
    assert clk.slept == [8], clk.slept           # shared helper: Retry-After + 1


def test_retry_after_http_date_is_honoured():
    when = email.utils.format_datetime(dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=40),
                                       usegmt=True)
    s, http, clk = _paced([_Resp(429, {"Retry-After": when}), _Resp(200, body={"ok": 1})], interval=0.0)
    ssb._post_data(s, "T1", {})
    assert len(clk.slept) == 1 and 37 <= clk.slept[0] <= 42, clk.slept


def test_backoff_without_retry_after_is_window_scale_and_capped(monkeypatch):
    monkeypatch.setattr(ssb, "MAX_ATTEMPTS", 7)
    s, http, clk = _paced([_Resp(429)], interval=0.0)
    with pytest.raises(ssb.Throttled) as ei:
        ssb._get_meta(s, "T1")
    assert not ei.value.over_budget
    assert clk.slept == [16, 31, 61, 61, 61, 61], clk.slept     # 15, 30, 60, cap ... (+1 each)
    assert max(clk.slept) <= ssb.THROTTLE_CAP_S + 1
    assert s.requests == 7


def test_default_attempts_then_throttled_is_a_transient_error():
    s, http, clk = _paced([_Resp(429)], interval=0.0)
    with pytest.raises(ssb.TransientError):
        ssb._post_data(s, "T1", {})
    assert clk.slept == [16, 31, 61]


def test_a_wait_longer_than_the_budget_left_is_never_slept():
    class _DL:                                    # a real-shaped Deadline: 10 s left
        budget, t0 = 10.0, time.monotonic()
    s, http, clk = _paced([_Resp(429)], interval=0.0, deadline=_DL())
    with pytest.raises(ssb.Throttled) as ei:
        ssb._get_meta(s, "T1")
    assert ei.value.over_budget
    assert clk.slept == [], "a 16 s back-off with 10 s of budget left must not be slept"
    assert s.requests == 1


def test_a_retry_after_shorter_than_the_budget_is_slept():
    class _DL:
        budget, t0 = 600.0, time.monotonic()
    s, http, clk = _paced([_Resp(429, {"Retry-After": "3"}), _Resp(200, body={"ok": 1})],
                          interval=0.0, deadline=_DL())
    assert ssb._get_meta(s, "T1") == {"ok": 1}
    assert clk.slept == [4]


# --------------------------------------------------------------------------- the sweep
class _Deadline:
    def spent(self):
        return False

    def elapsed_min(self):
        return 1.0


def _wire(monkeypatch, tmp_path, meta, tables_per_group=3):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(ssb.config, "source_dir", lambda source: str(tmp_path))
    groups = ("Aa", "Bb", "Cc")
    cat = [{"id": f"{g}{i}"} for g in groups for i in range(tables_per_group)]
    monkeypatch.setattr(ssb, "_load_catalog", lambda out_dir: cat)
    monkeypatch.setattr(ssb, "_group_of", lambda tid: tid[:2])
    for g in groups:
        p = tmp_path / f"grp_{g}.parquet"
        if not p.exists():
            pq.write_table(pa.table({"series_key": ["x"], "obs_date": pa.array([dt.date(2026, 1, 1)]),
                                     "value": [1.0]}), p)
    monkeypatch.setattr(ssb, "_per_table_max", lambda path: {})
    monkeypatch.setattr(ssb, "Deadline", lambda minutes=None: _Deadline())
    monkeypatch.setattr(ssb, "RATE", 0)
    monkeypatch.setattr(ssb, "_get_meta", meta)


def _cycle(tmp_path):
    return ssb.RotationCycle(str(tmp_path), ["grp_Aa.parquet", "grp_Bb.parquet", "grp_Cc.parquet"])


def test_a_429_streak_stops_the_sweep_and_defers_the_rest(monkeypatch, tmp_path):
    asked = []

    def _meta(sess, tid):
        asked.append(tid)
        raise ssb.Throttled(f"ssb meta {tid}: HTTP 429 after 4 tries")
    _wire(monkeypatch, tmp_path, _meta)
    res = ssb.update(None, None)
    assert asked == ["Aa0", "Aa1"], "after 2 throttled tables in a row nothing more is requested"
    assert res.status == "partial"
    note = res.error or ""
    assert "Aa0: metadata" in note and "Aa1: metadata" in note, "the throttled tables stay NAMED failures"
    # Aa2 (rest of the cut group) + grp_Bb + grp_Cc = 3 deferrals, said in the note
    assert "sweep stopped: SSB throttled" in note and "3 sub-unit(s) deferred" in note, note
    cyc = _cycle(tmp_path)
    assert cyc.failing == {}, "a throttle is SSB's state: it must not move a group toward quarantine"
    assert "grp_Aa.parquet" in cyc.unvisited() and cyc.in_flight is None


def test_one_throttled_table_then_an_answer_does_not_stop_the_sweep(monkeypatch, tmp_path):
    asked = []

    def _meta(sess, tid):
        asked.append(tid)
        if tid in ("Aa0", "Bb1"):                 # two throttles, never in a row
            raise ssb.Throttled(f"ssb meta {tid}: HTTP 429 after 4 tries")
        return None
    _wire(monkeypatch, tmp_path, _meta)
    res = ssb.update(None, None)
    assert len(asked) == 9, asked
    assert "SSB throttled" not in (res.error or "")
    assert "Aa0: metadata" in (res.error or ""), "a throttled table is still a NAMED transient failure"


def test_other_transient_failures_break_the_streak(monkeypatch, tmp_path):
    asked = []

    def _meta(sess, tid):
        asked.append(tid)
        if tid in ("Aa0", "Aa2"):
            raise ssb.Throttled(f"ssb meta {tid}: HTTP 429 after 4 tries")
        if tid == "Aa1":
            raise ssb.TransientError(f"ssb meta {tid}: HTTP 503")
        return None
    _wire(monkeypatch, tmp_path, _meta)
    ssb.update(None, None)
    assert len(asked) == 9, asked


def test_a_wait_past_the_budget_stops_the_sweep_as_a_deferral_not_a_failure(monkeypatch, tmp_path):
    asked = []

    def _meta(sess, tid):
        asked.append(tid)
        if tid == "Aa1":
            raise ssb.Throttled(f"ssb meta {tid}: HTTP 429, wait 61 s is longer than the 20 s budget left",
                                over_budget=True)
        return None
    _wire(monkeypatch, tmp_path, _meta)
    res = ssb.update(None, None)
    assert asked == ["Aa0", "Aa1"]
    note = res.error or ""
    assert res.status == "partial" and "none failed" in note, note
    assert "Aa1: metadata" in note and "table deferred" in note
    assert "grp_Bb.parquet: an HTTP 429 wait was longer than the budget left" in note
    cyc = _cycle(tmp_path)
    assert cyc.failing == {} and "grp_Aa.parquet" in cyc.unvisited()


def test_a_throttle_on_the_data_post_counts_toward_the_streak(monkeypatch, tmp_path):
    """Aa0's GET answers and its POST is throttled; Aa1's GET is throttled: two in a row -> stop."""
    asked, posted = [], []
    good_meta = {"variables": [{"code": "Tid", "time": True, "values": ["2026M08"]}]}

    def _meta(sess, tid):
        asked.append(tid)
        if tid == "Aa1":
            raise ssb.Throttled(f"ssb meta {tid}: HTTP 429 after 4 tries")
        return good_meta
    _wire(monkeypatch, tmp_path, _meta)
    monkeypatch.setattr(ssb, "_time_var", lambda variables: ("Tid", ["2026M08"]))
    monkeypatch.setattr(ssb, "_newer_codes", lambda vals, floor: ["2026M08"])
    monkeypatch.setattr(ssb, "_build_query", lambda variables, tc, newer: [{"code": "Tid"}])

    def _post(sess, tid, body):
        posted.append(tid)
        raise ssb.Throttled(f"ssb data {tid}: HTTP 429 after 4 tries")
    monkeypatch.setattr(ssb, "_post_data", _post)
    res = ssb.update(None, None)
    assert asked == ["Aa0", "Aa1"] and posted == ["Aa0"], (asked, posted)
    assert "sweep stopped: SSB throttled" in (res.error or "")


def test_an_answered_get_breaks_a_post_throttle_streak(monkeypatch, tmp_path):
    """Control for the test above: every GET answers, every POST is throttled. A GET that answers is
    SSB answering, so the streak never reaches 2 and every table is tried."""
    posted = []
    _wire(monkeypatch, tmp_path, lambda sess, tid: {"variables": [{"code": "Tid", "time": True,
                                                                   "values": ["2026M08"]}]})
    monkeypatch.setattr(ssb, "_time_var", lambda variables: ("Tid", ["2026M08"]))
    monkeypatch.setattr(ssb, "_newer_codes", lambda vals, floor: ["2026M08"])
    monkeypatch.setattr(ssb, "_build_query", lambda variables, tc, newer: [{"code": "Tid"}])

    def _post(sess, tid, body):
        posted.append(tid)
        raise ssb.Throttled(f"ssb data {tid}: HTTP 429 after 4 tries")
    monkeypatch.setattr(ssb, "_post_data", _post)
    ssb.update(None, None)
    assert len(posted) == 9


# --------------------------------------------------------------------------- review AR-351 follow-ups
def test_request_timeouts_are_cut_to_the_budget_left_but_not_below_the_floor():
    class _DL:
        budget, t0 = 30.0, time.monotonic()
    seen = []

    class _Http:
        def get(self, url, **kw):
            seen.append(kw["timeout"])
            return _Resp(200, body={"ok": 1})
    s = ssb._PacedSession(0.0, deadline=_DL(), session=_Http())
    s.get("u", timeout=60)
    assert 25 <= seen[-1] <= 30, seen
    _DL.t0 = time.monotonic() - 29.5                      # half a second of budget left
    s.get("u", timeout=60)
    assert seen[-1] == ssb.REQUEST_TIMEOUT_FLOOR_S, seen
    s2 = ssb._PacedSession(0.0, deadline=None, session=_Http())
    s2.get("u", timeout=60)
    assert seen[-1] == 60, "without a real deadline the timeout is untouched"


def test_a_post_answered_403_is_a_named_failure_not_an_empty_table():
    s, http, clk = _paced([_Resp(403)], interval=0.0)
    with pytest.raises(ssb.TransientError) as ei:
        ssb._post_data(s, "T1", {})
    assert "HTTP 403" in str(ei.value)[-55:], "the label the note keeps must still say 403"
    assert s.requests == 1, "a 403 is not retried"


def test_a_post_answered_400_is_still_legitimately_empty():
    s, http, clk = _paced([_Resp(400)], interval=0.0)
    assert ssb._post_data(s, "T1", {}) is None
