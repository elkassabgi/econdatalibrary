"""ssb pacing, end to end: the REAL update(), the REAL _get_meta/_post_data and the REAL
_PacedSession, with only the HTTP session, the clock/sleep and the JSON-stat parsing faked.

Ported from the adversarial review of this change (AR-351), which wrote them as probes; p2 failed
on the first draft because the stored note did not say the sweep had stopped.
"""
from __future__ import annotations

import datetime as dt
import os
import sys

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.strategies.fetchers import ssb  # noqa: E402


class _Resp:
    def __init__(self, code, body=None, headers=None):
        self.status_code, self._body, self.headers = code, body, headers or {}

    def json(self):
        if self._body is None:
            raise ValueError("no json")
        return self._body


META = {"variables": [{"code": "Tid", "values": ["2026"], "time": True}]}


class _FakeSession:
    """script(tid, method, n) -> _Resp. Records every request as (t_monotonic_fake, method, tid)."""
    def __init__(self, script, clock):
        self.script, self.clock, self.log, self.n = script, clock, [], {}

    def _go(self, method, url):
        tid = url.rsplit("/", 1)[-1]
        k = (tid, method)
        self.n[k] = self.n.get(k, 0) + 1
        self.log.append((self.clock.t, method, tid))
        return self.script(tid, method, self.n[k])

    def get(self, url, **kw):
        return self._go("GET", url)

    def post(self, url, **kw):
        return self._go("POST", url)


class _Clock:
    def __init__(self):
        self.t = 1000.0
        self.sleeps = []

    def monotonic(self):
        return self.t

    def sleep(self, s):
        self.sleeps.append(s)
        self.t += max(0.0, s)


def _wire(monkeypatch, tmp_path, script, groups=("Aa", "Bb", "Cc"), per_group=3, budget_min=40.0):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(ssb.config, "source_dir", lambda source: str(tmp_path))
    cat = [{"id": f"{g}{i}"} for g in groups for i in range(per_group)]
    monkeypatch.setattr(ssb, "_load_catalog", lambda out_dir: cat)
    for g in groups:
        p = tmp_path / f"grp_{g}.parquet"
        if not p.exists():
            pq.write_table(pa.table({"series_key": ["SSB:old:x=0"], "obs_date": pa.array([dt.date(2020, 1, 1)]),
                                     "value": [1.0]}), p)
    monkeypatch.setattr(ssb, "_per_table_max", lambda path: {})
    clock = _Clock()
    # one fake clock for the pacer, the Deadline and every sleep
    monkeypatch.setattr(ssb.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(ssb.time, "sleep", clock.sleep)
    monkeypatch.setattr(ssb, "BUDGET_MIN", budget_min)
    sess = _FakeSession(script, clock)
    monkeypatch.setattr(ssb.requests, "Session", lambda: sess)
    monkeypatch.setattr(ssb, "_time_var", lambda variables: ("Tid", ["2026"]))
    monkeypatch.setattr(ssb, "_newer_codes", lambda vals, floor: ["2026"])
    monkeypatch.setattr(ssb, "_build_query", lambda v, t, n: [{"code": "Tid"}])
    monkeypatch.setattr(ssb, "parse_jsonstat2",
                        lambda resp, tid, mt=None: [(f"SSB:{tid}:x=1", dt.date(2026, 12, 31), 5.0)])
    return sess, clock


def _cycle(tmp_path, groups=("Aa", "Bb", "Cc")):
    return ssb.RotationCycle(str(tmp_path), [f"grp_{g}.parquet" for g in groups])


def _ok(tid, method, n):
    return _Resp(200, META if method == "GET" else {"value": [5.0]})


def test_p1_every_request_start_is_spaced_including_get_to_post(monkeypatch, tmp_path):
    sess, clock = _wire(monkeypatch, tmp_path, _ok, groups=("Aa",), per_group=3)
    ssb.update(None, None)
    ts = [t for t, _, _ in sess.log]
    assert len(ts) == 6, sess.log                       # 3 x (GET + POST)
    gaps = [b - a for a, b in zip(ts, ts[1:])]
    assert min(gaps) >= ssb.MIN_INTERVAL_S - 1e-9, gaps
    assert ssb.MIN_INTERVAL_S >= 2.0


def test_p2_streak_stop_keeps_earlier_rows_and_releases_the_group(monkeypatch, tmp_path):
    def script(tid, method, n):
        if tid in ("Aa1", "Aa2") or tid.startswith(("Bb", "Cc")):
            return _Resp(429)
        return _ok(tid, method, n)
    sess, clock = _wire(monkeypatch, tmp_path, script)
    res = ssb.update(None, None)
    keys = pq.read_table(tmp_path / "grp_Aa.parquet").column("series_key").to_pylist()
    assert "SSB:Aa0:x=1" in keys, "rows fetched before the stop must be merged"
    asked = {tid for _, _, tid in sess.log}
    assert not any(t.startswith(("Bb", "Cc")) for t in asked), sorted(asked)
    cyc = _cycle(tmp_path)
    assert cyc.failing == {} and cyc.in_flight is None, (cyc.failing, cyc.in_flight)
    assert "grp_Aa.parquet" in cyc.unvisited()
    assert res.status == "partial"
    assert "throttled" in (res.error or "").lower(), res.error
    note = res.error or ""
    assert "sweep stopped" in note, note
    assert "grp_Bb.parquet" in note and "grp_Cc.parquet" in note, "the deferred groups must be named"
    # every 429 wait on the window's scale, never 1-4 s
    waits = [s for s in clock.sleeps if s > ssb.MIN_INTERVAL_S + 0.5]
    assert waits and min(waits) >= 15, clock.sleeps


def test_p3_retry_after_is_honoured_and_capped(monkeypatch, tmp_path):
    def script(tid, method, n):
        if tid == "Aa0" and method == "GET" and n == 1:
            return _Resp(429, headers={"Retry-After": "7"})
        if tid == "Aa1" and method == "GET" and n == 1:
            return _Resp(429, headers={"Retry-After": "86400"})
        return _ok(tid, method, n)
    sess, clock = _wire(monkeypatch, tmp_path, script, groups=("Aa",), per_group=2)
    res = ssb.update(None, None)
    big = [s for s in clock.sleeps if s > ssb.MIN_INTERVAL_S + 0.5]
    # Retry-After 7 is waited (8 s with the helper's +1). Retry-After 86400 is NOT waited and NOT
    # retried: it is longer than one window, so the sweep stops at once (second review, finding 2).
    assert big == [8], clock.sleeps
    assert [tid for _, m, tid in sess.log if tid == "Aa1"] == ["Aa1"], "Aa1 asked once, never retried"
    assert "sweep stopped" in (res.error or "")


def test_p4_wait_longer_than_budget_left_is_not_slept_and_is_a_deferral(monkeypatch, tmp_path):
    def script(tid, method, n):
        if tid == "Aa1":
            return _Resp(429)
        return _ok(tid, method, n)
    # 0.5 min = 30 s budget: first back-off 16 s fits, the second (31 s) does not
    sess, clock = _wire(monkeypatch, tmp_path, script, budget_min=0.5)
    res = ssb.update(None, None)
    assert all(s < 30 for s in clock.sleeps), clock.sleeps
    cyc = _cycle(tmp_path)
    assert cyc.failing == {} and "grp_Aa.parquet" in cyc.unvisited(), (cyc.failing, cyc.visited)
    keys = pq.read_table(tmp_path / "grp_Aa.parquet").column("series_key").to_pylist()
    assert "SSB:Aa0:x=1" in keys
    assert res.status == "partial"
    note = res.error or ""
    assert "an HTTP 429 wait was longer than the budget left" in note, note
    assert "min spent" not in note, "the budget was not spent: a 'budget N min spent' label is false"


def test_p5_a_single_throttled_table_does_not_stop_but_marks_the_group_failed(monkeypatch, tmp_path):
    """Documents finding 3: one stray throttled-out table counts the group FAILED (quarantine path)."""
    def script(tid, method, n):
        if tid == "Bb1":
            return _Resp(429)
        return _ok(tid, method, n)
    sess, clock = _wire(monkeypatch, tmp_path, script)
    ssb.update(None, None)
    asked = {tid for _, _, tid in sess.log}
    assert "Cc2" in asked, "a single throttled table must not stop the sweep"
    cyc = _cycle(tmp_path)
    # DOCUMENTED, not a bug fixed here: one stray throttled-out table is a named transient failure
    # and its group stays owed with one failure counted. Two such passes in a row quarantine the
    # group; quarantine still attempts it every pass and a clean visit clears the count.
    assert cyc.failing == {"grp_Bb.parquet": 1}, cyc.failing


def test_p6_streak_spanning_two_groups(monkeypatch, tmp_path):
    """Last table of Aa throttled, first of Bb throttled -> stop in Bb. Aa: failed (its table
    failed), Bb: released. Nothing after Bb0 asked."""
    def script(tid, method, n):
        if tid in ("Aa2", "Bb0"):
            return _Resp(429)
        return _ok(tid, method, n)
    sess, clock = _wire(monkeypatch, tmp_path, script)
    res = ssb.update(None, None)
    asked = [tid for _, _, tid in sess.log]
    assert "Bb1" not in asked and not any(t.startswith("Cc") for t in asked), asked
    cyc = _cycle(tmp_path)
    assert cyc.in_flight is None
    assert "grp_Bb.parquet" not in cyc.failing, cyc.failing
    assert cyc.failing == {"grp_Aa.parquet": 1}, "Aa's last table failed before the stop: Aa failed"
    assert "sweep stopped" in (res.error or "")


def test_p7_requests_per_60s_never_exceed_30(monkeypatch, tmp_path):
    def script(tid, method, n):
        if tid in ("Aa1",) and n <= 2:
            return _Resp(503)                      # 5xx retries go through the pacer too
        return _ok(tid, method, n)
    sess, clock = _wire(monkeypatch, tmp_path, script, groups=("Aa", "Bb", "Cc"), per_group=10)
    ssb.update(None, None)
    ts = [t for t, _, _ in sess.log]
    worst = max(sum(1 for u in ts if t <= u < t + 60) for t in ts)
    assert worst <= 30, worst
    assert worst <= 24, "2.5 s pacing allows at most 24 starts in any 60 s"
