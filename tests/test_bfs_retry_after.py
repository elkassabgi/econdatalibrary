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


# ---- review AR-471: the boundaries the new reader creates (R1870, R1634) ---------------------------

def _http_date(seconds_from_now, tz):
    when = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=seconds_from_now)
    return email.utils.format_datetime(when.astimezone(tz) if tz else when.replace(tzinfo=None))


@BOTH
@pytest.mark.parametrize("hostile", ["²", "9" * 5000, "١٢"])   # '²', 5,000 digits, Arabic-Indic 12
def test_a_hostile_digit_header_never_crashes(slept, kind, hostile):
    """isdigit() is True for all three; int() raised ValueError on the first two and ended the pass."""
    s = _Sess([_Resp(429, headers={"Retry-After": hostile}), _Resp(200, {"ok": 1})])
    if len(hostile) > 6 and hostile.isascii():
        with pytest.raises(TransientError):          # a huge ASCII value is over the cap: give up
            _call(kind, s)
        assert slept == [] and s.calls == 1
    else:
        assert _call(kind, s) == {"ok": 1}           # not ASCII digits: no usable header, old back-off
        assert slept == [1], slept


@BOTH
def test_seven_ascii_digits_are_over_the_cap(slept, kind):
    s = _Sess([_Resp(429, headers={"Retry-After": "1234567"}), _Resp(200, {"ok": 1})])
    with pytest.raises(TransientError):
        _call(kind, s)
    assert slept == [] and s.calls == 1


@BOTH
def test_a_saturated_value_is_over_the_cap_and_stops(slept, kind):
    """The shared parser saturates > 6 significant digits; bfs must read that as over its cap."""
    from updater.strategies.fetchers import _common
    assert _common.RETRY_AFTER_SATURATED_S > B.RETRY_AFTER_CAP_S
    s = _Sess([_Resp(429, headers={"Retry-After": "1" + "0" * 40}), _Resp(200, {"ok": 1})])
    with pytest.raises(B.RetryAfterStop):
        _call(kind, s)
    assert slept == [] and s.calls == 1


@BOTH
@pytest.mark.parametrize("raw,want", [("000060", [61]), ("0000061", None)])
def test_leading_zeros_do_not_move_the_cap(slept, kind, raw, want):
    s = _Sess([_Resp(429, headers={"Retry-After": raw}), _Resp(200, {"ok": 1})])
    if want is None:
        with pytest.raises(B.RetryAfterStop):
            _call(kind, s)
        assert slept == []
    else:
        assert _call(kind, s) == {"ok": 1} and slept == want


@BOTH
def test_a_broken_headers_object_falls_back(slept, kind):
    class Boom:
        def get(self, k):
            raise RuntimeError("headers object is broken")
    s = _Sess([_Resp(429), _Resp(200, {"ok": 1})])
    s.responses[0].headers = Boom()
    assert _call(kind, s) == {"ok": 1}
    assert slept == [1], slept


@BOTH
@pytest.mark.parametrize("tz", [None, dt.timezone(dt.timedelta(hours=1)),
                                dt.timezone(dt.timedelta(hours=-5))])
def test_http_date_naive_and_offset_forms(slept, kind, tz):
    """'-0000' parses tz-naive (read as UTC); '+0100' / '-0500' parse tz-aware."""
    s = _Sess([_Resp(429, headers={"Retry-After": _http_date(30, tz)}), _Resp(200, {"ok": 1})])
    assert _call(kind, s) == {"ok": 1}
    assert len(slept) == 1 and 25 <= slept[0] <= 32, (tz, slept)


@BOTH
def test_a_past_http_date_waits_the_floor_never_negative(slept, kind):
    s = _Sess([_Resp(429, headers={"Retry-After": _http_date(-600, dt.timezone.utc)}), _Resp(200, {"ok": 1})])
    assert _call(kind, s) == {"ok": 1}
    assert slept == [2], slept                       # max(raw, 1) + 1, never a negative sleep


@BOTH
def test_one_over_the_cap_gives_up(slept, kind):
    s = _Sess([_Resp(429, headers={"Retry-After": str(B.RETRY_AFTER_CAP_S + 1)}), _Resp(200, {})])
    with pytest.raises(TransientError):
        _call(kind, s)
    assert slept == [] and s.calls == 1


# ---- the per-pass Retry-After budget ---------------------------------------------------------------

@BOTH
def test_budget_allows_a_wait_equal_to_what_is_left(slept, kind):
    s = _Sess([_Resp(429, headers={"Retry-After": "4"}), _Resp(200, {"ok": 1})])
    s._bfs_ra_left = 5
    assert _call(kind, s) == {"ok": 1}
    assert slept == [5] and s._bfs_ra_left == 0


@BOTH
def test_budget_refuses_a_wait_longer_than_what_is_left(slept, kind):
    s = _Sess([_Resp(429, headers={"Retry-After": "4"}), _Resp(200, {"ok": 1})])
    s._bfs_ra_left = 4.9
    with pytest.raises(B.RetryAfterBudgetSpent):
        _call(kind, s)
    assert slept == [] and s.calls == 1


@BOTH
def test_budget_is_charged_across_requests(slept, kind):
    s = _Sess([_Resp(429, headers={"Retry-After": "4"}), _Resp(200, {"ok": 1}),
               _Resp(429, headers={"Retry-After": "4"}), _Resp(200, {"ok": 1}),
               _Resp(429, headers={"Retry-After": "4"})])
    s._bfs_ra_left = 12
    assert _call(kind, s) == {"ok": 1} and _call(kind, s) == {"ok": 1}
    with pytest.raises(B.RetryAfterBudgetSpent):
        _call(kind, s)
    assert slept == [5, 5], slept


@BOTH
def test_negative_control_the_old_back_off_is_not_charged(slept, kind):
    s = _Sess([_Resp(429), _Resp(503), _Resp(502), _Resp(200, {"ok": 1})])
    s._bfs_ra_left = 0
    assert _call(kind, s) == {"ok": 1}
    assert slept == [1, 2, 4]


@pytest.mark.parametrize("env,want", [(None, 900.0), ("45", 900.0), ("30", 600.0),
                                      ("180", 3600.0), ("garbage", 900.0)])
def test_budget_is_a_third_of_the_unit_timeout(monkeypatch, env, want):
    if env is None:
        monkeypatch.delenv("AQUEDUCT_UNIT_TIMEOUT_MIN", raising=False)
    else:
        monkeypatch.setenv("AQUEDUCT_UNIT_TIMEOUT_MIN", env)
    assert B._retry_after_pass_budget_s() == want


# ---- update(): a budget stop defers the rest, still merges, and rotates -----------------------------

D0 = dt.date(2025, 12, 31)


class _PassSess:
    """GET catalog / GET meta / POST data. Tables in `throttled` answer every meta GET with 429."""

    def __init__(self, tables, throttled, header, on="get"):
        self.tables, self.throttled, self.header, self.on = tables, set(throttled), header, on
        self.meta_order = []
        self.post_order = []

    def get(self, url, headers=None, timeout=None):
        if url == f"{B.BASE_EN}/":
            return _Resp(200, [{"dbid": t} for t in self.tables])
        dbid = url.rstrip("/").split("/")[-2]
        self.meta_order.append(dbid)
        if self.on == "get" and dbid in self.throttled:
            return _Resp(429, headers=self.header)
        return _Resp(200, {"variables": [{"code": "Jahr", "time": True}]})

    def post(self, url, json=None, headers=None, timeout=None):
        dbid = url.rstrip("/").split("/")[-2]
        self.post_order.append(dbid)
        if self.on == "post" and dbid in self.throttled:
            return _Resp(429, headers=self.header)
        return _Resp(200, {"dbid": dbid})


@pytest.fixture
def pass_env(monkeypatch, tmp_path, slept):
    store = {}
    merged = []
    monkeypatch.setattr(B, "RATE", 0)
    monkeypatch.setattr(B.config, "source_dir", lambda src: str(tmp_path / src))
    monkeypatch.setattr(B.blob, "row_count", lambda p: 0)
    monkeypatch.setattr(B, "_dbid_max", lambda p: {})
    monkeypatch.setattr(B, "_build_query", lambda variables, since_max: (["q"], 1))
    monkeypatch.setattr(B, "parse_jsonstat2_bfs", lambda resp, prefix, tc: [(f"{prefix}:x=1", D0, 1.0)])
    monkeypatch.setattr(B.blob, "read_bytes", lambda p: store.get(p.replace("\\", "/").split("/")[-1]))
    monkeypatch.setattr(B.blob, "write_bytes_atomic",
                        lambda p, b: store.__setitem__(p.replace("\\", "/").split("/")[-1], b))

    def fake_merge(path, tbl, **kw):
        merged.append(tbl.column("series_key").to_pylist())
        return tbl.num_rows, D0
    monkeypatch.setattr(B.merge, "merge_and_write", fake_merge)

    def go(sess):
        monkeypatch.setattr(B.requests, "Session", lambda: sess)
        return B.update(unit=None, since=None)
    go.store, go.merged = store, merged
    go.waits = lambda: [x for x in slept if x]      # RATE is 0 here; keep only the real waits
    return go


TABLES = [f"T{i}" for i in range(10)]


def test_a_budget_stop_defers_the_rest_and_still_merges(monkeypatch, pass_env):
    monkeypatch.setenv("AQUEDUCT_UNIT_TIMEOUT_MIN", "6")          # budget 120 s
    s = _PassSess(TABLES, TABLES[2:], {"Retry-After": "60"})
    res = pass_env(s)
    assert s.meta_order == ["T0", "T1", "T2", "T2"], s.meta_order   # stopped on T2's 2nd answer
    assert pass_env.waits() == [61], pass_env.waits()                 # 61 <= 120; 61 more > 59 left
    assert sum(pass_env.waits()) <= 120
    assert pass_env.merged == [["BFS:T0:x=1", "BFS:T1:x=1"]]          # what was fetched merged
    assert res.status == "partial"
    assert res.error.startswith("1/3 sub-unit(s) transient-failed"), res.error
    assert "T2: metadata GET stopped" in res.error and "7 later table(s) deferred" in res.error
    assert b'"after": "T2"' in pass_env.store["_rotation.json"]


def test_a_budget_stop_on_the_data_post_defers_the_rest_too(monkeypatch, pass_env):
    monkeypatch.setenv("AQUEDUCT_UNIT_TIMEOUT_MIN", "6")
    s = _PassSess(TABLES, TABLES[2:], {"Retry-After": "60"}, on="post")
    res = pass_env(s)
    assert s.meta_order == ["T0", "T1", "T2"] and s.post_order == ["T0", "T1", "T2", "T2"]
    assert pass_env.merged == [["BFS:T0:x=1", "BFS:T1:x=1"]]
    assert "T2: data POST stopped" in res.error and "7 later table(s) deferred" in res.error
    assert b'"after": "T2"' in pass_env.store["_rotation.json"]


def test_a_retry_after_over_the_cap_stops_the_pass_at_once(monkeypatch, pass_env):
    """'Retry-After: 3600' on every answer: one request, no sleep, the rest deferred - not 654
    more requests inside the window the server asked us to keep out of (ssb #177's escalation)."""
    s = _PassSess(TABLES, TABLES[2:], {"Retry-After": "3600"})
    res = pass_env(s)
    assert s.meta_order == ["T0", "T1", "T2"] and pass_env.waits() == []
    assert pass_env.merged == [["BFS:T0:x=1", "BFS:T1:x=1"]]
    assert "Retry-After 3600 s" in res.error and "7 later table(s) deferred" in res.error


def test_the_pass_after_a_stop_starts_at_the_first_deferred_table(monkeypatch, pass_env):
    pass_env.store["_rotation.json"] = b'{"after": "T2"}'
    s = _PassSess(TABLES, [], {})
    res = pass_env(s)
    assert s.meta_order == TABLES[3:] + TABLES[:3], s.meta_order
    assert res.status == "ok"
    assert "_rotation.json" in pass_env.store and pass_env.store["_rotation.json"] == b'{"after": "T2"}'


def test_negative_control_no_header_never_stops_the_pass(monkeypatch, pass_env):
    monkeypatch.setenv("AQUEDUCT_UNIT_TIMEOUT_MIN", "6")
    s = _PassSess(TABLES, TABLES[2:], {})
    res = pass_env(s)
    assert [t for t in TABLES if t in s.meta_order] == TABLES        # every table attempted
    assert "deferred" not in res.error
    assert res.error.startswith("8/10 sub-unit(s) transient-failed"), res.error
    assert "_rotation.json" not in pass_env.store                     # no stop, no bookmark


def test_the_worst_case_pass_is_bounded_by_the_budget(monkeypatch, pass_env):
    """R1870: state the bound PER PASS. Every answer 'Retry-After: 60' on 654 tables, default timeout."""
    monkeypatch.delenv("AQUEDUCT_UNIT_TIMEOUT_MIN", raising=False)
    tables = [f"T{i}" for i in range(654)]
    s = _PassSess(tables, tables, {"Retry-After": "60"})
    res = pass_env(s)
    assert sum(pass_env.waits()) <= 900, sum(pass_env.waits())
    # 4 tables x 4 requests (3 waits of 61 s) = 732 s, then 2 more waits on the 5th (854 s) and a
    # third would pass the 900 s budget: 19 requests in all, instead of 654 x 4.
    assert len(s.meta_order) == 19, len(s.meta_order)
    assert res.status == "partial" and "later table(s) deferred" in res.error
