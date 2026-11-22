"""The statcan lane's strike rule (2026-11-22, ledger R2026; design review R2027).

On 2026-11-18 an edge refusal of about an hour (HTTP 403 on every zip) quarantined 35 cubes in 16 minutes, and
pre-release zips served for minutes quarantined 9 more by the count gate; getAllCubesListLite answered 403 for ~19 h
before that and killed the lane 210 times. These tests pin the four changes - spaced strikes, "not now" codes
transient, an enumeration that cannot end the process, a per-iteration circuit breaker - and replay both events
END TO END: the network is a fake www150 answering by a fake clock, and the lane's own _call / _download / parse /
gates / merge run for real (only the network and the bucket are faked, as in tests/test_statcan_lane.py).
"""
from __future__ import annotations

import datetime as dt
import io
import json
import os
import re
import sys
import types

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
pytest.importorskip("duckdb")
import requests  # noqa: E402
from requests.structures import CaseInsensitiveDict  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import jobs.statcan_lane as lane  # noqa: E402
from updater import merge  # noqa: E402
from updater.errors import DefinitiveError, TransientError  # noqa: E402
from updater.strategies.fetchers import statcan as sc  # noqa: E402
from tests.test_statcan_lane import (PID, REL, STORED_2013, _Bucket, _heartbeat, _history_world,  # noqa: E402,F401
                                     _net, _no_network, _row, _state, _stored, _zip, world)

UTC = dt.timezone.utc
EDGE = {"server": "cloudflare", "cf-ray": "8e00000000000abc-YYZ"}
IP = "203.0.113.7"
BLOCK = f"<html><title>Attention Required!</title> Your IP: {IP} </html>".encode()
OLD_LM = "Mon, 19 Oct 2026 12:30:41 GMT"
NEW_LM = "Mon, 16 Nov 2026 13:30:50 GMT"
# the 35 class-A cubes of 2026-11-18 (report_table.md) and 4 more of that release (39 owed at 14:28Z)
CLASS_A = ("10100111 10100141 14100004 14100005 14100006 14100007 14100008 14100009 14100010 14100011 "
           "14100323 14100343 14100344 14100346 14100453 14100454 14100455 14100456 18100205 34100143 "
           "34100145 34100148 34100149 34100150 34100151 34100152 34100154 34100155 34100156 34100157 "
           "34100158 34100159 34100160 34100161 34100162 17100061 17100062 17100063 17100064").split()
CLASS_B = ("18100001", "18100004", "18100006", "18100256")


def _t(s):
    return dt.datetime.strptime(s, "%Y-%m-%dT%H:%MZ").replace(tzinfo=UTC)


def _z(t):
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t

    def step(self, minutes=5):
        self.t += dt.timedelta(minutes=minutes)


@pytest.fixture
def clock(monkeypatch):
    c = Clock(_t("2026-11-17T18:15Z"))
    monkeypatch.setattr(lane, "_now", c)                     # the lane's beats and stamps use the same clock
    monkeypatch.setattr(sc, "time", types.SimpleNamespace(sleep=lambda s: None))     # no real waits
    return c


class _Resp:
    def __init__(self, status, body=b"", obj=None, headers=None):
        self.status_code = status
        self.content = body
        self._obj = obj
        self.headers = CaseInsensitiveDict(headers or {})

    def json(self):
        if self._obj is None:
            raise ValueError("not json")
        return self._obj

    def iter_content(self, chunk_size=1):
        for i in range(0, len(self.content), chunk_size or 1):
            yield self.content[i:i + chunk_size]

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _rows_for(periods):
    return [_row(p, "Canada", v, coord, str(float(i + k)))
            for i, p in enumerate(periods) for k, (v, coord) in enumerate((("v1", "1.1"), ("v2", "1.2")))]


class StatCan:
    """A fake www150 (WDS + the zip host), answering by the clock. Each cube: its release, the rows its zip holds
    before and after `rebuilt` (a pre-release copy served until then), and getCubeMetadata's counts of the new rows."""

    def __init__(self, clock):
        self.clock = clock
        self.cubes = {}
        self.lite_status = lambda t: 200
        self.zip_status = lambda pid, t: 200
        self.calls = []                                     # (time, kind, pid, status)

    def add(self, pid, release, old=("2026-08", "2026-09"), new=("2026-08", "2026-09", "2026-10"), rebuilt=None,
            end="2026-10-01"):
        self.cubes[str(pid)] = {"release": release, "old": _rows_for(old), "new": _rows_for(new),
                                "rebuilt": rebuilt, "end": end}

    def lite(self):
        out = [{"productId": 90000000 + i, "releaseTime": "2020-01-01T08:30", "cubeEndDate": "2019-12-01",
                "frequencyCode": 6} for i in range(1000)]
        out += [{"productId": int(p), "releaseTime": c["release"], "cubeEndDate": c["end"], "frequencyCode": 6}
                for p, c in self.cubes.items()]
        return out

    def _zip_rows(self, pid):
        c = self.cubes[pid]
        if c["rebuilt"] is not None and self.clock.t < c["rebuilt"]:
            return c["old"], OLD_LM
        return c["new"], NEW_LM

    def get(self, url, headers=None, timeout=None, stream=False, **k):
        t = self.clock.t
        if url.endswith("/getAllCubesListLite"):
            st = self.lite_status(t)
            self.calls.append((t, "lite", None, st))
            return _Resp(200, obj=self.lite()) if st == 200 else _Resp(st, BLOCK, headers=EDGE)
        m = re.search(r"/getFullTableDownloadCSV/(\d+)/en$", url)
        if m:
            self.calls.append((t, "table", m.group(1), 200))
            return _Resp(200, obj={"status": "SUCCESS", "object": f"https://zips.example.invalid/{m.group(1)}-eng.zip"})
        m = re.search(r"zips\.example\.invalid/(\d+)-eng\.zip$", url)
        if m:
            pid = m.group(1)
            st = self.zip_status(pid, t)
            self.calls.append((t, "zip", pid, st))
            if st != 200:
                return _Resp(st, BLOCK, headers=EDGE)
            rows, lm = self._zip_rows(pid)
            buf = io.BytesIO()
            _zip(buf, rows, pid=pid)
            body = buf.getvalue()
            return _Resp(200, body, headers=dict(EDGE, **{"Content-Length": str(len(body)), "Last-Modified": lm}))
        raise AssertionError(f"unexpected GET {url}")

    def post(self, url, json=None, headers=None, timeout=None, **k):
        assert url.endswith("/getCubeMetadata"), url
        pid = str(json[0]["productId"])
        rows = self.cubes[pid]["new"]
        self.calls.append((self.clock.t, "meta", pid, 200))
        return _Resp(200, obj=[{"status": "SUCCESS", "object": {
            "nbDatapointsCube": len(rows), "nbSeriesCube": len({r[7] for r in rows})}}])


@pytest.fixture
def statcan(clock, monkeypatch):
    s = StatCan(clock)
    monkeypatch.setattr(requests, "get", s.get)
    monkeypatch.setattr(requests, "post", s.post)
    return s


def _seed(world, pids, prev, last_enum, extra=None):
    """A lane state where every pid is merged and served at its previous release (the record before the event)."""
    st = {"version": 1, "floor": lane.FLOOR, "cubes": {}, "parsed_rows": {}, "new_cubes": {},
          "last_enum_utc": last_enum}
    for p in pids:
        _stored(world / f"{p}.parquet", [("v1", "2026-08-01", 0.0, "Canada", "1.1"),
                                         ("v2", "2026-08-01", 1.0, "Canada", "1.2"),
                                         ("v1", "2026-09-01", 1.0, "Canada", "1.1"),
                                         ("v2", "2026-09-01", 2.0, "Canada", "1.2")])
        st["cubes"][p] = {"release": prev, "merged": prev, "served": prev}
    st["cubes"].update(extra or {})
    (world / "_lane_state.json").write_text(json.dumps(st))


def _progress(world):
    return json.loads((world / "_lane_progress.json").read_text())


def _verdict_or_red(world, now):
    try:
        return sc.verdict(_state(world), _progress(world), now)[1]
    except DefinitiveError as e:
        return "RED " + str(e)


# --------------------------------------------------------------------------- #
# item 2 - "not now" codes are transient, in both helpers; item 5 - what a refusal logs
# --------------------------------------------------------------------------- #
def test_the_not_now_set_is_the_briefs_and_both_helpers_use_it():
    want = {401, 403, 408, 425, 520, 521, 522, 523, 524, 525, 526, 530}
    assert want <= sc.TRANSIENT_HTTP and {429, 500, 502, 503, 504} <= sc.TRANSIENT_HTTP
    assert not ({400, 404, 410} & sc.TRANSIENT_HTTP)


@pytest.mark.parametrize("code", sorted(sc.TRANSIENT_HTTP))
@pytest.mark.parametrize("method", ["GET", "POST"])
def test_call_books_every_not_now_code_transient_after_its_tries(code, method, monkeypatch, capsys):
    calls = []

    def answer(url, *a, **k):
        calls.append(url)
        return _Resp(code, BLOCK, headers=EDGE)
    monkeypatch.setattr(requests, "get" if method == "GET" else "post", answer)
    monkeypatch.setattr(sc, "time", types.SimpleNamespace(sleep=lambda s: None))
    with pytest.raises(TransientError) as ei:
        sc._get("getAllCubesListLite") if method == "GET" else sc._post("getCubeMetadata", [{"productId": 1}])
    assert len(calls) == 5 and ei.value.http_status == code
    msg = str(ei.value)
    assert f"HTTP {code}" in msg and "'cloudflare'" in msg and "YYZ" in msg
    assert IP not in msg, "body bytes must never reach an exception text (design review F7)"
    out = capsys.readouterr().out
    assert out.count(f"HTTP {code} (try ") == 5 and "transient" in out and IP in out, out


@pytest.mark.parametrize("code", [400, 404, 410])
def test_call_keeps_other_codes_definitive_at_once(code, monkeypatch, capsys):
    calls = []
    monkeypatch.setattr(requests, "get", lambda url, *a, **k: calls.append(url) or _Resp(code, BLOCK, headers=EDGE))
    with pytest.raises(DefinitiveError) as ei:
        sc._get("getFullTableDownloadCSV/1/en")
    assert len(calls) == 1 and ei.value.http_status == code and IP not in str(ei.value)
    assert f"HTTP {code} (try 1/5, definitive) server='cloudflare'" in capsys.readouterr().out


@pytest.mark.parametrize("code", sorted(sc.TRANSIENT_HTTP))
def test_download_books_every_not_now_code_transient_after_its_tries(code, monkeypatch, capsys, tmp_path):
    calls = []
    monkeypatch.setattr(requests, "get", lambda url, *a, **k: calls.append(url) or _Resp(code, BLOCK, headers=EDGE))
    monkeypatch.setattr(sc, "time", types.SimpleNamespace(sleep=lambda s: None))
    with pytest.raises(TransientError) as ei:
        sc._download("https://zips.example.invalid/1-eng.zip", str(tmp_path / "z.zip"))
    assert len(calls) == 5 and ei.value.http_status == code and IP not in str(ei.value)
    out = capsys.readouterr().out
    assert out.count(f"HTTP {code} (try ") == 5 and "cf-ray='8e00000000000abc-YYZ'" in out and IP in out


@pytest.mark.parametrize("code", [400, 404, 410])
def test_download_keeps_other_codes_definitive_at_once(code, monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(requests, "get", lambda url, *a, **k: calls.append(url) or _Resp(code, BLOCK, headers=EDGE))
    with pytest.raises(DefinitiveError) as ei:
        sc._download("https://zips.example.invalid/1-eng.zip", str(tmp_path / "z.zip"))
    assert len(calls) == 1 and ei.value.http_status == code and IP not in str(ei.value)


def test_download_records_the_zips_last_modified(monkeypatch, tmp_path):
    body = b"x" * 10
    monkeypatch.setattr(requests, "get", lambda url, *a, **k: _Resp(
        200, body, headers=dict(EDGE, **{"Content-Length": "10", "Last-Modified": NEW_LM})))
    meta = {}
    assert sc._download("https://zips.example.invalid/1-eng.zip", str(tmp_path / "z.zip"), meta=meta) == 10
    assert meta == {"last_modified": NEW_LM, "server": "cloudflare", "cf_ray": "8e00000000000abc-YYZ"}


def _fetch_env(monkeypatch, rows, counts, lm=NEW_LM):
    monkeypatch.setattr(sc, "_cube_counts", lambda pid: counts)
    monkeypatch.setattr(sc, "_get", lambda ep, **k: {"status": "SUCCESS", "object": "https://zips.example.invalid/x"})

    def get(url, *a, **k):
        buf = io.BytesIO()
        _zip(buf, rows)
        b = buf.getvalue()
        return _Resp(200, b, headers={"Content-Length": str(len(b)), "Last-Modified": lm})
    monkeypatch.setattr(requests, "get", get)


def test_every_count_gate_decision_is_logged_with_both_counts_and_the_last_modified(world, monkeypatch, capsys):
    rows = _rows_for(("2026-08", "2026-09"))
    _fetch_env(monkeypatch, rows, (4, 2))
    path, st = sc._fetch_cube_whole(PID)
    sc._remove_quietly(path)
    out = capsys.readouterr().out
    assert (f"count gate passed - parsed 4 rows / 2 series vs getCubeMetadata 4 / 2; zip Last-Modified {NEW_LM}"
            in out), out
    assert st["zip_last_modified"] == NEW_LM and (st["want_rows"], st["want_series"]) == (4, 2)
    _fetch_env(monkeypatch, rows, (6, 2), lm=OLD_LM)
    with pytest.raises(DefinitiveError, match=f"zip Last-Modified {OLD_LM}") as ei:
        sc._fetch_cube_whole(PID)
    assert "incomplete against StatCan's own counts" in str(ei.value)
    assert (f"count gate REFUSED - parsed 4 rows / 2 series vs getCubeMetadata 6 / 2; zip Last-Modified {OLD_LM}"
            in capsys.readouterr().out)


def test_a_vectorless_cubes_gate_line_says_no_counts_were_compared(world, monkeypatch, capsys):
    coord = [_row("2026-01", "Windsor", "", f"1.{i}", str(i)) for i in range(1, 6)]
    _fetch_env(monkeypatch, coord, (99, 1))
    path, st = sc._fetch_cube_whole(PID)
    sc._remove_quietly(path)
    assert st["needs_stored_floor"] is True
    assert "count gate passed - vectorless, counts not compared" in capsys.readouterr().out


# --------------------------------------------------------------------------- #
# item 1 - strikes spaced in time
# --------------------------------------------------------------------------- #
T0 = _t("2026-11-18T14:30Z")


def _h(h):
    return T0 + dt.timedelta(hours=h)


def test_strikes_wait_1_h_then_until_12_h_after_the_first_and_only_then_quarantine(capsys):
    c = {"release": "r1"}
    assert lane._deterministic(c, "1", "short", _h(0)) is False
    assert c["fails"] == 1 and c["first_strike_utc"] == _z(_h(0)) and c["retry_after"] == _z(_h(1))
    lane._deterministic(c, "1", "short", _h(1))
    assert c["fails"] == 2 and c["retry_after"] == _z(_h(12)) and not c.get("quarantined")
    lane._deterministic(c, "1", "short", _h(12))
    assert c["fails"] == 3 and c["quarantined"] is True
    out = capsys.readouterr().out
    assert "strike 1 of 3" in out and "strike 2 of 3" in out and "QUARANTINED after 3 failures" in out


def test_three_strikes_inside_12_h_never_quarantine_whatever_rewrote_retry_after():
    c = {"release": "r1"}
    lane._deterministic(c, "1", "x", _h(0))
    lane._backoff(c, "a transient in between rewrote retry_after early", _h(0.5))
    lane._deterministic(c, "1", "x", _h(0.7))
    lane._backoff(c, "and again", _h(1))
    lane._deterministic(c, "1", "x", _h(11.9))                       # third strike, 11.9 h after the first
    assert c["fails"] == 3 and not c.get("quarantined")
    assert c["retry_after"] == _z(_h(17.9)), "the next try is 6 h on, past first + 12 h"
    lane._deterministic(c, "1", "x", _h(17.9))
    assert c["quarantined"] is True


def test_strike_one_always_overwrites_a_stale_first_strike_stamp():
    old = _z(_h(-30))
    for c in ({"release": "r2", "fail_release": "r1", "fails": 2, "first_strike_utc": old},     # older release
              {"release": "r1", "fail_release": "r1", "fails": 0, "first_strike_utc": old}):    # a partial clear
        lane._deterministic(c, "1", "x", _h(0))
        assert c["fails"] == 1 and c["first_strike_utc"] == _z(_h(0)), c
        lane._deterministic(c, "1", "x", _h(1))
        lane._deterministic(c, "1", "x", _h(7))
        assert c["fails"] == 3 and not c.get("quarantined"), "a stale stamp shortened the span"


@pytest.mark.parametrize("stamp", [None, "garbage"])
def test_a_later_strike_with_no_readable_stamp_starts_the_span_now(stamp):
    c = {"release": "r1", "fail_release": "r1", "fails": 2}
    if stamp:
        c["first_strike_utc"] = stamp
    lane._deterministic(c, "1", "x", _h(0))
    assert c["fails"] == 3 and not c.get("quarantined") and c["first_strike_utc"] == _z(_h(0))
    assert c["retry_after"] == _z(_h(12))
    lane._deterministic(c, "1", "x", _h(12))
    assert c["quarantined"] is True


def test_a_deferred_quarantine_is_not_set_and_not_announced(capsys):
    c = {"release": "r1", "fail_release": "r1", "fails": 2, "first_strike_utc": _z(_h(-12))}
    assert lane._deterministic(c, "1", "HTTP 404", _h(0), defer_quarantine=True) is True
    assert not c.get("quarantined") and c["fails"] == 3 and c["retry_after"] == _z(_h(6))
    out = capsys.readouterr().out
    assert "held for this iteration's circuit-breaker decision" in out and "QUARANTINED" not in out


def test_the_spacing_constants():
    assert lane.STRIKE_RETRY_MIN == (60.0, 360.0) and lane.QUARANTINE_MIN_SPAN_H == 12.0
    assert sc.QUARANTINE_AFTER == 3


# --------------------------------------------------------------------------- #
# the replays (items 1-4 together, end to end through the lane's real code)
# --------------------------------------------------------------------------- #
def test_replay_2026_11_17_18_the_enumeration_403_then_39_zip_403s(world, clock, statcan, capsys):
    """11-17 ~19:15Z to 11-18 14:28Z getAllCubesListLite answered 403 (210 relaunch deaths); then 39 cubes released
    11-18T13:30 were owed and every zip answered 403 (assumed here for 1 h; the lane quarantined all 39 by 14:44:42Z).
    Under the new rule: the process never ends, the enumeration backs off, statcan reads red after 6 h of it, no
    cube gets a strike, none is quarantined, at most 3 zips are refused per iteration, and all 39 merge."""
    prev = "2026-10-22T08:30"
    for p in CLASS_A:
        statcan.add(p, prev)
    _seed(world, CLASS_A, prev, "2026-11-17T18:14:27Z")       # the last good enumeration, ~1 h before
    lite_down = (_t("2026-11-17T19:15Z"), _t("2026-11-18T14:28Z"))
    statcan.lite_status = lambda t: 403 if lite_down[0] <= t < lite_down[1] else 200
    gh = _heartbeat()
    clock.t = lite_down[0]
    first_red = None
    while clock.t < lite_down[1]:
        r = lane.run(put=_Bucket(), now_fn=clock)                    # never raises: that IS the change
        assert r["worked"] == 0
        v = _verdict_or_red(world, clock.t)
        if v.startswith("RED") and first_red is None:
            first_red = clock.t
            assert "cannot ENUMERATE" in v and IP not in v, v
            lp = json.loads(open(lane.LOCAL_PROGRESS, encoding="utf-8").read())
            assert "cannot ENUMERATE" in gh._lane_problem(lp, clock.t), "CI sees the same red"
        clock.step()
    attempts = sorted({t for t, kind, _p, st in statcan.calls if kind == "lite"})
    lite_gets = [c for c in statcan.calls if c[0] in attempts and c[1] == "lite"]
    print(f"MEASURED enumeration attempts over the 19.2 h outage: {len(attempts)} ({len(lite_gets)} GETs)",
          file=sys.__stdout__, flush=True)                                  # past capsys: a figure for the record
    assert 15 <= len(attempts) <= 25 and len(lite_gets) == 5 * len(attempts), (len(attempts), len(lite_gets))
    assert first_red is not None and dt.timedelta(hours=6) < first_red - lite_down[0] <= dt.timedelta(hours=6, minutes=5)
    ef = _state(world)["enum_fail"]
    assert ef["since"] == _z(lite_down[0]) and ef["http_status"] == 403 and IP not in json.dumps(ef)
    assert IP not in (world / "_lane_progress.json").read_text()
    # 14:28Z: the list answers, 39 cubes are owed at 13:30, and their zips answer 403 for an hour
    for p in CLASS_A:
        statcan.cubes[p]["release"] = "2026-11-18T13:30"
    # the zips refuse for an hour FROM THE FIRST ITERATION THAT READS THE LIST (the lane's enumeration backoff
    # finds the list back within ENUM_BACKOFF_MAX_MIN of 14:28Z), so the replay keeps the event's shape: 39 owed
    # cubes meeting a refusing zip host
    phase2 = clock.t
    zip_down = []

    def zip_status(pid, t):
        if not zip_down:
            ok = [c[0] for c in statcan.calls if c[1] == "lite" and c[3] == 200 and c[0] >= phase2]
            zip_down.extend([min(ok), min(ok) + dt.timedelta(hours=1)])
        return 403 if zip_down[0] <= t < zip_down[1] else 200
    statcan.zip_status = zip_status
    # a cube merged earlier whose serve is still owed sorts AFTER the 39: serving must go on through a trip (C1)
    st = _state(world)
    st["cubes"][str(PID)] = {"release": "2026-11-18T13:31", "merged": "2026-11-18T13:31", "served": None}
    (world / "_lane_state.json").write_text(json.dumps(st))
    statcan.calls.clear()
    b = _Bucket()
    tail_served_at = None
    while clock.t < _t("2026-11-19T14:28Z"):
        before = len(statcan.calls)
        lane.run(put=b, now_fn=clock)
        new = statcan.calls[before:]
        cubes = _state(world)["cubes"]
        assert not [p for p, c in cubes.items() if c.get("quarantined")], clock.t
        assert not [p for p, c in cubes.items() if int(c.get("fails") or 0)], "a strike was booked"
        assert len({p for _t2, k, p, s in new if k == "zip" and s != 200}) <= lane.BREAKER_MIN_CUBES
        if tail_served_at is None and cubes[str(PID)].get("served"):
            tail_served_at = clock.t
        clock.step()
    assert tail_served_at == phase2, "the serve-owed cube waited"
    print(f"MEASURED the list read again {zip_down[0] - lite_down[1]} after it answered", file=sys.__stdout__,
          flush=True)
    assert zip_down[0] - lite_down[1] <= dt.timedelta(minutes=lane.ENUM_BACKOFF_MAX_MIN)
    first_zip = [c for c in statcan.calls if c[1] == "zip" and c[0] == zip_down[0]]
    assert len({c[2] for c in first_zip}) == lane.BREAKER_MIN_CUBES, "the trip came at the 3rd refused cube"
    window_gets = [c for c in statcan.calls if c[1] == "lite" and zip_down[0] <= c[0] < zip_down[1]]
    print(f"MEASURED cube-list GETs in the 1 h zip-refusal window: {len(window_gets)}", file=sys.__stdout__,
          flush=True)
    assert 4 <= len(window_gets) <= 6, "the window was exercised, at no more than the base rule's rate"
    cubes = _state(world)["cubes"]
    assert all(cubes[p]["merged"] == cubes[p]["served"] == "2026-11-18T13:30" for p in CLASS_A)
    last_merge = max(t for t, k, p, s in statcan.calls if k == "zip" and s == 200)
    print(f"MEASURED last merge after the window: {last_merge - zip_down[1]}; first red after "
          f"{first_red - lite_down[0]}", file=sys.__stdout__, flush=True)
    assert last_merge - zip_down[1] <= dt.timedelta(minutes=lane.BREAKER_BACKOFF_MAX_MIN + 5)
    st = _state(world)
    assert "breaker" not in st and "enum_fail" not in st
    out = capsys.readouterr().out
    assert "CIRCUIT BREAKER: HTTP 403" in out and "circuit breaker cleared" in out and "QUARANTINED" not in out


def test_replay_class_b_a_pre_release_zip_refused_then_passing_17_min_later(world, clock, statcan, capsys):
    """11-16: 18100001/04/06/256 released 13:30, refused by the count gate at ~14:45 (the zip still the 10-19 copy:
    Last-Modified before the release, one period short of getCubeMetadata), struck 3 times in ~11 min and
    quarantined. Here the zip is rebuilt 17 min after the first refusal: one strike each, the 1 h retry merges."""
    rebuilt = _t("2026-11-16T15:02Z")
    for p in CLASS_B:
        statcan.add(p, "2026-11-16T13:30", rebuilt=rebuilt)
    _seed(world, CLASS_B, "2026-10-19T08:30", "2026-11-16T13:40:00Z")
    clock.t = _t("2026-11-16T14:45Z")
    seen = []
    while clock.t < _t("2026-11-16T17:00Z"):
        lane.run(put=_Bucket(), now_fn=clock)
        cubes = _state(world)["cubes"]
        assert not [p for p in CLASS_B if cubes[p].get("quarantined")], clock.t
        seen.append((clock.t, tuple(int(cubes[p].get("fails") or 0) for p in CLASS_B)))
        clock.step()
    assert seen[0][1] == (1, 1, 1, 1) and all(f in ((1, 1, 1, 1), (0, 0, 0, 0)) for _t2, f in seen)
    zips = sorted({t for t, k, p, s in statcan.calls if k == "zip"})
    assert zips == [_t("2026-11-16T14:45Z"), _t("2026-11-16T15:45Z")], zips
    cubes = _state(world)["cubes"]
    assert all(cubes[p]["merged"] == "2026-11-16T13:30" for p in CLASS_B)
    out = capsys.readouterr().out
    assert (f"count gate REFUSED - parsed 4 rows / 2 series vs getCubeMetadata 6 / 2; zip Last-Modified {OLD_LM}"
            in out)
    assert f"count gate passed - parsed 6 rows / 2 series vs getCubeMetadata 6 / 2; zip Last-Modified {NEW_LM}" in out
    assert "strike 1 of 3" in out and "QUARANTINED" not in out


def _drive(world, clock, until, check=None):
    while clock.t < until:
        lane.run(put=_Bucket(), now_fn=clock)
        if check:
            check(clock.t)
        clock.step()


def test_a_count_gate_that_refuses_for_ever_quarantines_at_12_h_not_before(world, clock, statcan):
    statcan.add("18100001", "2026-11-16T13:30", rebuilt=_t("2027-01-01T00:00Z"))      # never rebuilt
    _seed(world, ["18100001"], "2026-10-19T08:30", "2026-11-16T13:40:00Z")
    t0 = clock.t = _t("2026-11-16T14:45Z")
    when = []
    _drive(world, clock, t0 + dt.timedelta(hours=13),
           lambda t: when.append(t) if _state(world)["cubes"]["18100001"].get("quarantined") and not when else None)
    assert when == [t0 + dt.timedelta(hours=12)], when
    assert sorted({t for t, k, p, s in statcan.calls if k == "zip"}) == [t0, t0 + dt.timedelta(hours=1),
                                                                         t0 + dt.timedelta(hours=12)]


def test_one_cube_refused_404_for_ever_quarantines_at_12_h(world, clock, statcan):
    statcan.add("10100111", "2026-11-18T13:30")
    statcan.zip_status = lambda pid, t: 404
    _seed(world, ["10100111"], "2026-10-15T08:30", "2026-11-18T13:00:00Z")
    t0 = clock.t = _t("2026-11-18T14:30Z")
    when = []
    _drive(world, clock, t0 + dt.timedelta(hours=13),
           lambda t: when.append(t) if _state(world)["cubes"]["10100111"].get("quarantined") and not when else None)
    assert when == [t0 + dt.timedelta(hours=12)]


def _first_red(world, clock, until, want):
    red = []

    def check(t):
        if not red:
            v = _verdict_or_red(world, t)
            if v.startswith("RED"):
                assert want in v, v
                red.append(t)
    _drive(world, clock, until, check)
    return red[0] if red else None


def test_one_cube_refused_403_for_ever_turns_red_failing_within_about_5_h_and_is_never_quarantined(world, clock,
                                                                                                    statcan):
    statcan.add("10100111", "2026-11-18T13:30")
    statcan.zip_status = lambda pid, t: 403
    _seed(world, ["10100111"], "2026-10-15T08:30", "2026-11-18T13:00:00Z")
    t0 = clock.t = _t("2026-11-18T14:30Z")
    red = _first_red(world, clock, t0 + dt.timedelta(hours=8), "keep FAILING")
    print(f"MEASURED one cube, 403 for ever: red FAILING after {red - t0}")
    assert red is not None and red - t0 <= dt.timedelta(hours=5, minutes=20)
    assert not _state(world)["cubes"]["10100111"].get("quarantined")


@pytest.mark.parametrize("code", [403, 404])
def test_a_mass_refusal_for_ever_never_strikes_and_turns_red_failing_within_a_bound(world, clock, statcan, code):
    pids = CLASS_A[:6]
    for p in pids:
        statcan.add(p, "2026-11-18T13:30")
    statcan.zip_status = lambda pid, t: code
    _seed(world, pids, "2026-10-15T08:30", "2026-11-18T13:00:00Z")
    t0 = clock.t = _t("2026-11-18T14:30Z")

    def no_strike(t):
        cubes = _state(world)["cubes"]
        assert not [p for p in pids if cubes[p].get("quarantined") or int(cubes[p].get("fails") or 0)], t
    red = []

    def check(t):
        no_strike(t)
        if not red:
            v = _verdict_or_red(world, t)
            if v.startswith("RED"):
                assert "circuit breaker has held every merge back" in v and IP not in v, v
                red.append(t)
    _drive(world, clock, t0 + dt.timedelta(hours=19), check)
    print(f"MEASURED 6 cubes, HTTP {code} for ever: red (breaker held) after {red[0] - t0}")
    assert red and dt.timedelta(hours=6) < red[0] - t0 <= dt.timedelta(hours=6, minutes=10)
    # the cube-list GETs after the first hour stay at the idle rate (<= 3 an hour), never every 5 minutes
    for hour in range(1, 19):
        lo, hi = t0 + dt.timedelta(hours=hour), t0 + dt.timedelta(hours=hour + 1)
        n = sum(1 for t, k, p, s in statcan.calls if k == "lite" and lo <= t < hi)
        assert n <= 3, (hour, n)


def test_a_19_h_zip_refusal_over_many_cubes_recovers_within_one_breaker_backoff(world, clock, statcan):
    pids = CLASS_A[:12]
    for p in pids:
        statcan.add(p, "2026-11-18T13:30")
    t0 = _t("2026-11-18T14:30Z")
    end = t0 + dt.timedelta(hours=19)
    statcan.zip_status = lambda pid, t: 403 if t < end else 200
    _seed(world, pids, "2026-10-15T08:30", "2026-11-18T13:00:00Z")
    clock.t = t0
    _drive(world, clock, end + dt.timedelta(hours=3))
    cubes = _state(world)["cubes"]
    assert all(cubes[p]["merged"] == "2026-11-18T13:30" for p in pids)
    last = max(t for t, k, p, s in statcan.calls if k == "zip" and s == 200)
    print(f"MEASURED last merge after a 19 h window: {last - end}")
    # one hold at most, plus the iteration after the breaker closes (the freed cubes are due from then)
    assert last - end <= dt.timedelta(minutes=lane.BREAKER_BACKOFF_MAX_MIN + 10)
    lite = [t for t, k, p, s in statcan.calls if k == "lite" and t0 + dt.timedelta(hours=1) <= t < end]
    print(f"MEASURED cube-list GETs per hour, hours 1-19 of the window: {len(lite) / 18:.2f}")
    assert len(lite) <= 3 * 18


def test_a_half_open_breaker_probes_one_cube_and_frees_the_rest_when_it_answers(world, clock, statcan, capsys):
    pids = CLASS_A[:4]
    for p in pids:
        statcan.add(p, "2026-11-18T13:30")
    statcan.zip_status = lambda pid, t: 403
    _seed(world, pids, "2026-10-15T08:30", "2026-11-18T13:00:00Z")
    clock.t = t0 = _t("2026-11-18T14:30Z")
    lane.run(put=_Bucket(), now_fn=clock)                            # 3 refused -> trip, until t0 + 10 min
    st = _state(world)
    far = _z(t0 + dt.timedelta(hours=10))
    for p in pids[:3]:
        st["cubes"][p]["retry_after"] = far                         # each waits a long backoff of its own
    st["cubes"][pids[3]]["retry_after"] = far
    (world / "_lane_state.json").write_text(json.dumps(st))
    statcan.zip_status = lambda pid, t: 200                         # the host is back
    statcan.calls.clear()
    clock.t = t0 + dt.timedelta(minutes=10)                          # the hold has ended: half-open
    lane.run(put=_Bucket(), now_fn=clock)
    zips = [p for t, k, p, s in statcan.calls if k == "zip"]
    assert zips == [sorted(pids)[0]], "one probe, ignoring its own backoff"
    st = _state(world)
    assert "breaker" not in st and all(not st["cubes"][p].get("retry_after") for p in pids[:3]), \
        "the cubes refused with the trip's status are due again"
    assert st["cubes"][pids[3]]["retry_after"] == far, "a cube the trip did not refuse keeps its own backoff"
    clock.step()
    lane.run(put=_Bucket(), now_fn=clock)
    st = _state(world)
    assert all(st["cubes"][p]["merged"] == "2026-11-18T13:30" for p in pids[:3])
    assert "circuit breaker cleared" in capsys.readouterr().out


def test_a_half_open_iteration_trips_again_after_3_same_status_refusals_untried_cubes_first(world, clock, statcan):
    pids = CLASS_A[:4]
    for p in pids:
        statcan.add(p, "2026-11-18T13:30")
    statcan.zip_status = lambda pid, t: 403
    _seed(world, pids, "2026-10-15T08:30", "2026-11-18T13:00:00Z")
    clock.t = t0 = _t("2026-11-18T14:30Z")
    lane.run(put=_Bucket(), now_fn=clock)
    untried = [p for p in pids if p not in _state(world)["breaker"]["cubes"]]
    statcan.calls.clear()
    clock.t = t0 + dt.timedelta(minutes=10)
    r = lane.run(put=_Bucket(), now_fn=clock)
    tried = [p for t, k, p, s in statcan.calls if k == "zip"]
    assert r["breaker"] == 403 and len(set(tried)) == lane.BREAKER_MIN_CUBES and tried[0] == untried[0], tried
    b = _state(world)["breaker"]
    assert b["trips"] == 2 and b["until"] == _z(clock.t + dt.timedelta(minutes=20)) and b["since"] == _z(t0)


def test_a_half_open_iteration_that_runs_out_of_cubes_trips_again(world, clock, statcan):
    """Only 2 cubes are due at the hold's end and both are refused with the trip's status: no 3rd refusal, but the
    iteration must not end undecided (it would probe again 5 min later)."""
    pids = CLASS_A[:3]
    for p in pids:
        statcan.add(p, "2026-11-18T13:30")
    statcan.zip_status = lambda pid, t: 403
    _seed(world, pids, "2026-10-15T08:30", "2026-11-18T13:00:00Z")
    clock.t = t0 = _t("2026-11-18T14:30Z")
    lane.run(put=_Bucket(), now_fn=clock)
    st = _state(world)
    st["cubes"][pids[2]]["retry_after"] = _z(t0 + dt.timedelta(hours=5))   # only 2 due at the hold's end
    (world / "_lane_state.json").write_text(json.dumps(st))
    clock.t = t0 + dt.timedelta(minutes=10)
    r = lane.run(put=_Bucket(), now_fn=clock)
    assert r["breaker"] == 403 and _state(world)["breaker"]["trips"] == 2


# --------------------------------------------------------------------------- #
# item 4 - the circuit breaker, piece by piece
# --------------------------------------------------------------------------- #
def test_a_trip_stops_merges_holds_the_rest_serves_the_tail_and_clears_on_a_fetch(world, clock, statcan, capsys):
    pids = CLASS_A[:5]
    for p in pids:
        statcan.add(p, "2026-11-18T13:30")
    statcan.zip_status = lambda pid, t: 403
    _seed(world, pids, "2026-10-15T08:30", "2026-11-18T13:00:00Z",
          extra={str(PID): {"release": "2026-11-18T13:31", "merged": "2026-11-18T13:31", "served": None}})
    clock.t = t0 = _t("2026-11-18T14:30Z")
    b = _Bucket()
    r = lane.run(put=b, now_fn=clock)
    assert r["breaker"] == 403
    tried = [p for _t2, k, p, s in statcan.calls if k == "zip"]
    assert len(set(tried)) == 3 and len(tried) == 15, "3 cubes, 5 tries each"
    cubes = _state(world)["cubes"]
    untried = [p for p in pids if p not in tried]
    assert all(not cubes[p].get("transient_fails") and not cubes[p].get("retry_after") for p in untried)
    assert cubes[str(PID)]["served"] == "2026-11-18T13:31" and b.put, "the tail serve went on"
    brk = _state(world)["breaker"]
    assert brk["status"] == 403 and brk["trips"] == 1 and brk["until"] == _z(t0 + dt.timedelta(minutes=10))
    assert _progress(world)["breaker"]["until"] == brk["until"]
    assert "CIRCUIT BREAKER: HTTP 403 refused 3 cube(s)" in capsys.readouterr().out
    statcan.calls.clear()
    clock.step(5)                                   # held: no merge, no cube-list GET (idle, enumerated 5 min ago)
    assert lane.run(put=b, now_fn=clock) == {"enumerated": False, "worked": 0}
    assert statcan.calls == []
    clock.step(5)                                   # the hold ends; the host answers again
    statcan.zip_status = lambda pid, t: 200
    lane.run(put=b, now_fn=clock)
    st = _state(world)
    assert "breaker" not in st and all(st["cubes"][p]["merged"] == "2026-11-18T13:30" for p in pids)
    assert "circuit breaker cleared" in capsys.readouterr().out


def test_a_trip_puts_back_every_strike_and_the_held_quarantine(world, clock, statcan, capsys):
    """Three 404s in one iteration; one of the cubes already has 2 strikes 12 h old, so its 3rd strike reaches the
    quarantine rule - the quarantine and its QUARANTINED line wait for the breaker, which puts them back (C3)."""
    pids = CLASS_A[:3]
    for p in pids:
        statcan.add(p, "2026-11-18T13:30")
    statcan.zip_status = lambda pid, t: 404
    t0 = _t("2026-11-18T14:30Z")
    old = {"release": "2026-11-18T13:30", "merged": "2026-10-15T08:30", "served": "2026-10-15T08:30",
           "fails": 2, "fail_release": "2026-11-18T13:30", "first_strike_utc": _z(t0 - dt.timedelta(hours=12)),
           "retry_after": _z(t0)}
    _seed(world, pids, "2026-10-15T08:30", "2026-11-18T13:00:00Z")
    st = _state(world)
    st["cubes"][pids[0]].update(old)
    (world / "_lane_state.json").write_text(json.dumps(st))
    clock.t = t0
    lane.run(put=_Bucket(), now_fn=clock)
    cubes = _state(world)["cubes"]
    assert not [p for p in pids if cubes[p].get("quarantined")]
    assert cubes[pids[0]]["fails"] == 2 and cubes[pids[0]]["first_strike_utc"] == old["first_strike_utc"]
    assert all(int(cubes[p].get("fails") or 0) == 0 for p in pids[1:])
    assert all(cubes[p]["transient_fails"] == 1 and "CIRCUIT BREAKER" in cubes[p]["last_error"] for p in pids)
    assert all("merge_in_flight" not in cubes[p] for p in pids)
    out = capsys.readouterr().out
    assert "QUARANTINED" not in out and "held for this iteration's circuit-breaker decision" in out


def test_fewer_than_three_refusals_keep_their_strikes_and_the_held_quarantine_lands(world, clock, statcan, capsys):
    pids = CLASS_A[:2]
    for p in pids:
        statcan.add(p, "2026-11-18T13:30")
    statcan.zip_status = lambda pid, t: 404
    t0 = _t("2026-11-18T14:30Z")
    _seed(world, pids, "2026-10-15T08:30", "2026-11-18T13:00:00Z")
    st = _state(world)
    st["cubes"][pids[0]].update({"release": "2026-11-18T13:30", "fails": 2, "fail_release": "2026-11-18T13:30",
                                 "first_strike_utc": _z(t0 - dt.timedelta(hours=12)), "retry_after": _z(t0)})
    st["cubes"][pids[1]]["release"] = "2026-11-18T13:30"
    (world / "_lane_state.json").write_text(json.dumps(st))
    clock.t = t0
    r = lane.run(put=_Bucket(), now_fn=clock)
    assert "breaker" not in r
    cubes = _state(world)["cubes"]
    assert cubes[pids[0]]["quarantined"] is True and cubes[pids[0]]["fails"] == 3
    assert cubes[pids[1]]["fails"] == 1 and not cubes[pids[1]].get("quarantined")
    assert f"{pids[0]}: QUARANTINED after 3 failures" in capsys.readouterr().out


def test_an_ended_hold_with_nothing_left_to_merge_is_cleared_not_published_for_ever(world, clock, statcan, capsys):
    statcan.add("10100111", "2026-10-15T08:30")
    _seed(world, ["10100111"], "2026-10-15T08:30", "2026-11-18T13:00:00Z")
    st = _state(world)
    st["breaker"] = {"status": 403, "cubes": ["10100111"], "trips": 2, "tripped_utc": "2026-11-18T13:50:00Z",
                     "until": "2026-11-18T14:10:00Z"}
    (world / "_lane_state.json").write_text(json.dumps(st))
    clock.t = _t("2026-11-18T14:30Z")
    lane.run(put=_Bucket(), now_fn=clock)
    assert "breaker" not in _state(world) and "breaker" not in _progress(world)
    assert "circuit breaker cleared: no merge-owed cube waits on it" in capsys.readouterr().out


# --------------------------------------------------------------------------- #
# the half-open state, from the verification review's probes (review_verify, R2028: P1-P3, P6, P7; V1-V5)
# --------------------------------------------------------------------------- #
def _per_hour(calls, kind, t0, hours):
    return [sum(1 for t, k, _p, _s in calls if k == kind and t0 + dt.timedelta(hours=h) <= t
                < t0 + dt.timedelta(hours=h + 1)) for h in range(hours)]


def _rekeyed():
    return [_row(p, "Canada", vec, coord, str(float(i + k)))
            for i, p in enumerate(("2026-08", "2026-09", "2026-10"))
            for k, (vec, coord) in enumerate((("x1", "9.1"), ("x2", "9.2")))]


def test_a_strike_after_the_fetch_keeps_its_wait_through_a_breaker_close(world, clock, statcan):
    """P1 / V2: a cube refused in a 403 trip, fetched at the hold's end with a re-keyed table, gets a strike; the
    close in the same iteration frees the OTHER refused cubes, never this one (its refused_http went with the strike)."""
    pids = CLASS_A[:4]
    for p in pids:
        statcan.add(p, "2026-11-18T13:30")
    statcan.zip_status = lambda pid, t: 403
    _seed(world, pids, "2026-10-15T08:30", "2026-11-18T13:00:00Z")
    clock.t = t0 = _t("2026-11-18T14:30Z")
    lane.run(put=_Bucket(), now_fn=clock)                               # 3 x 403 -> trip
    victim = sorted(_state(world)["breaker"]["cubes"])[0]
    statcan.cubes[victim]["new"] = _rekeyed()                           # the key gate strikes it
    statcan.zip_status = lambda pid, t: 200
    statcan.calls.clear()
    for _ in range(32):
        clock.step()
        lane.run(put=_Bucket(), now_fn=clock)
    fetches = sorted({t for t, k, p, s in statcan.calls if k == "zip" and p == victim})
    c = _state(world)["cubes"][victim]
    assert len(fetches) >= 2 and fetches[1] - fetches[0] >= dt.timedelta(hours=1), [str(f - t0) for f in fetches]
    assert c["fails"] >= 1 and "refused_http" not in c


@pytest.mark.parametrize("mode", ["reset", "503", "nozip"])
def test_a_half_open_attempt_is_one_per_hold_whatever_it_meets(world, clock, statcan, monkeypatch, mode, capsys):
    """P2 / V1: after a 403 trip the host answers with a connection reset, a different status, or a WDS 200 with no
    zip URL. Every outcome leaves the half-open state, so the cube list is read at most once per hold and the
    strikes keep their spacing."""
    pids = CLASS_A[:4]
    for p in pids:
        statcan.add(p, "2026-11-18T13:30")
    statcan.zip_status = lambda pid, t: 403
    _seed(world, pids, "2026-10-15T08:30", "2026-11-18T13:00:00Z")
    clock.t = t0 = _t("2026-11-18T14:30Z")
    lane.run(put=_Bucket(), now_fn=clock)
    real = statcan.get

    def get(url, *a, **k):
        t = statcan.clock.t
        if mode == "reset" and "zips.example.invalid" in url:
            statcan.calls.append((t, "zip", url.rsplit("/", 1)[1][:8], "reset"))
            raise requests.ConnectionError("Connection reset by peer")
        if mode == "nozip" and "getFullTableDownloadCSV" in url:
            statcan.calls.append((t, "table", url.split("/")[-2], "FAILED"))
            return _Resp(200, obj={"status": "FAILED", "object": "service unavailable"})
        return real(url, *a, **k)
    monkeypatch.setattr(requests, "get", get)
    if mode == "503":
        statcan.zip_status = lambda pid, t: 503
    statcan.calls.clear()
    hours, strikes_at = 13, {}
    while clock.t < t0 + dt.timedelta(hours=hours):
        clock.step()
        lane.run(put=_Bucket(), now_fn=clock)
        for p, c in _state(world)["cubes"].items():
            if p in pids and int(c.get("fails") or 0) > len(strikes_at.get(p, [])):
                strikes_at.setdefault(p, []).append(clock.t)
                assert not c.get("quarantined") or clock.t - strikes_at[p][0] >= dt.timedelta(hours=12)
    lite = _per_hour(statcan.calls, "lite", t0, hours)
    print(f"MEASURED half-open mode {mode}: cube-list GETs per hour {lite}; strikes "
          f"{ {p: len(ts) for p, ts in strikes_at.items()} }", file=sys.__stdout__, flush=True)
    assert max(lite[1:]) <= 3, lite
    for p, ts in strikes_at.items():
        gaps = [b - a for a, b in zip(ts, ts[1:])]
        assert all(g >= dt.timedelta(hours=1) for g in gaps), (p, [str(g) for g in gaps])
    if mode == "nozip":
        # a 200 that StatCan sends with no zip URL is an ANSWER: the breaker closes (V4), it is not armed again
        assert "breaker" not in _state(world) and "armed again" not in capsys.readouterr().out
    if mode in ("reset", "503"):
        kind = "zip"
        per_hold = [len({p for t, k, p, _s in statcan.calls if k == kind and t == tt})
                    for tt in sorted({t for t, k, _p, _s in statcan.calls if k == kind})]
        assert max(per_hold) <= lane.BREAKER_MIN_CUBES, "at most 3 merge attempts per hold"
        assert _state(world)["breaker"]["trips"] >= 5


@pytest.mark.parametrize("code,nbad", [(403, 3), (404, 3), (404, 6)])
def test_cubes_refused_for_ever_do_not_hold_the_others_back(world, clock, statcan, code, nbad):
    """P3 / P3c / V3: a few cubes refused for ever sort first (older release) and trip the breaker; the cubes that
    answer are tried first in the half-open iteration and merge within one hold. The refused ones still turn red."""
    bad, good = list(CLASS_A[:nbad]), list(CLASS_A[nbad:nbad + 20])
    for p in bad:
        statcan.add(p, "2026-11-18T13:00")
    for p in good:
        statcan.add(p, "2026-11-18T13:30")
    statcan.zip_status = lambda pid, t: code if pid in bad else 200
    _seed(world, bad + good, "2026-10-15T08:30", "2026-11-18T12:50:00Z")
    clock.t = t0 = _t("2026-11-18T14:30Z")
    done, red = None, None
    while clock.t < t0 + dt.timedelta(hours=9):
        lane.run(put=_Bucket(), now_fn=clock)
        cubes = _state(world)["cubes"]
        if done is None and all(cubes[p].get("merged") == "2026-11-18T13:30" for p in good):
            done = clock.t - t0
        if red is None and _verdict_or_red(world, clock.t).startswith("RED"):
            red = clock.t - t0
        clock.step()
    print(f"MEASURED {nbad} cube(s) HTTP {code} for ever: 20 others merged after {done}; red after {red}",
          file=sys.__stdout__, flush=True)
    assert done is not None and done <= dt.timedelta(minutes=lane.BREAKER_BACKOFF_MAX_MIN + 10)    # V3: <= 70 min
    assert red is not None and red <= dt.timedelta(hours=8)


def test_after_the_first_cycle_a_new_batch_is_not_held_back_either(world, clock, statcan):
    """P3b: the same 3 refused cubes, then StatCan releases the 20 again: they merge within an hour."""
    bad, good = list(CLASS_A[:3]), list(CLASS_A[3:23])
    for p in bad:
        statcan.add(p, "2026-11-18T13:00")
    for p in good:
        statcan.add(p, "2026-11-18T13:30")
    statcan.zip_status = lambda pid, t: 404 if pid in bad else 200
    _seed(world, bad + good, "2026-10-15T08:30", "2026-11-18T12:50:00Z")
    clock.t = _t("2026-11-18T14:30Z")

    def until_merged(rel, limit_h):
        start = clock.t
        while clock.t < start + dt.timedelta(hours=limit_h):
            lane.run(put=_Bucket(), now_fn=clock)
            if all(_state(world)["cubes"][p].get("merged") == rel for p in good):
                return clock.t - start
            clock.step()
        return None
    assert until_merged("2026-11-18T13:30", 30) is not None
    for p in good:
        statcan.cubes[p]["release"] = "2026-11-19T08:30"
    clock.t = max(clock.t, _t("2026-11-19T08:35Z"))
    second = until_merged("2026-11-19T08:30", 30)
    assert second is not None and second <= dt.timedelta(hours=1), second


def test_no_end_date_read_while_a_hold_lasts(world, clock, statcan):
    """RV05 / C1: an idle iteration during a hold may enumerate (hourly) but must not read the end dates - the held
    cubes are not worked in it."""
    pids = CLASS_A[:3]
    for p in pids:
        statcan.add(p, "2026-11-18T13:30")
    _seed(world, pids, "2026-10-15T08:30", "2026-11-18T13:00:00Z")
    t0 = _t("2026-11-18T14:30Z")
    st = _state(world)
    for p in pids:
        st["cubes"][p].update(release="2026-11-18T13:30", transient_fails=4, refused_http=403,
                              retry_after=_z(t0 - dt.timedelta(minutes=5)))
    st["breaker"] = {"status": 403, "cubes": pids, "trips": 4, "tripped_utc": _z(t0 - dt.timedelta(minutes=10)),
                     "until": _z(t0 + dt.timedelta(minutes=50)), "since": _z(t0 - dt.timedelta(hours=2))}
    st["last_enum_utc"] = _z(t0 - dt.timedelta(hours=2))
    (world / "_lane_state.json").write_text(json.dumps(st))
    reads = []
    clock.t = t0
    r = lane.run(put=_Bucket(), now_fn=clock, ends_fn=lambda: reads.append(1) or ({}, None))
    assert r["enumerated"] is True and reads == [], "the ends were read for cubes the hold keeps back"
    assert not [c for c in statcan.calls if c[1] == "zip"]


def test_a_history_hold_leaves_no_merge_in_flight(world, monkeypatch):
    """RV17: the history floor holds a CIMT-style cube before any write - the attempt published nothing."""
    _history_world(world, monkeypatch, STORED_2013)
    _net(monkeypatch, [_row("2014-01", "Aden", "v3", "2.1", "2.5"), _row("2014-02", "Aden", "v3", "2.1", "3")], (2, 1))
    lane.run(enumerate_releases=lambda since: {PID: REL}, put=_Bucket(), ends_fn=lambda: ({}, None))
    c = _state(world)["cubes"][str(PID)]
    assert c["quarantined"] is True and "history_hold" in c and "merge_in_flight" not in c


def test_the_half_open_probe_never_takes_a_struck_cube(world, clock, statcan):
    """V2: with a half-open breaker and nothing due, the probe may take only a cube waiting out a TRANSIENT backoff
    from the trip's status. A struck cube's wait stands: it is not fetched before its retry_after."""
    statcan.add("10100111", "2026-11-18T13:00")                     # struck; the OLDER release, first by order
    statcan.add("14100004", "2026-11-18T13:30")                     # waits out the trip's transient backoff
    _seed(world, ["10100111", "14100004"], "2026-10-15T08:30", "2026-11-18T13:00:00Z")
    t0 = _t("2026-11-18T14:30Z")
    st = _state(world)
    st["cubes"]["10100111"].update(release="2026-11-18T13:00", fails=1, fail_release="2026-11-18T13:00",
                                   first_strike_utc=_z(t0 - dt.timedelta(minutes=10)),
                                   retry_after=_z(t0 + dt.timedelta(minutes=50)))
    st["cubes"]["14100004"].update(release="2026-11-18T13:30", transient_fails=1, refused_http=403,
                                   retry_after=_z(t0 + dt.timedelta(minutes=50)))
    st["breaker"] = {"status": 403, "cubes": ["14100004"], "trips": 1, "tripped_utc": _z(t0 - dt.timedelta(minutes=11)),
                     "until": _z(t0 - dt.timedelta(minutes=1)), "since": _z(t0 - dt.timedelta(minutes=11))}
    st["last_enum_utc"] = _z(t0 - dt.timedelta(hours=2))
    (world / "_lane_state.json").write_text(json.dumps(st))
    clock.t = t0
    lane.run(put=_Bucket(), now_fn=clock)
    zips = [c[2] for c in statcan.calls if c[1] == "zip"]
    assert zips == ["14100004"], "the probe is the cube waiting on the breaker, never the struck one"
    assert _state(world)["cubes"]["10100111"]["retry_after"] == _z(t0 + dt.timedelta(minutes=50))


def test_a_half_open_refusal_with_another_status_is_an_answer_and_closes_the_breaker(world, clock, statcan, capsys):
    """RV03 / W2: after a 403 trip the first half-open attempt meets 404 on ONE cube. That is StatCan answering about
    that cube, not the trip's refusal: the breaker closes (its `since` cannot age into a red), the cube's own strike
    stands, and the trip's cubes - due again - merge in the same iteration."""
    pids = CLASS_A[:4]
    for p in pids:
        statcan.add(p, "2026-11-18T13:30")
    statcan.zip_status = lambda pid, t: 403
    _seed(world, pids, "2026-10-15T08:30", "2026-11-18T13:00:00Z")
    clock.t = t0 = _t("2026-11-18T14:30Z")
    lane.run(put=_Bucket(), now_fn=clock)
    untried = [p for p in pids if p not in _state(world)["breaker"]["cubes"]]
    assert len(untried) == 1
    statcan.zip_status = lambda pid, t: 404 if pid == untried[0] else 200
    statcan.calls.clear()
    clock.t = t0 + dt.timedelta(minutes=10)
    r = lane.run(put=_Bucket(), now_fn=clock)
    assert "breaker" not in r and "breaker" not in _state(world), "another status closes; it does not trip again"
    assert [p for t, k, p, s in statcan.calls if k == "zip"][0] == untried[0], "the untried cube goes first"
    st = _state(world)
    assert st["cubes"][untried[0]]["fails"] == 1, "its own strike stands"
    assert all(st["cubes"][p]["merged"] == "2026-11-18T13:30" for p in pids if p not in untried)
    assert "circuit breaker cleared: StatCan answered" in capsys.readouterr().out


def test_another_status_on_many_cubes_trips_a_new_breaker_that_names_it(world, clock, statcan):
    """W2: after a 403 trip the edge answers 404 on every zip. The first 404 closes the 403 breaker; the 404s that
    follow trip a NEW breaker for 404 (closed rule, 3 in one iteration): its record, and any red, name HTTP 404."""
    pids = CLASS_A[:4]
    for p in pids:
        statcan.add(p, "2026-11-18T13:30")
    statcan.zip_status = lambda pid, t: 403
    _seed(world, pids, "2026-10-15T08:30", "2026-11-18T13:00:00Z")
    clock.t = t0 = _t("2026-11-18T14:30Z")
    lane.run(put=_Bucket(), now_fn=clock)
    statcan.zip_status = lambda pid, t: 404
    clock.t = t0 + dt.timedelta(minutes=10)
    r = lane.run(put=_Bucket(), now_fn=clock)
    b = _state(world)["breaker"]
    assert r["breaker"] == 404 and b["status"] == 404 and b["trips"] == 1 and b["since"] == _z(clock.t)
    cubes = _state(world)["cubes"]
    assert not [p for p in pids if int(cubes[p].get("fails") or 0)], "no strike for the new trip's status"


def test_p9_the_breaker_red_names_a_hold_that_exists(world, clock, statcan):
    """The verification review's P9 (G2): a 403 trip on 3 cubes, then they answer 404 for ever. The 403 record must
    not age into a red while StatCan answers; any breaker red names the status it really holds for."""
    pids = list(CLASS_A[:3])
    for p in pids:
        statcan.add(p, "2026-11-18T13:30")
    t0 = _t("2026-11-18T14:30Z")
    statcan.zip_status = lambda pid, t: 403 if t < t0 + dt.timedelta(minutes=10) else 404
    _seed(world, pids, "2026-10-15T08:30", "2026-11-18T13:00:00Z")
    clock.t = t0
    first_red, red_text = None, None
    while clock.t < t0 + dt.timedelta(hours=16):
        lane.run(put=_Bucket(), now_fn=clock)
        v = _verdict_or_red(world, clock.t)
        if v.startswith("RED") and first_red is None:
            first_red, red_text = clock.t - t0, v
        b = _state(world).get("breaker")
        assert b is None or b["status"] in (403, 404) and (b["status"] == 404 or clock.t - t0 < dt.timedelta(hours=1))
        clock.step()
    print(f"MEASURED P9 first red after {first_red}: {str(red_text)[:200]}", file=sys.__stdout__, flush=True)
    assert first_red is None or "circuit breaker" not in red_text or "HTTP 404" in red_text, red_text


@pytest.mark.parametrize("mode,nbroken", [("reset", 3), ("store", 3), ("reset", 6)])
def test_cubes_failing_for_their_own_reason_do_not_hold_the_others_after_a_trip(world, clock, statcan, monkeypatch,
                                                                                mode, nbroken):
    """The verification review's P8 (G1/W1): a few cubes fail for their own reason with no status (a reset on their
    own zip, an unreadable stored copy) and sort first; the edge refuses every zip with 403 for 30 min, which trips
    the breaker. When it ends, the 20 healthy cubes must merge within one hold - the broken ones go last and one
    failure does not decide the half-open iteration alone."""
    broken = list(CLASS_A[:nbroken])
    trip = list(CLASS_A[nbroken:nbroken + 3])
    good = list(CLASS_A[nbroken + 3:nbroken + 23])
    for p in broken:
        statcan.add(p, "2026-11-18T12:00")
    for p in trip:
        statcan.add(p, "2026-11-18T13:00")
    for p in good:
        statcan.add(p, "2026-11-18T13:30")
    t0 = _t("2026-11-18T14:30Z")
    block_end = t0 + dt.timedelta(minutes=30)
    statcan.zip_status = lambda pid, t: 403 if t < block_end else 200
    _seed(world, broken + trip + good, "2026-10-15T08:30", "2026-11-18T12:50:00Z")
    if mode == "store":
        for p in broken:
            (world / f"{p}.parquet").write_bytes(b"not a parquet file")
    real = statcan.get

    def get(url, *a, **k):
        if mode == "reset" and "zips.example.invalid" in url and url.rsplit("/", 1)[1][:8] in broken:
            statcan.calls.append((statcan.clock.t, "zip", url.rsplit("/", 1)[1][:8], "reset"))
            raise requests.ConnectionError("Connection reset by peer")
        return real(url, *a, **k)
    monkeypatch.setattr(requests, "get", get)
    clock.t = t0
    done = None
    while clock.t < t0 + dt.timedelta(hours=6):
        lane.run(put=_Bucket(), now_fn=clock)
        if all(_state(world)["cubes"][p].get("merged") == "2026-11-18T13:30" for p in good):
            done = clock.t - t0
            break
        clock.step()
    print(f"MEASURED P8 mode={mode} broken={nbroken}: 20 healthy cubes merged after {done}", file=sys.__stdout__,
          flush=True)
    assert done is not None and done <= dt.timedelta(minutes=30 + 70), done


def test_an_absent_cube_does_not_decide_a_half_open_iteration(world, clock, statcan):
    """N11: a released cube we do not hold asks StatCan nothing ('absent'); it must not arm the hold again."""
    statcan.add("14100004", "2026-11-18T13:30")
    statcan.add("99990001", "2026-11-18T13:00")                     # released, not in the store
    _seed(world, ["14100004"], "2026-10-15T08:30", "2026-11-18T13:00:00Z")
    t0 = _t("2026-11-18T14:30Z")
    st = _state(world)
    st["cubes"]["14100004"].update(release="2026-11-18T13:30", transient_fails=1, refused_http=403,
                                   retry_after=_z(t0 + dt.timedelta(hours=3)))
    st["breaker"] = {"status": 403, "cubes": ["14100004"], "trips": 2, "tripped_utc": _z(t0 - dt.timedelta(minutes=30)),
                     "until": _z(t0 - dt.timedelta(minutes=1)), "since": _z(t0 - dt.timedelta(minutes=40))}
    st["last_enum_utc"] = _z(t0 - dt.timedelta(hours=2))
    (world / "_lane_state.json").write_text(json.dumps(st))
    clock.t = t0
    lane.run(put=_Bucket(), now_fn=clock)
    st = _state(world)
    assert st["cubes"]["99990001"].get("not_held") is True
    assert st["breaker"]["trips"] == 2, "an absent cube armed the hold again"


def test_a_serve_owed_cube_is_served_in_a_later_iteration_while_a_hold_lasts(world, clock, statcan):
    """RV21 / C1: serving goes on through a hold, not only in the trip's own iteration."""
    pids = CLASS_A[:3]
    for p in pids:
        statcan.add(p, "2026-11-18T13:30")
    statcan.zip_status = lambda pid, t: 403
    _seed(world, pids, "2026-10-15T08:30", "2026-11-18T13:00:00Z")
    clock.t = t0 = _t("2026-11-18T14:30Z")
    lane.run(put=_Bucket(), now_fn=clock)
    st = _state(world)
    assert lane._breaker_holds(st, t0 + dt.timedelta(minutes=5))
    st["cubes"][str(PID)] = {"release": "2026-11-18T13:31", "merged": "2026-11-18T13:31", "served": None}
    (world / "_lane_state.json").write_text(json.dumps(st))
    clock.t = t0 + dt.timedelta(minutes=5)
    b = _Bucket()
    lane.run(put=b, now_fn=clock)
    assert _state(world)["cubes"][str(PID)]["served"] == "2026-11-18T13:31" and b.put
    assert not [c for c in statcan.calls if c[1] == "zip" and c[0] == clock.t], "and no merge while held"


def test_a_re_release_drops_an_old_strikes_wait(world, clock, statcan):
    """L08 / L09 / V4: a cube with 2 strikes on its old release waits until first + 12 h; StatCan re-releases it,
    and the enumeration that lists the new release works it at once."""
    statcan.add("10100111", "2026-11-18T13:30")
    _seed(world, ["10100111"], "2026-10-15T08:30", "2026-11-18T13:00:00Z")
    t0 = _t("2026-11-18T14:30Z")
    st = _state(world)
    st["cubes"]["10100111"].update(release="2026-11-18T13:30", fails=2, fail_release="2026-11-18T13:30",
                                   first_strike_utc=_z(t0 - dt.timedelta(hours=1)),
                                   retry_after=_z(t0 + dt.timedelta(hours=11)))
    st["last_enum_utc"] = _z(t0 - dt.timedelta(hours=2))
    (world / "_lane_state.json").write_text(json.dumps(st))
    statcan.cubes["10100111"]["release"] = "2026-11-18T18:50"          # re-released
    clock.t = t0
    lane.run(put=_Bucket(), now_fn=clock)
    c = _state(world)["cubes"]["10100111"]
    assert c["merged"] == "2026-11-18T18:50" and not c.get("fails"), c


def test_a_stale_publisher_end_is_not_read_current_while_the_enumeration_fails(world, clock, statcan):
    """P6 / RV23 / R2012: an earlier `publisher` record must not settle a merge as current without a fresh list."""
    statcan.add("10100111", "2026-11-18T13:30")
    _seed(world, ["10100111"], "2026-10-15T08:30", "2026-11-18T13:00:00Z")
    st = _state(world)
    st["cubes"]["10100111"]["release"] = "2026-11-18T13:30"
    st["cubes"]["10100111"]["publisher"] = {"end": "2026-10-01", "freq": 6, "read_utc": "2026-11-18T13:00:00Z"}
    (world / "_lane_state.json").write_text(json.dumps(st))
    statcan.lite_status = lambda t: 403
    clock.t = _t("2026-11-18T14:30Z")
    lane.run(put=_Bucket(), now_fn=clock)
    c = _state(world)["cubes"]["10100111"]
    assert c["merged"] == "2026-11-18T13:30" and c["upstream"]["class"] == "unmeasured", c["upstream"]


def test_an_unexpected_exception_in_the_fetch_published_nothing(world, clock, statcan, monkeypatch):
    """P7 / V5: e.g. a full disk while writing the zip (OSError, not caught by _download)."""
    statcan.add("10100111", "2026-11-18T13:30")
    _seed(world, ["10100111"], "2026-10-15T08:30", "2026-11-18T13:00:00Z")
    st = _state(world)
    st["cubes"]["10100111"]["release"] = "2026-11-18T13:30"
    (world / "_lane_state.json").write_text(json.dumps(st))

    def boom(pid):
        raise OSError(28, "No space left on device")
    monkeypatch.setattr(sc, "_fetch_cube_whole", boom)
    clock.t = _t("2026-11-18T14:30Z")
    lane.run(put=_Bucket(), now_fn=clock)
    c = _state(world)["cubes"]["10100111"]
    assert "merge_in_flight" not in c and c["transient_fails"] == 1 and "OSError" in c["last_error"]


def test_the_breaker_red_starts_after_6_h_in_every_state():
    st = {"cubes": {}}

    def brk(h, since=None):
        return {"status": 403, "cubes": ["10100111"], "trips": 7, "until": _z(VNOW + dt.timedelta(minutes=30)),
                "since": since or _z(VNOW - dt.timedelta(hours=h))}
    assert sc.verdict(st, _beat(breaker=brk(5.9)), VNOW)[0] == "ok"
    for p in (_beat(breaker=brk(6.1)), _beat(breaker=brk(0, since="garbage")),
              _beat(breaker=brk(6.1), owed=DRAINING, last_progress_utc=_z(VNOW - dt.timedelta(hours=1)))):
        with pytest.raises(DefinitiveError, match="circuit breaker has held every merge back"):
            sc.verdict(st, p, VNOW)
    assert "circuit breaker" in _heartbeat()._lane_problem(_beat(breaker=brk(6.1)), VNOW)


def test_a_record_no_cube_waits_on_is_cleared_before_it_ages_into_a_red(world, clock, statcan, capsys):
    """W2 / mutant W2b: the hold has ended and the only merge-owed cube is STRUCK (its own wait, no refused_http):
    the breaker holds nothing, so its record goes - kept, its `since` would read red after 6 h naming a refusal
    that no longer holds anything back."""
    statcan.add("10100111", "2026-11-18T13:30")
    _seed(world, ["10100111"], "2026-10-15T08:30", "2026-11-18T13:00:00Z")
    t0 = _t("2026-11-18T14:30Z")
    st = _state(world)
    st["cubes"]["10100111"].update(release="2026-11-18T13:30", fails=1, fail_release="2026-11-18T13:30",
                                   first_strike_utc=_z(t0 - dt.timedelta(minutes=30)),
                                   retry_after=_z(t0 + dt.timedelta(hours=5)))
    st["breaker"] = {"status": 403, "cubes": ["10100111"], "trips": 3, "tripped_utc": _z(t0 - dt.timedelta(hours=1)),
                     "until": _z(t0 - dt.timedelta(minutes=5)), "since": _z(t0 - dt.timedelta(hours=5, minutes=50))}
    (world / "_lane_state.json").write_text(json.dumps(st))
    clock.t = t0
    lane.run(put=_Bucket(), now_fn=clock)
    assert "breaker" not in _state(world) and "breaker" not in _progress(world)
    assert "circuit breaker cleared: no merge-owed cube waits on it" in capsys.readouterr().out
    clock.t = t0 + dt.timedelta(hours=1)
    lane.run(put=_Bucket(), now_fn=clock)
    assert not _verdict_or_red(world, clock.t).startswith("RED")


def test_the_breaker_and_enumeration_backoffs_stay_under_the_guards_stall_kill():
    src = open(os.path.join(ROOT, "tools", "machine", "RELAUNCH_GUARD.ps1.workstation-copy"),
               encoding="utf-8-sig").read()
    stall_h = float(re.search(r"^\$STALL_HOURS = (\d+(?:\.\d+)?)", src, re.M).group(1))
    assert stall_h == 3
    for cap in (lane.ENUM_BACKOFF_MAX_MIN, lane.BREAKER_BACKOFF_MAX_MIN):
        assert 0 < cap <= 60 and cap * 60 < stall_h * 3600


# --------------------------------------------------------------------------- #
# item 3 - the enumeration cannot end the process
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("exc", [TransientError("statcan getAllCubesListLite HTTP 524 (server='cloudflare')"),
                                 DefinitiveError("statcan getAllCubesListLite HTTP 404"),
                                 KeyError("productId")])
def test_a_failed_enumeration_is_recorded_logged_on_stdout_and_backed_off(world, clock, exc, capsys):
    calls = []

    def enum(since):
        calls.append(clock.t)
        raise exc
    clock.t = t0 = _t("2026-11-17T19:15Z")
    r = lane.run(enumerate_releases=enum, put=_Bucket(), now_fn=clock)
    assert r == {"enumerated": False, "worked": 0, "enum_failed": True}
    ef = _state(world)["enum_fail"]
    assert ef["since"] == _z(t0) and ef["count"] == 1 and ef["retry_after"] == _z(t0 + dt.timedelta(minutes=10))
    assert _progress(world)["state"] == "enumeration_failing" and _progress(world)["enum_fail"] == ef
    cap = capsys.readouterr()
    assert "ENUMERATION FAILED (attempt 1" in cap.out and "ENUMERATION FAILED" not in cap.err
    if isinstance(exc, KeyError):
        assert "Traceback" in cap.out, "a code fault prints its traceback where the guard reads"
    for minutes, want_calls in ((5, 1), (10, 2), (25, 2), (30, 3)):
        clock.t = t0 + dt.timedelta(minutes=minutes)
        lane.run(enumerate_releases=enum, put=_Bucket(), now_fn=clock)        # a new run() = a relaunch, too
        assert len(calls) == want_calls, (minutes, calls)
    assert _state(world)["enum_fail"]["since"] == _z(t0) and _state(world)["enum_fail"]["count"] == 3


def test_owed_cubes_are_still_worked_while_the_enumeration_fails_and_read_unmeasured(world, clock, statcan):
    statcan.add("10100111", "2026-11-18T13:30")
    _seed(world, ["10100111"], "2026-10-15T08:30", "2026-11-18T13:00:00Z")
    st = _state(world)
    st["cubes"]["10100111"]["release"] = "2026-11-18T13:30"            # owed from an earlier enumeration
    (world / "_lane_state.json").write_text(json.dumps(st))
    statcan.lite_status = lambda t: 524
    clock.t = _t("2026-11-18T14:30Z")
    r = lane.run(put=_Bucket(), now_fn=clock)
    c = _state(world)["cubes"]["10100111"]
    assert r["enumerated"] is False and r.get("enum_failed") and c["merged"] == "2026-11-18T13:30"
    assert c["upstream"]["class"] == "unmeasured" and "not read this iteration" in c["upstream"]["reason"]
    assert [k for t, k, p, s in statcan.calls if k == "lite"] == ["lite"] * 5, "no end-date read without a list"


def test_the_enumeration_answering_again_clears_the_record(world, clock, statcan, capsys):
    statcan.lite_status = lambda t: 403
    clock.t = t0 = _t("2026-11-17T19:15Z")
    lane.run(put=_Bucket(), now_fn=clock)
    assert _state(world)["enum_fail"]["http_status"] == 403
    statcan.lite_status = lambda t: 200
    clock.t = t0 + dt.timedelta(minutes=10)
    r = lane.run(put=_Bucket(), now_fn=clock)
    assert r["enumerated"] is True and "enum_fail" not in _state(world) and "enum_fail" not in _progress(world)
    assert "the enumeration answered again after 1 failed attempt(s)" in capsys.readouterr().out


# --------------------------------------------------------------------------- #
# the verdict: the reds are bounded in every state (design review F2/C2)
# --------------------------------------------------------------------------- #
VNOW = dt.datetime(2026, 11, 18, 12, 0, tzinfo=UTC)


def _beat(**kw):
    p = {"beat_utc": _z(VNOW - dt.timedelta(minutes=5)), "state": "idle", "current": None,
         "owed": {"merge": 0, "serve": 0, "quarantined": [], "new_cubes": 0, "oldest_owed_release": None}}
    p.update(kw)
    return p


def _ef(hours_ago, since=None):
    return {"since": since or _z(VNOW - dt.timedelta(hours=hours_ago)), "count": 9, "http_status": 403,
            "last_error": "TransientError: statcan getAllCubesListLite HTTP 403 (server='cloudflare' cf-ray='x')"}


DRAINING = {"merge": 143, "serve": 0, "quarantined": [], "new_cubes": 0, "oldest_owed_release": "2026-06-05T08:30"}


def test_the_enumeration_red_starts_after_6_h_in_every_state():
    st = {"cubes": {}}
    assert sc.verdict(st, _beat(enum_fail=_ef(5.9)), VNOW)[0] == "ok"
    for p in (_beat(enum_fail=_ef(6.1)), _beat(enum_fail=_ef(0, since="garbage")),
              _beat(enum_fail=_ef(0, since=_z(VNOW + dt.timedelta(hours=1)))),
              _beat(enum_fail=_ef(6.1), owed=DRAINING, last_progress_utc=_z(VNOW - dt.timedelta(hours=1)))):
        with pytest.raises(DefinitiveError, match="cannot ENUMERATE"):
            sc.verdict(st, p, VNOW)
    draining = _beat(owed=DRAINING, last_progress_utc=_z(VNOW - dt.timedelta(hours=1)))
    assert "DRAINING" in sc.verdict(st, draining, VNOW)[1], "the control: without enum_fail it drains"
    gh = _heartbeat()
    assert "cannot ENUMERATE" in gh._lane_problem(dict(draining, enum_fail=_ef(6.1)), VNOW)
    assert gh._lane_problem(dict(draining, enum_fail=_ef(5.9)), VNOW) is None


def test_failing_cubes_are_red_even_while_the_lane_drains():
    st = {"cubes": {}}
    p = _beat(owed=dict(DRAINING, failing=["10100111 (6x: whole-table fetch failed: HTTP 403)"]),
              last_progress_utc=_z(VNOW - dt.timedelta(hours=1)))
    with pytest.raises(DefinitiveError, match="keep FAILING"):
        sc.verdict(st, p, VNOW)
    assert "keep FAILING" in _heartbeat()._lane_problem(p, VNOW)


# --------------------------------------------------------------------------- #
# item 5 - merge_in_flight never outlives an attempt that published nothing (and never loses one that did)
# --------------------------------------------------------------------------- #
def _one_owed(world, statcan, prior=None, served="2026-10-15T08:30"):
    statcan.add("10100111", "2026-11-18T13:30")
    _seed(world, ["10100111"], "2026-10-15T08:30", "2026-11-18T13:00:00Z")
    st = _state(world)
    c = st["cubes"]["10100111"]
    c.update(release="2026-11-18T13:30", served=served)
    if prior:
        c["merge_in_flight"] = prior
    (world / "_lane_state.json").write_text(json.dumps(st))


@pytest.mark.parametrize("code", [403, 404])
def test_a_refused_fetch_leaves_no_merge_in_flight(world, clock, statcan, code):
    _one_owed(world, statcan)
    statcan.zip_status = lambda pid, t: code
    clock.t = _t("2026-11-18T14:30Z")
    lane.run(put=_Bucket(), now_fn=clock)
    assert "merge_in_flight" not in _state(world)["cubes"]["10100111"]


def test_a_data_refusal_puts_back_an_earlier_in_flight_merge_and_the_next_merge_serves(world, clock, statcan,
                                                                                       monkeypatch, capsys):
    _one_owed(world, statcan, prior="2026-10-15T08:30")
    real = merge.merge_and_write_bounded

    def refuse(*a, **k):
        raise DefinitiveError("refusing shrink 4->1 at x (< 97% of existing)")      # no cause: a data refusal
    monkeypatch.setattr(merge, "merge_and_write_bounded", refuse)
    clock.t = _t("2026-11-18T14:30Z")
    lane.run(put=_Bucket(), now_fn=clock)
    c = _state(world)["cubes"]["10100111"]
    assert c["merge_in_flight"] == "2026-10-15T08:30" and c["fails"] == 1, "a plain pop would lose it (R1098 P1)"
    monkeypatch.setattr(merge, "merge_and_write_bounded", real)
    st = _state(world)
    st["cubes"]["10100111"]["retry_after"] = None
    (world / "_lane_state.json").write_text(json.dumps(st))
    statcan.cubes["10100111"]["new"] = _rows_for(("2026-08", "2026-09"))            # identical to the store
    capsys.readouterr()
    lane.run(put=_Bucket(), now_fn=clock)
    c = _state(world)["cubes"]["10100111"]
    assert c["merged"] == c["served"] == "2026-11-18T13:30" and "merge_in_flight" not in c
    # an identical table settles as "unchanged" WITHOUT a serve unless an earlier merge is still in flight
    out = capsys.readouterr().out
    assert "merged;" in out and "10100111: served " in out, out      # served by serve_owed_cube, not settled


@pytest.mark.parametrize("exc", [OSError("disk"), "environmental"])
def test_a_failure_inside_the_merge_that_may_follow_the_publish_keeps_the_flag(world, clock, statcan, monkeypatch,
                                                                               exc):
    _one_owed(world, statcan)

    def fail(*a, **k):
        if exc == "environmental":
            raise DefinitiveError("bounded merge at x failed before publishing (OSError)") from OSError("spill")
        raise exc
    monkeypatch.setattr(merge, "merge_and_write_bounded", fail)
    clock.t = _t("2026-11-18T14:30Z")
    lane.run(put=_Bucket(), now_fn=clock)
    c = _state(world)["cubes"]["10100111"]
    assert c["merge_in_flight"] == "2026-11-18T13:30" and c["transient_fails"] == 1 and not c.get("fails")


def test_no_body_bytes_reach_the_state_the_progress_or_the_verdict(world, clock, statcan):
    pids = CLASS_A[:3]
    for p in pids:
        statcan.add(p, "2026-11-18T13:30")
    statcan.zip_status = lambda pid, t: 403
    statcan.lite_status = lambda t: 403
    _seed(world, pids, "2026-10-15T08:30", "2026-11-18T08:00:00Z")
    st = _state(world)
    for p in pids:
        st["cubes"][p]["release"] = "2026-11-18T13:30"
    (world / "_lane_state.json").write_text(json.dumps(st))
    clock.t = _t("2026-11-18T14:30Z")
    for _i in range(100):
        lane.run(put=_Bucket(), now_fn=clock)
        clock.step()
    for f in ("_lane_state.json", "_lane_progress.json"):
        assert IP not in (world / f).read_text(), f
    assert IP not in open(lane.LOCAL_PROGRESS, encoding="utf-8").read()
    v = _verdict_or_red(world, clock.t)
    assert v.startswith("RED") and IP not in v and "cloudflare" in v, v
