"""census: a stop before the pass's window end, owed-table marks that outlive no kill, merge-measured
change, and stops that never read as ok (PLAN R10.3 step 4; the wid template is econ #205).

Measured on main 5084a528d with production code (F:/work/econ_census_cap_tmp/pk_main.log, and
review AR-407's own instruments):
  * census built Deadline(BUDGET_MIN=20) and never read the run ceiling; on the desktop
    AQUEDUCT_BUDGET_MIN_OVERRIDE=360 replaces the 20, so it started flows until the ps1 HARD STOP;
  * a pass killed after its merges published returned no Result, and the next pass re-merged
    identical rows (added=0), ended no_change and the CSV step never ran; a revision-only
    republish did the same;
  * a budget stop between time windows, or the 6-month tail cap, ended ok/no_change.
Every update()-level test pins the clock (R1701): _flows() -> split_retired() reads today's date.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sqlite3
import sys
import time
import types

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater import merge  # noqa: E402
from updater.errors import DefinitiveError, TransientError  # noqa: E402
from updater.strategies.fetchers import _census_retired as R  # noqa: E402
from updater.strategies.fetchers import census as cs  # noqa: E402

UNIT = types.SimpleNamespace(source_id="census", unit_id="_all")
TODAYS = [dt.date(2026, 10, 30), dt.date(2026, 11, 20)]      # before and after the BETA removal


def _ingest_stub():
    def series_key_of(row, hidx, dim_cols, path):
        parts = [path.replace("timeseries/", "")]
        for c in dim_cols:
            i = hidx.get(c)
            if i is not None and i < len(row) and row[i] not in (None, ""):
                parts.append(f"{c}={row[i]}")
        return "|".join(parts)

    def parse_obs_date(d):
        y, m = str(d).split("-")[:2]
        return dt.date(int(y), int(m), 1)
    return types.SimpleNamespace(series_key_of=series_key_of, parse_obs_date=parse_obs_date)


def _months(first, last):
    y, m = int(first[:4]), int(first[5:7])
    out = []
    while f"{y:04d}-{m:02d}" <= last:
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return out


EITS_H = ["cell_value", "category_code", "program_code", "geo_level_code", "time_slot_id", "time", "us"]
HS_H = ["E_COMMODITY", "CTY_CODE", "ALL_VAL_MO", "time"]
NA_H = ["NAICS", "COMM_LVL", "CTY_CODE", "ALL_VAL_MO", "time"]
COMM = ("NA2", "NA3", "NA4")


def _eits_rows(flow, months, revised=False):
    out = []
    for mo in months:
        for cat in ("C1", "C2"):
            v = 100 + int(mo[5:7]) + (5 if cat == "C2" else 0) + (0.5 if revised and mo == "2026-02" else 0)
            out.append([str(v), cat, "PX", "US", "0", mo, "1"])
    return out


class _Resp:
    def __init__(self, status, body=None, text=""):
        self.status_code = status
        self._body = body
        self.text = text or ("" if body is None else json.dumps(body))
        self.headers = {"Content-Type": "application/json"}

    def json(self):
        return self._body


class _API:
    """A Census-shaped API. eits: `time=from YYYY`; intltrade: one month per request."""

    def __init__(self):
        self.eits = "same"                    # same | new (adds 2026-03) | revise (2026-02 moves)
        self.it_last = "2026-09"              # newest month the intltrade flows publish
        self.fail = set()                     # (flow, time, COMM_LVL or None) -> HTTP 503
        self.slow = set()                     # flows whose request needs the FULL timeout
        self.on_call = None                   # hook(n_call, flow, params)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        flow = url.split("/timeseries/", 1)[1]
        p = dict(params or {})
        self.calls.append((flow, p.get("time"), p.get("COMM_LVL"), timeout))
        if self.on_call:
            self.on_call(len(self.calls), flow, p)
        if flow in self.slow and timeout is not None and timeout < cs.TIMEOUT_LARGE:
            raise requests.Timeout(f"planted: needs {cs.TIMEOUT_LARGE}s, given {timeout}")
        tv = p["time"]
        if (flow, tv, p.get("COMM_LVL")) in self.fail:
            return _Resp(503, text="busy")
        if flow.startswith("eits/"):
            months = ["2026-01", "2026-02"] + (["2026-03"] if self.eits == "new" else [])
            want = tv.replace("from ", "")
            rows = [r for r in _eits_rows(flow, months, self.eits == "revise") if r[5].startswith(want)]
            return _Resp(200, [EITS_H] + rows) if rows else _Resp(204)
        if tv > self.it_last:
            return _Resp(204)
        if flow == "intltrade/exports/hs":
            return _Resp(200, [HS_H] + [[c, "-", f"{int(tv[5:7])}{c}", tv] for c in ("01", "02")])
        if flow == "intltrade/exports/naics":
            cl = p.get("COMM_LVL")
            return _Resp(200, [NA_H, ["11", cl, "-", f"{tv}{cl}", tv]])
        return _Resp(404, text="unknown flow")


class _Sess:
    def __init__(self, api):
        self.api = api
        self.headers = {}

    def get(self, url, params=None, timeout=None):
        return self.api.get(url, params=params, timeout=timeout)


def _write_eits(d, flow):
    rows = _eits_rows(flow, ["2026-01", "2026-02"])
    keys = [f"{flow}|category_code={r[1]}|program_code=PX|geo_level_code=US|us=1" for r in rows]
    pq.write_table(pa.table({
        "series_key": keys,
        "obs_date": pa.array([dt.date(int(r[5][:4]), int(r[5][5:]), 1) for r in rows], pa.date32()),
        "cell_value": [r[0] for r in rows], "category_code": [r[1] for r in rows],
        "program_code": ["PX"] * 4, "geo_level_code": ["US"] * 4, "time_slot_id": ["0"] * 4,
        "time": [r[5] for r in rows], "us": ["1"] * 4}),
        os.path.join(d, cs._store_name(flow) + ".parquet"))


def _write_hs(d, last):
    ms = _months("2025-11", last)
    pq.write_table(pa.table({
        "series_key": [f"intltrade/exports/hs|E_COMMODITY={c}" for _m in ms for c in ("01", "02")],
        "obs_date": pa.array([dt.date(int(m[:4]), int(m[5:]), 1) for m in ms for _c in "ab"], pa.date32()),
        "E_COMMODITY": [c for _m in ms for c in ("01", "02")], "CTY_CODE": ["-"] * (2 * len(ms)),
        "ALL_VAL_MO": [f"{int(m[5:7])}{c}" for m in ms for c in ("01", "02")],
        "time": [m for m in ms for _c in "ab"]}), os.path.join(d, "intltrade__exports__hs.parquet"))


def _write_naics(d):
    rows = [(m, cl) for m in ("2025-12", "2026-01") for cl in COMM]
    pq.write_table(pa.table({
        "series_key": [f"intltrade/exports/naics|NAICS=11|COMM_LVL={cl}" for _m, cl in rows],
        "obs_date": pa.array([dt.date(int(m[:4]), int(m[5:]), 1) for m, _cl in rows], pa.date32()),
        "NAICS": ["11"] * 6, "COMM_LVL": [cl for _m, cl in rows], "CTY_CODE": ["-"] * 6,
        "ALL_VAL_MO": [f"{m}{cl}" for m, cl in rows], "time": [m for m, _cl in rows]}),
        os.path.join(d, "intltrade__exports__naics.parquet"))


@pytest.fixture
def env(tmp_path, monkeypatch):
    """A store dir, a fake API, no run ceiling, the default margin, a pinned clock."""
    monkeypatch.setenv("AQUEDUCT_BACKEND", "local")
    for v in ("AQUEDUCT_BUDGET_MIN_OVERRIDE", "AQUEDUCT_CENSUS_MARGIN_MIN", R.ENV_ACTIVATE):
        monkeypatch.delenv(v, raising=False)
    monkeypatch.setattr(R, "today_utc", lambda: dt.date(2026, 10, 30))
    monkeypatch.setattr(cs.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(cs, "_ingest", _ingest_stub)
    monkeypatch.setattr(cs, "_api_key", lambda: "test-key-not-real")
    monkeypatch.setattr(cs.time, "sleep", lambda s: None)
    monkeypatch.setattr(cs, "RATE", 0.0)
    api = _API()
    monkeypatch.setattr(cs.requests, "Session", lambda: _Sess(api))
    from updater import orchestrate as O
    monkeypatch.setattr(O, "_RUN_DEADLINE_TS", None)
    return types.SimpleNamespace(d=str(tmp_path), api=api, O=O, mp=monkeypatch)


def _max(d, stem):
    return str(max(pq.read_table(os.path.join(d, stem + ".parquet"), columns=["obs_date"])
                   .column("obs_date").to_pylist()))


def _bookmark(d):
    p = os.path.join(d, "_rotation.json")
    return json.load(open(p)).get("after") if os.path.exists(p) else None


class _Killed(BaseException):
    """Stands in for a process kill: not an Exception, so no handler in update() catches it."""


def _kill_after(env, k):
    real = merge.merge_and_write
    n = [0]

    def _m(*a, **kw):
        r = real(*a, **kw)
        n[0] += 1
        if n[0] >= k:
            raise _Killed()
        return r
    env.mp.setattr(cs.merge, "merge_and_write", _m)
    return real


# ---- (b) a kill after the merges published, (c) a revision-only republish -----------------------

@pytest.mark.parametrize("today", TODAYS)
def test_a_pass_after_a_kill_after_the_last_merge_hands_every_table_over(env, today):
    env.mp.setattr(R, "today_utc", lambda: today)
    for f in ("eits/fa", "eits/fb", "eits/fc"):
        _write_eits(env.d, f)
    env.api.eits = "new"
    real = _kill_after(env, 3)
    with pytest.raises(_Killed):
        cs.update(UNIT, None)
    assert all(_max(env.d, s) == "2026-03-01" for s in ("eits__fa", "eits__fb", "eits__fc"))
    env.mp.setattr(cs.merge, "merge_and_write", real)
    res = cs.update(UNIT, None)                 # nothing new upstream: identical re-merges
    assert res.status in ("ok", "partial"), res     # reaches orchestrate's CSV step
    assert sorted(res.series_cursors) == ["eits__fa", "eits__fb", "eits__fc"], res
    assert env.O._should_derive_csvs(res.status, res.changed_keys)
    # the marks settled with that Result: a third quiet pass owes nothing and says so
    res3 = cs.update(UNIT, None)
    assert res3.status == "no_change" and not res3.series_cursors and res3.merged_rows == 0, res3


def test_a_kill_before_the_owed_mark_settles_leaves_merging_marks_on_disk(env):
    for f in ("eits/fa", "eits/fb"):
        _write_eits(env.d, f)
    env.api.eits = "new"
    _kill_after(env, 2)
    with pytest.raises(_Killed):
        cs.update(UNIT, None)
    marks = json.load(open(os.path.join(env.d, cs.OWED_FILE)))
    assert marks == {"eits__fa": "owed", "eits__fb": "merging"}, marks


def test_a_revision_only_republish_reaches_the_csv_step(env):
    _write_eits(env.d, "eits/fa")
    env.api.eits = "revise"
    res = cs.update(UNIT, None)
    assert res.status == "ok" and sorted(res.series_cursors) == ["eits__fa"], res
    assert "revised stored values" in res.error


def test_an_identical_refetch_is_no_change_with_no_cursors_and_merged_rows_zero(env):
    """O2: an identical re-merge is not cursored, and the orchestrator is told nothing merged."""
    _write_eits(env.d, "eits/fa")
    _write_hs(env.d, "2026-09")
    res = cs.update(UNIT, None)
    assert res.status == "no_change" and not res.series_cursors and res.merged_rows == 0, res


def test_a_structural_flow_does_not_strand_what_other_flows_merged(env):
    """Route B (O7): finalize raises on a structural flow, the changed table is still handed over."""
    _write_eits(env.d, "eits/fa")
    _write_eits(env.d, "eits/fb")
    env.api.eits = "new"
    real_get = env.api.get
    env.api.get = lambda url, params=None, timeout=None: (
        _Resp(400, text="error: unknown variable 'planted'") if url.endswith("/fb")
        else real_get(url, params=params, timeout=timeout))
    res = cs.update(UNIT, None)
    assert res.status == "partial" and sorted(res.series_cursors) == ["eits__fa"], res
    assert "REFUSED by the API" in res.error and "HTTP 400" in res.error
    assert json.load(open(os.path.join(env.d, cs.OWED_FILE))) == {}, "the handed-over mark settles"


# ---- (a) the window end ---------------------------------------------------------------------

def test_a_passed_run_ceiling_starts_nothing_even_with_the_desktop_override(env):
    _write_eits(env.d, "eits/fa")
    _write_hs(env.d, "2026-01")
    env.mp.setenv("AQUEDUCT_BUDGET_MIN_OVERRIDE", "360")
    env.mp.setattr(env.O, "_RUN_DEADLINE_TS", time.time() - 1)
    res = cs.update(UNIT, None)
    assert env.api.calls == []
    assert res.status == "partial" and "deferred at the window end" in res.error, res
    assert res.merged_rows == 0


def test_the_margin_floor_stops_before_the_ceiling(env):
    _write_eits(env.d, "eits/fa")
    env.mp.setattr(env.O, "_RUN_DEADLINE_TS", time.time() + 120)     # 2 min left < 5-min floor
    res = cs.update(UNIT, None)
    assert env.api.calls == [] and res.status == "partial", res
    env.mp.setenv("AQUEDUCT_CENSUS_MARGIN_MIN", "0.5")                # 30 s floor: room to work
    res = cs.update(UNIT, None)
    assert [c[0] for c in env.api.calls] == ["eits/fa"] and res.status == "no_change", res


@pytest.mark.parametrize("raw", ["-1", "nan", "inf", "x"])
def test_a_bad_margin_value_falls_back_to_the_default(monkeypatch, raw, capsys):
    monkeypatch.setenv("AQUEDUCT_CENSUS_MARGIN_MIN", raw)
    assert cs._margin_floor_s() == cs.MARGIN_FLOOR_MIN_DEFAULT * 60
    assert "using" in capsys.readouterr().out


def test_no_ceiling_means_no_window_check(env, capsys):
    _write_eits(env.d, "eits/fa")
    res = cs.update(UNIT, None)
    assert res.status == "no_change"
    assert "no run ceiling set - the window-end check is off" in capsys.readouterr().out


def test_every_flow_worked_prints_one_timing_line(env, capsys):
    _write_eits(env.d, "eits/fa")
    _write_hs(env.d, "2026-08")
    cs.update(UNIT, None)
    out = capsys.readouterr().out
    assert out.count("[census] flow ") == 2
    assert "done intltrade/exports/hs:" in out and "request(s)" in out and "total" in out


# ---- O1: the request clamp ------------------------------------------------------------------

def test_a_request_is_clamped_to_the_time_left_and_a_clamped_timeout_is_a_stop():
    seen = []

    class S:
        def get(self, url, params=None, timeout=None):
            seen.append(timeout)
            raise requests.Timeout("planted")
    with pytest.raises(cs._WindowStop):
        cs._fetch(S(), "intltrade/exports/hs", ["ALL_VAL_MO"], "2026-08", "k", None, {},
                  req_end_ts=time.time() + 30)
    assert len(seen) == 1 and seen[0] <= 30.5, seen        # one attempt, clamped, no retry
    seen.clear()
    with pytest.raises(cs._WindowStop):
        cs._fetch(S(), "intltrade/exports/hs", ["ALL_VAL_MO"], "2026-08", "k", None, {},
                  req_end_ts=time.time() - 1)
    assert seen == [], "a request is not started past the reserved tail"


def test_without_a_ceiling_the_request_keeps_its_full_timeout_and_retries(monkeypatch):
    monkeypatch.setattr(cs.time, "sleep", lambda s: None)
    seen = []

    class S:
        def get(self, url, params=None, timeout=None):
            seen.append(timeout)
            raise requests.Timeout("planted")
    with pytest.raises(TransientError):
        cs._fetch(S(), "intltrade/exports/hs", ["ALL_VAL_MO"], "2026-08", "k", None, {})
    assert seen == [cs.TIMEOUT_LARGE] * cs.MAX_ATTEMPTS


def test_a_slow_head_flow_does_not_hold_the_head_over_three_passes(env):
    """O3 ii / R313: the head flow's first request cannot finish in the time left; the stop makes
    no progress, so the bookmark is NOT wound back and the rotation moves on."""
    _write_eits(env.d, "eits/fa")
    _write_hs(env.d, "2026-01")
    env.api.slow.add("intltrade/exports/hs")
    env.mp.setenv("AQUEDUCT_CENSUS_MARGIN_MIN", "0.5")
    with open(os.path.join(env.d, "_rotation.json"), "w") as fh:
        json.dump({"after": "eits/fa"}, fh)                  # hs leads the first pass
    firsts, statuses = [], []
    for _ in range(3):
        env.api.calls.clear()
        env.mp.setattr(env.O, "_RUN_DEADLINE_TS", time.time() + 120)
        res = cs.update(UNIT, None)
        firsts.append(env.api.calls[0][0] if env.api.calls else None)
        statuses.append((res.status, res.error))
    assert firsts[1] == "eits/fa", firsts
    assert firsts.count("intltrade/exports/hs") <= 2, firsts
    assert statuses[0][0] == "partial" and "transient" not in statuses[0][1], statuses


# ---- (g) a stop inside a flow, and the tail cap ---------------------------------------------

def test_a_stop_between_windows_merges_the_complete_prefix_is_partial_and_winds_back(env):
    _write_eits(env.d, "eits/fa")
    _write_hs(env.d, "2026-01")
    env.mp.setenv("AQUEDUCT_CENSUS_MARGIN_MIN", "0.5")
    env.mp.setattr(env.O, "_RUN_DEADLINE_TS", time.time() + 3600)
    with open(os.path.join(env.d, "_rotation.json"), "w") as fh:
        json.dump({"after": "eits/fa"}, fh)                  # hs leads

    off = [0.0]                                              # census's clock, moved by the API
    env.mp.setattr(cs, "time", types.SimpleNamespace(time=lambda: time.time() + off[0],
                                                     sleep=lambda s: None,
                                                     monotonic=time.monotonic))

    def hook(n, flow, params):
        if n == 2:                                           # during hs's 2nd window: ceiling passed
            off[0] = 3600.0
    env.api.on_call = hook
    res = cs.update(UNIT, None)
    assert res.status == "partial", res
    assert "intltrade/exports/hs: stopped at the window end after 2 of 6 window(s)" in res.error
    assert _max(env.d, "intltrade__exports__hs") == "2026-02-01"
    assert sorted(res.series_cursors) == ["intltrade__exports__hs"]
    assert _bookmark(env.d) == "eits/fa", "wound back: the next walk resumes AT hs"


def test_a_failure_merges_only_the_contiguous_prefix_of_complete_windows(env):
    """O4: window 2026-03 fails; 2026-02 merges, nothing from 2026-04 on, no hole."""
    _write_hs(env.d, "2026-01")
    env.api.fail.add(("intltrade/exports/hs", "2026-03", None))
    res = cs.update(UNIT, None)
    assert res.status == "partial" and "transient" in res.error, res
    assert _max(env.d, "intltrade__exports__hs") == "2026-02-01"
    assert sorted(res.series_cursors) == ["intltrade__exports__hs"]


def test_a_failed_split_part_keeps_its_whole_month_out(env):
    _write_naics(env.d)
    env.api.fail.add(("intltrade/exports/naics", "2026-02", "NA3"))
    cs.update(UNIT, None)
    t = pq.read_table(os.path.join(env.d, "intltrade__exports__naics.parquet")).to_pylist()
    assert not [r for r in t if r["obs_date"] >= dt.date(2026, 2, 1)], "no part-month merged"


def test_still_behind_after_the_tail_cap_is_partial(env):
    _write_hs(env.d, "2026-01")                              # upstream to 2026-09
    res = cs.update(UNIT, None)
    assert res.status == "partial" and "6-month tail cap reached at 2026-06" in res.error, res
    assert _max(env.d, "intltrade__exports__hs") == "2026-06-01"


def test_a_store_far_behind_an_upstream_with_nothing_newer_is_not_partial(env):
    """O8: the cap is 'behind' only when its LAST window returned data (R231)."""
    _write_hs(env.d, "2026-01")
    env.api.it_last = "2026-01"
    res = cs.update(UNIT, None)
    assert res.status == "no_change", res


# ---- the owed marker fails closed; the bookmark is honest -----------------------------------

def test_a_corrupt_owed_marker_stops_census_with_the_recovery_line(env):
    _write_eits(env.d, "eits/fa")
    with open(os.path.join(env.d, cs.OWED_FILE), "w") as fh:
        fh.write('{"eits__fa": "maybe"}')
    with pytest.raises(DefinitiveError, match="removing it alone loses every owed table"):
        cs.update(UNIT, None)
    assert env.api.calls == []


def test_a_raising_owed_marker_read_is_transient(env):
    _write_eits(env.d, "eits/fa")
    real = cs.blob.read_bytes
    env.mp.setattr(cs.blob, "read_bytes", lambda p: (_ for _ in ()).throw(OSError("planted"))
                   if p.endswith(cs.OWED_FILE) else real(p))
    with pytest.raises(TransientError, match=cs.OWED_FILE):
        cs.update(UNIT, None)


def test_an_unsaved_pre_merge_mark_means_no_merge(env):
    _write_eits(env.d, "eits/fa")
    env.api.eits = "new"
    real = cs.blob.write_bytes_atomic

    def w(p, data):
        if p.endswith(cs.OWED_FILE):
            raise OSError("planted")
        return real(p, data)
    env.mp.setattr(cs.blob, "write_bytes_atomic", w)
    res = cs.update(UNIT, None)
    assert res.status == "partial" and "pre-merge owed mark not saved" in res.error, res
    assert _max(env.d, "eits__fa") == "2026-02-01", "nothing merged without its mark"


def test_a_raising_bookmark_read_is_a_named_transient(env):
    _write_eits(env.d, "eits/fa")
    real = cs.blob.read_bytes
    env.mp.setattr(cs.blob, "read_bytes", lambda p: (_ for _ in ()).throw(OSError("planted"))
                   if p.endswith("_rotation.json") else real(p))
    with pytest.raises(TransientError, match="_rotation.json"):
        cs.update(UNIT, None)


def test_an_unsaved_bookmark_is_said(env, capsys):
    _write_eits(env.d, "eits/fa")
    real = cs.blob.write_bytes_atomic

    def w(p, data):
        if p.endswith("_rotation.json"):
            raise OSError("planted")
        return real(p, data)
    env.mp.setattr(cs.blob, "write_bytes_atomic", w)
    res = cs.update(UNIT, None)
    assert "rotation bookmark _rotation.json could NOT be saved" in res.error
    assert "NOT saved" in capsys.readouterr().out


def test_a_merge_value_error_costs_the_flow_not_the_source(env):
    """O5: merge raises ValueError for a malformed request of its own."""
    _write_eits(env.d, "eits/fa")
    _write_eits(env.d, "eits/fb")
    env.api.eits = "new"
    real = merge.merge_and_write
    env.mp.setattr(cs.merge, "merge_and_write", lambda p, *a, **k: (
        (_ for _ in ()).throw(ValueError("planted")) if "eits__fa" in p else real(p, *a, **k)))
    res = cs.update(UNIT, None)
    assert res.status == "partial" and "eits/fa: merge refused" in res.error, res
    assert sorted(res.series_cursors) == ["eits__fb"]


# ---- O6: a carried table maps through the real orchestrator, from a mirror level with the store --

def test_a_carried_split_part_table_maps_to_its_catalogue_parts(env, tmp_path):
    _write_eits(env.d, "eits/m3")
    with open(os.path.join(env.d, cs.OWED_FILE), "w") as fh:
        json.dump({"eits__m3": "owed"}, fh)
    res = cs.update(UNIT, None)
    assert res.status == "ok" and res.series_cursors == {"eits__m3": "2026-02-01"}, res
    db = tmp_path / "cat.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?)",
                    [("census:eits__m3#no", "census"), ("census:eits__m3#yes", "census"),
                     ("census:eits__other", "census")])
    con.commit()
    con.close()
    env.mp.setenv("ECONDL_CATALOG", str(db))
    ids, unmapped = env.O._catalog_ids_for("census", res.series_cursors)
    assert sorted(ids) == ["census:eits__m3#no", "census:eits__m3#yes"] and not unmapped


def test_a_carried_table_is_read_from_the_store_object_not_a_stale_mirror(env):
    """Under r2 the derive reads the LOCAL mirror (R383): blob.local_copy's bytes replace it."""
    _write_eits(env.d, "eits/fa")
    path = os.path.join(env.d, "eits__fa.parquet")
    fresh = os.path.join(env.d, "fresh.src")
    t = pq.read_table(path)
    pq.write_table(t.slice(0, 2), path)                      # a stale local mirror
    pq.write_table(t, fresh)                                 # the store object's bytes
    env.mp.setattr(cs.blob, "local_copy", lambda p: (fresh, True) if p == path else (p, False))
    with open(os.path.join(env.d, cs.OWED_FILE), "w") as fh:
        json.dump({"eits__fa": "owed"}, fh)
    cs.update(UNIT, None)
    assert pq.read_table(path).num_rows == 4 and not os.path.exists(fresh)


def test_an_owed_table_with_no_store_object_is_released_by_name(env):
    """AR-407 review 3 ii: no object -> nothing to derive; keeping the mark would be partial for
    ever (R231). Released, named in the note, and the pass is not demoted for it."""
    _write_eits(env.d, "eits/fa")
    with open(os.path.join(env.d, cs.OWED_FILE), "w") as fh:
        json.dump({"eits__gone": "owed"}, fh)
    res = cs.update(UNIT, None)
    assert res.status == "no_change", res
    assert "1 owed table(s) RELEASED, no store object to derive from [eits__gone]" in res.error
    assert json.load(open(os.path.join(env.d, cs.OWED_FILE))) == {}
    assert cs.update(UNIT, None).error == "no new rows", "released once, not every pass"


def test_an_owed_table_the_store_cannot_read_keeps_its_mark(env):
    _write_eits(env.d, "eits/fa")
    _write_eits(env.d, "eits/fb")
    os.replace(os.path.join(env.d, "eits__fb.parquet"), os.path.join(env.d, "bds.parquet"))
    with open(os.path.join(env.d, cs.OWED_FILE), "w") as fh:
        json.dump({"bds": "owed"}, fh)                       # a table no flow of this pass visits
    env.mp.setattr(cs.blob, "local_copy", lambda p: (_ for _ in ()).throw(OSError("planted GET")))
    res = cs.update(UNIT, None)
    assert res.status == "partial" and "not readable from the store" in res.error, res
    assert json.load(open(os.path.join(env.d, cs.OWED_FILE))) == {"bds": "owed"}


def test_owed_tables_are_not_leveled_after_the_stop(env):
    """AR-407 review 3 i: the leveling download is under the stop check."""
    _write_eits(env.d, "eits/fa")
    with open(os.path.join(env.d, cs.OWED_FILE), "w") as fh:
        json.dump({"eits__fa": "owed"}, fh)
    calls = []
    env.mp.setattr(cs.blob, "local_copy", lambda p: calls.append(p) or (p, False))
    env.mp.setattr(env.O, "_RUN_DEADLINE_TS", time.time() - 1)
    res = cs.update(UNIT, None)
    assert calls == [] and res.status == "partial" and "mark kept" in res.error, res
    assert json.load(open(os.path.join(env.d, cs.OWED_FILE))) == {"eits__fa": "owed"}


# ---- the margin itself: floor + 2 x (longest request + longest merge) -------------------------

def _clock(env):
    off = [0.0]
    env.mp.setattr(cs, "time", types.SimpleNamespace(time=lambda: time.time() + off[0],
                                                     sleep=lambda s: None,
                                                     monotonic=time.monotonic))
    return off


def test_the_margin_counts_the_longest_request_not_only_the_floor(env):
    """Ceiling 50 s away, floor 30 s, the first request takes 10 s: 40 s left is above the floor
    (so the request clamp would still start the next request) but below floor + 2 x 10 = 50 s,
    so the next window is NOT requested."""
    _write_hs(env.d, "2026-01")
    env.mp.setenv("AQUEDUCT_CENSUS_MARGIN_MIN", "0.5")
    off = _clock(env)
    env.mp.setattr(env.O, "_RUN_DEADLINE_TS", time.time() + 50)

    def hook(n, flow, params):
        off[0] += 10.0                                       # every request takes 10 s
    env.api.on_call = hook
    res = cs.update(UNIT, None)
    assert [c[1] for c in env.api.calls] == ["2026-01"], env.api.calls
    assert res.status == "partial" and "stopped at the window end after 1 of 6" in res.error, res


def test_a_slow_owed_table_leveling_counts_in_the_margin(env):
    """The leveling download is timed like a request: 10 s of it leaves 40 s, under 30 + 2 x 10."""
    _write_eits(env.d, "eits/fa")
    os.replace(os.path.join(env.d, "eits__fa.parquet"), os.path.join(env.d, "bds.parquet"))
    _write_hs(env.d, "2026-01")
    with open(os.path.join(env.d, cs.OWED_FILE), "w") as fh:
        json.dump({"bds": "owed"}, fh)
    env.mp.setenv("AQUEDUCT_CENSUS_MARGIN_MIN", "0.5")
    off = _clock(env)
    env.mp.setattr(env.O, "_RUN_DEADLINE_TS", time.time() + 50)

    def slow_copy(p):
        off[0] += 10.0
        return (p, False)
    env.mp.setattr(cs.blob, "local_copy", slow_copy)
    res = cs.update(UNIT, None)
    assert env.api.calls == [] and res.status == "partial", (env.api.calls, res)
    assert sorted(res.series_cursors) == ["bds"], "the leveled table is still handed over"


def test_a_stop_after_only_the_boundary_window_does_not_wind_back(env):
    """No progress (the boundary month re-merged, the stored max did not move): the bookmark stays
    on the flow so the rotation moves past it (R313)."""
    _write_eits(env.d, "eits/fa")
    _write_hs(env.d, "2026-01")
    env.mp.setenv("AQUEDUCT_CENSUS_MARGIN_MIN", "0.5")
    off = _clock(env)
    env.mp.setattr(env.O, "_RUN_DEADLINE_TS", time.time() + 3600)
    with open(os.path.join(env.d, "_rotation.json"), "w") as fh:
        json.dump({"after": "eits/fa"}, fh)                  # hs leads

    def hook(n, flow, params):
        if n == 1:
            off[0] = 3600.0                                  # the ceiling passes during window 1
    env.api.on_call = hook
    res = cs.update(UNIT, None)
    assert res.status == "partial" and "after 1 of 6" in res.error, res
    assert _max(env.d, "intltrade__exports__hs") == "2026-01-01"
    assert _bookmark(env.d) == "intltrade/exports/hs", "no progress: not wound back"
