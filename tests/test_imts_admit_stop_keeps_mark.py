"""imf_imts_direct ADMITTED by the start gate and then WINDOW-STOPPING, end to end through the REAL run_once
(review of #215: the advisory gap - no test drove both halves together for this unit).

#215 admits a declared window-stoppable unit (registry `window_stop: true`) on its own reserve (imf_imts_direct:
5 + 30 + 20 = 55 min) and makes a window stop before any durable progress NOT A TURN: no unit_state write, a
`window_stopped` runs row, a leftover-drain candidate, and an order key, declared_last_turn_utc = max(last_turn,
attempt, kill, last window stop), that can only move the unit BACK. #208 gave imf_imts_direct its period slices,
its window stop and its owed mark. These tests hold, for imf_imts_direct itself:

  (a) admitted (60-min pass >= 55), it pulls one slice and stops at the window WITH progress: a real turn -
      `partial`, last_attempt stamped, last_success untouched, a `partial` runs row, the slice kept, the owed
      mark untouched, the csv retry-queue drain reached, NOT a leftover-drain candidate.
  (b) admitted, it stops with NO progress (a kept slow slice from an earlier pass sets a margin the 60-min
      window cannot clear): unit_state byte-identical, the owed mark and the kept slice untouched, a
      `window_stopped` runs row, a leftover-drain candidate, still due by the REAL is_due; on the next run_once
      its place in the order is not improved - it moves behind every unit that did not run - and the debt its
      owed mark carries is still handed over by the first pass that merges.
  (c) control, the twin of (b) with a longer window: the same setup pulls one more slice, so the stop IS a turn
      and the same harness sees every write (b) asserts is absent.

What is REAL: run_once (the gate, _window_stop_reserve over the real fetcher module, order_units,
declared_last_turn_utc, _window_stop_no_progress, the run loop and its records), the imf_imts_direct strategy's
is_due and run, the fetcher (slices, margin, stop, owed mark), the StateStore. What is FAKE: the IMF endpoint and
the fetcher's clock (tests/_imts_fake.py, through test_imts_window_cap's env/rig fixtures), detect_change (a
token, as in that rig), three companion units (budget-skipped: they never run), and the leftover drain (a
recorder - it is a candidate list we assert on, not a drain we run).
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
import time
import types

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tests"))

from test_imts_window_cap import SRC, TABLE_IDS, _slices, env, rig  # noqa: E402,F401  (fixtures)
from updater import config, orchestrate, registry  # noqa: E402
from updater.strategies.base import Unit  # noqa: E402
from updater.strategies.fetchers import imf_imts_direct as F  # noqa: E402

KEY = f"{SRC}/_all"
SLOW_S = 2000.0           # one data request on the fake clock: 33.3 min, so 2 x it (66.7 min) beats a 60-min pass
# Companions: undeclared, same cadence (monthly) and band as imf_imts_direct (3 h on record), BUDGET SKIPPED in
# every pass below (their reserve is the 90-min cap). They never run, so their order keys never move.
CMPS = {"cmp_a": 50, "cmp_b": 30, "cmp_c": 10}          # days since their last success
IMTS_SUCCESS_D, IMTS_ATTEMPT_D = 45, 40


def _iso(days_ago):
    return (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days_ago)).isoformat(timespec="seconds")


def _mark():
    p = os.path.join(config.source_dir(SRC), F.OWED_FILE)
    with open(p, encoding="utf-8") as fh:
        return json.load(fh)


def _plant_mark(obj):
    with open(os.path.join(config.source_dir(SRC), F.OWED_FILE), "w", encoding="utf-8") as fh:
        json.dump(obj, fh)


class _Sim:
    """The companions' strategy. They are budget-skipped, so run() must never be reached."""

    def __init__(self):
        self.due = True

    def is_due(self, unit, us, **k):
        return self.due

    def detect_change(self, unit, us):
        return "cmp:test"

    def run(self, unit, since=None):
        raise AssertionError(f"{unit.key} is budget-skipped in these passes and must not run")


@pytest.fixture
def mrig(rig, monkeypatch):
    """test_imts_window_cap's rig, widened to FOUR units so the start gate applies (a single-source dispatch is
    never budget-gated) and the order has something to be compared with."""
    imts_unit = next(u for u in registry.all_units() if u.source_id == SRC)
    assert imts_unit.config["window_stop"] is True
    comps = [Unit(source_id=s, unit_id="_all", strategy="sim", cadence="monthly", out_paths=[],
                  config={"run_location": "local", "live": True, "window_stop": False}) for s in CMPS]
    monkeypatch.setattr(registry, "all_units", lambda *a, **k: [imts_unit] + comps)
    real_strategy = orchestrate.get_strategy(imts_unit.strategy)
    sim = _Sim()

    class _Imts:
        """The REAL strategy's is_due and run; detect_change answers a token (as in test_imts_window_cap)."""

        def is_due(self, unit, us, **k):
            return real_strategy.is_due(unit, us)

        def detect_change(self, unit, us):
            return "imts:test"

        def run(self, unit, since=None):
            return real_strategy.run(unit, since=since)

    imts_strat = _Imts()
    monkeypatch.setattr(orchestrate, "get_strategy", lambda name: sim if name == "sim" else imts_strat)
    real_adapter = orchestrate._has_adapter
    monkeypatch.setattr(orchestrate, "_has_adapter", lambda u: u.source_id in CMPS or real_adapter(u))
    orders, candidates = [], []
    real_order = orchestrate.order_units

    def order_spy(units, costs, staleness_key, **kw):
        out = real_order(units, costs, staleness_key, **kw)
        orders.append([u.source_id for u in out])
        return out
    monkeypatch.setattr(orchestrate, "order_units", order_spy)
    monkeypatch.setattr(orchestrate, "_drain_leftover_retries",
                        lambda cands, blob, store: candidates.append(sorted(cands)))
    for s in [SRC] + list(CMPS):
        rig.state.log_run(s, "_all", "ok", dur_s=3 * 3600.0)       # same cost band: 3 h on record
    for s, d in CMPS.items():
        rig.state.upsert_unit(s, "_all", strategy="sim", status="ok", last_success_utc=_iso(d),
                              last_attempt_utc=_iso(d))

    def run(budget_min, per_request=SLOW_S):
        monkeypatch.setenv("AQUEDUCT_RUN_BUDGET_MIN", str(budget_min))
        rig.env.imf.per_request = per_request
        rig.env.imf.requests.clear()
        rig.blob.keys.clear()
        rig.env.clock.t = time.time()          # run_once sets its ceiling from the real clock
        res = orchestrate.run_once(sources=[SRC] + list(CMPS), store=rig.state, blob=rig.blob)
        return dict(res)

    def imts_runs():
        return [r for r in rig.state.recent_runs(200) if r["source_id"] == SRC]

    m = types.SimpleNamespace(rig=rig, env=rig.env, state=rig.state, sim=sim, run=run, orders=orders,
                              candidates=candidates, imts_runs=imts_runs, unit=imts_unit,
                              strat=imts_strat)
    yield m


def _seed_resume_after_a_slow_slice(m, monkeypatch):
    """The state a pass 40 days ago left: it pulled ONE slow slice (2000 s on the clock) and stopped at its window
    with progress, recorded as `partial`; its owed mark carries a debt ("owed": an earlier merge's keys were never
    handed over). The slice is kept, and its 2000 s sets the next pass's margin to 2 x 2000 s = 66.7 min."""
    m.env.setup("base")
    _plant_mark({"flow": "IMTS", "form": "owed", "utc": "2026-09-20T10:00:00Z"})
    m.env.imf.up, m.env.imf.per_request = "add", SLOW_S
    m.env.clock.t = time.time() - IMTS_ATTEMPT_D * 86400.0
    m.env.ceiling(3600.0)
    res0 = F.update(None, None)
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", None)
    assert res0.status == "partial" and res0.window_stop == "progress", res0
    assert _slices() == ["slice_1900-1959.parquet"]
    m.state.upsert_unit(SRC, "_all", strategy=m.unit.strategy, status="partial",
                        last_success_utc=_iso(IMTS_SUCCESS_D), last_attempt_utc=_iso(IMTS_ATTEMPT_D),
                        last_error=res0.error)
    assert "owed keys of an earlier merge" in res0.error


# ---- (a) admitted, stops WITH progress: a real turn ----------------------------------------------------------

def test_a_admitted_imts_that_stops_with_progress_records_a_turn(mrig, capsys):
    m = mrig
    m.env.setup("base")                                    # settled owed mark: {}
    m.env.imf.up = "add"
    m.state.upsert_unit(SRC, "_all", strategy=m.unit.strategy, status="ok",
                        last_success_utc=_iso(IMTS_SUCCESS_D), last_attempt_utc=_iso(IMTS_SUCCESS_D))
    before = m.state.get_unit(SRC, "_all")
    t_start = orchestrate.now_utc()
    results = m.run(60)                                    # 60 >= 55: admitted; slice 1 (33 min), then 2 x 33 > 26
    out = capsys.readouterr().out
    assert "WINDOW STOP imf_imts_direct/_all: passes the start gate on its declared 55-min reserve" in out, out
    assert results == {KEY: "partial"}, results
    assert all(f"BUDGET SKIP {s}/_all" in out for s in CMPS), out
    assert len(m.env.imf.data_requests()) == 1
    row = m.state.get_unit(SRC, "_all")
    assert row["status"] == "partial"
    assert row["last_success_utc"] == before["last_success_utc"], "a stop is never a success"
    assert row["last_attempt_utc"] >= t_start, "a progressing stop IS a turn: last_attempt is stamped"
    assert "deferred" in (row["last_error"] or "") and "1960-1969" in row["last_error"], row
    assert m.imts_runs()[0]["status"] == "partial"
    assert not any(r["status"] == "window_stopped" for r in m.imts_runs())
    assert m.state.window_stop_streak(SRC, "_all") == (0, 0.0)
    assert _slices() == ["slice_1900-1959.parquet"], "the pulled slice is kept for a later merge"
    assert _mark() == {}, "a stop never touches the owed mark"
    assert m.rig.drained == [SRC], "a merged_rows=0 partial still reaches the csv retry-queue drain"
    assert m.candidates == [sorted(CMPS)], "it ran its update: NOT a leftover-drain candidate"


# ---- (b) admitted, stops with NO progress: not a turn, keeps its mark ------------------------------------------

def test_b_an_admitted_imts_that_stops_with_no_progress_keeps_its_mark(mrig, monkeypatch, capsys):
    m = mrig
    _seed_resume_after_a_slow_slice(m, monkeypatch)
    before = m.state.get_unit(SRC, "_all")
    mark_before, slices_before = _mark(), _slices()
    assert m.strat.is_due(m.unit, before), "precondition: due"

    # PASS 1: 60 min left >= the 55-min reserve, so the gate ADMITS it; the next slice needs 66.7 min.
    results = m.run(60)
    out = capsys.readouterr().out
    assert "WINDOW STOP imf_imts_direct/_all: passes the start gate on its declared 55-min reserve" in out, out
    assert results == {KEY: "window_stopped"}, results
    assert "stopped at the run ceiling before any durable progress - not recorded as a turn" in out, out
    assert m.env.imf.data_requests() == [], "no slice was pulled"
    assert m.state.get_unit(SRC, "_all") == before, "NO unit_state write: status, stamps and last_error intact"
    assert _mark() == mark_before and _mark()["form"] == "owed", "the owed mark (its debt) is untouched"
    assert _slices() == slices_before, "the kept slice is untouched"
    runs1 = m.imts_runs()
    assert runs1[0]["status"] == "window_stopped" and "all work deferred" in runs1[0]["note"], runs1[0]
    assert m.state.window_stop_streak(SRC, "_all")[0] == 1
    assert m.candidates == [sorted([SRC] + list(CMPS))], "a non-turn IS a leftover-drain candidate"
    assert m.rig.drained == [], "no update result was recorded, so no csv retry-queue drain ran for it"
    assert m.strat.is_due(m.unit, m.state.get_unit(SRC, "_all")), "still due by the REAL is_due"
    order1 = m.orders[-1]
    assert order1 == ["cmp_a", SRC, "cmp_b", "cmp_c"], order1   # key: max(45 d success, 40 d attempt) = 40 d

    # PASS 2: the next run_once. Its place is NOT improved: the window stop is its newest turn, so it moves behind
    # every unit that did not run (cmp_b at 30 d, cmp_c at 10 d) - never ahead of one.
    results = m.run(60)
    out = capsys.readouterr().out
    order2 = m.orders[-1]
    assert order2.index(SRC) >= order1.index(SRC), (order1, order2)
    assert order2 == ["cmp_a", "cmp_b", "cmp_c", SRC], order2
    assert results == {KEY: "window_stopped"}, results
    assert "2 consecutive window_stopped run(s)" in out, out
    assert m.state.get_unit(SRC, "_all") == before, "still no unit_state write"
    assert _mark() == mark_before and _slices() == slices_before

    # PASS 3: a window that fits (companions not due): the kept slice is REUSED and the owed debt is still handed
    # over - every table, not only the one the merge changed.
    m.sim.due = False
    results = m.run(240, per_request=1.0)
    assert results == {KEY: "ok"}, results
    asked = [u.split("?")[1] for u in m.env.imf.data_requests()]
    assert not any(q.startswith("startPeriod=1900&") for q in asked), asked
    assert m.rig.put_ids() == TABLE_IDS, "the debt survived both non-turns: every key handed over"
    assert _mark()["form"] == "handed"
    assert (m.state.get_unit(SRC, "_all")["last_error"] or "").startswith(_mark()["token"])
    assert m.state.window_stop_streak(SRC, "_all") == (0, 0.0), "a real turn ends the streak"


# ---- (c) control: the same setup with a longer window IS a turn ------------------------------------------------

def test_c_control_the_same_setup_with_a_longer_window_is_a_turn(mrig, monkeypatch, capsys):
    m = mrig
    _seed_resume_after_a_slow_slice(m, monkeypatch)
    before = m.state.get_unit(SRC, "_all")
    t_start = orchestrate.now_utc()
    results = m.run(89)                                    # 89 >= 66.7: slice 2 is pulled, then 2 x 33 > 56
    out = capsys.readouterr().out
    assert "WINDOW STOP imf_imts_direct/_all: passes the start gate on its declared 55-min reserve" in out, out
    assert all(f"BUDGET SKIP {s}/_all" in out for s in CMPS), out
    assert results == {KEY: "partial"}, results
    assert len(m.env.imf.data_requests()) == 1
    row = m.state.get_unit(SRC, "_all")
    assert row != before, "the harness sees unit_state writes when the stop is a turn"
    assert row["status"] == "partial" and row["last_attempt_utc"] >= t_start
    assert row["last_success_utc"] == before["last_success_utc"]
    assert row["last_error"] != before["last_error"] and "owed keys of an earlier merge" in row["last_error"]
    assert m.imts_runs()[0]["status"] == "partial"
    assert m.state.window_stop_streak(SRC, "_all") == (0, 0.0)
    assert _slices() == ["slice_1900-1959.parquet", "slice_1960-1969.parquet"]
    assert _mark()["form"] == "owed", "the debt rides on, as in (b)"
    assert m.candidates == [sorted(CMPS)], "it ran its update: not a leftover-drain candidate"
    assert m.rig.drained == [SRC]
    # A progressing stop moves its key back too (last_attempt is now its newest turn).
    m.sim.due = True
    m.run(60)
    assert m.orders[-1] == ["cmp_a", "cmp_b", "cmp_c", SRC], m.orders[-1]
