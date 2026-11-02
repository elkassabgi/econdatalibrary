"""Window stops of DECLARED units through the REAL run_once, over several passes (review AR-415 B and C).

B: a window stop made before any durable progress (Result.window_stop == "none") is NOT a turn: no unit state is
   written, so a unit with no success on record does not land on the partial clock (base.Strategy.is_due) and
   stays due; a `window_stopped` runs row keeps it visible (streak + minutes, WARNING past 5 runs or 90 min) and
   no cost estimate reads it.
C: declared units ROTATE: their order key is max(success, attempt, kill, last window_stopped run)
   (orchestrate.declared_last_turn_utc), so a unit whose passes progress and stop no longer holds the head of
   its band for ever (AR-415's wedge). That key can only move a unit BACK, never ahead.

What is REAL: run_once, order_units, overdue_key, last_turn_utc, declared_last_turn_utc, the budget gate,
_window_stop_reserve, _window_stop_no_progress, base.Strategy.is_due, the StateStore (a fresh file),
run_cost_estimate, last_kill_utc, last_window_stop_utc and the leases. What is FAKE: the registry (a unit list),
the fetcher work (a script on a virtual clock), the CSV phase and the leftover drain (stubs). Pattern: review
AR-415's run_once simulator.
"""
from __future__ import annotations

import random
import re
import types
from datetime import datetime, timedelta, timezone

import pytest

from updater import orchestrate, registry
from updater.state import StateStore
from updater.strategies import fetchers
from updater.strategies.base import Result, Strategy, Unit


class _Clock:
    def __init__(self, t):
        self.t = float(t)

    def time(self):
        return self.t

    def sleep(self, s):
        self.t += s

    def __getattr__(self, name):
        import time as _t
        return getattr(_t, name)


def _iso(days_ago):
    return (datetime.now(timezone.utc) - timedelta(days=days_ago)).isoformat(timespec="seconds")


class _Strat(Strategy):
    """The real is_due (partial clock included); a scripted run() on the virtual clock."""
    name = "sim"

    def __init__(self, sim):
        self.sim = sim

    def detect_change(self, unit, us):
        self.sim.clock.t += 6.0
        return "v-new"

    def run(self, unit, since=None):
        return self.sim.work(unit)


class Sim:
    """specs: sid -> dict(est_s, success_d, attempt_d, cadence, declared reserve (min) or None, kind, ...).
    kind "stoppable": work_min of work in 1-min steps, stopping when the time left is under margin_min; a stop
    with no step done returns window_stop "none" (if `none_spends` it spends that many minutes first, the cut
    imts slice), else "progress"; work done is kept across passes. kind "blind": run_min, ignores the ceiling."""

    def __init__(self, tmp_path, monkeypatch, specs):
        self.mp, self.specs = monkeypatch, specs
        self.clock = _Clock(1_900_000_000.0)
        tmp_path.mkdir(parents=True, exist_ok=True)
        self.store = StateStore(str(tmp_path / "state.db"))
        self.left = {sid: sp.get("work_min", 0.0) for sid, sp in specs.items()}
        self.ran: list = []
        for sid, sp in specs.items():
            if sp.get("est_s"):
                self.store.log_run(sid, "_all", "partial", dur_s=float(sp["est_s"]))
            kw = {"strategy": "sim", "status": "partial"}
            if sp.get("success_d") is not None:
                kw.update(last_success_utc=_iso(sp["success_d"]), status="ok")
            if sp.get("attempt_d") is not None:
                kw["last_attempt_utc"] = _iso(sp["attempt_d"])
            if len(kw) > 2:
                self.store.upsert_unit(sid, "_all", **kw)
        self.units = [Unit(source_id=sid, unit_id="_all", strategy="sim", cadence=sp.get("cadence", "monthly"),
                           out_paths=[], config={"run_location": "local", "live": True,
                                                 "window_stop": sp.get("reserve") is not None})
                      for sid, sp in specs.items()]
        mods = {sid: types.SimpleNamespace(window_admission_reserve_min=lambda r=sp["reserve"]: r)
                for sid, sp in specs.items() if sp.get("reserve") is not None}
        for k in ("AQUEDUCT_LIVE_ONLY", "GITHUB_ACTIONS", "AQUEDUCT_UNIT_TIMEOUT_MIN"):
            monkeypatch.delenv(k, raising=False)
        mp = monkeypatch
        mp.setattr(orchestrate, "time", self.clock)
        mp.setattr(orchestrate, "_RUN_DEADLINE_TS", None)
        mp.setattr(registry, "validate", lambda *a, **k: [])
        mp.setattr(registry, "all_units", lambda *a, **k: list(self.units))
        mp.setattr(fetchers, "get", lambda sid: mods[sid])
        mp.setattr(orchestrate, "_has_adapter", lambda u: True)
        mp.setattr(orchestrate, "_wrong_location", lambda u: False)
        mp.setattr(orchestrate, "_protected", lambda u: False)
        mp.setattr(orchestrate, "_is_live", lambda u: True)
        mp.setattr(orchestrate, "_served_by_lane", lambda sid: False)
        mp.setattr(orchestrate, "_derive_changed_csvs", lambda unit, res, blob, st: ([], None, [], {}))
        mp.setattr(orchestrate, "_drain_csv_retry_queue", lambda *a, **k: None)
        mp.setattr(orchestrate, "_drain_leftover_retries", lambda *a, **k: None)
        strat = _Strat(self)
        mp.setattr(orchestrate, "get_strategy", lambda name: strat)

    def work(self, unit):
        sp, sid = self.specs[unit.source_id], unit.source_id
        self.ran.append((self.pass_no, sid))
        if sp.get("kind") == "blind":
            self.clock.t += sp["run_min"] * 60.0
            return Result(status="ok", obs=1, new_vintage="v-new", merged_rows=1)
        dl, steps = orchestrate._RUN_DEADLINE_TS, 0
        while self.left[sid] > 0:
            if dl is not None and dl - self.clock.t < sp["margin_min"] * 60.0:
                break
            self.clock.t += 60.0
            self.left[sid] -= 1.0
            steps += 1
        if self.left[sid] <= 0:
            return Result(status="ok", obs=99, new_vintage="v-new", merged_rows=steps)
        if not steps:
            self.clock.t += sp.get("none_spends", 0.0) * 60.0
        return Result(status="partial", obs=99_000_000, new_vintage="date-tail", merged_rows=0,
                      changed_keys={}, error="deferred by budget, not reached in this pass",
                      window_stop="progress" if steps else "none")

    def run_pass(self, budget, capsys=None):
        self.pass_no = getattr(self, "pass_no", 0) + 1
        self.clock.t += 6 * 3600.0
        self.mp.setenv("AQUEDUCT_RUN_BUDGET_MIN", str(budget))
        orchestrate.run_once(store=self.store)
        return capsys.readouterr().out if capsys else ""

    def turns(self, sid):
        return [p for p, s in self.ran if s == sid]


# ------------------------------------------------------------------ C: rotation (AR-415's wedge)

WEDGE = {
    "s_small": dict(est_s=300, success_d=2, cadence="daily", kind="blind", run_min=5),
    "u_giant": dict(est_s=8000, success_d=60, kind="blind", run_min=100),
    "g_oldsuccess": dict(est_s=7000, success_d=49, reserve=20.0, kind="stoppable", work_min=10_000, margin_min=5),
    "g_wid_like": dict(est_s=4923, success_d=15, cadence="irregular", reserve=20.0, kind="stoppable",
                       work_min=10_000, margin_min=5),
    "g_never_weekly": dict(est_s=8000, attempt_d=17, cadence="weekly", reserve=20.0, kind="stoppable",
                           work_min=10_000, margin_min=5),
}


def test_every_declared_unit_is_admitted_within_three_short_passes(tmp_path, monkeypatch, capsys):
    sim = Sim(tmp_path, monkeypatch, WEDGE)
    for _ in range(5):
        sim.run_pass(85, capsys)
    for sid in ("g_oldsuccess", "g_wid_like", "g_never_weekly"):
        assert sim.turns(sid) and min(sim.turns(sid)) <= 3, (sid, sim.ran)
    assert len(sim.turns("g_oldsuccess")) < 5, "the old-success unit no longer takes every pass"
    assert sim.turns("s_small") == [1], "the small undeclared unit runs when due (daily: pass 1 only here)"
    assert sim.turns("u_giant") == [], "the undeclared giant keeps today's 90-min reserve (85 < 90)"


def test_the_undeclared_giant_still_leads_a_long_window(tmp_path, monkeypatch, capsys):
    sim = Sim(tmp_path, monkeypatch, WEDGE)
    for _ in range(3):
        sim.run_pass(85, capsys)
    out = sim.run_pass(200, capsys)
    assert sim.turns("u_giant") == [4], sim.ran
    first = [m.group(1) for m in re.finditer(r">>> (\w+)/_all", out)]
    assert first[0] == "u_giant", first       # its 60-day-old success outranks every declared unit's fresh turn


def test_declared_key_can_only_move_a_unit_back(monkeypatch):
    rnd = random.Random(7)
    now = datetime.now(timezone.utc)
    stamp = lambda: None if rnd.random() < 0.25 else (now - timedelta(days=rnd.uniform(0, 90))).isoformat()
    for _ in range(2000):
        st = {"last_success_utc": stamp(), "last_attempt_utc": stamp()}
        kill, ws = stamp(), stamp()
        cad = rnd.choice(["daily", "weekly", "monthly", "irregular", None])
        base = orchestrate.overdue_key(orchestrate.last_turn_utc(st, kill), cad, now.isoformat())
        decl = orchestrate.overdue_key(orchestrate.declared_last_turn_utc(st, kill, ws), cad, now.isoformat())
        assert decl >= base, (st, kill, ws)
    assert orchestrate.declared_last_turn_utc({}, None, "garbage") is None


def test_undeclared_units_keep_last_turn_utc(tmp_path, monkeypatch, capsys):
    """Same specs with every declaration removed: no unit is ordered by the declared key."""
    specs = {k: {kk: vv for kk, vv in v.items() if kk != "reserve"} for k, v in WEDGE.items()}
    sim = Sim(tmp_path, monkeypatch, specs)
    called = []
    monkeypatch.setattr(orchestrate, "declared_last_turn_utc", lambda *a: called.append(a))
    sim.run_pass(85, capsys)
    assert called == []


# ------------------------------------------------------------------ B: a no-progress stop is not a turn

TINY = {
    "g_census_like": dict(est_s=6300, attempt_d=9, reserve=17.0, kind="stoppable", work_min=600, margin_min=30),
}


def test_a_no_progress_stop_writes_no_unit_state_and_the_unit_runs_in_the_next_long_window(
        tmp_path, monkeypatch, capsys):
    sim = Sim(tmp_path, monkeypatch, TINY)
    before = sim.store.get_unit("g_census_like", "_all")
    out = sim.run_pass(20, capsys)                         # admitted (17 <= 20) but its next step needs 30
    assert sim.turns("g_census_like") == [1]
    assert "WINDOW STOP g_census_like/_all: stopped at the run ceiling before any durable progress" in out
    assert sim.store.get_unit("g_census_like", "_all") == before, "no status, no last_attempt"
    rows = [r for r in sim.store.recent_runs() if r["source_id"] == "g_census_like"]
    assert rows[0]["status"] == "window_stopped" and "deferred" in rows[0]["note"]
    assert _Strat(sim).is_due(sim.units[0], sim.store.get_unit("g_census_like", "_all"))
    sim.run_pass(120, capsys)
    assert sim.turns("g_census_like") == [1, 2], "still due: the stop did not put it on the partial clock"


def test_a_no_progress_stop_makes_the_unit_a_leftover_drain_candidate(tmp_path, monkeypatch, capsys):
    """Nothing touched its store, so the #212 leftover drain may treat it like NOT DUE (review AR-415 item 5).
    A progressing stop is NOT a candidate (it ran its update)."""
    sim = Sim(tmp_path, monkeypatch, {"g_census_like": dict(TINY["g_census_like"], margin_min=70),
                                      "g_prog": dict(TINY["g_census_like"], margin_min=5)})
    seen = []
    monkeypatch.setattr(orchestrate, "_drain_leftover_retries", lambda c, b, s: seen.append(sorted(c)))
    sim.run_pass(60, capsys)
    assert sim.turns("g_census_like") == [1] and sim.turns("g_prog") == [1]
    assert seen == [["g_census_like"]], seen


def test_a_progressing_stop_is_recorded_as_today(tmp_path, monkeypatch, capsys):
    spec = {"g_census_like": dict(TINY["g_census_like"], margin_min=5)}
    sim = Sim(tmp_path, monkeypatch, spec)
    sim.run_pass(60, capsys)
    st = sim.store.get_unit("g_census_like", "_all")
    assert st["status"] == "partial" and st["last_attempt_utc"] > _iso(1)


def test_an_undeclared_unit_returning_none_is_recorded_as_today(tmp_path, monkeypatch, capsys):
    spec = {"u_liar": {kk: vv for kk, vv in TINY["g_census_like"].items() if kk != "reserve"}}
    sim = Sim(tmp_path, monkeypatch, spec)
    monkeypatch.setenv("AQUEDUCT_UNIT_TIMEOUT_MIN", "0")    # admit it in a short window (reserve 0, as today)
    sim.run_pass(20, capsys)
    assert sim.store.get_unit("u_liar", "_all")["status"] == "partial"


def test_the_streak_warns_after_five_runs_and_on_minutes(tmp_path, monkeypatch, capsys):
    sim = Sim(tmp_path, monkeypatch, TINY)
    outs = [sim.run_pass(20, capsys) for _ in range(5)]
    assert "WARNING g_census_like/_all" not in "".join(outs[:4])
    assert "5 consecutive window_stopped run(s)" in outs[4] and "WARNING g_census_like/_all" in outs[4]
    spec = {"g_cut": dict(TINY["g_census_like"], margin_min=200, none_spends=95)}   # a cut slice: 95 min, "none"
    sim2 = Sim(tmp_path / "b", monkeypatch, spec)
    out = sim2.run_pass(190, capsys)                       # 190 min left < its 200-min margin: "none"
    assert "1 consecutive window_stopped run(s)" in out and "WARNING g_cut/_all" in out


def test_a_real_turn_resets_the_streak(tmp_path, monkeypatch, capsys):
    sim = Sim(tmp_path, monkeypatch, TINY)
    sim.run_pass(20, capsys)
    sim.store.log_run("g_census_like", "_all", "partial", dur_s=60.0)
    assert sim.store.window_stop_streak("g_census_like", "_all") == (0, 0.0)


def test_window_stopped_rows_move_no_cost_estimate(tmp_path):
    store = StateStore(str(tmp_path / "s.db"))
    store.log_run("x", "_all", "partial", dur_s=7000.0)
    for _ in range(6):
        store.log_run("x", "_all", "window_stopped", dur_s=3.0)
    store.log_run("y", "_all", "window_stopped", dur_s=3.0)
    est = store.run_cost_estimate()
    assert est["x"] == 7000.0 and "y" not in est
    assert store.last_kill_utc() == {}
    assert set(store.last_window_stop_utc()) == {("x", "_all"), ("y", "_all")}


def test_a_no_progress_result_with_changed_keys_or_merged_rows_is_a_turn():
    unit = Unit(source_id="d", unit_id="_all", strategy="sim", cadence="monthly", out_paths=[],
                config={"window_stop": True})
    none = dict(status="partial", window_stop="none")
    assert orchestrate._window_stop_no_progress(unit, Result(**none, changed_keys={}, merged_rows=0, obs=10**8))
    assert not orchestrate._window_stop_no_progress(unit, Result(**none, changed_keys={"k": None}))
    assert not orchestrate._window_stop_no_progress(unit, Result(**none, series_cursors={"k": "2026-01-01"}))
    assert not orchestrate._window_stop_no_progress(unit, Result(**none, merged_rows=3))
    assert not orchestrate._window_stop_no_progress(unit, Result(**none, cursor_cap_hit=True))
    assert not orchestrate._window_stop_no_progress(unit, Result(status="ok", window_stop="none"))
    assert not orchestrate._window_stop_no_progress(unit, Result(status="partial", window_stop="progress"))
    undeclared = Unit(source_id="d", unit_id="_all", strategy="sim", cadence="monthly", out_paths=[], config={})
    assert not orchestrate._window_stop_no_progress(undeclared, Result(**none))
