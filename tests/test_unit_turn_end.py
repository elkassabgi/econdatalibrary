"""A unit's own turn end bounds its whole CSV phase (Result.turn_end_ts, orchestrate._UNIT_TURN_END_TS).

WHY (2026-11-24, phase B review B1 of the istat window_stop PRs). istat's per-turn cap (AQUEDUCT_ISTAT_TURN_MAX_MIN)
ended update() at the cap, but run_once's CSV phase after it read only the RUN ceiling: the fresh derive at
min(AQUEDUCT_DERIVE_BUDGET_MIN 45, time left to the run ceiling), then the unit's csv retry drain at another
min(45, ...). Measured on E: (logs/local_heavy_updater_20261024-214510.log and 20261025-220113.log), each of those
turns spent two 45-min budgets after the fetch. So a "60-min" turn could run about 40 + 90 min.

Now a fetcher that knows its own turn end puts it on the Result; run_once sets it just before the unit's CSV phase
and clears it in the unit's `finally`, and _remaining_run_min() is the time to the EARLIER end. Every reader in the
phase follows: _capped_derive_budget (fresh derive, owed repay, in-loop drain) and _csv_fence_min (the phase's
fence and the drain's). The tests below pin the helpers, then go through the REAL run_once with one unit that gives
a turn end and one that does not. Every test that pins the new bound fails on 9f33aed33 (no such field); the
negative controls (no turn end, a later one) hold on both.
"""
from __future__ import annotations

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater import orchestrate, registry  # noqa: E402
from updater.state import StateStore  # noqa: E402
from updater.strategies.base import Result, Strategy, Unit  # noqa: E402


@pytest.fixture
def clean(monkeypatch):
    monkeypatch.delenv("AQUEDUCT_DERIVE_BUDGET_MIN", raising=False)
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", None)
    monkeypatch.setattr(orchestrate, "_UNIT_TURN_END_TS", None)
    yield
    orchestrate._end_unit_turn_end()


def _res(turn_end=None):
    return Result(status="partial", obs=1, series_cursors={"k": "2026-01-01"}, turn_end_ts=turn_end)


def test_without_a_turn_end_nothing_changes(clean, monkeypatch):
    """NEGATIVE CONTROL: no turn end on the Result - the run ceiling alone, as before (45 of 100 min left)."""
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", time.time() + 100 * 60)
    assert orchestrate._begin_unit_turn_end(_res()) is None
    assert orchestrate._remaining_run_min() == pytest.approx(100.0, abs=0.1)
    assert orchestrate._capped_derive_budget()["budget_min"] == pytest.approx(45.0)
    assert orchestrate._csv_fence_min() == pytest.approx(60.0)


def test_the_turn_end_bounds_the_derive_budget_and_the_fence_and_ends_with_the_phase(clean, monkeypatch):
    """Run ceiling 100 min away, the unit's turn ends in 12: the derive budget is 12, not 45, and the fence 12 + 2.
    Cleared, both are back to the run ceiling's."""
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", time.time() + 100 * 60)
    orchestrate._begin_unit_turn_end(_res(time.time() + 12 * 60))
    assert orchestrate._remaining_run_min() == pytest.approx(12.0, abs=0.1)
    assert orchestrate._capped_derive_budget()["budget_min"] == pytest.approx(12.0, abs=0.1)
    assert orchestrate._csv_fence_min() == pytest.approx(14.0, abs=0.1)
    orchestrate._end_unit_turn_end()
    assert orchestrate._capped_derive_budget()["budget_min"] == pytest.approx(45.0)


def test_a_turn_end_after_the_run_ceiling_changes_nothing(clean, monkeypatch):
    """NEGATIVE CONTROL: the run ceiling (10 min) comes first, so it is the bound, as before."""
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", time.time() + 10 * 60)
    orchestrate._begin_unit_turn_end(_res(time.time() + 30 * 60))
    assert orchestrate._capped_derive_budget()["budget_min"] == pytest.approx(10.0, abs=0.1)


def test_with_no_run_ceiling_the_turn_end_alone_bounds_it(clean):
    """No run ceiling (outside run_once, or AQUEDUCT_RUN_BUDGET_MIN <= 0): before, the derive was unbounded ({}).
    With a turn end of 20 min it is min(45, 20)."""
    assert orchestrate._capped_derive_budget() == {}
    orchestrate._begin_unit_turn_end(_res(time.time() + 20 * 60))
    assert orchestrate._capped_derive_budget()["budget_min"] == pytest.approx(20.0, abs=0.1)


def test_a_turn_end_already_passed_defers_everything(clean, monkeypatch):
    """The fetch ran to its own turn end: the derive gets the 0.05-min floor (it defers every id to csv_retry_queue,
    never an unbounded 0) and the fence its past-the-ceiling floor."""
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", time.time() + 100 * 60)
    orchestrate._begin_unit_turn_end(_res(time.time() - 60))
    assert orchestrate._capped_derive_budget()["budget_min"] == pytest.approx(0.05)
    assert orchestrate._csv_fence_min() == orchestrate._PAST_CEILING_MIN


@pytest.mark.parametrize("bad", ["soon", float("nan"), float("inf"), object()])
def test_a_turn_end_that_is_not_a_finite_number_is_ignored(clean, monkeypatch, bad):
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", time.time() + 100 * 60)
    assert orchestrate._begin_unit_turn_end(_res(bad)) is None
    assert orchestrate._capped_derive_budget()["budget_min"] == pytest.approx(45.0)


# ------------------------------------------------------------------ through the REAL run_once

class _Strat(Strategy):
    name = "sim"

    def __init__(self, plan):
        self.plan = plan

    def detect_change(self, unit, us):
        return "v-new"

    def run(self, unit, since=None):
        return self.plan[unit.source_id]()


def _run_once(tmp_path, monkeypatch, plan, derive=None):
    """run_once over the units in `plan` ({source: () -> Result}) with the CSV phase stubbed to RECORD what the real
    phase would be given: _capped_derive_budget() and _csv_fence_min() inside the fresh derive, and
    _capped_derive_budget() inside the in-loop retry drain (it is called with no deadline, so that is its budget)."""
    seen = {}
    units = [Unit(source_id=s, unit_id="_all", strategy="sim", cadence="daily", out_paths=[],
                  config={"run_location": "local", "live": True}) for s in plan]
    mp = monkeypatch
    for k in ("AQUEDUCT_LIVE_ONLY", "GITHUB_ACTIONS", "AQUEDUCT_UNIT_TIMEOUT_MIN", "AQUEDUCT_DERIVE_BUDGET_MIN"):
        mp.delenv(k, raising=False)
    mp.setenv("AQUEDUCT_RUN_BUDGET_MIN", "240")
    mp.setattr(orchestrate, "_RUN_DEADLINE_TS", None)
    mp.setattr(orchestrate, "hard_timeout_available", lambda: False)
    mp.setattr(registry, "validate", lambda *a, **k: [])
    mp.setattr(registry, "all_units", lambda *a, **k: list(units))
    for name, fn in (("_has_adapter", lambda u: True), ("_wrong_location", lambda u: False),
                     ("_protected", lambda u: False), ("_is_live", lambda u: True),
                     ("_served_by_lane", lambda sid: False)):
        mp.setattr(orchestrate, name, fn)

    def _derive(unit, res, blob, st):
        seen.setdefault(unit.source_id, {})["derive"] = orchestrate._capped_derive_budget().get("budget_min")
        seen[unit.source_id]["fence"] = orchestrate._csv_fence_min()
        if derive:
            derive(unit)
        return [], None, [], {}

    def _drain(unit, blob, store, *a, **k):
        seen.setdefault(unit.source_id, {})["drain"] = orchestrate._capped_derive_budget().get("budget_min")

    def _leftover(*a, **k):
        seen["leftover_turn_end"] = orchestrate._UNIT_TURN_END_TS
        seen["leftover_rem"] = orchestrate._remaining_run_min()

    mp.setattr(orchestrate, "_derive_changed_csvs", _derive)
    mp.setattr(orchestrate, "_drain_csv_retry_queue", _drain)
    mp.setattr(orchestrate, "_drain_leftover_retries", _leftover)
    mp.setattr(orchestrate, "get_strategy", lambda name: _Strat(plan))
    store = StateStore(str(tmp_path / "state.db"))
    orchestrate.run_once(store=store)
    return seen, store


def test_run_once_bounds_the_csv_phase_of_a_unit_with_a_turn_end_and_only_that_unit(tmp_path, monkeypatch):
    """`capped` returns a turn end 7 min ahead (its fetcher's per-turn cap); `plain` returns none. In the real
    run_once, capped's fresh derive and its retry drain get 7 min and its fence 9; plain's get the run's 45 and
    60, so the turn end did not leak into the next unit; and nothing is left set after the run."""
    plan = {"capped": lambda: _res(time.time() + 7 * 60), "plain": lambda: _res()}
    seen, _store = _run_once(tmp_path, monkeypatch, plan)
    assert seen["capped"]["derive"] == pytest.approx(7.0, abs=0.2), seen
    assert seen["capped"]["drain"] == pytest.approx(7.0, abs=0.2), seen
    assert seen["capped"]["fence"] == pytest.approx(9.0, abs=0.2), seen
    assert seen["plain"]["derive"] == pytest.approx(45.0) and seen["plain"]["drain"] == pytest.approx(45.0), seen
    assert seen["plain"]["fence"] == pytest.approx(60.0), seen
    assert orchestrate._UNIT_TURN_END_TS is None


def test_the_turn_end_is_cleared_when_the_csv_phase_raises(tmp_path, monkeypatch):
    """capped's CSV phase raises: run_once books it transient_fail (the outer except) and the unit's `finally`
    clears the turn end, so plain's phase still gets the run's 45 min."""
    def boom(unit):
        if unit.source_id == "capped":
            raise RuntimeError("planted: the derive crashed")

    plan = {"capped": lambda: _res(time.time() + 7 * 60), "plain": lambda: _res()}
    seen, store = _run_once(tmp_path, monkeypatch, plan, derive=boom)
    assert seen["capped"]["derive"] == pytest.approx(7.0, abs=0.2), seen
    assert seen["plain"]["derive"] == pytest.approx(45.0), seen
    assert (store.get_unit("capped", "_all") or {}).get("status") == "transient_fail"
    assert orchestrate._UNIT_TURN_END_TS is None


def test_the_leftover_drain_after_the_loop_never_sees_a_units_turn_end(tmp_path, monkeypatch):
    """The capped unit is the LAST (only) unit: after its turn, the leftover drain that follows the unit loop reads
    the run ceiling (about 240 min), not the capped unit's 7 - the clear in the unit's `finally` did it, since no
    later unit's set would have."""
    seen, _store = _run_once(tmp_path, monkeypatch, {"capped": lambda: _res(time.time() + 7 * 60)})
    assert seen["capped"]["derive"] == pytest.approx(7.0, abs=0.2), seen
    if "leftover_rem" in seen:
        assert seen["leftover_turn_end"] is None and seen["leftover_rem"] > 200.0, seen
    assert orchestrate._UNIT_TURN_END_TS is None


def test_the_unit_turn_end_is_set_only_around_the_csv_phase_source_order():
    """The set precedes the CSV phase's first reader and the clear is in the unit's `finally` (pinned in the
    source, so a refactor that moves the set after the fence, or drops the clear, fails here)."""
    import inspect
    src = inspect.getsource(orchestrate.run_once)
    i_set = src.find("_begin_unit_turn_end(res)")
    i_fence = src.find("_csv_fence = _csv_fence_min()")
    i_drain = src.find("_drain_csv_retry_queue(unit, blob, store)")
    i_fin = src.find("_end_unit_turn_end()")
    assert -1 not in (i_set, i_fence, i_drain, i_fin), (i_set, i_fence, i_drain, i_fin)
    assert i_set < i_fence < i_drain < i_fin
    assert "        finally:\n            _end_unit_turn_end()" in src.replace("\r\n", "\n"), \
        "the clear is not the first statement of the unit's finally"
