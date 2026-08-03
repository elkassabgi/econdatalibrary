"""Regression gate: the run order must not let expensive sources starve cheap ones.

WHAT WENT WRONG. The orchestrator ordered units by staleness alone. Staleness is blind to
COST, so the 27 live sources that take >=10 min each (1,031 min of work against a 240-min
budget) interleaved with everything else and the budget ran out after ~20 sources — 76 were
NOT ATTEMPTED on the 2026-08-02 06:00 run. Among the skipped: `cnb`, whose entire run takes
4.9 SECONDS, and `frankfurter` at 5.6s. Both are daily FX feeds on a 2-day SLA; both went
RED-SLA in the health gate having done nothing wrong. A 5-second job cannot meet a 2-day SLA
if it queues behind a 400-minute one.

WHAT THE ORDER MUST GUARANTEE, and what each test here pins:
  1. cheap before expensive, so the cheap band drains early and cheaply;
  2. staleness order preserved WITHIN a band, so the old anti-starvation property still holds;
  3. a never-run source still goes first overall — it has no cost on record precisely because
     it has never had a turn, so "unknown" must not mean "last";
  4. the estimate is the MAX over recent runs, not the latest or the mean: one fast `no_change`
     must not smuggle a 40-minute source into the lane sized for cheap ones.

(4) is the one worth stating twice. Under-estimating is the failure that re-creates the
original bug; over-estimating merely costs a source its place in the fast lane.
"""
from __future__ import annotations
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from updater.orchestrate import order_units   # noqa: E402
from updater.state import StateStore          # noqa: E402


@pytest.fixture()
def store(tmp_path):
    s = StateStore(str(tmp_path / "state.db"))
    yield s
    s.close()


class _Unit:
    """Minimal stand-in: order_units touches only .source_id (via costs) and the key fn."""
    def __init__(self, sid):
        self.source_id = sid
        self.unit_id = "_all"
        self.key = f"{sid}/_all"

    def __repr__(self):
        return self.source_id


def _order(names, costs, state, fast=120.0):
    """Call the PRODUCTION ordering, not a copy of it.

    Re-implementing the rule here would prove only that the test agrees with itself; the
    claim is about what the RUN does, so the run and the test must call the same function.
    """
    units = [_Unit(n) for n in names]
    got = order_units(units, costs, lambda u: (state.get(u.source_id, ""), u.key),
                      fast_lane_seconds=fast)
    return [u.source_id for u in got]


def test_cost_estimate_is_max_over_recent_runs(store):
    # A source that is usually instant but occasionally takes 40 min is EXPENSIVE.
    for d in (2.0, 3.0, 2400.0, 2.5, 1.0):
        store.log_run("spiky", "_all", "ok", dur_s=d)
    store.log_run("steady", "_all", "no_change", dur_s=4.9)

    est = store.run_cost_estimate()
    assert est["spiky"] == 2400.0, (
        "cost estimate must be the MAX over recent runs — taking the latest (1.0s) would put "
        "a 40-minute source in the fast lane, which is the starvation bug this prevents")
    assert est["steady"] == 4.9
    assert "never_run" not in est, (
        "a source with no runs must be ABSENT, not 0.0 — the caller decides what unknown "
        "means rather than being handed a fabricated 'free'")


def test_cost_estimate_window_is_bounded(store):
    # Ancient history must fall out of the window, or a source that was once slow stays
    # branded for ever and never returns to the fast lane after it is fixed.
    store.log_run("reformed", "_all", "ok", dur_s=3000.0)
    for _ in range(5):
        store.log_run("reformed", "_all", "ok", dur_s=5.0)
    assert store.run_cost_estimate(sample=5)["reformed"] == 5.0


def test_cheap_sources_are_not_starved_by_expensive_ones():
    units = ["big_stale", "cnb", "frankfurter", "big_fresh"]
    costs = {"big_stale": 24000.0, "big_fresh": 3000.0, "cnb": 4.9, "frankfurter": 5.6}
    # The expensive one is the STALEST, so under staleness-only it ran first and ate the run.
    state = {"big_stale": "2026-06-01", "cnb": "2026-07-31",
             "frankfurter": "2026-07-31", "big_fresh": "2026-08-02"}

    got = _order(units, costs, state)
    assert got.index("cnb") < got.index("big_stale"), (
        "a 4.9-second source must not queue behind a 400-minute one")
    assert got[:2] == ["cnb", "frankfurter"], got
    assert got[2:] == ["big_stale", "big_fresh"], (
        "within the expensive band the old staleness order must still hold")


def test_never_run_source_goes_first_overall():
    units = ["brand_new", "cheap_old", "expensive_ancient"]
    costs = {"cheap_old": 5.0, "expensive_ancient": 9000.0}   # brand_new absent = never run
    state = {"cheap_old": "2026-07-01", "expensive_ancient": "2026-01-01"}
    assert _order(units, costs, state)[0] == "brand_new", (
        "a never-run source has no cost on record BECAUSE it has never had a turn; "
        "ordering it last is the starvation the whole rule exists to undo")


def test_staleness_still_orders_within_a_band():
    units = ["a", "b", "c"]
    costs = {u: 5.0 for u in units}          # all cheap -> one band
    state = {"a": "2026-08-01", "b": "2026-06-01", "c": "2026-07-01"}
    assert _order(units, costs, state) == ["b", "c", "a"]
