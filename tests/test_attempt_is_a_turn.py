"""An attempt counts as a turn when a pass ORDERS its units (2026-11-06, plan item B2).

WHY. orchestrate.last_turn_utc keyed on `last_success_utc`, falling back to `last_attempt_utc`. A `partial` never
stamps last_success (R231), so a unit whose passes end partial kept its OLD success as its order key and led its
band on every pass. Measured on the workstation route (logs/local_heavy_updater_2026{10,11}*.log, state.db copy
read with immutable=1): istat (success 2026-07-14, partial in every run since) ran in 16 of 35 passes and took 34% of
all unit seconds; on 2026-11-05 istat (7,469 s) and bea (success 2026-09-16, 5,496 s) used the 223-min pass, and
census (no success, attempted 2026-10-27) was BUDGET SKIP or NOT ATTEMPTED in the 11 passes after its last run.

THE RULE: the state stamp is the later of last_success_utc and last_attempt_utc (the kill rule is unchanged). These
tests pin it with the measured stamps, prove the order through the real run_once, and pin the property the fix is
for - every due unit in a band gets a turn within as many admitting passes as the band has units. Each test has a
negative control that reproduces the old clock through the same production function, so it cannot pass by accident.
"""
from __future__ import annotations
import pytest

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.orchestrate import last_turn_utc, order_units, overdue_key  # noqa: E402


@pytest.fixture(autouse=True)
def _restore_run_deadline(monkeypatch):
    """run_once sets the module global orchestrate._RUN_DEADLINE_TS and nothing clears it; left set, it leaks into
    later test files (tests/test_csv_coverage_note.py failed 7 tests on CI, review AR-450 round 5, R1839)."""
    from updater import orchestrate as _orch
    monkeypatch.setattr(_orch, "_RUN_DEADLINE_TS", _orch._RUN_DEADLINE_TS)

NOW = "2026-11-06T22:40:00+00:00"

# unit_state on the desktop lineage, read 2026-11-06 from a copy of state.db (F:/work/starve/state_q.log)
MEASURED = {
    "istat": {"last_success_utc": "2026-07-14T11:23:06+00:00", "last_attempt_utc": "2026-11-06T00:49:23+00:00"},
    "bea": {"last_success_utc": "2026-09-16T23:57:40+00:00", "last_attempt_utc": "2026-11-06T02:20:56+00:00"},
    "census": {"last_success_utc": None, "last_attempt_utc": "2026-10-27T01:46:27+00:00"},
    "eia": {"last_success_utc": "2026-10-26T01:37:23+00:00", "last_attempt_utc": "2026-10-26T01:37:23+00:00"},
    "usda": {"last_success_utc": "2026-10-02T13:39:05+00:00", "last_attempt_utc": "2026-10-02T13:39:05+00:00"},
}
EST_S = {s: 7000.0 for s in MEASURED}       # all band 3 (estimate >= 3600 s for each, measured)


class _U:
    def __init__(self, sid):
        self.source_id, self.unit_id, self.config = sid, "_all", {}
        self.key = f"{sid}/_all"


def _old_clock(st):
    """What the OLD clock read, fed through the SAME production function: success when one exists."""
    return {"last_success_utc": st.get("last_success_utc"),
            "last_attempt_utc": None if st.get("last_success_utc") else st.get("last_attempt_utc")}


def _order(state, old=False):
    def key(u):
        st = state[u.source_id]
        return (overdue_key(last_turn_utc(_old_clock(st) if old else st, None), None, NOW), u.key)
    return [u.source_id for u in order_units([_U(s) for s in state], EST_S, key)]


# ---- the pure rule -------------------------------------------------------------------------

def test_a_newer_attempt_is_the_last_turn():
    assert last_turn_utc(MEASURED["istat"], None) == "2026-11-06T00:49:23+00:00"


def test_a_newer_success_still_wins():
    st = {"last_success_utc": "2026-10-10T00:00:00+00:00", "last_attempt_utc": "2026-10-01T00:00:00+00:00"}
    assert last_turn_utc(st, None) == "2026-10-10T00:00:00+00:00"


def test_a_later_kill_still_wins_over_both():
    assert last_turn_utc(MEASURED["istat"], "2026-11-06T05:00:00+00:00") == "2026-11-06T05:00:00+00:00"


def test_an_unreadable_attempt_never_makes_a_unit_more_overdue():
    st = {"last_success_utc": "2026-10-10T00:00:00+00:00", "last_attempt_utc": "garbage"}
    assert last_turn_utc(st, None) == "2026-10-10T00:00:00+00:00"


def test_an_unreadable_success_yields_to_a_readable_attempt():
    st = {"last_success_utc": "garbage", "last_attempt_utc": "2026-10-10T00:00:00+00:00"}
    assert last_turn_utc(st, None) == "2026-10-10T00:00:00+00:00"


def test_an_attempt_dated_in_the_future_is_ignored():
    st = {"last_success_utc": "2026-07-14T11:23:06+00:00", "last_attempt_utc": "2099-01-01T00:00:00+00:00"}
    assert last_turn_utc(st, None) == "2026-07-14T11:23:06+00:00"


def test_an_offset_less_attempt_does_not_raise():
    st = {"last_success_utc": "2026-07-14T11:23:06+00:00", "last_attempt_utc": "2026-11-06T00:49:23"}
    assert last_turn_utc(st, None) == "2026-11-06T00:49:23"


def test_the_new_stamp_is_never_earlier_than_the_old_one():
    """Every input only makes a unit's OWN key later. (Relative order can still change either way: a unit whose
    key did not move sorts ahead of one whose key moved back.)"""
    from datetime import datetime, timezone

    def p(s):
        d = datetime.fromisoformat(s)
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    for st in MEASURED.values():
        new, old = last_turn_utc(st, None), last_turn_utc(_old_clock(st), None)
        assert p(new) >= p(old), (st, new, old)


# ---- the ordering, with the measured stamps --------------------------------------------------

def test_negative_control_the_old_clock_puts_istat_and_bea_first():
    got = _order(MEASURED, old=True)
    assert got[:2] == ["istat", "bea"], got
    assert got.index("census") > got.index("usda"), got


def test_attempt_clock_puts_the_units_that_waited_first():
    got = _order(MEASURED)
    # waiting since 10-02, 10-26 and 10-27; istat and bea both ran in the 2026-11-05 pass
    assert got == ["usda", "eia", "census", "istat", "bea"], got


# ---- the property: a bounded rotation ----------------------------------------------------------

def _simulate(old, passes, n=8, admit=1):
    """n units in one band, all due every pass (partial: never a success after the first). Each pass admits the
    first `admit` units of the production order and stamps their attempt, as a partial run does. Unit u0 holds an
    OLD success, the shape of istat. Returns {unit: attempts}."""
    from datetime import datetime, timedelta, timezone
    t0 = datetime(2026, 10, 1, tzinfo=timezone.utc)
    state = {f"u{i}": {"last_success_utc": None, "last_attempt_utc": (t0 + timedelta(hours=i)).isoformat()}
             for i in range(n)}
    state["u0"] = {"last_success_utc": "2026-07-14T11:23:06+00:00", "last_attempt_utc": t0.isoformat()}
    est_s = {s: 7000.0 for s in state}
    runs = {s: 0 for s in state}
    for k in range(passes):
        now = (t0 + timedelta(days=1 + k)).isoformat()

        def key(u):
            st = state[u.source_id]
            return (overdue_key(last_turn_utc(_old_clock(st) if old else st, None), None, now), u.key)
        order = [u.source_id for u in order_units([_U(s) for s in state], est_s, key)]
        for s in order[:admit]:
            runs[s] += 1
            state[s]["last_attempt_utc"] = now     # a partial: attempt stamped, success untouched
    return runs


def test_every_unit_gets_a_turn_within_n_admitting_passes():
    runs = _simulate(old=False, passes=8, n=8, admit=1)
    assert all(v == 1 for v in runs.values()), runs


def test_negative_control_the_old_clock_lets_one_partial_unit_take_every_turn():
    runs = _simulate(old=True, passes=8, n=8, admit=1)
    assert runs["u0"] == 8, runs
    assert sum(1 for v in runs.values() if v == 0) == 7, runs


# ---- wiring, through the real run_once ---------------------------------------------------------

def test_run_once_orders_by_the_later_of_success_and_attempt(tmp_path, monkeypatch):
    """order_units is replaced by a probe that records the key run_once hands it and stops the run before anything
    is fetched. istat's key must come from its 2026-11-06 attempt, not its 2026-07-14 success."""
    from updater import orchestrate as orch
    from updater.state import StateStore
    store = StateStore(str(tmp_path / "state.db"))
    for sid in ("istat", "census"):
        st = MEASURED[sid]
        store.upsert_unit(sid, "_all", strategy="planted", status="partial",
                          last_success_utc=st["last_success_utc"], last_attempt_utc=st["last_attempt_utc"])
    seen = {}

    class _Stop(Exception):
        pass

    def probe(units, est, staleness_key, *a, **k):
        for u in units:
            seen[u.source_id] = staleness_key(u)
        raise _Stop()
    monkeypatch.setattr(orch, "order_units", probe)
    try:
        orch.run_once(sources=["istat", "census"], dry=True, store=store)
    except _Stop:
        pass
    assert set(seen) >= {"istat", "census"}, seen
    assert seen["istat"][0] > seen["census"][0], seen      # istat (attempted later) sorts AFTER census
