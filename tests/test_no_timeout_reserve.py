"""Without a per-unit hard timeout, an unstoppable unit reserves its latest real run (2026-11-06, review AR-450).

WHY. admission_reserve caps a reserve at 2 x unit_timeout_min (90 min) because the per-unit SIGALRM can cut a unit
there. The workstation runs Windows, which has no setitimer, so nothing cuts a unit that does not declare
window_stop: it runs until it ends or the ps1 hard stop kills it, and the kill loses the rest of the pass. AR-450
replayed PR #241's order change (attempt is a turn) and found hard stops rising 8 -> 18 in its 'today' row, all of
them such units (unctad food pair, biotrademerch, oecd, usda) admitted with 90 min left and killed.

THE RULE: hard_timeout False and no declared window_stop -> the reserve is at least last_real_s, the unit's newest
ok/no_change/partial/killed_external duration (StateStore.last_real_duration). Not the MAX of five: eia's MAX holds
one 70,234 s outlier (2026-08-22) and a reserve built on it locked eia out (R1709). The negative controls below
pin that POSIX (hard_timeout True), declared units, never-run units and a disabled timeout keep today's reserve.
"""
from __future__ import annotations

import os
import sqlite3
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import orchestrate  # noqa: E402
from updater.orchestrate import admission_reserve  # noqa: E402


class _U:
    def __init__(self, sid, declared=False):
        self.source_id, self.unit_id = sid, "_all"
        self.config = {"window_stop": True} if declared else {}
        self.key = f"{sid}/_all"


# the 2026-11-06 state copy (F:/work/starve/state_q.log): MAX-of-5 estimate, newest real run
FOOD = (13962.0, 8870.0)       # unctad_tradefoodcatbyproc: kills 13,962 s (09-21) and 8,870 s (10-12, newest)
EIA = (70234.5, 3716.7)        # eia: the 08-22 outlier inside its five, newest ok 3,716.7 s


def test_an_unstoppable_unit_reserves_its_latest_real_run_without_a_hard_timeout():
    need, basis, ws, bad = admission_reserve(_U("unctad_tradefoodcatbyproc"), FOOD[0], 45.0,
                                             last_real_s=FOOD[1], hard_timeout=False)
    assert abs(need - 8870.0 / 60.0) < 1e-9, need          # 147.8 min, not the 90-min cap
    assert "latest real run" in basis and ws is None and bad is None


def test_negative_control_with_a_hard_timeout_the_reserve_is_unchanged():
    need, _b, _w, _x = admission_reserve(_U("unctad_tradefoodcatbyproc"), FOOD[0], 45.0,
                                         last_real_s=FOOD[1], hard_timeout=True)
    assert need == 90.0


def test_the_outlier_in_the_max_of_five_does_not_lock_eia_out():
    # R1709: a reserve from eia's MAX-of-5 (70,234 s = 1,170 min) fits no pass. Its newest real run is 62 min, under
    # the normal 90, so its reserve stays 90.
    need, _b, _w, _x = admission_reserve(_U("eia"), EIA[0], 45.0, last_real_s=EIA[1], hard_timeout=False)
    assert need == 90.0


def test_a_short_unit_keeps_its_small_reserve():
    need, _b, _w, _x = admission_reserve(_U("bis"), 133.7, 45.0, last_real_s=130.0, hard_timeout=False)
    assert need == max(2 * 1.5 * 133.7 / 60.0, 10.0)        # 10 min, as before: the latest run is shorter


def test_a_declared_unit_keeps_its_declared_reserve(monkeypatch):
    monkeypatch.setattr(orchestrate, "_window_stop_reserve", lambda u: (75.0, None))
    need, _b, ws, _x = admission_reserve(_U("unctad_criticalmineralstradebypart", declared=True), 12120.0, 45.0,
                                         last_real_s=10862.7, hard_timeout=False)
    assert need == 75.0 and ws == 75.0


def test_a_never_run_unit_keeps_the_worst_case():
    need, _b, _w, _x = admission_reserve(_U("new_source"), None, 45.0, last_real_s=None, hard_timeout=False)
    assert need == 90.0


def test_a_disabled_timeout_still_reserves_nothing():
    need, _b, _w, _x = admission_reserve(_U("unctad_tradefoodcatbyproc"), FOOD[0], 0.0,
                                         last_real_s=FOOD[1], hard_timeout=False)
    assert need == 0.0


def test_the_platform_default_reads_setitimer(monkeypatch):
    import signal
    monkeypatch.delattr(signal, "setitimer", raising=False)
    assert orchestrate.hard_timeout_available() is False
    need, _b, _w, _x = admission_reserve(_U("unctad_tradefoodcatbyproc"), FOOD[0], 45.0, last_real_s=FOOD[1])
    assert abs(need - 8870.0 / 60.0) < 1e-9
    monkeypatch.setattr(signal, "setitimer", lambda *a: None, raising=False)
    assert orchestrate.hard_timeout_available() is True
    need, _b, _w, _x = admission_reserve(_U("unctad_tradefoodcatbyproc"), FOOD[0], 45.0, last_real_s=FOOD[1])
    assert need == 90.0


# ---- the ceiling: no lock-out (review AR-450 round 2, R1838) -----------------------------------------

TFP = 13456.0          # unctad_tradefoodprocbycat's newest real run (a kill): 224.3 min, longer than every recent pass
HUGE = 400 * 60.0      # a run longer than any pass at all
RECENT_BUDGETS = [35, 20, 60, 40, 30, 55, 40, 40, 90, 72, 223]     # the 11 desktop passes since 2026-10-30, measured
AHEAD_MIN = 13.0       # minutes the units ahead of it may use in the long pass (223 - 210)


def _need(last_real_s):
    return admission_reserve(_U("unctad_tradefoodprocbycat"), last_real_s, 45.0, last_real_s=last_real_s,
                             hard_timeout=False)[0]


def test_the_reserve_never_exceeds_the_ceiling():
    assert _need(TFP) == orchestrate.WORKSTATION_RESERVE_CEILING_MIN == 210.0
    assert _need(HUGE) == 210.0
    assert abs(_need(8870.0) - 8870.0 / 60.0) < 1e-9        # under the ceiling: unchanged (147.8 min)


def test_the_ceiling_is_below_the_longest_measured_pass():
    assert orchestrate.WORKSTATION_RESERVE_CEILING_MIN + AHEAD_MIN <= max(RECENT_BUDGETS)


def _passes_until_admitted(last_real_s, ahead_min, max_passes=60):
    """One desktop pass a day, budgets cycling through RECENT_BUDGETS; the unit's turn comes `ahead_min` minutes into
    each pass. It is refused, so its figure never changes. Returns the 1-based pass that admits it, or None."""
    for k in range(max_passes):
        budget = RECENT_BUDGETS[k % len(RECENT_BUDGETS)]
        if not orchestrate.start_gate_refuses(ahead_min * 60.0, _need(last_real_s), budget * 60.0):
            return k + 1
    return None


@pytest.mark.parametrize("last_real_s", [TFP, HUGE])
def test_a_unit_whose_last_run_is_longer_than_every_budget_is_still_admitted_within_k_passes(last_real_s):
    assert last_real_s / 60.0 > max(RECENT_BUDGETS)
    # K = one cycle of the measured budgets: the 223-min pass, its 11th, admits it with up to 13 min used ahead of it
    assert _passes_until_admitted(last_real_s, ahead_min=AHEAD_MIN) == len(RECENT_BUDGETS)


def test_negative_control_without_the_ceiling_it_is_never_admitted(monkeypatch):
    monkeypatch.setattr(orchestrate, "WORKSTATION_RESERVE_CEILING_MIN", float("inf"))
    assert _passes_until_admitted(TFP, ahead_min=0.0) is None


def test_the_ceiling_alone_is_not_the_guarantee():
    # if the units ahead take more than 13 min of the longest pass, the 210-min reserve no longer fits - the AR-450
    # round-2 sim found exactly this (census and wid run first in lower bands). The band-head rule below closes it.
    assert _passes_until_admitted(TFP, ahead_min=AHEAD_MIN + 1.0) is None


# ---- the band head: the guarantee (review AR-450 round 2, R1838) --------------------------------------------

def test_the_band_head_is_offered_the_normal_reserve():
    need, basis, _w, _x = admission_reserve(_U("unctad_tradefoodprocbycat"), TFP, 45.0, last_real_s=TFP,
                                            hard_timeout=False, band_head=True)
    assert need == 90.0 and "worst case" in basis
    # a declared unit and a disabled timeout are untouched by it
    assert admission_reserve(_U("x"), TFP, 0.0, last_real_s=TFP, hard_timeout=False, band_head=True)[0] == 0.0


def _rotation(band_head_rule, n=5, passes=60, ahead_min=40.0, run_min=400.0):
    """n units in one band, each with a run longer than every pass (400 min, so every admitted turn ends in a hard
    stop, which is recorded as a kill = a turn). Budgets cycle RECENT_BUDGETS; the band's turn comes `ahead_min` into
    the pass (more than the ceiling's 13). Production order_units / last_turn_utc / overdue_key / admission_reserve /
    start_gate_refuses decide. Returns {unit: first pass (1-based) it was admitted, or None}."""
    from datetime import datetime, timedelta, timezone
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    units = [_U(f"g{i}") for i in range(n)]
    kill = {u.source_id: (t0 + timedelta(hours=i)).isoformat() for i, u in enumerate(units)}
    est = {u.source_id: run_min * 60.0 for u in units}
    first = {u.source_id: None for u in units}
    for k in range(passes):
        now = t0 + timedelta(days=1 + k)
        budget = RECENT_BUDGETS[k % len(RECENT_BUDGETS)]
        order = orchestrate.order_units(units, est, lambda u: (orchestrate.overdue_key(
            orchestrate.last_turn_utc({}, kill[u.source_id]), None, now.isoformat()), u.key))
        heads = set()
        for u in order:
            band = orchestrate.cost_band(est[u.source_id])
            head = band_head_rule and band not in heads
            heads.add(band)
            need = admission_reserve(u, est[u.source_id], 45.0, last_real_s=est[u.source_id], hard_timeout=False,
                                     band_head=head)[0]
            if not orchestrate.start_gate_refuses(ahead_min * 60.0, need, budget * 60.0):
                if first[u.source_id] is None:
                    first[u.source_id] = k + 1
                kill[u.source_id] = now.isoformat()       # killed at the hard stop: the pass ends, it was a turn
                break
    return first


def test_every_unit_whose_run_exceeds_every_pass_gets_a_turn_within_k_passes():
    first = _rotation(band_head_rule=True)
    # each admitting pass (a 223-min pass: 90 + 40 ahead fits only there and in none of the others) serves the head,
    # and the head rotates: unit i is admitted in the (i+1)-th 223-min pass, K = n x 11 passes
    assert all(v is not None for v in first.values()), first
    assert sorted(first.values()) == [11, 22, 33, 44, 55], first


def test_negative_control_without_the_band_head_rule_none_is_ever_admitted():
    assert all(v is None for v in _rotation(band_head_rule=False).values())


# ---- the store read -----------------------------------------------------------------------------

def test_last_real_duration_is_the_newest_run_that_got_somewhere(tmp_path):
    from updater.state import DDL, StateStore
    db = str(tmp_path / "state.db")
    con = sqlite3.connect(db)
    con.executescript(DDL)
    con.executemany("INSERT INTO runs(ts_utc,source_id,unit_id,status,obs,dur_s,note) VALUES (?,?,?,?,?,?,?)", [
        ("2026-08-22T21:00:00+00:00", "eia", "_all", "partial", 0, 70234.5, ""),
        ("2026-10-26T01:37:23+00:00", "eia", "_all", "ok", 0, 3716.7, ""),
        ("2026-10-27T00:00:00+00:00", "eia", "_all", "transient_fail", 0, 5.0, ""),      # not a real run
        ("2026-10-12T02:47:52+00:00", "food", "_all", "killed_external", 0, 8870.0, ""),
        ("2026-10-13T00:00:00+00:00", "food", "_all", "window_stopped", 0, 1.0, ""),      # not a turn
    ])
    con.commit()
    con.close()
    st = StateStore(db)
    assert st.last_real_duration() == {"eia": 3716.7, "food": 8870.0}
    est = st.run_cost_estimate()                    # its floor is the same query, so the estimate is unchanged
    assert est["eia"] == 70234.5 and est["food"] == 8870.0


# ---- wiring, through the real run_once ------------------------------------------------------------

def test_run_once_hands_the_latest_real_run_to_admission(tmp_path, monkeypatch):
    from updater.state import StateStore
    monkeypatch.setattr(orchestrate, "hard_timeout_available", lambda: False)     # the workstation, on any machine
    store = StateStore(str(tmp_path / "state.db"))
    store.db.execute("INSERT INTO runs(ts_utc,source_id,unit_id,status,obs,dur_s,note) VALUES (?,?,?,?,?,?,?)",
                     ("2026-10-12T02:47:52+00:00", "istat", "_all", "killed_external", 0, 8870.0, "planted"))
    store.db.commit()
    seen = {}

    class _Stop(Exception):
        pass

    def probe(unit, est_s, unit_timeout_min=None, last_real_s=None, hard_timeout=None, band_head=False):
        assert hard_timeout is False
        seen[unit.source_id] = (last_real_s, band_head)
        raise _Stop()
    monkeypatch.setattr(orchestrate, "admission_reserve", probe)
    try:
        orchestrate.run_once(sources=["istat"], dry=True, store=store)
    except _Stop:
        pass
    # the only unit of its band, and due (no unit state): it is its band's head
    assert seen == {"istat": (8870.0, True)}, seen
