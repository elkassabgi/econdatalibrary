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
    store = StateStore(str(tmp_path / "state.db"))
    store.db.execute("INSERT INTO runs(ts_utc,source_id,unit_id,status,obs,dur_s,note) VALUES (?,?,?,?,?,?,?)",
                     ("2026-10-12T02:47:52+00:00", "istat", "_all", "killed_external", 0, 8870.0, "planted"))
    store.db.commit()
    seen = {}

    class _Stop(Exception):
        pass

    def probe(unit, est_s, unit_timeout_min=None, last_real_s=None, hard_timeout=None):
        seen[unit.source_id] = last_real_s
        raise _Stop()
    monkeypatch.setattr(orchestrate, "admission_reserve", probe)
    try:
        orchestrate.run_once(sources=["istat"], dry=True, store=store)
    except _Stop:
        pass
    assert seen == {"istat": 8870.0}, seen
