"""A hard-stopped attempt counts as a turn when the desktop pass ORDERS its units (2026-09-17).

WHY. The staleness clock read `last_success_utc or last_attempt_utc`, and a unit killed by the pass's wall
clock writes neither. So a giant that cannot finish in one pass grew more overdue with every kill and led its
cost band forever: unctad_tradefoodcatbyproc was killed on 09-02, 09-14, 09-15 and 09-17 while statcan, census,
eia, oecd and five more giants in the same band were never admitted. A replay of the real pass with the repo's
own run_once/order_units found it; these tests pin the rule and use that replay's numbers.

The negative control reproduces the starvation with the old clock, so the ordering test cannot pass by accident.
"""
from __future__ import annotations

import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.orchestrate import last_turn_utc, order_units, overdue_key  # noqa: E402

NOW = "2026-09-17T22:37:00+00:00"


class _U:
    def __init__(self, sid):
        self.source_id, self.unit_id, self.config = sid, "_all", {}
        self.key = f"{sid}/_all"


# ---- the pure rule -------------------------------------------------------------------------

def test_a_later_kill_is_the_last_turn():
    st = {"last_success_utc": None, "last_attempt_utc": "2026-08-17T07:55:27+00:00"}
    assert last_turn_utc(st, "2026-09-17T02:26:44+00:00") == "2026-09-17T02:26:44+00:00"


def test_an_earlier_kill_does_not_override_a_newer_success():
    st = {"last_success_utc": "2026-09-16T00:00:00+00:00"}
    assert last_turn_utc(st, "2026-09-11T02:34:13+00:00") == "2026-09-16T00:00:00+00:00"


def test_no_kill_leaves_the_old_clock_exactly():
    st = {"last_success_utc": "2026-09-04T01:02:02+00:00", "last_attempt_utc": "2026-09-10T00:00:00+00:00"}
    assert last_turn_utc(st, None) == "2026-09-04T01:02:02+00:00"   # success first, as before
    assert last_turn_utc({}, None) is None                           # never-run stays never-run


def test_a_kill_is_a_turn_for_a_unit_with_no_state_stamp():
    assert last_turn_utc({}, "2026-09-17T02:26:44+00:00") == "2026-09-17T02:26:44+00:00"


def test_a_malformed_kill_never_makes_a_unit_more_overdue():
    st = {"last_attempt_utc": "2026-09-01T00:00:00+00:00"}
    assert last_turn_utc(st, "not-a-date") == "2026-09-01T00:00:00+00:00"


def test_a_malformed_state_stamp_yields_to_a_parseable_kill():
    assert last_turn_utc({"last_attempt_utc": "garbage"}, "2026-09-17T02:26:44+00:00") == "2026-09-17T02:26:44+00:00"


def test_an_offset_less_stamp_on_either_side_does_not_raise():
    """naive vs aware comparison raises TypeError; one hand-written row must not abort the pass."""
    assert last_turn_utc({"last_attempt_utc": "2026-08-17T07:55:27"}, "2026-09-17T02:26:44+00:00") \
        == "2026-09-17T02:26:44+00:00"
    assert last_turn_utc({"last_success_utc": "2026-09-16T00:00:00+00:00"}, "2026-09-11T02:34:13") \
        == "2026-09-16T00:00:00+00:00"
    assert last_turn_utc({"last_success_utc": "2026-09-16"}, "2026-09-17T02:26:44+00:00") \
        == "2026-09-17T02:26:44+00:00"                 # a date-only stamp parses naive too


def test_run_once_feeds_the_kill_into_the_ordering_it_uses(tmp_path, monkeypatch):
    """WIRING, through the real run_once: the staleness key handed to order_units must reflect a kill row.
    order_units is replaced by a probe that records the key and stops the run before anything is fetched."""
    from updater import orchestrate as orch
    from updater.state import StateStore
    store = StateStore(str(tmp_path / "state.db"))
    from datetime import datetime, timedelta, timezone
    recent = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    store.db.execute("INSERT INTO runs(ts_utc,source_id,unit_id,status,obs,dur_s,note) VALUES (?,?,?,?,?,?,?)",
                     (recent, "worldbank", "_all", "killed_external", 0, 9000.0, "planted"))
    store.db.commit()
    seen = {}

    class _Stop(Exception):
        pass

    def probe(units, costs, staleness_key, *a, **k):
        for u in units:
            seen[(u.source_id, u.unit_id)] = staleness_key(u)
        raise _Stop()
    monkeypatch.setattr(orch, "order_units", probe)
    try:
        orch.run_once(sources=["worldbank"], dry=True, store=store)
    except _Stop:
        pass
    key = seen.get(("worldbank", "_all"))
    assert key is not None, f"run_once never reached order_units with worldbank: {sorted(seen)}"
    # the store has no unit_state row for worldbank, so WITHOUT the kill its key is -inf (never run); with a kill five
    # minutes ago it is a small finite number just below zero
    assert -0.01 < key[0] <= 0, key


def test_a_kill_dated_in_the_future_is_ignored():
    st = {"last_attempt_utc": "2026-08-17T07:55:27+00:00"}
    assert last_turn_utc(st, "2099-01-01T00:00:00+00:00") == "2026-08-17T07:55:27+00:00"


# ---- the store read ------------------------------------------------------------------------

def test_store_returns_the_newest_kill_per_unit_and_ignores_other_statuses(tmp_path):
    from updater.state import DDL, StateStore
    db = str(tmp_path / "state.db")
    con = sqlite3.connect(db)
    con.executescript(DDL)
    con.executemany("INSERT INTO runs(ts_utc,source_id,unit_id,status,obs,dur_s,note) VALUES (?,?,?,?,?,?,?)", [
        ("2026-09-02T02:30:52+00:00", "giant", "_all", "killed_external", 0, 10653.0, ""),
        ("2026-09-17T02:26:44+00:00", "giant", "_all", "killed_external", 0, 8907.0, ""),
        ("2026-09-18T00:00:00+00:00", "giant", "_all", "ok", 1, 5.0, ""),          # later, but not a kill
        ("2026-09-11T02:34:13+00:00", "other", "_all", "killed_external", 0, 13567.0, ""),
    ])
    con.commit()
    con.close()
    kills = StateStore(db).last_kill_utc()
    assert kills[("giant", "_all")] == "2026-09-17T02:26:44+00:00"
    assert kills[("other", "_all")] == "2026-09-11T02:34:13+00:00"
    assert ("absent", "_all") not in kills


# ---- the ordering, with the replay's measured values ----------------------------------------

STATE = {
    # never succeeded; last attempt the 2026-08-17 CI partial; killed 4x since, last 09-17
    "unctad_giant": ({"last_success_utc": None, "last_attempt_utc": "2026-08-17T07:55:27+00:00"},
                     "2026-09-17T02:26:44+00:00"),
    # last success a 7.9 s no_change on 09-04; killed once, 09-11
    "statcan": ({"last_success_utc": "2026-09-04T01:02:02+00:00"}, "2026-09-11T02:34:13+00:00"),
    # never killed, never succeeded locally; last attempt 08-18
    "oecd": ({"last_success_utc": None, "last_attempt_utc": "2026-08-18T00:00:00+00:00"}, None),
}
COSTS = {"unctad_giant": 10653.0, "statcan": 13567.0, "oecd": 24402.0}   # all band 3


def _order(count_kills):
    units = [_U(s) for s in STATE]

    def key(u):
        st, kill = STATE[u.source_id]
        last = last_turn_utc(st, kill if count_kills else None)
        return (overdue_key(last, None, NOW), u.key)
    return [u.source_id for u in order_units(units, COSTS, key)]


def test_negative_control_the_old_clock_puts_the_killed_giant_first():
    assert _order(count_kills=False)[0] == "unctad_giant"


def test_counting_kills_puts_the_units_that_never_had_a_turn_first():
    got = _order(count_kills=True)
    assert got[0] == "oecd", got                  # 30 days since its last turn
    assert got.index("statcan") < got.index("unctad_giant"), got
    assert got[-1] == "unctad_giant", got          # killed hours ago: last in the band
