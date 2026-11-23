"""statcan: a quarantined cube stays on the public owed clock (2026-11-23; ledger R2026, R2074, R2075).

THE DEFECT. jobs/statcan_lane.summarise() kept quarantined cubes off the oldest-owed clock, and the reporter's amber
"N cube(s) QUARANTINED" opening is in LANE_CURRENT_PATTERN, so statcan never read "late" on the public API while it
held cubes it had not merged for weeks (R2026: 3 cubes owed 54-58 days). A cube re-released while it still owed a
merge restarted its clock too: run() lifts a quarantine on a re-release, and the strike rule needs strikes spanning
QUARANTINE_MIN_SPAN_H (12 h) to put it back, so for those hours the cube is owed and not quarantined (R2074).

THE CHANGE. An owed-since clock (statcan.owed_since_clock): every merge-owed cube that is quarantined or carries a
valid `owed_since` (the oldest release it owes, kept across re-releases by run(), popped by a merge). Past
OWED_AGE_MAX_DAYS the reporter's note opens "statcan: N cube(s) are BEHIND", which the whitelist does not match, so
the public side reads late. LANE_CURRENT_PATTERN, the worker and CI's green outcome are unchanged.

Only the network and the bucket are faked (tests/test_statcan_lane.py, tests/test_statcan_strike_rule.py).
"""
from __future__ import annotations

import datetime as dt
import json
import os
import re
import sys

import pytest

pytest.importorskip("pyarrow")
pytest.importorskip("duckdb")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import jobs.statcan_lane as lane  # noqa: E402
from updater.errors import DefinitiveError  # noqa: E402
from updater.strategies.fetchers import statcan as sc  # noqa: E402
from tests.test_statcan_lane import (PID, REL, STORED_2013, _Bucket, _heartbeat, _history_world,  # noqa: E402,F401
                                     _net, _no_network, _rel, _row, _state, _stored, world)
from tests.test_statcan_strike_rule import (_progress, _seed, _t, clock, statcan)  # noqa: E402,F401

RX = re.compile(sc.LANE_CURRENT_PATTERN)
NOW = dt.datetime(2026, 11, 23, 12, 0, tzinfo=dt.timezone.utc)
BEHIND = "statcan: 1 cube(s) are BEHIND"


def _z(t):
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def _rel_days_ago(d, now=NOW):
    return (now - dt.timedelta(days=d)).strftime("%Y-%m-%dT%H:%M")


def _beat(st, now=NOW, **extra):
    b = {"beat_utc": _z(now - dt.timedelta(minutes=3)), "current": None, "state": "idle", "owed": lane.summarise(st)}
    b.update(extra)
    return b


def _held(release, **more):
    c = {"release": release, "quarantined": True, "fails": 3, "fail_release": release,
         "merged": "2026-08-01T12:30", "served": "2026-08-01T12:30"}
    c.update(more)
    return c


def _note(st, beat, now=NOW):
    """(status, note, current?) - a raise is red and reads not current."""
    try:
        status, msg, _n = sc.verdict(st, beat, now)
    except DefinitiveError as e:
        status, msg = "RED", str(e)
    return status, msg, bool(RX.match(msg))


# --------------------------------------------------------------------------- #
# T1 / T2 / T8 - summarise() names the held cube, verdict() reads it not current
# --------------------------------------------------------------------------- #
def test_t1_summarise_puts_a_quarantined_cube_on_the_owed_since_clock_and_keeps_every_old_key():
    st = {"cubes": {"10100145": _held(_rel_days_ago(24))}}
    o = lane.summarise(st)
    assert o["owed_since_clock"] == [{"pid": "10100145", "owed_since": _rel_days_ago(24), "kind": "quarantined"}]
    assert o["oldest_owed_since"] == _rel_days_ago(24)
    # PINS: the keys the lift tool and the old tests read keep their meaning
    assert o["quarantined"] == ["10100145"] and o["oldest_owed_release"] is None and o["merge"] == 0


def test_t2_a_quarantined_cube_past_the_sla_reads_not_current_and_one_inside_it_reads_current():
    st = {"cubes": {"10100145": _held(_rel_days_ago(24))}}
    status, msg, cur = _note(st, _beat(st))
    assert status == "partial" and not cur and msg.startswith(BEHIND), msg
    assert "10100145 (owed since " in msg and "quarantined)" in msg
    assert "| 1 cube(s) QUARANTINED" in msg, "the amber segment still follows, after the BEHIND one"
    # CONTROL (passes on main too): the same cube released 5 days ago is an owed cube inside the SLA
    st = {"cubes": {"10100145": _held(_rel_days_ago(5))}}
    status, msg, cur = _note(st, _beat(st))
    assert status == "partial" and cur and msg.startswith("statcan: 1 cube(s) QUARANTINED "), msg


@pytest.mark.parametrize("days, current", [(14, True), (15, False)])
def test_t8_the_boundary_is_the_owed_age_blocks_own_greater_than(days, current):
    st = {"cubes": {"1": _held(_rel_days_ago(days))}}
    assert _note(st, _beat(st))[2] is current


def test_an_unreadable_clock_counts_as_past_like_the_owed_age_block():
    st = {"cubes": {"1": _held("not-a-date")}}
    status, msg, cur = _note(st, _beat(st))
    assert not cur and msg.startswith(BEHIND) and "unreadable" in msg, msg


# --------------------------------------------------------------------------- #
# C1 (R2074) - the re-strike window, driven through the lane's real run()
# --------------------------------------------------------------------------- #
def test_c1_a_re_released_held_cube_is_not_current_at_any_iteration_of_the_re_strike_window(world, clock, statcan):
    """The LAST past-SLA held cube is re-released. run() lifts its quarantine (statcan_lane.run, RE-RELEASED), the
    count gate keeps refusing it (a zip behind the release, as class C), and the strike rule re-quarantines it only
    after strikes spanning 12 h. At EVERY 5-minute iteration of that window summarise() -> verdict() must not read
    current. On main the first iteration reads 'lane current' (the cube is owed for a 5-minute-old release)."""
    p = "18100212"
    old, new = "2026-11-05T13:30", "2026-12-08T13:30"
    statcan.add(p, new, rebuilt=_t("2027-03-01T00:00Z"))          # the zip never catches up: refused every time
    _seed(world, [p], "2026-10-01T12:30", "2026-12-08T10:00:00Z", extra={p: dict(
        _held(old, merged="2026-10-01T12:30", served="2026-10-01T12:30"), first_strike_utc="2026-11-05T15:00:00Z")})
    t0 = clock.t = _t("2026-12-08T13:35Z")
    seen = []
    while clock.t <= t0 + dt.timedelta(hours=12, minutes=5):
        lane.run(put=_Bucket(), now_fn=clock)
        c = _state(world)["cubes"][p]
        status, msg, cur = _note(_state(world), _progress(world), clock.t)
        assert not cur, (clock.t, msg)
        assert status == "partial" and msg.startswith(BEHIND), (clock.t, msg)
        assert c.get("owed_since") == old, (clock.t, c)
        assert f"{p} (owed since 2026-11-05, " in msg, msg
        seen.append((clock.t - t0, bool(c.get("quarantined")), int(c.get("fails") or 0)))
        clock.step()
    # THE WINDOW WAS EXERCISED (R2029): owed and NOT quarantined from the re-release to +12 h, through +1, +6, +11 h,
    # then quarantined again by the third spaced strike
    out = {h: q for d, q, _f in seen for h in (0, 1, 6, 11, 12) if d == dt.timedelta(hours=h)}
    assert out == {0: False, 1: False, 6: False, 11: False, 12: True}, out
    assert [f for d, _q, f in seen if d in (dt.timedelta(0), dt.timedelta(hours=1), dt.timedelta(hours=12))] == [1, 2, 3]
    assert sum(1 for _d, q, _f in seen if not q) >= 12 * 12, "the not-quarantined window lasted the 12 h"


def test_c1_a_cube_owed_across_a_re_release_is_named_so(world, clock, statcan):
    p = "18100212"
    statcan.add(p, "2026-12-08T13:30", rebuilt=_t("2027-03-01T00:00Z"))
    _seed(world, [p], "2026-10-01T12:30", "2026-12-08T10:00:00Z", extra={p: _held("2026-11-05T13:30")})
    clock.t = _t("2026-12-09T01:00Z")
    lane.run(put=_Bucket(), now_fn=clock)
    o = _progress(world)["owed"]
    assert o["owed_since_clock"] == [{"pid": p, "owed_since": "2026-11-05T13:30", "kind": sc.OWED_KIND_CARRIED}]
    assert o["quarantined"] == [] and o["oldest_owed_release"] == "2026-12-08T13:30", \
        "the release clock restarted - the defect the owed-since clock covers"


# --------------------------------------------------------------------------- #
# T3 / T4 - owed_since is set on a re-release, kept across the next, popped by a merge
# --------------------------------------------------------------------------- #
def test_t3_owed_since_survives_a_second_re_release_and_is_not_set_for_a_merged_cube(world, monkeypatch):
    _net(monkeypatch, [_row("2026-01", "Windsor", "v1", "1.1", "1")], (99, 1))      # refused: short table
    st = {"version": 1, "floor": lane.FLOOR, "parsed_rows": {}, "new_cubes": {}, "last_enum_utc": None,
          "cubes": {str(PID): {"release": "2026-09-01T08:30", "merged": "2026-08-01T08:30", "served": "2026-08-01T08:30"},
                    "10100001": {"release": "2026-09-01T08:30", "merged": "2026-09-01T08:30",
                                 "served": "2026-09-01T08:30"}}}
    (world / "_lane_state.json").write_text(json.dumps(st))
    _stored(world / "10100001.parquet", [("v7", "2026-01-01", 1.0, "Aden", "2.1")])
    lane.run(enumerate_releases=_rel({PID: "2026-09-08T08:30", 10100001: "2026-09-08T08:30"}), put=_Bucket(),
             ends_fn=lambda: ({}, None), max_cubes=0)
    cubes = _state(world)["cubes"]
    assert cubes[str(PID)]["owed_since"] == "2026-09-01T08:30", "owed for its old release when the new one came"
    assert "owed_since" not in cubes["10100001"], "a cube merged at its old release owed nothing older"
    lane.run(enumerate_releases=_rel({PID: "2026-09-15T08:30"}), put=_Bucket(), ends_fn=lambda: ({}, None),
             max_cubes=0, now_fn=lambda: lane._now() + dt.timedelta(hours=2))
    assert _state(world)["cubes"][str(PID)]["owed_since"] == "2026-09-01T08:30", "a later re-release keeps it"


def test_t4_a_merge_pops_owed_since(world, monkeypatch):
    from tests.test_statcan_lane import NEW
    _net(monkeypatch, NEW, (5, 3))
    st = {"version": 1, "floor": lane.FLOOR, "parsed_rows": {}, "new_cubes": {}, "last_enum_utc": None,
          "cubes": {str(PID): {"release": "2026-09-01T08:30", "merged": "2026-08-01T08:30",
                               "served": "2026-08-01T08:30", "quarantined": True, "fails": 3,
                               "fail_release": "2026-09-01T08:30"}}}
    (world / "_lane_state.json").write_text(json.dumps(st))
    lane.run(enumerate_releases=_rel({PID: REL}), put=_Bucket(), ends_fn=lambda: ({}, None))
    c = _state(world)["cubes"][str(PID)]
    assert c["merged"] == REL and "owed_since" not in c and not c.get("quarantined"), c
    assert json.loads((world / "_lane_progress.json").read_text())["owed"]["owed_since_clock"] == []


def test_t4_the_absent_path_pops_owed_since(world, monkeypatch):
    _net(monkeypatch, [], (0, 0))
    st = {"version": 1, "floor": lane.FLOOR, "parsed_rows": {}, "new_cubes": {}, "last_enum_utc": None,
          "cubes": {"99999999": {"release": "2026-09-01T08:30", "owed_since": "2026-08-01T08:30"}}}
    (world / "_lane_state.json").write_text(json.dumps(st))
    lane.run(enumerate_releases=_rel({99999999: "2026-09-01T08:30"}), put=_Bucket(), ends_fn=lambda: ({}, None))
    c = _state(world)["cubes"]["99999999"]
    assert c.get("not_held") and "owed_since" not in c, c


def test_t4_a_planted_owed_since_not_later_than_merged_is_ignored():
    cubes = {"1": {"release": "2026-11-20T08:30", "merged": "2026-11-01T08:30", "owed_since": "2026-10-01T08:30"},
             "2": {"release": "2026-11-20T08:30", "merged": "2026-11-01T08:30", "owed_since": "2026-11-01T08:30"},
             "3": {"release": "2026-11-20T08:30", "merged": None, "owed_since": "2026-10-01T08:30"},
             "4": {"release": "2026-11-20T08:30", "merged": "2026-11-20T08:30", "owed_since": "2026-10-01T08:30"}}
    assert sc.owed_since_clock(cubes) == [{"pid": "3", "owed_since": "2026-10-01T08:30", "kind": sc.OWED_KIND_CARRIED}]


def test_the_lane_and_the_reporter_share_one_merge_owed_rule():
    for c in ({}, {"release": "r1"}, {"release": "r2", "merged": "r1"}, {"release": "r1", "merged": "r1"},
              {"release": None, "merged": None}, {"release": "r1", "merged": "r2"}):
        assert lane.merge_owed(c) is sc.cube_merge_owed(c), c


# --------------------------------------------------------------------------- #
# C5 - a history hold stays on the public clock, and the note names it
# --------------------------------------------------------------------------- #
def test_c5_a_history_hold_past_the_sla_reads_not_current_and_is_named(world, monkeypatch):
    _history_world(world, monkeypatch, STORED_2013)
    _net(monkeypatch, [_row("2014-01", "Aden", "v3", "2.1", "2.5"), _row("2014-02", "Aden", "v3", "2.1", "3")], (2, 1))
    lane.run(enumerate_releases=_rel({PID: REL}), put=_Bucket(), ends_fn=lambda: ({}, None))
    st = _state(world)
    assert st["cubes"][str(PID)]["history_hold"]["release"] == REL
    now = dt.datetime.strptime(REL, "%Y-%m-%dT%H:%M").replace(tzinfo=dt.timezone.utc) + dt.timedelta(days=15)
    status, msg, cur = _note(st, _beat(st, now), now)
    assert status == "partial" and not cur and msg.startswith(BEHIND), msg
    assert f"{PID} (owed since {REL[:10]}, history hold)" in msg, msg
    # inside the SLA the same hold reads as today's amber, still naming nothing late
    now = now - dt.timedelta(days=5)
    assert _note(st, _beat(st, now), now)[2] is True


# --------------------------------------------------------------------------- #
# T6 (R2027) - every earlier verdict branch, with a held cube past the SLA, reads not current
# --------------------------------------------------------------------------- #
def _with(st, **extra):
    b = _beat(st)
    owed = dict(b["owed"])
    owed.update(extra.pop("owed", {}))
    b.update(extra, owed=owed)
    return b


@pytest.mark.parametrize("name, mk", [
    ("dead beat", lambda st: _with(st, beat_utc=_z(NOW - dt.timedelta(hours=10)))),
    ("store unreachable", lambda st: _with(st, state="store_unreachable")),
    ("enumeration failing", lambda st: _with(st, enum_fail={"since": _z(NOW - dt.timedelta(hours=7)), "count": 9})),
    ("breaker", lambda st: _with(st, breaker={"status": 403, "since": _z(NOW - dt.timedelta(hours=7)), "trips": 9})),
    ("failing", lambda st: _with(st, owed={"failing": ["2: store fault"]})),
    ("draining", lambda st: _with(st, last_progress_utc=_z(NOW - dt.timedelta(hours=1)),
                                  owed={"merge": 5, "oldest_owed_release": "2026-10-01T08:30"})),
    ("behind not progressing", lambda st: _with(st, owed={"merge": 5, "oldest_owed_release": "2026-10-01T08:30"})),
    ("amber serve refused", lambda st: _with(st, owed={"serve_refused": ["2: split map"]})),
    ("amber new-cube debt", lambda st: _with(st, owed={"new_cubes": 1, "oldest_new_cube": "2026-09-01T08:30"})),
    ("amber only", lambda st: _with(st)),
])
def test_t6_every_branch_with_a_held_cube_past_the_sla_reads_not_current(name, mk):
    st = {"cubes": {"10100145": _held(_rel_days_ago(24))}}
    assert _note(st, mk(st))[2] is False, name


# --------------------------------------------------------------------------- #
# the clock's two sources: the state (the reporter) and the beat (CI)
# --------------------------------------------------------------------------- #
def test_the_beat_alone_carries_the_clock_when_the_state_holds_no_cubes():
    st = {"cubes": {"10100145": _held(_rel_days_ago(24))}}
    beat = _beat(st)
    status, msg, cur = _note({"cubes": {}}, beat)
    assert status == "partial" and not cur and msg.startswith(BEHIND), msg


def test_the_state_decides_when_the_beat_comes_from_a_lane_on_older_code():
    """THE RESTART ORDER: after the E: pull the reporter runs this code while the lane may still run the old one,
    whose beat has no owed_since_clock. The reporter reads the cubes themselves, so the flip does not wait for a
    lane restart."""
    st = {"cubes": {"10100145": _held(_rel_days_ago(24))}}
    beat = _beat(st)
    for k in ("owed_since_clock", "oldest_owed_since"):
        beat["owed"].pop(k)
    status, msg, cur = _note(st, beat)
    assert not cur and msg.startswith(BEHIND), msg


def test_ci_stays_green_on_a_behind_note():
    """P1 (pin): tools/guard_heartbeat.py --check judges the beat with verdict({"cubes": {}}, beat): BEHIND is
    `partial`, not a raise, so CI's heartbeat stays green (R244) - the clock is met on the PUBLIC side only."""
    gh = _heartbeat()
    now = dt.datetime.now(dt.timezone.utc)
    st = {"cubes": {"10100145": _held(_rel_days_ago(24, now))}}
    beat = _beat(st, now)
    assert _note({"cubes": {}}, beat, now)[1].startswith(BEHIND)
    assert gh._lane_problem(beat, now) is None


# --------------------------------------------------------------------------- #
# C6 - the note's size, its digest kind, its documentation
# --------------------------------------------------------------------------- #
def test_c6_the_note_names_at_most_twenty_cubes_and_counts_the_rest():
    st = {"cubes": {f"1010{i:04d}": _held(_rel_days_ago(20 + i)) for i in range(25)}}
    msg = _note(st, _beat(st))[1]
    assert msg.startswith("statcan: 25 cube(s) are BEHIND") and ", and 5 more" in msg, msg
    behind_segment = msg.split(" | ")[0]
    assert behind_segment.count("(owed since ") == sc.BEHIND_NAMED_MAX
    assert "10100024 (owed since" in behind_segment, "oldest first"


def test_c6_the_note_sorts_as_other_in_the_digest():
    from updater import send_digest
    st = {"cubes": {"10100145": _held(_rel_days_ago(24))}}
    assert send_digest.kind_of(_note(st, _beat(st))[1]) == (4, "OTHER")


def test_c6_the_runbook_and_the_module_docstring_explain_behind():
    """The page is generated (tools/gen_runbook.py copies the fetcher's module docstring into its section 3), so
    the explanation lives in the docstring and survives a regeneration; the page carries it today."""
    page = open(os.path.join(ROOT, "docs", "runbook", "statcan.md"), encoding="utf-8").read()
    for text in (sc.__doc__, page):
        assert 'WHAT "BEHIND" MEANS' in text and "cube(s) are BEHIND" in text and "CI stays GREEN" in text
        assert "history hold" in text and "Do NOT" in text
