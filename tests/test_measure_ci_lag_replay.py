"""tools/measure_ci_lag.py `replay61` - the starvation plan's 6.1 replay (W windows, D3 back-off, the lease).

Every case below is PLANTED: a few hand-made runs on one day, where the right answer can be worked out with a pencil
and is written next to the assertion. Each group has a negative control - the same input with the one fact the
mechanism depends on removed - so a model that ignores that fact fails the test rather than passing it by luck.
"""
import datetime as dt
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools"))

import ci_writer_gate as g  # noqa: E402
import measure_ci_lag as m  # noqa: E402

DAILY, HEAVY = "updater-daily.yml", "updater-heavy.yml"
_ids = iter(range(1, 10 ** 6))


def T(s):
    return dt.datetime.fromisoformat(s).replace(tzinfo=dt.timezone.utc)


def run(wf, created, exec_min, event="schedule", read=0.5, ok=True, pushes=1):
    c = T(created)
    return m.CloudRun(wf, next(_ids), event, c, "completed", "success", float(exec_min), read, float(exec_min), ok,
                      pushes, c, c + dt.timedelta(minutes=exec_min), True)


def day_history(drop_daily_morning=False):
    """2026-10-01, every cron with its run (created, minutes):
    the day before: heavy 15:05 (10), daily 18:05 (60);
    heavy 03:00 -> 03:30 (60); daily 06:00 -> 08:00 (120); heavy 15:00 -> 15:10 (30); daily 18:00 -> 18:00 (60)."""
    rs = [run(HEAVY, "2026-09-30T15:05", 10), run(DAILY, "2026-09-30T18:05", 60),
          run(HEAVY, "2026-10-01T03:30", 60), run(HEAVY, "2026-10-01T15:10", 30), run(DAILY, "2026-10-01T18:00", 60)]
    if not drop_daily_morning:
        rs.append(run(DAILY, "2026-10-01T08:00", 120))
    return sorted(rs, key=lambda r: r.created)


# ------------------------------------------------------------------ free windows, two instruments

def test_free_windows_hand_computed_and_production_assess_agrees():
    since, until = T("2026-10-01T00:00"), T("2026-10-02T00:00")
    rs = day_history()
    # busy: 03:00-04:30 (cron then run), 06:00-10:00, 15:00-15:40, 18:00-19:00
    want = [(T("2026-10-01T00:00"), T("2026-10-01T03:00")), (T("2026-10-01T04:30"), T("2026-10-01T06:00")),
            (T("2026-10-01T10:00"), T("2026-10-01T15:00")), (T("2026-10-01T15:40"), T("2026-10-01T18:00")),
            (T("2026-10-01T19:00"), T("2026-10-02T00:00"))]
    assert m.free_windows(rs, since, until) == want
    assert m.gate_minutes_production(rs, since, until) == want
    s = m.window_stats(want, since, until)
    # 180 + 90 + 300 + 140 + 300; usable = minus 25: >=150 -> 180, 300, 300; >=90 -> those and 140
    assert (s["free_min"], s["windows"], s["ge150"], s["ge90"], s["longest"]) == (1010, 5, 3, 4, 300)


def test_free_windows_negative_control_a_cron_without_its_run_blocks_twelve_hours():
    since, until = T("2026-10-01T00:00"), T("2026-10-02T00:00")
    rs = day_history(drop_daily_morning=True)
    free = m.free_windows(rs, since, until)
    assert free == m.gate_minutes_production(rs, since, until)
    # the 06:00 cron now blocks until 18:00: 180 + 90 + 300
    assert sum(m._mins(a, b) for a, b in free) == 570
    assert sum(m._mins(a, b) for a, b in m.free_windows(rs, since, until, pending=False)) > 570


def test_until_block_fast_equals_production_on_a_planted_day():
    rs = day_history()
    per = m._as_of_lists(rs)
    for t, want in (("2026-10-01T00:00", 180), ("2026-10-01T04:30", 90), ("2026-10-01T12:34", 150),
                    ("2026-10-01T19:00", 480), ("2026-10-01T16:01", 120)):
        unf, sch = m._runs_as_of(per, T(t))
        assert not g.assess(unf, T(t), None, sch)[0], t
        assert g.minutes_until_block(unf, T(t), None, sched=sch) == want == m.until_block_fast(T(t)), t


# ------------------------------------------------------------------ the unit book (production order + admission)

def book(work, outcome=None, cadence=None, state=None, rows=None):
    cad = {s: (cadence or {}).get(s, "daily") for s in work}
    return m.UnitBook(cad, state or {}, rows or [], work, outcome or {}, (), ())


def test_admission_reserve_and_cost_band_order():
    # est from the runs rows: cheap 5 min (band 1: 300 s), big 100 min (band 3); reserve big = min(90, 300) = 90
    rows = [("2026-09-01T00:00:00+00:00", "cheap", "_all", "ok", 300.0),
            ("2026-09-01T00:00:00+00:00", "big", "_all", "ok", 6000.0)]
    b = book({"cheap": 5.0, "big": 100.0}, rows=rows)
    r = b.run_pass(T("2026-10-01T00:00"), 60.0)
    assert [x[0] for x in r["ran"]] == ["cheap"]                 # big: 5 + 90 > 60 -> BUDGET SKIP
    b = book({"cheap": 5.0, "big": 100.0}, rows=rows)
    r = b.run_pass(T("2026-10-01T00:00"), 200.0)
    assert [(x[0], x[1]) for x in r["ran"]] == [("cheap", 5.0), ("big", 100.0)] and r["used_min"] == 105.0


def test_hard_stop_kills_and_loses_progress_but_w_stops_and_keeps_it():
    rows = [("2026-09-01T00:00:00+00:00", "unctad_x", "_all", "ok", 6000.0)]
    # W off: admitted (90 <= 200), runs to the hard stop at 200 + 10, killed, work unchanged
    b = book({"unctad_x": 300.0}, rows=rows)
    r = b.run_pass(T("2026-10-01T00:00"), 200.0)
    assert r["hard_stop"] == "unctad_x" and r["used_min"] == 210.0 and b.remaining["unctad_x"] == 300.0
    assert b.db.execute("SELECT status, dur_s FROM runs ORDER BY id DESC LIMIT 1").fetchone() == ("killed_external",
                                                                                                  210 * 60.0)
    # W on, clocked (unctad_*): stopped at 200, keeps 200 - tail 90 = 110 of progress, no hard stop
    b = book({"unctad_x": 300.0}, rows=rows)
    r = b.run_pass(T("2026-10-01T00:00"), 200.0, w_on=True, tail_min=90.0)
    assert r["hard_stop"] is None and r["stopped"] == "unctad_x" and r["used_min"] == 200.0
    assert b.remaining["unctad_x"] == 190.0 and b.is_due("unctad_x", T("2026-10-01T04:00"))
    # negative control: W on but the unit is NOT on the clocked list (oecd) -> killed exactly as with W off
    rows = [("2026-09-01T00:00:00+00:00", "oecd", "_all", "ok", 6000.0)]
    b = book({"oecd": 300.0}, rows=rows)
    r = b.run_pass(T("2026-10-01T00:00"), 200.0, w_on=True)
    assert r["hard_stop"] == "oecd" and b.remaining["oecd"] == 300.0


def test_is_due_is_production_and_a_partial_behind_an_old_success_stays_due():
    st = {"p": {"last_success_utc": "2026-07-14T00:00:00+00:00"}, "q": {}}
    b = book({"p": 10.0, "q": 10.0}, outcome={"p": "partial", "q": "ok"}, cadence={"p": "monthly", "q": "monthly"},
             state=st)
    b.run_pass(T("2026-10-01T00:00"), 100.0)
    assert b.is_due("p", T("2026-10-01T01:00"))         # partial: last_success not moved -> still due (istat's shape)
    assert not b.is_due("q", T("2026-10-01T01:00"))     # ok: due again only after 0.9 x 28 days
    assert b.owed(T("2026-10-01T01:00")) == 10.0


def test_unit_book_from_a_copy_reads_outcome_from_desktop_rows_only(tmp_path):
    import sqlite3
    db = tmp_path / "state.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE unit_state(source_id TEXT, unit_id TEXT, strategy TEXT, upstream_vintage TEXT, "
                "last_success_utc TEXT, last_attempt_utc TEXT, status TEXT, last_obs_date TEXT, obs_count INTEGER, "
                "attempt_count INTEGER, last_error TEXT)")
    con.execute("CREATE TABLE runs(id INTEGER PRIMARY KEY AUTOINCREMENT, ts_utc TEXT, source_id TEXT, unit_id TEXT, "
                "status TEXT, obs INTEGER, dur_s REAL, note TEXT)")
    con.execute("INSERT INTO unit_state(source_id, unit_id, last_success_utc, status) VALUES "
                "('a', '_all', '2026-09-01T00:00:00+00:00', 'partial')")
    con.executemany("INSERT INTO runs(ts_utc, source_id, unit_id, status, dur_s) VALUES (?, 'a', '_all', ?, ?)",
                    [("2026-10-01T10:00:00+00:00", "ok", 3000.0),          # inside a desktop pass
                     ("2026-10-02T12:00:00+00:00", "partial", 2700.0),     # a cloud row (45.0 min, the CI cap)
                     ("2026-10-03T12:00:00+00:00", "killed_external", 6000.0),
                     ("2026-10-30T00:00:00+00:00", "ok", 1.0)])           # after the copy: ignored
    con.commit()
    con.close()
    copy = T("2026-10-28T00:00")
    win = [(T("2026-10-01T09:00"), T("2026-10-01T11:00"))]
    b = m.load_unit_book(str(db), copy, dt.timedelta(days=-10), cadences={"a": "monthly"}, desktop_windows=win)
    assert b.outcome["a"] == "ok" and b.work["a"] == 100.0 and b.censored == {"a"}
    assert b.state["a"]["last_success_utc"] == "2026-08-22T00:00:00+00:00"          # moved by the shift
    # negative control: without the pass windows the cloud row decides
    b = m.load_unit_book(str(db), copy, dt.timedelta(0), cadences={"a": "monthly"})
    assert b.outcome["a"] == "partial"


# ------------------------------------------------------------------ the concurrency group

def make_replay(cfg, runs, units=None, start="2026-10-01T19:00", end="2026-10-02T00:00", last_success=None):
    b = book(units or {})
    tmpl = {DAILY: run(DAILY, "2026-09-30T00:00", 60), HEAVY: run(HEAVY, "2026-09-30T00:00", 30)}
    return m.Replay(cfg, runs, b, T(start), T(end), {DAILY: 8.0, HEAVY: 2.0}, tmpl,
                    last_success or T("2026-09-01T00:00"), None)


def test_group_one_running_one_pending_newer_arrival_evicts():
    a = run(DAILY, "2026-10-01T20:00", 60)
    bb = run(HEAVY, "2026-10-01T20:10", 30)
    c = run(DAILY, "2026-10-01T20:20", 30, event="workflow_dispatch")
    rp = make_replay(m.ReplayConfig(min_hours=10 ** 9), [a, bb, c]).run()
    starts = {r.run_id: (s, e) for _w, s, e, _d, r in rp.done}
    assert starts[a.run_id] == (T("2026-10-01T20:00"), T("2026-10-01T21:00"))
    assert starts[c.run_id] == (T("2026-10-01T21:00"), T("2026-10-01T21:30"))   # waited for a
    assert [(r.run_id, t) for r, _arr, t in rp.evicted] == [(bb.run_id, T("2026-10-01T20:20"))]
    assert bb.run_id not in starts


# ------------------------------------------------------------------ the lease, fairness and re-dispatch

def lease_history():
    """Warm-up: both writers completed before the span. In the span: one daily dispatch at 19:30 (60 min)."""
    return [run(DAILY, "2026-09-30T18:05", 60), run(HEAVY, "2026-10-01T15:05", 10), run(DAILY, "2026-10-01T18:00", 30),
            run(DAILY, "2026-10-01T19:30", 60, event="workflow_dispatch")]


def lease_cfg(**kw):
    base = dict(policy="lease", lease_min=180.0, min_hours=1.0, pull_min=2.0, push_min=7.0)
    base.update(kw)
    return m.ReplayConfig(**base)


def test_lease_defers_a_run_and_redispatch_restores_both_writers():
    rp = make_replay(lease_cfg(dispatch=True), lease_history(), units={"u": 100.0}).run()
    # 19:00 lease (first-lease rule: both writers have a completed run); W 2 + pull 2 -> updater 19:04, unit 100 min,
    # push 7 -> release 20:51. The 19:30 dispatch reads the state inside it -> DEFERRED, holds the group 8 min.
    lz = rp.leases[0]
    assert (lz["taken"], lz["release"], lz["stood_down"]) == (T("2026-10-01T19:00"), T("2026-10-01T20:51"), False)
    deferred = [(w, s) for w, s, _e, d, _r in rp.done if d]
    assert deferred == [(DAILY, T("2026-10-01T19:30"))]
    disp = [(w, s, e) for w, s, e, d, r in rp.done if r.dispatched]
    # release 20:51 -> daily (the deferred one first) created 20:52, its own 60 min; heavy (not deferred) dispatched
    # at the 20:55 tick (nothing pending), created 20:56, pending behind daily, runs 21:52-22:02 with the durations
    # of heavy's newest real run that had ended by then (15:05, 10 min)
    assert disp[:2] == [(DAILY, T("2026-10-01T20:52"), T("2026-10-01T21:52")),
                        (HEAVY, T("2026-10-01T21:52"), T("2026-10-01T22:02"))]
    # both writers completed after the release -> the next lease at the 22:05 tick. `u` is not due any more, so that
    # lease advances nothing, stamps no success, and the next tick past its release leases again: an EMPTY lease
    # still makes both writers owed and costs two dispatched runs (the churn the sweep's `emp` column counts)
    assert rp.leases[1]["taken"] == T("2026-10-01T22:05")
    assert rp.leases[1]["release"] == T("2026-10-01T22:16")
    assert not rp.passes[1]["advanced"] and len(disp) > 2
    assert m.replay_metrics(rp, 100.0, ["u"])["collisions"] == 0


def test_lease_without_redispatch_waits_for_both_writers():
    rp = make_replay(lease_cfg(dispatch=False), lease_history(), units={"u": 100.0}).run()
    assert len(rp.leases) == 1                         # no completed run of either writer after 20:51 in the span
    assert not any(r.dispatched for *_x, r in rp.done)
    assert rp.passes[0]["mode"] == "lease" and all(p["mode"] == "gate" for p in rp.passes[1:])


def test_negative_control_no_lease_means_no_deferral_and_a_collision():
    rp = make_replay(m.ReplayConfig(policy="today", min_hours=1.0, pull_min=2.0, push_min=7.0), lease_history(),
                     units={"u": 100.0}).run()
    assert not any(d for _w, _s, _e, d, _r in rp.done)
    # gate pass at 19:00 (until the 03:00 cron: 480 - 25 = 455 min budget); the 19:30 dispatch overlaps it
    assert rp.passes[0]["mode"] == "gate" and rp.passes[0]["budget"] == 455.0
    assert m.replay_metrics(rp, 100.0, ["u"])["collisions"] == 1


def test_fairness_refuses_until_each_writer_completed_after_the_reference():
    rp = make_replay(lease_cfg(), lease_history())
    for r in lease_history()[:3]:
        rp.done.append((r.wf, r.hist_start, r.hist_end, False, r))
    rp.ref = None
    assert rp._fair()                                    # first-lease rule: both have completed rows
    rp.ref = T("2026-10-01T16:00")
    assert not rp._fair()                                # heavy's newest completion 15:15 is older
    h = run(HEAVY, "2026-10-01T16:30", 10)
    rp.done.append((HEAVY, h.hist_start, h.hist_end, False, h))
    assert rp._fair()
    hd = run(HEAVY, "2026-10-01T17:00", 10)
    rp.ref = T("2026-10-01T16:50")
    rp.done.append((HEAVY, hd.hist_start, hd.hist_end, True, hd))   # a DEFERRED run is not a completion
    assert not rp._fair()


def test_stand_down_when_a_run_is_created_inside_w():
    hist = lease_history()[:3] + [run(DAILY, "2026-10-01T19:01", 60, event="workflow_dispatch")]
    rp = make_replay(lease_cfg(dispatch=False), hist, units={"u": 100.0}).run()
    assert rp.leases[0]["stood_down"] and rp.leases[0]["release"] == T("2026-10-01T19:02")
    assert [d for w, s, _e, d, _r in rp.done if s == T("2026-10-01T19:01")] == [True]


# ------------------------------------------------------------------ D3

def test_d3_holds_a_short_window_while_the_no_progress_stamp_is_young():
    # 14:00: next cron 15:00 -> until-block 60, usable 35 (< 150 wanted); 40 h since the last success (> 36)
    t = T("2026-10-01T14:00")
    hist = day_history()

    def attempt(backoff, noprog_h_ago):
        rp = make_replay(m.ReplayConfig(policy="today", backoff_h=backoff), hist, start="2026-10-01T13:00")
        rp.last_noprog = None if noprog_h_ago is None else t - dt.timedelta(hours=noprog_h_ago)
        return rp._gate_attempt(t, 40.0)

    assert attempt(None, 5) == "pass"        # today: past MaxHours a short window is taken
    assert attempt(12, 5) == "hold"          # D3: the empty pass 5 h ago is younger than 12 h
    assert attempt(12, 13) == "pass"         # older than the back-off -> taken
    assert attempt(4, 5) == "pass"


# ------------------------------------------------------------------ the logs

def test_wrapper_log_overheads_cross_midnight(tmp_path):
    p = tmp_path / "local_heavy_20261025-220113.log"
    p.write_text("﻿[22:01:13] local heavy updater starting\n[22:01:42] per-source budget override: 360 min; "
                 "whole-run budget: 275 min\n[22:01:42] pull-state ...\n[22:04:30] running updater for 30 source(s)"
                 "\n[01:37:33] updater exit code: 0\n[01:37:33] push-state ...\n[01:45:02] cadence stamped - state "
                 "committed (3 unit(s) advanced)\n", encoding="utf-8")
    (tmp_path / "local_heavy_20261025-215553.log").write_text("[21:55:53] CI writer gate: BLOCKED\n", encoding="utf-8")
    ps = m.desktop_passes(str(tmp_path))
    assert len(ps) == 1 and ps[0]["budget"] == 275 and ps[0]["stamped"]
    assert ps[0]["push_done"] == T("2026-10-26T01:45:02")
    oh = m.pass_overheads(ps)
    assert round(oh["pull_median"], 2) == 2.8 and round(oh["push_median"], 2) == 7.48


def test_tail_lower_bound_counts_both_csv_phases_of_one_unit_run(tmp_path):
    (tmp_path / "local_heavy_updater_20261025-220113.log").write_text(
        "[orchestrator] >>> istat/_all (strategy=sdmx_delta)\n  derive budget of 45 min spent - 1 id(s)\n"
        "  derive budget of 45 min spent - 2 id(s)\n[orchestrator] <<< istat/_all took 5,594s\n"
        "[orchestrator] >>> bea/_all (x)\n  derive budget of 45 min spent\n[orchestrator] <<< bea/_all took 1,892s\n"
        "[orchestrator] >>> bls/_all (x)\n[orchestrator] <<< bls/_all took 1,563s\n", encoding="utf-8")
    tp = m.measure_tail_probe(str(tmp_path))
    assert tp["unit_runs"] == 3 and tp["tail_lower_bound_min"] == 90.0
    assert [(x[2], x[0]) for x in tp["csv_budget_hits"]] == [("istat", 90.0), ("bea", 45.0)]
    assert tp["timed_phase_lines"] == 0


# ------------------------------------------------------------------ the verdict

def test_verdict_needs_both_drain_and_two_runs_a_day():
    rp = make_replay(lease_cfg(dispatch=False), lease_history(), units={"u": 100.0}).run()
    met = m.replay_metrics(rp, 100.0, ["u"])
    assert met["drains"]                                   # u finished once and is not due again (daily, ok)
    # no dispatch, the only run in the span deferred: 0 completed runs a day for both writers
    assert met["updater-daily.yml"]["runs_day"] == 0.0 and met["updater-daily.yml"]["deferred"] == 1
    assert not met["cloud_ge2"] and not met["A_ok"]
    # negative control for "drains": a unit due at the start that never finished a turn
    assert m.replay_metrics(rp, 100.0, ["u", "never_ran"])["never_finished"] == ["never_ran"]
    assert not m.replay_metrics(rp, 100.0, ["u", "never_ran"])["drains"]
    # and the cloud half alone: pretend both writers completed 3 runs a day
    met["updater-daily.yml"]["runs_day"] = met["updater-heavy.yml"]["runs_day"] = 3.0
    assert all(met[wf]["runs_day"] >= 2.0 for wf in g.WRITERS)


@pytest.mark.parametrize("since_success_h, due", [(0.5, False), (23.0, True)])
def test_book_owed_is_the_sum_of_the_due_units_work(since_success_h, due):
    now = T("2026-10-01T00:00")
    b = book({"a": 30.0, "b": 12.5},
             state={"b": {"last_success_utc": m._iso(now - dt.timedelta(hours=since_success_h))}})
    assert b.owed(now) == (42.5 if due else 30.0)          # daily cadence: due after 0.9 x 24 h = 21.6 h
