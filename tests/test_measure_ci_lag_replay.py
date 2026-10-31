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
                    [("2026-09-01T10:00:00+00:00", "partial", 9000.0),      # 150 min: a longer turn on record
                     ("2026-09-02T10:00:00+00:00", "transient_fail", 7200.0),  # 120 min
                     ("2026-10-01T10:00:00+00:00", "ok", 3000.0),          # inside a desktop pass
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
    # AR-394 F1: a censored term takes the largest lower bound on record, then the multiplier
    kw = dict(cadences={"a": "monthly"}, desktop_windows=win)
    assert m.load_unit_book(str(db), copy, dt.timedelta(0), censored_bound="kills", **kw).work["a"] == 120.0
    assert m.load_unit_book(str(db), copy, dt.timedelta(0), censored_bound="tightest", **kw).work["a"] == 150.0
    assert m.load_unit_book(str(db), copy, dt.timedelta(0), censored_bound="tightest", censored_mult=1.5,
                            **kw).work["a"] == 225.0
    with pytest.raises(ValueError):
        m.load_unit_book(str(db), copy, dt.timedelta(0), censored_bound="loosest", **kw)


def test_uncensored_terms_ignore_the_bound_and_the_multiplier(tmp_path):
    import sqlite3
    db = tmp_path / "state.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE unit_state(source_id TEXT, unit_id TEXT, strategy TEXT, upstream_vintage TEXT, "
                "last_success_utc TEXT, last_attempt_utc TEXT, status TEXT, last_obs_date TEXT, obs_count INTEGER, "
                "attempt_count INTEGER, last_error TEXT)")
    con.execute("CREATE TABLE runs(id INTEGER PRIMARY KEY AUTOINCREMENT, ts_utc TEXT, source_id TEXT, unit_id TEXT, "
                "status TEXT, obs INTEGER, dur_s REAL, note TEXT)")
    con.executemany("INSERT INTO runs(ts_utc, source_id, unit_id, status, dur_s) VALUES (?, 'b', '_all', ?, ?)",
                    [("2026-10-01T10:00:00+00:00", "killed_external", 9000.0),
                     ("2026-10-02T10:00:00+00:00", "ok", 600.0)])          # newest row finished: not censored
    con.commit()
    con.close()
    b = m.load_unit_book(str(db), T("2026-10-28T00:00"), dt.timedelta(0), cadences={"b": "daily"},
                         censored_bound="tightest", censored_mult=2.0)
    assert b.work["b"] == 10.0 and not b.censored


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


def test_a_leftover_csv_retry_drain_is_not_a_unit_run_nor_a_tail(tmp_path):
    """orchestrate._drain_leftover_retries prints a `(csv retry drain)` block after the unit loop. Its spent budget
    must not be added to the no_change unit whose `>>>` it follows (which never prints `<<<`), nor counted as a run."""
    (tmp_path / "local_heavy_updater_20261025-220113.log").write_text(
        "[orchestrator] >>> bea/_all (x)\n  derive budget of 45 min spent\n[orchestrator] <<< bea/_all took 1,892s\n"
        "[orchestrator] >>> wid/_all (strategy=overwrite_if_changed, cadence=irregular)\n"
        "[orchestrator] >>> oecd/_all (strategy=giant_changed_units, cadence=monthly)\n"
        "[oecd] 25/75 flows - +4,354 rows, 0 quiet, 23 transient, 0 structural, 295s\n"
        "[orchestrator] <<< oecd/_all took 400s\n"
        "[orchestrator] >>> wid/_all (csv retry drain)\n  derive budget of 155 min spent - 9 id(s)\n"
        "[orchestrator] <<< wid/_all took 9,300s (csv retry drain: attempted 9, cleared 0, still queued 9)\n"
        "[orchestrator] >>> oecd/_all (csv retry drain)\n"
        "[oecd] 50/75 flows - +4,354 rows, 0 quiet, 23 transient, 0 structural, 9,295s\n"
        "[orchestrator] <<< oecd/_all took 60s (csv retry drain: attempted 1, cleared 1, still queued 0)\n",
        encoding="utf-8")
    tp = m.measure_tail_probe(str(tmp_path))
    assert tp["unit_runs"] == 3 and tp["tail_lower_bound_min"] == 45.0, tp
    assert [(x[2], x[0]) for x in tp["csv_budget_hits"]] == [("bea", 45.0)]
    fb = m.giant_flow_blocks(str(tmp_path), "oecd")
    assert fb["unit_runs"] == 1 and [(round(b[0] * 60), b[3], b[4]) for b in fb["blocks"]] == [(295, 0, 25)], fb


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
    # the positive case through the code (review AR-394 F3): one day with 3 completed runs per writer and a unit that
    # finishes -> drains, cloud >= 2/day, A = YES; then one heavy run fewer than 2/day -> A = NO
    day = [run(DAILY, "2026-10-02T01:00", 30), run(DAILY, "2026-10-02T08:00", 30), run(DAILY, "2026-10-02T16:00", 30),
           run(HEAVY, "2026-10-02T04:00", 30), run(HEAVY, "2026-10-02T12:00", 30), run(HEAVY, "2026-10-02T20:00", 30)]
    warm = [run(DAILY, "2026-10-01T10:00", 30), run(HEAVY, "2026-10-01T11:00", 30)]
    rp = make_replay(m.ReplayConfig(policy="today", min_hours=10 ** 9), warm + day, units={"u": 10.0},
                     start="2026-10-02T00:00", end="2026-10-03T00:00").run()
    rp.book.finished["u"] = 1
    rp.book.remaining["u"] = 0.0
    met = m.replay_metrics(rp, 10.0, ["u"])
    assert (met["updater-daily.yml"]["runs_day"], met["updater-heavy.yml"]["runs_day"]) == (3.0, 3.0)
    assert met["drains"] and met["cloud_ge2"] and met["A_ok"]
    # longest gap: heavy's warm-up run ended 10-01 11:30, its next one 10-02 04:30 -> 17 h (under A5's 30 h)
    assert met["longest_gap_h"] == 17.0 and not met["gap_over_30h"]
    rp = make_replay(m.ReplayConfig(policy="today", min_hours=10 ** 9), warm + day[:4], units={"u": 10.0},
                     start="2026-10-02T00:00", end="2026-10-03T00:00").run()
    rp.book.finished["u"] = 1
    met = m.replay_metrics(rp, 10.0, ["u"])
    assert met["updater-heavy.yml"]["runs_day"] == 1.0 and not met["cloud_ge2"] and not met["A_ok"]


# ------------------------------------------------------------------ AR-394 F3: the three surviving mutants

def _jobs_run(wf, conclusion, push):
    r = {"databaseId": 7, "createdAt": "2026-10-01T10:00:00Z", "updatedAt": "2026-10-01T12:00:00Z",
         "status": "completed", "conclusion": conclusion, "event": "schedule", "startedAt": "2026-10-01T10:00:00Z"}
    steps = [{"name": "Pull state from R2", "startedAt": "2026-10-01T10:01:00Z", "completedAt": "2026-10-01T10:03:00Z",
              "conclusion": "success"},
             {"name": "Push state to R2 (compare-and-swap)", "startedAt": "2026-10-01T11:50:00Z",
              "completedAt": "2026-10-01T11:55:00Z", "conclusion": push}]
    jobs = [{"name": "setup" if wf == HEAVY else "update", "startedAt": "2026-10-01T10:00:30Z",
             "completedAt": "2026-10-01T12:00:00Z", "steps": steps}]
    return m._cloud_run(wf, r, jobs)


def test_completed_rule_daily_is_its_push_heavy_is_its_conclusion():
    # daily: the health gate makes every run `failure`, so a successful push is what completes it (A5)
    assert _jobs_run(DAILY, "failure", "success").completed_ok
    assert not _jobs_run(DAILY, "failure", "failure").completed_ok
    # heavy: `success` only when every leg's pull, updater and push (or no-op) succeeded; a pushed but failed run
    # is NOT completed (kills the mutant "heavy completed = any finished run")
    assert _jobs_run(HEAVY, "success", "success").completed_ok
    assert not _jobs_run(HEAVY, "failure", "success").completed_ok
    assert not _jobs_run(HEAVY, "cancelled", "success").completed_ok


def test_no_dispatch_while_a_run_of_the_group_is_pending():
    rp = make_replay(lease_cfg(dispatch=True), lease_history())
    t = T("2026-10-01T21:00")
    rp.ref, rp.dispatch_q = T("2026-10-01T20:00"), [HEAVY]
    busy, waiting = run(DAILY, "2026-10-01T20:30", 60), run(DAILY, "2026-10-01T20:40", 60)
    rp.running = (busy, busy.created, busy.created + dt.timedelta(minutes=60), False)
    rp.pending = (waiting, waiting.created)
    assert not rp._try_dispatch(t) and rp.dispatched == [] and rp.dispatch_q == [HEAVY]   # A3.6
    rp.pending = None
    assert rp._try_dispatch(t) and [r.wf for r in rp.dispatched] == [HEAVY]               # positive control


def test_a_run_reads_the_lease_when_its_job_starts_not_when_it_is_created():
    rp = make_replay(lease_cfg(), lease_history())
    rp.lease = (T("2026-10-01T19:00"), T("2026-10-01T20:00"))
    queued_in = run(DAILY, "2026-10-01T18:00", 30)        # created before the lease, job starts inside it
    rp._start(queued_in, T("2026-10-01T19:10"))
    assert rp.running[3] is True
    rp.running = None
    queued_out = run(HEAVY, "2026-10-01T19:50", 30)       # created inside, job starts after the release
    rp._start(queued_out, T("2026-10-01T20:10"))
    assert rp.running[3] is False


# ------------------------------------------------------------------ AR-394 F2 / F4

def test_lease_budget_is_l_minus_the_ps1_margin():
    rp = make_replay(lease_cfg(lease_margin_min=25.0), lease_history(), units={"u": 300.0}).run()
    assert rp.passes[0]["mode"] == "lease" and rp.passes[0]["budget"] == 155.0
    rp = make_replay(lease_cfg(), lease_history(), units={"u": 300.0}).run()
    assert rp.passes[0]["budget"] == 180.0


def test_manual_dispatches_can_be_excluded_from_completed_runs():
    manual = run(DAILY, "2026-10-01T20:00", 30, event="workflow_dispatch")
    sched = run(HEAVY, "2026-10-01T21:00", 30)
    hist = [run(DAILY, "2026-09-30T18:05", 60), run(HEAVY, "2026-10-01T15:05", 10), manual, sched]
    for man, want in ((True, 1.0 / (4 / 24)), (False, 0.0)):
        rp = make_replay(m.ReplayConfig(policy="today", min_hours=10 ** 9, count_manual=man), hist,
                         start="2026-10-01T20:00", end="2026-10-02T00:00").run()
        met = m.replay_metrics(rp, 0.0, [])
        assert met["updater-daily.yml"]["runs_day"] == pytest.approx(want)
        assert met["updater-heavy.yml"]["runs_day"] == pytest.approx(6.0)   # the scheduled run always counts
        assert rp._completed_after(DAILY, T("2026-10-01T19:00")) is man


@pytest.mark.parametrize("since_success_h, due", [(0.5, False), (23.0, True)])
def test_book_owed_is_the_sum_of_the_due_units_work(since_success_h, due):
    now = T("2026-10-01T00:00")
    b = book({"a": 30.0, "b": 12.5},
             state={"b": {"last_success_utc": m._iso(now - dt.timedelta(hours=since_success_h))}})
    assert b.owed(now) == (42.5 if due else 30.0)          # daily cadence: due after 0.9 x 24 h = 21.6 h


# ================================================================== PR 2 (PLAN rev 10 R10.3 item 2): the extensions
# Each flag: a planted case with the answer worked by hand, and a control where the one fact the flag reads is
# removed, so a flag that does nothing (or does something else) fails.

BIG = 6000.0                                                # 100 min on record: band 3, admission reserve 90 min


def b2_book(order, p_attempt="2026-09-29T00:00:00+00:00", kill_r=None, **kw):
    """p: partial, success 07-14, attempt `p_attempt` (istat's shape); q: ok, success 09-02; r: ok, success 09-03,
    and a kill row if `kill_r`. All monthly, all due at 10-01 (28 x 0.9 = 25.2 d), all in band 3."""
    st = {"p": {"last_success_utc": "2026-07-14T00:00:00+00:00", "last_attempt_utc": p_attempt, "status": "partial"},
          "q": {"last_success_utc": "2026-09-02T00:00:00+00:00", "last_attempt_utc": "2026-09-02T00:00:00+00:00",
                "status": "ok"},
          "r": {"last_success_utc": "2026-09-03T00:00:00+00:00", "last_attempt_utc": "2026-09-03T00:00:00+00:00",
                "status": "ok"}}
    rows = [("2026-07-01T00:00:00+00:00", s, "_all", "ok", BIG) for s in ("p", "q", "r")]
    if kill_r:
        rows.append((kill_r, "r", "_all", "killed_external", BIG))
    return m.UnitBook({s: "monthly" for s in st}, st, rows, {"p": 100.0, "q": 50.0, "r": 20.0},
                      {"p": "partial", "q": "ok", "r": "ok"}, order=order, **kw)


def _grid(b):
    return [b.is_due(s, T("2026-10-01T00:00") + dt.timedelta(days=k)) for s in sorted(b.units) for k in range(0, 60, 3)]


def test_b2_order_attempt_changes_only_the_order():
    t0 = T("2026-10-01T00:00")
    prod, att = b2_book("production"), b2_book("attempt")
    assert _grid(prod) == _grid(att)                       # due-ness is the same before the pass ...
    rp, ra = prod.run_pass(t0, 1000.0), att.run_pass(t0, 1000.0)
    # production: p by its 07-14 success (-79/30), q 09-02 (-29/30), r 09-03; B2: p by its 09-29 attempt (-2/30)
    assert [x[0] for x in rp["ran"]] == ["p", "q", "r"]
    assert [x[0] for x in ra["ran"]] == ["q", "r", "p"]
    # ... and everything except the order is identical: what ran, for how long, with which status, the minutes,
    # the counters, every unit's status, and due-ness on a 60-day grid after the pass
    assert sorted(rp["ran"]) == sorted(ra["ran"]) and rp["used_min"] == ra["used_min"] == 170.0
    assert (rp["advanced"], rp["hard_stop"], rp["stopped"]) == (ra["advanced"], ra["hard_stop"], ra["stopped"])
    assert prod.attempts == att.attempts and prod.finished == att.finished and prod.remaining == att.remaining
    assert {s: prod.state[s]["status"] for s in prod.units} == {s: att.state[s]["status"] for s in att.units}
    assert _grid(prod) == _grid(att) and prod.owed(T("2026-10-05T00:00")) == att.owed(T("2026-10-05T00:00"))


def test_b2_negative_control_no_newer_attempt_means_the_production_order():
    # p's attempt equals its success: nothing for B2 to read, so the two orders must agree unit for unit
    prod = b2_book("production", p_attempt="2026-07-14T00:00:00+00:00")
    att = b2_book("attempt", p_attempt="2026-07-14T00:00:00+00:00")
    # the stamp itself, unit by unit, equals production last_turn_utc (read before any pass moves a stamp)
    for s in ("p", "q", "r"):
        assert att._last_turn(att.state[s], None) == prod._last_turn(prod.state[s], None) == \
            prod._orch.last_turn_utc(prod.state[s], None)
    assert prod.run_pass(T("2026-10-01T00:00"), 1000.0)["ran"] == att.run_pass(T("2026-10-01T00:00"), 1000.0)["ran"]


def test_b2_keeps_the_production_kill_rule():
    # r was killed on 09-30, after its 09-03 success: both orders sort r by the kill (production last_turn_utc), so
    # r goes LAST in production too; B2 must not drop the kill
    prod = b2_book("production", kill_r="2026-09-30T00:00:00+00:00")
    att = b2_book("attempt", kill_r="2026-09-30T00:00:00+00:00")
    assert [x[0] for x in prod.run_pass(T("2026-10-01T00:00"), 1000.0)["ran"]] == ["p", "q", "r"]
    assert [x[0] for x in att.run_pass(T("2026-10-01T00:00"), 1000.0)["ran"]] == ["q", "p", "r"]   # kill 09-30 > p 09-29
    with pytest.raises(ValueError):
        b2_book("newest")


def test_b2_changes_admission_only_through_the_order():
    # 200 min, a 90-min reserve each: production runs p (0 -> 100), q (100 + 90 <= 200 -> 150) and refuses r
    # (150 + 90 > 200); B2 runs q (-> 50), r (50 + 90 <= 200 -> 70), p (70 + 90 <= 200 -> 170). Same units, same
    # rule; the order decides who is refused.
    assert [x[0] for x in b2_book("production").run_pass(T("2026-10-01T00:00"), 200.0)["ran"]] == ["p", "q"]
    assert [x[0] for x in b2_book("attempt").run_pass(T("2026-10-01T00:00"), 200.0)["ran"]] == ["q", "r", "p"]


# ------------------------------------------------------------------ --due-from-attempt

def test_due_from_attempt_puts_a_partial_behind_an_old_success_on_the_partial_clock():
    st = {"p": {"last_success_utc": "2026-07-14T00:00:00+00:00", "last_attempt_utc": "2026-10-01T00:00:00+00:00",
                "status": "partial"}}
    prod = book({"p": 60.0}, cadence={"p": "monthly"}, state=st)
    flag = m.UnitBook({"p": "monthly"}, st, [], {"p": 60.0}, {}, due_from_attempt=True)
    for day, want in ((1, False), (6, False), (7, True)):  # min(28, 7) x 0.9 = 6.3 days from the 10-01 attempt
        t = T("2026-10-01T00:00") + dt.timedelta(days=day)
        assert prod.is_due("p", t) and flag.is_due("p", t) is want, day     # production: due at every pass
    assert prod.owed(T("2026-10-03T00:00")) == 60.0 and flag.owed(T("2026-10-03T00:00")) == 0.0


@pytest.mark.parametrize("row", [
    {"last_success_utc": "2026-09-30T00:00:00+00:00", "last_attempt_utc": "2026-09-30T00:00:00+00:00", "status": "ok"},
    {"last_success_utc": "2026-07-14T00:00:00+00:00", "last_attempt_utc": "2026-10-01T00:00:00+00:00",
     "status": "transient_fail"},
    {"last_attempt_utc": "2026-10-01T00:00:00+00:00", "status": "partial"},
    {"last_success_utc": "2026-10-01T00:00:00+00:00", "last_attempt_utc": "2026-09-01T00:00:00+00:00",
     "status": "partial"},
    {"last_success_utc": "2026-07-14T00:00:00+00:00", "last_attempt_utc": "not a date", "status": "partial"},
])
def test_due_from_attempt_controls_every_other_unit_is_production(row):
    st = {"u": dict(row)}
    prod = book({"u": 10.0}, cadence={"u": "monthly"}, state=st)
    flag = m.UnitBook({"u": "monthly"}, st, [], {"u": 10.0}, {}, due_from_attempt=True)
    for day in range(0, 40):
        t = T("2026-10-01T00:00") + dt.timedelta(days=day)
        assert prod.is_due("u", t) == flag.is_due("u", t), (row, day)


def test_due_from_attempt_after_a_replayed_partial_turn():
    # p finishes a turn as partial at 10-01 00:10; production keeps it due (old success), the flag waits 6.3 days
    st = {"p": {"last_success_utc": "2026-07-14T00:00:00+00:00", "status": "partial",
                "last_attempt_utc": "2026-08-01T00:00:00+00:00"}}
    rows = [("2026-07-01T00:00:00+00:00", "p", "_all", "partial", 600.0)]
    for flag in (False, True):
        b = m.UnitBook({"p": "monthly"}, st, rows, {"p": 10.0}, {"p": "partial"}, due_from_attempt=flag)
        assert b.is_due("p", T("2026-10-01T00:00"))                         # due before the turn either way
        b.run_pass(T("2026-10-01T00:00"), 100.0)
        assert b.state["p"]["last_attempt_utc"] == "2026-10-01T00:10:00+00:00"
        assert b.is_due("p", T("2026-10-02T00:00")) is (not flag)


# ------------------------------------------------------------------ --clocked-extra, whole flows

def test_clocked_extra_whole_flows_keep_only_whole_pieces():
    rows = [("2026-09-01T00:00:00+00:00", "oecd", "_all", "ok", BIG)]
    # W on, oecd extra, per-flow bound 60: budget 200 -> stopped at 200; window 200 - tail 90 = 110 -> 1 whole
    # piece of 60 kept -> 240 left (divisible minutes would keep 110 -> 190)
    b = m.UnitBook({"oecd": "monthly"}, {}, rows, {"oecd": 300.0}, {}, clocked_extra=("oecd",),
                   whole_flow={"oecd": 60.0})
    r = b.run_pass(T("2026-10-01T00:00"), 200.0, w_on=True, tail_min=90.0)
    assert r["hard_stop"] is None and r["stopped"] == "oecd" and r["used_min"] == 200.0
    assert b.remaining["oecd"] == 240.0
    assert b.db.execute("SELECT status FROM runs ORDER BY id DESC LIMIT 1").fetchone() == ("transient_fail",)
    # the next pass: 240 > 200 again -> one more piece -> 180; the third: 180 <= 200 -> finishes
    b.run_pass(T("2026-10-02T00:00"), 200.0, w_on=True, tail_min=90.0)
    assert b.remaining["oecd"] == 180.0
    r = b.run_pass(T("2026-10-03T00:00"), 200.0, w_on=True, tail_min=90.0)
    assert r["ran"] == [("oecd", 180.0, "ok")] and b.remaining["oecd"] == 300.0 and b.finished["oecd"] == 1


def test_clocked_extra_controls():
    rows = [("2026-09-01T00:00:00+00:00", s, "_all", "ok", BIG) for s in ("oecd", "foo")]

    def one(src, **kw):
        b = m.UnitBook({src: "monthly"}, {}, rows, {src: 300.0}, {}, **kw)
        return b, b.run_pass(T("2026-10-01T00:00"), 200.0, w_on=True, tail_min=90.0)

    # a divisible extra (no bound): keeps the whole window, 300 - 110 = 190
    b, r = one("foo", clocked_extra=("foo",))
    assert r["stopped"] == "foo" and b.remaining["foo"] == 190.0
    # a flow longer than the window keeps nothing: 300 left, every pass, forever
    b, r = one("oecd", clocked_extra=("oecd",), whole_flow={"oecd": 120.0})
    assert r["stopped"] == "oecd" and b.remaining["oecd"] == 300.0
    # not on the list: killed at the hard stop exactly as before (W on)
    b, r = one("oecd")
    assert r["hard_stop"] == "oecd" and r["used_min"] == 210.0 and b.remaining["oecd"] == 300.0
    # W off: the extra list does nothing (as for W_CLOCKED)
    b = m.UnitBook({"oecd": "monthly"}, {}, rows, {"oecd": 300.0}, {}, clocked_extra=("oecd",),
                   whole_flow={"oecd": 60.0})
    r = b.run_pass(T("2026-10-01T00:00"), 200.0, w_on=False)
    assert r["hard_stop"] == "oecd" and b.remaining["oecd"] == 300.0
    # --kill-keeps-progress with whole flows: a kill at 210 keeps 3 pieces of 60
    b = m.UnitBook({"oecd": "monthly"}, {}, rows, {"oecd": 300.0}, {}, clocked_extra=("oecd",),
                   whole_flow={"oecd": 60.0})
    b.run_pass(T("2026-10-01T00:00"), 200.0, w_on=False, kill_keeps_progress=True)
    assert b.remaining["oecd"] == 120.0
    # oecd is never divisible: refused without its per-flow bound
    with pytest.raises(ValueError):
        m.UnitBook({"oecd": "monthly"}, {}, rows, {"oecd": 300.0}, {}, clocked_extra=("oecd",))


def _oecd_log(tmp_path):
    (tmp_path / "local_heavy_updater_20261018-230130.log").write_text(
        "[oecd] 25/99 flows � +0 rows, 0 quiet, 0 transient, 0 structural, 999,999s\n"       # before >>>: ignored
        "[orchestrator] >>> oecd/_all (strategy=giant_changed_units, cadence=monthly)\n"
        "[oecd] catalogue 1,550 flow(s); selected 75 changed/new/redo\n"
        "[oecd] 25/75 flows � +4,354 rows, 0 quiet, 23 transient, 0 structural, 295s\n"
        "[oecd] 50/75 flows � +503,578 rows, 0 quiet, 35 transient, 0 structural, 1,379s\n"
        "[bea] 25/30 flows — +1 rows, 0 quiet, 0 transient, 0 structural, 99,000s\n"            # another source
        "[orchestrator] >>> oecd/_all (strategy=giant_changed_units, cadence=monthly)\n"
        "[oecd] 25/75 flows — +0 rows, 0 quiet, 20 transient, 0 structural, 600s\n",
        encoding="utf-8")
    (tmp_path / "guard_localheavy_0811_1622.log").write_text(
        "[orchestrator] >>> oecd/_all (strategy=giant_changed_units, cadence=monthly)\n"
        "[oecd] 225/400 flows � +12,885,660 rows, 3 quiet, 39 transient, 56 structural, 15,175s\n"
        "[oecd] 250/400 flows � +137,543,173 rows, 3 quiet, 40 transient, 56 structural, 19,154s\n",
        encoding="utf-8")
    (tmp_path / "local_heavy_20261018-230130.log").write_text(
        "[oecd] 25/75 flows - +0 rows, 0 quiet, 0 transient, 0 structural, 88,888s\n", encoding="utf-8")  # wrapper


def test_giant_flow_blocks_reads_25_flow_blocks_per_unit_run(tmp_path):
    _oecd_log(tmp_path)
    fb = m.giant_flow_blocks(str(tmp_path), "oecd")
    # blocks: 0->25 295 s, 25->50 1,084 s, 0->25 600 s (second run), 0->225 15,175 s, 225->250 3,979 s
    assert [(round(b[0] * 60), b[3], b[4]) for b in fb["blocks"]] == [
        (15175, 0, 225), (3979, 225, 250), (1084, 25, 50), (600, 0, 25), (295, 0, 25)]
    assert fb["unit_runs"] == 3 and fb["files"] == 2 and fb["largest_min"] == pytest.approx(15175 / 60)
    assert m.giant_flow_blocks(str(tmp_path), "census")["blocks"] == []                   # no line: no bound


def test_resolve_whole_flow_derives_states_and_refuses(tmp_path):
    import argparse
    (tmp_path / "local_heavy_updater_20261018-230130.log").write_text(
        "[orchestrator] >>> oecd/_all (strategy=giant_changed_units, cadence=monthly)\n"
        "[oecd] 25/75 flows - +4,354 rows, 0 quiet, 23 transient, 0 structural, 295s\n"
        "[oecd] 50/75 flows - +503,578 rows, 0 quiet, 35 transient, 0 structural, 3,295s\n", encoding="utf-8")
    a = argparse.Namespace(clocked_extra=["oecd", "foo"], flow_bound=[], logs=str(tmp_path))
    lines = m.resolve_whole_flow(a)
    assert a.whole_flow == {"oecd": 50.0}                                    # (3,295 - 295) / 60, the largest block
    assert any("CONDITIONAL" in x and "50 min" in x and "ASSUMED" in x for x in lines)
    assert any("1.79B rows" in x for x in lines) and any(x.startswith("foo: clocked in divisible") for x in lines)
    a = argparse.Namespace(clocked_extra=["oecd"], flow_bound=[("oecd", 120.0)], logs=str(tmp_path))
    lines = m.resolve_whole_flow(a)
    assert a.whole_flow == {"oecd": 120.0} and any("given by --flow-bound" in x for x in lines)
    with pytest.raises(SystemExit):                                          # a bound for an unclocked source
        m.resolve_whole_flow(argparse.Namespace(clocked_extra=[], flow_bound=[("oecd", 9.0)], logs=str(tmp_path)))
    (tmp_path / "empty").mkdir()
    with pytest.raises(SystemExit):                                          # whole flows, no log line, no bound
        m.resolve_whole_flow(argparse.Namespace(clocked_extra=["oecd"], flow_bound=[], logs=str(tmp_path / "empty")))


# ------------------------------------------------------------------ --drop-cron (C)

def c_history():
    """day_history plus: a manual daily dispatch at 09-30 18:01 (BEFORE that cron's scheduled run) and at 10-01 18:30,
    and a SECOND scheduled daily run at 10-01 20:00."""
    return sorted(day_history() + [run(DAILY, "2026-09-30T18:01", 10, event="workflow_dispatch"),
                                   run(DAILY, "2026-10-01T18:30", 10, event="workflow_dispatch"),
                                   run(DAILY, "2026-10-01T20:00", 10)], key=lambda r: r.created)


def test_drop_cron_removes_exactly_the_runs_credited_to_it():
    rs = c_history()
    kept, dropped = m.drop_cron_runs(rs, DAILY, 18)
    # credited to an 18:00 cron: the first scheduled daily run in [18:00, +12 h): 09-30 18:05 and 10-01 18:00
    assert sorted(r.created for r in dropped) == [T("2026-09-30T18:05"), T("2026-10-01T18:00")]
    assert len(kept) == len(rs) - 2
    assert T("2026-10-01T18:30") in [r.created for r in kept]           # a manual dispatch is not a cron's run,
    assert T("2026-09-30T18:01") in [r.created for r in kept]           # even when it comes first in the window
    assert T("2026-10-01T20:00") in [r.created for r in kept]           # nor is a second run in the same window
    assert T("2026-10-01T08:00") in [r.created for r in kept]           # the 06:00 cron's run stays
    # control: dropping heavy 15 takes heavy's 15:05 and 15:10 runs and no daily run
    _k, d = m.drop_cron_runs(rs, HEAVY, 15)
    assert sorted(r.created for r in d) == [T("2026-09-30T15:05"), T("2026-10-01T15:10")]
    with pytest.raises(ValueError):
        m.drop_cron_runs(rs, DAILY, 17)                                  # not a cron: never silently nothing
    with pytest.raises(ValueError):
        m.writers_without([("updater-nope.yml", 6)])
    assert m.writers_without([(DAILY, 18)]) == {DAILY: (6,), HEAVY: (3, 15)}


def test_drop_cron_credits_runs_at_their_real_lags():
    # review AR-403 F1: the real 18:00Z runs are created 19:56Z..01:57Z next day (start lag median 173, max 477 min),
    # never in the 18:xx hour; a 06:00Z run was once created at 17:49 (709 min late) and belongs to 06:00
    rs = [run(DAILY, "2026-09-01T06:10", 60),                              # 06 cron, day 1
          run(DAILY, "2026-09-01T21:30", 60),                              # 18 cron day 1, 3.5 h late
          run(DAILY, "2026-09-01T18:30", 10, event="workflow_dispatch"),   # manual in the window: stays
          run(HEAVY, "2026-09-01T18:20", 30),                              # the other writer: stays
          run(DAILY, "2026-09-02T06:05", 60),
          run(DAILY, "2026-09-03T01:57", 60),                              # 18 cron of 09-02, after midnight
          run(DAILY, "2026-09-03T17:49", 60),                              # 06 cron of 09-03, 709 min late: stays
          run(DAILY, "2026-09-03T20:00", 60),                              # 18 cron of 09-03
          run(DAILY, "2026-09-04T19:00", 60),                              # 18 cron of 09-04
          run(DAILY, "2026-09-04T19:30", 60)]                              # a second run in that window: stays
    kept, dropped = m.drop_cron_runs(rs, DAILY, 18)
    assert sorted(r.created for r in dropped) == [T("2026-09-01T21:30"), T("2026-09-03T01:57"),
                                                  T("2026-09-03T20:00"), T("2026-09-04T19:00")]
    assert sorted(r.created for r in kept) == sorted([T("2026-09-01T06:10"), T("2026-09-01T18:30"),
                                                      T("2026-09-01T18:20"), T("2026-09-02T06:05"),
                                                      T("2026-09-03T17:49"), T("2026-09-04T19:30")])


def test_gate_attempt_without_init_falls_back_to_the_gates_crons():
    # review AR-403 F0: tests/test_d3_noprogress_backoff.py builds a Replay with object.__new__; _gate_attempt must
    # not need `writers` (gate's WRITERS), and a cut set on the object is still honoured
    def attempt(writers=None):
        r = object.__new__(m.Replay)
        r.cfg = m.ReplayConfig(policy="today")
        r.last_noprog = None
        r._inflight = lambda: False
        r._cron_pending = lambda t: False
        got = []
        r._pass = lambda t, budget, mode: got.append(budget)
        if writers is not None:
            r.writers = writers
        r._gate_attempt(T("2026-10-01T15:45"), 100.0)
        return got

    assert attempt() == [110.0]                                    # until the 18:00 cron: 135 - 25
    assert attempt(m.writers_without([(DAILY, 18)])) == [650.0]    # until heavy 03:00: 675 - 25


def test_drop_cron_frees_the_window_in_the_gate_model():
    since, until = T("2026-10-01T00:00"), T("2026-10-02T00:00")
    rs = day_history()
    w = m.writers_without([(DAILY, 18)])
    kept, _d = m.drop_cron_runs(rs, DAILY, 18)

    def free(runs, writers):                 # free_windows' model, with the cron hours as a parameter
        busy = [(max(r.created, since), min(r.hist_end, until)) for r in runs if r.hist_end > since
                and r.created < until]
        busy += [(max(c, since), min(e, until)) for c, e, _w in m.cron_blocks(runs, since, until, writers)
                 if e > since and c < until]
        return 1440 - sum(m._mins(a, b) for a, b in m._merge([b for b in busy if b[1] > b[0]]))

    # PR #200's day: 1010 free; C: the 18:00 cron and its run gone -> busy 03-04:30, 06-10, 15-15:40 -> 1070
    assert free(rs, None) == 1010 == sum(m._mins(a, b) for a, b in m.free_windows(rs, since, until))
    assert free(kept, w) == 1070
    # controls: the runs cut but the cron kept -> 10-01's 18:00 cron blocks 18-24 and 09-30's (its 18:05 run cut too)
    # blocks 00-06, so 1010 - 300 - 180 - 90 = 440; the cron cut but the run kept -> the run still occupies 18-19 (1010)
    assert free(kept, None) == 440
    assert free(rs, w) == 1010
    # until_block_fast: at 16:01 the next block was the 18:00 cron (120); under C it is heavy 03:00 (659 -> 660)
    assert m.until_block_fast(T("2026-10-01T16:01")) == 120
    assert m.until_block_fast(T("2026-10-01T16:01"), w) == 660
    assert m.until_block_fast(T("2026-10-01T16:01"), {DAILY: (), HEAVY: ()}) == 720


def test_drop_cron_reaches_the_replayed_gate():
    # a today-policy pass at the 15:45 tick: until-block 135 -> budget 110 (MaxHours past, so a short window is taken);
    # under C the next block is 03:00 -> 675 -> budget 650
    def passes(cfg, runs):
        return make_replay(cfg, runs, units={"u": 10.0}, start="2026-10-01T15:45").run().passes

    kept, _d = m.drop_cron_runs(day_history(), DAILY, 18)
    today = passes(m.ReplayConfig(policy="today", min_hours=1.0), day_history())
    c = passes(m.ReplayConfig(policy="today", min_hours=1.0, drop_cron=((DAILY, 18),)), kept)
    assert today[0]["budget"] == 110.0 and c[0]["budget"] == 650.0
    # the 18:00 cron no longer blocks: under C the gate passes again inside 18:00-19:00 (today: the cron's run is in
    # flight 18:00-19:00, so no pass starts there)
    assert any(T("2026-10-01T18:00") <= p["start"] < T("2026-10-01T19:00") for p in c)
    assert not any(T("2026-10-01T18:00") <= p["start"] < T("2026-10-01T19:00") for p in today)


def test_simulate_refuses_a_cron_cut_without_its_history():
    inp = m.ReplayInputs([], {}, {}, "x", T("2026-10-28T13:45"), T("2026-10-14T22:00"), T("2026-10-28T22:00"),
                         T("2026-10-14T00:00"), None, {}, [])
    with pytest.raises(ValueError):
        m.simulate(m.ReplayConfig(drop_cron=((DAILY, 18),)), inp)


# ------------------------------------------------------------------ --cloud-floor

def _floor_replay():
    day = [run(DAILY, "2026-10-02T08:00", 30), run(HEAVY, "2026-10-02T04:00", 30), run(HEAVY, "2026-10-02T12:00", 30)]
    warm = [run(DAILY, "2026-10-01T10:00", 30), run(HEAVY, "2026-10-01T11:00", 30)]
    rp = make_replay(m.ReplayConfig(policy="today", min_hours=10 ** 9), warm + day, units={"u": 10.0},
                     start="2026-10-02T00:00", end="2026-10-03T00:00").run()
    rp.book.finished["u"], rp.book.remaining["u"] = 1, 0.0
    return rp


def test_cloud_floor_replaces_the_two_runs_a_day():
    rp = _floor_replay()
    two = m.replay_metrics(rp, 10.0, ["u"])                                  # daily 1/day, heavy 2/day
    assert (two["updater-daily.yml"]["runs_day"], two["updater-heavy.yml"]["runs_day"]) == (1.0, 2.0)
    assert two["drains"] and not two["cloud_ge2"] and not two["A_ok"]
    assert "cloud_floor" not in two and "cloud_ok" not in two and "cloud_floor" not in two["cfg"]
    one = m.replay_metrics(rp, 10.0, ["u"], m.replace(rp.cfg, cloud_floor=1.0))
    assert one["cloud_ok"] and one["A_ok"] and one["cloud_floor"] == 1.0 and one["cfg"]["cloud_floor"] == 1.0
    assert not one["cloud_ge2"]                                               # the literal 2/day test is kept
    three = m.replay_metrics(rp, 10.0, ["u"], m.replace(rp.cfg, cloud_floor=3.0))
    assert not three["cloud_ok"] and not three["A_ok"]
    assert "cloud<2 A=NO" in m._fmt_row(two) and "cloud>=1 A=YES" in m._fmt_row(one)
    assert one["label"].endswith(" floor=1") and two["label"] == rp.cfg.label()
    with pytest.raises(ValueError):                                          # only the floor may differ
        m.replay_metrics(rp, 10.0, ["u"], m.replace(rp.cfg, min_hours=5.0))


# ------------------------------------------------------------------ defaults: PR #200's output, byte for byte

PR200_CFG_KEYS = ["policy", "dispatch", "lease_min", "min_hours", "backoff_h", "w_on", "tail_min", "kill_keeps_progress",
                  "pull_min", "push_min", "dispatch_latency_min", "tick_min", "want_block", "max_hours", "d3_leases",
                  "lease_margin_min", "censored_bound", "censored_mult", "count_manual"]


def test_defaults_leave_the_label_and_the_json_as_pr200():
    c = m.ReplayConfig(policy="lease", lease_min=360.0, lease_margin_min=25.0, min_hours=6.0, backoff_h=8.0,
                       w_on=True, count_manual=False)
    assert list(m.cfg_dict(c)) == PR200_CFG_KEYS
    assert c.label() == "A L=360-25 MinH=6 D3=8 W=on disp=yes c=tx1 man=excl"
    assert m.ReplayConfig(policy="today").label() == "today MinH=20 D3=off W=off c=tx1"
    for kw, mark in ((dict(order="attempt"), " ord=attempt"), (dict(clocked_extra=("oecd",),
                     whole_flow=(("oecd", 75.6),)), " clk+=oecd(75.6)"),
                     (dict(drop_cron=((DAILY, 18),)), " C=-daily18"), (dict(due_from_attempt=True), " due=att"),
                     (dict(cloud_floor=1.0), " floor=1")):
        x = m.replace(c, **kw)
        assert mark in x.label() and set(m.cfg_dict(x)) - set(PR200_CFG_KEYS) == set(kw), kw


def _cli_inputs(tmp_path):
    """A planted `fetch` dir (both writers, 2026-10-18 .. 10-28), one desktop pass log and a state copy: enough for
    cmd_replay61 to run a 2-day span end to end. Runs are 5 min late, except at real lags (review AR-403 F1): the
    daily 18:00Z runs are created at 21:30, 10-22's at 01:57 next day, and 10-20's 06:00Z run at 17:49."""
    import json
    import sqlite3
    hist = tmp_path / "hist"
    (hist / "jobs").mkdir(parents=True)
    rid = iter(range(1000, 10 ** 6))

    def iso(t):
        return t.strftime("%Y-%m-%dT%H:%M:%SZ")

    for wf, hours, minutes in ((DAILY, (6, 18), 60), (HEAVY, (3, 15), 30)):
        rows = []
        for day in range(18, 29):
            for h in hours:
                c = T(f"2026-10-{day:02d}T{h:02d}:05")
                if wf == DAILY and h == 18:
                    c = T(f"2026-10-{day + 1:02d}T01:57") if day == 22 else T(f"2026-10-{day:02d}T21:30")
                elif wf == DAILY and day == 20:
                    c = T("2026-10-20T17:49")
                e = c + dt.timedelta(minutes=minutes)
                i = next(rid)
                rows.append({"databaseId": i, "createdAt": iso(c), "startedAt": iso(c), "updatedAt": iso(e),
                             "event": "schedule", "status": "completed", "conclusion": "success", "attempt": 1})
                steps = [{"name": "Pull state from R2", "startedAt": iso(c + dt.timedelta(minutes=1)),
                          "completedAt": iso(c + dt.timedelta(minutes=3)), "conclusion": "success"},
                         {"name": "Health gate (fail past 2x SLA)", "startedAt": iso(e - dt.timedelta(minutes=4)),
                          "completedAt": iso(e - dt.timedelta(minutes=2)), "conclusion": "success"},
                         {"name": "Push state to R2 (compare-and-swap)", "startedAt": iso(e - dt.timedelta(minutes=8)),
                          "completedAt": iso(e - dt.timedelta(minutes=5)), "conclusion": "success"}]
                jobs = [{"name": "setup" if wf == HEAVY else "update", "startedAt": iso(c), "completedAt": iso(e),
                         "steps": steps}]
                (hist / "jobs" / f"{i}.json").write_text(json.dumps({"jobs": jobs}), encoding="utf-8")
        (hist / f"runs_{wf}.json").write_text(json.dumps(rows), encoding="utf-8")
    logs = tmp_path / "logs"
    logs.mkdir()
    (logs / "local_heavy_20261025-220113.log").write_text(
        "[22:01:13] local heavy updater starting\n[22:01:42] whole-run budget: 275 min\n[22:01:42] pull-state ...\n"
        "[22:04:30] running updater for 30 source(s)\n[01:37:33] push-state ...\n"
        "[01:45:02] cadence stamped - state committed (3 unit(s) advanced)\n", encoding="utf-8")
    db = tmp_path / "state.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE unit_state(source_id TEXT, unit_id TEXT, strategy TEXT, upstream_vintage TEXT, "
                "last_success_utc TEXT, last_attempt_utc TEXT, status TEXT, last_obs_date TEXT, obs_count INTEGER, "
                "attempt_count INTEGER, last_error TEXT)")
    con.execute("CREATE TABLE runs(id INTEGER PRIMARY KEY AUTOINCREMENT, ts_utc TEXT, source_id TEXT, unit_id TEXT, "
                "status TEXT, obs INTEGER, dur_s REAL, note TEXT)")
    con.execute("INSERT INTO unit_state(source_id, unit_id, last_success_utc, last_attempt_utc, status) VALUES "
                "('istat', '_all', '2026-07-14T00:00:00+00:00', '2026-10-25T23:00:00+00:00', 'partial')")
    con.execute("INSERT INTO runs(ts_utc, source_id, unit_id, status, dur_s) VALUES "
                "('2026-10-25T23:00:00+00:00', 'istat', '_all', 'partial', 3000.0)")
    # bea: ok, success 09-01 - between istat's success and its attempt, in the same cost band (50 min)
    con.execute("INSERT INTO unit_state(source_id, unit_id, last_success_utc, last_attempt_utc, status) VALUES "
                "('bea', '_all', '2026-09-01T00:00:00+00:00', '2026-09-01T00:00:00+00:00', 'ok')")
    con.execute("INSERT INTO runs(ts_utc, source_id, unit_id, status, dur_s) VALUES "
                "('2026-09-01T00:00:00+00:00', 'bea', '_all', 'ok', 3000.0)")
    con.commit()
    con.close()
    return ["--history", str(hist), "--logs", str(logs), "--state", str(db), "--days", "2", "--lease", "300",
            "--min-hours", "6", "--backoff", "8", "--w", "off", "--lease-margin", "25", "--count-manual", "no",
            "--dispatch", "yes", "--censored-mult", "1"]


def _cli(monkeypatch, capsys, base, extra, json_path):
    import json
    monkeypatch.setattr(sys, "argv", ["x", "replay61"] + base + extra + ["--json", str(json_path)])
    m.main()
    out = capsys.readouterr().out
    rows = [ln for ln in out.splitlines() if ln.startswith(("today ", "A L="))]
    return out, rows, json.loads(json_path.read_text(encoding="utf-8"))


def test_cmd_replay61_runs_each_extension_end_to_end(tmp_path, monkeypatch, capsys):
    base = _cli_inputs(tmp_path)
    out, rows, js = _cli(monkeypatch, capsys, base, [], tmp_path / "d.json")
    # no flag: one today row and one lease row, PR #200's labels and cfg keys, no extension line
    assert [r.split("  ")[0].strip() for r in rows] == ["today MinH=6 D3=8 W=off c=tx1 man=excl",
                                                         "A L=300-25 MinH=6 D3=8 W=off disp=yes c=tx1 man=excl"]
    assert "extensions (PR 2)" not in out and "cloud floor" not in out and ">= 2.0 completed" in out
    assert [list(x["cfg"]) for x in js] == [PR200_CFG_KEYS] * 2
    # --cloud-floor 1 2: every scenario once per floor, from ONE replay (same numbers, different verdict basis)
    out, rows, js = _cli(monkeypatch, capsys, base, ["--cloud-floor", "1", "2"], tmp_path / "f.json")
    assert len(rows) == 4 and [x["cfg"].get("cloud_floor", 2.0) for x in js] == [1.0, 2.0, 1.0, 2.0]
    assert js[0]["owed_end"] == js[1]["owed_end"] and js[0]["updater-daily.yml"] == js[1]["updater-daily.yml"]
    assert "floor=1" in rows[0] and "floor=" not in rows[1] and "the row's floor (1 / 2" in out
    # --order production attempt: both orders, the attempt rows labelled
    out, rows, js = _cli(monkeypatch, capsys, base, ["--order", "production", "attempt"], tmp_path / "o.json")
    assert len(rows) == 4 and ["ord=attempt" in r for r in rows] == [False, False, True, True]
    assert [x["cfg"].get("order", "production") for x in js] == ["production", "production", "attempt", "attempt"]
    # --drop-cron: the cut is stated with its count - 11 days x one 18:00Z run, 10 at 21:30 (210 min) and one at
    # 01:57 next day (477 min)
    out, rows, js = _cli(monkeypatch, capsys, base, ["--drop-cron", "updater-daily.yml:18"], tmp_path / "c.json")
    assert "C: dropped the 18:00Z cron of updater-daily.yml from the gate model (crons now (6,)) and the 11 " \
           "scheduled history run(s)" in out and "start lag min 210, median 210, max 477 min" in out
    assert all("C=-daily18" in r for r in rows) and all(x["cfg"]["drop_cron"] == [[DAILY, 18]] for x in js)
    # --clocked-extra oecd with a given bound: the CONDITIONAL statement, and the label carries the bound
    out, rows, js = _cli(monkeypatch, capsys, base, ["--clocked-extra", "oecd", "--flow-bound", "oecd=60"],
                         tmp_path / "k.json")
    assert "CONDITIONAL on the bound 'no single oecd flow takes longer than 60 min' (given by --flow-bound)" in out
    assert all("clk+=oecd(60)" in r for r in rows) and js[0]["cfg"]["whole_flow"] == [["oecd", 60.0]]
    # --due-from-attempt: istat (partial since 10-25, success 07-14) leaves the owed set at the start
    out, rows, js = _cli(monkeypatch, capsys, base, ["--due-from-attempt"], tmp_path / "a.json")
    _o, _r, js0 = _cli(monkeypatch, capsys, base, [], tmp_path / "d2.json")
    assert all("due=att" in r for r in rows)
    assert js0[0]["owed_start"] - js[0]["owed_start"] == pytest.approx(50.0)     # istat's 3,000-s turn


def test_cli_inputs_carry_the_cut_and_simulate_carries_the_order(tmp_path):
    import argparse
    base = _cli_inputs(tmp_path)
    kw = dict(zip(base[0:6:2], base[1:6:2]))
    args = argparse.Namespace(history=kw["--history"], logs=kw["--logs"], state=kw["--state"], days=2.0,
                              copy_utc="2026-10-28T13:45:00Z", end="2026-10-28T22:00:00Z", drop_cron=[])
    inp = m.load_replay_inputs(args)
    args.drop_cron = [(DAILY, 18)]
    cut = m.load_replay_inputs(args)
    # the 11 runs credited to the 18:00 cron leave the history the replay reads, not only the printed count
    assert len(inp.runs) - len(cut.runs) == 11 and len(cut.dropped) == 11 and cut.drop_cron == ((DAILY, 18),)
    late = {T(f"2026-10-{d:02d}T21:30") for d in range(18, 29) if d != 22} | {T("2026-10-23T01:57")}
    assert {r.created for r in cut.dropped} == late                          # the 18:00Z runs at their real lags
    assert not any(r.created in late for r in cut.runs)
    assert T("2026-10-20T17:49") in {r.created for r in cut.runs if r.wf == DAILY}   # 06:00Z's late run stays
    # simulate() hands the order to the unit book: istat (success 07-14, attempt 10-25) and bea (success 09-01),
    # both in band 2, swap places in the first pass
    def first_pass(order):
        cfg = m.ReplayConfig(policy="today", min_hours=6.0, backoff_h=8.0, order=order, pull_min=2.0, push_min=7.0)
        rp, _o, _d = m.simulate(cfg, inp)
        ran = [x[0] for x in rp.passes[0]["ran"]]
        return ran.index("istat") < ran.index("bea")
    assert first_pass("production") and not first_pass("attempt")


def test_b2_an_unparseable_attempt_is_ignored_as_production_ignores_it():
    # q's attempt stamp is unparseable: B2 must sort q by its success (as production does), not treat the junk stamp
    # as never-run (overdue_key's -inf would put q first)
    st = {"p": {"last_success_utc": "2026-08-01T00:00:00+00:00", "last_attempt_utc": "2026-08-01T00:00:00+00:00",
                "status": "ok"},
          "q": {"last_success_utc": "2026-09-01T00:00:00+00:00", "last_attempt_utc": "garbage", "status": "ok"}}
    rows = [("2026-07-01T00:00:00+00:00", s, "_all", "ok", BIG) for s in st]
    for order in ("production", "attempt"):
        b = m.UnitBook({s: "monthly" for s in st}, st, rows, {"p": 10.0, "q": 10.0}, {}, order=order)
        assert [x[0] for x in b.run_pass(T("2026-10-01T00:00"), 1000.0)["ran"]] == ["p", "q"], order


def test_cli_parsers_and_baseline_refuses_the_extensions(monkeypatch):
    import argparse
    assert m._wf_hour("updater-daily.yml:18") == (DAILY, 18)
    assert m._src_minutes("oecd=75.6") == ("oecd", 75.6)
    for bad in ("updater-daily.yml:17", "nope.yml:6", "updater-daily.yml"):
        with pytest.raises(argparse.ArgumentTypeError):
            m._wf_hour(bad)
    for bad in ("oecd", "oecd=", "oecd=x", "oecd=0"):
        with pytest.raises(argparse.ArgumentTypeError):
            m._src_minutes(bad)
    for flag in (["--order", "attempt"], ["--due-from-attempt"], ["--cloud-floor", "1"],
                 ["--drop-cron", "updater-daily.yml:18"], ["--clocked-extra", "oecd"]):
        monkeypatch.setattr(sys, "argv", ["x", "replay61", "--baseline", "--history", "h", "--logs", "l",
                                          "--state", "s"] + flag)
        with pytest.raises(SystemExit) as e:
            m.main()
        assert "takes none of" in str(e.value.code), flag
