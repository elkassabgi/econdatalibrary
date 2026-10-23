"""tools/ci_writer_gate.py: the desktop heavy pass must not overlap a cloud state writer - including a
scheduled run GitHub has not started yet, which is what collided on 2026-09-08, 09-13 and 09-15.

The run lists below reach back to 2026-08-20 so that a decision taken at the
earliest collision has a real 14-day history to measure, rather than falling back because the fixture
happened to start two days earlier.

The fixtures are the scheduled runs GitHub actually created (gh run list, measured 2026-09-15, extended
2026-10-20 with every scheduled run created 2026-09-16..10-20) and the start times of the desktop passes
whose pull/push window overlapped one (logs/local_heavy_*.log).
"""
import pathlib
import datetime as dt
import os
import re
import shutil
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools"))

import ci_writer_gate as g  # noqa: E402

DAILY_CREATED = [
    # 2026-09-16..10-20, added 2026-10-20 (gh run list --workflow updater-daily.yml --event schedule)
    "2026-10-20T11:04", "2026-10-19T22:00", "2026-10-19T12:27", "2026-10-18T20:07", "2026-10-18T10:33",
    "2026-10-17T20:08", "2026-10-17T10:17", "2026-10-16T20:31", "2026-10-16T10:55", "2026-10-15T20:56",
    "2026-10-15T11:00", "2026-10-14T21:17", "2026-10-14T11:23", "2026-10-13T22:18", "2026-10-13T13:45",
    "2026-10-13T00:09", "2026-10-12T14:42", "2026-10-11T21:29", "2026-10-11T12:17", "2026-10-10T21:10",
    "2026-10-10T12:04", "2026-10-09T22:16", "2026-10-09T12:45", "2026-10-08T22:54", "2026-10-08T12:59",
    "2026-10-07T22:43", "2026-10-07T12:50", "2026-10-06T22:20", "2026-10-06T12:56", "2026-10-05T23:45",
    "2026-10-05T13:53", "2026-10-04T20:52", "2026-10-04T11:55", "2026-10-03T20:36", "2026-10-03T11:15",
    "2026-10-02T21:50", "2026-10-02T12:03", "2026-10-01T22:21", "2026-10-01T12:38", "2026-09-30T21:53",
    "2026-09-30T12:05", "2026-09-29T21:53", "2026-09-29T12:19", "2026-09-28T22:56", "2026-09-28T13:11",
    "2026-09-27T20:53", "2026-09-27T11:36", "2026-09-26T20:39", "2026-09-26T10:58", "2026-09-25T21:06",
    "2026-09-25T11:22", "2026-09-24T21:09", "2026-09-24T11:17", "2026-09-23T21:05", "2026-09-23T10:58",
    "2026-09-22T20:54", "2026-09-22T11:08", "2026-09-21T21:42", "2026-09-21T12:15", "2026-09-20T20:20",
    "2026-09-20T10:50", "2026-09-19T20:03", "2026-09-19T10:28", "2026-09-18T20:25", "2026-09-18T10:44",
    "2026-09-17T20:57", "2026-09-17T11:11", "2026-09-16T20:53",
    # measured 2026-09-15/16
    "2026-09-16T11:00", "2026-09-15T20:52", "2026-09-15T11:18", "2026-09-14T21:29", "2026-09-14T12:06",
    "2026-09-13T20:20", "2026-09-13T11:16", "2026-09-12T20:11", "2026-09-12T10:16", "2026-09-11T20:28",
    "2026-09-11T10:47", "2026-09-10T20:26", "2026-09-10T10:50", "2026-09-09T20:26", "2026-09-09T10:54",
    "2026-09-08T20:44", "2026-09-08T10:48", "2026-09-07T21:07", "2026-09-07T11:50", "2026-09-06T20:00",
    "2026-09-06T10:28", "2026-09-05T19:56", "2026-09-05T10:07", "2026-09-04T20:17", "2026-09-04T10:49",
    "2026-09-03T20:31", "2026-09-03T10:50", "2026-09-02T20:30", "2026-09-02T10:50", "2026-09-01T20:34",
    "2026-09-01T11:16", "2026-08-31T22:28", "2026-08-30T20:19", "2026-08-30T10:51", "2026-08-29T20:13",
    "2026-08-29T11:54", "2026-08-29T01:20", "2026-08-28T17:49", "2026-08-28T01:57", "2026-08-27T17:00",
    "2026-08-26T19:39", "2026-08-26T06:27", "2026-08-25T18:27", "2026-08-25T06:24", "2026-08-24T18:28",
    "2026-08-24T06:33", "2026-08-23T18:17", "2026-08-23T06:21", "2026-08-22T18:17", "2026-08-22T06:19",
    "2026-08-21T18:24", "2026-08-21T06:25", "2026-08-20T18:25", "2026-08-20T06:24",
]
HEAVY_CREATED = [
    # 2026-09-17..10-20, added 2026-10-20 (gh run list --workflow updater-heavy.yml --event schedule)
    "2026-10-20T08:07", "2026-10-19T20:15", "2026-10-19T08:25", "2026-10-18T17:36", "2026-10-18T07:42",
    "2026-10-17T17:34", "2026-10-17T07:27", "2026-10-16T18:28", "2026-10-16T07:50", "2026-10-15T18:43",
    "2026-10-15T07:56", "2026-10-14T18:59", "2026-10-14T08:19", "2026-10-13T20:23", "2026-10-13T10:01",
    "2026-10-12T22:16", "2026-10-12T10:08", "2026-10-11T19:13", "2026-10-11T09:36", "2026-10-10T18:58",
    "2026-10-10T09:19", "2026-10-09T19:51", "2026-10-09T09:55", "2026-10-08T20:16", "2026-10-08T09:51",
    "2026-10-07T20:14", "2026-10-07T09:41", "2026-10-06T19:52", "2026-10-06T09:42", "2026-10-05T21:41",
    "2026-10-05T09:56", "2026-10-04T18:25", "2026-10-04T09:16", "2026-10-03T18:22", "2026-10-03T08:48",
    "2026-10-02T19:35", "2026-10-02T09:16", "2026-10-01T19:55", "2026-10-01T09:42", "2026-09-30T19:40",
    "2026-09-30T09:14", "2026-09-29T19:41", "2026-09-29T09:24", "2026-09-28T20:51", "2026-09-28T09:17",
    "2026-09-27T18:41", "2026-09-27T08:52", "2026-09-26T18:06", "2026-09-26T08:15", "2026-09-25T18:58",
    "2026-09-25T08:28", "2026-09-24T18:42", "2026-09-24T08:04", "2026-09-23T18:41", "2026-09-23T08:13",
    "2026-09-22T18:22", "2026-09-22T08:11", "2026-09-21T19:45", "2026-09-21T08:30", "2026-09-20T17:44",
    "2026-09-20T08:13", "2026-09-19T17:30", "2026-09-19T07:45", "2026-09-18T17:55", "2026-09-18T07:55",
    "2026-09-17T18:38", "2026-09-17T08:19",
    # measured 2026-09-15/16
    "2026-09-16T18:30", "2026-09-16T08:13", "2026-09-15T18:32", "2026-09-15T08:19", "2026-09-14T19:38",
    "2026-09-14T08:29", "2026-09-13T17:44", "2026-09-13T07:55", "2026-09-12T17:28", "2026-09-12T07:37",
    "2026-09-11T17:57", "2026-09-11T07:42", "2026-09-10T17:55", "2026-09-10T07:48", "2026-09-09T18:03",
    "2026-09-09T07:47", "2026-09-08T18:08", "2026-09-08T07:45", "2026-09-07T18:53", "2026-09-07T07:49",
    "2026-09-06T17:22", "2026-09-06T07:32", "2026-09-05T16:58", "2026-09-05T07:19", "2026-09-04T17:51",
    "2026-09-04T07:38", "2026-09-03T18:08", "2026-09-03T07:41", "2026-09-02T18:09", "2026-09-02T07:33",
    "2026-09-01T18:00", "2026-09-01T08:13", "2026-08-31T20:25", "2026-08-30T18:16", "2026-08-30T09:07",
    "2026-08-29T18:02", "2026-08-29T09:56", "2026-08-28T23:46", "2026-08-28T15:14", "2026-08-28T00:09",
    "2026-08-27T14:02", "2026-08-26T16:10", "2026-08-26T03:57", "2026-08-25T15:33", "2026-08-25T03:53",
    "2026-08-24T15:29", "2026-08-24T04:00", "2026-08-23T15:13", "2026-08-23T03:55", "2026-08-22T15:12",
    "2026-08-22T03:47", "2026-08-21T15:23", "2026-08-21T03:55", "2026-08-20T15:24", "2026-08-20T03:52",
]
ACTIVE = {"updater-daily.yml": "active", "updater-heavy.yml": "active"}


def at(s):
    return dt.datetime.fromisoformat(s).replace(tzinfo=dt.timezone.utc)


def history_as_of(now, status="completed"):
    """The scheduled runs GitHub had created by `now`, newest first, as `gh run list --json` shows them.
    Every run here had ended by the times these tests use (daily runs end within 5 h 55 min, heavy 5 h)."""
    def rows(created):
        return [{"createdAt": c + ":00Z", "event": "schedule", "status": status}
                for c in created if at(c) <= now]
    return {"updater-daily.yml": rows(DAILY_CREATED), "updater-heavy.yml": rows(HEAVY_CREATED)}


RUNNER_PS1 = pathlib.Path(__file__).resolve().parents[1] / "tools" / "run_local_heavy.ps1"


def old_check(runs):
    """What run_local_heavy.ps1 asked before: is an updater-daily run in flight?"""
    return any(r["status"] != "completed" for r in runs["updater-daily.yml"])


@pytest.mark.parametrize("pass_start", [
    "2026-09-08T10:31:28",   # desktop pass whose push was refused (pull 10:31, push 13:09)
    "2026-09-13T10:32:09",   # desktop pass whose push was refused (pull 10:32, push 14:26)
    "2026-09-15T10:38:56",   # desktop pass that pushed first; the cloud run's push was refused at 15:28
])
def test_every_recorded_collision_is_blocked_and_the_old_check_let_it_through(pass_start):
    now = at(pass_start)
    runs = history_as_of(now)
    assert not old_check(runs), "fixture error: the old check must have said CI idle"
    blocked, reason = g.decide(runs, now, ACTIVE)
    assert blocked, reason
    assert "updater-daily.yml 06:00Z cron has not started yet" in reason, reason



def test_a_run_in_flight_blocks():
    now = at("2026-09-15T12:00:00")
    runs = history_as_of(now)
    runs["updater-heavy.yml"][0] = dict(runs["updater-heavy.yml"][0], status="in_progress")
    blocked, reason = g.decide(runs, now, ACTIVE)
    assert blocked and "in_progress" in reason, reason


def test_clear_after_the_evening_runs_and_before_the_next_cron():
    # daily 18:00Z run 21:29->22:36 and heavy 15:00Z run 19:38->20:57 had both completed by 23:30
    now = at("2026-09-14T23:30:00")
    blocked, reason = g.decide(history_as_of(now), now, ACTIVE)
    assert not blocked, reason


def test_a_pending_heavy_cron_blocks_after_the_morning_daily_run():
    now = at("2026-09-15T15:40:00")      # the 15:00Z heavy cron had no run yet
    blocked, reason = g.decide(history_as_of(now), now, ACTIVE)
    assert blocked and "updater-heavy.yml 15:00Z cron has not started yet" in reason, reason


# THE FAILURES THAT ENDED THE 300-MINUTE CEILING. Each desktop pass below started while the 06:00Z cron was more
# than 300 min old and had no run; the gate said CLEAR (logs/local_heavy_<stamp>.log, "CI writer gate: CLEAR"),
# the cloud run was created a few minutes later and pushed first, and the pass's push was refused:
#   pass 20260924-110309  gate CLEAR 11:03:25Z  run created 11:17:48Z (317.8 min)  push-state FAILED (2) 14:50:50Z
#   pass 20261007-122218  gate CLEAR 12:22:32Z  run created 12:50:29Z (410.5 min)  push-state FAILED (2) 14:50:18Z
#   pass 20261020-105957  gate CLEAR 11:00:13Z  run created 11:04:16Z (304.3 min)  push-state FAILED (2) 14:48:11Z
# No run of either workflow (any event) was in flight at any of these instants (gh run list createdAt/updatedAt,
# read 2026-10-20), so the CLEAR came from the cron check alone - which is what history_as_of reproduces.
PUSH_FAILURES = [
    ("2026-09-24T11:03:09", "2026-09-24T11:03:25", "2026-09-24T11:17"),
    ("2026-10-07T12:22:18", "2026-10-07T12:22:32", "2026-10-07T12:50"),
    ("2026-10-20T10:59:57", "2026-10-20T11:00:13", "2026-10-20T11:04"),
]
OLD_CEILING = dt.timedelta(minutes=300)     # (largest cron gap 9 h) - (FREE_BLOCK_MIN 4 h), removed 2026-10-20


def cleared_by_the_old_ceiling(runs, now):
    """What the old rule could at most have blocked: nothing in flight, and no un-started cron younger than 300 min.

    The old horizon was min(measured p90, 300 min), so wherever this says CLEAR the old gate said CLEAR too."""
    if any(r["status"] != "completed" for rows in runs.values() for r in rows):
        return False
    for wf, hours in g.WRITERS.items():
        for back in (1, 0):
            day = (now - dt.timedelta(days=back)).date()
            for h in hours:
                cron = dt.datetime(day.year, day.month, day.day, h, tzinfo=dt.timezone.utc)
                if cron <= now < cron + OLD_CEILING and not any(
                        cron <= g._parse(r["createdAt"]) < cron + g.CRON_SPACING for r in runs[wf]):
                    return False
    return True


@pytest.mark.parametrize("pass_start,gate_read,run_created", PUSH_FAILURES)
def test_every_push_failure_since_2026_09_20_now_reads_blocked(pass_start, gate_read, run_created):
    for instant in (pass_start, gate_read):
        now = at(instant)
        runs = history_as_of(now)
        assert run_created not in [r["createdAt"][:16] for r in runs["updater-daily.yml"]], "fixture: run not yet created"
        assert at(run_created + ":00") > now, "fixture: the run came after the decision"
        # NEGATIVE CONTROL: the fixture really is the defect - the old ceiling let this instant through.
        if instant == gate_read:
            assert cleared_by_the_old_ceiling(runs, now), f"{instant}: fixture does not reproduce the old CLEAR"
        blocked, reason = g.decide(runs, now, ACTIVE)
        assert blocked, f"{instant}: {reason}"
        assert "updater-daily.yml 06:00Z cron has not started yet" in reason, reason


# EVERY desktop pass since 2026-09-20 that the gate let start (the "CI writer gate: CLEAR" line of each
# logs/local_heavy_<stamp>.log, 34 passes), split by whether a scheduled cloud run was created between that
# CLEAR and the pass's last line. 10 were overlapped: the 3 refused desktop pushes above, and 7 whose desktop
# push won - the "Push state to R2" step of each of those 7 cloud runs reads failure (gh run view --json jobs,
# runs 36567385772, 36863134400, 37004436689, 37119058547, 37200338220, 37320360751, 38407606463). The other
# 24 were not overlapped.
OVERLAPPED_CLEARS = ["2026-09-24T11:03:25", "2026-09-29T11:02:46", "2026-10-01T11:02:08", "2026-10-02T11:01:28",
                     "2026-10-03T11:00:34", "2026-10-04T11:02:47", "2026-10-05T11:25:45", "2026-10-07T12:22:32",
                     "2026-10-13T12:36:38", "2026-10-20T11:00:13"]
UNOVERLAPPED_CLEARS = ["2026-09-20T12:28:32", "2026-09-20T13:37:01", "2026-09-20T13:47:34", "2026-09-20T22:31:37",
                       "2026-09-21T13:11:25", "2026-09-21T13:32:26", "2026-09-21T13:42:58", "2026-09-21T13:53:30",
                       "2026-09-21T23:23:32", "2026-09-24T01:11:53", "2026-09-24T01:54:26", "2026-09-24T02:05:02",
                       "2026-09-24T02:15:40", "2026-09-25T01:06:53", "2026-09-26T13:20:50", "2026-10-08T00:10:54",
                       "2026-10-08T23:10:18", "2026-10-09T22:39:56", "2026-10-10T23:22:25", "2026-10-11T23:50:31",
                       "2026-10-15T02:19:03", "2026-10-17T01:27:29", "2026-10-17T23:20:28", "2026-10-18T23:01:46"]


@pytest.mark.parametrize("instant", OVERLAPPED_CLEARS)
def test_every_real_pass_a_cloud_run_overlapped_now_reads_blocked(instant):
    now = at(instant)
    runs = history_as_of(now)
    assert cleared_by_the_old_ceiling(runs, now), "control: the old ceiling said CLEAR here"
    blocked, reason = g.decide(runs, now, ACTIVE)
    assert blocked and "cron has not started yet" in reason, reason


@pytest.mark.parametrize("instant", UNOVERLAPPED_CLEARS)
def test_no_real_pass_that_was_not_overlapped_is_taken_away(instant):
    now = at(instant)
    blocked, reason = g.decide(history_as_of(now), now, ACTIVE)
    assert not blocked, reason


def test_the_block_ends_when_the_late_run_has_been_created_and_finished():
    """Control: the rule is 'until the run is created', not a longer fixed window.

    2026-10-20: the 06:00Z daily run was created 11:04:16Z and completed 13:43:41Z; the 03:00Z heavy run was
    created 08:07:39Z and completed 10:33:08Z; the next cron is 15:00Z. Nothing was in flight at 13:45Z."""
    now = at("2026-10-20T13:45:00")
    blocked, reason = g.decide(history_as_of(now), now, ACTIVE)
    assert not blocked, reason
    mins = g.minutes_until_block(history_as_of(now), now, ACTIVE)
    assert mins == 75, f"the next block must be the 15:00Z cron, 75 min away; got {mins}"


def test_the_late_run_blocks_while_it_is_in_flight():
    now = at("2026-10-20T11:05:00")          # the 06:00Z run was created 11:04:16Z and ran until 13:43:41Z
    runs = history_as_of(now)
    runs["updater-daily.yml"][0] = dict(runs["updater-daily.yml"][0], status="in_progress")
    blocked, reason = g.decide(runs, now, ACTIVE)
    assert blocked and "in_progress" in reason, reason


def test_the_window_covers_every_lag_measured_over_60_days():
    """237 scheduled runs of both writers, 2026-08-21..10-20 (createdAt - cron): median 278, p99 661, max 710 min
    (as paired within 12 h; the one ambiguous window is the KNOWN RESIDUAL in the gate). A window shorter than the
    measured maximum is a CLEAR that the next late run collides with.
    """
    assert g.PENDING_LIMIT >= dt.timedelta(minutes=710)
    assert g.PENDING_LIMIT <= g.CRON_SPACING, "a run later than CRON_SPACING is credited to the next cron"


def _all_started_but(now, missing, lag_min=30, days=3):
    """Every cron of the last `days` started `lag_min` late and completed, except the (workflow, cron) in missing."""
    out = {wf: [] for wf in g.WRITERS}
    for wf, hours in g.WRITERS.items():
        for d in range(days):
            day = (now - dt.timedelta(days=d)).replace(hour=0, minute=0, second=0, microsecond=0)
            for h in hours:
                cron = day + dt.timedelta(hours=h)
                created = cron + dt.timedelta(minutes=lag_min)
                if created <= now and (wf, cron) not in missing:
                    out[wf].append({"createdAt": created.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                    "event": "schedule", "status": "completed"})
        out[wf].sort(key=lambda r: r["createdAt"], reverse=True)
    return out


def test_an_unstarted_cron_blocks_for_the_whole_measured_lag_and_then_hands_over():
    cron = at("2026-10-14T06:00:00")
    missing = {("updater-daily.yml", cron)}
    now = cron + g.PENDING_LIMIT - dt.timedelta(minutes=1)     # 719 min; the 60-day maximum lag was 710
    blocked, reason = g.decide(_all_started_but(now, missing), now, ACTIVE)
    assert blocked and "updater-daily.yml 06:00Z cron has not started yet" in reason, reason

    # At the limit the 06:00Z cron can no longer be credited a run; the 18:00Z cron is due and blocks itself.
    now = cron + g.PENDING_LIMIT
    blocked, reason = g.decide(_all_started_but(now, missing), now, ACTIVE)
    assert blocked and "updater-daily.yml 18:00Z cron has not started yet" in reason, reason

    # A skipped cron costs the desktop until the NEXT cron's run is created and done - then it is clear.
    now = cron + g.PENDING_LIMIT + dt.timedelta(minutes=40)
    blocked, reason = g.decide(_all_started_but(now, missing), now, ACTIVE)
    assert not blocked, reason


def test_if_github_stops_creating_scheduled_runs_the_desktop_gets_no_clear_time():
    """The cost of the rule, pinned so it is a decision and not a surprise (R1033's arithmetic, true for this case).

    With no scheduled run created after 2026-10-14T00:00Z, every cron from 03:00Z on blocks for 12 h, and the four
    windows cover the clock. The only release is -SkipCiCheck; run_local_heavy.ps1 warns after 2 x $MinHours."""
    cutoff = at("2026-10-14T00:00:00")
    t, clear = at("2026-10-14T03:00:00"), []
    while t < at("2026-10-15T03:00:00"):
        hist = {wf: [r for r in rows if g._parse(r["createdAt"]) < cutoff]
                for wf, rows in _all_started_but(t, set()).items()}
        if not g.decide(hist, t, ACTIVE)[0]:
            clear.append(t)
        t += dt.timedelta(minutes=5)
    assert clear == [], f"expected no clear instant in a scheduler stall, got {clear[:3]}"
    # control: the same walk with the runs present does find clear time, so the walk can see it
    t, found = at("2026-10-14T03:00:00"), False
    while t < at("2026-10-15T03:00:00") and not found:
        found = not g.decide(_all_started_but(t, set()), t, ACTIVE)[0]
        t += dt.timedelta(minutes=5)
    assert found, "control: with every run present the walk must find a clear instant"


# A TYPICAL DAY, from the medians of 2026-08-21..10-20 (gh run list, createdAt and updatedAt of the scheduled
# runs): start lag 03Z 314, 06Z 315, 15Z 208, 18Z 173 min; duration daily 171, heavy 78 min.
TYPICAL_LAG = {("updater-heavy.yml", 3): 314, ("updater-daily.yml", 6): 315,
               ("updater-heavy.yml", 15): 208, ("updater-daily.yml", 18): 173}
TYPICAL_DUR = {"updater-daily.yml": 171, "updater-heavy.yml": 78}


def _typical_history(now):
    out = {wf: [] for wf in g.WRITERS}
    for wf, hours in g.WRITERS.items():
        for d in range(3):
            day = (now - dt.timedelta(days=d)).replace(hour=0, minute=0, second=0, microsecond=0)
            for h in hours:
                created = day + dt.timedelta(hours=h, minutes=TYPICAL_LAG[(wf, h)])
                if created <= now:
                    done = created + dt.timedelta(minutes=TYPICAL_DUR[wf]) <= now
                    out[wf].append({"createdAt": created.strftime("%Y-%m-%dT%H:%M:%SZ"), "event": "schedule",
                                    "status": "completed" if done else "in_progress"})
        out[wf].sort(key=lambda r: r["createdAt"], reverse=True)
    return out


def test_on_a_typical_day_the_runner_still_gets_a_window_it_will_start_in():
    """Replaces the coupling to the removed FREE_BLOCK_MIN. run_local_heavy.ps1 starts only when the gate's
    --until-block answer minus $marginMin reaches $WantBlockMin (before $MaxHours). On a day at the measured
    medians the only such window is after the 18:00Z run (23:44Z -> 03:00Z); raising either PowerShell default
    past it means the desktop waits for the $MaxHours escape every day, and this test says so."""
    src = pathlib.Path(RUNNER_PS1).read_text(encoding="utf-8", errors="replace")
    want = re.search(r"\$WantBlockMin\s*=\s*(\d+)", src)
    margin = re.search(r"\$marginMin\s*=\s*(\d+)", src)
    assert want and margin, f"cannot read the runner's thresholds from {RUNNER_PS1}; this test measures nothing"
    best, t = 0, at("2026-10-14T00:00:00")
    while t < at("2026-10-15T00:00:00"):
        hist = _typical_history(t)
        if not g.decide(hist, t, ACTIVE)[0]:
            best = max(best, g.minutes_until_block(hist, t, ACTIVE))
        t += dt.timedelta(minutes=5)
    usable = best - int(margin.group(1))
    assert usable >= int(want.group(1)), (
        f"on a typical day the best window gives {usable} usable min; the runner wants {want.group(1)}")


def test_an_unstarted_cron_from_yesterday_still_blocks_after_midnight():
    cron = at("2026-10-14T18:00:00")
    now = at("2026-10-15T01:00:00")              # 420 min late: the old ceiling had released it at 23:00Z
    runs = _all_started_but(now, {("updater-daily.yml", cron)})
    assert cleared_by_the_old_ceiling(runs, now), "control: the old rule said CLEAR here"
    blocked, reason = g.decide(runs, now, ACTIVE)
    assert blocked and "updater-daily.yml 18:00Z cron has not started yet" in reason, reason


def test_a_pending_cron_past_the_warning_threshold_is_flagged_not_hidden():
    cron = at("2026-09-15T15:00:00")
    now = cron + g.WARN_PENDING_AFTER + dt.timedelta(minutes=5)
    runs = history_as_of(at("2026-09-15T12:00:00"))
    runs["updater-daily.yml"].insert(0, {"createdAt": "2026-09-15T20:00:00Z", "event": "schedule", "status": "completed"})
    blocked, reason, notes = g.assess(runs, now, ACTIVE)
    assert blocked and "updater-heavy.yml 15:00Z cron has not started yet" in reason, reason
    assert any("may have skipped it" in n for n in notes), notes


def test_a_manual_dispatch_does_not_count_as_the_scheduled_run():
    now = at("2026-09-15T10:38:56")
    runs = history_as_of(now)
    runs["updater-daily.yml"].insert(0, {"createdAt": "2026-09-15T09:00:00Z", "event": "workflow_dispatch",
                                         "status": "completed"})
    assert g.decide(runs, now, ACTIVE)[0]


def test_a_disabled_workflow_is_not_waited_for():
    now = at("2026-09-15T15:40:00")      # heavy 15:00Z pending, but the workflow is disabled
    states = {"updater-daily.yml": "active", "updater-heavy.yml": "disabled_manually"}
    blocked, reason, notes = g.assess(history_as_of(now), now, states)
    assert not blocked, reason
    assert any("disabled_manually" in n for n in notes), notes


def test_a_workflow_missing_from_githubs_list_is_still_waited_for():
    now = at("2026-09-15T15:40:00")
    blocked, reason, notes = g.assess(history_as_of(now), now, {"updater-daily.yml": "active"})
    assert blocked and "updater-heavy.yml 15:00Z" in reason, reason
    assert any("not in GitHub's workflow list" in n for n in notes), notes


def test_without_workflow_states_every_writer_is_waited_for():
    now = at("2026-09-15T15:40:00")
    blocked, reason, notes = g.assess(history_as_of(now), now, None)
    assert blocked and "updater-heavy.yml 15:00Z" in reason, reason


@pytest.mark.parametrize("status", ["queued", "waiting", "pending", "requested", "in_progress"])
def test_an_unfinished_run_blocks_however_old_and_an_old_one_warns(status):
    now = at("2026-09-14T23:30:00")
    runs = history_as_of(now)
    runs["updater-daily.yml"].insert(0, {"createdAt": "2026-09-13T20:00:00Z", "event": "workflow_dispatch",
                                         "status": status})
    blocked, reason, notes = g.assess(runs, now, ACTIVE)
    assert blocked and status in reason, reason
    assert any("more than a day" in n for n in notes), notes


def test_main_exit_codes_notes_and_unknown_is_never_clear(capsys):
    now = at("2026-09-15T10:38:56")
    runs = history_as_of(now)
    assert g.main(fetch=lambda wf: runs[wf], fetch_states=lambda: ACTIVE, now=now) == g.EXIT_BLOCKED
    later = at("2026-09-14T23:30:00")
    assert g.main(fetch=lambda wf: history_as_of(later)[wf], fetch_states=lambda: ACTIVE, now=later) == g.EXIT_CLEAR

    def broken(*_a):
        raise RuntimeError("gh: not logged in")
    assert g.main(fetch=broken, fetch_states=lambda: ACTIVE, now=now) == g.EXIT_UNKNOWN
    assert g.main(fetch=lambda wf: runs[wf], fetch_states=broken, now=now) == g.EXIT_UNKNOWN
    assert g.main(fetch=lambda wf: [{"event": "schedule"}], fetch_states=lambda: ACTIVE, now=now) == g.EXIT_UNKNOWN
    out = capsys.readouterr().out
    assert "UNKNOWN" in out and "CLEAR: " in out and "BLOCKED: " in out


def test_the_writer_list_matches_the_workflows():
    for wf, hours in g.WRITERS.items():
        body = open(os.path.join(ROOT, ".github", "workflows", wf), encoding="utf-8").read()
        crons = sorted(int(m.group(1)) for m in re.finditer(r"cron:\s*'0 (\d+) \* \* \*'", body))
        assert crons == sorted(hours), (wf, crons)
        assert "--pull-state" in body and "--push-state" in body, wf


def test_every_workflow_that_pushes_state_is_in_the_writer_list():
    wdir = os.path.join(ROOT, ".github", "workflows")
    pushers = sorted(f for f in os.listdir(wdir)
                     if f.endswith((".yml", ".yaml")) and "--push-state" in open(os.path.join(wdir, f), encoding="utf-8").read())
    assert pushers == sorted(g.WRITERS), pushers


def test_every_stderr_redirect_in_the_runner_is_outside_Stop():
    """2>&1 on a native command under $ErrorActionPreference = 'Stop' terminates Windows PowerShell 5.1
    as soon as the child writes to stderr (measured 2026-09-15), so every redirect runs under Continue."""
    body = open(os.path.join(ROOT, "tools", "run_local_heavy.ps1"), encoding="utf-8", errors="replace").read()
    code_lines = [(i, ln) for i, ln in enumerate(body.splitlines()) if "2>&1" in ln and not ln.lstrip().startswith("#")]
    assert len(code_lines) >= 2, "expected the lister and gate redirects at least"
    lines = body.splitlines()
    for i, ln in code_lines:
        before = "\n".join(lines[max(0, i - 8):i])
        after = "\n".join(lines[i:i + 8])
        assert "$ErrorActionPreference = 'Continue'" in before, (i + 1, ln.strip())
        assert "$ErrorActionPreference = $prevEap" in after, (i + 1, ln.strip())


POWERSHELL = shutil.which("powershell") or shutil.which("pwsh")


@pytest.mark.skipif(POWERSHELL is None, reason="no PowerShell on this host")
def test_stderr_from_a_native_call_under_Continue_reaches_the_exit_code_check(tmp_path):
    script = tmp_path / "probe.ps1"
    script.write_text(
        "$ErrorActionPreference = 'Stop'\n"
        "$prevEap = $ErrorActionPreference; $ErrorActionPreference = 'Continue'\n"
        "try { $o = (& '" + sys.executable + "' -c \"import sys; sys.stderr.write('boom'); sys.exit(7)\" 2>&1 "
        "| Out-String).Trim(); $rc = $LASTEXITCODE } finally { $ErrorActionPreference = $prevEap }\n"
        "'rc=' + $rc\n", encoding="utf-8")
    r = subprocess.run([POWERSHELL, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(script)],
                       capture_output=True, text=True, timeout=120)
    assert "rc=7" in r.stdout, (r.stdout, r.stderr)


def test_the_runner_calls_the_gate_instead_of_the_old_single_workflow_check():
    body = open(os.path.join(ROOT, "tools", "run_local_heavy.ps1"), encoding="utf-8", errors="replace").read()
    assert "ci_writer_gate.py" in body
    assert "--workflow=updater-daily.yml --limit 5" not in body


def test_minutes_until_block_is_zero_while_the_gate_blocks():
    now = at("2026-09-15T15:40:00")          # a pending heavy cron, already asserted blocked above
    runs = history_as_of(now)
    assert g.decide(runs, now, ACTIVE)[0]
    assert g.minutes_until_block(runs, now, ACTIVE) == 0


def test_minutes_until_block_counts_forward_to_the_next_block_and_agrees_with_decide():
    """The answer must be the same rule, not a second copy of the schedule: step to it and check both sides."""
    now = at("2026-09-15T00:30:00")
    runs = history_as_of(now)
    assert not g.decide(runs, now, ACTIVE)[0], "fixture: this instant should be clear"
    mins = g.minutes_until_block(runs, now, ACTIVE)
    assert 0 < mins < 12 * 60, mins
    just_before = now + dt.timedelta(minutes=mins - 5)
    at_block = now + dt.timedelta(minutes=mins)
    assert not g.decide(runs, just_before, ACTIVE)[0], "gate blocks before the reported time"
    assert g.decide(runs, at_block, ACTIVE)[0], "gate does not block at the reported time"


def test_minutes_until_block_never_exceeds_its_lookahead():
    now = at("2026-09-15T00:30:00")
    runs = history_as_of(now)
    look = dt.timedelta(hours=2)
    assert g.minutes_until_block(runs, now, ACTIVE, lookahead=look) <= int(look.total_seconds() // 60)


def test_the_runner_budget_clamp_would_get_a_usable_answer_at_the_start_of_a_clear_block():
    """The point of the whole change: the runner must be told how long it really has, not what a fixed list says.

    run_local_heavy.ps1 aborts below 20 usable minutes after subtracting a 25 min margin, so any answer it acts on
    has to clear 45 minutes to be worth starting.
    """
    now = at("2026-09-15T00:30:00")
    mins = g.minutes_until_block(history_as_of(now), now, ACTIVE)
    assert mins >= 45, f"only {mins} min before the gate blocks; the runner would abort rather than start"


def _flood(now, n=60):
    """A run list dominated by manual dispatches, as `gh run list` without --event returns it."""
    return {wf: [{"createdAt": (now - dt.timedelta(minutes=7 * i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                  "event": "workflow_dispatch", "status": "completed"} for i in range(n)]
            for wf in g.WRITERS}


def _sched_all_started(now, lag_min=30, days=6):
    """A schedule-only history in which every recent cron started `lag_min` after its scheduled time."""
    out = {}
    for wf, hours in g.WRITERS.items():
        rows = []
        for d in range(days):
            day = (now - dt.timedelta(days=d)).replace(hour=0, minute=0, second=0, microsecond=0)
            for h in hours:
                created = day + dt.timedelta(hours=h, minutes=lag_min)
                if created <= now:
                    rows.append({"createdAt": created.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                 "event": "schedule", "status": "completed"})
        rows.sort(key=lambda r: r["createdAt"], reverse=True)
        out[wf] = rows
    return out


# 06:45Z. Every cron here started 30 min late, with the 06:00Z run created at 06:30Z: the schedule-only list
# says every cron of the last 12 h HAS started (CLEAR), while a dispatch-flooded list has no scheduled rows,
# so every one of them reads as never started.
SPLIT_NOW = "2026-09-12T06:45:00"


def test_the_cron_check_reads_the_schedule_only_list_not_the_dispatch_flooded_one():
    """A burst of manual dispatches must not make a started cron read as never-started.

    This repo holds 114 workflow_dispatch runs against 86 scheduled ones for updater-daily, and the unfiltered
    fetch is capped at 50 rows, so the scheduled rows really can all be evicted.
    """
    now = at(SPLIT_NOW)
    sched = _sched_all_started(now)
    flooded = _flood(now)

    blocked, reason, _notes = g.assess(flooded, now, ACTIVE, sched)
    assert not blocked, "every cron had started; the gate should be clear: " + reason

    # NEGATIVE CONTROL: without the schedule-only list the same inputs must produce the phantom pending cron.
    blocked_bad, reason_bad, _n = g.assess(flooded, now, ACTIVE)
    assert blocked_bad and "cron has not started yet" in reason_bad, (
        "control did not reproduce the bug the split prevents, so this test proves nothing: " + reason_bad)


def test_the_in_flight_check_still_sees_a_manual_dispatch():
    """The other half: the split must not blind the in-flight check to a dispatch, which holds the store too."""
    now = at(SPLIT_NOW)
    runs = {wf: [] for wf in g.WRITERS}
    runs["updater-daily.yml"] = [{"createdAt": "2026-09-12T06:40:00Z", "event": "workflow_dispatch",
                                  "status": "in_progress"}]
    blocked, reason, _notes = g.assess(runs, now, ACTIVE, _sched_all_started(now))
    assert blocked and "is in_progress" in reason, reason


def test_minutes_until_block_uses_the_schedule_only_list_too():
    """The budget clamp must answer from the same inputs as the gate, or the two can disagree."""
    now = at(SPLIT_NOW)
    with_split = g.minutes_until_block(_flood(now), now, ACTIVE, sched=_sched_all_started(now))
    without = g.minutes_until_block(_flood(now), now, ACTIVE)
    assert with_split > 0, with_split
    assert without == 0, "control: the flooded list should block immediately, so the clamp should read 0"


def test_main_threads_the_schedule_only_fetch_through():
    """main() must not collapse the two fetches into one when a caller supplies both."""
    now = at(SPLIT_NOW)
    sched = _sched_all_started(now)
    flooded = _flood(now)
    rc = g.main(fetch=lambda wf: flooded[wf], fetch_states=lambda: ACTIVE, now=now,
                fetch_sched=lambda wf: sched[wf])
    assert rc == g.EXIT_CLEAR, rc
    rc_collapsed = g.main(fetch=lambda wf: flooded[wf], fetch_states=lambda: ACTIVE, now=now)
    assert rc_collapsed == g.EXIT_BLOCKED, (
        "control: without the schedule-only fetch the flooded list must block on a phantom pending cron")


def test_the_runner_escape_is_longer_than_its_cadence_floor():
    """$MaxHours must exceed $MinHours, or the wait-for-a-better-window rule can never hold at all."""
    src = pathlib.Path(RUNNER_PS1).read_text(encoding="utf-8", errors="replace")
    mx = re.search(r"\$MaxHours\s*=\s*(\d+)", src)
    mn = re.search(r"\$MinHours\s*=\s*(\d+)", src)
    assert mx and mn, "cannot read the runner's cadence parameters"
    assert int(mx.group(1)) > int(mn.group(1)), (
        f"$MaxHours {mx.group(1)} must be greater than $MinHours {mn.group(1)}")


# --- AN EMPTY SCHEDULED-RUN LISTING (ledger R1660) ------------------------------------------------------------
# `gh run list --event schedule` can answer [] for a writer that has scheduled runs. Before the fix, that one
# empty read made every cron of the last 12 h read as not started.

class _Seq:
    """A fake --event schedule fetch: answers from `answers[wf]` in order (the last answer repeats), counting calls.
    An answer that is an Exception instance is raised."""
    def __init__(self, answers):
        self.answers, self.calls = answers, {wf: 0 for wf in answers}

    def __call__(self, wf):
        seq = self.answers[wf]
        a = seq[min(self.calls[wf], len(seq) - 1)]
        self.calls[wf] += 1
        if isinstance(a, Exception):
            raise a
        return list(a)


def _with_sched_rows(now, keep=50):
    """An unfiltered listing that DOES carry the scheduled runs (newest first, capped like RUN_LIMIT)."""
    sched, flood = _sched_all_started(now), _flood(now, n=10)
    return {wf: sorted(sched[wf] + flood[wf], key=lambda r: r["createdAt"], reverse=True)[:keep] for wf in g.WRITERS}


def test_an_empty_then_full_scheduled_listing_is_read_again_not_believed(capsys):
    now = at(SPLIT_NOW)
    sched, unfiltered = _sched_all_started(now), _flood(now)       # the unfiltered list has NO scheduled row
    fs, sleeps = _Seq({wf: [[], sched[wf]] for wf in g.WRITERS}), []
    rc = g.main(fetch=lambda wf: unfiltered[wf], fetch_states=lambda: ACTIVE, now=now, fetch_sched=fs,
                sleep=sleeps.append)
    out = capsys.readouterr().out
    assert rc == g.EXIT_CLEAR, out
    assert fs.calls == {wf: 2 for wf in g.WRITERS}, fs.calls
    assert sleeps == [g.SCHED_RETRY_PAUSE_S] * len(g.WRITERS), sleeps
    assert "came back empty, then held" in out, out

    # NEGATIVE CONTROL: the same instant with a listing that stays empty must still block (fail closed).
    fs = _Seq({wf: [[]] for wf in g.WRITERS})
    rc = g.main(fetch=lambda wf: unfiltered[wf], fetch_states=lambda: ACTIVE, now=now, fetch_sched=fs,
                sleep=lambda s: None)
    out = capsys.readouterr().out
    assert rc == g.EXIT_BLOCKED and "cron has not started yet" in out, out


def test_an_empty_filtered_listing_takes_the_scheduled_rows_of_the_unfiltered_one(capsys):
    now = at(SPLIT_NOW)
    unfiltered = _with_sched_rows(now)
    assert all(any(r["event"] == "schedule" for r in unfiltered[wf]) for wf in g.WRITERS), "fixture lost its rows"
    fs = _Seq({wf: [[]] for wf in g.WRITERS})                      # empty on every read
    rc = g.main(fetch=lambda wf: unfiltered[wf], fetch_states=lambda: ACTIVE, now=now, fetch_sched=fs,
                sleep=lambda s: None)
    out = capsys.readouterr().out
    assert rc == g.EXIT_CLEAR, out
    assert "scheduled run(s) in the unfiltered listing instead" in out, out
    assert fs.calls == {wf: 1 + g.SCHED_EMPTY_RETRIES for wf in g.WRITERS}, fs.calls

    # NEGATIVE CONTROL: the same rows as manual dispatches are not scheduled runs and must not rescue a cron.
    as_dispatch = {wf: [dict(r, event="workflow_dispatch") for r in rows] for wf, rows in unfiltered.items()}
    rc = g.main(fetch=lambda wf: as_dispatch[wf], fetch_states=lambda: ACTIVE, now=now,
                fetch_sched=_Seq({wf: [[]] for wf in g.WRITERS}), sleep=lambda s: None)
    out = capsys.readouterr().out
    assert rc == g.EXIT_BLOCKED, out
    assert "holds no scheduled run either" in out and "in the unfiltered listing instead" not in out, out


def test_a_truly_empty_scheduled_history_still_blocks(capsys):
    """No scheduled run anywhere: the cron check must keep failing closed, never CLEAR and never skip the writer."""
    now = at("2026-09-14T23:30:00")
    hist = history_as_of(now)
    # control: with a readable history this instant is CLEAR, so a BLOCK below is caused by the empty listing
    assert g.main(fetch=lambda wf: hist[wf], fetch_states=lambda: ACTIVE, now=now) == g.EXIT_CLEAR
    capsys.readouterr()
    for unfiltered in ({wf: [] for wf in g.WRITERS}, _flood(now)):
        rc = g.main(fetch=lambda wf: unfiltered[wf], fetch_states=lambda: ACTIVE, now=now,
                    fetch_sched=_Seq({wf: [[]] for wf in g.WRITERS}), sleep=lambda s: None)
        out = capsys.readouterr().out
        assert rc == g.EXIT_BLOCKED and "BLOCKED: " in out and "cron has not started yet" in out, out
        assert "holds no scheduled run either" in out, out
        # the budget clamp must read 0, and print ONLY the number (the runner parses the whole output as an int)
        rc = g.main(fetch=lambda wf: unfiltered[wf], fetch_states=lambda: ACTIVE, now=now, until_block=True,
                    fetch_sched=_Seq({wf: [[]] for wf in g.WRITERS}), sleep=lambda s: None)
        assert rc == g.EXIT_CLEAR and capsys.readouterr().out.strip() == "0"


def test_until_block_prints_only_the_number_after_an_empty_read(capsys):
    now = at(SPLIT_NOW)
    sched, unfiltered = _sched_all_started(now), _flood(now)
    rc = g.main(fetch=lambda wf: unfiltered[wf], fetch_states=lambda: ACTIVE, now=now, until_block=True,
                fetch_sched=_Seq({wf: [[], sched[wf]] for wf in g.WRITERS}), sleep=lambda s: None)
    out = capsys.readouterr().out.strip()
    assert rc == g.EXIT_CLEAR and re.fullmatch(r"\d+", out) and int(out) > 0, out


def test_a_read_that_raises_on_the_retry_is_unknown_never_clear(capsys):
    now = at(SPLIT_NOW)
    sched, unfiltered = _sched_all_started(now), _with_sched_rows(now)
    for until_block in (False, True):
        fs = _Seq({wf: [[], RuntimeError("gh: HTTP 502")] for wf in g.WRITERS})
        rc = g.main(fetch=lambda wf: unfiltered[wf], fetch_states=lambda: ACTIVE, now=now, until_block=until_block,
                    fetch_sched=fs, sleep=lambda s: None)
        out = capsys.readouterr().out
        assert rc == g.EXIT_UNKNOWN and out.startswith("UNKNOWN") and "502" in out, out


def test_a_normal_read_is_one_call_no_pause_and_no_note():
    now = at(SPLIT_NOW)
    sched, unfiltered = _sched_all_started(now), _with_sched_rows(now)
    sleeps = []
    for wf in g.WRITERS:
        fs = _Seq({wf: [sched[wf]]})
        rows, notes = g.read_sched(wf, fs, unfiltered[wf], sleep=sleeps.append)
        assert fs.calls[wf] == 1 and notes == [] and rows == sched[wf], (fs.calls, notes)
    assert sleeps == []


def test_the_retry_is_bounded_and_short():
    assert 1 <= g.SCHED_EMPTY_RETRIES <= 3 and 0 < g.SCHED_RETRY_PAUSE_S <= 5
    wf, sleeps = "updater-heavy.yml", []
    fs = _Seq({wf: [[]]})
    rows, notes = g.read_sched(wf, fs, [], sleep=sleeps.append)
    assert rows == [] and fs.calls[wf] == 1 + g.SCHED_EMPTY_RETRIES and len(sleeps) == g.SCHED_EMPTY_RETRIES
    assert any(f"empty on {1 + g.SCHED_EMPTY_RETRIES} read(s)" in n for n in notes), notes


def test_a_listing_that_lacks_the_newest_scheduled_run_gains_it_from_the_unfiltered_one():
    """A non-empty but short filtered answer: the 06:30Z daily run is in the unfiltered list only."""
    now = at(SPLIT_NOW)
    sched, unfiltered = _sched_all_started(now), _with_sched_rows(now)
    short = {wf: list(rows) for wf, rows in sched.items()}
    assert short["updater-daily.yml"][0]["createdAt"] == "2026-09-12T06:30:00Z"
    short["updater-daily.yml"] = short["updater-daily.yml"][1:]
    blocked, reason, _n = g.assess(unfiltered, now, ACTIVE, short)
    assert blocked and "06:00Z cron has not started yet" in reason, "control: the short listing alone blocks"
    rows, notes = g.read_sched("updater-daily.yml", lambda wf: short[wf], unfiltered["updater-daily.yml"])
    assert rows[0]["createdAt"] == "2026-09-12T06:30:00Z" and any("lacked 1 scheduled run" in n for n in notes)
    assert len({r["createdAt"] for r in rows}) == len(rows), "a run must not be added twice"
    fixed = dict(short, **{"updater-daily.yml": rows})
    assert not g.assess(unfiltered, now, ACTIVE, fixed)[0]


# --- AN EMPTY UNFILTERED LISTING (review AR-375) ----------------------------------------------------------------
# main() read the unfiltered listing ONCE and the in-flight check read only it. One empty answer for a writer whose
# scheduled run was in progress hid the run, and the same run credited its cron as started from the scheduled
# listing: CLEAR, exit 0, during a cloud run. Fixed twice: the empty answer is read again and a still-empty one
# blocks (`unread`), and the in-flight check reads the scheduled listing too. Each fix is tested on its own below.

AR375_NOW = "2026-10-22T21:30:00"


def _iso(d):
    return d.strftime("%Y-%m-%dT%H:%M:%SZ")


def _ar375(status="in_progress"):
    """The probe of AR-375 (F:/work/ar375/probe_gate_failopen.py): a scheduled daily run created 20 min ago with
    `status`, and every cron of the last 12 h credited. With status 'completed' it is a truly quiet system."""
    now = at(AR375_NOW)
    run = {"createdAt": _iso(now - dt.timedelta(minutes=20)), "event": "schedule", "status": status}
    def done(h):
        return {"createdAt": _iso(now - dt.timedelta(hours=h)), "event": "schedule", "status": "completed"}
    return now, {"updater-daily.yml": [run, done(12), done(24)], "updater-heavy.yml": [done(3), done(15)]}


def _main(now, unfiltered, sched, until_block=False, states=ACTIVE, sleep=lambda s: None):
    return g.main(fetch=unfiltered, fetch_states=lambda: states, now=now, until_block=until_block,
                  fetch_sched=sched, sleep=sleep)


@pytest.mark.parametrize("daily_answers", [[[]], "empty-then-full"], ids=["empty-on-every-read", "empty-once"])
def test_AR375_probe_an_empty_unfiltered_answer_never_clears_a_run_in_progress(capsys, daily_answers):
    now, full = _ar375()
    answers = {wf: [full[wf]] for wf in g.WRITERS}
    answers["updater-daily.yml"] = [[], full["updater-daily.yml"]] if daily_answers == "empty-then-full" else [[]]
    rc = _main(now, _Seq(answers), lambda wf: list(full[wf]))
    out = capsys.readouterr().out
    assert rc in (g.EXIT_BLOCKED, g.EXIT_UNKNOWN) and "CLEAR" not in out, out
    assert rc == g.EXIT_BLOCKED and "is in_progress" in out, out
    # --until-block reads the same listings: it must say "no free time", as exactly one integer
    rc = _main(now, _Seq(answers), lambda wf: list(full[wf]), until_block=True)
    out = capsys.readouterr().out
    assert rc == g.EXIT_CLEAR and out.strip() == "0", out


def test_AR375_positive_control_and_a_truly_quiet_system(capsys):
    now, full = _ar375()
    # positive control: the unfiltered listing answered, so the plain in-flight check blocks on it
    rc = _main(now, lambda wf: list(full[wf]), lambda wf: list(full[wf]))
    out = capsys.readouterr().out
    assert rc == g.EXIT_BLOCKED and "is in_progress" in out and "scheduled-run listing" not in out, out
    # a truly quiet system: the same history with the run finished stays CLEAR, and --until-block counts forward
    now, quiet = _ar375("completed")
    rc = _main(now, lambda wf: list(quiet[wf]), lambda wf: list(quiet[wf]))
    out = capsys.readouterr().out
    assert rc == g.EXIT_CLEAR and out.strip() == "CLEAR: no cloud state writer in flight or pending", out
    rc = _main(now, lambda wf: list(quiet[wf]), lambda wf: list(quiet[wf]), until_block=True)
    out = capsys.readouterr().out.strip()
    assert rc == g.EXIT_CLEAR and re.fullmatch(r"\d+", out) and int(out) > 0, out


def test_an_empty_unfiltered_answer_is_read_again_and_a_quiet_answer_then_clears(capsys):
    """Fix 1 on its own: the re-read. Without it the empty first answer would block (unread) for nothing."""
    now, quiet = _ar375("completed")
    fu, sleeps = _Seq({wf: [[], quiet[wf]] for wf in g.WRITERS}), []
    rc = _main(now, fu, lambda wf: list(quiet[wf]), sleep=sleeps.append)
    out = capsys.readouterr().out
    assert rc == g.EXIT_CLEAR, out
    assert fu.calls == {wf: 2 for wf in g.WRITERS}, fu.calls
    assert sleeps == [g.SCHED_RETRY_PAUSE_S] * len(g.WRITERS), sleeps
    assert "unfiltered run listing came back empty, then held" in out, out
    # NEGATIVE CONTROL: a listing that stays empty is not "nothing in flight"
    rc = _main(now, _Seq({wf: [[]] for wf in g.WRITERS}), lambda wf: list(quiet[wf]))
    out = capsys.readouterr().out
    assert rc == g.EXIT_BLOCKED and "cannot be ruled out" in out, out


def test_the_re_read_finds_a_manual_dispatch_the_scheduled_listing_cannot_show(capsys):
    """A dispatch is in no scheduled listing, so only the re-read of the unfiltered one can see it."""
    now, quiet = _ar375("completed")
    dispatch = {"createdAt": _iso(now - dt.timedelta(minutes=5)), "event": "workflow_dispatch",
                "status": "in_progress"}
    answers = {wf: [quiet[wf]] for wf in g.WRITERS}
    answers["updater-heavy.yml"] = [[], [dispatch] + quiet["updater-heavy.yml"]]
    rc = _main(now, _Seq(answers), lambda wf: list(quiet[wf]))
    out = capsys.readouterr().out
    assert rc == g.EXIT_BLOCKED, out
    assert f"BLOCKED: updater-heavy.yml run created {dispatch['createdAt']} is in_progress" in out, out


@pytest.mark.parametrize("status", ["queued", "waiting", "pending", "requested", "in_progress"])
def test_an_unfinished_run_in_the_scheduled_listing_blocks_on_its_own(status):
    """Fix 2 on its own: a non-empty unfiltered listing that does not show the run unfinished (all completed here),
    while the scheduled listing does. Before AR-375 this read CLEAR: the run credited its cron and nothing else
    looked at its status."""
    now, quiet = _ar375("completed")
    _now, sched = _ar375(status)
    blocked, reason, _n = g.assess(quiet, now, ACTIVE, sched)
    assert blocked and f"is {status}" in reason and "scheduled-run listing" in reason, reason
    # control: the same scheduled listing with the run finished is clear
    assert not g.assess(quiet, now, ACTIVE, quiet)[0]
    assert g.minutes_until_block(quiet, now, ACTIVE, sched=sched) == 0


@pytest.mark.parametrize("status", ["queued", "waiting", "pending", "requested", "in_progress"])
def test_an_unfinished_scheduled_listing_row_older_than_a_day_still_blocks_and_warns(status):
    """Review AR-376 (surviving mutant S4): age never excuses an unfinished row - 30 h old here, past
    WARN_UNFINISHED_AFTER, only in the scheduled listing. It blocks, and it warns that it may be stuck."""
    now, quiet = _ar375("completed")
    old = {"createdAt": _iso(now - dt.timedelta(hours=30)), "event": "schedule", "status": status}
    assert now - _parse_iso(old["createdAt"]) > g.WARN_UNFINISHED_AFTER
    sched = {wf: list(rows) for wf, rows in quiet.items()}
    sched["updater-daily.yml"] = sorted(sched["updater-daily.yml"] + [old], key=lambda r: r["createdAt"],
                                        reverse=True)
    blocked, reason, notes = g.assess(quiet, now, ACTIVE, sched)
    assert blocked and f"is {status}" in reason and old["createdAt"] in reason, reason
    assert any("more than a day" in n for n in notes), notes
    assert g.minutes_until_block(quiet, now, ACTIVE, sched=sched) == 0


def _parse_iso(s):
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))


def test_a_still_empty_unfiltered_listing_blocks_on_its_own(capsys):
    """The `unread` rule on its own: no unfinished run anywhere, every cron credited, but one writer's unfiltered
    listing read empty three times. Before AR-375 that writer's in-flight check had nothing to look at: CLEAR."""
    now, quiet = _ar375("completed")
    answers = {wf: [quiet[wf]] for wf in g.WRITERS}
    answers["updater-heavy.yml"] = [[]]
    fu = _Seq(answers)
    rc = _main(now, fu, lambda wf: list(quiet[wf]))
    out = capsys.readouterr().out
    assert rc == g.EXIT_BLOCKED, out
    assert fu.calls["updater-heavy.yml"] == 1 + g.SCHED_EMPTY_RETRIES and fu.calls["updater-daily.yml"] == 1
    assert "BLOCKED: updater-heavy.yml run listing came back empty on every read" in out, out
    assert "WARNING: updater-heavy.yml unfiltered run listing came back empty on 3 read(s)" in out, out
    rc = _main(now, _Seq(answers), lambda wf: list(quiet[wf]), until_block=True)
    out = capsys.readouterr().out
    assert rc == g.EXIT_CLEAR and out.strip() == "0", out
    runs = dict(quiet, **{"updater-heavy.yml": []})
    assert g.minutes_until_block(runs, now, ACTIVE, sched=quiet, unread={"updater-heavy.yml"}) == 0
    assert g.minutes_until_block(runs, now, ACTIVE, sched=quiet) > 0, "control: without `unread` it is clear"


def test_an_unread_writer_blocks_even_when_its_workflow_is_disabled(capsys):
    """The in-flight check ignores the workflow state (AR-366): a disabled workflow can still hold a run."""
    now, quiet = _ar375("completed")
    states = dict(ACTIVE, **{"updater-heavy.yml": "disabled_manually"})
    rc = _main(now, lambda wf: list(quiet[wf]), lambda wf: list(quiet[wf]), states=states)
    out = capsys.readouterr().out
    assert rc == g.EXIT_CLEAR and "its crons are not waited for" in out, "control: " + out
    answers = {wf: [quiet[wf]] for wf in g.WRITERS}
    answers["updater-heavy.yml"] = [[]]
    rc = _main(now, _Seq(answers), lambda wf: list(quiet[wf]), states=states)
    out = capsys.readouterr().out
    assert rc == g.EXIT_BLOCKED and "came back empty on every read" in out, out


def test_read_unfiltered_is_one_call_normally_and_bounded_when_empty():
    now, quiet = _ar375("completed")
    wf, sleeps = "updater-daily.yml", []
    fu = _Seq({wf: [quiet[wf]]})
    assert g.read_unfiltered(wf, fu, sleep=sleeps.append) == (quiet[wf], []) and fu.calls[wf] == 1 and sleeps == []
    fu = _Seq({wf: [[]]})
    rows, notes = g.read_unfiltered(wf, fu, sleep=sleeps.append)
    assert rows == [] and fu.calls[wf] == 1 + g.SCHED_EMPTY_RETRIES
    assert sleeps == [g.SCHED_RETRY_PAUSE_S] * g.SCHED_EMPTY_RETRIES
    assert any(f"empty on {1 + g.SCHED_EMPTY_RETRIES} read(s)" in n for n in notes), notes


def test_an_unfiltered_read_that_raises_on_the_retry_is_unknown_never_clear(capsys):
    now, quiet = _ar375("completed")
    for until_block in (False, True):
        fu = _Seq({wf: [[], RuntimeError("gh: HTTP 502")] for wf in g.WRITERS})
        rc = _main(now, fu, lambda wf: list(quiet[wf]), until_block=until_block)
        out = capsys.readouterr().out
        assert rc == g.EXIT_UNKNOWN and out.startswith("UNKNOWN") and "502" in out, out


def test_the_worst_case_time_note_matches_the_constants():
    """The note in ci_writer_gate.py states a ceiling for one gate call; it must follow the re-read constants.
    5 single reads, plus SCHED_EMPTY_RETRIES re-reads per writer of each of the TWO listings."""
    src = open(os.path.join(ROOT, "tools", "ci_writer_gate.py"), encoding="utf-8").read()
    m = re.search(r"#\s+now\s+\+ 2 retries per writer of the unfiltered listing too = .*= (\d+) s", src)
    assert m, "the worst-case line for the unfiltered re-reads is missing"
    timeout = 60
    expect = 5 * timeout + 2 * len(g.WRITERS) * g.SCHED_EMPTY_RETRIES * (timeout + g.SCHED_RETRY_PAUSE_S)
    assert int(m.group(1)) == expect, (m.group(1), expect)
