"""tools/ci_writer_gate.py: the desktop heavy pass must not overlap a cloud state writer - including a
scheduled run GitHub has not started yet, which is what collided on 2026-09-08, 09-13 and 09-15.

The fixtures are the scheduled runs GitHub actually created (gh run list, measured 2026-09-15) and the
start times of the desktop passes whose pull/push window overlapped one (logs/local_heavy_*.log).
"""
import datetime as dt
import os
import re
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools"))

import ci_writer_gate as g  # noqa: E402

DAILY_CREATED = [
    "2026-09-15T11:18", "2026-09-14T21:29", "2026-09-14T12:06", "2026-09-13T20:20", "2026-09-13T11:16",
    "2026-09-12T20:11", "2026-09-12T10:16", "2026-09-11T20:28", "2026-09-11T10:47", "2026-09-10T20:26",
    "2026-09-10T10:50", "2026-09-09T20:26", "2026-09-09T10:54", "2026-09-08T20:44", "2026-09-08T10:48",
    "2026-09-07T21:07", "2026-09-07T11:50",
]
HEAVY_CREATED = [
    "2026-09-15T08:19", "2026-09-14T19:38", "2026-09-14T08:29", "2026-09-13T17:44", "2026-09-13T07:55",
    "2026-09-12T17:28", "2026-09-12T07:37", "2026-09-11T17:57", "2026-09-11T07:42", "2026-09-10T17:55",
    "2026-09-10T07:48", "2026-09-09T18:03", "2026-09-09T07:47", "2026-09-08T18:08", "2026-09-08T07:45",
    "2026-09-07T18:53", "2026-09-07T07:49",
]


def at(s):
    return dt.datetime.fromisoformat(s).replace(tzinfo=dt.timezone.utc)


def history_as_of(now, status="completed"):
    """The scheduled runs GitHub had created by `now`, newest first, as `gh run list --json` shows them."""
    def rows(created):
        return [{"createdAt": c + ":00Z", "event": "schedule", "status": status}
                for c in created if at(c) <= now]
    return {"updater-daily.yml": rows(DAILY_CREATED), "updater-heavy.yml": rows(HEAVY_CREATED)}


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
    blocked, reason = g.decide(runs, now)
    assert blocked, reason
    assert "updater-daily.yml 06:00Z cron has not started yet" in reason, reason


def test_a_run_in_flight_blocks():
    now = at("2026-09-15T12:00:00")
    runs = history_as_of(now)
    runs["updater-heavy.yml"][0] = dict(runs["updater-heavy.yml"][0], status="in_progress")
    blocked, reason = g.decide(runs, now)
    assert blocked and "in_progress" in reason, reason


def test_clear_after_the_evening_run_and_before_the_next_cron():
    now = at("2026-09-14T23:30:00")      # daily 18:00Z run created 21:29, heavy 15:00Z run created 19:38
    blocked, reason = g.decide(history_as_of(now), now)
    assert not blocked, reason


def test_a_pending_heavy_cron_blocks_after_the_morning_daily_run():
    now = at("2026-09-15T15:40:00")      # the 15:00Z heavy cron had no run yet
    blocked, reason = g.decide(history_as_of(now), now)
    assert blocked and "updater-heavy.yml 15:00Z cron has not started yet" in reason, reason


def test_a_manual_dispatch_does_not_count_as_the_scheduled_run():
    now = at("2026-09-15T10:38:56")
    runs = history_as_of(now)
    runs["updater-daily.yml"].insert(0, {"createdAt": "2026-09-15T09:00:00Z", "event": "workflow_dispatch",
                                         "status": "completed"})
    assert g.decide(runs, now)[0]


def test_a_cron_github_never_ran_stops_blocking_after_the_horizon():
    runs = {"updater-daily.yml": [{"createdAt": "2026-09-15T19:00:00Z", "event": "schedule", "status": "completed"},
                                  {"createdAt": "2026-09-15T11:18:00Z", "event": "schedule", "status": "completed"},
                                  {"createdAt": "2026-09-14T21:29:00Z", "event": "schedule", "status": "completed"}],
            "updater-heavy.yml": [{"createdAt": "2026-09-15T08:19:00Z", "event": "schedule", "status": "completed"}]}
    inside = at("2026-09-15T22:59:00")   # 7 h 59 min after the 15:00Z heavy cron, which never ran
    outside = at("2026-09-15T23:01:00")  # 8 h 01 min after it
    assert g.decide(runs, inside)[0]
    assert not g.decide(runs, outside)[0], g.decide(runs, outside)[1]


def test_main_exit_codes_and_unknown_is_never_clear(capsys):
    now = at("2026-09-15T10:38:56")
    runs = history_as_of(now)
    assert g.main(fetch=lambda wf: runs[wf], now=now) == g.EXIT_BLOCKED
    later = at("2026-09-14T23:30:00")
    assert g.main(fetch=lambda wf: history_as_of(later)[wf], now=later) == g.EXIT_CLEAR

    def broken(wf):
        raise RuntimeError("gh: not logged in")
    assert g.main(fetch=broken, now=now) == g.EXIT_UNKNOWN
    assert g.main(fetch=lambda wf: [{"event": "schedule"}], now=now) == g.EXIT_UNKNOWN   # malformed rows
    assert "UNKNOWN" in capsys.readouterr().out


def test_the_writer_list_matches_the_workflows():
    for wf, hours in g.WRITERS.items():
        body = open(os.path.join(ROOT, ".github", "workflows", wf), encoding="utf-8").read()
        crons = sorted(int(m.group(1)) for m in re.finditer(r"cron:\s*'0 (\d+) \* \* \*'", body))
        assert crons == sorted(hours), (wf, crons)
        assert "--pull-state" in body and "--push-state" in body, wf


def test_the_runner_calls_the_gate_instead_of_the_old_single_workflow_check():
    body = open(os.path.join(ROOT, "tools", "run_local_heavy.ps1"), encoding="utf-8", errors="replace").read()
    assert "ci_writer_gate.py" in body
    assert "--workflow=updater-daily.yml --limit 5" not in body
