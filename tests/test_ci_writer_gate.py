"""tools/ci_writer_gate.py: the desktop heavy pass must not overlap a cloud state writer - including a
scheduled run GitHub has not started yet, which is what collided on 2026-09-08, 09-13 and 09-15.

The fixtures are the scheduled runs GitHub actually created (gh run list, measured 2026-09-15) and the
start times of the desktop passes whose pull/push window overlapped one (logs/local_heavy_*.log).
"""
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
ACTIVE = {"updater-daily.yml": "active", "updater-heavy.yml": "active"}


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


def test_the_longest_measured_lag_is_still_inside_the_pending_horizon():
    # 710 min, the longest start lag among the 175 scheduled runs GitHub listed on 2026-09-15
    cron = at("2026-09-15T15:00:00")
    now = cron + dt.timedelta(minutes=710)
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


def test_a_run_unfinished_for_more_than_a_day_is_reported_not_obeyed():
    now = at("2026-09-14T23:30:00")
    runs = history_as_of(now)
    runs["updater-daily.yml"].insert(0, {"createdAt": "2026-09-13T20:00:00Z", "event": "workflow_dispatch",
                                         "status": "waiting"})
    blocked, reason, notes = g.assess(runs, now, ACTIVE)
    assert not blocked, reason
    assert any("more than a day" in n for n in notes), notes
    runs["updater-daily.yml"][0]["createdAt"] = "2026-09-14T23:00:00Z"   # a fresh one still blocks
    assert g.decide(runs, now, ACTIVE)[0]


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


def test_the_runner_relaxes_Stop_around_both_python_calls():
    """2>&1 on a native command under $ErrorActionPreference = 'Stop' terminates Windows PowerShell 5.1
    as soon as the child writes to stderr (measured 2026-09-15), so both calls run under Continue."""
    body = open(os.path.join(ROOT, "tools", "run_local_heavy.ps1"), encoding="utf-8", errors="replace").read()
    for call in ("& $pythonExe $lister 2>&1", "& $pythonExe $gate 2>&1"):
        i = body.index(call)
        assert "$ErrorActionPreference = 'Continue'" in body[max(0, i - 200):i], call
        assert "$ErrorActionPreference = $prevEap" in body[i:i + 200], call


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
                       capture_output=True, text=True)
    assert "rc=7" in r.stdout, (r.stdout, r.stderr)


def test_the_runner_calls_the_gate_instead_of_the_old_single_workflow_check():
    body = open(os.path.join(ROOT, "tools", "run_local_heavy.ps1"), encoding="utf-8", errors="replace").read()
    assert "ci_writer_gate.py" in body
    assert "--workflow=updater-daily.yml --limit 5" not in body
