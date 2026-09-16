"""tools/ci_writer_gate.py: the desktop heavy pass must not overlap a cloud state writer - including a
scheduled run GitHub has not started yet, which is what collided on 2026-09-08, 09-13 and 09-15.

The run lists below reach back to 2026-08-20 so that a decision taken at the
earliest collision has a real 14-day history to measure, rather than falling back because the fixture
happened to start two days earlier.

The fixtures are the scheduled runs GitHub actually created (gh run list, measured 2026-09-15) and the
start times of the desktop passes whose pull/push window overlapped one (logs/local_heavy_*.log).
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


COLLISIONS = ["2026-09-08T10:31:28", "2026-09-13T10:32:09", "2026-09-15T10:38:56"]


def _longest_free_run(span_min):
    """Longest contiguous stretch of the 24 h clock left clear by pending windows of `span_min` at every cron."""
    covered = set()
    for hrs in g.WRITERS.values():
        for h in hrs:
            for k in range(span_min):
                covered.add((h * 60 + k) % 1440)
    free = set(range(1440)) - covered
    if not free:
        return 0
    best = 0
    for s in free:
        if (s - 1) % 1440 in free:
            continue                      # not the head of a run
        ln, m = 0, s
        while m % 1440 in free and ln <= 1440:
            ln, m = ln + 1, m + 1
        best = max(best, ln)
    return best


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


def test_the_ceiling_is_derived_from_the_crons_and_always_leaves_a_block_a_pass_can_use():
    """The invariant the 12 h constant violated, asserted on the value the gate actually applies.

    max_pending_horizon() is computed from the cron layout, so moving a cron moves the ceiling with it. Whatever
    the measured lag does - and it has risen every week since 2026-W34 - the union of the pending windows must
    leave a contiguous block at least FREE_BLOCK_MIN long. With a 12 h window the four crons at 03/06/15/18Z
    covered 24 h of 24 and the desktop ran 0 passes in 112 attempts (R1032/R1033).
    """
    ceiling = g.max_pending_horizon()
    longest = _longest_free_run(int(ceiling.total_seconds() // 60))
    want = int(g.FREE_BLOCK_MIN.total_seconds() // 60)
    assert longest >= want, f"ceiling {ceiling} leaves only {longest} min of clear clock; a pass needs {want}"


def test_the_invariant_rejects_a_window_as_long_as_the_cron_gap():
    """Negative control: the invariant must reject values that would leave no clear clock at all.

    Named for what it checks, not for a cause: the 12 h constant was blamed for a zero-pass day and that
    attribution is withdrawn (R1042). It is still a value this invariant must refuse.
    """
    assert _longest_free_run(12 * 60) == 0, "a 12 h window must leave no clear clock - it is why this exists"
    assert _longest_free_run(9 * 60) == 0, "a window as long as the largest cron gap must leave nothing either"


def test_the_ceiling_still_blocks_every_recorded_collision():
    """The two properties this gate must hold at once, stated together so neither can be traded away quietly.

    The three collisions began 272-278 min after the 06:00Z cron, so any ceiling below that unblocks one of them.
    This is also why the runner's static blackouts cannot stay: they open at 10:30Z, 272 min after that cron, so
    "spare the runner's free periods" and "block the recorded collisions" have no common solution.
    """
    latest = max(g._parse(c + "Z") - dt.datetime(2026, 9, int(c[8:10]), 6, tzinfo=dt.timezone.utc)
                 for c in COLLISIONS)
    assert g.max_pending_horizon() >= latest, (
        f"ceiling {g.max_pending_horizon()} is shorter than the latest recorded collision at {latest}")


def test_the_measured_horizon_is_bounded_by_the_ceiling_whatever_the_history_says():
    """A rising lag must not be able to grow the window past the invariant."""
    now = at("2026-09-15T12:00:00")
    absurd = [{"createdAt": (at("2026-09-01T06:00:00") + dt.timedelta(days=d, hours=11)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"), "event": "schedule", "status": "completed"} for d in range(14)]
    assert g.pending_horizon(absurd, (6, 18), now) <= g.max_pending_horizon()
    assert g.pending_horizon([], (6, 18), now) <= g.max_pending_horizon()


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


def test_the_free_block_is_not_allowed_to_be_defined_down_to_nothing():
    """The invariant above compares the clear clock against FREE_BLOCK_MIN, so FREE_BLOCK_MIN itself needs a floor.

    Without this, setting FREE_BLOCK_MIN to zero satisfies "leaves a block a pass can use" with a block of zero
    minutes, and the ceiling grows to the full 9 h cron gap - the tiled clock of R1033 again, reached by changing
    the yardstick instead of the measurement. The floor is the measured pass: 228 min, 10:38-14:26Z on 2026-09-15.
    """
    assert g.FREE_BLOCK_MIN >= dt.timedelta(minutes=228), (
        "FREE_BLOCK_MIN is below the length of a measured desktop pass, so the invariant guards nothing")


def test_a_skipped_cron_does_not_shift_every_later_pairing():
    """R1034. GitHub skips scheduled runs; a loop that pairs each cron with the next unconsumed run turns one
    skip into a cascade, crediting later runs to crons days earlier. That reported a median lag of 32,690 min
    against a true 132, and the suite stayed green because absurd values clamp to the ceiling.

    Here every run is exactly 120 min late and one cron never ran. Every lag must still read 120.
    """
    now = at("2026-09-15T12:00:00")
    skipped = at("2026-09-05T18:00:00")
    rows, day = [], at("2026-09-01T06:00:00")
    while day < now:
        for h in (6, 18):
            cron = day.replace(hour=h)
            if cron >= now or cron == skipped:
                continue
            rows.append({"createdAt": (cron + dt.timedelta(minutes=120)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                         "event": "schedule", "status": "completed"})
        day += dt.timedelta(days=1)

    lags = g._scheduled_lags(rows, (6, 18), now)
    assert lags, "no lags computed at all"
    assert max(lags) <= 130, f"a single skipped cron shifted the pairing; worst lags {sorted(lags)[-4:]}"
    assert min(lags) >= 110, f"pairing drifted early; best lags {sorted(lags)[:4]}"


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


# 06:45Z, and the instant matters. Every cron here started 30 min late, so the MEASURED horizon floors at
# 60 min and the 06:00Z pending window is 06:00-07:00Z. Outside it assess() returns on the window test and
# never evaluates `started`, so a later instant tests nothing - mutation caught exactly that. Inside it,
# with the run already created at 06:30Z: the schedule-only list says the cron HAS started (CLEAR), while a
# dispatch-flooded list has no scheduled rows, falls back to the 3 h horizon and invents a pending cron.
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


def test_the_runner_threshold_fits_inside_the_block_the_gate_guarantees():
    """The two halves of this mechanism are in different languages and nothing coupled them.

    run_local_heavy.ps1 refuses to start unless it sees $WantBlockMin + $marginMin minutes of clear time.
    ci_writer_gate.py guarantees FREE_BLOCK_MIN. If the first ever exceeds the second the desktop stops running
    and nothing says why - which is R1032's failure, reachable by editing one PowerShell default.
    """
    src = pathlib.Path(RUNNER_PS1).read_text(encoding="utf-8", errors="replace")
    want = re.search(r"\$WantBlockMin\s*=\s*(\d+)", src)
    margin = re.search(r"\$marginMin\s*=\s*(\d+)", src)
    assert want and margin, f"cannot read the runner's thresholds from {RUNNER_PS1}; this test measures nothing"
    need = int(want.group(1)) + int(margin.group(1))
    have = int(g.FREE_BLOCK_MIN.total_seconds() // 60)
    assert need <= have, (
        f"the runner wants {need} min of clear time but the gate only guarantees {have}; "
        "the desktop would hold for ever without ever saying why")


def test_the_runner_escape_is_longer_than_its_cadence_floor():
    """$MaxHours must exceed $MinHours, or the wait-for-a-better-window rule can never hold at all."""
    src = pathlib.Path(RUNNER_PS1).read_text(encoding="utf-8", errors="replace")
    mx = re.search(r"\$MaxHours\s*=\s*(\d+)", src)
    mn = re.search(r"\$MinHours\s*=\s*(\d+)", src)
    assert mx and mn, "cannot read the runner's cadence parameters"
    assert int(mx.group(1)) > int(mn.group(1)), (
        f"$MaxHours {mx.group(1)} must be greater than $MinHours {mn.group(1)}")
