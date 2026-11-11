"""Is a cloud state writer running, or about to? The desktop heavy pass must not overlap one.

WHY (2026-09-15). The desktop pass (tools/run_local_heavy.ps1) and the cloud updater workflows all pull
data/_aqueduct/state.db from R2 and push it back with an ETag compare-and-swap (updater/run.py), so
when two passes overlap the second push is refused and that pass's whole bookkeeping is thrown away
(ledger R5). The runner used to ask only "is an updater-daily run in flight?", plus fixed windows
measured on 2026-08-23, when GitHub started scheduled runs 13-55 min late. Measured over every
scheduled run GitHub still lists (2026-07-04..09-16, 201 runs, `python tools/measure_ci_lag.py lag`):
the start lag has a median of 132 min (daily) and 148 (heavy), a p95 of 354 and 329, and a maximum of
710 and 663 - and it is RISING, with pooled weekly medians of 25 min in 2026-W34 against 301 in W38.
A desktop pass started at 10:38Z on 2026-09-15 saw nothing in flight; the 06:00Z run started under it
at 11:18Z and the cloud run's push was refused at 15:28Z. On 09-08 and 09-13 the desktop pass lost.

So a cron whose time has passed but whose run GitHub has not created yet is as busy as a run in
flight. The gate blocks on either, for both state-writing workflows, and never answers CLEAR when it
cannot read CI. It errs towards blocking, because a wrong CLEAR costs a pass's whole bookkeeping while a
wrong BLOCK costs one tick (run_local_heavy.ps1 -SkipCiCheck overrides it):
  - every run that is not completed blocks, however old; one older than a day also prints a warning;
  - a cron blocks from its scheduled time until its scheduled run is created, at most PENDING_LIMIT
    (pending_limit(): the time to that workflow's next cron - 24 h for the one daily cron, 9 h and 15 h for the
    two heavy ones - the same window in which the gate credits a run to that cron). There is no shorter ceiling
    any more - see PENDING_LIMIT for the measurement that removed it;
  - only a workflow GitHub reports as explicitly not active (disabled) is not waited for; one GitHub
    does not list at all is still waited for, with a warning;
  - an EMPTY run listing is never read as "nothing there": it is read again, and a writer whose unfiltered
    listing stays empty blocks, because a run of it in flight cannot be ruled out (R1660, review AR-375);
    an unfinished run in EITHER listing (unfiltered or scheduled) blocks.

  python tools/ci_writer_gate.py      # exit 0 CLEAR, 3 BLOCKED (reason printed), 2 cannot tell
"""
from __future__ import annotations

import datetime as dt
import json
import os
import re
import subprocess
import sys
import time

# Workflow file -> UTC hours of its schedule crons (.github/workflows/<file>); both run
# `python -m updater.run --pull-state ... --push-state`. tests/test_ci_writer_gate.py and
# tests/test_scheduler_stall.py read the workflow files and fail when this table and their crons disagree.
#
# SCHEDULE OF 2026-11-04 (owner's decision): the 18:00Z updater-daily cron is removed and the 15:00Z
# updater-heavy cron moves to 12:00Z, so that the desktop pass gets a window at night. Before that the
# schedule was daily (6, 18) and heavy (3, 15): two crons per workflow, 12 h apart, and every window
# below was the one constant 12 h. Now a workflow's crons are not evenly spaced (heavy 03->12 is 9 h,
# 12->03 is 15 h; daily has one cron, 24 h from the next), so the window is per cron: see cron_windows().
WRITERS = {"updater-daily.yml": (6,), "updater-heavy.yml": (3, 12)}
# The table before 2026-11-04. The gate never reads it; it is here, next to the one in force, for tools that replay
# history from before that day (tools/measure_ci_lag.py --schedule until-2026-11-04) and for the tests that do.
WRITERS_UNTIL_2026_11_04 = {"updater-daily.yml": (6, 18), "updater-heavy.yml": (3, 15)}


def cron_windows(writers: dict | None = None) -> dict:
    """{workflow: {cron hour: window}}: the time from each cron to the NEXT cron of the same workflow.

    This is the window in which the gate credits a scheduled run to that cron (the `started` test in assess)
    and the longest a cron with no run still blocks (PENDING_LIMIT below explains why the two are the same).
    The windows of one workflow tile the clock without overlapping, so a run is credited to at most one cron.
    A workflow with one cron a day gets 24 h. `writers` defaults to WRITERS (tools/measure_ci_lag.py passes a
    cut schedule)."""
    out = {}
    for wf, hours in (WRITERS if writers is None else writers).items():
        hs = sorted(set(hours))
        out[wf] = {h: dt.timedelta(hours=((hs[(i + 1) % len(hs)] - h) % 24) or 24) for i, h in enumerate(hs)}
    return out


def pending_limit(wf: str, hour: int, writers: dict | None = None) -> dt.timedelta:
    """The credit window and blocking ceiling of the `hour`:00Z cron of `wf` (cron_windows)."""
    return cron_windows(writers)[wf][hour]


# A RUN THAT MAY BELONG TO THE PREVIOUS CRON IS NOT CREDITED TO THIS ONE (review AR-426, coordinator's decision of
# 2026-11-04). Heavy's 03:00Z window is 9 h, shorter than that cron's measured lag tail: the scheduled heavy run of
# 2026-08-27 was created 14:02Z, a 03:00Z run 662 min late. Credited to the 12:00Z cron by window alone, it would make
# the gate read CLEAR once it finished, while the real 12:00Z run had not been created - the collision this gate exists
# to prevent. So when the previous cron of the same workflow has NO run in its own window, a run created before that
# previous cron + CREDIT_AMBIGUITY may be its late run, and does not start this cron. This cron then needs a run created
# after that instant, or a SECOND run in its window. CREDIT_AMBIGUITY is the measured lag ceiling (60-day maximum 710
# min, rounded up). Where every window is at least that long - the table before 2026-11-04 (all 12 h), and daily's one
# cron (24 h) - the ambiguous span is empty and the rule is the plain window rule. What it gives up: after a 03:00Z cron
# GitHub really skipped, a 12:00Z run created before 15:00Z is not believed, so the gate blocks until the next heavy run
# or until the 12:00Z window ends at 03:00Z - about one night of desktop time (3 skips of any cron in 60 days).
CREDIT_AMBIGUITY = dt.timedelta(hours=12)


def credit_instant(created: list, cron: dt.datetime, hour: int, wf: str, windows: dict | None = None):
    """When the `hour`:00Z cron at `cron` counts as started: the createdAt of the scheduled run that starts it, or None.

    `created`: the createdAt of the writer's SCHEDULED runs, sorted. `windows`: cron_windows() (default: of WRITERS).
    One rule for the gate (assess) and for tools/measure_ci_lag.py (cron_blocks), so the two cannot drift apart."""
    windows = cron_windows() if windows is None else windows
    ws = windows[wf]
    own = [c for c in created if cron <= c < cron + ws[hour]]
    if not own:
        return None
    hs = sorted(ws)
    prev_h = hs[hs.index(hour) - 1]
    prev_cron = cron - ws[prev_h]                       # the previous cron's window ends exactly at this cron
    ambiguous_until = prev_cron + CREDIT_AMBIGUITY
    if ambiguous_until <= cron or any(prev_cron <= c < cron for c in created):
        return own[0]
    later = [c for c in own if c >= ambiguous_until]
    return min(own[1:2] + later[:1]) if (len(own) > 1 or later) else None


# ---------------------------------------------------------------- slots (desktop slot dispatch, PLAN V2, AR-470)
# SINCE THE DESKTOP SLOT DISPATCHER (tools/econ_dispatch.py) the desktop starts each cron's work at the cron's own
# time, and the cron, which GitHub still creates hours late, stays as the fallback. Every run names the slot it
# serves in its run-name (the workflows' `run-name:`), so a run can be credited to exactly one cron:
#     "slot heavy-03Z-2026-11-12"   a workflow_dispatch with the `slot` input (and no source / force / dry_run)
#     "cron 0 3 * * *"              a scheduled run; its slot date is the latest 03:00Z at or before createdAt
# Anything else (history from before this change, a manual source/force/dry_run dispatch) has no slot.
#
# ONE predicate decides whether a run SETTLES its slot - run_settles() below - and the in-workflow guard
# (tools/slot_check.py), this gate (slot_credited) and the dispatcher all call it (review AR-470 H2; a test pins
# that they do). An earlier run of the slot settles it when it is completed, not cancelled, and a "Pull state"
# step of it succeeded: a run that failed before its pull did no work, and "success" alone is wrong here
# (updater-daily ends `failure` on its health gate after the work is pushed - 38 of 38 scheduled runs since
# 2026-10-20, AR-470 (c)). The second run of a settled slot is a no-op: its guard job ends before any state pull.
SLOT_KIND = {"updater-daily.yml": "daily", "updater-heavy.yml": "heavy"}
REPO = os.environ.get("ECON_GH_REPO", "elkassabgi/econdatalibrary")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MARKER_PATH = os.path.join(ROOT, "logs", "econ_dispatch.marker")
# The dispatcher writes the marker BEFORE it reads the desktop lock and before `gh workflow run`, and refreshes it
# just before the call; the gate blocks for MARKER_BLOCK after its time - "dispatched, maybe not listed yet". After
# that the dispatched run is listed (in flight: blocks) or its cron is still uncredited (blocks), see
# slot_credited().
MARKER_BLOCK = dt.timedelta(minutes=10)
PULL_STEP_PREFIX = "Pull state"
_TAG_RE = re.compile(r"^(daily|heavy)-([0-9]{2})Z-([0-9]{4})-([0-9]{2})-([0-9]{2})$")
_CRON_RE = re.compile(r"^0 ([0-9]{1,2}) \* \* \*$")


def slot_tag(wf: str, cron: dt.datetime) -> str:
    """'heavy-03Z-2026-11-12' for the 03:00Z cron of updater-heavy.yml on 2026-11-12."""
    return f"{SLOT_KIND[wf]}-{cron:%H}Z-{cron:%Y-%m-%d}"


def parse_slot_tag(tag: str, wf: str, writers: dict | None = None) -> dt.datetime | None:
    """The cron instant a tag names, or None when the tag is malformed or names another workflow or hour."""
    m = _TAG_RE.match((tag or "").strip())
    if not m or m.group(1) != SLOT_KIND.get(wf):
        return None
    h = int(m.group(2))
    if h not in (WRITERS if writers is None else writers).get(wf, ()):
        return None
    try:
        return dt.datetime(int(m.group(3)), int(m.group(4)), int(m.group(5)), h, tzinfo=dt.timezone.utc)
    except ValueError:
        return None


def cron_hour(expr: str, wf: str, writers: dict | None = None) -> int | None:
    """The hour of a '0 H * * *' cron of `wf`, or None."""
    m = _CRON_RE.match((expr or "").strip())
    if not m:
        return None
    h = int(m.group(1))
    return h if h in (WRITERS if writers is None else writers).get(wf, ()) else None


def cron_slot_of(hour: int, created: dt.datetime) -> dt.datetime:
    """The latest hour:00Z at or before `created`: the cron a scheduled run of that cron was created for (start lag
    is below 24 h - 60-day maximum 710 min)."""
    c = created.replace(hour=hour, minute=0, second=0, microsecond=0)
    return c if c <= created else c - dt.timedelta(days=1)


def run_slot(wf: str, row: dict, writers: dict | None = None):
    """(cron instant, 'slot' | 'cron') for a run whose title names its slot, else None. A 'slot' title counts only on
    a workflow_dispatch and a 'cron' title only on a schedule event."""
    title = (row.get("displayTitle") or "").strip()
    ev = row.get("event")
    if title.startswith("slot ") and ev == "workflow_dispatch":
        c = parse_slot_tag(title[5:], wf, writers)
        return (c, "slot") if c is not None else None
    if title.startswith("cron ") and ev == "schedule":
        h = cron_hour(title[5:], wf, writers)
        if h is None or not row.get("createdAt"):
            return None
        return cron_slot_of(h, _parse(row["createdAt"])), "cron"
    return None


def run_settles(row: dict, jobs) -> bool:
    """THE settled predicate (AR-470 H2). `row`: a run listing row; `jobs`: that run's jobs with their steps
    ([{"name", "steps": [{"name", "conclusion"}]}]), or None when they could not be read (= not settled).

    True only when the run is completed, did not end cancelled, and some job's "Pull state" step succeeded."""
    if row.get("status") != "completed" or (row.get("conclusion") or "cancelled") == "cancelled":
        return False
    if not jobs:
        return False
    return any((s.get("name") or "").startswith(PULL_STEP_PREFIX) and s.get("conclusion") == "success"
               for j in jobs for s in (j.get("steps") or []))


def slot_credited(wf: str, hour: int, cron: dt.datetime, rows: list, sched_rows: list, settled=frozenset(),
                  windows: dict | None = None) -> bool:
    """Has the `hour`:00Z cron at `cron` of `wf` started, for the gate's pending rule (and the dispatcher's "is this
    slot still open")?

    rows: every listed run of `wf` (both listings); sched_rows: its scheduled runs; settled: the databaseIds of
    completed slot-tagged runs for which run_settles() is True (main() reads their jobs).
      - a run titled with THIS slot - its own cron run or a slot dispatch - credits it while it is unfinished (the
        in-flight check blocks anyway) and, once finished, only if it settled (run_settles). A late no-op cron
        does not settle, but the run it deferred to did. An evicted or failed-closed cron run leaves the cron
        uncredited until its window ends, and the dispatcher may serve the slot in that time (AR-470 round 2 F3);
      - a titled run never credits another cron, by window or otherwise (AR-470 C7);
      - runs with no slot title (history from before titles) keep credit_instant()'s window rule, ambiguity
        included."""
    for r in rows:
        s = run_slot(wf, r)
        if s is None or s[0] != cron:
            continue
        # The same rule for a titled cron run and a slot dispatch (review AR-470 round 2, F3): an evicted cron run,
        # or one whose guard failed closed, did no work, so it must not read as the cron's start - else the gate
        # reads CLEAR over a slot nothing served, and the dispatcher would see no open slot to serve.
        if r.get("status") != "completed":
            return True
        if r.get("conclusion") != "cancelled" and r.get("databaseId") in settled:
            return True
    legacy = sorted(_parse(r["createdAt"]) for r in sched_rows
                    if r.get("event") == "schedule" and run_slot(wf, r) is None)
    return credit_instant(legacy, cron, hour, wf, windows) is not None


def read_marker(path: str | None = None, now: dt.datetime | None = None) -> dt.datetime | None:
    """The time in the dispatcher's marker, or None when there is no marker. A marker that exists but cannot be read
    or parsed answers its file time, and failing that `now` (= blocks): it must never read as absent."""
    path = MARKER_PATH if path is None else path
    if not os.path.exists(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return _parse(f.readline().strip())
    except Exception:  # noqa: BLE001
        try:
            return dt.datetime.fromtimestamp(os.path.getmtime(path), dt.timezone.utc)
        except Exception:  # noqa: BLE001
            return now or dt.datetime.now(dt.timezone.utc)


# How long a cron that has not started still blocks a desktop pass: until its run is created, at most this.
#
# WHY THERE IS NO SHORTER CEILING (2026-10-20). Until then the window was the p90 of the workflow's own recent
# start lag, clamped to a ceiling of (largest cron gap 9 h) - (a 4 h free block) = 300 min, so that the windows
# could never cover the whole nominal clock. The cron did not keep to that ceiling. Every state-push failure of
# the desktop pass since 2026-09-20 had one cause: the gate said CLEAR because the 06:00Z cron was more than
# 300 min old, and the cloud run was created under the pass a few minutes later and pushed first:
#     2026-09-24  CLEAR 11:03:25Z, run created 11:17:48Z (317.8 min after the cron)
#     2026-10-07  CLEAR 12:22:32Z, run created 12:50:29Z (410.5 min)
#     2026-10-20  CLEAR 11:00:13Z, run created 11:04:16Z (304.3 min); push refused at 14:48:11Z
# The same CLEAR cost the CLOUD its record 7 more times in that period (09-29, 10-01..10-05, 10-13: the desktop
# pushed first and the cloud run's "Push state to R2" step failed). Of the 34 desktop passes since 2026-09-20
# that got a CLEAR, these 10 were overlapped by a scheduled run; this rule blocks all 10 and none of the 24.
# Start lag (createdAt - cron, the field this gate reads) of the 237 scheduled runs of both writers over
# 2026-08-21..10-20: median 278, p90 402, p95 428, p99 661, max 710 min; 83 of 237 runs came later than
# 300 min, 13 later than 420, 4 later than 540. The 06:00Z cron alone: median 315, 35 of 59 later than 300.
# The weekly median rose from 21 min (2026-W34) to 377 (W41). That pairing credits a run to a cron only within
# 12 h, so it cannot see a lag longer than that: 3 crons had no run in their 12 h, and the 15:00Z heavy window
# of 2026-08-28 held TWO runs (15:14:02Z, 23:46:21Z) after a 03:00Z cron with none - either a 734 min lag or
# a skip plus an extra run, and the data cannot say which.
#
# So the lag is not bounded below the cron gap: the observed maximum of the 06:00Z (710), 03:00Z (663) and
# 15:00Z (550) crons each exceeds the 540 min largest gap. No ceiling can both cover the lag and leave a free
# block on the NOMINAL clock, and the free block was a property of the nominal clock only. A cron's window ends
# when its run is created, and the run is then in flight, which blocks anyway - so the window length matters
# only for a cron whose run has NOT been created, and a CLEAR in that state starts a pass that the run will
# overlap when it is created, unless the cron was skipped or the run comes after the pass has pushed. A
# measured, clamped horizon is also biased LOW exactly when it matters: it predicts how late an un-started
# cron will be from the crons that did start, so the slow run is absent from its own sample.
#
# The limit is the time to the workflow's next cron (cron_windows; it was CRON_SPACING = 12 h while every
# workflow had two crons 12 h apart) because that is the window in which the gate credits a run to a cron at
# all (the `started` test below, and the same rule in tools/measure_ci_lag.py): a run created later than that
# is indistinguishable from the next cron's run. At that point the workflow's next cron is due and blocks in its
# own right, so for one workflow the block is continuous from a cron until a run is created. What a skipped
# cron costs: the desktop waits for the next cron's run (3 skips in the 60 days above). What it costs if
# GitHub stops creating scheduled runs without disabling the workflow: the desktop waits until it does, or
# until someone runs with -SkipCiCheck. run_local_heavy.ps1 warns after 2 x $MinHours without a pass, but that
# is a line in a local log and it fires routinely (review AR-362). The alert that leaves the machine is
# tools/scheduler_stall.py: the guard loop evaluates it, and it warns when a writer has had no scheduled run, or
# has held an unfinished run (in either listing, as this gate reads them - review AR-376), for 36 h. It runs in
# SHADOW mode (records the mail, sends nothing) until the owner
# creates its flag file (ledger R1661), and it reads the scheduled runs through read_sched() below (R1660).
# KNOWN RESIDUAL: a run created more than 12 h after its cron is credited to the next cron of that workflow,
# and the next cron's own run then arrives unannounced (the 2026-08-28 shape above: once 15:14Z had finished,
# this rule would read the heavy writer as done until 23:46Z; on that day the daily writer blocked anyway).
# One such window in 60 days; the in-flight check still blocks the moment the second run is created.
# This restores the 12 h window of PR #29, which was blamed for a zero-pass day (R1032/R1033); that
# attribution was withdrawn (R1042): a window ends when its run appears, and runs do appear.
#
# SINCE THE 2026-11-04 SCHEDULE the windows are daily 06:00Z 24 h, heavy 03:00Z 9 h, heavy 12:00Z 15 h.
#   - The daily window grew from 12 h to 24 h. With one cron a day, a 12 h window would answer CLEAR from 18:00Z
#     while a 06:00Z run not yet created could still arrive (the 18:00Z cron used to take over the block at
#     that point); 24 h keeps the block continuous until a run is created, as before. A skipped daily cron now
#     blocks the desktop until the NEXT day's run.
#   - The heavy 03:00Z window shrank from 12 h to 9 h, and that is below the measured lag tail (60-day maximum
#     of the 03:00Z cron 663 min, and 4 of the 237 runs of both writers later than 540; over the 30 days to
#     2026-11-04 the 03:00Z maximum was 448, measured from the gh run history on that day). The
#     block stays continuous - at 12:00Z the 12:00Z cron blocks in its own right. Crons 9 h apart cannot tell a
#     03:00Z run created more than 9 h late from the 12:00Z run by window alone, so a run that may be the 03:00Z
#     cron's late run does not start the 12:00Z cron (CREDIT_AMBIGUITY, review AR-426): the gate keeps blocking
#     until a second heavy run or the end of the 12:00Z window, giving up about one night after a 03:00Z
#     cron GitHub really skipped. (The real case: the heavy run of 2026-08-27, created 14:02Z, 662 min late.)
#
# Cost and benefit, measured on one frozen snapshot of that history with this repo's own instrument
# (tools/measure_ci_lag.py, 60 days to 2026-10-20, old rule vs this one):
#     replay  CLEAR time 338 -> 281 min/day (last 28 days 214 -> 171, last 14 days 217 -> 184)
#     sweep   runner rule (want 150 min, max 36 h): 48 -> 43 passes, 9,667 -> 7,732 pass minutes,
#             collisions 18 -> 0 (16 of the 48 old passes collided, so collision-free pass minutes
#             6,379 -> 7,732); longest gap between pass starts 62.4 -> 73.4 h
# Both read scheduled runs only. A second simulation that also feeds manual dispatches to the in-flight check
# agrees: 60 days 48 -> 43 passes, 9,957 -> 7,972 pass minutes, collisions 18 -> 2 (both left are dispatches,
# which no gate can predict); last 28 days 21 -> 17 passes, 4,207 -> 2,527 pass minutes, 11 -> 1 collisions.
# The CLEAR time that goes is the time in which a pass would be overlapped. Re-measure with `replay` / `sweep`.
# "PENDING_LIMIT" (the name the comment above goes by) is no longer one constant: it is pending_limit(wf, hour),
# from cron_windows() above.
# A note, not a decision: a cron this late is in the tail of the measured lag (5 of 237 runs came later than
# 8 h in the 60 days above), so it is worth saying that GitHub may have skipped it. Clamped below the limit
# so it can always fire before the block ends.
WARN_PENDING_AFTER = dt.timedelta(hours=8)
WARN_UNFINISHED_AFTER = dt.timedelta(hours=24)
RUN_LIMIT = 50            # unfiltered, for the in-flight check
SCHED_LIMIT = 200         # scheduled runs only, for the cron check
WORKFLOW_LIMIT = 200
# displayTitle carries the slot (run_slot); databaseId orders runs (AR-470 H2) and keys settled_runs.
RUN_FIELDS = "createdAt,event,status,conclusion,displayTitle,databaseId"
EXIT_CLEAR, EXIT_UNKNOWN, EXIT_BLOCKED = 0, 2, 3


def _parse(ts: str) -> dt.datetime:
    return dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))


def assess(runs: dict, now: dt.datetime, states: dict | None = None,
           sched: dict | None = None, unread=(), settled=frozenset(),
           marker_at: dt.datetime | None = None) -> tuple[bool, str, list]:
    """runs: {workflow file: [{"createdAt", "event", "status"}, ...]} as `gh run list --json` gives them;
    states: {workflow file: "active" | "disabled_manually" | ...}, or None when not read;
    sched: the same shape but SCHEDULED runs only, reaching further back - used for "has this cron started?".
    Defaults to `runs`, which is what the tests pass;
    unread: the writers whose unfiltered listing came back EMPTY on every read (read_unfiltered). Each one
    BLOCKS, whatever its workflow state: the gate cannot see whether a run of it is in flight (review AR-375).

    Why two lists. The in-flight check must see every run, because a workflow_dispatch writes the state store
    exactly like a cron does. The cron logic must see an unbroken history of SCHEDULED runs, and a single
    count-bounded unfiltered fetch cannot be both: this repo holds 334 daily runs of which 228 are not
    scheduled, so a burst of dispatches would push the scheduled rows out of a 50-row window and the gate would
    read "this cron never started" for every cron at once.

    The in-flight check reads BOTH lists (review AR-375). A scheduled run is a run like any other, and the
    scheduled listing carries its status too; reading only the unfiltered listing let one empty unfiltered answer
    hide a scheduled run in progress, while that same run credited its cron as started - CLEAR during a cloud run.

    settled: databaseIds of completed slot dispatches that settled their slot (slot_credited); marker_at: the
    dispatcher's marker time (read_marker), which blocks for MARKER_BLOCK - a dispatch may be sent and not listed.

    Returns (blocked, reason, notes)."""
    notes = []
    sched = runs if sched is None else sched
    if marker_at is not None and (now - marker_at < MARKER_BLOCK):
        return True, (f"the desktop dispatcher marked a dispatch at {marker_at:%Y-%m-%dT%H:%M:%SZ}; its run may not "
                      f"be listed yet (blocking {int(MARKER_BLOCK.total_seconds() // 60)} min from the mark)"), notes
    for wf in WRITERS:
        listings = [(runs.get(wf, []), "")]
        if sched is not runs:
            listings.append((sched.get(wf, []), " (seen in the scheduled-run listing; the unfiltered listing did "
                                                 "not show it unfinished)"))
        for rows, where in listings:
            for r in rows:
                if r["status"] == "completed":
                    continue
                if now - _parse(r["createdAt"]) > WARN_UNFINISHED_AFTER:
                    notes.append(f"WARNING: {wf} run created {r['createdAt']} has been {r['status']} for more "
                                 f"than a day; if it is stuck, cancel it (or run with -SkipCiCheck)")
                return True, f"{wf} run created {r['createdAt']} is {r['status']}{where}", notes
    windows = cron_windows()
    for wf, hours in WRITERS.items():
        state = None if states is None else states.get(wf)
        if states is not None and state is None:
            notes.append(f"WARNING: {wf} is not in GitHub's workflow list; still waiting for its crons")
        elif state is not None and state != "active":
            notes.append(f"NOTE: {wf} is {state}; its crons are not waited for")
            continue
        created = sorted(_parse(r["createdAt"]) for r in sched.get(wf, [])
                         if r["event"] == "schedule" and run_slot(wf, r) is None)
        both = list(runs.get(wf, [])) + ([] if sched is runs else list(sched.get(wf, [])))
        for back in (1, 0):
            day = (now - dt.timedelta(days=back)).date()
            for h in hours:
                horizon = windows[wf][h]
                cron = dt.datetime(day.year, day.month, day.day, h, tzinfo=dt.timezone.utc)
                if not cron <= now < cron + horizon:
                    continue
                # The same window as pending_limit(): a run is credited to this cron only if it was created
                # within it, and the cron blocks for exactly as long as it can still be credited one. A run that
                # may be the previous cron's late run does not count (CREDIT_AMBIGUITY).
                started = slot_credited(wf, h, cron, both, sched.get(wf, []), settled, windows)
                if not started:
                    late = int((now - cron).total_seconds() // 60)
                    if any(cron <= c < cron + horizon for c in created):
                        notes.append(f"NOTE: {wf} {cron:%H:%M}Z: the run in its window was created before "
                                     f"{CREDIT_AMBIGUITY.total_seconds() / 3600:.0f} h after the previous cron, which "
                                     f"has no run of its own, so it may be that cron's late run; waiting for another")
                    if now - cron >= min(WARN_PENDING_AFTER, horizon * 0.75):
                        notes.append(f"WARNING: {wf} {cron:%H:%M}Z cron has had no run for {late} min; "
                                     f"GitHub may have skipped it")
                    return True, (f"{wf} {cron:%H:%M}Z cron has not started yet ({late} min after its "
                                  f"scheduled time; blocking until its run is created, at most "
                                  f"{int(horizon.total_seconds() // 60)} min)"), notes
    for wf in WRITERS:
        if wf in unread:
            return True, (f"{wf} run listing came back empty on every read, so a run of it in flight (a manual "
                          f"dispatch included) cannot be ruled out (review AR-375)"), notes
    return False, "no cloud state writer in flight or pending", notes


def decide(runs: dict, now: dt.datetime, states: dict | None = None) -> tuple[bool, str]:
    blocked, reason, _notes = assess(runs, now, states)
    return blocked, reason


def minutes_until_block(runs: dict, now: dt.datetime, states: dict | None = None,
                        lookahead: dt.timedelta = dt.timedelta(hours=12),
                        step: dt.timedelta = dt.timedelta(minutes=5), sched: dict | None = None,
                        unread=(), settled=frozenset(), marker_at: dt.datetime | None = None) -> int:
    """How long from `now` before this same rule would block, in whole minutes.

    run_local_heavy.ps1 needs two answers, not one: "may I start?" and "when must I be finished?". It has been
    answering the second from its own static blackout list, built from the NOMINAL cron times - a schedule the
    runs no longer follow, which is why its free periods and the real quiet hours are nearly disjoint. Asking the
    gate instead means one model of the schedule rather than two that disagree.

    Returns 0 if the gate blocks right now, and the lookahead if it would not block within it. This walks the
    same assess() the gate uses rather than reimplementing the windows, so the two answers cannot drift apart.

    The walk holds the run history FROZEN at `now`. That used to make the answer optimistic: a cron that had not
    started was still un-started at every future instant examined, so its window closed on schedule and the walk
    saw free time beyond it. Since PENDING_LIMIT a cron with no run blocks NOW, so at any instant this is asked
    after a CLEAR, every cron of the last PENDING_LIMIT has its run, and the next block the walk finds is the next
    cron's own scheduled time - which is the earliest instant that cron can produce a run. What the frozen walk
    still cannot see: a workflow_dispatch created later, and a run created more than PENDING_LIMIT after its cron
    (see the KNOWN RESIDUAL at PENDING_LIMIT: one ambiguous window in 2026-08-21..10-20). Re-measure with
    `python tools/measure_ci_lag.py sweep`, which models the runner's wait rule.

    `unread` (see assess) blocks at every instant, so a writer whose listing read empty gives 0.
    """
    if assess(runs, now, states, sched, unread, settled, marker_at)[0]:
        return 0
    t, limit = now, now + lookahead
    while t < limit:
        t += step
        if assess(runs, t, states, sched, unread, settled, marker_at)[0]:
            return max(0, int((t - now).total_seconds() // 60))
    return int(lookahead.total_seconds() // 60)


def _gh_json(args: list, timeout: int = 60):
    out = subprocess.run(["gh", *args], capture_output=True, text=True, timeout=timeout)
    if out.returncode != 0:
        raise RuntimeError(f"gh {' '.join(args)} failed: {out.stderr.strip()[:200]}")
    data = json.loads(out.stdout)
    if not isinstance(data, list):
        raise RuntimeError(f"gh {' '.join(args)} returned {type(data).__name__}, not a list")
    return data


def _gh_runs(wf: str, timeout: int = 60) -> list:
    """Every recent run of this workflow, whatever triggered it: a manual dispatch holds the state store too.

    Its answer can be EMPTY for a writer that has runs (the R1660 class), so main() reads it through
    read_unfiltered(), never directly (review AR-375)."""
    return _gh_json(["run", "list", "--workflow", wf, "--limit", str(RUN_LIMIT), "--json", RUN_FIELDS],
                    timeout=timeout)


def _gh_sched(wf: str, timeout: int = 60) -> list:
    """Scheduled runs only, so a burst of dispatches cannot hide whether a cron started (see assess).

    Its answer can be EMPTY for a writer that has scheduled runs (R1660), so no reader uses it directly: main()
    and tools/scheduler_stall.py both go through read_sched(), which reads an empty answer again and cross-checks
    it against the unfiltered listing. tools/scheduler_stall.py passes a shorter timeout (it runs inside the guard
    tick). Raises on a gh failure, a timeout, or an answer that is not a list."""
    return _gh_json(["run", "list", "--workflow", wf, "--event", "schedule", "--limit", str(SCHED_LIMIT),
                     "--json", RUN_FIELDS], timeout=timeout)


def _gh_jobs(run_id, timeout: int = 60) -> list:
    """The jobs of one run with their steps' names and conclusions (for run_settles). Raises on any failure."""
    out = subprocess.run(["gh", "api", f"repos/{REPO}/actions/runs/{int(run_id)}/jobs?per_page=100", "--jq",
                          "[.jobs[] | {name, status, conclusion, steps: [.steps[]? | {name, conclusion}]}]"],
                         capture_output=True, text=True, timeout=timeout)
    if out.returncode != 0:
        raise RuntimeError(f"gh api jobs of run {run_id} failed: {out.stderr.strip()[:200]}")
    data = json.loads(out.stdout)
    if not isinstance(data, list):
        raise RuntimeError(f"jobs of run {run_id}: not a list")
    return data


def settled_runs(runs: dict, sched: dict, now: dt.datetime, fetch_jobs=_gh_jobs) -> tuple[frozenset, list]:
    """The databaseIds of the completed titled runs (slot dispatches and titled cron runs, of the last 2 days) that
    settle their slot, by run_settles() on their jobs. A run whose jobs cannot be read is NOT settled (its cron then
    still blocks: fail closed), and a note says so."""
    out, notes, seen = set(), [], set()
    for wf in WRITERS:
        for r in list(runs.get(wf, [])) + list(sched.get(wf, [])):
            rid = r.get("databaseId")
            s = run_slot(wf, r)
            if (rid is None or rid in seen or s is None or r.get("status") != "completed"
                    or r.get("conclusion") == "cancelled"):
                continue
            seen.add(rid)
            if now - s[0] > dt.timedelta(days=2):
                continue
            try:
                if run_settles(r, fetch_jobs(rid)):
                    out.add(rid)
            except Exception as e:  # noqa: BLE001
                notes.append(f"WARNING: jobs of {wf} run {rid} could not be read ({type(e).__name__}); its slot is "
                             f"treated as not settled")
    return frozenset(out), notes


def _gh_states(timeout: int = 60) -> dict:
    rows = _gh_json(["workflow", "list", "--all", "--limit", str(WORKFLOW_LIMIT), "--json", "path,state"],
                    timeout=timeout)
    return {r["path"].rsplit("/", 1)[-1]: r["state"] for r in rows}


# AN EMPTY SCHEDULED-RUN LISTING IS NOT EVIDENCE THAT NO CRON STARTED (ledger R1660, 2026-10-22).
# `gh run list --workflow <wf> --event schedule` sometimes answers an empty list, exit 0, for a writer that has
# scheduled runs: at 15:00:51Z it returned 0 rows for updater-heavy.yml, whose newest scheduled run was created at
# 08:14:42Z, and at 14:57:42Z, about 3 min earlier, the same call returned 0 rows for updater-daily.yml. assess()
# then finds no run for any cron of the last 12 h and BLOCKS. That fails closed, but every such read takes a tick
# from the desktop's windows for nothing.
#
# How often, measured afterwards (MEASURED_EMPTY_RATE): the same call, with the same arguments as _gh_sched, read
# 64 times per writer 30-60 s apart, 2026-10-22 18:37:02Z..19:43:14Z (128 reads), next to the unfiltered call at
# the same moment: 0 of 64 empty for each writer, 0 errors, and no non-empty answer lacked a scheduled run that
# the unfiltered one held. A second, independent sample (review AR-374) read 0 of 78. So 0 of 206 in all - the
# empties are EPISODIC, not a steady rate: R1660's two came about 3 min apart and then none in the next 206 reads.
# A rate measured outside an episode says nothing about one, which is why this guard does not depend on it.
# Instrument: `gh run list --workflow <wf> --event schedule --limit 200 --json createdAt,event,status` and
# `gh run list --workflow <wf> --limit 50 --json createdAt,event,status`, counted per read (ledger R1660).
MEASURED_EMPTY_RATE = {"updater-daily.yml": (0, 64), "updater-heavy.yml": (0, 64)}   # (empty, reads), 2026-10-22
#
# So an empty filtered answer is read again, and is cross-checked against the unfiltered listing main() has
# already fetched (no extra call). Every schedule-event row in the unfiltered listing is a real scheduled run
# GitHub reports, so adding the ones the filtered answer lacks can only credit a cron with a run that exists.
# The unfiltered listing is the newest RUN_LIMIT runs of any trigger, so it holds at most a SUBSET of the
# scheduled history; a subset can only make a cron read as not started, never as started. So this can turn a
# false BLOCK into the right answer and can never turn a true BLOCK into CLEAR.
# What stays BLOCKED: an empty filtered listing on every read AND no schedule-event row in the unfiltered one.
# That is either a real "no scheduled run" (GitHub stopped creating them) or an empty read during a burst of
# dispatches; this code cannot tell those apart, so it keeps the fail-closed answer and says so in a warning.
# A read that RAISES (gh failed, timed out, or answered something that is not a list) is not retried: it
# propagates to main(), which answers UNKNOWN and exits 2, exactly as before.
#
# AND AN EMPTY UNFILTERED LISTING IS NOT EVIDENCE THAT NO RUN IS IN FLIGHT (review AR-375, 2026-10-23). The
# in-flight check used to read only the unfiltered listing, fetched once. One empty answer for a writer whose run
# was in progress hid that run, and the run (a scheduled one) still credited its cron as started from the
# scheduled listing: CLEAR, exit 0, during a cloud run - the R1660 class failing OPEN, where the scheduled-listing
# case fails closed. Two independent fixes, each enough for that shape on its own:
#   1. read_unfiltered() reads an empty unfiltered answer again, with the same bound as read_sched(). A writer
#      whose unfiltered listing is STILL empty BLOCKS (assess's `unread`). Not UNKNOWN, and why: (a) every writer
#      has runs in its newest-RUN_LIMIT listing (hundreds of runs each), so an empty one is a read fault, and the
#      gate already answers a confirmed-empty SCHEDULED listing with BLOCKED (every cron reads as not started) -
#      one listing, one answer; (b) both answers fail closed for both callers (run_local_heavy.ps1: exit 3 is
#      ABORT, exit 2 is "cannot tell"; tools/derive_csv_bulk.py: `--until-block` 0 is fewer free minutes than its
#      window, a non-zero exit is "cannot tell"), but BLOCKED keeps `--until-block` printing ONE integer (0), and
#      keeps #193's pinned answer for an all-empty read (test_a_truly_empty_scheduled_history_still_blocks);
#      (c) the empties are episodic and short (R1660: two about 3 min apart), so "next tick will check again" is
#      the right instruction. The reason line says what is true: the listing read empty, so a run in flight
#      cannot be ruled out. tools/scheduler_stall.py books the same read UNDETERMINED (it judges a stall, not a
#      start) and mails "blind" after 36 h of it, so a persistent empty listing still reaches a person.
#   2. assess()'s in-flight check also reads the SCHEDULED listing, which carries each run's status. Measured
#      2026-10-23T12:20Z before relying on it: 180 daily and 168 heavy scheduled rows, and the only one not
#      completed was the daily run in progress at that moment (created 11:10:35Z, in both listings), so the
#      deeper history adds no stale block.
#
# Bound: at most SCHED_EMPTY_RETRIES extra reads per writer and listing, SCHED_RETRY_PAUSE_S apart, and only after
# an EMPTY answer; each read keeps _gh_json's own 60 s subprocess timeout. A normal read is unchanged (one call
# per writer and listing).
# WORST-CASE TIME of one gate call, every gh call at its 60 s timeout:
#     #193     2 unfiltered + 2 scheduled + 1 workflow list         = 5 x 60                    = 300 s
#              + 2 retries per writer of the scheduled listing      = 300 + 2 x 2 x (60 + 2)    = 548 s
#     now      + 2 retries per writer of the unfiltered listing too = 548 + 2 x 2 x (60 + 2)    = 796 s
# A timed-out read RAISES rather than answering empty, so every term cannot really be at 60 s at once; the line
# is a ceiling, not an estimate. Measured reads take 5.6 s (unfiltered) and 9.5 s (scheduled) at most (the 128
# reads above), so a retried empty read adds about 2 x (2 + 6) = 16 s (unfiltered) or 2 x (2 + 10) = 24 s
# (scheduled) per writer in practice. Against the callers:
#   - tools/derive_csv_bulk.py runs `--until-block` with a 300 s timeout. The old worst case already met it; past
#     it, subprocess raises TimeoutExpired, which that tool treats as "cannot tell" and refuses to clear. Fails
#     closed.
#   - tools/run_local_heavy.ps1 runs the gate and then `--until-block` with no timeout. It waits; exit 3 is BLOCKED,
#     any other non-zero exit or an `--until-block` answer that is not one integer is "cannot tell", and in both
#     cases the pass does not start. Fails closed.
# A read that times out RAISES (it is not an empty answer), so it is never retried: main() answers UNKNOWN, exit 2.
SCHED_EMPTY_RETRIES = 2
SCHED_RETRY_PAUSE_S = 2.0


def _read_again_if_empty(wf: str, fetch, sleep=time.sleep) -> tuple[list, int]:
    """(rows, reads): fetch(wf), and while the answer is EMPTY, up to SCHED_EMPTY_RETRIES more reads,
    SCHED_RETRY_PAUSE_S apart. The one bounded re-read both listings use (R1660, review AR-375). A read that
    raises propagates."""
    rows, reads = list(fetch(wf)), 1
    while not rows and reads <= SCHED_EMPTY_RETRIES:
        sleep(SCHED_RETRY_PAUSE_S)
        rows, reads = list(fetch(wf)), reads + 1
    return rows, reads


def read_unfiltered(wf: str, fetch, sleep=time.sleep) -> tuple[list, list]:
    """The unfiltered runs of `wf` for assess()'s in-flight check, and the notes about how they were read.

    fetch(wf) -> list of runs (the unfiltered listing). An empty answer is read again (_read_again_if_empty). Still
    empty -> [] and a WARNING; main() then passes the writer to assess() as `unread`, which BLOCKS (see the block
    above for why BLOCKED and not UNKNOWN)."""
    rows, reads = _read_again_if_empty(wf, fetch, sleep)
    if not rows:
        return rows, [f"WARNING: {wf} unfiltered run listing came back empty on {reads} read(s); a run in flight "
                      f"cannot be ruled out, so this writer blocks (review AR-375)"]
    return rows, ([f"NOTE: {wf} unfiltered run listing came back empty, then held {len(rows)} run(s) on read "
                   f"{reads} (review AR-375)"] if reads > 1 else [])


def read_sched(wf: str, fetch_sched, unfiltered: list, sleep=time.sleep) -> tuple[list, list]:
    """The scheduled runs of `wf` for assess(), and the notes about how they were read.

    fetch_sched(wf) -> list of scheduled runs (the `--event schedule` listing); unfiltered: that writer's
    unfiltered listing, as main() already fetched it. See the block above for why each step is safe."""
    notes = []
    rows, reads = _read_again_if_empty(wf, fetch_sched, sleep)
    seen = {r.get("createdAt") for r in rows}
    extra = [r for r in unfiltered if r.get("event") == "schedule" and r.get("createdAt") not in seen]
    if not rows and extra:
        notes.append(f"WARNING: {wf} scheduled-run listing came back empty on {reads} read(s); using the "
                     f"{len(extra)} scheduled run(s) in the unfiltered listing instead (R1660)")
    elif not rows:
        notes.append(f"WARNING: {wf} scheduled-run listing came back empty on {reads} read(s) and the unfiltered "
                     f"listing holds no scheduled run either; every cron of the last 12 h reads as not started")
    elif reads > 1:
        notes.append(f"NOTE: {wf} scheduled-run listing came back empty, then held {len(rows)} run(s) on read "
                     f"{reads} (R1660)")
    if extra and rows:
        notes.append(f"NOTE: {wf} scheduled-run listing lacked {len(extra)} scheduled run(s) that the unfiltered "
                     f"listing holds; added them")
    if extra:
        rows = sorted(rows + extra, key=lambda r: r["createdAt"], reverse=True)
    return rows, notes


def main(fetch=_gh_runs, fetch_states=_gh_states, now=None, until_block=False, fetch_sched=None,
         sleep=time.sleep, fetch_jobs=None, marker_path=None) -> int:
    try:
        # The unfiltered listing first, never a raw answer: an empty one is read again, and one still empty
        # makes that writer `unread`, which blocks (review AR-375). read_sched() then cross-checks against it.
        runs, read_notes = {}, []
        for wf in WRITERS:
            runs[wf], wf_notes = read_unfiltered(wf, fetch, sleep=sleep)
            read_notes += wf_notes
        unread = frozenset(wf for wf in WRITERS if not runs[wf])
        getter = fetch_sched if fetch_sched is not None else (_gh_sched if fetch is _gh_runs else fetch)
        sched = {}
        for wf in WRITERS:
            sched[wf], wf_notes = read_sched(wf, getter, runs[wf], sleep=sleep)
            read_notes += wf_notes
        states = fetch_states()
        now = now or dt.datetime.now(dt.timezone.utc)
        # Slot dispatches settle their cron only through run_settles() on their jobs (AR-470 H2). The real jobs
        # reader only with the real run reader: a test that fakes the listings never reaches GitHub.
        jobs_reader = fetch_jobs if fetch_jobs is not None else (_gh_jobs if fetch is _gh_runs else (lambda _r: None))
        settled, settle_notes = settled_runs(runs, sched, now, jobs_reader)
        read_notes += settle_notes
        marker_at = read_marker(marker_path, now)
        if until_block:
            # For the runner's budget clamp. Prints minutes only, so a caller can read it directly (the runner
            # parses the whole output as one integer, so the read notes are NOT printed here; the plain gate
            # call the runner makes first prints them); an `unread` writer reads 0; a failure to read CI still
            # prints UNKNOWN and exits 2, which the caller must treat as "do not start".
            print(minutes_until_block(runs, now, states, sched=sched, unread=unread, settled=settled,
                                      marker_at=marker_at))
            return EXIT_CLEAR
        blocked, reason, notes = assess(runs, now, states, sched, unread, settled, marker_at)
    except Exception as e:  # noqa: BLE001 - failing to read CI is "cannot tell", never "clear"
        print(f"UNKNOWN: could not read CI runs ({type(e).__name__}: {str(e)[:200]})")
        return EXIT_UNKNOWN
    for n in read_notes + notes:
        print(n)
    print(("BLOCKED: " if blocked else "CLEAR: ") + reason)
    return EXIT_BLOCKED if blocked else EXIT_CLEAR


if __name__ == "__main__":
    sys.exit(main(until_block="--until-block" in sys.argv[1:]))
