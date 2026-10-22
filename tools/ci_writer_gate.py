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
    (12 h, the same window in which the gate credits a run to that cron). There is no shorter ceiling
    any more - see PENDING_LIMIT for the measurement that removed it;
  - only a workflow GitHub reports as explicitly not active (disabled) is not waited for; one GitHub
    does not list at all is still waited for, with a warning.

  python tools/ci_writer_gate.py      # exit 0 CLEAR, 3 BLOCKED (reason printed), 2 cannot tell
"""
from __future__ import annotations

import datetime as dt
import json
import subprocess
import sys

# Workflow file -> UTC hours of its schedule crons (.github/workflows/<file>); both run
# `python -m updater.run --pull-state ... --push-state`.
WRITERS = {"updater-daily.yml": (6, 18), "updater-heavy.yml": (3, 15)}
CRON_SPACING = dt.timedelta(hours=12)          # between a workflow's two crons
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
# The limit is CRON_SPACING because that is the window in which the gate credits a run to a cron at all (the
# `started` test below, and the same rule in tools/measure_ci_lag.py): a run created later than that is
# indistinguishable from the next cron's run. At that point the workflow's next cron is due and blocks in its
# own right, so for one workflow the block is continuous from a cron until a run is created. What a skipped
# cron costs: the desktop waits for the next cron's run (3 skips in the 60 days above). What it costs if
# GitHub stops creating scheduled runs without disabling the workflow: the desktop waits until it does, or
# until someone runs with -SkipCiCheck. run_local_heavy.ps1 warns after 2 x $MinHours without a pass, but that
# is a line in a local log and it fires routinely (review AR-362). The alert that leaves the machine is
# tools/scheduler_stall.py: the guard loop mails the owner when a writer has had no scheduled run for 36 h.
# KNOWN RESIDUAL: a run created more than 12 h after its cron is credited to the next cron of that workflow,
# and the next cron's own run then arrives unannounced (the 2026-08-28 shape above: once 15:14Z had finished,
# this rule would read the heavy writer as done until 23:46Z; on that day the daily writer blocked anyway).
# One such window in 60 days; the in-flight check still blocks the moment the second run is created.
# This restores the 12 h window of PR #29, which was blamed for a zero-pass day (R1032/R1033); that
# attribution was withdrawn (R1042): a window ends when its run appears, and runs do appear.
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
PENDING_LIMIT = CRON_SPACING
# A note, not a decision: a cron this late is in the tail of the measured lag (5 of 237 runs came later than
# 8 h in the 60 days above), so it is worth saying that GitHub may have skipped it. Clamped below the limit
# so it can always fire before the block ends.
WARN_PENDING_AFTER = dt.timedelta(hours=8)
WARN_UNFINISHED_AFTER = dt.timedelta(hours=24)
RUN_LIMIT = 50            # unfiltered, for the in-flight check
SCHED_LIMIT = 200         # scheduled runs only, for the cron check
WORKFLOW_LIMIT = 200
EXIT_CLEAR, EXIT_UNKNOWN, EXIT_BLOCKED = 0, 2, 3


def _parse(ts: str) -> dt.datetime:
    return dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))


def assess(runs: dict, now: dt.datetime, states: dict | None = None,
           sched: dict | None = None) -> tuple[bool, str, list]:
    """runs: {workflow file: [{"createdAt", "event", "status"}, ...]} as `gh run list --json` gives them;
    states: {workflow file: "active" | "disabled_manually" | ...}, or None when not read;
    sched: the same shape but SCHEDULED runs only, reaching further back - used for "has this cron started?".
    Defaults to `runs`, which is what the tests pass.

    Why two lists. The in-flight check must see every run, because a workflow_dispatch writes the state store
    exactly like a cron does. The cron logic must see an unbroken history of SCHEDULED runs, and a single
    count-bounded unfiltered fetch cannot be both: this repo holds 334 daily runs of which 228 are not
    scheduled, so a burst of dispatches would push the scheduled rows out of a 50-row window and the gate would
    read "this cron never started" for every cron at once.

    Returns (blocked, reason, notes)."""
    notes = []
    sched = runs if sched is None else sched
    for wf in WRITERS:
        for r in runs.get(wf, []):
            if r["status"] == "completed":
                continue
            if now - _parse(r["createdAt"]) > WARN_UNFINISHED_AFTER:
                notes.append(f"WARNING: {wf} run created {r['createdAt']} has been {r['status']} for more than a "
                             f"day; if it is stuck, cancel it (or run with -SkipCiCheck)")
            return True, f"{wf} run created {r['createdAt']} is {r['status']}", notes
    for wf, hours in WRITERS.items():
        horizon = PENDING_LIMIT
        state = None if states is None else states.get(wf)
        if states is not None and state is None:
            notes.append(f"WARNING: {wf} is not in GitHub's workflow list; still waiting for its crons")
        elif state is not None and state != "active":
            notes.append(f"NOTE: {wf} is {state}; its crons are not waited for")
            continue
        for back in (1, 0):
            day = (now - dt.timedelta(days=back)).date()
            for h in hours:
                cron = dt.datetime(day.year, day.month, day.day, h, tzinfo=dt.timezone.utc)
                if not cron <= now < cron + horizon:
                    continue
                # The same window as PENDING_LIMIT: a run is credited to this cron only if it was created
                # within it, and the cron blocks for exactly as long as it can still be credited one.
                started = any(r["event"] == "schedule" and cron <= _parse(r["createdAt"]) < cron + PENDING_LIMIT
                              for r in sched.get(wf, []))
                if not started:
                    late = int((now - cron).total_seconds() // 60)
                    if now - cron >= min(WARN_PENDING_AFTER, horizon * 0.75):
                        notes.append(f"WARNING: {wf} {cron:%H:%M}Z cron has had no run for {late} min; "
                                     f"GitHub may have skipped it")
                    return True, (f"{wf} {cron:%H:%M}Z cron has not started yet ({late} min after its "
                                  f"scheduled time; blocking until its run is created, at most "
                                  f"{int(horizon.total_seconds() // 60)} min)"), notes
    return False, "no cloud state writer in flight or pending", notes


def decide(runs: dict, now: dt.datetime, states: dict | None = None) -> tuple[bool, str]:
    blocked, reason, _notes = assess(runs, now, states)
    return blocked, reason


def minutes_until_block(runs: dict, now: dt.datetime, states: dict | None = None,
                        lookahead: dt.timedelta = dt.timedelta(hours=12),
                        step: dt.timedelta = dt.timedelta(minutes=5), sched: dict | None = None) -> int:
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
    """
    if assess(runs, now, states, sched)[0]:
        return 0
    t, limit = now, now + lookahead
    while t < limit:
        t += step
        if assess(runs, t, states, sched)[0]:
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


def _gh_runs(wf: str) -> list:
    """Every recent run of this workflow, whatever triggered it: a manual dispatch holds the state store too."""
    return _gh_json(["run", "list", "--workflow", wf, "--limit", str(RUN_LIMIT), "--json", "createdAt,event,status"])


def _gh_sched(wf: str, timeout: int = 60) -> list:
    """Scheduled runs only, so a burst of dispatches cannot hide whether a cron started (see assess).
    tools/scheduler_stall.py reads the same list with a shorter timeout (it runs inside the guard tick)."""
    return _gh_json(["run", "list", "--workflow", wf, "--event", "schedule", "--limit", str(SCHED_LIMIT),
                     "--json", "createdAt,event,status"], timeout=timeout)


def _gh_states(timeout: int = 60) -> dict:
    rows = _gh_json(["workflow", "list", "--all", "--limit", str(WORKFLOW_LIMIT), "--json", "path,state"],
                    timeout=timeout)
    return {r["path"].rsplit("/", 1)[-1]: r["state"] for r in rows}


def main(fetch=_gh_runs, fetch_states=_gh_states, now=None, until_block=False, fetch_sched=None) -> int:
    try:
        runs = {wf: fetch(wf) for wf in WRITERS}
        getter = fetch_sched if fetch_sched is not None else (_gh_sched if fetch is _gh_runs else fetch)
        sched = {wf: getter(wf) for wf in WRITERS}
        states = fetch_states()
        now = now or dt.datetime.now(dt.timezone.utc)
        if until_block:
            # For the runner's budget clamp. Prints minutes only, so a caller can read it directly; a failure to
            # read CI still prints UNKNOWN and exits 2, which the caller must treat as "do not start".
            print(minutes_until_block(runs, now, states, sched=sched))
            return EXIT_CLEAR
        blocked, reason, notes = assess(runs, now, states, sched)
    except Exception as e:  # noqa: BLE001 - failing to read CI is "cannot tell", never "clear"
        print(f"UNKNOWN: could not read CI runs ({type(e).__name__}: {str(e)[:200]})")
        return EXIT_UNKNOWN
    for n in notes:
        print(n)
    print(("BLOCKED: " if blocked else "CLEAR: ") + reason)
    return EXIT_BLOCKED if blocked else EXIT_CLEAR


if __name__ == "__main__":
    sys.exit(main(until_block="--until-block" in sys.argv[1:]))
