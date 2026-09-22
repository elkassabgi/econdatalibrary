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
  - a cron counts as pending until its scheduled run appears; the pending window is MEASURED from that
    workflow's own recent starts at decision time (see pending_horizon), because the lag is not
    stationary - pooled weekly medians ran 25 min in 2026-W34 and 301 in W38. It is bounded below by a
    floor and above by a ceiling derived from the cron layout, so the windows can never tile the clock
    however late GitHub becomes. Sizing it is worth about 120 clear minutes a week against a replay of
    real history, so it is a correctness fix, not a throughput one (R1035, and R1042 correcting R1033);
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
# How long a cron that has not started still blocks a desktop pass.
#
# This is NOT a constant, because the thing it models is not stationary. Pooled weekly medians of the start lag
# over the 201 scheduled runs GitHub still listed on 2026-09-16: 159, 179, 110, 126, 170, 106, 51, 25, 140, 260,
# 256, 301 min. It fell to 25 min in W34 and has risen every week since. Three constants were tried and all three
# were wrong for the reasons recorded in R1035: the p95 of the pooled lag (368 min) and 4.5 h were both fitted to
# a lag that moves, and 4.5 h additionally unblocked all three recorded collisions, which began 1-9 min after the
# runner's old free periods open. (The 12 h value was ALSO blamed for a zero-pass day; that attribution is
# withdrawn - see R1042 - it was worth ~120 clear minutes a week, not the day.)
#
# WHAT THIS IS WORTH, measured before believing it: forcing each horizon through a replay of 7 days of real
# history gives 2,285 clear min at 12 h, 2,400 at 5 h and 2,405 measured, against a constant 2,905 min of
# in-flight blocking that no horizon touches. The whole change is ~120 clear minutes a WEEK. The earlier
# claim that a 12 h window 'tiles the clock' was wrong: a window ends when its run appears, and runs do
# appear (R1042 corrects R1033). The real constraint on the desktop was the runner's 20 h cadence clock,
# which made it eligible at 10:32Z - inside a window a cloud writer occupied on 14 of the last 14 days
# (R1037). Keep this measured rather than constant because the lag is not stationary, not because it buys
# back the day.
#
# So the horizon is measured per workflow from that workflow's own recent scheduled starts, at decision time,
# and clamped at both ends:
#   floor   - a quiet or unreadable history must not silently disable the pending check;
#   ceiling - derived from the crons themselves so the union of the windows can NEVER tile the clock. With the
#             largest gap between consecutive crons at 9 h (06->15 and 18->03), a ceiling of
#             9 h - FREE_BLOCK_MIN leaves a block at least FREE_BLOCK_MIN long every day, by construction.
# tests/test_ci_writer_gate.py asserts the ceiling really is derived and really does leave that block.
# KNOWN BIAS, not fixable from this data: the horizon must predict how late a cron that has NOT started will
# be, and it is estimated from the crons that DID start. On a day the scheduler is unusually slow the slow run is
# by construction absent from the sample, so this reads LOW exactly when it matters. A second, smaller bias runs
# the same way: when GitHub skips a cron entirely, the skip and a very late start are indistinguishable here, and
# _scheduled_lags resolves the ambiguity as "skipped", dropping the observation rather than recording a huge lag.
# Both are why the ceiling exists and why the in-flight check is never relaxed.
LAG_PERCENTILE = 0.90                           # cover 9 late starts in 10, then let the in-flight check take over
LAG_WINDOW_DAYS = 14                            # recent behaviour only; W34's 25 min does not describe today
LAG_MIN_SAMPLES = 4                             # below this the history says nothing; fall back to the floor
FREE_BLOCK_MIN = dt.timedelta(hours=4)          # a measured desktop pass is 228 min (10:38-14:26Z, 2026-09-15)
MIN_PENDING_HORIZON = dt.timedelta(hours=1)
PENDING_HORIZON = dt.timedelta(hours=3)         # fallback ONLY: used when the history cannot be measured
WARN_PENDING_AFTER = dt.timedelta(hours=4)      # clamped below the effective horizon so it can always fire
WARN_UNFINISHED_AFTER = dt.timedelta(hours=24)
RUN_LIMIT = 50            # unfiltered, for the in-flight check
SCHED_LIMIT = 200         # scheduled runs only, for the cron check and the lag estimate
WORKFLOW_LIMIT = 200
EXIT_CLEAR, EXIT_UNKNOWN, EXIT_BLOCKED = 0, 2, 3


def _parse(ts: str) -> dt.datetime:
    return dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))



def max_pending_horizon() -> dt.timedelta:
    """The largest horizon that still leaves FREE_BLOCK_MIN of clear clock every day.

    Derived from the crons rather than written down, so moving a cron moves the ceiling with it. The pending
    windows are anchored at the cron hours; the clear time available between two consecutive crons is their gap
    minus the horizon, so the binding constraint is the LARGEST gap - anything smaller is covered by the window
    that starts at its own cron.
    """
    hours = sorted({h for hrs in WRITERS.values() for h in hrs})
    gaps = [dt.timedelta(hours=(b - a)) for a, b in zip(hours, hours[1:])]
    gaps.append(dt.timedelta(hours=(hours[0] + 24 - hours[-1])))
    return max(gaps) - FREE_BLOCK_MIN


def _scheduled_lags(rows: list, hours: tuple, now: dt.datetime) -> list:
    """Start lag in minutes for recent scheduled runs, matched to crons in chronological order.

    Greedy chronological matching, NOT "the most recent cron at or before the start". This runs PER WORKFLOW, so
    the candidate crons are CRON_SPACING apart (12 h), never 3 h - the 03:00 and 06:00 crons belong to different
    workflows and are never compared. The failure it avoids is the other one: GitHub skips scheduled runs, and a
    loop that pairs each cron with the next unconsumed run shifts the whole sequence after one skip, crediting
    runs to crons days earlier. That reported a median lag of 32,690 min against a true 132 (R1034).

    A run more than CRON_SPACING after its cron is treated as belonging to a later cron, and this cron is
    recorded as skipped. A genuinely enormous lag is therefore indistinguishable from a skip and is dropped
    rather than recorded; see the LAG_PERCENTILE note above for why that bias is tolerated and bounded.
    """
    starts = sorted(_parse(r["createdAt"]) for r in rows
                    if r.get("event") == "schedule" and r.get("createdAt"))
    if not starts:
        return []
    horizon_start = now - dt.timedelta(days=LAG_WINDOW_DAYS)
    crons = []
    day = (horizon_start - dt.timedelta(days=1)).date()
    while day <= now.date():
        for h in hours:
            crons.append(dt.datetime(day.year, day.month, day.day, h, tzinfo=dt.timezone.utc))
        day += dt.timedelta(days=1)
    crons.sort()

    lags, i = [], 0
    for cron in crons:
        while i < len(starts) and starts[i] < cron:
            i += 1
        if i >= len(starts):
            break
        gap = starts[i] - cron
        if gap > CRON_SPACING:
            # This cron never ran: GitHub skips them. Leave the run for the cron it actually belongs to -
            # consuming it here would shift every later pairing and credit runs to crons days earlier.
            continue
        if cron >= horizon_start:
            lags.append(gap.total_seconds() / 60.0)
        i += 1
    return lags


def pending_horizon(rows: list, hours: tuple, now: dt.datetime) -> dt.timedelta:
    """How long this workflow's un-started cron should block, measured from its own recent starts."""
    ceiling = max_pending_horizon()
    lags = _scheduled_lags(rows, hours, now)
    if len(lags) < LAG_MIN_SAMPLES:
        return min(max(PENDING_HORIZON, MIN_PENDING_HORIZON), ceiling)
    lags.sort()
    idx = min(len(lags) - 1, int(len(lags) * LAG_PERCENTILE))
    measured = dt.timedelta(minutes=lags[idx])
    return min(max(measured, MIN_PENDING_HORIZON), ceiling)

def assess(runs: dict, now: dt.datetime, states: dict | None = None,
           sched: dict | None = None) -> tuple[bool, str, list]:
    """runs: {workflow file: [{"createdAt", "event", "status"}, ...]} as `gh run list --json` gives them;
    states: {workflow file: "active" | "disabled_manually" | ...}, or None when not read;
    sched: the same shape but SCHEDULED runs only, reaching further back - used for "has this cron started?"
    and for the lag estimate. Defaults to `runs`, which is what the tests pass.

    Why two lists. The in-flight check must see every run, because a workflow_dispatch writes the state store
    exactly like a cron does. The cron and lag logic must see a long, unbroken history of SCHEDULED runs, and a
    single count-bounded unfiltered fetch cannot be both: this repo holds 334 daily runs of which 228 are not
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
        horizon = pending_horizon(sched.get(wf, []), hours, now)
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
                started = any(r["event"] == "schedule" and cron <= _parse(r["createdAt"]) < cron + CRON_SPACING
                              for r in sched.get(wf, []))
                if not started:
                    late = int((now - cron).total_seconds() // 60)
                    if now - cron >= min(WARN_PENDING_AFTER, horizon * 0.75):
                        notes.append(f"WARNING: {wf} {cron:%H:%M}Z cron has had no run for {late} min; "
                                     f"GitHub may have skipped it")
                    return True, (f"{wf} {cron:%H:%M}Z cron has not started yet ({late} min after its "
                                  f"scheduled time; waiting up to "
                                  f"{int(horizon.total_seconds() // 60)} min, measured from this "
                                  f"workflow's own recent starts)"), notes
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

    KNOWN OPTIMISM: the walk holds the run history FROZEN at `now`. A cron that has not started yet is still
    un-started at every future instant it examines, so its pending window closes on schedule and the walk sees
    free time beyond it - where in reality the run may by then have started and be in flight. The answer is
    therefore an upper bound on free time, and the caller is clamping a budget with it. This is the same
    residual the horizon itself carries (a run later than the window collides, and the loser is whichever side
    pushes second, R5), and it is bounded by the ceiling. How big: the largest daily lag in the recent window is
    366 min against a 300 min ceiling, so up to ~66 min of overestimate; over the full history the gate quotes
    (max 710 min) it reaches ~410 min. Re-measure with `python tools/measure_ci_lag.py sweep`, which models the
    runner's wait rule; `replay` models only this gate and cannot settle a collision count.
    """
    if assess(runs, now, states, sched)[0]:
        return 0
    t, limit = now, now + lookahead
    while t < limit:
        t += step
        if assess(runs, t, states, sched)[0]:
            return max(0, int((t - now).total_seconds() // 60))
    return int(lookahead.total_seconds() // 60)


def _gh_json(args: list):
    out = subprocess.run(["gh", *args], capture_output=True, text=True, timeout=60)
    if out.returncode != 0:
        raise RuntimeError(f"gh {' '.join(args)} failed: {out.stderr.strip()[:200]}")
    data = json.loads(out.stdout)
    if not isinstance(data, list):
        raise RuntimeError(f"gh {' '.join(args)} returned {type(data).__name__}, not a list")
    return data


def _gh_runs(wf: str) -> list:
    """Every recent run of this workflow, whatever triggered it: a manual dispatch holds the state store too."""
    return _gh_json(["run", "list", "--workflow", wf, "--limit", str(RUN_LIMIT), "--json", "createdAt,event,status"])


def _gh_sched(wf: str) -> list:
    """Scheduled runs only, reaching back far enough to measure a lag window (see assess for why it is separate)."""
    return _gh_json(["run", "list", "--workflow", wf, "--event", "schedule", "--limit", str(SCHED_LIMIT),
                     "--json", "createdAt,event,status"])


def _gh_states() -> dict:
    rows = _gh_json(["workflow", "list", "--all", "--limit", str(WORKFLOW_LIMIT), "--json", "path,state"])
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
