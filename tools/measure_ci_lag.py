"""Measure how GitHub's scheduler actually behaves, and what the writer gate would allow because of it.

Why this exists. tools/ci_writer_gate.py and tools/run_local_heavy.ps1 both decide when the desktop may hold the
shared state store, and both were originally written against the NOMINAL cron times. They are not a good model of
the system: the start lag is large and not stationary. Every figure quoted in the gate's own comments, in
.claude/NUMBERS.md and in the ledger entries around R1032/R1033 comes from this file, so they can be re-measured
rather than believed.

    python tools/measure_ci_lag.py lag          # start-lag distribution, weekly, per workflow and pooled
    python tools/measure_ci_lag.py occupancy    # which hours the cloud writers really occupy
    python tools/measure_ci_lag.py collisions   # how often a start lands in a window the desktop may use
    python tools/measure_ci_lag.py replay       # replay the gate over real history: clear minutes per day
    python tools/measure_ci_lag.py fetch --out DIR                 # freeze runs + jobs of both writers (gh, read-only)
    python tools/measure_ci_lag.py replay61 --baseline --history DIR --logs LOGS --state COPY   # reproduce first
    python tools/measure_ci_lag.py replay61 --history DIR --logs LOGS --state COPY              # the 6.1 sweep

All read-only: `gh run list` / `gh run view` only, the desktop logs read, a state COPY opened ?immutable=1. Nothing
here writes to the repo's state, deploys or runs the updater. `replay61` is the starvation plan's section-6.1 replay
(the W windows, D3's no-progress back-off, the desktop state lease with fairness and re-dispatch); its model and every
assumption are described in the block above `cmd_fetch`.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ci_writer_gate as g  # noqa: E402

# HISTORICAL, kept only so `collisions` can score the design that was replaced: these are the windows the
# runner's static blackout list used to leave open. That list is DELETED - run_local_heavy.ps1 now asks the
# gate - so this is a record of the old model, not a description of the running one. Do not read it as the
# current free windows; `windows` takes them as arguments and `sweep` derives them from the gate.
RUNNER_FREE_HISTORICAL = [(10 * 60 + 30, 14 * 60 + 40), (22 * 60 + 30, 2 * 60 + 40)]
MEASURED_PASS_MIN = 228          # desktop pass 10:38-14:26Z, 2026-09-15 (logs/local_heavy_*.log)


def _gh(wf: str, fields: str, limit: int = 200) -> list:
    out = subprocess.run(["gh", "run", "list", "--workflow", wf, "--event", "schedule",
                          "--json", fields, "-L", str(limit)],
                         capture_output=True, text=True, timeout=180)
    if out.returncode != 0:
        sys.exit(f"gh failed for {wf}: {out.stderr.strip()[:300]}")
    return json.loads(out.stdout)


def _parse(s):
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00")) if s else None


def _start(r):
    return _parse(r.get("startedAt") or r.get("createdAt"))


def _lags(rows: list, hours: tuple) -> list:
    """(start, lag_minutes) matched to crons in chronological order.

    Greedy matching, not "nearest preceding cron": with lags near 300 min and crons 3 h apart, a late 03:00 run
    starting at 08:00 would otherwise be credited to the 06:00 cron as 120 min and deflate the very quantity
    being measured.
    """
    starts = sorted(s for s in (_start(r) for r in rows) if s)
    if not starts:
        return []
    crons = []
    day = starts[0].date() - dt.timedelta(days=1)
    last = starts[-1].date() + dt.timedelta(days=1)
    while day <= last:
        for h in hours:
            crons.append(dt.datetime(day.year, day.month, day.day, h, tzinfo=dt.timezone.utc))
        day += dt.timedelta(days=1)
    crons.sort()
    out, i = [], 0
    for cron in crons:
        while i < len(starts) and starts[i] < cron:
            i += 1
        if i >= len(starts):
            break
        gap = starts[i] - cron
        if gap > g.CRON_SPACING:
            continue                 # cron skipped by GitHub; do not consume the next cron's run
        out.append((starts[i], gap.total_seconds() / 60.0))
        i += 1
    return out


def _pct(vals, p):
    v = sorted(vals)
    return v[min(len(v) - 1, int(len(v) * p))] if v else float("nan")


def cmd_lag(_args):
    pooled = []
    for wf, hours in g.WRITERS.items():
        rows = _lags(_gh(wf, "startedAt,createdAt"), hours)
        pooled += rows
        v = [l for _s, l in rows]
        print(f"{wf}: n={len(v)}  median {_pct(v, .5):.0f}  p90 {_pct(v, .9):.0f}  p95 {_pct(v, .95):.0f}  "
              f"max {max(v):.0f} min" if v else f"{wf}: no runs")
    print()
    by_week = defaultdict(list)
    for s, l in pooled:
        by_week[s.isocalendar()[:2]].append(l)
    print("pooled weekly median start lag (min):")
    meds = []
    for wk in sorted(by_week):
        v = by_week[wk]
        med = _pct(v, .5)
        meds.append(med)
        print(f"  {wk[0]}-W{wk[1]:<3} n={len(v):>3}  median {med:>5.0f}  p90 {_pct(v, .9):>5.0f}  max {max(v):>5.0f}")
    if len(meds) >= 6:
        print(f"\nfirst three weeks' medians {[f'{m:.0f}' for m in meds[:3]]} -> "
              f"last three {[f'{m:.0f}' for m in meds[-3:]]}")
    print("\nA rising series means no fixed window is safe: the rule must read this history at decision time.")


def cmd_occupancy(args):
    spans, newest, skipped = [], None, 0
    for wf in g.WRITERS:
        for r in _gh(wf, "startedAt,updatedAt,createdAt,status"):
            st, en = _start(r), _parse(r.get("updatedAt"))
            if st is None:
                continue
            newest = st if newest is None else max(newest, st)
            if r.get("status") != "completed" or en is None or en <= st:
                skipped += 1
                continue
            spans.append((st, en))
    cut = newest - dt.timedelta(days=args.days)
    recent = [s for s in spans if s[0] >= cut]
    durs = [(en - st).total_seconds() / 60 for st, en in recent]
    print(f"{len(recent)} finished scheduled runs in the last {args.days} d (skipped {skipped} unfinished)")
    if durs:
        print(f"duration: median {_pct(durs, .5):.0f}  p95 {_pct(durs, .95):.0f}  max {max(durs):.0f} min")
    days_seen, occ = set(), Counter()
    for st, en in recent:
        days_seen.add(st.date())
        t = st.replace(second=0, microsecond=0)
        marked = set()
        while t < en and len(marked) <= 1440:
            marked.add(t.hour * 60 + t.minute)
            t += dt.timedelta(minutes=1)
        for m in marked:
            occ[m] += 1
    n = max(1, len(days_seen))
    print(f"\nshare of {n} days each hour was occupied by a cloud writer:")
    for h in range(24):
        pct = 100.0 * (sum(occ[h * 60 + k] for k in range(60)) / 60.0) / n
        print(f"  {h:02d}:00Z {pct:5.0f}%  " + "#" * int(pct / 5))
    free = {m for m in range(1440) if 100.0 * occ[m] / n <= args.tol}
    best = (0, None)
    for s in sorted(free):
        if (s - 1) % 1440 in free:
            continue
        ln, m = 0, s
        while m % 1440 in free and ln <= 1440:
            ln, m = ln + 1, m + 1
        if ln > best[0]:
            best = (ln, s)
    if best[1] is None:
        print(f"\nno block is free on more than {100 - args.tol:.0f}% of days")
    else:
        ln, s = best
        e = (s + ln) % 1440
        print(f"\nlargest block occupied on <= {args.tol:.0f}% of days: "
              f"{s // 60:02d}:{s % 60:02d}-{e // 60:02d}:{e % 60:02d}Z ({ln} min); "
              f"a {MEASURED_PASS_MIN} min pass {'fits' if ln >= MEASURED_PASS_MIN else 'does NOT fit'}")


def _in_runner_free(t):
    v = t.hour * 60 + t.minute
    return any((a <= v < b) if a < b else (v >= a or v < b) for a, b in RUNNER_FREE_HISTORICAL)


def cmd_collisions(args):
    rows = []
    for wf, hours in g.WRITERS.items():
        rows += [(s, wf, l) for s, l in _lags(_gh(wf, "startedAt,createdAt"), hours)]
    rows.sort()
    newest = rows[-1][0]
    print("a 'collision' here is a cloud run STARTING inside a window the runner's static blackouts leave open,")
    print("i.e. while a desktop pass that began when the blackout lifted is still holding the state store.\n")
    for days, label in ((7, "last 7 days"), (14, "last 14 days"), (10 ** 4, "all history")):
        sel = [r for r in rows if r[0] >= newest - dt.timedelta(days=days)]
        if not sel:
            continue
        hit = sum(1 for s, _w, _l in sel if _in_runner_free(s))
        print(f"  {label:<14} n={len(sel):>3}  median lag {_pct([l for _s, _w, l in sel], .5):>4.0f} min  "
              f"started inside a desktop window: {hit:>3} of {len(sel):>3} = {100.0 * hit / len(sel):>5.1f}%")
    print(f"\nmost recent {args.show} morning daily starts:")
    for s, wf, l in [r for r in rows if r[1] == "updater-daily.yml" and 4 <= (s := r[0]).hour <= 14][-args.show:]:
        print(f"  {s:%Y-%m-%d %H:%M}Z  {l:>4.0f} min late  {'INSIDE a desktop window' if _in_runner_free(s) else ''}")


def cmd_replay(args):
    raw = {wf: _gh(wf, "createdAt,updatedAt,status") for wf in g.WRITERS}
    active = {wf: "active" for wf in g.WRITERS}

    def runs_as_of(now):
        out = {}
        for wf, rows in raw.items():
            seen = []
            for r in rows:
                c = _parse(r.get("createdAt"))
                if c is None or c > now:
                    continue
                en = _parse(r.get("updatedAt"))
                done = en is not None and en <= now and r.get("status") == "completed"
                seen.append({"createdAt": c.strftime("%Y-%m-%dT%H:%M:%SZ"), "event": "schedule",
                             "status": "completed" if done else "in_progress"})
            seen.sort(key=lambda r: r["createdAt"], reverse=True)
            out[wf] = seen[:g.RUN_LIMIT]
        return out

    newest = max(_parse(r["createdAt"]) for rows in raw.values() for r in rows)
    end_day = newest.replace(hour=0, minute=0, second=0, microsecond=0)
    begin = end_day - dt.timedelta(days=args.days - 1)
    print(f"replaying {args.days} days ending {end_day:%Y-%m-%d}; a desktop pass measures {MEASURED_PASS_MIN} min")
    print("(walked continuously: a clear block that spans midnight is ONE block, which is the whole point -")
    print(" segmenting by calendar day reported 0 full passes where there were 8)\n")

    step = dt.timedelta(minutes=args.step)
    blocks, cur, t = [], None, begin
    while t < newest:
        clear = not g.decide(runs_as_of(t), t, active)[0]
        if clear and cur is None:
            cur = t
        elif not clear and cur is not None:
            blocks.append((cur, t))
            cur = None
        t += step
    if cur is not None:
        blocks.append((cur, t))

    total_clear = sum(int((b - a).total_seconds() // 60) for a, b in blocks)
    usable = [(a, b) for a, b in blocks if (b - a).total_seconds() // 60 >= MEASURED_PASS_MIN]
    for a, b in blocks:
        mins = int((b - a).total_seconds() // 60)
        if mins < 30:
            continue
        print(f"  {a:%Y-%m-%d %H:%M}Z -> {b:%m-%d %H:%M}Z  {mins:>4} min"
              f"{'   A FULL PASS FITS' if mins >= MEASURED_PASS_MIN else ''}")
    print(f"\nmean clear time {total_clear / args.days:.0f} min/day; "
          f"{len(usable)} of {len(blocks)} clear blocks are long enough for a full {MEASURED_PASS_MIN} min pass")
    print("A shortened pass is not a lost pass: the giants resume part-by-part (R190) and the runner clamps its")
    print("budget to the time available, so a clamped pass defers work rather than discarding it.")


def cmd_windows(args):
    """Per-WINDOW occupancy: the share of days on which a writer was in flight at some point inside a window.

    Distinct from `occupancy`, which reports a per-minute share. A window free on 90% of days minute by minute
    can still be spoiled on most days as a window, and it is the window that a desktop pass needs.
    """
    spans, newest = [], None
    for wf in g.WRITERS:
        for r in _gh(wf, "startedAt,updatedAt,createdAt,status"):
            st, en = _start(r), _parse(r.get("updatedAt"))
            if st is None:
                continue
            newest = st if newest is None else max(newest, st)
            if r.get("status") == "completed" and en and en > st:
                spans.append((st, en))
    named = dict(w.split("=", 1) for w in args.window)
    print(f"newest run start {newest:%Y-%m-%d %H:%M}Z; {len(spans)} finished runs\n")
    print(f"{'window':<28}" + "".join(f"{str(d) + ' days':>16}" for d in args.days))
    for name, rng in named.items():
        lo_s, hi_s = rng.split("-")
        lo = int(lo_s[:2]) * 60 + int(lo_s[2:])
        hi = int(hi_s[:2]) * 60 + int(hi_s[2:])
        cells = []
        for days in args.days:
            cut = newest - dt.timedelta(days=days)
            sel = [s for s in spans if s[1] >= cut]
            touched = examined = 0
            d = min((s[0] for s in sel), default=newest).date()
            while d <= newest.date():
                start = dt.datetime(d.year, d.month, d.day, lo // 60, lo % 60, tzinfo=dt.timezone.utc)
                end = start + dt.timedelta(minutes=(hi - lo) % 1440)
                if start >= cut and end <= newest:
                    examined += 1
                    touched += 1 if any(st < end and en > start for st, en in sel) else 0
                d += dt.timedelta(days=1)
            cells.append(f"{touched}/{examined} = {100.0 * touched / examined:.1f}%" if examined else "n/a")
        print(f"{name:<28}" + "".join(f"{c:>16}" for c in cells))
    print("\nShare of days a cloud writer was in flight inside the window. Each such day costs a pass (R5).")


def cmd_sweep(args):
    """Replay the runner's start rule over real history and score the (want_block, max_hours) pair.

    The rule: eligible once MIN_HOURS have passed since the last pass ENDED; then wait for a window with at
    least `want_block` usable minutes; start regardless once `max_hours` have passed. Scored on collisions
    first (a cloud run starting inside a desktop pass costs one side its whole bookkeeping, R5) and then on
    pass minutes delivered.

    What this simulation ASSUMES, so its numbers are read with the right caution: every pass succeeds; a pass
    lasts min(usable, FULL_PASS); the stamp is written at the pass END; and the cloud history is replayed
    unchanged, though a differently-behaving desktop would in reality have changed which cloud pushes won.
    It is a comparison between rules on identical inputs, not a forecast.
    """
    raw = {wf: _gh(wf, "createdAt,updatedAt,status") for wf in g.WRITERS}
    cloud = sorted(_parse(r["createdAt"]) for rows in raw.values() for r in rows if r.get("createdAt"))
    newest = max(cloud)
    start_at = (newest - dt.timedelta(days=args.days)).replace(hour=0, minute=0, second=0, microsecond=0)
    active = {wf: "active" for wf in g.WRITERS}
    tick = dt.timedelta(minutes=5)
    cache = {}

    def runs_as_of(now):
        key = now.replace(second=0, microsecond=0)
        if key not in cache:
            out = {}
            for wf, rows in raw.items():
                seen = []
                for r in rows:
                    c = _parse(r.get("createdAt"))
                    if c is None or c > now:
                        continue
                    en = _parse(r.get("updatedAt"))
                    done = en is not None and en <= now and r.get("status") == "completed"
                    seen.append({"createdAt": c.strftime("%Y-%m-%dT%H:%M:%SZ"), "event": "schedule",
                                 "status": "completed" if done else "in_progress"})
                seen.sort(key=lambda r: r["createdAt"], reverse=True)
                out[wf] = seen[:g.RUN_LIMIT]
            cache[key] = out
        return cache[key]

    def run(want, maxh):
        last, passes, now = start_at - dt.timedelta(hours=args.min_hours), [], start_at
        while now < newest:
            since = (now - last).total_seconds() / 3600.0
            if since < args.min_hours:
                now += tick
                continue
            rr = runs_as_of(now)
            if g.decide(rr, now, active)[0]:
                now += tick
                continue
            usable = g.minutes_until_block(rr, now, active) - args.margin
            if usable < args.min_usable or (usable < want and since < maxh):
                now += tick
                continue
            end = now + dt.timedelta(minutes=min(usable, MEASURED_PASS_MIN))
            passes.append((now, end))
            last, now = end, end
        return passes

    print(f"{args.days} days, {start_at:%Y-%m-%d}..{newest:%Y-%m-%d}; cadence floor {args.min_hours} h, "
          f"margin {args.margin} min, full pass {MEASURED_PASS_MIN} min\n")
    print(f"{'want':>6}{'maxh':>6}{'passes':>8}{'pass min':>10}{'mean':>7}{'full':>6}{'collisions':>12}")
    best = None
    for want in args.want:
        for maxh in args.maxh:
            p = run(want, maxh)
            total = sum(int((b - a).total_seconds() // 60) for a, b in p)
            full = sum(1 for a, b in p if (b - a).total_seconds() // 60 >= MEASURED_PASS_MIN)
            coll = sum(1 for s in cloud for a, b in p if a < s < b)
            flag = ""
            if best is None or (coll, -total) < best[0]:
                best, flag = ((coll, -total), want, maxh), "  <- best"
            print(f"{want:>6}{maxh:>6}{len(p):>8}{total:>10}{(total / len(p) if p else 0):>7.0f}"
                  f"{full:>6}{coll:>12}{flag}")
    print(f"\nbest by (fewest collisions, then most pass minutes): "
          f"want_block={best[1]} max_hours={best[2]}")
    for a, b in run(best[1], best[2]):
        hit = [s for s in cloud if a < s < b]
        print(f"  {a:%Y-%m-%d %H:%M}Z -> {b:%H:%M}Z  {int((b - a).total_seconds() // 60):>4} min" +
              (f"   COLLISION at {hit[0]:%H:%M}Z" if hit else ""))


# ======================================================================================================
# 6.1 REPLAY: the W windows, D3's no-progress back-off and the desktop state lease (starvation plan rev 9,
# section 6.1, reviewed AR-393 round 9). Every function from here to cmd_fetch is a MODEL; the inputs are frozen
# files (`fetch`, the desktop wrapper logs, one state copy), so a re-run on the same files gives the same numbers.
#
# What is held from history, per cloud run: when GitHub CREATED it (the arrival), how long its jobs ran once the
# first one started (queue time excluded, job level - section 2.3 of the plan: a queued run reads the state when its
# job STARTS), when inside that it reads the state (updater-daily: the "Pull state" step; updater-heavy: the `setup`
# job, where A puts its one lease check), when its last push ended, and whether a push succeeded (A5's `completed`).
# What is re-simulated: the shared `aqueduct-updater` concurrency group (one run in progress, at most ONE pending,
# a newer arrival evicts the pending one - updater-heavy.yml 51-55), the lease (A1-A3), the fairness rule (A3.2),
# re-dispatch (A3.6), the runner's start rule (run_local_heavy.ps1), and the orchestrator's order and admission over
# the local units (production order_units / overdue_key / last_turn_utc / is_due / run_cost_estimate are CALLED, not
# copied; the admission reserve is a two-line formula inside run_once and is restated with its line number).
# The PR 2 extensions (see WHOLE_FLOW below) are candidate RULES, not production: --order attempt hands
# last_turn_utc a different stamp, --due-from-attempt calls production's own partial clock (base._due_after) for one
# more class of unit, and --drop-cron edits the schedule. Each is off by default.
# ======================================================================================================
import heapq          # noqa: E402
import math           # noqa: E402
import re             # noqa: E402
import sqlite3        # noqa: E402
import statistics     # noqa: E402
from bisect import bisect_left, bisect_right   # noqa: E402
from dataclasses import asdict, dataclass, replace   # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.append(ROOT)          # for the production updater.* functions the unit book calls

# run_local_heavy.ps1, read at origin/main e2e69cf41 (the values the replay's "today" must reproduce)
PS1_MARGIN_MIN = 25          # $marginMin
PS1_MIN_USABLE = 20          # `if ($untilCron -lt 20)` -> ABORT
PS1_WANT_BLOCK = 150         # $WantBlockMin
PS1_MAX_HOURS = 36           # $MaxHours
PS1_MIN_HOURS = 20           # $MinHours
PS1_GRACE_MIN = 10           # $graceMin (the HARD STOP fires this long after the budget)
PS1_BUDGET_CAP = 2880        # AQUEDUCT_RUN_BUDGET_MIN default before the clamp
# orchestrate.run_once admission (updater/orchestrate.py 2548-2566): need = min(2 x timeout, max(2 x 1.5 x est, 10))
# with the timeout `_unit_timeout_min()` = 45 when AQUEDUCT_UNIT_TIMEOUT_MIN is unset; W never sets it (plan W rev 3).
UNIT_TIMEOUT_MIN = 45.0
# Plan 3.1 / 3.2: re-read wait W = 120 s; lease expiry E = 2 x 31.3 min; the holder's push refuses 2 min before.
LEASE_W_MIN = 2.0
LEASE_E_MIN = 63.0
# The resumable list of plan W5 (bea, census, istat by their Deadlines; wid, ons_uk, imf_imts_direct, unctad_* once
# their stop points land - W3b). "W on" clocks exactly these; "W off" clocks none, which is what W does in the near
# term (W3b: CLOCKED ships with every margin None).
W_CLOCKED = ("bea", "census", "istat", "wid", "ons_uk", "imf_imts_direct")
W_CLOCKED_PREFIX = ("unctad_",)
# A finished turn of these sources advances nothing the progress probe reads (tools/unit_progress_probe.py diffs
# last_success_utc and upstream_vintage). Evidence: istat's partial note "vintage not advanced", the 10-24 pass that
# ran only istat (13,751 s) and stamped nothing, and the 10-25 pass that finished istat, bea, bls, eia and reported
# "3 unit(s) advanced".
NO_ADVANCE = frozenset({"istat"})
# Not work: statcan is a 2-s reporter (jobs/statcan_lane.py does its work, plan section 1); cbs_nl and gus_dbw are
# PROTECTED (orchestrate.FIRSTPASS_DIRS) and never attempted.
NOT_WORK = frozenset({"statcan", "cbs_nl", "gus_dbw"})
# PLAN rev 10 R10.2/R10.3 item 2 (PR 2): replay extensions. Every one is OFF by default, and with all of them off
# the output is the output of PR #200 byte for byte (rows, labels, json).
#   --order attempt      B2: staleness from the later of last_success, last_attempt and the last kill (production
#                        orders by last_turn_utc, which reads last_success first). It changes the ORDER only.
#   --clocked-extra SRC  treat SRC as clocked/resumable like W_CLOCKED (only while W is on, as for W_CLOCKED).
#   --flow-bound SRC=MIN a stop keeps only WHOLE pieces of MIN minutes (a giant's per-flow loop stops between
#                        flows, _giant.py). A source in WHOLE_FLOW must have one: oecd's largest flow is 1.79B rows
#                        (ledger R244), so its work is never divisible minutes.
#   --drop-cron WF:HH    remove that cron from the gate model AND the scheduled history runs credited to it.
#   --cloud-floor N      completed runs/day per writer in the strict rule (default 2, the plan 6.1 floor).
#   --due-from-attempt   a partial behind an older success is due on the 7-day partial clock from its attempt.
WHOLE_FLOW = frozenset({"oecd"})
ORDERS = ("production", "attempt")


def _ts(s):
    if not s:
        return None
    d = _parse(s)
    return d if d.year >= 2000 else None


def _stamp_or_none(s):
    """A state stamp as an aware datetime (offset-less read as UTC, as orchestrate.last_turn_utc does), or None."""
    if not s:
        return None
    try:
        d = dt.datetime.fromisoformat(str(s))
    except (ValueError, TypeError):
        return None
    return d if d.tzinfo else d.replace(tzinfo=dt.timezone.utc)


def _mins(a, b) -> float:
    return (b - a).total_seconds() / 60.0


def _iso(t: dt.datetime) -> str:
    return t.astimezone(dt.timezone.utc).isoformat(timespec="seconds")


@dataclass
class CloudRun:
    wf: str
    run_id: int
    event: str
    created: dt.datetime
    status: str
    conclusion: str
    exec_min: float          # first job start -> run end (queue excluded)
    read_off_min: float      # first job start -> the state read (daily "Pull state"; heavy `setup`)
    push_off_min: float      # first job start -> end of its last successful "Push state" step (else exec_min)
    completed_ok: bool       # A5 `completed`: daily = a "Push state" step succeeded; heavy = run conclusion success
    pushes: int              # successful "Push state" steps (every one writes a backup, plan 2.4)
    hist_start: dt.datetime | None
    hist_end: dt.datetime | None
    has_jobs: bool
    dispatched: bool = False  # created by the replay's own re-dispatch (A3.6), not by history


def _cloud_run(wf: str, r: dict, jobs) -> CloudRun:
    created = _parse(r["createdAt"])
    end = _parse(r["updatedAt"]) if r.get("status") == "completed" else None
    starts = [t for t in (_ts(j.get("startedAt")) for j in (jobs or [])) if t]
    if jobs is None:
        first = _ts(r.get("startedAt")) or created      # run level only: startedAt == createdAt (plan 2.3)
    else:
        first = min(starts) if starts else None
    exec_min = max(0.0, _mins(first, end)) if (end and first) else 0.0
    read_off, push_off, pushes = 0.0, exec_min, 0
    if jobs and first:
        steps = [s for j in jobs for s in (j.get("steps") or [])]
        if wf == "updater-daily.yml":
            ps = sorted(t for t in (_ts(s.get("startedAt")) for s in steps if s["name"].startswith("Pull state")) if t)
            if ps:
                read_off = max(0.0, _mins(first, ps[0]))
        else:
            su = [t for t in (_ts(j.get("startedAt")) for j in jobs if j.get("name") == "setup") if t]
            if su:
                read_off = max(0.0, _mins(first, min(su)))
        pe = [t for t in (_ts(s.get("completedAt")) for s in steps
                          if s["name"].startswith("Push state") and s.get("conclusion") == "success") if t]
        pushes = len(pe)
        if pe:
            push_off = max(0.0, _mins(first, max(pe)))
    if wf == "updater-daily.yml":
        ok = pushes >= 1 if jobs is not None else r.get("conclusion") in ("success", "failure")
    else:
        ok = r.get("conclusion") == "success"
    return CloudRun(wf, int(r["databaseId"]), r.get("event", ""), created, r.get("status", ""),
                    r.get("conclusion") or "", exec_min, read_off, push_off, bool(ok and end), pushes, first, end,
                    jobs is not None)


def load_cloud_history(hist_dir: str) -> list:
    """Every run of both writers from a `fetch` directory, oldest first."""
    out = []
    for wf in g.WRITERS:
        with open(os.path.join(hist_dir, f"runs_{wf}.json"), encoding="utf-8") as f:
            rows = json.load(f)
        for r in rows:
            path = os.path.join(hist_dir, "jobs", f"{r['databaseId']}.json")
            jobs = None
            if os.path.exists(path):
                with open(path, encoding="utf-8") as f:
                    jobs = json.load(f).get("jobs")
            out.append(_cloud_run(wf, r, jobs))
    out.sort(key=lambda x: (x.created, x.run_id))
    return out


def cron_blocks(runs: list, since: dt.datetime, until: dt.datetime, writers: dict | None = None) -> list:
    """[(cron, end, wf)]: ci_writer_gate.assess's pending rule as intervals. A cron blocks from its time until a
    SCHEDULE run of that writer is created within PENDING_LIMIT of it, at most PENDING_LIMIT. Under the lease it does
    not block at all (A); scheduled runs are created at their historical times in every scenario, so this list is
    the same in all of them. `writers` ({wf: cron hours}) defaults to the gate's WRITERS (--drop-cron passes fewer)."""
    writers = g.WRITERS if writers is None else writers
    created = {wf: sorted(r.created for r in runs if r.wf == wf and r.event == "schedule") for wf in writers}
    out = []
    day = (since - dt.timedelta(days=1)).date()
    while dt.datetime(day.year, day.month, day.day, tzinfo=dt.timezone.utc) <= until:
        for wf, hours in writers.items():
            for h in hours:
                cron = dt.datetime(day.year, day.month, day.day, h, tzinfo=dt.timezone.utc)
                c = created[wf]
                i = bisect_left(c, cron)
                c1 = c[i] if i < len(c) and c[i] < cron + g.PENDING_LIMIT else None
                end = c1 if c1 is not None else cron + g.PENDING_LIMIT
                if end > cron:
                    out.append((cron, end, wf))
        day += dt.timedelta(days=1)
    return sorted(out)


def _merge(iv: list) -> list:
    out = []
    for s, e in sorted(iv):
        if out and s <= out[-1][1]:
            out[-1][1] = max(out[-1][1], e)
        else:
            out.append([s, e])
    return out


def free_windows(runs: list, since: dt.datetime, until: dt.datetime, pending: bool = True) -> list:
    """The gate's free windows over history, as intervals: a run blocks from createdAt until it completed (any
    event - a dispatch writes the state too), a cron per cron_blocks. Same model as the planner's analyze_runs.py
    (plan 2.2), cross-checked minute by minute against the production assess() by gate_minutes_production."""
    busy = [(max(r.created, since), min(r.hist_end or until, until)) for r in runs
            if (r.hist_end or until) > since and r.created < until]
    if pending:
        busy += [(max(c, since), min(e, until)) for c, e, _w in cron_blocks(runs, since, until) if e > since and c < until]
    free, t = [], since
    for s, e in _merge([b for b in busy if b[1] > b[0]]):
        if s > t:
            free.append((t, s))
        t = max(t, e)
    if t < until:
        free.append((t, until))
    return free


def window_stats(free: list, since: dt.datetime, until: dt.datetime, margin: float = PS1_MARGIN_MIN) -> dict:
    fm = [_mins(a, b) for a, b in free]
    days = _mins(since, until) / 1440.0
    return {"days": days, "free_min": sum(fm), "per_day": sum(fm) / days if days else 0.0, "windows": len(fm),
            "ge90": sum(1 for m in fm if m - margin >= 90), "ge150": sum(1 for m in fm if m - margin >= 150),
            "longest": max(fm) if fm else 0.0}


def _as_of_lists(runs: list):
    """Pre-sorted views so the production assess() can be asked about any instant without rebuilding history."""
    per = {}
    for wf in g.WRITERS:
        rs = sorted((r for r in runs if r.wf == wf), key=lambda r: r.created)
        per[wf] = (rs, [r.created for r in rs], [r for r in rs if r.event == "schedule"],
                   [r.created for r in rs if r.event == "schedule"])
    return per


def _runs_as_of(per: dict, now: dt.datetime):
    unf, sch = {}, {}
    for wf, (rs, cs, ss, scs) in per.items():
        def row(r):
            done = r.hist_end is not None and r.hist_end <= now
            return {"createdAt": r.created.strftime("%Y-%m-%dT%H:%M:%SZ"), "event": r.event,
                    "status": "completed" if done else "in_progress"}
        i = bisect_right(cs, now)
        unf[wf] = [row(r) for r in reversed(rs[max(0, i - g.RUN_LIMIT):i])]
        j = bisect_right(scs, now)
        sch[wf] = [row(r) for r in reversed(ss[max(0, j - g.SCHED_LIMIT):j])]
    return unf, sch


def gate_minutes_production(runs: list, since: dt.datetime, until: dt.datetime, step_min: int = 1) -> list:
    """Free windows from the PRODUCTION ci_writer_gate.assess(), asked once per `step_min` minute with the
    listings as gh would have returned them at that instant (unfiltered newest RUN_LIMIT, scheduled newest
    SCHED_LIMIT). The control for free_windows: two instruments, one answer (R0 rule 1)."""
    per = _as_of_lists(runs)
    free, cur, t = [], None, since
    step = dt.timedelta(minutes=step_min)
    while t < until:
        unf, sch = _runs_as_of(per, t)
        clear = not g.assess(unf, t, None, sch)[0]
        if clear and cur is None:
            cur = t
        elif not clear and cur is not None:
            free.append((cur, t))
            cur = None
        t += step
    if cur is not None:
        free.append((cur, until))
    return free


def until_block_fast(t: dt.datetime, writers: dict | None = None) -> int:
    """minutes_until_block() after a CLEAR answer: every cron of the last PENDING_LIMIT has its run (else the gate
    would block now) and nothing is in flight, so the frozen walk first blocks at the next cron of either writer,
    found on the walk's own 5-minute grid from `t`. Checked against the production function in `replay61 --check`.
    `writers` defaults to the gate's WRITERS; with no cron left at all the walk's 720-min ceiling answers."""
    best = None
    for hours in (g.WRITERS if writers is None else writers).values():
        for back in (0, 1):
            d = (t + dt.timedelta(days=back)).date()
            for h in hours:
                c = dt.datetime(d.year, d.month, d.day, h, tzinfo=dt.timezone.utc)
                if c > t and (best is None or c < best):
                    best = c
    if best is None:
        return 720
    k = math.ceil(_mins(t, best) / 5.0 - 1e-9)
    return int(min(k * 5, 720))


def writers_without(drops) -> dict:
    """The gate's WRITERS with the (wf, hour) crons in `drops` removed. An unknown writer or hour is an error, so
    a typo cannot silently drop nothing."""
    out = {wf: tuple(hs) for wf, hs in g.WRITERS.items()}
    for wf, h in drops:
        if wf not in out or h not in out[wf]:
            raise ValueError(f"--drop-cron {wf}:{h}: not a cron of the gate's WRITERS {g.WRITERS}")
        out[wf] = tuple(x for x in out[wf] if x != h)
    return out


def drop_cron_runs(runs: list, wf: str, hour: int) -> tuple:
    """C (plan R10.2): the SCHEDULED runs of `wf` credited to its `hour`:00Z cron, removed from history.

    A run is credited to a cron by the gate's own rule (cron_blocks, ci_writer_gate's `started` test): the first
    scheduled run of that writer created in [cron, cron + PENDING_LIMIT). A writer's crons are PENDING_LIMIT apart,
    so the windows do not overlap and a run is credited to at most one cron. The gate's KNOWN RESIDUAL applies: a run
    more than 12 h late is credited to the next cron. Manual dispatches are never credited to a cron and stay, and so
    does a second scheduled run inside one window. Returns (kept, dropped)."""
    writers_without([(wf, hour)])                                   # validates wf and hour
    sched = sorted((r for r in runs if r.wf == wf and r.event == "schedule"), key=lambda r: (r.created, r.run_id))
    cs = [r.created for r in sched]
    dropped = []
    if sched:
        day = (cs[0] - dt.timedelta(days=1)).date()
        while dt.datetime(day.year, day.month, day.day, tzinfo=dt.timezone.utc) <= cs[-1]:
            cron = dt.datetime(day.year, day.month, day.day, hour, tzinfo=dt.timezone.utc)
            i = bisect_left(cs, cron)
            if i < len(cs) and cs[i] < cron + g.PENDING_LIMIT:
                dropped.append(sched[i])
            day += dt.timedelta(days=1)
    gone = {id(r) for r in dropped}
    return [r for r in runs if id(r) not in gone], dropped


# ------------------------------------------------------------------ desktop logs: margins, tail, probe, passes

_HMS = re.compile(r"^﻿?\[(\d\d):(\d\d):(\d\d)\] (.*)$")


def _stamp_dt(stamp: str) -> dt.datetime:
    return dt.datetime.strptime(stamp, "%Y%m%d-%H%M%S").replace(tzinfo=dt.timezone.utc)


def parse_wrapper_log(path: str) -> dict:
    """One run_local_heavy wrapper log -> its timed events. Times carry only HH:MM:SS, so the date comes from the
    file's stamp and advances at each wrap past midnight."""
    stamp = os.path.basename(path)[len("local_heavy_"):-len(".log")]
    base = _stamp_dt(stamp)
    day, last, ev = base.replace(hour=0, minute=0, second=0), None, []
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            m = _HMS.match(line.rstrip("\r\n"))
            if not m:
                continue
            t = day.replace(hour=int(m.group(1)), minute=int(m.group(2)), second=int(m.group(3)))
            if last is not None and t < last - dt.timedelta(hours=1):
                day += dt.timedelta(days=1)
                t += dt.timedelta(days=1)
            last = t
            ev.append((t, m.group(4)))
    out = {"stamp": stamp, "start": base, "events": ev}
    for t, msg in ev:
        if msg.startswith("pull-state ..."):
            out["pull_at"] = t
        elif msg.startswith("running updater"):
            out["updater_at"] = t
        elif msg.startswith("push-state ..."):
            out["push_at"] = t
        elif msg.startswith("cadence stamped") or msg.startswith("cadence NOT stamped") or \
                msg.startswith("push-state FAILED"):
            out.setdefault("push_done", t)
            out["stamped"] = msg.startswith("cadence stamped")
        elif msg.startswith("whole-run budget") or msg.startswith("per-source budget override"):
            m2 = re.search(r"whole-run budget: (\d+) min", msg)
            if m2:
                out["budget"] = int(m2.group(1))
        elif msg.startswith("HARD STOP"):
            out["hard_stop"] = True
    return out


def desktop_passes(logdir: str, since_stamp: str = "") -> list:
    """Every wrapper log that pulled state (a pass), oldest first. Plain listdir + sort, never mtime order."""
    names = sorted(n for n in os.listdir(logdir)
                   if re.fullmatch(r"local_heavy_\d{8}-\d{6}\.log", n) and n[len("local_heavy_"):] >= since_stamp)
    out = []
    for n in names:
        p = parse_wrapper_log(os.path.join(logdir, n))
        if "pull_at" in p:
            out.append(p)
    return out


def pass_overheads(passes: list) -> dict:
    """Pull (pull-state -> updater start) and push (push-state -> the stamp line) minutes, measured per pass."""
    pull = [_mins(p["pull_at"], p["updater_at"]) for p in passes if "updater_at" in p]
    push = [_mins(p["push_at"], p["push_done"]) for p in passes if "push_at" in p and "push_done" in p]
    return {"n_pull": len(pull), "n_push": len(push),
            "pull_median": statistics.median(pull) if pull else float("nan"),
            "push_median": statistics.median(push) if push else float("nan"),
            "pull_max": max(pull) if pull else float("nan"), "push_max": max(push) if push else float("nan")}


_DERIVE_SPENT = re.compile(r"derive budget of ([0-9.]+) min spent")
_TOOK = re.compile(r"^\[orchestrator\] <<< ([^/\s]+)/\S+ took ([0-9,]+)s")
_START = re.compile(r"^\[orchestrator\] >>> ([^/\s]+)/")
_TIMED = re.compile(r"(probe|detect_change|csv phase|finaliz)\w*[^\n]*?\b[0-9.]+ ?s\b", re.I)


def measure_tail_probe(logdir: str) -> dict:
    """What the updater logs say about W's `tail` (CSV phase + finalize) and `P` (probe), plan W2.

    The logs carry NO phase timestamps, so neither is measured directly. What they do carry:
      - per unit, `derive budget of N min spent` each time a CSV phase ran its whole derive budget - main derive
        and the csv-retry drain each have one, so a unit run with two such lines spent >= 2 x N in CSV phases;
      - `<<< src took Ns` for every unit that finished.
    So `tail` gets a measured LOWER bound (the largest sum of spent derive budgets in one unit run); the code's
    upper bound per CSV phase is the fence min(60, rem + 2) (orchestrate._csv_fence_min). `P` cannot be bounded
    from the logs at all: the count of timed probe/CSV lines is returned so the reader can see it is zero."""
    names = sorted(n for n in os.listdir(logdir) if re.fullmatch(r"local_heavy_updater_\d{8}-\d{6}\.log", n))
    unit_runs, spent, timed = 0, [], 0
    for n in names:
        cur, acc = None, 0.0
        with open(os.path.join(logdir, n), encoding="utf-8", errors="replace") as f:
            for line in f:
                m = _START.match(line)
                if m:
                    cur, acc = m.group(1), 0.0
                    unit_runs += 1
                    continue
                m = _DERIVE_SPENT.search(line)
                if m and cur:
                    acc += float(m.group(1))
                if _TIMED.search(line):
                    timed += 1
                m = _TOOK.match(line)
                if m and cur == m.group(1):
                    if acc > 0:
                        spent.append((acc, n, cur, int(m.group(2).replace(",", ""))))
                    cur, acc = None, 0.0
    spent.sort(reverse=True)
    return {"logs": len(names), "unit_runs": unit_runs, "csv_budget_hits": spent,
            "tail_lower_bound_min": spent[0][0] if spent else 0.0, "timed_phase_lines": timed}


_GIANT_PROGRESS = r"^\[{sid}\] (\d+)/(\d+) flows\b.*?, ([0-9,]+)s\s*$"


def giant_flow_blocks(logdir: str, sid: str) -> dict:
    """What the desktop logs say about the time of ONE flow of giant `sid` (for --clocked-extra in whole flows).

    _giant.run_giant prints a progress line every 25 flows - "[oecd] 50/144 flows - +14,126 rows, ..., 4,163s" -
    with the seconds since the unit started. No line times a single flow. So the logs give BLOCKS: the time between
    two progress lines of one unit run (the first block also holds the catalogue read). Every flow inside a block
    took at most the block's time, so the largest block is the only per-flow bound the logs can give, and it holds
    only for the flows that were timed; a flow that never finished inside a block (a killed run's last flows) is not
    bounded by anything. Reads the desktop updater logs (local_heavy_updater_*.log) and the older guard loop logs
    (guard_localheavy_*.log), both written on this machine. Returns the blocks, largest first."""
    prog = re.compile(_GIANT_PROGRESS.format(sid=re.escape(sid)))
    start = re.compile(r"^\[orchestrator\] >>> " + re.escape(sid) + r"/")
    names = sorted(n for n in os.listdir(logdir)
                   if re.fullmatch(r"(local_heavy_updater_\d{8}-\d{6}|guard_localheavy_\d{4}_\d{4})\.log", n))
    blocks, unit_runs = [], 0
    for n in names:
        prev = None
        with open(os.path.join(logdir, n), encoding="utf-8", errors="replace") as f:
            for ln, line in enumerate(f, 1):
                line = line.rstrip("\r\n")
                if start.match(line):
                    prev, unit_runs = (0, 0.0), unit_runs + 1
                    continue
                m = prog.match(line)
                if m and prev is not None:
                    k, s = int(m.group(1)), float(m.group(3).replace(",", ""))
                    if k > prev[0] and s >= prev[1]:
                        blocks.append(((s - prev[1]) / 60.0, n, ln, prev[0], k))
                    prev = (k, s)
    blocks.sort(reverse=True)
    return {"files": len(names), "unit_runs": unit_runs, "blocks": blocks,
            "largest_min": blocks[0][0] if blocks else None}


# ------------------------------------------------------------------ the local units (one state copy)

@dataclass
class SimUnit:
    source_id: str
    cadence: str
    unit_id: str = "_all"

    @property
    def key(self):
        return f"{self.source_id}/{self.unit_id}"

    @property
    def config(self):
        return {"cadence": self.cadence}


class _StoreShim:
    """StateStore.run_cost_estimate / last_kill_utc read only `self.db`; this lets the replay call them unchanged."""
    def __init__(self, db):
        self.db = db


def local_units_from_registry(path: str | None = None) -> dict:
    """{source_id: cadence} for run_location: local, from updater/registry.yaml (the list run_local_heavy.ps1 uses,
    via tools/_list_local_sources.py), minus NOT_WORK."""
    import yaml
    path = path or os.path.join(ROOT, "updater", "registry.yaml")
    with open(path, encoding="utf-8") as f:
        d = yaml.safe_load(f) or {}
    return {e["source_id"]: (e.get("cadence") or "monthly") for e in (d.get("sources") or [])
            if e.get("run_location") == "local" and e.get("source_id") and e["source_id"] not in NOT_WORK}


class UnitBook:
    """The owed work of the local units, advanced pass by pass with the production scheduling rules.

    Work per turn (minutes) = the duration of the unit's NEWEST `runs` row in the copy (the plan's q_owed rule; a
    killed or transient row is a censored LOWER bound, listed in `censored`). A finished turn's status is the newest
    ok / no_change / partial row of that source (`outcome`; 'ok' when there is none - listed in `assumed_ok`)."""

    def __init__(self, cadences: dict, state: dict, runs_rows: list, work: dict, outcome: dict,
                 censored=(), assumed_ok=(), order: str = "production", clocked_extra=(), whole_flow=None,
                 due_from_attempt: bool = False):
        from updater.strategies.base import Strategy
        from updater.strategies import base
        from updater import orchestrate as orch
        from updater.state import StateStore
        self._is_due = Strategy.is_due
        self._base = base
        self._orch, self._store = orch, StateStore
        if order not in ORDERS:
            raise ValueError(f"order must be one of {ORDERS}")
        # The PR 2 extensions (see WHOLE_FLOW above); the defaults are PR #200's model exactly.
        self.order, self.clocked_extra = order, frozenset(clocked_extra)
        self.whole_flow = dict(whole_flow or {})
        self.due_from_attempt = due_from_attempt
        for s in self.clocked_extra & WHOLE_FLOW:
            if not self.whole_flow.get(s):
                raise ValueError(f"{s} is clocked only in whole flows: give its per-flow bound (whole_flow)")
        self.units = {s: SimUnit(s, c) for s, c in sorted(cadences.items())}
        self.state = {s: dict(state.get(s) or {}) for s in self.units}
        self.work = {s: float(work.get(s, 0.0)) for s in self.units}
        self.remaining = dict(self.work)
        self.outcome = {s: outcome.get(s, "ok") for s in self.units}
        self.censored, self.assumed_ok = set(censored), set(assumed_ok)
        self.db = sqlite3.connect(":memory:")
        self.db.execute("CREATE TABLE runs(id INTEGER PRIMARY KEY AUTOINCREMENT, ts_utc TEXT, source_id TEXT, "
                        "unit_id TEXT, status TEXT, obs INTEGER, dur_s REAL, note TEXT)")
        self.db.executemany("INSERT INTO runs(ts_utc, source_id, unit_id, status, dur_s) VALUES (?,?,?,?,?)",
                            [r for r in runs_rows if r[1] in self.units])
        self.attempts = Counter()
        self.finished = Counter()

    def is_due(self, sid: str, now: dt.datetime) -> bool:
        st = self.state[sid]
        if self.due_from_attempt and self._partial_behind_success(st):
            # --due-from-attempt (plan R10.2, istat's demand): production's partial-retry clock, from the attempt,
            # for a partial whose attempt is newer than an old success. Production is_due measures from
            # last_success_utc whenever one exists, so such a unit is due at every pass.
            b = self._base
            return b._due_after(min(b.CADENCE_DAYS.get(self.units[sid].cadence, 7), b.PARTIAL_RETRY_DAYS),
                                st.get("last_attempt_utc"), now)
        return self._is_due(None, self.units[sid], st, now)

    @staticmethod
    def _partial_behind_success(st: dict) -> bool:
        ls, la = st.get("last_success_utc"), st.get("last_attempt_utc")
        if st.get("status") != "partial" or not ls or not la:
            return False                     # no success on record: production already uses the partial clock
        a, s = _stamp_or_none(la), _stamp_or_none(ls)
        return a is not None and s is not None and a > s

    def _clocked(self, sid: str, w_on: bool) -> bool:
        return w_on and (sid in W_CLOCKED or sid.startswith(W_CLOCKED_PREFIX) or sid in self.clocked_extra)

    def _resumable(self, sid: str) -> bool:
        return sid in W_CLOCKED or sid.startswith(W_CLOCKED_PREFIX) or sid in self.clocked_extra

    def _kept(self, sid: str, minutes: float) -> float:
        """Progress a stop keeps out of `minutes` of update window: all of it, or for a whole-flow source only the
        whole pieces of its per-flow bound that fit (a flow in flight at the stop is lost)."""
        b = self.whole_flow.get(sid)
        if not b:
            return minutes
        return b * math.floor(minutes / b + 1e-9)

    def _last_turn(self, st: dict, kill):
        """The stamp the order sorts on. production: orchestrate.last_turn_utc (last_success, else last_attempt,
        or the kill if later). attempt (B2): the later of last_success and last_attempt, then the same production
        kill rule - only the stamp handed in changes."""
        if self.order == "production":
            return self._orch.last_turn_utc(st, kill)
        ls, la = st.get("last_success_utc"), st.get("last_attempt_utc")
        s, a = _stamp_or_none(ls), _stamp_or_none(la)
        later = la if (a is not None and (s is None or a > s)) else (ls or la)
        return self._orch.last_turn_utc({"last_success_utc": later}, kill)

    def owed(self, now: dt.datetime) -> float:
        return sum(self.remaining[s] for s in self.units if self.is_due(s, now))

    def due_set(self, now: dt.datetime) -> list:
        return [s for s in self.units if self.is_due(s, now)]

    def _row(self, t, sid, status, dur_min):
        self.db.execute("INSERT INTO runs(ts_utc, source_id, unit_id, status, dur_s) VALUES (?,?,?,?,?)",
                        (_iso(t), sid, "_all", status, dur_min * 60.0))

    def run_pass(self, start: dt.datetime, budget_min: float, w_on: bool = False, tail_min: float = 90.0,
                 kill_keeps_progress: bool = False, grace_min: float = PS1_GRACE_MIN) -> dict:
        """One updater run of the whole local route with budget `budget_min`, starting at `start`.

        Order: production order_units (cost band, then cadence-normalised staleness of last_turn_utc). Admission:
        orchestrate.py 2548-2566. A unit that is admitted and due runs its remaining work:
          - W off, or a unit not on the clocked list: to completion, or to the HARD STOP at budget + grace, where it
            is killed (a `killed_external` row, tools/record_killed_unit.py) and the pass ends; its progress is lost
            unless `kill_keeps_progress` (a sensitivity case - plan W5 says no unit has shown a persisted bookmark);
          - W on and clocked: to completion if it fits the time left, else stopped at the run deadline, keeping
            (time left - tail) minutes of progress (the update window is rem - tail, plan W2), recorded as a
            transient_fail (the UnitTimeout path)."""
        orch, store = self._orch, self._store
        shim = _StoreShim(self.db)
        costs = store.run_cost_estimate(shim)
        kills = store.last_kill_utc(shim)
        now_iso = _iso(start)

        def staleness(u):
            last = self._last_turn(self.state[u.source_id], kills.get((u.source_id, u.unit_id)))
            return (orch.overdue_key(last, u.cadence, now_iso), u.key)

        order = orch.order_units(list(self.units.values()), costs, staleness)
        e, advanced, hard, stopped, ran = 0.0, False, None, None, []
        for u in order:
            sid = u.source_id
            est = costs.get(sid)
            need = 2 * UNIT_TIMEOUT_MIN if est is None else min(2 * UNIT_TIMEOUT_MIN, max(2 * 1.5 * (est / 60.0),
                                                                                        10.0))
            if e + need > budget_min:
                continue                                   # BUDGET SKIP
            t_unit = start + dt.timedelta(minutes=e)
            if not self.is_due(sid, t_unit):
                continue                                   # NOT DUE
            self.attempts[sid] += 1
            R = self.remaining[sid]
            clocked = self._clocked(sid, w_on)
            limit = budget_min - e if clocked else budget_min + grace_min - e
            if R <= limit:
                e += R
                t_end = start + dt.timedelta(minutes=e)
                st, status = self.state[sid], self.outcome[sid]
                st["last_attempt_utc"], st["status"] = _iso(t_end), status
                if status in ("ok", "no_change"):
                    st["last_success_utc"] = _iso(t_end)
                self._row(t_end, sid, status, R)
                self.remaining[sid] = self.work[sid]
                self.finished[sid] += 1
                advanced = advanced or sid not in NO_ADVANCE
                ran.append((sid, R, status))
                continue
            t_end = start + dt.timedelta(minutes=e + limit)
            if clocked:
                self.remaining[sid] = R - self._kept(sid, max(0.0, limit - tail_min))
                self.state[sid]["last_attempt_utc"], self.state[sid]["status"] = _iso(t_end), "transient_fail"
                self._row(t_end, sid, "transient_fail", limit)
                stopped = sid
                ran.append((sid, limit, "stopped"))
                e += limit
                continue                                   # every later unit is budget-skipped (e == budget)
            if kill_keeps_progress and self._resumable(sid):
                self.remaining[sid] = R - self._kept(sid, limit)
            self._row(t_end, sid, "killed_external", limit)
            hard = sid
            ran.append((sid, limit, "killed"))
            e += limit
            break
        return {"used_min": e, "advanced": advanced, "hard_stop": hard, "stopped": stopped, "ran": ran}


CENSORED_BOUNDS = ("newest", "kills", "tightest")


def load_unit_book(db_path: str, copy_utc: dt.datetime, shift: dt.timedelta, cadences: dict | None = None,
                   desktop_windows: list | None = None, censored_bound: str = "newest",
                   censored_mult: float = 1.0, **book_kw) -> UnitBook:
    """The owed units from one state COPY, opened read-only (?immutable=1), every timestamp moved by `shift` so the
    copy's instant lands on the replay's first instant. Rows dated after the copy are ignored.

    `desktop_windows` ([(updater start, push end)] of the real desktop passes, from the wrapper logs): when given, a
    finished turn's outcome is read only from rows written inside a desktop pass, so a cloud row - e.g. the
    45.0-min `partial` that is the CI timeout, censored by R625 rule 2 - cannot decide it. A source with no finished
    desktop row is assumed to end `ok` (listed in `assumed_ok`).

    CENSORED WORK TERMS (review AR-394 F1). A unit whose newest row is a kill or a transient has no measured full
    turn: every number the copy holds is a LOWER bound. `censored_bound` picks which one is used for those units only:
      - "newest": the newest row (the plan's q_owed rule; reproduces its 1,578.6 - the baseline uses this);
      - "kills": the largest killed / transient row (AR-394's tightest-kill figure, 1,858.5 at the copy);
      - "tightest": the largest killed / transient / partial row - a partial is a turn that ran at least that long
        (oecd's 406.7-min partial of 2026-08-02), so this is the tightest lower bound the copy holds.
    `censored_mult` then multiplies those terms (x1, x1.5, x2 in the sweep): the real turn is longer by an unknown
    factor, and a verdict that flips inside that range is not shown by the replay.

    `book_kw` (order, clocked_extra, whole_flow, due_from_attempt) go to UnitBook unchanged; they change no work
    term, state stamp or outcome read here."""
    if censored_bound not in CENSORED_BOUNDS:
        raise ValueError(f"censored_bound must be one of {CENSORED_BOUNDS}")
    cadences = cadences or local_units_from_registry()
    con = sqlite3.connect(f"file:{db_path}?immutable=1", uri=True)
    wins = sorted(desktop_windows or [])
    wstarts = [a for a, _b in wins]

    def on_desktop(ts) -> bool:
        if desktop_windows is None:
            return True
        d = dt.datetime.fromisoformat(str(ts))
        d = d if d.tzinfo else d.replace(tzinfo=dt.timezone.utc)
        i = bisect_right(wstarts, d) - 1
        return i >= 0 and wins[i][0] <= d <= wins[i][1]

    def mv(s):
        if not s:
            return s
        d = dt.datetime.fromisoformat(str(s))
        d = d if d.tzinfo else d.replace(tzinfo=dt.timezone.utc)
        return _iso(d + shift)

    srcs = sorted(cadences)
    q = ",".join("?" * len(srcs))
    state = {}
    for sid, ls, la, st in con.execute(f"SELECT source_id, last_success_utc, last_attempt_utc, status FROM "
                                       f"unit_state WHERE unit_id='_all' AND source_id IN ({q})", srcs):
        state[sid] = {"last_success_utc": mv(ls), "last_attempt_utc": mv(la), "status": st}
    raw = list(con.execute(f"SELECT ts_utc, source_id, unit_id, status, dur_s FROM runs WHERE source_id IN ({q}) "
                           f"AND ts_utc <= ? ORDER BY id", srcs + [_iso(copy_utc)]))
    rows = [(mv(ts), sid, uid, status, dur) for ts, sid, uid, status, dur in raw]
    work, outcome, censored, assumed = {}, {}, [], []
    for sid in srcs:
        nr = [r for r in rows if r[1] == sid]
        if nr:
            work[sid] = (nr[-1][4] or 0.0) / 60.0
            if nr[-1][3] in ("killed_external", "transient_fail"):
                censored.append(sid)
                kinds = {"newest": (), "kills": ("killed_external", "transient_fail"),
                         "tightest": ("killed_external", "transient_fail", "partial")}[censored_bound]
                bounds = [(x[4] or 0.0) / 60.0 for x in nr if x[3] in kinds]
                work[sid] = max([work[sid]] + bounds) * censored_mult
        good = [r for r, o in zip(rows, raw) if r[1] == sid and r[3] in ("ok", "no_change", "partial")
                and on_desktop(o[0])]
        if good:
            outcome[sid] = good[-1][3]
        else:
            assumed.append(sid)
    con.close()
    return UnitBook(cadences, state, rows, work, outcome, censored, assumed, **book_kw)


# ------------------------------------------------------------------ the simulator

@dataclass
class ReplayConfig:
    policy: str = "today"            # "today": the ps1 + gate as on main; "lease": plan A (fallback = today)
    dispatch: bool = True            # A3.6 re-dispatch works (True) or the credential cannot dispatch (False)
    lease_min: float = 240.0         # L
    min_hours: float = PS1_MIN_HOURS
    backoff_h: float | None = None   # D3 NoProgressBackoffH; None = no D3 (today)
    w_on: bool = False               # W with every resumable candidate clocked (W off = the near term, W3b)
    tail_min: float = 90.0
    kill_keeps_progress: bool = False
    pull_min: float = 2.0
    push_min: float = 6.9
    dispatch_latency_min: float = 1.0
    tick_min: float = 5.0
    want_block: float = PS1_WANT_BLOCK
    max_hours: float = PS1_MAX_HOURS
    # NOT in the plan (a sensitivity case): D3 as written holds only windows below WantBlockMin, and a lease window
    # is L >= 180, so under A an EMPTY lease is never backed off. True also holds lease attempts while the
    # no-progress stamp is younger than backoff_h.
    d3_leases: bool = False
    # Review AR-394 F2. The updater's budget under a lease. Plan A3.5 says "pass with budget L"; plan A4 says the
    # token's --until-block returns min(L, minutes to expires - E) = L, and run_local_heavy.ps1 then takes its
    # $marginMin = 25 off whatever --until-block answers (`$untilCron = $untilBlock - $marginMin`). With the ps1
    # unchanged - and A4 changes only the gate - the pass runs with L - 25. The verdict uses 25.
    lease_margin_min: float = 0.0
    # Review AR-394 F1: which lower bound a censored work term takes, and its multiplier (see load_unit_book).
    censored_bound: str = "tightest"
    censored_mult: float = 1.0
    # Review AR-394 F4: do historical MANUAL dispatches (most were single-source proofs) count as completed cloud
    # runs - for the >= 2/day half and for A3.2's fairness? They always occupy the queue either way. The replay's
    # own re-dispatches are full runs and always count.
    count_manual: bool = True
    # PLAN rev 10 R10.3 item 2 (PR 2). The defaults are PR #200's model; a field left at its default is left out of
    # the label and of the json `cfg`, so the output with no new flag is PR #200's byte for byte.
    order: str = "production"          # B2: "attempt" = the later of last_success / last_attempt / last kill
    clocked_extra: tuple = ()          # sources clocked like W_CLOCKED while W is on
    whole_flow: tuple = ()             # ((source, per-flow bound min), ...): a stop keeps whole pieces only
    drop_cron: tuple = ()              # ((wf, hour), ...): C - the cron and its credited history runs removed
    cloud_floor: float = 2.0           # completed runs/day per writer in the strict rule
    due_from_attempt: bool = False     # a partial behind an old success is due on the partial clock

    def writers(self) -> dict:
        return writers_without(self.drop_cron) if self.drop_cron else g.WRITERS

    def _ext(self) -> str:
        out = ""
        if self.order != "production":
            out += f" ord={self.order}"
        if self.clocked_extra:
            wf = dict(self.whole_flow)
            out += " clk+=" + ",".join(s + (f"({wf[s]:g})" if s in wf else "") for s in self.clocked_extra)
        if self.drop_cron:
            out += " C=" + ",".join(f"-{w.split('.')[0].replace('updater-', '')}{h:02d}" for w, h in self.drop_cron)
        if self.due_from_attempt:
            out += " due=att"
        if self.cloud_floor != 2.0:
            out += f" floor={self.cloud_floor:g}"
        return out

    def label(self) -> str:
        d3 = "off" if self.backoff_h is None else f"{self.backoff_h:g}" + ("+L" if self.d3_leases else "")
        tail = (f" c={self.censored_bound[0]}x{self.censored_mult:g}"
                f"{'' if self.count_manual else ' man=excl'}{self._ext()}")
        if self.policy == "today":
            return f"today MinH={self.min_hours:g} D3={d3} W={'on' if self.w_on else 'off'}{tail}"
        return (f"A L={self.lease_min:g}-{self.lease_margin_min:g} MinH={self.min_hours:g} D3={d3} "
                f"W={'on' if self.w_on else 'off'} disp={'yes' if self.dispatch else 'no'}{tail}")


_EXT_DEFAULTS = {"order": "production", "clocked_extra": (), "whole_flow": (), "drop_cron": (), "cloud_floor": 2.0,
                 "due_from_attempt": False}


def cfg_dict(cfg: ReplayConfig) -> dict:
    """asdict(cfg) without the PR 2 fields that sit at their defaults (so PR #200's json is unchanged)."""
    d = asdict(cfg)
    for k, v in _EXT_DEFAULTS.items():
        if (tuple(d[k]) if isinstance(v, tuple) else d[k]) == v:
            del d[k]
    return d


def _counts(r: "CloudRun", cfg: ReplayConfig) -> bool:
    """Is this finished, non-deferred run a `completed` row for A3.2 and for runs/day?"""
    if not r.completed_ok:
        return False
    return cfg.count_manual or r.dispatched or r.event != "workflow_dispatch"


def deferred_minutes(hist_dir: str, since: dt.datetime, until: dt.datetime) -> dict:
    """How long a DEFERRED run holds the concurrency group (A5), from the measured steps of real runs created in
    [since, until): updater-daily = first job start -> "Pull state" start, + the pull (a --read-only pull costs the
    same download), + the "Health gate" step, + everything after it; updater-heavy = its `setup` job (a deferring
    setup emits an empty matrix, so no leg runs) + 1 min for A5's new `record` job (not measurable yet).
    Returns the medians and the sample sizes."""
    parts = defaultdict(list)
    for wf in g.WRITERS:
        with open(os.path.join(hist_dir, f"runs_{wf}.json"), encoding="utf-8") as f:
            rows = json.load(f)
        for r in rows:
            c = _parse(r["createdAt"])
            path = os.path.join(hist_dir, "jobs", f"{r['databaseId']}.json")
            if not (since <= c < until) or r.get("status") != "completed" or not os.path.exists(path):
                continue
            with open(path, encoding="utf-8") as f:
                jobs = json.load(f).get("jobs") or []
            starts = [t for t in (_ts(j.get("startedAt")) for j in jobs) if t]
            if not starts:
                continue
            first, end = min(starts), _parse(r["updatedAt"])
            if wf == "updater-daily.yml":
                steps = {s["name"].split(" (")[0]: s for j in jobs for s in (j.get("steps") or [])}
                ps = next((s for n, s in steps.items() if n.startswith("Pull state")), None)
                hg = next((s for n, s in steps.items() if n.startswith("Health gate")), None)
                if ps and hg and _ts(ps.get("startedAt")) and _ts(hg.get("completedAt")):
                    parts["d_lead"].append(_mins(first, _ts(ps["startedAt"])))
                    parts["d_pull"].append(_mins(_ts(ps["startedAt"]), _ts(ps["completedAt"])))
                    parts["d_health"].append(_mins(_ts(hg["startedAt"]), _ts(hg["completedAt"])))
                    parts["d_post"].append(_mins(_ts(hg["completedAt"]), end))
            else:
                su = [j for j in jobs if j.get("name") == "setup" and _ts(j.get("startedAt")) and
                      _ts(j.get("completedAt"))]
                if su:
                    parts["h_setup"].append(_mins(_ts(su[0]["startedAt"]), _ts(su[0]["completedAt"])))
    med = {k: statistics.median(v) for k, v in parts.items() if v}
    daily = sum(med.get(k, 0.0) for k in ("d_lead", "d_pull", "d_health", "d_post"))
    return {"updater-daily.yml": daily, "updater-heavy.yml": med.get("h_setup", 1.0) + 1.0,
            "parts": med, "n": {k: len(v) for k, v in parts.items()}}


def dispatch_templates(runs: list, since: dt.datetime, until: dt.datetime) -> dict:
    """What a re-dispatched run of a writer that was NOT deferred looks like (A3.6 dispatches every OWED writer,
    and after a lease both are owed by A3.2's rule): the median measured scheduled run of that writer in the span.
    A writer that WAS deferred is re-dispatched with that deferred run's own measured durations."""
    out = {}
    for wf in g.WRITERS:
        sel = [r for r in runs if r.wf == wf and r.event == "schedule" and r.has_jobs and r.completed_ok
               and since <= r.created < until]
        out[wf] = CloudRun(wf, 0, "workflow_dispatch", since, "completed", "",
                           statistics.median(r.exec_min for r in sel), statistics.median(r.read_off_min for r in sel),
                           statistics.median(r.push_off_min for r in sel), True,
                           int(statistics.median(r.pushes for r in sel)), None, None, True, dispatched=True)
    return out


class Replay:
    """One scenario over [start, end) with a warm-up of the cloud group before `start` (the desktop is idle in the
    warm-up, so the group's state at `start` is history's)."""

    PRIO_DONE, PRIO_RELEASE, PRIO_ARRIVE, PRIO_TICK = 0, 1, 2, 3

    def __init__(self, cfg: ReplayConfig, runs: list, book: UnitBook, start: dt.datetime, end: dt.datetime,
                 deferred_min: dict, templates: dict, last_success: dt.datetime, last_noprogress: dt.datetime | None,
                 warmup: dt.timedelta = dt.timedelta(days=3)):
        self.cfg, self.book, self.start, self.end = cfg, book, start, end
        self.runs = [r for r in runs if start - warmup <= r.created < end]
        self.writers = cfg.writers()          # the gate's crons (--drop-cron: fewer; history is cut by the caller)
        self.blocks = cron_blocks(runs, start - warmup, end, self.writers)
        self.block_starts = [b[0] for b in self.blocks]
        self.deferred_min, self.templates = deferred_minutes_default(deferred_min), templates
        self.ev, self.seq = [], 0
        self.running, self.pending = None, None
        self.lease = None                     # (taken, release) of the CURRENT lease
        self.leases, self.passes = [], []
        self.ref = None                       # A3.2 reference time of the last lease (None: first-lease rule holds)
        self.busy_until = start
        self.last_success, self.last_noprog = last_success, last_noprogress
        self.done = []                        # (wf, start, end, deferred, run)
        self.evicted = []
        self.dispatch_q, self.dispatched = [], []
        self.lease_deferred = {}              # wf -> the newest run deferred under the current lease
        self.arrivals = sorted(r.created for r in self.runs)
        self._real_by_end = {}
        for wf in g.WRITERS:
            rs = sorted((r for r in runs if r.wf == wf and r.has_jobs and r.hist_end is not None
                         and r.event == "schedule"), key=lambda r: r.hist_end)
            self._real_by_end[wf] = (rs, [r.hist_end for r in rs])
        self.owed_series = []
        for r in self.runs:
            self._push(r.created, self.PRIO_ARRIVE, ("arrive", r))
        t = start
        while t < end:
            self._push(t, self.PRIO_TICK, ("tick", None))
            t += dt.timedelta(minutes=cfg.tick_min)

    def _push(self, t, prio, item):
        self.seq += 1
        heapq.heappush(self.ev, (t, prio, self.seq, item))

    # -- the concurrency group
    def _lease_active(self, x: dt.datetime) -> bool:
        return self.lease is not None and self.lease[0] <= x < self.lease[1]

    def _start(self, r: CloudRun, s: dt.datetime):
        deferred = self.cfg.policy == "lease" and self._lease_active(s + dt.timedelta(minutes=r.read_off_min))
        dur = self.deferred_min[r.wf] if deferred else r.exec_min
        self.running = (r, s, s + dt.timedelta(minutes=dur), deferred)
        if deferred:
            self.lease_deferred[r.wf] = r
        self._push(self.running[2], self.PRIO_DONE, ("done", None))

    def _arrive(self, r: CloudRun, t: dt.datetime):
        if self.running is None and self.pending is None:
            self._start(r, t)
        elif self.pending is None:
            self.pending = (r, t)
        else:
            self.evicted.append((self.pending[0], self.pending[1], t))
            if self.pending[0].dispatched and self.pending[0].wf not in self.dispatch_q:
                self.dispatch_q.insert(0, self.pending[0].wf)
            self.pending = (r, t)

    def _done(self, t: dt.datetime):
        r, s, e, deferred = self.running
        self.done.append((r.wf, s, e, deferred, r))
        self.running = None
        if self.pending is not None:
            nxt, _arr = self.pending
            self.pending = None
            self._start(nxt, t)

    def _inflight(self) -> bool:
        return self.running is not None or self.pending is not None

    def _cron_pending(self, t: dt.datetime) -> bool:
        i = bisect_right(self.block_starts, t)
        for c, e, _wf in self.blocks[max(0, i - 8):i]:
            if c <= t < e:
                return True
        return False

    def _completed_after(self, wf: str, ref: dt.datetime | None) -> bool:
        for w, _s, e, deferred, r in reversed(self.done):
            if w == wf and not deferred and _counts(r, self.cfg):
                return ref is None or e > ref
        return False

    def _fair(self) -> bool:
        if self.ref is None:
            return all(self._completed_after(wf, None) for wf in g.WRITERS)   # the first-lease rule (A3.2)
        return all(self._completed_after(wf, self.ref) for wf in g.WRITERS)

    # -- the desktop
    def _pass(self, t: dt.datetime, budget: float, mode: str):
        cfg = self.cfg
        pre = LEASE_W_MIN if mode == "lease" else 0.0
        upd = t + dt.timedelta(minutes=pre + cfg.pull_min)
        res = self.book.run_pass(upd, budget, cfg.w_on, cfg.tail_min, cfg.kill_keeps_progress)
        push_end = upd + dt.timedelta(minutes=res["used_min"] + cfg.push_min)
        if res["advanced"]:
            self.last_success = push_end
        else:
            self.last_noprog = push_end
        self.passes.append({"mode": mode, "start": t, "pull": t + dt.timedelta(minutes=pre), "end": push_end,
                            "budget": budget, **res})
        self.busy_until = push_end
        return push_end

    def _gate_attempt(self, t: dt.datetime, since_h: float):
        cfg = self.cfg
        if self._inflight() or self._cron_pending(t):
            return "blocked"
        # A Replay built without __init__ (tests/test_d3_noprogress_backoff.py uses object.__new__) has no
        # `writers`: fall back to the gate's WRITERS. With the gate's own crons the call is PR #200's exactly (that
        # test also stands in a one-argument until_block_fast); only a --drop-cron cut passes its writers.
        w = getattr(self, "writers", None) or g.WRITERS
        ub = until_block_fast(t) if w == g.WRITERS else until_block_fast(t, w)
        until_cron = ub - PS1_MARGIN_MIN
        if until_cron < PS1_MIN_USABLE:
            return "short"
        young = (cfg.backoff_h is not None and self.last_noprog is not None
                 and _mins(self.last_noprog, t) < cfg.backoff_h * 60.0)
        if until_cron < cfg.want_block and (since_h < cfg.max_hours or young):
            return "hold"
        self._pass(t, float(min(PS1_BUDGET_CAP, until_cron)), "gate")
        return "pass"

    def _lease_attempt(self, t: dt.datetime):
        cfg = self.cfg
        self.lease_deferred = {}
        w_end = t + dt.timedelta(minutes=LEASE_W_MIN)
        i = bisect_right(self.arrivals, t)
        if i < len(self.arrivals) and self.arrivals[i] <= w_end:
            self.lease = (t, w_end)                       # A3.4: an unfinished run at the re-read -> stand down
            self.leases.append({"taken": t, "release": w_end, "stood_down": True})
            self.busy_until = w_end
            self._push(w_end, self.PRIO_RELEASE, ("release", None))
            return "stand_down"
        # The pass is computed whole at its start (the unit book needs no cloud input), so the lease's release is
        # known before any cloud event inside it is processed; every run that reads the state inside it defers.
        release = self._pass(t, float(cfg.lease_min) - cfg.lease_margin_min, "lease")
        expires = t + dt.timedelta(minutes=LEASE_W_MIN + cfg.lease_min + LEASE_E_MIN)
        self.lease = (t, release)
        self.leases.append({"taken": t, "release": release, "stood_down": False,
                            "push_refused": release > expires - dt.timedelta(minutes=2)})
        self._push(release, self.PRIO_RELEASE, ("release", None))
        return "lease"

    def _release(self, t: dt.datetime):
        self.ref = t
        if self.cfg.dispatch:
            first = sorted(self.lease_deferred, key=lambda w: self.lease_deferred[w].created)
            self.dispatch_q = first + [w for w in g.WRITERS if w not in first]
            self._try_dispatch(t)

    def _latest_real(self, wf: str, t: dt.datetime):
        """The writer's newest historical run that had ENDED by `t` (job-level times known): the stand-in for an
        owed writer that was not deferred. Local in time on purpose - its durations and whether its push succeeded
        follow the cloud's own condition then (heavy's duration doubled W40 -> W41, plan 2.3; a heavy leg failing
        for days fails a dispatched run too), where a span-wide median would not."""
        rs, ends = self._real_by_end[wf]
        i = bisect_right(ends, t)
        return rs[i - 1] if i else None

    def _try_dispatch(self, t: dt.datetime) -> bool:
        """A3.6: one owed writer at a time, never while a run of the group is queued or pending."""
        while self.dispatch_q and self._completed_after(self.dispatch_q[0], self.ref):
            self.dispatch_q.pop(0)
        if not self.dispatch_q or self.pending is not None or (self.lease and self.lease[0] <= t < self.lease[1]):
            return False
        wf = self.dispatch_q[0]
        if self.running is not None and self.running[0].wf == wf:
            return False
        src = self.lease_deferred.get(wf) or self._latest_real(wf, t) or self.templates[wf]
        r = CloudRun(wf, -len(self.dispatched) - 1, "workflow_dispatch", t + dt.timedelta(
            minutes=self.cfg.dispatch_latency_min), "completed", "", src.exec_min, src.read_off_min, src.push_off_min,
            src.completed_ok, src.pushes, None, None, True, dispatched=True)
        self.dispatch_q.pop(0)
        self.dispatched.append(r)
        self._push(r.created, self.PRIO_ARRIVE, ("arrive", r))
        return True

    def _tick(self, t: dt.datetime):
        cfg = self.cfg
        if cfg.policy == "lease" and cfg.dispatch and t >= self.busy_until and self._try_dispatch(t):
            return
        if t < self.busy_until or t >= self.end:
            return
        since_h = _mins(self.last_success, t) / 60.0
        if since_h < cfg.min_hours:
            return
        if cfg.policy == "lease":
            if self._inflight():
                return
            young = (cfg.backoff_h is not None and self.last_noprog is not None
                     and _mins(self.last_noprog, t) < cfg.backoff_h * 60.0)
            if self._fair() and not self.dispatch_q and not (cfg.d3_leases and young):
                self._lease_attempt(t)
                return
        self._gate_attempt(t, since_h)

    def run(self) -> "Replay":
        while self.ev:
            t, _p, _s, (kind, item) = heapq.heappop(self.ev)
            if kind == "done":
                self._done(t)
            elif kind == "arrive":
                self._arrive(item, t)
            elif kind == "release":
                if self.lease and self.lease[1] == t:
                    self._release(t)
            elif kind == "tick":
                if len(self.owed_series) == 0 or (t - self.owed_series[-1][0]) >= dt.timedelta(hours=6):
                    self.owed_series.append((t, self.book.owed(t)))
                self._tick(t)
        return self


def deferred_minutes_default(d: dict) -> dict:
    return {"updater-daily.yml": d.get("updater-daily.yml", 8.0), "updater-heavy.yml": d.get("updater-heavy.yml",
                                                                                              2.0)}


def _overlap(a0, a1, b0, b1) -> bool:
    return a0 < b1 and b0 < a1


def replay_metrics(rp: Replay, owed_start: float, due_start: list, cfg: ReplayConfig | None = None) -> dict:
    """The plan 6.1 outputs for one scenario, over [start, end). `cfg` (default: the replay's) may differ from the
    replay's only in cloud_floor - the floor is a verdict input, not a simulation input, so one replay serves every
    floor."""
    cfg = rp.cfg if cfg is None else cfg
    if cfg != rp.cfg and replace(cfg, cloud_floor=rp.cfg.cloud_floor) != rp.cfg:
        raise ValueError("replay_metrics: cfg may differ from the replay's only in cloud_floor")
    S, E = rp.start, rp.end
    days = _mins(S, E) / 1440.0
    ps = [p for p in rp.passes if S <= p["start"] < E]
    out = {"label": cfg.label(), "days": days,
           "window_min_day": sum(p["budget"] for p in ps) / days,
           "used_min_day": sum(p["used_min"] for p in ps) / days,
           "passes": len(ps), "lease_passes": sum(1 for p in ps if p["mode"] == "lease"),
           "gate_passes": sum(1 for p in ps if p["mode"] == "gate"),
           "stand_downs": sum(1 for x in rp.leases if x["stood_down"] and S <= x["taken"] < E),
           "push_refused": sum(1 for x in rp.leases if x.get("push_refused")),
           "hard_stops": sum(1 for p in ps if p["hard_stop"]),
           "empty_passes": sum(1 for p in ps if not p["advanced"]),
           "desktop_pushes_day": len(ps) / days}
    cloud_state = []
    for wf in g.WRITERS:
        comp = sorted(e for w, _s, e, d, r in rp.done if w == wf and not d and _counts(r, rp.cfg))
        prev = [e for e in comp if e < S]
        inside = [e for e in comp if S <= e < E]
        pts = ([prev[-1]] if prev else [S]) + inside + [E]
        gaps = [_mins(a, b) / 60.0 for a, b in zip(pts, pts[1:])]
        sel = [(s, d, r) for w, s, _e, d, r in rp.done if w == wf and S <= s < E]
        out[wf] = {"runs_day": len(inside) / days, "longest_gap_h": max(gaps),
                   "deferred": sum(1 for _s, d, _r in sel if d),
                   "dispatched": sum(1 for _s, d, r in sel if r.dispatched and not d),
                   "evicted": sum(1 for r, _a, t in rp.evicted if r.wf == wf and S <= t < E),
                   "evicted_hist": sum(1 for r, _a, t in rp.evicted if r.wf == wf and S <= t < E
                                       and not r.dispatched),
                   "pushes_day": sum(r.pushes for _s, d, r in sel if not d) / days}
        cloud_state += [(s + dt.timedelta(minutes=r.read_off_min), s + dt.timedelta(minutes=r.push_off_min), r)
                        for s, d, r in sel if not d]
    out["collisions"] = sum(1 for p in ps for a, b, _r in cloud_state if _overlap(p["pull"], p["end"], a, b))
    owed_end = rp.book.owed(E)
    att = {s: rp.book.attempts[s] for s in due_start}
    fin = {s: rp.book.finished[s] for s in due_start}
    out.update({"owed_start": owed_start, "owed_end": owed_end,
                "due_start": len(due_start), "attempts_per_due_unit": (sum(att.values()) / len(att)) if att else 0.0,
                "min_attempts": min(att.values()) if att else 0,
                "never_finished": sorted(s for s, n in fin.items() if n == 0),
                "attempts": att, "finished": fin})
    out["drains"] = owed_end < owed_start and not out["never_finished"]
    out["cloud_ge2"] = all(out[wf]["runs_day"] >= 2.0 for wf in g.WRITERS)
    cloud_ok = out["cloud_ge2"]
    if cfg.cloud_floor != 2.0:          # --cloud-floor: the strict rule's floor; cloud_ge2 keeps its literal meaning
        out["cloud_floor"] = cfg.cloud_floor
        out["cloud_ok"] = cloud_ok = all(out[wf]["runs_day"] >= cfg.cloud_floor for wf in g.WRITERS)
    out["A_ok"] = out["drains"] and cloud_ok
    # A5's deferral alarm fires when a writer has no completed non-deferred run for 30 h (review AR-394 F4).
    out["longest_gap_h"] = max(out[wf]["longest_gap_h"] for wf in g.WRITERS)
    out["gap_over_30h"] = out["longest_gap_h"] > 30.0
    out["cfg"] = cfg_dict(cfg)
    return out


@dataclass
class ReplayInputs:
    runs: list
    deferred: dict
    templates: dict
    db_path: str
    copy_utc: dt.datetime
    start: dt.datetime
    end: dt.datetime
    last_success: dt.datetime          # the ps1 stamp in force at the copy, moved to `start`
    last_noprogress: dt.datetime | None
    overheads: dict
    desktop_windows: list
    drop_cron: tuple = ()              # the crons whose credited runs were cut from `runs` (load_replay_inputs)
    dropped: tuple = ()                # those runs


def simulate(cfg: ReplayConfig, inp: ReplayInputs):
    """One scenario's replay, before any verdict: (replay, owed at the start, units due at the start)."""
    if tuple(cfg.drop_cron) != tuple(inp.drop_cron):
        raise ValueError(f"cfg.drop_cron {cfg.drop_cron} != the inputs' cut {inp.drop_cron}: the cron and its "
                         f"history runs must go together")
    shift = inp.start - inp.copy_utc
    book = load_unit_book(inp.db_path, inp.copy_utc, shift, desktop_windows=inp.desktop_windows,
                          censored_bound=cfg.censored_bound, censored_mult=cfg.censored_mult, order=cfg.order,
                          clocked_extra=cfg.clocked_extra, whole_flow=dict(cfg.whole_flow),
                          due_from_attempt=cfg.due_from_attempt)
    owed0, due0 = book.owed(inp.start), book.due_set(inp.start)
    rp = Replay(cfg, inp.runs, book, inp.start, inp.end, inp.deferred, inp.templates, inp.last_success,
                inp.last_noprogress).run()
    return rp, owed0, due0


def run_scenario(cfg: ReplayConfig, inp: ReplayInputs) -> dict:
    rp, owed0, due0 = simulate(cfg, inp)
    m = replay_metrics(rp, owed0, due0)
    m["_replay"] = rp
    return m


def _fmt_row(m: dict) -> str:
    d, h = m["updater-daily.yml"], m["updater-heavy.yml"]
    return (f"{m['label']:<66} {m['window_min_day']:6.0f} {m['used_min_day']:6.0f} {m['passes']:4d} "
            f"{m['lease_passes']:4d} {m['stand_downs']:3d} {m['hard_stops']:3d} {m['empty_passes']:3d} "
            f"{m['owed_start']:7.1f} {m['owed_end']:7.1f} {m['attempts_per_due_unit']:5.2f} {m['min_attempts']:3d} "
            f"{len(m['never_finished']):3d} {d['runs_day']:5.2f} {h['runs_day']:5.2f} "
            f"{max(d['longest_gap_h'], h['longest_gap_h']):6.1f} {d['deferred'] + h['deferred']:4d} "
            f"{d['dispatched'] + h['dispatched']:4d} {d['evicted'] + h['evicted']:3d} "
            f"{d['evicted_hist'] + h['evicted_hist']:3d} {m['collisions']:3d} "
            f"{m['desktop_pushes_day']:5.2f} {d['pushes_day'] + h['pushes_day']:6.1f}  "
            f"{'drain' if m['drains'] else 'NO-drain'} "
            f"cloud{'>=' if m.get('cloud_ok', m['cloud_ge2']) else '<'}{m.get('cloud_floor', 2.0):g} "
            f"A={'YES' if m['A_ok'] else 'NO'}")


_HEADER = (f"{'setting':<66} {'win/d':>6} {'used/d':>6} {'pass':>4} {'leas':>4} {'sd':>3} {'hs':>3} {'emp':>3} "
           f"{'owed0':>7} {'owedE':>7} {'att/u':>5} {'min':>3} {'nf':>3} {'dly/d':>5} {'hvy/d':>5} {'gap_h':>6} "
           f"{'dfr':>4} {'dsp':>4} {'ev':>3} {'evh':>3} {'col':>3} {'dp/d':>5} {'cp/d':>6}  verdict")


def load_replay_inputs(args) -> ReplayInputs:
    end = _parse(args.end)
    start = end - dt.timedelta(days=args.days)
    runs = load_cloud_history(args.history)
    drops = tuple(dict.fromkeys(getattr(args, "drop_cron", None) or ()))
    dropped = []
    for wf, h in drops:                                  # C: before anything reads the history
        runs, d = drop_cron_runs(runs, wf, h)
        dropped += d
    oldest = {wf: min((r.created for r in runs if r.wf == wf), default=None) for wf in g.WRITERS}
    for wf, o in oldest.items():
        if o is None or o > start - dt.timedelta(days=3):
            sys.exit(f"history for {wf} starts {o}, after the replay's warm-up {start - dt.timedelta(days=3)}; "
                     f"fetch more (--limit) or shorten --days")
    nojobs = [r for r in runs if start - dt.timedelta(days=3) <= r.created < end and not r.has_jobs]
    if nojobs:
        print(f"WARNING: {len(nojobs)} run(s) in the span have no job file; they use run-level times "
              f"(startedAt == createdAt): {[r.run_id for r in nojobs][:10]}")
    passes = desktop_passes(args.logs)
    copy_utc = _parse(args.copy_utc)
    before = [p for p in passes if p.get("push_done") and p["push_done"] <= copy_utc]
    ls = max((p["push_done"] for p in before if p.get("stamped")), default=copy_utc - dt.timedelta(hours=60))
    ln = max((p["push_done"] for p in before if not p.get("stamped")), default=None)
    shift = start - copy_utc
    return ReplayInputs(runs, deferred_minutes(args.history, start, end), dispatch_templates(runs, start, end),
                        args.state, copy_utc, start, end, ls + shift, (ln + shift) if ln else None,
                        pass_overheads([p for p in passes if p["stamp"] >= "20260830"]),
                        [(p["updater_at"], p["push_done"]) for p in passes if "updater_at" in p and "push_done" in p],
                        drops, tuple(dropped))


def _cfg_from(args, **kw) -> ReplayConfig:
    base = dict(pull_min=args.pull_min, push_min=args.push_min, tail_min=args.tail_min,
                kill_keeps_progress=args.kill_keeps_progress, d3_leases=args.d3_leases,
                dispatch_latency_min=args.dispatch_latency, censored_bound=args.censored_bound,
                clocked_extra=tuple(dict.fromkeys(args.clocked_extra or ())),
                whole_flow=tuple(sorted(args.whole_flow.items())), drop_cron=tuple(dict.fromkeys(args.drop_cron or ())),
                due_from_attempt=args.due_from_attempt)
    base.update(kw)
    return ReplayConfig(**base)


def _src_minutes(s: str) -> tuple:
    src, _, v = s.partition("=")
    if not src or not v:
        raise argparse.ArgumentTypeError(f"expected SRC=MINUTES, got {s!r}")
    try:
        x = float(v)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected SRC=MINUTES, got {s!r}") from None
    if not x > 0:
        raise argparse.ArgumentTypeError(f"{s!r}: the per-flow bound must be > 0 minutes")
    return src, x


def _wf_hour(s: str) -> tuple:
    wf, _, h = s.rpartition(":")
    try:
        out = (wf, int(h))
        writers_without([out])
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"--drop-cron {s!r}: expected WF:HH, a cron of {g.WRITERS} ({e})") from None
    return out


def resolve_whole_flow(args) -> list:
    """{source: per-flow bound} for every --clocked-extra source that is stopped in whole flows, and the lines that
    say where each bound came from. A WHOLE_FLOW source takes --flow-bound if given, else the largest 25-flow block
    the desktop logs time (giant_flow_blocks); with neither it is refused. Every row that clocks it is CONDITIONAL
    on that bound, and the lines say so."""
    given = dict(args.flow_bound or [])
    stray = sorted(set(given) - set(args.clocked_extra or ()))
    if stray:
        sys.exit(f"--flow-bound for {stray}: not clocked (add --clocked-extra) - a bound nothing uses is a typo")
    out, lines = {}, []
    for s in args.clocked_extra or ():
        if s in given:
            out[s] = given[s]
        if s not in WHOLE_FLOW and s not in given:
            lines.append(f"{s}: clocked in divisible minutes (no --flow-bound given; not in WHOLE_FLOW)")
            continue
        fb = giant_flow_blocks(args.logs, s)
        meas = (f"the logs time {s} in blocks of 25 flows only ({len(fb['blocks'])} blocks in {fb['unit_runs']} "
                f"unit runs, {fb['files']} log files); ")
        if fb["blocks"]:
            b0 = fb["blocks"][0]
            meas += f"largest block {b0[0]:.1f} min ({b0[1]} line {b0[2]}: flows {b0[3]}->{b0[4]})"
        else:
            meas += "no block found"
        if s not in out:
            if fb["largest_min"] is None:
                sys.exit(f"{s} is clocked only in whole flows and the logs time none of its flows: give --flow-bound "
                         f"{s}=MIN")
            out[s] = round(fb["largest_min"], 1)
            how = "ASSUMED = the largest measured 25-flow block"
        else:
            how = "given by --flow-bound"
        lines.append(f"{s}: clocked in WHOLE FLOWS. No log line times one flow: {meas}. Every row with {s} clocked is "
                     f"CONDITIONAL on the bound 'no single {s} flow takes longer than {out[s]:g} min' ({how}); the "
                     f"turn is modelled as equal pieces of that bound, and a stop keeps only the whole pieces done "
                     f"before the update window ends (time left - tail).")
        if s == "oecd":
            lines.append("oecd: its largest flow file is 1.79B rows (ledger R244); no log line shows how long that "
                         "flow takes to fetch, so the bound above is not shown to cover it.")
    args.whole_flow = out
    return lines


def cmd_replay61(args):
    """Plan 6.1. `--baseline` reproduces the plan's measured figures first; the sweep runs only if they match."""
    if args.baseline:
        ext = [f for f, on in (("--order", args.order != ["production"]), ("--clocked-extra", args.clocked_extra),
                               ("--flow-bound", args.flow_bound), ("--drop-cron", args.drop_cron),
                               ("--cloud-floor", args.cloud_floor != [2.0]),
                               ("--due-from-attempt", args.due_from_attempt)) if on]
        if ext:
            sys.exit(f"--baseline reproduces today's measured figures; it takes none of {ext}")
        ok = baseline61(args)
        sys.exit(0 if ok else 1)
    ext_lines = resolve_whole_flow(args)
    inp = load_replay_inputs(args)
    if args.pull_min is None:
        args.pull_min = round(inp.overheads["pull_median"], 1)
    if args.push_min is None:
        args.push_min = round(inp.overheads["push_median"], 1)
    print(f"span {inp.start:%Y-%m-%dT%H:%MZ} .. {inp.end:%Y-%m-%dT%H:%MZ} ({_mins(inp.start, inp.end) / 1440:.2f} d); "
          f"state copy {inp.copy_utc:%Y-%m-%dT%H:%MZ} moved to the span start")
    print(f"desktop pull {args.pull_min} min, push {args.push_min} min (medians of {inp.overheads['n_pull']} / "
          f"{inp.overheads['n_push']} passes since 2026-08-30); tail {args.tail_min} min; deferred run holds the group "
          f"daily {inp.deferred['updater-daily.yml']:.1f} min, heavy {inp.deferred['updater-heavy.yml']:.1f} min "
          f"(parts {json.dumps({k: round(v, 2) for k, v in inp.deferred['parts'].items()})}, n {inp.deferred['n']})")
    t = inp.templates
    print("dispatch template (median scheduled run): " + "; ".join(
        f"{wf} exec {t[wf].exec_min:.0f} read +{t[wf].read_off_min:.1f} push +{t[wf].push_off_min:.0f} pushes "
        f"{t[wf].pushes}" for wf in g.WRITERS))
    print(f"initial ps1 stamps (moved): last success {inp.last_success:%Y-%m-%dT%H:%MZ}, last no-progress "
          f"{inp.last_noprogress:%Y-%m-%dT%H:%MZ}" if inp.last_noprogress else "")
    print(f"sensitivity knobs: kill keeps progress {args.kill_keeps_progress}; D3 also holds leases {args.d3_leases}; "
          f"dispatch latency {args.dispatch_latency} min")
    if (args.order != ["production"] or args.clocked_extra or args.drop_cron or args.cloud_floor != [2.0]
            or args.due_from_attempt):
        print(f"extensions (PR 2): order {args.order}; clocked extra (W on only) {args.clocked_extra or '-'}; "
              f"due from attempt {args.due_from_attempt}; cloud floor {args.cloud_floor} runs/day per writer")
        for wf, h in inp.drop_cron:
            got = [r for r in inp.dropped if r.wf == wf]
            lag = sorted(_mins(r.created.replace(hour=h, minute=0, second=0, microsecond=0)
                               if r.created.hour >= h else
                               (r.created - dt.timedelta(days=1)).replace(hour=h, minute=0, second=0, microsecond=0),
                               r.created) for r in got)
            print(f"C: dropped the {h:02d}:00Z cron of {wf} from the gate model (crons now "
                  f"{writers_without(inp.drop_cron)[wf]}) and the {len(got)} scheduled history "
                  f"run(s) credited to it ({min(got, key=lambda r: r.created).created:%Y-%m-%d} .. "
                  f"{max(got, key=lambda r: r.created).created:%Y-%m-%d}; start lag min {lag[0]:.0f}, median "
                  f"{statistics.median(lag):.0f}, max {lag[-1]:.0f} min)" if got else
                  f"C: dropped the {h:02d}:00Z cron of {wf}; NO history run was credited to it")
        for ln in ext_lines:
            print(ln)
    print()
    print(_HEADER)
    rows = []
    scen = []
    for order in args.order:
        for cm in args.censored_mult:
            for man in args.count_manual:
                for w_on in args.w:
                    for mh in ([] if args.no_today else args.min_hours):
                        for bo in args.backoff:
                            scen.append(_cfg_from(args, policy="today", min_hours=mh, backoff_h=bo, w_on=w_on,
                                                  censored_mult=cm, count_manual=man, order=order))
                    for L in args.lease:
                        for mg in args.lease_margin:
                            for mh in args.min_hours:
                                for bo in (args.lease_backoff if args.lease_backoff is not None else args.backoff):
                                    for dsp in args.dispatch:
                                        scen.append(_cfg_from(args, policy="lease", lease_min=L, lease_margin_min=mg,
                                                              min_hours=mh, backoff_h=bo, w_on=w_on, dispatch=dsp,
                                                              censored_mult=cm, count_manual=man, order=order))
    for cfg in scen:
        rp, owed0, due0 = simulate(cfg, inp)
        for fl in args.cloud_floor:                     # the floor is a verdict input: one replay serves all
            m = replay_metrics(rp, owed0, due0, replace(cfg, cloud_floor=fl))
            rows.append(m)
            print(_fmt_row(m), flush=True)
    if args.json:
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump([{k: v for k, v in m.items() if k != "_replay"} for m in rows], f, indent=1, default=str)
    print("\ncolumns: win/d = desktop window (budget) min/day; used/d = updater min/day; pass / leas = passes / of "
          "them under a lease; sd = lease stand-downs (a run created inside W); hs = hard stops; emp = passes that "
          "advanced nothing; owed0 / owedE = owed minutes at the start / end; att/u = attempts per unit due at the "
          "start; min = fewest attempts of those units; nf = of those units, how many never finished a turn; "
          "dly/d hvy/d = completed non-deferred cloud runs/day; gap_h = longest gap between completed runs of "
          "either writer; dfr = deferred runs; dsp = re-dispatched runs that ran; ev = evicted pending runs (evh = of them, "
          "runs GitHub created - a cron or manual run lost to the queue); col = "
          "collisions; dp/d = desktop pushes/day; cp/d = cloud pushes/day (every heavy leg push counted)")
    floor = ("2.0" if args.cloud_floor == [2.0] else
             "the row's floor (" + " / ".join(f"{x:g}" for x in args.cloud_floor) + ", --cloud-floor)")
    print("verdict: A=YES only if the owed set drains (owed at the end below the start AND every unit due at the start "
          f"finished a turn) AND each writer keeps >= {floor} completed non-deferred runs/day (plan 6.1).")


def baseline61(args) -> bool:
    """The 'today' reproduction (plan 2.2 / 2.6 / A6a), each figure with its tolerance, before any sweep."""
    ok = True

    def check(name, got, want, tol):
        nonlocal ok
        good = abs(got - want) <= tol
        ok = ok and good
        print(f"  {'PASS' if good else 'FAIL'}  {name}: expected {want}, got {round(got, 2)} (tolerance +/-{tol})")

    since, until = _parse("2026-10-14T18:00:00Z"), _parse("2026-10-28T22:00:00Z")
    print(f"1. free windows {since:%Y-%m-%dT%H:%MZ} .. {until:%Y-%m-%dT%H:%MZ} (plan 2.2: 217 min/day, 29 windows, "
          f">=90 usable 9, >=150 usable 7, longest 318)")
    sources = []
    if args.planner_runs:
        rows = []
        for wf, fn in zip(g.WRITERS, args.planner_runs):
            with open(fn, encoding="utf-8") as f:
                rows += [_cloud_run(wf, r, None) for r in json.load(f)]
        sources.append(("planner's frozen gh json (22:25Z)", rows))
    sources.append((f"this fetch ({args.history})", load_cloud_history(args.history)))
    for name, runs in sources:
        for inst, free in (("interval model", free_windows(runs, since, until)),
                           ("production assess(), 1-min steps", gate_minutes_production(runs, since, until))):
            s = window_stats(free, since, until)
            print(f"  [{name}; {inst}] {s['free_min']:.0f} min = {s['per_day']:.1f} min/day over {s['days']:.2f} d; "
                  f"windows {s['windows']}; >=90 {s['ge90']}; >=150 {s['ge150']}; longest {s['longest']:.0f}")
            check("free min/day", s["per_day"], 217, 1.5)
            check("windows", s["windows"], 29, 0)
            check(">=150 usable", s["ge150"], 7, 0)
            check(">=90 usable", s["ge90"], 9, 0)
            check("longest", s["longest"], 318, 1.5)
    runs = sources[-1][1]
    print("2. until_block_fast == production minutes_until_block at every CLEAR 5-min tick of that span")
    per = _as_of_lists(runs)
    n = bad = 0
    t = since
    while t < until:
        unf, sch = _runs_as_of(per, t)
        if not g.assess(unf, t, None, sch)[0]:
            n += 1
            prod = g.minutes_until_block(unf, t, None, sched=sch)
            if prod != until_block_fast(t):
                bad += 1
        t += dt.timedelta(minutes=5)
    check(f"mismatches over {n} clear ticks", bad, 0, 0)
    inp = load_replay_inputs(args)
    print(f"3. the re-simulated concurrency group reproduces history (no lease): first-job starts of the runs created "
          f"in {inp.start:%Y-%m-%d} .. {inp.end:%Y-%m-%d}")
    shift = inp.start - inp.copy_utc
    book = load_unit_book(inp.db_path, inp.copy_utc, shift)
    rp = Replay(ReplayConfig(policy="today", min_hours=10 ** 9), inp.runs, book, inp.start, inp.end, inp.deferred,
                inp.templates, inp.last_success, inp.last_noprogress).run()   # the desktop never starts: cloud only
    diffs = [abs(_mins(s, r.hist_start)) for _w, s, _e, _d, r in rp.done
             if r.has_jobs and r.hist_start and inp.start <= r.created < inp.end]
    big = sum(1 for d in diffs if d > 1.0)
    print(f"   {len(diffs)} runs compared; max |sim - history| {max(diffs):.1f} min; > 1 min: {big}; "
          f"evictions simulated {len(rp.evicted)}")
    check("runs off by > 1 min", big, 0, 0)
    print("4. updater-heavy liveness since 2026-10-14T18:00Z (plan A6a, AR-393 r7: 29 runs, 27 success, 1.92/day, "
          "gap median 12.2 h, max 24.1 h; AR-393's list ended 2026-10-29T01:05Z)")
    hs = [r for r in inp.runs if r.wf == "updater-heavy.yml" and r.created >= _parse("2026-10-14T18:00:00Z")
          and r.created <= _parse("2026-10-29T01:05:00Z")]
    succ = sorted(r.hist_end for r in hs if r.conclusion == "success")
    gaps = [_mins(a, b) / 60 for a, b in zip(succ, succ[1:])]
    span_d = _mins(_parse("2026-10-14T18:00:00Z"), _parse("2026-10-29T01:05:00Z")) / 1440
    print(f"   runs {len(hs)}, success {len(succ)}, {len(succ) / span_d:.2f}/day, gap median "
          f"{statistics.median(gaps):.1f} h, max {max(gaps):.1f} h")
    check("heavy successes", len(succ), 27, 1)
    check("heavy gap max h", max(gaps), 24.1, 0.15)
    print(f"5. owed at the copy {inp.copy_utc:%Y-%m-%dT%H:%MZ} (plan 2.6 / q_owed.py: 1,578.6 min over 15 due units)")
    b0 = load_unit_book(inp.db_path, inp.copy_utc, dt.timedelta(0), desktop_windows=inp.desktop_windows)
    check("owed min", b0.owed(inp.copy_utc), 1578.6, 0.15)
    check("due units", len(b0.due_set(inp.copy_utc)), 15, 0)
    print(f"   due: {', '.join(b0.due_set(inp.copy_utc))}")
    bk = load_unit_book(inp.db_path, inp.copy_utc, dt.timedelta(0), desktop_windows=inp.desktop_windows,
                        censored_bound="kills")
    check("owed min, censored terms at their largest kill/transient (AR-394 F1)", bk.owed(inp.copy_utc), 1858.5, 0.15)
    bt = load_unit_book(inp.db_path, inp.copy_utc, dt.timedelta(0), desktop_windows=inp.desktop_windows,
                        censored_bound="tightest")
    print(f"   owed with the TIGHTEST bound (largest kill/transient/partial; the sweep's default): "
          f"{bt.owed(inp.copy_utc):.1f} min; censored terms: " + ", ".join(
              f"{s} {b0.work[s]:.1f}->{bk.work[s]:.1f}->{bt.work[s]:.1f}" for s in sorted(b0.censored)))
    print(f"   censored work terms (killed/transient newest row): {', '.join(sorted(b0.censored)) or '-'}")
    print(f"   finished-turn outcome per unit (newest ok/no_change/partial row written inside a desktop pass, "
          f"{len(inp.desktop_windows)} pass windows): " + ", ".join(
              f"{s}={b0.outcome[s]}{'(assumed)' if s in b0.assumed_ok else ''}" for s in sorted(b0.units)))
    tp = measure_tail_probe(args.logs)
    print(f"6. W inputs from the updater logs ({tp['logs']} logs, {tp['unit_runs']} unit runs): CSV-phase derive "
          f"budgets spent in {len(tp['csv_budget_hits'])} unit runs; largest in one unit run "
          f"{tp['tail_lower_bound_min']:.0f} min (a LOWER bound on tail); timed probe/CSV lines {tp['timed_phase_lines']}"
          f" (so P is not measurable from these logs)")
    for acc, n_, src, took in tp["csv_budget_hits"]:
        print(f"     {n_}  {src:<10} derive budgets spent {acc:.0f} min, unit took {took:,} s")
    oh = inp.overheads
    print(f"7. pass overheads since 2026-08-30: pull median {oh['pull_median']:.1f} (max {oh['pull_max']:.1f}, "
          f"n {oh['n_pull']}), push median {oh['push_median']:.1f} (max {oh['push_max']:.1f}, n {oh['n_push']}) "
          f"(plan 3.2, 52 passes since 09-15: pull median 2.0, push 6.9)")
    print(f"\nBASELINE {'REPRODUCED' if ok else 'NOT REPRODUCED - do not sweep'}")
    return ok


def cmd_fetch(args):
    """Freeze the cloud history the lease replay reads: every run of both writers (any event) and, for each run
    created inside the window, its jobs and steps. Read-only gh calls; the files are the replay's only cloud input,
    so a re-run of the replay on the same directory gives the same numbers."""
    os.makedirs(os.path.join(args.out, "jobs"), exist_ok=True)
    cut = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=args.days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    for wf in g.WRITERS:
        out = subprocess.run(["gh", "run", "list", "--workflow", wf, "--limit", str(args.limit), "--json",
                              "databaseId,createdAt,startedAt,updatedAt,event,status,conclusion,attempt"],
                             capture_output=True, text=True, timeout=300)
        if out.returncode != 0:
            sys.exit(f"gh failed for {wf}: {out.stderr.strip()[:300]}")
        rows = json.loads(out.stdout)
        with open(os.path.join(args.out, f"runs_{wf}.json"), "w", encoding="utf-8") as f:
            json.dump(rows, f, indent=0)
        want = [r for r in rows if r["createdAt"] >= cut]
        if rows and min(r["createdAt"] for r in rows) > cut:
            print(f"WARNING: {wf}: the {len(rows)}-run listing starts after {cut}; raise --limit")
        failed = 0
        for r in want:
            path = os.path.join(args.out, "jobs", f"{r['databaseId']}.json")
            if os.path.exists(path) and r.get("status") == "completed" and not args.refresh:
                continue
            v = subprocess.run(["gh", "run", "view", str(r["databaseId"]), "--json", "jobs,createdAt,updatedAt,"
                                "status,conclusion,attempt"], capture_output=True, text=True, timeout=180)
            if v.returncode != 0:
                failed += 1
                continue
            with open(path, "w", encoding="utf-8") as f:
                f.write(v.stdout)
        print(f"{wf}: {len(rows)} runs listed, {len(want)} created since {cut}, {failed} job reads FAILED")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("fetch")
    p.add_argument("--out", required=True)
    p.add_argument("--days", type=int, default=75)
    p.add_argument("--limit", type=int, default=400)
    p.add_argument("--refresh", action="store_true")
    p.set_defaults(fn=cmd_fetch)
    p = sub.add_parser("replay61", help="plan 6.1: W windows, D3 back-off, the lease model (see the block above "
                                        "cmd_fetch)")
    p.add_argument("--history", required=True, help="a `fetch` directory")
    p.add_argument("--logs", required=True, help="the desktop wrapper/updater log directory (read only)")
    p.add_argument("--state", required=True, help="a state.db COPY (opened ?immutable=1)")
    p.add_argument("--copy-utc", default="2026-10-28T13:45:00Z")
    p.add_argument("--end", default="2026-10-28T22:00:00Z")
    p.add_argument("--days", type=float, default=60.0)
    p.add_argument("--baseline", action="store_true")
    p.add_argument("--planner-runs", nargs=2, metavar=("DAILY_JSON", "HEAVY_JSON"))
    p.add_argument("--lease", type=float, nargs="+", default=[180, 240, 300, 360])
    p.add_argument("--min-hours", type=float, nargs="+", default=[6, 10, 14, 20])
    p.add_argument("--backoff", type=lambda s: None if s == "off" else float(s), nargs="+", default=[4, 8, 12])
    p.add_argument("--w", type=lambda s: s == "on", nargs="+", default=[False, True], help="off / on")
    p.add_argument("--tail-min", type=float, default=90.0)
    p.add_argument("--pull-min", type=float, default=None)
    p.add_argument("--push-min", type=float, default=None)
    p.add_argument("--kill-keeps-progress", action="store_true")
    p.add_argument("--d3-leases", action="store_true", help="sensitivity, not in the plan: D3 also holds leases")
    p.add_argument("--dispatch-latency", type=float, default=1.0, help="minutes from dispatch to the run's creation")
    p.add_argument("--no-today", action="store_true", help="skip the 'today' rows")
    p.add_argument("--censored-bound", choices=CENSORED_BOUNDS, default="tightest",
                   help="lower bound a censored work term takes (load_unit_book; AR-394 F1)")
    p.add_argument("--censored-mult", type=float, nargs="+", default=[1.0, 1.5, 2.0])
    p.add_argument("--lease-margin", type=float, nargs="+", default=[25.0, 0.0],
                   help="minutes taken off L for the pass budget; 25 = plan A4 + the ps1 $marginMin (AR-394 F2)")
    p.add_argument("--count-manual", type=lambda s: s == "yes", nargs="+", default=[False, True],
                   help="yes / no: historical manual dispatches count as completed cloud runs (AR-394 F4)")
    p.add_argument("--dispatch", type=lambda s: s == "yes", nargs="+", default=[True, False], help="yes / no")
    p.add_argument("--lease-backoff", type=lambda s: None if s == "off" else float(s), nargs="+", default=None,
                   help="D3 values for the lease rows (default: --backoff); D3 never binds a lease of L >= 150")
    # PLAN rev 10 R10.3 item 2 (PR 2); see WHOLE_FLOW above. All off by default.
    p.add_argument("--order", choices=ORDERS, nargs="+", default=["production"],
                   help="unit order: production last_turn_utc, or 'attempt' (B2: the later of last success, last "
                        "attempt and last kill)")
    p.add_argument("--clocked-extra", action="append", default=[], metavar="SRC",
                   help="repeatable: clock SRC like W_CLOCKED while W is on")
    p.add_argument("--flow-bound", action="append", type=_src_minutes, default=[], metavar="SRC=MIN",
                   help="repeatable: a clocked SRC keeps only whole pieces of MIN minutes (default for oecd: the "
                        "largest 25-flow block in the logs)")
    p.add_argument("--drop-cron", action="append", type=_wf_hour, default=[], metavar="WF:HH",
                   help="repeatable: remove that cron from the gate model and the history runs credited to it (C)")
    p.add_argument("--cloud-floor", type=float, nargs="+", default=[2.0],
                   help="completed runs/day per writer in the strict rule (default 2)")
    p.add_argument("--due-from-attempt", action="store_true",
                   help="a partial behind an older success is due on the 7-day partial clock from its attempt")
    p.add_argument("--json")
    p.set_defaults(fn=cmd_replay61)
    sub.add_parser("lag").set_defaults(fn=cmd_lag)
    p = sub.add_parser("occupancy")
    p.add_argument("--days", type=int, default=14)
    p.add_argument("--tol", type=float, default=10.0)
    p.set_defaults(fn=cmd_occupancy)
    p = sub.add_parser("collisions")
    p.add_argument("--show", type=int, default=14)
    p.set_defaults(fn=cmd_collisions)
    p = sub.add_parser("windows")
    p.add_argument("--window", action="append", default=[
        "quiet 00:56-07:32Z=0056-0732", "runner free 10:30-14:40Z=1030-1440",
        "runner free 22:30-02:40Z=2230-0240"], help="NAME=HHMM-HHMM")
    p.add_argument("--days", type=int, nargs="+", default=[14, 21])
    p.set_defaults(fn=cmd_windows)
    p = sub.add_parser("sweep")
    p.add_argument("--days", type=int, default=14)
    p.add_argument("--want", type=int, nargs="+", default=[120, 150, 180])
    p.add_argument("--maxh", type=int, nargs="+", default=[30, 36, 42, 48])
    p.add_argument("--min-hours", type=int, default=20)
    p.add_argument("--margin", type=int, default=25)
    p.add_argument("--min-usable", type=int, default=20)
    p.set_defaults(fn=cmd_sweep)
    p = sub.add_parser("replay")
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--step", type=int, default=5)
    p.set_defaults(fn=cmd_replay)
    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
