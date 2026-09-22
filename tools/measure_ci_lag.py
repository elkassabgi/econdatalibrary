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

All read-only: `gh run list` only. Nothing here writes, deploys or runs the updater.
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


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
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
