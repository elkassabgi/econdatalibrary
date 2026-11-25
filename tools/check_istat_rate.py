"""The receipt for "the combined ISTAT request rate from this workstation stays under the limit".

No combined-rate figure is reported without this tool (DESIGN 6.7, C4b). It reads the shared pacer's
append-only request log(s) and, optionally, process logs (guard logs, pass logs) for the lines the pacer could
not put in the shared log (`[istat-pacer] FAILSAFE-TOKEN <R line>`, `[istat-pacer] UNLOGGED <line>`), and prints:

  * tokens per participant, kind and class; fail-safe tokens; abandoned (late-wake, never sent) tokens;
  * the maximum number of ACTUAL starts in any CLOSED window of --window seconds (default 60), overall and per
    participant, with the window start (verdict V2: on actual sends, not reserved slots; a token with no
    outcome line is counted at its slot and the count of those is printed);
  * the windows over the budget (4) and over ISTAT's limit (5);
  * the maximum slip (actual start - slot) and any start outside [slot, slot + SLIP_MAX_S];
  * GETs per flow for the updater (the input DESIGN Q6 assumes); penalties; outcomes.

Every closed window that INTERSECTS the range is counted, including the starts within one window on either side
of it. Without --log it reads every month from (--from minus 48 h + 133 s: a slot at a penalty end can be
reserved that long before it starts) to --to.

Exit 0 only when tokens > 0, the overall maximum <= the budget, and every start is inside its slot's validity.
Exit 1 when a window holds more than the budget, or a start fell outside [slot, slot + SLIP_MAX_S].
Exit 2 when it cannot measure: no log, a NAMED log missing, an unreadable file, an unparseable line, an outcome
line whose reservation is in no log read, or 0 tokens in the range. Inability to measure is a failure, never a
pass.

Usage:
  python tools/check_istat_rate.py --from 2026-11-25T03:00:00Z --to 2026-11-25T05:00:00Z
      [--log FILE ...] [--process-log FILE ...] [--window 60] [--budget 4] [--limit 5] [--slip-max 2]
Without --log it reads <STATE_DIR>/requests_YYYYMM.log for every month the range touches.
Read-only: it never opens the pacer's lock and never sends a request.
"""
from __future__ import annotations

import argparse
import collections
import datetime as dt
import hashlib
import os
import re
import statistics
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from jobs import _istat_pacer as pacer                       # noqa: E402

_NUM = r"-?\d+(?:\.\d+)?"
_KV = re.compile(r"([a-z_]+)=(\S*)")
_LINE = re.compile(r"^(R|P|T) (\S+) (" + _NUM + r") (.*)$")
_LINE_NOEPOCH = re.compile(r"^(O|X) (\S+) (.*)$")
_PREFIXES = ("[istat-pacer] FAILSAFE-TOKEN ", "[istat-pacer] UNLOGGED ")
_REQ = {"R": ("pid", "part", "host", "kind", "class", "flow", "mode", "waited", "tok"),
        "O": ("tok", "outcome", "el"),
        "X": ("tok", "reason", "slip"),
        "P": ("pid", "part", "seconds", "until"),
        "T": ("pid", "turn", "what")}


class Unparseable(ValueError):
    pass


def parse_line(text: str) -> dict:
    """One pacer log line -> dict(kind, utc, epoch?, fields). Unparseable raises."""
    text = text.rstrip("\r\n")
    m = _LINE.match(text)
    if m:
        kind, utc, epoch, rest = m.group(1), m.group(2), float(m.group(3)), m.group(4)
    else:
        m = _LINE_NOEPOCH.match(text)
        if not m:
            raise Unparseable(text[:160])
        kind, utc, rest, epoch = m.group(1), m.group(2), m.group(3), None
    fields = dict(_KV.findall(rest))
    leftover = _KV.sub("", rest).strip()
    if leftover:
        raise Unparseable(text[:160])
    missing = [k for k in _REQ[kind] if k not in fields]
    if missing:
        raise Unparseable(f"{text[:160]} (missing {missing})")
    try:
        for k in ("waited", "el", "slip", "seconds", "until", "turn", "start"):
            if k in fields:
                fields[k] = float(fields[k])
        if "pid" in fields:
            fields["pid"] = int(fields["pid"])
    except ValueError:
        raise Unparseable(text[:160]) from None
    return {"kind": kind, "utc": utc, "epoch": epoch, "f": fields}


def parse_time(s: str) -> float:
    s = s.strip()
    if re.fullmatch(_NUM, s):
        return float(s)
    d = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d.timestamp()


def iso(t: float) -> str:
    try:
        return dt.datetime.fromtimestamp(t, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4] + "Z"
    except (OverflowError, OSError, ValueError):
        return f"{t:.3f}"


def reservation_lookback_s() -> float:
    """How long before its start a token can be RESERVED: a slot at a 429 penalty end (up to 48 h) behind a
    full queue. Its R line is in the log of the month it was reserved in."""
    return pacer.PENALTY_MAX_S + 2 * pacer.N_MAX * pacer.GAP_S + pacer.FUTURE_SLACK_S


def default_logs(t_from: float, t_to: float) -> list:
    """requests_YYYYMM.log for every UTC month in [t_from - the reservation lookback, t_to]: an R line can sit
    in an earlier month than its start (review)."""
    out = []
    d = dt.datetime.fromtimestamp(max(t_from - reservation_lookback_s(), 0), dt.timezone.utc)
    d = d.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end = dt.datetime.fromtimestamp(max(t_to, 0), dt.timezone.utc)
    while d <= end:
        out.append(os.path.join(pacer.STATE_DIR, f"requests_{d:%Y%m}.log"))
        d = (d.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
    return out


def max_in_window(starts, window: float, lo=None, hi=None):
    """(max count of starts in any CLOSED window [t, t + window], the t of the first such window). With lo/hi,
    only windows whose start t is in [lo, hi] are considered (the windows that intersect a range); the count
    still includes every start given, inside the range or not."""
    s = sorted(starts)
    best, best_t, j = 0, None, 0
    for i in range(len(s)):
        if j < i:
            j = i
        while j + 1 < len(s) and s[j + 1] <= s[i] + window:
            j += 1
        if (lo is not None and s[i] < lo) or (hi is not None and s[i] > hi):
            continue
        if j - i + 1 > best:
            best, best_t = j - i + 1, s[i]
    return best, best_t


def windows_over(starts, window: float, threshold: int, lo=None, hi=None):
    """Start times t of closed windows [t, t + window] (t at a start, in [lo, hi]) holding more than
    `threshold` starts."""
    s = sorted(starts)
    out, j = [], 0
    for i in range(len(s)):
        if j < i:
            j = i
        while j + 1 < len(s) and s[j + 1] <= s[i] + window:
            j += 1
        if (lo is not None and s[i] < lo) or (hi is not None and s[i] > hi):
            continue
        if j - i + 1 > threshold:
            out.append((s[i], j - i + 1))
    return out


def load(paths, process_logs, problems):
    """-> list of parsed records with their source. Problems (unreadable, unparseable) go to `problems`."""
    recs = []
    for path in paths:
        try:
            with open(path, "rb") as fh:
                data = fh.read()
        except OSError as e:
            problems.append(f"cannot read {path}: {e}")
            continue
        n_lines = data.count(b"\n")
        print(f"  log {path}: {n_lines} line(s), sha256 {hashlib.sha256(data).hexdigest()[:16]}")
        for n, raw in enumerate(data.decode("ascii", "replace").splitlines(), 1):
            if not raw.strip():
                continue
            try:
                r = parse_line(raw)
            except Unparseable as e:
                problems.append(f"{path}:{n}: unparseable: {e}")
                continue
            r["src"] = "log"
            recs.append(r)
    for path in process_logs:
        try:
            with open(path, "rb") as fh:
                data = fh.read()
        except OSError as e:
            problems.append(f"cannot read {path}: {e}")
            continue
        hits = 0
        for n, raw in enumerate(data.decode("utf-8", "replace").splitlines(), 1):
            for pre in _PREFIXES:
                k = raw.find(pre)
                if k < 0:
                    continue
                hits += 1
                try:
                    r = parse_line(raw[k + len(pre):])
                except Unparseable as e:
                    problems.append(f"{path}:{n}: unparseable pacer line: {e}")
                    break
                r["src"] = "failsafe" if pre.startswith("[istat-pacer] FAILSAFE") else "unlogged"
                recs.append(r)
                break
        print(f"  process log {path}: {hits} pacer line(s), sha256 {hashlib.sha256(data).hexdigest()[:16]}")
    return recs


def analyse(recs, t_from, t_to, window, budget, limit, slip_max):
    tokens, dup_conflicts, seen_ids = {}, [], set()
    outcomes, abandoned = collections.defaultdict(list), set()
    penalties, turns = [], []
    for r in recs:
        f = r["f"]
        if r["kind"] == "R":
            key = (f["tok"], round(r["epoch"], 3))
            if key in tokens:
                continue                                     # the same line seen twice (log and process log)
            if f["tok"] in seen_ids:
                dup_conflicts.append(f["tok"])               # same id, different slot: both are counted
            seen_ids.add(f["tok"])
            tokens[key] = {"tok": f["tok"], "slot": r["epoch"], "part": f["part"], "kind": f["kind"],
                           "class": f["class"], "flow": f["flow"], "mode": f["mode"], "host": f["host"]}
        elif r["kind"] == "O":
            outcomes[f["tok"]].append(f)
        elif r["kind"] == "X":
            abandoned.add(f["tok"])
        elif r["kind"] == "P":
            penalties.append((r["epoch"], f))
        elif r["kind"] == "T":
            turns.append((r["epoch"], f))
    starts, no_start, both = [], 0, []
    for key, t in tokens.items():
        outs = outcomes.get(t["tok"], [])
        with_start = [o for o in outs if "start" in o]
        if t["tok"] in abandoned and not with_start:
            t["abandoned"] = True
            continue
        if t["tok"] in abandoned:
            both.append(t["tok"])                            # abandoned AND sent: count it (conservative)
        if with_start:
            o = min(with_start, key=lambda o: abs(o["start"] - t["slot"]))
            t["start"], t["outcome"] = o["start"], o["outcome"]
        else:
            t["start"], no_start, t["at_slot"] = t["slot"], no_start + 1, True
            t["outcome"] = outs[0]["outcome"] if outs else None
        t["slip"] = t["start"] - t["slot"]
        starts.append(t)
    # An outcome whose reservation is not in the logs read (an R line in an unread month, or a lost line) is
    # still a send: count it at its start, and the measurement is incomplete (review).
    orphans = []
    for tok, outs in outcomes.items():
        if tok in seen_ids:
            continue
        for o in outs:
            if "start" in o:
                orphans.append({"tok": tok, "slot": o["start"], "start": o["start"], "slip": 0.0, "part": "?",
                                "kind": "?", "class": "?", "flow": "-", "mode": "?", "host": "?",
                                "outcome": o["outcome"], "orphan": True})
    starts += orphans
    in_range = [t for t in starts if t_from <= t["start"] <= t_to]
    span = [t for t in starts if t_from - window <= t["start"] <= t_to + window]
    late = [t for t in in_range if t["slip"] > slip_max + 1e-6]
    early = [t for t in in_range if t["slip"] < -1e-3]
    ab_in_range = [t for t in tokens.values() if t.get("abandoned") and t_from <= t["slot"] <= t_to]
    orphans_in_span = [t for t in orphans if t_from - window <= t["start"] <= t_to + window]
    return (in_range, span, orphans_in_span, ab_in_range, no_start, late, early, both, dup_conflicts,
            penalties, turns)


def report(in_range, span, orphans, ab, no_start, late, early, both, dups, penalties, turns, t_from, t_to,
           window, budget, limit, slip_max):
    print(f"range {iso(t_from)} .. {iso(t_to)}; closed window {window:g} s; budget {budget}; "
          f"ISTAT's limit {limit}; slip max {slip_max:g} s")
    print(f"every closed window that INTERSECTS the range is counted, with the starts just outside it "
          f"({len(span) - len(in_range)} start(s) in the {window:g} s on either side)")
    if orphans:
        print(f"  ORPHAN OUTCOMES: {len(orphans)} outcome line(s) with no reservation line in the logs read "
              f"(counted at their start): {[t['tok'] for t in orphans][:10]}")
    at_slot = sum(1 for t in in_range if t.get("at_slot"))
    print(f"tokens started in range: {len(in_range)}  (abandoned, never sent: {len(ab)}; counted at their "
          f"slot for want of an outcome line with start=: {at_slot})")
    by = collections.Counter((t["part"], t["kind"], t["class"]) for t in in_range)
    for (part, kind, cls), n in sorted(by.items()):
        print(f"  {part:<13} {kind:<4} {cls:<14} {n}")
    fs = [t for t in in_range if t["mode"] == "failsafe"]
    print(f"fail-safe tokens: {len(fs)}")
    starts = [t["start"] for t in span]
    lo, hi = t_from - window, t_to
    mx, at = max_in_window(starts, window, lo, hi)
    print(f"MAX starts in any closed {window:g}-s window, overall: {mx}"
          + (f" (window from {iso(at)})" if at is not None else ""))
    for part in sorted({t["part"] for t in in_range}):
        pm, pat = max_in_window([t["start"] for t in span if t["part"] == part], window, lo, hi)
        print(f"  {part:<13} max {pm}" + (f" (from {iso(pat)})" if pat is not None else ""))
    over_b = windows_over(starts, window, budget, lo, hi)
    over_l = windows_over(starts, window, limit, lo, hi)
    print(f"windows over the budget ({budget}): {len(over_b)}; over ISTAT's limit ({limit}): {len(over_l)}")
    for t0, n in over_b[:20]:
        print(f"  OVER BUDGET {iso(t0)}: {n} starts")
    slips = [t["slip"] for t in in_range]
    print(f"max slip (actual start - slot): {max(slips):.3f} s" if slips else "max slip: n/a")
    for t in late[:20]:
        print(f"  LATE START tok={t['tok']} slip {t['slip']:.3f} s > {slip_max:g} s")
    for t in early[:20]:
        print(f"  EARLY START tok={t['tok']} started {-t['slip']:.3f} s BEFORE its slot")
    if both:
        print(f"  tokens both abandoned and sent (counted as sent): {len(both)}")
    if dups:
        print(f"  token ids seen with two different slots (each counted): {sorted(set(dups))[:10]}")
    upd = [t for t in in_range if t["part"] == "updater" and t["kind"] == "http" and t["flow"] != "-"]
    if upd:
        per = sorted(collections.Counter(t["flow"] for t in upd).values())
        print(f"updater GETs per flow: {len(per)} flow(s), min {per[0]}, median {statistics.median(per):g}, "
              f"max {per[-1]}, mean {sum(per) / len(per):.2f}")
    else:
        print("updater GETs per flow: no updater HTTP token with a flow in range")
    pin = [(e, f) for e, f in penalties if t_from <= e <= t_to]
    print(f"penalties (429) in range: {len(pin)}" +
          (f"; longest {max(f['seconds'] for _, f in pin):g} s" if pin else ""))
    tin = [(e, f) for e, f in turns if t_from <= e <= t_to]
    print(f"turn marker lines in range: {len(tin)}")
    oc = collections.Counter(t.get("outcome") or "(no outcome line)" for t in in_range)
    print("outcomes: " + ", ".join(f"{k} {v}" for k, v in sorted(oc.items())))
    return mx, late, early


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--from", dest="t_from", required=True, help="UTC ISO time or epoch")
    ap.add_argument("--to", dest="t_to", required=True, help="UTC ISO time or epoch")
    ap.add_argument("--log", action="append", default=[], help="a request log (repeatable)")
    ap.add_argument("--process-log", action="append", default=[], help="a guard or pass log (repeatable)")
    ap.add_argument("--window", type=float, default=60.0, help="closed window length in s (default 60)")
    ap.add_argument("--budget", type=int, default=pacer.BUDGET_PER_MIN)
    ap.add_argument("--limit", type=int, default=5, help="ISTAT's documented limit per window (5)")
    ap.add_argument("--slip-max", type=float, default=pacer.SLIP_MAX_S)
    a = ap.parse_args(argv)
    try:
        t_from, t_to = parse_time(a.t_from), parse_time(a.t_to)
    except ValueError as e:
        print(f"CANNOT MEASURE: bad time: {e}")
        return 2
    if t_to < t_from:
        print("CANNOT MEASURE: --to is before --from")
        return 2
    paths = a.log or default_logs(t_from, t_to)
    existing = [p for p in paths if os.path.isfile(p)]
    print(f"check_istat_rate: {len(existing)} of {len(paths)} request log(s) present, "
          f"{len(a.process_log)} process log(s)")
    problems = [f"no such log: {p}" for p in paths if p not in existing]
    named_missing = [p for p in a.log + a.process_log if not os.path.isfile(p)]
    if named_missing:
        # a log the caller NAMED and that is not there is a measurement gap, never "nothing happened" (review)
        for p in named_missing:
            print(f"  named log missing: {p}")
        print(f"CANNOT MEASURE: {len(named_missing)} named log(s) missing (exit 2)")
        return 2
    if not existing and not a.process_log:
        for p in problems:
            print("  " + p)
        print("CANNOT MEASURE: no log to read (exit 2)")
        return 2
    recs = load(existing, a.process_log, problems)
    unreadable = [p for p in problems if not p.startswith("no such log")]
    for p in problems:
        print("  " + p)
    if unreadable:
        print(f"CANNOT MEASURE: {len(unreadable)} unreadable or unparseable line(s)/file(s) (exit 2)")
        return 2
    (in_range, span, orphans, ab, no_start, late, early, both, dups, pens, turns) = analyse(
        recs, t_from, t_to, a.window, a.budget, a.limit, a.slip_max)
    mx, late, early = report(in_range, span, orphans, ab, no_start, late, early, both, dups, pens, turns, t_from,
                             t_to, a.window, a.budget, a.limit, a.slip_max)
    if not in_range:
        print("CANNOT MEASURE: 0 tokens in the range (exit 2)")
        return 2
    if mx > a.budget or late or early:
        print(f"FAIL: max {mx} in a closed {a.window:g}-s window (budget {a.budget}); "
              f"{len(late)} late and {len(early)} early start(s) (exit 1)")
        return 1
    if orphans:
        print(f"CANNOT MEASURE: {len(orphans)} outcome line(s) whose reservation is in no log read - read the "
              f"earlier month's log too (exit 2)")
        return 2
    print(f"OK: {len(in_range)} token(s), max {mx} in any closed {a.window:g}-s window <= budget {a.budget} (exit 0)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
