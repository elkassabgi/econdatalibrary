"""How many recorded run durations are CENSORED — a timeout's ceiling rather than a cost?

R684: a fix was withdrawn because it acted on `dur_s = 2700.0`, which is not what that unit cost
but what the CI 45-minute SIGALRM allowed it. `run_cost_estimate` takes a MAX over runs, and its
floor deliberately excludes failures while its max does not, so one censored row bands a source
as a giant for ever.

The known enforcement ceilings:

    2700 s   the default per-unit SIGALRM, 45 min
   10800 s   AQUEDUCT_UNIT_TIMEOUT_MIN=180 in updater-heavy.yml
   the local pass's own external kill at (run budget + 10 min), which VARIES per night

This counts rows sitting within a small epsilon of a fixed ceiling, and reports what each
affected source's estimate would be with and without them. It changes nothing — the point is to
size the problem before proposing a fix, because the last fix here was rejected for acting
without that.

Local, read-only.
"""
import os
import sqlite3
import sys
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
STATE = os.path.join(ROOT, "data", "_aqueduct", "state.db")

CEILINGS = {2700.0: "45-min per-unit SIGALRM", 10800.0: "180-min heavy timeout"}
EPS = 5.0                      # seconds either side; a real run landing this close is possible
                               # but rare, and is reported rather than assumed away


def main() -> int:
    con = sqlite3.connect("file:%s?mode=ro" % STATE, uri=True)
    con.execute("PRAGMA busy_timeout=8000")
    rows = con.execute(
        "SELECT source_id, status, dur_s, ts_utc FROM runs WHERE dur_s IS NOT NULL").fetchall()

    per_source = defaultdict(list)
    censored = []
    for sid, status, dur, ts in rows:
        per_source[sid].append((float(dur), status, ts))
        for c, name in CEILINGS.items():
            if abs(float(dur) - c) <= EPS:
                censored.append((sid, float(dur), status, str(ts)[:10], name))

    print(f"{len(rows):,} run rows carrying a duration; "
          f"{len(censored)} sit within {EPS:.0f}s of a known ceiling\n")

    if censored:
        print(f"{'source':<36}{'dur_s':>10}  {'status':<16}{'date':<12}ceiling")
        for sid, dur, status, day, name in sorted(censored):
            print(f"{sid[:34]:<36}{dur:>10.1f}  {str(status):<16}{day:<12}{name}")

    print()
    print("WHAT THE ESTIMATE WOULD BE WITHOUT THEM (max dur_s per source):")
    print(f"{'source':<36}{'with censored':>15}{'without':>12}  other runs")
    changed = 0
    for sid, runs in sorted(per_source.items()):
        durs = [d for d, _, _ in runs]
        clean = [d for d, _, _ in runs
                 if not any(abs(d - c) <= EPS for c in CEILINGS)]
        if not clean or max(durs) == max(clean):
            continue
        changed += 1
        others = ", ".join(f"{d / 60:.1f}m" for d in sorted(clean, reverse=True)[:4])
        print(f"{sid[:34]:<36}{max(durs) / 60:>13.1f}m{max(clean) / 60:>11.1f}m  {others}")

    print()
    print(f"{changed} source(s) would be estimated differently.")
    print()
    print("NOT COUNTED, and it matters: the LOCAL pass kills at (run budget + 10 min), which is")
    print("clamped nightly to the time before the next CI cron — so its ceiling is a DIFFERENT")
    print("number every night and cannot be recognised after the fact. That is the whole")
    print("argument for stamping censorship into the row at WRITE time rather than inferring it")
    print("later, and this tool can only see the two fixed ceilings.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
