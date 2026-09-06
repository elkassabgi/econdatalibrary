"""How often does the guard actually relaunch each tracked job, and how often does it tick?

WHY THIS IS IN THE REPO. `tools/guard_heartbeat.py` carries these figures in a comment, and a
number in a shipped comment needs its own instrument exactly as much as a number in a report
does. An earlier version of that comment said gus_dbw was relaunched "every ~53 min"; I had
copied it from a review instead of measuring it, and it is wrong by a factor of ~25 (R804 #4).
The correction then cited this script while it existed only in a session temp directory, which
is a citation to nowhere (R653, R808 #5). So it lives here.

    python tools/measure_relaunch_cadence.py

Instrument: logs/_guard.log, whose relaunch lines look like

    2026-09-06T03:11:11  relaunched cbs_nl

TIMEZONE. That file stamps LOCAL time with no zone marker, while the heartbeat stamps UTC with a
Z - which is why 03:16:27 in one file is 08:16:27Z in the other (R803's neighbourhood). Gaps are
differences, so the offset cancels and the cadences below are unaffected; the absolute stamps in
the header line are LOCAL and are labelled so.

WHAT "TICK CADENCE" MEANS HERE, precisely: the gap between consecutive distinct timestamps
appearing anywhere in this log. The loop only writes when it does something, so this is the
cadence of ticks that ACTED, not of every tick - a distinction worth keeping, because the loop's
own sleep is 300 s and the measured median comes out above it.
"""
from __future__ import annotations
import collections
import datetime as dt
import os
import re
import statistics
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
LOG = os.path.join(ROOT, "logs", "_guard.log")
RELAUNCH = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\s+relaunched\s+(\S+)")
STAMP = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})")


def _human(s: float) -> str:
    if s < 90:
        return f"{s:.0f}s"
    if s < 5400:
        return f"{s / 60:.1f} min"
    return f"{s / 3600:.1f} h"


def main() -> int:
    # Derived by default; an explicit path so the tool can be pointed at the WORKSTATION's log
    # from a worktree, which has no logs/ of its own. Deriving and then hardcoding a fallback is
    # what put a production path in another tool in this same store (R807 #5), so there is no
    # fallback: name the file or run it where the log is.
    global LOG
    if len(sys.argv) > 1:
        LOG = sys.argv[1]
    if not os.path.exists(LOG):
        print(f"no guard log at {LOG} — this measures the WORKSTATION's log and there is none "
              f"here (CI has no guard loop).", file=sys.stderr)
        return 1
    events: dict = collections.defaultdict(list)
    ticks: list = []
    with open(LOG, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            m = RELAUNCH.match(line)
            if m:
                events[m.group(2)].append(dt.datetime.fromisoformat(m.group(1)))
            t = STAMP.match(line)
            if t:
                ticks.append(dt.datetime.fromisoformat(t.group(1)))

    print(f"{LOG}")
    print(f"  {sum(len(v) for v in events.values()):,} relaunch lines, {len(events)} job(s)")
    if ticks:
        print(f"  spans {min(ticks).isoformat()} .. {max(ticks).isoformat()}  (LOCAL time)")

    print(f"\n{'job':<18}{'n':>7}{'median gap':>14}{'mean gap':>13}{'min':>10}")
    for job in sorted(events, key=lambda k: -len(events[k])):
        ts = sorted(events[job])
        if len(ts) < 2:
            print(f"{job:<18}{len(ts):>7}{'(one event)':>14}")
            continue
        gaps = [(b - a).total_seconds() for a, b in zip(ts, ts[1:])]
        print(f"{job:<18}{len(ts):>7}{_human(statistics.median(gaps)):>14}"
              f"{_human(statistics.mean(gaps)):>13}{_human(min(gaps)):>10}")

    uniq = sorted(set(ticks))
    gaps = [(b - a).total_seconds() for a, b in zip(uniq, uniq[1:])]
    # Gaps over an hour are outages, not cadence — the loop was dead (R803's class). Excluded,
    # and counted, so the exclusion is visible rather than silent.
    live = [g for g in gaps if g < 3600]
    if live:
        print(f"\ntick cadence (gaps under 1 h): n={len(live)}  "
              f"median {statistics.median(live):.1f}s  mean {statistics.mean(live):.1f}s")
        print(f"  excluded {len(gaps) - len(live)} gap(s) of an hour or more — those are "
              f"outages, not cadence")
    return 0


if __name__ == "__main__":
    sys.exit(main())
