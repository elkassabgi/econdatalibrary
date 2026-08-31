"""Record the unit an EXTERNAL kill silently un-recorded, so its cost estimate stays honest.

THE HOLE (found 2026-08-31, investigating why 18 live local sources had not succeeded in 7+
days). `store.log_run` writes a unit's duration when the unit completes or raises — from
inside Python. When `run_local_heavy.ps1` hard-stops the whole updater at its wall-clock
budget (exit 124), the in-flight unit's process dies mid-flight and NO run row is written.

`state.run_cost_estimate()` is MAX(dur_s) over the last 5 recorded runs. Its own docstring
names under-estimation as "the failure the lane exists to prevent" — and the external-kill
path produces exactly that, invisibly: the killed giant keeps whatever cheap estimate its old
recorded runs gave it, re-enters the cheap band, and eats the next night's whole budget too.
Measured: `unctad_tradefoodcatbyproc` consumed ~140 min of a 153-min pass on 2026-08-30/31,
was killed, left no row, and would have led the queue again tonight while `bea`, `eia`,
`census`, `statcan`, `oecd`, `noaa` — all RED or ATTENTION in the health gate — waited again.

WHAT THIS DOES. Parse the updater log the runner just killed, find the last `>>> src/unit`
with no matching `<<<`, attribute the unaccounted elapsed to it, and write ONE run row with
status `killed_external`. MAX then sees the true cost and re-bands the source honestly.

Attribution is deliberately coarse: killed-unit seconds = total updater elapsed minus the
completed units' own `took Ns` figures. Startup and skip overhead land on the killed unit,
OVER-estimating slightly — the direction run_cost_estimate documents as safe (a too-high
estimate costs a source its fast lane, recoverable; too-low starves the fleet).

Runs BEFORE push-state in the runner's kill path, while this machine is still the one state
writer (R5). Idempotent enough for its call site: the runner invokes it once per kill.

Usage:
    py tools/record_killed_unit.py <updater_log_path> <total_elapsed_seconds> [--apply]

Without --apply it prints what it would record (the runner passes --apply).
"""
from __future__ import annotations

import argparse
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

# `took {dur:,.0f}s` — the orchestrator COMMA-GROUPS seconds, so oecd prints `took 24,480s`.
# Review caught v1's `\d+` matching nothing there: on any night with a completed unit >= 1,000s
# (the giant route's normal case) the done-set went empty, two units looked open, and the tool
# refused — a silent no-op on exactly the logs it exists for.
_START = re.compile(r"\[orchestrator\] >>> (\S+?)/(\S+) ")
_DONE = re.compile(r"\[orchestrator\] <<< (\S+?)/(\S+) took ([\d,]+(?:\.\d+)?)s")
# A LOCKED unit prints `>>>` FIRST (orchestrate.py:1475) and only then the lock verdict — and
# the kill MANUFACTURES this shape every time: the killed giant's lease persists (48h TTL for
# giants), so the next pass's log carries `>>> giant/...` followed by `LOCKED giant/...`. That
# trailing pair must not swallow the attribution: LOCKED closes its unit like `<<<` does.
_LOCKED = re.compile(r"\[orchestrator\] LOCKED (\S+?)/(\S+?)\b")


def parse_killed(log_text: str, total_elapsed_s: float):
    """-> (source_id, unit_id, attributed_seconds) or None when every unit completed.

    THE IN-FLIGHT UNIT IS THE LAST `>>>` WITHOUT A `<<<` — not "the only one". Review
    demonstrated that four orchestrator exit paths never print `<<<` at all: an earned
    no_change prints NOTHING, a LOCKED unit and the detect-phase failure paths `continue`
    before the closing line. So orphan `>>>` lines are NORMAL in a healthy log, and v1's
    refuse-on->1-open fired on legitimate killed passes (including the one the kill itself
    manufactures: the killed giant's lease persists, the NEXT pass logs `>>> ... LOCKED`).
    The orchestrator is strictly serial, so every `>>>` before the last one is closed by
    construction; its unaccounted time lands in the residual — the over-estimate direction
    run_cost_estimate documents as safe.
    """
    started: list[tuple[str, str]] = []
    done_s = 0.0
    done: set[tuple[str, str]] = set()
    for line in log_text.splitlines():
        m = _START.search(line)
        if m:
            started.append((m.group(1), m.group(2)))
            continue
        m = _DONE.search(line)
        if m:
            done.add((m.group(1), m.group(2)))
            done_s += float(m.group(3).replace(",", ""))
            continue
        m = _LOCKED.search(line)
        if m:
            done.add((m.group(1), m.group(2)))   # locked = never ran; ~0s, closes the unit
    if not started:
        return None
    last = started[-1]
    if last in done:
        return None                     # the final unit completed; the pass ended cleanly
    src, unit = last
    attributed = max(60.0, total_elapsed_s - done_s)
    return src, unit, round(attributed, 1)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("log_path")
    ap.add_argument("total_elapsed_s", type=float)
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    text = open(a.log_path, encoding="utf-8", errors="replace").read()
    hit = parse_killed(text, a.total_elapsed_s)
    if hit is None:
        print("every started unit completed — nothing was killed mid-flight, nothing to do")
        return 0
    src, unit, secs = hit
    print("killed in flight: %s/%s — attributing %.1fs (%.1f min)" % (src, unit, secs, secs / 60))
    if not a.apply:
        print("(dry run — pass --apply to record)")
        return 0

    from updater.state import StateStore  # noqa: E402  (import here so --help costs nothing)
    st = StateStore()
    st.log_run(src, unit, "killed_external", obs=0, dur_s=secs,
               note="hard-stopped by run_local_heavy wall-clock budget; recorded by "
                    "tools/record_killed_unit.py so run_cost_estimate re-bands this source "
                    "honestly instead of letting a stale cheap estimate starve the fleet")
    print("recorded: %s/%s killed_external dur_s=%.1f" % (src, unit, secs))
    return 0


if __name__ == "__main__":
    sys.exit(main())
