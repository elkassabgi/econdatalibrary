"""Is a cloud state writer running, or about to? The desktop heavy pass must not overlap one.

WHY (2026-09-15). The desktop pass (tools/run_local_heavy.ps1) and the cloud updater workflows all pull
data/_aqueduct/state.db from R2 and push it back with an ETag compare-and-swap (updater/run.py), so
when two passes overlap the second push is refused and that pass's whole bookkeeping is thrown away
(ledger R5). The runner used to ask only "is an updater-daily run in flight?", plus fixed windows
measured on 2026-08-23, when GitHub started scheduled runs 13-55 min late. Measured 2026-09-03..15 the
lag is hours: updater-daily's 06:00Z cron started 10:07-12:06Z and its 18:00Z cron 19:56-21:29Z;
updater-heavy's 03:00Z cron started 07:19-08:29Z and its 15:00Z cron 16:58-19:38Z. A desktop pass
started at 10:38Z on 2026-09-15 saw nothing in flight, the 06:00Z run started under it at 11:18Z, and
the cloud run's push was refused at 15:28Z; on 09-08 and 09-13 the desktop pass lost instead.

So a cron whose time has passed but whose run GitHub has not created yet is as busy as a run in
flight. This gate blocks on either, for both state-writing workflows, and never answers CLEAR when it
cannot read CI.

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
# The longest measured start lag is 366 min (updater-daily's 06:00Z cron on 2026-09-14 started 12:06Z).
# A cron GitHub never runs stops blocking after this long.
MAX_START_LAG = dt.timedelta(hours=8)
CRON_SPACING = dt.timedelta(hours=12)
EXIT_CLEAR, EXIT_UNKNOWN, EXIT_BLOCKED = 0, 2, 3


def _parse(ts: str) -> dt.datetime:
    return dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))


def decide(runs: dict, now: dt.datetime) -> tuple[bool, str]:
    """runs: {workflow file: [{"createdAt", "event", "status"}, ...]} as `gh run list --json` gives them.
    Returns (blocked, reason)."""
    for wf in WRITERS:
        for r in runs.get(wf, []):
            if r["status"] != "completed":
                return True, f"{wf} run created {r['createdAt']} is {r['status']}"
    for wf, hours in WRITERS.items():
        for back in (1, 0):
            day = (now - dt.timedelta(days=back)).date()
            for h in hours:
                cron = dt.datetime(day.year, day.month, day.day, h, tzinfo=dt.timezone.utc)
                if not cron <= now < cron + MAX_START_LAG:
                    continue
                started = any(r["event"] == "schedule" and cron <= _parse(r["createdAt"]) < cron + CRON_SPACING
                              for r in runs.get(wf, []))
                if not started:
                    late = int((now - cron).total_seconds() // 60)
                    return True, (f"{wf} {cron:%H:%M}Z cron has not started yet ({late} min after its "
                                  f"scheduled time; GitHub has started these up to ~6 h late)")
    return False, "no cloud state writer in flight or pending"


def _gh_runs(wf: str) -> list:
    out = subprocess.run(["gh", "run", "list", "--workflow", wf, "--limit", "20",
                          "--json", "createdAt,event,status"],
                         capture_output=True, text=True, timeout=60)
    if out.returncode != 0:
        raise RuntimeError(f"gh run list --workflow {wf} failed: {out.stderr.strip()[:200]}")
    data = json.loads(out.stdout)
    if not isinstance(data, list):
        raise RuntimeError(f"gh run list --workflow {wf} returned {type(data).__name__}, not a list")
    return data


def main(fetch=_gh_runs, now=None) -> int:
    try:
        runs = {wf: fetch(wf) for wf in WRITERS}
        blocked, reason = decide(runs, now or dt.datetime.now(dt.timezone.utc))
    except Exception as e:  # noqa: BLE001 - failing to read CI is "cannot tell", never "clear"
        print(f"UNKNOWN: could not read CI runs ({type(e).__name__}: {str(e)[:200]})")
        return EXIT_UNKNOWN
    print(("BLOCKED: " if blocked else "CLEAR: ") + reason)
    return EXIT_BLOCKED if blocked else EXIT_CLEAR


if __name__ == "__main__":
    sys.exit(main())
