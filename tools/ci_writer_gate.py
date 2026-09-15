"""Is a cloud state writer running, or about to? The desktop heavy pass must not overlap one.

WHY (2026-09-15). The desktop pass (tools/run_local_heavy.ps1) and the cloud updater workflows all pull
data/_aqueduct/state.db from R2 and push it back with an ETag compare-and-swap (updater/run.py), so
when two passes overlap the second push is refused and that pass's whole bookkeeping is thrown away
(ledger R5). The runner used to ask only "is an updater-daily run in flight?", plus fixed windows
measured on 2026-08-23, when GitHub started scheduled runs 13-55 min late. Measured over every
scheduled run GitHub still lists (2026-07-25..09-15, 175 runs): the start lag has a median of 133-143
min, a 95th percentile of 366-368 min and a maximum of 710 min, and 5 runs started more than 8 h late.
A desktop pass started at 10:38Z on 2026-09-15 saw nothing in flight; the 06:00Z run started under it
at 11:18Z and the cloud run's push was refused at 15:28Z. On 09-08 and 09-13 the desktop pass lost.

So a cron whose time has passed but whose run GitHub has not created yet is as busy as a run in
flight. This gate blocks on either, for both state-writing workflows, and never answers CLEAR when it
cannot read CI. A cron counts as pending until its scheduled run appears or the workflow's next cron
arrives (12 h, above the longest lag measured); after 8 h it also prints a warning, because GitHub may
have skipped it. A disabled workflow cannot write, so its crons are not pending. A run left unfinished
for more than a day (GitHub cancels queued runs after 24 h) is reported instead of blocking for ever.

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
PENDING_HORIZON = CRON_SPACING                  # longest measured start lag: 710 min
WARN_PENDING_AFTER = dt.timedelta(hours=8)
STUCK_AFTER = dt.timedelta(hours=24)
RUN_LIMIT = 50
EXIT_CLEAR, EXIT_UNKNOWN, EXIT_BLOCKED = 0, 2, 3


def _parse(ts: str) -> dt.datetime:
    return dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))


def assess(runs: dict, now: dt.datetime, states: dict | None = None) -> tuple[bool, str, list]:
    """runs: {workflow file: [{"createdAt", "event", "status"}, ...]} as `gh run list --json` gives them;
    states: {workflow file: "active" | "disabled_manually" | ...} or None when unknown.
    Returns (blocked, reason, notes)."""
    notes = []
    for wf in WRITERS:
        for r in runs.get(wf, []):
            if r["status"] == "completed":
                continue
            if now - _parse(r["createdAt"]) > STUCK_AFTER:
                notes.append(f"WARNING: {wf} run created {r['createdAt']} is still {r['status']} after more than "
                             f"a day; not treated as a writer - cancel it if it is stuck")
                continue
            return True, f"{wf} run created {r['createdAt']} is {r['status']}", notes
    for wf, hours in WRITERS.items():
        if states is not None and states.get(wf) != "active":
            notes.append(f"NOTE: {wf} is {states.get(wf, 'not listed')}; its crons are not waited for")
            continue
        for back in (1, 0):
            day = (now - dt.timedelta(days=back)).date()
            for h in hours:
                cron = dt.datetime(day.year, day.month, day.day, h, tzinfo=dt.timezone.utc)
                if not cron <= now < cron + PENDING_HORIZON:
                    continue
                started = any(r["event"] == "schedule" and cron <= _parse(r["createdAt"]) < cron + CRON_SPACING
                              for r in runs.get(wf, []))
                if not started:
                    late = int((now - cron).total_seconds() // 60)
                    if now - cron >= WARN_PENDING_AFTER:
                        notes.append(f"WARNING: {wf} {cron:%H:%M}Z cron has had no run for {late} min; "
                                     f"GitHub may have skipped it")
                    return True, (f"{wf} {cron:%H:%M}Z cron has not started yet ({late} min after its "
                                  f"scheduled time; GitHub has started these up to ~12 h late)"), notes
    return False, "no cloud state writer in flight or pending", notes


def decide(runs: dict, now: dt.datetime, states: dict | None = None) -> tuple[bool, str]:
    blocked, reason, _notes = assess(runs, now, states)
    return blocked, reason


def _gh_json(args: list):
    out = subprocess.run(["gh", *args], capture_output=True, text=True, timeout=60)
    if out.returncode != 0:
        raise RuntimeError(f"gh {' '.join(args[:3])} failed: {out.stderr.strip()[:200]}")
    data = json.loads(out.stdout)
    if not isinstance(data, list):
        raise RuntimeError(f"gh {' '.join(args[:3])} returned {type(data).__name__}, not a list")
    return data


def _gh_runs(wf: str) -> list:
    return _gh_json(["run", "list", "--workflow", wf, "--limit", str(RUN_LIMIT), "--json", "createdAt,event,status"])


def _gh_states() -> dict:
    rows = _gh_json(["workflow", "list", "--all", "--json", "path,state"])
    return {r["path"].rsplit("/", 1)[-1]: r["state"] for r in rows}


def main(fetch=_gh_runs, fetch_states=_gh_states, now=None) -> int:
    try:
        runs = {wf: fetch(wf) for wf in WRITERS}
        states = fetch_states()
        blocked, reason, notes = assess(runs, now or dt.datetime.now(dt.timezone.utc), states)
    except Exception as e:  # noqa: BLE001 - failing to read CI is "cannot tell", never "clear"
        print(f"UNKNOWN: could not read CI runs ({type(e).__name__}: {str(e)[:200]})")
        return EXIT_UNKNOWN
    for n in notes:
        print(n)
    print(("BLOCKED: " if blocked else "CLEAR: ") + reason)
    return EXIT_BLOCKED if blocked else EXIT_CLEAR


if __name__ == "__main__":
    sys.exit(main())
