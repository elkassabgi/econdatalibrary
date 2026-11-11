"""The guard job of updater-daily.yml and updater-heavy.yml: is this run the REAL run of its slot, or the second
run of a slot an earlier run already served (then a no-op that ends before any state pull)?

WHY (PLAN V2, review AR-470). The desktop dispatcher (tools/econ_dispatch.py) starts each cron's work at the
cron's own time; GitHub still creates the cron's own run hours later. That late run must not do the work again:
with the slot credited to the dispatch, the writer gate (tools/ci_writer_gate.py) lets the desktop pass take the
state store, so a late cron that runs for real would be an unannounced second writer under a desktop pass (R5).
This guard is therefore R5-critical, and it FAILS CLOSED on every doubt (AR-470 H3): the job goes red, prints why,
and the `update` job (daily) / the matrix (heavy) never starts - no state is pulled or pushed.

Outputs (GITHUB_OUTPUT): real=true  - run the work;
                         real=false - an earlier run of this slot settled it; this run is a no-op.
Exit 1 (and no `real` output) = fail closed.

    a workflow_dispatch WITHOUT a slot          -> real (a manual/proof run; it never settles a slot)
    a workflow_dispatch with a slot and any of source / force / dry_run -> fail closed (such a run carries no slot
                                                   title, so it must not be treated as the slot's run)
    a slot dispatch or a scheduled run          -> list this workflow's runs (up to LISTING_READS reads) until THIS
                                                   run is in the listing (the positive control: an empty or stale
                                                   listing is never read as "no earlier run", R1660); then every
                                                   EARLIER run of the same slot (by databaseId) must be completed
                                                   (workflow-level concurrency: an earlier one cannot still be
                                                   running while this one holds the group - if one is, the listing
                                                   is stale: fail closed); if one of them settles the slot
                                                   (ci_writer_gate.run_settles on its jobs) -> no-op, else real.
Inputs come from the environment only (never `${{ }}` inside `run:`, AR-470 L3): SLOT_WORKFLOW, SLOT_EVENT,
SLOT_SCHEDULE, SLOT_INPUT, SLOT_SOURCE, SLOT_FORCE, SLOT_DRY_RUN, GITHUB_RUN_ID, GITHUB_OUTPUT; GH_TOKEN for gh.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ci_writer_gate as g  # noqa: E402

LISTING_LIMIT = 100          # >= 50 (AR-470 H3); a slot's runs are all created within one day of its cron
LISTING_READS = 3
READ_PAUSE_S = 10.0
FIELDS = "databaseId,displayTitle,createdAt,status,conclusion,event"


class FailClosed(Exception):
    pass


def _truthy(v) -> bool:
    return str(v or "").strip().lower() in ("true", "1", "yes")


def gh_list(wf: str) -> list:
    out = subprocess.run(["gh", "run", "list", "--repo", g.REPO, "--workflow", wf, "--limit", str(LISTING_LIMIT),
                          "--json", FIELDS], capture_output=True, text=True, timeout=60)
    if out.returncode != 0:
        raise FailClosed(f"gh run list failed: {out.stderr.strip()[:200]}")
    data = json.loads(out.stdout)
    if not isinstance(data, list):
        raise FailClosed("gh run list did not answer a list")
    return data


def decide(env: dict, list_runs=gh_list, fetch_jobs=g._gh_jobs, sleep=time.sleep) -> tuple[bool, str]:
    """(real, why). Raises FailClosed on every doubt. Pure apart from the two readers it is given."""
    wf = env.get("SLOT_WORKFLOW", "")
    if wf not in g.SLOT_KIND:
        raise FailClosed(f"SLOT_WORKFLOW {wf!r} is not a writer of {sorted(g.SLOT_KIND)}")
    event = env.get("SLOT_EVENT", "")
    # The workflow passes the event; the runner sets GITHUB_EVENT_NAME itself. They must agree (F2): a wiring slip
    # that made a scheduled run read as a slotless dispatch would make every late cron run for real.
    if event != env.get("GITHUB_EVENT_NAME", ""):
        raise FailClosed(f"SLOT_EVENT {event!r} != GITHUB_EVENT_NAME {env.get('GITHUB_EVENT_NAME')!r}")
    slot_in = (env.get("SLOT_INPUT") or "").strip()
    passthrough = [n for n, v in (("source", (env.get("SLOT_SOURCE") or "").strip()),
                                  ("force", _truthy(env.get("SLOT_FORCE"))),
                                  ("dry_run", _truthy(env.get("SLOT_DRY_RUN")))) if v]
    if event == "workflow_dispatch" and not slot_in:
        return True, "a dispatch without a slot: always real, never settles a slot"
    if event == "workflow_dispatch":
        if passthrough:
            raise FailClosed(f"a slot dispatch must not carry {', '.join(passthrough)} (it would not be titled as "
                             f"the slot's run, and a targeted run must never settle a full pass)")
        if g.parse_slot_tag(slot_in, wf) is None:
            raise FailClosed(f"slot {slot_in!r} is not a slot of {wf}")
    elif event == "schedule":
        if g.cron_hour(env.get("SLOT_SCHEDULE", ""), wf) is None:
            raise FailClosed(f"schedule {env.get('SLOT_SCHEDULE')!r} is not a cron of {wf} in the gate's WRITERS")
    else:
        raise FailClosed(f"event {event!r}: neither a dispatch nor a schedule")
    try:
        own_id = int(env.get("GITHUB_RUN_ID", ""))
    except ValueError:
        raise FailClosed("GITHUB_RUN_ID is not a number") from None

    rows, own, last_err = [], None, ""
    for i in range(LISTING_READS):
        if i:
            sleep(READ_PAUSE_S)
        try:
            rows = list(list_runs(wf))          # a failed read is retried inside the same budget (F1)
        except Exception as e:  # noqa: BLE001
            rows, last_err = [], f"{type(e).__name__}: {str(e)[:160]}"
            continue
        own = next((r for r in rows if r.get("databaseId") == own_id), None)
        if own is not None:
            break
    if own is None:
        raise FailClosed(f"run {own_id} is not in the listing after {LISTING_READS} reads (R1660: an empty or "
                         f"stale listing is not 'no earlier run'){'; last error ' + last_err if last_err else ''}")
    me = g.run_slot(wf, own)
    if me is None:
        raise FailClosed(f"this run's own title {own.get('displayTitle')!r} names no slot (run-name not applied?)")
    cron = me[0]
    # THE LISTING MUST REACH BACK PAST THE SLOT'S CRON, AT ANY LENGTH (review AR-470 round 2, F1). Every run of a
    # slot is created at or after its cron, so a listing that holds a run created BEFORE the cron holds every
    # earlier run of the slot - unless the listing is a partial answer, which is exactly what this rejects: a
    # listing of [own run] alone, or a short one that starts after the cron, is not "no earlier run".
    if not any(g._parse(r["createdAt"]) < cron for r in rows if r.get("createdAt")):
        raise FailClosed(f"the listing ({len(rows)} rows) holds no run created before the slot's cron "
                         f"{cron:%Y-%m-%dT%H:%M:%SZ}; an earlier run of the slot may be missing from it")
    earlier = sorted((r for r in rows if r.get("databaseId") is not None and r["databaseId"] < own_id
                      and (g.run_slot(wf, r) or (None,))[0] == cron), key=lambda r: r["databaseId"])
    for r in earlier:
        if r.get("status") != "completed":
            raise FailClosed(f"an earlier run of slot {g.slot_tag(wf, cron)} ({r['databaseId']}) is "
                             f"{r.get('status')}; under workflow-level concurrency it cannot be, so the listing is "
                             f"stale")
        if r.get("conclusion") == "cancelled":
            continue
        try:
            jobs = fetch_jobs(r["databaseId"])
        except Exception as e:  # noqa: BLE001
            raise FailClosed(f"jobs of earlier run {r['databaseId']} could not be read ({type(e).__name__})") from None
        if g.run_settles(r, jobs):
            return False, (f"slot {g.slot_tag(wf, cron)} was settled by run {r['databaseId']} "
                           f"({r.get('event')}, {r.get('conclusion')}): this run is a no-op")
    return True, (f"slot {g.slot_tag(wf, cron)}: no earlier run settled it ({len(earlier)} earlier run(s) of the "
                  f"slot listed): this run is real")


def main(env=None, list_runs=gh_list, fetch_jobs=g._gh_jobs, sleep=time.sleep) -> int:
    env = dict(os.environ) if env is None else env
    try:
        real, why = decide(env, list_runs, fetch_jobs, sleep)
    except FailClosed as e:
        print(f"::error::slot guard FAILED CLOSED - no state will be pulled or pushed by this run: {e}")
        return 1
    print(f"slot guard: real={'true' if real else 'false'} - {why}")
    if not real:
        print(f"::notice::{why}")
    out = env.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a", encoding="utf-8") as f:
            f.write(f"real={'true' if real else 'false'}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
