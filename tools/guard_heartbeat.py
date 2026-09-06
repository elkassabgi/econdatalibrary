"""Publish the workstation watchdog's heartbeat to R2, and read it back from CI.

WHY. The 17 cloud-infeasible sources (noaa, bea, comtrade, ons_uk, wid, eia, ...) update ONLY
from the workstation, driven by RELAUNCH_GUARD_LOOP.ps1. When that loop dies, those sources
stop and nothing says so: the cloud health gate deliberately declines to judge
`run_location: local` sources, and its silence about them is by design.

`health.route_silence` added the coarse net — three days with no successful local run anywhere
reddens the gate. Its own docstring is explicit that this CANNOT catch a short outage: on
2026-08-02 the loop died at 15:16, the local heavy pass went ~7h past due, and bis/bls/
cepii_gravity/faostat still carried successes from the day before, so no three-day threshold
could have fired. It said the instrument for that case is "the guard's own heartbeat on that
machine" — and that heartbeat was a LOCAL FILE that nothing outside the machine ever read. This
is the missing half: the beat, published where CI can see it.

WHAT IS PUBLISHED. `_aqueduct/guard_heartbeat.json`, holding the UTC instant of the last
COMPLETED guard tick plus which tracked jobs were alive at that moment.

DELIBERATELY NOT THE STATE STORE. The state store is single-writer via ETag compare-and-swap
(R5); a 5-minutely write from a second machine would contend with the updater for no reason.
This is its own key, written blindly, read by anyone.

AGE COMES FROM THE CONTENT, NOT FROM LastModified. The loop stamps the instant it finished a
tick and that string is what `--check` measures. An object's LastModified says when R2 accepted
a PUT, which is a fact about the upload, not about the watchdog — and a re-upload of a stale
body would look perfectly fresh.

THE OBSERVER CANNOT FAKE IT. R260: a liveness probe once matched its own command line and
reported a dead loop alive. Here the writer is the loop on the workstation and the reader is a
gate in CI — a different machine, different process. Nothing the reader does can produce a beat.

    python tools/guard_heartbeat.py --publish          # workstation, once per guard tick
    python tools/guard_heartbeat.py --check            # CI; exit 1 if stale
    python tools/guard_heartbeat.py --check --max-age-min 45
"""
from __future__ import annotations
import argparse
import datetime as dt
import json
import os
import shlex
import socket
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import r2_util                                    # noqa: E402

BUCKET = "econ-data"
KEY = "_aqueduct/guard_heartbeat.json"

# The loop ticks every 5 minutes. 45 allows several consecutive missed ticks (a slow guard
# call is killed at 120s and the loop continues, so a single overrun must not cry wolf)
# while still catching a dead watchdog long before the local heavy pass is due again.
DEFAULT_MAX_AGE_MIN = 45.0

# Jobs the guard is responsible for keeping alive. Reported so a beat distinguishes "the
# watchdog is running" from "the watchdog is running AND its crawlers are up".
TRACKED = ("ingest_cbs_nl.py", "ingest_gus_dbw.py", "ingest_istat_sliced.py")


FLAP_FLOOR_S = 60     # a tracked crawler younger than this is being RELAUNCHED, not working


def _script_tokens(cmdline: str) -> "set[str]":
    """The BASENAMES of the .py arguments in a command line.

    Comparing basenames of whitespace/quote-delimited tokens is the whole anti-substring rule:
    `jobs/ingest_cbs_nl.py` and `"E:/x/jobs/ingest_cbs_nl.py"` both yield `ingest_cbs_nl.py`,
    while `my_ingest_cbs_nl.py` and `ingest_cbs_nl.pyz` yield themselves and match nothing. A
    token that is not a .py path is ignored entirely, so a `--source` value or a prose fragment
    cannot masquerade as the script being run.
    """
    toks = set()
    try:
        parts = shlex.split(cmdline, posix=False) if cmdline else []
    except ValueError:
        parts = cmdline.split()      # unbalanced quote: degrade, never raise
    for raw in parts:
        tok = raw.strip("\"'")
        if tok.lower().endswith(".py"):
            toks.add(os.path.basename(tok.replace("\\", "/")))
    return toks


def _alive_jobs_detail() -> "list[dict]":
    """Tracked ingesters actually running, each with the age of its process.

    R799/R457 — A BARE SUBSTRING OVER THE CONCATENATED COMMAND LINES IS NOT LIVENESS. The old
    form joined every python.exe command line into one blob and asked `if t in cmds`. That is
    true when ANY process merely MENTIONS the name — an audit script, an editor, a grep, this
    very query — and it is true for a process in its first second. It matched
    `ingest_istat_sliced.py` about two seconds after the guard relaunched it, INSIDE the nine
    seconds that process lived before its designed preflight exit (esploradati times out at
    TCP:443 and sdmx.istat.it 302-redirects to itself), so the beat CI reads published
    `jobs_alive=3/3` while istat had crawled nothing at all. R457 is "a name match is not
    liveness", and this is that rule reintroduced in the publisher that watches the guard.

    Two changes. The name must appear as its own PATH COMPONENT within a SINGLE process's
    command line, so a mention inside a longer word or an unrelated argument cannot count. And
    every match carries its AGE, because "a process exists" and "a crawler is working" differ by
    exactly the nine seconds that failure lived in — `publish` uses the age to separate the two
    rather than reporting a flapping relaunch as a healthy crawl.

    Best-effort and never raises: an unreadable process table yields [], which upstream reports
    as fewer jobs than tracked, never as healthy.
    """
    try:
        import subprocess
        ps = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | "
             "Select-Object ProcessId, "
             "@{n='AgeS';e={[int]((Get-Date) - $_.CreationDate).TotalSeconds}}, "
             "CommandLine | ConvertTo-Json -Compress"],
            capture_output=True, text=True, timeout=60)
        data = json.loads(ps.stdout or "null")
    except Exception:                                        # noqa: BLE001
        return []
    if data is None:
        return []
    if isinstance(data, dict):        # ConvertTo-Json emits a bare object when there is one row
        data = [data]
    out = []
    for t in TRACKED:
        for p in data:
            if not isinstance(p, dict):
                continue
            if t in _script_tokens(p.get("CommandLine") or ""):
                age = p.get("AgeS")
                out.append({"name": t, "pid": p.get("ProcessId"),
                            "age_s": age if isinstance(age, int) else None})
                break
    return out


def _alive_jobs() -> "list[str]":
    """Names of the tracked ingesters currently running, flapping ones included."""
    return [d["name"] for d in _alive_jobs_detail()]


def _emptiness_verdict() -> dict:
    """Run the crawl-emptiness audit and carry its verdict in the beat.

    WHY IT RIDES THE HEARTBEAT (R274: name the reader). cbs_nl once fetched 144,000,000
    rows and wrote ZERO for weeks — the signal existed the whole time, in checkpoints on
    this machine, with nobody reading it. An audit only the workstation can see is a log,
    not monitoring; CI reads this beat every day, so the verdict travels to a reader who
    can act. Failure to RUN the audit is reported as unknown, never as clean."""
    import subprocess
    tool = os.path.join(os.path.dirname(os.path.abspath(__file__)), "audit_crawl_emptiness.py")
    try:
        r = subprocess.run([sys.executable, tool, "--json"], capture_output=True, text=True,
                           encoding="utf-8", errors="replace", timeout=900)
        reports = json.loads(r.stdout)
        return {"ran": True,
                "fetch_without_write": sum(len(x["empty"]) for x in reports),
                "unreadable": sum(len(x["unreadable"]) for x in reports),
                "detail": {x["source"]: [e[0] for e in x["empty"]][:10] for x in reports}}
    except Exception as e:                                   # noqa: BLE001
        return {"ran": False, "error": f"{type(e).__name__}: {e}"[:200]}


def publish() -> int:
    # `jobs_alive` now means WORKING, not merely present. A tracked crawler younger than
    # FLAP_FLOOR_S is one the guard has just relaunched, and counting it as alive is what let
    # `jobs_alive=3/3` be published while istat_sliced was exiting after nine seconds every
    # cycle (R799). Flapping jobs are carried in their own field so the distinction reaches the
    # reader instead of being averaged away.
    detail = _alive_jobs_detail()
    working = [d for d in detail if (d.get("age_s") or 0) >= FLAP_FLOOR_S]
    flapping = [d for d in detail if (d.get("age_s") or 0) < FLAP_FLOOR_S]
    body = {
        "utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "host": socket.gethostname(),
        "jobs_alive": [d["name"] for d in working],
        "jobs_flapping": flapping,
        "flap_floor_s": FLAP_FLOOR_S,
        "tracked": list(TRACKED),
        "emptiness": _emptiness_verdict(),
    }
    c = r2_util.client(write=True)
    c.put_object(Bucket=BUCKET, Key=KEY,
                 Body=json.dumps(body, indent=2).encode("utf-8"),
                 ContentType="application/json")
    print(f"published {KEY}: {body['utc']} host={body['host']} "
          f"jobs_alive={len(body['jobs_alive'])}/{len(TRACKED)}"
          + (f" FLAPPING={len(flapping)}" if flapping else ""))
    return 0


def check(max_age_min: float) -> int:
    c = r2_util.client()
    try:
        raw = c.get_object(Bucket=BUCKET, Key=KEY)["Body"].read()
    except Exception as e:                                   # noqa: BLE001
        # ABSENT IS NOT HEALTHY, but it is also not proof of an outage — it is proof the
        # instrument was never installed. Say which, so nobody reads a missing file as a pass.
        print(f"GUARD HEARTBEAT ABSENT ({type(e).__name__}) — {KEY} has never been published. "
              f"The workstation route is UNINSTRUMENTED, not proven healthy.")
        return 1
    try:
        body = json.loads(raw.decode("utf-8"))
        beat = dt.datetime.fromisoformat(body["utc"])
    except Exception as e:                                   # noqa: BLE001
        print(f"GUARD HEARTBEAT UNREADABLE ({type(e).__name__}): {raw[:200]!r}")
        return 1
    if beat.tzinfo is None:
        beat = beat.replace(tzinfo=dt.timezone.utc)
    age = (dt.datetime.now(dt.timezone.utc) - beat).total_seconds() / 60.0
    alive, tracked = body.get("jobs_alive") or [], body.get("tracked") or []
    flapping = body.get("jobs_flapping") or []
    where = (f"host={body.get('host','?')} jobs_alive={len(alive)}/{len(tracked)}"
             + (f" FLAPPING={len(flapping)}" if flapping else ""))

    if age > max_age_min:
        print(f"GUARD HEARTBEAT STALE: last tick {beat.isoformat()} "
              f"({age:.1f} min ago > {max_age_min:.0f}) — {where}")
        print("  The workstation watchdog is not ticking. The 17 run_location=local sources "
              "have NO other update path; nothing else in CI will notice this.")
        return 1

    if flapping:
        # Reported, never failed: this run is already red for other reasons on most days, and a
        # crawler restarting is a workstation condition, not a CI one. But it must be SAID —
        # the whole point of R799 is that `3/3` was published while one of the three was dying
        # after nine seconds every cycle, and nobody could see the difference.
        floor = body.get("flap_floor_s", FLAP_FLOOR_S)
        names = ", ".join(f"{f.get('name')} ({f.get('age_s')}s old)"
                          for f in flapping if isinstance(f, dict))
        print(f"  FLAPPING: {names} — younger than {floor}s, so the guard is RELAUNCHING these, "
              f"not watching them work. A name match is not liveness (R457/R799); check the "
              f"job's own log before reading the count above as health.")

    emp = body.get("emptiness") or {}
    if emp.get("ran") and emp.get("fetch_without_write"):
        print(f"CRAWL EMPTINESS DEFECT: {emp['fetch_without_write']} unit(s) are FETCHING AND "
              f"WRITING NOTHING — {emp.get('detail')}")
        print("  This is the cbs_nl class (144,000,000 rows fetched, 0 written, for weeks). "
              "Read that unit's parser before letting the crawl continue.")
        return 1
    if emp and not emp.get("ran"):
        print(f"  NOTE: crawl-emptiness audit did NOT run on the workstation "
              f"({emp.get('error')}) — emptiness is UNKNOWN this tick, not clean.")

    print(f"guard heartbeat OK: {age:.1f} min old ({beat.isoformat()}) — {where}"
          + (f", emptiness clean ({emp.get('fetch_without_write', 0)} defect units)"
             if emp.get("ran") else ""))
    if tracked and len(alive) < len(tracked):
        # The loop is alive but not doing its job — a different failure from a dead loop, and
        # one a bare timestamp would hide. Reported, not failed: a crawler that has FINISHED is
        # legitimately absent, and this tool cannot tell finished from dead.
        missing = [t for t in tracked if t not in alive]
        print(f"  NOTE: watchdog alive but {len(missing)} tracked job(s) not running: "
              f"{', '.join(missing)} — finished or dead, this cannot distinguish them.")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--publish", action="store_true", help="workstation: stamp a completed tick")
    g.add_argument("--check", action="store_true", help="CI: fail if the beat is stale")
    ap.add_argument("--max-age-min", type=float, default=DEFAULT_MAX_AGE_MIN)
    a = ap.parse_args()
    return publish() if a.publish else check(a.max_age_min)


if __name__ == "__main__":
    raise SystemExit(main())
