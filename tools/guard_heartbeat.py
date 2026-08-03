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


def _alive_jobs() -> "list[str]":
    """Which tracked ingesters are currently running. Best-effort: never raises."""
    out = []
    try:
        import subprocess
        ps = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" "
             "| Select-Object -ExpandProperty CommandLine"],
            capture_output=True, text=True, timeout=60)
        cmds = ps.stdout or ""
        for t in TRACKED:
            if t in cmds:
                out.append(t)
    except Exception:                                        # noqa: BLE001
        pass
    return out


def publish() -> int:
    body = {
        "utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "host": socket.gethostname(),
        "jobs_alive": _alive_jobs(),
        "tracked": list(TRACKED),
    }
    c = r2_util.client(write=True)
    c.put_object(Bucket=BUCKET, Key=KEY,
                 Body=json.dumps(body, indent=2).encode("utf-8"),
                 ContentType="application/json")
    print(f"published {KEY}: {body['utc']} host={body['host']} "
          f"jobs_alive={len(body['jobs_alive'])}/{len(TRACKED)}")
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
    where = f"host={body.get('host','?')} jobs_alive={len(alive)}/{len(tracked)}"

    if age > max_age_min:
        print(f"GUARD HEARTBEAT STALE: last tick {beat.isoformat()} "
              f"({age:.1f} min ago > {max_age_min:.0f}) — {where}")
        print("  The workstation watchdog is not ticking. The 17 run_location=local sources "
              "have NO other update path; nothing else in CI will notice this.")
        return 1

    print(f"guard heartbeat OK: {age:.1f} min old ({beat.isoformat()}) — {where}")
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
