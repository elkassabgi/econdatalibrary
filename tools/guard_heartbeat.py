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


# One guard tick is ~316 s measured over logs/_guard.log (median, n=1568), so a tracked process
# younger than this was (re)started within the last tick. That is a FACT worth printing, not a
# verdict: cbs_nl is relaunched legitimately every ~68 min and gus_dbw every ~53 min, so roughly
# one beat in six catches a perfectly healthy crawler in its first seconds.
RELAUNCH_WINDOW_S = 330


def _script_of(cmdline: str) -> "str | None":
    """The basename of the script a python command line is RUNNING, or None.

    THE SCRIPT IS THE FIRST .py TOKEN, AND ONLY THAT ONE. Returning every .py token was the
    previous version's bug and it re-opened the hole it was written to close: a tracked name
    passed as an ARGUMENT counted as the job running, so `python other.py --note ingest_x.py`,
    `python -m py_compile ingest_x.py` and `pytest tests/t.py ingest_x.py` all forged a beat.
    Those are precisely the command lines a session runs while investigating a sick crawler, so
    the instrument would lie hardest exactly when someone was looking at it (R260: the observer
    must not be able to fake the beat).

    `-m` and `-c` take a module or a program text rather than a path, so any .py token after one
    of them is an argument and never the script.

    Basenames, so `jobs/ingest_x.py` and `"E:\\...\\jobs\\ingest_x.py"` agree while
    `my_ingest_x.py` and `ingest_x.pyz` match nothing.

    An unparseable command line returns None - "not running" - rather than falling back to a
    whitespace split, which is how the previous version restored the very substring bug it
    replaced. This function only feeds the HEARTBEAT; the guard's own relaunch decision lives in
    RELAUNCH_GUARD.ps1, so failing closed here mis-reports and never mis-relaunches.
    """
    if not cmdline:
        return None
    try:
        parts = shlex.split(cmdline, posix=False)
    except ValueError:
        return None
    for raw in parts[1:]:                       # [0] is the interpreter
        tok = raw.strip("\"'")
        if tok in ("-m", "-c"):
            return None
        if tok.lower().endswith(".py"):
            return os.path.basename(tok.replace("\\", "/"))
    return None


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

    Two changes. A tracked name counts only when it is the SCRIPT a single process is running
    (see `_script_of`), never a mention anywhere in the concatenated blob. And every match
    carries its process id and AGE, because "a process exists" and "a crawler is working" differ
    by exactly the nine seconds that failure lived in.

    THE AGE IS REPORTED, NOT JUDGED. An earlier version of this fix dropped young processes out
    of `jobs_alive` on a 60-second floor, which is wrong: measured over logs/_guard.log,
    cbs_nl is legitimately relaunched 372 times at a 68.2-minute median and gus_dbw 38 times at
    53.3 minutes, and `publish` runs ~2 s after a tick — so about one beat in six would have
    called a perfectly healthy crawler dead, and the existing "not running" note would then have
    printed something false. One sample per tick cannot separate a restart from a crash loop
    (R54), so this publishes the fact — pid and age — and leaves the verdict to a reader who can
    look at the job's own log.

    Returns None, never [], when the process table could not be read: "unknown" and "nothing is
    running" are different, and a blind instrument must not read as a clean one.
    """
    ok, data = False, []
    try:
        import subprocess
        ps = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             "[Console]::OutputEncoding=[Text.Encoding]::UTF8; "
             "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | "
             "Select-Object ProcessId, "
             "@{n='AgeS';e={[int]((Get-Date) - $_.CreationDate).TotalSeconds}}, "
             "CommandLine | ConvertTo-Json -Compress"],
            # ENCODING IS NOT OPTIONAL HERE. `text=True` decodes PowerShell's stdout with the
            # ANSI code page, so ONE unrelated process anywhere on the box with a non-ASCII path
            # raises UnicodeDecodeError, the bare except swallows it, and the beat reports
            # jobs_alive=[] while both crawlers are running - a BLIND instrument reading as a
            # clean one. Reproduced: a throwaway process with "u-umlaut" in its path took this
            # function from two live crawlers to zero. This fleet crawls Dutch, Polish and
            # Italian sources. `_emptiness_verdict` below already got this right.
            capture_output=True, timeout=60)
        if ps.returncode == 0:
            data = json.loads(ps.stdout.decode("utf-8", "replace") or "null")
            ok = True
    except Exception:                                        # noqa: BLE001
        ok, data = False, []
    if not ok:
        # An unreadable process table is UNKNOWN, never "nothing is running". The caller marks
        # the beat so a reader cannot mistake a blind sample for an empty machine.
        return None
    if data is None:
        # A SUCCESSFUL query with no rows: PowerShell prints nothing, which becomes "null". That
        # is a machine with no python processes - genuinely empty, and NOT the unknown above.
        # Collapsing the two was a real regression, caught by this file's own test.
        data = []
    if isinstance(data, dict):        # ConvertTo-Json emits a bare object when there is one row
        data = [data]
    out = []
    for t in TRACKED:
        for p in data:
            if not isinstance(p, dict):
                continue
            if _script_of(p.get("CommandLine") or "") == t:
                age = p.get("AgeS")
                out.append({"name": t, "pid": p.get("ProcessId"),
                            "age_s": age if isinstance(age, int) and age >= 0 else None})
                break
    return out


def _alive_jobs() -> "list[str]":
    """Names of the tracked ingesters currently running ([] if the table was unreadable)."""
    return [d["name"] for d in (_alive_jobs_detail() or [])]


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
    # `jobs_alive` keeps its meaning - the tracked ingesters PRESENT - so every existing reader
    # is unchanged. What was missing is why `3/3` could be published while istat_sliced was
    # exiting after nine seconds (R799): the beat carried no way to tell a crawl from a
    # relaunch. `jobs_detail` adds the pid and age of each one, and `table_ok` says whether the
    # process table was readable at all, so a blind sample can never be read as an empty
    # machine.
    detail = _alive_jobs_detail()
    body = {
        "utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "host": socket.gethostname(),
        "jobs_alive": [d["name"] for d in (detail or [])],
        "jobs_detail": detail or [],
        "table_ok": detail is not None,
        "relaunch_window_s": RELAUNCH_WINDOW_S,
        "tracked": list(TRACKED),
        "emptiness": _emptiness_verdict(),
    }
    c = r2_util.client(write=True)
    c.put_object(Bucket=BUCKET, Key=KEY,
                 Body=json.dumps(body, indent=2).encode("utf-8"),
                 ContentType="application/json")
    print(f"published {KEY}: {body['utc']} host={body['host']} "
          + (f"jobs_alive={len(body['jobs_alive'])}/{len(TRACKED)}" if detail is not None
             else "jobs_alive=UNKNOWN (process table unreadable)"))
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
    detail = body.get("jobs_detail")
    detail = detail if isinstance(detail, list) else []
    table_ok = body.get("table_ok", True)          # older beats have no such field
    where = (f"host={body.get('host','?')} jobs_alive={len(alive)}/{len(tracked)}"
             if table_ok else
             f"host={body.get('host','?')} jobs_alive=UNKNOWN (process table unreadable)")

    if age > max_age_min:
        print(f"GUARD HEARTBEAT STALE: last tick {beat.isoformat()} "
              f"({age:.1f} min ago > {max_age_min:.0f}) — {where}")
        print("  The workstation watchdog is not ticking. The 17 run_location=local sources "
              "have NO other update path; nothing else in CI will notice this.")
        return 1

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
    # The ages, under the line they annotate. This is the half R799 found missing: `3/3` was
    # published every tick while istat_sliced exited after nine seconds, and nothing in the beat
    # let a reader see it. A job restarted within the last guard tick is FLAGGED, not judged —
    # cbs_nl is legitimately relaunched every ~68 min, so this fires on a healthy crawler about
    # one beat in six, and calling that dead is a worse lie than the one being fixed.
    window = body.get("relaunch_window_s", RELAUNCH_WINDOW_S)
    young = [d for d in detail
             if isinstance(d, dict) and isinstance(d.get("age_s"), int) and d["age_s"] < window]
    if detail:
        print("  jobs: " + ", ".join(
            f"{d.get('name')} (pid {d.get('pid')}, {d.get('age_s')}s)"
            for d in detail if isinstance(d, dict)))
    if young:
        print(f"  RESTARTED within the last guard tick ({window}s): "
              + ", ".join(f"{d.get('name')} ({d.get('age_s')}s old)" for d in young)
              + " — one sample cannot tell a normal relaunch from a crash loop (R54). If the "
                "same job is this young on consecutive beats, read its own log: a process that "
                "exists is not a crawler that is working (R457/R799).")

    if not table_ok:
        print("  NOTE: the workstation could not read its own process table, so job liveness is "
              "UNKNOWN this tick, not empty. An unreadable instrument is not a clean one.")
    elif tracked and len(alive) < len(tracked):
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
