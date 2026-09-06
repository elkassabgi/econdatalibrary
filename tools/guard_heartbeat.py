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


# A tracked process younger than this was (re)started within the last guard tick, which is a FACT
# worth printing and never a verdict. Measured by me over logs/_guard.log (2026-06-15..2026-09-06,
# tools/measure_relaunch_cadence.py) rather than copied: tick cadence median 315.0 s
# (n=2,850 gaps under 1 h), and the relaunch cadences the window has to tolerate are
# istat_sliced 5.3 min (n=1,642), cbs_nl 68.2 min (n=372) and gus_dbw 22.2 h (n=38).
# An earlier draft of this comment said gus_dbw was "every ~53 min" — I had taken that from a
# review instead of measuring it and it was wrong by a factor of ~25 (R804 #4). A number in a
# shipped comment needs its own instrument, same as a number in a report.
RELAUNCH_WINDOW_S = 330


# Interpreter flags that do NOT change which file python runs. Anything not on these two lists -
# including `-`, `-c`, `-m`, a combined `-mpdb`, a bundled `-c"..."`, or any long option - makes
# the command line unrecognised and `_script_of` refuses it. An ALLOW-list, because the deny-list
# is what kept leaking: two rounds of review found `python - X.py`, `python -c"..." X.py` and
# `python -mpdb X.py` each forging a match against a rule that named `-m` and `-c` (R802, R804).
_SAFE_FLAGS = frozenset(("-u", "-B", "-E", "-s", "-S", "-O", "-OO", "-q", "-I", "-b", "-d", "-v"))
_SAFE_FLAGS_WITH_ARG = frozenset(("-X", "-W"))


def _script_of(cmdline: str) -> "str | None":
    """The basename of the script a python command line is RUNNING, or None.

    FAIL CLOSED. The script is the first NON-FLAG token, every flag before it must be one this
    module recognises as harmless, and anything else refuses. Two earlier shapes both leaked:
    scanning for any .py token let `python audit_x.py --script ingest_x.py` forge a beat (R802),
    and then naming `-m`/`-c` as the only dangerous flags let `python - X.py`,
    `python -c"..." X.py` and `python -mpdb X.py` through (R804) - all three demonstrated on
    real spawned processes, and `python -` is this session's own tooling shape. The set of ways
    to make python run something other than its first path argument is not closed, so a
    matcher over that set cannot be either; the allow-list is.

    THE PRICE, STATED HONESTLY. The only form that launches a TRACKED job today is
    `RELAUNCH_GUARD.ps1:25-40`'s `$python jobs/ingest_<x>.py`, and it resolves. An earlier
    version of this comment claimed the long-job shim's `python -u ... <script>` as a second
    measured case; it is not one. `$longJobs` is `@()` and has never held a tracked job, and the
    argv actually preserved at `:101` is `-u -m core.derive_csv`, which this rule REFUSES — `-m`
    means python runs a module, not that path. That refusal is correct and irrelevant here,
    because core.derive_csv is not a tracked ingester. `-u`, `-X` and `-W` are on the list
    because they are harmless, not because anything currently uses them.

    A refused form is reported as "not running", which under-reports rather than mis-attributes.
    That is the safe direction for a beat, and it is NOT loud: the beat says `2/3` and "finished
    or dead" like any other absence. Nothing binds this list to RELAUNCH_GUARD.ps1's argv, so a
    launcher change is a silent coverage loss until someone re-reads both.

    Why refusing is the safe direction here: this feeds only the HEARTBEAT. The guard's own
    relaunch decision lives in RELAUNCH_GUARD.ps1 and reads its own process list, so a refusal
    here under-reports a beat and can never cause a duplicate crawler.

    Basenames, so `jobs/ingest_x.py` and `"E:\\...\\jobs\\ingest_x.py"` agree while
    `my_ingest_x.py` and `ingest_x.pyz` match nothing.
    """
    if not cmdline:
        return None
    try:
        parts = shlex.split(cmdline, posix=False)
    except ValueError:
        return None
    i = 1                                       # [0] is the interpreter
    while i < len(parts):
        tok = parts[i].strip("\"'")
        if tok.startswith("-"):
            if tok in _SAFE_FLAGS:
                i += 1
                continue
            if tok in _SAFE_FLAGS_WITH_ARG:
                i += 2
                continue
            return None                         # unknown flag: refuse, do not scan past it
        return (os.path.basename(tok.replace("\\", "/"))
                if tok.lower().endswith(".py") else None)
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
    of `jobs_alive` on a 60-second floor, which is wrong: measured over logs/_guard.log, cbs_nl
    is legitimately relaunched 372 times at a 68.2-minute median, and `publish` runs ~2 s after a
    tick — so roughly one beat in six would have called a perfectly healthy crawler dead, and the
    existing "not running" note would then have printed something false. One sample per tick
    cannot separate a restart from a crash loop (R54), so this publishes the fact — pid and age —
    and leaves the verdict to a reader who can look at the job's own log.

    THIS DOES NOT REDDEN CI, AND THAT IS A CHOICE I AM NOT DRESSING UP. Three reviews asked why
    the gate still cannot fail on the R799 condition. Failing on it needs a two-tick signal — the
    same job young on CONSECUTIVE beats, or a changed pid between them — and one sample cannot
    carry that; guessing from one sample is exactly the 60-second floor that had to be reverted.

    I previously wrote that this publisher "keeps no state across ticks", and that was false: it
    could read the PREVIOUS beat it just wrote, and `logs/_guard.log` records every relaunch with
    a timestamp (110 istat relaunches in 24 h, measured). Both signals are free and sitting
    beside this function. Implementing the comparison is real work with its own failure modes,
    and I chose not to bundle it into a change that had already failed review three times — but
    the reason is scope, not impossibility, and pretending otherwise put a wrong claim in a
    comment (R808 #6). Until it exists, this beat CARRIES the fact and asserts no health it
    cannot establish.

    Returns None, never [], when the process table could not be read: "unknown" and "nothing is
    running" are different, and a blind instrument must not read as a clean one.
    """
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
    except Exception:                                        # noqa: BLE001
        return None
    rows = _parse_process_table(ps.stdout, ps.returncode)
    return None if rows is None else _match_tracked(rows)


def _parse_process_table(stdout: bytes, returncode: int) -> "list | None":
    """Raw PowerShell bytes -> rows, or None when the sample is not trustworthy.

    SPLIT OUT SO A TEST CAN DRIVE THE REAL DECODE. The previous version's headline encoding test
    stubbed `subprocess.run`, so the decode it existed to pin never executed and the test passed
    against the broken code too (R804 finding 3). Here the bytes are the argument, so a test can
    hand it the real 0x81 that used to blind the instrument.

    ENCODING IS NOT OPTIONAL. `text=True` decoded PowerShell's stdout with the ANSI code page, so
    ONE unrelated process anywhere on the box with a path outside cp1252 raised
    UnicodeDecodeError, a bare except swallowed it, and the beat reported jobs_alive=[] while
    both crawlers were running - a BLIND instrument reading as a clean one. Demonstrated with a
    path containing U+0141 (UTF-8 C5 81; 0x81 is undefined in cp1252, and an o-umlaut probe
    proves nothing because cp1252 has it). This fleet crawls Dutch, Polish and Italian sources.

    None means UNKNOWN; [] means a successful query that found no python processes. Those are
    different, and collapsing them made a blind sample read as an empty machine.
    """
    if returncode != 0:
        return None
    try:
        data = json.loads(stdout.decode("utf-8", "replace") or "null")
    except Exception:                                        # noqa: BLE001
        return None
    if data is None:                  # a successful query with no rows prints nothing
        return []
    if isinstance(data, dict):        # ConvertTo-Json emits a bare object when there is one row
        return [data]
    return data if isinstance(data, list) else None


def _match_tracked(rows: list) -> "list[dict]":
    """Rows -> the tracked ingesters among them, with pid and age."""
    out = []
    for t in TRACKED:
        for p in rows:
            if not isinstance(p, dict):
                continue
            if _script_of(p.get("CommandLine") or "") == t:
                age = p.get("AgeS")
                out.append({"name": t, "pid": p.get("ProcessId"),
                            "age_s": age if isinstance(age, int) and age >= 0 else None})
                break
    return out


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

    def _age(d):
        a = d.get("age_s")
        return f"{a}s" if isinstance(a, int) else "age unknown"

    if detail:
        print("  jobs: " + ", ".join(f"{d.get('name')} (pid {d.get('pid')}, {_age(d)})"
                                     for d in detail if isinstance(d, dict)))
    if young:
        # ONE LINE, not a paragraph. istat_sliced is relaunched on a 5.3-minute median (n=1,642),
        # so this fires on essentially every run; a lecture repeated for ever is how a signal
        # stops being read, which is the failure that let a 44-day red streak go unexamined.
        # The ages above are the information; this only says which one to look at.
        print(f"  restarted within the last guard tick ({window}s): "
              + ", ".join(f"{d.get('name')} {_age(d)} ago" for d in young)
              + " — read that job's log; one sample cannot tell a relaunch from a crash loop.")

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
