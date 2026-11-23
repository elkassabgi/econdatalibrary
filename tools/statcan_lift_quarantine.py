"""Lift the statcan lane's quarantine on NAMED cubes - the 44 that the lane's old strike rule parked on 2026-11-13..18.

WHY (2026-11-22, ledger R2025/R2026; diagnosis F:/work/econ_finish/statcan_quarantine_20261122/REPORT.md, review
PASS WITH CONDITIONS). jobs/statcan_lane.py quarantines a cube after 3 deterministic strikes on one release, and only
a re-release lifts it. Before PR #317 the 3 strikes had no spacing: an edge HTTP 403 of about an hour on 2026-11-18
quarantined 35 cubes in 16 minutes (class A), and pre-release zips served for minutes quarantined 9 more through the
count gate (class B). Those 44 are a LANE fault. PR #317 spaces strikes (1 h, then 6 h; a quarantine needs 3 strikes
over at least 12 h), books 403 as transient and adds a circuit breaker - but the 44 stay quarantined until StatCan
re-releases them (a month for most, up to a year for the annual 36100400 / 36100658). This tool lifts exactly those
44, so the lane retries them under the NEW rule. Classes C (10100145, 18100212, 18100275) and D (36100487,
36100488) are NOT lifted: their refusals are not shown to be the lane's fault. Design and review:
F:/work/econ_finish/statcan_lift_pr/DESIGN.md, review_design/VERDICT.md.

WHAT IT CHANGES - per named cube, the lane's own re-release lift (run(), "RE-RELEASED") plus fail_release:
    pop quarantined; fails = 0; pop fail_release; pop first_strike_utc; pop retry_after
and NOTHING else: not merge_in_flight (the struck attempts published nothing - both strike paths are inside
statcan._fetch_cube_whole, before the merge - and a set flag only makes the next merge serve, the safe direction),
not last_error, not any other cube, not any top-level key. The document is written back byte-for-byte as
jobs/statcan_lane.save_state writes it (json.dumps(state, sort_keys=True)), through updater.blob - the lane's own
write path (local mirror, then one R2 PUT).

WHICH CUBES. Only those in LIFT (pinned below, each with the release it was quarantined on), named with --pids (a
subset) or read from the diagnosis table with --from-table2 (which must reproduce LIFT exactly). There is no way to
say "every quarantined cube".

WHEN. Only after the lane runs the NEW strike rule: every live lane process must have started after the last write of
this checkout's jobs/statcan_lane.py and updater/strategies/fetchers/statcan.py, and it must have finished an
iteration that enumerated. Under the old code a refusal on the first retry would quarantine the cube again in ~15
minutes.

    python tools/statcan_lift_quarantine.py --from-table2 <table2.tsv>                 # PLAN: read only, no lock
    python tools/statcan_lift_quarantine.py --from-table2 <table2.tsv> --apply \\
        --expect-before-sha <hex> --expect-after-sha <hex>                             # under the writer lock
    python tools/statcan_lift_quarantine.py --check logs/statcan_lift_quarantine/<stamp>_receipt.json

Run it with the interpreter that runs the lane, WITHOUT -I (pyarrow, boto3 and requests may live in the user site).
PLAN exits 0 when ready, 1 when not; APPLY 0 only when the write is read back and verified; CHECK 0 PASS, 3 PENDING,
1 FAIL; 2 is a usage error. Refused after T0, like the lane (core.cutover).
"""
from __future__ import annotations

import argparse
import copy
import csv
import datetime as dt
import hashlib
import json
import math
import os
import re
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import catalog_path, cutover                            # noqa: E402
from updater import blob, writer_lock                              # noqa: E402
from updater.strategies.fetchers import statcan as sc              # noqa: E402

import jobs.statcan_lane as lane                                    # noqa: E402

LOCK_NAME = lane.LOCK_NAME
WHAT = "tools/statcan_lift_quarantine.py (an --apply run)"
# THE LIVE CHECKOUT: --apply writes only there (design R3). Tests point it at a temporary folder.
LIVE_ROOT = catalog_path.LIVE_STORE_ROOT
RECEIPTS = os.path.join(ROOT, "logs", "statcan_lift_quarantine")
# The apply holds the lane's lock; the lane waits on it (it logs "another writer holds" once and sleeps 300 s). The
# whole read-check-backup part must end within this, or the write is not started (review F13).
LOCK_HOLD_MAX_S = 240.0
# The read-back after the write: a GET that raises is tried again before the receipt says READBACK_FAILED.
READBACK_TRIES, READBACK_WAIT_S = 3, 2.0
# The lane beats at the end of every 5-minute iteration: a beat older than this is not a loop that is alive (R6).
BEAT_MAX_AGE_MIN = 15.0
# --check: a cube still untouched this long after the lift is a FAIL, not PENDING (design section 6).
CHECK_DEADLINE_H = 24.0
LIFT_FIELDS = ("quarantined", "fails", "fail_release", "first_strike_utc", "retry_after")
TABLE2_CLASS = {"ACCESS_REFUSED": "A", "STALE_COPY": "B", "PERSISTENT_OFFSET": "C", "SERIES_DROPPED": "D"}

# THE 44 (diagnosis table2.tsv, classes ACCESS_REFUSED and STALE_COPY; re-measured on the 2026-11-22T09:06:11Z
# snapshot of the lane state: quarantined, 3 strikes, release == fail_release == the release below, merge_in_flight
# == release, no first_strike_utc / refused_http / history_hold, last_error of the class's signature).
_A_RELEASE = "2026-11-18T13:30"
_A = ("10100111 10100141 14100004 14100005 14100006 14100007 14100008 14100009 14100010 14100011 14100323 14100343 "
      "14100344 14100346 14100453 14100454 14100455 14100456 18100205 34100143 34100145 34100148 34100149 34100150 "
      "34100151 34100152 34100154 34100155 34100156 34100157 34100158 34100159 34100160 34100161 34100162").split()
_B = {"16100047": "2026-11-13T13:30", "18100001": "2026-11-16T13:30", "18100004": "2026-11-16T13:30",
      "18100006": "2026-11-16T13:30", "18100256": "2026-11-16T13:30", "20100003": "2026-11-13T13:30",
      "36100400": "2026-11-05T13:30", "36100434": "2026-10-30T12:30", "36100658": "2026-11-05T13:30"}
LIFT = {**{p: ("A", _A_RELEASE) for p in _A}, **{p: ("B", r) for p, r in _B.items()}}
# Classes C (PERSISTENT_OFFSET) and D (SERIES_DROPPED): NOT a lane fault shown - never lifted by this tool.
EXCLUDED = {"10100145": "C", "18100212": "C", "18100275": "C", "36100487": "D", "36100488": "D"}

# THE STRIKE THAT PRECEDED ANY WRITE (design C12). A: the old _download's refusal of THIS cube's zip, exactly. B: the
# count gate's refusal of THIS cube. Both are raised inside statcan._fetch_cube_whole, before merge_and_write_bounded.
_SIG_A = "whole-table fetch refused: statcan zip https://www150.statcan.gc.ca/n1/tbl/csv/{pid}-eng.zip HTTP 403"
_SIG_B = "whole-table fetch refused: statcan {pid}: incomplete against StatCan's own counts - "

# --check: a last_error that says the STORE failed (merge_cube's R753 branches, run()'s 'absent'): never a pass.
_STORE_FAULT = re.compile(r"the store does not have it|stored cube unreadable|the store could not be asked|R753")
# --check: a transient whose cause is THIS machine, not a StatCan answer: not a pass at the deadline (review F3 d).
_NOT_STATCAN = re.compile(r"merge failed on this machine|key check failed|^unexpected |history-floor gate could not")
_PY_NAME = re.compile(r"(?i)^(python[\w.-]*|pyw?(\.exe)?)$")
_ISO = "%Y-%m-%dT%H:%M:%SZ"


class Usage(Exception):
    """A command-line error (exit 2)."""


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _iso(t: dt.datetime) -> str:
    return t.strftime(_ISO)


def _parse_iso(s):
    try:
        return dt.datetime.strptime(str(s), _ISO).replace(tzinfo=dt.timezone.utc)
    except (TypeError, ValueError):
        return None


def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def after_bytes(st: dict) -> bytes:
    """THE ONE serialisation of a lane state, for plan and apply alike: exactly jobs/statcan_lane.save_state's."""
    return json.dumps(st, sort_keys=True).encode("utf-8")


# --------------------------------------------------------------------------- #
# selection
# --------------------------------------------------------------------------- #
def parse_pids(text: str) -> list:
    pids = [s.strip() for s in str(text).split(",") if s.strip()]
    if not pids:
        raise Usage("--pids names no cube")
    dup = sorted({p for p in pids if pids.count(p) > 1})
    if dup:
        raise Usage(f"--pids names a cube twice: {dup}")
    return pids


def read_table2(path: str) -> tuple:
    """(rows A+B {pid: (class, release)}, rows C+D {pid: class}) from the diagnosis table, by the tool's own read."""
    with open(path, encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh, delimiter="\t"))
    if not rows or not {"pid", "class2", "release"} <= set(rows[0]):
        raise ValueError(f"{path}: not the diagnosis table (needs the columns pid, class2, release)")
    ab, cd, seen = {}, {}, set()
    for r in rows:
        pid, k = str(r["pid"]).strip(), str(r["class2"]).strip()
        if pid in seen:
            raise ValueError(f"{path}: pid {pid} appears twice")
        seen.add(pid)
        if k not in TABLE2_CLASS:
            raise ValueError(f"{path}: pid {pid} has an unknown class {k!r}")
        cls = TABLE2_CLASS[k]
        if cls in ("A", "B"):
            ab[pid] = (cls, str(r["release"]).strip())
        else:
            cd[pid] = cls
    return ab, cd


def table2_problems(ab: dict, cd: dict) -> list:
    out = []
    if set(ab) != set(LIFT):
        out.append(f"table2 classes A+B differ from the pinned LIFT: only in the table {sorted(set(ab) - set(LIFT))}, "
                   f"only pinned {sorted(set(LIFT) - set(ab))}")
    for p in sorted(set(ab) & set(LIFT)):
        if ab[p] != LIFT[p]:
            out.append(f"table2 row {p} is {ab[p]}, pinned {LIFT[p]}")
    if cd != EXCLUDED:
        out.append(f"table2 classes C+D {dict(sorted(cd.items()))} differ from the pinned EXCLUDED "
                   f"{dict(sorted(EXCLUDED.items()))}")
    return out


def selection_problems(pids: list) -> list:
    out = []
    for p in pids:
        if p in EXCLUDED:
            out.append(f"C2 {p} is class {EXCLUDED[p]} - classes C and D are never lifted by this tool")
        elif p not in LIFT:
            out.append(f"C1 {p} is not one of the 44 pinned cubes (LIFT)")
    return out


# --------------------------------------------------------------------------- #
# the document and the per-cube checks
# --------------------------------------------------------------------------- #
def parse_state(raw) -> tuple:
    """(state, problems). R8: the document parses, holds a `cubes` dict, and is in the lane's own serialisation."""
    if raw is None:
        return None, ["R8 the lane state document is absent"]
    try:
        st = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as e:
        return None, [f"R8 the lane state does not parse: {type(e).__name__}: {e}"]
    if not isinstance(st, dict) or not isinstance(st.get("cubes"), dict):
        return None, ["R8 the lane state has no `cubes` dict"]
    if after_bytes(st) != raw:
        return st, ["R8 the lane state is not in the lane's own serialisation (json.dumps sort_keys round-trip "
                    "differs): something else wrote it, and a rewrite would change bytes outside the named cubes"]
    return st, []


def cube_problems(st: dict, pids: list) -> list:
    out = []
    for p in pids:
        if p not in LIFT:
            continue                                     # C1/C2 reported by selection_problems
        cls, rel = LIFT[p]
        c = st["cubes"].get(p)
        if not isinstance(c, dict):
            out.append(f"C3 {p} is not in the lane state")
            continue
        if c.get("quarantined") is not True:
            out.append(f"C4 {p} is not quarantined (quarantined={c.get('quarantined')!r})")
        if c.get("release") != rel or c.get("fail_release") != rel:
            out.append(f"C5 {p} release {c.get('release')!r} / fail_release {c.get('fail_release')!r} is not the "
                       f"diagnosed quarantine's release {rel!r} (re-released, or quarantined on another release)")
        if c.get("fails") != sc.QUARANTINE_AFTER:
            out.append(f"C6 {p} fails {c.get('fails')!r}, not {sc.QUARANTINE_AFTER}")
        if "first_strike_utc" in c:
            out.append(f"C7 {p} carries first_strike_utc {c['first_strike_utc']!r}: struck under the NEW rule; this "
                       f"tool lifts only the old rule's quarantines")
        if "refused_http" in c:
            out.append(f"C8 {p} carries refused_http {c['refused_http']!r} (a circuit-breaker mark)")
        if "history_hold" in c:
            out.append(f"C9 {p} carries a history_hold (a deliberate hold, not a strike)")
        mif = c.get("merge_in_flight")
        if mif is not None and mif != c.get("release"):
            out.append(f"C10 {p} merge_in_flight {mif!r} is not its release {c.get('release')!r}: an attempt of another "
                       f"release may have published")
        if not lane.merge_owed(c):
            out.append(f"C11 {p} owes no merge (merged {c.get('merged')!r} >= release {c.get('release')!r})")
        le = str(c.get("last_error") or "")
        ok = le == _SIG_A.format(pid=p) if cls == "A" else le.startswith(_SIG_B.format(pid=p))
        if not ok:
            out.append(f"C12 {p} (class {cls}) last_error is not its class's strike signature: {le[:160]!r}")
        if int(c.get("transient_fails") or 0) >= lane.TRANSIENT_RED_AFTER:
            out.append(f"C13 {p} transient_fails {c.get('transient_fails')} >= {lane.TRANSIENT_RED_AFTER}: lifted, it "
                       f"would turn the verdict red FAILING at once")
    return out


def doc_problems(st: dict) -> list:
    out = []
    if st.get("enum_fail"):
        out.append(f"R7 the lane state carries enum_fail ({st['enum_fail']}): StatCan's cube list is failing - lift later")
    if st.get("breaker"):
        out.append(f"R7 the lane state carries a circuit-breaker record ({st['breaker']}): StatCan refuses now - lift "
                   f"later")
    return out


def lift(st: dict, pids: list) -> tuple:
    """(after state, {pid: (before fields, after fields)}). The input is not changed."""
    after = copy.deepcopy(st)
    changes = {}
    for p in pids:
        c = after["cubes"][p]
        before = {k: c[k] for k in LIFT_FIELDS if k in c}
        c.pop("quarantined", None)
        c["fails"] = 0
        c.pop("fail_release", None)
        c.pop("first_strike_utc", None)
        c.pop("retry_after", None)
        changes[p] = (before, {k: c[k] for k in LIFT_FIELDS if k in c})
    return after, changes


def structural_diff(before: dict, after: dict) -> list:
    """Every (scope, key, field) that differs between two parsed lane states. A different instrument from the lift
    itself: it compares the two whole documents."""
    out = []
    for k in sorted(set(before) | set(after)):
        if k != "cubes" and before.get(k, "<absent>") != after.get(k, "<absent>"):
            out.append(("top", k, None))
    bc, ac = before.get("cubes") or {}, after.get("cubes") or {}
    for p in sorted(set(bc) | set(ac)):
        b, a = bc.get(p), ac.get(p)
        if not isinstance(b, dict) or not isinstance(a, dict):
            if b != a:
                out.append(("cube", p, "<whole>"))
            continue
        for f in sorted(set(b) | set(a)):
            if b.get(f, "<absent>") != a.get(f, "<absent>"):
                out.append(("cube", p, f))
    return out


def expected_diff(st: dict, pids: list) -> list:
    out = []
    for p in sorted(pids):
        c = st["cubes"][p]
        for f in LIFT_FIELDS:
            if f == "fails":
                if c.get("fails", "<absent>") != 0:
                    out.append(("cube", p, f))
            elif f in c:
                out.append(("cube", p, f))
    return sorted(out)


# --------------------------------------------------------------------------- #
# the lane: which code runs it, and did it finish an iteration on that code
# --------------------------------------------------------------------------- #
def _same(a, b) -> bool:
    return os.path.normcase(os.path.realpath(str(a))) == os.path.normcase(os.path.realpath(str(b)))


def live_problems() -> list:
    """R3. --apply writes only the live checkout's files. EXACT paths (review F6): a worktree nested inside the live
    checkout is 'under' it but has its own lock file, while its R2 key is still the production object."""
    store = os.path.join(LIVE_ROOT, "data", "clean_full", "statcan")
    want = (("the tool's code root", ROOT, LIVE_ROOT),
            ("the lane module", lane.__file__, os.path.join(LIVE_ROOT, "jobs", "statcan_lane.py")),
            ("the statcan store dir", sc.OUT_DIR, store),
            ("the lane state", lane.STATE, os.path.join(store, "_lane_state.json")),
            ("the lane progress", lane.PROGRESS, os.path.join(store, "_lane_progress.json")),
            ("the writer-lock dir", writer_lock.LOCK_DIR, os.path.join(LIVE_ROOT, "logs")),
            ("the receipts dir", RECEIPTS, os.path.join(LIVE_ROOT, "logs", "statcan_lift_quarantine")))
    return [f"R3 {name} is {got}, not {exp}" for name, got, exp in want if not _same(got, exp)]


def code_files() -> dict:
    """The two files that carry the strike rule, and their last write (this machine's clock)."""
    out = {}
    for f in (lane.__file__, sc.__file__):
        out[os.path.realpath(f)] = os.stat(f).st_mtime
    return out


def lane_processes() -> tuple:
    """([{pid, cmdline, cwd, create_time}], [unreadable]) - every python process with 'statcan_lane' anywhere in its
    argv (the guard's `python -u jobs/statcan_lane.py`, a `-m jobs.statcan_lane`, a test of it). Fails CLOSED: a
    python process whose argv, cwd or start time cannot be read is returned as unreadable (review F5)."""
    import psutil                                                    # noqa: PLC0415
    found, unreadable = [], []
    for p in psutil.process_iter():
        try:
            name = p.name()
        except psutil.NoSuchProcess:
            continue
        except Exception as e:                                       # noqa: BLE001
            unreadable.append(f"pid {p.pid}: name unreadable ({type(e).__name__})")
            continue
        # every interpreter name form is looked at - python, python3, python3.14t, pythonw, py, pyw, any case (V5)
        if not _PY_NAME.match(str(name)):
            continue
        try:
            cmd, cwd, ct = list(p.cmdline()), p.cwd(), float(p.create_time())
        except psutil.NoSuchProcess:
            continue
        except Exception as e:                                       # noqa: BLE001
            unreadable.append(f"pid {p.pid} ({name}): {type(e).__name__}: {str(e)[:80]}")
            continue
        if any("statcan_lane" in str(a).lower() for a in cmd):     # Windows paths are case-blind (review V5)
            found.append({"pid": p.pid, "cmdline": cmd, "cwd": cwd, "create_time": ct})
    return found, unreadable


def _script_of(proc: dict):
    """The file a lane process runs: the first non-option argv element (or -m's module), joined with its cwd."""
    cmd, cwd = list(proc.get("cmdline") or []), proc.get("cwd") or ""
    i = 1
    while i < len(cmd):
        a = str(cmd[i])
        if a == "-m" and i + 1 < len(cmd):
            return os.path.join(cwd, *str(cmd[i + 1]).split(".")) + ".py"
        if a in ("-c",):
            return None
        if a in ("-X", "-W", "-Q"):
            i += 2
            continue
        if a.startswith("-"):
            i += 1
            continue
        return a if os.path.isabs(a) else os.path.join(cwd, a)
    return None


def lane_problems(st: dict, progress, procs: list, unreadable: list, now: dt.datetime) -> list:
    """R5 (the new code runs) and R6 (it finished an iteration that enumerated). Both are needed: the old code also
    beats 'idle' with its pid, so only the process's start against the code's last write tells old from new."""
    out = [f"R5 a python process cannot be judged: {u}" for u in unreadable]
    files = code_files()
    newest = max(files.values())
    if not procs:
        out.append("R5 no live lane process (python ... statcan_lane): nothing proves which code will work the cubes")
    for pr in procs:
        script = _script_of(pr)
        if script is None or not _same(script, lane.__file__):
            out.append(f"R5 pid {pr['pid']} {pr['cmdline']} (cwd {pr['cwd']}) does not run this checkout's lane "
                       f"module {lane.__file__}")
        if not pr["create_time"] > newest:
            out.append(f"R5 pid {pr['pid']} started {_iso(dt.datetime.fromtimestamp(pr['create_time'], dt.timezone.utc))}"
                       f", before the last write of the strike-rule code "
                       f"({_iso(dt.datetime.fromtimestamp(newest, dt.timezone.utc))}): it may run the OLD rule")
    if not isinstance(progress, dict):
        out.append("R6 the lane progress record is absent or unreadable")
        return out
    by_pid = {pr["pid"]: pr for pr in procs}
    me = by_pid.get(progress.get("pid"))
    if me is None:
        out.append(f"R6 the progress record's pid {progress.get('pid')!r} is not a live lane process")
        return out
    start = dt.datetime.fromtimestamp(math.floor(me["create_time"]), dt.timezone.utc)
    beat = _parse_iso(progress.get("beat_utc"))
    if beat is None or beat < start:
        out.append(f"R6 no beat from pid {me['pid']} since it started ({_iso(start)}): beat_utc {progress.get('beat_utc')!r}")
    elif beat > now + dt.timedelta(minutes=1) or (now - beat).total_seconds() > BEAT_MAX_AGE_MIN * 60:
        out.append(f"R6 the last beat {progress.get('beat_utc')} is not within {BEAT_MAX_AGE_MIN:.0f} min of now "
                   f"({_iso(now)})")
    if progress.get("state") != "idle" or progress.get("current") is not None:
        out.append(f"R6 the lane is not idle between iterations: state {progress.get('state')!r}, current "
                   f"{progress.get('current')!r}")
    le = _parse_iso(st.get("last_enum_utc"))
    if le is None or le < start:
        out.append(f"R6 no enumeration since pid {me['pid']} started ({_iso(start)}): last_enum_utc "
                   f"{st.get('last_enum_utc')!r}")
    return out


# --------------------------------------------------------------------------- #
# reading
# --------------------------------------------------------------------------- #
def _read_file(path):
    with open(path, "rb") as fh:
        return fh.read()


def read_state(a):
    """The document of record: R2 through updater.blob (the backend is pinned to r2), or an offline copy."""
    return _read_file(a.state_file) if a.state_file else blob.read_bytes(lane.STATE)


def read_progress(a):
    """The lane's progress record: R2 through updater.blob, or an offline copy. With --state-file and no
    --progress-file it is NOT read (None - R6 then refuses): an offline plan never reaches the store."""
    if a.progress_file:
        raw = _read_file(a.progress_file)
    elif a.state_file:
        return None
    else:
        raw = blob.read_bytes(lane.PROGRESS)
    if raw is None:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return "unreadable"


def mirror_bytes():
    """The lane state's local mirror (write_bytes_atomic writes it before the R2 PUT); None when absent."""
    try:
        return _read_file(lane.STATE)
    except FileNotFoundError:
        return None


def _write_local(path: str, data: bytes) -> None:
    """The LOCAL half of updater.blob.write_bytes_atomic (tmp + os.replace, retried): used only to put the before
    bytes back on the mirror after an R2 PUT that failed (review F9)."""
    tmp = f"{path}.{os.getpid()}.lift.tmp"
    with open(tmp, "wb") as fh:
        fh.write(data)
    try:
        for attempt in range(6):
            try:
                os.replace(tmp, path)
                break
            except PermissionError:
                if attempt == 5:
                    raise
                time.sleep(0.2 * (2 ** attempt))
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


# --------------------------------------------------------------------------- #
# one evaluation, shared by plan and apply
# --------------------------------------------------------------------------- #
def evaluate(a, pids: list, now: dt.datetime) -> dict:
    ev = {"pids": pids, "problems": list(selection_problems(pids))}
    if a.from_table2:
        try:
            ab, cd = read_table2(a.from_table2)
            ev["table2"] = {"ab": len(ab), "cd": len(cd)}
            ev["problems"] += table2_problems(ab, cd)
        except (OSError, ValueError) as e:
            ev["problems"].append(f"table2 unreadable: {type(e).__name__}: {e}")
    raw = read_state(a)
    ev["raw"] = raw
    st, probs = parse_state(raw)
    ev["problems"] += probs
    ev["state"] = st
    if st is None:
        return ev
    ev["problems"] += doc_problems(st)
    ev["problems"] += cube_problems(st, pids)
    if not a.state_file:
        ev["mirror"] = mirror_bytes()
    progress = read_progress(a)
    ev["progress"] = progress
    procs, unreadable = lane_processes()
    ev["procs"], ev["unreadable"] = procs, unreadable
    ev["problems"] += lane_problems(st, progress, procs, unreadable, now)
    liftable = [p for p in pids if p in LIFT and isinstance(st["cubes"].get(p), dict)]
    after, changes = lift(st, liftable)
    ev["after_state"], ev["changes"] = after, changes
    ev["after"] = after_bytes(after)
    got, want = structural_diff(st, after), expected_diff(st, liftable)
    if sorted(got) != want:
        ev["problems"].append(f"the structural diff {got[:10]} is not the planned lift {want[:10]}")
    ev["diff_n"] = len(got)
    return ev


def _fmt(d: dict) -> str:
    return json.dumps(d, sort_keys=True)


def report(ev: dict, a, now: dt.datetime, mode: str) -> None:
    st = ev.get("state")
    print(f"[lift] {mode} at {_iso(now)}; backend {os.environ.get('AQUEDUCT_BACKEND')}; state "
          f"{a.state_file or ('R2 ' + blob._path_to_key(lane.STATE))}", flush=True)
    if "table2" in ev:
        print(f"[lift] table2 {a.from_table2}: classes A+B {ev['table2']['ab']} rows, C+D {ev['table2']['cd']} rows; "
              f"pinned LIFT {len(LIFT)}, EXCLUDED {len(EXCLUDED)}", flush=True)
    print(f"[lift] {len(ev['pids'])} cube(s) named: {' '.join(ev['pids'])}", flush=True)
    rec = writer_lock.owner(LOCK_NAME)
    print(f"[lift] R4 writer lock {writer_lock.lock_path(LOCK_NAME)}: "
          f"{'HELD by ' + _fmt(rec) if rec else 'free'}" + (" (plan takes no lock)" if mode == "PLAN" else ""),
          flush=True)
    for pr in ev.get("procs") or []:
        print(f"[lift] lane process pid {pr['pid']} started "
              f"{_iso(dt.datetime.fromtimestamp(pr['create_time'], dt.timezone.utc))} cwd {pr['cwd']} argv "
              f"{pr['cmdline']}", flush=True)
    for f, m in code_files().items():
        print(f"[lift] code {f} last written {_iso(dt.datetime.fromtimestamp(m, dt.timezone.utc))}", flush=True)
    pg = ev.get("progress")
    if isinstance(pg, dict):
        print(f"[lift] progress pid {pg.get('pid')} state {pg.get('state')!r} current {pg.get('current')!r} beat "
              f"{pg.get('beat_utc')}; state last_enum_utc {st.get('last_enum_utc') if st else None}", flush=True)
    raw = ev.get("raw")
    if raw is not None:
        print(f"[lift] BEFORE sha256 {sha256(raw)} ({len(raw):,} bytes)", flush=True)
    if "mirror" in ev:
        m = ev["mirror"]
        print(f"[lift] local mirror {lane.STATE}: "
              f"{'absent' if m is None else sha256(m) + (' (= R2)' if m == raw else ' (DIFFERS from R2)')}", flush=True)
    if st is not None and "changes" in ev:
        for p in ev["pids"]:
            if p not in ev["changes"]:
                continue
            c = st["cubes"][p]
            b, af = ev["changes"][p]
            print(f"  {p} {LIFT[p][0]} release {c.get('release')} merged {c.get('merged')} served {c.get('served')} "
                  f"mif {c.get('merge_in_flight')} transient_fails {c.get('transient_fails')} bytes {c.get('bytes')}: "
                  f"{_fmt(b)} -> {_fmt(af)}", flush=True)
        q0 = len(lane.summarise(st)["quarantined"])
        q1 = len(lane.summarise(ev["after_state"])["quarantined"])
        order = lane.order_owed(ev["after_state"], now)
        pos = {p: order.index(p) + 1 for p in ev["changes"] if p in order}
        print(f"[lift] AFTER sha256 {sha256(ev['after'])} ({len(ev['after']):,} bytes); structural diff "
              f"{ev['diff_n']} field(s); summarise() quarantined {q0} -> {q1}; order_owed() after the lift: "
              f"{len(pos)} of {len(ev['changes'])} named cube(s) due, positions {min(pos.values()) if pos else '-'}.."
              f"{max(pos.values()) if pos else '-'} of {len(order)}", flush=True)
    for pr in ev["problems"]:
        print(f"[lift] REFUSE {pr}", flush=True)


# --------------------------------------------------------------------------- #
# modes
# --------------------------------------------------------------------------- #
def plan(a, pids: list, now_fn=None) -> int:
    now = (now_fn or _now)()
    ev = evaluate(a, pids, now)
    problems = list(ev["problems"]) + live_problems()
    ev["problems"] = problems
    report(ev, a, now, "PLAN")
    if problems:
        print(f"[lift] NOT READY: {len(problems)} problem(s) above. Nothing was written.", flush=True)
        return 1
    print(f"[lift] READY: apply with --expect-before-sha {sha256(ev['raw'])} --expect-after-sha {sha256(ev['after'])}"
          f". Nothing was written.", flush=True)
    return 0


def _receipt(path: str, body: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(body, fh, indent=1, sort_keys=True)


def apply(a, pids: list, now_fn=None, clock=None) -> int:
    now_fn, clock = now_fn or _now, clock or time.monotonic
    if a.state_file or a.progress_file:
        print("[lift] REFUSE R10 --apply reads and writes the store of record; --state-file / --progress-file are for "
              "plan and check only", flush=True)
        return 1
    bad = live_problems()
    if bad:
        for b in bad:
            print(f"[lift] REFUSE {b}", flush=True)
        print("[lift] REFUSED: --apply writes only the live checkout's files. Nothing was written.", flush=True)
        return 1
    if not writer_lock.acquire(LOCK_NAME, what=WHAT):
        rec = writer_lock.owner(LOCK_NAME) or {}
        print(f"[lift] REFUSE R4 {writer_lock.lock_path(LOCK_NAME)} is held ({_fmt(rec)}): the lane is working or "
              f"another writer runs. Try again between two lane iterations. Nothing was written.", flush=True)
        return 1
    t0 = clock()
    try:
        now = now_fn()
        ev = evaluate(a, pids, now)
        report(ev, a, now, "APPLY")
        raw = ev.get("raw")
        problems = list(ev["problems"])
        if raw is not None and sha256(raw) != a.expect_before_sha:
            problems.append(f"R9 the document is {sha256(raw)}, not the planned {a.expect_before_sha}: it changed "
                            f"since the plan - run the plan again")
        if "after" in ev and sha256(ev["after"]) != a.expect_after_sha:
            problems.append(f"R9 the lift gives {sha256(ev['after'])}, not the planned {a.expect_after_sha}")
        if problems:
            for pr in problems[len(ev["problems"]):]:
                print(f"[lift] REFUSE {pr}", flush=True)
            print("[lift] REFUSED. Nothing was written.", flush=True)
            return 1
        after = ev["after"]
        stamp = _iso(now).replace(":", "").replace("-", "")
        backup = os.path.join(RECEIPTS, f"{stamp}_before_{sha256(raw)[:12]}.json")
        os.makedirs(RECEIPTS, exist_ok=True)
        try:
            with open(backup, "xb") as fh:                         # never over an earlier backup
                fh.write(raw)
        except FileExistsError:
            print(f"[lift] REFUSE the backup {backup} exists already (a second apply in the same second?). Nothing "
                  f"was written.", flush=True)
            return 1
        if sha256(_read_file(backup)) != sha256(raw):
            print(f"[lift] REFUSE the backup {backup} does not read back as written. Nothing else was written.",
                  flush=True)
            return 1
        print(f"[lift] backup {backup} ({len(raw):,} bytes, sha256 {sha256(raw)})", flush=True)
        spent = clock() - t0
        if spent > LOCK_HOLD_MAX_S:
            print(f"[lift] REFUSE the checks took {spent:.0f} s under the lock (limit {LOCK_HOLD_MAX_S:.0f} s); the "
                  f"write is not started. Nothing was written but the backup.", flush=True)
            return 1
        receipt_path = os.path.join(RECEIPTS, f"{stamp}_receipt.json")
        # (no os.path.relpath here: it raises when ROOT and this file are on two drives, as a test's ROOT can be)
        body = {"utc_lift": _iso(now), "tool": "tools/statcan_lift_quarantine.py",
                "tool_sha256": sha256(_read_file(__file__)), "r2_key": blob._path_to_key(lane.STATE),
                "before_sha256": sha256(raw), "after_sha256": sha256(after), "backup": backup,
                "lock": writer_lock.owner(LOCK_NAME), "procs": ev["procs"], "code_files": code_files(),
                "progress": ev["progress"],
                "cubes": {p: {"class": LIFT[p][0], "release": LIFT[p][1], "before": b, "after": af,
                              "after_transient_fails": int(ev["after_state"]["cubes"][p].get("transient_fails") or 0)}
                          for p, (b, af) in ev["changes"].items()}}
        # THE INTENT RECEIPT, BEFORE THE WRITE (verification review V3): every exit from here on leaves a receipt that
        # names the backup and both shas; the status below replaces WRITING on every path that returns.
        body["status"] = "WRITING"
        _receipt(receipt_path, body)
        try:
            blob.write_bytes_atomic(lane.STATE, after)            # the lane's own writer: mirror, then one R2 PUT
        except Exception as e:                                    # noqa: BLE001 - which document does R2 hold now?
            print(f"[lift] the write raised {type(e).__name__}: {str(e)[:200]}", flush=True)
            try:
                got = blob.read_bytes(lane.STATE)
            except Exception as e2:                               # noqa: BLE001
                body["status"] = "UNKNOWN"
                _receipt(receipt_path, body)
                print(f"[lift] UNKNOWN: R2 cannot be read back ({type(e2).__name__}: {str(e2)[:160]}). The document "
                      f"of record is either the before ({sha256(raw)}) or the after ({sha256(after)}); read it before "
                      f"anything else. Receipt {receipt_path}", flush=True)
                return 1
            if got == raw:
                # write_bytes_atomic replaced the mirror BEFORE its PUT: put the before bytes back, so no reader of
                # the mirror sees a lift that did not happen (review F9)
                try:
                    _write_local(lane.STATE, raw)
                except Exception as e3:                           # noqa: BLE001
                    print(f"[lift] the mirror could not be put back: {type(e3).__name__}: {str(e3)[:160]}", flush=True)
                ok = mirror_bytes() == raw
                body["status"] = "FAILED_NOTHING_CHANGED"
                _receipt(receipt_path, body)
                print(f"[lift] FAILED: R2 still holds the before document; the local mirror was put back "
                      f"({'verified' if ok else 'NOT VERIFIED'}). Nothing changed for the lane. Receipt {receipt_path}",
                      flush=True)
                return 1
            if got != after:
                body["status"] = "OTHER_DOCUMENT"
                _receipt(receipt_path, body)
                print(f"[lift] FAILED: R2 holds a THIRD document ({sha256(got) if got is not None else 'absent'}), "
                      f"neither the before nor the after. A person reads it first. Receipt {receipt_path}", flush=True)
                return 1
            print("[lift] R2 holds the after document although the write raised: verifying", flush=True)
        rb, why = None, None
        for attempt in range(READBACK_TRIES):
            try:
                rb = blob.read_bytes(lane.STATE)
                break
            except Exception as e:                                # noqa: BLE001
                why = f"{type(e).__name__}: {str(e)[:160]}"
                if attempt + 1 < READBACK_TRIES:
                    time.sleep(READBACK_WAIT_S)
        else:
            body["status"] = "READBACK_FAILED"
            body["readback_error"] = why
            _receipt(receipt_path, body)
            print(f"[lift] READ-BACK FAILED {READBACK_TRIES} times ({why}): the write returned, but what R2 holds is "
                  f"NOT VERIFIED - the before ({sha256(raw)}) or the after ({sha256(after)}). A person reads it before "
                  f"anything else. Receipt {receipt_path}", flush=True)
            return 1
        try:
            mir = mirror_bytes()
        except Exception as e:                                    # noqa: BLE001 - e.g. a PermissionError: not verified
            print(f"[lift] the mirror cannot be read back: {type(e).__name__}: {str(e)[:160]}", flush=True)
            mir = None
        checks = {"r2_read_back_is_after": rb == after, "mirror_is_after": mir == after}
        st_rb, probs = parse_state(rb)
        checks["read_back_parses"] = st_rb is not None and not probs
        if st_rb is not None:
            checks["diff_is_the_planned_lift"] = (sorted(structural_diff(ev["state"], st_rb))
                                                  == expected_diff(ev["state"], list(ev["changes"])))
            q0 = set(lane.summarise(ev["state"])["quarantined"])
            q1 = set(lane.summarise(st_rb)["quarantined"])
            checks["quarantined_is_before_minus_lifted"] = q1 == q0 - set(ev["changes"])
        ok = all(checks.values()) and len(checks) == 5
        body["status"] = "LIFTED" if ok else "WRITTEN_NOT_VERIFIED"
        body["checks"] = checks
        _receipt(receipt_path, body)
        for k, v in checks.items():
            print(f"[lift] check {k}: {'PASS' if v else 'FAIL'}", flush=True)
        print(f"[lift] {'LIFTED' if ok else 'WRITTEN BUT NOT VERIFIED'}: {len(ev['changes'])} cube(s); receipt "
              f"{receipt_path}. Next: --check {receipt_path} at about +2 h and at +{CHECK_DEADLINE_H:.0f} h.",
              flush=True)
        return 0 if ok else 1
    finally:
        writer_lock.release(LOCK_NAME)


def classify(c, lifted_rel: str, lifted: dt.datetime, now: dt.datetime, after_tf: int) -> tuple:
    """(class, kind) of one lifted cube now. kind: pass | wait (PENDING before the deadline, FAIL at it) | fail.
    Judged on a fixed list of outcome fields; the order matters (a store fault first, review F3)."""
    if not isinstance(c, dict):
        return "MISSING", "fail"
    le = str(c.get("last_error") or "")
    if c.get("not_held") or _STORE_FAULT.search(le):
        return "NOT_HELD", "fail"
    fs, ra = _parse_iso(c.get("first_strike_utc")), _parse_iso(c.get("retry_after"))
    span = dt.timedelta(hours=lane.QUARANTINE_MIN_SPAN_H)
    if c.get("quarantined"):
        # NEW RULE only when the quarantine carries a strike stamp from after the lift, and both the spacing the new
        # rule writes (retry_after >= first + 12 h) and the time since the first strike reach the 12 h span (F4)
        if fs is not None and fs >= lifted and ra is not None and ra - fs >= span and now - fs >= span:
            return "QUARANTINED_NEW_RULE", "pass"
        return "OLD_RULE", "fail"
    rel, merged = c.get("release"), c.get("merged")
    if merged and rel and merged >= rel and merged >= lifted_rel:
        return ("MERGED", "pass") if c.get("served") and c["served"] >= merged else ("MERGED_NOT_SERVED", "wait")
    if rel != lifted_rel:
        return "RE_RELEASED", "pass"
    tf, fails = int(c.get("transient_fails") or 0), int(c.get("fails") or 0)
    if fails >= 1:
        if fs is not None and fs >= lifted and ra is not None and ra - fs >= dt.timedelta(minutes=lane.STRIKE_RETRY_MIN[0]):
            return "HELD_STRIKE", "pass"
        return "OLD_RULE", "fail"
    # A TRANSIENT SINCE THE LIFT, judged on the count alone (verification review V2). Every lane writer of the fields
    # judged here, at 8025b45ed (grep retry_after / transient_fails / refused_http / not_held over jobs/statcan_lane.py):
    #   _backoff               transient_fails + 1, retry_after = now + 10 min .. 12 h        -> HELD_TRANSIENT
    #   _deterministic         fails + 1, first_strike_utc, retry_after (or a quarantine)     -> HELD_STRIKE / quarantined
    #   _trip_breaker          the pre-attempt snapshot back, then _backoff, refused_http     -> HELD_TRANSIENT
    #   _breaker_close         POPS retry_after of a cube waiting on the trip's status        -> HELD_TRANSIENT (no ra)
    #   merge success          fails 0, transient_fails 0, retry_after None, merged = release -> MERGED
    #   serve success/refusal  served = merged / serve_refusal + retry_after                  -> MERGED / MERGED_NOT_SERVED
    #   run() RE-RELEASED      new release; quarantined, fails, first_strike_utc, retry_after dropped -> RE_RELEASED
    #   run() 'absent'         merged = served = release, not_held                          -> NOT_HELD (first)
    # The lift pops retry_after and every later writer sets it ahead of `now`, so a retry_after at or before the lift
    # cannot occur; a breaker close leaves NONE. So the transient count decides, never the retry_after.
    if tf > after_tf:
        if tf >= lane.TRANSIENT_RED_AFTER or _NOT_STATCAN.search(le):
            return "HELD_TRANSIENT_RED", "wait"
        return "HELD_TRANSIENT", "pass"
    if fs is None and tf == after_tf and ra is None:
        return "UNTOUCHED", "wait"
    return "UNCLASSIFIED", "fail"


def check(a, now_fn=None) -> int:
    now = (now_fn or _now)()
    with open(a.check, encoding="utf-8") as fh:
        rec = json.load(fh)
    lifted = _parse_iso(rec.get("utc_lift"))
    if lifted is None or not isinstance(rec.get("cubes"), dict) or not rec["cubes"]:
        print(f"[lift] FAIL: {a.check} is not a lift receipt (no utc_lift or no cubes)", flush=True)
        return 1
    if rec.get("status") != "LIFTED":
        # ONLY A VERIFIED LIFT IS JUDGED (verification review V4): on a refused PUT nothing was lifted, and judging
        # those cubes would name them OLD_RULE re-quarantines; on an unverified write nobody knows what R2 holds.
        print(f"[lift] FAIL: this receipt records no verified lift: status {rec.get('status')!r}. "
              + ("Nothing was lifted - plan again." if rec.get("status") == "FAILED_NOTHING_CHANGED" else
                 "The write's outcome is not known: a person reads the lane state first (a plan prints every named "
                 "cube's fields; a cube without `quarantined` was lifted) before anything else is run."), flush=True)
        return 1
    try:
        raw = read_state(a)
    except Exception as e:                                       # noqa: BLE001
        print(f"[lift] FAIL: the lane state cannot be read: {type(e).__name__}: {str(e)[:200]}", flush=True)
        return 1
    st, probs = parse_state(raw)
    if st is None:
        print(f"[lift] FAIL: {probs}", flush=True)
        return 1
    hours = (now - lifted).total_seconds() / 3600.0
    late = hours >= CHECK_DEADLINE_H
    counts, rows = {}, []
    for p, r in sorted(rec["cubes"].items()):
        cls, kind = classify(st["cubes"].get(p), r["release"], lifted, now, int(r.get("after_transient_fails") or 0))
        counts[cls] = counts.get(cls, 0) + 1
        c = st["cubes"].get(p) or {}
        rows.append((p, cls, kind))
        print(f"  {p} {r.get('class')} {cls:<22} release {c.get('release')} merged {c.get('merged')} served "
              f"{c.get('served')} fails {c.get('fails')} first_strike {c.get('first_strike_utc')} retry_after "
              f"{c.get('retry_after')} transient {c.get('transient_fails')} last_error {str(c.get('last_error'))[:90]!r}",
              flush=True)
    causes = [k for k in ("breaker", "enum_fail") if st.get(k)]
    fails = [p for p, _c, k in rows if k == "fail" or (k == "wait" and late)]
    waits = [p for p, _c, k in rows if k == "wait" and not late]
    print(f"[lift] {_iso(now)}: {hours:.1f} h after the lift ({rec['utc_lift']}); "
          f"{', '.join(f'{k} {v}' for k, v in sorted(counts.items()))}"
          + (f"; the state carries {causes}: an UNTOUCHED cube waits on it" if causes else ""), flush=True)
    if fails:
        print(f"[lift] FAIL: {len(fails)} cube(s): {' '.join(fails)}", flush=True)
        return 1
    if waits:
        print(f"[lift] PENDING: {len(waits)} cube(s) not yet settled ({' '.join(waits)}); a FAIL from "
              f"{CHECK_DEADLINE_H:.0f} h after the lift", flush=True)
        return 3
    print(f"[lift] PASS: every lifted cube merged, is held under the new rule, or was re-released", flush=True)
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0], allow_abbrev=False)
    sel = ap.add_mutually_exclusive_group()
    sel.add_argument("--pids", help="comma-separated subset of the 44 pinned cubes")
    sel.add_argument("--from-table2", help="the diagnosis table2.tsv: classes A+B, which must equal the pinned 44")
    ap.add_argument("--apply", action="store_true", help="write (default: a read-only plan)")
    ap.add_argument("--expect-before-sha", help="--apply: sha256 of the document the plan read")
    ap.add_argument("--expect-after-sha", help="--apply: sha256 of the document the plan would write")
    ap.add_argument("--check", metavar="RECEIPT", help="read-only: judge each lifted cube against an apply receipt")
    ap.add_argument("--state-file", help="plan/check only: read this copy of _lane_state.json instead of the store")
    ap.add_argument("--progress-file", help="plan only: read this copy of _lane_progress.json instead of the store")
    try:
        a = ap.parse_args(argv)
    except SystemExit as e:
        return 2 if e.code else 0
    try:
        if a.check:
            if a.pids or a.from_table2 or a.apply:
                raise Usage("--check takes the cubes from its receipt: no --pids, --from-table2 or --apply")
        else:
            if not (a.pids or a.from_table2):
                raise Usage("name the cubes: --pids or --from-table2")
            if a.apply and not (a.expect_before_sha and a.expect_after_sha):
                raise Usage("--apply needs both --expect-before-sha and --expect-after-sha from a plan")
        if a.pids:
            pids = parse_pids(a.pids)
        elif a.from_table2:
            # THE TOOL'S OWN READ of the diagnosis table; evaluate() reads it again and refuses unless it equals LIFT
            try:
                pids = sorted(read_table2(a.from_table2)[0])
            except (OSError, ValueError):
                pids = []
        else:
            pids = []
    except Usage as e:
        print(f"[lift] usage: {e}", flush=True)
        return 2
    cutover.refuse_if_cut_over("statcan_lift_quarantine - the lane state is the R2 store's before T0, and its "
                               "post-T0 writer is the owner's T0 decision (jobs/statcan_lane.POST_T0_READY)")
    # THE STORE OF RECORD BEFORE T0 IS R2, as for the lane: set when nothing chose a backend, refused when something
    # chose another (jobs/statcan_lane.pin_backend). A plan and a check read the same store.
    lane.pin_backend()
    if a.check:
        return check(a)
    if a.apply:
        return apply(a, pids)
    return plan(a, pids)


if __name__ == "__main__":
    sys.exit(main())
