"""statcan LANE AFTER T0 - the self-hosted twin of jobs/statcan_lane.py (T0 build item B3, 2026-11-28).

WHY A SECOND MODULE. jobs/statcan_lane.py is statcan's only update path and it is BEFORE T0 ONLY: it merges into
and serves the R2 copy, so it refuses once econ is cut over (core.cutover; its POST_T0_READY = False). The owner's
coordinator ruled (T0 plan rev 6.2, decision (a)) that the post-T0 lane is a NEW module beside it, so the running lane
is never edited or restarted for this. Design: F:/work/econ_finish/b3_statcan_post_t0/DESIGN.md.

WHAT IT RUNS. The lane's OWN code, unchanged. This module loads a PRIVATE instance of jobs/statcan_lane.py by path
(module name _statcan_lane_for_post_t0; `jobs.statcan_lane` itself is never patched) and sets two names on that
instance only:
  * serve_owed_cube -> _serve_owed_under_writer_lock: one cube's serve runs inside the catalogue writer lock (below);
  * Progress        -> a subclass whose every beat carries "lane": MODULE_ID, so a post-T0 reader can tell this
                       lane's beat from the pre-T0 lane's last one (the file is shared; R1279 rule (b)).
tests/test_statcan_selfhost_lane.py pins that run() calls both names exactly once, and pins the lane's bytes.

BEFORE T0 IT REFUSES - the mirror of the old lane's refusal after T0. It stays RESIDENT, re-checks the flag every
iteration, writes nothing at all (no lock, no state, no beat, no blob store opened) and says so about once an hour;
--once (a hand trial) exits non-zero at once. Never a self-written .DONE (R1281): one unreadable flag would retire
it for good.

AFTER T0 (AQUEDUCT_BACKEND=selfhost, set here when unset; any other value refuses). Every store path primitive of
updater.blob is then LOCAL: the cube parquets and the lane's three sidecars (_lane_state.json, _lane_progress.json,
_catalogue_debt.json) are files under data/clean_full/statcan - the same files the old lane last wrote locally
before T0, so the cube state carries over. Served CSVs go to the self-hosted blob store (updater.blob.SelfhostBlob,
F:\\econ_live\\blobs). R2 and D1 are never asked for.

THE TWO LOCKS.
  * logs/statcan_writer.lock (updater/writer_lock.py): the statcan REGION, for each whole iteration - as the old lane.
  * the CATALOGUE writer lock (core.catalog_path.writer_lock, E:\\econ_live\\state\\writer.lock). After T0 every
    SelfhostBlob write needs it (updater/blob.py _refuse_or_own_store_write), and a write made without it TAKES it
    for the rest of the process - for a resident lane, for ever, refusing every nightly pass, swap and backup. So
    it is taken ONLY around one cube's serve (_serve_owed_under_writer_lock) and let go at once; a served CSV write
    outside that session is refused (make_put); a lock found already held by this process ends the process (the OS
    frees an OS lock at exit). When another process holds it - the updater holds it for its whole run, up to 2,880
    min after T0 (tools/run_local_heavy.ps1) - the cube's serve is NOT a failure: it waits for the next point of
    the ONE RETRY GRID (_grid_retry) and tries again, with no strike and no transient count, while merges go on; a
    wait older than statcan.LOCK_WAIT_RED_HOURS reads red. The lane also YIELDS (no new session while a desktop pass
    or a waiter's .want marker is there, and a gap between sessions); a lock that cannot be judged at all is
    BLOCKED, never a wait.
    Merges take no catalogue lock. THE REGION RULE: after T0 the statcan region (the cube parquets and the lane's
    three sidecars) has ONE writer, whoever holds logs/statcan_writer.lock; the blob index and the catalogue are
    written only inside the serve session, or never (tests/test_statcan_selfhost_lane.py pins the writer inventory).

BLOCKED. A lane that cannot work must not keep a healthy-looking beat, and must not crash-loop under the guard
(R1279). Before each iteration it checks the store lists at least one cube, the blob store opens, the split map
reads, its own records parse and no held or catalogued cube has lost its parquet; any failure publishes state
"blocked" with the reason (the reporter reads it red, updater/strategies/fetchers/statcan.py) and the lane stays
resident, without calling run() - so without asking StatCan anything. The one check that reads the CATALOGUE (a
not-held cube with catalogued ids - lost_cube_check) runs outside the writer lock, so it never stops the merges it
does not protect (code review round 2, G1): its answer is read once per booking and kept in the lane state, and a
read that fails is skipped - the lane merges on and reads red only when the check has failed for
statcan.LOST_CHECK_RED_HOURS. A failure met INSIDE run() (a catalogue writer lock that cannot be judged, the lane's
own refusal) is a HOLD in the lane state (`blocked_hold`): published on every beat (red), and run() - with its
enumeration - is not called again before the hold's next grid point (code review F2: a raise with no retry
enumerated every 5 minutes). A lock hold whose cause was the lock FILE (an OSError) is judged again when the lane
yields to a desktop pass or a waiter, by opening the file only (round 2, G2). A wrong backend or a checkout that is
not the live one writes NOTHING (its beat goes stale and reads red after 2-3 h): writing a beat there would write
the wrong store. Any other exception ends the process loudly, as in the old lane.
"""
from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import importlib.util
import json
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import catalog_path, cutover                           # noqa: E402
from updater import blob, config, writer_lock                    # noqa: E402
from updater.strategies.fetchers import statcan as sc            # noqa: E402

# Parsed (never imported) by tools/selfhost/t0_ready.py's statcan-lane check, through the registry's lane_post_t0.
POST_T0_READY = True
MODULE_ID = "jobs/statcan_selfhost_lane.py"
GUARD_JOB = "statcan_selfhost"                 # its entry in tools/machine/RELAUNCH_GUARD.ps1.post-t0-copy
BACKEND = "selfhost"
LANE_PATH = os.path.join(ROOT, "jobs", "statcan_lane.py")
PRIVATE_NAME = "_statcan_lane_for_post_t0"
# A cube whose serve found the catalogue writer lock held (or yielded it, below) is tried again at the next point of
# the ONE RETRY GRID, every LOCK_WAIT_RETRY_MIN (_grid_retry). Not a backoff (no transient count, no strike): the
# holder is another writer doing its job, and the merges go on meanwhile. NOT LESS THAN THE LANE'S IDLE ENUMERATION
# INTERVAL (jobs/statcan_lane.py ENUM_MIN_INTERVAL_MIN, 60): run() enumerates StatCan on EVERY non-idle iteration and a
# cube that is due again makes the iteration non-idle, so a 10-minute retry was 6 getAllCubesListLite calls an hour
# for a whole 48 h pass (design review C2, R2250; R2027). The same grid times the blocked hold (code review F2).
LOCK_WAIT_RETRY_MIN = 60.0
# THE YIELD RULE (design review C5). The other users of the catalogue writer lock fail at once (the updater) or wait
# with a bound (the swap 30 min, a backup); the lane draining a serve backlog in back-to-back sessions would leave them
# only millisecond gaps. So no NEW serve session starts while
#   * logs/local_heavy.lock names a LIVE desktop pass (it is written before the pass takes the writer lock), or
#   * a WANT marker - <the writer lock's path>.want, any content, written by a waiter (a backup, the swap, a tool) - is
#     younger than WANT_FRESH_MIN (a contract for B7 and tools/selfhost/swap.py, named in the PR), and
#   * two sessions are at least LOCK_GAP_S apart (the lane sleeps the rest of the gap before the next one).
LOCK_GAP_S = 60.0
WANT_FRESH_MIN = 30.0
# THE LOST-CUBE CHECK (code review F3; round 2, G1). A cube the state books NOT HELD with catalogued ids and no parquet
# is a lost store file and BLOCKS the lane. That check is the lane's one catalogue read OUTSIDE the writer lock, so it
# must not stop the merges it does not protect (they never read the catalogue). Measured before this change
# (review_code/VERDICT_round2.md, P7): a catalogue held EXCLUSIVE by another writer stalled the iteration 309 s and
# ended it BLOCKED with no merge, and a hot journal would have kept it so (only a lock holder rolls one back, and the
# lane's own lock session is inside run(), which was never reached). So: the answer is read ONCE per not-held booking
# and kept in the lane state (`lost_checked`); the read waits at most LOST_CHECK_BUSY_S for a busy catalogue; a read
# that fails is SKIPPED - the iteration merges on, the failure is kept in the state (`lost_check_fail`, on every beat)
# and the reporter reads it red after statcan.LOST_CHECK_RED_HOURS.
LOST_CHECK_BUSY_S = 15.0
LOCAL_HEAVY_LOCK = os.path.join(ROOT, "logs", "local_heavy.lock")
REFUSED_LOG_EVERY_S = 3600      # under the guard's 3 h stall check ($STALL_HOURS), which keys on the log's mtime
BLOCKED_LOG_EVERY_S = 3600


REFUSED_WHY = (f"{MODULE_ID} is statcan's lane AFTER T0 (the self-hosted store); before T0 jobs/statcan_lane.py "
               f"serves statcan from R2, so this one waits and writes nothing")
LEAK_WHY = ("REFUSING: the catalogue writer lock (core.catalog_path) is already held by this process outside a "
            "cube's serve - a write took it for the rest of the process, which would refuse every other writer "
            "for ever. Ending the process so the OS frees it.")

_ORIG: dict = {}


def _install(m) -> None:
    """The two names this lane changes, on the PRIVATE instance `m` only."""
    _ORIG["serve_owed_cube"] = m.serve_owed_cube
    _ORIG["Progress"] = base = m.Progress

    class MarkedProgress(base):
        """The lane's Progress, every beat marked with this module, and the catalogue-writer-lock wait: the count of
        cubes waiting and the OLDEST wait, over cubes that still owe a serve and are not quarantined NOW (a stamp left
        on a cube that left the serve set is not counted). The reporter reads the oldest wait red past
        statcan.LOCK_WAIT_RED_HOURS (design review C1: a lane starved of the lock kept merging and read green)."""

        def __init__(self, st):
            super().__init__(st)
            self.d["lane"] = MODULE_ID

        def beat(self, state=None, current="keep", **counters):
            waits = [c["writer_lock_wait_since"] for c in self.st.get("cubes", {}).values()
                     if isinstance(c, dict) and c.get("writer_lock_wait_since") and not c.get("quarantined")
                     and m.serve_owed(c)]
            self.d["writer_lock_waits"] = len(waits)
            if waits:
                self.d["writer_lock_wait"] = {"since": min(waits), "cubes": len(waits),
                                              "holder": _wait_logged.get("why")}
            else:
                self.d.pop("writer_lock_wait", None)
            # THE BLOCKED HOLD ON EVERY BEAT (code review F2): an idle or working beat between two blocked ones must
            # not read healthy, so the hold travels with the state, not with the beat's own state name
            hold = self.st.get("blocked_hold")
            if hold:
                self.d["blocked_hold"] = hold
            else:
                self.d.pop("blocked_hold", None)
            # ... and a lost-cube check that cannot read the catalogue (round 2, G1): the merges go on, the
            # reporter reads it red past statcan.LOST_CHECK_RED_HOURS, on every beat
            fail = self.st.get("lost_check_fail")
            if fail:
                self.d["lost_check_fail"] = fail
            else:
                self.d.pop("lost_check_fail", None)
            return super().beat(state, current, **counters)

    m.Progress = MarkedProgress
    m.serve_owed_cube = _serve_owed_under_writer_lock


def _load_lane():
    m = sys.modules.get(PRIVATE_NAME)
    if m is None:
        spec = importlib.util.spec_from_file_location(PRIVATE_NAME, LANE_PATH)
        m = importlib.util.module_from_spec(spec)
        sys.modules[PRIVATE_NAME] = m
        # NO .pyc FOR THIS LOAD (design review C12.2): the guard passes no -B, and "before T0 it writes nothing" should
        # not have a jobs/__pycache__ exception. (Modules imported later, after T0, are another matter.)
        prev, sys.dont_write_bytecode = sys.dont_write_bytecode, True
        try:
            spec.loader.exec_module(m)
            _install(m)
        except BaseException:
            sys.modules.pop(PRIVATE_NAME, None)
            raise
        finally:
            sys.dont_write_bytecode = prev
    return m


# --------------------------------------------------------------------------- #
# the catalogue writer lock, around one cube's serve
# --------------------------------------------------------------------------- #
_wait_logged: dict = {"at": None, "why": None}
_session: dict = {"released_mono": None}      # when this process last let the catalogue writer lock go
_run_flags: dict = {"lock_blocked": False}    # this run() found the catalogue writer lock unusable (reset per run)


def _grid_retry(now: dt.datetime) -> dt.datetime:
    """THE ONE RETRY GRID (code review F1): the first whole multiple of LOCK_WAIT_RETRY_MIN since the Unix epoch after
    `now` - the top of the next UTC hour. EVERY lock wait and the blocked hold retry there, whenever they were tried,
    so all waiting cubes come due in ONE iteration per grid point, and run() enumerates in that iteration only.
    Per-cube `now + 60 min` kept each cube's own minute (merges take wall time between serves), and every due
    iteration is non-idle and enumerates: measured 95 cube-list GETs in 48 h with 2 cubes, 564 with 12. A grid
    anchored at last_enum_utc is not enough either: every enumeration moves that anchor, so cubes tried before and
    after a merge longer than the step split into two phases again for the rest of the hold
    (tests/test_statcan_selfhost_lane.py, the C2 tests)."""
    step = int(LOCK_WAIT_RETRY_MIN * 60)
    nxt = (int(now.timestamp()) // step + 1) * step
    return dt.datetime.fromtimestamp(nxt, dt.timezone.utc)


def _set_hold(st: dict, why: str, kind: str, now: dt.datetime, oserror: bool = False) -> dict:
    """The lane-wide BLOCKED hold in the lane state (code review F2): kind "lock" (the catalogue writer lock cannot be
    judged) or "refusal" (the lane's own refusal inside run()). It keeps its first `since`, names the reason and its
    next try on the grid; iteration() calls no run() - so asks StatCan nothing - before that try. `oserror` (a lock
    hold only): the lock FILE could not be made or opened, which a yielding lane can judge again by opening the file
    (round 2, G2); any other cause (a refused hot-journal recovery) is judged only by taking the lock."""
    old = st.get("blocked_hold") if isinstance(st.get("blocked_hold"), dict) else {}
    st["blocked_hold"] = {"kind": kind, "since": old.get("since") or L._iso(now), "why": why[:500],
                          "retry_after": L._iso(_grid_retry(now))}
    if oserror:
        st["blocked_hold"]["oserror"] = True
    return st["blocked_hold"]


def _clear_lock_hold(st: dict, how: str = "the catalogue writer lock can be judged again") -> None:
    """The catalogue writer lock was judged (taken, or held by another process), or its file opens again while the
    lane yields (G2): a "lock" hold is over."""
    h = st.get("blocked_hold")
    if isinstance(h, dict) and h.get("kind") == "lock":
        st.pop("blocked_hold", None)
        print(f"[lane] {how} - the BLOCKED hold since {h.get('since')} is over", flush=True)


def _hold_text(h: dict) -> str:
    y = h.get("yielding") if isinstance(h.get("yielding"), dict) else None
    note = (f"; not judged again since {y.get('since')}: the lane yields ({str(y.get('to'))[:200]})" if y else "")
    return (f"{h.get('why')} - BLOCKED since {h.get('since')}, next try {h.get('retry_after')} "
            f"(until then the lane asks StatCan nothing){note}")


def _lock_wait(pid: str, c: dict, progress, now: dt.datetime, why: str) -> str:
    """The lock is held elsewhere, or yielded to a waiter: the cube is tried again at the next grid point
    (_grid_retry). retry_after keeps it out of order_owed meanwhile, so the iterations of a long hold stay idle and
    enumerate at most once per grid point (R2027, R2250, code review F1). No transient count, no last_error, no
    strike. The wait's start is kept on the cube (writer_lock_wait_since) until the cube is served: the reporter reads
    a wait older than LOCK_WAIT_RED_HOURS red."""
    first = c.get("writer_lock_wait_since")
    c["writer_lock_wait_since"] = first or L._iso(now)
    c["retry_after"] = L._iso(_grid_retry(now))
    mono = time.monotonic()
    if first is None or _wait_logged["at"] is None or mono - _wait_logged["at"] >= BLOCKED_LOG_EVERY_S:
        print(f"[lane] {pid}: serve WAITS for the catalogue writer lock ({why[:200]}); waiting since "
              f"{c['writer_lock_wait_since']}, next try {c['retry_after']} - merges go on", flush=True)
        _wait_logged["at"] = mono
    _wait_logged["why"] = why[:200]
    progress.beat("waiting_for_writer_lock", current=None)
    return "lock_wait"


def _pass_holder() -> "str | None":
    """A LIVE desktop pass named by logs/local_heavy.lock ("<pid>,<StartTime ticks UTC>", run_local_heavy.ps1), as a
    reason to yield; None when there is no lock file or its pid is gone or recycled. A file that cannot be judged is
    treated as live (yield: the cost is a wait, the other way is a refused pass)."""
    try:
        with open(LOCAL_HEAVY_LOCK, encoding="utf-8-sig") as fh:
            parts = fh.readline().strip().split(",")
        pid = int(parts[0])
        ticks = int(parts[1]) if len(parts) > 1 and parts[1].strip() else None
    except FileNotFoundError:
        return None
    except Exception as e:                                           # noqa: BLE001
        return f"logs/local_heavy.lock cannot be judged ({type(e).__name__}) - yielding as if a pass were running"
    try:
        st = writer_lock._start_time(pid)
    except Exception:                                                # noqa: BLE001
        return f"a desktop pass (pid {pid}, logs/local_heavy.lock) cannot be judged - yielding"
    if st is None:
        return None                                                  # the pass is gone: a stale lock
    if ticks is not None:
        # .NET ticks: 100 ns since 0001-01-01 UTC; 62,135,596,800 s from there to the Unix epoch
        if abs((st + 62_135_596_800) * 10_000_000 - ticks) > 2 * 10_000_000:
            return None                                              # a recycled pid, not the pass
    return f"a desktop pass is running (pid {pid}, logs/local_heavy.lock)"


def _want_marker() -> "str | None":
    """A fresh <writer lock>.want marker: someone (a backup, the swap, a tool) is waiting for the lock."""
    p = catalog_path.LOCK_PATH + ".want"
    try:
        age_min = (time.time() - os.stat(p).st_mtime) / 60.0
    except (FileNotFoundError, NotADirectoryError):
        return None
    except OSError as e:
        return f"{p} cannot be read ({type(e).__name__}) - yielding"
    return f"a waiter's marker {p} is {age_min:.0f} min old" if age_min < WANT_FRESH_MIN else None


def _yield_reason() -> "str | None":
    """THE YIELD RULE's two signals (design review C5): a live desktop pass, or a fresh waiter's marker."""
    return _pass_holder() or _want_marker()


def _lock_file_problem() -> "str | None":
    """Can the catalogue writer lock's FILE be made and opened - what core.catalog_path.writer_lock() does before it
    locks? No lock is taken and no byte is written ("a+b" creates the file only when it is missing). None = yes."""
    try:
        os.makedirs(os.path.dirname(catalog_path.LOCK_PATH), exist_ok=True)
        with open(catalog_path.LOCK_PATH, "a+b"):
            pass
    except OSError as e:
        return f"{type(e).__name__}: {str(e)[:300]}"
    return None


def _yield_wait(pid: str, c: dict, progress, now: dt.datetime, why: str) -> str:
    """The serve YIELDS (a live desktop pass, a waiter's marker), so the lock itself is not tried. A "lock" hold must
    not stand on a stale reason for the whole pass meanwhile (code review round 2, G2: one lock fault, then a 48 h
    pass, read BLOCKED for 48 h and merged once an hour). A hold whose cause was the lock FILE (an OSError) is judged
    again here by opening the file only - if it opens, the hold is over and run() goes on every iteration; any other
    hold (a refused hot-journal recovery, judged only by taking the lock) stands, and says that it was not judged
    again because the lane yields."""
    h = progress.st.get("blocked_hold")
    if isinstance(h, dict) and h.get("kind") == "lock":
        problem = _lock_file_problem() if h.get("oserror") else "not a lock-file fault"
        if problem is None:
            _clear_lock_hold(progress.st, "the catalogue writer lock's file opens again (the lane yields, so the "
                                          "lock itself was not tried)")
        else:
            old = h.get("yielding") if isinstance(h.get("yielding"), dict) else {}
            h["yielding"] = {"since": old.get("since") or L._iso(now), "to": why[:200]}
    return _lock_wait(pid, c, progress, now, f"yielding: {why}")


def _serve_owed_under_writer_lock(pid, c, debt, smap, put, progress, now):
    """SEAM 1: the lane's serve_owed_cube, inside core.catalog_path.writer_lock(), taken here and let go when the
    cube's serve returns (serve_cube joins its PUT queue before returning, so every PUT of the cube is inside).
    Only "held elsewhere" (CutoverRefused from the lock itself) is a wait; any other failure to take the lock is
    BLOCKED (design review C4): the lane-wide hold goes into the state (run() saves it after this returns), the rest
    of this run tries the lock no more, and iteration() publishes BLOCKED and holds run() until the next grid point
    (code review F2). run() ignores what this returns."""
    if catalog_path._held is not None:
        raise SystemExit(LEAK_WHY)
    if _run_flags["lock_blocked"]:
        return "lock_blocked"                                        # this run met it already: no second try
    why = _yield_reason()
    if why:
        return _yield_wait(pid, c, progress, now, why)
    last = _session["released_mono"]
    if last is not None and time.monotonic() - last < LOCK_GAP_S:
        time.sleep(LOCK_GAP_S - (time.monotonic() - last))          # leave other waiters a real gap
        # ... and the gap is when a waiter comes (code review F7): ask again before taking the lock
        why = _yield_reason()
        if why:
            return _yield_wait(pid, c, progress, now, why)
    stack = contextlib.ExitStack()
    try:
        stack.enter_context(catalog_path.writer_lock())
    except cutover.CutoverRefused as e:                              # held by another process: a wait
        stack.close()
        _clear_lock_hold(progress.st)                                # the lock could be judged
        return _lock_wait(pid, c, progress, now, f"held elsewhere: {e}")
    except Exception as e:                                           # noqa: BLE001 - cannot judge the lock: BLOCKED
        stack.close()
        _run_flags["lock_blocked"] = True
        h = _set_hold(progress.st, f"the catalogue writer lock {catalog_path.LOCK_PATH} could not be taken "
                                   f"({type(e).__name__}: {str(e)[:300]})", "lock", now,
                      oserror=isinstance(e, OSError))
        _say_blocked(_hold_text(h))
        return "lock_blocked"
    try:
        with stack:
            _clear_lock_hold(progress.st)
            c.pop("writer_lock_wait_since", None)
            return _ORIG["serve_owed_cube"](pid, c, debt, smap, put, progress, now)
    finally:
        _session["released_mono"] = time.monotonic()


def make_put(store):
    """put(key, csv_body) into the self-hosted blob store, ONLY inside a serve's writer-lock session. Outside it
    the write is refused: a SelfhostBlob write without the lock would take it for the life of this process."""
    from updater import derive                                       # noqa: PLC0415

    def put(key, body):
        if catalog_path._held is None:
            raise RuntimeError(f"REFUSED {key}: a served CSV is written only inside the lane's catalogue "
                               f"writer-lock session (jobs/statcan_selfhost_lane.py)")
        if not derive._put_with_retry(store, key, body):
            raise RuntimeError(f"PUT failed (refused at once, or {derive.PUT_TRIES} tries used up - see the line "
                               f"above)")
    return put


# --------------------------------------------------------------------------- #
# the checks before an iteration
# --------------------------------------------------------------------------- #
def pin_backend(environ=None, cfg=None) -> "str | None":
    """AQUEDUCT_BACKEND=selfhost: set when unset (the guard passes no environment); any other value is a reason to
    write nothing (r2 would route the store to the frozen R2 copy, local is not the store the origin serves)."""
    environ = os.environ if environ is None else environ
    cfg = config if cfg is None else cfg
    got = environ.get("AQUEDUCT_BACKEND")
    if got is None:
        environ["AQUEDUCT_BACKEND"] = BACKEND
        cfg.BACKEND = BACKEND
        return None
    if got.strip().lower() != BACKEND:
        return f"AQUEDUCT_BACKEND is {got!r}; {MODULE_ID} runs only with {BACKEND} after T0 - unset it or set {BACKEND}"
    cfg.BACKEND = BACKEND
    return None


def live_checkout_problem() -> "str | None":
    try:
        blob.refuse_unless_live_checkout(f"{MODULE_ID} (it serves statcan's CSVs)")
    except cutover.CutoverRefused as e:
        return str(e)
    return None


_STORE: dict = {}                # root -> the ONE SelfhostBlob of this process (a BlobStore keeps its connection open)


def _blob_store():
    """The self-hosted CSV store - after T0 blob.csv_store() returns exactly this class. Opened once per process
    and root: a BlobStore holds a write connection for its life, so one per 5-minute iteration would leak them.
    Its index is checked on EVERY call, so a store that disappears is seen at the next iteration."""
    root = blob.SELFHOST_BLOB_ROOT
    if not os.path.isfile(os.path.join(root, "index.db")):
        raise FileNotFoundError(f"no blob store at {root} (index.db is missing; it is never created here)")
    s = _STORE.get(root)
    if s is None:
        s = blob.SelfhostBlob(root)
        s.store                              # opens it now
        _STORE[root] = s
    return s


def store_problems():
    """(reason, put, names): reason None, a put and the store's file names when the lane can work. Writes nothing to
    the store or the state, and reads no catalogue (the lost-cube check is lost_cube_check, round 2 G1)."""
    try:
        store = _blob_store()
    except Exception as e:                                           # noqa: BLE001
        return (f"the self-hosted blob store {blob.SELFHOST_BLOB_ROOT} cannot be opened ({type(e).__name__}: {e})",
                None, None)
    try:
        cubes = [f for f in blob.list_parquets(sc.OUT_DIR) if not os.path.basename(f).startswith("_")]
    except Exception as e:                                           # noqa: BLE001
        return f"the store {sc.OUT_DIR} cannot be listed ({type(e).__name__}: {e})", None, None
    if not cubes:
        # BEFORE run(), never after: run() books every released cube it cannot find as held-by-nobody and saves
        # that before its own store-absent guard raises (jobs/statcan_lane.py, the R753 block)
        return f"the store lists 0 cubes under {sc.OUT_DIR} - unreachable, not a quiet publisher (R753)", None, None
    try:
        with open(L.SPLIT_MAP, encoding="utf-8") as fh:
            smap = json.load(fh)
    except (OSError, ValueError) as e:
        return (f"the split map {L.SPLIT_MAP} is unreadable ({e!r}); serving without it would re-key every split "
                f"cube", None, None)
    if not isinstance(smap, dict) or not smap:
        return f"the split map {L.SPLIT_MAP} holds no split decisions", None, None
    # THE LANE'S OWN RECORDS PARSE (design review C6b): load_state() and the debt read raise on a torn file - inside
    # run() that is a crash per guard tick; here it is BLOCKED. Neither is ever reset (a reset would re-owe nothing).
    try:
        st = L.load_state()
    except Exception as e:                                           # noqa: BLE001
        return f"the lane state {L.STATE} is unreadable ({type(e).__name__}: {str(e)[:200]})", None, None
    try:
        L._read_json(L.DEBT, {"cubes": {}, "new_cubes": {}})
    except Exception as e:                                           # noqa: BLE001
        return f"the catalogue debt {L.DEBT} is unreadable ({type(e).__name__}: {str(e)[:200]})", None, None
    # EVERY CUBE THE STATE CALLS HELD HAS ITS PARQUET (design review C6a). After T0 this store is the only copy, and
    # run() books a cube it cannot find as held-by-nobody and settles it; one lost file would go silent for ever.
    names = {os.path.basename(f) for f in cubes}
    lost = held_without_parquet(st, names)
    if lost:
        return (f"{len(lost)} cube(s) the lane state records as held have no parquet in {sc.OUT_DIR} "
                f"(first: {', '.join(lost[:5])}) - restore them from a backup; the lane cannot re-pull them",
                None, None)
    return None, make_put(store), names


def held_without_parquet(st: dict, names: set) -> list:
    """The cubes the state records as HELD (merged or served, not not_held) that have no `<pid>.parquet` among `names`
    - each one BLOCKS the lane (design review C6a). tools/selfhost/statcan_lost_cubes.py counts them too (G3)."""
    cubes = st.get("cubes") if isinstance(st.get("cubes"), dict) else {}
    return sorted(str(p) for p, c in cubes.items()
                  if isinstance(c, dict) and (c.get("merged") or c.get("served")) and not c.get("not_held")
                  and f"{p}.parquet" not in names)


def not_held_without_parquet(st: dict, names: set) -> dict:
    """pid -> booked release, for every cube the state books not_held that has no `<pid>.parquet` among `names` -
    the candidates of the lost-cube check. tools/selfhost/statcan_lost_cubes.py (the S13-prep census on copies, G3)
    applies the same rule; tests/test_statcan_selfhost_lane.py pins the two against each other."""
    cubes = st.get("cubes") if isinstance(st.get("cubes"), dict) else {}
    return {str(p): c.get("release") for p, c in cubes.items()
            if isinstance(c, dict) and c.get("not_held") and f"{p}.parquet" not in names}


_lost_logged: dict = {"at": None}


def lost_cube_check(st: dict, names: set, now: dt.datetime) -> "tuple[str | None, bool]":
    """(reason to BLOCK or None, whether `st` changed). NO "NOT HELD" CUBE IS A LOST ONE (code review F3): run()
    books a released cube it cannot find as not held and settles it; a CATALOGUED cube is never new, so a not-held
    cube with catalogued ids and no parquet is a lost store file whose release was booked settled (by run(), one
    iteration before this check can see it when the state had never seen the cube) - users keep its old CSVs and
    nothing else says so. It BLOCKS the lane (a lost file is for a human to restore).

    THE READ MUST NOT STOP THE MERGES (round 2, G1; LOST_CHECK_BUSY_S above):
      * each booking is read ONCE: `lost_checked` {pid: {release, catalogued}} keeps the answer for the release the
        cube was booked at; a new not-held booking (StatCan released it again) is read again, and an entry whose cube
        has its parquet again (or is held again) is dropped. An answer "catalogued" is read again on every iteration
        - the lane is BLOCKED then anyway - so ids removed from the catalogue end the block; if that read fails, the
        kept answer stands;
      * a read that fails (busy past LOST_CHECK_BUSY_S, a hot journal, a refusal) is SKIPPED: `lost_check_fail`
        {since, count, pending, last_error} goes into the state, the merges go on, and the reporter reads it red past
        statcan.LOST_CHECK_RED_HOURS; the next read that works removes it."""
    cand = not_held_without_parquet(st, names)
    old = st.get("lost_checked") if isinstance(st.get("lost_checked"), dict) else {}
    known = {p: dict(a) for p, a in old.items()
             if p in cand and isinstance(a, dict) and a.get("release") == cand[p]}
    todo = sorted(p for p in cand if p not in known or known[p].get("catalogued"))
    failed = None
    if todo:
        try:
            hits = _catalogued(todo)
        except Exception as e:                                       # noqa: BLE001 - skipped, never a block
            failed = f"{type(e).__name__}: {str(e)[:200]}"
        else:
            for p in todo:
                known[p] = {"release": cand[p], "catalogued": p in hits}
    changed = False
    if known != old:
        if known:
            st["lost_checked"] = known
        else:
            st.pop("lost_checked", None)
        changed = True
    prev = st.get("lost_check_fail") if isinstance(st.get("lost_check_fail"), dict) else None
    if failed is not None:
        pending = sum(1 for p in todo if p not in known)
        st["lost_check_fail"] = {"since": (prev or {}).get("since") or L._iso(now),
                                 "count": int((prev or {}).get("count") or 0) + 1, "pending": pending,
                                 "last_error": failed}
        changed = True
        mono = time.monotonic()
        if prev is None or _lost_logged["at"] is None or mono - _lost_logged["at"] >= BLOCKED_LOG_EVERY_S:
            print(f"[lane] {L._iso(L._now())} the lost-cube check cannot read the catalogue {L.CATALOGUE} "
                  f"({failed}) - SKIPPED, the merges go on; {pending} not-held booking(s) unchecked since "
                  f"{st['lost_check_fail']['since']}", flush=True)
            _lost_logged["at"] = mono
    elif prev is not None:
        st.pop("lost_check_fail", None)
        changed = True
        _lost_logged["at"] = None
        print(f"[lane] {L._iso(L._now())} the lost-cube check reads the catalogue again (it failed since "
              f"{prev.get('since')})", flush=True)
    gone = sorted(p for p, a in known.items() if a.get("catalogued"))
    if gone:
        return (f"{len(gone)} cube(s) the lane state books as NOT HELD have catalogued ids and no parquet in "
                f"{sc.OUT_DIR} (first: {', '.join(gone[:5])}) - lost store files, not new cubes: restore each "
                f"parquet from a backup; its booked release stays unmerged until StatCan releases it again"), changed
    return None, changed


def _catalogued(pids) -> set:
    """The pids among `pids` with at least one catalogued id - one primary-key range read each (jobs/statcan_lane.py
    catalogued_ids' range) over ONE read-only connection that waits at most LOST_CHECK_BUSY_S for a busy catalogue."""
    con = catalog_path.connect_path(L.CATALOGUE, write=False, timeout=LOST_CHECK_BUSY_S)
    try:
        out = set()
        for p in pids:
            lo = L._tool().unit_id(str(p))
            if con.execute("SELECT 1 FROM series WHERE series_id >= ? AND series_id < ? LIMIT 1",
                           (lo, lo + "$")).fetchone():
                out.add(p)
        return out
    finally:
        con.close()


_blocked_logged: dict = {"why": None, "at": None}


def _say_blocked(why: str) -> None:
    mono = time.monotonic()
    if why != _blocked_logged["why"] or _blocked_logged["at"] is None or mono - _blocked_logged["at"] >= BLOCKED_LOG_EVERY_S:
        print(f"[lane] {L._iso(L._now())} BLOCKED: {why} - nothing merged or served until it clears", flush=True)
        _blocked_logged.update(why=why, at=mono)


def publish_blocked(why: str) -> None:
    """State "blocked" with its reason, in the lane's own beat (the reporter reads it red)."""
    _say_blocked(why)
    try:
        st = L.load_state()
    except Exception as e:                                           # noqa: BLE001
        why = f"{why}; and the lane state is unreadable ({type(e).__name__}: {e})"
        body = json.dumps({"pid": os.getpid(), "state": "blocked", "blocked": why[:500], "current": None,
                           "beat_utc": L._iso(L._now()), "lane": MODULE_ID}, sort_keys=True).encode("utf-8")
        blob.write_bytes_atomic(L.PROGRESS, body)
        os.makedirs(os.path.dirname(L.LOCAL_PROGRESS), exist_ok=True)
        with open(L.LOCAL_PROGRESS + ".tmp", "wb") as fh:
            fh.write(body)
        os.replace(L.LOCAL_PROGRESS + ".tmp", L.LOCAL_PROGRESS)
        return
    p = L.Progress(st)
    p.d["blocked"] = why[:500]
    p.beat("blocked", current=None)


def iteration(max_cubes=None, run_kw=None) -> int:
    """One iteration, with the region lock held by the caller. 0 = worked (or idle), 2 = blocked."""
    if catalog_path._held is not None:
        raise SystemExit(LEAK_WHY)
    why, put, names = store_problems()
    if why:
        publish_blocked(why)
        return 2
    now = ((run_kw or {}).get("now_fn") or L._now)()
    st = L.load_state()                                         # store_problems() proved the state parses
    # THE LOST-CUBE CHECK (code review F3; round 2, G1): a lost cube BLOCKS; a catalogue it cannot read does not
    why, changed = lost_cube_check(st, names, now)
    if changed:
        L.save_state(st)                                        # this process holds the region lock (main)
    if why:
        publish_blocked(why)
        return 2
    hold = st.get("blocked_hold")
    if isinstance(hold, dict):
        ra = L._parse_iso(hold.get("retry_after"))
        if ra is not None and ra > now:
            # HELD (code review F2): no run() before the hold's grid point - so no enumeration, no StatCan call
            publish_blocked(_hold_text(hold))
            return 2
    _run_flags["lock_blocked"] = False
    try:
        r = L.run(max_cubes=max_cubes, put=put, **(run_kw or {}))
    except SystemExit as e:
        if str(e) == LEAK_WHY:
            raise                                       # a lock leaked into this process: end it, the OS frees it
        # the lane's own refusals inside run() (the split map, the store-absent guard): resident, red, held to the
        # grid (not a loop, and not an enumeration every 5 minutes)
        why = f"the lane refused inside its run: {e}"
        try:
            st = L.load_state()
            why = _hold_text(_set_hold(st, why, "refusal", now))
            L.save_state(st)
        except Exception:                               # noqa: BLE001 - an unreadable state: the next preflight
            pass                                        # sees it, before run()
        publish_blocked(why)
        return 2
    # AFTER A RUN THAT RETURNED: a "lock" hold stands while a cube still owes a serve (a run that tried no serve
    # moves it to the next grid point); a "refusal" hold, or one with nothing left to serve, is over.
    st = L.load_state()
    hold = st.get("blocked_hold")
    if isinstance(hold, dict):
        owed = any(isinstance(c, dict) and L.serve_owed(c) and not c.get("quarantined")
                   for c in st.get("cubes", {}).values())
        if hold.get("kind") == "lock" and owed:
            if not _run_flags["lock_blocked"]:
                hold["retry_after"] = L._iso(_grid_retry(now))
                L.save_state(st)
            publish_blocked(_hold_text(hold))
            return 2
        st.pop("blocked_hold", None)
        L.save_state(st)
        print(f"[lane] {L._iso(L._now())} the BLOCKED hold since {hold.get('since')} is over "
              f"({'the run returned' if hold.get('kind') != 'lock' else 'no cube owes a serve'})", flush=True)
    _blocked_logged.update(why=None, at=None)
    if r.get("worked") or r.get("enumerated"):
        print(f"[lane] {L._iso(L._now())} iteration: {r}", flush=True)
    return 0


# The private lane instance, loaded once every function above exists (seam 1 is one of them).
L = _load_lane()


# --------------------------------------------------------------------------- #
# the process
# --------------------------------------------------------------------------- #
def main(argv=None, *, sleep=None, monotonic=None, run_kw=None) -> int:
    """`sleep`, `monotonic` and `run_kw` are for tests; the guard passes none."""
    sleep = sleep or time.sleep
    monotonic = monotonic or time.monotonic
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--max-cubes", type=int, default=None,
                    help="stop after this many cubes (a trial run; the rest stay owed)")
    ap.add_argument("--once", action="store_true", help="one iteration, then exit (a trial run)")
    a = ap.parse_args(argv)
    refused_logged_at = None
    held_by_other_logged = False
    while True:
        if not cutover.is_cut_over():
            # BEFORE T0: nothing below runs - no pin, no lock, no file, no blob store
            if a.once:
                raise SystemExit(f"REFUSING before T0: {REFUSED_WHY} ({cutover.FLAG_PATH} does not exist)")
            if refused_logged_at is None or monotonic() - refused_logged_at >= REFUSED_LOG_EVERY_S:
                print(f"[lane] {L._iso(L._now())} REFUSING before T0: {REFUSED_WHY}. Waiting - nothing is written; "
                      f"it starts by itself once {cutover.FLAG_PATH} exists", flush=True)
                refused_logged_at = monotonic()
            sleep(L.IDLE_SLEEP_S)
            continue
        if refused_logged_at is not None:
            print(f"[lane] {L._iso(L._now())} the cutover flag is present - starting", flush=True)
            refused_logged_at = None
        why = pin_backend() or live_checkout_problem()
        if why:
            _say_blocked(why)              # writes NOTHING: a beat here would land in the wrong store
            if a.once:
                return 2
            sleep(L.IDLE_SLEEP_S)
            continue
        rc = 0
        if writer_lock.acquire(L.LOCK_NAME, what=MODULE_ID):
            held_by_other_logged = False
            try:
                rc = iteration(max_cubes=a.max_cubes, run_kw=run_kw)
            finally:
                writer_lock.release(L.LOCK_NAME)
        elif not held_by_other_logged:
            rec = writer_lock.owner(L.LOCK_NAME) or {}
            print(f"[lane] {L._iso(L._now())} another writer holds {writer_lock.lock_path(L.LOCK_NAME)} "
                  f"(pid {rec.get('pid')}, {rec.get('what')}); waiting", flush=True)
            held_by_other_logged = True
        if a.once:
            return rc
        sleep(L.IDLE_SLEEP_S)


if __name__ == "__main__":
    # the guard launch sets no PYTHONIOENCODING (R126/R234): a title with a non-ANSI character must not end the lane
    for _stream in (sys.stdout, sys.stderr):
        try:
            _stream.reconfigure(errors="backslashreplace")
        except Exception:                                            # noqa: BLE001
            pass
    sys.exit(main())
