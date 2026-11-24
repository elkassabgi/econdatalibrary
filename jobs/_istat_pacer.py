"""One pacing rule for every ISTAT request from this workstation (shared, cross-process).

THE LIMIT. ISTAT, "SDMX web services", https://www.istat.it/en/classifications-and-tools/sdmx-web-services/
(read 2026-11-24): "a limit of 5 queries per minute has been set for each IP", and a block of 1 to 2 days
when it is exceeded. Same text in Italian: https://www.istat.it/classificazioni-e-strumenti/web-services-sdmx/
("un limite per singolo IP di 5 query al minuto"). Secondary: ondata, https://ondata.github.io/guida-api-istat/
(read 2026-11-24), the source the repo cited before. Neither page says whether the window is fixed or sliding,
whether a structure query, a redirect hop, a timed-out request or a bare TCP connect counts, or whether the
hosts share one counter. So this module takes the conservative reading of every unknown and never probes ISTAT
to find the real limit.

THE RULE.
  * ONE budget per machine for every host equal to `istat.it` or ending in `.istat.it` (esploradati, sdmx,
    www, avvisi). Every HTTP request actually sent and every bare TCP connect takes one token.
  * GAP_S = 60 / 4 + 1 = 16 s between the starts of any two tokens, from any process. 16 x 4 = 64 > 60, so no
    closed 60-s window holds more than 4 starts. ISTAT's limit is 5.
  * The shared state is one never-deleted file, `<STATE_DIR>\\pacer.oslock`, with an OS byte-range lock on
    bytes [0, 256) (the R2052/R2057 pattern of F:/econ_selfhost_probe/inst/ledger_add.py v2). The slot record
    sits inside the locked region at offset 16. The lock is held only for the read-modify-write of the record
    (milliseconds), never across a sleep or a request. No age rule, no delete: the OS drops a dead holder's
    lock; a waiter gives up after LOCK_WAIT_MAX_S and goes fail-safe.
  * FAIL-SAFE (budget / N_MAX): on any pacer failure a process paces ALONE at one token per
    FAILSAFE_GAP_S = N_MAX x GAP_S = 64 s. The earliest start after a failure is now + 64 s, never now (verdict
    V1); at that time the process tries the shared lock once more and uses it if it works. A missing, empty,
    torn or unknown-version record means "a request was just sent" (next = now + 64 s). The bound in mixed
    mode: the shared stream (<= 4 per closed 60 s) plus k processes in PERSISTENT fail-safe is <= 4 + k per
    closed 60 s; with every participant in fail-safe and at most N_MAX = 4 of them it is <= 4.
  * A token is valid only in [slot, slot + SLIP_MAX_S] (V2). A process that wakes later does not send: it
    abandons the token (an `X` line) and takes a new one. The request log carries the ACTUAL start.
  * The updater has priority during its turn: `begin_turn()` writes a self-expiring marker; a low-priority
    participant (crawler, tools) never reserves the future, takes a token only "now" when now >= next and
    now >= pen, and while the marker is live at most one per LOW_TURN_GAP_S = 61 s.
  * A 429 calls `penalize()`: pen = max(pen, now + clamp(seconds, 60 s, 48 h)), for every participant.
  * WSL on this workstation sends nothing (V4): it shares the workstation's IP and cannot share the Windows
    lock. Any other platform without `msvcrt` (Linux CI: another IP) runs the fail-safe always.

THE RECEIPT. `<STATE_DIR>\\requests_YYYYMM.log`, append-only, written under the pacer lock:
  R <utc> <slot epoch> pid=<n> part=<participant> host=<host> kind=<http|tcp> class=<path class> flow=<id|->
    mode=<shared|failsafe> waited=<s> tok=<pid>-<seq>                          (reservation)
  O <utc> tok=<id> outcome=<text> el=<s> start=<actual start epoch> slip=<s>  (outcome; start = ACTUAL send)
  X <utc> tok=<id> reason=late slip=<s>                                       (abandoned, never sent)
  P <utc> <epoch> pid=<n> part=<participant> seconds=<s> until=<epoch>        (429 penalty)
  T <utc> <epoch> pid=<n> turn=<epoch> what=<begin|end>                       (turn marker)
A fail-safe token cannot rely on the shared folder: it prints `[istat-pacer] FAILSAFE-TOKEN <R line>` to the
process's own log, and a line the pacer could not append prints `[istat-pacer] UNLOGGED <line>`.
`tools/check_istat_rate.py` reads both and is the only instrument for a combined-rate figure.

THIS IS NOT ENFORCED FOR: a curl, a browser, or any process outside this code. An ad-hoc HTTP request or TCP
probe to any ISTAT host from this workstation spends the same budget; send it through `acquire`, or from a
second egress.

Design: F:/work/econ_finish/istat_pacing/DESIGN.md section 6; review VERDICT_design.md V1-V14.
Python 3.11 compatible (CI); the workstation runs 3.14.
"""
from __future__ import annotations

import datetime as _dt
import email.utils as _eu
import math
import os
import platform as _platform
import re
import threading
import time
import urllib.parse as _up
import urllib.request as _ur
import zlib
from typing import NamedTuple

try:                                   # Windows only; elsewhere the pacer is fail-safe (or no-send on WSL)
    import msvcrt                      # type: ignore
except ImportError:                    # pragma: no cover - POSIX
    msvcrt = None                      # type: ignore

# ───────────────────────────── 6.1 constants (the ONLY definition of the budget) ─────────────────────────────
BUDGET_PER_MIN = 4                     # ISTAT's 5, minus one for margin
GAP_S = 60.0 / BUDGET_PER_MIN + 1.0    # 16.0: min gap between any two token starts (> 15 s)
N_MAX = 4                              # participants that can run at once: updater, guard crawler, manual, spare
FAILSAFE_GAP_S = N_MAX * GAP_S         # 64.0: one process alone in fail-safe
LOW_TURN_GAP_S = 61.0                  # a low-priority participant's own gap while the turn marker is live
PENALTY_MIN_S = 60.0                   # after a 429: at least 60 s or Retry-After ...
PENALTY_MAX_S = 48 * 3600.0            # ... at most 48 h (V6: ISTAT's stated block is 1 to 2 days)
TURN_MARKER_MAX_S = 6 * 3600.0         # clamp for `turn`
TURN_DEFAULT_S = 30 * 60.0             # begin_turn(None): now + 30 min, renewed at each updater token
LOCK_WAIT_MAX_S = 5.0                  # bounded lock wait, then fail-safe
LOCK_POLL_S = 0.05                     # poll the lock every 50 ms
SLIP_MAX_S = 2.0                       # V2: < 4 x GAP_S - 60 = 4 s
LAST_CHECK_S = 0.5                     # D3: the shared penalty is re-read this long before a slot ...
PEN_CHECK_LOCK_S = 0.3                 # ... under a lock waited for at most this long (busy = skip the check)
FUTURE_SLACK_S = 5.0                   # a `next` further than N_MAX x GAP_S + this is clamped
WAIT_LOG_EVERY_S = 30 * 60.0           # a long wait logs at its start and every 30 min (C10)
FAILSAFE_BANNER_EVERY_S = 30 * 60.0
YIELD_POLL_S = 60.0                    # yield_to_turn re-reads the marker at least this often

PRODUCTION_STATE_DIR = r"E:\research\econfindatalibrary\logs\istat_pacer"
STATE_DIR = PRODUCTION_STATE_DIR       # fixed, never ROOT-relative (C9c); tests monkeypatch it (6.8)
LOCK_NAME = "pacer.oslock"
LOCK_REGION = 256
RECORD_OFFSET = 16
HEADER = b"ekd-istat-pacer\n"          # bytes [0, 16)
RECORD_TAG = "ekd-istat-pacer"
RECORD_VERSION = 1

PARTICIPANTS = ("updater", "crawler", "tool:catalog", "tool:probe")
KINDS = ("http", "tcp")


def is_istat_host(host_or_url) -> bool:
    """ISTAT_HOST: a host equal to `istat.it` or ending in `.istat.it` needs a token."""
    h = host_of(host_or_url)
    return h == "istat.it" or h.endswith(".istat.it")


ISTAT_HOST = is_istat_host


def host_of(host_or_url) -> str:
    """The lower-case host of a URL or a `host[:port]` string."""
    s = str(host_or_url or "").strip()
    if "://" in s:
        h = _up.urlsplit(s).hostname or ""
    else:
        h = s.split("/")[0]
        if h.startswith("["):
            h = h[1:].split("]")[0]
        elif h.count(":") == 1:
            h = h.split(":")[0]
    return h.strip().rstrip(".").lower()


# ───────────────────────────────────────────── errors and the token ─────────────────────────────────────────────
class WindowEnd(Exception):
    """No slot can start by deadline_ts - 1 s. The record is NOT written. The caller defers (R303)."""

    def __init__(self, message="", slot=None, deadline_ts=None):
        super().__init__(message)
        self.slot = slot
        self.deadline_ts = deadline_ts


class NoSendPlatform(WindowEnd):
    """WSL on this workstation: it shares the workstation's IP and cannot share the Windows lock (V4)."""


class RecordError(ValueError):
    """A missing, empty, torn or unknown-version record."""


class RecordNewer(RecordError):
    """A record written by a NEWER pacer version: never overwritten."""


class _PacerFailure(Exception):
    """Any failure of the shared path: this token goes fail-safe."""


class Token:
    """Token(id, slot, waited, mode) plus what the request log needs. `start` is the ACTUAL send start."""

    __slots__ = ("id", "slot", "waited", "mode", "participant", "host", "kind", "path_class", "flow",
                 "start", "reason", "mslot", "mslip")

    def __init__(self, id, slot, waited, mode, participant="", host="", kind="", path_class="", flow=None,
                 start=None, reason=None):
        self.mslot = None                  # the slot on time.monotonic(), when the clock has one (D6)
        self.mslip = None                  # the monotonic slip of the actual start
        self.id = id
        self.slot = slot
        self.waited = waited
        self.mode = mode
        self.participant = participant
        self.host = host
        self.kind = kind
        self.path_class = path_class
        self.flow = flow
        self.start = start
        self.reason = reason

    def _core(self):
        return (self.id, self.slot, self.waited, self.mode)

    def __iter__(self):
        return iter(self._core())

    def __getitem__(self, i):
        return self._core()[i]

    def __len__(self):
        return 4

    def __repr__(self):
        return (f"Token(id={self.id!r}, slot={self.slot!r}, waited={self.waited!r}, mode={self.mode!r}, "
                f"participant={self.participant!r}, host={self.host!r}, start={self.start!r})")


class Record(NamedTuple):
    """The slot record. next = earliest start of the next token; upd / low = last start by the updater / by a
    low-priority participant; turn = turn marker end; pen = 429 penalty end; seq = tokens reserved."""
    next: float = 0.0
    upd: float = 0.0
    low: float = 0.0
    turn: float = 0.0
    pen: float = 0.0
    seq: int = 0
    mnext: float = 0.0          # D2: `next` on time.monotonic() (0 = no copy)
    mw: float = 0.0             # D2: time.monotonic() at the write (a larger value than now = another boot)

    def __getitem__(self, key):                       # rec["next"] as well as rec[0]
        if isinstance(key, str):
            return getattr(self, key)
        return tuple.__getitem__(self, key)

    def get(self, key, default=None):
        return getattr(self, key, default)


_FIELDS = ("next", "upd", "low", "turn", "pen")
_MONO_FIELDS = ("mnext", "mw")
_EPOCH_MAX = 9999999999.0


def _epoch(v) -> float:
    v = float(v)
    if v != v:                                        # NaN
        raise RecordError("NaN in record")
    return min(max(v, 0.0), _EPOCH_MAX)


def _epoch_text(v) -> str:
    """Fixed width (17 chars), microseconds, rounded UP: a stored time is never earlier than the one computed
    (3 decimals rounded to nearest gave the next reader a slot up to 0.5 ms early - found by TB7)."""
    us = math.ceil(_epoch(v) * 1_000_000 - 1e-3)
    return f"{us // 1_000_000:010d}.{us % 1_000_000:06d}"


def format_record(rec) -> bytes:
    """One fixed-width ASCII line with a crc32 over everything before ` crc=`."""
    rec = _coerce_record(rec)
    parts = [f"{RECORD_TAG}/{RECORD_VERSION}"]
    for name in _FIELDS:
        parts.append(f"{name}={_epoch_text(getattr(rec, name))}")
    parts.append(f"seq={int(rec.seq) % 10 ** 12:012d}")
    for name in _MONO_FIELDS:
        parts.append(f"{name}={_epoch_text(getattr(rec, name))}")
    body = " ".join(parts)
    crc = zlib.crc32(body.encode("ascii")) & 0xFFFFFFFF
    return (body + f" crc={crc:08x}\n").encode("ascii")


_REC_RE = re.compile(r"^ekd-istat-pacer/(\d+) (.*) crc=([0-9a-f]{8})$")


def parse_record(raw) -> Record:
    """bytes or str (from offset 16) -> Record; RecordError (missing/empty/torn/version) otherwise."""
    if raw is None:
        raise RecordError("missing")
    if isinstance(raw, str):
        raw = raw.encode("ascii", "replace")
    raw = bytes(raw)
    if not raw.strip(b"\0 \r\n\t"):
        raise RecordError("empty")
    line, nl, _ = raw.partition(b"\n")
    if not nl:
        raise RecordError("torn (no line end)")
    try:
        text = line.decode("ascii")
    except UnicodeDecodeError:
        raise RecordError("torn (not ASCII)") from None
    m = _REC_RE.match(text)
    if not m:
        vm = re.match(r"^ekd-istat-pacer/(\d+)\b", text)
        if vm and int(vm.group(1)) > RECORD_VERSION:
            raise RecordNewer(f"version {vm.group(1)}")
        raise RecordError("torn (bad shape)")
    version = int(m.group(1))
    if version > RECORD_VERSION:
        raise RecordNewer(f"version {version}")
    if version != RECORD_VERSION:
        raise RecordError(f"unknown version {version}")
    body = text[: text.rindex(" crc=")]
    if (zlib.crc32(body.encode("ascii")) & 0xFFFFFFFF) != int(m.group(3), 16):
        raise RecordError("torn (bad crc)")
    vals = {}
    for item in m.group(2).split(" "):
        k, eq, v = item.partition("=")
        if not eq:
            raise RecordError("torn (bad field)")
        vals[k] = v
    try:
        rec = Record(*(float(vals[n]) for n in _FIELDS), seq=int(vals["seq"]),
                     **{n: float(vals[n]) for n in _MONO_FIELDS})
    except (KeyError, ValueError):
        raise RecordError("torn (bad field)") from None
    for n in _FIELDS + _MONO_FIELDS:
        _epoch(getattr(rec, n))
    return rec


def _coerce_record(record) -> Record:
    if isinstance(record, Record):
        return record
    if isinstance(record, (bytes, bytearray, str)):
        return parse_record(record)
    if hasattr(record, "get") or hasattr(record, "__getitem__"):
        get = record.get if hasattr(record, "get") else (lambda k, d=0: record[k])
        try:
            return Record(*(float(get(n, 0.0) or 0.0) for n in _FIELDS), seq=int(get("seq", 0) or 0),
                          **{n: float(get(n, 0.0) or 0.0) for n in _MONO_FIELDS})
        except (TypeError, ValueError, KeyError, IndexError) as e:
            raise RecordError(f"not a record: {type(e).__name__}") from None
    raise RecordError(f"not a record: {type(record).__name__}")


def _just_sent(now) -> Record:
    """A missing, empty, torn or unknown-version record means "a request was just sent" (C3). Its seq starts
    from the clock (ms), so token ids `<pid>-<seq>` never restart after a repair."""
    return Record(next=now + FAILSAFE_GAP_S, seq=int(max(0.0, now) * 1000) % 10 ** 11)


def _future_limit(now: float, pen: float) -> float:
    """The latest `next` a legitimate reservation can leave, plus FUTURE_SLACK_S. base = max(now, pen). A
    bad-record (or clamped) slot is base + N_MAX x GAP_S, and up to N_MAX FIFO reservers can queue behind it,
    each one GAP_S later: next <= base + 2 x N_MAX x GAP_S (128 s). LIMIT: this holds for at most N_MAX + 1
    reservations outstanding at once (each participant process has at most one: acquire() returns before the
    next call); a 6th outstanding FIFO reservation after a bad record would be clamped onto the first."""
    return max(now, pen) + 2 * N_MAX * GAP_S + FUTURE_SLACK_S


def _clamp(rec: Record, now: float) -> Record:
    """DESIGN Q3, corrected (build A, see NOTES.md): a `next` later than any legitimate reservation can leave
    (clock moved back, test residue) is clamped to max(now, pen) + N_MAX x GAP_S, never to now. The DESIGN's
    threshold, now + N_MAX x GAP_S + 5 s, also cut LEGITIMATE values: the bad-record rule reserves now + 64 s
    and leaves next = now + 80 s, and a 429 penalty reserves pen and leaves pen + 16 s; the next reader then
    clamped that next back to the slot already reserved, and two tokens shared one instant (found by the TB1,
    TB2 and TB7 bound tests on their first run). pen <= now + PENALTY_MAX_S, turn <= now + TURN_MARKER_MAX_S;
    upd is never later than the limit, low never later than now + slack."""
    pen = min(rec.pen, now + PENALTY_MAX_S)
    base = max(now, pen)
    limit = _future_limit(now, pen)
    nxt = rec.next if rec.next <= limit else base + N_MAX * GAP_S
    upd = min(rec.upd, limit)
    low = min(rec.low, now) if rec.low > now + FUTURE_SLACK_S else rec.low
    turn = min(rec.turn, now + TURN_MARKER_MAX_S)
    return Record(nxt, upd, low, turn, pen, int(rec.seq), rec.mnext, rec.mw)


def _mono_of(clock):
    """The clock's monotonic reading, or None when it has none (a test's fake clock: the copy is then off)."""
    m = getattr(clock, "monotonic", None)
    return float(m()) if m is not None else None


def _mono_valid(rec: Record, mono_now) -> bool:
    """D2: the monotonic copy counts only within one boot (mw <= now) and at a legitimate distance."""
    if mono_now is None or rec.mw <= 0.0 or rec.mw > mono_now + 1e-6:
        return False
    d = rec.mnext - mono_now
    return 0.0 < d <= 2 * N_MAX * GAP_S + FUTURE_SLACK_S


def _apply_mono(rec: Record, now: float, mono_now) -> Record:
    """D2: a valid monotonic copy moves the wall `next` LATER (a forward wall step cannot shorten a gap);
    it never moves it earlier, and an invalid one is ignored (another boot cannot block)."""
    if _mono_valid(rec, mono_now):
        later = now + (rec.mnext - mono_now)
        if later > rec.next:
            return rec._replace(next=later)
    return rec


def _with_mono(rec: Record, now: float, mono_now) -> Record:
    """Stamp the monotonic copy of `next` onto a record about to be written (cleared without a monotonic
    clock, so a fake-clock writer never leaves a stale copy behind)."""
    if mono_now is None:
        return rec._replace(mnext=0.0, mw=0.0)
    return rec._replace(mnext=mono_now + max(0.0, rec.next - now), mw=mono_now)


# ───────────────────────────────────────────── 6.2 the PURE core ─────────────────────────────────────────────
def next_slot(record, now, participant, own_last, turn_live=None, *, own_gap=None, mono_now=None):
    """-> (slot or None, retry_at, new_record or None). No I/O.

    Updater: FIFO reservation, slot = max(now, next, pen, own_last + own_gap); next = slot + GAP_S.
    Low priority: slot = now only when now >= next, now >= pen, now >= own_last + own_gap and, while the turn
    marker is live, now >= low + LOW_TURN_GAP_S; otherwise no slot and retry_at = the earliest time it could
    pass. A record that is None, empty, torn or of an unknown version reads as "a request was just sent".
    mono_now (optional): the reader's time.monotonic(); a valid monotonic copy of next moves next later
    (D2), and the new record carries a fresh copy."""
    now = float(now)
    try:
        rec = _coerce_record(record) if record is not None else _just_sent(now)
        for n in _FIELDS + _MONO_FIELDS:
            _epoch(getattr(rec, n))
    except RecordError:
        rec = _just_sent(now)
    rec = _apply_mono(_clamp(rec, now), now, mono_now)
    if turn_live is None:
        turn_live = rec.turn > now
    gap = float(own_gap) if own_gap is not None else own_gap_s(participant)
    own_last = float(own_last or 0.0)
    if participant == "updater":
        slot = max(now, rec.next, rec.pen, own_last + gap)
        new = rec._replace(next=slot + GAP_S, upd=slot, seq=rec.seq + 1)
        return slot, slot, _with_mono(new, now, mono_now)
    earliest = max(rec.next, rec.pen, own_last + gap)
    if turn_live:
        earliest = max(earliest, rec.low + LOW_TURN_GAP_S)
    if now >= earliest:
        new = rec._replace(next=now + GAP_S, low=now, seq=rec.seq + 1)
        return now, now, _with_mono(new, now, mono_now)
    return None, earliest, None


def own_gap_s(participant, environ=None) -> float:
    """GAP_S for everyone; for the crawler max(GAP_S, 60 / ISTAT_QPM). ISTAT_QPM may only LOWER the rate: a
    value above the budget, zero, negative or not a number is ignored with one log line (DESIGN Q7, C11)."""
    if participant != "crawler":
        return GAP_S
    env = os.environ if environ is None else environ
    raw = env.get("ISTAT_QPM")
    if raw is None or not str(raw).strip():
        return GAP_S
    try:
        q = float(str(raw).strip())
    except ValueError:
        _log_once(f"qpm-nan:{raw}", f"[istat-pacer] ISTAT_QPM={raw!r} is not a number: ignored "
                  f"(gap {GAP_S:g} s)")
        return GAP_S
    if not (q > 0.0) or q != q:
        _log_once(f"qpm-neg:{raw}", f"[istat-pacer] ISTAT_QPM={raw!r} is not positive: ignored (gap {GAP_S:g} s)")
        return GAP_S
    if q > BUDGET_PER_MIN:
        _log_once(f"qpm-high:{raw}", f"[istat-pacer] ISTAT_QPM={raw!r} is above the shared budget "
                  f"{BUDGET_PER_MIN}/min: ignored, it may only LOWER the crawler's rate (gap {GAP_S:g} s)")
        return GAP_S
    return max(GAP_S, 60.0 / q)


# ───────────────────────────────────────────── process state ─────────────────────────────────────────────
_PROC_LOCK = threading.RLock()
_OWN_LAST: dict = {}                   # participant -> this process's last ACTUAL start
_LOCAL_PEN = 0.0                       # a penalty this process knows, honoured in fail-safe too
_TURN_RENEW = False                    # begin_turn(None): the updater renews the marker at each token
_FS: dict = {}                         # fail-safe episode: since, banner_at, reason, tokens
_LOGGED_ONCE: set = set()
_STATS: dict = {}
_FS_SEQ = [0]
_LAST_FS_START = [0.0]                 # start of this process's last fail-safe token

# TEST SEAMS. Production never sets them.
_TEST_OVERRIDE = None                  # V7: tests/conftest.py installs a recording zero-wait pacer here
_REAL_SLEPT_S = 0.0                    # V7: real seconds this process slept inside the pacer
_OPENED_PATHS: list = []               # 6.8: every state path this process TRIED to open (bounded), refused or not
_real_perf = time.perf_counter         # bound at import: a test that patches time.* cannot hide real sleep
_real_sleep = time.sleep
_real_monotonic = time.monotonic


def _new_stats() -> dict:
    return {"tokens": 0, "shared": 0, "failsafe": 0, "abandoned": 0, "waited_s": 0.0, "errors": 0,
            "unlock_errors": 0, "log_errors": 0, "window_end": 0, "penalties": 0}


def _reset_process_state() -> None:
    """Tests: forget this process's pacing memory."""
    global _LOCAL_PEN, _TURN_RENEW, _REAL_SLEPT_S
    with _PROC_LOCK:
        _OWN_LAST.clear()
        _LOCAL_PEN = 0.0
        _TURN_RENEW = False
        _FS.clear()
        _LOGGED_ONCE.clear()
        _STATS.clear()
        _STATS.update(_new_stats())
        _FS_SEQ[0] = 0
        _LAST_FS_START[0] = 0.0
        del _OPENED_PATHS[:]
        _REAL_SLEPT_S = 0.0


_STATS.update(_new_stats())


def stats() -> dict:
    """This process's pacer counters (tokens, shared, failsafe, abandoned, waited_s, errors, ...)."""
    with _PROC_LOCK:
        return dict(_STATS)


def _log(msg: str) -> None:
    try:
        print(msg, flush=True)
    except Exception:                                  # noqa: BLE001 - a closed stdout must not stop a token
        pass


def _log_once(key: str, msg: str) -> None:
    if key not in _LOGGED_ONCE:
        _LOGGED_ONCE.add(key)
        _log(msg)


def _error(what: str, exc=None, counter: str = "errors") -> None:
    with _PROC_LOCK:
        _STATS["errors"] = _STATS.get("errors", 0) + 1
        if counter != "errors":
            _STATS[counter] = _STATS.get(counter, 0) + 1
    _log(f"[istat-pacer] ERROR {what}" + (f": {type(exc).__name__}: {exc}" if exc is not None else ""))


def _add_real(dt_s: float) -> None:
    global _REAL_SLEPT_S
    _REAL_SLEPT_S += max(0.0, dt_s)


def _sleep(clock, seconds: float) -> None:
    """Every pacer wait goes through here, so the V7 guard can measure REAL time slept."""
    if seconds <= 0:
        return
    t0 = _real_perf()
    try:
        clock.sleep(seconds)
    finally:
        _add_real(_real_perf() - t0)


def _sleep_until(clock, target: float) -> None:
    """Wait until the wall time `target`, by DURATION (D6): the duration is read once from the wall clock and
    then slept against a monotonic deadline, so a wall step during the sleep neither adds to the wait nor
    cuts it. A clock without monotonic() (a test's fake clock) is slept on its own time(). A clock that never
    advances is a broken test clock: refuse."""
    dur = target - clock.time()
    if dur <= 0:
        return
    mono = getattr(clock, "monotonic", None)
    read = mono if mono is not None else clock.time
    deadline = read() + dur
    stuck = 0
    while True:
        before = read()
        if before >= deadline:
            return
        _sleep(clock, deadline - before)
        if read() <= before:
            stuck += 1
            if stuck > 10000:
                raise RuntimeError("istat pacer: the clock does not advance on sleep")
        else:
            stuck = 0


def _slot_remaining(clock, tok) -> float:
    """Seconds until the token's slot: on the monotonic clock when the token has a monotonic slot (D6)."""
    mono = _mono_of(clock)
    if mono is not None and getattr(tok, "mslot", None) is not None:
        return tok.mslot - mono
    return tok.slot - clock.time()


def _wait_for_slot(clock, tok, offset: float) -> None:
    """Sleep until (slot + offset), against the monotonic slot when there is one: a wall step during the
    sleep changes nothing (D6). Without one (a fake clock), by the wall-clock duration."""
    mono = getattr(clock, "monotonic", None)
    if mono is None or getattr(tok, "mslot", None) is None:
        _sleep_until(clock, tok.slot + offset)
        return
    target = tok.mslot + offset
    stuck = 0
    while True:
        before = float(mono())
        if before >= target:
            return
        _sleep(clock, target - before)
        if float(mono()) <= before:
            stuck += 1
            if stuck > 10000:
                raise RuntimeError("istat pacer: the clock does not advance on sleep")
        else:
            stuck = 0


def _iso(t: float) -> str:
    try:
        return _dt.datetime.fromtimestamp(float(t), _dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    except (OverflowError, OSError, ValueError):
        return "-"


def _f(v) -> str:
    """A log field value: no whitespace, never empty."""
    s = re.sub(r"\s+", "_", str(v if v is not None else "").strip())
    return s or "-"


def _uname_release() -> str:
    return _platform.uname().release


def _is_wsl() -> bool:
    if msvcrt is not None:
        return False
    try:
        return "microsoft" in str(_uname_release()).lower()
    except Exception:                                  # noqa: BLE001
        return False


def _under_pytest() -> bool:
    return "PYTEST_CURRENT_TEST" in os.environ


def _is_production_dir(path: str) -> bool:
    return os.path.normcase(os.path.abspath(path)) == os.path.normcase(os.path.abspath(PRODUCTION_STATE_DIR))


def _check_state_dir() -> str:
    d = STATE_DIR
    if _under_pytest() and _is_production_dir(d):
        # 6.8: no test may create or open the machine's pacer file. A test that forgot the conftest
        # redirect gets the fail-safe, and nothing on E: is touched.
        raise _PacerFailure(f"production state folder refused under pytest: {d}")
    return d


# ───────────────────────────────────────────── the locked file ─────────────────────────────────────────────
def _note_path(p: str) -> None:
    if len(_OPENED_PATHS) < 10000:
        _OPENED_PATHS.append(p)


def _open_and_lock(wait=None) -> int:
    """os.open the never-deleted lock file and take bytes [0, 256) with LK_NBLCK, polling every LOCK_POLL_S
    for at most `wait` (default LOCK_WAIT_MAX_S). A PermissionError from os.open under churn is contention
    (R2057)."""
    wait_s = LOCK_WAIT_MAX_S if wait is None else float(wait)
    if msvcrt is None:
        raise _PacerFailure("no msvcrt on this platform (fail-safe always)")
    path = os.path.join(STATE_DIR, LOCK_NAME)
    _note_path(path)                                   # recorded BEFORE the refusal: the 6.8 guard reads it
    d = _check_state_dir()
    try:
        os.makedirs(d, exist_ok=True)
    except OSError as e:
        raise _PacerFailure(f"state folder unusable: {d}: {type(e).__name__}: {e}") from None
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_BINARY", 0)
    t0 = _real_monotonic()
    while True:
        fd = None
        try:
            fd = os.open(path, flags)
        except PermissionError:
            fd = None
        except OSError as e:
            raise _PacerFailure(f"lock file unusable: {path}: {type(e).__name__}: {e}") from None
        if fd is not None:
            try:
                os.lseek(fd, 0, 0)
                msvcrt.locking(fd, msvcrt.LK_NBLCK, LOCK_REGION)
                return fd
            except OSError:
                os.close(fd)
        if _real_monotonic() - t0 >= wait_s:
            raise _PacerFailure(f"lock busy for more than {wait_s:g} s: {path}")
        t1 = _real_perf()
        _real_sleep(LOCK_POLL_S)
        _add_real(_real_perf() - t1)


def _unlock_close(fd: int) -> None:
    """Seek to 0 BEFORE the unlock (V3): msvcrt.locking counts from the CURRENT position, and the read and
    write left it past offset 16. Never raises: a failure here is logged, and closing the handle releases
    the lock anyway."""
    try:
        os.lseek(fd, 0, 0)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, LOCK_REGION)
    except OSError as e:
        _error("unlock failed (the handle close below releases it)", e, counter="unlock_errors")
    finally:
        try:
            os.close(fd)
        except OSError as e:
            _error("close failed", e)


def _read_record(fd: int) -> bytes:
    os.lseek(fd, RECORD_OFFSET, 0)
    return os.read(fd, LOCK_REGION - RECORD_OFFSET)


def _write_record(fd: int, rec: Record) -> None:
    """THE COMMIT POINT of a token. Before it, a failure gives a fail-safe token; after it, the token uses
    its committed slot exactly once (V3)."""
    data = HEADER + format_record(rec)
    os.lseek(fd, 0, 0)
    n = os.write(fd, data)
    if n != len(data):
        raise OSError(f"short write {n} of {len(data)}")


def _append_lines(lines) -> None:
    """Append whole lines to requests_YYYYMM.log (by each line's own time). Only ever under the pacer lock,
    so lines never interleave. Raises on failure: the caller decides what that costs."""
    by_file: dict = {}
    for t, text in lines:
        name = "requests_%s.log" % _month(t)
        by_file.setdefault(name, []).append(text if text.endswith("\n") else text + "\n")
    d = STATE_DIR
    for name, texts in by_file.items():
        path = os.path.join(d, name)
        _note_path(path)
        fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | getattr(os, "O_BINARY", 0))
        try:
            data = "".join(texts).encode("ascii", "replace")
            if os.write(fd, data) != len(data):
                raise OSError("short append")
        finally:
            os.close(fd)


def _month(t: float) -> str:
    try:
        return _dt.datetime.fromtimestamp(float(t), _dt.timezone.utc).strftime("%Y%m")
    except (OverflowError, OSError, ValueError):
        return "000000"


def _unlogged(lines) -> None:
    for _, text in lines:
        _log("[istat-pacer] UNLOGGED " + text.rstrip("\n"))


def _locked_rmw(clock, fn):
    """Lock, read, fn(rec_or_None, bad_reason, now) -> (new_record or None, lines, result); write if a new
    record; append lines; unlock. A RecordNewer record is never overwritten. Raises _PacerFailure before a
    write; never after it."""
    fd = _open_and_lock()
    try:
        raw = _read_record(fd)
        now = clock.time()
        try:
            rec, bad = parse_record(raw), None
        except RecordNewer as e:
            raise _PacerFailure(f"record written by a NEWER pacer ({e}); not overwritten") from None
        except RecordError as e:
            rec, bad = None, str(e)
        new, lines, result = fn(rec, bad, now)
        if new is not None:
            _write_record(fd, new)                     # COMMIT POINT
        if lines:
            try:
                _append_lines(lines)
            except Exception as e:                     # noqa: BLE001 - after the commit nothing may undo it
                _error("request log append failed", e, counter="log_errors")
                _unlogged(lines)
        return result
    except _PacerFailure:
        raise
    except WindowEnd:
        raise
    except Exception as e:                             # noqa: BLE001
        raise _PacerFailure(f"unexpected {type(e).__name__}: {e}") from None
    finally:
        _unlock_close(fd)


# ───────────────────────────────────────────── 6.3 acquire ─────────────────────────────────────────────
def _check_args(participant, host, kind, path_class, flow):
    if participant not in PARTICIPANTS:
        raise ValueError(f"istat pacer: unknown participant {participant!r} (one of {PARTICIPANTS})")
    if kind not in KINDS:
        raise ValueError(f"istat pacer: unknown kind {kind!r} (one of {KINDS})")
    h = host_of(host)
    if not is_istat_host(h):
        raise ValueError(f"istat pacer: {host!r} is not an ISTAT host; only *.istat.it needs a token")
    return participant, h, kind, _f(path_class), (None if flow is None else _f(flow))


def _r_line(tok: Token, now: float) -> str:
    return (f"R {_iso(now)} {tok.slot:.3f} pid={os.getpid()} part={tok.participant} host={tok.host} "
            f"kind={tok.kind} class={tok.path_class} flow={_f(tok.flow) if tok.flow else '-'} mode={tok.mode} "
            f"waited={max(0.0, tok.slot - now):.3f} tok={tok.id}")


def _deadline_check(start: float, deadline_ts, what: str) -> None:
    if deadline_ts is not None and start > float(deadline_ts) - 1.0:
        with _PROC_LOCK:
            _STATS["window_end"] = _STATS.get("window_end", 0) + 1
        raise WindowEnd(f"istat pacer: {what} {start:.3f} is later than deadline - 1 s "
                        f"({float(deadline_ts) - 1.0:.3f}); not started", slot=start, deadline_ts=deadline_ts)


def _failsafe_enter(reason: str, now: float) -> None:
    with _PROC_LOCK:
        if not _FS:
            _FS.update(since=now, banner_at=None, reason=reason, tokens=0)
        _FS["reason"] = reason
        if _FS.get("banner_at") is None or now - _FS["banner_at"] >= FAILSAFE_BANNER_EVERY_S:
            _FS["banner_at"] = now
            banner = True
        else:
            banner = False
    if banner:
        _log(f"[istat-pacer] FAIL-SAFE: {reason}; this process paces ALONE at 1 request per "
             f"{FAILSAFE_GAP_S:g} s")


def _failsafe_leave() -> None:
    with _PROC_LOCK:
        was = dict(_FS)
        _FS.clear()
    if was:
        _log(f"[istat-pacer] shared mode restored after {was.get('tokens', 0)} fail-safe token(s) "
             f"(reason was: {was.get('reason')})")


def _shared_step(participant, host, kind, path_class, flow, deadline_ts, clock, pending):
    """One locked read-modify-write. -> ("slot", Token) | ("wait", retry_at). Raises WindowEnd (record not
    written) or _PacerFailure (nothing committed)."""
    def fn(rec, bad, now):
        own_last = _OWN_LAST.get(participant, 0.0)
        gap = own_gap_s(participant)
        if _LAST_FS_START[0]:
            # After a fail-safe token (which the shared record never saw) the first shared slot is no earlier
            # than that token + FAILSAFE_GAP_S, so the transition back adds nothing to the 4 + k bound (review).
            own_last = max(own_last, _LAST_FS_START[0] + FAILSAFE_GAP_S - gap)
        if _LOCAL_PEN:
            # A penalty this process knows holds in shared mode too, and survives a torn record; writing it
            # makes it shared at the next good lock (review finding 1).
            base = rec if rec is not None else _just_sent(now)
            if _LOCAL_PEN > base.pen:
                rec = base._replace(pen=_LOCAL_PEN)
        mono_now = _mono_of(clock)
        slot, retry_at, new = next_slot(rec, now, participant, own_last, own_gap=gap, mono_now=mono_now)
        lines = list(pending)
        if bad is not None:
            _log_once(f"bad-record:{bad}", f"[istat-pacer] record {bad}: read as 'a request was just sent'")
        if slot is None:
            _deadline_check(max(retry_at, now), deadline_ts, "earliest eligible time")   # before any write
            # A bad record is repaired as "just sent" (rec is then None, or the just-sent record with this
            # process's penalty merged in). D1b: a record whose `next` the clamp moved (residue far in the
            # future) is written back clamped, so nobody waits for the wall clock to reach the residue.
            repair = None
            if bad is not None:
                repair = _clamp(rec if rec is not None else _just_sent(now), now)
            elif rec is not None and rec.next > _future_limit(now, min(rec.pen, now + PENALTY_MAX_S)):
                repair = _clamp(rec, now)
            if repair is not None:
                repair = _with_mono(_apply_mono(repair, now, mono_now), now, mono_now)
            return repair, lines, ("wait", retry_at)
        _deadline_check(slot, deadline_ts, "slot")                                     # before any write
        if participant == "updater" and _TURN_RENEW:
            new = new._replace(turn=max(new.turn, now + min(TURN_DEFAULT_S, TURN_MARKER_MAX_S)))
        tok = Token(f"{os.getpid()}-{new.seq}", slot, 0.0, "shared", participant, host, kind, path_class, flow)
        tok.mslot = None if mono_now is None else mono_now + (slot - now)       # D6: the slot on the monotonic clock
        lines.append((now, _r_line(tok, now)))
        return new, lines, ("slot", tok)
    result = _locked_rmw(clock, fn)
    del pending[:]
    return result


def acquire(participant, host, kind, path_class, *, deadline_ts=None, flow=None, clock=time) -> Token:
    """Take one token for ONE request to an ISTAT host and return when it may start.

    participant: updater | crawler | tool:catalog | tool:probe. kind: http | tcp. clock: the CALLER's time
    module (anything with .time() and .sleep()). Returns Token(id, slot, waited, mode) with mode shared or
    failsafe and `start` = the clock at return (send at once, or call started() right before the send).
    Raises WindowEnd only when no slot can start by deadline_ts - 1 s (NoSendPlatform on WSL). Never raises
    for a pacer failure: that token is fail-safe."""
    if _TEST_OVERRIDE is not None:
        return _TEST_OVERRIDE.acquire(participant, host, kind, path_class, deadline_ts=deadline_ts, flow=flow,
                                      clock=clock)
    participant, host, kind, path_class, flow = _check_args(participant, host, kind, path_class, flow)
    if _is_wsl():
        _log_once("wsl", "[istat-pacer] NO-SEND: WSL shares this workstation's IP and cannot share the Windows "
                  "pacer lock; this process sends NO request to ISTAT (verdict V4)")
        raise NoSendPlatform("istat pacer: WSL sends no ISTAT request")
    t_call = clock.time()
    pending: list = []                                  # X lines for abandoned tokens, appended at the next lock
    try:
        return _acquire_loop(participant, host, kind, path_class, flow, deadline_ts, clock, pending, t_call)
    except WindowEnd:
        if pending:
            _unlogged(pending)
        raise


def _acquire_loop(participant, host, kind, path_class, flow, deadline_ts, clock, pending, t_call):
    retried_after_failure = False
    while True:
        try:
            with _PROC_LOCK:                            # threads of one process take turns; never across a sleep
                kind_, val = _shared_step(participant, host, kind, path_class, flow, deadline_ts, clock, pending)
        except WindowEnd:
            raise
        except _PacerFailure as e:
            kind_, val = "fail", str(e)
        except Exception as e:                          # noqa: BLE001 - a pacer bug must not send unpaced
            kind_, val = "fail", f"unexpected {type(e).__name__}: {e}"
        if kind_ == "fail":
            now = clock.time()
            own_last = _OWN_LAST.get(participant, 0.0)
            if not retried_after_failure:
                # V1(a): never start at once after a failure. Wait FAILSAFE_GAP_S, then try the lock again.
                earliest = max(now + FAILSAFE_GAP_S, own_last + FAILSAFE_GAP_S, _LOCAL_PEN)
                _deadline_check(earliest, deadline_ts, "fail-safe start")
                _failsafe_enter(val, now)
                _sleep_until(clock, earliest)
                retried_after_failure = True
                continue
            start = max(now, own_last + FAILSAFE_GAP_S, _LOCAL_PEN)
            _deadline_check(start, deadline_ts, "fail-safe start")
            _sleep_until(clock, start)
            with _PROC_LOCK:
                _FS_SEQ[0] += 1
                tok_id = f"{os.getpid()}-f{_FS_SEQ[0]}"
            tok = Token(tok_id, start, 0.0, "failsafe", participant, host, kind, path_class, flow, reason=val)
            _log("[istat-pacer] FAILSAFE-TOKEN " + _r_line(tok, start))
            if pending:
                _unlogged(pending)
                del pending[:]
            with _PROC_LOCK:
                _FS["tokens"] = _FS.get("tokens", 0) + 1
                _STATS["failsafe"] += 1
                _LAST_FS_START[0] = max(_LAST_FS_START[0], start)
            return _finish(tok, clock, t_call)
        retried_after_failure = False
        if _FS:
            _failsafe_leave()
        if kind_ == "wait":
            now = clock.time()
            retry_at = max(float(val), now)
            if retry_at - now > 60.0:
                key = f"wait:{participant}:{int(now // WAIT_LOG_EVERY_S)}"
                _log_once(key, f"[istat-pacer] {participant} waits for a token until {_iso(retry_at)} "
                          f"({(retry_at - now) / 60:.1f} min)")
            _sleep_until(clock, min(retry_at, now + WAIT_LOG_EVERY_S))
            continue
        tok = val                                       # committed: use this slot exactly once
        if _slot_remaining(clock, tok) > LAST_CHECK_S:
            # D3: a penalty ANOTHER process set after this slot was reserved. Re-read it before the last
            # LAST_CHECK_S of the wait (so no file I/O comes after the final slip check).
            _wait_for_slot(clock, tok, -LAST_CHECK_S)
            pen = _peek_pen(clock)
            if pen is not None and pen > tok.slot + 1e-6:
                now = clock.time()
                with _PROC_LOCK:
                    _STATS["abandoned"] += 1
                _log(f"[istat-pacer] token {tok.id} abandoned: a penalty until {_iso(pen)} covers its slot")
                pending.append((now, f"X {_iso(now)} tok={tok.id} reason=penalty slip={now - tok.slot:.3f}"))
                continue
        _wait_for_slot(clock, tok, 0.0)
        now = clock.time()
        mono = _mono_of(clock)
        slip = (mono - tok.mslot) if (mono is not None and tok.mslot is not None) else (now - tok.slot)
        if now < _LOCAL_PEN:
            # A 429 arrived (in this process) after this slot was reserved: do not send into the penalty.
            with _PROC_LOCK:
                _STATS["abandoned"] += 1
            _log(f"[istat-pacer] token {tok.id} abandoned: a penalty until {_iso(_LOCAL_PEN)} began after it "
                 f"was reserved")
            pending.append((now, f"X {_iso(now)} tok={tok.id} reason=penalty slip={now - tok.slot:.3f}"))
            continue
        if slip > SLIP_MAX_S:
            # V2(b): woke too late (measured on the monotonic clock when there is one, D6). Do not send; the
            # next lock session logs the X line.
            with _PROC_LOCK:
                _STATS["abandoned"] += 1
            _log(f"[istat-pacer] token {tok.id} abandoned: woke {slip:.3f} s after its slot "
                 f"(> {SLIP_MAX_S:g} s); taking a new one")
            pending.append((now, f"X {_iso(now)} tok={tok.id} reason=late slip={slip:.3f}"))
            continue
        tok.mslip = slip if tok.mslot is not None else None
        with _PROC_LOCK:
            _STATS["shared"] += 1
        return _finish(tok, clock, t_call)


def _finish(tok: Token, clock, t_call: float) -> Token:
    now = clock.time()
    tok.start = now
    tok.waited = max(0.0, now - t_call)
    with _PROC_LOCK:
        _OWN_LAST[tok.participant] = max(_OWN_LAST.get(tok.participant, 0.0), now)
        _STATS["tokens"] += 1
        _STATS["waited_s"] += tok.waited
    return tok


def started(token, clock=time) -> float:
    """Record the ACTUAL send start (V2(a)): call it right before sess.get / create_connection / urlopen when
    anything runs between acquire() and the send. outcome() logs it with its slip."""
    t = clock.time()
    token.start = t
    mono = _mono_of(clock)
    if mono is not None and getattr(token, "mslot", None) is not None:
        token.mslip = mono - token.mslot
    try:
        slip = token.mslip if getattr(token, "mslip", None) is not None else t - float(token.slot)
        late = token.mode in ("shared", "failsafe") and slip > SLIP_MAX_S
    except (TypeError, ValueError):
        late = False
    if late:
        # Not enforced here (the caller is about to send); counted, logged, and the receipt fails on it.
        with _PROC_LOCK:
            _STATS["late_sends"] = _STATS.get("late_sends", 0) + 1
        _log(f"[istat-pacer] LATE SEND tok={token.id}: {slip:.3f} s after its slot "
             f"(> {SLIP_MAX_S:g} s); send at once after acquire()")
    return t


def outcome(token, text, elapsed_s) -> None:
    """Append the outcome line (6.7) with the ACTUAL start and its slip. Never raises."""
    if _TEST_OVERRIDE is not None:
        return _TEST_OVERRIDE.outcome(token, text, elapsed_s)
    try:
        start = token.start if token.start is not None else token.slot
        el = float(elapsed_s) if elapsed_s is not None else 0.0
        t_line = float(start) + max(0.0, el)
        line = (f"O {_iso(t_line)} tok={_f(token.id)} outcome={_f(text)} el={el:.3f} start={float(start):.3f} "
                f"slip={float(start) - float(token.slot):.3f}")
        if getattr(token, "mslip", None) is not None:
            line += f" mslip={float(token.mslip):.3f}"      # D6: the slip on the monotonic clock
        lines = [(t_line, line)]
        if token.mode != "shared":
            _unlogged(lines)
            return
        try:
            with _PROC_LOCK:
                fd = _open_and_lock()
                try:
                    _append_lines(lines)
                finally:
                    _unlock_close(fd)
        except Exception as e:                         # noqa: BLE001
            _error("outcome not logged", e, counter="log_errors")
            _unlogged(lines)
    except Exception as e:                             # noqa: BLE001
        _error("outcome failed", e)


# ───────────────────────────────────────────── 6.5 penalty, 6.6 turn marker ─────────────────────────────────────
def penalize(seconds, participant, clock=time) -> float:
    """After a 429: pen = max(pen, now + clamp(seconds, PENALTY_MIN_S, PENALTY_MAX_S)), for EVERY participant.
    Returns the penalty end. Never raises: a penalty that cannot be shared still holds in this process."""
    global _LOCAL_PEN
    if _TEST_OVERRIDE is not None:
        return _TEST_OVERRIDE.penalize(seconds, participant, clock=clock)
    try:
        s = float(seconds)
    except (TypeError, ValueError):
        s = PENALTY_MIN_S
    if s != s:
        s = PENALTY_MIN_S
    s = min(max(s, PENALTY_MIN_S), PENALTY_MAX_S)
    now = clock.time()
    until = now + s
    with _PROC_LOCK:
        _LOCAL_PEN = max(_LOCAL_PEN, until)
        _STATS["penalties"] += 1
    p = _f(participant)
    line = (now, f"P {_iso(now)} {now:.3f} pid={os.getpid()} part={p} seconds={s:.3f} until={until:.3f}")

    def fn(rec, bad, now2):
        # D7: an empty or torn record is "a request was just sent" here too; D2: keep the monotonic copy
        mono = _mono_of(clock)
        base = _apply_mono(_clamp(rec if rec is not None else _just_sent(now2), now2), now2, mono)
        return _with_mono(base._replace(pen=max(base.pen, until)), now2, mono), [line], None
    try:
        with _PROC_LOCK:
            _locked_rmw(clock, fn)
    except Exception as e:                             # noqa: BLE001
        _log(f"[istat-pacer] penalty NOT shared yet ({e}); it holds in this process, and its next good lock "
             f"writes it for everyone")
        _unlogged([line])
    return until


def _write_turn(clock, value_fn, what: str) -> bool:
    def fn(rec, bad, now):
        # D7: an empty or torn record is "a request was just sent" here too; D2: keep the monotonic copy
        mono = _mono_of(clock)
        base = _apply_mono(_clamp(rec if rec is not None else _just_sent(now), now), now, mono)
        turn = value_fn(now)
        line = (now, f"T {_iso(now)} {now:.3f} pid={os.getpid()} turn={turn:.3f} what={what}")
        return _with_mono(base._replace(turn=turn), now, mono), [line], True
    try:
        with _PROC_LOCK:
            return bool(_locked_rmw(clock, fn))
    except Exception as e:                             # noqa: BLE001
        _log(f"[istat-pacer] turn marker not written ({what}: {e}); the bucket still bounds the rate, "
             f"only the updater's priority is lost")
        return False


def begin_turn(end_ts=None, clock=time) -> bool:
    """The updater's turn marker: turn = min(end_ts, now + TURN_MARKER_MAX_S). end_ts None = now + 30 min,
    renewed at each updater token. Self-expiring: a dead updater's marker ends by itself (C10)."""
    global _TURN_RENEW
    if _TEST_OVERRIDE is not None:
        return _TEST_OVERRIDE.begin_turn(end_ts, clock=clock)
    with _PROC_LOCK:
        _TURN_RENEW = end_ts is None
    if end_ts is None:
        return _write_turn(clock, lambda now: now + min(TURN_DEFAULT_S, TURN_MARKER_MAX_S), "begin")
    end = float(end_ts)
    return _write_turn(clock, lambda now: min(end, now + TURN_MARKER_MAX_S), "begin")


def end_turn(clock=time) -> bool:
    """turn = now (the updater's `finally`)."""
    global _TURN_RENEW
    if _TEST_OVERRIDE is not None:
        return _TEST_OVERRIDE.end_turn(clock=clock)
    with _PROC_LOCK:
        _TURN_RENEW = False
    return _write_turn(clock, lambda now: now, "end")


def _peek_pen(clock):
    """D3: the shared penalty end (wall time) read under a SHORT lock (PEN_CHECK_LOCK_S), or None when it
    cannot be read now (no shared state, a busy lock, a bad record). Writes nothing. Never raises."""
    if msvcrt is None:
        return None
    try:
        with _PROC_LOCK:
            fd = _open_and_lock(wait=PEN_CHECK_LOCK_S)
            try:
                raw = _read_record(fd)
            finally:
                _unlock_close(fd)
        rec = parse_record(raw)
    except Exception:                                  # noqa: BLE001
        return None
    return _clamp(rec, clock.time()).pen


def _read_turn(clock):
    """The marker's end, or None (no shared state, a bad record, or a failure). Never raises."""
    if msvcrt is None:
        return None
    try:
        with _PROC_LOCK:
            fd = _open_and_lock()
            try:
                raw = _read_record(fd)
            finally:
                _unlock_close(fd)
        rec = parse_record(raw)
    except Exception:                                  # noqa: BLE001
        return None
    return _clamp(rec, clock.time()).turn


def yield_to_turn(log=print, clock=time) -> float:
    """The crawler's flow-boundary wait: while the updater's marker is live, log `yielding to the istat updater
    turn until <T> (<n> min)` at the start and every 30 min, and sleep. No single wait is longer than the
    marker's remaining time (clamped to 6 h). Returns the seconds waited. An unreadable marker does not hold
    the crawler: the bucket still bounds the combined rate (6.6)."""
    if _TEST_OVERRIDE is not None:
        return _TEST_OVERRIDE.yield_to_turn(log=log, clock=clock)
    waited, last_log = 0.0, None
    while True:
        turn = _read_turn(clock)
        now = clock.time()
        if turn is None or turn <= now:
            if waited:
                log(f"[istat-pacer] the istat updater turn is over; resuming after {waited / 60:.1f} min")
            return waited
        remaining = turn - now
        if last_log is None or now - last_log >= WAIT_LOG_EVERY_S:
            log(f"[istat-pacer] yielding to the istat updater turn until {_iso(turn)} ({remaining / 60:.0f} min)")
            last_log = now
        chunk = min(remaining, YIELD_POLL_S)
        _sleep(clock, chunk)
        waited += chunk


def describe() -> str:
    """`[istat-pacer] state <path>; mode shared|FAIL-SAFE (<reason>); gap 16 s; budget 4/min` (print once per
    run). Probes the lock once; writes nothing."""
    if _TEST_OVERRIDE is not None:
        return _TEST_OVERRIDE.describe()
    path = os.path.join(STATE_DIR, LOCK_NAME)
    if _is_wsl():
        mode = "NO-SEND (WSL: shares this workstation's IP, cannot share the Windows lock)"
    else:
        try:
            with _PROC_LOCK:
                fd = _open_and_lock()
                try:
                    raw = _read_record(fd)
                finally:
                    _unlock_close(fd)
            try:
                parse_record(raw)
                mode = "shared"
            except RecordNewer as e:
                mode = f"FAIL-SAFE (record written by a NEWER pacer: {e})"
            except RecordError:
                mode = "shared"                        # a missing record is written by the first token
        except _PacerFailure as e:
            mode = f"FAIL-SAFE ({e})"
        except Exception as e:                         # noqa: BLE001
            mode = f"FAIL-SAFE (unexpected {type(e).__name__}: {e})"
    return f"[istat-pacer] state {path}; mode {mode}; gap {GAP_S:g} s; budget {BUDGET_PER_MIN}/min"


# ───────────────────────────────────────── 6.4 / 6.5 helpers (phase B wires them) ─────────────────────────────────
def redirect_decision(request_url, status, location, followed=0):
    """-> ("follow", target_url) | ("dead", reason) | ("not-redirect", "").

    Every ISTAT request is sent with allow_redirects=False. V5(a): a same-host 3xx to a DIFFERENT path is
    followed ONCE, with its own token; a self-redirect (same path), a cross-host redirect (or a scheme or
    port change), a 3xx without a Location, or a second redirect makes the host unusable for the run."""
    try:
        code = int(status)
    except (TypeError, ValueError):
        return "not-redirect", ""
    if not 300 <= code <= 399:
        return "not-redirect", ""
    if followed:
        return "dead", f"a second redirect ({code} -> {location}) after one followed"
    if not location:
        return "dead", f"{code} without a Location"
    target = _up.urljoin(str(request_url), str(location))
    a, b = _up.urlsplit(str(request_url)), _up.urlsplit(target)
    if (a.scheme.lower(), host_of(request_url), a.port) != (b.scheme.lower(), host_of(target), b.port):
        return "dead", f"cross-host redirect {code} -> {target}"
    if (a.path or "/") == (b.path or "/"):
        return "dead", f"self-redirect {code} -> {target}"
    return "follow", target


class NoRedirectHandler(_ur.HTTPRedirectHandler):
    """urllib: never follow. A 3xx comes back as urllib.error.HTTPError with its code and Location."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D401
        return None


def no_redirect_opener(*handlers):
    """An urllib opener that does not follow redirects (the catalog tool's opener)."""
    return _ur.build_opener(NoRedirectHandler, *handlers)


_DIGITS = re.compile(r"[0-9]+")


def retry_after_seconds(value, now=None, log=None) -> float:
    """A 429's Retry-After -> the penalty in seconds, in [PENALTY_MIN_S, PENALTY_MAX_S] (V6). delta-seconds or
    an HTTP-date (RFC 9110 10.2.3); absent = PENALTY_MIN_S; unparseable = PENALTY_MIN_S and one log line."""
    if value is None or not str(value).strip():
        return PENALTY_MIN_S
    s = str(value).strip()
    if _DIGITS.fullmatch(s):
        # a valid delta-seconds of any length: more than 12 digits is past the cap anyway (and int() of
        # 4,300+ digits raises on 3.11+, float() of a 400-digit int overflows)
        secs = float(int(s)) if len(s.lstrip("0")) <= 12 else PENALTY_MAX_S
    else:
        try:
            d = _eu.parsedate_to_datetime(s)
        except (TypeError, ValueError, IndexError, OverflowError):
            d = None
        if d is None:
            (log or _log)(f"[istat-pacer] Retry-After {s[:80]!r} is unparseable: using {PENALTY_MIN_S:g} s")
            return PENALTY_MIN_S
        if d.tzinfo is None:
            d = d.replace(tzinfo=_dt.timezone.utc)
        secs = d.timestamp() - (time.time() if now is None else float(now))
    return min(max(secs, PENALTY_MIN_S), PENALTY_MAX_S)
