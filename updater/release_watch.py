r"""Release watch, phase 1: is the publisher's current release the one the store holds?

Build spec: Revision 5 of the release-watch design (R4.ALT, "desktop + beat"). Phase 1 is EXACTLY the unctad
trio below. Admitting another unit is a separate reviewed PR that shows the three admission tests (the probe can
move when the publisher releases; no fallback chain; keyless).

WHERE EACH PART RUNS
  * desktop, once a day: tools/selfhost/run_release_watch.ps1 -IfDue (started by the guard loop) runs
    `python -m updater.release_watch --desktop`. The driver starts ONE sandboxed child per unit, reads the
    publisher's token P, and updates data/_release_watch/record.json atomically. It holds publisher facts only:
    it NEVER opens state.db or catalog.db (R5), so it cannot race a data run and needs no T0 gate.
  * desktop, every guard tick: tools/guard_heartbeat.py puts beat_value() into body["release_watch"]. It only
    reads the record and the runner's status file.
  * CI, every digest: updater/send_digest.py reads the beat and CI's own state.db (mode=ro) and calls
    digest_view(). The rules (NEW, MOVED, cleared, acks, LATE, stale) are applied there, where the real store
    token T is.

THE RECORD (`ekd-release-watch/1`), per unit "<source_id>/<unit_id>":
  last_p             the last REAL probe token (never a failure)
  first_seen_utc     when last_p was first seen
  last_probe_utc     the last ATTEMPT; last_probe_ok whether it gave a real P; last_ok_probe_utc the last real P
  cnc_days           UTC days with at least one attempt and no real P; reset by a real P
  events             event 0 = the initial token; each change of P appends {from, to, since_utc}. A failure
                     appends nothing and never creates the unit. Over EVENTS_MAX: keep event 0 and the newest
                     19, and set `trimmed` (the digest then always says "(history trimmed)" on a MOVED line).
  last_error         "<kind>: <text>" of the last failed attempt, scrubbed, at most ERROR_MAX characters
A unit whose probes have only ever failed has no entry in `units`; its attempts are counted under `pending`. The
file itself is created only on the first real P (a day-1 COULD NOT CHECK creates nothing).

RUN STATUS. The runner (run_release_watch.ps1) owns logs/release_watch.last.json: it writes `started` BEFORE the
python child starts (so before the first probe), and `ended`, `rc` and `last_ok_started` at the end; a run killed
by the 15-minute limit records rc 96 (the run_notices convention). The digest's LATE rule reads that file through
the beat, so a dead runner reads LATE, not "same". A run in which NO unit produced a real P exits 3: it is retried
in 2 h, keeps the old last_ok_started, and reads LATE after 30 h (a sandbox that refuses every probe is not "ok").

THE CHILD SANDBOX (one per unit, subprocess, 90 s timeout). The environment is an ALLOW-list (child_env): nothing
else is inherited, so no key, token or proxy setting reaches it. It is started `-I -B` (isolated: PYTHON* variables
and the user site are ignored; no .pyc written); the parent passes its own site-packages directories as arguments,
because on the workstation `requests` lives in the user site, which -I removes. The FIRST thing the child does,
before those directories (and their .pth lines) are added, is install an AUDIT HOOK (sys.addaudithook, which
cannot be removed) that raises WatchSideEffect (a BaseException, so `except Exception` in a fetcher cannot swallow
it) on:
  * any `open` (builtins, io, _io, FileIO, os.open, io.open_code, any name) of a credential-shaped file name
    (.env*, *.env, *.key, *_key, .dev.vars, credentials*, *secret*, *.token, token.txt; \\?\ prefixes, :streams and
    Win32 trailing dots are stripped first), of a path under <repo>/data or the temp DATA_ROOT / STATE_DIR, or of
    ANY path outside the read allow-list (<repo>/updater, jobs, core; the interpreter; its site directories; the
    sandbox temp); a write anywhere but the sandbox temp;
  * listing a refused path; copying, moving or deleting outside the sandbox (shutil's native copy included);
  * starting a process, ctypes, sqlite3.connect, writing the registry;
  * DNS or http.client traffic to any host but PROBE_HOSTS, and any HTTP method but GET / HEAD (urllib included).
On top of it the named routes are patched for clear refusals: builtins/io/os/os.path/pathlib file calls, every
updater.blob function and Blob method, merge.merge_and_write, sqlite3.connect, and requests: GET and HEAD only,
trust_env = False on every Session (no netrc, proxy env or CA-bundle env).
Any refusal marks the run, so a probe that caught the exception still reads COULD NOT CHECK (sandbox).
The child prints exactly one JSON line {"p": P|null, "error": null|"<kind>: <text>"}. The parent maps a non-zero
exit, a timeout, any other stdout, a null p, a sandbox refusal, or a P that is not a `version|lastUpdated` token
(TOKEN_RE: nothing else can leave through P) to COULD NOT CHECK; nothing else is success.
A socket may connect only to an address a lookup of a probe host returned; binds only to loopback port 0 (urllib3's
import-time IPv6 test). The child's error TEXT never travels: what is recorded, beaten and mailed is a fixed phrase
plus an exception type from a closed list (the raw text is printed only by --probe). Listing a directory outside
the read allow-list is refused.
RESIDUAL, stated: os.stat-type metadata calls have no audit event, so a compromised probe could encode a file's
size or time into a token-shaped P (a few dozen bits a run; never content, since every read is guarded).

RUNBOOK (operator)
  PUBLISHER MOVED <from> -> <to> since <date>
      Confirm it on the publisher's page. A version move wipes a giant's version-gated spill walk, so record it on
      the giant's issue and raise its desktop_lane_exit work with the owner. The line clears only when the unit
      stamps T == <to>, or under a reviewed ack in updater/release_acks.yaml (at most 30 days).
  release watch LATE
      Check logs/release_watch.last.json and logs/release_watch_*.log, the lock (logs/release_watch.lock), and
      whether the machine's RELAUNCH_GUARD.ps1 has the release-watch launch block.
  could not check x>=3 d
      Run the child by hand: python -m updater.release_watch --probe <sid> _all. Fix the probe or the sandbox.
  stale / unreadable beat
      The guard loop is down (the same fault as the notices section): follow the reboot-recovery steps.
  Never write unit_state by hand (R5). Write any mistake to MISTAKES.md immediately.

    python -m updater.release_watch --desktop              # the runner only (it holds the lock)
    python -m updater.release_watch --probe <sid> <uid>    # one sandboxed probe, printed, nothing written
"""
from __future__ import annotations

import datetime as _dt
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCHEMA = "ekd-release-watch/1"

# Paths (C5). NEVER under data/_aqueduct (STATE_DIR) or data/clean_full (DATA_ROOT): tests/test_release_watch.py
# resolves each one against config and refuses it.
RECORD_PATH = os.path.join(ROOT, "data", "_release_watch", "record.json")
LOCK_PATH = os.path.join(ROOT, "logs", "release_watch.lock")
STATUS_PATH = os.path.join(ROOT, "logs", "release_watch.last.json")
LOG_DIR = os.path.join(ROOT, "logs")
LOG_PATTERN = "release_watch_*.log"
ACKS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "release_acks.yaml")
PATH_CONSTANTS = (RECORD_PATH, LOCK_PATH, STATUS_PATH, LOG_DIR, os.path.join(LOG_DIR, LOG_PATTERN))

# Phase 1, exactly (C8). Each is publisher-stamped `version|lastUpdated` read from a URL that names the dataset,
# with one GET and no fallback chain, and keyless (_unctad.py current_vintage).
PHASE1_UNITS = (("unctad_biotrademerch", "_all"),
                ("unctad_tradefoodcatbyproc", "_all"),
                ("unctad_tradefoodprocbycat", "_all"))

EVENTS_MAX = 20            # keep event 0 and the newest EVENTS_MAX - 1
TOKEN_MAX = 80             # beat: tokens truncated to this
ERROR_MAX = 160            # record and beat: error text truncated to this
BEAT_MAX_BYTES = 8192      # the whole beat value, compact JSON; larger -> unreadable, never a partial record
CHILD_TIMEOUT_S = 90
CNC_STANDALONE_DAYS = 3    # a could-not-check streak this long gets its own digest line
ACK_MAX_DAYS = 30
_ACK_KEYS = {"source_id", "unit_id", "to_token", "reason_quote", "reason_url", "by", "acked_utc", "expires_utc"}
_ACK_REQUIRED = ("source_id", "unit_id", "to_token", "reason_quote", "reason_url", "by", "acked_utc", "expires_utc")
NOT_A_TOKEN = ("date-tail", "force")      # placeholders unit_state can hold; never a real T
WITHHELD_NO_GATE = "names withheld: denylist unreadable"


class WatchSideEffect(BaseException):
    """A sandbox refusal inside the probe child. BaseException on purpose: a fetcher's `except Exception` must not
    turn a refused read into a quiet fallback."""


class RecordError(ValueError):
    """record.json exists but is not a valid `ekd-release-watch/1` record."""


# ------------------------------------------------------------------ time
def utcnow() -> _dt.datetime:
    return _dt.datetime.now(_dt.timezone.utc)


def iso(t: _dt.datetime) -> str:
    return t.astimezone(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_utc(s):
    """A tz-aware UTC datetime, or None. A naive stamp is UTC (the runner writes ...Z)."""
    if not s or not isinstance(s, str):
        return None
    try:
        t = _dt.datetime.fromisoformat(s.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return t if t.tzinfo else t.replace(tzinfo=_dt.timezone.utc)


def _day(s) -> str:
    t = parse_utc(s)
    return t.date().isoformat() if t else "?"


# ------------------------------------------------------------------ the record (pure)
def unit_key(sid: str, uid: str) -> str:
    return f"{sid}/{uid}"


def validate_record(rec) -> None:
    """Raise RecordError unless `rec` is a usable record. Only shape is checked; values are the watch's own."""
    if not isinstance(rec, dict):
        raise RecordError("not a JSON object")
    if rec.get("schema") != SCHEMA:
        raise RecordError(f"schema is {str(rec.get('schema'))[:40]!r}, not {SCHEMA}")
    units = rec.get("units")
    if not isinstance(units, dict):
        raise RecordError("`units` is not an object")
    for i, u in enumerate(units.values(), 1):        # unit keys are never quoted: a key can be gated
        if not isinstance(u, dict) or not isinstance(u.get("last_p"), str) or not u["last_p"]:
            raise RecordError(f"unit #{i} has no last_p")
        ev = u.get("events")
        if not isinstance(ev, list) or not ev or not all(isinstance(e, dict) and isinstance(e.get("to"), str)
                                                        for e in ev):
            raise RecordError(f"unit #{i} has no valid events")
        if not isinstance(u.get("cnc_days", 0), int):
            raise RecordError(f"unit #{i}: cnc_days is not an integer")
    if not isinstance(rec.get("pending", {}), dict):
        raise RecordError("`pending` is not an object")


def load_record(path: str = None):
    """The record, or None when the file does not exist. A file that exists and is invalid RAISES RecordError:
    the driver then keeps the file untouched and the beat publishes `unreadable`."""
    path = path or RECORD_PATH
    try:
        with open(path, encoding="utf-8") as fh:
            raw = fh.read()
    except FileNotFoundError:
        return None
    try:
        rec = json.loads(raw)
    except ValueError as e:
        raise RecordError(f"not JSON ({e})") from None
    validate_record(rec)
    return rec


def _trim(u: dict) -> None:
    ev = u["events"]
    if len(ev) > EVENTS_MAX:
        u["events"] = [ev[0]] + ev[-(EVENTS_MAX - 1):]
        u["trimmed"] = True


def apply_outcome(rec, sid: str, uid: str, outcome: dict, now: _dt.datetime):
    """Fold one probe outcome into the record and return it (a new record on the first real P, else `rec`).

    outcome: {"p": str|None, "error": str|None}. Returns None when there is no record and the probe failed: a
    failure never creates the record (C8 / R3.1)."""
    key = unit_key(sid, uid)
    p = outcome.get("p")
    real = isinstance(p, str) and p != ""
    stamp, today = iso(now), now.astimezone(_dt.timezone.utc).date().isoformat()
    if rec is None:
        if not real:
            return None
        rec = {"schema": SCHEMA, "run": {}, "units": {}, "pending": {}}
    units, pending = rec.setdefault("units", {}), rec.setdefault("pending", {})
    u = units.get(key)
    if real:
        if u is None:
            units[key] = {"last_p": p, "first_seen_utc": stamp, "last_probe_utc": stamp, "last_probe_ok": True,
                          "last_ok_probe_utc": stamp, "cnc_days": 0, "cnc_last_day": None, "last_error": None,
                          "trimmed": False,
                          "events": [{"to": p, "from": None, "since_utc": stamp, "initial": True}]}
        else:
            if p != u["last_p"]:
                u["events"].append({"from": u["last_p"], "to": p, "since_utc": stamp})
                u["last_p"], u["first_seen_utc"] = p, stamp
                _trim(u)
            u.update(last_probe_utc=stamp, last_probe_ok=True, last_ok_probe_utc=stamp, cnc_days=0,
                     cnc_last_day=None, last_error=None)
        pending.pop(key, None)
        return rec
    err = str(outcome.get("error") or "probe: no token")[:ERROR_MAX]
    target = u if u is not None else pending.setdefault(key, {"cnc_days": 0, "cnc_last_day": None})
    if target.get("cnc_last_day") != today:
        target["cnc_days"] = int(target.get("cnc_days") or 0) + 1
        target["cnc_last_day"] = today
    target.update(last_probe_utc=stamp, last_error=err)
    if u is not None:
        u["last_probe_ok"] = False
    return rec


def write_atomic(path: str, obj) -> None:
    """A temporary file in the same directory, fsync, then os.replace (C4): a run killed at any point leaves either
    the old file or the new one, never a torn one."""
    d = os.path.dirname(path)
    os.makedirs(d, exist_ok=True)
    tmp = os.path.join(d, f".{os.path.basename(path)}.{os.getpid()}.tmp")
    data = json.dumps(obj, indent=1, sort_keys=True).encode("utf-8")
    try:
        with open(tmp, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        for attempt in range(10):
            try:
                os.replace(tmp, path)
                break
            except PermissionError:          # Windows: a reader (the guard tick) holds the file open for a moment
                if attempt == 9:
                    raise
                import time
                time.sleep(0.1 * (attempt + 1))
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


# ------------------------------------------------------------------ the child
_GUARD = {"tripped": [], "roots": (), "allowed": (), "writable": (), "listable": (), "ips": set(), "on": False,
          "audit": False}
# The hosts the phase-1 probe may reach: jobs/ingest_unctad_ds.META's host (a test pins the two together).
PROBE_HOSTS = frozenset({"unctadstat-api.unctad.org"})
# A phase-1 token is the publisher's `version|lastUpdated` (_unctad._token). Anything else from a child is refused
# as `shape`, so no bypass can carry file content out through P.
TOKEN_RE = r"^\d{1,9}\|\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,7})?Z?$"
_CODE_EXT = (".py", ".pyc", ".pyd", ".pyi", ".pth", ".dll", ".so")


def _norm(s: str) -> str:
    """The path Windows would actually open: no \\\\?\\ or \\\\.\\ prefix, no :stream suffix on the last part, no
    trailing dots or spaces (Win32 drops them)."""
    t = s.replace("/", "\\") if os.name == "nt" else s
    for pre in ("\\\\?\\UNC\\", "\\\\?\\", "\\\\.\\", "\\??\\"):
        if t.upper().startswith(pre.upper()):
            t = ("\\\\" if pre.upper().endswith("UNC\\") else "") + t[len(pre):]
            break
    head, _sep, base = t.replace("\\", "/").rpartition("/")
    if os.name == "nt" and ":" in base:
        base = base.split(":", 1)[0]
    base = base.rstrip(". ") if os.name == "nt" else base
    return (head + "/" + base) if _sep else base


def _secret_name(base: str) -> bool:
    """Credential-shaped file names (the shapes .gitignore keeps out of git): .env*, *.env, *.key, *_key, .*_key,
    .dev.vars, credentials*, *secret*, *.token, token.txt, *_key.hex. Code files are exempt (stdlib secrets.py)."""
    b = base.lower()
    if b.endswith(_CODE_EXT):
        return False
    return (b.startswith(".env") or b.endswith(".env") or b.endswith(".key") or b.endswith("_key")
            or b.endswith("_key.hex") or b == ".dev.vars" or b.startswith("credentials") or "secret" in b
            or b.endswith(".token") or b == "token.txt")


def _fs(p):
    if isinstance(p, int) or p is None:
        return None
    try:
        s = os.fspath(p)
    except TypeError:
        return None
    return os.fsdecode(s) if isinstance(s, bytes) else s


def _under(full: str, roots) -> bool:
    return any(full == r or full.startswith(r.rstrip("\\/") + os.sep) for r in roots)


def _refusal(p):
    """Why the sandbox refuses path `p` by NAME or PLACE, or None. Never touches the filesystem."""
    s = _fs(p)
    if s is None:                                       # a file descriptor: its path was checked when opened
        return None
    n = _norm(s)
    if _secret_name(os.path.basename(n.rstrip("/"))):
        return ".env" if os.path.basename(n.rstrip("/")).lower().find(".env") >= 0 else "credential-shaped name"
    full = os.path.normcase(os.path.abspath(n))
    if _under(full, _GUARD["roots"]):
        return "guarded path"
    return None


def _outside(p):
    """For an OPEN by the child: a path outside the read allow-list (the code, the interpreter, its site
    directories, the sandbox temp) is refused, whatever its name."""
    s = _fs(p)
    if s is None or not _GUARD["allowed"]:
        return None
    full = os.path.normcase(os.path.abspath(_norm(s)))
    return None if _under(full, _GUARD["allowed"]) else "outside the read allow-list"


def _writes(args) -> bool:
    """Does an `open` audit event (path, mode, flags) open for writing?"""
    mode = args[1] if len(args) > 1 else None
    flags = args[2] if len(args) > 2 else 0
    if isinstance(mode, str) and any(c in mode for c in "wax+"):
        return True
    try:
        return bool(int(flags or 0) & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_APPEND | os.O_TRUNC))
    except (TypeError, ValueError):
        return True


def _unlistable(p):
    """Listing a directory outside the read allow-list (plus the repo root itself, which import needs) is refused."""
    s = _fs(p)
    if s is None or not _GUARD["listable"]:
        return None
    full = os.path.normcase(os.path.abspath(_norm(s)))
    return None if (_under(full, _GUARD["listable"]) or full in _GUARD["listable"]) else "listing outside the allow-list"


def _unwritable(p):
    """A write, create, delete or rename outside the sandbox's own temp is refused."""
    s = _fs(p)
    if s is None:
        return None
    full = os.path.normcase(os.path.abspath(_norm(s)))
    return None if _under(full, _GUARD["writable"]) else "write outside the sandbox"


_AUDIT_PATH_EVENTS = {"os.listdir": 1, "os.scandir": 1, "os.mkdir": 1, "os.remove": 1, "os.rmdir": 1,
                      "os.rename": 2, "os.link": 2, "os.symlink": 2, "os.truncate": 1, "os.chmod": 1,
                      "shutil.copyfile": 2, "shutil.copytree": 2, "shutil.move": 2, "shutil.rmtree": 1,
                      "shutil.copymode": 2, "shutil.copystat": 2, "_winapi.CopyFile2": 2}
_READ_FIRST = {"shutil.copyfile", "shutil.copytree", "shutil.copymode", "shutil.copystat", "_winapi.CopyFile2"}
_AUDIT_REFUSED = ("subprocess.Popen", "os.system", "os.exec", "os.spawn", "os.posix_spawn", "os.startfile",
                  "os.fork", "os.forkpty", "pty.spawn", "_winapi.CreateProcess", "ctypes.dlsym",
                  "ctypes.call_function", "sqlite3.connect", "webbrowser.open", "winreg.CreateKey",
                  "winreg.SetValue", "winreg.DeleteKey")
_HTTP_OK = (b"GET ", b"HEAD ")


def _audit_reason(event: str, args) -> "str | None":
    """The refusal for one audit event, or None. PURE over _GUARD (tests call it with fake args)."""
    if event == "open":
        why = _refusal(args[0]) or _outside(args[0])
        if why is None and _writes(args) and _fs(args[0]) is not None:
            why = _unwritable(args[0])
        return why
    if event in _AUDIT_PATH_EVENTS:
        for a in args[:_AUDIT_PATH_EVENTS[event]]:
            why = _refusal(a)
            if why is None and event in ("os.listdir", "os.scandir"):
                why = _unlistable(a)
            elif why is None:
                why = _outside(a) if (event in _READ_FIRST and a is args[0]) else _unwritable(a)
            if why:
                return why
        return None
    if event in ("socket.connect", "socket.sendto", "socket.sendmsg"):
        addr = args[1] if len(args) > 1 else None
        host = addr[0] if isinstance(addr, tuple) and addr else None
        # only an address that a lookup of a PROBE_HOSTS name returned (install_audit_guard's getaddrinfo wrapper)
        return None if host in _GUARD["ips"] else "a connection to an address no probe host resolved to"
    if event == "socket.bind":
        # urllib3 tests for IPv6 at import by binding ('::1', 0); a loopback bind sends nothing. Anything else is
        # refused, and connect() above still needs a probe host's address.
        addr = args[1] if len(args) > 1 else None
        if isinstance(addr, tuple) and len(addr) >= 2 and addr[0] in ("::1", "127.0.0.1") and addr[1] == 0:
            return None
        return "binding a socket is not allowed in a probe"
    if event in _AUDIT_REFUSED:
        return "not allowed in a probe"
    if event == "ctypes.dlopen":
        # dlopen(NULL) is how `import ctypes` binds pythonapi on POSIX; any NAMED library is refused, and every
        # symbol lookup (ctypes.dlsym) and foreign call (ctypes.call_function) is refused above.
        return None if (not args or args[0] is None) else "loading a native library is not allowed in a probe"
    if event in ("socket.getaddrinfo", "socket.gethostbyname", "socket.gethostbyname_ex"):
        # never echo the name: a refusal text must carry nothing the child chose
        return None if str(args[0]).lower().rstrip(".") in PROBE_HOSTS else "a host that is not a probe host"
    if event == "http.client.send":
        conn, data = args[0], args[1]
        host = str(getattr(conn, "host", "")).lower().rstrip(".")
        if host not in PROBE_HOSTS:
            return "http to a host that is not a probe host"
        head = bytes(data[:8]) if isinstance(data, (bytes, bytearray, memoryview)) else b""
        if head and head[:1].isupper() and not head.startswith(_HTTP_OK) and b" " in head \
                and head.split(b" ", 1)[0].isalpha() and head.split(b" ", 1)[0].isupper():
            return "an http method other than GET or HEAD"
        return None
    if event == "urllib.Request":
        _url, data, _h, method = (list(args) + [None] * 4)[:4]
        if data is not None or (method or "GET").upper() not in ("GET", "HEAD"):
            return "urllib non-GET not allowed"
        return None
    return None


def _audit(event, args):
    if not _GUARD["audit"]:
        return
    why = _audit_reason(event, args)
    if why:
        _GUARD["audit"] = False                         # the refusal path itself must not re-enter
        try:
            _GUARD["tripped"].append(f"{event} refused ({why})")
        finally:
            _GUARD["audit"] = True
        raise WatchSideEffect(f"{event} refused ({why})")


def install_audit_guard(roots, allowed, writable, listable=()) -> None:
    """FIRST thing in the child (before the site directories, so no .pth line runs unguarded). An audit hook
    cannot be removed: it covers routes the attribute patches below cannot see (_io.open, io.FileIO, os.open
    by any name, \\\\?\\ paths, subprocess, shutil's native copy, ctypes, urllib, raw http.client)."""
    _GUARD["roots"] = tuple(os.path.normcase(os.path.abspath(r)) for r in roots)
    _GUARD["allowed"] = tuple(os.path.normcase(os.path.abspath(r)) for r in allowed if r)
    _GUARD["writable"] = tuple(os.path.normcase(os.path.abspath(r)) for r in writable if r)
    _GUARD["listable"] = _GUARD["allowed"] + tuple(os.path.normcase(os.path.abspath(r)) for r in listable if r)
    import socket
    orig = socket.getaddrinfo

    def getaddrinfo(host, *a, **k):
        res = orig(host, *a, **k)                       # the audit event refuses any other host before this
        if str(host).lower().rstrip(".") in PROBE_HOSTS:
            _GUARD["ips"].update(r[4][0] for r in res if r and len(r) > 4 and r[4])
        return res
    socket.getaddrinfo = getaddrinfo
    sys.addaudithook(_audit)
    _GUARD["audit"] = True


def _trip(what: str):
    _GUARD["tripped"].append(what)
    raise WatchSideEffect(what)


def _path_guard(fn, name, argn=1):
    def wrapped(*a, **k):
        if _GUARD["on"]:
            for x in a[:argn]:
                why = _refusal(x)
                if why:
                    _trip(f"{name} refused ({why})")
        return fn(*a, **k)
    wrapped.__name__ = getattr(fn, "__name__", name)
    wrapped._release_watch_guard = True
    return wrapped


def _refuse(name):
    def wrapped(*a, **k):
        _trip(f"{name} refused")
    wrapped._release_watch_guard = True
    return wrapped


def install_file_guards(roots) -> None:
    """Guards on every listed file route. `roots`: the directories under which nothing may be touched."""
    import builtins
    import io
    import pathlib
    _GUARD["roots"] = tuple(os.path.normcase(os.path.abspath(r)) for r in roots)
    builtins.open = _path_guard(builtins.open, "open")
    io.open = _path_guard(io.open, "io.open")
    for name in ("open", "scandir", "listdir", "stat", "lstat", "access", "mkdir", "makedirs", "remove", "unlink",
                 "rmdir"):
        if hasattr(os, name):
            setattr(os, name, _path_guard(getattr(os, name), f"os.{name}"))
    for name in ("rename", "replace"):
        setattr(os, name, _path_guard(getattr(os, name), f"os.{name}", argn=2))
    for name in ("exists", "isfile", "isdir", "lexists"):
        setattr(os.path, name, _path_guard(getattr(os.path, name), f"os.path.{name}"))
    for name in ("open", "read_text", "read_bytes", "write_text", "write_bytes", "exists", "stat", "is_file",
                 "is_dir", "iterdir"):
        if hasattr(pathlib.Path, name):
            setattr(pathlib.Path, name, _path_guard(getattr(pathlib.Path, name), f"Path.{name}"))
    _GUARD["on"] = True


def install_store_guards() -> None:
    """Every updater.blob read and write, merge.merge_and_write and sqlite3.connect raise. Imported here, AFTER the
    env is set (config reads it once at import) and before any fetcher is imported."""
    import inspect
    import sqlite3
    from updater import blob, merge
    for name, obj in list(vars(blob).items()):
        if name.startswith("__"):
            continue
        if inspect.isfunction(obj) and obj.__module__ == blob.__name__:
            setattr(blob, name, _refuse(f"blob.{name}"))
        elif inspect.isclass(obj) and obj.__module__ == blob.__name__:
            for m, f in list(vars(obj).items()):
                if inspect.isfunction(f) and not m.startswith("__"):
                    setattr(obj, m, _refuse(f"blob.{name}.{m}"))
            obj.__init__ = _refuse(f"blob.{name}")
    merge.merge_and_write = _refuse("merge.merge_and_write")
    sqlite3.connect = _refuse("sqlite3.connect")
    sqlite3.dbapi2.connect = sqlite3.connect


def _distrust(session) -> None:
    """No netrc, no proxy env, no CA-bundle env: requests reads all three only when trust_env is True."""
    session.trust_env = False


def install_http_guards() -> None:
    """GET and HEAD only, and never trust the environment (netrc, proxies, CA bundle)."""
    import requests
    import requests.api
    import requests.sessions

    Session = requests.sessions.Session
    orig_init, orig_request, orig_send = Session.__init__, Session.request, Session.send

    def init(self, *a, **k):
        orig_init(self, *a, **k)
        _distrust(self)

    def request(self, method, url, *a, **k):
        if str(method).upper() not in ("GET", "HEAD"):
            _trip(f"requests {str(method).upper()} refused")
        _distrust(self)
        return orig_request(self, method, url, *a, **k)

    def send(self, req, **k):
        if str(getattr(req, "method", "")).upper() not in ("GET", "HEAD"):
            _trip(f"requests {getattr(req, 'method', '?')} refused")
        _distrust(self)
        return orig_send(self, req, **k)

    Session.__init__, Session.request, Session.send = init, request, send

    def via_session(method):
        def call(url, **kwargs):
            with Session() as s:
                return s.request(method=method, url=url, **kwargs)
        return call

    for mod in (requests, requests.api):
        mod.get, mod.head = via_session("GET"), via_session("HEAD")
        for m in ("post", "put", "patch", "delete", "options"):
            setattr(mod, m, _refuse(f"requests.{m}"))
        mod.request = lambda method, url, **kw: via_session(str(method).upper())(url, **kw)


def _production_probe(sid: str, uid: str):
    """The probe exactly as production runs it (R3.8): the unit from registry.to_units, strategies.get(...),
    detect_change(u, None), then the token the strategy read."""
    from updater import registry, strategies
    entries = {e["source_id"]: e for e in registry.load().get("sources", [])}
    if sid not in entries:
        raise LookupError(f"{sid} has no registry entry")
    units = [u for u in registry.to_units(entries[sid]) if u.unit_id == uid]
    if not units:
        raise LookupError(f"{sid} has no unit {uid}")
    strat = strategies.get(units[0].strategy)
    strat.detect_change(units[0], None)
    return getattr(strat, "_cur", None)


def child_main(argv=None) -> int:
    """argv: code_root guard_root sid uid site_dirs_json [stub_dir stub_module]. Prints ONE JSON line."""
    argv = list(sys.argv[1:] if argv is None else argv)
    real_stdout = sys.stdout
    out = {"p": None, "error": None}
    try:
        code_root, guard_root, sid, uid, site_json = argv[:5]
        stub = argv[5:7] if len(argv) >= 7 else None
        if (sid, uid) not in PHASE1_UNITS:
            raise LookupError(f"{sid}/{uid} is not a phase-1 unit")
        sites = json.loads(site_json)
        roots = [os.path.join(guard_root, "data")]
        roots += [os.environ[k] for k in ("AQUEDUCT_DATA_ROOT", "AQUEDUCT_STATE_DIR") if os.environ.get(k)]
        sandbox = os.path.dirname(os.path.abspath(os.environ["HOME"]))       # run_child's relw_ temp dir
        allowed = [os.path.join(code_root, d) for d in ("updater", "jobs", "core")]
        allowed += [sys.base_prefix, sys.prefix, sandbox] + list(sites) + ([stub[0]] if stub else [])
        # stdlib ctypes binds kernel32 when it is imported (Windows); something in the probe chain imports it. Bound
        # here, before the hook, so the hook can refuse every NEW library, symbol and foreign call afterwards.
        import ctypes                                   # noqa: F401
        install_audit_guard(roots, allowed, [sandbox], [code_root])  # FIRST: before any .pth line or import
        import site
        for d in sites:
            site.addsitedir(d)
        install_file_guards(roots)
        sys.stdout = sys.stderr                     # a fetcher's print must not break the one-line protocol
        install_store_guards()
        install_http_guards()
        if stub:
            sys.path.insert(0, stub[0])
            import importlib
            p = importlib.import_module(stub[1]).probe(sid, uid)
        else:
            p = _production_probe(sid, uid)
        if _GUARD["tripped"]:
            out["error"] = f"WatchSideEffect: {_GUARD['tripped'][0]}"
        elif isinstance(p, str) and p:
            out["p"] = p
        else:
            out["error"] = "none: the probe returned no token"
    except BaseException as e:                       # noqa: BLE001 - one JSON line whatever happened
        kind = "WatchSideEffect" if (_GUARD["tripped"] or isinstance(e, WatchSideEffect)) else type(e).__name__
        text = _GUARD["tripped"][0] if _GUARD["tripped"] else str(e)
        out = {"p": None, "error": f"{kind}: {text}"[:400]}
    sys.stdout = real_stdout
    real_stdout.write(json.dumps(out, ensure_ascii=True) + "\n")
    real_stdout.flush()
    return 0


CHILD_BOOT = "import sys; sys.path.insert(0, sys.argv[1]); import updater.release_watch as w; w.child_main()"


def _token_ok(p, token_re) -> bool:
    import re
    return token_re is None or bool(re.fullmatch(token_re, p))
_ENV_PASS = ("PATH", "SYSTEMROOT", "TEMP", "TMP")


def child_env(sandbox: str, parent_env=None) -> dict:
    """The child's WHOLE environment, from an allow-list (C2). Nothing else is inherited."""
    src = os.environ if parent_env is None else parent_env
    up = {k.upper(): v for k, v in src.items()}
    env = {k: up[k] for k in _ENV_PASS if k in up}
    data, state, home = (os.path.join(sandbox, n) for n in ("data_root", "state_dir", "home"))
    for d in (data, state, home):
        os.makedirs(d, exist_ok=True)
    env.update(AQUEDUCT_BACKEND="local", AQUEDUCT_DATA_ROOT=data, AQUEDUCT_STATE_DIR=state,
               HOME=home, USERPROFILE=home, APPDATA=home, LOCALAPPDATA=home,
               # '<' and '>' are not legal in a Windows file name, and nothing creates this path elsewhere
               ECONDL_CATALOG=os.path.join(sandbox, "no<such>catalog", "catalog.db"))
    return env


def _site_dirs() -> list:
    return [p for p in sys.path if p and os.path.basename(p.rstrip("\\/")).lower() in ("site-packages",
                                                                                      "dist-packages")]


_ERR_TYPES = frozenset({
    "none", "HTTPError", "ConnectionError", "ConnectTimeout", "ReadTimeout", "Timeout", "SSLError", "ProxyError",
    "ChunkedEncodingError", "ContentDecodingError", "JSONDecodeError", "RequestException", "TooManyRedirects",
    "ValueError", "KeyError", "TypeError", "LookupError", "RuntimeError", "OSError", "SystemExit", "ImportError",
    "ModuleNotFoundError", "AttributeError", "UnsupportedLayout"})


def classify(rc, stdout: str, timed_out: bool, token_re=TOKEN_RE) -> dict:
    """{"p": str|None, "kind": ..., "error": str|None} from one child's exit. Only a clean, single JSON line with a
    string p is success."""
    if timed_out:
        return {"p": None, "kind": "timeout", "error": f"timeout: no answer in {CHILD_TIMEOUT_S} s"}
    if rc != 0:
        return {"p": None, "kind": "exit", "error": f"exit: the probe child exited {rc}"}
    lines = [l for l in (stdout or "").splitlines() if l.strip()]
    try:
        if len(lines) != 1:
            raise ValueError(f"{len(lines)} output lines")
        d = json.loads(lines[0])
        if not isinstance(d, dict) or set(d) != {"p", "error"}:
            raise ValueError("not the {p, error} object")
    except ValueError as e:
        return {"p": None, "kind": "protocol", "error": f"protocol: {str(e)[:100]}"}
    p, err = d["p"], d["error"]
    if err or not (isinstance(p, str) and p):
        # THE CHILD'S TEXT NEVER TRAVELS. `error` is free text a compromised probe could fill with what it read (a
        # directory listing came out this way in review), so what is recorded, beaten and mailed is a FIXED phrase
        # plus an exception type from a closed list. The raw text stays in `detail`, which only --probe prints.
        raw = str(err or "none: the probe returned no token")
        if raw.startswith("WatchSideEffect"):
            kind, fixed = "sandbox", "sandbox: a guarded route was refused (run --probe by hand for the route)"
        else:
            m = re.match(r"[A-Za-z_][A-Za-z0-9_]{0,40}", raw)
            t = m.group(0) if m and m.group(0) in _ERR_TYPES else "another error"
            kind, fixed = "probe", f"probe: {t}"
        return {"p": None, "kind": kind, "error": fixed, "detail": raw[:400]}
    if not _token_ok(p, token_re):
        return {"p": None, "kind": "shape", "error": "shape: the child's token is not a version|lastUpdated token"}
    return {"p": p, "kind": "ok", "error": None}


def run_child(sid: str, uid: str, *, code_root: str = None, guard_root: str = None, stub=None,
              timeout: float = CHILD_TIMEOUT_S, parent_env=None, token_re=TOKEN_RE) -> dict:
    """One sandboxed probe. `stub` (dir, module) replaces the production probe and `token_re=None` lifts the
    token-shape check: tests only."""
    import shutil
    import subprocess
    import tempfile
    code_root = code_root or ROOT
    sandbox = tempfile.mkdtemp(prefix="relw_")
    try:
        argv = [sys.executable, "-I", "-B", "-c", CHILD_BOOT, code_root, guard_root or code_root, sid, uid,
                json.dumps(_site_dirs())] + (list(stub) if stub else [])
        try:
            r = subprocess.run(argv, env=child_env(sandbox, parent_env), cwd=code_root, capture_output=True,
                               text=True, encoding="utf-8", errors="replace", timeout=timeout)
        except subprocess.TimeoutExpired:
            return classify(None, "", True, token_re)
        return classify(r.returncode, r.stdout, False, token_re)
    finally:
        shutil.rmtree(sandbox, ignore_errors=True)


# ------------------------------------------------------------------ the desktop driver
def _scrubber():
    """The stuck alarm's scrubber over the committed denylist (fails closed when it cannot be read)."""
    from updater.health import _stuck_scrubber, gated_ids_or_none   # noqa: PLC0415
    return _stuck_scrubber(gated_ids_or_none())


def desktop_main(now_fn=utcnow, probe=run_child, record_path: str = None) -> int:
    record_path = record_path or RECORD_PATH
    started = now_fn()
    try:
        rec = load_record(record_path)
    except RecordError as e:
        print(f"release watch: {record_path} is unreadable ({e}); it is KEPT unchanged and nothing is probed - "
              f"repair or move it by hand", flush=True)
        return 2
    scrub = _scrubber()
    bootstrap = rec is None
    got = []
    for sid, uid in PHASE1_UNITS:
        o = probe(sid, uid)
        o = dict(o, error=scrub(o.get("error")) if o.get("error") else None)
        print(f"release watch: {unit_key(sid, uid)}: " + (f"P={o['p']}" if o.get("p") else
                                                         f"COULD NOT CHECK ({o['error']})"), flush=True)
        got.append((sid, uid, o, now_fn()))
    real = sum(1 for _s, _u, o, _t in got if isinstance(o.get("p"), str) and o["p"])
    # Folded after every probe has answered, so on the bootstrap day a unit probed BEFORE the first success still
    # has its failure counted (under `pending`). With no real P at all, nothing is created (C8 / R3.1).
    if rec is None and real:
        rec = {"schema": SCHEMA, "run": {}, "units": {}, "pending": {}}
    for sid, uid, o, t in got:
        rec = apply_outcome(rec, sid, uid, o, t)
    # NO REAL TOKEN FROM ANY UNIT is a failed run (rc 3), not an ok one: the runner then retries in 2 h, keeps the
    # old last_ok_started, and the digest says "last run exited 3" and, after 30 h, LATE. Without this a sandbox
    # that refuses every probe on day 1 read as "no record (no real probe yet)" for ever.
    rc = 0 if real else 3
    if rec is None:
        print("release watch: no real token from any unit - no record is created (a failure never starts one); "
              "exit 3", flush=True)
        return rc
    rec["run"] = {"started_utc": iso(started), "ended_utc": iso(now_fn()), "rc": rc,
                  "last_ok_started_utc": iso(started) if rc == 0 else (rec.get("run") or {}).get(
                      "last_ok_started_utc")}
    write_atomic(record_path, rec)
    if bootstrap:
        print(f"release watch: BOOTSTRAP - {record_path} created ({len(rec['units'])} unit(s) with a real token)",
              flush=True)
    if rc:
        print("release watch: no real token from any unit this run; exit 3", flush=True)
    return rc


def probe_main(sid: str, uid: str) -> int:
    o = run_child(sid, uid)
    s = _scrubber()
    # by hand only: the operator also sees the child's own `detail` (scrubbed); the record never holds it
    o = dict(o, error=s(o.get("error")) if o.get("error") else None,
             **({"detail": s(o["detail"])} if o.get("detail") else {}))
    print(json.dumps(o, indent=1))
    return 0 if o.get("p") else 1


# ------------------------------------------------------------------ the beat (desktop, every guard tick)
def _read_status(path: str):
    try:
        with open(path, encoding="utf-8-sig") as fh:
            s = json.load(fh)
    except FileNotFoundError:
        return None
    except Exception as e:                               # noqa: BLE001
        return {"unreadable": f"{type(e).__name__}: {e}"[:200]}
    if not isinstance(s, dict):
        return {"unreadable": "not a JSON object"}
    return {"started_utc": s.get("started"), "ended_utc": s.get("ended"), "rc": s.get("rc"),
            "last_ok_started_utc": s.get("last_ok_started"), "note": (str(s["note"])[:ERROR_MAX]
                                                                       if s.get("note") else None)}


def _gated_from_denylist():
    """Lower-case gated ids via core.gen_denylist.committed_gate (the digest's reader), or None."""
    try:
        if ROOT not in sys.path:
            sys.path.insert(0, ROOT)
        from core.gen_denylist import committed_gate    # noqa: PLC0415
        return {s.lower() for s in committed_gate()}
    except Exception:                                    # noqa: BLE001
        return None


def _t(s, n=TOKEN_MAX):
    return None if s is None else str(s)[:n]


def beat_value(record_path: str = None, status_path: str = None, gated_reader=None, scrubber_for=None) -> dict:
    """body["release_watch"]: the scrubbed record plus the runner's status. RAISES on a corrupt record; the caller
    (tools/guard_heartbeat._release_watch_beat) turns any fault into {"unreadable": ...}. Never a partial record:
    over BEAT_MAX_BYTES it is {"unreadable": "record over size cap"}."""
    rec = load_record(record_path or RECORD_PATH)
    run = _read_status(status_path or STATUS_PATH)
    if run is None and rec is not None:
        run = rec.get("run") or None
    gated = (gated_reader or _gated_from_denylist)()
    if gated is None:                                    # first scrub pass (C7): names nothing at all
        n = len((rec or {}).get("units") or {}) + len((rec or {}).get("pending") or {})
        return {"schema": SCHEMA, "run": run, "units_n": n, "names": "withheld: denylist unreadable"}
    if scrubber_for is None:
        from updater.health import _stuck_scrubber      # noqa: PLC0415
        scrubber_for = _stuck_scrubber
    scrub = scrubber_for(gated)

    def keep(key):
        return str(key).split("/", 1)[0].lower() not in gated

    units = {}
    for key, u in ((rec or {}).get("units") or {}).items():
        if not keep(key):
            continue
        ev = list(u.get("events") or [])
        trimmed = bool(u.get("trimmed")) or len(ev) > EVENTS_MAX
        if len(ev) > EVENTS_MAX:
            ev = [ev[0]] + ev[-(EVENTS_MAX - 1):]
        units[key] = {"last_p": _t(u.get("last_p")), "first_seen_utc": u.get("first_seen_utc"),
                      "last_probe_utc": u.get("last_probe_utc"), "last_probe_ok": u.get("last_probe_ok") is True,
                      "last_ok_probe_utc": u.get("last_ok_probe_utc"), "cnc_days": int(u.get("cnc_days") or 0),
                      "last_error": scrub(_t(u.get("last_error"), ERROR_MAX)), "trimmed": trimmed,
                      "events": [dict({"to": _t(e.get("to")), "from": _t(e.get("from")),
                                       "since_utc": e.get("since_utc")}, **({"initial": True} if e.get("initial")
                                                                            else {}))
                                 for e in ev]}
    pending = {k: {"cnc_days": int(v.get("cnc_days") or 0), "last_probe_utc": v.get("last_probe_utc"),
                   "last_error": scrub(_t(v.get("last_error"), ERROR_MAX))}
               for k, v in ((rec or {}).get("pending") or {}).items() if keep(k) and isinstance(v, dict)}
    out = {"schema": SCHEMA, "run": run, "units": units, "pending": pending}
    if len(json.dumps(out, separators=(",", ":"))) > BEAT_MAX_BYTES:
        return {"unreadable": "record over size cap"}
    return out


# ------------------------------------------------------------------ the digest (CI)
def store_tokens(rows) -> dict:
    """{(sid, uid): T} from unit_state rows (source_id, unit_id, upstream_vintage). NULL, '' and the placeholders
    `date-tail` / `force` are not a token (R2.2)."""
    out = {}
    for r in rows or ():
        sid, uid, v = r[0], r[1], r[2]
        if isinstance(v, str) and v.strip() and v.strip() not in NOT_A_TOKEN:
            out[(str(sid), str(uid))] = v
    return out


def read_store_tokens(state_path: str) -> list:
    """unit_state rows for the phase-1 units over a READ-ONLY connection (mode=ro never creates a file)."""
    import sqlite3                                       # noqa: PLC0415
    if not os.path.isfile(state_path):
        raise FileNotFoundError(state_path)
    con = sqlite3.connect("file:" + str(state_path).replace("\\", "/") + "?mode=ro", uri=True)
    try:
        q = ",".join("?" for _ in PHASE1_UNITS)
        return con.execute(f"SELECT source_id, unit_id, upstream_vintage FROM unit_state WHERE source_id IN ({q})",
                           [s for s, _u in PHASE1_UNITS]).fetchall()
    finally:
        con.close()


def load_acks_doc(path: str = None):
    try:
        import yaml                                      # noqa: PLC0415
        with open(path or ACKS_PATH, encoding="utf-8") as fh:
            return yaml.safe_load(fh)
    except Exception as e:                               # noqa: BLE001
        return e


def _ack_date(v) -> _dt.date:
    if isinstance(v, _dt.datetime):
        return v.date()
    if isinstance(v, _dt.date):
        return v
    return _dt.date.fromisoformat(str(v).strip()[:10])


def validate_acks(doc, today: _dt.date):
    """([ack], None) or ([], why). ONE bad entry voids the WHOLE file (R3.2): every line then shows."""
    if isinstance(doc, BaseException):
        return [], f"could not be read ({type(doc).__name__})"
    if doc is None:
        return [], None
    if not isinstance(doc, dict) or set(doc) - {"acks"}:
        return [], "the top level must be a mapping whose only key is `acks`"
    items = doc.get("acks")
    if items is None:
        return [], None
    if not isinstance(items, list):
        return [], "`acks` must be a list"
    out = []
    for i, a in enumerate(items, 1):
        where = f"ack #{i}"
        if not isinstance(a, dict):
            return [], f"{where} is not a mapping"
        if set(a) - _ACK_KEYS:
            return [], f"{where} has unknown key(s) {sorted(set(a) - _ACK_KEYS)}"
        for k in _ACK_REQUIRED:
            if a.get(k) is None or not str(a.get(k)).strip():
                return [], f"{where} has no {k}"
        if not str(a["reason_url"]).strip().startswith(("http://", "https://")):
            return [], f"{where}: reason_url is not an http(s) URL"
        try:
            acked, expires = _ack_date(a["acked_utc"]), _ack_date(a["expires_utc"])
        except (TypeError, ValueError):
            return [], f"{where}: acked_utc / expires_utc is not a date"
        if acked > today:
            return [], f"{where}: acked_utc {acked} is in the future"
        if expires < acked or (expires - acked).days > ACK_MAX_DAYS:
            return [], f"{where}: expires_utc {expires} is more than {ACK_MAX_DAYS} days after acked_utc"
        out.append({"source_id": str(a["source_id"]), "unit_id": str(a["unit_id"]),
                    "to_token": str(a["to_token"]), "expires": expires})
    return out, None


def open_move(events, T, trimmed: bool):
    """The open PUBLISHER MOVED run as {from, to, since, trimmed}, or None.

    Open = the newest event's `to` != T, and at least one non-initial event after the newest event with to == T
    (or any non-initial event when T is absent). Its since-date is that of the open run's FIRST event. P equal to
    the previous P changes nothing (no event is ever appended for it), so a move stays open until T catches up."""
    ev = [e for e in (events or []) if isinstance(e, dict)]
    if not ev:
        return None
    newest = ev[-1]
    if T is not None and newest.get("to") == T:
        return None                                      # cleared: the store stamps the latest token
    start, hits = 0, []
    if T is not None:
        hits = [i for i, e in enumerate(ev) if e.get("to") == T]
        if hits:
            start = hits[-1] + 1
    run = [e for e in ev[start:] if not e.get("initial")]
    if not run:
        return None
    since = run[0].get("since_utc")
    if trimmed and not hits:
        # the open run may have begun inside the trimmed stretch: the oldest KEPT event is the only bound we hold
        since = ev[0].get("since_utc")
    return {"from": run[0].get("from"), "to": newest.get("to"), "since": since,
            "trimmed": bool(trimmed),
            "t_unseen": T is not None and not hits and not trimmed}


def digest_view(beat, unit_rows, acks_doc, gated, now: _dt.datetime, beat_old_h: float, run_late_h: float) -> dict:
    """{"lines": [...], "suffix": {(sid, uid): "release: ..."}} - the whole release section, PURE.

    beat       the guard beat (dict, or {"unreadable": why})
    unit_rows  unit_state rows (source_id, unit_id, upstream_vintage) from CI's state.db, or an exception
    gated      lower-case denylist ids, or None (then the section names nothing: counts only)
    """
    def section(lines):
        return {"lines": ["  release watch (publisher releases, phase 1: the unctad trio):"] + lines + [""],
                "suffix": {}}

    from updater.health import _stuck_scrubber           # noqa: PLC0415 - the stuck alarm's scrubber
    scrub = _stuck_scrubber(None if gated is None else set(gated))   # None: every free text is withheld
    if not isinstance(beat, dict) or "unreadable" in beat:
        why = beat.get("unreadable") if isinstance(beat, dict) else "no beat"
        return section([f"  release watch: could not read the workstation beat ({scrub(str(why)[:120])})"])
    rw = beat.get("release_watch")
    if rw is None:
        return section(["  release watch: not running on the workstation"])
    if not isinstance(rw, dict) or "unreadable" in rw:
        why = rw.get("unreadable") if isinstance(rw, dict) else "not an object"
        return section([f"  release watch: could not read the workstation record ({scrub(str(why)[:120])})"])
    b = parse_utc(beat.get("utc"))
    age_h = (now - b).total_seconds() / 3600 if b else None
    stale = age_h is None or age_h > beat_old_h
    lines = []
    if stale:
        lines.append("  release watch: workstation beat is " + ("undated" if age_h is None else f"{age_h:.0f} h old")
                     + " - publisher releases unknown")
    run = rw.get("run") if isinstance(rw.get("run"), dict) else None
    if not stale:                                        # LATE is judged only from a fresh beat (C1)
        if run and run.get("unreadable"):
            lines.append(f"  release watch LATE (the runner's status file is unreadable: "
                         f"{scrub(str(run['unreadable'])[:100])})")
        else:
            ok = parse_utc((run or {}).get("last_ok_started_utc"))
            rc = (run or {}).get("rc")
            tail = "" if rc in (0, None) else f" last run exited {rc}"
            if ok is None or (now - ok).total_seconds() > run_late_h * 3600:
                lines.append(f"  release watch LATE (last ok run {(run or {}).get('last_ok_started_utc') or 'none'})"
                             + tail)
            elif tail:
                lines.append(f"  release watch:{tail} (last ok run {run.get('last_ok_started_utc')})")
    if gated is None or "names" in rw:                   # second scrub pass (C7): name nothing
        # the phase-1 count, less any unit the digest's denylist gates (never the beat's units_n, which a desktop
        # with an unreadable denylist counted with gated units in it - R888)
        n = len([u for u in PHASE1_UNITS if gated is None or u[0].lower() not in gated])
        lines.append(f"  release watch: units {n}; {WITHHELD_NO_GATE}")
        return section(lines)
    if isinstance(unit_rows, BaseException):
        lines.append(f"  release watch: the store's tokens could not be read ({type(unit_rows).__name__}) - every "
                     f"recorded move shows, nothing is judged cleared or NEW")
        tokens = {}
    else:
        tokens = store_tokens(unit_rows)
    acks, acks_err = validate_acks(acks_doc, now.date())
    if acks_err:
        lines.append(f"  release_acks.yaml IGNORED: {acks_err} - every line shows")
    units, pending = rw.get("units") or {}, rw.get("pending") or {}
    counts = {"same": 0, "new": 0, "moved-open": 0, "acknowledged": 0, "no record": 0, "could not check": 0}
    if stale:
        counts["unknown (stale beat)"] = 0
    longest, standalone, acked_lines, used, suffix, no_token = 0, [], [], set(), {}, 0
    for sid, uid in PHASE1_UNITS:
        if sid.lower() in gated:
            continue                                     # neither named nor counted (R888)
        key = unit_key(sid, uid)
        name = f"{sid}/{scrub(uid)}"
        u = units.get(key)
        T = tokens.get((sid, uid))
        if T is not None:
            T = T[:TOKEN_MAX]                            # the beat carries tokens cut to TOKEN_MAX: compare like that
        if not isinstance(u, dict):
            pend = pending.get(key) if isinstance(pending.get(key), dict) else None
            counts["no record"] += 1
            if pend and pend.get("cnc_days"):
                n = int(pend["cnc_days"])
                longest = max(longest, n)
                why = f"no real probe yet, could not check x{n} d: {scrub(pend.get('last_error'))}"
                if n >= CNC_STANDALONE_DAYS:
                    standalone.append(f"  release watch COULD NOT CHECK {name} x{n} d (never a real probe; "
                                      f"{scrub(pend.get('last_error'))})")
            else:
                why = "no real probe yet"
            suffix[(sid, uid)] = f"release: no record ({why})"
            continue
        last_p = scrub(u.get("last_p"))
        mv = open_move(u.get("events"), None if isinstance(unit_rows, BaseException) else T, u.get("trimmed"))
        cnc = int(u.get("cnc_days") or 0)
        if u.get("last_probe_ok") is not True:
            longest = max(longest, cnc)
        if mv:
            when = (f"since {_day(mv['since'])}" + (" (history trimmed)" if mv["trimmed"] else ""))
            text = f"PUBLISHER MOVED {scrub(mv['from'])} -> {scrub(mv['to'])} {when}"
            if T is not None:
                text += f"; store stamped {scrub(T)}"
                if mv.get("t_unseen"):
                    text += " (store token predates the record)"
            ack = next((a for i, a in enumerate(acks) if a["source_id"] == sid and a["unit_id"] == uid
                        and a["to_token"][:TOKEN_MAX] == mv["to"]), None)
            if ack is not None:
                used.add(id(ack))
            if ack is not None and now.date() <= ack["expires"]:
                counts["acknowledged"] += 1
                acked_lines.append(f"     {name}: {text} - acknowledged (expires {ack['expires']})")
                suffix[(sid, uid)] = f"release: {text} (acknowledged, expires {ack['expires']})"
            else:
                counts["moved-open"] += 1
                exp = f" - ACK EXPIRED {ack['expires']}" if ack is not None else ""
                standalone.append(f"  release watch {name}: {text}{exp}")
                suffix[(sid, uid)] = f"release: {text}"
        elif T is not None and T != str(u.get("last_p"))[:TOKEN_MAX]:
            counts["new"] += 1
            text = f"NEW RELEASE: publisher at {last_p}, store stamped {scrub(T)}"
            standalone.append(f"  release watch {name}: {text}")
            suffix[(sid, uid)] = f"release: {text}"
        elif stale:                                      # "same" is never judged from a stale beat (C1)
            counts["unknown (stale beat)"] += 1
            suffix[(sid, uid)] = "release: unknown (workstation beat stale)"
        elif u.get("last_probe_ok") is True:
            counts["same"] += 1
            suffix[(sid, uid)] = f"release: same as of {_day(u.get('last_probe_utc'))}"
            if T is None:
                # R2.2 rule 2: with no store token, "same" says only that the PUBLISHER has not moved since the
                # watch's first look, never that the store holds this release. Said on the line and counted.
                no_token += 1
                suffix[(sid, uid)] += " (publisher unchanged; store token not recorded)"
        else:
            counts["could not check"] += 1
            suffix[(sid, uid)] = (f"release: could not check x{cnc} d (last real probe "
                                  f"{_day(u.get('last_ok_probe_utc'))})")
        if u.get("last_probe_ok") is not True and cnc >= CNC_STANDALONE_DAYS:
            standalone.append(f"  release watch COULD NOT CHECK {name} x{cnc} d (last real probe "
                              f"{_day(u.get('last_ok_probe_utc'))}; {scrub(u.get('last_error'))})")
    n = sum(counts.values())
    lines.append(f"  release watch: units {n}, " + ", ".join(f"{k} {v}" for k, v in counts.items())
                 + f" (longest could-not-check streak {longest} d)"
                 + (f"; no store token on record for {no_token} of the same" if no_token else ""))
    lines += standalone
    if acked_lines:
        lines.append("  release watch, acknowledged (moved here, not hidden; updater/release_acks.yaml):")
        lines += acked_lines
    for a in acks:
        if id(a) not in used and a["source_id"].lower() not in gated:
            lines.append(f"  stale release ack - remove it: {a['source_id']}/{scrub(a['unit_id'])} "
                         f"to_token={scrub(a['to_token'])} matches no open move")
    out = section(lines)
    out["suffix"] = suffix
    return out


def main(argv=None) -> int:
    import argparse
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--desktop", action="store_true", help="probe every phase-1 unit and update the record")
    g.add_argument("--probe", nargs=2, metavar=("SOURCE_ID", "UNIT_ID"), help="one sandboxed probe, nothing written")
    a = ap.parse_args(argv)
    if a.probe:
        if tuple(a.probe) not in PHASE1_UNITS:
            print(f"refused: {a.probe[0]}/{a.probe[1]} is not a phase-1 unit ({', '.join(unit_key(*u) for u in PHASE1_UNITS)})")
            return 2
        return probe_main(*a.probe)
    return desktop_main()


if __name__ == "__main__":
    sys.exit(main())
