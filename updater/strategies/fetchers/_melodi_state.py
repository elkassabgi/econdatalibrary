"""State, clock and store plumbing for the insee_melodi fetcher's resumable re-pull (AR-330).

Private to fetchers/insee_melodi.py. Nothing here talks to INSEE.

WHAT LIVES WHERE (all through updater.blob, so it is R2 in CI and the local store after T0):
  clean_full/insee_melodi/_flows.json        the fetcher's state; ONLY the fetcher writes it
  clean_full/insee_melodi/_force_origin.json operator-owned; the fetcher only READS it
  clean_full/insee_melodi/_repull/<FLOW>/<attempt>/<nnnnn>.chunk   staged pages of one attempt
The served <FLOW>.parquet is never touched by a pull until the flow is complete per INSEE paging.

CONDITIONAL WRITES (R2-4). _flows.json is written with ONE single-part PUT, so its R2 ETag is md5 of its
bytes. The If-Match value is md5 of the bytes THIS process read or wrote - it describes exactly those
bytes, so there is no HEAD-after-GET window. A multipart object (never written by us) would carry another
ETag and the conditional PUT is refused, loudly. After T0 the store is local files: the compare-and-swap
is sha256 of the current file compared under the single-writer lock (blob._refuse_or_own_store_write),
then an atomic replace.

The served-parquet publishes (merge.merge_and_write_bounded) are NOT conditional: two concurrent
writers of one flow can still lose each other's rows (last writer wins). The attempt lease below
protects the repair only.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
import time
import uuid

from ... import blob, config

SCHEMA = 1
STATE_FILE = "_flows.json"
FORCE_FILE = "_force_origin.json"
REPULL_DIR = "_repull"

# Statuses. "complete per INSEE paging" is the strongest claim this fetcher makes (C15): no INSEE total
# exists, so isLast + the page+1 probe + the cross-page duplicate check are internal evidence only.
UNCLASSIFIED = "unclassified"
LEGACY = "legacy"
NEEDS_ORIGIN = "needs_origin"
NEEDS_REFRESH = "needs_refresh"          # a due refresh too large for the walk (> one chunk)
IN_PROGRESS = "in_progress"
MERGE_PENDING = "merge_pending"
COMPLETE = "complete_per_paging"
EMPTY = "empty_per_paging"
WITHDRAWN = "withdrawn_suspect"
UNSTABLE = "unstable_paging"
STATUSES = frozenset({UNCLASSIFIED, LEGACY, NEEDS_ORIGIN, NEEDS_REFRESH, IN_PROGRESS, MERGE_PENDING,
                      COMPLETE, EMPTY, WITHDRAWN, UNSTABLE})
# Work that keeps the unit due at the next run (transient_fail with changed_keys, the PR #151 precedent).
REPAIR_PENDING = frozenset({UNCLASSIFIED, NEEDS_ORIGIN, NEEDS_REFRESH, IN_PROGRESS, MERGE_PENDING})
WALKABLE = frozenset({LEGACY, COMPLETE, EMPTY})

ATTEMPT_RE = re.compile(r"^a\d{14}-[0-9a-f]{6}$")
FLOW_RE = re.compile(r"^[A-Z0-9_]+$")
# C10: the ONLY keys a delete may touch.
CHUNK_KEY_RE = re.compile(r"^clean_full/insee_melodi/_repull/([A-Z0-9_]+)/(a\d{14}-[0-9a-f]{6})/\d{5}\.chunk$")

# Test-only bypass of the pre-T0 local-backend refusal (accepted by AR-330 round 2). Never set in code.
_ALLOW_LOCAL_BACKEND = False


class StateConflict(Exception):
    """Another writer changed _flows.json since this process read or wrote it."""


def utcnow() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def source_dir() -> str:
    return config.source_dir("insee_melodi")


def _cut_over() -> bool:
    from core import cutover                                         # noqa: PLC0415
    return cutover.is_cut_over()


def backend_kind() -> str:
    """'r2' (pre-T0 CI and workstation runs), 'local-post-t0', or 'local-pre-t0' (refused)."""
    if blob._r2_routed() is not None:
        return "r2"
    return "local-post-t0" if _cut_over() else "local-pre-t0"


def refuse_wrong_backend() -> None:
    """Before T0 the store is R2; a local-backend run would build state and chunks nobody reads."""
    if backend_kind() == "local-pre-t0" and not _ALLOW_LOCAL_BACKEND:
        from ...errors import DefinitiveError                        # noqa: PLC0415
        raise DefinitiveError("insee_melodi: refused - before T0 the store is R2; run with "
                              "AQUEDUCT_BACKEND=r2 (a local-backend run would write state nobody reads)")


def new_attempt_id() -> str:
    return "a" + dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:6]


def owner_id() -> str:
    import platform                                                  # noqa: PLC0415
    run = os.environ.get("GITHUB_RUN_ID")
    return f"gh:{run}" if run else f"{platform.node() or 'host'}:{os.getpid()}"


def _digest(data: bytes) -> str:
    """The CAS token for bytes: md5 on R2 (== a single-part ETag), sha256 for local files."""
    return hashlib.md5(data).hexdigest() if backend_kind() == "r2" else hashlib.sha256(data).hexdigest()


# --------------------------------------------------------------------------------------------- #
# _flows.json
# --------------------------------------------------------------------------------------------- #
class FlowState:
    """The loaded _flows.json plus the CAS token of the bytes it came from."""

    def __init__(self, data: dict, token: str | None, notes: list):
        self.data = data
        self.token = token                 # None = the object did not exist when read
        self.notes = notes                 # loud findings made while loading (corrupt copy, ...)

    @property
    def flows(self) -> dict:
        return self.data["flows"]

    def flow(self, code: str) -> dict:
        return self.flows.setdefault(code, {"status": UNCLASSIFIED})

    def path(self) -> str:
        return os.path.join(source_dir(), STATE_FILE)

    @staticmethod
    def empty() -> dict:
        return {"schema": SCHEMA, "written_utc": None, "writer": None, "flows": {},
                "identity_kind": "r2-etag" if backend_kind() == "r2" else "size-mtime_ns"}

    @classmethod
    def load(cls) -> "FlowState":
        path = os.path.join(source_dir(), STATE_FILE)
        raw = blob.read_bytes(path)
        if raw is None:
            return cls(cls.empty(), None, [])
        token = _digest(raw)
        try:
            data = json.loads(raw.decode("utf-8"))
            if not isinstance(data, dict) or not isinstance(data.get("flows"), dict):
                raise ValueError("no flows object")
        except (ValueError, UnicodeDecodeError) as e:
            copy = f"{path}.corrupt-{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}"
            blob.write_bytes_atomic(copy, raw)
            return cls(cls.empty(), token,
                       [f"STATE CORRUPT: {STATE_FILE} unreadable ({type(e).__name__}); copied to "
                        f"{os.path.basename(copy)} and re-bootstrapped"])
        if data.get("schema") != SCHEMA:
            from ...errors import DefinitiveError                    # noqa: PLC0415
            raise DefinitiveError(f"insee_melodi: {STATE_FILE} schema {data.get('schema')!r} != {SCHEMA}; "
                                  f"refused rather than guessed (a newer or older fetcher wrote it)")
        for code, fe in data["flows"].items():
            if fe.get("status") not in STATUSES:
                fe["status"] = UNCLASSIFIED
        return cls(data, token, [])

    def save(self) -> None:
        self.data["written_utc"] = utcnow()
        self.data["writer"] = owner_id()
        body = json.dumps(self.data, indent=1, sort_keys=True).encode("utf-8")
        path = self.path()
        kind = backend_kind()
        if kind == "r2":
            r2 = blob._r2_routed()
            key = blob._path_to_key(path)
            if self.token is None:
                new = r2.put_if(key, body, if_none_match=True)
            else:
                new = r2.put_if(key, body, if_match=self.token)
            if new is None:
                raise StateConflict(f"{STATE_FILE} changed since this pass read it (another writer)")
            self.token = new or hashlib.md5(body).hexdigest()
            return
        # local store (after T0, or a test): compare-and-swap under the single-writer lock
        if kind == "local-post-t0":
            blob._refuse_or_own_store_write(f"a write of insee_melodi/{STATE_FILE}")
        cur = None
        if os.path.exists(path):
            with open(path, "rb") as fh:
                cur = fh.read()
        if (None if cur is None else hashlib.sha256(cur).hexdigest()) != self.token:
            raise StateConflict(f"{STATE_FILE} changed since this pass read it (another writer)")
        blob.write_bytes_atomic(path, body)
        self.token = hashlib.sha256(body).hexdigest()


def read_force() -> dict:
    """The operator-owned file. Read only. {"flows": [...], "unblock_repair_after_utc": "<iso>"}."""
    raw = blob.read_bytes(os.path.join(source_dir(), FORCE_FILE))
    if not raw:
        return {}
    try:
        d = json.loads(raw.decode("utf-8"))
        return d if isinstance(d, dict) else {}
    except (ValueError, UnicodeDecodeError):
        return {"_unreadable": True}


# --------------------------------------------------------------------------------------------- #
# served-object identity (C8 / R2-4b): one HEAD, never a whole-object GET to decide
# --------------------------------------------------------------------------------------------- #
def identity(path: str) -> dict | None:
    r2 = blob._r2_routed()
    if r2 is not None:
        meta = r2.head_meta(blob._path_to_key(path))
        if meta is None:
            return None
        return {"size": int(meta.get("ContentLength") or 0),
                "etag": str(meta.get("ETag") or "").strip('"')}
    if not os.path.exists(path):
        return None
    st = os.stat(path)
    return {"size": st.st_size, "mtime_ns": st.st_mtime_ns}


def same_identity(a: dict | None, b: dict | None) -> bool:
    if not a or not b:
        return False
    keys = ("size", "etag") if "etag" in a or "etag" in b else ("size", "mtime_ns")
    return all(a.get(k) == b.get(k) for k in keys)


# --------------------------------------------------------------------------------------------- #
# chunks (C10)
# --------------------------------------------------------------------------------------------- #
def chunk_path(flow: str, attempt: str, n: int) -> str:
    if not FLOW_RE.match(flow) or not ATTEMPT_RE.match(attempt):
        raise ValueError(f"refusing a chunk path for flow={flow!r} attempt={attempt!r}")
    return os.path.join(source_dir(), REPULL_DIR, flow, attempt, f"{n:05d}.chunk")


def delete_chunk(path: str, *, flow: str, current_attempt: str | None) -> bool:
    """Delete ONE staging object, only if its key fully matches CHUNK_KEY_RE, names `flow`, and is not
    the current attempt's. Also removes the local scratch-mirror copy. Returns True when deleted."""
    key = blob._path_to_key(path).replace("\\", "/")
    m = CHUNK_KEY_RE.match(key)
    if not m or m.group(1) != flow or (current_attempt is not None and m.group(2) == current_attempt):
        return False
    kind = backend_kind()
    if kind == "r2" or kind == "local-post-t0":
        blob.delete_store_object(path)                                 # R2 before T0; locked local after
    elif os.path.exists(path):
        os.remove(path)                                                # local pre-T0: tests only
    if kind == "r2" and os.path.exists(path):
        os.remove(path)                                                # the runner's scratch mirror
    return True


def staging_dir() -> str:
    """R2-8: beside the bounded-merge spill dir (ECONDL_DUCKDB_TMP, else <root>/data/_duckdb_spill),
    never the system temp dir (C: or D: on the workstation) and never D:."""
    base = os.environ.get("ECONDL_DUCKDB_TMP") or os.path.join(config.ROOT, "data", "_duckdb_spill")
    d = os.path.join(base, "insee_melodi_staging")
    if os.path.splitdrive(os.path.abspath(d))[0].upper() == "D:":
        from ...errors import DefinitiveError                        # noqa: PLC0415
        raise DefinitiveError(f"insee_melodi: staging dir {d} is on D:, where work is banned - set "
                              f"ECONDL_DUCKDB_TMP to a folder on another drive")
    os.makedirs(d, exist_ok=True)
    return d


# --------------------------------------------------------------------------------------------- #
# the pass clock (R2-3)
# --------------------------------------------------------------------------------------------- #
HARD_MARGIN_S = 5 * 60          # stop this far before the real kill
SOFT_STOP_S = 35 * 60           # no new page after this


def unit_window_s() -> tuple[float, str]:
    """Seconds this unit may run, and where the number came from.

    The ARMED alarm is the truth when one is live: orchestrate clamps the window to the remaining run
    ceiling (_unit_window_min), so a late-starting unit can have much less than 45 minutes, and
    signal.getitimer reads what is actually left on it. With no alarm (Windows, after T0) the clamped
    window itself; failing that the configured timeout. The fetcher keeps its own hard end in every case."""
    try:
        from ... import orchestrate as _o                            # noqa: PLC0415
        dl = getattr(_o, "_ARMED_DEADLINE", None)
        if dl is not None and getattr(dl, "armed", False):
            import signal                                            # noqa: PLC0415
            rem = signal.getitimer(signal.ITIMER_REAL)[0]
            if rem and rem > 0:
                return float(rem), "armed alarm"
        w = float(_o._unit_window_min())
        if w > 0:
            return w * 60.0, "clamped window"
    except Exception:                                                # noqa: BLE001
        pass
    try:
        return float(os.environ.get("AQUEDUCT_UNIT_TIMEOUT_MIN", "45")) * 60.0, "AQUEDUCT_UNIT_TIMEOUT_MIN"
    except ValueError:
        return 45 * 60.0, "default 45 min"


class Clock:
    def __init__(self, now=time.monotonic, window_s: float | None = None, source: str | None = None):
        self.now = now
        self.t0 = now()
        if window_s is None:
            window_s, source = unit_window_s()
        self.window_s, self.source = window_s, source
        self.hard_end = self.t0 + max(window_s - HARD_MARGIN_S, 60.0)
        self.soft_end = min(self.t0 + SOFT_STOP_S, self.hard_end)

    def fits(self, seconds: float) -> bool:
        """Can work of `seconds` start now and end before the hard end?"""
        return self.now() + seconds <= self.hard_end

    def left_soft(self) -> float:
        return self.soft_end - self.now()

    def at(self, minutes_from_start: float) -> float:
        return min(self.t0 + minutes_from_start * 60.0, self.soft_end)


def merge_margin_s(rows: int) -> float:
    """Seconds to reserve before starting the (uninterruptible) DuckDB merge of `rows` staged rows.
    a3 measured 10.8 s for 8.6M rows on the workstation; a runner is slower, so this is generous and the
    first CI run logs the real duration (the 'merge took' line) to calibrate it."""
    return 120.0 + rows / 100_000.0
