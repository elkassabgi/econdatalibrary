"""IMF International Trade in Goods by partner country — DIRECT from api.imf.org
(flow IMTS, agency IMF.STA). Successor to Direction of Trade Statistics (DOTS).

WHY THIS ONE EXISTS. `imf_dot` holds 101,000 relay-era series (catalogued, SERVED, frozen — no
fetcher and no registry entry), the largest frozen block after imf_ifs. IMF RENAMED the dataset:
its /dataflow catalogue has NO flow whose id contains DOT, but carries IMTS, agency IMF.STA,
v1.0.0, whose own description reads "The International trade in goods by partner country
dataset (formerly Direction of Trade Statistics (DOTS))". Read from the catalogue on
2026-08-05, not guessed — the rename class (PSBSFAD→PSBS, PCTOT→CTOT, GS_*, HPD) is ledger
R74/R75, and the first probe "no DOT flow exists" was ITSELF wrong until a known-present
control (BOP) exposed an attribute-order bug in my regex, twice (R338/R329).

MEASURED SHAPE, from the API's own responses (never assumed): 4 dims, all codelisted —
COUNTRY.INDICATOR.COUNTERPART_COUNTRY.FREQUENCY (that key order returns data; FREQUENCY-first
returns an empty body) — 359 country codes, e.g. `IMTS:USA.MG_CIF_USD.ABW.A`. The STORE keys have
five parts in the order of the store's sidecar key_dims [COUNTERPART_COUNTRY, COUNTRY, FREQUENCY,
INDICATOR, METHODOLOGY]: 472,238 keys, 71,797,861 rows, 1948-12-31..2026-04-30 (an F: copy of the
store, 2026-10-30).

Adding `imf_imts_direct` takes nothing away from `imf_dot`; whether it supersedes it is the
reserved re-key question (#46).

THIS SOURCE NO LONGER USES _imf_direct.run (2026-10-30, PLAN R10.3 step 4). The other imf_*_direct
modules still do, and _imf_direct.py / jobs/ingest_imf_direct.py are unchanged. _imf_direct.run
makes ONE whole-flow GET and slices only when the parse hits a size error, which never happens on
the 383 GB workstation that runs this source (run_location: local). On 2026-10-10 that GET ran
6,575 s and reached 5,036,310,528 bytes before the ps1 hard stop killed it, keeping nothing. It has no
stop point and does not fit the windows the desktop pass gives. So this module pulls the flow in
PERIOD SLICES on every pass (_plan), stops before the pass's window end, and keeps finished slices
on local disk until the merge succeeds.

  * WINDOWS (_plan): 1900-1959, decades from 1960, then 2020..Y-2, then Y-1..Y+1 (the newest, re-pulled
    on EVERY merging pass and never reused, so a month IMF appends while slices wait is in the merge).
    Slices are named and keyed by their BOUNDS, never by position, so a year rollover cannot pair a
    kept file with a different window. Decades rather than years because every document repeats the
    series headers, and the ingester chose decades to keep the request count small (about 10 requests
    a sweep). The size of a slice is NOT MEASURED; the largest decade (2010s, 16,958,363 rows, 23.6% of
    the store) is ESTIMATED at ~27-30 min from the 10-10 average rate. The per-slice timing lines are
    the measurement.
  * STOP: before each slice the time left to orchestrate._RUN_DEADLINE_TS must be at least
    max(AQUEDUCT_IMTS_MARGIN_MIN floor, 2 x the longest slice this pass) - for the newest slice PLUS
    the merge reserve; before the merge, at least AQUEDUCT_IMTS_MERGE_RESERVE_MIN. Both defaults are
    CHOSEN: floor 5 min (a first pass has no slice history), reserve 20 min (about 3x the 394.7 s one
    warm measurement took on an F: copy of the store: 147.7 s merge_and_write + 247.0 s read-back and
    cursor fold, n=1; the concat, the R2 transfer and peak memory were not measured). A stop books
    every slice not reached as deferred (finalize's deferral sentence: ROTATING when a slice was
    pulled), merges nothing, reports merged_rows=0, never ok and never last_success. With no ceiling
    nothing stops.
  * EVERY REQUEST: the socket timeout is clamped to the time left at the start and RE-CLAMPED before
    every body chunk (through the response's socket; where that private path is not there, the start
    clamp holds, so a late stall can wait at most ing.TIMEOUT = 300 s past the window end), the body
    is read in chunks with a wall-clock check between them, a 1.0 s pace separates requests (CHOSEN:
    the repo records no api.imf.org limit; jobs/ingest_imf_commodity.py paces 1.0 s), 429/503 are
    transient with Retry-After honoured, retry sleeps count against the deadline and no retry starts
    past it. The margin starts from the slowest kept slice's recorded time, not from zero.
  * A CUT SLICE (review AR-410 W6): a slice the window end stops mid-request keeps nothing, but its
    elapsed time is saved in the marker as a LOWER BOUND on that window's cost (the largest seen, kept
    across a store change because it measures the request, not the store; aged out after the open
    window's age bound). Before that slice is tried again its margin is at least 2 x that bound, so a
    pass does not start the 2010s on a margin from faster slices and throw the partial body away again.
    The bound is per slice, deliberately not folded into the pass-wide longest: one cut decade must not
    raise the margin of every other slice. A pass whose whole window is under that slice's margin can
    never finish it, and its stop sentence says so ("window shorter than the slice's measured lower
    bound" when the window is under the bound itself); the release is a longer window (operator).
  * KEY DIMS are PINNED to the store's sidecar (<store>.dims.json). A slice whose identity attributes
    differ is refused by name (see _refuse_dims for the release path), never filled with "" and never
    dropped: a per-slice attribute union would otherwise mint a second key grain (R22). A missing or
    corrupt sidecar is a named TransientError; nothing falls back to the union.
  * A slice answering 400/404 is a refusal of THAT window, named as such - not "the flow moved".
    A store holding data before the first window's start is refused rather than left unrefreshed.

KEPT SLICES are LOCAL (data/_imts_slices/<source>/), deliberately not through blob: scratch for the
one machine that runs this source. The marker (fails closed on write) records each slice's bounds,
fetch time, rows and dims, and the store's row count; a store that changed since discards them.
Closed windows (ending before Y-2) keep for AQUEDUCT_IMTS_CLOSED_MAX_AGE_DAYS (365, CHOSEN), the
2020..Y-2 window for AQUEDUCT_IMTS_OPEN_MAX_AGE_DAYS (31, CHOSEN), the newest never. RESIDUAL of the
365 d bound: inside a chain of passes that stop before the merge, a closed decade pulled early in the
chain is merged as pulled, so an IMF revision to it made after that pull can wait up to 365 d (the
next sweep after the chain merges re-pulls it). A pass deletes
.sdmx.tmp / .parquet.tmp left in that directory by a kill; it never touches anything outside it.

CHANGE SIGNAL, OWED MARK, ROUTE B (PR B, 2026-10-30; review AR-410 O9, classes (c), (b), (j)).
  * (c) The merge reports its changed keys (merge_and_write report_changed_keys=True) with a PER-CALL
    changed_keys_cap = the assembled table's own row count, so the report cannot refuse on size. They
    are Result.changed_keys, the set the CSV step derives (orchestrate._derive_changed_csvs; {} = nothing
    changed). A revision with no added row is revised_unit, so it reads ok and the CSV step runs. A
    report refused anyway (a ValueError before any I/O) runs the plain merge and the change is UNKNOWN:
    every key is handed over, never a truncated list. No cap on the SET: the catalogue is table grain
    (orchestrate._TABLE_GRAIN: 472,238 keys map to 2,937 table ids), so all keys cost the same derive
    every ok pass on main already paid. series_cursors stay ALL keys, as before (health's frontier).
  * (b) data/clean_full/<source>/_imts_owed.json, THROUGH BLOB beside the store and its .dims.json, on
    the census precedent (_census_owed.json). The debt belongs to the STORE: the store goes through blob
    (r2 today, the local store after T0), so a mark kept anywhere else could be lost or stale while the
    store it speaks for moves. The kept slices stay LOCAL: losing them means a re-pull, never a CSV.
    Forms: "merging" is written BEFORE merge_and_write publishes (that save FAILS CLOSED: nothing is
    merged); "owed" after the merge when it changed something or a debt was carried ({} when it changed
    nothing); "handed" when the update returns a Result carrying the keys - its token starts the
    Result's error. A pass that finds "merging" or "owed" hands over EVERY stored key (the mark does not
    record which keys) and never ends no_change, also when its own re-merge changes nothing (S1n). A
    "handed" mark is settled ONLY when a later pass finds its token in unit_state.last_error, which the
    orchestrator writes AFTER the CSV step, without the csv-fence note; otherwise every key is handed
    over again. So a kill after the update returns and before the orchestrator records the CSV step
    loses nothing. With NO state.db at all the update ran outside the orchestrator (which creates it
    before any update), and the mark settles as census's does. Over-reporting re-derives identical
    CSVs; it never leaves a changed one behind. The
    read fails closed: a raising read = TransientError; content that is not a mark = DefinitiveError,
    whose recovery is to write a valid "owed" mark; an explicit {} = nothing owed. An ABSENT mark beside
    an EXISTING store is read as "owed" ONCE (review AR-410, R1766): main re-derived every key on every
    pass that added a row, and wrote no mark, so a store that main left with stale CSVs (a kill after its
    merge, a revision-only pass that read no_change) carries a debt nothing records. The first pass of
    this code hands every stored key over, and from then on the mark is always present. With no store
    an absent mark is nothing owed (nothing is served). A pass that returns without merging (a stop, a
    transient) keeps the mark and names it as deferred.
  * (j) ROUTE B: a structural refusal (_refuse), a refused merge, or a finalize that raises, while
    anything changed or is owed, returns `partial` WITH every stored key's cursor, the keys as
    changed_keys, and the refusal text. It raises only when nothing changed and nothing is owed.
STATED RESIDUALS: the mark is settled by evidence in state.db, so a pass that cannot read an existing
state.db hands every key over on every merging pass - the derive work main did on every ok pass, never a
lost key; outside the orchestrator (no state.db) a kill after the return is not covered, as in census
and wid; a "merging" mark left by a kill BEFORE the merge
published over-reports once; a pass that stops before the merge carries the debt to the next merging
pass, not this one; the first pass after the switch from main hands every stored key over once (the
absent mark), whether or not main left a debt - an over-report, never a lost key.
"""
from __future__ import annotations

import datetime as dt
import json
import math
import os
import secrets
import sqlite3
import sys
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ... import blob, config, merge
from ...errors import DefinitiveError, TransientError
from ..base import Result
from . import _imf_direct as _base
from ._common import Tally, finalize, retry_after_seconds
from jobs import ingest_imf_direct as ing

FLOW, AGENCY, SOURCE = "IMTS", "IMF.STA", "imf_imts_direct"

FIRST_START = 1900                   # the store starts 1948-12-31; refused if it ever starts earlier
FIRST_END = 1959
PACE_S = 1.0                         # CHOSEN: no published api.imf.org limit in the repo
MARGIN_FLOOR_MIN_DEFAULT = 5.0       # CHOSEN
MERGE_RESERVE_MIN_DEFAULT = 20.0     # CHOSEN, ~3x one measured 394.7 s (n=1)
CLOSED_MAX_AGE_DAYS_DEFAULT = 365.0  # CHOSEN
OPEN_MAX_AGE_DAYS_DEFAULT = 31.0     # CHOSEN: one monthly cadence
MARKER = "marker.json"
CHUNK = 1 << 20

# THE OWED MARK (PR B, see the module docstring): data/clean_full/<source>/_imts_owed.json, THROUGH BLOB.
OWED_FILE = "_imts_owed.json"
_MERGING, _OWED, _HANDED = "merging", "owed", "handed"
_FORMS = (_MERGING, _OWED, _HANDED)
_FENCE_NOTE = "csv phase exceeded its"     # orchestrate's csv-fence note: the hand-over was NOT derived
_DEBT_LABEL = "owed keys of an earlier merge (mark kept; a pass that merges hands them over)"


def current_vintage(unit):
    return _base.vintage(FLOW)


def update(unit, us):
    return run_sliced(FLOW, AGENCY, SOURCE)


# --------------------------------------------------------------------------------------------
# helpers

def _today() -> dt.date:
    return dt.datetime.fromtimestamp(time.time(), dt.timezone.utc).date()


def _utc(ts) -> str:
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _plan(today: "dt.date | None" = None) -> list:
    """[(label, start, end, kind), ...] - contiguous, disjoint windows covering FIRST_START..Y+1.
    kind: "closed" (ends before Y-2), "open" (ends at Y-2), "newest" (Y-1..Y+1, never reused)."""
    y = (today or _today()).year
    out = [(FIRST_START, FIRST_END)]
    d = FIRST_END + 1
    while d + 9 <= y - 2:
        out.append((d, d + 9))
        d += 10
    if d <= y - 2:
        out.append((d, y - 2))
    plan = [(f"{a}-{b}", str(a), str(b), "open" if b >= y - 2 else "closed") for a, b in out]
    plan.append((f"{y - 1}-{y + 1}", str(y - 1), str(y + 1), "newest"))
    return plan


def _window_end_ts():
    """The pass's run ceiling as an epoch timestamp, or None. Read through sys.modules (as wid and
    census do), so this module never imports the orchestrator."""
    orch = sys.modules.get("updater.orchestrate")
    ts = getattr(orch, "_RUN_DEADLINE_TS", None) if orch is not None else None
    if isinstance(ts, bool) or not isinstance(ts, (int, float)) or not math.isfinite(ts):
        return None
    return float(ts)


def _env_num(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        v = float(raw)
    except (TypeError, ValueError):
        v = float("nan")
    if not (math.isfinite(v) and v >= 0):
        print(f"[{SOURCE}] {name}={raw!r} is not a finite number >= 0 - using {default:g}", flush=True)
        return default
    return v


def _slice_dir(source_id: str) -> str:
    return os.path.join(os.path.dirname(config.DATA_ROOT), "_imts_slices", source_id)


def _slice_path(d: str, label: str) -> str:
    return os.path.join(d, f"slice_{label}.parquet")


class _Stop(Exception):
    """The window end arrived: nothing from the request in flight is kept."""


class _Deadline:
    def __init__(self, end_ts):
        self.end = end_ts

    def left(self):
        return None if self.end is None else self.end - time.time()

    def sleep(self, s: float) -> None:
        """A sleep that counts against the deadline: it refuses to sleep past the window end."""
        left = self.left()
        if left is not None and s >= left:
            raise _Stop(f"a {s:.0f}s wait would pass the window end ({max(left, 0):.0f}s left)")
        time.sleep(s)

    def timeout(self, cap: float) -> float:
        left = self.left()
        if left is None:
            return cap
        if left <= 1.0:
            raise _Stop(f"{max(left, 0):.0f}s left before the window end")
        return min(cap, left)


def _socket_of(resp):
    """The socket under a urllib response (http.client: resp.fp is a BufferedReader over a SocketIO
    whose _sock is the socket), or None - a private path, used best-effort only."""
    try:
        s = resp.fp.raw._sock
        return s if hasattr(s, "settimeout") else None
    except Exception:                                        # noqa: BLE001
        return None


def _get_to_file(url: str, dest: str, dl: _Deadline) -> int:
    """GET url into dest, bounded by the deadline (see the module docstring). 400/404 propagate."""
    last = None
    for attempt in range(ing.RETRIES):
        try:
            req = urllib.request.Request(url, headers=ing.UA)
            n = 0
            with urllib.request.urlopen(req, timeout=dl.timeout(ing.TIMEOUT)) as r, \
                    open(dest, "wb") as fh:
                rd = getattr(r, "read1", None) or r.read
                sock = _socket_of(r)
                while True:
                    left = dl.left()
                    if left is not None and left <= 0:
                        raise _Stop(f"window end reached after {n:,} bytes")
                    if sock is not None and left is not None:
                        # RE-CLAMP per chunk, so a stall late in the body waits at most until the
                        # window end, not a full ing.TIMEOUT past it. Where the socket cannot be
                        # reached, the clamp set at request start holds (<= ing.TIMEOUT past it).
                        try:
                            sock.settimeout(dl.timeout(ing.TIMEOUT))
                        except OSError:
                            pass
                    chunk = rd(CHUNK)
                    if not chunk:
                        break
                    fh.write(chunk)
                    n += len(chunk)
            return n
        except _Stop:
            raise
        except urllib.error.HTTPError as e:
            if e.code in (400, 404):
                raise
            last = e
            wait = (retry_after_seconds(e, default=5 * (attempt + 1)) if e.code in (429, 503)
                    else 5 * (attempt + 1))
        except Exception as e:                               # noqa: BLE001
            if type(e).__name__ == "UnitTimeout":
                raise
            last = e
            wait = 5 * (attempt + 1)
        if attempt + 1 < ing.RETRIES:
            dl.sleep(wait)
    raise TransientError(f"GET failed after {ing.RETRIES} tries ({last!r})")


def _flow_version(flow: str, dl: _Deadline):
    """The dataflow version - reported only, never a change signal (R1101) - clamped like every
    request. None when the catalogue is unreachable; that never stops the pull."""
    try:
        req = urllib.request.Request(f"{ing.BASE}/dataflow", headers=ing.UA)
        with urllib.request.urlopen(req, timeout=dl.timeout(180)) as r:
            root = ET.fromstring(r.read())
    except _Stop:
        raise
    except Exception as e:                                   # noqa: BLE001
        if type(e).__name__ == "UnitTimeout":
            raise
        print(f"[{SOURCE}] dataflow catalogue unreachable ({e!r}); continuing", flush=True)
        return None
    for e in root.iter():
        if e.tag.split("}")[-1] == "Dataflow" and (e.get("id") or "").upper() == flow:
            return e.get("version") or ""
    return None


def _pinned_dims(path: str, store_rows: int):
    """The store's key order from its own sidecar. Once a store exists this FAILS CLOSED with a NAMED
    TransientError (absent, corrupt or unreadable sidecar): nothing is pulled and nothing falls back
    to a slice's attribute union. With no store yet (first ingest) -> None, and the slices must then
    agree with each other."""
    name = os.path.basename(path) + ".dims.json"
    try:
        raw = blob.read_bytes(path + ".dims.json")
    except Exception as e:                                   # noqa: BLE001
        raise TransientError(f"{SOURCE}: key-dims sidecar {name} unreadable: {e!r}") from e
    if raw is None:
        if store_rows == 0:
            return None
        raise TransientError(
            f"{SOURCE}: key-dims sidecar {name} MISSING while the store holds {store_rows:,} rows - "
            f"refusing to build keys by guess. RECOVERY (operator): publish the sidecar whose key_dims "
            f"match the stored 5-part keys through blob (the desktop copy of data/clean_full/{SOURCE}/"
            f"{name})")
    try:
        dims = json.loads(raw.decode("utf-8"))["key_dims"]
        ok = isinstance(dims, list) and bool(dims) and all(isinstance(x, str) and x for x in dims)
    except Exception:                                        # noqa: BLE001
        ok = False
    if not ok:
        raise TransientError(
            f"{SOURCE}: key-dims sidecar {name} CORRUPT - refusing to build keys by guess. RECOVERY "
            f"(operator): re-publish a sidecar whose key_dims match the stored keys")
    return list(dims)


def _refuse_dims(label: str, got: list, pinned: list) -> str:
    """The structural line for a slice whose identity attributes differ from the store's key_dims.

    RELEASE PATH, an operator decision and never automatic: if IMF really changed the flow's identity
    attributes, every key changes shape - a RE-GRAIN (R22). Archive the store, clean re-pull the whole
    flow under the new dims into a fresh store, publish the new sidecar, re-catalogue, and only then
    let the scheduled pass run again. A slice that merely LACKS an attribute is refused the same way:
    filling the part with "" would mint keys the store has never held."""
    extra = sorted(set(got) - set(pinned))
    missing = sorted(set(pinned) - set(got))
    return (f"{label}: identity attributes differ from the store's key_dims (new {extra or 'none'}, "
            f"missing {missing or 'none'}) - REFUSED, nothing merged; a changed key shape is an "
            f"operator re-key (R22 clean re-pull, see imf_imts_direct._refuse_dims)")


def _parse_slice(xml_path: str, flow: str, start: str, end: str, pinned):
    """(table | None | "REFUSED", identity attributes seen, stats) for one slice document. Keys use
    `pinned` when given. Observations dated outside [start, end] are dropped and counted, so no row
    can land in two slices."""
    dimset: set = set()
    n_series = 0
    for el in ing.iter_series(xml_path):
        dimset |= set(el.attrib)
        n_series += 1
    got = sorted(dimset - ing.NON_IDENTITY)
    if not n_series:
        return None, got, {"series": 0}
    if pinned is not None and set(got) != set(pinned):
        return "REFUSED", got, {"series": n_series}
    dims = list(pinned) if pinned is not None else got
    lo, hi = int(start), int(end)
    keys, dates, vals = [], [], []
    st = {"series": n_series, "empty": 0, "bad_date": 0, "bad_value": 0, "outside": 0, "dup": 0}
    for s in ing.iter_series(xml_path):
        key = f"{flow}:" + ".".join((s.attrib.get(d) or "").replace(".", "_") for d in dims)
        for o in s:
            if o.tag.split("}")[-1] != "Obs":
                continue
            v = o.attrib.get("OBS_VALUE")
            if v in (None, "", "NaN"):
                st["empty"] += 1
                continue
            d = ing.parse_period(o.attrib.get("TIME_PERIOD", ""))
            if d is None:
                st["bad_date"] += 1
                continue
            if not (lo <= d.year <= hi):
                st["outside"] += 1
                continue
            try:
                fv = float(v)
            except ValueError:
                st["bad_value"] += 1
                continue
            keys.append(key)
            dates.append(d)
            vals.append(fv)
    tbl = pa.table({"series_key": pa.array(keys, pa.string()),
                    "obs_date": pa.array(dates, pa.date32()),
                    "value": pa.array(vals, pa.float64())})
    if tbl.num_rows:
        # Twice inside ONE document: the whole-flow pull had the same exposure and merge's dedup keeps
        # the last, so it is counted and printed, not refused.
        st["dup"] = tbl.num_rows - tbl.group_by(["series_key", "obs_date"]).aggregate([]).num_rows
    return tbl, dims, st


def _load_marker(d: str) -> dict:
    p = os.path.join(d, MARKER)
    if not os.path.exists(p):
        return {}
    try:
        with open(p, encoding="utf-8") as fh:
            obj = json.load(fh)
        if not isinstance(obj, dict) or not isinstance(obj.get("slices"), dict):
            raise ValueError("not a marker object")
        return obj
    except Exception as e:                                   # noqa: BLE001
        # Toward a re-pull: the kept slices are discarded (data stays correct, progress is lost).
        print(f"[{SOURCE}] slice marker unreadable ({e!r}) - discarding kept slices", flush=True)
        return {}


def _save_marker(d: str, obj: dict) -> None:
    """FAILS CLOSED: a marker that cannot be written would make a later pass distrust the slices, so
    the pass stops (TransientError) rather than pull work it would then discard."""
    p = os.path.join(d, MARKER)
    tmp = p + ".tmp"
    try:
        os.makedirs(d, exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(obj, fh, indent=1, sort_keys=True)
        os.replace(tmp, p)
    except OSError as e:
        raise TransientError(f"{SOURCE}: slice marker not writable ({e!r})") from e


def _clear_dir(d: str, keep=()) -> None:
    if not os.path.isdir(d):
        return
    for f in os.listdir(d):
        if f in keep:
            continue
        try:
            os.remove(os.path.join(d, f))
        except OSError:
            pass


def _store_min_date(path: str):
    """The store's earliest obs_date, from the parquet footer statistics (no data read); falls back to
    reading the one column. None when there is no store."""
    if not blob.exists(path):
        return None
    try:
        md = blob.read_metadata(path)
        col = md.schema.to_arrow_schema().get_field_index("obs_date")
        mins = [md.row_group(i).column(col).statistics.min for i in range(md.num_row_groups)]
        if mins and all(m is not None for m in mins):
            return min(mins)
    except Exception:                                        # noqa: BLE001
        pass
    return pc.min(blob.read_table(path, columns=["obs_date"]).column("obs_date")).as_py()


# --------------------------------------------------------------------------------------------
# the owed mark (PR B)

def _owed_path(out_dir: str) -> str:
    return os.path.join(out_dir, OWED_FILE)


def _load_owed(out_dir: str) -> dict:
    """The owed mark: {} (nothing owed), {"flow", "form", "utc"[, "token"]}, or None when ABSENT (the
    caller decides: beside an existing store it is owed once, see the module docstring). FAILS CLOSED:
    a read that RAISES -> TransientError (retry later, nothing pulled); content that is present but not
    a mark -> DefinitiveError naming its recovery. Returning {} for an unusable mark would drop the
    debt, and the CSVs it owes would never be derived."""
    try:
        raw = blob.read_bytes(_owed_path(out_dir))
    except Exception as e:                                   # noqa: BLE001
        raise TransientError(f"{SOURCE}: owed mark {OWED_FILE} unreadable: {e!r}") from e
    if raw is None:
        return None
    try:
        obj = json.loads(raw.decode("utf-8"))
        ok = isinstance(obj, dict) and (obj == {} or (
            obj.get("flow") == FLOW and obj.get("form") in _FORMS and isinstance(obj.get("utc"), str)
            and (obj["form"] != _HANDED or (isinstance(obj.get("token"), str) and bool(obj["token"])))))
    except (ValueError, UnicodeDecodeError):
        ok = False
    if not ok:
        raise DefinitiveError(
            f"{SOURCE}: owed mark {OWED_FILE} is present but is not a mark; existing data kept. "
            f"RECOVERY (operator): overwrite it through blob with "
            f'{{"flow": "{FLOW}", "form": "owed", "utc": "<now>"}} - the source\'s next pass then hands every '
            f"stored key to the CSV step and settles the mark. An explicit {{}} instead would drop the "
            f"keys it owes.")
    return obj


def _save_owed(out_dir: str, mark: dict) -> bool:
    """Persist the mark through blob (atomic); True when written. Only the pre-merge "merging" save
    must fail closed, and its caller checks the return; any other failed save leaves an older form
    behind, which over-reports (re-derives identical CSVs) and never under-reports."""
    try:
        blob.write_bytes_atomic(_owed_path(out_dir),
                                json.dumps(mark, indent=1, sort_keys=True).encode("utf-8"))
        return True
    except Exception as e:                                   # noqa: BLE001
        print(f"[{SOURCE}] owed mark NOT saved ({e!r})", flush=True)
        return False


def _mark(form: str, **kw) -> dict:
    return {"flow": FLOW, "form": form, "utc": _utc(time.time()), **kw}


def _handed_recorded(source_id: str, token: str):
    """(recorded, why). A "handed" mark is settled only on EVIDENCE that the orchestrator persisted the
    hand-over: it writes the Result's error, which starts with the token, into unit_state.last_error
    AFTER the CSV step (orchestrate.run_once: CSV phase, retry queue, then upsert_unit). A csv-fence
    note there means the phase was abandoned, so the keys were NOT derived. Anything unreadable
    (a locked or corrupt state.db, a missing table) counts as NOT recorded: the keys are handed over
    again, which over-reports and never loses the debt.

    NO state.db AT ALL is the one exception: the orchestrator creates it (StateStore() at the start of
    run_once) before any update runs, so its absence means this update, and the one that took the
    hand-over, ran OUTSIDE the orchestrator (a test, a harness, a direct call). That Result went to
    its caller, and nothing will ever record it here, so waiting for a record would hand every key
    over on every pass for ever. It settles, as census and wid settle on return."""
    if not os.path.exists(config.STATE_DB):
        return True, "no state.db: not run under the orchestrator, so the hand-over went to its caller"
    try:
        con = sqlite3.connect(f"file:{config.STATE_DB}?mode=ro", uri=True, timeout=5)
        try:
            rows = con.execute("SELECT last_error FROM unit_state WHERE source_id=?",
                               (source_id,)).fetchall()
        finally:
            con.close()
    except Exception as e:                                   # noqa: BLE001
        return False, f"state not readable ({type(e).__name__})"
    for (err,) in rows:
        if err and token in err:
            if _FENCE_NOTE in err:
                return False, "recorded, but the csv phase was abandoned at its fence"
            return True, "recorded"
    return False, "not recorded (a kill before the orchestrator's CSV step?)"


def _fold_cursors(tbl) -> dict:
    cursors = {}
    for k, d in zip(tbl.column("series_key").to_pylist(), tbl.column("obs_date").to_pylist()):
        if d:
            iso = d.isoformat()
            if k not in cursors or iso > cursors[k]:
                cursors[k] = iso
    return cursors


# --------------------------------------------------------------------------------------------

def _margin_terms(floor_s: float, longest: float, lb: float, add: float) -> str:
    """Name the term that set a slice's margin, max(floor, 2 x longest, 2 x lb) + add: the largest
    of the three (ties go to the bound, then the slowest slice), then the merge reserve if added."""
    if 2 * lb >= 2 * longest and 2 * lb >= floor_s:
        t = f"2 x the slice's measured lower bound ({2 * lb / 60:.1f} min)"
    elif 2 * longest >= floor_s:
        t = f"2 x the slowest slice this pass ({2 * longest / 60:.1f} min)"
    else:
        t = f"the {floor_s / 60:.1f}-min floor"
    return t + (f" plus the {add / 60:.1f}-min merge reserve the newest slice needs" if add else "")


def run_sliced(flow: str, agency: str, source_id: str):
    tally = Tally()
    out_dir = config.source_dir(source_id)
    path = os.path.join(out_dir, f"{source_id}.parquet")
    before = blob.row_count(path) if blob.exists(path) else 0
    t_pass = time.time()
    end_ts = _window_end_ts()
    win_s = None if end_ts is None else end_ts - t_pass     # the whole window this pass was given
    dl = _Deadline(end_ts)
    floor_s = _env_num("AQUEDUCT_IMTS_MARGIN_MIN", MARGIN_FLOOR_MIN_DEFAULT) * 60
    reserve_s = _env_num("AQUEDUCT_IMTS_MERGE_RESERVE_MIN", MERGE_RESERVE_MIN_DEFAULT) * 60
    age_s = {"closed": _env_num("AQUEDUCT_IMTS_CLOSED_MAX_AGE_DAYS", CLOSED_MAX_AGE_DAYS_DEFAULT) * 86400,
             "open": _env_num("AQUEDUCT_IMTS_OPEN_MAX_AGE_DAYS", OPEN_MAX_AGE_DAYS_DEFAULT) * 86400}
    plan = _plan()
    print(f"[{SOURCE}] sliced pull, {len(plan)} windows; run ceiling "
          f"{_utc(end_ts) if end_ts is not None else 'none'}; margin floor {floor_s / 60:g} min, "
          f"merge reserve {reserve_s / 60:g} min", flush=True)
    floor_env = len(plan) + 1          # finalize's all-empty guard is for empty WINDOWS, not slices

    debt = {"on": False}               # an owed mark this pass must hand over (set below)

    def _fin(**kw):
        # Every path that returns without merging: an owed debt stays on its mark, and says so.
        if debt["on"]:
            tally.deferred_unit(_DEBT_LABEL)
        return finalize(tally, kw.pop("rows", before), kw.pop("mx", None) or _base._max_date(path),
                        source=source_id, empty_window_floor=floor_env, **kw)

    def _handed(res):
        """THE MARK'S LAST FORM: this Result carries the keys. The token goes at the START of the
        Result's error (orchestrate clips the end, never the start), and a later pass settles the
        mark only when the token shows the orchestrator recorded this Result after its CSV step."""
        token = f"hand-over {_utc(time.time())}-{secrets.token_hex(6)}"
        if _save_owed(out_dir, _mark(_HANDED, token=token)):
            print(f"[{SOURCE}] owed mark -> handed ({token}); settled by the first later pass that finds "
                  f"it recorded", flush=True)
        res.error = f"{token}: {res.error or ''}"
        return res

    def _hand_over_all(why: str):
        """ROUTE B with a debt and nothing merged (a refusal, a refused merge): `partial` WITH every
        stored key, and the refusal text. Without the store there is nothing to derive: the mark is
        released by name. A store that cannot be read keeps the mark."""
        if not blob.exists(path):
            _save_owed(out_dir, {})
            raise DefinitiveError(f"{why}; owed mark RELEASED: the store holds no object, so nothing "
                                  f"can be derived")
        try:
            t = blob.read_table(path, columns=["series_key", "obs_date"])
        except Exception as e:                                   # noqa: BLE001
            raise DefinitiveError(f"{why}; owed mark kept - the store could not be read to hand its "
                                  f"keys over ({e!r})") from e
        cur = _fold_cursors(t)
        print(f"[{SOURCE}] {why} - ROUTE B: {len(cur):,} owed key(s) handed over anyway", flush=True)
        return _handed(Result(
            status="partial", obs=t.num_rows,
            last_obs_date=pc.max(t.column("obs_date")).as_py() if t.num_rows else None,
            new_vintage="date-tail", series_cursors=cur, changed_keys=dict(cur),
            error=f"{why}; {len(cur):,} key(s) owed by an earlier merge handed over to the CSV step"))

    def _refuse(label: str):
        tally.structural_unit(label)
        try:
            return _fin()                                 # raises DefinitiveError (structural)
        except DefinitiveError as e:
            if not debt["on"]:
                raise                                     # nothing changed, nothing owed: as before
            return _hand_over_all(str(e))

    sdir = _slice_dir(source_id)
    os.makedirs(sdir, exist_ok=True)
    for f in os.listdir(sdir):                        # a kill's leftovers are never reusable
        if f.endswith(".sdmx.tmp") or f.endswith(".parquet.tmp") or f.endswith(".json.tmp"):
            try:
                os.remove(os.path.join(sdir, f))
            except OSError:
                pass
    if os.path.isdir(out_dir):                        # a kill inside an owed-mark write (local mirror)
        for f in os.listdir(out_dir):
            if f.startswith(OWED_FILE + ".") and f.endswith(".tmp"):
                try:
                    os.remove(os.path.join(out_dir, f))
                except OSError:
                    pass

    # THE OWED MARK, before anything else can return (see the module docstring). Fails closed.
    owed = _load_owed(out_dir)
    if owed is None:
        if before > 0:
            # ABSENT beside an existing store: owed ONCE (main wrote no mark and may have left stale CSVs).
            owed = _mark(_OWED)
            print(f"[{SOURCE}] no owed mark beside a store of {before:,} rows (the first pass after the "
                  f"switch from main, or a lost mark) - every stored key goes to the CSV step once",
                  flush=True)
        else:
            owed = {}
    if owed.get("form") == _HANDED:
        recorded, how = _handed_recorded(source_id, owed["token"])
        if recorded:
            print(f"[{SOURCE}] owed mark settled: {owed['token']} was recorded by the orchestrator after "
                  f"its CSV step", flush=True)
            _save_owed(out_dir, {})
            owed = {}
        else:
            debt["on"] = True
            print(f"[{SOURCE}] owed mark {owed['token']} {how} - every stored key is handed over again",
                  flush=True)
    elif owed:
        debt["on"] = True
        print(f"[{SOURCE}] owed mark '{owed['form']}' ({owed['utc']}): an earlier merge's keys were never "
              f"handed over - every stored key goes to the CSV step", flush=True)

    pinned = _pinned_dims(path, before)
    smin = _store_min_date(path)
    if smin is not None and smin.year < FIRST_START:
        return _refuse(f"{flow}: the store holds data from {smin.isoformat()}, before the first window's "
                       f"start {FIRST_START}; refused rather than leave it unrefreshed (move FIRST_START)")

    pulled: list = []

    def _stop(why: str, todo: list):
        for lab in pulled:
            tally.empty_unit(f"{lab} pulled, kept for the merge")
        for lab in todo:
            tally.deferred_unit(lab)
        print(f"[{SOURCE}] STOP before the window end: {why}; {len(todo)} deferred; kept slices stay "
              f"on disk for a later pass", flush=True)
        return _fin(merged_rows=0)

    def _todo(i: int, have: dict) -> list:
        return [x[0] for x in plan[i:] if x[0] not in have] + ["merge"]

    try:
        ver = _flow_version(flow, dl)
    except _Stop as e:
        return _stop(str(e), _todo(0, {}))
    now = time.time()
    marker = _load_marker(sdir)
    kept = marker.get("slices", {}) if marker.get("store_rows") == before else {}
    if marker and marker.get("store_rows") != before:
        print(f"[{SOURCE}] store changed since the kept slices began ({marker.get('store_rows')} -> "
              f"{before} rows) - discarding them", flush=True)
    # W6: lower bounds left by slices the window end cut (see the module docstring)
    old_cut = marker.get("cut") if isinstance(marker.get("cut"), dict) else {}
    cut = {}
    for lab, a, b, _kind in plan:
        rec = old_cut.get(lab)
        if (isinstance(rec, dict) and rec.get("start") == a and rec.get("end") == b
                and isinstance(rec.get("cut_ts"), (int, float)) and not isinstance(rec.get("cut_ts"), bool)
                and isinstance(rec.get("elapsed_s"), (int, float))
                and not isinstance(rec.get("elapsed_s"), bool)
                and math.isfinite(rec["elapsed_s"]) and rec["elapsed_s"] > 0
                and now - rec["cut_ts"] <= age_s["open"]):
            cut[lab] = {"start": a, "end": b, "cut_ts": rec["cut_ts"], "elapsed_s": float(rec["elapsed_s"])}
    if pinned is None:
        seen = {tuple(r["dims"]) for r in kept.values() if isinstance(r, dict) and r.get("dims")}
        if len(seen) == 1:                            # first ingest, resumed under its own dims
            pinned = list(next(iter(seen)))
    have = {}
    for lab, a, b, kind in plan:
        rec = kept.get(lab)
        p = _slice_path(sdir, lab)
        if (kind == "newest" or not isinstance(rec, dict) or rec.get("start") != a or rec.get("end") != b
                or not isinstance(rec.get("fetched_ts"), (int, float)) or not rec.get("rows")
                or now - rec["fetched_ts"] > age_s[kind] or rec.get("dims") != pinned
                or not os.path.exists(p)):
            continue
        try:
            if pq.read_metadata(p).num_rows != rec["rows"]:
                continue
        except Exception:                             # noqa: BLE001
            continue
        have[lab] = rec
    _clear_dir(sdir, keep={os.path.basename(_slice_path(sdir, lab)) for lab in have} | {MARKER})
    for lab in have:
        cut.pop(lab, None)
    marker = {"flow": flow, "version": ver, "store_rows": before, "slices": dict(have), "cut": dict(cut)}
    _save_marker(sdir, marker)
    if have:
        print(f"[{SOURCE}] resuming: {len(have)} kept slice(s): {', '.join(have)}", flush=True)

    # The margin starts from the slowest KEPT slice (its recorded download + parse), so a pass that
    # resumes does not start a long decade on the 5-min floor alone and throw the partial body away.
    longest = max([float(r.get("download_s") or 0) + float(r.get("parse_s") or 0)
                   for r in have.values()] or [0.0])
    last_req = None
    empties: list = []
    for i, (lab, a, b, kind) in enumerate(plan):
        if lab in have:
            continue
        left = dl.left()
        lb = cut[lab]["elapsed_s"] if lab in cut else 0.0     # W6: this slice's own measured lower bound
        add = reserve_s if kind == "newest" else 0.0
        need = max(floor_s, 2 * longest, 2 * lb) + add
        if left is not None and left < need:
            why = f"{left / 60:.1f} min left, under the {need / 60:.1f}-min margin before slice {lab}"
            if lb and win_s is not None and win_s < need:
                # The sentence names the term that set the margin, never one the window clears (AR-410).
                if win_s < lb:
                    why += (f"; window shorter than the slice's measured lower bound (the pass's window "
                            f"{win_s / 60:.1f} min, the slice cut after {lb / 60:.1f} min on an earlier "
                            f"pass): it cannot finish in a window this short")
                elif win_s < 2 * lb:
                    why += (f"; the pass's window ({win_s / 60:.1f} min) is under 2 x the slice's measured "
                            f"lower bound ({lb / 60:.1f} min, cut on an earlier pass): it cannot start in a "
                            f"window this short")
                else:
                    why += (f"; the pass's window ({win_s / 60:.1f} min) clears 2 x the slice's measured "
                            f"lower bound ({2 * lb / 60:.1f} min) but is under the {need / 60:.1f}-min "
                            f"margin, set by {_margin_terms(floor_s, longest, lb, add)}")
                why += "; release: a longer window (operator)"
            return _stop(why, _todo(i, have))
        url = f"{ing.BASE}/data/{agency},{flow}/all?startPeriod={a}&endPeriod={b}"
        xml_tmp = _slice_path(sdir, lab) + ".sdmx.tmp"
        t0 = None
        try:
            if last_req is not None and PACE_S - (time.time() - last_req) > 0:
                dl.sleep(PACE_S - (time.time() - last_req))
            t0 = time.time()
            nb = _get_to_file(url, xml_tmp, dl)
            last_req = t1 = time.time()
            tbl, dims, st = _parse_slice(xml_tmp, flow, a, b, pinned)
        except _Stop as e:
            if t0 is not None:                        # W6: the cut slice's elapsed time is a lower bound
                el = max(time.time() - t0, cut[lab]["elapsed_s"] if lab in cut else 0.0)
                cut[lab] = {"start": a, "end": b, "cut_ts": time.time(), "elapsed_s": round(el, 1)}
                marker["cut"] = dict(cut)
                _save_marker(sdir, marker)
                e = f"{e} (cut after {el:.1f}s; recorded as this slice's lower bound)"
            return _stop(f"slice {lab}: {e}", _todo(i, have))
        except urllib.error.HTTPError as e:
            if e.code in (400, 404):
                return _refuse(f"window {lab}: HTTP {e.code} for startPeriod={a}&endPeriod={b} - this "
                               f"window refused (not a flow move: the other windows were not asked)")
            tally.transient_unit(f"{lab}: HTTP {e.code}")
            return _fin(merged_rows=0)
        except TransientError as e:
            for x in pulled:
                tally.empty_unit(f"{x} pulled, kept for the merge")
            tally.transient_unit(f"{lab}: {e}"[:200])
            return _fin(merged_rows=0)
        finally:
            try:
                if os.path.exists(xml_tmp):
                    os.remove(xml_tmp)
            except OSError:
                pass
        t2 = time.time()
        if isinstance(tbl, str):
            return _refuse(_refuse_dims(lab, dims, pinned))
        rows = 0 if tbl is None else tbl.num_rows
        if rows:
            if pinned is None:
                pinned = dims                         # first ingest: the later slices must agree
            sp = _slice_path(sdir, lab)
            pq.write_table(tbl, sp + ".tmp", compression="zstd")
            os.replace(sp + ".tmp", sp)
        else:
            empties.append(lab)
        longest = max(longest, t2 - t0)
        cut.pop(lab, None)
        have[lab] = {"start": a, "end": b, "fetched_ts": t0, "rows": rows, "bytes": nb, "dims": pinned,
                     "download_s": round(t1 - t0, 1), "parse_s": round(t2 - t1, 1)}
        pulled.append(lab)
        marker["slices"] = {k: v for k, v in have.items() if v.get("rows")}
        marker["cut"] = dict(cut)
        _save_marker(sdir, marker)
        print(f"[{SOURCE}] slice {i + 1}/{len(plan)} {lab}: {nb:,} B, download {t1 - t0:.1f}s, parse "
              f"{t2 - t1:.1f}s, {rows:,} obs / {st.get('series', 0):,} series"
              + (f"; {st['outside']:,} obs outside the window dropped" if st.get("outside") else "")
              + (f"; WARNING {st['dup']:,} duplicate (key, date) rows in one document" if st.get("dup")
                 else "")
              + (f"; WARNING dropped bad_date={st['bad_date']:,} bad_value={st['bad_value']:,}"
                 if st.get("bad_date") or st.get("bad_value") else ""), flush=True)

    left = dl.left()
    if left is not None and left < reserve_s:
        return _stop(f"{left / 60:.1f} min left, under the {reserve_s / 60:.0f}-min merge reserve; "
                     f"every closed slice is kept", ["merge"])

    parts = [_slice_path(sdir, lab) for lab, _, _, _ in plan if have[lab]["rows"]]
    if not parts:
        _clear_dir(sdir)
        return _refuse(f"{flow}: every one of {len(plan)} slices was empty")
    tm = time.time()
    fresh = pa.concat_tables([pq.read_table(p) for p in parts]).combine_chunks()
    if fresh.num_rows < before // 2:
        _clear_dir(sdir)
        return _refuse(f"{flow}: sliced pull assembled {fresh.num_rows:,} rows, under the {before // 2:,} "
                       f"floor (half the store); refusing to publish a partial as complete")
    # TWO-PHASE MARK, phase 1: "merging" (or "owed", if a debt is carried) BEFORE the merge publishes, so
    # a kill between the publish and the Result leaves a mark a later pass hands over. FAIL CLOSED: if
    # the mark is not stored, nothing is merged (the kept slices stay; a later pass merges them).
    if not _save_owed(out_dir, _mark(_OWED if debt["on"] else _MERGING)):
        for lab in pulled:
            tally.empty_unit(f"{lab} pulled, kept for the merge")
        tally.transient_unit("merge: pre-merge owed mark not saved - not merged, kept slices stay")
        return _fin(merged_rows=0)
    # (c) THE CHANGE SIGNAL: the merge's own report. The cap is PER CALL, this table's own size, so the
    # report can never refuse on size; if it refuses anyway (before any I/O, a ValueError), the plain
    # merge runs and the change is UNKNOWN, which hands every key over - never a truncated list.
    try:
        try:
            _n, _last, changed = merge.merge_and_write(path, fresh, mode="merge", dedup_keys=_base.DEDUP,
                                                       report_changed_keys=True,
                                                       changed_keys_cap=max(fresh.num_rows, 1))
        except ValueError as e:
            if not str(e).startswith("report_changed_keys refused"):
                raise
            print(f"[{SOURCE}] change report refused ({e}) - plain merge, every key handed over",
                  flush=True)
            merge.merge_and_write(path, fresh, mode="merge", dedup_keys=_base.DEDUP)
            changed = None
    except DefinitiveError as e:
        # merge refuses BEFORE it writes (never-shrink, a dropped column): nothing was published, so
        # the mark goes back to what it was. With a debt, ROUTE B hands it over; without, as before.
        _save_owed(out_dir, owed)
        if not debt["on"]:
            raise
        return _hand_over_all(f"{flow}: merge refused ({e})")
    t_merged = time.time()
    owes = debt["on"] or changed is None or bool(changed)
    # TWO-PHASE MARK, phase 2: the merge landed. A change (or a carried debt) is OWED until a Result
    # carries it; an identical merge owes nothing. A failed save leaves "merging": an over-report.
    _save_owed(out_dir, _mark(_OWED) if owes else {})
    tbl = blob.read_table(path)
    added = max(0, tbl.num_rows - before)
    if added:
        tally.added_unit(added, flow)
    elif changed is None or changed:
        tally.revised_unit(flow)                      # served values changed, no row added
    else:
        tally.added_unit(0, flow)                     # merged and identical
    # Cursors stay ALL keys (per-series freshness, health's frontier), as before PR B.
    cursors = _fold_cursors(tbl)
    # THE DERIVE SET: the merge-measured changed keys; EVERY key when a debt is carried (an owed mark
    # does not record which keys an earlier merge changed) or the change is unknown.
    ck = dict(cursors) if (debt["on"] or changed is None) else {k: v for k, v in changed.items()}
    if before == 0:
        blob.write_bytes_atomic(path + ".dims.json", json.dumps(
            {"flow": flow, "agency": agency, "key_dims": pinned}, indent=1).encode("utf-8"))
    print(f"[{SOURCE}] merged {fresh.num_rows:,} rows from {len(parts)} slice(s): concat+merge "
          f"{t_merged - tm:.1f}s, read-back+cursors {time.time() - t_merged:.1f}s; store "
          f"{tbl.num_rows:,} rows; {len(changed) if changed is not None else 'unknown'} key(s) changed"
          + (f", {len(ck):,} handed over (owed mark)" if debt["on"] else "")
          + (f"; empty windows {', '.join(empties)}" if empties else ""), flush=True)
    _clear_dir(sdir)                                  # slices go only AFTER the merge succeeded
    mx = pc.max(tbl.column("obs_date")).as_py()
    try:
        res = finalize(tally, tbl.num_rows, mx, source=source_id, series_cursors=cursors)
    except DefinitiveError as e:
        if not owes:
            raise                                     # nothing changed, nothing owed: as before
        # ROUTE B (j): partial WITH the cursors and the refusal text, never a lost hand-over.
        print(f"[{SOURCE}] {e} - ROUTE B: {len(ck):,} changed/owed key(s) handed over anyway", flush=True)
        res = Result(status="partial", obs=tbl.num_rows, last_obs_date=mx, new_vintage="date-tail",
                     series_cursors=cursors,
                     error=f"{e}; {len(ck):,} changed/owed key(s) handed over to the CSV step")
    res.changed_keys = ck
    if debt["on"] and res.status == "no_change":
        # Nothing new, but owed keys are handed over: the CSV step must run, and only ok/partial reach
        # it (orchestrate._should_derive_csvs).
        res.status = "ok"
        res.error = f"{res.error}; {len(ck):,} key(s) carried from an earlier merge's owed mark"
    return _handed(res) if owes else res
