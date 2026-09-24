"""statcan LANE - refresh every released cube whole, then serve its catalogued parts. No time budget.

WHY A LANE, NOT A PASS (2026-09-23; ledger R1091-R1094; NUMBERS.md 2026-09-23 rows). Inside a
local-heavy pass statcan gets a 20-76 minute slot, and the pass itself is clamped to the next cloud
state-writer window. A whole-table refresh costs ~4.3 MB/s to download, ~187k rows/s to parse and
~171k rows/s to merge, so 108 backlog cubes need more than any slot and 12100152 alone is ~380 min.
Four review rounds of budget machinery (a head slot, giants, an admission flag) each failed on a new
edge. The served side was worse: users read TABLE-grain parts, and the orchestrator's inline derive
re-scans the whole cube per part (1.9-4.3 s/part) with ~0 minutes left after statcan's merge.

So this process owns statcan's store region while it works. It never writes state.db, so it needs
no state-writer window. RELAUNCH_GUARD.ps1 keeps ONE of it alive; it iterates every IDLE_SLEEP_S,
taking logs/statcan_writer.lock for each iteration only (see main), and an idle iteration does not
even call StatCan (ENUM_MIN_INTERVAL_MIN). The orchestrator's statcan unit is now a REPORTER that
reads what this lane publishes (updater/strategies/fetchers/statcan.py).

PER CUBE, OLDEST RELEASE FIRST (size breaks ties; smallest-first starved the old giants, R1092):
  1. MERGE  - the whole table (getFullTableDownloadCSV), parsed by the bulk ingester's own
              parse_zip_to_parquet, proven complete against StatCan's counts, proven keyed like
              the stored cube, merged with merge_and_write_bounded (keep-old, never-shrink,
              atomic). All of that is the fetcher's code, imported, not copied.
  2. RECORD - `merged` and the serve debt are saved BEFORE anything is served (two-phase: a kill
              between the merge and the serve leaves the debt on record, never a cube that
              looks served).
  3. SERVE  - the cube's CATALOGUED parts, re-derived under the split it is already served with
              (read-only from _split_map.json - this lane never writes the map), in ONE sorted
              scan, byte-identical to tools/derive_statcan_tables.py (its own part_expr, unit_id,
              csv_key and _rows_csv). Only ids the catalogue holds are written. Parts the refresh
              CREATES and catalogued parts it STRANDS go to _catalogue_debt.json: the catalogue
              step is the D1 sync, frozen by the owner's decision, so they are recorded, not acted
              on.
  4. `served` is saved. Progress is published after every phase.

WHAT IT DOES NOT DO (v1). Brand-new cubes (released, not held) are NOT ingested: they are listed
under `new_cubes` in the debt file and counted by the reporter. Ingesting them is the same fetch,
but nothing could serve them until catalogued, and why each is absent has not been checked.
"""
from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import json
import os
import queue
import sys
import threading
import time
import traceback

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import blob, config, merge, writer_lock           # noqa: E402
from updater.errors import DefinitiveError, TransientError      # noqa: E402
from updater.strategies.fetchers import statcan as sc           # noqa: E402

LOCK_NAME = "statcan_writer"
STATE = os.path.join(sc.OUT_DIR, "_lane_state.json")            # blob-routed
PROGRESS = os.path.join(sc.OUT_DIR, "_lane_progress.json")      # blob-routed: the reporter reads it
DEBT = os.path.join(sc.OUT_DIR, "_catalogue_debt.json")         # blob-routed
# LOCAL and READ-ONLY. tools/derive_statcan_tables.py writes it; a scoped run of that tool killed
# mid-way can still truncate it on origin/main (design review finding 2), so this lane never opens
# it for writing and re-reads it every launch.
SPLIT_MAP = os.path.join(ROOT, "data", "clean_full", "statcan", "_split_map.json")
CATALOG_DB = os.path.join(ROOT, "data", "catalog.db")
LOCAL_PROGRESS = os.path.join(ROOT, "logs", "statcan_lane.progress.json")
PREFIX = "series"

# THE BACKLOG'S START. 505 cubes were released between 2026-07-29 and 2026-09-17 while the fetcher
# read a single-day feed as a since-feed (statcan.py `_changed_releases`); nothing older is owed.
FLOOR = "2026-07-29"
# An idle launch (nothing owed, nothing due for retry) calls StatCan at most this often. The guard
# ticks every ~5 min, and the enumeration is one ~8,270-cube request.
ENUM_MIN_INTERVAL_MIN = 60.0
# The production derive's row cap (RELAUNCH_GUARD.ps1's retired derive_statcan argv: --max-rows
# 3000000). A cube served WHOLE that has grown past it is still served whole - it is catalogued,
# and a stale object is worse than a large one - and booked as debt, never re-split here.
MAX_ROWS = 3_000_000
PUT_WORKERS = 16
# Between iterations. Each iteration beats (so an idle lane beats every 5 min, well inside the
# reporter's 3-hour idle allowance) and calls StatCan only past ENUM_MIN_INTERVAL_MIN.
IDLE_SLEEP_S = 300
# A transient failure is retried after 10 min, doubling, capped at 12 h.
BACKOFF_MIN, BACKOFF_MAX_MIN = 10.0, 12 * 60.0
# This many transient failures in a row (10+20+40+80+160 min of backoff, ~5 h) and the cube is named
# to the reporter as FAILING - red - with its last error (lane review P3).
TRANSIENT_RED_AFTER = 6

_TOOL = os.path.join(ROOT, "tools", "derive_statcan_tables.py")


def _tool():
    """tools/derive_statcan_tables.py, for its PURE functions only (part_expr, unit_id, csv_key,
    _rows_csv): the byte format and the id scheme. Importing it runs no derive and touches no map."""
    m = sys.modules.get("_derive_statcan_tables_for_lane")
    if m is None:
        spec = importlib.util.spec_from_file_location("_derive_statcan_tables_for_lane", _TOOL)
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        sys.modules["_derive_statcan_tables_for_lane"] = m
    return m


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _iso(t: dt.datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso(s):
    try:
        return dt.datetime.strptime(str(s), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------- #
# state, progress, debt - all blob-routed JSON, written atomically
# --------------------------------------------------------------------------- #
def _read_json(path, default):
    raw = blob.read_bytes(path)
    if raw is None:
        return default
    return json.loads(raw.decode("utf-8"))        # a corrupt state file raises: never silently reset


def load_state() -> dict:
    """BLOB-ROUTED (tests/test_giant_state_blob_routed.py pins it): the reporter reads this from R2.
    A corrupt file raises rather than resetting - a reset would re-owe nothing and hide the backlog."""
    raw = blob.read_bytes(STATE)
    st = json.loads(raw.decode("utf-8")) if raw is not None else {}
    st.setdefault("version", 1)
    st.setdefault("floor", FLOOR)
    st.setdefault("cubes", {})
    st.setdefault("parsed_rows", {})
    st.setdefault("new_cubes", {})
    st.setdefault("last_enum_utc", None)
    return st


def save_state(st: dict) -> None:
    blob.write_bytes_atomic(STATE, json.dumps(st, sort_keys=True).encode("utf-8"))


class Progress:
    """What the lane is doing, written FROM THE WORK PATH at every phase boundary and every
    PROGRESS_EVERY_PUTS parts - never from a timer thread, which would beat while the work hung.
    The reporter allows max(2 h, the current cube's estimate x 1.5) between beats."""
    PROGRESS_EVERY_PUTS = 500

    def __init__(self, st):
        self.st = st
        self.d = {"pid": os.getpid(), "started_utc": _iso(_now()), "state": "starting",
                  "current": None, "counters": {"cubes_merged": 0, "cubes_served": 0,
                                                "parts_put": 0, "rows_merged": 0,
                                                "bytes_downloaded": 0}}
        # THE PROGRESS STAMP OUTLIVES THE ITERATION (lane review round 3, P8/P9): a new Progress is
        # built every 5-minute iteration and every relaunch, and one that started without the
        # stamp published an idle beat with none - a healthy drain then read "NOT PROGRESSING"
        # (red) between cubes. It is kept in the lane STATE, saved after every cube.
        if st.get("last_progress_utc"):
            self.d["last_progress_utc"] = st["last_progress_utc"]

    def beat(self, state=None, current="keep", **counters):
        if state is not None:
            self.d["state"] = state
        if current != "keep":
            self.d["current"] = current
        for k, v in counters.items():
            self.d["counters"][k] = self.d["counters"].get(k, 0) + v
        if any(v for k, v in counters.items() if k in ("cubes_merged", "cubes_served", "parts_put")):
            # PROGRESS, not a beat: the reporter reads a lane that is behind its SLA but still
            # finishing cubes as DRAINING (amber), and one that has stopped finishing them as red
            # (design condition 7: age of the oldest owed release AND progress).
            self.d["last_progress_utc"] = self.st["last_progress_utc"] = _iso(_now())
        self.d["beat_utc"] = _iso(_now())
        self.d["owed"] = summarise(self.st)
        body = json.dumps(self.d, sort_keys=True).encode("utf-8")
        blob.write_bytes_atomic(PROGRESS, body)
        try:
            os.makedirs(os.path.dirname(LOCAL_PROGRESS), exist_ok=True)
            tmp = LOCAL_PROGRESS + ".tmp"
            with open(tmp, "wb") as fh:
                fh.write(body)
            os.replace(tmp, LOCAL_PROGRESS)
        except OSError:
            pass                        # the local copy is a convenience; the blob one is the beat


def summarise(st: dict) -> dict:
    """The counts the reporter judges: owed merges, owed serves, the OLDEST owed release (held
    against the SLA), and three NAMED lists that need a human and so are kept out of that clock -
    quarantined cubes, serve-refused cubes, and cubes failing transiently TRANSIENT_RED_AFTER times
    in a row (lane review P3: a cube failing for ever read `ok` until its release aged out)."""
    owe_m, owe_s, quar, refused, failing, oldest = [], [], [], [], [], None
    for p, c in st.get("cubes", {}).items():
        rel = c.get("release")
        if c.get("quarantined"):
            quar.append(p)
            continue
        if int(c.get("transient_fails") or 0) >= TRANSIENT_RED_AFTER:
            failing.append(f"{p} ({c.get('transient_fails')}x: {str(c.get('last_error'))[:100]})")
        if merge_owed(c):
            owe_m.append(p)
            oldest = rel if oldest is None or (rel and rel < oldest) else oldest
        elif serve_owed(c):
            if c.get("serve_refusal"):
                refused.append(f"{p} ({c['serve_refusal'][:100]})")
                continue
            owe_s.append(p)
            m = c.get("merged")
            oldest = m if oldest is None or (m and m < oldest) else oldest
    new = st.get("new_cubes", {})
    return {"merge": len(owe_m), "serve": len(owe_s), "quarantined": sorted(quar),
            "serve_refused": sorted(refused), "failing": sorted(failing),
            "new_cubes": len(new), "oldest_new_cube": min(new.values()) if new else None,
            "oldest_owed_release": oldest}


def merge_owed(c: dict) -> bool:
    rel = c.get("release")
    return bool(rel) and (c.get("merged") is None or c["merged"] < rel)


def serve_owed(c: dict) -> bool:
    m = c.get("merged")
    return bool(m) and (c.get("served") is None or c["served"] < m)


def _due(c: dict, now: dt.datetime) -> bool:
    ra = _parse_iso(c.get("retry_after"))
    return ra is None or ra <= now


# --------------------------------------------------------------------------- #
# serving - one sorted scan per cube, catalogued ids only, no map writes
# --------------------------------------------------------------------------- #
def catalogued_ids(pid: str, catalog_db: str | None = None) -> set:
    """Every catalogued id of one cube, by a PRIMARY-KEY RANGE read of the local catalogue. The
    product id is a fixed 8 digits, so 'statcan:<pid>' and 'statcan:<pid>#...' both sort inside
    [lo, lo + '$') ('$' 0x24 sorts just after '#' 0x23)."""
    import sqlite3                                                   # noqa: PLC0415
    lo = _tool().unit_id(str(pid))
    con = sqlite3.connect(f"file:{catalog_db or CATALOG_DB}?mode=ro", uri=True, timeout=180)
    try:
        return {r[0] for r in con.execute(
            "SELECT series_id FROM series WHERE series_id >= ? AND series_id < ?", (lo, lo + "$"))}
    finally:
        con.close()


def _split_columns(dim: str) -> list:
    if dim.startswith("coordinate:"):
        return ["coordinate"]
    return dim.split("+", 1) if "+" in dim else [dim]


def serve_plan(pid: str, n_rows: int, schema_names, smap: dict, catalogued: set):
    """(dim, refusal, notes). dim None = the cube is served WHOLE; a refusal means nothing may be
    written (the cube's ids cannot be reproduced) and the cube stays serve-owed as debt."""
    served_whole = _tool().unit_id(str(pid)) in catalogued
    served_parts = any("#" in s for s in catalogued)
    entry = smap.get(str(pid)) or {}
    notes = []
    if not catalogued:
        # NOT A REFUSAL (lane review P2): 7 stored cubes have no catalogued id today (18100103,
        # 34100102 and the 5 the derive refuses). Nothing is written - no id reaches a user - and
        # every id it would emit is catalogue debt; refusing kept it owed, then RED, for ever.
        notes.append("not catalogued: nothing written, every id it emits is catalogue debt")
        dim = entry.get("dim") or None
        if dim and not all(c in set(schema_names) for c in _split_columns(dim)):
            dim = None
        return dim, None, notes
    if entry.get("dim"):
        dim = entry["dim"]
        if not all(c in set(schema_names) for c in _split_columns(dim)):
            return None, f"its recorded split {dim!r} names a column the refreshed cube lacks", notes
        if served_whole:
            notes.append("catalogued whole AND as parts; the whole id is not refreshed here")
        return dim, None, notes
    if served_parts:
        return None, "served as parts but no split is recorded in _split_map.json", notes
    if n_rows > MAX_ROWS:
        notes.append(f"served whole at {n_rows:,} rows, over the {MAX_ROWS:,} derive cap")
    return None, None, notes


def serve_cube(pid: str, local_parquet: str, smap: dict, catalogued: set, put, progress=None,
               memory_limit: str = "4GB") -> dict:
    """Re-derive cube `pid`'s CATALOGUED ids from `local_parquet` and hand each body to
    `put(key, gzipped_body)`. The SELECT, ORDER BY, duplicate collapse and body bytes are
    tools/derive_statcan_tables.py's own (19/19 parts byte-identical to core.derive_csv, NUMBERS.md
    2026-09-23). Returns {status, put, put_errors, new, vanished, null_part_rows, notes, refusal}."""
    import duckdb                                                    # noqa: PLC0415
    import pyarrow.parquet as pq                                     # noqa: PLC0415
    from core import r2_util                                         # noqa: PLC0415
    t = _tool()
    md = pq.read_metadata(local_parquet)
    dim, refusal, notes = serve_plan(pid, md.num_rows, md.schema.to_arrow_schema().names, smap,
                                     catalogued)
    out = {"status": "ok", "put": 0, "put_errors": 0, "new": [], "vanished": [],
           "null_part_rows": 0, "notes": notes, "refusal": refusal}
    if refusal:
        out["status"] = "refused"
        return out
    f = local_parquet.replace("\\", "/").replace("'", "''")
    if dim:
        sel = f"{t.part_expr(dim)} AS part, series_key, obs_date, value"
        order = "part, series_key, obs_date, value"
    else:
        sel = "'' AS part, series_key, obs_date, value"
        order = "series_key, obs_date, value"
    emitted: set = set()
    q: queue.Queue = queue.Queue(maxsize=64)
    lock = threading.Lock()
    STOP = object()

    def worker():
        while True:
            item = q.get()
            try:
                if item is STOP:
                    return
                key, body = item
                try:
                    put(key, body)
                    with lock:
                        out["put"] += 1
                        n = out["put"]
                    if progress is not None and n % Progress.PROGRESS_EVERY_PUTS == 0:
                        with lock:
                            progress.beat(parts_put=Progress.PROGRESS_EVERY_PUTS)
                except Exception as e:                               # noqa: BLE001
                    with lock:
                        out["put_errors"] += 1
                        if out["put_errors"] <= 3:
                            print(f"  [lane] {pid}: PUT FAILED {key}: {str(e)[:120]}", flush=True)
            finally:
                q.task_done()

    # A PRIVATE, CAPPED SPILL (lane review item 5): the ORDER BY over a cube of up to ~201M rows
    # spills, and an uncapped spill on the system temp can fill the disk the store copy and the
    # merge need. The merge's own helpers: a per-call directory (R228) and a cap of free space
    # minus the reserve, which refuses up front - caught by the caller as transient.
    import shutil                                                    # noqa: PLC0415
    spill = merge._bounded_spill_dir()
    try:
        cap_mb = merge._bounded_temp_cap(spill, 0) // 1_000_000
    except Exception:
        shutil.rmtree(spill, ignore_errors=True)
        raise
    threads = [threading.Thread(target=worker, daemon=True) for _ in range(PUT_WORKERS)]
    for th in threads:
        th.start()
    con = duckdb.connect()
    try:
        con.execute(f"SET memory_limit='{memory_limit}'")
        con.execute("SET temp_directory='%s'" % spill.replace("\\", "/").replace("'", "''"))
        con.execute(f"SET max_temp_directory_size='{cap_mb}MB'")
        con.execute("SET preserve_insertion_order=false")
        con.execute("SET enable_progress_bar=false")
        cur = con.execute(f"SELECT {sel} FROM read_parquet('{f}') "
                          f"WHERE value IS NOT NULL AND obs_date IS NOT NULL ORDER BY {order}")
        cur_part, rows, last = None, [], None

        def flush(part):
            if not rows:
                return
            if dim and part in (None, ""):
                out["null_part_rows"] += len(rows)     # no part id exists for a NULL split value
                return
            sid = t.unit_id(str(pid), part or None)
            emitted.add(sid)
            if sid not in catalogued:
                return                                 # a NEW id: debt, never written (see module)
            q.put((t.csv_key(PREFIX, sid), r2_util.gzip_bytes(t._rows_csv(rows))))

        while True:
            batch = cur.fetchmany(200_000)
            if not batch:
                break
            for part, k, d, v in batch:
                if part != cur_part:
                    flush(cur_part)
                    cur_part, rows, last = part, [], None
                if (k, d) == last:
                    rows[-1] = (k, d.isoformat(), v)
                    continue
                last = (k, d)
                rows.append((k, d.isoformat(), v))
        flush(cur_part)
    finally:
        con.close()
        for _ in threads:
            q.put(STOP)
        q.join()
        shutil.rmtree(spill, ignore_errors=True)
    out["new"] = sorted(emitted - catalogued)
    whole = t.unit_id(str(pid))
    # a cube served as parts keeps any catalogued whole id out of "vanished": it is not re-derived
    # here (noted above), and calling it stranded would invite retiring it
    out["vanished"] = sorted(s for s in catalogued - emitted if not (dim and s == whole))
    if out["put_errors"]:
        out["status"] = "put_failed"
    return out


def _r2_put():
    """put(key, body) for the econ-data bucket, with core.derive_csv's backoff. The body is already
    gzipped, and _put_with_backoff gzips again, so its raw put loop is reused on the gzipped bytes."""
    from core import r2_util                                         # noqa: PLC0415
    s3 = r2_util.client(write=True)

    def put(key, body):
        for attempt in range(7):
            try:
                s3.put_object(Bucket=blob.R2_BUCKET, Key=key, Body=body, ContentType="text/csv",
                              ContentEncoding="gzip")
                return
            except Exception as e:                                   # noqa: BLE001
                if attempt == 6:
                    raise
                print(f"  [lane] PUT retry {attempt + 1}/7 in {2 ** attempt}s ({str(e)[:70]})",
                      flush=True)
                time.sleep(2 ** attempt)
    return put


# --------------------------------------------------------------------------- #
# one cube
# --------------------------------------------------------------------------- #
def _backoff(c: dict, reason: str, now: dt.datetime) -> None:
    n = int(c.get("transient_fails") or 0) + 1
    c["transient_fails"] = n
    c["retry_after"] = _iso(now + dt.timedelta(minutes=min(BACKOFF_MIN * 2 ** (n - 1),
                                                            BACKOFF_MAX_MIN)))
    c["last_error"] = reason[:300]


def _deterministic(c: dict, pid: str, reason: str) -> None:
    rel = c.get("release")
    n = int(c.get("fails") or 0) + 1 if c.get("fail_release") == rel else 1
    c.update(fails=n, fail_release=rel, last_error=reason[:300])
    if n >= sc.QUARANTINE_AFTER:
        c["quarantined"] = True
        print(f"[lane] {pid}: QUARANTINED after {n} failures on its {rel} release - not attempted "
              f"again until StatCan re-releases it: {reason[:200]}", flush=True)


def merge_cube(pid: str, c: dict, st: dict, progress: Progress, now: dt.datetime) -> str:
    """Refresh one held cube's store object. Returns 'merged', 'unchanged', 'transient',
    'definitive' or 'absent'. On success sets c['merged'] (and c['served'] too when nothing a user
    reads changed)."""
    import pyarrow.parquet as pq                                     # noqa: PLC0415
    path = os.path.join(sc.OUT_DIR, f"{pid}.parquet")
    try:
        present = blob.exists(path)
    except Exception as e:                                           # noqa: BLE001
        _backoff(c, f"the store could not be asked: {type(e).__name__}: {e}", now)
        return "transient"
    if not present:
        if os.path.exists(os.path.join(sc.OUT_DIR, f"{pid}.done")):
            _backoff(c, f"the ingest record ({pid}.done) says it is held; the store does not "
                        f"have it - a store fault (R753), not a new cube", now)
            return "transient"
        return "absent"
    rel = c["release"]
    # THE MERGE IS RECORDED AS IN FLIGHT BEFORE IT CAN PUBLISH (lane review P1). The store object is
    # published inside merge_and_write_bounded and the state is saved after it; a kill, an R2 blip
    # or a refused os.replace between the two left the NEW store and the OLD state. The relaunch
    # then merged the identical table, saw nothing change and settled the cube as served with zero
    # PUTs - users kept the old CSVs for ever. A merge that finds a prior attempt still in flight
    # therefore serves, whatever the merge reports.
    prior_in_flight = c.get("merge_in_flight")
    c["merge_in_flight"] = rel
    save_state(st)
    progress.beat("merging", current={"pid": pid, "phase": "merge", "release": rel,
                                      "started_utc": _iso(_now()), "est_min": c.get("est_min")})
    copy = new_path = None
    try:
        try:
            copy = blob.local_copy(path)
            if copy is None:
                raise FileNotFoundError(path)
            before = pq.read_metadata(copy[0]).num_rows
        except Exception as e:                                       # noqa: BLE001
            _backoff(c, f"stored cube unreadable: {type(e).__name__}: {e}", now)
            return "transient"
        try:
            new_path, s = sc._fetch_cube_whole(int(pid))
        except TransientError as e:
            _backoff(c, f"whole-table fetch failed: {e}", now)
            return "transient"
        except DefinitiveError as e:
            _deterministic(c, pid, f"whole-table fetch refused: {e}")
            return "definitive"
        progress.beat(bytes_downloaded=int(s.get("zip_bytes") or 0))
        n_new, n_series = int(s["n_obs"]), int(s["n_series"])
        try:
            total, both = sc._key_overlap(copy[0], new_path)
        except Exception as e:                                       # noqa: BLE001
            _backoff(c, f"key check failed: {type(e).__name__}: {e}", now)
            return "transient"
        floor = int(st["parsed_rows"].get(pid, before))
        if s.get("needs_stored_floor") and n_new < floor:
            _deterministic(c, pid, f"the vectorless table has {n_new:,} rows, fewer than the "
                                   f"{floor:,} of its last accepted parse; not merged. If StatCan "
                                   f"really trimmed it, set parsed_rows[{pid}] in _lane_state.json")
            return "definitive"
        if total and both < total * sc.KEY_OVERLAP_MIN:
            _deterministic(c, pid, f"only {both:,} of {total:,} stored keys reappear "
                                   f"(< {sc.KEY_OVERLAP_MIN:.0%}): re-keyed upstream; not merged")
            return "definitive"
        reported = (not s.get("series_capped")) and n_series <= merge.CHANGED_KEYS_CAP
        try:
            if reported:
                n, md, ch = merge.merge_and_write_bounded(
                    path, new_path=new_path, dedup_keys=sc.DEDUP, report_changed_keys=True,
                    changed_keys_cap=max(n_new, 1), stored_copy=copy[0])
            else:
                n, md = merge.merge_and_write_bounded(path, new_path=new_path, dedup_keys=sc.DEDUP,
                                                      stored_copy=copy[0])
                ch = None
        except (OSError, MemoryError) as e:
            _backoff(c, f"merge failed on this machine: {type(e).__name__}: {e}", now)
            return "transient"
        except DefinitiveError as e:
            if sc._environmental(e):
                _backoff(c, f"merge failed on this machine: {e}", now)
                return "transient"
            _deterministic(c, pid, f"merge guard refused over {before:,} stored rows: {e}")
            return "definitive"
    finally:
        sc._remove_quietly(new_path)
        if copy is not None and copy[1]:
            sc._remove_quietly(copy[0])
    if s.get("needs_stored_floor"):
        st["parsed_rows"][pid] = n_new
    delta = max(0, n - before)
    first_visit = c.get("served") is None or bool(c.pop("not_held", None))
    c.update(merged=rel, max_obs=md or c.get("max_obs"), rows=n, fails=0, transient_fails=0,
             retry_after=None, last_error=None)
    c.pop("fail_release", None)
    c.pop("merge_in_flight", None)
    progress.beat(cubes_merged=1, rows_merged=delta)
    # "UNCHANGED, SO ALREADY SERVED" holds only against a serve THIS LANE recorded. On a cube's
    # first visit the served CSVs were built by whatever ran before - and the old fetcher merged
    # (+19,653 rows on 2026-08-02) while the inline derive got ~0 minutes (R1094), so they may lag
    # the store. First visits and interrupted merges always serve.
    changed = ch is None or bool(ch) or bool(delta) or first_visit or prior_in_flight is not None
    if not changed:
        c["served"] = rel                  # identical to what this lane last served
        return "unchanged"
    return "merged"


def serve_owed_cube(pid: str, c: dict, debt: dict, smap: dict, put, progress: Progress,
                    now: dt.datetime) -> str:
    """Serve one merged cube. Returns 'served', 'refused' (debt, stays owed) or 'transient'."""
    path = os.path.join(sc.OUT_DIR, f"{pid}.parquet")
    progress.beat("serving", current={"pid": pid, "phase": "serve", "release": c.get("merged"),
                                      "started_utc": _iso(_now()), "est_min": c.get("est_min")})
    try:
        cat = catalogued_ids(pid)
    except Exception as e:                                           # noqa: BLE001
        _backoff(c, f"catalogue unreadable: {type(e).__name__}: {e}", now)
        return "transient"
    copy = None
    try:
        try:
            copy = blob.local_copy(path)
            if copy is None:
                raise FileNotFoundError(path)
        except Exception as e:                                       # noqa: BLE001
            _backoff(c, f"merged cube unreadable for serving: {type(e).__name__}: {e}", now)
            return "transient"
        try:
            r = serve_cube(pid, copy[0], smap, cat, put, progress)
        except Exception as e:                                       # noqa: BLE001
            _backoff(c, f"serve scan failed: {type(e).__name__}: {e}", now)
            return "transient"
    finally:
        if copy is not None and copy[1]:
            sc._remove_quietly(copy[0])
    entry = {"release": c.get("merged"), "new": r["new"], "vanished": r["vanished"],
             "notes": r["notes"], "refusal": r["refusal"], "null_part_rows": r["null_part_rows"],
             "recorded_utc": _iso(_now())}
    if r["new"] or r["vanished"] or r["notes"] or r["refusal"]:
        debt.setdefault("cubes", {})[pid] = entry
    else:
        debt.setdefault("cubes", {}).pop(pid, None)
    if r["status"] == "put_failed":
        _backoff(c, f"{r['put_errors']} PUT(s) failed of {r['put'] + r['put_errors']}", now)
        return "transient"
    if r["status"] == "refused":
        # Nothing CAN be written until the catalogue or the split map changes - a human's job. The
        # cube is NAMED to the reporter as serve-refused (partial, like a quarantine), kept out of
        # the owed-age clock that would otherwise turn it into a permanent RED (lane review P2),
        # and retried on the slow backoff in case the map or catalogue has been fixed.
        c["serve_refusal"] = r["refusal"]
        c["retry_after"] = _iso(now + dt.timedelta(minutes=BACKOFF_MAX_MIN))
        return "refused"
    c.update(served=c.get("merged"), transient_fails=0, retry_after=None, last_error=None)
    c.pop("serve_refusal", None)
    if r["put"] % Progress.PROGRESS_EVERY_PUTS:
        progress.beat(parts_put=r["put"] % Progress.PROGRESS_EVERY_PUTS)   # the counter is exact
    progress.beat(cubes_served=1)
    print(f"[lane] {pid}: served {r['put']:,} part(s)"
          + (f"; {len(r['new'])} new / {len(r['vanished'])} stranded id(s) booked as debt"
             if r["new"] or r["vanished"] else ""), flush=True)
    return "served"


# --------------------------------------------------------------------------- #
# the run
# --------------------------------------------------------------------------- #
def _est_min(pid: str):
    try:
        b = blob.stored_size(os.path.join(sc.OUT_DIR, f"{pid}.parquet"))
    except Exception:                                                # noqa: BLE001
        return None, None
    if b is None:
        return None, None
    return b, round(b * sc.EST_SECONDS_PER_STORED_BYTE / 60.0, 1)


def order_owed(st: dict, now: dt.datetime) -> list:
    """Pids owing work and due, OLDEST owed release first, stored size breaking ties."""
    rows = []
    for p, c in st["cubes"].items():
        if c.get("quarantined") or not _due(c, now):
            continue
        if merge_owed(c):
            rows.append((c["release"], c.get("bytes") or 0, p))
        elif serve_owed(c):
            rows.append((c["merged"], c.get("bytes") or 0, p))
    return [p for _r, _b, p in sorted(rows)]


def run(enumerate_releases=None, put=None, now_fn=_now, max_cubes=None) -> dict:
    """One launch: enumerate (unless idle and recent), then work every owed cube to empty.
    Injected `enumerate_releases` / `put` are for tests; production uses StatCan and R2."""
    st = load_state()
    progress = Progress(st)
    now = now_fn()
    last = _parse_iso(st.get("last_enum_utc"))
    idle = not order_owed(st, now)
    if idle and last is not None and (now - last).total_seconds() < ENUM_MIN_INTERVAL_MIN * 60:
        progress.beat("idle", current=None)
        return {"enumerated": False, "worked": 0}
    rel = (enumerate_releases or sc._changed_releases)(dt.date.fromisoformat(st["floor"]))
    for pid, rt in rel.items():
        c = st["cubes"].setdefault(str(pid), {})
        if c.get("release") != rt:
            c["release"] = rt
            if c.get("quarantined") and c.get("fail_release") != rt:
                c.pop("quarantined", None)           # re-released: tried again
                c["fails"] = 0
    st["last_enum_utc"] = _iso(now)
    save_state(st)
    progress.beat("working", current=None)
    debt = _read_json(DEBT, {"cubes": {}, "new_cubes": {}})
    # DELIBERATELY LOCAL, not blob-routed: tools/derive_statcan_tables.py writes the map on this
    # machine only, and R2 holds no copy. Opened read-only; this lane never writes it.
    try:
        with open(SPLIT_MAP, encoding="utf-8") as fh:
            smap = json.load(fh)
    except (OSError, ValueError) as e:
        raise SystemExit(f"REFUSING: the split map {SPLIT_MAP} is unreadable ({e!r}); serving "
                         f"without it would re-key every split cube") from e
    if not isinstance(smap, dict) or not smap:
        raise SystemExit(f"REFUSING: the split map {SPLIT_MAP} holds no split decisions")
    put = put or _r2_put()
    # SIZES BEFORE THE ORDER: the tiebreak (and the reporter's per-cube allowance) needs each owed
    # cube's stored size, read once per cube (one HEAD under r2) and kept in state. Read inside the
    # loop, as first written, every first-visit cube tied at 0 bytes (mutation M6 survived).
    for pid in order_owed(st, now):
        c = st["cubes"][pid]
        if c.get("bytes") is None and merge_owed(c):
            c["bytes"], c["est_min"] = _est_min(pid)
    held = absent = worked = 0
    for pid in order_owed(st, now):
        if max_cubes is not None and worked >= max_cubes:
            break
        c = st["cubes"][pid]
        now = now_fn()
        if merge_owed(c):
            c["bytes"], c["est_min"] = _est_min(pid)       # fresh: a new release changes the size
            try:
                r = merge_cube(pid, c, st, progress, now)
            except Exception as e:                                   # noqa: BLE001
                _backoff(c, f"unexpected {type(e).__name__}: {e}", now)
                traceback.print_exc()
                r = "transient"
            if r == "absent":
                absent += 1
                st["new_cubes"][pid] = c["release"]
                debt.setdefault("new_cubes", {})[pid] = c["release"]
                c["merged"] = c["served"] = c["release"]   # nothing held: nothing to refresh
                c["not_held"] = True
            else:
                held += 1
            print(f"[lane] {pid}: merge {r}"
                  + (f" ({c.get('last_error')})" if r in ("transient", "definitive") else ""),
                  flush=True)
            save_state(st)                                   # BEFORE serving: two-phase
            blob.write_bytes_atomic(DEBT, json.dumps(debt, sort_keys=True).encode("utf-8"))
            if r not in ("merged",):
                worked += 1
                progress.beat()
                continue
        if serve_owed(c):
            r = serve_owed_cube(pid, c, debt, smap, put, progress, now_fn())
            save_state(st)
            blob.write_bytes_atomic(DEBT, json.dumps(debt, sort_keys=True).encode("utf-8"))
        worked += 1
        progress.beat()
    # THE STORE-ABSENT GUARD (R753): cubes released and NONE found in the store means the store
    # is unreachable from this backend, not that StatCan released only new cubes.
    if absent and not held:
        listed = [f for f in blob.list_parquets(sc.OUT_DIR) if not os.path.basename(f).startswith("_")]
        if not listed:
            progress.beat("store_unreachable", current=None)
            raise SystemExit(f"statcan lane: the store holds ZERO cubes under {sc.OUT_DIR} "
                             f"(backend={config.BACKEND}) while {absent} released cube(s) were "
                             f"looked for - the store is unreachable, not a quiet publisher")
    progress.beat("idle", current=None)
    return {"enumerated": True, "worked": worked, "held": held, "absent": absent}


def pin_backend(environ=None, cfg=None) -> None:
    """THE LANE WRITES THE SERVED STORE ON R2 (design review finding 1). A local-backend run would
    merge into this machine's mirror while the reporter reads R2. RELAUNCH_GUARD.ps1's `$jobs`
    launch passes no environment, so the lane SETS the backend when nothing chose one, and REFUSES
    when something chose another. blob reads the variable per call; config.BACKEND is set too so
    every message names the truth."""
    environ = os.environ if environ is None else environ
    cfg = config if cfg is None else cfg
    got = environ.get("AQUEDUCT_BACKEND")
    if got is None:
        environ["AQUEDUCT_BACKEND"] = "r2"
        cfg.BACKEND = "r2"
    elif got.strip().lower() != "r2":
        raise SystemExit(f"REFUSING: AQUEDUCT_BACKEND must be r2 for the statcan lane (it is {got!r})")
    else:
        cfg.BACKEND = "r2"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--max-cubes", type=int, default=None,
                    help="stop after this many cubes (a trial run; the rest stay owed)")
    ap.add_argument("--once", action="store_true", help="one iteration, then exit (a trial run)")
    a = ap.parse_args(argv)
    pin_backend()
    # ONE PROCESS, LOOPING, THE LOCK HELD ONLY WHILE WORKING. A launch-per-guard-tick lane left a
    # log pair every 5 minutes (~288 a day) that nothing prunes; a lane holding the lock for its
    # whole life would refuse every other statcan writer - including every all-source
    # core.derive_csv run - for ever. So: iterate, take the lock for the iteration, release it,
    # sleep. An exception ends the process loudly and the guard relaunches it.
    held_by_other_logged = False
    while True:
        if writer_lock.acquire(LOCK_NAME, what="jobs/statcan_lane.py"):
            held_by_other_logged = False
            try:
                r = run(max_cubes=a.max_cubes)
            finally:
                writer_lock.release(LOCK_NAME)
            if r.get("worked") or r.get("enumerated"):
                print(f"[lane] {_iso(_now())} iteration: {r}", flush=True)
        elif not held_by_other_logged:
            rec = writer_lock.owner(LOCK_NAME) or {}
            print(f"[lane] {_iso(_now())} another writer holds {writer_lock.lock_path(LOCK_NAME)} "
                  f"(pid {rec.get('pid')}, {rec.get('what')}); waiting", flush=True)
            held_by_other_logged = True
        if a.once:
            return 0
        time.sleep(IDLE_SLEEP_S)


if __name__ == "__main__":
    sys.exit(main())
