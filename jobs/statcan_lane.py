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

THREE ADDITIONS (owner leave 2026-11-22; design F:/work/econ_finish/statcan_lane_pr/DESIGN.md):
  * STALE UPSTREAM. Every merged cube gets an `upstream` record: the end of the parsed zip against the
    publisher's cubeEndDate (getAllCubesListLite, read at enumeration, BEFORE the fetch), compared by
    frequency with tools/audit_statcan_upstream.compare (imported, not copied). A zip that ends before
    the publisher's last period is logged "merge stale_upstream", not "merge merged", and named in
    the progress record. The merge itself is unchanged: a stale zip can still carry revisions.
  * PRE-FLOOR RECONCILE. RECONCILE_PIDS names the held cubes whose latest release is BEFORE FLOOR and
    later than their stored copy (measured 2026-11-22). They are owed like any released cube.
  * HISTORY FLOOR. For the six CIMT cubes (HISTORY_FLOOR_PIDS) a refreshed table whose first VALUED
    date is later than the stored copy's is held (quarantined for that release), never merged: a
    keep-old merge would splice the stored older vintage under the new one.

THE STRIKE RULE (2026-11-22, ledger R2026; design F:/work/econ_finish/statcan_strike_pr/DESIGN.md). On 2026-11-18
an edge refusal of about an hour quarantined 35 cubes in 16 minutes, and pre-release zips served for minutes
quarantined 9 more by the count gate. Four changes, so a refusal that lasts minutes or hours cannot park a cube for a
release cycle, and a refusal that lasts for ever still turns red or quarantines within a bounded time:
  * SPACED STRIKES. A deterministic failure waits 1 h, then 6 h; the 3rd strike on one release quarantines only
    when the strikes span QUARANTINE_MIN_SPAN_H (12 h). A re-release drops the old strikes and their spacing.
  * "NOT NOW" CODES ARE TRANSIENT (statcan.TRANSIENT_HTTP: 401, 403, 408, 425, 429, 5xx, 520-526, 530).
  * THE ENUMERATION CANNOT END THE PROCESS: a failure is recorded (`enum_fail`), logged and backed off; verdict()
    reads it red after statcan.ENUM_FAIL_RED_HOURS (6 h).
  * A CIRCUIT BREAKER: one HTTP status refusing BREAKER_MIN_CUBES (3) cubes in one iteration books no strike for
    it, stops the merge attempts (serving goes on) and holds every merge-owed cube back for 10-60 min; after the
    hold, at most 3 attempts decide: closed, tripped again, or held again.
  Every refusal is logged with its status, server and cf-ray (and the first body bytes, in the log line only), and
  every count-gate decision with the parsed and published counts and the zip's Last-Modified.

BEFORE T0 ONLY (self-hosting plan, 2026-09-28). The lane merges into the R2 store and serves R2, so it
REFUSES once econ is cut over (core.cutover): its post-T0 backend and writer lock are a T0 design
decision, not a default. Until then it reads the catalogue through core.catalog_path and writes every
CSV through the shared CSV store (updater.blob.csv_store + derive._put_with_retry), like the derive
tool it mirrors.
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

from core import catalog_path, cutover                           # noqa: E402
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
CATALOGUE = catalog_path.under(ROOT)                           # opened via connect_path (read-only)
LOCAL_PROGRESS = os.path.join(ROOT, "logs", "statcan_lane.progress.json")
# Whether the lane may run after T0. False: it serves R2 and takes its own statcan_writer lock, and its
# self-hosted backend and core.catalog_path writer lock are not designed (the owner's T0 decision).
# tools/selfhost/t0_ready.py reads this by parsing (never importing) and refuses T0 while it is False.
# WHOEVER FLIPS IT TO True must also make the post-T0 readers report the lane - guard_heartbeat.check_url
# and the worker's /v1/guard-heartbeat carry no statcan_lane field today (port review, rule (c)) - and
# replace the resident refusal in main() with the self-hosted path.
POST_T0_READY = False
PREFIX = "series"

# THE BACKLOG'S START. 505 cubes were released between 2026-07-29 and 2026-09-17 while the fetcher
# read a single-day feed as a since-feed (statcan.py `_changed_releases`). Nothing released before it
# is owed, EXCEPT the measured RECONCILE_PIDS below.
FLOOR = "2026-07-29"
# THE PRE-FLOOR RECONCILE (owner leave 2026-11-22). The floor hid every cube whose LATEST release is
# before it, so a held cube released between its stored copy (written 2026-06-03..06-05, restored to R2
# on 2026-09-05) and the floor was never owed. Measured 2026-11-22 by the lane's own owed rule (release
# later than the stored copy), not by end date - an end date can prove a cube behind, never current
# (R1968): getAllCubesListLite at 05:35:06Z x the local mirror's file times, over every catalogued cube
# (F:/work/econ_finish/statcan_lane_pr/reconcile_class_20261122.log). 143 cubes: 136 the detector reads
# behind by end date (tools/audit_statcan_upstream.py, R2 run 2026-11-19) + 7 it reads current but
# owed by the same release rule (3 of the 7 were checked against today's zips: all 3 carry revised
# values; the other 4 were not checked). NOT 10100085: its release (2017) is older than its stored copy and its own
# zip ends where the store ends (1992-11-19) - a publisher-metadata question, not owed work.
# A member is owed until merged at its release; later it is an ordinary cube. Inert after that.
RECONCILE_PIDS = frozenset("""
11100062 11100063 11100064 11100094 11100095 11100096 11100097 11100098 11100099
11100106 11100111 11100114 11100115 11100116 11100117 11100118 11100119 11100120
11100121 11100122 11100124 11100133 13100768 13100783 13100810 13100879 18100007
18100021 18100139 18100140 18100287 18100290 22100004 22100005 22100006 23100001
23100073 23100081 23100082 23100083 23100084 23100086 23100253 23100254 23100268
23100272 23100305 23100306 27100005 27100006 27100007 27100008 27100009 27100010
27100011 27100012 27100013 27100014 27100026 27100027 27100028 27100029 32100017
32100056 32100099 32100356 32100358 33100005 33100570 35100001 35100002 35100026
35100060 35100061 35100062 35100063 35100064 35100066 35100067 35100068 35100069
35100071 35100073 35100074 35100075 35100119 35100125 35100126 35100127 35100156
35100157 35100170 35100177 35100178 35100179 35100180 35100181 35100182 35100183
35100184 35100185 35100186 35100187 35100188 35100189 35100190 35100191 35100206
35100207 35100208 36100038 36100042 36100474 36100608 36100610 36100611 36100652
36100653 36100654 36100655 36100669 37100264 37100265 37100266 37100267 37100268
37100271 37100272 37100273 37100274 37100279 37100280 37100281 37100282 37100283
37100284 37100285 37100286 37100287 37100288 37100292 38100032 38100141
""".split())
# The enumeration asks for EVERY listed cube (one request either way) and the lane applies the floor
# itself - `release[:10] >= floor`, the test _changed_releases applies - plus the reconcile set. A member
# whose release ever moved backward cannot drop out silently this way.
ENUM_ALL = dt.date(1900, 1, 1)
# THE HISTORY FLOOR (owner leave 2026-11-22). StatCan's full-table zips of these six CIMT cubes have not
# been rebuilt since 2023-11-09 (HEAD Last-Modified), and four of them now START later at the publisher
# (cubeStartDate 2014-01-01) than our stored history (2013 or 1988; local mirror footers 2026-11-22).
# Armed BEFORE the rebuilt zip exists, so its first merge cannot splice our older vintage under the new.
# LIMITS: it guards THIS LANE's merge only - not jobs/ingest_statcan.py (the bulk ingester) or any other
# writer - and it judges the cube as a whole: a refresh that keeps one valued 1988 series passes.
HISTORY_FLOOR_PIDS = frozenset({"12100147", "12100148", "12100149", "12100150", "12100151", "12100152"})
# The publisher end-date read is informational: bounded, and its failure never blocks a merge.
ENDS_TRIES, ENDS_TIMEOUT_S = 2, 300
# An idle launch (nothing owed, nothing due for retry) calls StatCan at most this often. The guard
# ticks every ~5 min, and the enumeration is one ~8,270-cube request.
ENUM_MIN_INTERVAL_MIN = 60.0
# The production derive's row cap (RELAUNCH_GUARD.ps1's retired derive_statcan argv: --max-rows
# 3000000). A cube served WHOLE that has grown past it is still served whole - it is catalogued,
# and a stale object is worse than a large one - and never re-split here. The over-cap note it
# records is a NOTE, not catalogue debt: the derive and the cataloguer keep such a cube whole too
# (the one rule, AR-173).
MAX_ROWS = 3_000_000
PUT_WORKERS = 16
# THE STORE MERGE'S MEMORY. merge.BOUNDED_MEMORY_LIMIT (2 GB) is sized for the 16 GB cloud runner, and the
# lane - which runs on the workstation only (statcan is run_location: local) - merged under it. Read on
# 2026-10-10: 23 cubes of 5.6 to 427 million stored rows have failed EVERY merge since the lane began, with
# DuckDB's OutOfMemoryException at "1.8 GiB/1.8 GiB used" (270 such lines in the lane's current log, 220 in
# the two before it, and no other merge error). A failing cube backs off and the lane goes on - the same log
# holds 130 merges of 122 other cubes - so these 23 alone never caught up; the oldest of them is owed since
# the release of 2026-09-08.
# Offline, on copies, cube 14100034 (8,840,560 stored rows) merged with a table of the same size: the 2 GB
# default fails in about 4 s with that error; 4 GB and 16 GB merge it, with 3.0 to 3.3 GiB of process memory
# at the peak (both call forms of the lane, and also with a new table that revises 10 % of the values:
# review AR-276). The largest of them, cube 12100152 (427,009,412 stored rows, a 4.07 GB object, merged with a copy of itself): 16 GB merges it in 6,456 s on a busy machine, with 15.95 GiB of process memory at the peak and about 81 GB of spill on disk; smaller limits were not tried for it.
# AQUEDUCT_BOUNDED_MERGE_MEMORY still overrides, as it does for every other caller of the bounded merge.
MERGE_MEMORY_LIMIT = "16GB"
# Between iterations. Each iteration beats (so an idle lane beats every 5 min, well inside the
# reporter's 3-hour idle allowance) and calls StatCan only past ENUM_MIN_INTERVAL_MIN.
IDLE_SLEEP_S = 300
# A transient failure is retried after 10 min, doubling, capped at 12 h.
BACKOFF_MIN, BACKOFF_MAX_MIN = 10.0, 12 * 60.0
# This many transient failures in a row (10+20+40+80+160 min of backoff, ~5 h) and the cube is named
# to the reporter as FAILING - red - with its last error (lane review P3).
TRANSIENT_RED_AFTER = 6
# STRIKES ARE SPACED IN TIME (2026-11-22, ledger R2026; design F:/work/econ_finish/statcan_strike_pr/DESIGN.md).
# A deterministic failure used to set no retry_after, so its 3 strikes landed in 3 consecutive 5-minute iterations:
# on 2026-11-18 an edge refusal of about an hour quarantined 35 cubes in 16 minutes, and a pre-release zip served
# for minutes quarantined 9 more by the count gate (20 such refusals passed 5-17 minutes later). Now strike 1 waits
# 1 h, every later strike 6 h, and a cube is quarantined only by its 3rd strike on ONE release when the strikes span
# at least QUARANTINE_MIN_SPAN_H - read from the first strike's stamp, never from retry_after.
STRIKE_RETRY_MIN = (60.0, 360.0)          # after strike 1; after every later strike
QUARANTINE_MIN_SPAN_H = 12.0
# THE ENUMERATION BACKOFF (R2026). getAllCubesListLite answered 403 for ~19 h on 11-17/18 and the bare call killed
# the process 210 times. A failure now backs off 10, 20, 40, 60, 60 ... min. Every attempt writes a stdout line,
# and the cap stays well under the guard's 3 h stall kill (RELAUNCH_GUARD $STALL_HOURS, keyed on the log's mtime).
ENUM_BACKOFF_MIN, ENUM_BACKOFF_MAX_MIN = 10.0, 60.0
# THE CIRCUIT BREAKER (R2026). One HTTP status refusing this many cubes in one iteration is the host, not the
# cubes: no strike is booked for it, merge attempts stop, serving goes on, and every merge-owed cube is held back
# by a lane-level record until its `until` (10, 20, 40, 60, 60 ... min) - so the cube list and the end dates are
# read once per hold (2 GETs), never every 5 minutes (design review F1/C1). When a hold ends the breaker is
# HALF-OPEN: any answer from StatCan (another status included) closes it; BREAKER_MIN_CUBES attempts with no answer
# trip it again (if the trip's status was among them) or arm the hold again - see run() and _probe (verification
# review V1-V3, W1-W2).
BREAKER_MIN_CUBES = 3
BREAKER_BACKOFF_MIN, BREAKER_BACKOFF_MAX_MIN = 10.0, 60.0
# What a breaker trip puts back for a cube it refused with a strike (design review C3): the state before the attempt.
_RESTORE_FIELDS = ("fails", "fail_release", "first_strike_utc", "retry_after", "quarantined", "last_error",
                   "transient_fails", "merge_in_flight")

_TOOL = os.path.join(ROOT, "tools", "derive_statcan_tables.py")
_UPSTREAM = os.path.join(ROOT, "tools", "audit_statcan_upstream.py")


def _upstream():
    """tools/audit_statcan_upstream.py (PR #312), for its PURE functions only (compare, _date_part,
    Unmeasurable): the period comparison measured on getAllCubesListLite. Importing it reads nothing and
    builds no client; the detector itself never imports this lane."""
    m = sys.modules.get("_audit_statcan_upstream_for_lane")
    if m is None:
        spec = importlib.util.spec_from_file_location("_audit_statcan_upstream_for_lane", _UPSTREAM)
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        sys.modules["_audit_statcan_upstream_for_lane"] = m
    return m


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
        # A failing enumeration and a breaker hold are published from the STATE at every beat, so a relaunch and
        # an idle iteration carry them (R1100). verdict() turns the first red after ENUM_FAIL_RED_HOURS.
        for k in ("enum_fail", "breaker"):
            v = self.st.get(k)
            if v:
                self.d[k] = dict(v) if isinstance(v, dict) else v
            else:
                self.d.pop(k, None)
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
    # THREE MORE NAMED LISTS (2026-11-22), also kept off the owed-age clock: a publisher's frozen zip
    # or a cut history is not the lane falling behind. Each record counts only for the release the
    # cube holds NOW, so a flag never outlives a later release. The reporter does not read them; the
    # reader is named in DESIGN.md (the daily maintenance check of logs/statcan_lane.progress.json).
    stale, unmeasured, held = [], [], []
    upstream = {"current": 0, "ahead": 0, "stale_upstream": 0, "unmeasured": 0}
    for p, c in st.get("cubes", {}).items():
        rel = c.get("release")
        hh = c.get("history_hold") or {}
        if c.get("quarantined") and hh.get("release") and hh.get("release") == rel == c.get("fail_release"):
            held.append({"pid": p, "release": rel, "new_first": hh.get("new_first"),
                         "stored_first": hh.get("stored_first")})
        if c.get("quarantined"):
            quar.append(p)
            continue
        u = c.get("upstream") or {}
        if u.get("release") and u.get("release") == c.get("merged") == rel and u.get("class") in upstream:
            upstream[u["class"]] += 1
            # DICTS keyed by pid AND release, so a reader can acknowledge one release of one cube
            # (round-2 review F2: the frozen CIMT zips would otherwise alarm every day)
            if u["class"] == "stale_upstream":
                stale.append({"pid": p, "release": rel, "zip_end": u.get("zip_end"),
                              "publisher_end": u.get("publisher_end"), "lag": u.get("lag")})
            elif u["class"] == "unmeasured":
                unmeasured.append({"pid": p, "release": rel, "reason": u.get("reason")})
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
            "oldest_owed_release": oldest,
            "stale_upstream": sorted(stale, key=lambda d: d["pid"]),
            "upstream_unmeasured": sorted(unmeasured, key=lambda d: d["pid"]),
            "history_held": sorted(held, key=lambda d: d["pid"]), "upstream": upstream}


def merge_owed(c: dict) -> bool:
    rel = c.get("release")
    return bool(rel) and (c.get("merged") is None or c["merged"] < rel)


def serve_owed(c: dict) -> bool:
    m = c.get("merged")
    return bool(m) and (c.get("served") is None or c["served"] < m)


def _due(c: dict, now: dt.datetime) -> bool:
    ra = _parse_iso(c.get("retry_after"))
    return ra is None or ra <= now


def _breaker_holds(st: dict, now: dt.datetime) -> bool:
    """OPEN: True while a circuit-breaker trip holds every merge-owed cube back (serving is not held)."""
    b = st.get("breaker")
    until = _parse_iso(b.get("until")) if isinstance(b, dict) else None
    return until is not None and until > now


def _trip_refused(st: dict, c: dict) -> bool:
    """True when a breaker record exists and the cube waits out a TRANSIENT backoff from that trip's status. A
    strike pops refused_http (_deterministic), so a struck cube is never this (verification review V2)."""
    b = st.get("breaker")
    return isinstance(b, dict) and b.get("status") is not None and c.get("refused_http") == b.get("status")


def _probe(st: dict, now: dt.datetime):
    """HALF-OPEN: a trip's hold has ended and StatCan has not answered since. When no merge-owed cube is due, the
    first cube waiting out a TRANSIENT backoff from the trip's status (`_trip_refused`) is attempted anyway, so a
    recovered host is found within one hold, not one cube backoff. Never a struck cube: a strike's wait stands. The
    half-open iteration always decides (run(): closed, tripped again or held again), so a probe is attempted at
    most once per hold. Returns the pid, or None."""
    b = st.get("breaker")
    if not isinstance(b, dict) or _breaker_holds(st, now):
        return None
    owed = [(p, c) for p, c in st["cubes"].items() if merge_owed(c) and not c.get("quarantined")]
    if any(_due(c, now) for _p, c in owed):
        return None
    cands = sorted((c["release"], c.get("bytes") or 0, p) for p, c in owed if _trip_refused(st, c))
    return cands[0][2] if cands else None


# --------------------------------------------------------------------------- #
# serving - one sorted scan per cube, catalogued ids only, no map writes
# --------------------------------------------------------------------------- #
def catalogued_ids(pid: str, catalog_db: str | None = None) -> set:
    """Every catalogued id of one cube, by a PRIMARY-KEY RANGE read of the local catalogue. The
    product id is a fixed 8 digits, so 'statcan:<pid>' and 'statcan:<pid>#...' both sort inside
    [lo, lo + '$') ('$' 0x24 sorts just after '#' 0x23)."""
    lo = _tool().unit_id(str(pid))
    con = catalog_path.connect_path(catalog_db or CATALOGUE, write=False, timeout=180)
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
    # THE ONE RULE (divergence found by AR-166, rule approved by AR-173): a whole cube stays whole over the cap - the same verdict as
    # tools/derive_statcan_tables.pinned_split and the cataloguer's kept_whole
    # (tests/test_statcan_lane.py::test_the_lane_and_the_derive_tool_apply_ONE_rule)
    if n_rows > MAX_ROWS:
        notes.append(f"served whole at {n_rows:,} rows, over the {MAX_ROWS:,} derive cap")
    return None, None, notes


def _merge_memory() -> str:
    """The DuckDB memory limit for a store merge: the environment's, else MERGE_MEMORY_LIMIT."""
    return os.environ.get("AQUEDUCT_BOUNDED_MERGE_MEMORY") or MERGE_MEMORY_LIMIT


def serve_cube(pid: str, local_parquet: str, smap: dict, catalogued: set, put, progress=None,
               memory_limit: str = "4GB") -> dict:
    """Re-derive cube `pid`'s CATALOGUED ids from `local_parquet` and hand each body to
    `put(key, csv_body)` - the PLAIN CSV; the store gzips it at rest (see _store_put). The SELECT, ORDER BY, duplicate collapse and body bytes are
    tools/derive_statcan_tables.py's own (19/19 parts byte-identical to core.derive_csv, NUMBERS.md
    2026-09-23). Returns {status, put, put_errors, new, vanished, null_part_rows, notes, refusal}."""
    import duckdb                                                    # noqa: PLC0415
    import pyarrow.parquet as pq                                     # noqa: PLC0415
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
    # PLAIN bodies now (the store gzips them and records their csvmd5), so the queue holds one per
    # worker, not the 64 compressed bodies it held when the lane gzipped at enqueue
    q: queue.Queue = queue.Queue(maxsize=PUT_WORKERS)
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
            q.put((t.csv_key(PREFIX, sid), t._rows_csv(rows)))

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


def _store_put():
    """put(key, csv_body) into the CSV store - updater.blob.csv_store, the one every series-CSV writer
    uses (R2 before T0) - with the updater's 7 app-level tries (derive._put_with_retry). The body is the
    PLAIN CSV: put_atomic gzips it at rest and records its pre-compression md5 (csvmd5), so a part whose
    bytes the store already holds is not uploaded again (R2Blob._already_holds - the skip that saves
    ~71% of paid PUTs). A failed or refused PUT raises, and serve_cube counts it as a put error."""
    from updater import derive                                       # noqa: PLC0415
    store = blob.csv_store(pool=PUT_WORKERS)

    def put(key, body):
        if not derive._put_with_retry(store, key, body):
            raise RuntimeError(f"PUT failed (refused at once, or {derive.PUT_TRIES} tries used up - "
                               f"see the line above)")
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


def _quarantine(c: dict, pid: str, reason: str) -> None:
    c["quarantined"] = True
    print(f"[lane] {pid}: QUARANTINED after {c.get('fails')} failures on its {c.get('release')} release (first "
          f"strike {c.get('first_strike_utc')}) - not attempted again until StatCan re-releases it: "
          f"{reason[:200]}", flush=True)


def _deterministic(c: dict, pid: str, reason: str, now: dt.datetime, defer_quarantine: bool = False) -> bool:
    """Book one DETERMINISTIC strike on the cube's current release, spaced in time (STRIKE_RETRY_MIN), and
    quarantine on the QUARANTINE_AFTER-th strike only when the strikes span QUARANTINE_MIN_SPAN_H. Strike 1 always
    OVERWRITES first_strike_utc (a stale stamp from an older release must not shorten the span); on a later strike a
    missing or unreadable stamp starts the span now, which can only delay a quarantine. With `defer_quarantine` a
    strike that would quarantine does not: it returns True and the caller decides (the circuit breaker, R2026)."""
    rel = c.get("release")
    n = int(c.get("fails") or 0) + 1 if c.get("fail_release") == rel else 1
    first = _parse_iso(c.get("first_strike_utc")) if n > 1 else None
    if first is None:
        first = now
    c.update(fails=n, fail_release=rel, first_strike_utc=_iso(first), last_error=reason[:300])
    # A STRIKE'S WAIT STANDS: no breaker close frees it and no half-open probe skips it, whichever gate struck
    # (verification review F3/V2 - a stale refused_http from an earlier transient refusal let a close free it)
    c.pop("refused_http", None)
    span_h = (now - first).total_seconds() / 3600.0
    if n >= sc.QUARANTINE_AFTER and span_h >= QUARANTINE_MIN_SPAN_H:
        if defer_quarantine:
            c["retry_after"] = _iso(now + dt.timedelta(minutes=STRIKE_RETRY_MIN[1]))
            print(f"[lane] {pid}: strike {n} on its {rel} release (first strike {_iso(first)}) would quarantine it; "
                  f"held for this iteration's circuit-breaker decision: {reason[:200]}", flush=True)
            return True
        _quarantine(c, pid, reason)
        return False
    ra = now + dt.timedelta(minutes=STRIKE_RETRY_MIN[0] if n == 1 else STRIKE_RETRY_MIN[1])
    if n >= sc.QUARANTINE_AFTER - 1:
        ra = max(ra, first + dt.timedelta(hours=QUARANTINE_MIN_SPAN_H))
    c["retry_after"] = _iso(ra)
    print(f"[lane] {pid}: strike {n} of {sc.QUARANTINE_AFTER} on its {rel} release (first strike {_iso(first)}); "
          f"next try not before {_iso(ra)} - a quarantine needs {sc.QUARANTINE_AFTER} strikes over at least "
          f"{QUARANTINE_MIN_SPAN_H:.0f} h: {reason[:200]}", flush=True)
    return False


# --------------------------------------------------------------------------- #
# the publisher's end date (stale_upstream) and the history floor
# --------------------------------------------------------------------------- #
def publisher_ends(get=None) -> tuple:
    """({pid: {"end": "YYYY-MM-DD" | None, "freq": frequencyCode}}, error | None) from ONE
    getAllCubesListLite GET - the endpoint whose cubeEndDate convention the detector measured (the
    FIRST day of the last period for month-step frequencies, the week's end for weekly). run() reads it
    AFTER the release list and BEFORE any fetch, and only when a cube is merge-owed (the order and the
    one-enumeration false stale it allows are explained there). Never raises: an end date it cannot
    read makes the cube's record `unmeasured`, never `current`, and never blocks a merge."""
    try:
        j = (get or sc._get)("getAllCubesListLite", tries=ENDS_TRIES, timeout=ENDS_TIMEOUT_S)
        cubes = j if isinstance(j, list) else (j.get("object") if isinstance(j, dict) else None)
        if not isinstance(cubes, list) or len(cubes) < sc._LITE_FLOOR:
            n = len(cubes) if isinstance(cubes, list) else "no list"
            return {}, f"getAllCubesListLite returned {n} cube(s), below the {sc._LITE_FLOOR} floor"
        out = {}
        for o in cubes:
            if isinstance(o, dict) and o.get("productId") is not None:
                end = o.get("cubeEndDate")
                out[str(o["productId"])] = {"end": end[:10] if isinstance(end, str) and end else None,
                                            "freq": o.get("frequencyCode")}
        return out, None
    except Exception as e:                                           # noqa: BLE001
        return {}, f"{type(e).__name__}: {str(e)[:160]}"


def classify_upstream(zip_end, pub: dict) -> dict:
    """{class, lag, reason} for a parsed zip that ends at `zip_end` (ISO date, the parser's own max
    obs_date over every row) against the publisher record `pub` ({"end", "freq"} or {"error"}).
    class: current | ahead | stale_upstream | unmeasured. The comparison is the detector's compare():
    by frequency, a store date anywhere inside the publisher's last period is current. Raises nothing
    the caller does not catch (see _upstream_record)."""
    U = _upstream()
    if not pub or pub.get("error"):
        return {"class": "unmeasured", "lag": None,
                "reason": f"publisher end not read: {(pub or {}).get('error') or 'no record'}"}
    if not zip_end:
        return {"class": "unmeasured", "lag": None, "reason": "the parse gave no end date"}
    try:
        pub_end = U._date_part(pub.get("end"))
        cls, lag = U.compare(dt.date.fromisoformat(str(zip_end)[:10]), pub_end, pub.get("freq"))
    except U.Unmeasurable as e:
        return {"class": "unmeasured", "lag": None, "reason": str(e)[:200]}
    except ValueError as e:
        return {"class": "unmeasured", "lag": None, "reason": f"unparseable zip end {zip_end!r} ({e})"}
    return {"class": "stale_upstream" if cls == "behind" else cls, "lag": lag, "reason": None}


def _upstream_record(rel, s: dict, pub: dict) -> dict:
    """The cube's `upstream` record for release `rel`. CANNOT RAISE (design review F9): it is written
    after the merge is recorded, and a crash here must never turn a published merge into a retry."""
    try:
        r = classify_upstream(s.get("end"), pub)
    except Exception as e:                                           # noqa: BLE001
        r = {"class": "unmeasured", "lag": None,
             "reason": f"the classifier raised {type(e).__name__}: {str(e)[:120]}"}
    pub = pub or {}
    r.update(release=rel, zip_start=s.get("start"), zip_end=s.get("end"),
             publisher_end=pub.get("end"), freq=pub.get("freq"), end_read_utc=pub.get("read_utc"))
    return r


def _first_valued(path: str):
    """min(obs_date) over the rows WITH A VALUE (exact, DuckDB) - None when no row has one. Raises on
    a read error. Valued, because the merge keeps the LAST row per (series_key, obs_date) whatever its
    value: a refreshed table that keeps the old periods with blank values would blank stored history."""
    import duckdb                                                    # noqa: PLC0415
    f = path.replace("\\", "/").replace("'", "''")
    con = duckdb.connect()
    try:
        con.execute(f"SET memory_limit='{merge.BOUNDED_MEMORY_LIMIT}'")
        v = con.execute(f"SELECT min(obs_date) FROM read_parquet('{f}') WHERE value IS NOT NULL").fetchone()[0]
    finally:
        con.close()
    if v is not None and not isinstance(v, dt.date):
        raise TypeError(f"min(obs_date) is {type(v).__name__}, not a date")
    return v


def _history_gate(pid: str, c: dict, stored_copy: str, before: int, new_path: str, n_new: int,
                  s: dict, rel, now: dt.datetime):
    """For an ARMED cube (HISTORY_FLOOR_PIDS): None to go on, or the merge outcome when the refreshed
    table must not be merged. Runs before every other gate. Fails CLOSED: a date it cannot read is a
    transient (retried, never merged blindly); a NULL only means 'nothing to judge' when the FOOTER
    says 0 rows (R503)."""
    if pid not in HISTORY_FLOOR_PIDS:
        return None
    if n_new == 0 or before == 0:
        return None          # 0 new rows: the count gates refuse it; 0 stored rows: nothing to protect
    try:
        stored_first = _first_valued(stored_copy)
        new_first = _first_valued(new_path)
    except Exception as e:                                           # noqa: BLE001
        _backoff(c, f"history-floor gate could not read the first valued dates: {type(e).__name__}: {e}", now)
        return "transient"
    if stored_first is None:
        _backoff(c, "history-floor gate: the stored copy has rows but no value - cannot judge", now)
        return "transient"
    if new_first is not None and new_first <= stored_first:
        return None
    why = (f"HISTORY FLOOR: the refreshed table's first valued date is "
           f"{new_first.isoformat() if new_first else 'none (no value at all)'}, the stored copy's is "
           f"{stored_first.isoformat()}: StatCan has cut history this store holds. NOT merged (keep-old would "
           f"splice the stored older vintage under the new one). A person decides: (a) keep-old merge - remove "
           f"{pid} from HISTORY_FLOOR_PIDS in a reviewed PR; it takes effect at StatCan's NEXT release of the "
           f"cube after the pull and a lane restart; (b) replace with the new vintage - that deletes stored "
           f"history StatCan no longer serves: the owner's decision.")
    c.update(quarantined=True, fails=sc.QUARANTINE_AFTER, fail_release=rel, last_error=why[:300],
             history_hold={"release": rel, "stored_first": stored_first.isoformat(),
                           "new_first": new_first.isoformat() if new_first else None,
                           "new_last": s.get("end"), "reason": why, "recorded_utc": _iso(now)})
    print(f"[lane] {pid}: HELD by the history floor for its {rel} release - {why}", flush=True)
    return "definitive"


def merge_cube(pid: str, c: dict, st: dict, progress: Progress, now: dt.datetime, attempt=None) -> str:
    """Refresh one held cube's store object. Returns 'merged', 'unchanged', 'transient',
    'definitive' or 'absent'. On success sets c['merged'] (and c['served'] too when nothing a user
    reads changed). `attempt` (a dict, optional, never persisted) receives what run()'s circuit breaker needs:
    http_status (StatCan refused the fetch with a status), fetched (the fetch returned), pending_quarantine
    (the strike would quarantine; held for the breaker decision)."""
    attempt = {} if attempt is None else attempt
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
    # the publisher's end as read at enumeration - BEFORE the fetch below (publisher_ends)
    pub = dict(c.get("publisher") or {})
    c["merge_in_flight"] = rel
    save_state(st)

    def nothing_published():
        # THIS ATTEMPT PUBLISHED NOTHING (2026-11-22; the report found 49 of 49 quarantined cubes still flagged):
        # the flag goes back to what it was BEFORE the attempt - never a plain pop, so a merge an EARLIER attempt
        # may have published (R1098 P1) still forces its serve. Called only where no write can have happened.
        if prior_in_flight is None:
            c.pop("merge_in_flight", None)
        else:
            c["merge_in_flight"] = prior_in_flight

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
            nothing_published()
            return "transient"
        try:
            new_path, s = sc._fetch_cube_whole(int(pid))
        except TransientError as e:
            attempt["http_status"] = getattr(e, "http_status", None)
            _backoff(c, f"whole-table fetch failed: {e}", now)
            if attempt["http_status"] is not None:
                c["refused_http"] = attempt["http_status"]     # a breaker that closes makes it due again
            # (no status - a reset, a timeout: no answer at all. A cube that waits on a breaker keeps waiting on
            # it, so the record and its one-decision-per-hold rule stay in force - verification review V1)
            nothing_published()
            return "transient"
        except DefinitiveError as e:
            attempt["http_status"] = getattr(e, "http_status", None)
            # no status: StatCan ANSWERED (200) with something unusable - no zip URL, a short or unparseable table.
            # For the circuit breaker that is a host that answers (run(): a half-open iteration closes on it).
            attempt["host_answered"] = attempt["http_status"] is None
            # a refusal WITH an HTTP status may be one of many this iteration: its quarantine waits for run()'s
            # circuit-breaker decision (design review C3)
            attempt["pending_quarantine"] = _deterministic(
                c, pid, f"whole-table fetch refused: {e}", now,
                defer_quarantine=attempt["http_status"] is not None)
            nothing_published()
            return "definitive"
        except Exception as e:                                       # noqa: BLE001
            # NOT a StatCan answer (e.g. a full disk while writing the zip - _download does not catch OSError):
            # transient, and it published nothing (verification review F5)
            _backoff(c, f"unexpected {type(e).__name__} in the whole-table fetch: {e}", now)
            traceback.print_exc()
            nothing_published()
            return "transient"
        attempt["fetched"] = attempt["host_answered"] = True
        progress.beat(bytes_downloaded=int(s.get("zip_bytes") or 0))
        n_new, n_series = int(s["n_obs"]), int(s["n_series"])
        held = _history_gate(pid, c, copy[0], before, new_path, n_new, s, rel, now)
        if held is not None:
            nothing_published()
            return held
        try:
            total, both = sc._key_overlap(copy[0], new_path)
        except Exception as e:                                       # noqa: BLE001
            _backoff(c, f"key check failed: {type(e).__name__}: {e}", now)
            nothing_published()
            return "transient"
        floor = int(st["parsed_rows"].get(pid, before))
        if s.get("needs_stored_floor") and n_new < floor:
            _deterministic(c, pid, f"the vectorless table has {n_new:,} rows, fewer than the "
                                   f"{floor:,} of its last accepted parse; not merged. If StatCan "
                                   f"really trimmed it, set parsed_rows[{pid}] in _lane_state.json", now)
            nothing_published()
            return "definitive"
        if total and both < total * sc.KEY_OVERLAP_MIN:
            _deterministic(c, pid, f"only {both:,} of {total:,} stored keys reappear "
                                   f"(< {sc.KEY_OVERLAP_MIN:.0%}): re-keyed upstream; not merged", now)
            nothing_published()
            return "definitive"
        reported = (not s.get("series_capped")) and n_series <= merge.CHANGED_KEYS_CAP
        try:
            if reported:
                n, md, ch = merge.merge_and_write_bounded(
                    path, new_path=new_path, dedup_keys=sc.DEDUP, report_changed_keys=True,
                    changed_keys_cap=max(n_new, 1), stored_copy=copy[0], memory_limit=_merge_memory())
            else:
                n, md = merge.merge_and_write_bounded(path, new_path=new_path, dedup_keys=sc.DEDUP,
                                                      stored_copy=copy[0], memory_limit=_merge_memory())
                ch = None
        except (OSError, MemoryError) as e:
            # may follow the publish (merge.py re-raises after `published`): merge_in_flight stays set
            _backoff(c, f"merge failed on this machine: {type(e).__name__}: {e}", now)
            return "transient"
        except DefinitiveError as e:
            if sc._environmental(e):
                _backoff(c, f"merge failed on this machine: {e}", now)
                return "transient"
            # not sc._environmental(e): the merge guard's data refusal, raised with no cause BEFORE any write
            # (merge.py: every uncaused DefinitiveError precedes the publish - design review H1)
            _deterministic(c, pid, f"merge guard refused over {before:,} stored rows: {e}", now)
            nothing_published()
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
    c.pop("first_strike_utc", None)
    c.pop("refused_http", None)
    c.pop("merge_in_flight", None)
    c.pop("history_hold", None)            # a release that passed the history floor clears an old hold
    c["upstream"] = _upstream_record(rel, s, pub)      # after the merge is recorded; cannot raise
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
    """Pids owing work and due, OLDEST owed release first, stored size breaking ties. While a circuit-breaker
    trip holds (`_breaker_holds`), merge-owed cubes are not due; serve-owed cubes still are. When the hold has
    ended (`_probe`), one merge-owed cube may be worked although its own retry_after is ahead. While a breaker
    record exists, the cubes the trip refused (`_trip_refused`) and every cube with a transient failure of its own
    go LAST: a few cubes refused for ever, or failing for their own reason, sorted first by their old release, must
    not decide the half-open iteration before the other cubes are tried (verification review F2/V3, G1/W1)."""
    hold = _breaker_holds(st, now)
    probe = _probe(st, now)
    rec = isinstance(st.get("breaker"), dict)
    rows = []
    for p, c in st["cubes"].items():
        if c.get("quarantined"):
            continue
        if merge_owed(c):
            if hold or not (_due(c, now) or p == probe):
                continue
            rows.append((1 if rec and (_trip_refused(st, c) or int(c.get("transient_fails") or 0) > 0) else 0,
                         c["release"], c.get("bytes") or 0, p))
        elif serve_owed(c):
            if not _due(c, now):
                continue
            rows.append((0, c["merged"], c.get("bytes") or 0, p))
    return [p for _f, _r, _b, p in sorted(rows)]


def select_releases(listed: dict, floor: str) -> dict:
    """The releases the lane owes from one enumeration of EVERY listed cube: a release on/after the
    floor (`release[:10] >= floor`, _changed_releases' own test), or a RECONCILE_PIDS member at any
    date. Keys are compared as strings: _changed_releases returns int product ids."""
    return {pid: rt for pid, rt in listed.items()
            if str(rt)[:10] >= floor or str(pid) in RECONCILE_PIDS}


def _merge_line(pid: str, r: str, c: dict) -> str:
    """The log line of one merge outcome. "merge merged" stays the text for a current cube; a zip that
    ends before the publisher's cube says so (the reason this exists: CIMT logged "merged" for a 2023 zip)."""
    if r in ("transient", "definitive"):
        return f"[lane] {pid}: merge {r} ({c.get('last_error')})"
    u = c.get("upstream") or {}
    if r in ("merged", "unchanged") and u.get("release") and u.get("release") == c.get("merged"):
        if u.get("class") == "stale_upstream":
            return (f"[lane] {pid}: merge stale_upstream ({r}; the full-table zip ends {u.get('zip_end')}, the "
                    f"publisher's cube ends {u.get('publisher_end')}: {u.get('lag')} period(s) behind, "
                    f"frequency {u.get('freq')})")
        if u.get("class") == "unmeasured":
            return f"[lane] {pid}: merge {r} (publisher end unmeasured: {u.get('reason')})"
        if u.get("class") == "ahead":
            return (f"[lane] {pid}: merge {r} (the zip ends {u.get('zip_end')}, after the publisher's cube "
                    f"end {u.get('publisher_end')})")
    return f"[lane] {pid}: merge {r}"


def run(enumerate_releases=None, put=None, now_fn=_now, max_cubes=None, ends_fn=None) -> dict:
    """One launch: enumerate (unless idle and recent), then work every owed cube to empty.
    Injected `enumerate_releases` / `put` / `ends_fn` are for tests; production uses StatCan and R2."""
    st = load_state()
    progress = Progress(st)
    now = now_fn()
    last = _parse_iso(st.get("last_enum_utc"))
    ef = st.get("enum_fail") or None
    idle = not order_owed(st, now)
    recent = last is not None and (now - last).total_seconds() < ENUM_MIN_INTERVAL_MIN * 60
    # A FAILING ENUMERATION BACKS OFF (enum_fail.retry_after): an idle lane does not call StatCan before it.
    enum_due = not ef or _due(ef, now)
    if idle and (recent or not enum_due):
        progress.beat("enumeration_failing" if ef else "idle", current=None)
        return {"enumerated": False, "worked": 0}
    floor = st["floor"]
    # THE ENUMERATION CANNOT END THE PROCESS (2026-11-22, R2026). A 403 or 524 from getAllCubesListLite, a short
    # list (TransientError) or a code fault killed every launch - 210 relaunch deaths on 11-17/18. Now it is
    # recorded (`enum_fail`, in the state, published in every beat), logged and backed off; the lane stays up and
    # still works the cubes already owed. verdict() reads it red after statcan.ENUM_FAIL_RED_HOURS, so a
    # permanent failure is not hidden. SystemExit and KeyboardInterrupt still end the process.
    listed = None
    if enum_due:
        try:
            # _changed_releases prints "released on/after 1900-01-01": it is asked for every cube (ENUM_ALL).
            listed = (enumerate_releases or sc._changed_releases)(ENUM_ALL)
        except Exception as e:                                       # noqa: BLE001 - recorded, red after a bound
            _enum_failed(st, e, now)
            save_state(st)
            if idle:
                progress.beat("enumeration_failing", current=None)
                return {"enumerated": False, "worked": 0, "enum_failed": True}
        else:
            if ef:
                print(f"[lane] {_iso(now)} the enumeration answered again after {ef.get('count')} failed "
                      f"attempt(s) since {ef.get('since')}", flush=True)
            st.pop("enum_fail", None)
    recon_new, owed_now = 0, []
    rel = select_releases(listed, floor) if listed is not None else {}
    for pid, rt in rel.items():
        c = st["cubes"].setdefault(str(pid), {})
        if c.get("release") != rt:
            c["release"] = rt
            if str(pid) in RECONCILE_PIDS and str(rt)[:10] < floor:
                recon_new += 1
            if (c.get("quarantined") or int(c.get("fails") or 0) > 0) and c.get("fail_release") != rt:
                # RE-RELEASED: the strikes on the old release no longer count - the quarantine lifts (as before)
                # and so does their spacing, or a re-release could wait up to 12 h behind an old strike.
                c.pop("quarantined", None)           # re-released: tried again
                c["fails"] = 0
                c.pop("first_strike_utc", None)
                c.pop("retry_after", None)
    hold = _breaker_holds(st, now)
    # A RECORD NOTHING WAITS ON GOES (verification review G2/W2): once its hold has ended and no merge-owed cube
    # waits out a transient backoff from its status, it holds nothing, and its `since` must not age into a red.
    brec = st.get("breaker") if isinstance(st.get("breaker"), dict) else None
    if st.get("breaker") and not hold and not any(
            merge_owed(c) and not c.get("quarantined") and brec is not None
            and c.get("refused_http") == brec.get("status") for c in st["cubes"].values()):
        print(f"[lane] circuit breaker cleared: no merge-owed cube waits on it ({st['breaker']})", flush=True)
        st.pop("breaker", None)
    probe = _probe(st, now)

    def works_now(pid, c):
        # NOT a quarantined cube, and NOT one waiting out a backoff: merge_owed() is true for both, but
        # order_owed() works neither in this iteration, so they must not trigger the end-date GET on every
        # idle hourly enumeration (verification review V-F2, V2-F1). The enumeration that works them
        # (re-released, or due again) reads the ends first. A circuit-breaker hold counts the same way; the one
        # half-open probe is worked, so it is read.
        return merge_owed(c) and not c.get("quarantined") and not hold and (_due(c, now) or pid == probe)
    if listed is None:
        # NO FRESH RELEASE LIST: the cubes worked this iteration are owed from an earlier one. Their publisher end
        # is not read (it would be read without the release list it must follow, R2012), so their upstream record
        # is `unmeasured`, never `current`.
        for pid, c in st["cubes"].items():
            if works_now(pid, c):
                c["publisher"] = {"error": "the cube list was not read this iteration (enumeration failing)",
                                  "read_utc": _iso(now)}
    for pid in rel:
        c = st["cubes"][str(pid)]
        if works_now(str(pid), c):
            owed_now.append((str(pid), c))
    # THE PUBLISHER'S END DATES AFTER THE RELEASES and before any fetch, and only when a merge is owed
    # (design review round 2, R2012 F1). Read the other way round, a release landing between the two
    # reads was owed, its OLD end was compared with a zip fetched inside StatCan's rebuild window (28-127 s
    # after the release clock), and the old zip read "current" and settled the new release. In this order
    # that zip reads stale_upstream, which is true of it. The one false stale this order allows - a cube
    # owed for an older release whose new end arrives before its new release is listed - lasts one
    # enumeration: the next one owes the new release and the cube is fetched again.
    if owed_now:
        ends, ends_err = (ends_fn or publisher_ends)()
        if ends_err:
            print(f"[lane] publisher end dates NOT read this enumeration ({ends_err}): every merge this "
                  f"iteration is recorded 'upstream unmeasured'", flush=True)
        for pid, c in owed_now:
            e = ends.get(pid)
            c["publisher"] = ({"end": e["end"], "freq": e["freq"], "read_utc": _iso(now)} if e else
                              {"error": ends_err or "not in the publisher's cube list", "read_utc": _iso(now)})
    # The receipt line: once when members become owed, and at EVERY enumeration (about hourly when idle)
    # while a member is missing from the publisher's list - a withdrawn member stays visible.
    if listed is not None:
        missing = sorted(RECONCILE_PIDS - {str(p) for p in listed})
        if recon_new or missing:
            print(f"[lane] reconcile: {recon_new} of {len(RECONCILE_PIDS)} pre-floor cube(s) newly owed; "
                  f"{len(RECONCILE_PIDS) - len(missing)} listed; missing from the publisher list: "
                  f"{missing[:20]}{' ...' if len(missing) > 20 else ''}", flush=True)
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
    put = put or _store_put()
    # SIZES BEFORE THE ORDER: the tiebreak (and the reporter's per-cube allowance) needs each owed
    # cube's stored size, read once per cube (one HEAD under r2) and kept in state. Read inside the
    # loop, as first written, every first-visit cube tied at 0 bytes (mutation M6 survived).
    for pid in order_owed(st, now):
        c = st["cubes"][pid]
        if c.get("bytes") is None and merge_owed(c):
            c["bytes"], c["est_min"] = _est_min(pid)
    held = absent = worked = 0
    refused: dict = {}          # HTTP status -> [(pid, snapshot before the attempt, outcome, pending quarantine)]
    tripped = None              # the status that tripped the breaker in this iteration
    stop = False                # no more merge attempts in this iteration (a trip, or a hold armed again)
    # HALF-OPEN at the start of this iteration: an earlier trip's hold has ended and no fetch has returned since.
    # The merge attempts decide - cubes with a failure of their own and the trip's cubes LAST (order_owed) - and
    # the half-open state ends within this iteration (verification review V1-V3, W1-W2):
    #   StatCan answers (the fetch returned, a 200 it could not use, or ANOTHER status) -> the breaker closes and
    #     the rest goes on under the closed rule (a new status can trip a new breaker);
    #   BREAKER_MIN_CUBES attempts with no answer (the trip's status, no answer at all, a fault before the fetch)
    #     -> it trips again if any of them was the trip's status (no strike, hold doubles), else the hold is armed
    #     again (doubles); each attempt's own outcome stands;
    #   the due cubes run out first, none answered -> the same, after the loop.
    # So at most BREAKER_MIN_CUBES merge attempts per hold, and the cube list is read once per hold (2 GETs: the
    # list and the end dates).
    b0 = st.get("breaker") if isinstance(st.get("breaker"), dict) else None
    half_open_status = b0.get("status") if b0 is not None and not _breaker_holds(st, now) else None
    decided = half_open_status is None
    ho_tries, ho_last = 0, None             # half-open attempts this iteration, and the last one (pid, why)
    for pid in order_owed(st, now):
        if max_cubes is not None and worked >= max_cubes:
            break
        c = st["cubes"][pid]
        now = now_fn()
        if merge_owed(c):
            if stop:
                continue            # the breaker tripped (or armed again) this iteration: serving goes on
            c["bytes"], c["est_min"] = _est_min(pid)       # fresh: a new release changes the size
            snap = {k: c[k] for k in _RESTORE_FIELDS if k in c}
            attempt: dict = {}
            try:
                r = merge_cube(pid, c, st, progress, now, attempt)
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
            print(_merge_line(pid, r, c), flush=True)
            status = attempt.get("http_status") if r in ("transient", "definitive") else None
            if status is not None:
                refused.setdefault(status, []).append((pid, snap, r, bool(attempt.get("pending_quarantine"))))
            if not decided and r != "absent":                  # 'absent' asked StatCan nothing: not a decision
                ho_tries += 1
                ho_last = (pid, str(c.get("last_error") or r))
                if attempt.get("host_answered") or (status is not None and status != half_open_status):
                    # STATCAN ANSWERED: the fetch returned, a 200 it could not use, or ANOTHER status (an answer
                    # about this cube, not the trip's refusal - verification review G2/W2). The breaker closes; a
                    # refusal counted here still counts toward a new trip under the closed rule.
                    decided = True
                    _breaker_close(st, pid)
                elif ho_tries >= BREAKER_MIN_CUBES:
                    # BREAKER_MIN_CUBES half-open attempts and no answer (the trip's status, no answer at all, or a
                    # fault before the fetch): one cube failing for its own reason does not decide alone (G1/W1)
                    decided, stop = True, True
                    if refused.get(half_open_status):          # this iteration's refusals with the trip's status
                        _trip_breaker(st, half_open_status, refused[half_open_status], now)
                        tripped = half_open_status
                    else:
                        _rearm_breaker(st, now, *ho_last)
            elif status is not None and len(refused[status]) >= BREAKER_MIN_CUBES and not stop:
                _trip_breaker(st, status, refused[status], now)
                tripped, stop = status, True
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
    # A HALF-OPEN ITERATION THAT RAN OUT OF CUBES before it decided (fewer than BREAKER_MIN_CUBES attempts, none
    # answered): it trips again with the trip-status refusals it saw, or arms the hold again, so the next hold is
    # armed and nothing is retried every 5 minutes (no loop, V1).
    if not decided and ho_tries:
        if refused.get(half_open_status):
            _trip_breaker(st, half_open_status, refused[half_open_status], now)
            tripped = half_open_status
        else:
            _rearm_breaker(st, now, *ho_last)
        save_state(st)
    # THE QUARANTINES HELD FOR THE BREAKER DECISION (design review C3): a status that refused fewer than
    # BREAKER_MIN_CUBES cubes this iteration is about those cubes, so a strike that reached the quarantine rule
    # quarantines now. A tripped status's cubes were put back by _trip_breaker.
    applied = 0
    for status, rows in refused.items():
        if status == tripped:
            continue
        for pid, _snap, _r, pending in rows:
            if pending:
                c = st["cubes"][pid]
                _quarantine(c, pid, str(c.get("last_error") or ""))
                applied += 1
    if applied:
        save_state(st)
    # THE STORE-ABSENT GUARD (R753): cubes released and NONE found in the store means the store
    # is unreachable from this backend, not that StatCan released only new cubes.
    if absent and not held:
        stored = [f for f in blob.list_parquets(sc.OUT_DIR) if not os.path.basename(f).startswith("_")]
        if not stored:
            progress.beat("store_unreachable", current=None)
            raise SystemExit(f"statcan lane: the store holds ZERO cubes under {sc.OUT_DIR} "
                             f"(backend={config.BACKEND}) while {absent} released cube(s) were "
                             f"looked for - the store is unreachable, not a quiet publisher")
    progress.beat("enumeration_failing" if st.get("enum_fail") else "idle", current=None)
    out = {"enumerated": listed is not None, "worked": worked, "held": held, "absent": absent}
    if tripped is not None:
        out["breaker"] = tripped
    if listed is None and st.get("enum_fail"):
        out["enum_failed"] = True
    return out


def _enum_failed(st: dict, e: BaseException, now: dt.datetime) -> None:
    """Record one failed enumeration in the state (`enum_fail`), back off, and say so on STDOUT with flush - the
    guard's stall check reads the newest log that is not *.err.log (design review F8.3). A fault that is not a
    StatCan answer (a code bug) also prints its traceback there: it must be readable, and it turns red like any
    other failure after statcan.ENUM_FAIL_RED_HOURS. The text carries no body bytes (statcan._call keeps them in
    its own log line)."""
    ef = st.get("enum_fail") or {}
    n = int(ef.get("count") or 0) + 1
    since = ef.get("since") or _iso(now)
    nxt = now + dt.timedelta(minutes=min(ENUM_BACKOFF_MIN * 2 ** (n - 1), ENUM_BACKOFF_MAX_MIN))
    err = f"{type(e).__name__}: {str(e)[:300]}"
    st["enum_fail"] = {"since": since, "count": n, "last_utc": _iso(now), "retry_after": _iso(nxt),
                       "http_status": getattr(e, "http_status", None), "last_error": err}
    print(f"[lane] {_iso(now)} ENUMERATION FAILED (attempt {n} since {since}): {err} - the lane stays up and "
          f"tries again at {_iso(nxt)}; statcan reads red once this has lasted {sc.ENUM_FAIL_RED_HOURS:.0f} h",
          flush=True)
    if not isinstance(e, (TransientError, DefinitiveError)):
        traceback.print_exception(type(e), e, e.__traceback__, file=sys.stdout)
        sys.stdout.flush()


def _breaker_close(st: dict, pid: str) -> None:
    """The half-open attempt got an answer from StatCan (the fetch returned, or a 200 it could not use): the host
    answers again. The trip's record goes, and every cube that waits out a TRANSIENT backoff from that trip's status
    (`refused_http`) is due again - it waited for the host, not for itself. A struck cube carries no refused_http
    (_deterministic pops it), so its wait stands."""
    b = st.pop("breaker", None)
    status = b.get("status") if isinstance(b, dict) else None
    freed = []
    for p, c in st["cubes"].items():
        if status is not None and c.get("refused_http") == status and c.get("retry_after") and not c.get("quarantined"):
            c.pop("retry_after", None)
            freed.append(p)
    print(f"[lane] circuit breaker cleared: StatCan answered {pid}'s fetch; {len(freed)} cube(s) waiting out a "
          f"transient backoff from HTTP {status} are due again ({b})", flush=True)


def _rearm_breaker(st: dict, now: dt.datetime, pid: str, why: str) -> None:
    """The half-open attempts got no answer and none was a refusal with the trip's status (no answer at all, a
    fault before the fetch): the hold is armed again and doubles (cap BREAKER_BACKOFF_MAX_MIN), so no merge is
    attempted until it ends (verification review F1/V1). Each attempt's own outcome (a backoff) stands."""
    b = st["breaker"]
    trips = int(b.get("trips") or 0) + 1
    until = now + dt.timedelta(minutes=min(BREAKER_BACKOFF_MIN * 2 ** (trips - 1), BREAKER_BACKOFF_MAX_MIN))
    b.update(trips=trips, until=_iso(until), rearmed_utc=_iso(now), last_probe=pid)
    print(f"[lane] circuit breaker armed again: the half-open attempts (last {pid}) got no answer from StatCan "
          f"({why[:160]}) - merge attempts wait until {_iso(until)} (trip {trips}, HTTP {b.get('status')})",
          flush=True)


def _trip_breaker(st: dict, status: int, rows: list, now: dt.datetime) -> None:
    """HTTP `status` refused BREAKER_MIN_CUBES or more cubes this iteration: the host, not the cubes. Every cube it
    refused with a strike gets its fields back as they were before the attempt (quarantine included - its
    QUARANTINED line was held, never printed) and is booked TRANSIENT instead; a transient refusal already is. The
    lane-level hold keeps every merge-owed cube back until `until` (no strike, no transient count for them)."""
    prev = st.get("breaker") if isinstance(st.get("breaker"), dict) else {}
    trips = int(prev.get("trips") or 0) + 1
    until = now + dt.timedelta(minutes=min(BREAKER_BACKOFF_MIN * 2 ** (trips - 1), BREAKER_BACKOFF_MAX_MIN))
    pids = []
    for pid, snap, r, _pending in rows:
        pids.append(pid)
        if r != "definitive":
            continue
        c = st["cubes"][pid]
        why = str(c.get("last_error") or "")
        for k in _RESTORE_FIELDS:
            c.pop(k, None)
        c.update(snap)
        _backoff(c, f"CIRCUIT BREAKER (HTTP {status} refused {len(rows)} cube(s) in one iteration) - booked "
                    f"transient, no strike: {why}", now)
        c["refused_http"] = status
    # `since` is the streak's first trip (kept by a re-trip and a re-arm, gone with the record when StatCan
    # answers): statcan.verdict() reads a breaker held longer than BREAKER_RED_HOURS as red
    st["breaker"] = {"status": status, "cubes": pids, "trips": trips, "tripped_utc": _iso(now),
                     "until": _iso(until), "since": prev.get("since") or _iso(now)}
    print(f"[lane] CIRCUIT BREAKER: HTTP {status} refused {len(rows)} cube(s) in this iteration "
          f"({', '.join(pids)}) - no strike is booked for HTTP {status}; merge attempts stop until {_iso(until)} "
          f"(trip {trips}); serving goes on", flush=True)


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


REFUSED_WHY = ("jobs/statcan_lane.py merges into and serves the R2 store; its self-hosted backend and writer "
               "lock are a T0 decision not made yet (POST_T0_READY = False)")
REFUSED_LOG_EVERY_S = 3600      # well inside the guard's 3 h stall check, which keys on this log's mtime


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--max-cubes", type=int, default=None,
                    help="stop after this many cubes (a trial run; the rest stay owed)")
    ap.add_argument("--once", action="store_true", help="one iteration, then exit (a trial run)")
    a = ap.parse_args(argv)
    # ONE PROCESS, LOOPING, THE LOCK HELD ONLY WHILE WORKING. A launch-per-guard-tick lane left a
    # log pair every 5 minutes (~288 a day) that nothing prunes; a lane holding the lock for its
    # whole life would refuse every other statcan writer - including every all-source
    # core.derive_csv run - for ever. So: iterate, take the lock for the iteration, release it,
    # sleep. An exception ends the process loudly and the guard relaunches it - EXCEPT a failed enumeration, which
    # run() records and backs off (2026-11-22, R2026: 210 relaunch deaths on one 403).
    #
    # BEFORE T0 ONLY (module docstring). After the cutover the lane would merge into and serve the frozen
    # R2 copy, so it REFUSES - but it STAYS RESIDENT and re-checks the flag every iteration, touching
    # nothing and writing no file (port review rounds 1-2). A raise-and-exit would crash-loop: the guard
    # relaunches a dead job every 5 minutes. A self-written logs/statcan_lane.DONE would retire it for good
    # (the R475 class): core.cutover.is_cut_over() FAILS CLOSED on any error reading the flag, so one
    # unreadable ProgramData ACL would stop statcan permanently with no T0. Resident, a false positive
    # clears itself. It says so about once an hour, which keeps the guard's stall check quiet and the log
    # honest. --once (a hand trial) raises instead: a person running it should see the refusal at once.
    held_by_other_logged = False
    pinned = False
    refused_logged_at = None
    while True:
        if cutover.is_cut_over():
            if a.once:
                cutover.refuse_if_cut_over(REFUSED_WHY)
            if refused_logged_at is None or time.monotonic() - refused_logged_at >= REFUSED_LOG_EVERY_S:
                print(f"[lane] {_iso(_now())} REFUSING after T0: {REFUSED_WHY}. Waiting - nothing is "
                      f"written; the lane resumes by itself if the flag clears", flush=True)
                refused_logged_at = time.monotonic()
            time.sleep(IDLE_SLEEP_S)
            continue
        if refused_logged_at is not None:
            print(f"[lane] {_iso(_now())} the cutover flag cleared - resuming", flush=True)
            refused_logged_at = None
        if not pinned:
            pin_backend()               # only once the flag is clear: after T0 a launcher may set selfhost
            pinned = True
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
