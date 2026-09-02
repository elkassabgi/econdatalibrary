#!/usr/bin/env python3
"""CBS Netherlands OData v3 ingest — full statistical catalog.

License: CC BY 4.0 (Statistics Netherlands open data)
Source: opendata.cbs.nl/OData (v3 REST API)
Catalog: https://opendata.cbs.nl/ODataCatalog/Tables?$format=json

Strategy:
  * List all tables from the OData catalog (~8000+ tables)
  * For each table: GET /OData/{tableId}/TypedDataSet paginated
  * Map Period dimension → obs_date; all other dims → series_key
  * One Parquet per table; fully resumable

Run: python jobs/ingest_cbs_nl.py
     python jobs/ingest_cbs_nl.py --only 83439NED,37230NED
"""
from __future__ import annotations
import datetime as dt, json, os, sys, time, urllib.parse
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT   = os.path.join(ROOT, "data", "clean_full", "cbs_nl")
CAT   = "https://opendata.cbs.nl/ODataCatalog/Tables"
BASE  = "https://opendata.cbs.nl/ODataFeed/odata"   # ODataApi doesn't support $skip; ODataFeed does
UA    = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
         "Accept": "application/json"}
PAGE  = 10000


def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


LAST_ERROR: dict = {}      # why the most recent get_json gave up; read by the caller


def get_json(url: str, retries: int = 4) -> dict | list | None:
    LAST_ERROR.clear()
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=120)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 404, 500):  # 500 is permanent for CBS NL dead tables
                body = " ".join((r.text or "").split())[:200]
                LAST_ERROR.clear()
                LAST_ERROR.update(status=r.status_code, body=body, url=url)
                # Say WHY. This returned None silently, so a permanent upstream fault
                # surfaced only as "fetch failed at skip=N" with no reason, and the same
                # request was reissued every 68 minutes for 315 consecutive runs.
                log(f"  HTTP {r.status_code} (permanent): {body}")
                return None
            if r.status_code in (503, 429):
                log(f"  {r.status_code} throttle, sleeping 60s")
                time.sleep(60); continue
            log(f"  HTTP {r.status_code} attempt {attempt+1}: {url[-80:]}")
        except Exception as e:
            log(f"  ERR attempt {attempt+1}: {e}")
        time.sleep(8 * (attempt + 1))
    return None


BROKEN_FILE = "_upstream_broken.json"
BROKEN_RECHECK_DAYS = 7    # CBS may repair a table; never write it off permanently


def _broken_path(out_dir: str) -> str:
    return os.path.join(out_dir, BROKEN_FILE)


def load_broken(out_dir: str) -> dict:
    try:
        with open(_broken_path(out_dir), encoding="utf-8") as f:
            return json.load(f)
    except Exception:                                               # noqa: BLE001
        return {}


def mark_broken(out_dir: str, table_id: str, status: int, reason: str) -> None:
    """Record an upstream fault so the next run does not reissue the same request.

    CBS serves HTTP 500 with its own diagnostic for tables whose stored data it cannot
    read - "Fout bij het lezen van kolom 'New0': Unable to read beyond the end of the
    stream". That is corruption on their side and no amount of retrying fixes it. Before
    this, 37830 and 70745ned failed identically on every run since 2026-07-28: 315 passes,
    each re-walking 5,951 already-crawled tables for roughly 68 minutes, each ending with
    "checkpointed for resume next run" against a checkpoint that never moved.

    Recorded with a timestamp rather than a permanent blacklist, so a table CBS repairs is
    picked up again after BROKEN_RECHECK_DAYS instead of being written off forever.
    """
    reg = load_broken(out_dir)
    now = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    entry = reg.get(table_id) or {"first_seen": now}
    entry.update(status=status, reason=reason, last_seen=now)
    reg[table_id] = entry
    with open(_broken_path(out_dir), "w", encoding="utf-8") as f:
        json.dump(reg, f, indent=1, sort_keys=True)
    log(f"  {table_id}: recorded as upstream-broken ({status}); "
        f"will not be retried for {BROKEN_RECHECK_DAYS} days")


def broken_recently(out_dir: str, table_id: str) -> str | None:
    e = load_broken(out_dir).get(table_id)
    if not e:
        return None
    try:
        seen = dt.datetime.fromisoformat(e["last_seen"])
    except Exception:                                               # noqa: BLE001
        return None
    age = (dt.datetime.now(dt.timezone.utc) - seen).days
    if age >= BROKEN_RECHECK_DAYS:
        return None
    return f"{e.get('status')} {str(e.get('reason'))[:90]} ({age}d ago)"


MODIFIED_FILE = "_modified.json"
DEFERRED_FILE = "_repull_deferred.json"
ZERO_FILE = "_repull_zero.json"        # vintages whose re-pull produced ZERO observations (R589)
REFUSED_FILE = "_repull_refused.json"  # vintages whose re-pull was refused by the replacement floor
REPLACE_FLOOR = 0.5                     # a re-pull that keeps < 50% of the served rows is refused
ACCEPT_FILE = "_accept_shrink.json"     # the operator's explicit yes to a shrink, IN THE STORE (R598):
FORCE_SWEEP = False                     # --force-sweep: run the marker sweep on a < 90% catalogue listing
PROBE_FAIL_FILE = "_probe_failures.json"

# A revised table is re-pulled automatically only up to this size. Set from the MEASURED
# distribution of the 329 tables CBS had revised on 2026-08-24, not from a round number:
# median 22,560 rows, p90 2,044,848, and then five outliers (25.2M, 59.9M, 93.7M, 106.2M,
# 119.2M) holding 404.1M of the 680.0M total. 25M sits far above the body and below every
# outlier, so 324 of the 329 re-pull by themselves and the five expensive ones are RECORDED
# for an explicit decision rather than silently churning the crawler for days. R469: a
# ceiling the caller can forget is not a ceiling, so it lives here, not in the caller.
REPULL_MAX_ROWS = 25_000_000


def _mod_path(out_dir: str) -> str:
    return os.path.join(out_dir, MODIFIED_FILE)


def load_modified(out_dir: str) -> dict:
    try:
        with open(_mod_path(out_dir), encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def record_modified(out_dir: str, table_id: str, modified: str) -> None:
    """Record the vintage we just ingested. Written per table, atomically.

    Per table rather than per run because this crawler is killed and relaunched routinely
    (the guard restarts it; reboots interrupt it). A manifest flushed only at the end of a
    pass would lose a run of work every time and re-pull what it had already done.
    """
    if not modified:
        return
    m = load_modified(out_dir)
    if m.get(table_id) == modified:
        return
    m[table_id] = modified
    tmp = _mod_path(out_dir) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(m, f, indent=0, sort_keys=True)
    os.replace(tmp, _mod_path(out_dir))


def note_deferred_repull(out_dir: str, table_id: str, modified: str, rows: int) -> None:
    """Record a revision we are NOT acting on, so it is visible rather than forgotten."""
    p = os.path.join(out_dir, DEFERRED_FILE)
    try:
        with open(p, encoding="utf-8") as f:
            d = json.load(f)
    except Exception:
        d = {}
    if d.get(table_id, {}).get("upstream_modified") == modified:
        return
    d[table_id] = {"upstream_modified": modified, "rows": rows,
                   "ceiling": REPULL_MAX_ROWS,
                   "noted": dt.datetime.now().isoformat(timespec="seconds")}
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(d, f, indent=1, sort_keys=True)
    os.replace(tmp, p)


def _note_vintage(out_dir: str, fname: str, table_id: str, modified: str, extra: dict) -> None:
    p = os.path.join(out_dir, fname)
    try:
        with open(p, encoding="utf-8") as f:
            d = json.load(f)
    except Exception:
        d = {}
    d[table_id] = {"upstream_modified": modified, "noted": dt.datetime.now().isoformat(timespec="seconds"), **extra}
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(d, f, indent=1, sort_keys=True)
    os.replace(tmp, p)


def load_accepts(out_dir: str) -> dict:
    """{tid: {"vintage": <CBS stamp the operator saw refused>, "refused_rows": n}} (R600: a
    vintage-less yes was spent on a later, smaller re-pull the operator never saw)."""
    try:
        with open(os.path.join(out_dir, ACCEPT_FILE), encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _write_accepts(out_dir: str, d: dict) -> None:
    p = os.path.join(out_dir, ACCEPT_FILE)
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(d, f, indent=1, sort_keys=True)
    os.replace(tmp, p)


def record_accepts(out_dir: str, tids) -> None:
    """Record the operator's yes AGAINST the refusal it answers: the vintage and row count in
    `_repull_refused.json`. A tid with no refusal on record gets nothing (logged)."""
    try:
        with open(os.path.join(out_dir, REFUSED_FILE), encoding="utf-8") as f:
            refused = json.load(f)
    except Exception:
        refused = {}
    cur = load_accepts(out_dir)
    for tid in tids:
        r = refused.get(tid)
        if not r or not r.get("upstream_modified"):
            log(f"  --accept-shrink {tid}: nothing refused on record - not recorded")
            continue
        if r.get("reason") or not r.get("repull_rows"):
            log(f"  --accept-shrink {tid}: the refusal is '{r.get('reason') or 'no row count'}', not a shrink - "
                f"nothing to accept, not recorded (R604)")
            continue
        cur[tid] = {"vintage": r["upstream_modified"], "refused_rows": r.get("repull_rows"),
                    "served_rows": r.get("served_rows"), "noted": dt.datetime.now().isoformat(timespec="seconds")}
        log(f"  --accept-shrink {tid}: recorded for CBS vintage {r['upstream_modified']} "
            f"({r.get('served_rows')} -> {r.get('repull_rows')} rows)")
    _write_accepts(out_dir, cur)


ACCEPT_YIELD_FLOOR = 0.9   # an accepted re-pull must keep >= 90% of the rows the operator was shown


def accept_applies(out_dir: str, table_id: str, modified: str, new_rows=None) -> bool:
    """True only while CBS's stamp is the one the operator accepted AND, when the new yield is
    known, it is at least ACCEPT_YIELD_FLOOR of the count the operator saw (R604: a same-stamp
    re-pull yielding 5 rows was replaced on a yes given for 30)."""
    e = load_accepts(out_dir).get(table_id)
    if not e or e.get("vintage") != modified:
        return False
    if new_rows is not None and e.get("refused_rows"):
        return new_rows >= ACCEPT_YIELD_FLOOR * int(e["refused_rows"])
    return True


def drop_accept(out_dir: str, table_id: str, why: str) -> None:
    cur = load_accepts(out_dir)
    if table_id in cur:
        e = cur.pop(table_id)
        _write_accepts(out_dir, cur)
        log(f"  {table_id}: accepted shrink for vintage {e.get('vintage')} DROPPED - {why}")


def _bump_counter(out_dir: str, fname: str, table_id: str) -> int:
    p = os.path.join(out_dir, fname)
    try:
        with open(p, encoding="utf-8") as f:
            d = json.load(f)
    except Exception:
        d = {}
    d[table_id] = int(d.get(table_id, 0)) + 1
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(d, f, indent=1, sort_keys=True)
    os.replace(tmp, p)
    return d[table_id]


def _own_start_time() -> float:
    try:
        import psutil
        return float(psutil.Process().create_time())
    except Exception:
        return 0.0


def _reset_counter(out_dir: str, fname: str, table_id: str) -> None:
    p = os.path.join(out_dir, fname)
    try:
        with open(p, encoding="utf-8") as f:
            d = json.load(f)
    except Exception:
        return
    if table_id in d:
        del d[table_id]
        tmp = p + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f, indent=1, sort_keys=True)
        os.replace(tmp, p)


def _pid_alive(pid: int) -> bool:
    try:
        import psutil
        return psutil.pid_exists(int(pid))
    except Exception:
        return True   # cannot tell -> assume alive (never delete a possible owner's state)


STALE_STATE_HOURS = 2.0   # marker/checkpoint/parts touched more recently than this belong to a live run


def state_recently_touched(out_dir: str, table_id: str) -> bool:
    """R598: never clean re-pull state a running writer is still producing - a checkpoint or part
    written within STALE_STATE_HOURS is in flight whichever process wrote it."""
    import glob
    paths = [_repull_marker(out_dir, table_id), os.path.join(out_dir, f"{table_id}.ckpt.json")]
    paths += glob.glob(os.path.join(out_dir, f"{table_id}.part*.parquet"))
    newest = max((os.path.getmtime(x) for x in paths if os.path.exists(x)), default=0.0)
    return newest > 0 and (time.time() - newest) < STALE_STATE_HOURS * 3600


def marker_pid_alive(out_dir: str, table_id: str) -> bool:
    """The marker names a pid that is running (start time NOT checked - see marker_owner_alive)."""
    try:
        with open(_repull_marker(out_dir, table_id), encoding="utf-8") as f:
            pid = int(json.load(f).get("pid", 0))
    except Exception:
        return False
    return pid > 0 and pid != os.getpid() and _pid_alive(pid)


def marker_owner_alive(out_dir: str, table_id: str) -> bool:
    """True when the re-pull marker names a pid that is still running (an in-flight run - possibly
    an operator's --accept-shrink run - whose checkpoint and parts must not be touched, R598)."""
    try:
        with open(_repull_marker(out_dir, table_id), encoding="utf-8") as f:
            d = json.load(f)
        pid = int(d.get("pid", 0))
        started = float(d.get("pid_started", 0) or 0)
    except Exception:
        return False
    if not (pid > 0 and pid != os.getpid() and _pid_alive(pid)):
        return False
    if not started:
        return False   # a marker without a start time cannot prove its owner; the recency bound still protects fresh state
    try:
        import psutil
        return abs(psutil.Process(pid).create_time() - started) < 2.0   # R600: a recycled pid is not the owner
    except Exception:
        return True


def _clear_vintage(out_dir: str, fname: str, table_id: str) -> None:
    """Remove a table's entry from a registry (after a replacement, or an accepted shrink)."""
    p = os.path.join(out_dir, fname)
    try:
        with open(p, encoding="utf-8") as f:
            d = json.load(f)
    except Exception:
        return
    if table_id in d:
        del d[table_id]
        tmp = p + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f, indent=1, sort_keys=True)
        os.replace(tmp, p)


def sweep_markers(out_dir: str, catalogue_ids) -> None:
    """R598: a marker for a table CBS withdrew from its catalogue was never closed - nothing
    iterates markers, only the catalogue - and the publish tool skips a marked file for ever."""
    import glob
    for mp in glob.glob(os.path.join(out_dir, "*.repull.json")):
        tid = os.path.basename(mp)[:-len(".repull.json")]
        if catalogue_ids is not None and tid not in catalogue_ids:
            end_repull(out_dir, tid)
            clear_partials(out_dir, tid)
            ck = os.path.join(out_dir, f"{tid}.ckpt.json")
            if os.path.exists(ck):
                os.remove(ck)
            _note_vintage(out_dir, REFUSED_FILE, tid, "", {"reason": "withdrawn-from-catalogue"})
            log(f"  {tid}: re-pull marker closed - the table is no longer in CBS's catalogue "
                f"(recorded in {REFUSED_FILE}); the held copy stays")


def registry_summary(out_dir: str) -> None:
    """What is being HELD BACK, with age - printed at pass start and end (R596: the guard kills
    passes, so an end-only summary rarely prints)."""
    for fname, label in ((DEFERRED_FILE, "deferred (over the re-pull ceiling)"),
                         (ZERO_FILE, "zero-result re-pulls"), (REFUSED_FILE, "floor-refused / undatable re-pulls")):
        try:
            with open(os.path.join(out_dir, fname), encoding="utf-8") as f:
                reg = json.load(f)
        except Exception:
            continue
        if reg:
            oldest = min(v.get("noted", "") for v in reg.values())
            log(f"  registry {fname}: {len(reg)} table(s) {label}, oldest noted {oldest} - "
                f"each is a served copy behind CBS until someone decides")
    import glob
    live = []
    for mp in glob.glob(os.path.join(out_dir, "*.repull.json")):
        tid = os.path.basename(mp)[:-len(".repull.json")]
        age_h = (time.time() - os.path.getmtime(mp)) / 3600.0
        live.append((tid, age_h, marker_owner_alive(out_dir, tid)))
    if live:
        live.sort(key=lambda x: -x[1])
        log(f"  live re-pull markers: {len(live)} - " + ", ".join(
            f"{t} ({h:.1f} h{', owner alive' if a else ''})" for t, h, a in live[:10]))
    acc = load_accepts(out_dir)
    if acc:
        log("  accepted shrinks pending: " + ", ".join(f"{t} (vintage {e.get('vintage')})" for t, e in sorted(acc.items())))


def _vintage_noted(out_dir: str, fname: str, table_id: str, modified: str) -> bool:
    try:
        with open(os.path.join(out_dir, fname), encoding="utf-8") as f:
            return json.load(f).get(table_id, {}).get("upstream_modified") == modified
    except Exception:
        return False


def repull_verdict(out_dir: str, table_id: str, modified: str, out_path: str, rows: int):
    """Should this already-held table be crawled again? None means skip.

    R589: a vintage whose re-pull wrote ZERO observations, or was refused by the replacement
    floor, is recorded and NOT retried until CBS publishes a newer Modified stamp - otherwise
    the crawler re-pulls, writes nothing, keeps the marker, and loops forever.

    THIS IS THE AUTO-UPDATE. Before it the gate was `os.path.exists(out_path)`, so a table
    crawled once was never looked at again: the crawler completed a full 5,953-table pass
    every ~68 minutes and could not, by construction, pick up a single upstream revision.
    That is the shape of R453 — every liveness signal green on a job producing nothing.
    CBS publishes a per-table Modified timestamp in its own catalogue; comparing it against
    the vintage we hold is the entire mechanism.

    The vintage we hold comes from the manifest when there is an entry, and otherwise from
    the parquet mtime. The mtime fallback exists because 5,156 tables were crawled before
    this manifest did, and it is the honest default: it catches a table revised AFTER we
    wrote our copy on the very first run. Seeding every entry as current instead would go
    permanently blind to the 329 revisions that have already happened.

    Returns None (current), "TOO_BIG" (revised but over the ceiling), or the vintage string
    being replaced (re-pull).
    """
    if not modified:
        return None
    try:
        mdt = dt.datetime.fromisoformat(modified)
    except ValueError:
        return None
    held = load_modified(out_dir).get(table_id)
    hdt = None
    if held:
        try:
            hdt = dt.datetime.fromisoformat(held)
        except ValueError:
            hdt = None
    if hdt is None:
        hdt = dt.datetime.fromtimestamp(os.path.getmtime(out_path))
        held = hdt.isoformat(timespec="seconds") + " (mtime)"
    if mdt <= hdt:
        return None
    for fname, why in ((ZERO_FILE, "ZERO observations"), (REFUSED_FILE, "refused by the replacement floor")):
        if fname == REFUSED_FILE and accept_applies(out_dir, table_id, modified):
            continue   # R596/R598/R600: the operator's yes, for THIS vintage, reaches the floor
        if fname == REFUSED_FILE and load_accepts(out_dir).get(table_id):
            drop_accept(out_dir, table_id, f"CBS is now at {modified}, a vintage the operator did not see")
        if _vintage_noted(out_dir, fname, table_id, modified):
            log(f"  {table_id}: CBS vintage {modified} already re-pulled and {why} - not retried "
                f"until CBS publishes a newer stamp (R589)")
            return "NOT_RETRIED"   # R592: NOT None - the None branch records the vintage as held
    if rows > REPULL_MAX_ROWS:
        return "TOO_BIG"
    return held


def _repull_marker(out_dir: str, table_id: str) -> str:
    return os.path.join(out_dir, table_id + ".repull.json")


def repull_in_flight(out_dir: str, table_id: str) -> str:
    """Which upstream vintage the in-progress re-pull of this table is aiming at.

    WITHOUT THIS THE RE-PULL CANNOT FINISH. record_modified only runs on success, so a
    re-pull interrupted part way (the guard relaunches this crawler routinely, and a big
    table takes many hours) still looks un-re-pulled on the next run. The gate would
    decide RE-PULL again and clear_partials would delete the checkpoint the previous
    attempt just wrote — restarting from zero every run, forever, which is precisely the
    livelock of R453 rebuilt in new code. The marker lets the next run recognise its own
    unfinished work and resume it.
    """
    try:
        with open(_repull_marker(out_dir, table_id), encoding="utf-8") as f:
            return json.load(f).get("target") or ""
    except Exception:
        return ""


def begin_repull(out_dir: str, table_id: str, modified: str) -> None:
    tmp = _repull_marker(out_dir, table_id) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"target": modified, "pid": os.getpid(), "pid_started": _own_start_time(),   # the owner (R598/R600)
                   "started": dt.datetime.now().isoformat(timespec="seconds")}, f)
    os.replace(tmp, _repull_marker(out_dir, table_id))


def end_repull(out_dir: str, table_id: str) -> None:
    try:
        os.remove(_repull_marker(out_dir, table_id))
    except OSError:
        pass

def clear_partials(out_dir: str, table_id: str) -> None:
    """Remove any checkpoint and part files before a re-pull.

    Without this the resume branch would find the PREVIOUS crawl checkpoint and continue a
    stale walk, merging old rows into the new copy. A re-pull must start clean.
    """
    ck = os.path.join(out_dir, table_id + ".ckpt.json")
    if os.path.exists(ck):
        os.remove(ck)
    for i in range(1000):
        pp = os.path.join(out_dir, table_id + ".part" + str(i) + ".parquet")
        if os.path.exists(pp):
            os.remove(pp)

def get_catalog() -> list[dict]:
    """Get all CBS tables from the OData catalog."""
    url = f"{CAT}?$format=json&$top=10000"
    result = []
    while url:
        data = get_json(url)
        if not data:
            break
        if isinstance(data, dict):
            result.extend(data.get("value", []))
            url = data.get("odata.nextLink") or data.get("@odata.nextLink")
        else:
            result.extend(data)
            url = None
        if url:
            time.sleep(0.5)
    return result


# A year outside this range is not a period anyone published — see the 4-digit branch below for
# the measured case. Wide on purpose: genuine long history and real projections (un_wpp 2101,
# bfs scenarios to 2150) must be untouched by a guard aimed at classification codes.
_YEAR_LO, _YEAR_HI = 1500, 2100


def _year_ok(y: int) -> bool:
    return _YEAR_LO <= y <= _YEAR_HI


PERIOD_DISCARDS: dict = {}   # family -> count of period codes the parser returned None for / legacy-dated


def _discard(family: str, code: str) -> None:
    """Count what the parser drops or legacy-dates so ingest can LOG it per table (R573 rule 3)."""
    PERIOD_DISCARDS[family] = PERIOD_DISCARDS.get(family, 0) + 1


def _has_53_weeks(yr: int) -> bool:
    return dt.date(yr, 12, 28).isocalendar()[1] == 53


def _week_start(yr: int, w: int):
    """First day of CBS week `w` of calendar year `yr`, calendar-clipped: never before 1 January
    of `yr`; week 53 of a 52-ISO-week year is the Monday after ISO week 52 (the last 1-2 days of
    the year); beyond that None."""
    if w < 1 or w > 53:
        return None
    try:
        d = dt.date.fromisocalendar(yr, w, 1)
    except ValueError:
        if w != 53:
            return None
        d = dt.date.fromisocalendar(yr, 52, 1) + dt.timedelta(days=7)
    if d.year > yr:
        return None
    jan1 = dt.date(yr, 1, 1)
    return jan1 if d < jan1 else d


def discards_since(before: dict) -> dict:
    """Per-table discard delta: what THIS table's parse dropped or legacy-dated."""
    return {k: v - before.get(k, 0) for k, v in PERIOD_DISCARDS.items() if v - before.get(k, 0) > 0}


def parse_cbs_period(s: str) -> dt.date | None:
    """Parse CBS period codes.
    Annual:      '2022JJ00' or '2022'
    Monthly:     '2022MM01'
    Quarterly:   '2022KW01'
    Half-year:   '2022HJ01'
    School year: '2000SJ00'   -> CBS titles this "2000/'01"
    Two school years: '2003X001' -> CBS titles this "2003/'04 - 2004/'05"
    Exact date:  '19990924'   -> CBS titles this "1999, vrijdag 24 september"

    Returning None here DISCARDS THE WHOLE ROW in ingest_table, values and all. The
    three formats below were missing, and because every period of an affected table
    is the same format, that silently discarded 100% of 23 tables — 71493ned alone
    fetched 144,000,000 rows over 60 hours and wrote zero observations, with the
    measure column populated the entire time. Anything added here must be verified
    against the table's own Perioden titles, not guessed from the code letters.
    """
    s = (s or "").strip()
    try:
        if len(s) == 4 and s.isdigit():
            # A BARE 4-DIGIT CODE IS NOT AUTOMATICALLY A YEAR. Table 70170NED has no Perioden
            # dimension at all; its axes are GeboorteperiodeEersteKind and Opleidingsniveau, and
            # the first is PERIOD-NAMED but is a birth-cohort classification whose keys are
            # COMPRESSED YEAR RANGES — read live from CBS on 2026-08-03:
            #     '8589' = "Geboorteperiode: 1985-1989"
            #     '9094' = "Geboorteperiode: 1990-1994"
            #     '9597' = "Geboorteperiode: 1995-1997"
            # Unbounded, '9597' became the year 9597 — exactly the worst obs_date in cbs_nl's
            # store, across four files.
            #
            # Out of range yields None, which discards the row, and for a table like this that
            # is the correct outcome rather than a loss: it is a cross-tabulation with no time
            # axis, so it has no time-series observations to contribute. Note the docstring
            # above warns that None discards the whole row — that warning is about MISSING
            # formats, where real periods were being thrown away. This is the opposite case:
            # refusing to invent a period that was never published.
            y = int(s)
            return dt.date(y, 12, 31) if _year_ok(y) else None
        # Exact date, YYYYMMDD. MUST precede the generic <year><code> branch, which
        # would read '19990924' as year 1999 + code '09' and fall through to None.
        if len(s) == 8 and s.isdigit():
            y = int(s[:4])
            return dt.date(y, int(s[4:6]), int(s[6:8])) if _year_ok(y) else None
        if len(s) >= 6 and s[:4].isdigit():
            yr = int(s[:4]); rest = s[4:].upper()
            if not _year_ok(yr):
                return None
            if rest[:2] == "JJ":          # annual
                return dt.date(yr, 12, 31)
            # Dutch academic year yr/yr+1, dated to its END — consistent with JJ
            # dating an annual period to its last day, and correct for these tables,
            # whose measures (graduates, enrolments) are realised when the year ends.
            if rest[:2] == "SJ":          # schooljaar
                return dt.date(yr + 1, 7, 31)
            if rest == "X000":
                # CBS's generic "other period" slot: 15 meanings across 55 tables ("week 0
                # (3 dagen)", "januari-september", "Standaardfout", 5-year spans, "Oude
                # methode" school years ...) and the ONLY code family of 8 served tables
                # (136,862 rows). No global date is right, and None would empty those tables
                # (R589). The LEGACY dating (the X0 branch: yr+2, 31 July) is KEPT so the
                # served content is unchanged, and every occurrence is COUNTED so the
                # title-driven rule (open) can be sized. Never silently.
                _discard("X000-legacy-dated", s)
                return dt.date(yr + 2, 7, 31)
            # Span of TWO academic years starting at yr, so it ends with the year
            # beginning yr+1 -> July of yr+2 ('2003X001' is titled "2003/'04 - 2004/'05").
            if rest[:2] == "X0":          # two-school-year span
                return dt.date(yr + 2, 7, 31)
            if rest[:2] == "KW":          # quarter
                q = int(rest[2:4]) if rest[2:4].isdigit() else 0
                return dt.date(yr, (q-1)*3+1, 1) if 1 <= q <= 4 else None
            if rest[:2] == "MM":          # month
                m = int(rest[2:4]) if rest[2:4].isdigit() else 0
                return dt.date(yr, m, 1) if 1 <= m <= 12 else None
            if rest[:2] == "HJ":          # half-year: 'HJ01' = "1e halfjaar", 'HJ02' = "2e halfjaar"
                # R573: `rest[2:3]` read the '0' of 'HJ01', so BOTH halves dated to 1 July and
                # 86156NED held 455,024 duplicate (key, date) pairs with conflicting values.
                h = int(rest[2:4]) if rest[2:4].isdigit() else 0
                return dt.date(yr, 1 if h == 1 else 7, 1) if h in (1, 2) else None
            if rest[:1] == "W" and rest[1:].isdigit():
                # Week codes are 'W<k><nn>': k = weeks per period, nn = the period's ordinal.
                #   W1nn  one week    (70895ned: '1971W101' "1971 week 1" .. 'W153' "week 53 (1 dag)")
                #   W4nn  four weeks  (37456/72006: 'W401' "week 01 - 04" .. 'W413' "week 49 - 52";
                #                      'W415' or 'W417' "week 01 - 52 (gemiddelde)" = the annual
                #                      average; in 53-ISO-week years ALSO 'W414' "week 49 - 53
                #                      (5 weken)" and 'W417' "week 01 - 53 (gemiddelde)")
                # R573: `rest[1:3]` read '12' from 'W127' and collapsed 3,004 periods onto 413
                # dates. R588: CBS weeks are CALENDAR-YEAR-CLIPPED (week 1 starts on 1 January
                # when the ISO Monday is in December; week 53 exists in 52-ISO-week years).
                # R589: the 53-week-year variants share their START with W413 / the average, so
                # a period-start date cannot carry them - dropped and counted (a variant token
                # in the key is the fix; Ahmed's decision).
                if len(rest) == 4:
                    k, nn = int(rest[1]), int(rest[2:4])
                    if k == 0 or nn == 0:
                        _discard("W-zero", s)
                        return None
                    if k == 4 and nn >= 14:
                        # 53-ISO-week years carry variants beside the regular codes (37456,
                        # 72006ned, 2004/2009): W414 "week 49 - 53 (5 weken)" shares its START
                        # with W413, and W417 "week 01 - 53 (gemiddelde)" is a second annual
                        # average beside W415 "week 01 - 52 (gemiddelde)". A period-start date
                        # cannot carry two codes on one day, and dropping either loses data, so
                        # each variant gets a deterministic, distinct date and is COUNTED:
                        #   W414 -> the Monday of week 53 (its extra week);
                        #   W415 -> 31 Dec, or 30 Dec in a 53-week year (the average excluding
                        #           week 53); W417 (and any other average code) -> 31 Dec.
                        if nn == 14:
                            _discard("W4-53wk-variant-dated-by-extra-week", s)
                            w = 53
                        elif nn == 15 and _has_53_weeks(yr):
                            _discard("W4-52wk-average-in-53wk-year-dated-30-dec", s)
                            return dt.date(yr, 12, 30)
                        else:
                            return dt.date(yr, 12, 31)
                    else:
                        w = (nn - 1) * k + 1
                else:                                      # legacy 2-digit form, kept for safety
                    w = int(rest[1:3])
                d = _week_start(yr, w)
                if d is None:
                    _discard("W-out-of-range", s)
                return d
        # ISO date
        if len(s) == 10 and s[4] == "-":
            return dt.date.fromisoformat(s[:10])
    except Exception:
        pass
    return None


def get_table_columns(table_id: str) -> list[str] | None:
    """Get column names from first row of TypedDataSet."""
    url = f"{BASE}/{table_id}/TypedDataSet?$top=1"  # $top = $top (OData)
    data = get_json(url)
    if not data:
        return None
    rows = data.get("value", []) if isinstance(data, dict) else data
    if not rows:
        return None
    return list(rows[0].keys())


PARTITION_MIN_ROWS = 3_000_000     # below this, deep-offset cost is not worth partitioning


def table_row_count(table_id: str) -> int | None:
    """Total TypedDataSet rows, or None if the endpoint won't say."""
    try:
        r = requests.get(f"{BASE}/{table_id}/TypedDataSet/$count", headers=UA, timeout=120)
        return int(r.text.strip()) if r.status_code == 200 and r.text.strip().isdigit() else None
    except Exception:
        return None


def period_keys(table_id: str, period_col: str = "Perioden") -> list[str]:
    """The table's period-dimension values (partition keys), oldest first.

    Takes the ACTUAL column name. This was hardcoded to "Perioden", so for a table whose
    time dimension is named otherwise — 84808NED/84809NED use `JaarVanImmigratie` — it
    requested a dimension that does not exist, got [], and partitioning silently declined,
    leaving a 23-57M-row table on the quadratic deep-$skip walk. Same hardcoding mistake
    as the period-column detector it was written to support.
    """
    data = get_json(f"{BASE}/{table_id}/{period_col}?$format=json")
    if not data:
        return []
    rows = data.get("value", []) if isinstance(data, dict) else data
    return [r.get("Key") for r in rows if r.get("Key")]


_PERIOD_EXACT = ("perioden", "periods", "jaar", "period", "datum", "t_period")


def _find_period_col(table_id: str, cols: list[str]) -> str | None:
    """The column carrying the observation period, or None if the table has none.

    Exact-name matching alone is not enough. CBS names the time dimension after what it
    measures: 84809NED's is `JaarVanImmigratie` ("year of immigration"), which is not
    equal to "jaar" and so went undetected — the table then fetched 38,500,000 of its
    57,139,992 rows and wrote ZERO observations, because an undated row is dropped.

    So: exact match first, then any column whose NAME suggests a year/period AND whose
    VALUES actually parse as CBS periods. The value check is what keeps `Leeftijd` (age)
    and `MinderDan10VanDeTijd_9` (a measure) out — both merely contain "tijd", neither
    parses as a period.
    """
    exact = next((c for c in cols if c.lower() in _PERIOD_EXACT), None)
    if exact:
        return exact
    named = [c for c in cols
             if any(w in c.lower() for w in ("jaar", "year", "period", "datum"))]
    if not named:
        return None
    probe = get_json(f"{BASE}/{table_id}/TypedDataSet?$top=25")
    rows = (probe.get("value", []) if isinstance(probe, dict) else probe) or []
    if not rows:
        return None
    for c in named:
        vals = [r.get(c) for r in rows if r.get(c) not in (None, "")]
        if not vals:
            continue
        ok = sum(1 for v in vals if parse_cbs_period(str(v).strip()) is not None)
        if ok >= max(1, int(0.8 * len(vals))):     # the column really holds periods
            log(f"  {table_id}: period column detected by value = {c!r}")
            return c
    return None


def ingest_table(table_id: str, title: str, out_dir: str, modified: str = "") -> int:
    """Download all observations for one CBS table. Returns obs count.

    PARTITIONING: `$skip` on this API is O(offset) — measured on 71493ned,
    a 10,000-row page costs 2.8 s at $skip=0, 14.9 s at 40M and 46.1 s at 144M.
    Walking a large table with one growing offset is therefore QUADRATIC: 282.7M
    rows works out to ~14.8 days. Splitting on the Perioden dimension and filtering
    (`$filter=Perioden eq '...'`) keeps every offset shallow — the same table becomes
    22 partitions of ~12.8M rows, ~37 h, and each partition is independently
    resumable. Bigger pages do not help ($top is capped at 10,000 server-side) and
    neither does $select (payload halves, time does not).
    """
    out_path = os.path.join(out_dir, f"{table_id}.parquet")
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        verdict = repull_verdict(out_dir, table_id, modified, out_path, n)
        if verdict == "NOT_RETRIED":
            # The served copy is the OLD vintage; the manifest must keep saying so (R592: the
            # None branch below would stamp the NEW stamp and certify the freeze as current).
            # R596: an interrupt between recording and closing may have left a marker or a
            # checkpoint - clean them here, idempotently, so publishing is never blocked.
            if repull_in_flight(out_dir, table_id):
                if marker_owner_alive(out_dir, table_id) or state_recently_touched(out_dir, table_id):
                    return n   # R598: an in-flight run (an operator's accept run) owns this state
                end_repull(out_dir, table_id)
            clear_partials(out_dir, table_id)
            ck = os.path.join(out_dir, f"{table_id}.ckpt.json")
            if os.path.exists(ck):
                os.remove(ck)
            return n
        if verdict is None:
            log(f"  skip {table_id} ({n:,} rows)")
            record_modified(out_dir, table_id, modified)
            return n
        if verdict == "TOO_BIG":
            note_deferred_repull(out_dir, table_id, modified, n)
            log(f"  skip {table_id} ({n:,} rows) - CBS revised it {modified}, but it is "
                f"over the {REPULL_MAX_ROWS:,}-row automatic re-pull ceiling; recorded "
                f"in {DEFERRED_FILE} for an explicit decision")
            return n
        if repull_in_flight(out_dir, table_id) == modified:
            if marker_owner_alive(out_dir, table_id) or (marker_pid_alive(out_dir, table_id)
                                                          and state_recently_touched(out_dir, table_id)):
                log(f"  {table_id}: re-pull to {modified} is in flight in another process - standing down (R600/R604)")
                return n
            log(f"  RE-PULL {table_id}: resuming the in-flight re-pull to {modified} "
                f"(keeping its checkpoint; the {n:,}-row copy is still serving)")
        else:
            log(f"  RE-PULL {table_id}: CBS revised it {modified}; we hold {verdict}. "
                f"The {n:,}-row copy stays in place until the new crawl completes.")
            clear_partials(out_dir, table_id)
            begin_repull(out_dir, table_id, modified)

    # Discover columns from first row
    cols = get_table_columns(table_id)
    if cols is not None:
        _reset_counter(out_dir, PROBE_FAIL_FILE, table_id)
    if cols is None:
        if os.path.exists(out_path) and repull_in_flight(out_dir, table_id):
            end_repull(out_dir, table_id)   # transient: retried next pass, marker not left open (R596)
            k = _bump_counter(out_dir, PROBE_FAIL_FILE, table_id)
            log(f"  {table_id}: column probe failed during a re-pull ({k} consecutive) - marker closed, will retry")
        return 0  # table unavailable

    # Identify period and value columns
    period_col = _find_period_col(table_id, cols)
    discards_before = dict(PERIOD_DISCARDS)   # AFTER the probe (R592): per-table delta for DONE / ZERO lines
    if period_col is None:
        # NO TIME COLUMN -> every row would be discarded, because the row loop does
        #   d = parse_cbs_period(row.get(period_col or "Perioden", ""))
        #   if d is None: continue
        # and "Perioden" is not present. Previously this crawled the whole table and
        # threw away 100% of it in silence: 84809NED (57,139,992 rows) reached 38.5M
        # fetched with ZERO observations written, and 84808NED (23,253,048 rows) the
        # same, over 18 hours. REFUSE to crawl instead — a table we cannot date is not
        # ingestible as a time series, and finding that out costs one metadata call,
        # not 59 million rows.
        log(f"  SKIP {table_id}: no period column in {len(cols)} columns "
            f"(cannot date observations) — not crawled")
        if os.path.exists(out_path) and repull_in_flight(out_dir, table_id):
            end_repull(out_dir, table_id)   # structural: recorded, not retried until CBS changes it
            _note_vintage(out_dir, REFUSED_FILE, table_id, modified or "", {"reason": "undatable"})
            if table_id in load_accepts(out_dir):
                drop_accept(out_dir, table_id, "the table has no period column (undatable)")
            log(f"  {table_id}: held copy stays; vintage recorded as undatable in {REFUSED_FILE}")
        return 0
    # CBS TypedDataSet has numeric values in integer or decimal columns
    # Skip metadata/code columns (non-numeric) by checking name patterns
    skip_cols = {"ID", "StringValue", "ColorCode", "Status",
                 "odata.type", "odata.id"}
    if period_col:
        skip_cols.add(period_col)

    # Try to detect value column(s): all float/int columns that aren't period/dim
    # In CBS TypedDataSet the main value is often just an unnamed column or one numeric col
    # Let's just store all numeric values in long format.
    # Stream to disk in chunks — huge tables (85477NED: 40M+ source rows) cause
    # MemoryError if buffered entirely in Python lists.
    schema = pa.schema([("series_key", pa.string()),
                        ("obs_date",   pa.date32()),
                        ("value",      pa.float64())])
    tmp_path  = out_path + ".tmp"
    ckpt_path = os.path.join(out_dir, f"{table_id}.ckpt.json")
    if os.path.exists(tmp_path):
        os.remove(tmp_path)          # stale tmp from a crashed run (unclosed = unreadable)
    FLUSH_EVERY = 500_000            # obs buffered before flushing to a part file
    FLUSH_ROWS  = 2_000_000          # also checkpoint after this many source rows,
                                     # so sparse tables (few obs/row) still resume
                                     # close to where a reboot interrupted them

    def part_path(i: int) -> str:
        return os.path.join(out_dir, f"{table_id}.part{i}.parquet")

    all_keys, all_dates, all_vals = [], [], []
    fetch_error = False
    skip = 0
    parts = 0
    written = 0
    pidx = 0          # index into `partitions`; MUST be initialised here, not only in
                      # the checkpoint-resume branch — a table with no checkpoint (the
                      # normal case, and every table after a checkpoint reset) would
                      # otherwise hit UnboundLocalError on the first loop test.

    # Resume mid-table from checkpoint (reboot/crash during a huge download).
    # Flushes only happen at page boundaries, so resuming at the saved $skip
    # offset continues exactly where the flushed parts left off.
    if os.path.exists(ckpt_path):
        try:
            with open(ckpt_path) as f:
                ck = json.load(f)
            # Validate every part is a READABLE parquet, not just present —
            # a reboot can leave the in-progress part truncated/0-byte.
            for i in range(int(ck.get("parts", 0))):
                pq.read_metadata(part_path(i))  # raises if missing or corrupt
            skip, parts, written = int(ck["skip"]), int(ck["parts"]), int(ck["written"])
            pidx = int(ck.get("pidx", 0))
            log(f"  {table_id}: resuming at skip={skip:,} ({parts} parts, {written:,} obs already flushed)")
        except Exception:
            for i in range(1000):
                if os.path.exists(part_path(i)):
                    os.remove(part_path(i))
            os.remove(ckpt_path)
            skip = parts = written = pidx = 0

    # Partition plan. `partitions == [None]` reproduces the original single-stream
    # walk exactly; a list of period keys splits the table so no offset grows deep.
    partitions = [None]
    if period_col:
        total = table_row_count(table_id)
        if total and total >= PARTITION_MIN_ROWS:
            pk = period_keys(table_id, period_col)
            if len(pk) > 1:
                partitions = pk
                log(f"  {table_id}: {total:,} rows -> partitioning by {period_col} "
                    f"into {len(pk)} slices (avoids O(offset) deep-$skip cost)")

    last_ckpt_skip = skip
    rows_crawled_total = 0   # R592: `skip` is a per-partition offset and is 0 at every loop exit
    broken_now = False
    while pidx < len(partitions):
        part_val = partitions[pidx]
        flt = ""
        if part_val is not None:
            flt = "&$filter=" + urllib.parse.quote(f"{period_col} eq '{part_val}'", safe="")
        url = (f"{BASE}/{table_id}/TypedDataSet"
               f"?$top={PAGE}&$skip={skip}{flt}")
        data = get_json(url)
        if not data:
            if LAST_ERROR.get("status") == 500:
                # Upstream corruption, not an interrupted download. Calling this a
                # resumable checkpoint is what produced the 315-run loop.
                mark_broken(out_dir, table_id, 500, LAST_ERROR.get("body", ""))
                broken_now = True
                if os.path.exists(out_path) and repull_in_flight(out_dir, table_id):
                    end_repull(out_dir, table_id)   # R598: registered once, and not left open for 7 days
            else:
                # R596: a failed FIRST page (403, timeout) concludes nothing either - the old
                # guard `skip > 0 or pidx > 0` let it fall through to the ZERO path and freeze
                # the table until CBS bumped Modified.
                fetch_error = True
            break
        rows = data.get("value", []) if isinstance(data, dict) else data
        if not rows:
            # this partition is exhausted -> advance to the next, offset back to 0
            pidx += 1
            skip = 0
            last_ckpt_skip = 0
            with open(ckpt_path, "w") as f:
                json.dump({"skip": 0, "parts": parts, "written": written, "pidx": pidx}, f)
            continue

        for row in rows:
            period_raw = row.get(period_col or "Perioden", "")
            d = parse_cbs_period(str(period_raw).strip())
            if d is None:
                continue
            # Build key from all non-numeric / non-period columns
            dim_parts = []
            for col in cols:
                if col in skip_cols or col == period_col:
                    continue
                v = row.get(col)
                if v is None:
                    continue
                if isinstance(v, (int, float)):
                    continue  # numeric → candidate value
                if isinstance(v, str) and v.strip():
                    dim_parts.append(f"{col}={v.strip()}")
            series_key = ":".join(dim_parts) or table_id

            # All numeric values for this period+key
            for col in cols:
                if col in skip_cols or col == period_col:
                    continue
                v = row.get(col)
                if v is None:
                    continue
                if not isinstance(v, (int, float)):
                    continue
                try:
                    fv = float(v)
                except (TypeError, ValueError):
                    continue
                all_keys.append(f"{series_key}:{col}")
                all_dates.append(d)
                all_vals.append(fv)

        skip += len(rows)
        rows_crawled_total += len(rows)
        rows_since_ckpt = skip - last_ckpt_skip
        if len(all_vals) >= FLUSH_EVERY or (rows_since_ckpt >= FLUSH_ROWS and all_vals):
            batch = pa.table({
                "series_key": pa.array(all_keys,  pa.string()),
                "obs_date":   pa.array(all_dates, pa.date32()),
                "value":      pa.array(all_vals,  pa.float64()),
            })
            pq.write_table(batch, part_path(parts), compression="zstd")
            parts += 1
            written += len(all_vals)
            all_keys, all_dates, all_vals = [], [], []
            with open(ckpt_path, "w") as f:
                json.dump({"skip": skip, "parts": parts, "written": written, "pidx": pidx}, f)
            last_ckpt_skip = skip
            log(f"    {table_id}: flushed {written:,} obs (part {parts}, skip={skip:,})")
        elif rows_since_ckpt >= FLUSH_ROWS:
            # BUFFER EMPTY after a whole FLUSH_ROWS block. Say so, loudly. This is the
            # signature of every silent-loss bug this ingester has had — unparsed period
            # VALUES (SJ/X0/YYYYMMDD), then an undetected period COLUMN — and in each case
            # the job looked perfectly healthy from outside: process alive, log scrolling,
            # row counts climbing, and not one observation kept. 71493ned fetched
            # 144,000,000 rows that way; 84809NED 38,500,000. Nothing counted the discards,
            # so nothing could report them. Now the ratio is visible in the log the
            # supervisor already writes, and a sustained 0 is a defect, not a quiet table.
            log(f"    !! {table_id}: {skip:,} rows fetched, {written:,} obs written "
                f"— {rows_since_ckpt:,} rows in this block produced NOTHING "
                f"(period_col={period_col!r}) — check the parser before trusting this run")
            # buffer empty (sparse stretch) but many source rows scanned — persist
            # the skip offset so a reboot doesn't re-scan them (no part to write).
            with open(ckpt_path, "w") as f:
                json.dump({"skip": skip, "parts": parts, "written": written, "pidx": pidx}, f)
            last_ckpt_skip = skip
        if len(rows) < PAGE:
            # short page = end of THIS partition, not necessarily the table
            pidx += 1
            skip = 0
            last_ckpt_skip = 0
            with open(ckpt_path, "w") as f:
                json.dump({"skip": 0, "parts": parts, "written": written, "pidx": pidx}, f)
            if pidx >= len(partitions):
                break
            time.sleep(0.5)
            continue
        if skip % 50000 == 0:
            lbl = f" [{part_val}]" if part_val is not None else ""
            log(f"    {table_id}{lbl}: {skip:,} rows fetched...")
        time.sleep(0.5)

    if fetch_error:
        # Persist progress and keep the checkpoint so the next run resumes here
        if all_vals:
            batch = pa.table({
                "series_key": pa.array(all_keys,  pa.string()),
                "obs_date":   pa.array(all_dates, pa.date32()),
                "value":      pa.array(all_vals,  pa.float64()),
            })
            pq.write_table(batch, part_path(parts), compression="zstd")
            parts += 1
            written += len(all_vals)
            with open(ckpt_path, "w") as f:
                json.dump({"skip": skip, "parts": parts, "written": written, "pidx": pidx}, f)
        log(f"  WARNING {table_id}: fetch failed at skip={skip:,}; "
            f"{written:,} obs checkpointed for resume next run")
        return 0

    if all_vals:
        batch = pa.table({
            "series_key": pa.array(all_keys,  pa.string()),
            "obs_date":   pa.array(all_dates, pa.date32()),
            "value":      pa.array(all_vals,  pa.float64()),
        })
        pq.write_table(batch, part_path(parts), compression="zstd")
        parts += 1
        written += len(all_vals)
        all_keys, all_dates, all_vals = [], [], []

    if parts == 0:
        # Fetched rows but kept nothing. ALWAYS report the ratio: this exact outcome —
        # a completed crawl that wrote zero observations — is what 23 tables did for
        # weeks under the SJ/X0/YYYYMMDD parser gap, and what 84808/84809NED did for
        # 18 hours under the undetected period column, without ever producing a single
        # line anyone could grep for. R592: this line was DEAD (`skip` is 0 at every loop
        # exit) and never fired in 478 guard logs; it keys on rows_crawled_total now.
        if fetch_error or broken_now:
            return 0   # nothing is concluded: the checkpoint resumes it / mark_broken already registered it
        if rows_crawled_total > 0:
            log(f"  !! {table_id}: crawled {rows_crawled_total:,} rows and wrote ZERO observations "
                f"(period_col={period_col!r}, discards={discards_since(discards_before)}) "
                f"— this is a DEFECT, not an empty table")
        # R589/R592: a completed crawl that produced nothing must not leave a resumable
        # checkpoint or an open re-pull behind: record the vintage (so it is not retried
        # until CBS publishes a newer stamp), close the marker, clear the partials. The
        # served copy, if any, stays as it was.
        was_repull = os.path.exists(out_path) and bool(repull_in_flight(out_dir, table_id))
        if was_repull:
            end_repull(out_dir, table_id)          # close first (R596: an interrupt after recording
        clear_partials(out_dir, table_id)          # would otherwise leave the marker forever)
        if os.path.exists(ckpt_path):
            os.remove(ckpt_path)
        _note_vintage(out_dir, ZERO_FILE, table_id, modified or "", {"rows_crawled": rows_crawled_total,
                      "served_rows": pq.read_metadata(out_path).num_rows if os.path.exists(out_path) else 0})
        if was_repull:
            log(f"  {table_id}: re-pull produced nothing - served copy KEPT, vintage recorded in {ZERO_FILE}")
            if accept_applies(out_dir, table_id, modified or ""):
                drop_accept(out_dir, table_id, "the accepted re-pull produced nothing")
        else:
            log(f"  {table_id}: first crawl produced nothing - recorded in {ZERO_FILE}, checkpoint cleared")
        return 0

    # Concatenate part files into the final parquet (memory-bounded, one part at a time)
    writer = pq.ParquetWriter(tmp_path, schema, compression="zstd")
    for i in range(parts):
        writer.write_table(pq.read_table(part_path(i)))
    writer.close()
    # R589: never replace a served table with a fraction of itself. A re-pull that keeps fewer
    # than REPLACE_FLOOR of the served rows is refused, recorded, and the old file kept - the
    # cause (a parser change, a partial upstream response) is for a person to look at.
    if os.path.exists(out_path) and repull_in_flight(out_dir, table_id):
        try:
            old_n = pq.read_metadata(out_path).num_rows
        except Exception:                                               # noqa: BLE001
            old_n = 0
        new_n = pq.read_metadata(tmp_path).num_rows
        log(f"  {table_id}: replacing {old_n:,} served rows with {new_n:,} "
            f"({(new_n / old_n if old_n else 0):.1%}; discards={discards_since(discards_before)})")
        if old_n and new_n < REPLACE_FLOOR * old_n and not accept_applies(out_dir, table_id, modified, new_n):
            acc = load_accepts(out_dir).get(table_id)
            if acc and acc.get("vintage") == modified:
                log(f"  {table_id}: the accepted shrink was for {acc.get('refused_rows')} rows; this re-pull "
                    f"yields {new_n:,} - consent does not carry (R604), refusing afresh")
                # R606: the accept must not survive, or every pass re-crawls the table in full;
                # the refusal recorded below makes the next pass NOT_RETRIED
                drop_accept(out_dir, table_id, f"yield {new_n:,} < {ACCEPT_YIELD_FLOOR:.0%} of "
                                               f"{acc.get('refused_rows')} - re-run --accept-shrink={table_id} to accept {new_n:,}")
            log(f"  !! {table_id}: re-pull would replace {old_n:,} served rows with {new_n:,} "
                f"({new_n / old_n:.1%} < {REPLACE_FLOOR:.0%}) - REFUSED, served copy kept, vintage "
                f"recorded in {REFUSED_FILE} (discards={discards_since(discards_before)}); "
                f"re-run with --accept-shrink={table_id} to accept it")
            os.remove(tmp_path)
            for i in range(parts):
                os.remove(part_path(i))
            if os.path.exists(ckpt_path):
                os.remove(ckpt_path)
            end_repull(out_dir, table_id)          # close first, then record (R596)
            _note_vintage(out_dir, REFUSED_FILE, table_id, modified,
                          {"served_rows": old_n, "repull_rows": new_n, "floor": REPLACE_FLOOR})
            return 0
    os.replace(tmp_path, out_path)
    for i in range(parts):
        os.remove(part_path(i))
    if os.path.exists(ckpt_path):
        os.remove(ckpt_path)
    n = pq.read_metadata(out_path).num_rows
    record_modified(out_dir, table_id, modified)
    end_repull(out_dir, table_id)
    _clear_vintage(out_dir, ZERO_FILE, table_id)      # a real replacement supersedes any record
    _clear_vintage(out_dir, REFUSED_FILE, table_id)
    if table_id in load_accepts(out_dir):
        drop_accept(out_dir, table_id, "the accepted re-pull replaced the served copy")
    dd = discards_since(discards_before)
    log(f"  {table_id}: DONE {n:,} obs  [{title[:50]}]" + (f"  discards={dd}" if dd else ""))
    return n


def main():
    os.makedirs(OUT, exist_ok=True)
    only_ids: set[str] = set()
    accept_only = False
    for a in sys.argv[1:]:
        if a.startswith("--only"):
            raw = a.split("=", 1)[-1] if "=" in a else ""
            only_ids = set(raw.split(","))
        elif a.startswith("--accept-shrink="):
            ids = [x for x in a.split("=", 1)[1].split(",") if x]
            record_accepts(OUT, ids)   # R598/R600: the yes lives in the store, for the ONE crawler
            accept_only = True
        elif a == "--force-sweep":
            global FORCE_SWEEP
            FORCE_SWEEP = True
        elif not a.startswith("-"):
            only_ids.add(a)

    if accept_only and not only_ids:
        log("accept recorded; the guard's crawler will act on it - not starting a second crawler (R600)")
        return
    registry_summary(OUT)
    log("Fetching CBS Netherlands table catalog...")
    tables = get_catalog()
    log(f"Found {len(tables)} tables in catalog")
    manifest_n = len(load_modified(OUT))
    if tables and (FORCE_SWEEP or manifest_n == 0 or len(tables) >= 0.9 * manifest_n):
        sweep_markers(OUT, {t.get("Identifier", "") for t in tables})
    elif tables:
        log(f"  catalogue listing has {len(tables)} tables vs {manifest_n} held - partial listing, marker sweep skipped (R600)")

    if only_ids:
        tables = [t for t in tables if t.get("Identifier") in only_ids]
        log(f"Filtered to {len(tables)} tables")

    total = 0
    for i, tbl in enumerate(tables, 1):
        tid   = tbl.get("Identifier", "")
        title = tbl.get("Title", "") or tbl.get("ShortTitle", "")
        if not tid:
            continue
        log(f"[{i}/{len(tables)}] {tid}: {title[:60]}")
        why = broken_recently(OUT, tid)
        if why:
            log(f"  skip {tid}: upstream-broken {why}")
            continue
        total += ingest_table(tid, title, OUT, tbl.get("Modified") or "")
        time.sleep(0.3)

    log(f"DONE: {total:,} total CBS Netherlands observations")
    registry_summary(OUT)


if __name__ == "__main__":
    main()
