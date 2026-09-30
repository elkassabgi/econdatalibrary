"""S2 fetcher - Statistics Canada: the REPORTER for the statcan lane (2026-09-23).

statcan's refresh does not run here any more. It runs in jobs/statcan_lane.py, a process of its own
that RELAUNCH_GUARD.ps1 launches every guard tick: it refreshes every released cube WHOLE and then
serves that cube's catalogued parts, with no time budget. Why: inside a local-heavy pass statcan gets
a 20-76 minute slot, 108 backlog cubes need more than that and 12100152 alone ~380 min, and the
orchestrator's inline derive re-scans a whole cube per served part (1.9-4.3 s/part) with ~0 minutes
left. Four review rounds of budget machinery each failed on a new edge (ledger R1091-R1094).

This unit now READS what the lane publishes and turns it into an honest status - seconds of work,
nothing written to the store:
    _lane_progress.json   the lane's beat: when it last did work, what it is working on, how long that
                          is estimated to take, and the owed counts (jobs/statcan_lane.py `summarise`)
    _lane_state.json      per-cube release / merged / served, quarantine, the newest obs merged
Verdicts (design review finding 7 - `partial` is not an honest signal for a draining backlog):
    DefinitiveError  no beat, or a beat older than max(2 h, the current cube's estimate x 1.5) while
                     working (3 h while idle): the lane is dead or wedged;
                     or the OLDEST owed release is older than OWED_AGE_MAX_DAYS: alive, not keeping up;
                     or named cubes keep failing transiently (TRANSIENT_RED_AFTER in a row)
    partial          alive and current, but something needs a human: QUARANTINED cubes, merged cubes
                     whose served ids cannot be reproduced, or new-cube coverage debt older than
                     NEW_CUBE_DEBT_AMBER_DAYS
    ok               alive, and every owed release is younger than OWED_AGE_MAX_DAYS
WHERE THOSE LAND (lane review, stated rather than implied). The orchestrator books a fetcher's
DefinitiveError as `partial` too (orchestrate.py, `except DefinitiveError`), so in the health table
both read ATTENTION, and neither advances last_success - staleness then escalates on its own. This
unit runs on statcan's weekly cadence, so it can take ~6.5 days to look. The PROMPT red is CI's:
tools/guard_heartbeat.py --check applies this same `verdict` to the lane's beat every day and fails
the run (statcan is run_location: local, which the cloud health gate does not judge).
changed_keys is {}: the lane serves what it merges, and the registry's `served_by: lane` keeps the
orchestrator's CSV phase and retry drain off statcan (a second writer - design review finding 3).

The helpers below - enumeration, download, StatCan's own completeness counts, the key-overlap gate,
the whole-table fetch - are what the lane imports. They are unchanged from the whole-table fetcher.

Layout of the store the lane writes: ONE zstd parquet per cube (productId) under
clean_full/statcan/<pid>.parquet (~8,207 files), schema EXACTLY as written by jobs/ingest_statcan.py:
    series_key(string)  = StatCan VECTOR id, lowercase "v"+digits  (e.g. "v41690973");
                          Census wide-layout cubes: "<Coordinate>.<k>" (ingest_statcan._iter_census)
    obs_date(date32)    = REF_DATE parsed (annual->Dec-31, monthly/quarterly->day-1)
    value(float64)      = null when suppressed
    geo(string), uom(string), coordinate(string)
    status(string)      = StatCan's STATUS flag

WHY THE WHOLE TABLE AND NOT THE VECTOR TAIL (measured, NUMBERS.md 2026-09-23 rows). The tail
(getBulkVectorDataByRange) costs ~0.16 s PER VECTOR, so the release backlog of 478 cubes / 69,547,320
vectors was ~278,000 requests - months. The whole table costs ~4.3 MB/s + ~187,000 rows/s to parse +
~171,000 rows/s to merge: 24100058 (53,335,935 rows) in ~12 min against ~94 min of tail requests. It
also carries what the tail could not: revisions to any period, relabelled members (24100058: 590,276
rows of "Windsor" are now "Windsor - other locations"), the real STATUS flag, and the Census
wide-layout cubes, which have no vector ids.
"""
from __future__ import annotations
import csv
import datetime as dt
import json
import os
import time
import uuid
import zipfile
import zlib

import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result

OUT_DIR = os.path.join(config.DATA_ROOT, "statcan")
# What the lane publishes (jobs/statcan_lane.py STATE / PROGRESS - the same paths, blob-routed).
LANE_STATE = os.path.join(OUT_DIR, "_lane_state.json")
LANE_PROGRESS = os.path.join(OUT_DIR, "_lane_progress.json")

BASE = "https://www150.statcan.gc.ca/t1/wds/rest"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
      "Content-Type": "application/json"}
SOURCE = "statcan"

DEDUP = ("series_key", "obs_date")

# The whole-table endpoint: returns {"status": "SUCCESS", "object": <zip URL>} for one cube.
FULL_TABLE = "getFullTableDownloadCSV/{pid}/en"
# THE COMPLETENESS GATE is StatCan's own count, not ours. getCubeMetadata publishes
# nbDatapointsCube and nbSeriesCube per cube, and the ingester's parse reproduced BOTH exactly on
# every cube measured (NUMBERS.md 2026-09-23: 32100004 1,584/1,584, 33100330 10,080/10,080,
# 14100442 285,824/6,496, 24100058 53,335,935/35,264). A ratio against the STORED rows could not
# do this job: the merge keeps every stored row the new table lacks, so a short table would publish
# as "a few rows updated", and the stored count only ever grows, so a publisher that trims history
# would be refused for ever. VECTORLESS cubes (Census, coordinate-keyed) are the exception: their
# counts do not describe the table, and _check_complete says what stands in for them.
CUBE_META = "getCubeMetadata"
# THE KEY GATE. Counts cannot see a table re-keyed under us: StatCan filling the blank VECTOR cells
# of 12100147..12100152 (stored under COORDINATE keys, ingest_statcan._iter_standard's fallback)
# would pass every count and then ADD every series again under 'v...' keys beside the old ones -
# keep-old merging keeps both. At least this fraction of the stored cube's keys must reappear.
KEY_OVERLAP_MIN = 0.90
# Scratch space for one cube's zip and parsed parquet, removed in `finally`. Its own directory, so
# the bulk ingester's _tmp/ is never touched.
TMP_DIR = os.path.join(OUT_DIR, "_incr_tmp")
# Seconds of work per STORED byte - the lane's estimate for a cube, and so the reporter's allowance
# between beats. From 24100058 (253,240,781 stored bytes): download 110.8 s + parse 285.4 s + merge
# 310.9 s = 707 s, i.e. 2.8e-6 s/B; doubled for the merge's 2 GB default memory limit (the
# measurement ran at 4 GB) and one network.
EST_SECONDS_PER_STORED_BYTE = 5.6e-6
# A cube that fails DETERMINISTICALLY this many times on one release is quarantined by the lane.
QUARANTINE_AFTER = 3

# THE REPORTER'S THRESHOLDS.
# statcan's registry cadence is weekly; SLA_TOLERANCE 2 -> an owed release older than 14 days means
# the lane is not keeping up. The initial backlog (releases since 2026-07-29) is older than that, so
# statcan reads RED until the lane has drained it - which is the truth.
OWED_AGE_MAX_DAYS = 14
# Between beats: the lane beats at every phase boundary and every 500 parts, never from a timer. A
# phase can legitimately run as long as its cube's estimate (12100152 ~380 min), so the allowance
# while working is max(BEAT_MIN_HOURS, estimate x 1.5).
BEAT_MIN_HOURS = 2.0
BEAT_EST_MARGIN = 1.5
# While idle the lane beats every iteration (~5 min) and enumerates at least hourly.
IDLE_BEAT_MAX_HOURS = 3.0
# Behind the SLA but finished a cube (merged or served) within this long - or within the current
# cube's own allowance, if longer - reads DRAINING (amber) rather than red.
DRAIN_PROGRESS_MAX_HOURS = 6.0
# A beat or progress stamp this far ahead of the reader's clock is treated as unreadable.
FUTURE_SKEW_HOURS = 0.25
# Released cubes we do not hold are booked as coverage debt by the lane; past this age the debt is
# named (partial) so it cannot grow silently (lane review).
NEW_CUBE_DEBT_AMBER_DAYS = 30


def _ingester():
    """jobs/ingest_statcan.py, the producer of every stored cube. Imported lazily so importing this
    fetcher stays cheap, and imported rather than copied so the parse cannot drift from it."""
    import jobs.ingest_statcan as ing                                # noqa: PLC0415
    return ing


def _get(endpoint, tries=5, timeout=120):
    """GET a WDS endpoint. Returns parsed JSON on 200. TransientError on
    timeout/5xx/429/network/truncated-body (retry next run); DefinitiveError on a
    hard non-200 (!=429)."""
    return _call("GET", endpoint, None, tries, timeout)


def _post(endpoint, payload, tries=5, timeout=120):
    """POST a WDS endpoint. Same transient/definitive rules as _get."""
    return _call("POST", endpoint, payload, tries, timeout)


def _call(method, endpoint, payload, tries, timeout):
    url = f"{BASE}/{endpoint}"
    for a in range(tries):
        try:
            if method == "GET":
                r = requests.get(url, headers=UA, timeout=timeout)
            else:
                r = requests.post(url, json=payload, headers=UA, timeout=timeout)
        except (requests.Timeout, requests.ConnectionError) as e:
            if a == tries - 1:
                raise TransientError(f"statcan {endpoint}: {e}")
            time.sleep(min(2 ** a, 30)); continue
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError as e:
                if a == tries - 1:
                    raise TransientError(f"statcan {endpoint}: bad json on 200 ({e})")
                time.sleep(min(2 ** a, 30)); continue
        if r.status_code in (429, 500, 502, 503, 504):
            if a == tries - 1:
                raise TransientError(f"statcan {endpoint} HTTP {r.status_code}")
            time.sleep(min(2 ** a, 30)); continue
        raise DefinitiveError(f"statcan {endpoint} HTTP {r.status_code}")


_LITE_FLOOR = 1000     # StatCan publishes >8,000 cubes; a short list is a structural break


def _changed_pids(feed_since: dt.date):
    """The product ids of `_changed_releases`, as a set."""
    return set(_changed_releases(feed_since))


def _changed_releases(feed_since: dt.date) -> dict:
    """{productId: 'YYYY-MM-DDTHH:MM' release time} for every cube released on or after `feed_since`.

    NOT `getChangedCubeList`. THAT ENDPOINT IS A SINGLE-DAY FEED AND THIS FETCHER READ IT AS A
    SINCE-FEED FOR ITS ENTIRE LIFE — the one defect that explains statcan's staleness.

    Measured 2026-09-17 against www150 (never a relay), by the test a since-feed cannot pass:
    if `getChangedCubeList/<date>` meant "changed on or after <date>", its count could only ever
    be NON-INCREASING as the date advances, because a later date covers a subset. Observed:

        2026-08-18   3        2026-09-10   15        2026-09-15   14
        2026-08-27  52        2026-09-12    0        2026-09-16   15
        2026-09-03  30        2026-09-14   16        2026-09-17   60
        2026-09-07   0

    Five adjacent pairs increase, and two dates return ZERO while later dates return more. That
    is impossible for a since-feed and is exactly the shape of "changed ON <date>". Confirmed
    with a second, independent instrument: `getAllCubesListLite`'s per-day histogram of
    `releaseTime` reproduces those daily counts on 9 of the 10 days (the small shortfalls are
    cubes that changed that day AND again later, of which lite keeps only the latest release).

    What that cost: `update()` polls ONE date per run and then advances the watermark to today,
    so on a weekly cadence six days in seven were never inspected by any feed. Measured the same
    day: 505 cubes have been released since 2026-07-29 and WE HOLD 456 OF THEM. None of them
    could have reached us.

    The replacement is the one the runbook already named and nobody built. `getAllCubesListLite`
    returns the whole catalogue in a single request — measured HTTP 200, 8,270 cubes, and
    `releaseTime` present on 8,270 of 8,270 — so the honest "changed since" is a filter over it.
    One request, complete set, no watermark arithmetic that can skip a day.

    THE RELEASE DATE IS KEPT (2026-09-22), not just the id: a cube finished earlier in an open
    window and RE-released since must be fetched again, and only its release date says so.

    FAILS CLOSED, because the alternative is the silent empty result: an enumeration that comes
    back empty or implausibly short raises TransientError rather than yielding "no cubes
    changed", which would advance the watermark over a window nobody looked at. That is the
    mechanism this docstring exists to describe, so it must not be reintroduced by a quiet [].
    """
    j = _get("getAllCubesListLite", timeout=300)
    cubes = j if isinstance(j, list) else (j.get("object") if isinstance(j, dict) else None)
    if not isinstance(cubes, list) or len(cubes) < _LITE_FLOOR:
        raise TransientError(
            f"statcan getAllCubesListLite returned {len(cubes) if isinstance(cubes, list) else 'no list'} "
            f"cube(s), below the {_LITE_FLOOR} floor — refusing to read that as 'nothing changed'")
    cutoff = feed_since.isoformat()
    rel, dated = {}, 0
    for o in cubes:
        if not isinstance(o, dict):
            continue
        # THE FULL TIMESTAMP ('YYYY-MM-DDTHH:MM'), not the day: a cube fetched at 07:00 and
        # re-released at 08:30 the same day compared EQUAL on dates and was skipped (round-3 review).
        rt = str(o.get("releaseTime") or "")[:16]
        pid = o.get("productId")
        if pid is None or len(rt) < 10:
            continue
        dated += 1
        if rt[:10] >= cutoff:                 # ISO dates: lexicographic == chronological
            rel[int(pid)] = rt
    if not dated:
        raise TransientError(
            "statcan getAllCubesListLite returned cubes but none carried a usable releaseTime — "
            "the field this filter depends on is gone; refusing to report an empty change set")
    print(f"    statcan: {len(cubes):,} cubes listed, {dated:,} with a release date, "
          f"{len(rel):,} released on/after {cutoff}", flush=True)
    return rel


# --------------------------------------------------------------------------- #
# the whole-table path
# --------------------------------------------------------------------------- #
def _remove_quietly(p):
    try:
        if p and os.path.exists(p):
            os.remove(p)
    except OSError:
        pass


def _download(url, dest, tries=5):
    """Stream `url` to `dest`. TransientError on timeout / network / 429 / 5xx after `tries`, and on
    a body shorter or longer than its Content-Length (a truncated transfer must never reach the
    parser as if it were the table). DefinitiveError on any other non-200."""
    for a in range(tries):
        try:
            with requests.get(url, headers={"User-Agent": UA["User-Agent"]}, timeout=900,
                              stream=True) as r:
                if r.status_code in (429, 500, 502, 503, 504):
                    if a == tries - 1:
                        raise TransientError(f"statcan zip {url} HTTP {r.status_code}")
                    time.sleep(min(2 ** a, 30))
                    continue
                if r.status_code != 200:
                    raise DefinitiveError(f"statcan zip {url} HTTP {r.status_code}")
                want = r.headers.get("Content-Length")
                got = 0
                with open(dest, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        if chunk:
                            f.write(chunk)
                            got += len(chunk)
            if want is not None and want.isdigit() and int(want) != got:
                if a == tries - 1:
                    raise TransientError(f"statcan zip {url}: {got:,} of {int(want):,} bytes arrived")
                time.sleep(min(2 ** a, 30))
                continue
            return got
        except (requests.Timeout, requests.ConnectionError,
                requests.exceptions.ChunkedEncodingError) as e:
            if a == tries - 1:
                raise TransientError(f"statcan zip {url}: {type(e).__name__}: {e}") from e
            time.sleep(min(2 ** a, 30))
    raise TransientError(f"statcan zip {url}: no attempt succeeded")      # unreachable


def _cube_counts(pid):
    """(nbDatapointsCube, nbSeriesCube) from StatCan's getCubeMetadata. TransientError when the
    answer is not a SUCCESS carrying both as integers - a gate that cannot read its reference
    must not pass (failure class H)."""
    j = _post(CUBE_META, [{"productId": int(pid)}])
    item = j[0] if isinstance(j, list) and j else j
    o = item.get("object") if isinstance(item, dict) and item.get("status") == "SUCCESS" else None
    try:
        return int(o["nbDatapointsCube"]), int(o["nbSeriesCube"])
    except (TypeError, KeyError, ValueError) as e:
        raise TransientError(f"statcan {CUBE_META} {pid}: no usable counts ({str(j)[:160]})") from e


# Parser skips that say nothing about completeness: csv.reader yields [] for a blank line, and
# StatCan's Census CSVs end with two (98100001, 98100073).
_HARMLESS_SKIPS = {"blank_line"}


def _vectorless(st, want_series):
    """True for a cube StatCan's counts do not describe: the Census wide layout, or a table whose
    nbSeriesCube is 1 while it parses into many series (it has no vector ids - the coordinate-keyed
    12100147..12100152)."""
    return st.get("layout") == "census" or (want_series == 1 and int(st["n_series"]) > 1)


def _check_complete(pid, st, want_rows, want_series):
    """Raise DefinitiveError unless the parse is provably complete. Returns True when update() must
    also require at least the stored rows (see below).

    CUBES WITH VECTORS: the parse must reproduce StatCan's own counts exactly - nbDatapointsCube
    rows and, while the parser counted them exactly (below its SERIES_CAP), nbSeriesCube series.

    VECTORLESS CUBES: those counts do not describe the parse. nbSeriesCube is 1 for them, and the
    datapoint count disagrees in both directions - Census 98100073 publishes 16,636,456 against
    16,797,696 parsed cells (161,240 more, and not its 5,158,862 null cells), coordinate-keyed
    12100147 publishes 19,940,793 against 18,225,522 rows with the parser dropping NOTHING - so
    equality would refuse a correct table for ever (NUMBERS.md 2026-09-23). There the parse must
    skip no row for any reason but a blank line, and must hold at least the stored rows."""
    bad_skips = {k: v for k, v in (st.get("skipped") or {}).items() if k not in _HARMLESS_SKIPS}
    if _vectorless(st, want_series):
        if bad_skips:
            raise DefinitiveError(f"statcan {pid}: the parse of a vectorless cube dropped rows {bad_skips}")
        return True
    got_rows, got_series = int(st["n_obs"]), int(st["n_series"])
    bad = []
    if got_rows != want_rows:
        bad.append(f"{got_rows:,} rows parsed vs nbDatapointsCube {want_rows:,}")
    if not st.get("series_capped") and got_series != want_series:
        bad.append(f"{got_series:,} series parsed vs nbSeriesCube {want_series:,}")
    if bad:
        raise DefinitiveError(f"statcan {pid}: incomplete against StatCan's own counts - "
                              f"{'; '.join(bad)}; rows skipped by the parser {st.get('skipped')}")
    return False


def _fetch_cube_whole(pid):
    """Download cube `pid`'s full table, parse it with the bulk ingester's own
    parse_zip_to_parquet into a scratch parquet, and prove it complete against StatCan's counts.
    Returns (scratch_parquet_path, stats). The caller owns the returned file and removes it;
    everything else this writes is removed here, success or not.

    TransientError: a WDS call or the download failed, or the zip is corrupt (bad CRC, truncated
    deflate stream) - retry next run. DefinitiveError: StatCan answered but gave no zip URL, the
    CSV is in a layout the ingester does not recognise, or the parse does not reproduce StatCan's
    own counts - the cube needs a human."""
    want_rows, want_series = _cube_counts(pid)
    j = _get(FULL_TABLE.format(pid=pid))
    url = j.get("object") if isinstance(j, dict) and j.get("status") == "SUCCESS" else None
    if not isinstance(url, str) or not url.startswith("https://"):
        raise DefinitiveError(f"statcan {FULL_TABLE.format(pid=pid)} gave no zip URL: {str(j)[:200]}")
    os.makedirs(TMP_DIR, exist_ok=True)
    stem = os.path.join(TMP_DIR, f"{pid}.{os.getpid()}.{uuid.uuid4().hex[:8]}")
    zpath, out = stem + ".zip", stem + ".parquet"
    ok = False
    try:
        zip_bytes = _download(url, zpath)
        try:
            st = _ingester().parse_zip_to_parquet(zpath, out)
        except (zipfile.BadZipFile, zlib.error, EOFError) as e:
            raise TransientError(f"statcan {pid}: corrupt zip ({type(e).__name__}: {e})") from e
        except (OSError, MemoryError) as e:
            # THIS MACHINE, not the cube: a full disk or an allocation failure while writing the
            # scratch parquet. Transient, so it can never count toward a quarantine.
            raise TransientError(f"statcan {pid}: parse failed on this machine "
                                 f"({type(e).__name__}: {e})") from e
        except (csv.Error, StopIteration, UnicodeError, ValueError) as e:
            # THE TABLE, deterministically: an empty CSV (no header row), a malformed field, a
            # value Arrow refuses. Escaping update() used to crash every pass at the same cube
            # without ever quarantining it (round-3 review).
            raise DefinitiveError(f"statcan {pid}: unparseable table "
                                  f"({type(e).__name__}: {str(e)[:120]})") from e
        except RuntimeError as e:
            # the ingester's own refusals: no data CSV in the zip, or an unrecognised layout
            raise DefinitiveError(f"statcan {pid}: {e}") from e
        st["needs_stored_floor"] = _check_complete(pid, st, want_rows, want_series)
        st["zip_bytes"] = zip_bytes
        ok = True
        return out, st
    finally:
        for p in (zpath, out + ".part") + (() if ok else (out,)):
            _remove_quietly(p)


def _key_overlap(stored_path, new_path):
    """(stored keys, stored keys that reappear in the new table), by DuckDB over the two files."""
    import duckdb                                                    # noqa: PLC0415
    a = stored_path.replace("\\", "/").replace("'", "''")
    b = new_path.replace("\\", "/").replace("'", "''")
    con = duckdb.connect()
    try:
        con.execute(f"SET memory_limit='{merge.BOUNDED_MEMORY_LIMIT}'")
        total = con.execute(
            f"SELECT count(DISTINCT series_key) FROM read_parquet('{a}')").fetchone()[0]
        both = con.execute(
            f"SELECT count(*) FROM (SELECT DISTINCT series_key FROM read_parquet('{a}') "
            f"INTERSECT SELECT DISTINCT series_key FROM read_parquet('{b}'))").fetchone()[0]
    finally:
        con.close()
    return int(total), int(both)


def _environmental(e) -> bool:
    """True when a merge refusal is about THIS MACHINE, not the data: merge_and_write_bounded wraps
    a full disk, a spill-cap overflow, an allocation failure or a DuckDB I/O error as
    DefinitiveError `from` the original (merge.py), and refuses up front when the spill disk is too
    short. Those must be transient - counting them toward a quarantine would park an annual cube for
    a year over a disk-space blip (round-2 review, probe D). The data refusals (shrink, zero rows,
    a dropped or foreign column, an unsupported type) are raised with no cause."""
    return e.__cause__ is not None or "spill disk" in str(e)


def _read_lane(path):
    raw = blob.read_bytes(path)
    if raw is None:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as e:
        raise DefinitiveError(f"statcan: {path} is unreadable ({type(e).__name__}: {e}) - the lane's "
                              f"record cannot be judged") from e


def _hours_since(iso, now):
    """Hours from a published stamp to `now`; None when unreadable OR more than FUTURE_SKEW_HOURS in
    the future - a stamp from the future cannot vouch for anything (lane review round 3, P7: one 48 h
    ahead earned amber). Small skew between the workstation's clock and CI's is tolerated."""
    try:
        t = dt.datetime.strptime(str(iso), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)
    except (TypeError, ValueError):
        return None
    h = (now - t).total_seconds() / 3600.0
    return None if h < -FUTURE_SKEW_HOURS else max(h, 0.0)


def verdict(state, progress, now):
    """(status, message, newest_obs) for the lane's published record. Raises DefinitiveError for
    red. Pure: `update` supplies the record and the clock, tests supply their own."""
    if progress is None or state is None:
        raise DefinitiveError(
            "statcan: the lane has published no progress/state under this backend "
            f"({LANE_PROGRESS}) - jobs/statcan_lane.py has never run here, or the store is "
            "unreachable. Nothing refreshes statcan without it.")
    age_h = _hours_since(progress.get("beat_utc"), now)
    cur = progress.get("current") or None
    if cur:
        est = cur.get("est_min") or 0
        allowed = max(BEAT_MIN_HOURS, float(est) * BEAT_EST_MARGIN / 60.0)
        doing = f"cube {cur.get('pid')} ({cur.get('phase')}, est ~{est} min)"
    else:
        allowed, doing = IDLE_BEAT_MAX_HOURS, f"state {progress.get('state')!r}"
    if age_h is None or age_h > allowed:
        raise DefinitiveError(
            f"statcan: the lane is DEAD or wedged - last beat "
            f"{'unreadable' if age_h is None else f'{age_h:.1f} h ago'} (allowed {allowed:.1f} h) "
            f"while on {doing}. Check the guard (logs/_guard.log) and logs/statcan_lane.progress.json.")
    if progress.get("state") == "store_unreachable":
        raise DefinitiveError("statcan: the lane found the store holding ZERO cubes - the store is "
                              "unreachable from its backend (R753)")
    owed = progress.get("owed") or {}
    oldest = owed.get("oldest_owed_release")
    newest = None
    for c in (state.get("cubes") or {}).values():
        mo = c.get("max_obs")
        if mo and (newest is None or mo > newest):
            newest = mo
    tail = (f"{owed.get('merge', 0)} cube(s) owe a merge, {owed.get('serve', 0)} owe serving, "
            f"{owed.get('new_cubes', 0)} released cube(s) not held (catalogue debt)")
    if oldest:
        try:
            age_d = (now.date() - dt.date.fromisoformat(str(oldest)[:10])).days
        except ValueError:
            age_d = None
        if age_d is None or age_d > OWED_AGE_MAX_DAYS:
            # BEHIND, and then: still finishing cubes, or not? The initial backlog (releases since
            # 2026-07-29) is past the SLA on the lane's first beat, so an age rule alone reads RED
            # for the whole drain, and a job red for weeks stops being read (R244; lane review
            # round 2). Progress within the allowance -> DRAINING, amber. None -> red.
            prog_h = _hours_since(progress.get("last_progress_utc"), now)
            drain_allowed = max(DRAIN_PROGRESS_MAX_HOURS, allowed if cur else 0.0)
            if prog_h is not None and prog_h <= drain_allowed:
                return ("partial", f"statcan: the lane is DRAINING - behind (oldest owed release "
                                   f"{oldest}, {age_d} days) but it finished work {prog_h:.1f} h ago; "
                                   f"{tail}", newest)
            raise DefinitiveError(
                f"statcan: the lane is alive but BEHIND and NOT PROGRESSING - the oldest owed release "
                f"is {oldest} ({age_d} days, over {OWED_AGE_MAX_DAYS}), last finished work "
                f"{'never' if prog_h is None else f'{prog_h:.1f} h ago'} (allowed {drain_allowed:.1f} h); "
                f"{tail}")
    failing = owed.get("failing") or []
    if failing:
        raise DefinitiveError(
            f"statcan: {len(failing)} cube(s) keep FAILING in the lane (transient, again and again - "
            f"e.g. a store fault R753): {'; '.join(failing[:10])}; {tail}")
    amber = []
    quarantined = owed.get("quarantined") or []
    if quarantined:
        amber.append(f"{len(quarantined)} cube(s) QUARANTINED (each failed {QUARANTINE_AFTER}x on one "
                     f"release): {', '.join(quarantined[:20])}")
    refused = owed.get("serve_refused") or []
    if refused:
        amber.append(f"{len(refused)} merged cube(s) whose served ids cannot be reproduced (split map "
                     f"or catalogue needs a human): {'; '.join(refused[:10])}")
    oldest_new = owed.get("oldest_new_cube")
    if oldest_new:
        try:
            new_age = (now.date() - dt.date.fromisoformat(str(oldest_new)[:10])).days
        except ValueError:
            new_age = None
        if new_age is None or new_age > NEW_CUBE_DEBT_AMBER_DAYS:
            amber.append(f"{owed.get('new_cubes')} released cube(s) not held, the oldest since "
                         f"{oldest_new} ({new_age} days) - coverage debt nobody has ingested")
    if amber:
        return "partial", "statcan: " + " | ".join(amber) + f"; {tail}", newest
    return "ok", f"statcan: lane current - {tail}", newest


def update(unit, since) -> Result:
    """Report the lane. Writes nothing (see the module docstring)."""
    now = dt.datetime.now(dt.timezone.utc)
    status, msg, newest = verdict(_read_lane(LANE_STATE), _read_lane(LANE_PROGRESS), now)
    print(f"[statcan] {msg}", flush=True)
    last = newest or (str(since)[:10] if since else None)
    # changed_keys {}: the lane served what it merged (a real "nothing for the orchestrator to do").
    return Result(status=status, obs=0, last_obs_date=last, error=msg if status != "ok" else None,
                  changed_keys={})
