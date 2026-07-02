"""S2 fetcher — Statistics Canada WDS (extend_by_date).

Layout: ONE zstd parquet per cube (productId) under clean_full/statcan/<pid>.parquet
(~8,207 files), schema EXACTLY as written by jobs/ingest_statcan.py:
    series_key(string)  = StatCan VECTOR id, lowercase "v"+digits  (e.g. "v41690973")
    obs_date(date32)    = REF_DATE parsed (annual->Dec-31, monthly/quarterly->day-1)
    value(float64)      = null when suppressed
    geo(string), uom(string), coordinate(string)  = per-vector constants
    status(string)      = ".." when value is suppressed/null, "" otherwise
plus the bulk run's .done / .fail markers, _manifest.jsonl, _sizecache.json, _tmp/.
This fetcher OWNS only the per-cube *.parquet (it merges into them) and its own
_incr_state.json; it never touches the bulk markers/manifests or any other file.

WHY extend_by_date (a genuine date-tail), not a whole-cube re-download:
  getFullTableDownloadCSV (what the bulk ingester uses) is full-table only — no date
  filter. But WDS exposes a true date-tail keyed on the SAME vector ids already on
  disk: getBulkVectorDataByRange(vectorIds, startDataPointReleaseDate, end). It
  returns only the datapoints (RELEASE-dated) in the window — i.e. brand-new periods
  AND revisions to old periods — keyed by vectorId. So we:
    1. Poll getChangedCubeList(<watermark>) — cheap change-feed of changed productIds.
    2. For each changed cube WITH an on-disk parquet: read its distinct vectors and
       their (geo,uom,coordinate) constants from disk, then pull only datapoints
       released since the watermark via getBulkVectorDataByRange (chunked 250/req).
    3. Build rows in the EXACT 7-column on-disk schema (series_key="v"+vectorId;
       geo/uom/coordinate backfilled per-vector from disk; status="." . when value
       null) and merge.merge_and_write (dedup (series_key,obs_date); new value wins
       on revision; never-shrink; atomic). Existing series gain new dates / revised
       values — they are EXTENDED, never duplicated or shrunk.

Scope: only cubes already on disk are refreshed (the merge target must exist, and
the vector endpoint does not carry the dimension metadata needed to materialise a
brand-new cube from scratch). Brand-new cubes remain the bulk ingester's job; this
keeps the incremental run cheap and never invents geo/uom we cannot verify.

Honest-status contract (Tally + finalize):
  - Each changed cube is one sub-unit. A successful merge -> added_unit(n_new); a
    cube whose tail genuinely has 0 new datapoints -> empty_unit().
  - A timeout / 5xx / 429 / network drop -> transient_unit() (status 'partial'; the
    orchestrator does NOT advance last_success and the watermark is NOT advanced, so
    the same window is retried next run). Existing parquet is left untouched.
  - empty_window_floor is set very high: a quiet poll where the change-feed lists few
    or no cubes (StatCan does not release every cube every day) is LEGITIMATE and must
    not be laundered into a structural DefinitiveError. Genuine structural breaks
    surface at the HTTP/JSON layer instead.
"""
from __future__ import annotations
import datetime as dt
import json
import os
import time

import pyarrow as pa
import pyarrow.compute as pc
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Tally, finalize, sane_since

OUT_DIR = os.path.join(config.DATA_ROOT, "statcan")
STATE = os.path.join(OUT_DIR, "_incr_state.json")

BASE = "https://www150.statcan.gc.ca/t1/wds/rest"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
      "Content-Type": "application/json"}
SOURCE = "statcan"

DEDUP = ("series_key", "obs_date")

# WDS caps the vector endpoints at 300 ids/request; stay safely under.
VEC_CHUNK = 250
# How far back to look when there is no stored watermark yet (first incremental run).
# The change-feed + release-date window are both release-dated, so a generous backstop
# only re-confirms already-stored points (merge dedups them) — it never loses data.
COLD_LOOKBACK_DAYS = 30
# Re-poll the change-feed from a few days BEFORE the watermark to absorb clock/feed
# skew (a cube whose release lands right on the boundary must not be missed).
FEED_SLACK_DAYS = 2

# On-disk schema, byte-for-byte what jobs/ingest_statcan.py writes.
SCHEMA = pa.schema([
    ("series_key", pa.string()),
    ("obs_date", pa.date32()),
    ("value", pa.float64()),
    ("geo", pa.string()),
    ("uom", pa.string()),
    ("coordinate", pa.string()),
    ("status", pa.string()),
])


# --------------------------------------------------------------------------- #
# date parsing — identical semantics to the bulk ingester's parse_refdate so a
# merged row lands on the SAME obs_date the bulk file already uses (dedup works).
# --------------------------------------------------------------------------- #
def _parse_refper(p):
    if not p:
        return None
    p = p.strip()
    try:
        n = len(p)
        if n == 4 and p.isdigit():
            return dt.date(int(p), 12, 31)            # annual -> year-end
        if n == 7:                                     # YYYY-MM
            y, m = p.split("-")
            return dt.date(int(y), int(m), 1)
        if n == 10:                                    # YYYY-MM-DD
            y, m, d = p.split("-")
            return dt.date(int(y), int(m), int(d))
        if "/" in p:                                   # YYYY/YYYY fiscal range
            first = p.split("/")[0].strip()
            if first.isdigit() and len(first) == 4:
                return dt.date(int(first), 12, 31)
    except (ValueError, KeyError):
        return None
    return None


def _coord_trim(coord):
    """Vector endpoint returns a 10-part padded coordinate ('1.1.1.0.0.0.0.0.0.0');
    on-disk it is trimmed to the significant prefix ('1.1.1'). Drop trailing '.0'
    groups so the merged value matches the existing column (consistency, not a key)."""
    if not coord:
        return ""
    parts = coord.split(".")
    while len(parts) > 1 and parts[-1] == "0":
        parts.pop()
    return ".".join(parts)


# --------------------------------------------------------------------------- #
# state (release-date watermark + per-cube cursors)
# --------------------------------------------------------------------------- #
def _load_state():
    if os.path.exists(STATE):
        try:
            d = json.load(open(STATE))
            d.setdefault("last_release_date", None)   # 'YYYY-MM-DD'
            return d
        except Exception:
            pass
    return {"last_release_date": None}


def _save_state(state):
    tmp = STATE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f)
    os.replace(tmp, STATE)


def _post(endpoint, payload, tries=5):
    """POST a WDS endpoint. Returns parsed JSON on 200. TransientError on
    timeout/5xx/429/network/truncated-body (retry next run); DefinitiveError on a
    hard non-200 (!=429)."""
    url = f"{BASE}/{endpoint}"
    for a in range(tries):
        try:
            r = requests.post(url, json=payload, headers=UA, timeout=180)
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


def _get(endpoint, tries=5):
    """GET a WDS endpoint (used for the change-feed). Same transient/definitive rules."""
    url = f"{BASE}/{endpoint}"
    for a in range(tries):
        try:
            r = requests.get(url, headers=UA, timeout=120)
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


def _changed_pids(feed_since: dt.date):
    """getChangedCubeList(<date>) -> set of productIds changed on/after that date.
    TransientError if the envelope is missing (treated as a flaky read, retried)."""
    j = _get(f"getChangedCubeList/{feed_since.isoformat()}")
    if not isinstance(j, dict) or j.get("status") != "SUCCESS":
        raise TransientError(f"statcan getChangedCubeList bad envelope: {str(j)[:200]}")
    pids = set()
    for o in j.get("object") or []:
        pid = o.get("productId")
        if pid is not None:
            pids.add(int(pid))
    return pids


def _disk_vector_map(path):
    """Read an on-disk cube parquet -> {vectorId(int): (geo, uom, coordinate)}.
    Each vector maps to exactly one (geo,uom,coordinate) on disk (verified), so this
    backfill is lossless and keeps merged rows consistent with existing columns."""
    t = blob.read_table(path)
    d = t.to_pydict()
    out = {}
    keys = d.get("series_key", [])
    geos = d.get("geo", [])
    uoms = d.get("uom", [])
    coords = d.get("coordinate", [])
    for i, k in enumerate(keys):
        if not k or k[0] not in "vV":
            continue
        try:
            vid = int(k[1:])
        except ValueError:
            continue
        if vid in out:
            continue
        out[vid] = (geos[i] if i < len(geos) else None,
                    uoms[i] if i < len(uoms) else None,
                    coords[i] if i < len(coords) else None)
    return out


def _empty_table():
    return pa.table({n: pa.array([], type=f.type) for n, f in zip(SCHEMA.names, SCHEMA)},
                    schema=SCHEMA)


def _fetch_cube_tail(vmap, start_release: dt.date, end_release: dt.date):
    """Pull datapoints released in [start_release, end_release] for all of a cube's
    vectors, in chunks. Returns a table in the on-disk schema. Raises TransientError
    on any chunk's transient fault (caller marks the cube transient, leaves it for
    next run)."""
    vids = list(vmap)
    rows_k, rows_d, rows_v, rows_g, rows_u, rows_c, rows_s = [], [], [], [], [], [], []
    start_s = start_release.isoformat() + "T00:00"
    end_s = end_release.isoformat() + "T23:59"
    for i in range(0, len(vids), VEC_CHUNK):
        chunk = vids[i:i + VEC_CHUNK]
        payload = {"vectorIds": [str(v) for v in chunk],
                   "startDataPointReleaseDate": start_s,
                   "endDataPointReleaseDate": end_s}
        data = _post("getBulkVectorDataByRange", payload)
        if isinstance(data, dict):
            data = [data]
        for item in data or []:
            if not isinstance(item, dict) or item.get("status") != "SUCCESS":
                # A per-vector non-SUCCESS (e.g. throttled mid-list) is a transient
                # sub-fault: abort this cube cleanly so it retries next run rather
                # than publishing a half-window.
                raise TransientError("statcan getBulkVectorDataByRange: non-SUCCESS item")
            o = item.get("object") or {}
            vid = o.get("vectorId")
            if vid is None:
                continue
            vid = int(vid)
            geo, uom, coord_disk = vmap.get(vid, (None, None, None))
            for dp in o.get("vectorDataPoint") or []:
                od = _parse_refper(dp.get("refPer") or dp.get("refPerRaw"))
                if od is None:
                    continue
                raw = dp.get("value")
                try:
                    val = float(raw) if raw is not None else None
                except (TypeError, ValueError):
                    val = None
                # on-disk invariant: status==".." iff value is null/suppressed.
                status = ".." if val is None else ""
                rows_k.append(f"v{vid}")
                rows_d.append(od)
                rows_v.append(val)
                rows_g.append(geo)
                rows_u.append(uom)
                # prefer the disk coordinate (already trimmed & verified); fall back
                # to a trimmed endpoint coordinate for any vector not yet on disk.
                rows_c.append(coord_disk if coord_disk is not None
                              else _coord_trim(o.get("coordinate")))
                rows_s.append(status)
        time.sleep(0.2)   # polite pacing (WDS soft limit ~25 req/s/IP)
    return pa.table({
        "series_key": pa.array(rows_k, pa.string()),
        "obs_date": pa.array(rows_d, pa.date32()),
        "value": pa.array(rows_v, pa.float64()),
        "geo": pa.array(rows_g, pa.string()),
        "uom": pa.array(rows_u, pa.string()),
        "coordinate": pa.array(rows_c, pa.string()),
        "status": pa.array(rows_s, pa.string()),
    }, schema=SCHEMA)


def update(unit, since) -> Result:
    os.makedirs(OUT_DIR, exist_ok=True)
    state = _load_state()

    today = dt.date.today()
    # Release-date watermark. Prefer our own stored watermark; else the caller's
    # last_obs_date hint (guarded against corrupt far-future sentinels); else a cold
    # lookback. The change-feed is polled from a few days earlier to absorb skew.
    wm = state.get("last_release_date")
    if not wm:
        wm = sane_since(since, max_future_days=400)
    try:
        wm_date = dt.date.fromisoformat(str(wm)[:10]) if wm else None
    except ValueError:
        wm_date = None
    if wm_date is None:
        wm_date = today - dt.timedelta(days=COLD_LOOKBACK_DAYS)

    feed_since = wm_date - dt.timedelta(days=FEED_SLACK_DAYS)
    # window for datapoint release-date filter (same lower bound as the feed slack so
    # a cube flagged changed has its boundary points fetched too).
    win_start = feed_since

    # cheap change-feed (transient-safe). A flaky read raises -> partial, no advance.
    changed = _changed_pids(feed_since)

    tally = Tally()
    series_cursors: dict = {}
    maxd = None
    # advance the watermark only to the OLDEST release time that we have NOT fully
    # processed; on full success it becomes `today`. On any transient sub-fault we
    # leave it unchanged so the whole window is retried.
    all_ok = True

    for pid in sorted(changed):
        path = os.path.join(OUT_DIR, f"{pid}.parquet")
        if not blob.exists(path):
            # brand-new cube — out of scope for the incremental fetcher (bulk ingester
            # owns first ingest; vector endpoint lacks the dimension metadata to build
            # a faithful cube from scratch). Skip without counting as a sub-unit.
            continue
        try:
            vmap = _disk_vector_map(path)
        except Exception as e:  # corrupt/locked file -> transient, retry next run
            tally.transient_unit(); all_ok = False
            continue
        if not vmap:
            tally.empty_unit()
            continue
        try:
            tbl = _fetch_cube_tail(vmap, win_start, today)
        except TransientError:
            tally.transient_unit(); all_ok = False
            continue

        if tbl.num_rows == 0:
            # No datapoints released in the window for this cube — a legitimate quiet
            # cube (flagged changed for a metadata-only touch, or already captured).
            tally.empty_unit()
            md = merge._max_obs_date(blob.read_table(path))
            if md:
                series_cursors[str(pid)] = md
            continue

        before = blob.row_count(path)
        try:
            n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
        except DefinitiveError:
            # never-shrink / column-drop guard tripped -> keep existing data, surface
            # as a sub-unit failure rather than crashing the whole run.
            tally.structural_unit(); all_ok = False
            continue
        delta = max(0, n - before)
        tally.added_unit(delta)
        if md:
            series_cursors[str(pid)] = md
            try:
                md_d = dt.date.fromisoformat(md)
                if maxd is None or md_d > maxd:
                    maxd = md_d
            except ValueError:
                pass

    # Advance the watermark only on a clean pass (no transient/structural sub-fault),
    # so an interrupted/throttled run re-polls the same release window next time.
    if all_ok:
        state["last_release_date"] = today.isoformat()
        _save_state(state)

    last = maxd.isoformat() if maxd else (str(since)[:10] if since else None)

    # `obs` on the Result reports new rows merged this run (added). empty_window_floor
    # is very high: a poll where the change-feed lists few/no cubes, or every changed
    # cube's tail is empty, is LEGITIMATE for StatCan (it does not release every cube
    # every day) and must NOT raise a structural DefinitiveError. True structural /
    # transport breaks already surface in _get/_post and _changed_pids.
    return finalize(tally, tally.added, last, source=SOURCE,
                    series_cursors=series_cursors, empty_window_floor=10 ** 9)
