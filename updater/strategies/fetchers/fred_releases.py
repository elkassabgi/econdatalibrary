"""S2 fetcher — FRED public-domain release crawl (extend_by_date).

Layout: ONE parquet per FRED release under clean_full/fred/release_NNNNN.parquet
(~164 files), schema (series_id, title, frequency, units, obs_date, value), plus a
_manifest.json checkpoint. NOTE: these live in the SAME clean_full/fred/ dir as the
separate `fred` source's fred.parquet — this fetcher OWNS only release_*.parquet +
_manifest.json and never touches fred.parquet.

Refresh: the v2 fred/v2/release/observations endpoint accepts observation_start, so
for each already-stored release we read its max obs_date and fetch ONLY newer
observations (re-fetching the last stored day to catch same-day revisions), then
MERGE (dedup on (series_id, obs_date), new values win, never-shrink). The /releases
list is re-enumerated each run to also catch brand-new release_ids.

COPYRIGHT FILTER (preserved verbatim from jobs/ingest_fred_releases.py): only series
whose copyright_id is public-domain are stored; copyrighted series (e.g. Case-Shiller,
S&P, ICE/BAML) are NEVER written. 0 copyrighted series end up on disk.

0-OBS-MARKED-DONE BUG FIX: a release is recorded "done" only when it actually yields
public-domain rows. A release that upstream genuinely confirms empty (HTTP 200 with
zero public-domain series across all pages — e.g. an all-copyrighted release) is
recorded separately as "confirmed_empty" and is NOT written as a 0-row file. A
transient failure (timeout/5xx/429/network) raises TransientError and marks NOTHING,
so the release is re-attempted next run instead of being frozen. We never write a
0-row group and then skip it.
"""
from __future__ import annotations
import datetime as dt
import json
import os
import threading
import time

import pyarrow as pa
import pyarrow.compute as pc
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Tally, finalize

# The release files physically live in clean_full/fred/ (shared with the `fred`
# source), per the source's established storage layout — NOT clean_full/fred_releases/.
OUT_DIR = os.path.join(config.DATA_ROOT, "fred")
MANIFEST = os.path.join(OUT_DIR, "_manifest.json")

BASE = "https://api.stlouisfed.org/fred"
V2_BASE = "https://api.stlouisfed.org/fred/v2"

UA = "Econ-Fin Data Library admin@hfdatalibrary.com"

# Copyright filter — preserved exactly from the ingester.
PUBLIC_DOMAIN = {"public domain: citation requested", "public domain"}
COPYRIGHT_BLOCKLIST = {"copyright", "proprietary", "s&p", "case-shiller",
                       "ice", "baml", "ml bond"}

DEDUP = ("series_id", "obs_date")

SCHEMA = pa.schema([
    ("series_id",  pa.string()),
    ("title",      pa.string()),
    ("frequency",  pa.string()),
    ("units",      pa.string()),
    ("obs_date",   pa.date32()),
    ("value",      pa.float64()),
])


def _headers(key):
    return {"Authorization": f"Bearer {key}", "User-Agent": UA}


def _list_headers():
    # NOTE: the FRED v1 /releases endpoint does NOT accept the Bearer header (it
    # returns HTTP 400 "Variable api_key is not set"); only the v2 observations
    # endpoint does. So /releases must keep api_key as a query param. The secret is
    # kept out of every persisted error string via _redact() instead (defense in
    # depth — see _get_releases below).
    return {"User-Agent": UA}


def _redact(msg, key):
    """Belt-and-suspenders: strip the secret from any string before it reaches an
    error message (defense in depth even though the key is now header-only)."""
    s = str(msg)
    if key:
        s = s.replace(key, "***")
    return s


def _is_blocked(title_lower, notes_lower):
    return any(b in title_lower or b in notes_lower for b in COPYRIGHT_BLOCKLIST)


def _release_path(release_id: int) -> str:
    return os.path.join(OUT_DIR, f"release_{release_id:05d}.parquet")


def _max_obs_date(path) -> dt.date | None:
    """Max obs_date already stored for this release (None if no file/empty)."""
    if not blob.exists(path):
        return None
    t = blob.read_table(path)
    if t.num_rows == 0 or "obs_date" not in t.column_names:
        return None
    m = pc.max(t.column("obs_date")).as_py()
    if isinstance(m, dt.datetime):
        m = m.date()
    return m


def _load_manifest() -> dict:
    if os.path.exists(MANIFEST):
        try:
            d = json.load(open(MANIFEST))
            d.setdefault("done_release_ids", [])
            d.setdefault("confirmed_empty", [])
            return d
        except Exception:
            pass
    return {"done_release_ids": [], "confirmed_empty": []}


def _save_manifest(man: dict, total_obs: int) -> None:
    man = dict(man)
    man["done_release_ids"] = sorted(set(man.get("done_release_ids", [])))
    man["confirmed_empty"] = sorted(set(man.get("confirmed_empty", [])))
    man["total_obs"] = int(total_obs)
    tmp = MANIFEST + ".tmp"
    with open(tmp, "w") as f:
        json.dump(man, f)
    os.replace(tmp, MANIFEST)


def _get_releases(session, key, tries=5):
    """Enumerate all FRED release ids/names. Transient on 5xx/timeout."""
    all_rels = []
    offset = 0
    while True:
        d = None
        for a in range(tries):
            try:
                r = session.get(f"{BASE}/releases",
                                params={"api_key": key, "file_type": "json",
                                        "limit": 1000, "offset": offset},
                                headers=_list_headers(), timeout=60)
            except (requests.Timeout, requests.ConnectionError) as e:
                if a == tries - 1:
                    raise TransientError(f"FRED /releases offset={offset}: {_redact(e, key)}")
                time.sleep(min(2 ** a, 30)); continue
            if r.status_code == 200:
                # Parse JSON INSIDE the retry loop: a truncated/partial 200 body
                # (requests JSONDecodeError -> ValueError) is a realistic transient
                # fault and must be retried, not propagated raw out of update().
                try:
                    d = r.json()
                except ValueError as e:
                    if a == tries - 1:
                        raise TransientError(f"FRED /releases offset={offset}: bad json: {_redact(e, key)}")
                    time.sleep(min(2 ** a, 30)); continue
                break
            if r.status_code in (429, 500, 502, 503, 504):
                if a == tries - 1:
                    raise TransientError(f"FRED /releases HTTP {r.status_code}")
                time.sleep(min(2 ** a, 30)); continue
            raise DefinitiveError(f"FRED /releases HTTP {r.status_code}")
        if d is None:
            raise TransientError(f"FRED /releases offset={offset}: no response body")
        rels = d.get("releases", [])
        all_rels.extend(rels)
        if len(all_rels) >= int(d.get("count", 0)) or not rels:
            break
        offset += 1000
        time.sleep(0.2)
    return all_rels


def _stream_json(session, url, params, headers, t_start, deadline_s,
                 connect_to=15, read_gap_to=60):
    """GET with a HARD total wall-clock timeout enforced by a worker thread.

    A trickle ("slowloris") server can defeat BOTH the per-read socket timeout (it
    resets on every received byte) AND a between-chunk deadline check (urllib3 may
    block inside a single recv() that never returns control). The only robust guard
    is to run the request+read in a daemon thread and join() it with a hard timeout;
    if it overruns we abandon the thread and raise TransientError. The daemon thread
    dies with the process / when its socket eventually errors.

    Returns (status_code, parsed_json_or_None). Raises requests exceptions (for the
    caller's transient handling) or TransientError when the hard timeout is hit.
    """
    remaining = deadline_s - (time.monotonic() - t_start)
    if remaining <= 0:
        raise TransientError("deadline exceeded before request")

    box: dict = {}

    def _worker():
        try:
            r = session.get(url, params=params, headers=headers,
                            timeout=(connect_to, read_gap_to))
            try:
                if r.status_code != 200:
                    box["result"] = (r.status_code, None)
                    return
                import json as _json
                box["result"] = (200, _json.loads(r.content.decode("utf-8")))
            finally:
                r.close()
        except BaseException as e:  # noqa: BLE001 — surfaced to main thread below
            box["error"] = e

    th = threading.Thread(target=_worker, daemon=True)
    th.start()
    th.join(timeout=max(1.0, remaining))
    if th.is_alive():
        # Thread is wedged on a non-returning socket read; abandon it (daemon).
        raise TransientError(f"hard timeout after {remaining:.0f}s")
    if "error" in box:
        raise box["error"]
    return box["result"]


def _fetch_release(session, key, release_id, obs_start, tries=2, deadline_s=90):
    """Pull public-domain observations for one release via v2 cursor pagination.

    Returns (table, confirmed_empty_bool). confirmed_empty is True only when every
    page returned HTTP 200 and there were zero public-domain observations across the
    whole release — i.e. upstream genuinely has no public-domain data here. Raises
    TransientError on timeout/5xx/429/network; DefinitiveError on hard 4xx (!=404).
    A 404 for the release is treated as confirmed-empty (release has no obs endpoint).

    A per-release wall-clock budget (deadline_s) bounds total time across pages and
    retries, so a slow-trickle ("slowloris") server that defeats the per-read socket
    timeout still cannot hang the whole crawl — it raises TransientError and the
    release is re-queued for the next run with its existing data untouched.
    """
    sids, titles, freqs, units_list, dates, vals = [], [], [], [], [], []
    saw_pd = False
    cursor = None
    t_start = time.monotonic()
    while True:
        if time.monotonic() - t_start > deadline_s:
            raise TransientError(f"FRED rel {release_id}: exceeded {deadline_s}s budget")
        params = {"release_id": release_id, "format": "json", "limit": 500000}
        if obs_start:
            params["observation_start"] = obs_start
        if cursor:
            params["next_cursor"] = cursor

        d = None
        for a in range(tries):
            if time.monotonic() - t_start > deadline_s:
                raise TransientError(f"FRED rel {release_id}: exceeded {deadline_s}s budget")
            try:
                # Streaming GET with a HARD total deadline — defeats slow-trickle
                # servers that would otherwise hang the per-read socket timeout.
                status, d = _stream_json(session, f"{V2_BASE}/release/observations",
                                         params, _headers(key), t_start, deadline_s)
            except (requests.Timeout, requests.ConnectionError) as e:
                if a == tries - 1:
                    raise TransientError(f"FRED rel {release_id}: {_redact(e, key)}")
                time.sleep(min(2 ** a, 20)); continue
            if status == 200:
                break
            if status == 404:
                # Release has no observations endpoint — genuinely nothing to store.
                tbl = pa.table({c.name: pa.array([], type=c.type) for c in SCHEMA})
                return tbl.cast(SCHEMA), True
            if status in (429, 500, 502, 503, 504):
                if a == tries - 1:
                    raise TransientError(f"FRED rel {release_id} HTTP {status}")
                time.sleep(min(2 ** a, 20)); continue
            raise DefinitiveError(f"FRED rel {release_id} HTTP {status}")

        if d is None:
            raise TransientError(f"FRED rel {release_id}: no response body")
        for s in d.get("series", []):
            cid = (s.get("copyright_id") or "").lower()
            title = s.get("title", "")
            notes = s.get("notes", "")
            if cid not in PUBLIC_DOMAIN or _is_blocked(title.lower(), notes.lower()):
                continue  # copyrighted — NEVER stored
            saw_pd = True
            sid = s["series_id"]
            freq = s.get("frequency", "")
            unit = s.get("units", "")
            for obs in s.get("observations", []):
                v = obs.get("value", "")
                if v in (".", "", None):
                    continue
                try:
                    fv = float(v)
                except (ValueError, TypeError):
                    continue
                try:
                    od = dt.date.fromisoformat(obs["date"])
                except (ValueError, KeyError, TypeError):
                    continue
                sids.append(sid); titles.append(title); freqs.append(freq)
                units_list.append(unit); dates.append(od); vals.append(fv)

        if not d.get("has_more") or not d.get("next_cursor"):
            break
        cursor = d.get("next_cursor")
        time.sleep(0.1)

    tbl = pa.table({
        "series_id": pa.array(sids, pa.string()),
        "title":     pa.array(titles, pa.string()),
        "frequency": pa.array(freqs, pa.string()),
        "units":     pa.array(units_list, pa.string()),
        "obs_date":  pa.array(dates, pa.date32()),
        "value":     pa.array(vals, pa.float64()),
    }, schema=SCHEMA)
    # confirmed_empty only when upstream returned 200s with zero public-domain series.
    return tbl, (not saw_pd and tbl.num_rows == 0)


# R36: both of these READ through blob but LISTED with os.listdir behind an os.path.isdir
# guard. Under AQUEDUCT_BACKEND=r2 the local directory is absent on the runner, the guard
# short-circuits, and they report 0 rows and no frontier — values, not errors, so nothing
# downstream reads them as a failure to look.
def _release_files() -> list[str]:
    return [x for x in blob.list_parquets(OUT_DIR) if x.startswith("release_")]


def _total_rows() -> int:
    return sum(blob.row_count(os.path.join(OUT_DIR, x)) for x in _release_files())


def _global_max_date() -> str | None:
    best = None
    for x in _release_files():
        md = _max_obs_date(os.path.join(OUT_DIR, x))
        if md and (best is None or md > best):
            best = md
    return best.isoformat() if best else None


def update(unit, since) -> Result:
    try:
        from core.config import require as _require
        key = _require("FRED_API_KEY")
    except SystemExit as e:
        raise DefinitiveError(f"FRED_API_KEY missing from .env: {e}")
    except Exception as e:
        raise DefinitiveError(f"FRED_API_KEY unavailable: {e}")
    if not key:
        raise DefinitiveError("FRED_API_KEY missing from .env (required, Bearer v2)")

    os.makedirs(OUT_DIR, exist_ok=True)
    man = _load_manifest()
    confirmed_empty = set(man.get("confirmed_empty", []))
    done_ids = set(man.get("done_release_ids", []))

    # Caller-provided global lower bound (None or 'YYYY-MM-DD').
    since_global = None
    if since:
        try:
            since_global = dt.date.fromisoformat(str(since)[:10])
        except ValueError:
            since_global = None

    session = requests.Session()
    releases = _get_releases(session, key)
    if not releases:
        raise TransientError("FRED /releases returned no releases")

    maxd = None
    processed = 0
    tally = Tally()
    # Per-release file-identity cursors {release_NNNNN: 'YYYY-MM-DD'} = max obs_date
    # written/seen for that release this run, so a frozen release can't hide behind a
    # unit-level global max.
    series_cursors: dict = {}

    for rel in releases:
        rid = rel["id"]
        path = _release_path(rid)
        has_file = blob.exists(path)

        # Persist the manifest periodically so confirmed_empty / done accumulate even
        # if the run is interrupted — bounding the runtime of subsequent runs (they
        # then skip already-confirmed all-copyright releases instead of re-probing).
        processed += 1
        if processed % 25 == 0:
            _save_manifest({"done_release_ids": list(done_ids),
                            "confirmed_empty": list(confirmed_empty)}, _total_rows())

        # Skip re-probing releases upstream already confirmed all-copyrighted and that
        # have no stored data — they yield 0 PD obs every run (correct, not a failure).
        # This is NOT counted as a sub-unit attempt: it is a deliberate no-op, not an
        # upstream "empty", so it must not contribute to the all-empty structural floor.
        if not has_file and rid in confirmed_empty:
            continue

        stored_max = _max_obs_date(path)
        if stored_max is not None:
            # date-tail: re-fetch from the last stored day (catches same-day revisions).
            obs_start = stored_max.isoformat()
        else:
            # NEVER-STORED release (incl. a brand-new release_id that first appeared
            # AFTER the initial run): always FULL-backfill its history. The caller's
            # global `since_global` cursor must only constrain releases we already track
            # (stored_max not None); using it here would silently drop a new release's
            # entire prior history.
            obs_start = None

        try:
            tbl, conf_empty = _fetch_release(session, key, rid, obs_start)
        except TransientError:
            # Existing data untouched; do NOT mark done. Honest partial -> re-queued.
            tally.transient_unit()
            continue

        if tbl.num_rows == 0:
            if conf_empty and not has_file:
                # Upstream genuinely confirms empty (all-copyrighted / no obs endpoint).
                confirmed_empty.add(rid)
            # else: a tail fetch returned no NEW rows for an existing file — leave it
            # untouched (existing data preserved), do not write a 0-row group.
            # A 200 with zero public-domain rows is a LEGITIMATE empty for this source
            # (all-copyrighted release / quiet day), NOT a structural break.
            tally.empty_unit()
            # Preserve the existing on-disk frontier in the per-release cursor so a
            # populated-but-quiet release still reports a fresh cursor.
            if stored_max is not None:
                series_cursors[f"release_{rid:05d}"] = stored_max.isoformat()
            time.sleep(0.2)
            continue

        before = blob.row_count(path)
        n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
        delta = max(0, n - before)
        tally.added_unit(delta)
        if md:
            series_cursors[f"release_{rid:05d}"] = md
            md_d = dt.date.fromisoformat(md)
            if maxd is None or md_d > maxd:
                maxd = md_d
        done_ids.add(rid)
        confirmed_empty.discard(rid)  # it now has data
        time.sleep(0.2)

    total = _total_rows()
    _save_manifest({"done_release_ids": list(done_ids),
                    "confirmed_empty": list(confirmed_empty)}, total)

    last = (maxd.isoformat() if maxd else _global_max_date())

    # finalize() yields honest status: structural -> DefinitiveError, any transient
    # -> 'partial' (orchestrator does NOT advance last_success; re-run next tick),
    # else 'ok'/'no_change'. The all-empty structural floor is effectively disabled
    # for this source (empty_window_floor far above the ~164 releases): an all-quiet
    # day where every populated release tail-fetches 0 new rows is LEGITIMATE here
    # (FRED does not publish every release daily) and must not raise DefinitiveError.
    # Genuine structural breaks surface at the HTTP layer (transient/definitive in
    # _get_releases/_fetch_release), not as "200 parsed 0 rows across all releases".
    return finalize(tally, total, last, source="fred_releases",
                    series_cursors=series_cursors, empty_window_floor=10 ** 9)
