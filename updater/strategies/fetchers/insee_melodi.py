"""S3 (sdmx_delta) fetcher — INSEE Melodi, France. Keyless SDMX 2.1 catalog.

Layout (set by jobs/ingest_insee_melodi.py): ONE parquet per DATAFLOW under
clean_full/insee_melodi/<FLOW_CODE>.parquet with schema
  flow        : string  (constant within a file — the dataflow code)
  series_key  : string  ("DIM=val:DIM=val:..." over every dimension EXCEPT TIME_PERIOD,
                         sorted by dimension name — exactly the ingester's composite key)
  obs_date    : date32  (parsed from TIME_PERIOD: YYYY->Dec-31, YYYY-Qn->quarter-start,
                         YYYY-MM->month-start, YYYY-MM-DD->that day)
  value       : float64 (first non-null measure's .value)
The dedup identity is (flow, series_key, obs_date).

WHY THIS FETCHER WAS REBUILT (2026-10-16, design reviewed as AR-330, ledger R1588/R1589).
  * 46+ flows were served at EXACTLY one Melodi page (10,000 rows) - all 39 DS_RP_TD_* census tables
    among them - because pagination could end early (a 4xx or an empty page treated as "no more data")
    and nothing checked. INSEE publishes no total, so the store looked complete.
  * Every pass since 2026-10-06 was killed at the 45-minute unit limit: _fetch_flow_obs held every page's
    raw JSON in one list (~1.6 KB per observation, 16 GB runner) and a census flow's "tail"
    (startPeriod = its only year) is the whole flow; Melodi also ignores startPeriod for RP flows.

HOW A PASS RUNS NOW (slots in precedence order, all inside the 35-minute soft stop - R2-7):
  0. operator force: every flow listed in _force_origin.json (once per listing) is sent to needs_origin,
     whatever its status (see STATUSES AND THE OPERATOR below).
  1. identity: one HEAD per served file, compared with _flows.json; a changed object is re-classified and
     any attempt it carries is dropped (a tail never finishes a flow sent to origin - AR-331 X4); a
     missing served object is named in the note.
  2. CLASSIFY (<= 5 min, charged with the probes, their two controls and the page-determinism control):
     an unclassified flow is NEVER written by the walk (R2-2). needs_origin when ANY key-spelling
     partition (dimension-name set + GEO vintage) or the total is an exact positive multiple of 10,000,
     the flow is zero-row/absent, it is in CI_PAGE_MULTIPLE_DELTAS, or the operator forces it. Otherwise a
     one-time page-beyond probe (page floor(S/10,000)+2 of an origin query, S = the DISTINCT normalised
     (key without FREQ and GEO vintage, date) count - R2-1) decides: non-empty means INSEE serves more than
     we hold. An empty probe is NOT proof of completeness.
  3. WALK (until minute 15): the healthy flows, rotated; each is tailed only when INSEE's catalog
     `modified` changed, the catalog read failed, or 30 days passed. Every tail is STAGED like a re-pull;
     one that outgrows one chunk (100 pages) joins the repair queue.
  4. REPAIR (until the soft stop): ONE flow at a time - an in-progress or merge-pending attempt first,
     then needs_origin, then needs_refresh. Pages are parsed at once into Arrow, staged locally and
     uploaded every 100 pages as _repull/<FLOW>/<attempt>/<n>.chunk; the cursor lives in _flows.json.
  A flow is COMPLETE PER INSEE PAGING only when: paging.isLast is True (a full last page is confirmed by an
  empty/400 page+1); there are no duplicates ACROSS pages (within-page duplicates are a parse collision,
  labelled, never a restart - R2-5); the chunk footers sum to the staged rows. Then a measured merge:
  rows new or changed against the stored file decide changed_keys (an idempotent tail derives nothing);
  for an origin pull the stored rows INSEE no longer serves are counted as RESIDUE and KEPT (merge, never
  replace - Q1). Only then is the served file published, then the state, then the chunks deleted.

  While any repair work remains the pass returns `transient_fail` WITH changed_keys (the _giant / PR #151
  precedent): due at the next run, and the merged flows' CSVs are derived this pass. Its note carries the
  counters health and the digest show.

STATE AND STAGING (all through blob - R2 before T0, the local store after): `_flows.json` (this fetcher's
state; conditional writes - see _melodi_state.py), `_force_origin.json` (OPERATOR-owned, read only; its
format is under STATUSES AND THE OPERATOR below), `_repull/<FLOW>/<attempt>/<nnnnn>.chunk`. Local
staging lives beside the bounded-merge spill dir (ECONDL_DUCKDB_TMP), never D:.

STATUSES AND THE OPERATOR (each flow's `status` in `_flows.json`; the note names the stuck ones every pass):
  `unclassified`        not profiled yet (or its served object changed); never written by the walk.
  `legacy`              no exact-multiple partition; probed once with the page-beyond probe; walked.
  `needs_origin`        a full origin pull is queued (a bootstrap rule, a non-empty probe, or the operator).
  `needs_refresh`       a due tail too large for the walk, queued for the repair slot (it may carry a
                        parked attempt).
  `in_progress`         the ONE flow being pulled; its attempt and cursor are in `attempt`.
  `merge_pending`       every page staged, waiting for the merge (it needs its time margin).
  `complete_per_paging` an origin pull ended on isLast with every check met; walked.
  `empty_per_paging`    INSEE serves no observations for it; walked.
  `withdrawn_suspect`   origin page 1 empty over stored rows: withdrawn by INSEE? Stored rows kept.
  `unstable_paging`     3 restarts (pages changed under the attempt); its chunks are deleted.
  `broken`              3 failures with no committed chunk between them, or a pull that parsed nothing;
                        its chunks are deleted.
  The last three are worked by nobody until the operator acts. A flow whose merge is refused is NOT one of
  them: it stays `merge_pending` with every staged chunk, retried each pass, and the note escalates
  (MERGE REFUSED n pass(es)) - a refusal is a condition of the merge, never of the paging (AR-330 C11).
  THE OPERATOR'S FILE is `_force_origin.json` (the fetcher never writes it). Two forms of "flows":
      {"flows": ["DS_X", "DS_Y"], "unblock_repair_after_utc": "<iso>"}
      {"flows": {"DS_X": "<iso>", "DS_Y": "<iso>"}, "unblock_repair_after_utc": "<iso>"}
  - A listed flow, in ANY status, drops its attempt (unless that attempt is already an origin pull) and goes
    to `needs_origin`: this is the exit from broken, unstable_paging and withdrawn_suspect.
  - The force is PER FLOW and happens ONCE (recorded as the flow's `forced_key`). In the list form a flow is
    forced once while it stays listed: adding or removing another flow forces nothing again, and neither
    does any other field. To force a flow AGAIN, either use the object form and give that flow a new value
    (only that flow is forced again), or take it off the list for one pass and list it again. A
    top-level `utc` field has no effect.
  - `unblock_repair_after_utc` later than the REPAIR BLOCKED time lifts a failed page-determinism control;
    it never forces a flow.
  - Never hand-edit `_flows.json`: it is compare-and-swap protected and a hand edit races a later pass.

T0 STEP (before the R2 write key is revoked): let every in-progress attempt finish, or abandon it (list the
flow in `_force_origin.json` is NOT enough - it starts a new attempt), and delete every `_repull/` chunk on
R2 - after T0 they cannot be deleted. Broken and unstable flows hold none (they delete theirs); a parked
`needs_refresh` attempt does. mirror_sync and upload_r2 carry only .parquet, so `_flows.json` and
`_force_origin.json` must be copied to the local store explicitly.

LIMITS, stated: completeness is "per INSEE paging" (no INSEE total exists); the served-parquet publish is not
conditional (two writers of one flow: last writer wins - the attempt lease protects only the repair); the
stored rows INSEE no longer serves are kept (residue, reported) until the owner decides otherwise. Memory:
classification and finalize run DuckDB under PROFILE_MEMORY_LIMIT and spill to a private directory (AR-331
F6: the earlier arrow profile peaked at 7.3 GB on DS_FLORES_A5 and 16.5 GB at 25.9M rows); the bounded
merge has its own limit. Python 3.11 (CI) runs pyarrow and duckdb here for the first time on its first run.

License: Licence Ouverte / Open Licence 2.0. Source: INSEE Melodi www.insee.fr
"""
from __future__ import annotations
import datetime as dt
import hashlib
import http.client
import os
import random
import re
import shutil
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Tally, finalize, load_rotation, rotate_after, save_rotation
from . import _giant
from . import _melodi_state as S

UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
      "Accept": "application/json"}
BASE = "https://api.insee.fr/melodi"
SOURCE = "insee_melodi"
DEDUP = ("flow", "series_key", "obs_date")
PAGE_SIZE = 10000        # Melodi page size (observed hard cap)
PAGE_CEILING = 20000     # cumulative per-attempt bound so a malformed paging block can't loop forever
CHUNK_PAGES = 100        # pages per staged chunk object
MAX_ATTEMPTS = 5
TIMEOUT = 120
RATE = 2.1               # 30 req/min keyless limit -> >=2s between calls
PAGE_DELAY = 2.1         # between pages of the same flow (same 30 req/min budget)

CLASSIFY_UNTIL_MIN = 5   # slot ends (minutes into the pass); the soft stop (35) ends the repair slot
WALK_UNTIL_MIN = 15
BACKSTOP_DAYS = 30       # tail a complete flow at least this often whatever `modified` says (C7)
LEASE_STALE_MIN = 60
MAX_RESTARTS = 3
STALL_PASSES = 4

# C1(c): flows whose CI-logged per-run delta was an exact multiple of 10,000 - read from this fetcher's own
# "keeping N existing rows" lines in the updater-daily logs, window 2026-08-23..09-05 (the 2026-08-30 run,
# 33307479002, which cut DS_TOUR_CAP and DS_SIDE_*_COM to one page). Provenance, not a guess.
CI_PAGE_MULTIPLE_DELTAS = frozenset({
    "DS_RP_TD_MENAGES_NOC_COMP", "DS_SIDE_CREA_COM", "DS_SIDE_CREA_DEP_REG_NAT",
    "DS_SIDE_CREA_ENT_DEP_REG_NAT_CJ", "DS_SIDE_STOCKS_COM", "DS_TOUR_CAP"})
PROBE_EMPTY_CONTROL = "DS_COEFF_EURO_FRANC"     # 125 rows: its page-beyond probe must be empty or 400

_SCHEMA = pa.schema([("flow", pa.string()), ("series_key", pa.string()),
                     ("obs_date", pa.date32()), ("value", pa.float64())])


# --------------------------------------------------------------------------------------------- #
# parse helpers — copied from jobs/ingest_insee_melodi.py so storage is identical
# --------------------------------------------------------------------------------------------- #
def parse_date(s):
    """TIME_PERIOD -> date32. YYYY->Dec31, YYYY-Qn->quarter start, YYYY-MM->month
    start, YYYY-MM-DD->that day. (Verbatim from the ingester.)"""
    s = (s or "").strip()
    try:
        if re.match(r'\d{4}-Q\d', s):
            y, q = s.split("-Q")
            return dt.date(int(y), (int(q) - 1) * 3 + 1, 1)
        if re.match(r'\d{4}-\d{2}$', s):
            return dt.date(int(s[:4]), int(s[5:7]), 1)
        if re.match(r'\d{4}$', s):
            return dt.date(int(s), 12, 31)
        if re.match(r'\d{4}-\d{2}-\d{2}', s):
            return dt.date.fromisoformat(s[:10])
    except (ValueError, KeyError):
        pass
    return None


def _obs_to_row(obs):
    """One Melodi observation -> (series_key, obs_date, value) or None. Mirrors the
    ingester: series_key over all dims except TIME_PERIOD (sorted), first measure value."""
    dims = obs.get("dimensions", {})
    tp = dims.get("TIME_PERIOD", "")
    d = parse_date(tp)
    if d is None:
        return None
    measures = obs.get("measures", {})
    fv = None
    for mv in measures.values():
        if isinstance(mv, dict) and mv.get("value") is not None:
            try:
                fv = float(mv["value"])
                break
            except (ValueError, TypeError):
                pass
    if fv is None:
        return None
    key = ":".join(f"{k}={v}" for k, v in sorted(dims.items()) if k != "TIME_PERIOD")
    return key, d, fv


# --------------------------------------------------------------------------------------------- #
# http — every call is bounded by the pass clock (C6)
# --------------------------------------------------------------------------------------------- #
class _Stop(Exception):
    """The pass clock cannot fit the next request, retry or wait. Not a failure: flush and stop."""


def _get_flows(sess, clock):
    """GET /dataflow/all -> list of {code,label}. Catalog failure is structural for the whole source."""
    st, payload = _request(sess, f"{BASE}/dataflow/all", clock)
    if st == "nodata" or not isinstance(payload, list) or not payload:
        raise TransientError(f"{SOURCE}: /dataflow/all returned no flow list ({st})")
    return payload


def _request(sess, url, clock):
    """GET once, with retries that fit the clock. Returns ("ok", json) or ("nodata", http_code) for a
    400/404. Raises _Stop when the next attempt or wait cannot finish before the hard end, TransientError
    after the retry budget (and for any other 4xx: a per-flow failure, never a source-wide veto - C11)."""
    last = None
    for attempt in range(MAX_ATTEMPTS):
        if not clock.fits(TIMEOUT + 5):
            raise _Stop(f"no room for a {TIMEOUT}s request")
        try:
            r = sess.get(url, headers=UA, timeout=TIMEOUT)
            if r.status_code == 200:
                payload = r.json()            # a truncated multi-MB body fails HERE, inside the retry
        except (requests.RequestException, http.client.HTTPException, ValueError) as e:
            # every transport fault is a retry: Timeout, ConnectionError, ChunkedEncodingError and
            # ContentDecodingError (an IncompleteRead on a large page), bad JSON (review F1)
            last = str(e)[:120]
            wait = min(2 ** attempt, 30) + random.uniform(0, 1.0)
            if attempt == MAX_ATTEMPTS - 1:
                break
            if not clock.fits(wait + TIMEOUT + 5):
                raise _Stop("no room for a retry")
            time.sleep(wait)
            continue
        if r.status_code == 200:
            return "ok", payload
        elif r.status_code in (400, 404):
            return "nodata", r.status_code
        elif r.status_code == 429:
            ra = r.headers.get("Retry-After")
            wait = min(int(ra) if (ra and ra.isdigit()) else 60, 120)
            last = "HTTP 429"
            if attempt == MAX_ATTEMPTS - 1:
                break
            if not clock.fits(wait + TIMEOUT + 5):
                raise _Stop("no room for a 429 wait")
            time.sleep(wait)
            continue
        elif r.status_code in (500, 502, 503, 504):
            last = f"HTTP {r.status_code}"
        else:
            raise TransientError(f"{SOURCE} GET {url}: HTTP {r.status_code}")
        if attempt == MAX_ATTEMPTS - 1:
            break
        wait = min(2 ** attempt, 30) + random.uniform(0, 1.0)
        if not clock.fits(wait + TIMEOUT + 5):
            raise _Stop("no room for a retry")
        time.sleep(wait)
    raise TransientError(f"{SOURCE} GET {url}: {last}")


class _Page:
    __slots__ = ("raw_n", "table", "is_last", "nodata", "http", "within_dups", "fp", "paging", "min_year")

    def __init__(self, raw_n=0, table=None, is_last=False, nodata=False, http=None, within_dups=0,
                 fp=None, paging=None, min_year=None):
        self.raw_n, self.table, self.is_last, self.nodata, self.http = raw_n, table, is_last, nodata, http
        self.within_dups, self.fp, self.paging, self.min_year = within_dups, fp, paging, min_year


def _page_url(flow, start_year, page):
    sp = f"startPeriod={start_year}&" if start_year is not None else ""
    return f"{BASE}/data/{flow}?{sp}page={page}"


def _parse_page(flow, observations) -> tuple[pa.Table, int, str, int | None]:
    """Raw observations -> (Arrow table, within-page duplicate count, whole-page fingerprint, min year).
    The fingerprint is sha256 of the sorted (series_key, obs_date, value) of every parsed row (C4)."""
    keys, dates, vals = [], [], []
    for o in observations:
        row = _obs_to_row(o)
        if row is None:
            continue
        keys.append(row[0]); dates.append(row[1]); vals.append(row[2])
    pairs = set(zip(keys, dates))
    dups = len(keys) - len(pairs)
    h = hashlib.sha256()
    for k, d, v in sorted(zip(keys, (x.isoformat() for x in dates), (repr(x) for x in vals))):
        h.update(f"{k}\t{d}\t{v}\n".encode("utf-8"))
    tbl = pa.table({"flow": pa.array([flow] * len(keys), pa.string()),
                    "series_key": pa.array(keys, pa.string()),
                    "obs_date": pa.array(dates, pa.date32()),
                    "value": pa.array(vals, pa.float64())}, schema=_SCHEMA)
    return tbl, dups, h.hexdigest(), (min(d.year for d in dates) if dates else None)


def _fetch_page(sess, flow, start_year, page, clock) -> _Page:
    st, payload = _request(sess, _page_url(flow, start_year, page), clock)
    if st == "nodata":
        return _Page(nodata=True, http=payload)
    if not isinstance(payload, dict):
        raise TransientError(f"{flow}: page {page} body is not an object")
    obs = payload.get("observations") or []
    paging = payload.get("paging") if isinstance(payload.get("paging"), dict) else {}
    tbl, dups, fp, my = _parse_page(flow, obs)
    return _Page(raw_n=len(obs), table=tbl, is_last=paging.get("isLast") is True, within_dups=dups,
                 fp=fp, paging={k: paging.get(k) for k in sorted(paging) if k != "next"}, min_year=my)


def _catalog_modified(sess, flow, clock) -> str | None:
    """INSEE's `modified` for a flow from /melodi/catalog/{FLOW}, or None when it cannot be read. A None
    is treated as CHANGED by every caller (C7): never a reason to skip."""
    try:
        st, payload = _request(sess, f"{BASE}/catalog/{flow}", clock)
    except TransientError:
        return None
    if st != "ok":
        return None
    return _find_key(payload, "modified")


def _find_key(obj, key, depth=0):
    if depth > 4:
        return None
    if isinstance(obj, dict):
        v = obj.get(key)
        if isinstance(v, str) and v:
            return v
        for x in obj.values():
            r = _find_key(x, key, depth + 1)
            if r:
                return r
    elif isinstance(obj, list):
        for x in obj[:3]:
            r = _find_key(x, key, depth + 1)
            if r:
                return r
    return None


# --------------------------------------------------------------------------------------------- #
# classification (C1, R2-1, R2-2)
# --------------------------------------------------------------------------------------------- #
PROFILE_MEMORY_LIMIT = "2GB"     # DuckDB limit for the classification profile and finalize's queries (F6)


def _sq(path: str) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def _duck():
    """A DuckDB connection with a memory limit and a spill directory private to this call (the bounded
    merge's rules, merge.py: R228 private temp_directory, a spill cap). Returns (con, spill_dir). The
    caller closes the connection and removes the spill dir. AR-331 F6: the arrow profile peaked at 7.3 GB
    on DS_FLORES_A5 and 16.5 GB at 25.9M rows; this one spills to disk instead."""
    import duckdb                                                        # noqa: PLC0415
    lim = os.environ.get("AQUEDUCT_INSEE_DUCKDB_MEMORY") or PROFILE_MEMORY_LIMIT
    if not all(ch.isalnum() or ch == "." for ch in str(lim)):
        raise ValueError(f"bad DuckDB memory_limit {lim!r}")
    spill = merge._bounded_spill_dir()
    con = duckdb.connect()
    try:
        con.execute(f"SET memory_limit='{lim}'")
        con.execute(f"PRAGMA threads={merge.BOUNDED_THREADS}")
        con.execute("SET preserve_insertion_order=false")
        con.execute(f"SET temp_directory='{_sq(spill)}'")
        con.execute(f"SET max_temp_directory_size='{merge._bounded_temp_cap(spill, None) // 1_000_000}MB'")
    except Exception:
        con.close()
        shutil.rmtree(spill, ignore_errors=True)
        raise
    return con, spill


# The normalised key (R2-1): FREQ removed (it sorts first, so no leading ':' is left) and the GEO vintage
# dropped. The partition: the dimension-name set plus the GEO vintage.
_SQL_PART = ("regexp_replace(series_key, '=[^:]*', '', 'g') || '|' || "
             "coalesce(regexp_extract(series_key, 'GEO=(\\d{4})-', 1), '')")
_SQL_NORM = ("regexp_replace(regexp_replace(regexp_replace(series_key, '(^|:)FREQ=[^:]*', '', 'g'), "
             "'^:', ''), 'GEO=\\d{4}-', 'GEO=', 'g')")


def _partition_profile(path) -> dict | None:
    """Rows, max obs_date, rows in the max year, per-partition counts (dimension-name set + GEO vintage) and
    the DISTINCT normalised (key without FREQ and GEO vintage, date) count of the stored file. None when
    absent. Classification only - never to decide a walk. DuckDB under PROFILE_MEMORY_LIMIT, spilling to a
    private directory: memory no longer grows with the flow (AR-331 F6)."""
    lc = blob.local_copy(path)
    if lc is None:
        return None
    local, temp = lc
    con = spill = None
    try:
        con, spill = _duck()
        src = f"read_parquet('{_sq(local)}')"
        rows, mx = con.execute(f"SELECT count(*), max(obs_date) FROM {src}").fetchone()
        maxyear = 0
        if mx is not None:
            maxyear = con.execute(f"SELECT count(*) FROM {src} WHERE year(obs_date) = {int(mx.year)}").fetchone()[0]
        vc = {str(p): int(n) for p, n in con.execute(f"SELECT {_SQL_PART} AS p, count(*) FROM {src} GROUP BY 1").fetchall()}
        s_norm = con.execute(f"SELECT count(*) FROM (SELECT DISTINCT {_SQL_NORM} AS k, obs_date FROM {src})").fetchone()[0]
    finally:
        if con is not None:
            con.close()
        if spill:
            shutil.rmtree(spill, ignore_errors=True)
        if temp:
            try:
                os.remove(local)
            except OSError:
                pass
    return {"rows": int(rows), "partitions": vc, "s_norm": int(s_norm),
            "max_obs": mx.isoformat() if mx else None, "maxyear_rows": int(maxyear)}


def _rule_hits(code, prof, force, fe=None) -> str | None:
    """The rule that sends a flow to needs_origin at classification, or None.

    Operator force and "zero rows or no stored object" (C1(b)) apply to EVERY flow. The exact-multiple rules
    detect truncation by the OLD fetcher, so they do not apply to a flow this fetcher has already pulled from
    origin (`last_full_utc`): that pull ended on isLast, and its residue (old-spelling rows INSEE no longer
    serves, often exactly one page) would otherwise send it back to a full re-pull on every re-classification
    (review F3). AR-331 X3: the exemption sits BELOW the zero-row/absent rule."""
    if code in force:
        return "operator force_origin"
    if prof is None or prof["rows"] == 0:
        return "zero rows or no stored object"
    if (fe or {}).get("last_full_utc"):
        return None
    if code in CI_PAGE_MULTIPLE_DELTAS:
        return "a CI-logged delta was an exact multiple of 10,000"
    if prof["rows"] % PAGE_SIZE == 0:
        return f"total {prof['rows']:,} is an exact multiple of {PAGE_SIZE:,}"
    hits = [f"{p} = {n:,}" for p, n in prof["partitions"].items() if n > 0 and n % PAGE_SIZE == 0]
    if hits:
        return "a key-spelling partition is an exact multiple of 10,000: " + "; ".join(hits[:3])
    return None

def probe_page(s_norm: int) -> int:
    """R2-1: the page past what we hold, from the DISTINCT normalised count (old spellings and duplicates
    inflate the stored total: DS_FLORES_A5 holds 9,532,850 rows over 4,766,824 normalised pairs)."""
    return s_norm // PAGE_SIZE + 2


MAX_FLOW_FAILURES = 3


def _maxyear_rows(path, max_obs) -> int:
    """Rows of the merged file in its max year, read from the LOCAL file the merge just wrote (the store
    itself after T0, the runner's scratch mirror before). 0 when there is no local copy: an unknown size
    only means a large tail is discovered in the walk and parked there (never a second in-progress)."""
    if not max_obs or not os.path.exists(path):
        return 0
    try:
        od = pq.read_table(path, columns=["obs_date"]).column("obs_date")
        return int(pc.sum(pc.equal(pc.year(od), int(str(max_obs)[:4]))).as_py() or 0)
    except Exception:                                                # noqa: BLE001 - an estimate only
        return 0


def _fatal(e) -> bool:
    """Never swallow the orchestrator's hard limit or an interrupt. _giant's test (AR-331 R2-B): the
    UnitTimeout class, the orchestrator's UNIT_TIMEOUT_FIRED flag (DuckDB hands the alarm back as
    RuntimeError('Query interrupted') and merge.py re-raises it as such), or a native interruption."""
    return isinstance(e, (KeyboardInterrupt, SystemExit)) or _giant._is_unit_timeout(e)


# --------------------------------------------------------------------------------------------- #
# the pass
# --------------------------------------------------------------------------------------------- #
class _Pass:
    def __init__(self, sess, clock, state, force):
        self.sess, self.clock, self.st, self.force = sess, clock, state, force
        self.out_dir = S.source_dir()
        self.labels: list[str] = list(state.notes)
        self.changed: dict[str, str] = {}
        self.merged_rows = 0
        self.classified = 0
        self.walk = {"visited": 0, "merged": 0, "unchanged": 0, "deferred": []}
        self.tally = Tally()
        self.repair_flow = None
        self.repair_pages = 0
        self.conflict = False
        self.me = S.owner_id()
        self.force_raw = {}

    # ---------------------------------------------------------------- persistence
    def save(self):
        try:
            self.st.save()
        except S.StateConflict as e:
            self.conflict = True
            self.labels.append(f"STATE CONFLICT: {e} - repair work stopped this pass")
            raise

    def path(self, code):
        return os.path.join(self.out_dir, f"{code}.parquet")

    def flow_failed(self, code, where, e):
        """F1: a per-flow fault stays per-flow. Counted; after MAX_FLOW_FAILURES the flow is BROKEN - loud
        every pass and worked by nobody until the operator forces it (runbook). The counter resets on every
        committed chunk (AR-331 F5), so only failures without progress between them add up. A flow that goes
        BROKEN drops its attempt, state first and chunks second (AR-331 condition 5)."""
        fe = self.st.flow(code)
        if fe.get("status") == S.MERGE_PENDING:
            # AR-331 R2-A: every page of this flow is staged and checked. No fault after that point may
            # make it broken (broken deletes the chunks): it stays merge_pending, loud and escalating.
            self._merge_refused(code, where, e)
            return
        n = int(fe.get("failures", 0)) + 1
        fe["failures"] = n
        msg = f"{code}: {where} failed ({n}/{MAX_FLOW_FAILURES}) - {type(e).__name__}: {str(e)[:120]}"
        self.labels.append(f"FLOW FAILURE {msg}")
        self._book_transient(msg)
        try:
            if n >= MAX_FLOW_FAILURES:
                fe.update(status=S.BROKEN, why=f"{where} failed {n} times: {type(e).__name__}: {str(e)[:120]}")
                if not self._drop_attempt(code, "the flow is broken"):
                    self.save()
            else:
                self.save()
        except S.StateConflict:
            pass

    def _merge_refused(self, code, where, e):
        """A merge_pending flow whose merge (or finalize) failed: it KEEPS merge_pending and its chunks, and the
        note escalates with the count of passes refused. Only the operator's force (or a merge that later
        succeeds) ends it."""
        fe = self.st.flow(code)
        fe["status"] = S.MERGE_PENDING
        n = int(fe.get("merge_refusals", 0)) + 1
        fe["merge_refusals"] = n
        at = fe.get("attempt") or {}
        msg = (f"{code}: MERGE REFUSED {n} pass(es) - {where}: {type(e).__name__}: {str(e)[:120]}; kept "
               f"merge_pending with {int(at.get('chunks', 0))} staged chunk(s), {int(at.get('rows_staged', 0)):,} rows")
        if n >= MAX_FLOW_FAILURES:
            msg += " - NEEDS THE OWNER (the cause repeats every pass)"
        self.labels.append(msg)
        self._book_transient(msg)
        try:
            self.save()
        except S.StateConflict:
            pass

    def _drop_attempt(self, code, why) -> bool:
        """Abandon the flow's attempt: the state without it is saved FIRST, its chunks are deleted second (a
        kill between the two leaves only chunks no state names, swept by the next _sweep). False when there
        was no attempt."""
        fe = self.st.flow(code)
        at = fe.pop("attempt", None)
        if not at:
            return False
        self.save()
        if at.get("id"):
            self._sweep(code, keep=None)
            self.labels.append(f"{code}: attempt {at['id']} ({at.get('mode')}, {int(at.get('pages', 0))} pages "
                               f"staged) dropped - {why}")
        return True

    # ---------------------------------------------------------------- 0. operator force (C9)
    def apply_force(self, codes):
        """_force_origin.json acts on a flow in ANY status (AR-331 X1, X2), PER FLOW and ONCE (R2-C): the
        flow records the key S.force_entries gives it as `forced_key`, and the same key never forces it
        again. List form: forced once while it stays listed; the key is cleared when the flow leaves the list
        (only after a successful read of the file - an unreadable file clears nothing), so listing it again
        later forces it again. Object form: a new value for a flow forces that flow again. A forced flow drops
        its attempt unless that attempt already IS an origin pull, and leaves broken / unstable / withdrawn.
        An unclassified flow is forced by classification (it still needs its profile) and records the key
        there."""
        raw = self.force_raw or {}
        entries = S.force_entries(raw)
        self.force_keys = entries
        live = set(codes)
        active, changed = set(), False
        if not raw.get("_unreadable"):
            for code, fe in self.st.flows.items():
                if fe.get("forced_key") and code not in entries:
                    fe.pop("forced_key", None)                        # off the list: a later listing forces
                    changed = True
        for code, key in sorted(entries.items()):
            if not S.FLOW_RE.fullmatch(code) or code not in live:
                self.labels.append(f"{S.FORCE_FILE}: {code[:40]!r} is not a catalogued flow - ignored")
                continue
            fe = self.st.flow(code)
            if fe.get("forced_key") == key:
                continue                                              # this entry was used already
            at = fe.get("attempt") or {}
            status = fe.get("status")
            if status in (S.IN_PROGRESS, S.MERGE_PENDING) and at.get("mode") == "origin":
                fe["forced_key"] = key                                # already an origin pull
                changed = True
                continue
            if at.get("id") and not self._lease_ok(code):
                continue                                              # another writer's live attempt: later
            active.add(code)
            if status == S.UNCLASSIFIED:
                continue
            fe["forced_key"] = key
            self._drop_attempt(code, "operator force_origin")
            for k in ("failures", "probe", "merge_refusals"):
                fe.pop(k, None)
            fe.update(status=S.NEEDS_ORIGIN, why="operator force_origin", repair=True)
            self.labels.append(f"{code}: operator force_origin (was {status}) - an origin pull is queued")
            changed = True
        self.force = active
        if changed:
            self.save()

    # ---------------------------------------------------------------- 1. identity
    def sweep_done(self):
        """k4: a kill after a flow was marked complete but before its chunks were deleted leaves the
        attempt flagged done. Delete those chunks first thing (strict keys only, C10)."""
        swept = False
        for code, fe in self.st.flows.items():
            at = fe.get("attempt") or {}
            if at.get("done"):
                self._sweep(code, keep=None)
                fe.pop("attempt", None)
                swept = True
            elif fe.get("status") == S.IN_PROGRESS and at.get("walk"):
                # a walk tail a kill left in progress is PARKED, so only the repair slot's flow is in progress
                fe["status"] = S.NEEDS_REFRESH
                swept = True
        if swept:
            self.save()

    def check_identity(self, codes):
        """C8 + R2-4b: one HEAD per flow. A mismatch whose footer row count still equals the recorded rows
        (an outside rewrite of the same rows, or T0 changing the identity KIND from etag to size/mtime_ns) is
        RE-BASED, not re-classified (review F3); a real change re-classifies the flow and DROPS any attempt it
        carries (AR-331 X4: a parked tail must never finish a flow that classification sends to origin). A
        served object that disappeared is loud."""
        rebased = 0
        for code in codes:
            fe = self.st.flow(code)
            if fe.get("status") in (S.UNCLASSIFIED, S.IN_PROGRESS, S.MERGE_PENDING):
                continue
            served = fe.get("served") or {}
            cur = S.identity(self.path(code))
            if cur is None:
                if served.get("rows"):
                    fe["status"], fe["why"] = S.UNCLASSIFIED, "served object disappeared"
                    self.labels.append(f"SERVED OBJECT MISSING: {code} ({int(served['rows']):,} rows recorded) "
                                       f"- re-classified")
                    self._drop_attempt(code, "the served object disappeared")
                continue
            if S.same_identity(cur, served.get("identity")):
                continue
            rows = S.footer_rows(self.path(code))
            if rows is not None and rows == served.get("rows"):
                served["identity"] = cur
                fe["served"] = served
                rebased += 1
            else:
                fe["status"], fe["why"] = S.UNCLASSIFIED, (f"served object changed outside this fetcher "
                                                          f"({served.get('rows')} -> {rows} rows)")
                self.labels.append(f"{code}: served object changed outside this fetcher "
                                   f"({served.get('rows')} -> {rows} rows) - re-classified")
                self._drop_attempt(code, "the served object changed outside this fetcher")
        if rebased:
            print(f"[insee_melodi] {rebased} served identity(ies) re-based (rows unchanged)", flush=True)

    # ---------------------------------------------------------------- 2. classify
    def classify(self, codes):
        end = self.clock.at(CLASSIFY_UNTIL_MIN)
        todo = [c for c in codes if self.st.flow(c).get("status") == S.UNCLASSIFIED]
        todo.sort(key=lambda c: (c not in self.force and c not in CI_PAGE_MULTIPLE_DELTAS, c))
        legacy = []
        for code in todo:
            if self.clock.now() >= end:
                break
            fe = self.st.flow(code)
            try:
                prof = _partition_profile(self.path(code))
                ident = S.identity(self.path(code))
            except Exception as e:                                        # noqa: BLE001 - F1, per flow
                if _fatal(e):
                    raise
                self.flow_failed(code, "classification read", e)
                continue
            if fe.get("attempt"):
                self._drop_attempt(code, "re-classified")                 # X4: no attempt outlives a verdict
            fe["served"] = {"identity": ident, "rows": (prof or {}).get("rows", 0),
                            "max_obs": (prof or {}).get("max_obs"),
                            "maxyear_rows": (prof or {}).get("maxyear_rows", 0)}
            if prof:
                fe["partitions"], fe["s_norm"] = prof["partitions"], prof["s_norm"]
            else:
                fe.pop("partitions", None)                                # X3: never probe from a stale count
                fe.pop("s_norm", None)
            why = _rule_hits(code, prof, self.force, fe)
            if why:
                fe.update(status=S.NEEDS_ORIGIN, why=why, repair=True)
                if code in self.force:
                    fe["forced_key"] = (getattr(self, "force_keys", None) or {}).get(code)
                    fe.pop("failures", None)
            else:
                fe.update(status=S.LEGACY, why="no exact-multiple partition; probe pending")
                fe.pop("probe", None)
                legacy.append(code)
            self.classified += 1
        # A legacy flow whose probe was voided (or never ran) is probed again: unprobed is not a resting state.
        for code in codes:
            fe = self.st.flows.get(code) or {}
            if fe.get("status") == S.LEGACY and code not in legacy and \
                    (not fe.get("probe") or fe["probe"].get("void")):
                legacy.append(code)
        if legacy or self._determinism_needed():
            self.controls_and_probes(legacy, end)
        self.save()

    def _determinism_needed(self):
        return "determinism" not in self.st.data or self._unblocked()

    def _unblocked(self):
        blk = self.st.data.get("repair_blocked")
        after = (self.force_raw or {}).get("unblock_repair_after_utc")
        return bool(blk and after and str(after) > str(blk.get("utc")))

    def _control_flow(self):
        """F7: the positive control is the LARGEST classified flow holding at least two pages - its page 2
        must answer non-empty, and its page 1 twice is the page-determinism control. It does not depend on any
        flow still being truncated, so the controls stay runnable after the repair is done."""
        best = None
        for code, fe in self.st.flows.items():
            rows = int((fe.get("served") or {}).get("rows") or 0)
            if rows >= 2 * PAGE_SIZE and fe.get("status") not in (S.BROKEN, S.WITHDRAWN, S.UNSTABLE) \
                    and (best is None or rows > best[0]):
                best = (rows, code)
        return best[1] if best else None

    def controls_and_probes(self, legacy, end):
        """C1 probe controls, R2-6 page-determinism control, then the page-beyond probes - all charged to the
        classification slot (R2-7). Every path that cannot probe says so in the note (review F7)."""
        ctl = self._control_flow()
        if self._determinism_needed():
            if not ctl:
                if self._unblocked():
                    self.st.data.pop("repair_blocked", None)
                    self.labels.append("repair unblocked by the owner; the determinism control has no flow to run on")
                else:
                    self.labels.append("page-determinism control not run: no classified flow of 2+ pages yet")
            elif self.clock.now() < end:
                try:
                    a = _fetch_page(self.sess, ctl, None, 1, self.clock)
                    time.sleep(PAGE_DELAY)
                    b = _fetch_page(self.sess, ctl, None, 1, self.clock)
                except (_Stop, TransientError) as e:
                    self.labels.append(f"page-determinism control not run ({type(e).__name__}): {str(e)[:80]}")
                else:
                    same = (a.fp == b.fp and a.raw_n == b.raw_n)
                    self.st.data["determinism"] = {"utc": S.utcnow(), "flow": ctl, "same": same,
                                                   "raw": [a.raw_n, b.raw_n]}
                    print(f"[insee_melodi] page-determinism control {ctl} page 1 twice: "
                          f"{'IDENTICAL' if same else 'DIFFERENT'} ({a.raw_n}/{b.raw_n} obs)", flush=True)
                    if same:
                        self.st.data.pop("repair_blocked", None)
                    else:
                        self.st.data["repair_blocked"] = {"utc": S.utcnow(),
                                                          "why": "Melodi paging not deterministic",
                                                          "evidence": [a.fp[:16], b.fp[:16]]}
        if not legacy:
            return
        if self.clock.now() + 60 >= end:
            self.labels.append(f"probes not run this pass: classification slot spent - {len(legacy)} legacy "
                               f"flow(s) stay unprobed")
            return
        if not ctl:
            why = "no classified flow of 2+ pages to serve as the positive control"
            self.labels.append(f"PROBE VOID: {why} - {len(legacy)} flow(s) stay legacy-unprobed")
            for code in legacy:
                self.st.flow(code)["probe"] = {"void": why, "utc": S.utcnow()}
            return
        ctl_ok, why = self._probe_controls(ctl)
        if not ctl_ok:
            self.labels.append(f"PROBE VOID: {why} - {len(legacy)} flow(s) stay legacy-unprobed")
            for code in legacy:
                self.st.flow(code)["probe"] = {"void": why, "utc": S.utcnow()}
            return
        for code in legacy:
            if self.clock.now() >= end:
                break
            fe = self.st.flow(code)
            pg = probe_page(int(fe.get("s_norm") or fe["served"]["rows"]))
            try:
                p = _fetch_page(self.sess, code, None, pg, self.clock)
            except (_Stop, TransientError):
                break
            fe["probe"] = {"page": pg, "raw": p.raw_n, "http": p.http, "utc": S.utcnow()}
            if p.raw_n > 0:
                fe.update(status=S.NEEDS_ORIGIN, why=f"probe page {pg} returned {p.raw_n} obs", repair=True)
            else:
                fe["why"] = f"legacy - probe page {pg} empty (NOT proof of completeness)"
            time.sleep(RATE)

    def _probe_controls(self, ctl):
        try:
            pos = _fetch_page(self.sess, ctl, None, 2, self.clock)
            time.sleep(RATE)
            fe = self.st.flows.get(PROBE_EMPTY_CONTROL) or {}
            s = int(fe.get("s_norm") or (fe.get("served") or {}).get("rows") or 0)
            if not s:
                return False, f"empty control {PROBE_EMPTY_CONTROL} not classified yet"
            neg = _fetch_page(self.sess, PROBE_EMPTY_CONTROL, None, probe_page(s), self.clock)
            time.sleep(RATE)
        except (_Stop, TransientError) as e:
            return False, f"control request failed ({type(e).__name__})"
        if pos.raw_n == 0:
            return False, f"positive control {ctl} page 2 came back empty"
        if neg.raw_n > 0:
            return False, f"negative control {PROBE_EMPTY_CONTROL} page past its rows returned {neg.raw_n} obs"
        return True, ""

    # ---------------------------------------------------------------- 3. walk
    def _tail_pages_estimate(self, fe) -> int:
        """Pages a tail will cost, from state only (C8): the whole flow when INSEE ignores startPeriod or
        every stored row sits in the max year (a single-year flow's tail IS the flow); else the max year."""
        served = fe.get("served") or {}
        rows = int(served.get("rows") or 0)
        my = int(served.get("maxyear_rows") or 0)
        whole = fe.get("start_ignored") or (rows and my >= rows)
        return -(-(rows if whole else my) // PAGE_SIZE)

    def walk_slot(self, codes):
        # The walk's slot ends at minute 15 while repair work exists (C7: freshness keeps a reserved
        # slot); with nothing to repair it may use the pass up to the soft stop.
        repairing = any(fe.get("status") in S.REPAIR_PENDING for fe in self.st.flows.values())
        end = self.clock.at(WALK_UNTIL_MIN) if repairing else self.clock.soft_end
        eligible = [c for c in codes if self.st.flow(c).get("status") in S.WALKABLE]
        order = rotate_after(eligible, load_rotation(self.out_dir))
        for i, code in enumerate(order):
            if self.clock.now() >= end:
                self.walk["deferred"] = order[i:]
                for c in order[i:]:
                    self.tally.deferred_unit(c)
                break
            fe = self.st.flow(code)
            save_rotation(self.out_dir, code)
            try:
                mod = _catalog_modified(self.sess, code, self.clock)
            except _Stop:
                break
            time.sleep(RATE)
            last = fe.get("last_tail_utc") or fe.get("last_full_utc")
            stale = (not last) or (dt.datetime.now(dt.timezone.utc)
                                   - dt.datetime.fromisoformat(last)).days >= BACKSTOP_DAYS
            if mod is not None and mod == fe.get("modified") and not stale:
                self.walk["unchanged"] += 1
                continue
            self.walk["visited"] += 1
            if self._tail_pages_estimate(fe) > CHUNK_PAGES:
                # F4/C5: a pull larger than one chunk never starts in the walk; it queues behind the repair
                fe.update(status=S.NEEDS_REFRESH, why="due; its tail is larger than one chunk",
                          refresh_modified=mod)
                self.save()
                continue
            try:
                res = self.work(code, end, in_walk=True, modified=mod)
            except Exception as e:                                        # noqa: BLE001 - F1, per flow
                if _fatal(e):
                    raise
                self.flow_failed(code, "walk tail", e)
                continue
            if res == "complete" and code in self.changed:
                self.walk["merged"] += 1
            if res in ("stop", "conflict"):
                break

    # ---------------------------------------------------------------- 4. repair
    def repair_slot(self):
        blk = self.st.data.get("repair_blocked")
        if blk and not self._unblocked():
            self.labels.append("REPAIR BLOCKED: Melodi paging not deterministic "
                               f"(control {self.st.data.get('determinism', {}).get('flow')} at {blk.get('utc')}) "
                               "- the decision is the owner's")
            return
        tried = set()
        while self.clock.now() < self.clock.soft_end and not self.conflict:
            code = self._next_repair(tried)
            if code is None:
                return
            tried.add(code)
            self.repair_flow = code
            try:
                res = self.work(code, self.clock.soft_end, in_walk=False, modified=None)
            except Exception as e:                                        # noqa: BLE001 - F1, per flow
                if _fatal(e):
                    raise
                self.flow_failed(code, "repair pull", e)
                continue                                                  # never blocks the queue head
            if res in ("progress", "stop", "conflict"):
                return

    def _next_repair(self, tried=()):
        """MERGE_PENDING first, then the ONE in-progress flow (resumed), and only when nothing is in
        progress a new attempt: needs_origin before needs_refresh (F4: one flow in progress at a time)."""
        fl = self.st.flows
        for c, fe in sorted(fl.items(), key=lambda kv: (kv[1].get("attempt") or {}).get("started_utc") or ""):
            if c not in tried and fe.get("status") == S.MERGE_PENDING and self._lease_ok(c):
                return c
        inprog = [c for c, fe in fl.items() if fe.get("status") == S.IN_PROGRESS]
        for c in sorted(inprog):
            if c not in tried and self._lease_ok(c):
                return c
        if inprog:
            return None
        for status in (S.NEEDS_ORIGIN, S.NEEDS_REFRESH):
            for c in sorted(c for c, fe in fl.items() if fe.get("status") == status):
                if c not in tried and self._lease_ok(c):
                    return c
        return None

    def _lease_ok(self, code):
        at = self.st.flow(code).get("attempt") or {}
        if not at.get("owner") or at.get("owner") == self.me:
            return True
        try:
            age = (dt.datetime.now(dt.timezone.utc) - dt.datetime.fromisoformat(at.get("heartbeat_utc"))).total_seconds()
        except (TypeError, ValueError):
            return True
        if age < LEASE_STALE_MIN * 60:
            self.labels.append(f"{code}: attempt leased by {at.get('owner')} (heartbeat {int(age // 60)} min ago) - left alone")
            return False
        return True

    # ---------------------------------------------------------------- the pull engine
    def _new_attempt(self, code, mode, modified):
        fe = self.st.flow(code)
        served = fe.get("served") or {}
        start = None
        single_year = served.get("rows") and int(served.get("maxyear_rows") or 0) >= int(served.get("rows") or 0)
        if mode == "tail" and not fe.get("start_ignored") and served.get("max_obs") and not single_year:
            start = int(served["max_obs"][:4])
        at = {"id": S.new_attempt_id(), "owner": self.me, "heartbeat_utc": S.utcnow(),
              "started_utc": S.utcnow(), "mode": "origin" if start is None else "tail", "start": start,
              "next_page": 1, "chunks": 0, "rows_staged": 0, "raw_obs": 0, "pages": 0, "page_fp": None,
              "modified_at_start": modified, "passes": 0, "passes_without_progress": 0,
              "within_dups": 0, "restarts": (fe.get("attempt") or {}).get("restarts", 0)}
        fe["attempt"] = at
        return at

    def _restart(self, code, why):
        """Save FIRST, sweep second (review F5): a restart that loses the CAS race must not have deleted the
        winning state's chunks."""
        fe = self.st.flow(code)
        old = fe.get("attempt") or {}
        restarts = int(old.get("restarts", 0)) + 1
        self.tally.transient_unit(f"{code}: attempt restarted - {why}")
        self.labels.append(f"{code}: attempt restarted ({restarts}/{MAX_RESTARTS}) - {why}")
        if restarts >= MAX_RESTARTS:
            fe.update(status=S.UNSTABLE, why=f"{MAX_RESTARTS} restarts: {why}")
            fe.pop("attempt", None)
            self.labels.append(f"{code}: UNSTABLE PAGING after {MAX_RESTARTS} restarts - left for the owner")
        else:
            fe["status"] = S.NEEDS_ORIGIN if fe.get("repair") else S.NEEDS_REFRESH
            fe["attempt"] = {"restarts": restarts}
        self.save()
        self._sweep(code, keep=None)
        return "restart"

    def work(self, code, slot_end, *, in_walk, modified):
        """Advance one flow's pull until it completes, the slot ends, or a per-flow failure.
        Returns complete | progress | queued | transient | restart | stop | conflict | withdrawn | empty | broken."""
        fe = self.st.flow(code)
        at = fe.get("attempt") or {}
        try:
            if fe.get("status") == S.MERGE_PENDING:
                return self._finalize(code)
            if at.get("id") and at.get("mode") != "origin" and \
                    S.NEEDS_ORIGIN in (fe.get("status"), at.get("prior_status")):
                # AR-331 X4: a TAIL attempt never satisfies a needs_origin verdict. Dropped (not a restart:
                # it does not count toward unstable paging), and an origin attempt starts in its place.
                self._drop_attempt(code, "a tail attempt cannot satisfy needs_origin")
                fe["status"] = S.NEEDS_ORIGIN
                at = {}
            if not at.get("id"):
                if modified is None:
                    modified = fe.pop("refresh_modified", None) if not in_walk else None
                if modified is None and not in_walk:
                    modified = _catalog_modified(self.sess, code, self.clock)
                    if modified is None:
                        # C4/F6: an attempt whose `modified` is unknown can never prove its earlier pages
                        # are still current - do not start one.
                        self._book_transient(f"{code}: catalog `modified` unreadable - attempt not started")
                        return "stop"
                mode = "origin" if fe.get("status") == S.NEEDS_ORIGIN else "tail"
                prior = fe.get("status")
                at = self._new_attempt(code, mode, modified)
                at["prior_status"] = S.LEGACY if prior == S.NEEDS_REFRESH and not fe.get("last_full_utc") \
                    else (S.COMPLETE if prior == S.NEEDS_REFRESH else prior)
                at["walk"] = bool(in_walk)
                fe["status"] = S.IN_PROGRESS
                self.save()                                    # F5: the lease is on the store before any work
            else:
                at["owner"], at["heartbeat_utc"] = self.me, S.utcnow()
                at["walk"] = False
                fe["status"] = S.IN_PROGRESS
                self.save()                                    # F5: persist the lease BEFORE the resume check
                try:
                    ok = self._resume_check(code, at)
                except _ResumeDeferred as e:
                    # Could not read the page to compare: no progress this pass, counted (stall label at 4).
                    self._book_transient(f"{code}: resume check not read ({str(e)[:80]})")
                    self._progress(code, at, 0)
                    return "stop"
                if ok is not True:
                    return self._restart(code, ok)
            at["passes"] = int(at.get("passes", 0)) + 1
            return self._pages(code, fe, at, slot_end, in_walk)
        except S.StateConflict:
            return "conflict"

    def _resume_check(self, code, at):
        """C4: the whole last committed page must re-fetch identically, and `modified` must not have moved.
        An unreadable catalog defers (no progress, counted); an attempt started without a known `modified`
        restarts - nothing can show its earlier pages are still INSEE's (review F6)."""
        if int(at.get("next_page", 1)) <= 1:
            return True
        if at.get("modified_at_start") is None:
            return "the attempt began without a readable `modified`, so its staged pages cannot be shown current"
        try:
            p = _fetch_page(self.sess, code, at.get("start"), int(at["next_page"]) - 1, self.clock)
        except (_Stop, TransientError) as e:
            raise _ResumeDeferred(str(e)) from e
        time.sleep(PAGE_DELAY)
        if p.fp != at.get("page_fp"):
            return f"page {int(at['next_page']) - 1} changed since it was staged (paging not stable)"
        try:
            mod = _catalog_modified(self.sess, code, self.clock)
        except _Stop as e:
            raise _ResumeDeferred("no room for the catalog read") from e
        if mod is None:
            raise _ResumeDeferred("catalog `modified` unreadable")
        if mod != at["modified_at_start"]:
            return f"INSEE `modified` moved {at['modified_at_start']} -> {mod} during the attempt"
        return True

    def _pages(self, code, fe, at, slot_end, in_walk):
        """Fetch pages from the attempt's cursor. The CURSOR ADVANCES ONLY WHEN A CHUNK IS UPLOADED: pages
        are held in a local .chunkpart and counted in `pend`, and flush() uploads the chunk object, THEN
        moves next_page/rows_staged/page_fp and saves _flows.json. A kill anywhere loses at most the
        pages since the last flush, and never records a page that no chunk holds."""
        sdir = S.staging_dir()
        part = os.path.join(sdir, f"{code}.{at['id']}.open.chunkpart")
        writer = None
        pend = {"pages": 0, "rows": 0, "raw": 0, "dups": 0, "fp": None, "next": int(at["next_page"])}
        added_pages = 0

        def flush():
            nonlocal writer, added_pages
            if writer is None:
                return
            writer.close()
            writer = None
            with open(part, "rb") as fh:
                body = fh.read()
            blob.write_bytes_atomic(S.chunk_path(code, at["id"], int(at["chunks"])), body)
            os.remove(part)
            at["chunks"] = int(at["chunks"]) + 1
            at["next_page"] = pend["next"]
            at["rows_staged"] = int(at["rows_staged"]) + pend["rows"]
            at["raw_obs"] = int(at.get("raw_obs", 0)) + pend["raw"]
            at["pages"] = int(at["pages"]) + pend["pages"]
            at["within_dups"] = int(at["within_dups"]) + pend["dups"]
            at["page_fp"] = pend["fp"]
            added_pages += pend["pages"]
            pend.update(pages=0, rows=0, raw=0, dups=0)
            at["heartbeat_utc"] = S.utcnow()
            fe.pop("failures", None)                    # AR-331 F5: committed progress resets the counter
            self.save()

        def bail(kind, why=None):
            flush()
            if why:
                self._book_transient(why)
            self._progress(code, at, added_pages)
            return kind

        try:
            while True:
                page = pend["next"]
                if self.clock.now() >= slot_end or self.clock.now() >= self.clock.soft_end:
                    return bail("progress")
                if page > PAGE_CEILING:
                    return bail("transient", f"{code}: PAGE_CEILING {PAGE_CEILING} reached without isLast")
                try:
                    p = _fetch_page(self.sess, code, at.get("start"), page, self.clock)
                except _Stop:
                    return bail("stop")
                except TransientError as e:
                    return bail("transient", f"{code}: page {page} - {str(e)[:120]}")
                print(f"[insee_melodi] {code} page {page}: {p.raw_n} obs, paging {p.paging}"
                      + (f", HTTP {p.http}" if p.nodata else ""), flush=True)
                # ---- what this page means (D): nothing but isLast ends a flow ----
                if at.get("confirm_page") == page:
                    if p.nodata or p.raw_n == 0:
                        at.pop("confirm_page", None)
                        flush()
                        fe["status"] = S.MERGE_PENDING
                        self.save()
                        return self._finalize(code)
                    at.pop("confirm_page", None)
                    return bail("transient", f"{code}: isLast on page {page - 1} contradicted by {p.raw_n} obs "
                                             f"on page {page}")
                if page == 1 and (p.nodata or p.raw_n == 0):
                    return self._empty_first_page(code, fe, at, p)
                if p.nodata or (p.raw_n == 0 and not p.is_last):
                    return bail("transient", f"{code}: page {page} "
                                f"{'HTTP ' + str(p.http) if p.nodata else 'empty without isLast'} after "
                                f"{page - 1} full page(s) - never read as the end")
                if 0 < p.raw_n < PAGE_SIZE and not p.is_last:
                    return bail("transient", f"{code}: page {page} short ({p.raw_n} obs) without isLast")
                if at.get("mode") == "tail" and p.min_year is not None and at.get("start") \
                        and p.min_year < at["start"]:
                    fe["start_ignored"] = True                          # the server returned the whole flow
                    at["mode"] = "origin"
                if p.raw_n > 0:
                    if writer is None:
                        writer = pq.ParquetWriter(part, _SCHEMA, compression="zstd")
                    writer.write_table(p.table)
                    pend["pages"] += 1
                    pend["rows"] += p.table.num_rows
                    pend["raw"] += p.raw_n
                    pend["dups"] += p.within_dups
                    pend["fp"] = p.fp
                    if p.table.num_rows == 0:
                        self.labels.append(f"{code}: PARSE LOSS - page {page} carried {p.raw_n} observations "
                                           f"and none parsed (TIME_PERIOD or measures shape?)")
                    if p.within_dups:
                        self.labels.append(f"{code}: PARSE COLLISION - {p.within_dups} duplicate (key, date) "
                                           f"within page {page}; the parse key collapses distinct INSEE "
                                           f"observations (no restart: a re-pull cannot fix it)")
                pend["next"] = page + 1
                if p.is_last:
                    if p.raw_n == PAGE_SIZE:
                        time.sleep(PAGE_DELAY)
                        try:
                            nxt = _fetch_page(self.sess, code, at.get("start"), page + 1, self.clock)
                        except (_Stop, TransientError) as e:
                            # the isLast page is committed; the NEXT pass reads page+1 as the confirmation
                            at["confirm_page"] = page + 1
                            return bail("transient", f"{code}: page+1 confirmation not read ({str(e)[:60]})")
                        if not (nxt.nodata or nxt.raw_n == 0):
                            return bail("transient", f"{code}: isLast on full page {page} contradicted by "
                                                     f"{nxt.raw_n} obs on page {page + 1}")
                    flush()
                    fe["status"] = S.MERGE_PENDING
                    self.save()
                    return self._finalize(code)
                if pend["pages"] >= CHUNK_PAGES:
                    flush()
                    if in_walk:
                        # F4: PARKED, not in progress - the repair slot's flow stays the only one in progress;
                        # this attempt (chunk and cursor kept) resumes when needs_refresh comes up. A walk
                        # tail started without a readable `modified` could never pass the resume check
                        # (AR-331 F8): it is dropped instead, and the repair slot starts it afresh (reading
                        # `modified` first).
                        fe["status"] = S.NEEDS_REFRESH
                        if at.get("modified_at_start") is None:
                            self._drop_attempt(code, "a walk tail with no known `modified` cannot be resumed")
                            self.labels.append(f"{code}: tail outgrew one chunk - queued as needs_refresh")
                        else:
                            self.save()
                            self.labels.append(f"{code}: tail outgrew one chunk - parked as needs_refresh")
                        return "queued"
                time.sleep(PAGE_DELAY)
        finally:
            if writer is not None:
                writer.close()
            if os.path.exists(part):
                try:
                    os.remove(part)                                     # never uploaded = never counted
                except OSError:
                    pass

    def _book_transient(self, label):
        """A per-flow transient with its label (a method, so tests/test_label_names_are_bound.py sees the
        name bound - a nested helper's parameter is invisible to that conservative checker)."""
        self.tally.transient_unit(label)

    def _progress(self, code, at, added):
        self.repair_pages = added
        at["passes_without_progress"] = 0 if added else int(at.get("passes_without_progress", 0)) + 1
        if at["passes_without_progress"] >= STALL_PASSES:
            self.labels.append(f"REPAIR STALLED: {code} no page progress in {at['passes_without_progress']} passes")
        self.save()

    def _empty_first_page(self, code, fe, at, p):
        """Page 1 empty or 400. State first, sweep second (F5)."""
        rows = (fe.get("served") or {}).get("rows") or 0
        fe.pop("attempt", None)
        if at.get("mode") == "tail" and rows:
            # nothing at or after startPeriod: a quiet tail. The flow keeps the status it had.
            fe.update(status=at.get("prior_status") or S.LEGACY, last_tail_utc=S.utcnow(),
                      modified=at.get("modified_at_start") or fe.get("modified"))
            out = "complete"
        elif rows:
            fe.update(status=S.WITHDRAWN, why=f"origin page 1 {'HTTP ' + str(p.http) if p.nodata else 'empty'} "
                                              f"over {rows:,} stored rows - withdrawn by INSEE?")
            self.labels.append(f"{code}: WITHDRAWN? origin page 1 empty over {rows:,} stored rows")
            self._book_transient(f"{code}: origin page 1 empty over {rows:,} stored rows")
            out = "withdrawn"
        else:
            fe.update(status=S.EMPTY, why="INSEE serves no observations", last_full_utc=S.utcnow(),
                      modified=at.get("modified_at_start"))
            out = "empty"
        self.save()
        self._sweep(code, keep=None)
        return out

    # ---------------------------------------------------------------- finalize (C2, C3, C6, C12)
    def _finalize(self, code):
        fe = self.st.flow(code)
        at = fe.get("attempt") or {}
        if not self.clock.fits(S.merge_margin_s(int(at.get("rows_staged", 0)))):
            self.labels.append(f"{code}: merge deferred - {int(self.clock.hard_end - self.clock.now())}s left "
                               f"is under the margin for {at.get('rows_staged', 0):,} rows")
            return "progress"
        t_fin = time.monotonic()
        sdir = S.staging_dir()
        staged = os.path.join(sdir, f"{code}.{at['id']}.staged.chunkpart")
        total = 0
        w = None
        try:
            for n in range(int(at.get("chunks", 0))):
                body = blob.read_bytes(S.chunk_path(code, at["id"], n))
                if body is None:
                    return self._restart(code, f"chunk {n} missing")
                try:
                    t = pq.read_table(pa.BufferReader(body)).cast(_SCHEMA)
                except Exception as e:                                    # noqa: BLE001 - a corrupt chunk
                    if _fatal(e):
                        raise
                    return self._restart(code, f"chunk {n} unreadable ({type(e).__name__})")
                total += t.num_rows
                if w is None:
                    w = pq.ParquetWriter(staged, _SCHEMA, compression="zstd")
                w.write_table(t)
            if w is not None:
                w.close()
                w = None
            if total != int(at.get("rows_staged", 0)):
                return self._restart(code, f"chunk footers sum to {total:,}, cursor says {at.get('rows_staged', 0):,}")
            raw = int(at.get("raw_obs", 0))
            if total == 0 and (raw > 0 or (at.get("mode") == "origin" and (fe.get("served") or {}).get("rows"))):
                # F2: a pull that PARSED nothing is never complete - the old fetcher's structural break, per flow
                fe.pop("attempt", None)
                fe.update(status=S.BROKEN, why=f"{raw:,} observations fetched, none parsed - parser or "
                                               f"publisher shape changed; nothing merged")
                self.labels.append(f"{code}: PARSE LOSS - {raw:,} observations fetched, none parsed - BROKEN")
                self._book_transient(f"{code}: {raw:,} observations fetched, none parsed")
                self.save()
                self._sweep(code, keep=None)
                return "broken"
            stored = blob.local_copy(self.path(code))
            stored_local, stored_tmp = (stored if stored else (None, False))
            con = fspill = None
            try:
                con, fspill = _duck()            # AR-331 F6: memory-limited, private spill
                q = staged.replace("\\", "/").replace("'", "''")
                if total:
                    dup = con.execute(f"SELECT count(*) - count(DISTINCT (series_key, obs_date)) FROM read_parquet('{q}')").fetchone()[0]
                    if dup > int(at.get("within_dups", 0)):
                        return self._restart(code, f"{dup - int(at.get('within_dups', 0)):,} duplicate (key, date) "
                                                   f"ACROSS pages - unstable paging means missing rows too")
                changed, residue = total, {"rows": 0, "by_partition": {}}
                if stored_local and total:
                    s = stored_local.replace("\\", "/").replace("'", "''")
                    changed = con.execute(
                        f"SELECT count(*) FROM read_parquet('{q}') n LEFT JOIN read_parquet('{s}') o "
                        f"ON n.series_key = o.series_key AND n.obs_date = o.obs_date "
                        f"WHERE o.series_key IS NULL OR n.value IS DISTINCT FROM o.value").fetchone()[0]
                    if at.get("mode") == "origin":
                        rows = con.execute(
                            f"SELECT regexp_replace(o.series_key, '=[^:]*', '', 'g') || '|' || "
                            f"coalesce(regexp_extract(o.series_key, 'GEO=(\\d{{4}})-', 1), '') AS p, count(*) "
                            f"FROM read_parquet('{s}') o ANTI JOIN read_parquet('{q}') n "
                            f"ON n.series_key = o.series_key AND n.obs_date = o.obs_date GROUP BY 1").fetchall()
                        residue = {"rows": int(sum(r[1] for r in rows)),
                                   "by_partition": {str(r[0]): int(r[1]) for r in rows}}
                stored_stats = None
                if stored_local:
                    s = stored_local.replace("\\", "/").replace("'", "''")
                    n_s, mx_s = con.execute(f"SELECT count(*), max(obs_date) FROM read_parquet('{s}')").fetchone()
                    stored_stats = (int(n_s), mx_s.isoformat() if mx_s else None)
                con.close()
                # k3 (a kill after the publish, before the state write): the re-finalize finds nothing new,
                # but the served object is no longer the one this finalize started from - so the flow's CSV
                # is still owed, and it goes into changed_keys.
                pre = at.get("pre_merge_identity")
                ident_now = S.identity(self.path(code))
                landed_earlier = bool(pre) and not S.same_identity(pre, ident_now)
                if not changed and landed_earlier and stored_stats:
                    fe["served"] = {"identity": ident_now, "rows": stored_stats[0], "max_obs": stored_stats[1],
                                    "maxyear_rows": (fe.get("served") or {}).get("maxyear_rows", 0)}
                    self.changed[code] = stored_stats[1] or ""
                    fe["csv_owed"] = stored_stats[1] or ""
                    print(f"[insee_melodi] {code}: an earlier pass published this merge before its state "
                          f"write - its CSV is derived now", flush=True)
                if changed:
                    at["pre_merge_identity"] = ident_now
                    self.save()
                    t0 = time.monotonic()
                    try:
                        n_rows, md = merge.merge_and_write_bounded(
                            self.path(code), new_path=staged, dedup_keys=DEDUP,
                            stored_copy=stored_local if stored_local else None)
                    except DefinitiveError as e:
                        # AR-330 C11 / AR-331 R2-A: a refused merge is a condition of the MERGE (spill disk,
                        # spill cap, memory, a shrink or column refusal), never of the paging. The flow stays
                        # merge_pending with every staged chunk, however often it is refused; it is NOT a
                        # flow failure (that would end in broken, and broken deletes chunks).
                        self._merge_refused(code, "merge refused", e)
                        return "transient"
                    print(f"[insee_melodi] {code}: merge took {time.monotonic() - t0:.1f}s for "
                          f"{total:,} staged rows ({changed:,} new or changed)", flush=True)
                    self.changed[code] = md
                    # F1: the published merge's CSV is OWED until a returned Result hands it to the orchestrator
                    fe["csv_owed"] = md
                    self.merged_rows += int(changed)
                    my = _maxyear_rows(self.path(code), md)
                    fe["served"] = {"identity": S.identity(self.path(code)), "rows": int(n_rows), "max_obs": md,
                                    "maxyear_rows": my}
                else:
                    if not fe.get("served"):
                        fe["served"] = {"identity": S.identity(self.path(code)), "rows": 0, "max_obs": None}
            finally:
                if con is not None:
                    con.close()
                if fspill:
                    shutil.rmtree(fspill, ignore_errors=True)
                if stored_tmp and stored_local and os.path.exists(stored_local):
                    os.remove(stored_local)
        finally:
            if w is not None:
                w.close()
            if os.path.exists(staged):
                os.remove(staged)
        mode = at.get("mode")
        # Only an ORIGIN pull (or a flow already complete) earns "complete per INSEE paging": a finished
        # tail of a legacy flow proves its recent years, not its history. A tail whose attempt a needs_origin
        # verdict started keeps that verdict (final = prior), so a needs_origin flow comes to rest only
        # through an ORIGIN finalize (AR-331 X4; pinned by test_X4_layer_finalize_...).
        prior = at.get("prior_status")
        final = S.COMPLETE if (mode == "origin" or prior == S.COMPLETE) else (prior or S.LEGACY)
        fe.update(status=final, basis=mode, modified=at.get("modified_at_start"),
                  pages=int(at.get("pages", 0)),
                  why=(("complete per INSEE paging" if final == S.COMPLETE else f"{final}; last tail complete per INSEE paging")
                       + (f"; {residue['rows']:,} stored rows no longer served by INSEE kept under the merge "
                          f"decision" if residue["rows"] else "")))
        fe["last_full_utc" if mode == "origin" else "last_tail_utc"] = S.utcnow()
        fe.pop("failures", None)
        fe.pop("merge_refusals", None)
        if mode == "origin":
            fe["residue"] = residue
        if at.get("within_dups"):
            fe["parse_collisions"] = int(at["within_dups"])
        unparsed = int(at.get("raw_obs", 0)) - int(at.get("rows_staged", 0))
        fe["unparsed_obs"] = max(unparsed, 0)
        print(f"[insee_melodi] {code}: finalize took {time.monotonic() - t_fin:.1f}s in all "
              f"({total:,} staged rows)", flush=True)
        keep_id = at.get("id")
        fe["attempt"] = {"id": keep_id, "done": True}
        self.save()                                         # k3/k4: state after the publish, before deletes
        self._sweep(code, keep=None)
        fe.pop("attempt", None)
        self.tally.added_unit(int(changed))
        self.save()
        return "complete"

    def _sweep(self, code, keep):
        """Delete this flow's staging objects (C10): every attempt dir except `keep`."""
        base = os.path.join(self.out_dir, S.REPULL_DIR, code)
        n = 0
        for rel in S.list_staging(base):
            parts = rel.split("/")
            if len(parts) != 2 or parts[0] == keep:
                continue
            if S.delete_chunk(os.path.join(base, parts[0], parts[1]), flow=code, current_attempt=keep):
                n += 1
        return n

    # ---------------------------------------------------------------- the note (C13)
    def note(self):
        fl = self.st.flows
        tracked = [c for c, fe in fl.items() if fe.get("repair")]
        done = [c for c in tracked if fl[c].get("status") in (S.COMPLETE, S.EMPTY)]
        pending = [c for c, fe in fl.items() if fe.get("status") in S.REPAIR_PENDING]
        bits = [f"repair: {len(done)} of {len(tracked)} complete per INSEE paging, {len(pending)} flow(s) pending"]
        if self.repair_flow:
            at = fl.get(self.repair_flow, {}).get("attempt") or {}
            bits.append(f"current {self.repair_flow} page {at.get('next_page', '-')} "
                        f"({int(at.get('rows_staged', 0)):,} rows staged, +{self.repair_pages} pages this pass)")
        bits.append(f"walk: {self.walk['visited']} visited, {self.walk['merged']} merged, "
                    f"{self.walk['unchanged']} unchanged, {len(self.walk['deferred'])} not reached")
        bits.append(f"classified this pass: {self.classified}")
        res = sum(int((fe.get("residue") or {}).get("rows", 0)) for fe in fl.values())
        if res:
            parts = {}
            for fe in fl.values():
                for p, n in ((fe.get("residue") or {}).get("by_partition") or {}).items():
                    parts[p] = parts.get(p, 0) + int(n)
            top = ", ".join(f"{p} {n:,}" for p, n in sorted(parts.items(), key=lambda kv: -kv[1])[:3])
            bits.append(f"residue kept: {res:,} stored rows no longer served by INSEE (largest: {top})")
        unp = sum(int(fe.get("unparsed_obs") or 0) for fe in fl.values())
        if unp:
            bits.append(f"unparsed observations (no value or date): {unp:,}")
        for status, word in ((S.UNSTABLE, "UNSTABLE PAGING"), (S.WITHDRAWN, "WITHDRAWN?"), (S.BROKEN, "BROKEN")):
            names = sorted(c for c, fe in fl.items() if fe.get("status") == status)
            if names:
                bits.append(f"{word}: {', '.join(names[:8])}{' +' + str(len(names) - 8) if len(names) > 8 else ''}")
        labels = list(self.labels) + [str(x) for x in getattr(self.tally, "transient_ids", [])]
        return "; ".join(bits) + ("" if not labels else " | " + " | ".join(dict.fromkeys(labels)))


class _ResumeDeferred(Exception):
    pass


# --------------------------------------------------------------------------------------------- #
# contract entry point
# --------------------------------------------------------------------------------------------- #
def update(unit, since, *, _session=None, _clock=None) -> Result:
    S.refuse_wrong_backend()
    out_dir = S.source_dir()
    os.makedirs(out_dir, exist_ok=True)
    sess = _session or requests.Session()
    if _session is None:
        sess.headers.update(UA)
    clock = _clock or S.Clock()
    print(f"[insee_melodi] pass clock: window {clock.window_s / 60:.1f} min from {clock.source}; "
          f"staging {S.staging_dir()}", flush=True)
    try:
        state = S.FlowState.load()
    except DefinitiveError:
        raise                                            # the intended refusal of an unknown state schema
    except Exception as e:                               # noqa: BLE001 - AR-331 F7: a Result ALWAYS returns
        if _fatal(e):
            raise
        return Result(status="transient_fail", obs=0,
                      error=f"STATE NOT READ: {S.STATE_FILE} - {type(e).__name__}: {str(e)[:160]}; nothing done")
    try:
        force = S.read_force()
    except Exception as e:                               # noqa: BLE001
        if _fatal(e):
            raise
        force = {"_unreadable": True}
    pa_ = _Pass(sess, clock, state, set())
    pa_.force_raw = force
    if force.get("_unreadable"):
        pa_.labels.append(f"{S.FORCE_FILE} unreadable - ignored this pass")
    try:
        flows = _get_flows(sess, clock)
        time.sleep(RATE)
    except _Stop:
        return _no_catalogue(pa_, f"the window ({clock.window_s / 60:.1f} min) leaves no room for the "
                                  f"catalogue request")
    except Exception as e:                               # noqa: BLE001 - AR-331 F7
        if _fatal(e):
            raise
        return _no_catalogue(pa_, f"{type(e).__name__}: {str(e)[:160]}")
    all_codes = [f.get("code", "") or "" for f in flows]
    codes = sorted({c for c in all_codes if S.FLOW_RE.fullmatch(c)})
    dropped = sorted({c for c in all_codes if c and not S.FLOW_RE.fullmatch(c)})
    if dropped:
        pa_.labels.append(f"{len(dropped)} catalogue code(s) not of the [A-Z0-9_] form - not pulled: {', '.join(dropped[:5])}")
    try:
        pa_.sweep_done()
        pa_.apply_force(codes)
        pa_.check_identity(codes)
        pa_.classify(codes)
        pa_.walk_slot(codes)
        pa_.repair_slot()
    except (S.StateConflict, _Stop):
        pass
    except _ResumeDeferred as e:
        pa_.labels.append(f"resume check not read this pass ({str(e)[:80]})")
    except Exception as e:                                        # noqa: BLE001 - F1: a Result ALWAYS returns
        if _fatal(e):
            raise
        pa_.labels.append(f"PASS ERROR: {type(e).__name__}: {str(e)[:160]} - the pass stopped here")
    return _result(pa_, state, codes)


def _no_catalogue(pa_, why) -> Result:
    """The flow list could not be read: nothing was worked, and the pass says so (never ok or no_change)."""
    labels = " | ".join(dict.fromkeys(pa_.labels))
    return Result(status="transient_fail", obs=0,
                  error=f"CATALOGUE NOT READ: {why} - nothing done this pass" + (f" | {labels}" if labels else ""))

def _result(pa_, state, codes) -> Result:
    fl = state.flows
    live = set(codes)                      # a flow INSEE no longer lists cannot keep the source pending
    cursors = {c: (fe.get("served") or {}).get("max_obs") for c, fe in fl.items()
               if (fe.get("served") or {}).get("max_obs")}
    # F10: obs is the store total only once every live flow carries its served rows; until then 0, never a
    # fraction that reads as a collapse in obs_count
    classified = all((fl.get(c) or {}).get("status") != S.UNCLASSIFIED and "served" in (fl.get(c) or {})
                     for c in live)
    total = sum(int((fe.get("served") or {}).get("rows") or 0) for fe in fl.values()) if classified else 0
    maxd = max(cursors.values()) if cursors else None
    # F1: every merge whose CSV has not been handed over yet - this pass's and any earlier pass's that ended
    # before its Result was returned - goes into changed_keys; returning the Result hands the debt over.
    changed = dict(pa_.changed)
    for c, fe in fl.items():
        if fe.get("csv_owed") is not None:
            changed.setdefault(c, fe["csv_owed"])
            fe.pop("csv_owed", None)
    if changed:
        try:
            state.save()
        except S.StateConflict:
            pa_.labels.append("csv_owed not cleared (state conflict) - re-emitted by a later pass")
    pending = any(fl[c].get("status") in S.REPAIR_PENDING for c in live if c in fl) \
        or bool(state.data.get("repair_blocked")) or pa_.conflict
    if pending:
        return Result(status="transient_fail", obs=total, last_obs_date=maxd, series_cursors=cursors,
                      changed_keys=changed, merged_rows=pa_.merged_rows, error=pa_.note())
    res = finalize(pa_.tally, total, maxd, source=SOURCE, series_cursors=cursors,
                   empty_window_floor=len(codes) + 1, merged_rows=pa_.merged_rows)
    res.changed_keys = changed
    # LOUD FINDINGS (probe void, corrupt state, parse collision/loss, unstable, withdrawn or broken flows)
    # must not hide under "ok": they are appended, which also takes a pure-deferral note out of health's
    # ROTATING whitelist (health._DEFERRAL_BASE is anchored), and an ok/no_change becomes partial.
    loud = [x for x in dict.fromkeys(pa_.labels)]
    for status, word in ((S.UNSTABLE, "UNSTABLE PAGING"), (S.WITHDRAWN, "WITHDRAWN?"), (S.BROKEN, "BROKEN")):
        names = sorted(c for c in live if (fl.get(c) or {}).get("status") == status)
        if names:
            loud.append(f"{word}: {', '.join(names[:8])}")
    if loud:
        res.error = f"{res.error} | " + " | ".join(loud)
        if res.status in ("ok", "no_change"):
            res.status = "partial"
    return res
