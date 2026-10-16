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
  1. identity: one HEAD per served file, compared with _flows.json; a changed object is re-classified.
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
state; conditional writes - see _melodi_state.py), `_force_origin.json` (OPERATOR-owned, read only:
{"flows": [...], "unblock_repair_after_utc": "<iso>"}), `_repull/<FLOW>/<attempt>/<nnnnn>.chunk`. Local
staging lives beside the bounded-merge spill dir (ECONDL_DUCKDB_TMP), never D:.

T0 STEP (before the R2 write key is revoked): let every in-progress attempt finish, or abandon it, and delete
its `_repull/` chunks on R2 - after T0 they cannot be deleted. mirror_sync and upload_r2 carry only .parquet,
so `_flows.json` and `_force_origin.json` must be copied to the local store explicitly.

LIMITS, stated: completeness is "per INSEE paging" (no INSEE total exists); the served-parquet publish is not
conditional (two writers of one flow: last writer wins - the attempt lease protects only the repair); the
stored rows INSEE no longer serves are kept (residue, reported) until the owner decides otherwise.

License: Licence Ouverte / Open Licence 2.0. Source: INSEE Melodi www.insee.fr
"""
from __future__ import annotations
import datetime as dt
import hashlib
import os
import random
import re
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Tally, finalize, load_rotation, rotate_after, save_rotation
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
        except (requests.Timeout, requests.ConnectionError) as e:
            last = str(e)[:120]
            wait = min(2 ** attempt, 30) + random.uniform(0, 1.0)
            if attempt == MAX_ATTEMPTS - 1:
                break
            if not clock.fits(wait + TIMEOUT + 5):
                raise _Stop("no room for a retry")
            time.sleep(wait)
            continue
        if r.status_code == 200:
            try:
                return "ok", r.json()
            except ValueError as e:
                last = f"bad json: {e}"
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
_GEO_V = re.compile(r"GEO=(\d{4})-")


def _partition_profile(path) -> dict | None:
    """Rows, max obs_date, per-partition counts (dimension-name set + GEO vintage) and the DISTINCT
    normalised (key without FREQ and GEO vintage, date) count of the stored file. None when absent.
    Reads two columns of the served object once (classification only - never to decide a walk)."""
    lc = blob.local_copy(path)
    if lc is None:
        return None
    local, temp = lc
    try:
        t = pq.read_table(local, columns=["series_key", "obs_date"])
    finally:
        if temp:
            try:
                os.remove(local)
            except OSError:
                pass
    keys = t.column("series_key")
    dims = pc.replace_substring_regex(keys, r"=[^:]*", "")              # "A:B:C" - the dimension names
    vint = pc.coalesce(pc.struct_field(pc.extract_regex(keys, r"GEO=(?P<v>\d{4})-"), [0]),
                       pa.scalar("", pa.string()))
    part = pc.binary_join_element_wise(dims, vint, "|")
    vc = {str(d["values"]): int(d["counts"]) for d in pc.value_counts(part).to_pylist()}
    norm = pc.replace_substring_regex(keys, r"(^|:)FREQ=[^:]*", "")
    norm = pc.replace_substring_regex(norm, r"^:", "")                  # FREQ sorts first: no leading ':'
    norm = pc.replace_substring_regex(norm, r"GEO=\d{4}-", "GEO=")
    s_norm = pa.table({"k": norm, "d": t.column("obs_date")}).group_by(["k", "d"]).aggregate([]).num_rows
    mx = pc.max(t.column("obs_date")).as_py() if t.num_rows else None
    return {"rows": t.num_rows, "partitions": vc, "s_norm": s_norm,
            "max_obs": mx.isoformat() if mx else None}


def _rule_hits(code, prof, force) -> str | None:
    """The bootstrap rule that sends a flow to needs_origin, or None."""
    if code in force:
        return "operator force_origin"
    if code in CI_PAGE_MULTIPLE_DELTAS:
        return "a CI-logged delta was an exact multiple of 10,000"
    if prof is None or prof["rows"] == 0:
        return "zero rows or no stored object"
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
        if swept:
            self.save()

    def check_identity(self, codes):
        for code in codes:
            fe = self.st.flow(code)
            cur = S.identity(self.path(code))
            if fe.get("status") == S.UNCLASSIFIED:
                continue
            served = fe.get("served") or {}
            if cur is None and served.get("rows"):
                fe["status"], fe["why"] = S.UNCLASSIFIED, "served object disappeared"
            elif cur is not None and not S.same_identity(cur, served.get("identity")) \
                    and fe.get("status") not in (S.IN_PROGRESS, S.MERGE_PENDING):
                fe["status"], fe["why"] = S.UNCLASSIFIED, "served object changed outside this fetcher"

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
            prof = _partition_profile(self.path(code))
            ident = S.identity(self.path(code))
            fe["served"] = {"identity": ident, "rows": (prof or {}).get("rows", 0),
                            "max_obs": (prof or {}).get("max_obs")}
            if prof:
                fe["partitions"], fe["s_norm"] = prof["partitions"], prof["s_norm"]
            why = _rule_hits(code, prof, self.force)
            if why:
                fe.update(status=S.NEEDS_ORIGIN, why=why, repair=True)
            else:
                fe.update(status=S.LEGACY, why="no exact-multiple partition; probe pending")
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
        after = (getattr(self, "force_raw", None) or {}).get("unblock_repair_after_utc")
        return bool(blk and after and str(after) > str(blk.get("utc")))

    def _seed_control(self):
        for code, fe in sorted(self.st.flows.items()):
            if code.startswith("DS_RP_TD_") and (fe.get("served") or {}).get("rows") == PAGE_SIZE \
                    and fe.get("status") == S.NEEDS_ORIGIN:
                return code
        return None

    def controls_and_probes(self, legacy, end):
        """C1 probe controls, R2-6 page-determinism control, then the page-beyond probes. Charged to the
        classification slot (R2-7)."""
        seed = self._seed_control()
        if self._determinism_needed() and seed and self.clock.now() < end:
            try:
                a = _fetch_page(self.sess, seed, None, 1, self.clock)
                time.sleep(PAGE_DELAY)
                b = _fetch_page(self.sess, seed, None, 1, self.clock)
            except (_Stop, TransientError) as e:
                self.labels.append(f"page-determinism control not run ({type(e).__name__}): {str(e)[:80]}")
            else:
                same = (a.fp == b.fp and a.raw_n == b.raw_n)
                self.st.data["determinism"] = {"utc": S.utcnow(), "flow": seed, "same": same,
                                               "raw": [a.raw_n, b.raw_n]}
                print(f"[insee_melodi] page-determinism control {seed} page 1 twice: "
                      f"{'IDENTICAL' if same else 'DIFFERENT'} ({a.raw_n}/{b.raw_n} obs)", flush=True)
                if same:
                    self.st.data.pop("repair_blocked", None)
                else:
                    self.st.data["repair_blocked"] = {"utc": S.utcnow(),
                                                      "why": "Melodi paging not deterministic",
                                                      "evidence": [a.fp[:16], b.fp[:16]]}
        if not legacy:
            return
        if not seed:
            for code in legacy:
                self.st.flow(code)["probe"] = {"void": "no one-page seed left to control the probe",
                                               "utc": S.utcnow()}
            return
        ctl_ok, why = self._probe_controls(seed)
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

    def _probe_controls(self, seed):
        try:
            pos = _fetch_page(self.sess, seed, None, probe_page(PAGE_SIZE), self.clock)
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
            return False, f"positive control {seed} page 3 came back empty"
        if neg.raw_n > 0:
            return False, f"negative control {PROBE_EMPTY_CONTROL} page past its rows returned {neg.raw_n} obs"
        return True, ""

    # ---------------------------------------------------------------- 3. walk
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
            mod = _catalog_modified(self.sess, code, self.clock)
            time.sleep(RATE)
            last = fe.get("last_tail_utc") or fe.get("last_full_utc")
            stale = (not last) or (dt.datetime.now(dt.timezone.utc)
                                   - dt.datetime.fromisoformat(last)).days >= BACKSTOP_DAYS
            if mod is not None and mod == fe.get("modified") and not stale:
                self.walk["unchanged"] += 1
                continue
            self.walk["visited"] += 1
            if fe.get("start_ignored") and (fe.get("pages") or 0) > CHUNK_PAGES:
                fe.update(status=S.NEEDS_REFRESH, why="due; a full pull larger than one chunk")
                self.save()
                continue
            res = self.work(code, end, in_walk=True, modified=mod)
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
        while self.clock.now() < self.clock.soft_end and not self.conflict:
            code = self._next_repair()
            if code is None:
                return
            self.repair_flow = code
            res = self.work(code, self.clock.soft_end, in_walk=False, modified=None)
            if res != "complete":
                return

    def _next_repair(self):
        fl = self.st.flows
        for status in (S.MERGE_PENDING, S.IN_PROGRESS):
            cands = sorted((fe.get("attempt", {}).get("started_utc") or "", c)
                           for c, fe in fl.items() if fe.get("status") == status)
            for _, c in cands:
                if self._lease_ok(c):
                    return c
        for status in (S.NEEDS_ORIGIN, S.NEEDS_REFRESH):
            for c in sorted(c for c, fe in fl.items() if fe.get("status") == status):
                return c
        return None

    def _lease_ok(self, code):
        at = self.st.flow(code).get("attempt") or {}
        if not at or at.get("owner") == self.me:
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
        if mode == "tail" and not fe.get("start_ignored") and served.get("max_obs"):
            start = int(served["max_obs"][:4])
        at = {"id": S.new_attempt_id(), "owner": self.me, "heartbeat_utc": S.utcnow(),
              "started_utc": S.utcnow(), "mode": "origin" if start is None else "tail", "start": start,
              "next_page": 1, "chunks": 0, "rows_staged": 0, "pages": 0, "page_fp": None,
              "modified_at_start": modified, "passes": 0, "passes_without_progress": 0,
              "within_dups": 0, "restarts": (fe.get("attempt") or {}).get("restarts", 0)}
        fe["attempt"] = at
        return at

    def _restart(self, code, why):
        fe = self.st.flow(code)
        old = fe.get("attempt") or {}
        restarts = int(old.get("restarts", 0)) + 1
        self._sweep(code, keep=None)
        fe.pop("attempt", None)
        self.tally.transient_unit(f"{code}: attempt restarted - {why}")
        self.labels.append(f"{code}: attempt restarted ({restarts}/{MAX_RESTARTS}) - {why}")
        if restarts >= MAX_RESTARTS:
            fe.update(status=S.UNSTABLE, why=f"{MAX_RESTARTS} restarts: {why}")
            self.labels.append(f"{code}: UNSTABLE PAGING after {MAX_RESTARTS} restarts - left for the owner")
        else:
            fe["status"] = S.NEEDS_ORIGIN if fe.get("repair") else S.NEEDS_REFRESH
            fe["attempt"] = {"restarts": restarts}
        self.save()
        return "restart"

    def work(self, code, slot_end, *, in_walk, modified):
        """Advance one flow's pull until it completes, the slot ends, or a per-flow failure.
        Returns complete | progress | queued | transient | restart | stop | conflict | withdrawn | empty."""
        fe = self.st.flow(code)
        at = fe.get("attempt") or {}
        try:
            if fe.get("status") == S.MERGE_PENDING:
                return self._finalize(code)
            if not at.get("id"):
                if modified is None and not in_walk:
                    modified = _catalog_modified(self.sess, code, self.clock)
                mode = "origin" if fe.get("status") == S.NEEDS_ORIGIN else "tail"
                prior = fe.get("status")
                at = self._new_attempt(code, mode, modified)
                # EVERY started attempt is in_progress, walk tails included: a killed walk tail must be
                # resumed by the repair slot, never re-skipped by a quiet catalog gate (leaking its chunks).
                at["prior_status"] = prior
                fe["status"] = S.IN_PROGRESS
            else:
                try:
                    ok = self._resume_check(code, at)
                except _ResumeDeferred as e:
                    # Could not read the page to compare: no progress this pass, counted (stall label at 4).
                    self.tally.transient_unit(f"{code}: resume check not read ({str(e)[:80]})")
                    self._progress(code, at, 0)
                    return "stop"
                if ok is not True:
                    return self._restart(code, ok)
            at["owner"], at["heartbeat_utc"] = self.me, S.utcnow()
            at["passes"] = int(at.get("passes", 0)) + 1
            return self._pages(code, fe, at, slot_end, in_walk)
        except S.StateConflict:
            return "conflict"

    def _resume_check(self, code, at):
        """C4: the whole last committed page must re-fetch identically, and `modified` must not have moved."""
        if int(at.get("next_page", 1)) <= 1:
            return True
        try:
            p = _fetch_page(self.sess, code, at.get("start"), int(at["next_page"]) - 1, self.clock)
        except (_Stop, TransientError) as e:
            raise _ResumeDeferred(str(e)) from e
        time.sleep(PAGE_DELAY)
        if p.fp != at.get("page_fp"):
            return f"page {int(at['next_page']) - 1} changed since it was staged (paging not stable)"
        mod = _catalog_modified(self.sess, code, self.clock)
        if mod is not None and at.get("modified_at_start") is not None and mod != at["modified_at_start"]:
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
        pend = {"pages": 0, "rows": 0, "dups": 0, "fp": None, "next": int(at["next_page"])}
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
            at["pages"] = int(at["pages"]) + pend["pages"]
            at["within_dups"] = int(at["within_dups"]) + pend["dups"]
            at["page_fp"] = pend["fp"]
            added_pages += pend["pages"]
            pend.update(pages=0, rows=0, dups=0)
            at["heartbeat_utc"] = S.utcnow()
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
                    pend["dups"] += p.within_dups
                    pend["fp"] = p.fp
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
                        self.labels.append(f"{code}: tail outgrew one chunk - continues in the repair slot")
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
        rows = (fe.get("served") or {}).get("rows") or 0
        self._sweep(code, keep=None)
        fe.pop("attempt", None)
        if at.get("mode") == "tail" and rows:
            # nothing at or after startPeriod: a quiet tail. The flow keeps the status it had.
            fe.update(status=at.get("prior_status") or S.LEGACY, last_tail_utc=S.utcnow(),
                      modified=at.get("modified_at_start") or fe.get("modified"))
            self.save()
            return "complete"
        if rows:
            fe.update(status=S.WITHDRAWN, why=f"origin page 1 {'HTTP ' + str(p.http) if p.nodata else 'empty'} "
                                              f"over {rows:,} stored rows - withdrawn by INSEE?")
            self.labels.append(f"{code}: WITHDRAWN? origin page 1 empty over {rows:,} stored rows")
            self.tally.transient_unit(f"{code}: origin page 1 empty over {rows:,} stored rows")
            self.save()
            return "withdrawn"
        fe.update(status=S.EMPTY, why="INSEE serves no observations", last_full_utc=S.utcnow(),
                  modified=at.get("modified_at_start"))
        self.save()
        return "empty"

    # ---------------------------------------------------------------- finalize (C2, C3, C6, C12)
    def _finalize(self, code):
        fe = self.st.flow(code)
        at = fe.get("attempt") or {}
        if not self.clock.fits(S.merge_margin_s(int(at.get("rows_staged", 0)))):
            self.labels.append(f"{code}: merge deferred - {int(self.clock.hard_end - self.clock.now())}s left "
                               f"is under the margin for {at.get('rows_staged', 0):,} rows")
            return "progress"
        import duckdb                                                        # noqa: PLC0415
        sdir = S.staging_dir()
        staged = os.path.join(sdir, f"{code}.{at['id']}.staged.chunkpart")
        total = 0
        w = None
        try:
            for n in range(int(at.get("chunks", 0))):
                body = blob.read_bytes(S.chunk_path(code, at["id"], n))
                if body is None:
                    return self._restart(code, f"chunk {n} missing")
                t = pq.read_table(pa.BufferReader(body))
                total += t.num_rows
                if w is None:
                    w = pq.ParquetWriter(staged, _SCHEMA, compression="zstd")
                w.write_table(t.cast(_SCHEMA))
            if w is not None:
                w.close()
                w = None
            if total != int(at.get("rows_staged", 0)):
                return self._restart(code, f"chunk footers sum to {total:,}, cursor says {at.get('rows_staged', 0):,}")
            stored = blob.local_copy(self.path(code))
            stored_local, stored_tmp = (stored if stored else (None, False))
            try:
                con = duckdb.connect()
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
                    fe["served"] = {"identity": ident_now, "rows": stored_stats[0], "max_obs": stored_stats[1]}
                    self.changed[code] = stored_stats[1] or ""
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
                        fe["status"] = S.MERGE_PENDING
                        self.tally.transient_unit(f"{code}: merge refused, kept merge_pending - {str(e)[:120]}")
                        self.labels.append(f"{code}: MERGE REFUSED (kept merge_pending): {str(e)[:120]}")
                        self.save()
                        return "transient"
                    print(f"[insee_melodi] {code}: merge took {time.monotonic() - t0:.1f}s for "
                          f"{total:,} staged rows ({changed:,} new or changed)", flush=True)
                    self.changed[code] = md
                    self.merged_rows += int(changed)
                    fe["served"] = {"identity": S.identity(self.path(code)), "rows": int(n_rows), "max_obs": md}
                else:
                    if not fe.get("served"):
                        fe["served"] = {"identity": S.identity(self.path(code)), "rows": 0, "max_obs": None}
            finally:
                if stored_tmp and stored_local and os.path.exists(stored_local):
                    os.remove(stored_local)
        finally:
            if w is not None:
                w.close()
            if os.path.exists(staged):
                os.remove(staged)
        mode = at.get("mode")
        # Only an ORIGIN pull (or a flow already complete) earns "complete per INSEE paging": a finished
        # tail of a legacy flow proves its recent years, not its history.
        prior = at.get("prior_status")
        final = S.COMPLETE if (mode == "origin" or prior == S.COMPLETE) else (prior or S.LEGACY)
        fe.update(status=final, basis=mode, modified=at.get("modified_at_start"),
                  pages=int(at.get("pages", 0)),
                  why=(("complete per INSEE paging" if final == S.COMPLETE else f"{final}; last tail complete per INSEE paging")
                       + (f"; {residue['rows']:,} stored rows no longer served by INSEE kept under the merge "
                          f"decision" if residue["rows"] else "")))
        fe["last_full_utc" if mode == "origin" else "last_tail_utc"] = S.utcnow()
        if mode == "origin":
            fe["residue"] = residue
        if at.get("within_dups"):
            fe["parse_collisions"] = int(at["within_dups"])
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
        names = []
        r2 = blob._r2_routed()
        if r2 is not None:
            prefix = blob._path_to_key(base).replace("\\", "/") + "/"
            pag = r2.client.get_paginator("list_objects_v2")
            for pg in pag.paginate(Bucket=r2.bucket, Prefix=prefix):
                for o in pg.get("Contents", []) or []:
                    names.append(o["Key"][len(prefix):])
        elif os.path.isdir(base):
            for dp, _, fns in os.walk(base):
                for f in fns:
                    names.append(os.path.relpath(os.path.join(dp, f), base).replace("\\", "/"))
        n = 0
        for rel in names:
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
            bits.append(f"residue kept: {res:,} stored rows no longer served by INSEE")
        for status, word in ((S.UNSTABLE, "UNSTABLE PAGING"), (S.WITHDRAWN, "WITHDRAWN?")):
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
    state = S.FlowState.load()
    force = S.read_force()
    force_flows = set(force.get("flows") or []) if isinstance(force.get("flows"), list) else set()
    pa_ = _Pass(sess, clock, state, force_flows)
    pa_.force_raw = force
    if force.get("_unreadable"):
        pa_.labels.append(f"{S.FORCE_FILE} unreadable - ignored this pass")
    flows = _get_flows(sess, clock)
    time.sleep(RATE)
    codes = sorted({f.get("code", "") for f in flows if S.FLOW_RE.match(f.get("code", "") or "")})
    try:
        pa_.sweep_done()
        pa_.check_identity(codes)
        pa_.classify(codes)
        pa_.walk_slot(codes)
        pa_.repair_slot()
    except S.StateConflict:
        pass
    except _ResumeDeferred as e:
        pa_.labels.append(f"resume check not read this pass ({str(e)[:80]})")
    except _Stop:
        pass
    fl = state.flows
    cursors = {c: (fe.get("served") or {}).get("max_obs") for c, fe in fl.items()
               if (fe.get("served") or {}).get("max_obs")}
    total = sum(int((fe.get("served") or {}).get("rows") or 0) for fe in fl.values())
    maxd = max(cursors.values()) if cursors else None
    live = set(codes)                      # a flow INSEE no longer lists cannot keep the source pending
    pending = any(fl[c].get("status") in S.REPAIR_PENDING for c in live if c in fl) \
        or bool(state.data.get("repair_blocked")) or pa_.conflict
    if pending:
        return Result(status="transient_fail", obs=total, last_obs_date=maxd, series_cursors=cursors,
                      changed_keys=dict(pa_.changed), merged_rows=pa_.merged_rows, error=pa_.note())
    res = finalize(pa_.tally, total, maxd, source=SOURCE, series_cursors=cursors,
                   empty_window_floor=len(codes) + 1, merged_rows=pa_.merged_rows)
    res.changed_keys = dict(pa_.changed)
    # LOUD FINDINGS (probe void, corrupt state, parse collision, unstable or withdrawn flows) must not
    # hide under "ok": they are appended, which also takes a pure-deferral note out of health's ROTATING
    # whitelist (health._DEFERRAL_BASE is anchored), and an ok/no_change becomes partial.
    loud = [x for x in dict.fromkeys(pa_.labels)]
    for status, word in ((S.UNSTABLE, "UNSTABLE PAGING"), (S.WITHDRAWN, "WITHDRAWN?")):
        names = sorted(c for c in live if (fl.get(c) or {}).get("status") == status)
        if names:
            loud.append(f"{word}: {', '.join(names[:8])}")
    if loud:
        res.error = f"{res.error} | " + " | ".join(loud)
        if res.status in ("ok", "no_change"):
            res.status = "partial"
    return res
