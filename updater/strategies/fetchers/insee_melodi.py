"""S3 (sdmx_delta) fetcher — INSEE Melodi, France. Keyless SDMX 2.1 catalog.

Layout (set by jobs/ingest_insee_melodi.py): ONE parquet per DATAFLOW under
clean_full/insee_melodi/<FLOW_CODE>.parquet with schema
  flow        : string  (constant within a file — the dataflow code)
  series_key  : string  ("DIM=val:DIM=val:..." over every dimension EXCEPT TIME_PERIOD,
                         sorted by dimension name — exactly the ingester's composite key)
  obs_date    : date32  (parsed from TIME_PERIOD: YYYY->Dec-31, YYYY-Qn->quarter-start,
                         YYYY-MM->month-start, YYYY-MM-DD->that day)
  value       : float64 (first non-null measure's .value)

The dedup identity is (flow, series_key, obs_date): a TIME_PERIOD plus the full
dimension tuple uniquely identifies one observation, so a re-fetched (revised) value
overwrites the prior vintage (merge keeps the last row per key) instead of duplicating.

INCREMENTAL (date-tail per flow):
  Each dataflow is a sub-unit. We read the existing parquet's max(obs_date), take its
  YEAR, and request only that year forward via the SDMX 2.1 native filter
      GET /data/{FLOW}?startPeriod=<year>&page=N
  Re-fetching the boundary YEAR (not year+1) is deliberate: it catches in-place
  revisions to the latest annual/quarterly/monthly/daily periods (merge dedups the
  overlap) — the same revision-capture stance as the treasury/bcb fetchers. For a
  daily flow this is at most one year of daily rows, still tiny next to the full
  history. `updatedAfter` is NOT requested: Melodi's DSDs reject it (HTTP 400
  "La DSD ne possède pas le composant updatedAfter"), so startPeriod is the only
  supported server-side window — verified live 2026-06.

ENUMERATION / PARSE reused verbatim from jobs/ingest_insee_melodi.py:
  - flow catalog: GET /dataflow/all  (list of {code,label})
  - obs parse: dimensions{TIME_PERIOD,...} -> series_key + obs_date; first measure -> value
  - parse_date: the same YYYY / YYYY-Qn / YYYY-MM / YYYY-MM-DD handling.
Pagination is corrected vs the ingester: Melodi pages signal end via paging.isLast
(and a SHORT page), NOT a "next" key, so we follow until isLast / a < PAGE_SIZE page.

HONEST-STATUS CONTRACT (Tally + finalize):
  Per flow we record on a Tally:
    added_unit(n)     rows merged for the flow (n>0 new, n==0 nothing newer than boundary)
    empty_unit()      flow legitimately had no observations in the window
    transient_unit()  flow hit a TransientError (timeout/5xx/429/network/bad-json) — record
                      & KEEP GOING so one flaky flow can't strand the other ~107
    structural_unit() flow returned a 200 with a real envelope but, on a FULL (origin)
                      fetch of a previously-populated file, 0 parseable rows -> schema break
  finalize() -> 'partial' on any transient (orchestrator does NOT stamp success; re-runs);
  DefinitiveError on a structural break or a large all-empty window; else ok/no_change.
  merge_and_write preserves existing data in every failure path (never shrink).

License: Licence Ouverte / Open Licence 2.0. Source: INSEE Melodi www.insee.fr
"""
from __future__ import annotations
import datetime as dt
import os
import random
import re
import time

import pyarrow as pa
import pyarrow.compute as pc
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Tally, finalize

UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
      "Accept": "application/json"}
BASE = "https://api.insee.fr/melodi"
SOURCE = "insee_melodi"
DEDUP = ("flow", "series_key", "obs_date")
PAGE_SIZE = 10000        # Melodi page size (observed hard cap)
PAGE_CEILING = 2000      # absolute page bound so a malformed paging block can't loop forever
MAX_ATTEMPTS = 5
TIMEOUT = 120
RATE = 2.1               # 30 req/min keyless limit -> >=2s between calls
PAGE_DELAY = 2.1         # between pages of the same flow (same 30 req/min budget)


# --------------------------------------------------------------------------- #
# parse helpers — copied from jobs/ingest_insee_melodi.py so storage is identical
# --------------------------------------------------------------------------- #
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


# --------------------------------------------------------------------------- #
# http
# --------------------------------------------------------------------------- #
def _get_flows(sess):
    """GET /dataflow/all -> list of {code,label}. Catalog failure is structural for
    the whole source (we can't enumerate sub-units) -> raise."""
    last = None
    for attempt in range(MAX_ATTEMPTS):
        try:
            r = sess.get(f"{BASE}/dataflow/all", headers=UA, timeout=TIMEOUT)
        except (requests.Timeout, requests.ConnectionError) as e:
            last = str(e)[:120]
            time.sleep(min(2 ** attempt, 30) + random.uniform(0, 1.0))
            continue
        if r.status_code == 200:
            try:
                flows = r.json()
            except ValueError as e:
                last = f"bad json: {e}"
                time.sleep(min(2 ** attempt, 30)); continue
            if isinstance(flows, list) and flows:
                return flows
            last = "empty/non-list catalog"
            time.sleep(min(2 ** attempt, 30)); continue
        if r.status_code in (429, 500, 502, 503, 504):
            last = f"HTTP {r.status_code}"
            time.sleep(min(2 ** attempt, 30)); continue
        raise DefinitiveError(f"{SOURCE}: /dataflow/all HTTP {r.status_code}")
    raise TransientError(f"{SOURCE}: /dataflow/all failed: {last}")


def _fetch_flow_obs(sess, flow_code, start_year):
    """Page ALL observations for a flow from startPeriod=<start_year> forward.

    Returns (observations, had_real_envelope). had_real_envelope is True when the first
    page was a 200 carrying Melodi's real response envelope (a 'paging' block) — used to
    distinguish a structural 0-rows-from-a-real-body from a legitimately-empty window.

    Raises TransientError on timeout/5xx/429/network/bad-json after the retry budget;
    DefinitiveError on a hard 4xx that is not a 'no data' signal.

    Pagination: Melodi signals the last page via paging.isLast and/or a SHORT page
    (< PAGE_SIZE). It does NOT emit a 'next' key (the ingester's stop condition was
    wrong); we stop on isLast, a short page, or PAGE_CEILING.
    """
    out = []
    had_envelope = False
    page = 1
    while True:
        sp = f"?startPeriod={start_year}" if start_year is not None else "?"
        sep = "&" if start_year is not None else ""
        url = f"{BASE}/data/{flow_code}{sp}{sep}page={page}"
        payload = _request(sess, url)
        if payload is None:
            # explicit no-data signal (e.g. 404 / empty contract) -> empty window
            break
        paging = payload.get("paging", {}) or {}
        if isinstance(paging, dict):
            had_envelope = True
        page_obs = payload.get("observations", []) or []
        out.extend(page_obs)
        n = len(page_obs)
        if n < PAGE_SIZE:
            # SHORT page is Melodi's authoritative end-of-data signal.
            break
        if isinstance(paging, dict) and paging.get("isLast") is True:
            break
        # A FULL page (n >= PAGE_SIZE) is itself the loop's positive signal that MORE
        # pages exist — that is exactly why a SHORT page (above) means "done". So a full
        # page with no recognizable forward-paging key MUST NOT be treated as the end:
        # inferring "done" from a keyless paging block here would silently TRUNCATE any
        # flow whose data legitimately spans >PAGE_SIZE rows. Keep paging until a short
        # page or an explicit isLast=True is seen; PAGE_CEILING bounds a malformed loop.
        if page >= PAGE_CEILING:
            break
        page += 1
        time.sleep(PAGE_DELAY)
    return out, had_envelope


def _request(sess, url):
    """GET one Melodi page. Returns parsed JSON dict, or None on a 'no data' 4xx.
    Raises TransientError (timeout/5xx/429/network/bad-json after budget) or
    DefinitiveError (other hard 4xx)."""
    last = None
    for attempt in range(MAX_ATTEMPTS):
        try:
            r = sess.get(url, headers=UA, timeout=TIMEOUT)
        except (requests.Timeout, requests.ConnectionError) as e:
            last = str(e)[:120]
            if attempt == MAX_ATTEMPTS - 1:
                raise TransientError(f"{SOURCE} GET {url}: {last}")
            time.sleep(min(2 ** attempt, 30) + random.uniform(0, 1.0))
            continue
        if r.status_code == 429:
            last = "HTTP 429"
            # honor Retry-After if present, else a fixed 60s cool-off (matches ingester)
            ra = r.headers.get("Retry-After")
            wait = int(ra) if (ra and ra.isdigit()) else 60
            if attempt == MAX_ATTEMPTS - 1:
                raise TransientError(f"{SOURCE} GET {url}: {last}")
            time.sleep(min(wait, 120))
            continue
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError as e:
                last = f"bad json: {e}"
                if attempt == MAX_ATTEMPTS - 1:
                    raise TransientError(f"{SOURCE} GET {url}: {last}")
                time.sleep(min(2 ** attempt, 30) + random.uniform(0, 1.0))
                continue
        if r.status_code in (500, 502, 503, 504):
            last = f"HTTP {r.status_code}"
            if attempt == MAX_ATTEMPTS - 1:
                raise TransientError(f"{SOURCE} GET {url}: {last}")
            time.sleep(min(2 ** attempt, 30) + random.uniform(0, 1.0))
            continue
        # 404 (flow/window not found) or 400 (filter matched nothing) -> empty window,
        # not a hard error. A 400 that is the updatedAfter-style "bad request" is never
        # sent here (we don't use updatedAfter); a startPeriod past the data is empty.
        if r.status_code in (400, 404):
            return None
        raise DefinitiveError(f"{SOURCE} GET {url}: HTTP {r.status_code}")
    raise TransientError(f"{SOURCE} GET {url}: {last}")


# --------------------------------------------------------------------------- #
# per-flow boundary
# --------------------------------------------------------------------------- #
def _flow_max_obs(path):
    """Max obs_date on disk for a flow, or None if the file is missing/empty."""
    if not blob.exists(path):
        return None
    try:
        od = blob.read_table(path, columns=["obs_date"]).column("obs_date")
        mx = pc.max(od).as_py() if od.length() else None
        if isinstance(mx, dt.datetime):
            mx = mx.date()
        return mx
    except Exception:
        return None


# --------------------------------------------------------------------------- #
# contract entry point
# --------------------------------------------------------------------------- #
def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)

    sess = requests.Session()
    sess.headers.update(UA)

    flows = _get_flows(sess)   # raises Transient/Definitive if the catalog is unreachable
    time.sleep(RATE)

    tally = Tally()
    total = 0
    maxd = None
    cursors: dict[str, str] = {}   # flow_code -> max obs_date (per-flow freshness)

    for flow in flows:
        code = flow.get("code", "")
        if not code:
            continue
        path = os.path.join(out_dir, f"{code}.parquet")
        before = blob.row_count(path)

        on_disk_max = _flow_max_obs(path)
        # startPeriod = YEAR of the stored max (re-fetch boundary year for revisions);
        # if the flow has no on-disk history, fall back to `since` year, else full origin.
        if on_disk_max is not None:
            start_year = on_disk_max.year
        elif since:
            try:
                start_year = dt.date.fromisoformat(since).year
            except ValueError:
                start_year = None
        else:
            start_year = None  # full origin fetch (new/never-landed flow)

        # seed the per-flow cursor from the on-disk frontier so an untouched/empty
        # flow still reports its real freshness (can't hide behind the unit-level max).
        if on_disk_max is not None:
            cursors[code] = on_disk_max.isoformat()
            if maxd is None or on_disk_max.isoformat() > maxd:
                maxd = on_disk_max.isoformat()

        try:
            obs, had_envelope = _fetch_flow_obs(sess, code, start_year)
        except TransientError:
            # leave this flow's data untouched; record & keep going -> run is 'partial'
            tally.transient_unit()
            total += before
            time.sleep(RATE)
            continue

        if not obs:
            # 200 + real envelope but 0 rows on a FULL (origin) fetch of a previously
            # populated flow == structural break. A startPeriod tail returning nothing,
            # or a genuinely empty flow, is legitimately empty.
            if start_year is None and had_envelope and before > 0:
                tally.structural_unit()
            else:
                tally.empty_unit()
            total += before
            time.sleep(RATE)
            continue

        # parse observations -> rows
        keys: list[str] = []
        dates: list[dt.date] = []
        vals: list[float] = []
        for o in obs:
            row = _obs_to_row(o)
            if row is None:
                continue
            k, d, v = row
            keys.append(k)
            dates.append(d)
            vals.append(v)

        if not keys:
            # 200 whose envelope CARRIED observations (we are past the `if not obs`
            # guard, so `obs` is non-empty) but NONE parsed to a usable row. For an
            # on-disk flow (before > 0) this is a real parser/schema break on BOTH a
            # full origin fetch AND an incremental tail: startPeriod is INCLUSIVE, so a
            # healthy active flow MUST re-surface >=1 boundary observation that parses —
            # 0 parsed from a non-empty body means the obs shape changed (TIME_PERIOD /
            # measures keys drifted), not a quiet window. Surface it as structural so
            # finalize() raises DefinitiveError for human attention; merge_and_write has
            # already left the existing data untouched. A never-landed flow (before == 0)
            # with an unparseable origin body stays empty (nothing to break).
            if before > 0:
                tally.structural_unit()
            else:
                tally.empty_unit()
            total += before
            time.sleep(RATE)
            continue

        new_tbl = pa.table({
            "flow":       pa.array([code] * len(keys), pa.string()),
            "series_key": pa.array(keys, pa.string()),
            "obs_date":   pa.array(dates, pa.date32()),
            "value":      pa.array(vals, pa.float64()),
        })

        # merge ONLY via merge_and_write — never write parquet ourselves; never shrink.
        n, md = merge.merge_and_write(path, new_tbl, mode="merge", dedup_keys=DEDUP)
        total += n
        # A flow that returned & parsed real rows is a SUCCESSFUL sub-unit (data flowed),
        # even when the boundary-year re-fetch nets ZERO new rows after dedup. Mark it
        # added_unit(len(keys)) — NOT the net merge delta — so a healthy idempotent
        # re-run (every flow re-returns its boundary year, zero net-new) does NOT make
        # empty==attempted and false-trip the all-empty structural floor. The true
        # net-new delta is reflected in obs (total) and the finalize note via merge.
        tally.added_unit(len(keys))
        if md:
            cursors[code] = md
            if maxd is None or md > maxd:
                maxd = md
        time.sleep(RATE)

    # empty_window_floor = (#sub-units)-1 per the contract: a single quiet flow can't
    # trip the all-empty structural floor, but a wholesale all-empty window does.
    return finalize(tally, total, maxd, source=SOURCE, series_cursors=cursors,
                    empty_window_floor=len(flows) - 1)
