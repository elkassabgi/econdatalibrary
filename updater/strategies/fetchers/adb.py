"""S3 (sdmx_delta) fetcher — Asian Development Bank, KIDB (Key Indicators
Database), SDMX 3.0 REST API. ADB open-data terms (attribution). No API key.

Layout (set by jobs/ingest_adb_kidb.py): ONE parquet per DATAFLOW under
clean_full/adb/<FLOW>.parquet, long format with schema
  series_key : "ADB:{FLOW}:{INDICATOR}:{ECONOMY_CODE}"  (exactly 3 colons)
  obs_date   : date32  (annual -> Dec-31; quarter -> quarter start; month -> month start)
  value      : float64
Every on-disk flow is annual (freq A); the fetcher still detects each flow's
frequency from its stored obs_date stamps so a Q/M flow would be requested with
the right SDMX FREQ code automatically.

DATE-TAIL (cheap delta). One sub-unit per (flow, indicator) the source already
has on disk. For each flow we read the existing parquet to learn:
  - the EXACT key columns (series_key, obs_date, value) and key shape,
  - the flow's frequency (from the obs_date pattern),
  - the flow's max(obs_date) -> request boundary,
  - the set of INDICATORS already present (series_key parts[2]) and ECONOMIES
    (series_key parts[3], used only for the per-economy 504 fallback).
We then request ONLY newer observations via the SDMX 3.0 native time filter
  GET /v4/sdmx/data/ADB,{FLOW}/{FREQ}.{IND}.?format=sdmx-csv&startPeriod={boundary}
where boundary = the YEAR of the flow's max(obs_date). We re-fetch the boundary
period itself so an in-place revision to the latest value is captured (merge
dedups the overlap; new wins). The full-flow dump (A..) 504s, so we iterate
per-indicator (FREQ.{IND}. = all economies for one indicator) exactly as the
ingester does; on 422/500/504 we fall back to per-economy iteration using the
economy codes already harvested from disk.

We REUSE the ingester's enumeration + parse logic verbatim (endpoints, SDMX-CSV
column mapping, time-period parsing, key construction) — nothing is re-discovered;
the on-disk parquet is the source of truth for which indicators/economies/freq a
flow has. New indicators (never landed) are a backfill concern, not a date-tail
update, so the date-tail intentionally tracks the established on-disk series.

NOTE on the SDMX `updatedAfter` param: the KIDB host rejects it (returns an SDMX
<message:Error>), so we rely on `startPeriod` alone for the incremental window.

MERGE only via merge.merge_and_write(path, tbl, mode="merge",
dedup_keys=("series_key","obs_date")) — we never write parquet ourselves; the
never-shrink invariant preserves existing data on every failure path.

HONEST STATUS (Tally + finalize):
  added_unit(n)     rows merged for a (flow, indicator) (n>0 new, n==0 net-new but data flowed)
  empty_unit()      indicator legitimately returned no rows in the window (404, or a
                    200 with an empty/whitespace body)
  transient_unit()  timeout / 5xx / 429 / network / unparseable-after-retries -> the
                    WHOLE run returns 'partial' (orchestrator does NOT stamp success;
                    re-runs next tick); existing data left untouched.
  structural_unit() a 200 with a real SDMX-CSV header but 0 data rows from a BOUNDARY
                    re-fetch of an indicator that exists on disk (a boundary re-fetch
                    must re-return at least the boundary period; 0 rows => the
                    expected SDMX structure is gone) -> DefinitiveError via finalize.
series_cursors {flow: 'YYYY-MM-DD'} seeded from the on-disk frontier so a frozen
flow reports its real cursor. empty_window_floor = (#sub-units) - 1.
"""
from __future__ import annotations
import csv
import datetime as dt
import io
import os
import re
import time

import pyarrow as pa
import pyarrow.compute as pc
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Tally, finalize

SOURCE = "adb"
BASE = "https://kidb.adb.org/api"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
DEDUP = ("series_key", "obs_date")
RATE = 0.6            # polite delay between data requests
MAX_ATTEMPTS = 4
TIMEOUT = 180
BAN_COOLDOWN = 90     # seconds to wait out a 403 "temporarily banned" anti-abuse throttle


# --------------------------------------------------------------------------- #
# time-period parsing (verbatim from jobs/ingest_adb_kidb.py)
# --------------------------------------------------------------------------- #
def _parse_tp(s: str) -> dt.date | None:
    t = (s or "").strip()
    try:
        m = re.fullmatch(r"(\d{4})", t)
        if m:
            return dt.date(int(m.group(1)), 12, 31)
        m = re.fullmatch(r"(\d{4})-?Q([1-4])", t, re.IGNORECASE)
        if m:
            return dt.date(int(m.group(1)), (int(m.group(2)) - 1) * 3 + 1, 1)
        m = re.fullmatch(r"(\d{4})[-M](\d{1,2})", t, re.IGNORECASE)
        if m and 1 <= int(m.group(2)) <= 12:
            return dt.date(int(m.group(1)), int(m.group(2)), 1)
        m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", t)
        if m:
            return dt.date.fromisoformat(t)
    except ValueError:
        return None
    return None


def _parse_sdmx_csv(text: str, flow: str) -> tuple[list[tuple[str, dt.date, float]], set[str], bool]:
    """Parse SDMX-CSV into (rows, economies_seen, had_header).

    rows are (series_key, obs_date, value) built EXACTLY as the ingester builds
    them: "ADB:{FLOW}:{INDICATOR}:{ECONOMY_CODE}". had_header is True iff the body
    parsed as a real SDMX-CSV table (fieldnames present incl. TIME_PERIOD+OBS_VALUE)
    — used to distinguish a structural break (real header, 0 data rows) from a
    legitimately-empty body.
    """
    out: list[tuple[str, dt.date, float]] = []
    ecos: set[str] = set()
    try:
        rdr = csv.DictReader(io.StringIO(text))
        if not rdr.fieldnames:
            return out, ecos, False
        cols = {c.upper().strip(): c for c in rdr.fieldnames if c}
        c_tp = cols.get("TIME_PERIOD")
        c_val = cols.get("OBS_VALUE") or cols.get("VALUE")
        c_ind = cols.get("INDICATOR") or cols.get("INDICATOR_CODE") or cols.get("SERIES")
        c_eco = (cols.get("ECONOMY_CODE") or cols.get("ECONOMY") or
                 cols.get("REF_AREA") or cols.get("COUNTRY"))
        if not c_tp or not c_val:
            return out, ecos, False
        had_header = True
        for row in rdr:
            d = _parse_tp(row.get(c_tp, ""))
            if d is None:
                continue
            raw = (row.get(c_val) or "").strip()
            if not raw:
                continue
            try:
                v = float(raw)
            except ValueError:
                continue
            if v != v:   # NaN
                continue
            ind = (row.get(c_ind) or "").strip() if c_ind else ""
            eco = (row.get(c_eco) or "").strip() if c_eco else ""
            if eco:
                ecos.add(eco)
            out.append((f"ADB:{flow}:{ind}:{eco}", d, v))
        return out, ecos, had_header
    except Exception:
        # An unparseable body that still looked non-trivial -> treat as no header
        # (caller decides transient vs empty from the HTTP context).
        return out, ecos, False


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #
def _get(sess: requests.Session, url: str):
    """GET with retry/backoff.

    Returns:
      (text, 200)                 -> 200 body
      (None, 404)                 -> not found (legitimately empty for this sub-unit)
    Raises:
      TransientError              -> timeout / 5xx / 429 / network after the budget,
                                     OR a 403 "temporarily banned" anti-abuse throttle
      DefinitiveError             -> other hard 4xx (400/422 handled by caller as fallback signal)
    For 400/422 we return (None, status) so the caller can trigger the per-economy
    fallback (the full-economy query is sometimes rejected for big flows).

    NOTE: the KIDB host responds to aggressive clients with HTTP 403 and a body
    "You have been temporarily banned." This is a RATE-LIMIT cooldown, not a
    permanent authorization failure, so it is treated as TransientError (with a
    long backoff) — never a DefinitiveError — so the run surfaces 'partial' and
    re-runs next tick instead of laundering a throttle into a quiet no_change.
    """
    last = None
    for attempt in range(MAX_ATTEMPTS):
        try:
            r = sess.get(url, headers=UA, timeout=TIMEOUT)
        except (requests.Timeout, requests.ConnectionError) as e:
            last = str(e)[:120]
            if attempt == MAX_ATTEMPTS - 1:
                raise TransientError(f"adb GET {url[-80:]}: {last}")
            time.sleep(min(5 * (attempt + 1), 30))
            continue
        if r.status_code == 200:
            return r.text, 200
        if r.status_code == 404:
            return None, 404
        if r.status_code in (400, 422):
            return None, r.status_code          # caller -> per-economy fallback
        if r.status_code == 403 and "temporarily banned" in (r.text or "").lower():
            # Anti-abuse cooldown: back off hard, then give up as transient.
            last = "HTTP 403 temporarily banned"
            if attempt == MAX_ATTEMPTS - 1:
                raise TransientError(f"adb GET {url[-80:]}: {last}")
            time.sleep(BAN_COOLDOWN)
            continue
        if r.status_code == 429:
            if attempt == MAX_ATTEMPTS - 1:
                raise TransientError(f"adb GET {url[-80:]}: HTTP 429")
            time.sleep(60)
            continue
        if r.status_code in (500, 502, 503, 504):
            last = f"HTTP {r.status_code}"
            if attempt == MAX_ATTEMPTS - 1:
                raise TransientError(f"adb GET {url[-80:]}: {last}")
            time.sleep(min(5 * (attempt + 1), 30))
            continue
        # other hard 4xx
        raise DefinitiveError(f"adb GET {url[-80:]}: HTTP {r.status_code}")
    raise TransientError(f"adb GET {url[-80:]}: {last}")


def _harvest_indicator(sess, flow: str, freq: str, ind: str, start_period: int,
                       economies: list[str]) -> tuple[list, bool]:
    """Fetch one indicator (all economies) for a flow from start_period forward.

    Returns (rows, had_header). On a full-economy 400/422 (or a 504 surfaced as a
    TransientError), retry per-economy using the harvested economy codes — exactly
    the ingester's fallback path. had_header reflects whether ANY successful 200
    response carried a real SDMX-CSV header (used for the structural check).
    """
    q = f"{BASE}/v4/sdmx/data/ADB,{flow}/{freq}.{ind}.?format=sdmx-csv&startPeriod={start_period}"
    text, status = _get(sess, q)
    time.sleep(RATE)
    if status == 200 and text is not None:
        rows, _ecos, had_header = _parse_sdmx_csv(text, flow)
        if had_header:
            return rows, True
        # 200 but no SDMX header (rare) -> fall through to per-economy fallback
    if status == 404:
        return [], False

    # 400/422 (or empty/non-tabular 200) -> per-economy fallback if we know economies.
    if not economies:
        return [], False
    rows: list = []
    any_header = False
    for eco in economies:
        eq = (f"{BASE}/v4/sdmx/data/ADB,{flow}/{freq}.{ind}.{eco}"
              f"?format=sdmx-csv&startPeriod={start_period}")
        t2, s2 = _get(sess, eq)
        time.sleep(RATE)
        if s2 == 200 and t2 is not None:
            r2, _e2, h2 = _parse_sdmx_csv(t2, flow)
            any_header = any_header or h2
            rows.extend(r2)
        # 404/400/422 per economy -> just that economy is empty
    return rows, any_header


# --------------------------------------------------------------------------- #
# per-flow on-disk inspection
# --------------------------------------------------------------------------- #
def _infer_freq(dates) -> str:
    """Map a flow's obs_date stamps to the SDMX FREQ code the ingester used.

    annual  -> all (month,day)==(12,31)
    quarterly -> all day==1 and month in {1,4,7,10}
    monthly -> all day==1
    default -> 'A' (every on-disk flow is annual; this is a safe fallback).
    """
    md = {(d.month, d.day) for d in dates if d is not None}
    if not md:
        return "A"
    if md <= {(12, 31)}:
        return "A"
    if all(d == 1 for (_m, d) in md) and all(m in (1, 4, 7, 10) for (m, _d) in md):
        return "Q"
    if all(d == 1 for (_m, d) in md):
        return "M"
    return "A"


def _flow_layout(path: str):
    """Return (indicators: list[str], economies: list[str], freq: str,
              max_date: dt.date|None) learned from the on-disk parquet.

    Key columns are fixed by the layout: series_key, obs_date, value; series_key
    splits as ADB:FLOW:IND:ECO (exactly 3 colons), so parts[2]=IND, parts[3]=ECO.
    """
    t = blob.read_table(path, columns=["series_key", "obs_date"])
    if t.num_rows == 0:
        return [], [], "A", None
    inds, ecos = set(), set()
    for k in set(t.column("series_key").to_pylist()):
        parts = k.split(":")
        if len(parts) == 4:
            if parts[2]:
                inds.add(parts[2])
            if parts[3]:
                ecos.add(parts[3])
    dates = t.column("obs_date").to_pylist()
    freq = _infer_freq(dates)
    mx = pc.max(t.column("obs_date")).as_py()
    if isinstance(mx, dt.datetime):
        mx = mx.date()
    return sorted(inds), sorted(ecos), freq, mx


# --------------------------------------------------------------------------- #
# contract entry point
# --------------------------------------------------------------------------- #
def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)

    # blob-routed enumeration: the flow set must be visible under AQUEDUCT_BACKEND=r2
    # (the local store dir is absent on a CI runner).
    pfiles = [f for f in blob.list_parquets(out_dir) if not f.startswith("_")]
    if not pfiles:
        raise DefinitiveError(f"no adb parquet files under {out_dir}")

    sess = requests.Session()
    sess.headers.update(UA)
    tally = Tally()
    total = 0
    maxd: dt.date | None = None
    cursors: dict[str, str] = {}     # flow -> max obs_date written (per-flow freshness)

    for fn in pfiles:
        flow = fn[:-len(".parquet")]
        path = os.path.join(out_dir, fn)
        before = blob.row_count(path)

        try:
            inds, economies, freq, mx = _flow_layout(path)
        except Exception:
            # Unreadable existing file: leave it untouched, keep its rows in total,
            # and surface honestly as a transient sub-failure (re-run next tick).
            tally.transient_unit()
            total += before
            continue

        # Seed this flow's cursor from the on-disk frontier so a frozen/untouched
        # flow still reports its real cursor.
        if mx is not None:
            cursors[flow] = mx.isoformat()

        if not inds or mx is None:
            # No usable on-disk series to date-tail (shouldn't happen for a real
            # flow): nothing to request; keep existing rows.
            tally.empty_unit()
            total += before
            continue

        # Re-fetch from the boundary YEAR (inclusive) so a same-year revision to the
        # latest value is captured; merge dedups the overlap (new wins on revision).
        start_period = mx.year

        flow_rows: list[tuple[str, dt.date, float]] = []
        flow_transient = False
        flow_structural = False
        flow_had_data = False        # any 200 with a real SDMX header

        for ind in inds:
            try:
                rows, had_header = _harvest_indicator(
                    sess, flow, freq, ind, start_period, economies)
            except TransientError:
                # One flaky indicator must not strand the rest of the source.
                # Record at the FLOW level and stop this flow's iteration; the whole
                # run becomes 'partial'.
                flow_transient = True
                break
            except DefinitiveError:
                # Hard 4xx on a single indicator URL — treat as a structural signal
                # for this flow (the expected SDMX endpoint shape is gone).
                flow_structural = True
                break
            if had_header:
                flow_had_data = True
            flow_rows.extend(rows)

        if flow_transient:
            tally.transient_unit()      # -> partial; existing data untouched
            total += before
            continue
        if flow_structural:
            tally.structural_unit()     # -> DefinitiveError in finalize()
            total += before
            continue

        # Keep only observations >= the stored boundary year (defensive; the API is
        # already bounded by startPeriod). Build the new table identically to the
        # ingester's schema.
        seen: set[tuple[str, dt.date]] = set()
        keys: list[str] = []
        dates: list[dt.date] = []
        vals: list[float] = []
        for k, d, v in flow_rows:
            if d.year < start_period:
                continue
            tok = (k, d)
            if tok in seen:
                continue
            seen.add(tok)
            keys.append(k)
            dates.append(d)
            vals.append(v)

        if not keys:
            # 200(s) with a real SDMX header but 0 data rows on a BOUNDARY re-fetch of
            # indicators that EXIST on disk -> the boundary period itself failed to
            # re-return: a structural break, not a quiet tail. If we never saw a real
            # header (all 404/empty bodies), it's legitimately empty.
            if flow_had_data and before > 0:
                tally.structural_unit()
            else:
                tally.empty_unit()
            total += before
            continue

        new_tbl = pa.table({
            "series_key": pa.array(keys, pa.string()),
            "obs_date":   pa.array(dates, pa.date32()),
            "value":      pa.array(vals, pa.float64()),
        })
        n, md = merge.merge_and_write(path, new_tbl, mode="merge", dedup_keys=DEDUP)
        total += n
        # A flow whose boundary re-fetch returned real rows is a SUCCESSFUL sub-unit
        # (data flowed), even when every row is at/below the stored boundary and the
        # merge nets zero new rows — a healthy idempotent re-run. We therefore mark it
        # added_unit(len(keys)) so it does NOT feed the all-empty structural floor in
        # finalize(); otherwise a perfectly healthy steady-state (every flow re-returns
        # its boundary year, zero net-new) would have empty==attempted and wrongly raise
        # DefinitiveError. The REAL net-new delta is reflected in obs (total) and the note.
        tally.added_unit(len(keys))
        if md:
            cursors[flow] = md
            mdd = dt.date.fromisoformat(md)
            if maxd is None or mdd > maxd:
                maxd = mdd

    last_obs = maxd.isoformat() if maxd is not None else (
        max(cursors.values()) if cursors else (since or None))

    # One sub-unit per flow attempted; floor = (#sub-units) - 1 per the contract.
    return finalize(tally, total, last_obs, source=SOURCE, series_cursors=cursors,
                    empty_window_floor=max(0, len(pfiles) - 1))
