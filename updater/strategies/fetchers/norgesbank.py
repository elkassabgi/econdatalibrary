"""S3 (sdmx_delta) fetcher — Norges Bank (Norwegian Central Bank) SDMX-JSON API.

No API key (data.norges-bank.no is open). Public open data under Norges Bank terms.

LAYOUT THIS FETCHER OWNS
------------------------
ONE combined parquet: data/clean_full/norgesbank/norgesbank.parquet, written by
jobs/ingest_norgesbank.py, schema (series_key, obs_date, value) where
  series_key = "{FLOW}:{dim0.dim1.dim2...}"   (flow prefix, dot-joined dimension ids)
  obs_date   = date32  (daily YYYY-MM-DD, monthly -> day 1, annual -> Dec 31)
  value      = float64

The directory ALSO holds many sibling per-dataflow files (EXR.parquet,
MONEY_MARKET.parquet, ...) with a DIFFERENT schema ("FREQ=B:BASE_CUR=..." keys).
Those come from a SEPARATE/older ingester and are NOT this source's files — we never
touch them (the registry's out_paths_note flags them explicitly). We read & write
ONLY norgesbank.parquet.

SUB-UNITS = DATAFLOWS
---------------------
Each SDMX dataflow is a sub-unit. We process the UNION of:
  * the ingester's curated DATAFLOWS list (reused verbatim, not re-discovered), and
  * every flow already present in norgesbank.parquet
so we extend each existing flow AND backfill any curated flow missing from disk.

INCREMENTAL (date-tail)
-----------------------
The API supports SDMX 2.1 ?startPeriod=YYYY-MM-DD (verified live: a tight startPeriod
returns ONLY observations from that date forward). It has NO ?updatedAfter. So per
flow we read that flow's max(obs_date) from the combined parquet and request
  GET /api/data/{FLOW}?format=sdmx-json&startPeriod=<max_obs>     (INCLUSIVE boundary)
We anchor startPeriod AT the stored max (not max+1) for two reasons:
  1. an active flow re-surfaces its current/last period, so an in-place REVISION of
     the latest value is captured (merge dedups the overlap, new vintage wins); and
  2. it keeps a healthy idempotent re-run HONEST — a quiet flow still returns its
     boundary observation(s), so it's a SUCCESSFUL data-bearing sub-unit (added_unit,
     net-zero after dedup) rather than a 404/empty. If we used max+1, every flow would
     404 on a same-day re-run and the all-empty floor would FALSE-POSITIVE a structural
     break. startPeriod accepts a full ISO date for every frequency (daily/monthly/
     quarterly/annual) and the API floors it to the right granule. A flow with NO
     on-disk history is fetched in FULL (no startPeriod) as a first-time backfill.

PARSE
-----
We reuse the ingester's exact SDMX-JSON parser (jobs/ingest_norgesbank.py
parse_sdmx_json) so the series_key string and value/date handling are byte-identical
to what produced the on-disk data — merge dedup keys line up perfectly. (That parser
only understands ISO date granules; quarterly time strings like "2025-Q3" yield 0
parseable rows, which is why REGNET — a quarterly flow — has never landed in the
combined file. That is a legitimate parser limitation, NOT a structural break: a flow
whose envelope advertises ONLY non-ISO time granules (e.g. quarterly) is recorded
EMPTY when it parses 0 rows. BUT — finding S3-B2 — an ON-DISK flow whose 200 envelope
DOES carry series (dataSets[0].series non-empty) AND advertises an ISO-parseable time
granule, yet still parses to 0 rows, is a genuine parser/schema break (parse_sdmx_json
swallows ALL exceptions to a 0-row return): that case is recorded STRUCTURAL — see
below.)

HONEST STATUS (Tally + finalize)
--------------------------------
Per flow we record on a Tally:
  added_unit(n)     n new rows merged (n>0 ok, n==0 quiet tail)
  empty_unit()      200 with a valid SDMX envelope but no new observations in the tail
                    window (incl. 404/400 "nothing after the boundary" and the
                    quarterly-parser-yields-0 case)
  transient_unit()  timeout / 5xx / 429 / network drop / non-JSON 200 -> WHOLE run partial
  structural_unit() 200 whose SDMX envelope itself is GONE (no data.structure /
                    no data.dataSets on a FULL fetch of a previously-populated flow),
                    OR an ON-DISK flow whose 200 envelope carries a non-empty series
                    block with an ISO-parseable time granule yet parses to 0 rows (a
                    parser/schema break the inclusive boundary should never produce)
                    -> finalize raises DefinitiveError
finalize() => 'partial' on ANY transient (orchestrator does NOT stamp last_success;
unit re-runs), DefinitiveError on a structural break or a large all-empty window, else
'ok' (added>0) / 'no_change'. merge_and_write never shrinks, so existing data is always
preserved regardless of status.
"""
from __future__ import annotations

import datetime as dt
import os
import time

import pyarrow as pa
import pyarrow.compute as pc
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Tally, finalize

SOURCE = "norgesbank"
BASE = "https://data.norges-bank.no/api"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
DEDUP = ("series_key", "obs_date")
RATE = 0.6           # polite gap between flows
TIMEOUT = 120
MAX_ATTEMPTS = 4
PARQUET_NAME = "norgesbank.parquet"

# Curated dataflow list — copied verbatim from jobs/ingest_norgesbank.py (do not
# re-discover). We process the UNION of this and whatever flows are already on disk.
DATAFLOWS = [
    "EXR",
    "FINANCIAL_INDICATORS",
    "GOVT_GENERIC_RATES",
    "GOVT_KEYFIGURES",
    "GOVT_ZEROCOUPON",
    "IR",
    "MONEY_MARKET",
    "SHORT_RATES",
    "REGNET",
    "LIQUIDITY_STATISTICS",
]


# --------------------------------------------------------------------------- #
# SDMX-JSON parser — reused VERBATIM from jobs/ingest_norgesbank.py so series_key,
# value and date handling are byte-identical to the on-disk data.
# --------------------------------------------------------------------------- #
def parse_sdmx_json(data: dict, flow_id: str) -> list[tuple[str, dt.date, float]]:
    """Parse SDMX-JSON response into (series_key, date, value) tuples."""
    results: list[tuple[str, dt.date, float]] = []
    try:
        structure = data.get("data", {}).get("structure", {})
        dims = structure.get("dimensions", {})

        series_dims = dims.get("series", [])
        obs_dims = dims.get("observation", [])

        time_dim = obs_dims[0] if obs_dims else None
        time_values = [v.get("id", "") for v in (time_dim.get("values", []) if time_dim else [])]

        datasets = data.get("data", {}).get("dataSets", [])
        if not datasets:
            return results

        ds = datasets[0]
        series_dict = ds.get("series", {})

        for series_key_str, series_data in series_dict.items():
            indices = [int(x) for x in series_key_str.split(":")]
            label_parts = []
            for dim_idx, pos in enumerate(indices):
                if dim_idx < len(series_dims):
                    dim = series_dims[dim_idx]
                    vals = dim.get("values", [])
                    if pos < len(vals):
                        label_parts.append(vals[pos].get("id", str(pos)))
            series_label = (f"{flow_id}:" + ".".join(label_parts)
                            if label_parts else f"{flow_id}:{series_key_str}")

            obs = series_data.get("observations", {})
            for obs_idx_str, obs_val in obs.items():
                obs_idx = int(obs_idx_str)
                if obs_idx >= len(time_values):
                    continue
                time_str = time_values[obs_idx]
                v_raw = obs_val[0] if isinstance(obs_val, list) and obs_val else obs_val

                if v_raw is None:
                    continue
                try:
                    v = float(v_raw)
                    if v != v:        # NaN
                        continue
                except (ValueError, TypeError):
                    continue

                d = None
                try:
                    if len(time_str) == 10:
                        d = dt.date.fromisoformat(time_str)
                    elif len(time_str) == 7:
                        d = dt.date(int(time_str[:4]), int(time_str[5:7]), 1)
                    elif len(time_str) == 4:
                        d = dt.date(int(time_str), 12, 31)
                    elif len(time_str) > 10:
                        d = dt.date.fromisoformat(time_str[:10])
                except (ValueError, TypeError):
                    pass

                if d is None:
                    continue
                results.append((series_label, d, v))
    except Exception:
        # A genuine parse explosion on a 200 body is treated by the caller as a
        # structural signal (envelope present but unusable); re-raise nothing here,
        # let the caller see 0 rows + a present envelope and classify.
        return results
    return results


# --------------------------------------------------------------------------- #
# HTTP — honest transient/definitive classification (the ingester's get_json
# swallowed everything to None, which would launder a timeout into "no data";
# we must not).
# --------------------------------------------------------------------------- #
def _get_json(sess: requests.Session, url: str):
    """GET an SDMX-JSON dataflow.

    Returns:
      dict   -> parsed JSON body (HTTP 200)
      None   -> HTTP 404/400 (no data at/after the requested startPeriod — the API
                returns 404 when a flow has no observation in the window; legitimately
                empty for an incremental tail)
    Raises:
      TransientError  -> timeout / connection drop / 429 / 5xx / non-JSON 200, after
                         the retry budget
      DefinitiveError -> other hard 4xx (401/403/...)
    """
    last = None
    for attempt in range(MAX_ATTEMPTS):
        try:
            r = sess.get(url, headers=UA, timeout=TIMEOUT)
        except (requests.Timeout, requests.ConnectionError) as e:
            last = str(e)[:120]
            if attempt == MAX_ATTEMPTS - 1:
                raise TransientError(f"norgesbank GET {url[-70:]}: {last}")
            time.sleep(min(5 * (attempt + 1), 30))
            continue
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError as e:
                last = f"bad json: {e}"
                if attempt == MAX_ATTEMPTS - 1:
                    raise TransientError(f"norgesbank GET {url[-70:]}: {last}")
                time.sleep(min(5 * (attempt + 1), 30))
                continue
        if r.status_code in (400, 404):
            # No observations at/after startPeriod for this flow -> empty window.
            return None
        if r.status_code in (429, 500, 502, 503, 504):
            last = f"HTTP {r.status_code}"
            if attempt == MAX_ATTEMPTS - 1:
                raise TransientError(f"norgesbank GET {url[-70:]}: {last}")
            time.sleep(min(5 * (attempt + 1), 30))
            continue
        # other hard 4xx (auth, etc.)
        raise DefinitiveError(f"norgesbank GET {url[-70:]}: HTTP {r.status_code}")
    raise TransientError(f"norgesbank GET {url[-70:]}: {last}")


# --------------------------------------------------------------------------- #
# on-disk frontier
# --------------------------------------------------------------------------- #
def _flow_max_by_flow(path: str) -> dict[str, dt.date]:
    """Per-FLOW max(obs_date) from the combined parquet, keyed by flow prefix
    (the part of series_key before the first ':')."""
    out: dict[str, dt.date] = {}
    if not blob.exists(path):
        return out
    t = blob.read_table(path)
    if t.num_rows == 0 or "series_key" not in t.column_names:
        return out
    keys = t.column("series_key").to_pylist()
    dates = t.column("obs_date").to_pylist()
    for k, d in zip(keys, dates):
        if not k:
            continue
        flow = k.split(":", 1)[0]
        if isinstance(d, dt.datetime):
            d = d.date()
        if d is None:
            continue
        prev = out.get(flow)
        if prev is None or d > prev:
            out[flow] = d
    return out


def _envelope_ok(data: dict) -> bool:
    """A 200 body that still has the SDMX envelope (structure + dataSets keys).
    Used to tell a structural break (envelope gone) from a quiet-but-valid tail."""
    inner = data.get("data", {}) if isinstance(data, dict) else {}
    return ("structure" in inner) and ("dataSets" in inner)


def _has_nonempty_series(data: dict) -> bool:
    """True iff the SDMX envelope actually carries series data:
    data.dataSets[0].series is a non-empty dict. A healthy active flow re-fetched
    on its INCLUSIVE boundary MUST surface >=1 series (it already has on-disk rows),
    so a non-empty series block that parses to 0 rows is a real parser/schema break,
    whereas an EMPTY series block is a genuinely quiet 200 tail (still just empty)."""
    if not isinstance(data, dict):
        return False
    datasets = data.get("data", {}).get("dataSets", [])
    if not datasets or not isinstance(datasets, list):
        return False
    ds = datasets[0] if isinstance(datasets[0], dict) else {}
    series = ds.get("series", {})
    return isinstance(series, dict) and len(series) > 0


def _has_iso_parseable_time(data: dict) -> bool:
    """True iff the observation time dimension advertises at least one granule the
    parser's ISO-only date logic can turn into a date (len 10 / 7 / 4, or >10 ISO).

    This is the discriminator that keeps REGNET (and any quarterly/non-ISO flow)
    OUT of the structural bucket: quarterly time codes like '2025-Q3' (len 7 but the
    [5:7] slice 'Q3' is non-numeric) are NOT parseable by parse_sdmx_json, so such a
    flow legitimately yields 0 rows from a non-empty series block — that is the
    documented parser limitation, NOT a structural break. Only when the envelope DOES
    advertise an ISO-parseable granule yet the parser still produces 0 rows do we have
    a genuine schema/parser break worth flagging structural."""
    if not isinstance(data, dict):
        return False
    obs_dims = data.get("data", {}).get("structure", {}).get("dimensions", {}).get("observation", [])
    time_dim = obs_dims[0] if obs_dims else None
    time_values = [v.get("id", "") for v in (time_dim.get("values", []) if time_dim else [])]
    for ts in time_values:
        try:
            if len(ts) == 10:
                dt.date.fromisoformat(ts)
                return True
            elif len(ts) == 7:
                dt.date(int(ts[:4]), int(ts[5:7]), 1)
                return True
            elif len(ts) == 4:
                dt.date(int(ts), 12, 31)
                return True
            elif len(ts) > 10:
                dt.date.fromisoformat(ts[:10])
                return True
        except (ValueError, TypeError):
            continue
    return False


# --------------------------------------------------------------------------- #
# contract entry point
# --------------------------------------------------------------------------- #
def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, PARQUET_NAME)
    before = blob.row_count(path)

    flow_max = _flow_max_by_flow(path)
    # Sub-units = curated DATAFLOWS UNION flows already on disk (extend everything
    # present, backfill any curated flow that's missing).
    flows = list(dict.fromkeys(DATAFLOWS + sorted(flow_max.keys())))

    sess = requests.Session()
    tally = Tally()
    cursors: dict[str, str] = {}   # flow -> max obs_date (per-flow freshness)

    # Seed cursors from the on-disk frontier so a flow we don't advance this run
    # still reports its real cursor (a frozen flow can't hide behind the unit max).
    for fl, md in flow_max.items():
        cursors[fl] = md.isoformat()

    all_keys: list[str] = []
    all_dates: list[dt.date] = []
    all_vals: list[float] = []
    maxd: dt.date | None = None

    for flow in flows:
        fmax = flow_max.get(flow)
        url = f"{BASE}/data/{flow}?format=sdmx-json"
        full_fetch = fmax is None
        if not full_fetch:
            # INCLUSIVE boundary: anchor AT the stored max so an active flow re-returns
            # its last period (captures same-period revisions AND keeps a quiet re-run
            # honest as a data-bearing sub-unit). startPeriod takes a full ISO date for
            # every frequency and the API floors it to the right granule.
            url += f"&startPeriod={fmax.isoformat()}"

        try:
            data = _get_json(sess, url)
        except TransientError:
            # Leave this flow's existing rows untouched; record & keep going so one
            # flaky flow can't strand the rest -> run becomes 'partial'.
            tally.transient_unit()
            time.sleep(RATE)
            continue

        if data is None:
            # 404/400: nothing at/after the boundary -> legitimately empty tail.
            tally.empty_unit()
            time.sleep(RATE)
            continue

        # 200 body. If this was a FULL fetch of a previously-populated flow and the
        # SDMX envelope itself is gone, that's a structural/schema break.
        if full_fetch and not _envelope_ok(data):
            # full fetch of a flow we have no history for: an absent envelope just
            # means the flow doesn't exist / is empty upstream -> empty (not structural,
            # since we never had it). A missing envelope on a flow WE HAD history for
            # can't happen here (full_fetch only when fmax is None).
            tally.empty_unit()
            time.sleep(RATE)
            continue

        results = parse_sdmx_json(data, flow)

        if not results:
            # 200 + valid envelope but 0 parseable observations. Decide empty vs
            # structural HONESTLY (finding S3-B2):
            #
            #  * ON-DISK flow (not full_fetch): startPeriod is INCLUSIVE, so a healthy
            #    active flow MUST re-surface >=1 boundary observation. If the envelope
            #    actually carries series (dataSets[0].series non-empty) AND advertises
            #    an ISO-parseable time granule, yet the parser produced 0 rows, that is
            #    a real parser/schema break (parse_sdmx_json swallows ALL exceptions to
            #    a 0-row return) -> structural_unit() so finalize raises DefinitiveError.
            #  * The quarterly REGNET case (non-empty series whose time codes are
            #    '2025-Q3' etc.) advertises NO ISO-parseable granule, so it is excluded
            #    here and stays EMPTY — the documented parser limitation, not a break.
            #  * An EMPTY series block (no series at all) is a genuinely quiet 200 tail.
            #  * A FULL-fetch flow (no on-disk history) has no inclusive-boundary
            #    guarantee, so a 0-row body just means nothing landed -> empty.
            if (not full_fetch
                    and _has_nonempty_series(data)
                    and _has_iso_parseable_time(data)):
                tally.structural_unit()
            else:
                tally.empty_unit()
            time.sleep(RATE)
            continue

        n_seen = len(results)
        f_max_seen = max(d for _, d, _ in results)
        for k, d, v in results:
            all_keys.append(k)
            all_dates.append(d)
            all_vals.append(v)

        # provisional per-flow cursor (advanced again post-merge below)
        prev = cursors.get(flow)
        fm_iso = f_max_seen.isoformat()
        if prev is None or fm_iso > prev:
            cursors[flow] = fm_iso
        if maxd is None or f_max_seen > maxd:
            maxd = f_max_seen
        # Mark this flow as a SUCCESSFUL data-bearing sub-unit (rows flowed), even if
        # every row is at/below the boundary and the merge nets zero new (a healthy
        # idempotent re-run). Net-new is reflected in the final obs count.
        tally.added_unit(n_seen)
        time.sleep(RATE)

    last_obs = (maxd.isoformat() if maxd
                else (max(cursors.values()) if cursors else (since or None)))

    if not all_vals:
        # Nothing fetched. finalize() decides honest status: 'partial' if any flow
        # transient-failed; DefinitiveError on a large all-empty window; else
        # 'no_change' reporting the real on-disk frontier. Nothing merged means
        # nothing served went stale: report an EMPTY merge-measured changed set
        # (coherence met) rather than falling back to the flow-grain cursor keys,
        # which cannot map and would book unmapped-residue noise every quiet run.
        res = finalize(tally, before, last_obs, source=SOURCE,
                       series_cursors=cursors,
                       empty_window_floor=max(len(flows) - 1, 1))
        res.changed_keys = {}
        return res

    new_tbl = pa.table({
        "series_key": pa.array(all_keys, pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals, pa.float64()),
    })

    # THE CURSOR-CONTRACT PILOT (2026-08-31, PHASE3 brief step 4). norgesbank's cursor
    # keys are FLOW ids while its catalogue is SERIES grain (35,135 ids like
    # norgesbank:EXR:A.AUD.NOK.SP), so no §5.7 tier could ever map a changed key and NO
    # CSV was ever re-derived through the daily path — a measured 23-day live coherence
    # gap. The merge-measured channel fixes the GRAIN at the source: report_changed_keys
    # returns {store series_key: max changed obs_date} for exactly the keys whose served
    # value this merge changed (extension OR same-period revision — the startPeriod
    # boundary anchor exists to catch revisions, and max-date cursors cannot see them),
    # and store keys ARE catalogue keys here, so the mapper's exact tier hits every one.
    # `cursors` (per-FLOW frontier) keeps its health/freshness contract untouched.
    # Cardinality is bounded by the store (~36k distinct) — far under the 2M cap.
    n, merged_max, changed = merge.merge_and_write(
        path, new_tbl, mode="merge", dedup_keys=DEDUP, report_changed_keys=True)
    if merged_max and (last_obs is None or merged_max > last_obs):
        last_obs = merged_max

    # finalize honors transient -> 'partial' even though rows were merged.
    res = finalize(tally, n, last_obs, source=SOURCE, series_cursors=cursors,
                   empty_window_floor=max(len(flows) - 1, 1))
    res.changed_keys = changed
    return res
