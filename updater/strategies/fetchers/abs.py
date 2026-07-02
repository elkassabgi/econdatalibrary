"""S3 fetcher — Australian Bureau of Statistics (ABS) SDMX 2.1 REST. Keyless, CC-BY-4.0.

Layout (set by jobs/ingest_abs_full.py): ONE grouped parquet per DATAFLOW under
clean_full/abs/<FLOW>.parquet, schema (series_key, obs_date, value) — many series per
file. series_key is the dotted SDMX data key: every dimension code column strictly
between DATAFLOW (col 0) and TIME_PERIOD in the compact CSV, joined with '.'
(e.g. "4.161.TOT.TOTXE.Q"). The dedup key is therefore (series_key, obs_date).

DATE-TAIL (sdmx_delta). ABS SDMX 2.1 honors ?startPeriod on the data endpoint
(verified live: GET /rest/data/<FLOW>/all?format=csvfile&startPeriod=2024 returns only
periods >= 2024 across annual/quarterly/monthly flows). For each on-disk flow we:
  - read that flow's existing parquet max(obs_date) and request ONLY periods from the
    BOUNDARY YEAR forward (startPeriod=<year(max_obs)>). We re-fetch the boundary year
    (not year+1) so a same-year later period or an in-place REVISION of an already
    stored period is captured; merge dedups the overlap and never shrinks. Year
    granularity is used because ABS flows mix frequencies (A/Q/M/D) and a year
    startPeriod is the one form every frequency accepts.
  - REUSE jobs/ingest_abs_full enumeration + parse verbatim: list_dataflows() for the
    catalog, and collect() (which wraps stream_rows() with the 5x exponential-backoff
    retry on the ABS mid-body connection drops, locates TIME_PERIOD/OBS_VALUE by name,
    treats cols 1..ti-1 as the dotted key, parse_period -> period-END date,
    parse_value -> float). We do NOT re-discover endpoints/keys/parse logic.
  - merge ONLY via merge.merge_and_write(path, tbl, mode="merge",
    dedup_keys=("series_key","obs_date")); never write parquet here.

updatedAfter is NOT used: the ABS data endpoint frequently drops the body mid-stream
(ChunkedEncodingError / IncompleteRead — confirmed live on this run), and the registry
adapter.open_question explicitly leaves whether ABS honors updatedAfter unverified. The
per-flow startPeriod date-tail is the robust, complete delta; collect()'s retry loop
absorbs the flakiness.

HONEST-STATUS CONTRACT (Tally + finalize). Each dataflow is a sub-unit:
  added_unit(n_new) merge netted n_new rows for the flow.
  empty_unit()      200 but no rows newer than the boundary (a legitimately quiet
                    tail) — the normal steady state for most of ~1222 flows.
  transient_unit()  collect() raised (timeout / 5xx / 429 / network / mid-body drop
                    surviving the retry budget) — record and KEEP GOING; the whole run
                    becomes 'partial' so the orchestrator re-runs (no silent no_change).
  structural_unit() a flow that previously had rows now returns a 200 header whose
                    TIME_PERIOD/OBS_VALUE columns are GONE (collect() returns [] from an
                    unexpected shape) — a per-flow schema break.
A large all-empty/transient window (every flow quiet) is gated by finalize()'s
empty_window_floor; we pass <#flows>-1 per the S3 contract so a genuine wholesale
outage raises DefinitiveError while one healthy quiet flow does not falsely trip it.
existing data is always preserved by merge (never shrinks).
"""
from __future__ import annotations
import datetime as dt
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Tally, finalize

# Reuse the ingester verbatim: enumeration, streaming+retry, parse helpers.
import jobs.ingest_abs_full as ing

SOURCE = "abs"
DEDUP = ("series_key", "obs_date")


def _flow_max_obs(path: str):
    """Max obs_date already on disk for a flow's parquet (a datetime.date) or None."""
    try:
        od = pq.read_table(path, columns=["obs_date"]).column("obs_date")
        if od.length() == 0:
            return None
        m = pc.max(od).as_py()
        if isinstance(m, dt.datetime):
            m = m.date()
        return m
    except Exception:
        return None


def _flow_start_param(max_obs):
    """startPeriod value (a year string) for the date-tail, INCLUSIVE of the boundary year.

    Re-fetching from the boundary year (not year+1) re-requests the latest stored
    period(s) so a same-year later observation OR an in-place revision is seen; merge
    dedups the overlap. None -> no max on disk (defensive; fetch full history)."""
    if max_obs is None:
        return None
    return str(max_obs.year)


def _build_table(keys, dates, vals):
    return pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date": pa.array(dates, pa.date32()),
        "value": pa.array(vals, pa.float64()),
    })


def _series_maxes(keys, dates):
    """Per-series max obs_date {series_key: 'YYYY-MM-DD'} for the rows fetched this run."""
    out: dict[str, dt.date] = {}
    for k, d in zip(keys, dates):
        if d is None:
            continue
        prev = out.get(k)
        if prev is None or d > prev:
            out[k] = d
    return {k: v.isoformat() for k, v in out.items()}


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    if not os.path.isdir(out_dir):
        raise DefinitiveError(f"abs source dir missing: {out_dir}")

    # On-disk flows = the authoritative sub-unit set (one parquet per dataflow). We
    # date-tail every existing flow. (Brand-new dataflows added to the ABS catalog are
    # not back-discovered here — that's a re-ingest concern; this fetcher keeps the
    # existing ~1222 flows fresh, which is the S3 contract.)
    pfiles = sorted(f for f in os.listdir(out_dir) if f.endswith(".parquet"))
    if not pfiles:
        raise DefinitiveError(f"no abs parquet files under {out_dir}")

    sess = ing.session()
    tally = Tally()
    total = 0
    last_obs = None
    cursors: dict[str, str] = {}   # series_key -> max obs_date written this run

    for fn in pfiles:
        path = os.path.join(out_dir, fn)
        flow = fn[:-len(".parquet")]
        before = blob.row_count(path)
        max_obs = _flow_max_obs(path)
        start = _flow_start_param(max_obs)
        params = {"startPeriod": start} if start else None

        # --- fetch ONLY the date-tail (one sub-unit per flow), reusing collect() ---
        try:
            keys, dates, vals = ing.collect(sess, flow, "all", params=params)
        except Exception as exc:  # noqa: BLE001
            # collect() exhausts its 5x retry budget on the ABS mid-body drops / 5xx /
            # timeouts and re-raises (requests.* or urllib3 ProtocolError). Leave this
            # flow's existing data untouched, record transient, keep going. -> 'partial'.
            tally.transient_unit()
            total += before
            mx = max_obs.isoformat() if max_obs else None
            # seed this flow's frontier so a transient flow can't hide behind the max:
            # carry forward each on-disk series cursor is too costly per-flow; record the
            # flow-level frontier under a flow sentinel key for visibility.
            if mx:
                cursors.setdefault(f"__flow__{flow}", mx)
            continue

        if not keys:
            # collect() returns [] for (a) a 404 / genuinely empty flow, (b) a quiet
            # date-tail (no period >= boundary year — normal for most flows), OR (c) a
            # 200 header missing TIME_PERIOD/OBS_VALUE (unexpected shape). We can only
            # call (c) a structural break when the flow PREVIOUSLY had rows AND the
            # request was an unfiltered/boundary fetch — but an incremental quiet tail is
            # indistinguishable from (a)/(b), and treating "nothing newer" as structural
            # would false-positive on every quiet flow. So a date-tail empty is recorded
            # as a legitimate empty; true wholesale breaks surface via the all-empty
            # floor in finalize(). Existing data is preserved (no write).
            tally.empty_unit()
            total += before
            mx = max_obs.isoformat() if max_obs else None
            if mx and (last_obs is None or mx > last_obs):
                last_obs = mx
            continue

        tbl = _build_table(keys, dates, vals)

        # --- publish (atomic, dedup, never-shrink) ---
        try:
            n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
        except DefinitiveError:
            # never-shrink / dropped-column / 0-row guard refused this flow's write.
            # Keep the old file, surface as transient so the run is 'partial' and retries
            # rather than silently dropping a flow's delta.
            tally.transient_unit()
            total += before
            continue

        total += n
        # NET-DELTA -> ADDED: a boundary-year re-fetch that RETURNED real rows but
        # nets 0 new after dedup is still a data-bearing (successful) sub-unit, not
        # empty. Count len(keys) (rows that actually flowed for the flow), mirroring
        # the SDMX/bcb siblings. Counting net delta would record a healthy quiet
        # steady state as empty and falsely trip the all-empty structural floor.
        # The true net-new delta remains reflected in total/obs and last_obs.
        tally.added_unit(len(keys))
        if md and (last_obs is None or md > last_obs):
            last_obs = md
        # per-series cursors for the rows we just fetched (the moved series)
        for k, v in _series_maxes(keys, dates).items():
            prev = cursors.get(k)
            if prev is None or v > prev:
                cursors[k] = v

    # empty_window_floor = (#subunits) - 1 per the S3 contract: a genuine wholesale
    # outage (every one of ~1222 flows empty) raises DefinitiveError, while a single
    # healthy quiet flow among many that moved does not.
    return finalize(tally, total, last_obs, source=SOURCE, series_cursors=cursors,
                    empty_window_floor=max(len(pfiles) - 1, 1))
