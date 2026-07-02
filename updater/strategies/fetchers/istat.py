"""S3 (sdmx_delta) fetcher — ISTAT Italy. Keyless open data (CC BY 3.0 IT).

Layout (set by jobs/ingest_sdmx_nso.py + jobs/ingest_istat_sliced.py, SHARED dir):
ONE parquet per dataflow under clean_full/istat/<flow_id>.parquet, schema
  series_key : string  -- "DIM=val:DIM2=val2:..." over the flow's dimensions
                          (built by parse_sdmx_csv / parse_sdmx_xml; FREQ=A|M|Q...)
  obs_date   : date32  -- SDMX TIME_PERIOD normalized (annual->Dec-31, monthly->1st,
                          quarterly->first month of quarter, ...)
  value      : float64
The on-disk filename stem IS the SDMX dataflow id requested as data/IT1,<flow_id>/.

Each parquet (dataflow) is a SUB-UNIT. Per sub-unit this fetcher:
  - reads the existing parquet's max(obs_date) and asks ISTAT for ONLY newer
    observations via the SDMX 2.1 REST date-tail:  ?startPeriod=<year of max_obs>.
    We use the YEAR of the stored max as startPeriod (not the exact day): SDMX
    startPeriod accepts a bare year and ISTAT clamps to the flow's real range, so
    re-sweeping the boundary year is cheap AND catches same-year late obs / in-place
    revisions to the latest period — merge dedups the overlap on (series_key,obs_date).
    ISTAT exposes no `updatedAfter`, so a year date-tail is the honest incremental.
  - REUSES jobs/ingest_sdmx_nso.py verbatim for endpoints/agency/Accept and for the
    CSV/XML parse, so the keys produced line up byte-for-byte with the published files.
  - tries BOTH hosts (sdmx.istat.it fast classical host, esploradati.istat.it the
    granular DF_* host); a flow is served by whichever host carries it. A host that
    500s / 404s for a flow is just "this host doesn't serve it / no data here"; a flow
    is only TRANSIENT when EVERY host fails it with a timeout/5xx/conn error.
  - merges ONLY via merge.merge_and_write(path, tbl, mode="merge",
    dedup_keys=("series_key","obs_date")) — never writes parquet itself, never shrinks.

Honest status (Tally + finalize):
  added_unit(n)     -- newer rows merged for the flow
  empty_unit()      -- 200/404 "no records" in the window (legitimately nothing newer)
  transient_unit()  -- every host failed the flow (timeout/5xx/429/conn) -> run 'partial'
  structural_unit() -- a 200 with a real (non-trivial) body that parsed 0 rows on a
                       FULL (unfiltered) re-fetch of a previously-populated flow
A transient sub-failure makes the WHOLE run 'partial' (never silent no_change); a
structural break raises DefinitiveError; existing data is always preserved by merge.

NOTE: this is a LARGE source (~755 flows). The year date-tail keeps each request tiny
(only new periods), but the orchestrator run processes ALL flows. series_cursors carry
each flow's max obs_date so a frozen flow can't hide behind the unit-level max.
"""
from __future__ import annotations
import datetime as dt
import os
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests

from ... import config, blob, merge
from ...errors import TransientError
from ..base import Result
from ._common import Tally, finalize

# Reuse the ingester's UA + parsers verbatim (jobs/ is on sys.path for the orchestrator
# and the live-test; fall back to a by-path load otherwise).
try:
    from jobs.ingest_sdmx_nso import (  # type: ignore
        UA, parse_sdmx_csv, parse_sdmx_xml,
    )
except ImportError:  # pragma: no cover - path fallback
    import importlib.util as _ilu

    _src = os.path.join(config.ROOT, "jobs", "ingest_sdmx_nso.py")
    _spec = _ilu.spec_from_file_location("ingest_sdmx_nso", _src)
    _mod = _ilu.module_from_spec(_spec)
    _spec.loader.exec_module(_mod)  # type: ignore
    UA = _mod.UA
    parse_sdmx_csv = _mod.parse_sdmx_csv
    parse_sdmx_xml = _mod.parse_sdmx_xml

SOURCE = "istat"
AGENCY = "IT1"
DEDUP = ("series_key", "obs_date")

# Host order = preference (mirrors jobs/ingest_istat_sliced.py HOSTS). sdmx.istat.it
# answers fast when healthy; esploradati carries the granular DF_* flows but is flaky.
HOSTS = [
    ("sdmx",    "https://sdmx.istat.it/SDMXWS/rest/"),
    ("esplora", "https://esploradati.istat.it/SDMXWS/rest/"),
]
CSV_ACCEPT = "application/vnd.sdmx.data+csv;version=1.0.0"
XML_ACCEPT = "application/vnd.sdmx.genericdata+xml;version=2.1"

RATE = 1.0            # seconds between flows (polite; the ingester used 1.5)
TIMEOUT = 120         # per request (the ingester allows up to 300 for esploradati)
RETRIES = 3           # per host, for transient (timeout/5xx/conn) errors
TRANSIENT_HTTP = (429, 500, 502, 503, 504)

# Files in the istat dir that are NOT dataflow parquets.
_SKIP_FILES = {"_sliced_done.json", "_sliced_unrecoverable.json"}

# Per-call structure cache so we don't re-fetch the same FULL body twice when probing
# a flow for the structural signal.
_NONTRIVIAL_BODY = 256  # a 200 body smaller than this is not a "real" structured body


# --------------------------------------------------------------------------- #
# HTTP — one request, classified for the date-tail caller
# --------------------------------------------------------------------------- #
class _Resp:
    """Outcome of one GET: kind in {ok, empty, transient, http<code>}."""
    __slots__ = ("content", "kind", "status")

    def __init__(self, content=None, kind="transient", status=None):
        self.content = content
        self.kind = kind
        self.status = status


def _get(sess, url, accept, timeout=TIMEOUT) -> _Resp:
    """ONE GET (no internal backoff — retry/host-fallback is orchestrated by the
    caller so a dead host doesn't burn the whole budget before the other is tried).
    Returns a classified _Resp:
      ok         -> 200 with a body
      empty      -> 404 "NoRecordsFound" (nothing newer in the window) / 400 / 413 /
                    other hard 4xx (this host doesn't serve the flow this way)
      gone       -> 404 "Could not find Dataflow and/or DSD ..." — the flow has been
                    WITHDRAWN from this host's catalog (a structural-break candidate;
                    the caller still tries the OTHER host before concluding it)
      transient  -> timeout / 5xx / 429 / conn drop (retryable on this host)
    """
    hdrs = {**UA, "Accept": accept}
    try:
        r = sess.get(url, headers=hdrs, timeout=timeout)
    except (requests.Timeout, requests.ConnectionError):
        return _Resp(kind="transient")
    if r.status_code == 200:
        return _Resp(content=r.content, kind="ok", status=200)
    if r.status_code in TRANSIENT_HTTP:
        return _Resp(kind="transient", status=r.status_code)
    if r.status_code == 404:
        body = (r.content or b"").lstrip().lower()
        # "NoRecordsFound" = valid flow, just nothing in this date window -> empty.
        # "Could not find Dataflow and/or DSD ..." = flow withdrawn from catalog -> gone.
        if b"could not find" in body or b"dataflow" in body:
            return _Resp(kind="gone", status=404)
        return _Resp(kind="empty", status=404)
    # 400/413 = no such slice; any other hard 4xx = host doesn't serve this. -> empty.
    return _Resp(kind="empty", status=r.status_code)


def _data_url(base, flow_id, start_period):
    url = f"{base}data/{AGENCY},{flow_id}/"
    if start_period:
        url += f"?startPeriod={start_period}"
    return url


def _parse(content):
    """Dispatch CSV/XML on the body shape (same logic the ingester used)."""
    if not content:
        return [], [], []
    if content[:100].lstrip().startswith(b"<"):
        return parse_sdmx_xml(content)
    return parse_sdmx_csv(content)


def _try_host(sess, base, flow_id, start_period):
    """One host attempt. Returns (keys, dates, vals, kind) with kind in
    {ok, empty, gone, structural, transient}. On a 200-but-0-rows CSV, tries XML once;
    a 200 with a NON-TRIVIAL body that still parses 0 rows is 'structural' (schema
    break), while a tiny/empty 200 or a 404 NoRecordsFound is 'empty'."""
    url = _data_url(base, flow_id, start_period)
    resp = _get(sess, url, CSV_ACCEPT)
    if resp.kind in ("transient", "empty", "gone"):
        return [], [], [], resp.kind
    # ok
    k, d, v = _parse(resp.content)
    big = bool(resp.content) and len(resp.content) >= _NONTRIVIAL_BODY
    if not v:
        # 200 but no parsed rows from CSV — try XML in case the host served an odd body.
        resp2 = _get(sess, url, XML_ACCEPT)
        if resp2.kind == "ok":
            k, d, v = _parse(resp2.content)
            big = big or (bool(resp2.content) and len(resp2.content) >= _NONTRIVIAL_BODY)
        elif resp2.kind == "transient":
            return [], [], [], "transient"
    if v:
        return k, d, v, "ok"
    # 0 usable rows. A non-trivial 200 body that parses nothing is a schema/structural
    # break; a tiny/header-only 200 is just an empty window.
    return [], [], [], ("structural" if big else "empty")


def _fetch_flow(sess, flow_id, start_period, had_prior: bool):
    """Fetch a flow's date-tail across hosts. Returns (keys, dates, vals, outcome)
    where outcome in {ok, empty, transient, structural}.

    Strategy (cheap + honest): one first pass over BOTH hosts with NO backoff — a host
    that 500s/times-out is recorded and we move straight to the next host (so a dead
    host can't burn the budget before the live host is tried). The first host that
    serves rows wins immediately. Only when NO host gave a CONCLUSIVE answer in the
    first pass (every host transient-failed) do we retry the transient hosts with
    backoff (RETRIES-1 passes) — a real throttle clears, a genuinely-down host stays
    down. This is what keeps a permanently-down SECONDARY host (e.g. sdmx.istat.it
    500ing while esploradati serves everything) from laundering every flow into
    'transient': a conclusive empty/gone/structural from the live host is the answer.

    Outcome precedence (a CONCLUSIVE answer from ANY host beats a transient from
    another host):
      ok         -> a host served real rows
      structural -> a host returned a 200 real body parsing 0 rows, OR (only for a flow
                    that HAD prior data) a host reports the flow GONE from its catalog
                    (withdrawn) and NO host reports a normal empty window — a real break
      empty      -> a host conclusively returned no data (404 NoRecordsFound / no slice)
      transient  -> ONLY when no host gave any conclusive answer (all transient)
    """
    transient_bases = []
    saw_structural = False
    saw_gone = False
    saw_empty = False
    for _label, base in HOSTS:
        k, d, v, kind = _try_host(sess, base, flow_id, start_period)
        if kind == "ok":
            return k, d, v, "ok"
        if kind == "transient":
            transient_bases.append(base)
        elif kind == "structural":
            saw_structural = True
        elif kind == "gone":
            saw_gone = True
        else:
            saw_empty = True

    # Retry the transient hosts with backoff ONLY if no host gave a conclusive answer
    # this pass — otherwise the conclusive signal already settles the flow and a dead
    # secondary host must not turn it transient.
    have_conclusive = saw_structural or saw_gone or saw_empty
    if not have_conclusive:
        for attempt in range(1, RETRIES):
            if not transient_bases:
                break
            time.sleep(min(4 * attempt, 30))
            still = []
            for base in transient_bases:
                k, d, v, kind = _try_host(sess, base, flow_id, start_period)
                if kind == "ok":
                    return k, d, v, "ok"
                if kind == "transient":
                    still.append(base)
                elif kind == "structural":
                    saw_structural = True
                elif kind == "gone":
                    saw_gone = True
                else:
                    saw_empty = True
            transient_bases = still
            if saw_structural or saw_gone or saw_empty:
                break

    if saw_structural:
        return [], [], [], "structural"
    # A 'gone' (404 "Could not find Dataflow/DSD") is only a CONCLUSIVE signal when no
    # host is still in a transient/unknown state. If a host remains transient (e.g. the
    # canonical host is mid-outage / timing out) a single 'gone' from the other host is
    # UNCORROBORATED: during an ISTAT outage one host routinely 404s the whole catalog,
    # and concluding "withdrawn" there would false-DefinitiveError the whole source on a
    # flow that previously had data. So while any host is unresolved-transient, do NOT
    # let a lone 'gone' settle the flow — fall through to the transient re-queue below.
    gone_conclusive = saw_gone and not transient_bases
    # A flow withdrawn from a catalog (gone) AND never reported as a normal empty window,
    # that PREVIOUSLY had data, is a structural break — not a quiet tail. Requires the
    # 'gone' to be corroborated (no host left transient).
    if gone_conclusive and not saw_empty and had_prior:
        return [], [], [], "structural"
    # A host conclusively said "no data" -> empty wins over a dead secondary host. A
    # corroborated 'gone' (catalog withdrawal with no prior data, or alongside a normal
    # empty from the other host) is likewise a benign empty window.
    if saw_empty or gone_conclusive:
        return [], [], [], "empty"
    # No conclusive answer (every host transient-failed, OR the only non-transient signal
    # was an uncorroborated 'gone' while another host is still transient) -> re-queue.
    return [], [], [], "transient"


# --------------------------------------------------------------------------- #
# disk helpers
# --------------------------------------------------------------------------- #
def _flow_files(out_dir):
    return sorted(
        f for f in os.listdir(out_dir)
        if f.endswith(".parquet") and f not in _SKIP_FILES
    )


def _max_obs(path):
    """Max obs_date on disk for a flow, or None."""
    try:
        od = pq.read_table(path, columns=["obs_date"]).column("obs_date")
        mx = pc.max(od).as_py() if od.length() else None
        if isinstance(mx, dt.datetime):
            mx = mx.date()
        return mx
    except Exception:
        return None


def _start_period(max_d: dt.date | None) -> str | None:
    """SDMX startPeriod for the date-tail: the YEAR of the stored max obs_date (so the
    boundary year is re-swept for late obs / revisions). None -> full fetch (a flow
    with no on-disk obs gets backfilled from origin)."""
    if max_d is None:
        return None
    return str(max_d.year)


# --------------------------------------------------------------------------- #
# contract entry point
# --------------------------------------------------------------------------- #
def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)

    files = _flow_files(out_dir)
    sess = requests.Session()
    tally = Tally()
    cursors: dict[str, str] = {}   # flow_id -> max obs_date written/known
    total = 0
    last_obs = None

    # Optional bounded subset for a tractable one-shot test (env-only). The PRODUCTION
    # orchestrator never sets this, so the full ~755-flow sweep runs in production.
    limit_env = os.environ.get("ISTAT_MAX_FLOWS")
    if limit_env:
        try:
            files = files[: int(limit_env)]
        except ValueError:
            pass

    n_sub = len(files)
    for fn in files:
        path = os.path.join(out_dir, fn)
        flow_id = fn[: -len(".parquet")]
        before = blob.row_count(path)
        max_d = _max_obs(path)
        if max_d is not None:
            cursors[flow_id] = max_d.isoformat()   # seed cursor from on-disk frontier
            if last_obs is None or max_d.isoformat() > last_obs:
                last_obs = max_d.isoformat()

        sp = _start_period(max_d)
        keys, dates, vals, outcome = _fetch_flow(sess, flow_id, sp, had_prior=before > 0)
        time.sleep(RATE)

        if outcome == "transient":
            # Every host failed this flow -> keep existing data, re-queue (run partial).
            tally.transient_unit()
            total += before
            continue

        if outcome == "structural":
            # A 200 real body now parsing 0 rows, or the flow withdrawn from BOTH
            # catalogs while it previously held data -> schema/structural break.
            # finalize() raises DefinitiveError; existing data is kept by merge.
            tally.structural_unit()
            total += before
            continue

        if outcome == "empty":
            # Valid flow, nothing newer in the window (the common quiet-tail case).
            tally.empty_unit()
            total += before
            continue

        # outcome == ok: we have newer rows. Defensive length align (mirrors ingester).
        m = min(len(keys), len(dates), len(vals))
        keys, dates, vals = keys[:m], dates[:m], vals[:m]
        if m == 0:
            tally.empty_unit()
            total += before
            continue

        new_tbl = pa.table({
            "series_key": pa.array(keys, pa.string()),
            "obs_date":   pa.array(dates, pa.date32()),
            "value":      pa.array(vals, pa.float64()),
        })
        try:
            n, md = merge.merge_and_write(path, new_tbl, mode="merge", dedup_keys=DEDUP)
        except Exception:
            # merge guard tripped (would shrink / drop a column): keep old data, surface
            # as transient so the flow re-runs next tick with a fresh pull.
            tally.transient_unit()
            total += before
            continue

        total += n
        # A flow that served a 200 with real rows is a SUCCESSFUL sub-unit (data flowed),
        # even when every row is at/below the stored boundary and the merge nets zero new
        # rows (the boundary-YEAR re-sweep makes that the normal idempotent steady state).
        # Count it added_unit(>0) so it does NOT feed the all-empty structural floor —
        # otherwise a perfectly healthy re-run where every active flow re-returns its
        # boundary year (zero net-new) would have empty==attempted and falsely raise.
        # The REAL net-new delta is carried by `obs` (total) and merge's row count.
        tally.added_unit(m)
        if md:
            cursors[flow_id] = md
            if last_obs is None or md > last_obs:
                last_obs = md

    # empty_window_floor = n_sub - 1: a steady-state run where ALL flows are quiet (each
    # ISTAT flow updates only a few times a year) is legitimate no_change, NOT a break;
    # real breaks are caught precisely per-flow via structural_unit(). A whole-host
    # outage routes to 'partial' via transient_unit() instead of the floor.
    return finalize(tally, total, last_obs, source=SOURCE,
                    series_cursors=cursors,
                    empty_window_floor=max(n_sub - 1, 1))
