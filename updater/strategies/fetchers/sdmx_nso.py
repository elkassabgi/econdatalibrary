"""S4 fetcher — sdmx_nso (the generic SDMX national-statistics / IO-org CLASS).

`sdmx_nso` is NOT a single directory. There is no data/clean_full/sdmx_nso/ on disk;
its registry `out_dir: sdmx_nso` is an UMBRELLA over the many per-provider directories
that jobs/ingest_sdmx_nso.py writes (one zstd parquet per SDMX dataflow under
clean_full/<provider>/<flow_id>.parquet). This fetcher is the incremental updater for
the providers that ONLY the generic SDMX-NSO connector owns — i.e. SDMX 2.1 REST
national-statistics / international-org sources that have no other dedicated fetcher.

WHICH PROVIDERS THIS COVERS (and why exactly these)
---------------------------------------------------
The generic ingester populates many dirs, but most are ALREADY owned by a dedicated
fetcher + its own registry source_id (so this one must NOT double-write them):
  istat -> istat.py,  eurostat -> eurostat.py,  norgesbank -> norgesbank.py,
  bundesbank -> bundesbank.py,  ksh -> ksh.py,  adb -> adb.py,
  bis/idb/gus/stats_nz -> non-SDMX bulk_snapshot ingesters with their own source_id.
That leaves the dirs whose ONLY producer is jobs/ingest_sdmx_nso.py and which have
NO competing registry owner:  ecb_sdmx, ilo, insee_sdmx, unicef, stat_austria.

Of those, the load-bearing safety test is KEY PARITY: a fresh generic pull must
reproduce the EXACT on-disk series_key for a flow, or the per-flow merge won't dedup
and the file would grow a parallel-keyed copy (the eurostat `LAST UPDATE=` duplication
bug, in another guise). Verified live against the on-disk files:
  * ECB (ecb_sdmx): SAFE. 100/101 flows are dot/KEY-style ("AME.A.AUT.1.0.0.0.OVGD")
        reproduced exactly by the SDMX-CSV KEY column; 1 flow (AGR) is k=v style
        ("FREQ=M:REF_AREA=I10:...") reproduced exactly by the generic XML SeriesKey.
        -> per-flow key-style DETECTION picks CSV or XML so the merge always dedups.
  * ILO (ilo): SAFE. SDMX-CSV carries the FULL dimension columns; parse_sdmx_csv
        rebuilds "REF_AREA=ABW:FREQ=A:...:NOTE_CLASSIF=..." byte-for-byte == disk.
        (XML here TRUNCATES the key to 5 dims, so ILO must use the CSV path.)
  * stat_austria: dir is empty on disk -> nothing to merge today; supported if/when
        the bulk ingester first populates it (this fetcher only EXTENDS existing files).
  * insee_sdmx: EXCLUDED. Existing data is degenerate — only 4 distinct series_keys
        for 38,809 rows ("OBS_QUAL=DEF:OBS_TYPE=A"), built from obs ATTRIBUTES not the
        real dimensions (the StructureSpecific-XML compact branch missed them). A
        correct fresh pull yields the full dimension key, which would NOT dedup against
        the broken history -> key split + growth. Needs a one-time REBUILD/RE-KEY
        data-op (designed below, NOT executed). Until then INSEE is skipped here.
  * unicef: EXCLUDED. On-disk keys are bare ("REF_AREA=AC:INDICATOR=...") but UNICEF's
        current SDMX-CSV ships LABELLED headers ("REF_AREA:Geographic area",
        "TIME_PERIOD:Time period") that parse_sdmx_csv does not recognise (0 rows), and
        its XML path differs too. Reproducing the exact historical key needs the
        no-label content negotiation / parser the original build used; until that is
        pinned down, merging risks a key mismatch -> skipped (flagged, not silently
        dropped).

So as a CLASS this fetcher currently covers **ECB (ecb_sdmx) and ILO (ilo)** for live
incremental updates, plus stat_austria as a no-op-until-populated. INSEE and UNICEF are
explicitly deferred to a key-migration data-op. New providers are trivially added to
PROVIDERS once their key parity is verified the same way.

STRATEGY: giant_changed_units (as the registry assigns). These flows are CHEAP to poll
(a per-flow SDMX `?startPeriod=<year of stored max>` tail returns only new/late periods
+ in-place revisions to the latest period), so — exactly like istat.py and the
framework's date-tail guidance — we ALWAYS re-attempt the tail for every on-disk flow
and let the per-flow merge return no_change when upstream hasn't moved. ECB dataflow
versions are static ("1.0"), so a version-diff change-feed would never fire; the cheap
date-tail is the honest signal. current_vintage() therefore returns None so the
giant_changed_units adapter always runs us (safe: merge dedups + never-shrinks).

ENGINE: the shared run_giant() driver assumes ONE source_dir (eurostat/oecd are single
-dir). sdmx_nso is MULTI-dir, so this module runs its own per-flow loop, but reuses the
exact same primitives the rest of the framework uses:
  - jobs/ingest_sdmx_nso.py parsers (parse_sdmx_csv/parse_sdmx_xml) -> identical keys,
  - merge.merge_and_write (atomic, dedup-on-(series_key,obs_date), never-shrink),
  - _common.Tally/finalize -> honest ok/no_change/partial/structural status,
  - _giant.load_state/save_state sidecar (<provider_dir>/_giant_state.json) for per-flow
    cursors + last status, so a once-broken flow is always reselected (no freeze).

HONEST STATUS (Tally + finalize), per on-disk flow as a sub-unit:
  added_unit(n)    -- a flow served a 200 with real rows (data flowed; merge may net 0
                      new rows in steady state because the boundary year re-sweeps — that
                      is the normal idempotent case, still counted as a healthy sub-unit)
  empty_unit()     -- 404 NoRecordsFound / 200 with no usable rows in the tail window
  transient_unit() -- timeout / 5xx / 429 / conn drop / merge-guard trip -> run 'partial'
                      (orchestrator does NOT stamp last_success; flow retried next tick)
  structural_unit()-- a 200 with a NON-TRIVIAL body that parsed 0 rows on a flow that had
                      prior data (schema break) -> finalize() raises DefinitiveError
Existing parquet is never written by this module except through merge.merge_and_write, so
no failure path can shrink, empty, or duplicate good data.

┌──────────────────────────────────────────────────────────────────────────────────┐
│ DATA-OPS FLAGGED (DESIGNED, *NOT* EXECUTED) — needed before INSEE/UNICEF go live:   │
│  (1) insee_sdmx REBUILD: the existing clean_full/insee_sdmx/*.parquet were built    │
│      with obs-attribute-only keys (4 keys/file). Re-ingest each flow from            │
│      bdm.insee.fr SDMX generic XML with the full SeriesKey dimensions (the current   │
│      parse_sdmx_xml already does this correctly), write to a staging dir, verify     │
│      key cardinality, then atomically swap. NON-destructive to obs VALUES; gated,    │
│      backed-up job. After that, add "insee_sdmx" to PROVIDERS here.                  │
│  (2) unicef KEY/FORMAT pin: determine the content negotiation the original build     │
│      used (bare codes, no column labels) — likely `?format=csv` without             │
│      `&labels=both` / a specific Accept — confirm a fresh pull reproduces the bare   │
│      "REF_AREA=AC:INDICATOR=..." key, then add "unicef" to PROVIDERS.               │
│ This module NEVER performs either implicitly; both touch production parquet.         │
└──────────────────────────────────────────────────────────────────────────────────┘
"""
from __future__ import annotations

import datetime as dt
import os
import time

import pyarrow as pa
import pyarrow.compute as pc
import requests

from ... import config, blob, merge
from ...errors import TransientError
from ..base import Result
from ._common import Tally, finalize, sane_since
from . import _giant

# Reuse the ingester's UA + parsers VERBATIM so the keys we emit line up byte-for-byte
# with the published files (the whole point — merges extend, never duplicate).
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

SOURCE = "sdmx_nso"
DEDUP = ("series_key", "obs_date")

CSV_ACCEPT = "application/vnd.sdmx.data+csv;version=1.0.0"
XML_ACCEPT = "application/vnd.sdmx.genericdata+xml;version=2.1"

RATE = 1.0            # seconds between flows (polite)
TIMEOUT = 300         # per request
RETRIES = 3           # transient retries per flow (timeout/5xx/conn)
TRANSIENT_HTTP = (429, 500, 502, 503, 504)
_NONTRIVIAL_BODY = 256  # a 200 body smaller than this is not a "real" structured body

# Providers this fetcher owns. Each entry mirrors the relevant jobs/ingest_sdmx_nso.py
# PROVIDERS config (base/agency/Accept/trailing-slash/data_params), plus `out_dir` (the
# clean_full subdir its flows live in). ONLY providers whose generic pull reproduces the
# EXACT on-disk series_key are listed (verified live — see module docstring). INSEE and
# UNICEF are deliberately absent pending the flagged key-migration data-ops.
PROVIDERS = {
    "ecb_sdmx": {
        "base":   "https://data-api.ecb.europa.eu/service/",
        "agency": "ECB",
        "out_dir": "ecb_sdmx",
        "csv_accept": "text/csv",          # ECB rejects sdmx CSV mime types
        "xml_accept": "application/xml",
        "no_trailing_slash": True,         # ECB: trailing slash -> 404
        "min_param": "startPeriod",        # uses a date param; needs at least one
        # On ECB, the SDMX-CSV KEY column yields the FLAT "AME.A.AUT..." key (100/101
        # flows on disk) and the generic XML SeriesKey yields the k=v "FREQ=M:..." key
        # (the lone AGR flow). So a kv-style flow must be fetched as XML, a flat-style
        # flow as CSV. Verified live against the on-disk keys.
        "kv_format": "xml",
        "rate": 1.0, "timeout": 600,
    },
    "ilo": {
        "base":   "https://sdmx.ilo.org/rest/",
        "agency": "ILO",
        "out_dir": "ilo",
        "csv_accept": "text/csv",
        "xml_accept": "application/xml",
        "no_trailing_slash": False,
        "min_param": "startPeriod",
        # On ILO the SDMX-CSV carries the FULL dimension columns, so parse_sdmx_csv
        # rebuilds the k=v key byte-for-byte (incl. SOURCE / NOTE_CLASSIF). ILO's XML
        # SeriesKey TRUNCATES to 5 dims, so kv MUST come from CSV here, not XML.
        "kv_format": "csv",
        "rate": 1.5, "timeout": 600,
    },
    "stat_austria": {
        "base":   "https://data.statistik.gv.at/ogd/sdmx/",
        "agency": "STAT",
        "out_dir": "stat_austria",
        "csv_accept": "",                  # use the generic ingester defaults
        "xml_accept": "",
        "no_trailing_slash": False,
        "min_param": "startPeriod",
        # Empty on disk today; default kv->csv (the generic ingester's CSV-first path).
        # If/when populated, the per-flow key-style verification below catches any
        # mismatch (treated as transient, never a silent mis-key).
        "kv_format": "csv",
        "rate": 2.0, "timeout": 300,
    },
}

_SKIP_FILES = {"_giant_state.json"}  # never treat the sidecar as a dataflow parquet


# --------------------------------------------------------------------------- #
# HTTP — one classified GET (transient vs conclusive), no internal backoff so the
# caller orchestrates retries/budget (mirrors istat.py's _get/_Resp contract).
# --------------------------------------------------------------------------- #
class _Resp:
    __slots__ = ("content", "kind", "status")

    def __init__(self, content=None, kind="transient", status=None):
        self.content = content
        self.kind = kind
        self.status = status


def _get(sess, url, accept, timeout) -> _Resp:
    """ONE GET. kind in {ok, empty, transient}.
      ok        -> 200 with a body
      empty     -> 404 / 400 / 413 / other hard 4xx (no data / flow not served this way)
      transient -> timeout / 5xx / 429 / conn drop (retryable)
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
    # 404/400/413/other hard 4xx -> nothing here (the SDMX "NoRecordsFound" path).
    return _Resp(kind="empty", status=r.status_code)


# --------------------------------------------------------------------------- #
# key-style detection + matched parse
# --------------------------------------------------------------------------- #
def _disk_key_style(path) -> str:
    """Inspect the on-disk file's first series_key and return the parse path that
    reproduces it:  'kv'  -> generic XML (SeriesKey Values 'DIM=val:...'),
                    'flat'-> SDMX-CSV (KEY column 'A.B.C' or any non-'=' form).
    Default 'csv-first' when the file is absent/empty (brand-new flow): try CSV then XML
    exactly like the ingester. The choice is per-FLOW because one provider (ECB) mixes
    both styles across its flows."""
    try:
        t = blob.read_table(path, columns=["series_key"])
        if t.num_rows == 0:
            return "csv-first"
        k = t.column("series_key")[0].as_py() or ""
        return "kv" if "=" in k else "flat"
    except Exception:
        return "csv-first"


def _key_style(k: str) -> str:
    return "kv" if ("=" in (k or "")) else "flat"


def _format_for_style(cfg, style):
    """Which wire FORMAT reproduces the on-disk key style for THIS provider.

    The key-string style (kv vs flat) is not enough on its own: the SAME kv style is
    produced by CSV on ILO (full dim columns -> 'REF_AREA=..:FREQ=..') but by XML on ECB
    (SeriesKey Values), while a flat style ('AME.A.AUT..') only ever comes from a CSV KEY
    column. So we map: kv-style -> the provider's declared kv_format; flat-style -> csv;
    unknown (brand-new flow) -> csv-first (try CSV then XML, the ingester's dispatch)."""
    if style == "kv":
        return cfg.get("kv_format", "csv")
    if style == "flat":
        return "csv"
    return "csv-first"


def _accept_for_format(cfg, fmt):
    if fmt == "xml":
        return cfg.get("xml_accept") or XML_ACCEPT
    return cfg.get("csv_accept") or CSV_ACCEPT


def _parse_for_format(content, fmt):
    """Parse a 200 body with the parser for `fmt`. Returns (keys, dates, vals, big).
    big=True iff the body is non-trivial (structural-break signal). For 'csv-first'
    dispatch on the body shape exactly like the ingester."""
    if not content:
        return [], [], [], False
    big = len(content) >= _NONTRIVIAL_BODY
    is_xml_body = content[:100].lstrip().startswith(b"<")
    if fmt == "xml":
        # Want XML SeriesKey keys; only the XML parser yields them. A CSV body here would
        # mis-key, so refuse it (caller re-requests XML explicitly).
        if not is_xml_body:
            return [], [], [], big
        k, d, v = parse_sdmx_xml(content)
        return k, d, v, big
    if fmt == "csv":
        # Want the CSV path; an XML body cannot reproduce the CSV-derived key.
        if is_xml_body:
            return [], [], [], big
        k, d, v = parse_sdmx_csv(content)
        return k, d, v, big
    # csv-first (brand-new flow): mirror the ingester (CSV unless the body is XML).
    if is_xml_body:
        k, d, v = parse_sdmx_xml(content)
    else:
        k, d, v = parse_sdmx_csv(content)
    return k, d, v, big


# --------------------------------------------------------------------------- #
# URL + per-flow fetch
# --------------------------------------------------------------------------- #
def _data_url(cfg, flow_id, start_period):
    trail = "" if cfg.get("no_trailing_slash") else "/"
    url = f"{cfg['base']}data/{cfg['agency']},{flow_id}{trail}"
    if start_period:
        url += f"?startPeriod={start_period}"
    elif cfg.get("min_param") == "startPeriod" and cfg.get("no_trailing_slash"):
        # ECB needs at least one query param even on a full pull.
        url += "?startPeriod=1900-01-01"
    return url


def _get_with_retry(sess, url, accept, timeout):
    """One GET with transient backoff (RETRIES attempts). Returns a _Resp; kind stays
    'transient' if every attempt failed transiently."""
    resp = _get(sess, url, accept, timeout)
    attempt = 0
    while resp.kind == "transient" and attempt < RETRIES - 1:
        time.sleep(min(4 * (attempt + 1), 30))
        resp = _get(sess, url, accept, timeout)
        attempt += 1
    return resp


def _fetch_flow(sess, cfg, flow_id, start_period, style, had_prior):
    """Fetch one flow's date-tail and parse it into the EXACT on-disk key style. Returns
    (keys, dates, vals, outcome) with outcome in {ok, empty, transient, structural}.

    Picks the wire format that reproduces this flow's on-disk key style for this provider
    (kv->kv_format, flat->csv, new->csv-first). If the chosen format's body comes back in
    the other shape (CSV when we wanted XML, etc.) we re-request the right Accept once.
    CRITICAL: before returning 'ok' we VERIFY the produced key style matches the on-disk
    style — a style mismatch (which would split keys and grow the file) is surfaced as
    'transient' (re-tried, never silently mis-keyed), NOT merged."""
    timeout = cfg.get("timeout", TIMEOUT)
    url = _data_url(cfg, flow_id, start_period)
    fmt = _format_for_style(cfg, style)

    resp = _get_with_retry(sess, url, _accept_for_format(cfg, fmt), timeout)
    if resp.kind == "transient":
        return [], [], [], "transient"
    if resp.kind == "empty":
        return [], [], [], "empty"

    k, d, v, big = _parse_for_format(resp.content, fmt)

    # If we wanted a specific format but the server replied in the other shape, the parse
    # above returned nothing — re-request with the explicit Accept for `fmt` once.
    if not v and fmt in ("csv", "xml"):
        want_xml = (fmt == "xml")
        got_xml = resp.content[:100].lstrip().startswith(b"<")
        if want_xml != got_xml:
            resp2 = _get_with_retry(sess, url, _accept_for_format(cfg, fmt), timeout)
            if resp2.kind == "transient":
                return [], [], [], "transient"
            if resp2.kind == "ok":
                k, d, v, big2 = _parse_for_format(resp2.content, fmt)
                big = big or big2

    if not v:
        # 0 usable rows. A non-trivial 200 body that parses nothing on a flow that HAD
        # data is a schema/structural break; a tiny/header-only body is an empty window.
        return [], [], [], ("structural" if (big and had_prior) else "empty")

    # KEY-STYLE GUARD: never merge a tail whose key style differs from what's on disk
    # (that would dedup against nothing and grow the file). If a known-style flow comes
    # back in the wrong style, treat it as transient so it re-runs rather than corrupts.
    if style in ("kv", "flat") and _key_style(k[0]) != style:
        return [], [], [], "transient"

    return k, d, v, "ok"


# --------------------------------------------------------------------------- #
# disk helpers
# --------------------------------------------------------------------------- #
def _flow_files(out_dir):
    # blob-routed: the flow set must be visible under AQUEDUCT_BACKEND=r2.
    return [f for f in blob.list_parquets(out_dir) if f not in _SKIP_FILES]


def _max_obs(path):
    try:
        od = blob.read_table(path, columns=["obs_date"]).column("obs_date")
        mx = pc.max(od).as_py() if od.length() else None
        if isinstance(mx, dt.datetime):
            mx = mx.date()
        return mx
    except Exception:
        return None


def _start_period(max_d):
    """SDMX startPeriod for the date-tail: the YEAR of the stored max obs_date so the
    boundary year is re-swept for late obs / revisions. None -> full pull (a flow with
    no on-disk obs is backfilled from origin). Guards corrupt far-future sentinels via
    sane_since so a bad max can't filter everything out."""
    if max_d is None:
        return None
    safe = sane_since(max_d.isoformat() if isinstance(max_d, dt.date) else max_d)
    if safe is None:
        return None
    try:
        return str(dt.date.fromisoformat(str(safe)[:10]).year)
    except Exception:
        return None


# --------------------------------------------------------------------------- #
# per-provider sweep
# --------------------------------------------------------------------------- #
def _run_provider(prov, cfg, tally, cursors, limit=None):
    """Sweep one provider's on-disk flows (date-tail each, merge new rows). Mutates the
    shared Tally/cursors. Returns (total_rows_for_provider, max_obs_iso_or_None)."""
    out_dir = os.path.join(config.DATA_ROOT, cfg["out_dir"])
    files = _flow_files(out_dir)
    if limit is not None:
        files = files[:limit]
    if not files:
        return 0, None

    state = _giant.load_state(out_dir)
    sess = requests.Session()
    rate = cfg.get("rate", RATE)
    total = 0
    prov_last = None

    for fn in files:
        path = os.path.join(out_dir, fn)
        flow_id = fn[: -len(".parquet")]
        before = blob.row_count(path)
        max_d = _max_obs(path)
        ck = f"{prov}/{flow_id}"
        if max_d is not None:
            cursors[ck] = max_d.isoformat()
            if prov_last is None or max_d.isoformat() > prov_last:
                prov_last = max_d.isoformat()

        style = _disk_key_style(path)
        sp = _start_period(max_d)
        flow_st = dict(state.get(flow_id, {}))

        keys, dates, vals, outcome = _fetch_flow(
            sess, cfg, flow_id, sp, style, had_prior=before > 0)
        time.sleep(rate)

        if outcome == "transient":
            tally.transient_unit()
            flow_st.update(status="transient_fail")
            state[flow_id] = flow_st
            total += before
            continue
        if outcome == "structural":
            tally.structural_unit()
            flow_st.update(status="definitive_fail")
            state[flow_id] = flow_st
            total += before
            continue
        if outcome == "empty":
            tally.empty_unit()
            flow_st.update(status="empty")
            state[flow_id] = flow_st
            total += before
            continue

        # ok: defensive length align (mirrors the ingester).
        m = min(len(keys), len(dates), len(vals))
        keys, dates, vals = keys[:m], dates[:m], vals[:m]
        if m == 0:
            tally.empty_unit()
            flow_st.update(status="empty")
            state[flow_id] = flow_st
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
            # transient so the flow re-runs with a fresh pull next tick.
            tally.transient_unit()
            flow_st.update(status="partial")
            state[flow_id] = flow_st
            total += before
            continue

        total += n
        # A 200 with real rows is a SUCCESSFUL sub-unit even if the boundary-year
        # re-sweep nets zero new rows in steady state (same reasoning as istat.py): count
        # added_unit(m) so a healthy idempotent run doesn't trip the all-empty structural
        # floor. Real net-new is carried by `obs` (total) and merge's row count.
        tally.added_unit(m)
        flow_st.update(status="ok", last_obs_date=md, obs_count=n)
        state[flow_id] = flow_st
        if md:
            cursors[ck] = md
            if prov_last is None or md > prov_last:
                prov_last = md

    _giant.save_state(out_dir, state)
    return total, prov_last


# --------------------------------------------------------------------------- #
# contract entry points
# --------------------------------------------------------------------------- #
def current_vintage(unit) -> str | None:
    """Cheap probe used by giant_changed_units.detect_change. ECB dataflow versions are
    static ('1.0') so a version-diff would never fire; these flows are cheap date-tails,
    so we return None -> the strategy always runs us (safe: per-flow merge dedups +
    never-shrinks, and the fetcher itself returns no_change when nothing is newer)."""
    return None


def update(unit, since) -> Result:
    """Sweep every covered provider's on-disk flows with a per-flow SDMX date-tail and
    merge new/revised observations under dedup + never-shrink. One honest source-level
    Result aggregated across all providers; per-flow cursors keyed '<provider>/<flow>'."""
    tally = Tally()
    cursors: dict[str, str] = {}
    total = 0
    last_obs = None

    # Optional bounded subset for a tractable one-shot TEST (env only). Production never
    # sets these, so the full sweep runs in production.
    only_provs = os.environ.get("SDMX_NSO_ONLY_PROVIDERS")
    provs = (PROVIDERS if not only_provs
             else {p: PROVIDERS[p] for p in only_provs.split(",") if p in PROVIDERS})
    max_flows_env = os.environ.get("SDMX_NSO_MAX_FLOWS_PER_PROVIDER")
    limit = int(max_flows_env) if max_flows_env and max_flows_env.isdigit() else None

    n_sub_total = 0
    for prov, cfg in provs.items():
        out_dir = os.path.join(config.DATA_ROOT, cfg["out_dir"])
        n_sub_total += len(_flow_files(out_dir)[:limit] if limit is not None
                           else _flow_files(out_dir))

    for prov, cfg in provs.items():
        prov_total, prov_last = _run_provider(prov, cfg, tally, cursors, limit=limit)
        total += prov_total
        if prov_last and (last_obs is None or prov_last > last_obs):
            last_obs = prov_last

    # empty_window_floor = n-1: a steady-state tick where every flow is quiet (these
    # sources update only a few times a year) is legitimate no_change, NOT a break; real
    # breaks are caught per-flow via structural_unit(), whole-host outages via
    # transient_unit() -> 'partial'. With 0 flows attempted, finalize returns no_change.
    floor = max(n_sub_total - 1, 1)
    return finalize(tally, total, last_obs, source=SOURCE,
                    series_cursors=cursors, empty_window_floor=floor)
