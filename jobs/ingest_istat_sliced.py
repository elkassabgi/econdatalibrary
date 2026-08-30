#!/usr/bin/env python3
"""Complete ISTAT dataflows the normal pull left INCOMPLETE.

The normal ISTAT sweep (jobs/ingest_sdmx_nso.py, providers "istat" +
"istat_esploradati") writes one Parquet per dataflow into
data/clean_full/istat/.  A subset of flows never land because a single SDMX
data response is too large for the endpoint: the host returns HTTP 500 / times
out, the body exceeds what we can parse, or (via DBnomics historically) the
flow blows past the 100K-series cap.

This job sweeps the ISTAT catalog, finds every flow that has NO parquet yet
(or is marked errored), and retrieves it WITHOUT overloading the endpoint:

  1. Plain full pull           GET /data/IT1,{flow}/        (sdmx-csv)
  2. Time slicing              startPeriod/endPeriod in decade windows
     (pre-1960, 1960-69, ... 2030-39), accumulating + de-duping obs.
  3. Per-year slicing          for any decade that still 500s / is too big.
  4. Dimension slicing         split the key-path on the most granular
     dimension (territory/category/...), one request per code, when the
     time axis alone can't shrink the response. Requires the DSD, fetched
     from whichever ISTAT host answers.

Anything that survives all of the above on BOTH hosts is recorded in
_sliced_unrecoverable.json with the exact reason -- never fabricated, never
silently dropped.  Completed flow ids are checkpointed in _sliced_done.json
so the run is fully resumable (a watcher relaunches this after ISTAT, whose
esploradati host is chronically flaky, recovers).

Output: data/clean_full/istat/{flow_id}.parquet  -- SAME dir + filename
convention as ingest_sdmx_nso.py, so skip-existing dedupes across both jobs.
Schema: {series_key: string, obs_date: date32, value: float64}, zstd.

Run:
  python jobs/ingest_istat_sliced.py                 # full incomplete sweep
  python jobs/ingest_istat_sliced.py --only A,B      # just these flow ids
  python jobs/ingest_istat_sliced.py --list          # list incomplete, no DL
  python jobs/ingest_istat_sliced.py --max-size-mb 500
"""
from __future__ import annotations
import csv, datetime as dt, io, json, os, sys, time
import xml.etree.ElementTree as ET
from urllib.parse import urlsplit
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
sys.path.insert(0, ROOT)

# ── Reuse the house helpers from the main SDMX ingester verbatim where we can.
#    parse_sdmx_period / parse_sdmx_csv / parse_sdmx_xml are byte-for-byte the
#    same parsing logic the normal pull used, so output is identical.
from jobs.ingest_sdmx_nso import (  # noqa: E402
    UA, NS,
    parse_sdmx_period, parse_sdmx_csv, parse_sdmx_xml,
)

# ── ISTAT provider config (mirrors PROVIDERS["istat"] / "istat_esploradati").
#    Both hosts speak the same SDMX 2.1 REST dialect and the SAME agency (IT1)
#    and flow ids, so a flow can be fetched from whichever one is up.
AGENCY  = "IT1"
HOSTS   = [
    # (label, base) -- order = preference.
    # REORDERED 2026-07-25: sdmx.istat.it no longer serves SDMX. It answers every
    # request with `HTTP 302 -> http://avvisi.istat.it` (ISTAT's notices host), and
    # avvisi.istat.it fails the TLS handshake outright
    # (SSLV3_ALERT_HANDSHAKE_FAILURE), so the redirect surfaces as an SSLError on a
    # host whose own certificate is in fact valid (*.istat.it, verified by a raw
    # handshake). Preferring it meant every request burned 5 retries with 8/16/24/32 s
    # backoff before falling through, and the job produced NOTHING for hours while the
    # guard kept it alive. esploradati answers 200 with 13.6 MB in ~51 s — slow, but
    # it is the host that works, and it is the only one carrying the granular DF_*
    # flows anyway. Put sdmx back first if/when it stops redirecting.
    ("esplora", "https://esploradati.istat.it/SDMXWS/rest/"),
    ("sdmx",    "https://sdmx.istat.it/SDMXWS/rest/"),
]
# "Polite" was a guess, and it was 40 requests/minute against a publisher who documents a
# ceiling of FIVE and blocks for 1-2 days above it. These sleeps sit between slices in the
# callers; the real ceiling is now enforced centrally in http_get by _throttle(), which every
# request funnels through. Kept as a small extra courtesy gap, not as the limiter.
RATE        = 1.5      # seconds between slices; the HARD limit is ISTAT_QPM, see _throttle
TIMEOUT     = 300      # generous: esploradati can take minutes
RETRIES     = 5        # generous retries with backoff for the flaky host
MAX_SIZE    = 500 * 1024 * 1024   # ~500 MB single-response ceiling (overridable)

CSV_ACCEPT  = "application/vnd.sdmx.data+csv;version=1.0.0"
XML_ACCEPT  = "application/vnd.sdmx.genericdata+xml;version=2.1"
STR_ACCEPT  = "application/vnd.sdmx.structure+xml;version=2.1"

OUT_DIR     = os.path.join(ROOT, "data", "clean_full", "istat")
DONE_PATH   = os.path.join(OUT_DIR, "_sliced_done.json")
UNREC_PATH  = os.path.join(OUT_DIR, "_sliced_unrecoverable.json")

# Decade windows for time slicing. SDMX startPeriod/endPeriod are inclusive and
# accept bare years; ISTAT clamps to the flow's real range, so over-wide bounds
# are harmless. Pre-1960 swept as one open-ended chunk.
DECADES = [(None, "1959")] + [
    (str(y), str(y + 9)) for y in range(1960, 2040, 10)
]


def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


# ──────────────────────────────── HTTP ──────────────────────────────────────

class HttpResult:
    """Outcome of one GET: ok | http_error(code) | timeout | conn_error."""
    __slots__ = ("content", "status", "kind", "elapsed")

    def __init__(self, content=None, status=None, kind="ok", elapsed=0.0):
        self.content = content
        self.status  = status
        self.kind    = kind          # ok | http | timeout | conn
        self.elapsed = elapsed

    @property
    def ok(self):   return self.kind == "ok"
    @property
    def is_500(self): return self.kind == "http" and (self.status or 0) >= 500


# Hosts whose TLS handshake has already failed this run. "Abandoning host" was only
# ever true for the ONE request that hit it: the caller then fell back to per-year
# slicing and cheerfully retried the same dead host for every single year. Observed
# 2026-07-27 — nearly 3 hours burned walking 1990, 1991, 1992 ... through
# sdmx.istat.it (which 302s to avvisi.istat.it and fails the handshake), logging
# "abandoning host" each time and writing no data at all. A handshake failure is a
# property of the HOST, not of the slice, so record it once and skip it thereafter.
_DEAD_HOSTS: set[str] = set()

# Per-flow wall-clock budget. Without one, a single pathological flow starves every
# other flow in the queue: RICPOPRES2011 subdivides to decades, then to years, and
# each slice costs up to a 300 s timeout x retries, so one flow can hold the crawler
# for hours while 3,882 others wait behind it. The budget is enforced inside
# http_get -- the one chokepoint every subdivision path funnels through -- because
# enforcing it in the recursion would mean finding and patching each descent.
# Exceeding it is DEFERRAL, never a verdict about the data: the flow is left for the
# next run, not recorded as unrecoverable.
FLOW_BUDGET_S = 15 * 60
_FLOW_DEADLINE: float | None = None
_BUDGET_HIT = False


def _over_budget() -> bool:
    """True once the current flow's wall-clock budget is spent (and latch it)."""
    global _BUDGET_HIT
    if _FLOW_DEADLINE is not None and time.time() > _FLOW_DEADLINE:
        _BUDGET_HIT = True
        return True
    return False


# ─────────────────────── ISTAT's published rate limit ──────────────────────
# THE PUBLISHER DOCUMENTS 5 QUERIES PER MINUTE PER IP, AND BLOCKS FOR 1-2 DAYS WHEN YOU
# EXCEED IT. (ondata.github.io/guida-api-istat, read 2026-08-30: "5 query al minuto per ogni
# IP", block "compresa tra 1 e 2 giorni".) We had no limiter at all.
#
# MEASURED 2026-08-30, and it is our own fault, not Istat being slow:
#   * our log bursts to 11-12 requests/minute, against a documented ceiling of 5;
#   * DNS resolves esploradati.istat.it -> 193.204.90.13 fine, but a TCP connect to :443
#     is SILENTLY DROPPED after 21.0 s -- blackholed, not refused, which is what a firewall
#     block looks like and never what a busy server looks like;
#   * a control request to an unrelated host answered HTTP 200 in 0.4 s, so our network is
#     healthy;
#   * the crawler had done 275 of 2,483 flows in three days, with 743 timeouts and ZERO
#     completions in the last 2,000 log lines.
#
# THE VICIOUS PART: the block lasts 1-2 days, and every retry we send while blocked renews
# it. A hot retry loop against a rate-limit block is not merely useless, it is the thing
# preventing recovery. So this does two jobs -- pace requests below the ceiling, and when a
# block is detected, STOP POKING and let it expire.
ISTAT_QPM = int(os.environ.get("ISTAT_QPM", "4"))      # 4 < the documented 5, for margin
_MIN_GAP_S = 60.0 / max(1, ISTAT_QPM)
_last_request_at = 0.0

# A streak of connect-level failures with no reply at all is the block signature. HTTP
# errors do NOT count: a 500 means the server is talking to us.
BLOCK_STREAK = int(os.environ.get("ISTAT_BLOCK_STREAK", "12"))
BLOCK_COOLDOWN_S = int(os.environ.get("ISTAT_BLOCK_COOLDOWN_S", "3600"))
BLOCK_COOLDOWN_MAX_S = 6 * 3600
_conn_streak = 0
_blocks_seen = 0


def _throttle() -> None:
    """Space every request at least _MIN_GAP_S apart, process-wide."""
    global _last_request_at
    wait = _last_request_at + _MIN_GAP_S - time.time()
    if wait > 0:
        time.sleep(wait)
    _last_request_at = time.time()


def _note_reply() -> None:
    """ANY reply -- including a 4xx or 5xx -- proves we are not blocked."""
    global _conn_streak
    _conn_streak = 0


def _note_no_reply() -> bool:
    """Count a connect-level failure. True once the streak says we are blocked.

    R62's lesson applies here: a message that ANNOUNCES an action must have STATE behind
    it. This returns a decision the caller acts on, rather than logging an intention and
    re-dialling anyway."""
    global _conn_streak
    _conn_streak += 1
    return _conn_streak >= BLOCK_STREAK


def _cool_off() -> None:
    """Back off hard, escalating, because the block outlives any per-request backoff.

    Istat blocks for 1-2 days. Sleeping an hour and probing again costs one request per
    hour and lets the block expire; continuing at 11/min guarantees it never does."""
    global _blocks_seen, _conn_streak
    _blocks_seen += 1
    nap = min(BLOCK_COOLDOWN_MAX_S, BLOCK_COOLDOWN_S * (2 ** (_blocks_seen - 1)))
    log(f"  RATE-LIMIT BLOCK SUSPECTED: {_conn_streak} consecutive connect failures with no "
        f"reply. Istat publishes 5 req/min per IP and blocks 1-2 days when exceeded, and "
        f"every retry we send RENEWS it. Sleeping {nap // 60} min (block #{_blocks_seen}) "
        f"instead of poking. Pace this run with ISTAT_QPM (currently {ISTAT_QPM}/min).")
    time.sleep(nap)
    _conn_streak = 0


def http_get(url: str, accept: str, timeout: int = TIMEOUT,
             retries: int = RETRIES) -> HttpResult:
    """GET with backoff. Distinguishes 200 / 4xx / 5xx / timeout / conn-error
    so the slicer can decide whether to subdivide (5xx/timeout/size) or give up
    (persistent 4xx is a real 'no data')."""
    global _BUDGET_HIT
    if _over_budget():
        # Budget spent: stop issuing requests so the subdivision unwinds promptly.
        return HttpResult(None, None, "timeout", 0.0)
    host = urlsplit(url).netloc
    if host in _DEAD_HOSTS:
        # Already known dead: fail instantly so the caller moves to the next HOST
        # rather than re-paying the handshake timeout on every slice.
        return HttpResult(None, None, "conn", 0.0)
    hdrs = {**UA, "Accept": accept}
    last = HttpResult(kind="conn")
    for attempt in range(retries):
        # Re-check EVERY attempt, not just on entry. Checking only at the top bounds
        # a flow at budget + retries*timeout (15 + 5*300 s = 40 min here), because a
        # call that starts one second inside the deadline still runs its full retry
        # ladder — the deadline exists precisely for requests that hang, so those are
        # the ones it must be able to interrupt. Same shape as a p.wait(timeout=)
        # placed after the pipe is already drained: present, plausible, unreachable.
        if attempt and _over_budget():
            return last
        _throttle()          # never exceed the publisher's published 5 req/min per IP
        t = time.time()
        try:
            r = requests.get(url, headers=hdrs, timeout=timeout)
            el = time.time() - t
            _note_reply()    # the server spoke to us: whatever else is wrong, we are not blocked
            if r.status_code == 200:
                return HttpResult(r.content, 200, "ok", el)
            if r.status_code in (400, 404, 413):
                # Genuine "no such slice / not found / too large" -> definitive.
                return HttpResult(None, r.status_code, "http", el)
            if r.status_code == 429:
                log(f"  429 rate limit, sleeping 60s")
                time.sleep(60)
                last = HttpResult(None, 429, "http", el)
                continue
            # 5xx and anything else: retry with backoff, remember as http error
            log(f"  HTTP {r.status_code} attempt {attempt+1} ({el:.0f}s): ...{url[-72:]}")
            last = HttpResult(None, r.status_code, "http", el)
        except requests.exceptions.Timeout:
            el = time.time() - t
            log(f"  TIMEOUT attempt {attempt+1} ({el:.0f}s): ...{url[-72:]}")
            last = HttpResult(None, None, "timeout", el)
            if _note_no_reply():
                _cool_off()
                return last
        except requests.exceptions.SSLError as e:
            # A TLS handshake failure will not heal in 8 seconds — it means this host
            # (or whatever it redirects to) is misconfigured or decommissioned. Give up
            # on it IMMEDIATELY so the caller falls through to the next host, instead of
            # spending 8+16+24+32 s per URL discovering the same thing five times.
            # Observed: sdmx.istat.it 302s to avvisi.istat.it, which fails the handshake.
            el = time.time() - t
            _DEAD_HOSTS.add(host)       # sticky: never pay this handshake again this run
            log(f"  SSL FAIL, host {host} RETIRED for this run "
                f"({type(e).__name__}): ...{url[-72:]}")
            return HttpResult(None, None, "conn", el)
        except Exception as e:
            el = time.time() - t
            log(f"  ERR attempt {attempt+1} ({type(e).__name__}): ...{url[-72:]}")
            last = HttpResult(None, None, "conn", el)
            if _note_no_reply():
                _cool_off()
                return last
        time.sleep(8 * (attempt + 1))
    return last


# ──────────────────────────────── Dataflow catalog ─────────────────────────

def _parse_flow_ids(raw: bytes) -> list[dict]:
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as e:
        log(f"  catalog XML parse error: {e}")
        return []
    out = []
    for f in root.findall(".//str:Dataflow", NS):
        fid = f.get("id", "")
        if not fid:
            continue
        nm = f.find("com:Name", NS)
        out.append({
            "id":      fid,
            "agency":  f.get("agencyID", AGENCY),
            "version": f.get("version", "1.0"),
            "name":    nm.text if nm is not None else fid,
        })
    return out


def get_dataflows() -> list[dict]:
    """Union the catalogs from BOTH hosts, dedupe by flow id.

    esploradati exposes ~4,874 granular DF_* flows; sdmx.istat.it ~509 classical
    flows. We need the union so no incomplete flow is missed just because one
    host is down. detail=allstubs keeps the catalog light enough to survive the
    flaky host.
    """
    seen: dict[str, dict] = {}
    for label, base in HOSTS:
        got = None
        for url in (f"{base}dataflow/{AGENCY}?detail=allstubs",
                    f"{base}dataflow/{AGENCY}",
                    f"{base}dataflow/all?detail=allstubs"):
            res = http_get(url, STR_ACCEPT)
            if res.ok and res.content:
                got = _parse_flow_ids(res.content)
                if got:
                    break
        if got:
            new = 0
            for f in got:
                if f["id"] not in seen:
                    seen[f["id"]] = f
                    new += 1
            log(f"  catalog[{label}]: {len(got)} flows ({new} new) | total {len(seen)}")
        else:
            log(f"  catalog[{label}]: UNAVAILABLE (host down/erroring)")
    return list(seen.values())


# ──────────────────────────────── DSD / dimensions ─────────────────────────

# Per-run memo so we never re-fetch a structure we already resolved (or already
# failed to resolve). Negative caching matters: when a flow's DSD 500s, the
# per-year dimension-slice fallback would otherwise re-hammer the same dead URL
# for every single year.
_DIMS_CACHE: dict[str, list | None] = {}
_CL_CACHE:   dict[tuple, list]      = {}


def get_dimensions(flow: dict) -> list[dict] | None:
    """Return ordered dimensions [{id, position, codelist, agency}] for a flow,
    excluding TIME_PERIOD, by reading its DSD from whichever host answers.
    Used only when we need to slice by a dimension (key-path). None if no host
    can serve the structure. Memoized (incl. negative results) for the run."""
    fid = flow["id"]
    if fid in _DIMS_CACHE:
        return _DIMS_CACHE[fid]
    for label, base in HOSTS:
        url = (f"{base}dataflow/{AGENCY}/{fid}"
               f"?references=datastructure&detail=full")
        res = http_get(url, STR_ACCEPT)
        if not (res.ok and res.content):
            continue
        try:
            root = ET.fromstring(res.content)
        except ET.ParseError:
            continue
        dims = []
        for d in root.findall(".//str:DimensionList/str:Dimension", NS):
            did = d.get("id")
            pos = d.get("position")
            if not did or did == "TIME_PERIOD":
                continue
            enr = d.find(".//str:Enumeration/str:Ref", NS)
            dims.append({
                "id":       did,
                "position": int(pos) if pos and pos.isdigit() else len(dims) + 1,
                "codelist": enr.get("id") if enr is not None else None,
                "cl_agency": enr.get("agencyID") if enr is not None else AGENCY,
            })
        if dims:
            dims.sort(key=lambda x: x["position"])
            log(f"    DSD[{label}] {fid}: {len(dims)} dims "
                f"({', '.join(d['id'] for d in dims)})")
            _DIMS_CACHE[fid] = dims
            return dims
    _DIMS_CACHE[fid] = None
    return None


def get_codelist(cl_id: str, cl_agency: str) -> list[str]:
    """Return the code ids of a codelist (for dimension slicing). [] if no host
    serves it. Memoized for the run."""
    if not cl_id:
        return []
    ck = (cl_agency, cl_id)
    if ck in _CL_CACHE:
        return _CL_CACHE[ck]
    for label, base in HOSTS:
        res = http_get(f"{base}codelist/{cl_agency}/{cl_id}", STR_ACCEPT)
        if not (res.ok and res.content):
            continue
        try:
            root = ET.fromstring(res.content)
        except ET.ParseError:
            continue
        codes = [c.get("id") for c in root.findall(".//str:Code", NS)
                 if c.get("id")]
        if codes:
            _CL_CACHE[ck] = codes
            return codes
    _CL_CACHE[ck] = []
    return []


# ──────────────────────────────── data fetch + parse ───────────────────────

def _parse_data(content: bytes) -> tuple[list, list, list]:
    """Dispatch to the house CSV/XML parsers based on the response body."""
    if not content:
        return [], [], []
    head = content[:100].lstrip()
    if head.startswith(b"<"):
        return parse_sdmx_xml(content)
    return parse_sdmx_csv(content)


def build_data_url(base: str, flow_id: str, key_path: str,
                   start: str | None, end: str | None) -> str:
    # SDMX 2.1 REST: data/{agency},{flow}/{key}?startPeriod=..&endPeriod=..
    key = key_path or ""
    url = f"{base}data/{AGENCY},{flow_id}/{key}"
    params = []
    if start:
        params.append(f"startPeriod={start}")
    if end:
        params.append(f"endPeriod={end}")
    if params:
        url += "?" + "&".join(params)
    return url


def fetch_slice(flow_id: str, key_path: str = "",
                start: str | None = None, end: str | None = None,
                max_size: int = MAX_SIZE
                ) -> tuple[list, list, list, str, str]:
    """Try every host for one slice. Returns
    (keys, dates, values, status, host_label).

    status: ok | empty | toobig | http500 | timeout | conn | http<code>
    A 500/timeout/conn/toobig is a signal to the caller to subdivide further;
    'empty' means the slice genuinely has no observations.
    """
    worst = "conn"
    worst_host = ""
    for label, base in HOSTS:
        url = build_data_url(base, flow_id, key_path, start, end)
        res = http_get(url, CSV_ACCEPT, max(TIMEOUT, 60))
        if res.ok and res.content is not None:
            if len(res.content) > max_size:
                log(f"    {flow_id} [{label}] {key_path or 'ALL'} "
                    f"{start or ''}-{end or ''}: {len(res.content)/1e6:.0f} MB "
                    f"> {max_size/1e6:.0f} MB ceiling -> subdivide")
                worst, worst_host = "toobig", label
                continue
            k, d, v = _parse_data(res.content)
            if not v:
                # CSV had no rows -> try XML in case host served an odd body
                res2 = http_get(url, XML_ACCEPT, max(TIMEOUT, 60))
                if res2.ok and res2.content:
                    k, d, v = _parse_data(res2.content)
            if v:
                return k, d, v, "ok", label
            # genuine empty slice from a 200 response
            return [], [], [], "empty", label
        # not ok: classify for the caller
        if res.is_500:
            worst, worst_host = "http500", label
        elif res.kind == "timeout" and worst not in ("http500",):
            worst, worst_host = "timeout", label
        elif res.kind == "http" and worst not in ("http500", "timeout"):
            worst, worst_host = f"http{res.status}", label
        elif worst == "conn":
            worst_host = label
    return [], [], [], worst, worst_host


# ──────────────────────────────── slicing strategy ─────────────────────────

def _accumulate(store: dict, keys, dates, vals):
    """De-dupe obs by (series_key, obs_date) into store. Last write wins
    (revisions); identical chunks are idempotent -> safe to overlap slices."""
    for k, d, v in zip(keys, dates, vals):
        store[(k, d)] = v


def _retryable(status: str) -> bool:
    """True if the status means 'response too heavy' (subdivide), vs a hard
    'no data' (4xx empty)."""
    return status in ("toobig", "http500", "timeout", "conn") or (
        status.startswith("http") and status[4:].isdigit() and int(status[4:]) >= 500
    )


def pull_flow(flow: dict, max_size: int = MAX_SIZE
              ) -> tuple[dict, str, str]:
    """Retrieve a complete flow, escalating full -> decade -> year -> dimension.

    Returns (obs_store, method, detail) where obs_store maps (key,date)->value
    (possibly empty) and method is full|decade|year|dim|none. detail carries the
    failure reason when method == 'none'.
    """
    fid = flow["id"]

    # 1) Plain full pull -------------------------------------------------------
    k, d, v, status, host = fetch_slice(fid, max_size=max_size)
    if status == "ok":
        store: dict = {}
        _accumulate(store, k, d, v)
        return store, "full", host
    if status == "empty":
        return {}, "full", host           # legitimately empty flow
    if not _retryable(status):
        return {}, "none", f"full pull {status} on all hosts"
    log(f"    {fid}: full pull -> {status}; time-slicing by decade")

    # 2) Decade slicing --------------------------------------------------------
    store = {}
    failed_decades: list[tuple] = []
    any_ok = False
    any_toobig = False        # did any window fail purely on SIZE (narrowing helps)?
    decade_hard_fail = None
    last_decade_status = status
    for s, e in DECADES:
        kk, dd, vv, st, hh = fetch_slice(fid, start=s, end=e, max_size=max_size)
        label = f"{s or 'pre'}-{e}"
        if st == "ok":
            _accumulate(store, kk, dd, vv)
            any_ok = True
            log(f"      decade {label}: +{len(vv):,} obs (store {len(store):,})")
        elif st == "empty":
            pass
        elif _retryable(st):
            last_decade_status = st
            if st == "toobig":
                any_toobig = True
            log(f"      decade {label}: {st} -> per-year fallback")
            failed_decades.append((s, e))
        else:
            log(f"      decade {label}: {st} (skip)")
        time.sleep(RATE)

    # Short-circuit: every decade failed with a HARD server error (never a size
    # error, never a success) AND the DSD can't be fetched on any host. Narrowing
    # the period or splitting by dimension both require the server to serve SOME
    # request for this flow; it won't. Don't grind 60+ futile per-year calls.
    if (failed_decades and not any_ok and not any_toobig
            and get_dimensions(flow) is None):
        return store, "none", (
            f"every time window {last_decade_status} on all hosts and DSD "
            f"unavailable -> host cannot serve this flow (no slice possible)")

    # 3) Per-year fallback for decades that still choked. Narrow ranges
    #    sometimes succeed where a whole decade 500s, so this is worth trying
    #    even for non-size errors -- but we stop a decade early once it's clear
    #    every year returns the same hard error and no dimension split is possible.
    for s, e in failed_decades:
        if s is None:
            # open-ended pre-1960 chunk that 500s -> can't year-slice unboundedly
            decade_hard_fail = "pre-1960 chunk unretrievable"
            continue
        y0, y1 = int(s), int(e)
        decade_year_ok = False
        decade_year_fail = 0
        for y in range(y0, y1 + 1):
            ys = str(y)
            kk, dd, vv, st, hh = fetch_slice(fid, start=ys, end=ys,
                                             max_size=max_size)
            if st == "ok":
                _accumulate(store, kk, dd, vv)
                any_ok = True
                decade_year_ok = True
                log(f"        year {ys}: +{len(vv):,} obs (store {len(store):,})")
            elif st == "empty":
                decade_year_ok = True
            elif st == "toobig":
                # 4) Dimension slicing for this genuinely-too-big year --------
                if _dimension_slice(flow, store, start=ys, end=ys,
                                    max_size=max_size):
                    any_ok = True
                    decade_year_ok = True
                else:
                    decade_hard_fail = f"year {ys} toobig, dimension slice failed"
            elif _retryable(st):
                decade_year_fail += 1
                decade_hard_fail = f"year {ys} {st}"
                # If the first few years of this decade all hard-fail the same
                # way and no DSD exists, the rest will too -- bail this decade.
                if (decade_year_fail >= 3 and not decade_year_ok
                        and get_dimensions(flow) is None):
                    log(f"        decade {s}-{e}: first {decade_year_fail} years "
                        f"all {st}, DSD unavailable -> abandoning decade")
                    break
            time.sleep(RATE)

    if any_ok and store:
        method = "year" if failed_decades else "decade"
        return store, method, ""

    # If decade slicing produced nothing AND nothing was retryable-but-empty,
    # try a whole-flow dimension slice before giving up (covers flows that 500
    # on every time window but succeed when split by territory/category).
    log(f"    {fid}: time slicing yielded nothing; trying dimension slice")
    store = {}
    if _dimension_slice(flow, store, max_size=max_size) and store:
        return store, "dim", ""

    reason = decade_hard_fail or f"all slices failed (last full status {status})"
    return store, "none", reason


def _dimension_slice(flow: dict, store: dict,
                     start: str | None = None, end: str | None = None,
                     max_size: int = MAX_SIZE) -> bool:
    """Split the key-path on the BEST dimension (most codes among the small
    enumerated ones) and pull one request per code, optionally within a time
    window. Recurses one extra level if a single code is still too big.
    Returns True if any observations were added."""
    dims = get_dimensions(flow)
    if not dims:
        log(f"      dimension slice: no DSD available on any host -> cannot slice")
        return False

    # Resolve codelists; pick the dimension whose codelist is enumerable and
    # has the most codes (best split granularity) but isn't absurdly large.
    candidates = []
    for dim in dims:
        codes = get_codelist(dim["codelist"], dim.get("cl_agency", AGENCY))
        if 1 < len(codes) <= 4000:
            candidates.append((dim, codes))
    if not candidates:
        log(f"      dimension slice: no usable enumerated dimension")
        return False
    candidates.sort(key=lambda x: len(x[1]), reverse=True)
    split_dim, split_codes = candidates[0]
    pos = dims.index(split_dim)        # 0-based position among non-time dims
    ndim = len(dims)
    log(f"      dimension slice on {split_dim['id']} "
        f"({len(split_codes)} codes, pos {pos+1}/{ndim})")

    added = False
    fid = flow["id"]
    for ci, code in enumerate(split_codes, 1):
        # Build a full-width key: empty everywhere except the split position.
        parts = [""] * ndim
        parts[pos] = code
        key_path = ".".join(parts)
        kk, dd, vv, st, hh = fetch_slice(fid, key_path=key_path,
                                         start=start, end=end, max_size=max_size)
        if st == "ok":
            _accumulate(store, kk, dd, vv)
            added = True
            if ci % 25 == 0 or len(vv):
                log(f"        {split_dim['id']}={code} [{ci}/{len(split_codes)}]"
                    f": +{len(vv):,} (store {len(store):,})")
        elif st == "empty":
            pass
        elif _retryable(st) and ndim > 1:
            # One code still too big: split again on the next-best dimension,
            # keeping this code fixed. (Handled by a focused 2nd-dim sweep.)
            if _dimension_slice_second(flow, store, dims, pos, code,
                                       start=start, end=end, max_size=max_size):
                added = True
        time.sleep(RATE)
    return added


def _dimension_slice_second(flow: dict, store: dict, dims: list,
                            fixed_pos: int, fixed_code: str,
                            start: str | None, end: str | None,
                            max_size: int) -> bool:
    """Second-level split: fix one dimension, split on the next-best one."""
    fid = flow["id"]
    ndim = len(dims)
    # choose the largest OTHER enumerated dimension
    best = None
    for i, dim in enumerate(dims):
        if i == fixed_pos:
            continue
        codes = get_codelist(dim["codelist"], dim.get("cl_agency", AGENCY))
        if 1 < len(codes) <= 4000 and (best is None or len(codes) > len(best[2])):
            best = (i, dim, codes)
    if best is None:
        return False
    i2, dim2, codes2 = best
    added = False
    for code2 in codes2:
        parts = [""] * ndim
        parts[fixed_pos] = fixed_code
        parts[i2] = code2
        kk, dd, vv, st, hh = fetch_slice(fid, key_path=".".join(parts),
                                         start=start, end=end, max_size=max_size)
        if st == "ok":
            _accumulate(store, kk, dd, vv)
            added = True
        time.sleep(RATE)
    return added


# ──────────────────────────────── parquet write ────────────────────────────

def write_flow(flow_id: str, store: dict) -> int:
    """Write the de-duped obs store to one zstd parquet. Returns row count."""
    if not store:
        return 0
    # Deterministic order: by (series_key, obs_date)
    items = sorted(store.items(), key=lambda kv: (kv[0][0], kv[0][1]))
    keys  = [k for (k, _), _ in items]
    dates = [d for (_, d), _ in items]
    vals  = [v for _, v in items]
    out_path = os.path.join(OUT_DIR, f"{flow_id}.parquet")
    tbl = pa.table({
        "series_key": pa.array(keys,  pa.string()),
        "obs_date":   pa.array(dates, pa.date32()),
        "value":      pa.array(vals,  pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    return pq.read_metadata(out_path).num_rows


# ──────────────────────────────── checkpoint I/O ───────────────────────────

def _load_json(path: str, default):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception as e:
            log(f"  WARN could not read {os.path.basename(path)}: {e}")
    return default


def _save_json(path: str, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, indent=2, default=str)
    os.replace(tmp, path)


def is_incomplete(flow_id: str, done: set, unrec: dict) -> bool:
    """A flow is incomplete if no parquet exists for it yet. (A done-marker or
    a prior unrecoverable entry are handled by the caller's skip logic.)"""
    out_path = os.path.join(OUT_DIR, f"{flow_id}.parquet")
    return not os.path.exists(out_path)


# ──────────────────────────────── main ─────────────────────────────────────

def _reachable(host: str, port: int = 443, timeout: float = 6.0) -> bool:
    """TCP-connect probe. Cheap, and it discriminates the two ISTAT failure modes that
    otherwise look identical in the log (both end as 'host unavailable' after minutes of
    retries): a host that is DOWN at the network layer vs one that answers and misbehaves."""
    import socket
    try:
        socket.create_connection((host, port), timeout=timeout).close()
        return True
    except OSError:
        return False


def _preflight() -> bool:
    """FAIL FAST when ISTAT itself is down (R62 class: the old behaviour burned 5 x 21 s
    ladders per path on BOTH hosts — ~10 minutes per launch — and the guard relaunched
    it forever, so an upstream outage looked like a busy crawler).

    MEASURED 2026-08-07: esploradati.istat.it resolves (193.204.90.13) but TCP:443 times
    out; sdmx.istat.it connects instantly and 302-redirects EVERY SDMX path to its own
    root (an infinite self-redirect), so it cannot serve the catalogue either. With both
    in that state there is nothing to crawl, and saying so in one line beats discovering
    it again in ten minutes.
    """
    import urllib.parse
    import requests
    alive = []
    for label, base in HOSTS:
        host = urllib.parse.urlparse(base).hostname
        if not _reachable(host):
            print(f"  preflight: {label} ({host}) UNREACHABLE at TCP:443 - skipping", flush=True)
            continue
        # TCP-alive is not SERVING. sdmx.istat.it connects in 0.1 s and answers every SDMX
        # path with 302 -> its own root; following that is an infinite loop and retrying it
        # is pure waste. One un-followed request tells them apart.
        try:
            probe = requests.get(base + "dataflow/" + AGENCY, timeout=15,
                                 allow_redirects=False,
                                 headers={**UA, "Accept": STR_ACCEPT})
        except requests.RequestException as e:
            print(f"  preflight: {label} ({host}) probe failed ({type(e).__name__}) - skipping",
                  flush=True)
            continue
        loc = (probe.headers.get("Location") or "").rstrip("/")
        if probe.status_code in (301, 302, 303, 307, 308) and                 urllib.parse.urlparse(loc or "").hostname in (host, None):
            print(f"  preflight: {label} ({host}) SELF-REDIRECTS {probe.status_code} -> "
                  f"{loc or '(same host)'} - not serving SDMX, skipping", flush=True)
            continue
        alive.append(label)
    if not alive:
        print("  preflight: NO ISTAT HOST REACHABLE - upstream outage, exiting immediately "
              "(re-probe on the next guard tick; nothing to crawl, nothing lost)", flush=True)
        return False
    print(f"  preflight: reachable host(s): {', '.join(alive)}", flush=True)
    return True


def main():
    args = sys.argv[1:]
    if args and args[0] in ("-h", "--help"):
        print(__doc__); return

    if not _preflight():
        raise SystemExit(3)          # distinct code: upstream down, not a crawler fault

    only_ids: set = set()
    list_only = False
    max_size  = MAX_SIZE
    i = 0
    while i < len(args):
        a = args[i]
        if a == "--list":
            list_only = True
        elif a.startswith("--only"):
            val = a.split("=", 1)[1] if "=" in a else (args[i + 1] if i + 1 < len(args) else "")
            if "=" not in a:
                i += 1
            only_ids = set(filter(None, (x.strip() for x in val.split(","))))
        elif a.startswith("--max-size-mb"):
            val = a.split("=", 1)[1] if "=" in a else (args[i + 1] if i + 1 < len(args) else "")
            if "=" not in a:
                i += 1
            max_size = int(float(val) * 1024 * 1024)
        i += 1

    os.makedirs(OUT_DIR, exist_ok=True)
    done  = set(_load_json(DONE_PATH, []))
    unrec = _load_json(UNREC_PATH, {})

    log("ISTAT sliced completion pass")
    log(f"Output: {OUT_DIR}")
    log(f"Size ceiling per response: {max_size/1e6:.0f} MB | rate {RATE}s "
        f"| timeout {TIMEOUT}s | retries {RETRIES}")

    # ── Determine the working set ────────────────────────────────────────────
    if only_ids:
        # Explicit flows: fetch their catalog metadata if possible, else
        # synthesize a minimal record. In --only mode we INTENTIONALLY ignore
        # the done-checkpoint and existing-parquet skip so the slicing logic can
        # be exercised/verified on demand.
        catalog = {f["id"]: f for f in get_dataflows()}
        flows = []
        for fid in sorted(only_ids):
            flows.append(catalog.get(fid, {"id": fid, "agency": AGENCY,
                                           "version": "1.0", "name": fid}))
        log(f"--only mode: {len(flows)} flow(s) "
            f"(ignoring skip-existing + done checkpoint for testing)")
    else:
        log("Fetching dataflow catalog (union of both hosts)...")
        catalog_flows = get_dataflows()
        log(f"Catalog union: {len(catalog_flows)} flows")
        if not catalog_flows:
            log("FATAL: no catalog available from either ISTAT host "
                "(both down/erroring). Nothing to do; watcher will relaunch.")
            return
        flows = [f for f in catalog_flows
                 if is_incomplete(f["id"], done, unrec) and f["id"] not in done]
        log(f"Incomplete (no parquet yet, not done): {len(flows)} flows")

    if list_only:
        for f in flows:
            print(f"  {f['id']:48s}  {f.get('name','')}")
        return

    if not flows:
        log("Nothing incomplete. Done.")
        return

    total_obs   = 0
    n_written   = 0
    n_completed = 0
    n_unrec     = 0
    n_deferred  = 0
    for idx, flow in enumerate(flows, 1):
        fid = flow["id"]
        log(f"[{idx}/{len(flows)}] {fid}  [{flow.get('name','')[:54]}]")
        out_path = os.path.join(OUT_DIR, f"{fid}.parquet")

        # Resumability for the full run: skip if already present (unless --only).
        if not only_ids and os.path.exists(out_path):
            log(f"  already present, skipping")
            done.add(fid)
            continue

        globals()["_FLOW_DEADLINE"] = time.time() + FLOW_BUDGET_S
        globals()["_BUDGET_HIT"] = False
        try:
            store, method, detail = pull_flow(flow, max_size=max_size)
        except Exception as e:
            log(f"  EXCEPTION pulling {fid}: {type(e).__name__}: {e}")
            unrec[fid] = {"reason": f"exception {type(e).__name__}: {e}",
                          "ts": dt.datetime.now().isoformat(timespec="seconds")}
            _save_json(UNREC_PATH, unrec)
            n_unrec += 1
            continue

        if _BUDGET_HIT and (method == "none" or not store):
            # Ran out of budget, not out of options. Recording this as UNRECOVERABLE
            # would be a false verdict that permanently retires a flow whose only sin
            # is being slow, so leave it untouched: no done, no unrec, so the next run
            # picks it up exactly as it stands now.
            # Clear any stale UNRECOVERABLE verdict. Before the budget existed, a slow
            # flow that exhausted its retries was recorded as unrecoverable — a false
            # verdict this branch now knows to be wrong. Leaving the old entry would
            # make the log line ("NOT marked unrecoverable") true of this run and false
            # of the file anyone actually reads.
            if unrec.pop(fid, None) is not None:
                _save_json(UNREC_PATH, unrec)
                log(f"  cleared stale UNRECOVERABLE verdict on {fid} (it is slow, not broken)")
            log(f"  BUDGET ({FLOW_BUDGET_S // 60} min) exhausted on {fid} — deferred "
                f"to the next run, NOT marked unrecoverable")
            n_deferred += 1
            continue

        if method == "none":
            log(f"  UNRECOVERABLE: {detail}")
            unrec[fid] = {"reason": detail,
                          "ts": dt.datetime.now().isoformat(timespec="seconds")}
            _save_json(UNREC_PATH, unrec)
            n_unrec += 1
            continue

        if not store:
            # method != none but no obs: the flow is genuinely empty. Record so
            # the watcher doesn't keep retrying, but don't write an empty file.
            log(f"  {fid}: 0 obs (method {method}) -- flow empty, marking done")
            done.add(fid)
            unrec.pop(fid, None)
            _save_json(DONE_PATH, sorted(done))
            n_completed += 1
            continue

        n = write_flow(fid, store)
        total_obs += n
        n_written += 1
        n_completed += 1
        done.add(fid)
        unrec.pop(fid, None)          # clear any stale unrecoverable mark
        _save_json(DONE_PATH, sorted(done))
        if fid in unrec:
            _save_json(UNREC_PATH, unrec)
        log(f"  -> WROTE {fid}: {n:,} obs (method: {method}) | "
            f"running total {total_obs:,}")
        time.sleep(RATE)

    # Persist final state
    _save_json(DONE_PATH, sorted(done))
    _save_json(UNREC_PATH, unrec)

    log("=" * 60)
    log(f"DONE. flows completed this run: {n_completed} | "
        f"newly written parquet: {n_written} | "
        f"unrecoverable: {n_unrec} | deferred (budget): {n_deferred} | "
        f"new observations: {total_obs:,}")
    if unrec:
        log(f"Unrecoverable list ({len(unrec)} total) -> {UNREC_PATH}")


if __name__ == "__main__":
    main()
