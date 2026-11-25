"""S3 (sdmx_delta) fetcher — ISTAT Italy. Keyless open data (CC BY 3.0 IT).

Layout (set by the retired SDMX ingester (its parsers now live in jobs/_sdmx_common.py) + jobs/ingest_istat_sliced.py, SHARED dir):
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
  - REUSES the retired SDMX ingester (its parsers now live in jobs/_sdmx_common.py) verbatim for endpoints/agency/Accept and for the
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

WINDOW STOP (registry `window_stop: true`, 2026-11-24; plan review R2100, VERDICT_planA). update()
stops at the pass's run ceiling, or at its per-turn cap (AQUEDUCT_ISTAT_TURN_MAX_MIN): no flow starts
within margin + max(2 x the longest flow, FLOW_FLOOR_S) + the CSV allowance (CSV_S_PER_FLOW per flow
handed over) of it, and every GET is clamped to end by that point minus the margin. The cap bounds the
whole TURN: the Result carries its end (turn_end_ts) and run_once bounds the CSV phase by it (phase B
review B1). The start gate reserves window_admission_reserve_min() = margin 5 (chosen)
+ FLOW_FLOOR_S 16.0 (one pacer slot wait per GET included) + setup 1 (chosen) = 22.0 min, not the latest
real run. Result.window_stop is set
only by those stops: "none" when no flow was started (a non-turn, a `window_stopped` row), else
"progress". The every-host-dead break, the budget stop and the end of the list leave it None, so an
ISTAT outage stays a recorded `partial` turn. A structural flow no longer loses the run's hand-over:
the run returns `partial` WITH the cursors of every walked flow (ROUTE B, as census).

PACING (2026-11-25; DESIGN F:/work/econ_finish/istat_pacing/DESIGN.md, review VERDICT_design V1-V14).
ISTAT, "SDMX web services" (https://www.istat.it/en/classifications-and-tools/sdmx-web-services/, read
2026-11-24): "a limit of 5 queries per minute has been set for each IP", and a block of 1 to 2 days when
it is exceeded (secondary: ondata.github.io/guida-api-istat). Until this change this fetcher had NO
limiter: by omission, not by decision - the crawler's ISTAT_QPM came in commits that touched only the
crawler, and nothing here read it (R132/R46). In the 2026-11-22 turn at least 17 of its own GETs started
in one closed 60 s (DESIGN section 4, from 18 store writes). Now every request takes a token of ONE
budget per machine shared by every ISTAT host
and every process (jobs/_istat_pacer.py: 16 s between token starts, at most 4 in any closed 60 s):
  - a token before every GET and every TCP probe, taken BEFORE the request clamp, with the request end
    as its deadline; a slot past it is a deferral (_WindowEnd), never a failure and never a dead host;
  - allow_redirects=False on every GET: a 3xx is ONE request. A same-host redirect to a different path
    is followed once, with its own token (V5(a)); a self-, cross-host or second redirect marks the host
    unusable for the run (today's sdmx.istat.it: 3 tokens per run - 1 TCP probe and 2 GETs, because its 302
    to the site root is a same-host redirect and is followed once - instead of ~31 requests; review B 3a);
  - a 429 moves the shared next slot to its Retry-After (60 s to 48 h) for every participant;
  - update() writes the turn marker (begin_turn) before its loop and clears it (end_turn) in a
    `finally`, so the crawler starts no new flow while istat's turn runs; the progress lines carry
    `pacer: <n> token(s), waited <s> s, mode <mode>`.
An ad-hoc HTTP request or TCP probe to any ISTAT host from this workstation spends the shared budget.
Send it through `jobs/_istat_pacer.acquire`, or from a second egress. A curl or a browser hit is not
paced. The repo's own request paths all take tokens: this fetcher, jobs/ingest_istat_sliced.py (also
`--list` and `--only`, which run _preflight() first: one TCP connect and one GET per host),
tools/catalog_istat_flows.py (`--apply` with `--refresh-names` or a cache miss; a dry run sends nothing)
and tools/probe_istat_hosts.py. The combined rate is measured only by
`python tools/check_istat_rate.py --from <utc> --to <utc>`; it counts FAILSAFE-TOKEN and UNLOGGED lines from
the process logs too (pass `--process-log` the pass log and the guard_istat_sliced_*.log files). WHERE TO COUNT
the pacer's trouble lines on the workstation, per pass: `[istat-pacer] FAIL-SAFE`, `FAILSAFE-TOKEN` and
`UNLOGGED` in logs/local_heavy_updater_<stamp>.log (this fetcher, one log per pass) and in
logs/guard_istat_sliced_<stamp>.log (the crawler, one log per guard launch), and `ERROR outcome not logged` in
both: the phase-A tests saw a lock busy past its wait in 4 of 20 TB2 runs, cause unknown, so the production rate
is what says whether the 5-s lock wait is enough.
LIMITS, stated, not enforced:
  - OLD CODE IS NOT PACED. Any checkout at a commit before this change - E:/research/econ_wt_* worktrees, the
    F: clones, an old crawler process still running at pull time - sends unpaced. Run istat code only from a
    checkout that has jobs/_istat_pacer.py, and check the old crawler is gone before the first paced turn
    (verdict V9: a guard_istat_sliced_*.log written after the pull that carries an `[istat-pacer]` line).
  - ONE TOKEN PER socket.create_connection CALL. create_connection tries every address getaddrinfo returns, so if
    an ISTAT name ever resolves to several addresses a failed first connect is followed by another under the same
    token. The repo records one IPv4 address per host (.13, .1, .61); not measured, because no test resolves an
    ISTAT name.
  - V5 IS UNPROVEN FOR DATA URLs. No log before this change shows whether esploradati answers a DATA URL with a
    same-host 3xx (requests followed redirects silently). The first paced pass's receipt is the count of 3xx
    outcomes per host in the request log (`O ... outcome=<3xx>-><Location>:<follow|dead>` lines): a host whose
    data URLs 3xx to another path is followed once per request (two tokens); any other 3xx ends that host's run.
"""
from __future__ import annotations
import datetime as dt
import math
import os
import sys
import time

import pyarrow as pa
import pyarrow.compute as pc
import requests

from ... import config, blob, merge
from ...errors import DefinitiveError, TransientError
from ..base import Result
from ._common import (Deadline, Tally, finalize, load_rotation, rotate_after,
                      save_rotation)

# Reuse the shared SDMX parsers verbatim (jobs/ is on sys.path for the orchestrator
# and the live-test; fall back to a by-path load otherwise).
try:
    from jobs._sdmx_common import (  # type: ignore
        UA, parse_sdmx_csv, parse_sdmx_xml,
    )
except ImportError:  # pragma: no cover - path fallback
    import importlib.util as _ilu

    _src = os.path.join(config.ROOT, "jobs", "_sdmx_common.py")
    _spec = _ilu.spec_from_file_location("_sdmx_common", _src)
    _mod = _ilu.module_from_spec(_spec)
    _spec.loader.exec_module(_mod)  # type: ignore
    UA = _mod.UA
    parse_sdmx_csv = _mod.parse_sdmx_csv
    parse_sdmx_xml = _mod.parse_sdmx_xml

# THE SHARED ISTAT PACER (jobs/_istat_pacer.py; DESIGN 6.9): imported as jobs._sdmx_common is. The by-path
# fallback registers the module under its package name, so a process never holds two copies (two in-process
# "own last start" values would be two buckets).
try:
    from jobs import _istat_pacer as _pacer  # type: ignore
except ImportError:  # pragma: no cover - path fallback
    import importlib.util as _ilu

    _psrc = os.path.join(config.ROOT, "jobs", "_istat_pacer.py")
    _pspec = _ilu.spec_from_file_location("jobs._istat_pacer", _psrc)
    _pacer = _ilu.module_from_spec(_pspec)
    sys.modules["jobs._istat_pacer"] = _pacer
    _pspec.loader.exec_module(_pacer)  # type: ignore

SOURCE = "istat"
AGENCY = "IT1"
DEDUP = ("series_key", "obs_date")

# Host order = preference (mirrors jobs/ingest_istat_sliced.py HOSTS). sdmx.istat.it
# answers fast when healthy; esploradati carries the granular DF_* flows but is flaky.
HOSTS = [
    ("sdmx",    "https://sdmx.istat.it/SDMXWS/rest/"),
    ("esplora", "https://esploradati.istat.it/SDMXWS/rest/"),
]
# Bases found UNUSABLE during THIS run (redirect loop, or unreachable transport).
# Per-run, not persisted: a host ISTAT repairs must come back on the next run without
# anyone editing a list.
_DEAD_HOSTS: set = set()

# TRANSPORT DEATH IS A HOST FAULT TOO — and it was the expensive one. Until 2026-08-28
# only TooManyRedirects marked a base dead; a timeout or refused connection returned a
# bare `transient`, so a base that is dead AT THE SOCKET cost its full timeout on EVERY
# flow, forever. Measured that morning, with a control: esploradati.istat.it TCP 443
# times out (15s connect probe) while sdmx.istat.it and www.istat.it both open in 0.2s —
# ISTAT's outage, not our egress. esploradati carries HOST_TIMEOUT 300s and RETRIES 3,
# so one flow cost ~900s; istat has 2,483 flows and the workstation pass has a ~221-min
# budget, so istat consumed the ENTIRE nightly local-heavy budget and was hard-killed
# before recording a single run row. That starved the other 28 local sources: bls, eia,
# bea, statcan and fhfa were last ATTEMPTED 2026-08-18..22 and sat RED-DATA for it.
#
# Two guards, cheapest first, and both must respect the lesson HOST_TIMEOUT records —
# esploradati was measured SLOW BUT WORKING (76.0s/102.6s/123.7s full-body 200s on
# 2026-08-24), so slowness must never be read as death:
#   1. a TCP-connect probe (short, cached per run) — it separates "unreachable" from
#      "slow to serve a body", which a request timeout alone cannot do;
#   2. a consecutive-transport-failure counter as the backstop for a base that accepts
#      the connection and then hangs. ANY completed reply on that base resets it — a
#      404 NoRecordsFound proves the transport just as well as a 200 does.
_TCP_PROBE_TIMEOUT = 10      # connect only; a healthy-but-slow host still connects fast
# _HOST_DEAD_AFTER is defined below, next to RETRIES, because it must EXCEED it.
_TRANSPORT_FAILS: dict = {}
_TCP_PROBED: dict = {}


def _base_of(url: str) -> "str | None":
    """The HOSTS base a URL belongs to, or None."""
    for _lbl, base in HOSTS:
        if url.startswith(base):
            return base
    return None


def _mark_dead(base: str, why: str) -> None:
    if base not in _DEAD_HOSTS:
        print(f"[istat] {base} {why}; skipping it for the rest of this run", flush=True)
    _DEAD_HOSTS.add(base)


def _note_transport_failure(base: "str | None") -> None:
    """Count a timeout/conn-drop against a base; mark it dead at the threshold."""
    if not base:
        return
    n = _TRANSPORT_FAILS.get(base, 0) + 1
    _TRANSPORT_FAILS[base] = n
    if n >= _HOST_DEAD_AFTER:
        _mark_dead(base, f"failed transport {n}x consecutively with no success")


def _note_success(base: "str | None") -> None:
    """ANY completed HTTP response proves the TRANSPORT is alive — clear the streak.

    Called on every reply, not only 200s: a 404 "NoRecordsFound" is this fetcher's
    documented quiet-tail case and a whole run of them is a legitimate no_change, so
    scoring only 200s would let a host serving nothing but empty windows accrue
    transport failures until it was wrongly declared dead (review 2026-08-28).
    """
    if base:
        _TRANSPORT_FAILS[base] = 0


# THIS RUN'S REQUEST END (ceiling - margin, or None), for the TCP probe below: update() sets it and clears it
# when the run ends, like _DEAD_HOSTS, so the probe keeps its one-argument shape (the test rigs replace it with
# `lambda base: ...`).
_RUN_REQ_END = [None]
# THIS RUN'S PACER COUNTS, for the progress line: tokens taken, seconds waited for them, tokens per mode.
_RUN_PACE: dict = {"tokens": 0, "waited_s": 0.0, "modes": {}}


def _take_token(where: str, kind: str, path_class: str, deadline_ts, flow=None):
    """ONE token of the shared ISTAT budget for ONE request (DESIGN 7), taken BEFORE the request clamp reads the
    time left (C8a). The pacer refuses a slot later than deadline_ts - 1 s with WindowEnd (NoSendPlatform on WSL),
    and that becomes _WindowEnd: the flow is DEFERRED (R303), never failed, and no host is marked dead. A pacer
    wait is not a transport failure: only the `except` of the request itself calls _note_transport_failure (C7)."""
    try:
        tok = _pacer.acquire("updater", where, kind, path_class, deadline_ts=deadline_ts, flow=flow, clock=time)
    except _pacer.WindowEnd as e:
        raise _WindowEnd(f"no ISTAT pacer slot before the margin before the run ceiling ({e})") from e
    _RUN_PACE["tokens"] += 1
    _RUN_PACE["waited_s"] += float(getattr(tok, "waited", 0.0) or 0.0)
    mode = str(getattr(tok, "mode", "?"))
    _RUN_PACE["modes"][mode] = _RUN_PACE["modes"].get(mode, 0) + 1
    return tok


def _pace_text() -> str:
    """`pacer: <n> token(s), waited <s> s, mode <mode>` for this run (DESIGN 7)."""
    modes = _RUN_PACE["modes"]
    if not modes:
        mode = "-"
    elif len(modes) == 1:
        mode = next(iter(modes))
    else:
        mode = "mixed (" + ", ".join(f"{m} {n}" for m, n in sorted(modes.items())) + ")"
    return f"pacer: {_RUN_PACE['tokens']} token(s), waited {_RUN_PACE['waited_s']:.1f} s, mode {mode}"


def _tcp_reachable(base: str) -> bool:
    """One cached TCP-connect probe per base per run. True when the socket opens.

    THE CONNECT TAKES A TOKEN (DESIGN Q2: ISTAT does not say a bare connect is free), first, with this run's
    request end as its deadline; and the connect timeout is clamped to the time left, as _get clamps a GET. A
    connect cut by that clamp is the window ending, not the host failing: _WindowEnd, nothing cached."""
    if base in _TCP_PROBED:
        return _TCP_PROBED[base]
    import socket
    from urllib.parse import urlparse
    u = urlparse(base)
    host = u.hostname or ""
    port = u.port or (443 if u.scheme == "https" else 80)
    req_end_ts = _RUN_REQ_END[0]
    tok = _take_token(host, "tcp", "tcp", req_end_ts)
    timeout = float(_TCP_PROBE_TIMEOUT)
    if req_end_ts is not None:
        left = req_end_ts - time.time()
        if left <= 0:
            _pacer.outcome(tok, "window_end", 0.0)
            raise _WindowEnd("TCP probe not started: the margin before the run ceiling is reached")
        timeout = max(1.0, min(timeout, left))
    t0 = time.time()
    _pacer.started(tok, clock=time)
    try:
        socket.create_connection((host, port), timeout=timeout).close()
        ok, what = True, "tcp-open"
    except OSError as e:
        ok, what = False, f"tcp-{type(e).__name__}"
        if timeout < _TCP_PROBE_TIMEOUT and isinstance(e, (socket.timeout, TimeoutError)):
            _pacer.outcome(tok, "window_end", time.time() - t0)
            raise _WindowEnd(f"TCP probe cut at the margin before the run ceiling after {timeout:.0f} s") from e
    _pacer.outcome(tok, what, time.time() - t0)
    _TCP_PROBED[base] = ok
    return ok

CSV_ACCEPT = "application/vnd.sdmx.data+csv;version=1.0.0"
XML_ACCEPT = "application/vnd.sdmx.genericdata+xml;version=2.1"

RATE = 1.0            # seconds between flows (polite; the ingester used 1.5)
# Wall-clock budget for ONE istat run. Deliberately under the daily job's per-source window.
#
# READ THIS BEFORE TRUSTING IT ON THE DESKTOP. Deadline honours AQUEDUCT_BUDGET_MIN_OVERRIDE
# by REPLACING this constant (_common.py:119-136), and tools/run_local_heavy.ps1 sets that
# override to 360. So this bounds istat at 30 min in CI and at SIX HOURS on the local runner.
# The override exists to ENLARGE budgets for the dedicated 382 GB machine, so narrowing it
# globally would re-cap every other local source.
#
# SINCE 2026-11-23 THE PASS'S RUN CEILING BOUNDS istat AS WELL (see THE RUN CEILING below). On
# 2026-11-22 the 360-min budget let istat start flows until the local pass's hard stop killed it
# after 188 min with no Result, and the bookmark (then saved only after the loop) did not move.
# The ceiling check does not read this budget or the override, so the override cannot lift it.
BUDGET_MIN = float(os.environ.get("ISTAT_BUDGET_MIN", "30"))

TIMEOUT = 120         # per request, default

# PER-HOST TIMEOUT, BECAUSE THE SURVIVING HOST IS THE SLOW ONE. sdmx.istat.it is
# redirect-looping (see _get), so every request now lands on esploradati - and esploradati
# is slow, not broken. Measured 2026-08-24, three GETs of
# https://esploradati.istat.it/SDMXWS/rest/dataflow/IT1: 76.0s, 102.6s and 123.7s, each
# answering 200 with 13,618,919 bytes and zero redirects. At a flat 120s the third of those
# is a timeout that the caller would record as `transient` - a working host reported as
# failing, which is how a slow source gets misdiagnosed as a dead one. 300s is the
# allowance jobs/ingest_istat_sliced.py already gives this host, and this only raises the
# CEILING: a fast reply still returns as fast as it arrives.
HOST_TIMEOUT = {
    "https://esploradati.istat.it/SDMXWS/rest/": 300,
}
RETRIES = 3           # per host, for transient (timeout/5xx/conn) errors


def _backoff_s(attempt: int) -> float:
    """The sleep before retry pass `attempt` (1-based) in _fetch_flow. One definition, so the
    run-ceiling floor below is computed from the same numbers the retry loop sleeps."""
    return min(4 * attempt, 30)

# THRESHOLD > ONE FLOW'S RETRIES, OR THE RETRY LOOP KILLS A LIVE HOST BY ITSELF. The first
# cut set this to 3 — exactly RETRIES — and _fetch_flow's own backoff loop calls _try_host
# once per attempt against the same base, so ONE slow flow produced 3 increments and marked
# a WORKING host dead (adversarial review 2026-08-28, reproduced: esploradati serving its
# 13.6 MB body in >300s, a 2.4x slow day against the 123.7s measured 2026-08-24, suffices).
# That is exactly the misdiagnosis HOST_TIMEOUT's own comment forbids. Sustained failure
# across SEVERAL flows is the signal; one slow flow is not. Pinned by
# tests/test_istat_dead_host.py, which drives the REAL retry loop rather than the counter.
_HOST_DEAD_AFTER = 3 * RETRIES + 1
TRANSIENT_HTTP = (429, 500, 502, 503, 504)

# THE RUN CEILING (2026-11-23; the census / wid / imf_imts_direct pattern). When the
# orchestrator has set a run ceiling (orchestrate._RUN_DEADLINE_TS, from AQUEDUCT_RUN_BUDGET_MIN,
# which tools/run_local_heavy.ps1 clamps to the time left in the pass), a flow is started only if
#     time left >= margin + max(2 x the longest flow this run, FLOW_FLOOR_S)
# Otherwise the loop stops, and the flows not reached are booked as ONE deferred unit (not
# transient) that names their count, as the budget stop does. So the run returns a Result
# (cursors, CSV step) BEFORE the pass's hard stop kills it.
#   margin        AQUEDUCT_ISTAT_MARGIN_MIN, default 5 min: the tail for finalize and the
#                 orchestrator's CSV step. CHOSEN, not measured (as census and wid).
#   FLOW_FLOOR_S  the allowance before any flow of this run is timed: one flow whose every CSV
#                 GET on ONE live host runs to that host's timeout, each GET after ONE wait for its
#                 shared-pacer slot (jobs/_istat_pacer.GAP_S, 16 s; since 2026-11-25), RETRIES x
#                 (300 + 16) s + the backoff sleeps = 960 s (16.0 min). One live host is today's case
#                 (sdmx.istat.it answers a 3xx and is marked dead on its first GET). It is not the
#                 worst case: with both hosts live and timing out a flow costs 3 x (120 + 16 + 300 +
#                 16) + 12 = 1,368 s, a 200 that parses 0 rows adds an XML GET per attempt (about 32
#                 min on one host), and a 429 penalty or a fail-safe token (64 s) waits longer. The 2 x
#                 longest term covers those once one such flow has been seen. The first flow of a run
#                 also pays the TCP probes and sdmx's 3xx (at most 3 x 16 + 2 x 10 s, once per run).
# The check reads neither BUDGET_MIN nor AQUEDUCT_BUDGET_MIN_OVERRIDE. run_once always sets a
# ceiling (AQUEDUCT_RUN_BUDGET_MIN, default 240 min; CI sets its own), so the check is off only
# when AQUEDUCT_RUN_BUDGET_MIN <= 0 or update() is called outside run_once.
# THE REQUESTS ARE CLAMPED TOO (2026-11-24, C8): every GET's timeout is at most the time left to
# ceiling - margin, no GET or retry backoff starts after it, and a flow cut there is deferred, or
# transient when it met a real failure first (phase B review A1) (_get, _fetch_flow, _WindowEnd).
# So a flow that overruns its allowance (measured on 2026-11-22: one flow ~1,044 s, above the
# 960-s floor) can no longer carry its GETs past ceiling - margin. Since 2026-11-25 the pacer's slot
# is taken BEFORE the clamp reads the time left, and a slot later than ceiling - margin - 1 s is
# refused (the flow is deferred); the 10-s TCP probe takes a token and is clamped the same way. NOT
# clamped: the flow's store reads before its first GET and its merge after the body (bounded by the
# flow's size, not the clock), the 1-s RATE pause, and a body that trickles in (a requests timeout
# bounds each read gap, not the whole body).
MARGIN_FLOOR_MIN_DEFAULT = 5.0
FLOW_FLOOR_S = (RETRIES * (max([TIMEOUT, *HOST_TIMEOUT.values()]) + _pacer.GAP_S)
                + sum(_backoff_s(a) for a in range(1, RETRIES)))
# A progress line every PROGRESS_EVERY_FLOWS attempted flows or PROGRESS_EVERY_S seconds,
# whichever comes first, so a working sweep cannot look like a hang (2026-11-22: 188 min, 451
# flows merged, not one log line).
PROGRESS_EVERY_FLOWS = 50
PROGRESS_EVERY_S = 300.0


def _margin_floor_s() -> float:
    """AQUEDUCT_ISTAT_MARGIN_MIN in seconds. Read at each update so a test or a run can set it. A
    value that is not a finite number >= 0 falls back to the default and says so: a bad value must
    not remove the margin."""
    raw = os.environ.get("AQUEDUCT_ISTAT_MARGIN_MIN")
    if raw is None or raw == "":
        return MARGIN_FLOOR_MIN_DEFAULT * 60
    try:
        v = float(raw)
    except (TypeError, ValueError):
        v = float("nan")
    if not (math.isfinite(v) and v >= 0):
        print(f"[istat] AQUEDUCT_ISTAT_MARGIN_MIN={raw!r} is not a finite number >= 0 - using "
              f"{MARGIN_FLOOR_MIN_DEFAULT:g} min", flush=True)
        return MARGIN_FLOOR_MIN_DEFAULT * 60
    return v * 60


def _window_end_ts():
    """The pass's run ceiling as an epoch timestamp, or None when none is set. Read through
    sys.modules (as census, wid and imf_imts_direct do), so this module never imports the
    orchestrator."""
    orch = sys.modules.get("updater.orchestrate")
    ts = getattr(orch, "_RUN_DEADLINE_TS", None) if orch is not None else None
    if isinstance(ts, bool) or not isinstance(ts, (int, float)) or not math.isfinite(ts):
        return None
    return float(ts)


# THE DECLARATION (registry `window_stop: true`, 2026-11-24; the census / wid pattern; plan review R2100,
# VERDICT_planA). Undeclared, istat on the workstation (no hard per-unit timeout) reserves at the start gate at
# least its LATEST REAL RUN, falling to the normal reserve over 60 days of waiting (orchestrate.admission_reserve,
# RESERVE_DECAY_DAYS). A run that ends at the run ceiling (the check above) is as long as the time it was given,
# so that reserve tracks the longest pass it was given. It is bounded by the 60-day decay, not a permanent
# lock-out, but its floor is the normal reserve (90 min for istat's cost), so with less than 90 min left at its
# position an undeclared istat is never admitted. The measured instances are 2026-11-18 (155 min, from the
# 9,385-s run of 11-17) and 2026-11-23 (187 min, from the 11-22 kill, killed_external 11,306 s, on code from
# BEFORE the run-ceiling check). A declared unit reserves only what it needs to START A FLOW, STOP CLEANLY AND
# HAND OVER, which these functions state from the SAME helper and constant as the check in update(), so the gate
# and the fetcher cannot disagree.
#
# ADMISSION_SETUP_MIN: the time between the start gate and update()'s first check (the lease, the store
# listing and the bookmark read; detect_change is free for sdmx_delta). CHOSEN, not measured: no log line times
# it. Without it a unit admitted at exactly margin + floor could only stop at once ("none", not a turn).
ADMISSION_SETUP_MIN = 1.0


def window_admission_reserve_min() -> float:
    """THE START GATE'S RESERVE for istat, in minutes (registry `window_stop: true`; read by
    orchestrate._window_stop_reserve): window_stop_tail_min() + ADMISSION_SETUP_MIN = 5 + 16.0 + 1 = 22.0 by
    default (16.0 = FLOW_FLOOR_S, one pacer slot wait per GET included).

    The tail (margin + FLOW_FLOOR_S) is what update()'s run-ceiling check needs before the FIRST flow of a run;
    the setup allowance covers the time from the gate to that check, so a unit admitted at this reserve starts a
    flow. Both come from the SAME helper (_margin_floor_s: AQUEDUCT_ISTAT_MARGIN_MIN, default 5, chosen) and the
    SAME constant at call time. The margin is the hand-over: finalize (with ROUTE B), then the orchestrator's CSV
    step for the cursors of the flows walked, whose soft budget is capped by the time left to the ceiling
    (orchestrate._capped_derive_budget), so ids it does not reach go to csv_retry_queue, deferred.

    THE OVERRUN BOUND: every GET and TCP probe is clamped to ceiling - margin (_get, _tcp_reachable), and its
    pacer slot is refused past it. Not clamped: a flow's store reads and merge, the 1-s RATE pause, and a body
    that trickles in. The measured single-flow maximum is about 1,044 s (2026-11-22, unpaced), above
    FLOW_FLOOR_S = 960 s; its GETs can no longer pass ceiling - margin."""
    return window_stop_tail_min() + ADMISSION_SETUP_MIN


def window_stop_tail_min() -> float:
    """WHERE istat STOPS, in minutes before the run ceiling (for tools/measure_ci_lag.py's replay; nothing in
    the updater calls it): margin floor + FLOW_FLOOR_S = 5 + 16.0 = 21.0 by default, through the SAME helper and constant as
    update()'s check. Before any flow is timed no flow starts with less than this left. The flow started just
    before that point ends after it (a few seconds to a few minutes on the measured passes; every GET is clamped
    to ceiling - margin), and the 2 x longest term is NOT in this figure, nor the CSV allowance (CSV_S_PER_FLOW per
    flow handed over, which tools/measure_ci_lag.py's CsvStep models). Not census's or wid's figure (the margin
    alone): their first check needs only the floor; istat's needs margin + FLOW_FLOOR_S."""
    return (_margin_floor_s() + FLOW_FLOOR_S) / 60.0


def window_stop_piece_min() -> float:
    """The window an istat stop needs to count as PROGRESS, in minutes: 0. update() reports window_stop "none"
    only when a window stop came before any flow was started and no cursor is handed over, and the first flow is
    started whenever the first window check passes."""
    return 0.0


# THE PER-TURN CAP (2026-11-24, condition C7 of the plan review; the coordinator's decision). While istat is due
# - always, until it sets last_success again - a declared istat takes every pass's slack after its band-3
# predecessors, and the leftover csv retry drain and the `desktop_lane: last` unit lose it. The cap gives some of
# it back. AQUEDUCT_ISTAT_TURN_MAX_MIN = T: the turn ends as it would end at a run ceiling T minutes after it
# started - the same check, the same request clamp, the same bookmark save, the same window_stop signal, the same
# Result. So a capped turn starts no flow within margin + max(2 x longest, FLOW_FLOOR_S) + the CSV allowance (below)
# of its own end. AQUEDUCT_BUDGET_MIN_OVERRIDE does NOT lift it (the override reaches only Deadline).
# THE CAP BOUNDS THE TURN, NOT ONLY update() (phase B review B1): the Result carries the cap's end
# (Result.turn_end_ts) and run_once bounds the CSV phase after the fetch - the fresh derive and the retry drain -
# by it (orchestrate._UNIT_TURN_END_TS). Before, that phase read only the run ceiling and could add two 45-min
# budgets (measured on 2026-10-24 and 10-25) to a "60-min" turn.
#
# TURN_MAX_MIN_DEFAULT = 60, RE-DECIDED from the C6 simulation (2026-11-24: the C5 run again, with the CSV phase
# modelled; F:/work/econ_finish/istat_windowstop/sim/c6_<order>_<pace>_<density>.json, table c6_table.out): the same
# replay (tools/measure_ci_lag.py UnitBook.run_pass, production order and admission, called; 56 passes with the
# measured November budgets, in date order and reversed; the state copy of 2026-11-17), plus CsvStep - istat's fetch
# flow by flow (15.48 s a flow, measured on 2026-11-22; 24.47 s paced at 4 GETs/min), 5.65 catalogue ids a flow
# (14,267 / 2,526), the derive at 0.4 ids/s (measured), run_once's budget rule. Base density, in BOTH orders and
# both paces: a cap-60 turn, CSV phase included, is at most 39.9 min (main post-#325: up to 229.8) and leaves no
# CSV debt (main: 18,920-39,709 ids queued by the end); the leftover drain gets 2,307-2,439 min against main's
# 2,606-2,927 (79-94 %), against 1,126-1,209 with cap 90, 774-813 with cap 120 and 338-513 with no cap; eia finishes
# 37-39 times (main 30-34). The cost: istat walks 4,455-4,536 flows in 56 nights unpaced (one rotation of 2,526 in
# ~31 nights), 3,410-3,472 paced (~41 nights); cap 90 walks 4,853-6,367. What did NOT decide it: which arm gives eia
# the most finishes and its longest gap (both flip with the order). Dense sensitivity (20.3 ids a flow, every night,
# the 183_* region's density): cap 60 still bounds the turn (60.1 min) and queues the least of the declared arms
# (20,067-34,678 ids against main's 79,404-158,043). Model limits: equal flows and ids, istat never finishes, the
# leftover drain never drains istat's queue, the other units' work terms are their newest real runs.
TURN_MAX_MIN_DEFAULT = 60.0


def _turn_max_s() -> "float | None":
    """The per-turn cap in seconds, or None for no cap. Read at each update so a test or a run can set it.
    AQUEDUCT_ISTAT_TURN_MAX_MIN: unset or empty -> TURN_MAX_MIN_DEFAULT; "0" or "off" -> no cap; a value that is
    not a finite number > 0 -> the default, and it says so. A cap below window_admission_reserve_min() is raised to
    it, and it says so: under it no flow could ever start, so every capped turn would be a "none" non-turn."""
    raw = os.environ.get("AQUEDUCT_ISTAT_TURN_MAX_MIN")
    default = TURN_MAX_MIN_DEFAULT
    if raw is None or raw.strip() == "":
        v = default
    elif raw.strip().lower() in ("0", "off", "none"):
        v = None
    else:
        try:
            v = float(raw)
        except (TypeError, ValueError):
            v = float("nan")
        if not (math.isfinite(v) and v > 0):
            print(f"[istat] AQUEDUCT_ISTAT_TURN_MAX_MIN={raw!r} is not a finite number > 0 - using the default "
                  f"({'no cap' if default is None else f'{default:g} min'})", flush=True)
            v = default
    if v is None:
        return None
    floor = window_admission_reserve_min()
    if v < floor:
        print(f"[istat] per-turn cap {v:g} min is under the {floor:g}-min reserve, so no flow could start - "
              f"raised to {floor:g} min", flush=True)
        v = floor
    return v * 60.0


# THE CSV ALLOWANCE (2026-11-24, phase B review B1). Every flow update() walks is handed to run_once's CSV step
# (its cursor, see the seeding in update()), and that step runs inside the turn: bounded by the run ceiling, and
# since the review also by the per-turn cap (Result.turn_end_ts). A fetch that ran to its stop point left the step
# only the margin, so most of what it handed over was deferred to csv_retry_queue - which the next turn's fetch
# then squeezed again (C6, cap 60 with no allowance: 18,297-18,449 ids queued after 56 nights). So the stop check
# keeps CSV_S_PER_FLOW seconds per flow handed over so far: a flow starts only with at least margin + max(2 x the
# longest flow, FLOW_FLOOR_S) + (cursors so far) x CSV_S_PER_FLOW left, at the run ceiling and at the cap alike.
# CSV_S_PER_FLOW_DEFAULT = 14.1 s, MEASURED: the mean catalogue ids a flow hands over, 14,267 catalogued series
# (docs/runbook/istat.md) / 2,526 flows = 5.65, at the derive's 0.4 ids/s (printed by derive on 2026-11-13, 11-14
# and 11-16; ALREADY CURRENT ids cost the same). The real rotation is lumpy (183_277 alone carried ~4,800 ids on
# 2026-10-25), so a dense stretch still defers ids to the queue, and a sparse one leaves the step time for the
# drain; the mean is what keeps the queue level over a rotation. AQUEDUCT_ISTAT_CSV_S_PER_FLOW: unset or empty ->
# the default; "0" or "off" -> no allowance; a value that is not a finite number >= 0 -> the default, and it says
# so. The first flow of a run needs no allowance (nothing handed over yet), so the start gate's reserve is unchanged.
CSV_S_PER_FLOW_DEFAULT = 14.1


def _csv_s_per_flow() -> float:
    """The CSV allowance in seconds per flow handed over (see CSV_S_PER_FLOW_DEFAULT). Read at each update."""
    raw = os.environ.get("AQUEDUCT_ISTAT_CSV_S_PER_FLOW")
    if raw is None or raw.strip() == "":
        return CSV_S_PER_FLOW_DEFAULT
    if raw.strip().lower() in ("0", "off", "none"):
        return 0.0
    try:
        v = float(raw)
    except (TypeError, ValueError):
        v = float("nan")
    if not (math.isfinite(v) and v >= 0):
        print(f"[istat] AQUEDUCT_ISTAT_CSV_S_PER_FLOW={raw!r} is not a finite number >= 0 - using the default "
              f"({CSV_S_PER_FLOW_DEFAULT:g} s)", flush=True)
        return CSV_S_PER_FLOW_DEFAULT
    return v

# Files in the istat dir that are NOT dataflow parquets.
_SKIP_FILES = {"_sliced_done.json", "_sliced_unrecoverable.json"}

# Per-call structure cache so we don't re-fetch the same FULL body twice when probing
# a flow for the structural signal.
_NONTRIVIAL_BODY = 256  # a 200 body smaller than this is not a "real" structured body


# --------------------------------------------------------------------------- #
# HTTP — one request, classified for the date-tail caller
# --------------------------------------------------------------------------- #
class _WindowEnd(Exception):
    """A request (or a retry's backoff) would run past the margin before the run ceiling, so it is not started,
    or it was cut by its clamped timeout. Raised out of _get / _fetch_flow to update(), which stops the run, exactly
    as the run-ceiling check does (census's _WindowStop).

    `failed_attempts` (set by _fetch_flow): how many of THIS flow's host attempts had already failed for real (a
    transient answer: a timeout at the host's own timeout, a 5xx/429, a dropped or refused connection; NOT a
    redirect loop, see _failed_for_real) before the window ended. update() books a flow with none DEFERRED (nothing
    went wrong, the window ended) and a flow with at least one TRANSIENT (phase B review A1: two hosts that hang
    gave four genuine timeouts and the clamp cut the fifth GET; booked deferred, the run read as a clean deferral
    and health's stuck alarm read success)."""

    failed_attempts = 0


class _Resp:
    """Outcome of one GET: kind in {ok, empty, transient, http<code>}."""
    __slots__ = ("content", "kind", "status")

    def __init__(self, content=None, kind="transient", status=None):
        self.content = content
        self.kind = kind
        self.status = status


def _get(sess, url, accept, timeout=TIMEOUT, req_end_ts=None, flow=None) -> _Resp:
    """ONE GET (no internal backoff — retry/host-fallback is orchestrated by the
    caller so a dead host doesn't burn the whole budget before the other is tried).
    Returns a classified _Resp:
      ok         -> 200 with a body
      empty      -> 404 "NoRecordsFound" (nothing newer in the window) / 400 / 413 /
                    other hard 4xx (this host doesn't serve the flow this way)
      gone       -> 404 "Could not find Dataflow and/or DSD ..." — the flow has been
                    WITHDRAWN from this host's catalog (a structural-break candidate;
                    the caller still tries the OTHER host before concluding it)
      transient  -> timeout / 5xx / 429 / conn drop (retryable on this host), or a 3xx
                    that is not followed (the host is then unusable for the run)

    THE REQUEST IS CLAMPED TO THE WINDOW (2026-11-24, review of the istat window_stop plan, C8; census's
    AR-407 O1). `req_end_ts` is the run ceiling minus the margin (update() passes it; None = no ceiling).
    The timeout is at most the time left to it (never under 1 s), and with no time left the GET is not
    started. Both raise _WindowEnd. A timeout that the CLAMP caused is not a transport failure, so it does not
    count towards marking the host dead. A requests timeout bounds the connect and each read GAP, not the
    whole body: a body that trickles in can still end after req_end_ts (stated residual, as in census).

    PACED (2026-11-25, DESIGN 7). A token of the shared ISTAT budget comes FIRST, with req_end_ts as its deadline
    (a slot later than req_end_ts - 1 s is _WindowEnd before anything is sent: deferred, R303), then the clamp
    with the real time left, then the GET with allow_redirects=False, so every request sent is one token and a
    redirect hop is never sent unpaced (requests follows 30 by default: about 31 requests per sdmx.istat.it GET
    on 2026-11-22). A 3xx is classified by _pacer.redirect_decision: a same-host redirect to a DIFFERENT path is
    followed ONCE, with its own token (V5(a)); a self-redirect, a cross-host redirect, a 3xx without a Location
    or a second redirect marks the base unusable for the run, logs the status and the Location, and answers
    `transient` (its token stays spent), as TooManyRedirects did. A 429 shares its Retry-After (60 s to 48 h) as
    a penalty for every participant (_pacer.penalize) and stays `transient`. Every request's outcome is logged
    (_pacer.outcome). The pacer's wait is never counted by _note_transport_failure (C7): only a failure of the
    request itself is."""
    hdrs = {**UA, "Accept": accept}
    if timeout == TIMEOUT:                       # caller took the default -> host may raise it
        for _base, _t in HOST_TIMEOUT.items():
            if url.startswith(_base):
                timeout = _t
                break
    host_timeout = timeout
    base = _base_of(url)                         # a followed redirect stays on this host (V5(a))
    followed = 0
    while True:
        tok = _take_token(url, "http", "data", req_end_ts, flow=flow)     # the slot BEFORE the clamp (C8a)
        timeout = host_timeout
        if req_end_ts is not None:
            left = req_end_ts - time.time()
            if left <= 0:
                _pacer.outcome(tok, "window_end", 0.0)
                raise _WindowEnd("request not started: the margin before the run ceiling is reached")
            timeout = max(1.0, min(float(host_timeout), left))
        t0 = time.time()
        _pacer.started(tok, clock=time)
        try:
            r = sess.get(url, headers=hdrs, timeout=timeout, allow_redirects=False)
        except requests.TooManyRedirects:
            # Unreachable with allow_redirects=False; kept for a session that follows anyway. A HOST-level
            # fault, not a flow-level one: sdmx.istat.it began answering every /SDMXWS/rest/ path with a 302
            # back to its own homepage - measured 2026-08-23, 12 hops and still looping, while
            # esploradati.istat.it returns 200 with no redirect at all. TooManyRedirects was not in the except
            # clause below, so it escaped as UNEXPECTED and killed the whole source BEFORE the working host was
            # tried. istat last succeeded 2026-07-14 and had been attempted on every run for 40 days, recorded
            # transient_fail each time.
            _pacer.outcome(tok, "TooManyRedirects", time.time() - t0)
            if base:
                _mark_dead(base, "is redirect-looping")
            return _Resp(kind="transient")
        except (requests.Timeout, requests.ConnectionError) as e:
            _pacer.outcome(tok, "timeout" if isinstance(e, requests.Timeout) else "conn", time.time() - t0)
            if isinstance(e, requests.Timeout) and timeout < host_timeout:
                # The CLAMP cut it, not the host: the window ended. Not counted against the base.
                raise _WindowEnd(f"request cut at the margin before the run ceiling after {timeout:.0f} s "
                                 f"({type(e).__name__})") from e
            # Transport, not protocol: count it against the base (see _TRANSPORT_FAILS).
            _note_transport_failure(base)
            return _Resp(kind="transient")
        except BaseException as e:
            _pacer.outcome(tok, type(e).__name__, time.time() - t0)
            raise
        el = time.time() - t0
        status = r.status_code
        _note_success(base)                      # a reply of ANY status proves the transport
        if isinstance(status, int) and 300 <= status <= 399:
            loc = (getattr(r, "headers", None) or {}).get("Location")
            action, detail = _pacer.redirect_decision(url, status, loc, followed)
            _pacer.outcome(tok, f"{status}->{loc or '-'}:{action}", el)
            if action == "follow":
                print(f"[istat] {url} answers {status} -> {loc}: a same-host redirect to another path, followed "
                      f"once with its own token", flush=True)
                followed, url = 1, detail
                continue
            if base:
                _mark_dead(base, f"answers {status} -> {loc or '(no Location)'} ({detail}; not followed)")
            return _Resp(kind="transient", status=status)
        _pacer.outcome(tok, str(status), el)
        break
    if status == 200:
        return _Resp(content=r.content, kind="ok", status=200)
    if status == 429:
        ra = (getattr(r, "headers", None) or {}).get("Retry-After")
        until = _pacer.penalize(_pacer.retry_after_seconds(ra), "updater", clock=time)
        print(f"[istat] {url} answers 429 (Retry-After {ra!r}): every ISTAT participant waits until "
              f"{dt.datetime.fromtimestamp(until, dt.timezone.utc):%Y-%m-%dT%H:%M:%S}Z", flush=True)
        return _Resp(kind="transient", status=429)
    if status in TRANSIENT_HTTP:
        return _Resp(kind="transient", status=status)
    if status == 404:
        body = (r.content or b"").lstrip().lower()
        # "NoRecordsFound" = valid flow, just nothing in this date window -> empty.
        # "Could not find Dataflow and/or DSD ..." = flow withdrawn from catalog -> gone.
        if b"could not find" in body or b"dataflow" in body:
            return _Resp(kind="gone", status=404)
        return _Resp(kind="empty", status=404)
    # 400/413 = no such slice; any other hard 4xx = host doesn't serve this. -> empty.
    return _Resp(kind="empty", status=status)


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


def _try_host(sess, base, flow_id, start_period, req_end_ts=None):
    """One host attempt. Returns (keys, dates, vals, kind) with kind in
    {ok, empty, gone, structural, transient}. On a 200-but-0-rows CSV, tries XML once;
    a 200 with a NON-TRIVIAL body that still parses 0 rows is 'structural' (schema
    break), while a tiny/empty 200 or a 404 NoRecordsFound is 'empty'.

    Both GETs are clamped to `req_end_ts` (see _get). THE XML PATH: when the CSV body parses 0 rows and no time
    is left for the XML GET, _WindowEnd propagates and the flow is cut (_WindowEnd) - it is never booked structural on a
    CSV body alone, and a later pass starts at it (the bookmark does not name it)."""
    url = _data_url(base, flow_id, start_period)
    resp = _get(sess, url, CSV_ACCEPT, req_end_ts=req_end_ts, flow=flow_id)
    if resp.kind in ("transient", "empty", "gone"):
        return [], [], [], resp.kind
    # ok
    k, d, v = _parse(resp.content)
    big = bool(resp.content) and len(resp.content) >= _NONTRIVIAL_BODY
    if not v:
        # 200 but no parsed rows from CSV — try XML in case the host served an odd body.
        resp2 = _get(sess, url, XML_ACCEPT, req_end_ts=req_end_ts, flow=flow_id)
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


def _fetch_flow(sess, flow_id, start_period, had_prior: bool, req_end_ts=None):
    """Fetch a flow's date-tail across hosts. Returns (keys, dates, vals, outcome)
    where outcome in {ok, empty, transient, structural}. Raises _WindowEnd when a GET or a retry's
    backoff would cross `req_end_ts` (the run ceiling minus the margin; None = no ceiling).

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
    fails = [0]                   # this flow's genuine failed host attempts, for _WindowEnd.failed_attempts
    try:
        return _fetch_flow_counted(sess, flow_id, start_period, had_prior, req_end_ts, fails)
    except _WindowEnd as e:
        e.failed_attempts = fails[0]
        raise


def _failed_for_real(base) -> bool:
    """After a `transient` answer from `base`: was it a real failure of the host (a timeout at its own timeout, a
    5xx/429, a dropped or refused connection), as opposed to a redirect loop? A redirect loop marks the base dead
    at once (_get) with its transport streak under _HOST_DEAD_AFTER - sdmx.istat.it's shape since 2026-08-23, so
    the first GET there loops on every run - and it says nothing about this flow. A base the streak itself killed
    did fail for real."""
    return not (base in _DEAD_HOSTS and _TRANSPORT_FAILS.get(base, 0) < _HOST_DEAD_AFTER)


def _fetch_flow_counted(sess, flow_id, start_period, had_prior, req_end_ts, fails):
    """_fetch_flow's body. `fails` is a one-item list: each host attempt that answers `transient` and failed for
    real (_failed_for_real) adds 1, so the caller can say how many attempts had failed when the window cut the
    flow (see _WindowEnd)."""
    transient_bases = []
    saw_structural = False
    saw_gone = False
    saw_empty = False
    for _label, base in HOSTS:
        if base in _DEAD_HOSTS:
            continue          # dead this run; do not pay its timeout per flow
        # Cheapest possible discriminator, once per base per run: a base whose SOCKET
        # will not open cannot serve anything, and a healthy-but-slow host still
        # connects fast (HOST_TIMEOUT's 76-124s were full-body reads). Without this the
        # first flow alone pays RETRIES x HOST_TIMEOUT (~900s on esploradati).
        if not _tcp_reachable(base):
            _mark_dead(base, f"is unreachable (TCP connect failed in {_TCP_PROBE_TIMEOUT}s)")
            continue
        k, d, v, kind = _try_host(sess, base, flow_id, start_period, req_end_ts=req_end_ts)
        if kind == "ok":
            return k, d, v, "ok"
        if kind == "transient":
            transient_bases.append(base)
            fails[0] += _failed_for_real(base)
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
            # DEAD MEANS DEAD EVERYWHERE: a base marked dead during the first pass
            # must not be retried here (reviewer NOTE 3, AR-017).
            transient_bases = [b for b in transient_bases if b not in _DEAD_HOSTS]
            if not transient_bases:
                break
            if req_end_ts is not None and req_end_ts - time.time() <= _backoff_s(attempt):
                # no backoff (and so no retry) is started that would reach the margin before the ceiling
                raise _WindowEnd(f"retry abandoned: the {_backoff_s(attempt):g}-s backoff would reach the margin "
                                 f"before the run ceiling")
            time.sleep(_backoff_s(attempt))
            still = []
            for base in transient_bases:
                k, d, v, kind = _try_host(sess, base, flow_id, start_period, req_end_ts=req_end_ts)
                if kind == "ok":
                    return k, d, v, "ok"
                if kind == "transient":
                    still.append(base)
                    fails[0] += _failed_for_real(base)
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
    # blob-routed: the flow set must be visible under AQUEDUCT_BACKEND=r2.
    return [f for f in blob.list_parquets(out_dir) if f not in _SKIP_FILES]


def _max_obs(path):
    """Max obs_date on disk for a flow, or None."""
    try:
        od = blob.read_table(path, columns=["obs_date"]).column("obs_date")
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
_TURN_BEGUN = [False]


def update(unit, since) -> Result:
    """The contract entry point: one run of _update() inside the pacer's per-run frame (DESIGN 7). It prints the
    pacer's state line once, and ends the updater's turn marker in a `finally`, so a flow that raises (a kill
    included) cannot leave the crawler yielding to a turn nobody runs; the marker also expires by itself at the
    turn's end (C10). The run's request end, read by the TCP probe, is cleared with it."""
    _RUN_PACE.update(tokens=0, waited_s=0.0, modes={})
    _RUN_REQ_END[0] = None
    _TURN_BEGUN[0] = False
    print(_pacer.describe(), flush=True)
    try:
        return _update(unit, since)
    finally:
        if _TURN_BEGUN[0]:
            _TURN_BEGUN[0] = False
            _pacer.end_turn(clock=time)
        _RUN_REQ_END[0] = None
        print(f"[istat] this run's {_pace_text()}", flush=True)


def _update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)

    files = _flow_files(out_dir)
    sess = requests.Session()
    tally = Tally()
    cursors: dict[str, str] = {}   # flow_id -> max obs_date written/known
    total = 0
    last_obs = None
    flows_done = 0                 # flows actually ATTEMPTED against the publisher
    # These caches describe THIS run's hosts. Reset them here so the property is true by
    # construction rather than by istat happening to have one unit per process (R276-class).
    _DEAD_HOSTS.clear()
    _TRANSPORT_FAILS.clear()
    _TCP_PROBED.clear()

    # Optional bounded subset for a tractable one-shot test (env-only). The PRODUCTION
    # orchestrator never sets this, so the full ~755-flow sweep runs in production.
    limit_env = os.environ.get("ISTAT_MAX_FLOWS")
    if limit_env:
        try:
            files = files[: int(limit_env)]
        except ValueError:
            pass

    # BUDGET + ROTATION (AR-017 SHOULD-FIX 3, the gap this file's own comment below names:
    # "istat still has no Deadline of its own ... the day ISTAT recovers, ~2,442 flows x
    # >=1.0s RATE (plus two R2 reads each) will overrun the pass again").
    #
    # Measured 2026-09-01, which is why it is being closed now: a local heavy pass sat on
    # istat for 33+ minutes at 5.3 CPU-SECONDS — blocked on a socket, no per-flow logging,
    # nothing to stop it — while 16 due sources waited behind it. The orchestrator's hard
    # per-unit timeout does not exist on Windows ("no signal.setitimer"), so on the local
    # runner this source was unbounded.
    #
    # THE BUDGET ALONE WOULD BE R190. Deadline's own docstring records it: a budget over a
    # FIXED order re-walks the same prefix every run and the tail never drains. _flow_files
    # is stable and sorted, so the budget is paired with the shared rotation bookmark —
    # each run resumes after the last flow it attempted.
    bookmark = load_rotation(out_dir)
    files = rotate_after(files, bookmark)
    dl = Deadline(minutes=BUDGET_MIN)
    last_attempted = None

    # THE RUN CEILING (see the constants block). Read HERE, at update time, never at import.
    end_ts = _window_end_ts()
    floor_s = _margin_floor_s()
    # THE PER-TURN CAP (see TURN_MAX_MIN_DEFAULT): when it ends the turn before the run ceiling does, it IS this
    # turn's ceiling - the same check, clamp, bookmark, signal and Result below.
    stop_name = "run ceiling"
    turn_end_ts = None             # the cap's end when the cap ends this turn: Result.turn_end_ts (run_once bounds
    turn_max_s = _turn_max_s()     # the CSV phase by it too)
    if turn_max_s is not None:
        turn_end = time.time() + turn_max_s
        if end_ts is None or turn_end < end_ts:
            end_ts, stop_name, turn_end_ts = turn_end, "per-turn cap", turn_end
            print(f"[istat] per-turn cap {turn_max_s / 60:g} min (AQUEDUCT_ISTAT_TURN_MAX_MIN; "
                  f"AQUEDUCT_BUDGET_MIN_OVERRIDE does not lift it): this turn, its CSV step included, ends as at a "
                  f"run ceiling then", flush=True)
    csv_s = _csv_s_per_flow()      # THE CSV ALLOWANCE, per flow handed over (see CSV_S_PER_FLOW_DEFAULT)
    # Every GET ends by the ceiling minus the margin (_get's clamp), so the margin is left for the hand-over.
    req_end_ts = None if end_ts is None else end_ts - floor_s
    if end_ts is None:
        print(f"[istat] no run ceiling set - the window-end check is off "
              f"(budget {dl.budget_min:g} min only)", flush=True)
    else:
        print(f"[istat] {stop_name} "
              f"{dt.datetime.fromtimestamp(end_ts, dt.timezone.utc):%Y-%m-%dT%H:%M:%S}Z "
              f"({(end_ts - time.time()) / 60:.1f} min left); a flow starts only with at least "
              f"{floor_s / 60:g} min + max(2 x the longest flow, {FLOW_FLOOR_S / 60:.1f} min) + {csv_s:g} s per "
              f"flow handed over (the CSV step's allowance) left", flush=True)
    # THE PACER (DESIGN 7): the TCP probe's deadline is this run's request end, and the turn marker goes up
    # before the first token, so the crawler starts no new flow while this turn runs (update() takes it down).
    _RUN_REQ_END[0] = req_end_ts
    _pacer.begin_turn(end_ts, clock=time)
    _TURN_BEGUN[0] = True
    longest = 0.0                  # the longest flow this run, start of its checks to its bookmark
    n_merged = 0                   # flows whose rows went through merge_and_write this run
    stopped_at_ceiling = False     # the run-ceiling check ended the loop (Result.window_stop below)
    t_run = time.monotonic()
    t_progress = t_run

    n_sub = len(files)
    for fn in files:
        if dl.spent():
            print(f"[istat] budget of {BUDGET_MIN:g} min spent after {dl.elapsed_min():.1f} "
                  f"min — {flows_done} flow(s) attempted, {n_sub - flows_done} left for a "
                  f"later pass, which RESUMES after {last_attempted or bookmark!r} rather than "
                  f"restarting (deferred, not failed; merged rows keep their derive)", flush=True)
            # deferred_unit, NOT transient_unit (R303, and tests/test_tally_deferred_is_not_
            # _a_failure.py enforces it — my first cut used transient and turned the suite red).
            # "Transient" says something went wrong and retrying may help; "deferred" says
            # nothing went wrong and rotation takes it in a later pass. Both still return `partial`,
            # so the vintage does not stamp full coverage either way; only one of them tells
            # the operator to investigate.
            tally.deferred_unit(f"budget spent after {flows_done} flow(s)")
            break
        if end_ts is not None:
            left_s = end_ts - time.time()
            csv_need_s = csv_s * len(cursors)          # the CSV step of the flows handed over so far
            need_s = floor_s + max(2.0 * longest, FLOW_FLOOR_S) + csv_need_s
            if left_s < need_s:
                n_left = n_sub - flows_done
                # The ceiling check runs BEFORE the all-hosts-dead check, so a host that died in the last
                # flow is reported here as a ceiling stop: name the dead hosts so the outage stays readable.
                dead = (f"; hosts dead this run: {', '.join(sorted(_DEAD_HOSTS))}" if _DEAD_HOSTS else "")
                print(f"[istat] {stop_name}: {left_s / 60:.1f} min left, under the "
                      f"{need_s / 60:.1f}-min need ({floor_s / 60:g} min margin + max(2 x the "
                      f"longest flow {longest:.0f} s, {FLOW_FLOOR_S / 60:.1f} min) + {csv_need_s / 60:.1f} min "
                      f"for the CSV step of {len(cursors)} flow(s)) - "
                      f"{flows_done} flow(s) attempted, {n_merged} merged, {n_left} deferred to "
                      f"a later pass, which RESUMES after {last_attempted or bookmark!r} "
                      f"(deferred, not failed){dead}", flush=True)
                # deferred_unit, as the budget stop above: nothing failed (R303).
                tally.deferred_unit(f"{stop_name}: {n_left} flow(s) not started")
                stopped_at_ceiling = True
                break
        f0 = time.monotonic()
        path = os.path.join(out_dir, fn)
        flow_id = fn[: -len(".parquet")]
        before = blob.row_count(path)
        max_d = _max_obs(path)
        if max_d is not None:
            # EVERY WALKED FLOW IS HANDED OVER, NOT ONLY THE ONES WHOSE MERGE CHANGED THE STORE (checked for the
            # phase B review, A2; a merge-measured `changed_keys` would derive only those). A flow can merge nothing
            # this run and still hold a CSV the step must derive: it merged in an earlier run that handed nothing
            # over - a run KILLED by the pass's hard stop returns no Result (2026-11-22: 451 flows merged), a run
            # with a structural flow raised before ROUTE B (9 of 40 runs since 2026-09-01), and a csv-fence trip
            # queues nothing for istat (orchestrate._book_fence_trip is desktop_owed only). Re-walking it is what
            # derives it; a changed-only hand-over would leave those CSVs stale until ISTAT revises the flow again.
            # Ids the derive budget does not reach do stay in csv_retry_queue and drain from there (run_once).
            # THE COST: the step re-derives unchanged flows. 2026-10-24: 465 flows walked, all transient (nothing
            # merged), two 45-min derive budgets (7,341 then 2,952 ids deferred), turn 13,751 s. 2026-10-25: one
            # flow walked, 896 ids handled in 45 min, all 896 ALREADY CURRENT, then a 45-min retry drain; turn
            # 5,594 s. derive's ALREADY CURRENT check skips the upload, not the work (~0.4 ids/s either way).
            cursors[flow_id] = max_d.isoformat()   # seed cursor from on-disk frontier
            if last_obs is None or max_d.isoformat() > last_obs:
                last_obs = max_d.isoformat()

        # WHOLE-PUBLISHER OUTAGE: once every base is dead there is nothing left to ask,
        # and walking the remaining flows would still pay RATE (1.0s x 2,483 = ~41 min)
        # to learn the same fact 2,483 times. Stop and say so. The run books `partial`
        # (see the break below), so the vintage does not advance and the rest of the
        # nightly workstation budget goes to the other 28 local sources.
        #
        # THIS DOES NOT CLOSE THE STARVATION CLASS — it only stops istat paying for a
        # DOWN publisher.
        #
        # CORRECTED 2026-09-17, because half of what this comment used to say is no longer
        # true and the other half has gone from a prediction to a measurement.
        #
        # (a) "istat still has no Deadline of its own" — FALSE since the BUDGET+ROTATION block
        #     above (`dl = Deadline(minutes=BUDGET_MIN)`), which closed AR-017 SHOULD-FIX 3.
        #     The comment outlived its own fix by sitting 30 lines below it.
        # (b) "sits in the 120s fast lane on a cost estimate built from its own aborted runs"
        #     — this WAS true and is now observed rather than predicted. `run_cost_estimate`
        #     is MAX over the last 5 runs (state.py:322-328) and `order_units` puts a source
        #     in band 0 below FAST_LANE_SECONDS=120 (orchestrate.py:53,78-85). Replaying the
        #     estimator over istat's 19 recorded runs: it sat in band 0 for the TEN runs from
        #     2026-07-14 to 2026-09-04, every one of them a 15-42 s outage or host-dead abort,
        #     and left only when the 1,081 s sweep of 2026-09-06 entered the sample. It reads
        #     band 3 today (est 10,655 s). Controls: bls and ecb never re-enter band 0 after
        #     their first run.
        #
        # So the loop is real and self-closing in the wrong direction: abort fast -> look
        # cheap -> be guaranteed a nightly turn -> and the day the publisher recovers, the
        # first real sweep lands in a slot budgeted for a two-minute source. Five consecutive
        # aborts are enough to re-enter it, and this source has had six in a row (2026-08-30
        # to 2026-09-04). The Deadline bounds the damage; it does not fix the misclassification,
        # because the estimate is built from durations and an abort is genuinely short.
        if all(b in _DEAD_HOSTS for _l, b in HOSTS):
            # BREAK, NEVER RAISE. A TransientError here would book `transient_fail`, and
            # _should_derive_csvs admits {ok, partial}, and transient_fail only for a RESULT that
            # carries a non-empty changed set (_giant's hand-over) - a raised error carries none,
            # and istat reports no changed set - so a host dying at flow 801
            # would strand 800 flows' already-merged parquet rows with no CSV re-derive
            # and no series_cursors, the exact regression R380 closed (review 2026-08-28).
            # Breaking with this flow tallied transient makes finalize() return `partial`:
            # coverage incomplete, vintage NOT advanced, merged rows published.
            # Side effect worth knowing: obs_count then carries the PARTIAL sum of the
            # flows walked, not the whole store — state.py already documents obs_count as
            # not comparable across runs, and no gate reads it, so a shrinking number in
            # istat's runbook is this stop, not data loss.
            tally.transient_unit(f"{flow_id}: every ISTAT host unusable this run")
            print(f"[istat] every host unusable ({', '.join(sorted(_DEAD_HOSTS))}) — "
                  f"stopping after {flows_done} flow(s) attempted; merged rows keep their "
                  f"derive, vintage not advanced", flush=True)
            break

        sp = _start_period(max_d)
        window_cut = None              # a _WindowEnd that cut this flow AFTER a real failure (see below)
        try:
            keys, dates, vals, outcome = _fetch_flow(sess, flow_id, sp, had_prior=before > 0,
                                                     req_end_ts=req_end_ts)
        except _WindowEnd as e:
            # THE FLOW IS CUT AT THE WINDOW END (the request clamp, C8). It was started, so it counts as
            # attempted. Nothing of it merged. TWO CASES, by what the flow met before the cut:
            #  - no failed host attempt (its first GET was cut, or a slow 200 left no time for the XML GET): it is
            #    DEFERRED, not failed (R303), and NOT named in the bookmark - like a flow cut by a kill, the next
            #    pass starts AT it (re-run, nothing skipped);
            #  - at least one REAL failed attempt (phase B review A1; _WindowEnd.failed_attempts): it is TRANSIENT,
            #    exactly as a flow whose hosts all failed, and it falls through to the bookmark below like that flow
            #    (named, R784 (a), unless every host is dead: CUT BY HOST DEATH); the run then stops (after the
            #    bookmark). Booked deferred, two hosts that hang (four real timeouts, then the clamp cut the fifth
            #    GET) gave "0 sub-unit(s) attempted, none failed", which health's stuck alarm reads as a success,
            #    and pinned the bookmark at that flow turn after turn.
            if not getattr(e, "failed_attempts", 0):
                flows_done += 1
                n_left = n_sub - flows_done + 1
                print(f"[istat] {stop_name}: {flow_id} cut at the margin before the run ceiling ({e}) - "
                      f"{flows_done} flow(s) attempted, {n_merged} merged, {n_left} deferred to a later pass, "
                      f"which RESUMES at {fn!r} (deferred, not failed)", flush=True)
                tally.deferred_unit(f"{stop_name}: {flow_id} cut at the window end ({e}); "
                                    f"{n_left} flow(s) not finished")
                total += before
                stopped_at_ceiling = True      # a window stop with this flow started: "progress" below
                break
            window_cut = e
            keys, dates, vals, outcome = [], [], [], "transient"
        flows_done += 1
        if window_cut is None:
            time.sleep(RATE)

        # ONE EXIT PER FLOW: every outcome falls through to the bookmark below. The one `continue`
        # there is CUT BY HOST DEATH.
        if outcome == "transient":
            # Every host failed this flow -> keep existing data, re-queue (run partial).
            if window_cut is not None:
                tally.transient_unit(f"{flow_id}: {window_cut.failed_attempts} host attempt(s) failed, then the "
                                     f"window ended at the {stop_name} ({window_cut})")
            else:
                tally.transient_unit(f"{flow_id}: every host failed")
            total += before
        elif outcome == "structural":
            # A 200 real body now parsing 0 rows, or the flow withdrawn from BOTH
            # catalogs while it previously held data -> schema/structural break.
            # finalize() raises DefinitiveError; existing data is kept by merge.
            tally.structural_unit(
                f"{flow_id}: real body parsed 0 rows, or withdrawn from both catalogs, "
                f"over {before:,} stored")
            total += before
        elif outcome == "empty":
            # Valid flow, nothing newer in the window (the common quiet-tail case).
            tally.empty_unit()
            total += before
        else:
            # outcome == ok: we have newer rows. Defensive length align (mirrors ingester).
            m = min(len(keys), len(dates), len(vals))
            keys, dates, vals = keys[:m], dates[:m], vals[:m]
            if m == 0:
                tally.empty_unit()
                total += before
            else:
                new_tbl = pa.table({
                    "series_key": pa.array(keys, pa.string()),
                    "obs_date":   pa.array(dates, pa.date32()),
                    "value":      pa.array(vals, pa.float64()),
                })
                try:
                    n, md = merge.merge_and_write(path, new_tbl, mode="merge", dedup_keys=DEDUP)
                except Exception:
                    # merge guard tripped (would shrink / drop a column): keep old data, surface
                    # as transient so a later pass re-runs the flow with a fresh pull (the run is
                    # `partial`; base.Strategy.is_due decides when that pass comes).
                    tally.transient_unit(f"{flow_id}: merge guard tripped over {before:,} stored")
                    total += before
                else:
                    total += n
                    n_merged += 1
                    # A flow that served a 200 with real rows is a SUCCESSFUL sub-unit (data
                    # flowed), even when every row is at/below the stored boundary and the merge
                    # nets zero new rows (the boundary-YEAR re-sweep makes that the normal
                    # idempotent steady state). Count it added_unit(>0) so it does NOT feed the
                    # all-empty structural floor — otherwise a perfectly healthy re-run where every
                    # active flow re-returns its boundary year (zero net-new) would have
                    # empty==attempted and falsely raise. The REAL net-new delta is carried by
                    # `obs` (total) and merge's row count.
                    tally.added_unit(m)
                    if md:
                        cursors[flow_id] = md
                        if last_obs is None or md > last_obs:
                            last_obs = md

        # THE BOOKMARK, AFTER EVERY ATTEMPTED FLOW, WHATEVER ITS OUTCOME (R273, R784). Until
        # 2026-11-23 it was saved once, after the loop, and moved only on `ok`: a run killed by the
        # pass's hard stop (2026-11-22, 188 min, 451 flows merged) kept no position and the next
        # run walked the same flows again; and a flow that fails on every run would pin the
        # bookmark before it. It names the last flow whose attempt FINISHED. A flow cut by a kill
        # in mid-attempt is not named, so the next run starts at that flow (it re-runs; nothing is
        # skipped). A flow never started (budget, run ceiling, all hosts dead) is never named, and
        # neither is the flow IN WHICH the last usable host died (see CUT BY HOST DEATH below).
        # WHAT IT ASSERTS (R287 asks this): "every flow up to here was tried and its rows are in
        # the store", not "its rows reached the CSV step". It is read only by load_rotation above.
        # The CSV step derives from the Result's series_cursors, which this loop seeds from the
        # store for every flow it walks. THE COST, stated: a run that is KILLED returns no Result,
        # so the flows it merged are handed to the CSV step only when a later run walks them
        # again, one rotation later, not on the next run. Before this change the next run did walk
        # them again, but only because the bookmark had not moved - the same re-walk that kept
        # the tail of the list from ever being reached (R190). A run that stops at the ceiling or
        # the budget returns a Result and hands over every flow it walked - and since 2026-11-24 so
        # does a run with a structural flow (ROUTE B at the end of update()). A list of flows merged
        # but not handed over (wid's "owed" marks) would close the kill case; it is not built.
        #
        # CUT BY HOST DEATH (review of PR #325, F1). A flow that ends `transient` with EVERY host
        # in _DEAD_HOSTS was not answered by any host: the last usable host died during it. The
        # 2026-11-19/20 shape is one such flow - sdmx.istat.it redirect-loops on its one GET,
        # esploradati fails its TCP probe, and the run stops "after 1 flow(s) attempted". Naming
        # that flow would pass it on a night when no working host was asked about it, and it would
        # wait a whole rotation. So it is treated like a kill: it is not named, and the next run
        # starts at it. The next pass of the loop meets the all-hosts-dead (or a budget / ceiling)
        # stop and ends the run. A flow that failed while some host was still usable is named
        # (R784 (a): advance on attempt).
        if outcome == "transient" and all(b in _DEAD_HOSTS for _l, b in HOSTS):
            continue
        last_attempted = fn
        save_rotation(out_dir, last_attempted)
        now_m = time.monotonic()
        longest = max(longest, now_m - f0)
        if window_cut is not None:
            # A1: the window ended inside this flow after a real failure. Stop now, as the run-ceiling check
            # would before the next flow (less than the margin is left); the flows after it are deferred, not
            # failed. (With every host dead the `continue` above reaches that check instead, which says so.)
            n_left = n_sub - flows_done
            if n_left:
                tally.deferred_unit(f"{stop_name}: {n_left} flow(s) not started")
            print(f"[istat] {stop_name}: {flow_id} cut at the margin before the run ceiling ({window_cut}) after "
                  f"{window_cut.failed_attempts} failed host attempt(s) - transient, not deferred; {flows_done} "
                  f"flow(s) attempted, {n_merged} merged, {n_left} deferred to a later pass, which RESUMES after "
                  f"{fn!r}", flush=True)
            stopped_at_ceiling = True          # a window stop with this flow started: "progress" below
            break
        if flows_done % PROGRESS_EVERY_FLOWS == 0 or now_m - t_progress >= PROGRESS_EVERY_S:
            t_progress = now_m
            ceiling = ("no run ceiling" if end_ts is None
                       else f"{(end_ts - time.time()) / 60:.1f} min left to the run ceiling")
            # Flows are deferred only at a stop (budget or run ceiling), and that stop's line gives
            # the deferred count; here the same flows are still "left" (not reached yet).
            print(f"[istat] progress: {flows_done}/{n_sub} flow(s) attempted, {n_merged} merged, "
                  f"{tally.empty} empty, {tally.transient} transient, {tally.structural} "
                  f"structural, {n_sub - flows_done} left; {(now_m - t_run) / 60:.1f} min elapsed, "
                  f"{ceiling}; bookmark {fn}; {_pace_text()}", flush=True)

    # empty_window_floor = n_sub - 1: a steady-state run where ALL flows are quiet (each
    # ISTAT flow updates only a few times a year) is legitimate no_change, NOT a break;
    # real breaks are caught precisely per-flow via structural_unit(). A whole-host
    # outage routes to 'partial' via transient_unit() instead of the floor.
    # The resume point was saved inside the loop, once per attempted flow; there is no second
    # writer here, so nothing can overwrite it with an older value.

    try:
        res = finalize(tally, total, last_obs, source=SOURCE,
                       series_cursors=cursors,
                       empty_window_floor=max(n_sub - 1, 1))
    except DefinitiveError as e:
        # ROUTE B (census.py's shape, hagstofa's AR-407 O7; review of the istat window_stop plan, C1). finalize()
        # raises on ANY structural flow, and the orchestrator then books `partial` with NO Result: no CSV step,
        # no series cursors. Measured on the state copy (review VERDICT_planA F3): 9 of 40 istat runs since
        # 2026-09-01 ended in that raise, and every run that attempted 69 or more flows raised except one where
        # every flow failed - so what a long run merged almost never reached the served CSVs. Now the run is
        # `partial` WITH the cursors of every flow it walked, and the note still names every structural flow
        # (finalize's own text) plus what else the run left (transient, deferred). A structural flow's stored
        # data is untouched (merge never ran for it). With no cursor to hand over, or for any other raise (the
        # all-empty floor: nothing merged), the raise stands as before.
        if not (tally.structural and cursors):
            raise
        rest = [x for x in (f"{tally.transient} transient-failed" if tally.transient else "",
                            f"{tally.deferred} deferred, not reached in this pass" if tally.deferred else "") if x]
        note = (f"{e}; {len(cursors)} walked flow(s) handed over to the CSV step anyway"
                + (f"; also {', '.join(rest)}" if rest else ""))
        print(f"[istat] {note}", flush=True)
        res = Result(status="partial", obs=total, last_obs_date=last_obs, new_vintage="date-tail",
                     series_cursors=dict(cursors), merged_rows=None, error=note)
    if stopped_at_ceiling:
        # THE WINDOW-STOP SIGNAL (Result.window_stop; registry window_stop: true), set ONLY when the
        # run-ceiling check, a request cut at the window end, or the per-turn cap ended the loop. "none" only
        # when no flow was started and no cursor is handed over: orchestrate.run_once then records no unit
        # state and logs a `window_stopped` row. A stop after a flow was started, failed or not, is "progress"
        # (recorded `partial`), and so is a ROUTE B hand-over (it carries cursors). Every other end of the run
        # leaves window_stop None, so it is recorded as the turn it is: the every-host-dead break (an ISTAT
        # outage), the budget stop, and the end of the list. An outage must stay visible to health and to the
        # outage signal, never read as "the window was short".
        res.window_stop = "none" if (flows_done == 0 and not cursors) else "progress"
    # THE TURN'S OWN END (phase B review B1): when the per-turn cap ends this turn, run_once bounds the CSV phase
    # after it - the fresh derive and the retry drain - by the cap's end as by the run ceiling, however the fetch
    # ended (cap, outage, end of the list). With the run ceiling first, None: the ceiling bounds it already.
    res.turn_end_ts = turn_end_ts
    return res
