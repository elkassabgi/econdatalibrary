#!/usr/bin/env python3
"""Generic UNCTADstat dataset ingest — the publisher's own documented data API.

One source per CURRENT dataset (successor family to the 38 retired relay-era
unctad_* slugs; upstream re-coded all dataset ids, 0 of 38 match — see #70). The full
contract was read from the app's own generated code sample and PROVEN live 2026-08-07
(scratchpad unctad_auth_findings.md):

  catalogue+schema (keyless):
    GET https://unctadstat-api.unctad.org/api/datacenter               (99 datasets)
    GET https://unctadstat-api.unctad.org/api/reportMetadata/{DS}      (dims+measures+version)
  NO LANGUAGE SEGMENT. Both used to take a trailing `/en` (the last cloud unctad ok was
  2026-10-27 23:49Z). From the 2026-10-29 22:45Z updater-daily run on, every unctad probe failed
  with "404 ... reportMetadata/<DS>/en" (run 41054519780: 68 such lines); measured 2026-10-31
  00:45Z, `.../reportMetadata/US.TradeMerchTotal/en` and `.../datacenter/en` answer 404. Without the segment
  reportMetadata/US.TradeMerchTotal answers 200 with a body byte-identical to the /en answer
  recorded on 2026-10-20 (sha256 36d334b0...61e5, docs/evidence/
  unctad_trademerchgr_withdrawal_20261020.json), so the parse and the version|lastUpdated token
  are unchanged. The datacenter answer did change shape: see tools/title_unctad_from_api.py.
  observations (keyed — UNCTAD_CLIENT_ID / UNCTAD_API_KEY from .env, NEVER in code):
    POST https://unctadstat-user-api.unctad.org/{DS}/cur/Facts?culture=en
    headers ClientId / ClientSecret; multipart form $select/$filter/$format=csv

Series identity: non-time dimension codes in (rowAxe, pageAxe) order + the measure code,
joined with '.', e.g. '0000.02.M0100' = World / exports / US$-current. obs_date from the
isTime dimension (Year -> Dec-31, matching the family's annual convention elsewhere).
One Facts POST per measure group, base (magnitude=1) variant only — the magnitude
variants are the same number scaled, not new data.

Licence: CC BY 3.0 IGO — verbatim audit §UNCTAD (DATABASE_LICENSES_VERBATIM.md:2501-2507),
CLEARED, re-host OK with attribution.

Run: python jobs/ingest_unctad_ds.py US.TradeMerchTotal [--dry]
"""
from __future__ import annotations

import csv
import datetime as dt
import io
import os
import re
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

META = "https://unctadstat-api.unctad.org/api"
FACTS = "https://unctadstat-user-api.unctad.org"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

SCHEMA = pa.schema([
    ("series_key", pa.string()), ("obs_date", pa.date32()), ("value", pa.float64()),
])


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def creds():
    # Environment first (CI secrets), .env fallback (workstation). CI has no .env file,
    # so an env-less lookup there must fail LOUDLY, not FileNotFoundError obscurely.
    cid = os.environ.get("UNCTAD_CLIENT_ID")
    key = os.environ.get("UNCTAD_API_KEY")
    if cid and key:
        return cid, key
    env_path = os.path.join(ROOT, ".env")
    if os.path.exists(env_path):
        out = {}
        with open(env_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("UNCTAD_") and "=" in line:
                    k, v = line.split("=", 1)
                    out[k.strip()] = v.strip().strip('"').strip("'")
        cid = cid or out.get("UNCTAD_CLIENT_ID")
        key = key or out.get("UNCTAD_API_KEY")
    if not (cid and key):
        raise SystemExit("UNCTAD_CLIENT_ID / UNCTAD_API_KEY missing "
                         "(environment and .env both empty)")
    return cid, key


def report_metadata_url(ds_name: str) -> str:
    """The keyless metadata address. No `/en` segment: UNCTAD answers 404 to it since 2026-10-29."""
    return f"{META}/reportMetadata/{ds_name}"


def report_metadata(ds_name: str) -> dict:
    r = requests.get(report_metadata_url(ds_name), headers=UA, timeout=120)
    r.raise_for_status()
    return r.json()


# Mechanical slugs that would COLLIDE with a legacy relay-era source id get an
# explicit override. R399: source_id_for("US.Cpi_A") produced "unctad_cpia" — the exact
# id of a legacy source with 637 live series — and the ingest silently overwrote its
# store (recovered exactly from the served CSVs). The guard in ingest() now refuses any
# id that already exists in the catalog's source table unless its homepage records THIS
# dataset, so a future collision fails loudly instead of clobbering.
SOURCE_ID_OVERRIDES = {
    "US.Cpi_A": "unctad_cpi_annual",
}


def source_id_for(ds_name: str) -> str:
    # US.TradeMerchTotal -> unctad_trademerchtotal (successor naming; legacy slugs retired)
    if ds_name in SOURCE_ID_OVERRIDES:
        return SOURCE_ID_OVERRIDES[ds_name]
    return "unctad_" + ds_name.split(".", 1)[1].replace("_", "").lower()


def assert_no_source_collision(src: str, ds_name: str) -> None:
    """Refuse to write under a source id that belongs to something else (R399)."""
    from core import catalog_path                                     # noqa: PLC0415 - plan step 1
    cat = catalog_path.under(ROOT)
    if not os.path.exists(cat):
        return
    con = catalog_path.connect_path(cat, write=False, timeout=60)
    row = con.execute("SELECT homepage FROM source WHERE source_id=?", (src,)).fetchone()
    con.close()
    if row and (not row[0] or f"/dataviewer/{ds_name}" not in row[0]):
        raise SystemExit(
            f"COLLISION: source id {src!r} already exists in the catalog and its homepage "
            f"({row[0]!r}) does not record dataset {ds_name!r}. Add an entry to "
            f"SOURCE_ID_OVERRIDES instead of overwriting a legacy source (R399).")


def parse_time(v: str, is_year: bool) -> dt.date | None:
    s = str(v).strip()
    try:
        if is_year or (len(s) == 4 and s.isdigit()):
            return dt.date(int(s), 12, 31)
        if len(s) == 10:
            return dt.date.fromisoformat(s)
        if len(s) == 7 and s[4] == "-":
            return dt.date(int(s[:4]), int(s[5:7]), 1)
        # Quarterly '2005Q01' (US.MerchVolumeQuarterly's Quarter axis, isTime=true).
        # Period-start like Q conventions elsewhere in the repo: Q1->Jan .. Q4->Oct.
        if len(s) == 7 and s[4] in "Qq" and s[:4].isdigit() and s[5:].isdigit():
            q = int(s[5:])
            if 1 <= q <= 4:
                return dt.date(int(s[:4]), (q - 1) * 3 + 1, 1)
        # Monthly '1995M01' (US.UCPI_M / CommodityPrice_M family's Period axis).
        if len(s) == 7 and s[4] in "Mm" and s[:4].isdigit() and s[5:].isdigit():
            m = int(s[5:])
            if 1 <= m <= 12:
                return dt.date(int(s[:4]), m, 1)
        # Semiannual '2018S01' (US.PortCallsArrivals_S). S1->Jan, S2->Jul (period-start).
        if len(s) == 7 and s[4] in "Ss" and s[:4].isdigit() and s[5:].isdigit():
            h = int(s[5:])
            if 1 <= h <= 2:
                return dt.date(int(s[:4]), (h - 1) * 6 + 1, 1)
    except ValueError:
        pass
    return None


def parse_period_code(v: str) -> tuple[dt.date, str | None] | None:
    """Period-coded time axes (isTime FALSE, field 'Year', string codetype).

    MEASURED on US.TradeMerchGR (52 codes): TWO families share the axis —
      annual YoY   '19801981' (consecutive years, label '1981')  -> obs at end-year
      multi-year   '19921995', '19952000' (span averages)        -> obs at end-year
    and their END-YEARS COLLIDE (7 duplicates: an annual '19941995' AND the span
    '19921995' both end 1995). So the span family carries a series-key suffix by span
    LENGTH — '|SPAN=5Y' — which makes the six 5-year averages ONE proper time series
    (obs at each span's end-year) instead of a flood of single-observation series,
    while the usda-style suffix still separates a period-average from the annual
    figure under one identity. Within a span length the end-year is unique by
    construction (same length + same end = same code), so no collisions.
    Returns (obs_date, suffix_or_None); None for an unparseable code.
    """
    s = str(v).strip()
    if len(s) == 8 and s.isdigit():
        y0, y1 = int(s[:4]), int(s[4:])
        if 1500 < y0 <= y1 < 2200:
            if y1 - y0 == 1:
                return dt.date(y1, 12, 31), None
            return dt.date(y1, 12, 31), f"|SPAN={y1 - y0}Y"
    if len(s) == 4 and s.isdigit():
        return dt.date(int(s), 12, 31), None
    return None


class FactsUnreachable(RuntimeError):
    """A Facts POST kept dying transiently (stream truncation / 5xx) after all
    retries. The chunked puller SPLITS the offending work item on this — a smaller
    response is likelier to complete before whatever kills the stream — and only a
    single-cell-set item re-raises."""


class FactsSizeCap(RuntimeError):
    """The API refuses requests estimated over ~62,500 cells with HTTP 400:
    'The request exceed the maximal size. Maximal size : 62500. Estimated size : N.'
    (measured on US.IntraTrade, estimate 6,988,144). Carries both numbers so the
    caller can compute how many time-chunks are needed."""

    def __init__(self, cap: int, estimated: int):
        super().__init__(f"Facts size cap {cap} < estimated {estimated}")
        self.cap, self.estimated = cap, estimated


class WindowStop(Exception):
    """The pass's window end arrived before the next request (opt-in WINDOWED pulls only). Nothing from the
    request that was not started is lost; every finished chunk and every size-cap answer stays on disk."""


class Window:
    """The run ceiling for one WINDOWED pull (opt-in; every caller that passes no window is unchanged).

    end_ts: the pass's run ceiling as an epoch timestamp, or None (no ceiling: nothing ever stops).
    reserve_s: the time the caller keeps for what follows the network walk (the parse and the merge). The
    network phase ends at `end_ts - reserve_s`, so no request, retry or sleep can eat that reserve.
    Before every NETWORK request (never before a chunk re-read from the spill) `before_request` refuses to
    start one when the network time left is under max(floor, 2 x the longest request this pass). Every
    attempt is clamped to the network time left (`timeout`), and every retry sleep counts against it
    (`sleep`), so the WHOLE retry ladder of one request ends by `end_ts - reserve_s`. `record` keeps the
    longest request; `tick` prints one progress line at most every `tick_s` seconds."""

    def __init__(self, end_ts, floor_s: float = 300.0, tick_s: float = 60.0, say=None, reserve_s: float = 0.0):
        self.end, self.floor_s, self.tick_s = end_ts, float(floor_s), float(tick_s)
        self.reserve_s = float(reserve_s)
        self.longest = 0.0
        self.requests = 0
        self.posts = 0
        self._borrow = False
        self.say = say or log
        self._last_tick = time.monotonic()

    def left(self):
        """Time left to the window end itself (the assembly reserve check reads this)."""
        return None if self.end is None else self.end - time.time()

    def net_left(self):
        """Time left for network requests: to the window end minus the reserve."""
        return None if self.end is None else self.end - self.reserve_s - time.time()

    def need(self) -> float:
        return max(self.floor_s, 2.0 * self.longest)

    def _limit(self):
        """The deadline the request in flight is held to: the network deadline, or - for a request a pass
        that has POSTed nothing may BORROW from the reserve - the window end itself."""
        return self.left() if self._borrow else self.net_left()

    def before_request(self, what: str) -> None:
        left = self.net_left()
        self._borrow = False
        if left is not None and left < self.need():
            # PROGRESS GUARANTEE (review AR-411 U3, B4): while a pass has POSTed nothing yet - the metadata and
            # dimension GETs do not count - each request until its FIRST POST may be taken from the reserve
            # when the window end itself still allows it. With the presence walk (a reused chunk costs a stat)
            # this matters only for a window that is short from the start. After the first POST the stop
            # applies as usual and every fetched chunk is kept. NOT guaranteed (AR-411 B2): a first chunk
            # slower than the window end is cut, kept nowhere, and the next pass in an equally short window
            # meets it again; the release is a longer window.
            if self.posts == 0 and self.left() is not None and self.left() >= self.need():
                self._borrow = True
                self.say(f"  no Facts request yet this pass and {left / 60:.1f} min of network time left: "
                         f"{what} is taken from the assembly reserve so the pass makes progress")
                return
            raise WindowStop(f"{left / 60:.1f} min of network time left (window end minus the "
                             f"{self.reserve_s / 60:g}-min assembly reserve), under the {self.need() / 60:.1f}-min "
                             f"margin (max of the {self.floor_s / 60:.1f}-min floor and 2 x the longest request "
                             f"this pass, {self.longest:.1f}s) before {what}")

    def timeout(self, cap: float) -> float:
        left = self._limit()
        if left is None:
            return cap
        if left <= 1.0:
            raise WindowStop(f"{max(left, 0):.0f}s of network time left before the window end minus the reserve")
        return min(cap, left)

    def sleep(self, s: float) -> None:
        left = self._limit()
        if left is not None and s >= left:
            raise WindowStop(f"a {s:.0f}s retry wait would pass the network deadline ({max(left, 0):.0f}s left)")
        time.sleep(s)

    def record(self, seconds: float, post: bool = True) -> None:
        """One network request's time. post=False for a dimension-table GET: `posts` counts only the Facts
        POSTs (chunks and size-cap probes), the work a stop would keep (review AR-411 U3)."""
        self.requests += 1
        if post:
            self.posts += 1
        self._borrow = False
        self.longest = max(self.longest, float(seconds))

    def tick(self, msg: str) -> None:
        now = time.monotonic()
        if now - self._last_tick >= self.tick_s:
            self._last_tick = now
            left = self.left()
            self.say(f"  {msg}; {self.requests:,} request(s), longest {self.longest:.1f}s"
                     + (f", {left / 60:.1f} min left" if left is not None else ""))


def _post_with_deadline(url, headers, files, connect_timeout=20,
                        chunk_timeout=120, total_deadline=900):
    """POST with a hard TOTAL wall-clock deadline.

    requests' `timeout` bounds connect + BETWEEN-BYTES gaps only; a server (or
    broken NAT path) that drips a byte every few seconds evades it forever —
    measured 2026-08-12: one request at the M4023->M0100 measure boundary hung
    2h40m on a dead socket while an independent probe answered the identical
    leaf query in 0.3s. Streaming with a per-chunk timeout plus an explicit
    total deadline caps the worst case at one 15-minute attempt, which the
    caller's retry ladder re-issues on a fresh connection. Returns an object
    with .status_code and .text, like the plain call.
    """
    t0 = time.monotonic()
    with requests.post(url, headers=headers, files=files, stream=True,
                       timeout=(connect_timeout, chunk_timeout)) as r:
        chunks = []
        for chunk in r.iter_content(chunk_size=1 << 16):
            if chunk:
                chunks.append(chunk)
            if time.monotonic() - t0 > total_deadline:
                raise requests.RequestException(
                    f"total deadline {total_deadline}s exceeded "
                    f"({sum(len(c) for c in chunks)} bytes in)")
        body = b"".join(chunks)
        status, enc = r.status_code, (r.encoding or "utf-8")

    class _Resp:
        pass
    resp = _Resp()
    resp.status_code = status
    resp.text = body.decode(enc, "replace")
    return resp


def facts_csv(ds_name: str, select: str, cid: str, key: str, flt: str | None = None,
              window: "Window | None" = None) -> str:
    # window (opt-in): every attempt is clamped to the time left and every retry sleep counts against it, so
    # one call can no longer outlast the pass (~6 x 900 s + sleeps before). None = exactly as before.
    form = {"$select": select, "$format": "csv"}
    if flt:
        form["$filter"] = flt
    files = {k: (None, v) for k, v in form.items()}
    _sleep = window.sleep if window is not None else time.sleep
    for attempt in range(6):
        try:
            if window is None:
                r = _post_with_deadline(f"{FACTS}/{ds_name}/cur/Facts?culture=en",
                                        headers={"ClientId": cid, "ClientSecret": key, **UA},
                                        files=files)
            else:
                tot = window.timeout(900)
                r = _post_with_deadline(f"{FACTS}/{ds_name}/cur/Facts?culture=en",
                                        headers={"ClientId": cid, "ClientSecret": key, **UA},
                                        files=files, connect_timeout=min(20, tot),
                                        chunk_timeout=min(120, tot), total_deadline=tot)
            if r.status_code == 200:
                return r.text
            # 500 belongs here too: MEASURED 2026-08-12, a lone 500 at hour ~5.5 of
            # the M0100 campaign killed the whole run via the RuntimeError below,
            # same transient-gateway class as the 401 blip documented under it.
            if r.status_code in (429, 500, 502, 503, 504):
                _sleep(20 * (attempt + 1)); continue
            if r.status_code == 401:
                # MEASURED 2026-08-10: a 401 killed the US.BiotradeMerch pull at hour
                # ~19 — and the SAME credentials returned 200 through this exact code
                # path minutes later. UNCTAD's gateway emits transient 401s; treating
                # them as instantly fatal converts a blip into a total-loss crash.
                # Retry with long backoff; only a PERSISTENT 401 (all attempts) is a
                # real revocation and falls through to FactsUnreachable.
                log(f"  Facts HTTP 401 (transient gateway auth blip); "
                    f"retry {attempt + 1} in {60 * (attempt + 1)}s")
                _sleep(60 * (attempt + 1)); continue
            if r.status_code == 400 and "maximal size" in r.text.lower():
                m = re.search(r"Maximal size\s*:\s*(\d+).*?Estimated size\s*:\s*(\d+)",
                              r.text, re.S)
                if m:
                    raise FactsSizeCap(int(m.group(1)), int(m.group(2)))
            raise RuntimeError(f"Facts HTTP {r.status_code}: {r.text[:300]}")
        except requests.RequestException as e:
            log(f"  transient {type(e).__name__}; retry {attempt + 1}")
            _sleep(30 * (attempt + 1))
    raise FactsUnreachable(f"Facts unreachable for {ds_name} after retries")


DIM_RETRY = (401, 429, 500, 502, 503, 504)   # the statuses facts_csv retries on this gateway


def dim_rows(ds_name: str, version, dim_table: str, window: "Window | None" = None) -> list[dict]:
    """One dimension table -> its rows ({Code, Label, ...}) in the publisher's order.

    THE one request path for a dimension table. dim_codes (the chunked pull) and
    tools/title_unctad_from_api.py (the labels) both read through it, so the address, the
    credentials, the retries and what counts as a refusal cannot differ between the two.

    Read from the CREDENTIALED user API, the host the Facts pull itself uses. Until 2026-10-13
    this read the keyless `unctadstat-api.unctad.org/datamart-api/{DS}/{version}/{DimTable}`.
    That address now answers 403 with the body "Blocked" (measured 2026-10-13: two datasets, two
    User-Agents, with and without credentials; reportMetadata on the same host answers 200).
    Every chunked pull asks for a dimension before its first chunk, so the block stops every
    over-cap dataset. `{FACTS}/{DS}/{version}/{DimTable}` answers 200 with the same list:
    measured on US.TradeServCatByPartner, Years is equal through the pinned and the current
    path (21 codes, numbers, same order), and Economies is equal to the list the 2026-10-01
    walk used (its 10,217 cached size-cap answers all hit in a replay). Labels, measured on
    US.TradeMerchTotal: with the labels of release 10003 the title tool composes the label part
    of all 1,220 catalogued titles as the catalogue has them. Other datasets and dimensions are
    NOT measured.

    One path, no keyless fallback: a fallback would hide which address answered, and the
    keyless one is the address that is blocked. The credentials go in headers only, never in
    the URL: the caller renders a failed request's text into the run note.

    The transient statuses facts_csv retries are retried here too: one gateway blip on a
    dimension call would otherwise end the unit's whole turn. Any other status (400, 403, 404)
    is final at once. A failed CONNECTION is not retried here and leaves this function as it
    is raised (facts_csv does retry those); the keyless address is not tried for it either.

    A table the service CUT SHORT is refused, not returned: an OData answer that carries
    `@odata.nextLink` holds only the first page. For the chunked pull a short list would mean
    chunks that are never asked for, and nothing later would show it. Measured 2026-10-13: a
    2,222-row table came back whole with no next link; larger tables are not measured.
    """
    cid, key = creds()
    url = f"{FACTS}/{ds_name}/{version}/{dim_table}?$orderby=Order&culture=en"
    for attempt in range(4):
        if window is None:
            r = requests.get(url, headers={"ClientId": cid, "ClientSecret": key, **UA}, timeout=120)
        else:
            # Clamped per read; a body that drips under the read timeout is not bounded in total
            # (a small JSON table; stated residual of the windowed pull).
            r = requests.get(url, headers={"ClientId": cid, "ClientSecret": key, **UA},
                             timeout=window.timeout(120))
        if r.status_code in DIM_RETRY and attempt < 3:
            log(f"  dimension {dim_table}: HTTP {r.status_code}; retry {attempt + 1}")
            (window.sleep if window is not None else time.sleep)(20 * (attempt + 1))
            continue
        break
    r.raise_for_status()
    body = r.json()
    if isinstance(body, dict) and body.get("@odata.nextLink"):
        raise RuntimeError(f"{ds_name} dimension {dim_table}: the answer is only a first page "
                           f"(@odata.nextLink present, {len(body.get('value') or [])} rows); "
                           f"a cut list is refused")
    vals = body.get("value", body) if isinstance(body, dict) else body
    return list(vals)


def dim_codes(ds_name: str, version, dim_table: str, window: "Window | None" = None) -> list[str]:
    """One dimension table -> its codes in the publisher's order (for chunked Facts pulls).

    The request, its address and its rules are dim_rows, above. This keeps the codes only.

    What a different list would lose: not the cached chunks (a chunk is one leaf, and a leaf
    does not depend on the order of the list it was cut from) but the cached size-cap answers,
    which would be asked again over the network.
    """
    if window is None:
        return [x["Code"] for x in dim_rows(ds_name, version, dim_table) if x.get("Code") is not None]
    window.before_request(f"dimension {dim_table}")
    t0 = time.time()
    rows = dim_rows(ds_name, version, dim_table, window=window)
    window.record(time.time() - t0, post=False)
    return [x["Code"] for x in rows if x.get("Code") is not None]


CAPPED_FILE = "_capped.tsv"   # one "<cache key>\t<cap>\t<estimate>\t." line per size-capped probe


def _spill_dir(ds_name: str) -> str:
    return os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..",
                                        "data", "_unctad_spill", source_id_for(ds_name)))


LEGACY_TZ_SLACK_S = 86400.0   # lastUpdated carries no zone: an earlier-than test must hold for any zone


def _oldest_spill_mtime(spill_dir: str):
    ts = [e.stat().st_mtime for e in os.scandir(spill_dir) if e.name.endswith(".csv")] \
        if os.path.isdir(spill_dir) else []
    return min(ts) if ts else None


def _spill_token_matches(old, token: str, version, spill_dir: "str | None" = None) -> bool:
    """The WINDOWED pull's spill gate: the spills belong to this release when their token is the full release
    token (version|lastUpdated).

    LEGACY (review AR-411 O1): a bare version (what every pull wrote before the windowed one) equal to this
    release's version is accepted ONLY when the release's lastUpdated is earlier than the OLDEST spill's mtime
    by more than LEGACY_TZ_SLACK_S (lastUpdated carries no zone) - then every chunk was fetched after the
    release was last updated, so none can hold an earlier issue of it. Otherwise (a later or unparseable
    lastUpdated, no chunk on disk) the spills are discarded. The walk then rewrites the full token."""
    if old == token:
        return True
    if old is None or old != f"{version}" or not token.startswith(f"{version}|") or spill_dir is None:
        return False
    try:
        lu = dt.datetime.fromisoformat(token.split("|", 1)[1][:19]).replace(tzinfo=dt.timezone.utc).timestamp()
    except ValueError:
        return False
    oldest = _oldest_spill_mtime(spill_dir)
    return oldest is not None and lu + LEGACY_TZ_SLACK_S < oldest


def _load_capped(spill_dir: str, meta: dict, token: "str | None" = None) -> dict:
    """{cache key: (cap, estimate)} from an earlier pass under the SAME release; {} otherwise. A torn
    last line (a kill mid-append) is skipped: that probe is simply asked again. token (windowed pulls
    only): the full release token, compared by _spill_token_matches; None = the version, as before."""
    tp, cp = os.path.join(spill_dir, "_token.txt"), os.path.join(spill_dir, CAPPED_FILE)
    try:
        old = open(tp, encoding="utf-8").read().strip()
        if token is None:
            if old != f"{meta.get('version')}":
                return {}
        elif not _spill_token_matches(old, token, meta.get("version"), spill_dir):
            return {}
        lines = open(cp, encoding="utf-8").read().splitlines()
    except OSError:
        return {}
    out = {}
    for ln in lines:
        p = ln.split("\t")
        # the end marker: a line torn inside the estimate digits would still parse, with a wrong
        # estimate that changes the chunking and orphans every spill (review R1152)
        if len(p) == 4 and p[1].isdigit() and p[2].isdigit() and p[3] == ".":
            out[p[0]] = (int(p[1]), int(p[2]))
    return out


DIMS_FILE = "_dims.json"      # WINDOWED pulls only: {dimension table name: [codes]} under the spill token


class SpillRef(str):
    """WINDOWED pulls only: a chunk named by its spill file, read once in the parse (pull_rows). A str
    subclass holding the path, so lists of chunks keep their type."""


class SpillUnreadable(RuntimeError):
    """A spilled chunk could not be read in the parse: the file is removed, so a later pass fetches it."""


def read_chunk(item) -> str:
    """The CSV text of one chunk: a SpillRef is read from its file (removed if unreadable, see SpillUnreadable);
    anything else is the text itself."""
    if not isinstance(item, SpillRef):
        return item
    try:
        with open(str(item), encoding="utf-8") as fh:
            return fh.read()
    except (OSError, UnicodeDecodeError) as e:
        try:
            os.remove(str(item))
        except OSError:
            pass
        raise SpillUnreadable(f"spilled chunk {os.path.basename(str(item))} unreadable ({e!r}); removed, "
                              f"a later pass fetches it again") from e


def _load_dims(spill_dir: str, meta: dict, token: str) -> dict:
    """The dimension code lists an earlier WINDOWED pass of the SAME release cached beside its spills (review
    AR-411 U2): with them a fully spilled walk asks the network nothing after the metadata, so a tight window
    reaches the assembly instead of stopping after one measure's replay and replaying it again. Same gate as
    the spills and the size-cap answers (_spill_token_matches); anything unreadable is simply asked again."""
    tp, dp = os.path.join(spill_dir, "_token.txt"), os.path.join(spill_dir, DIMS_FILE)
    try:
        old = open(tp, encoding="utf-8").read().strip()
        if not _spill_token_matches(old, token, meta.get("version"), spill_dir):
            return {}
        import json as _json
        obj = _json.load(open(dp, encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if not isinstance(obj, dict):
        return {}
    return {k: v for k, v in obj.items() if isinstance(k, str) and isinstance(v, list)}


def _save_dims(spill_dir: str, dims: dict) -> None:
    import json as _json
    p = os.path.join(spill_dir, DIMS_FILE)
    try:
        with open(p + ".tmp", "w", encoding="utf-8") as fh:
            _json.dump(dims, fh)
        os.replace(p + ".tmp", p)
    except OSError as e:                     # only progress is lost: a later pass asks for the lists again
        log(f"  dimension cache {p} not saved ({e!r})")


def _record_cap(spill_dir: str, capped: dict, ck: str, e: "FactsSizeCap") -> None:
    if ck in capped:
        return
    capped[ck] = (e.cap, e.estimated)
    with open(os.path.join(spill_dir, CAPPED_FILE), "a", encoding="utf-8") as fh:
        fh.write(f"{ck}\t{e.cap}\t{e.estimated}\t.\n")


def facts_csv_chunked(ds_name: str, select: str, cid: str, key: str, meta: dict,
                      tdim: dict, progress=None, window: "Window | None" = None,
                      spill_token: "str | None" = None) -> list[str]:
    """Full-dataset pull that respects the size cap: try one POST; on FactsSizeCap,
    partition the TIME dimension's codes into groups sized from the error's own
    numbers (cap/estimated, 15% headroom) and pull per group; a group that still
    caps is split in half recursively (down to single codes).

    WINDOWED (opt-in, `window` and `spill_token` given; None = exactly as before): before every NETWORK
    request - the whole-measure probe, a dimension table, a leaf or a size-cap probe, never a chunk re-read
    from the spill - `window.before_request` may raise WindowStop; every request is clamped to the window,
    timed, and a progress line is printed at most once a minute. The spills are gated on the full release
    token (`spill_token`, see _spill_token_matches), and a spill directory whose token is ABSENT is said
    to be discarded before it is."""
    import hashlib
    _t_walk = time.time()
    spill_dir = _spill_dir(ds_name)
    capped = (_load_capped(spill_dir, meta) if spill_token is None
              else _load_capped(spill_dir, meta, token=spill_token))
    top = hashlib.sha1(repr((select, "ALL")).encode()).hexdigest()[:16]
    top_was_cached = top in capped       # the whole-measure probe is a size-cap answer too
    try:
        if top in capped:
            raise FactsSizeCap(*capped[top])                 # probed on an earlier pass
        if window is None:
            return [facts_csv(ds_name, select, cid, key)]
        window.before_request("the whole-measure probe")
        _t = time.time()
        try:
            return [facts_csv(ds_name, select, cid, key, window=window)]
        finally:
            window.record(time.time() - _t)
    except FactsSizeCap as e:
        # WINDOWED: the dimension code lists come from the cache under the release token when it has them
        # (review AR-411 U2), so a fully spilled walk asks the network nothing; new lists are saved below.
        dims = _load_dims(spill_dir, meta, spill_token) if window is not None else {}
        dims_new = {}

        def _codes(name):
            if window is None:
                return dim_codes(ds_name, meta.get("version"), name)
            if name in dims:
                return list(dims[name])
            got = dim_codes(ds_name, meta.get("version"), name, window=window)
            dims[name] = dims_new[name] = list(got)
            return got
        codes = _codes(tdim["name"])
        if not codes:
            raise
        frac = max(1e-6, e.cap / e.estimated * 0.85)
        per = max(1, int(len(codes) * frac))
        if progress:
            progress(f"  size cap {e.cap:,} < est {e.estimated:,}: chunking "
                     f"{len(codes)} {tdim['name']} codes, ~{per}/chunk")
        numeric = (tdim.get("codetype") == "number")
        tfield = tdim["field"]

        def flt_for(group):
            if numeric:
                return f"{tfield} in ({','.join(str(c) for c in group)})"
            return f"{tfield}/Code in ({','.join(repr(str(c)) for c in group)})"

        # Multi-level split: on datasets with partner/product dimensions ONE time code
        # alone can exceed the cap (measured: US.IntraTrade, 225,424 cells in a single
        # year vs the 62,500 cap) — and on the densest (US.NonPlasticSubstsTradeByPartner,
        # est 167,478 for one Economy x one Year) even one time code x one first-dim code
        # still caps, so the split recurses across EVERY key dim in axis order before
        # giving up. Work items are (time_group, [dim_codes_group per engaged kdim]).
        kdims = [d for axe in ("rowAxe", "colAxe", "pageAxe")
                 for d in (meta["defaults"].get(axe) or [])
                 if not (bool(d.get("isTime"))
                         or d.get("field", "").lower() in ("year", "period"))]
        kdim_codes: dict[int, list] = {}   # kdim index -> full code list (lazy)

        def flt_for_item(tgroup, restr):
            f = flt_for(tgroup)
            for di, dgroup in enumerate(restr):
                f += (f" and {kdims[di]['field']}/Code in "
                      f"({','.join(repr(str(c)) for c in dgroup)})")
            return f

        # CHUNK SPILL + RESUME (added 2026-08-10). The US.BiotradeMerch pull died on
        # a transient 401 at hour ~19 with EVERYTHING in memory — total loss, because
        # this loop held every chunk's CSV in `out` until the final write. Each
        # completed chunk now spills to disk keyed by a hash of its (tgroup, restr)
        # work item; a restart under the SAME dataset version replays the identical
        # deterministic split walk and reloads finished chunks instead of refetching.
        # Version-gated exactly like the IMF sliced resume (tests/test_imf_sliced_resume):
        # a new UNCTAD release wipes the spills — two vintages must never be assembled.
        import shutil as _sh
        token = spill_token or f"{meta.get('version')}"
        token_path = os.path.join(spill_dir, "_token.txt")
        if os.path.isdir(spill_dir):
            old = (open(token_path, encoding="utf-8").read().strip()
                   if os.path.exists(token_path) else None)
            same = (old == token) if spill_token is None else _spill_token_matches(
                old, token, meta.get("version"), spill_dir)
            if not same:
                if window is not None:
                    n_sp = sum(1 for x in os.listdir(spill_dir) if x.endswith(".csv"))
                    log(f"  {ds_name}: spill token {old!r} is not release {token!r}"
                        + (" (token file ABSENT)" if old is None else "")
                        + f" - discarding {n_sp:,} spilled chunk(s) and the size-cap answers")
                _sh.rmtree(spill_dir, ignore_errors=True)
                capped = {}                                  # another vintage's probes, too
            elif window is not None and old != token:
                log(f"  {ds_name}: legacy bare-version spill token {old!r} accepted for release {token!r} "
                    f"(its lastUpdated is earlier than every spilled chunk); rewritten as the full token")
        os.makedirs(spill_dir, exist_ok=True)
        with open(token_path, "w", encoding="utf-8") as fh:
            fh.write(token)
        # THE SIZE-CAP ANSWERS ARE CACHED WITH THE SPILLS (review R1151). Only leaves spilled, so a
        # resume replayed the walk by re-POSTing every capped probe - and a binary split has about as
        # many capped probes as leaves (US.TradeServCatByPartner: ~208 leaves a year), so a second pass
        # spent most of its time re-learning what the first already knew (a sibling spent 172 min on
        # the desktop and its spill timestamps show 1 new chunk, R1151). Same version gate as the spills:
        # a cap is a property of the query and the release. A stale cap only replays the split pass 1
        # made - never wrong rows. Only a SIZE CAP is cached: an unreachable request is asked again.
        _record_cap(spill_dir, capped, top, e)
        if window is not None:
            if dims_new or not os.path.exists(os.path.join(spill_dir, DIMS_FILE)):
                _save_dims(spill_dir, dims)            # after the gate: the directory holds THIS release

        reused = 0
        # size-cap answers this pass had to ask the API for (not in the cache). The whole-measure
        # probe that brought the walk here is the first of them.
        caps_asked = 0 if top_was_cached else 1
        out: list[str] = []
        stack = [(codes[i:i + per], []) for i in range(0, len(codes), per)]
        while stack:
            tgroup, restr = stack.pop(0)
            # The MEASURE must be part of the cache key. The select string embeds
            # M<code>; without it, measure #2's walk cache-hits measure #1's leaf
            # files and the row parser (which reads M<code>_Value) silently skips
            # every row in them — a measure that quietly assembles as EMPTY.
            # Caught 2026-08-12 on US.BiotradeMerch before any store was written.
            # Cost of the change: spills written before it (M4023's 199,981) no
            # longer cache-hit a resume — acceptable; that measure's store is
            # already assembled by tools/_merge_unctad_spills.py, which keys rows
            # by each file's own header and is unaffected by file names.
            ck = hashlib.sha1(repr((select, tgroup, restr)).encode()).hexdigest()[:16]
            sp = os.path.join(spill_dir, ck + ".csv")
            if window is not None and os.path.isfile(sp):
                # WINDOWED: walk by PRESENCE (review AR-411 B2/B3). The split decisions need only the size-cap
                # answers, the dimension lists and which chunk files exist, so a reused chunk costs a stat; its
                # contents are read once, in the parse (pull_rows), after the assembly check. The replay then
                # no longer eats the network window, and missing chunks late in the walk are fetched first.
                out.append(SpillRef(sp))
                reused += 1
                continue
            if window is None and os.path.exists(sp):
                try:
                    out.append(open(sp, encoding="utf-8").read())
                    reused += 1
                    continue
                except OSError:
                    pass  # unreadable spill -> refetch it
            try:
                if ck in capped:
                    raise FactsSizeCap(*capped[ck])          # probed on an earlier pass
                if window is None:
                    text = facts_csv(ds_name, select, cid, key,
                                     flt=flt_for_item(tgroup, restr))
                else:
                    window.tick(f"{ds_name} walk [{select.rsplit(', ', 1)[-1]}]: {len(out):,} chunks "
                                f"({reused:,} reused), {caps_asked:,} size-cap answers asked")
                    window.before_request(f"chunk {len(out) + 1:,} of [{select.rsplit(', ', 1)[-1]}]")
                    _t = time.time()
                    try:
                        text = facts_csv(ds_name, select, cid, key,
                                         flt=flt_for_item(tgroup, restr), window=window)
                    finally:
                        window.record(time.time() - _t)
                with open(sp + ".tmp", "w", encoding="utf-8") as fh:
                    fh.write(text)
                os.replace(sp + ".tmp", sp)
                out.append(text if window is None else SpillRef(sp))
            except (FactsSizeCap, FactsUnreachable) as split_err:
                if isinstance(split_err, FactsSizeCap):
                    if ck not in capped:
                        caps_asked += 1
                    _record_cap(spill_dir, capped, ck, split_err)
                atomic = (len(tgroup) == 1
                          and all(len(g) == 1 for g in restr)
                          and len(restr) >= len(kdims))
                if isinstance(split_err, FactsUnreachable) and atomic:
                    raise    # smallest possible request still dies — real outage
                # fall through to the split ladder below (identical for both causes)
                if len(tgroup) > 1:
                    mid = len(tgroup) // 2
                    stack[:0] = [(tgroup[:mid], restr), (tgroup[mid:], restr)]
                    continue
                # halve the deepest engaged dim group that can still split
                for di in range(len(restr) - 1, -1, -1):
                    if len(restr[di]) > 1:
                        mid = len(restr[di]) // 2
                        a = restr[:di] + [restr[di][:mid]] + restr[di + 1:]
                        b = restr[:di] + [restr[di][mid:]] + restr[di + 1:]
                        stack[:0] = [(tgroup, a), (tgroup, b)]
                        break
                else:
                    # every engaged dim is a single code — engage the NEXT key dim
                    nxt = len(restr)
                    if nxt >= len(kdims):
                        raise   # atomic cell set still too big — new layout class
                    if nxt not in kdim_codes:
                        kdim_codes[nxt] = _codes(kdims[nxt]["name"])
                        if window is not None and kdims[nxt]["name"] in dims_new:
                            _save_dims(spill_dir, dims)
                        if progress:
                            progress(f"  still caps at single codes: also splitting "
                                     f"by {kdims[nxt]['name']} "
                                     f"({len(kdim_codes[nxt])} codes)")
                    full = kdim_codes[nxt]
                    mid = max(1, len(full) // 2)
                    stack[:0] = [(tgroup, restr + [full[:mid]]),
                                 (tgroup, restr + [full[mid:]])]
        # ONE LINE THAT TELLS A RESUMED WALK FROM A RESTARTED ONE (review of 2026-10-13). A killed
        # pass and a finished pass leave the same run note, and a rising file count looks the same
        # whether this pass reused its cache or learned everything again. It is printed only when a
        # measure's walk COMPLETES; a pass that is stopped before that prints nothing, and its
        # receipt is the files it wrote.
        log(f"  {ds_name} chunk walk complete for [{select.rsplit(', ', 1)[-1]}]: {len(out):,} "
            f"chunks = {reused:,} reused from the spill + {len(out) - reused:,} fetched; "
            f"size-cap answers asked this pass: {caps_asked:,}")
        if window is not None:
            log(f"  {ds_name} walk timing [{select.rsplit(', ', 1)[-1]}]: {time.time() - _t_walk:.1f}s, "
                f"{window.requests:,} request(s) so far this pass, longest {window.longest:.1f}s")
        return out


class UnsupportedLayout(RuntimeError):
    """A dataset shape this generic machinery has not been taught. Callers decide
    whether that is fatal (ingest) or a structural unit-failure (fetcher)."""


def dataset_layout(meta: dict):
    """(kfields, tfield, is_year, period_axis, measures) from reportMetadata.

    Non-time dims in (rowAxe, colAxe, pageAxe) order form the series key; the single
    time axis is the observation axis. isTime is authoritative when SET — but
    PERIOD-CODED axes (growth-rate datasets) report isTime FALSE while still being
    the time axis (measured: US.TradeMerchGR's colAxe is name=Periods, field=Year,
    isTime=false, codes like '19921995'); the field name 'Year' is the API's own
    time marker in that layout. Measures: the magnitude-1 base per observation group
    (the variants are the same number scaled).
    """
    defaults = meta["defaults"]
    dims = [d for axe in ("rowAxe", "colAxe", "pageAxe") for d in defaults.get(axe) or []]

    def _is_time(d):
        # UNCTAD marks most time axes isTime; some layouts instead carry a bare
        # 'Year' field, and the growth-rate datasets (CreativeGoodsGR) a 'Period'
        # field whose codes are the 8-digit YYYYYYYY spans parse_period_code
        # already handles. A non-time dim named Period would fail loudly at parse.
        return (bool(d.get("isTime"))
                or d.get("field", "").lower() in ("year", "period"))

    time_dims = [d for d in dims if _is_time(d)]
    key_dims = [d for d in dims if not _is_time(d)]
    if len(time_dims) != 1:
        raise UnsupportedLayout(f"{meta.get('name')}: {len(time_dims)} time dims")
    tdim = time_dims[0]
    measures = []
    for grp in defaults.get("observations") or []:
        base = next((m for m in grp.get("measures", []) if m.get("magnitude") == 1), None)
        if base:
            measures.append(base["code"])
    if not measures:
        raise UnsupportedLayout(f"{meta.get('name')}: no magnitude-1 measures")
    return ([d["field"] for d in key_dims], tdim["field"],
            bool(tdim.get("isTime")) and tdim["field"].lower() == "year",
            not tdim.get("isTime"), measures)


def dataset_time_dim(meta: dict) -> dict:
    """The raw time-axis dict (name/field/codetype) — needed for chunked pulls.
    Same predicate as dataset_layout's _is_time (keep the two in lockstep —
    the CreativeGoodsGR retry failed here after only _is_time learned 'period')."""
    defaults = meta["defaults"]
    dims = [d for axe in ("rowAxe", "colAxe", "pageAxe") for d in defaults.get(axe) or []]
    for d in dims:
        if bool(d.get("isTime")) or d.get("field", "").lower() in ("year", "period"):
            return d
    raise UnsupportedLayout(f"{meta.get('name')}: no time dim")


def pull_rows(ds_name: str, cid: str, key: str, meta: dict, progress=None,
              measures_filter: list[str] | None = None, window: "Window | None" = None,
              spill_token: "str | None" = None, before_assembly=None):
    """Fetch + parse ALL observations for one dataset. THE single row-building path —
    the ingest below and every fetcher call this, so the parse rules cannot drift
    between the two (the insee_bdm/eurostat parity lesson, R-ledger 2026-08-08).
    Returns (rows_k, rows_d, rows_v).

    measures_filter: restrict the pull to these measure codes (e.g. ["M0100"]).
    Used to resume ONE measure's campaign when the others are already complete
    in the spill cache (US.BiotradeMerch 2026-08-12: M4023 done, M0100 unpulled)
    — the spills persist either way and tools/_merge_unctad_spills.py assembles
    the full store from all measures once each campaign has run."""
    kfields, tfield, is_year, period_axis, measures = dataset_layout(meta)
    if measures_filter:
        keep = {m[1:] if m.upper().startswith("M") else m for m in measures_filter}
        measures = [m for m in measures if m in keep]
        if not measures:
            raise SystemExit(f"--measures matched nothing; dataset has {keep} vs "
                             f"{dataset_layout(meta)[4]}")
    tdim = dataset_time_dim(meta)
    rows_k, rows_d, rows_v = [], [], []
    # WINDOWED (opt-in, `window` given; None = exactly as before): EVERY measure's walk runs first, then
    # `before_assembly()` (the caller's reserve check, which may raise WindowStop), then the parse - so a stop
    # never throws away a parse, and the parse and its time are measured on their own. The walk returns
    # SpillRef paths; the parse reads each chunk's file here, one at a time (read_chunk).
    walked = None
    if window is not None:
        walked = []
        for mcode in measures:
            select = ", ".join(f"{f}/Code" for f in kfields) + f", {tfield}, M{mcode}/Value"
            walked.append((mcode, facts_csv_chunked(ds_name, select, cid, key, meta, tdim, progress=progress,
                                                    window=window, spill_token=spill_token)))
        if before_assembly is not None:
            before_assembly()
        t_parse = time.time()
    for mcode in measures:
        if walked is None:
            select = ", ".join(f"{f}/Code" for f in kfields) + f", {tfield}, M{mcode}/Value"
            texts = facts_csv_chunked(ds_name, select, cid, key, meta, tdim, progress=progress)
        else:
            texts = dict(walked)[mcode]
        n0 = len(rows_k)
        recs = ((rec for text in texts for rec in csv.DictReader(io.StringIO(text))) if walked is None
                # WINDOWED: each chunk is read from its spill HERE, one at a time (the walk only stat'ed it)
                else (rec for item in texts for rec in csv.DictReader(io.StringIO(read_chunk(item)))))
        for rec in recs:
            vals = [rec.get(f"{f}_Code", "") for f in kfields]
            tv = rec.get(tfield) or rec.get(f"{tfield}_Code", "")
            vv = rec.get(f"M{mcode}_Value", "")
            suffix = None
            if period_axis:
                parsed = parse_period_code(tv)
                if parsed is None:
                    continue
                d, suffix = parsed
            else:
                d = parse_time(tv, is_year)
            if d is None or vv in ("", None):
                continue
            try:
                v = float(vv)
            except ValueError:
                continue
            rows_k.append(".".join(vals + [f"M{mcode}"]) + (suffix or ""))
            rows_d.append(d)
            rows_v.append(v)
        if progress:
            progress(f"  M{mcode}: {len(rows_k) - n0:,} obs")
    if walked is not None:
        log(f"  {ds_name} parse timing: {time.time() - t_parse:.1f}s for {len(rows_k):,} obs")

    # THE KEY HAS TO BE A KEY, AND THIS IS THE ONLY PLACE THAT CAN CHECK IT (R814).
    # `dataset_layout` derives kfields from the DEFAULT LAYOUT's axes, so a dimension the
    # publisher does not place on an axis is projected away by the `$select` above - and a
    # projection does not aggregate, it duplicates. Measured on US.TradeFoodProcCat_Cat_RCA:
    # `Flow` (01 Imports, 02 Exports) is on no axis, so both flows arrive under one key and the
    # store holds 648,241 rows over 362,203 distinct (series_key, obs_date) pairs, max group
    # size exactly 2. Its sibling: 712,550 over 395,121. Both are `partial` on EVERY recorded run
    # - 5 runs each in `runs`, earliest 2026-08-10T12:28:21Z, and `last_success_utc` is NULL for
    # both, so neither has ever succeeded - because never-shrink correctly refuses to collapse
    # 44% of the file. The fetch was never the problem. (An earlier draft of this comment said
    # "since 2026-08-30", which is 20 days and three runs late and invites a hunt for a
    # late-August regression that does not exist; corrected against state.db 2026-09-06.)
    #
    # THIS FUNCTION, not `ingest()`: the docstring above calls pull_rows "THE single row-building
    # path - the ingest below and every fetcher call this", and `_unctad.py:64` does exactly
    # that. A guard in `ingest()` would leave the nightly path unprotected, which is the
    # two-producers-one-rule mistake made in defillama a few hours ago (R805).
    #
    # Refusing is the safe direction: the store keeps what it has and the run says why, rather
    # than writing a file whose ids do not identify anything.
    pairs = len(set(zip(rows_k, rows_d)))
    if rows_k and pairs != len(rows_k):
        dup = len(rows_k) - pairs
        raise UnsupportedLayout(
            f"{ds_name}: the key does not identify a row - {len(rows_k):,} observations "
            f"collapse to {pairs:,} distinct (series_key, obs_date) pairs, {dup:,} duplicated "
            f"({dup / len(rows_k) * 100:.1f}%). A dimension the fact table varies along is "
            f"absent from the layout's axes and is being projected away by the $select; the "
            f"declared fact key in the dataset's $metadata names it. Existing data kept.")

    return rows_k, rows_d, rows_v


def ingest(ds_name: str, dry: bool, measures_only: list[str] | None = None) -> int:
    if measures_only and not dry:
        # A one-measure pull is a CACHE-FILLING campaign, never a store write: the
        # ingest's own write would replace the canonical parquet with a partial-
        # measure store. Assemble via tools/_merge_unctad_spills.py instead.
        raise SystemExit("--measures pulls a subset; it must run with --dry "
                         "(spills persist; merge with tools/_merge_unctad_spills.py)")
    cid, key = creds()
    meta = report_metadata(ds_name)
    src = source_id_for(ds_name)
    assert_no_source_collision(src, ds_name)
    out_dir = os.path.join(ROOT, "data", "clean_full", src)

    try:
        kfields, tfield, _, _, measures = dataset_layout(meta)
    except UnsupportedLayout as e:
        raise SystemExit(f"{e} — teach the job this layout")
    log(f"{ds_name} -> {src}: key dims {kfields}, time {tfield}, "
        f"measures {['M' + c for c in measures]}, version {meta.get('version')}")

    rows_k, rows_d, rows_v = pull_rows(ds_name, cid, key, meta, progress=log,
                                        measures_filter=measures_only)

    if not rows_k:
        log(f"  {ds_name}: 0 obs — refusing to write an empty store")
        return 0
    n_series = len(set(rows_k))
    if dry:
        log(f"  DRY: {len(rows_k):,} obs / {n_series:,} series")
        return len(rows_k)
    os.makedirs(out_dir, exist_ok=True)
    tbl = pa.table({"series_key": pa.array(rows_k, pa.string()),
                    "obs_date": pa.array(rows_d, pa.date32()),
                    "value": pa.array(rows_v, pa.float64())}, schema=SCHEMA)
    path = os.path.join(out_dir, f"{src}.parquet")
    pq.write_table(tbl, path, compression="zstd")
    n = pq.read_metadata(path).num_rows
    log(f"  WROTE {path}: {n:,} obs / {n_series:,} series")
    return n


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if not args:
        raise SystemExit("usage: ingest_unctad_ds.py <US.DatasetName> [--dry]")
    measures_only = None
    for a in sys.argv[1:]:
        if a.startswith("--measures="):
            measures_only = a.split("=", 1)[1].split(",")
    ingest(args[0], "--dry" in sys.argv, measures_only)
