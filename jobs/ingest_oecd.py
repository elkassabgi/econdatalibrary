#!/usr/bin/env python3
"""FULL-COVERAGE grouped ingest of the entire OECD SDMX catalog (Data Explorer).

Source: OECD (CC BY 4.0). license_id = cc-by-4.0 (from configs/sources.yaml).

Strategy (verified against sdmx.oecd.org, NSI Web Service v8.19.8.0):
  * Catalog = every dataflow from /public/rest/dataflow/all/all/latest (1509 flows).
  * Most flows are served from the DEFAULT root  https://sdmx.oecd.org/public .
    27 flows are isExternalReference=true and route to alternate roots
    (sti-public / dcd-public / archive) -- we resolve each flow's structureURL root.
  * Data pulled as SDMX-CSV (Accept: application/vnd.sdmx.data+csv) via
       /rest/data/<agency>,<id>,<ver>/all?dimensionAtObservation=AllDimensions
  * SDMX-CSV header = DATAFLOW, <dim1..dimN>, TIME_PERIOD, OBS_VALUE, <attrs...>.
    SERIES KEY = the dimension columns (everything strictly between DATAFLOW and
    TIME_PERIOD), joined with '.'  -> stored as column `series_key`.
  * OECD caps a response at ~1e6 observations; very large flows return HTTP 500 /
    never complete. FALLBACK: split the query by REF_AREA using the SDMX path-key
    (REF_AREA value + dots for the remaining dims, in DSD position order); the set
    of real reporters comes from /rest/availableconstraint/.../all?mode=available.

OUTPUT: ONE Parquet per dataflow ->
    data/clean_full/oecd/<AGENCY>__<DATAFLOW_ID>.parquet
  columns: series_key (str), obs_date (date32), value (float64),
           obs_status (str), time_raw (str)
A coarse per-dataflow row also gets written to a sidecar manifest (NOT catalog.db).

Memory is bounded: each dataflow is streamed to a temp .csv on disk, parsed in a
single pass, and written once; temp files are deleted after.

Usage:
  python jobs/ingest_oecd.py --catalog            # (re)download dataflow list, print counts
  python jobs/ingest_oecd.py --dry 8              # process first 8 flows, no Parquet writes
  python jobs/ingest_oecd.py --workers 6          # FULL run (default workers=6)
  python jobs/ingest_oecd.py --only DSD_FOO@DF_BAR   # process one flow (debug)
"""
from __future__ import annotations

import csv
import datetime as dt
import json
import os
import re
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = r"D:/research/econfindatalibrary"
sys.path.insert(0, ROOT)

RAW = os.path.join(ROOT, "data", "raw", "oecd")
OUT = os.path.join(ROOT, "data", "clean_full", "oecd")
TMP = os.path.join(RAW, "_tmp")
os.makedirs(RAW, exist_ok=True)
os.makedirs(OUT, exist_ok=True)
os.makedirs(TMP, exist_ok=True)

UA = "Econ-Fin Data Library admin@hfdatalibrary.com"
HDR_CSV = {"User-Agent": UA, "Accept": "application/vnd.sdmx.data+csv; charset=utf-8",
           "Accept-Encoding": "gzip, deflate"}
HDR_XML = {"User-Agent": UA, "Accept-Encoding": "gzip, deflate"}

DEFAULT_ROOT = "https://sdmx.oecd.org/public"
# csv stream sizing
FULL_TIMEOUT = 900          # seconds for a single full /all stream
SPLIT_TIMEOUT = 600         # per-REF_AREA stream
CONNECT_TIMEOUT = 30
CHUNK = 1 << 20
# row-group flush size: bounds peak RAM per worker to ~one batch of 5 column lists.
# Workers are THREADS in one process, so total buffer RAM ~= workers x this. Keep
# modest so several concurrent big flows don't blow the box.
BATCH_ROWS = 200_000
# The OECD endpoint is observed to serve multi-million-row responses in one shot
# (e.g. 3.97M rows OK), so we DO NOT pre-emptively split on row count. We only fall
# back to REF_AREA splitting when /all genuinely fails (500 / timeout / persistent
# 404 that is NOT a structural 'no data' error). A high guard remains to catch a
# response that looks truncated exactly at a round cap.
CAP_HINT = 100_000_000      # effectively off; real splits are failure-driven
# transient-404 retry: the NSI cache flakes to 404 on repeated big queries; genuine
# first-attempt 404s resolve on retry, while structural 404s carry a 'no data' body.
# Kept modest so a single quota-blocked flow does not stall a worker for many minutes;
# flows left 'failed'/'partial' are retried in a final low-concurrency pass.
FULL_RETRIES = 6
# fields that are NEVER part of the series key (attributes + time + value)
NON_KEY = {"TIME_PERIOD", "OBS_VALUE", "OBS_STATUS", "UNIT_MULT", "DECIMALS",
           "BASE_PER", "DATAFLOW"}
# 404 bodies that mean "this flow truly serves no data" (do not retry forever)
STRUCTURAL_404 = ("doesn't contain a mapping", "no mapping set",
                  "Could not find Dataflow")
# 404 body that means "this VERSION has no records" -- the advertised latest version
# is sometimes empty while an older version holds the data (e.g. FIRES v1.4 empty,
# v1.0 = 17MB). We retry across other versions before declaring a flow empty.
NORECORDS_404 = "norecordsfound"
# 429 body marker for OECD's application-level data-download quota (distinct from a
# generic Cloudflare burst 429). Triggers a longer global cooldown.
QUOTA_429 = "exceeded the number of requests"
# version-fallback (try older versions when the advertised one is NoRecordsFound) is
# quota-expensive; off in the bulk pass, enabled via --version-fallback for a targeted
# second pass over the flows the bulk pass marked 'empty'.
VERSION_FALLBACK = False

NS = {
    "m": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
    "s": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
    "c": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
}

_print_lock = threading.Lock()


def log(msg):
    with _print_lock:
        print(msg, flush=True)


# ----------------------------------------------------------------------------- global throttle
# OECD sits behind Cloudflare, which rate-limits bursts (HTTP 429). A token-bucket
# shared across all worker threads spaces request STARTS by >= MIN_INTERVAL seconds,
# so total request rate stays gentle regardless of concurrency. On a 429 anywhere we
# trip a cooldown that every thread observes (penalty box).
MIN_INTERVAL = float(os.environ.get("OECD_MIN_INTERVAL", "4.0"))  # seconds between starts
_gate_lock = threading.Lock()
_next_allowed = [0.0]
_cooldown_until = [0.0]


def throttle():
    """Block until this thread may start a request (respects spacing + any cooldown).
    Reserves exactly one slot under the lock, then sleeps until that slot OUTSIDE the
    lock. (Must NOT re-reserve in a loop, or _next_allowed races ahead of wall-clock
    and the sleep never ends.)"""
    with _gate_lock:
        now = time.time()
        start = max(now, _next_allowed[0], _cooldown_until[0])
        _next_allowed[0] = start + MIN_INTERVAL
    wait = start - time.time()
    if wait > 0:
        time.sleep(wait)


def trip_cooldown(seconds):
    """Put all threads in a penalty box for `seconds` after a 429."""
    with _gate_lock:
        _cooldown_until[0] = max(_cooldown_until[0], time.time() + seconds)


# ----------------------------------------------------------------------------- session
def make_session():
    s = requests.Session()
    retry = requests.adapters.Retry(
        total=4, backoff_factor=1.5,
        status_forcelist=(429, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    ad = requests.adapters.HTTPAdapter(max_retries=retry, pool_connections=12, pool_maxsize=12)
    s.mount("https://", ad)
    return s


# ----------------------------------------------------------------------------- catalog
def download_catalog(sess):
    """Download the full dataflow list and build a manifest with the serving root."""
    import xml.etree.ElementTree as ET
    url = f"{DEFAULT_ROOT}/rest/dataflow/all/all/latest"
    path = os.path.join(RAW, "dataflows_all.xml")
    log(f"[catalog] GET {url}")
    r = sess.get(url, headers=HDR_XML, timeout=(CONNECT_TIMEOUT, 180))
    r.raise_for_status()
    with open(path, "wb") as f:
        f.write(r.content)
    root = ET.fromstring(r.content)
    flows = []
    for df in root.findall(".//s:Dataflow", NS):
        agency = df.get("agencyID")
        did = df.get("id")
        ver = df.get("version")
        ext = df.get("isExternalReference") == "true"
        surl = df.get("structureURL") or ""
        name_el = df.find("c:Name", NS)
        name = name_el.text if name_el is not None else did
        # serving root: default unless external ref points elsewhere
        root_base = DEFAULT_ROOT
        if ext and surl:
            m = re.match(r"(https?://[^/]+/[^/]+)/", surl)
            if m:
                root_base = m.group(1)
        flows.append({
            "agency": agency, "id": did, "version": ver,
            "name": name, "root": root_base, "external": ext,
        })
    mpath = os.path.join(RAW, "manifest.json")
    with open(mpath, "w", encoding="utf-8") as f:
        json.dump(flows, f, ensure_ascii=False)
    log(f"[catalog] {len(flows)} dataflows  ->  {mpath}")
    from collections import Counter
    roots = Counter(x["root"] for x in flows)
    for k, v in roots.most_common():
        log(f"           root {k}: {v}")
    return flows


def load_manifest():
    mpath = os.path.join(RAW, "manifest.json")
    with open(mpath, encoding="utf-8") as f:
        return json.load(f)


# ----------------------------------------------------------------------------- period parse
_Q = re.compile(r"^(\d{4})-Q([1-4])$")
_M = re.compile(r"^(\d{4})-(\d{2})$")
_D = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
_S = re.compile(r"^(\d{4})-S([12])$")
_W = re.compile(r"^(\d{4})-W(\d{2})$")
_Y = re.compile(r"^(\d{4})$")


def parse_period(p):
    p = p.strip()
    if not p:
        return None
    m = _Y.match(p)
    if m:
        return dt.date(int(m.group(1)), 12, 31)
    m = _M.match(p)
    if m:
        try:
            return dt.date(int(m.group(1)), int(m.group(2)), 1)
        except ValueError:
            return None
    m = _Q.match(p)
    if m:
        return dt.date(int(m.group(1)), (int(m.group(2)) - 1) * 3 + 1, 1)
    m = _D.match(p)
    if m:
        try:
            return dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None
    m = _S.match(p)
    if m:
        return dt.date(int(m.group(1)), 1 if m.group(2) == "1" else 7, 1)
    m = _W.match(p)
    if m:
        try:
            return dt.date.fromisocalendar(int(m.group(1)), int(m.group(2)), 1)
        except ValueError:
            return None
    return None


def to_float(s):
    if s == "" or s is None:
        return None
    try:
        return float(s)
    except ValueError:
        return None


# ----------------------------------------------------------------------------- streaming GET
def stream_to_file(sess, url, dest, timeout):
    """Stream a GET response body to `dest`.
    Returns (http_status, bytes, ok_complete, err_body, retry_after).
    ok_complete is False on non-200 or on a mid-stream error. err_body holds up to
    ~300 chars of the error response (for structural-404 detection)."""
    throttle()
    try:
        with sess.get(url, headers=HDR_CSV, timeout=(CONNECT_TIMEOUT, timeout),
                      stream=True) as r:
            status = r.status_code
            if status != 200:
                body = ""
                try:
                    body = r.content[:300].decode("utf-8", "replace")
                except Exception:
                    pass
                ra = r.headers.get("Retry-After")
                if status == 429:
                    # OECD's application quota needs a real cooldown; a plain CF burst
                    # 429 clears fast. Penalty box is global so all threads ease off.
                    trip_cooldown(35.0 if QUOTA_429 in body.lower() else 12.0)
                return status, 0, False, body, ra
            n = 0
            with open(dest, "wb") as f:
                for chunk in r.iter_content(CHUNK):
                    if chunk:
                        f.write(chunk)
                        n += len(chunk)
            return status, n, True, "", None
    except requests.exceptions.RequestException as e:
        return -1, 0, False, f"{type(e).__name__}: {e}", None


def stream_full_retrying(sess, url, dest, timeout, retries=FULL_RETRIES):
    """Stream /all with retry/backoff. Returns
    (status, bytes, ok, err_body, structural_empty, no_records).
      structural_empty -> flow has no DSD mapping (truly nothing to fetch)
      no_records       -> THIS version returned NoRecordsFound (caller may try other
                          versions; a couple of retries guard against a quota-masked
                          NoRecordsFound)."""
    last = (-1, 0, False, "")
    nr_seen = 0
    for attempt in range(retries):
        st, nb, ok, body, ra = stream_to_file(sess, url, dest, timeout)
        if ok:
            return st, nb, True, "", False, False
        last = (st, nb, ok, body)
        lb = body.lower()
        if st == 404 and any(s.lower() in lb for s in STRUCTURAL_404):
            return st, nb, False, body, True, False
        if st == 404 and NORECORDS_404 in lb:
            nr_seen += 1
            if nr_seen >= 2:   # consistently empty for this version
                return st, nb, False, body, False, True
            time.sleep(3.0)
            continue
        _safe_rm(dest)
        # backoff. OECD's quota sends Retry-After:0 (useless) and resets after ~60-90s
        # of low activity, so for 429 we wait a fixed ~35s (long enough to outlast the
        # window) rather than a short exponential that just burns retries while blocked.
        if st == 429:
            wait = 35.0
        else:
            wait = min(60, 4 * (2 ** attempt)) + (attempt * 1.0)
            if ra:
                try:
                    wait = max(wait, float(ra))
                except (TypeError, ValueError):
                    pass
        time.sleep(wait)
    st, nb, ok, body = last
    nr = (st == 404 and NORECORDS_404 in (body or "").lower())
    return st, nb, ok, body, False, nr


# ----------------------------------------------------------------------------- CSV -> parquet
def parse_csv_keyidx(path):
    """Open a SDMX-CSV file, return (file_obj, reader, dim_idx, ti, vi, si).
    dim_idx = column indices forming the series key (between DATAFLOW and TIME_PERIOD).
    Caller iterates `reader`. Returns None if header missing."""
    f = open(path, "r", encoding="utf-8", newline="")
    reader = csv.reader(f)
    try:
        header = next(reader)
    except StopIteration:
        f.close()
        return None
    try:
        ti = header.index("TIME_PERIOD")
        vi = header.index("OBS_VALUE")
    except ValueError:
        f.close()
        return None
    si = header.index("OBS_STATUS") if "OBS_STATUS" in header else -1
    # dims = columns after DATAFLOW (idx 0) up to TIME_PERIOD
    dim_idx = [i for i in range(1, ti)]
    return f, reader, dim_idx, ti, vi, si


_SCHEMA = pa.schema([
    ("series_key", pa.string()),
    ("obs_date", pa.date32()),
    ("value", pa.float64()),
    ("obs_status", pa.string()),
    ("time_raw", pa.string()),
])


class BatchWriter:
    """BOUNDED-MEMORY grouped writer: streams a SDMX-CSV (or several, for the split
    path) into ONE Parquet file, flushing a row-group every BATCH_ROWS rows. Peak
    memory is ~one batch of column lists + the distinct-series set (the set holds only
    unique keys, typically <= a few million strings, NOT every observation).

    Writes to a .part file; caller calls finalize() to fsync/close and rename. If no
    rows were written the .part file is removed and (0,0) returned."""

    # cap the exact distinct-series set; beyond this we stop tracking exactly to keep
    # memory bounded and report the count as a floor (">="). ~2M keys ~= a few hundred
    # MB; with several concurrent giant flows (threads share the process) this matters.
    SERIES_CAP = 2_000_000

    def __init__(self, dest):
        self.dest = dest
        self.part = dest + ".part"
        self.writer = None
        self.n_obs = 0
        self.series = set()
        self.series_capped = False
        self._reset_buf()

    def _reset_buf(self):
        self.bk, self.bd, self.bv, self.bs, self.bt = [], [], [], [], []

    def _flush(self):
        if not self.bk:
            return
        if self.writer is None:
            self.writer = pq.ParquetWriter(self.part, _SCHEMA, compression="zstd")
        batch = pa.record_batch([
            pa.array(self.bk, type=pa.string()),
            pa.array(self.bd, type=pa.date32()),
            pa.array(self.bv, type=pa.float64()),
            pa.array(self.bs, type=pa.string()),
            pa.array(self.bt, type=pa.string()),
        ], schema=_SCHEMA)
        self.writer.write_batch(batch)
        self._reset_buf()

    def add_from_csv(self, path):
        got = parse_csv_keyidx(path)
        if got is None:
            return 0
        f, reader, dim_idx, ti, vi, si = got
        n = 0
        bk, bd, bv, bs, bt = self.bk, self.bd, self.bv, self.bs, self.bt
        seri = self.series
        with f:
            for row in reader:
                if len(row) <= ti:
                    continue
                praw = row[ti]
                od = parse_period(praw)
                if od is None:
                    continue
                val = to_float(row[vi])
                if val is None:
                    continue
                key = ".".join(row[i] for i in dim_idx)
                bk.append(key)
                bd.append(od)
                bv.append(val)
                bs.append(row[si] if (si >= 0 and si < len(row)) else "")
                bt.append(praw)
                if not self.series_capped:
                    seri.add(key)
                    if len(seri) >= self.SERIES_CAP:
                        self.series_capped = True
                n += 1
                if len(bk) >= BATCH_ROWS:
                    self._flush()
                    bk, bd, bv, bs, bt = self.bk, self.bd, self.bv, self.bs, self.bt
        self.n_obs += n
        return n

    def __len__(self):
        return self.n_obs

    def finalize(self):
        self._flush()
        if self.writer is None:
            # nothing written
            _safe_rm(self.part)
            return 0, 0
        self.writer.close()
        self.writer = None
        # atomic-ish replace
        try:
            if os.path.exists(self.dest):
                os.remove(self.dest)
            os.replace(self.part, self.dest)
        except OSError:
            # fall back to copy
            os.replace(self.part, self.dest)
        return self.n_obs, len(self.series)

    def abort(self):
        try:
            if self.writer is not None:
                self.writer.close()
        except Exception:
            pass
        _safe_rm(self.part)


# ----------------------------------------------------------------------------- structure helpers (split path)
def get_dim_order(sess, flow):
    """Return ordered dimension ids for a flow (DSD position order). Resolve via the
    flow's own structure (references=all gives the DSD)."""
    import xml.etree.ElementTree as ET
    base = flow["root"]
    url = (f"{base}/rest/dataflow/{flow['agency']}/{flow['id']}/{flow['version']}"
           f"?references=all&detail=referencepartial")
    throttle()
    r = sess.get(url, headers=HDR_XML, timeout=(CONNECT_TIMEOUT, 180))
    r.raise_for_status()
    root = ET.fromstring(r.content)
    dims = []
    for el in root.iter():
        if el.tag.split("}")[-1] == "Dimension" and el.get("position"):
            dims.append((int(el.get("position")), el.get("id")))
    dims.sort()
    return [d[1] for d in dims]


def get_ref_areas(sess, flow):
    """Actual REF_AREA reporter codes via availableconstraint."""
    import xml.etree.ElementTree as ET
    base = flow["root"]
    url = (f"{base}/rest/availableconstraint/"
           f"{flow['agency']},{flow['id']},{flow['version']}/all/all?mode=available")
    throttle()
    r = sess.get(url, headers=HDR_XML, timeout=(CONNECT_TIMEOUT, 300))
    r.raise_for_status()
    root = ET.fromstring(r.content)
    for kv in root.iter():
        if kv.tag.split("}")[-1] == "KeyValue" and kv.get("id") == "REF_AREA":
            return [v.text for v in kv if v.tag.split("}")[-1] == "Value" and v.text]
    return []


# ----------------------------------------------------------------------------- one dataflow
def flow_filename(flow):
    safe = (flow["agency"] + "__" + flow["id"]).replace("/", "_").replace(":", "_")
    return safe + ".parquet"


def data_url_all(flow):
    return (f"{flow['root']}/rest/data/"
            f"{flow['agency']},{flow['id']},{flow['version']}/all"
            f"?dimensionAtObservation=AllDimensions")


def data_url_key(flow, key):
    return (f"{flow['root']}/rest/data/"
            f"{flow['agency']},{flow['id']},{flow['version']}/{key}"
            f"?dimensionAtObservation=AllDimensions")


def _fetch_one_area(sess, flow, dims, ra_pos, area, sink, tmp):
    """Fetch one REF_AREA via path-key, append to sink. If that single area is itself
    too big (500/timeout after retries) AND a COUNTERPART_AREA dim exists, split it
    further by counterpart. Returns (ok, sub_failed_count)."""
    ndims = len(dims)
    parts = ["" if i != ra_pos else area for i in range(ndims)]
    key = ".".join(parts)
    st, nb, okk, body, _, _ = stream_full_retrying(
        sess, data_url_key(flow, key), tmp, SPLIT_TIMEOUT, retries=4)
    if okk:
        sink.add_from_csv(tmp)
        _safe_rm(tmp)
        return True, 0
    _safe_rm(tmp)
    # secondary split on COUNTERPART_AREA if present (handles BIMTS-6D giant reporters)
    if "COUNTERPART_AREA" in dims:
        cp_pos = dims.index("COUNTERPART_AREA")
        try:
            counterparts = _avail_values(sess, flow, "COUNTERPART_AREA",
                                         filt={"REF_AREA": area})
        except Exception:
            counterparts = []
        if counterparts:
            sub_fail = 0
            for cp in counterparts:
                p2 = ["" for _ in range(ndims)]
                p2[ra_pos] = area
                p2[cp_pos] = cp
                k2 = ".".join(p2)
                s2, n2, ok2, b2, _, _ = stream_full_retrying(
                    sess, data_url_key(flow, k2), tmp, SPLIT_TIMEOUT, retries=4)
                if ok2:
                    sink.add_from_csv(tmp)
                _safe_rm(tmp)
                if not ok2:
                    sub_fail += 1
            return (sub_fail == 0), sub_fail
    return False, 1


def _avail_values(sess, flow, dim_id, filt=None):
    """REF_AREA-style availability for an arbitrary dimension, optionally constrained.
    filt e.g. {'REF_AREA':'USA'} -> builds the /availableconstraint key."""
    import xml.etree.ElementTree as ET
    base = flow["root"]
    keypart = "all"
    url = (f"{base}/rest/availableconstraint/"
           f"{flow['agency']},{flow['id']},{flow['version']}/{keypart}/all"
           f"?mode=available")
    if filt:
        # add component constraints as query (NSI supports c[DIM]=val)
        qs = "".join(f"&c[{k}]={v}" for k, v in filt.items())
        url += qs
    throttle()
    r = sess.get(url, headers=HDR_XML, timeout=(CONNECT_TIMEOUT, 300))
    r.raise_for_status()
    root = ET.fromstring(r.content)
    for kv in root.iter():
        if kv.tag.split("}")[-1] == "KeyValue" and kv.get("id") == dim_id:
            return [v.text for v in kv if v.tag.split("}")[-1] == "Value" and v.text]
    return []


def get_versions(sess, flow):
    """All published versions of this dataflow id, DESCENDING (newest first). Used to
    fall back when the advertised latest version returns NoRecordsFound but an older
    version actually holds the data (observed on several OECD flows)."""
    import xml.etree.ElementTree as ET
    base = flow["root"]
    url = f"{base}/rest/dataflow/{flow['agency']}/{flow['id']}/all?detail=allstubs"
    throttle()
    try:
        r = sess.get(url, headers=HDR_XML, timeout=(CONNECT_TIMEOUT, 120))
        r.raise_for_status()
        root = ET.fromstring(r.content)
    except Exception:
        return [flow["version"]]
    vers = set()
    for df in root.iter():
        if df.tag.split("}")[-1] == "Dataflow" and df.get("id") == flow["id"]:
            v = df.get("version")
            if v:
                vers.add(v)
    vers.add(flow["version"])

    def vkey(s):
        out = []
        for part in str(s).split("."):
            try:
                out.append(int(part))
            except ValueError:
                out.append(-1)
        return out
    return sorted(vers, key=vkey, reverse=True)


def _pull_all_version(sess, flow, ver, tmp, retries=FULL_RETRIES):
    """Try /all for one explicit version. Returns the stream_full_retrying tuple."""
    f2 = dict(flow)
    f2["version"] = ver
    return stream_full_retrying(sess, data_url_all(f2), tmp, FULL_TIMEOUT, retries=retries)


def process_flow(sess, flow, dry=False):
    """Returns dict: {status, n_obs, n_series, mode, note, version}.
    Path A: full /all (with retry/backoff + version fallback). Most flows.
    Path B: REF_AREA split, only when /all genuinely fails (500/timeout/persistent
    non-structural 404). Path C (nested): COUNTERPART_AREA split for giant reporters.
    """
    dest = os.path.join(OUT, flow_filename(flow))
    tmp = os.path.join(TMP, flow_filename(flow).replace(".parquet", ".csv"))

    # ---- Path A: full /all with retry, then version fallback on NoRecordsFound ----
    url = data_url_all(flow)
    status, nbytes, ok, body, structural_empty, no_records = stream_full_retrying(
        sess, url, tmp, FULL_TIMEOUT)
    used_ver = flow["version"]

    if structural_empty:
        _safe_rm(tmp)
        return {"status": "empty", "n_obs": 0, "n_series": 0, "mode": "all",
                "note": f"no data (HTTP 404: {body[:80]})", "version": used_ver}

    if no_records and not ok and VERSION_FALLBACK:
        # the advertised version is empty -> try the other published versions.
        # DISABLED by default in the bulk pass (it is quota-expensive and can tar-pit);
        # run a targeted second pass with --version-fallback over the 'empty' flows.
        try:
            vers = get_versions(sess, flow)
        except Exception:
            vers = [flow["version"]]
        others = [v for v in vers if v != flow["version"]][:3]  # newest 3 others
        for v in others:
            st2, nb2, ok2, body2, se2, nr2 = _pull_all_version(
                sess, flow, v, tmp, retries=2)
            if ok2:
                ok, status, body, used_ver = True, st2, body2, v
                break
            if se2:
                _safe_rm(tmp)
                return {"status": "empty", "n_obs": 0, "n_series": 0, "mode": "all",
                        "note": f"no data across versions {vers}", "version": v}

    if ok:
        if dry:
            n = _count_csv_rows(tmp)
            _safe_rm(tmp)
            return {"status": "ok", "n_obs": n, "n_series": 0, "mode": "all",
                    "note": "", "version": used_ver}
        sink = BatchWriter(dest)
        try:
            sink.add_from_csv(tmp)
            nobs, nser = sink.finalize()
        except Exception:
            sink.abort()
            raise
        finally:
            _safe_rm(tmp)
        note = ""
        if used_ver != flow["version"]:
            note = f"version fallback {flow['version']}->{used_ver}"
        if sink.series_capped:
            note = (note + "; series>=cap (approx)").lstrip("; ")
        return {"status": "full", "n_obs": nobs, "n_series": nser, "mode": "all",
                "note": note, "version": used_ver}

    if no_records:
        # genuinely empty across all versions tried
        _safe_rm(tmp)
        return {"status": "empty", "n_obs": 0, "n_series": 0, "mode": "all",
                "note": "NoRecordsFound (all versions)", "version": used_ver}

    # /all failed (after retries) and not a structural-empty -> Path B
    note_all = f"/all failed status={status} ({body[:60]})"

    try:
        dims = get_dim_order(sess, flow)
    except Exception as e:
        return {"status": "failed", "n_obs": 0, "n_series": 0, "mode": "split",
                "note": f"{note_all}; dim-order fetch failed: {e}"}

    if "REF_AREA" not in dims:
        return {"status": "failed", "n_obs": 0, "n_series": 0, "mode": "split",
                "note": note_all + "; no REF_AREA dimension to split on"}

    ra_pos = dims.index("REF_AREA")
    try:
        areas = get_ref_areas(sess, flow)
    except Exception as e:
        return {"status": "failed", "n_obs": 0, "n_series": 0, "mode": "split",
                "note": note_all + f"; availability failed: {e}"}

    if not areas:
        return {"status": "failed", "n_obs": 0, "n_series": 0, "mode": "split",
                "note": note_all + "; no REF_AREA values listed"}

    if dry:
        return {"status": "would-split", "n_obs": 0, "n_series": 0, "mode": "split",
                "note": f"{note_all}; would split into {len(areas)} REF_AREA calls"}

    sink = BatchWriter(dest)
    n_ok = n_fail = 0
    try:
        for area in areas:
            okk, sub_fail = _fetch_one_area(sess, flow, dims, ra_pos, area, sink, tmp)
            if okk:
                n_ok += 1
            else:
                n_fail += 1
        nobs, nser = sink.finalize()
    except Exception:
        sink.abort()
        raise

    if n_fail == 0:
        return {"status": "full", "n_obs": nobs, "n_series": nser, "mode": "split",
                "note": f"REF_AREA split: {n_ok}/{len(areas)} areas"}
    return {"status": "partial", "n_obs": nobs, "n_series": nser, "mode": "split",
            "note": f"REF_AREA split: {n_ok} ok / {n_fail} failed of {len(areas)}"}


def _count_csv_rows(path):
    n = 0
    with open(path, "r", encoding="utf-8", newline="") as f:
        next(f, None)
        for _ in f:
            n += 1
    return n


def _safe_rm(p):
    try:
        os.remove(p)
    except OSError:
        pass


def report_disk():
    """Scan every Parquet in OUT and print the TRUE totals (independent of run logs)."""
    import glob
    files = sorted(glob.glob(os.path.join(OUT, "*.parquet")))
    total_obs = 0
    total_series = 0
    total_bytes = 0
    biggest = []
    for f in files:
        try:
            pf = pq.ParquetFile(f)
            nrows = pf.metadata.num_rows
        except Exception as e:  # noqa: BLE001
            log(f"  [bad parquet] {os.path.basename(f)}: {e}")
            continue
        # distinct series via a single column scan
        try:
            import pyarrow.compute as pc
            col = pf.read(columns=["series_key"])["series_key"]
            nser = pc.count_distinct(col).as_py()
        except Exception:
            nser = 0
        sz = os.path.getsize(f)
        total_obs += nrows
        total_series += nser
        total_bytes += sz
        biggest.append((nrows, os.path.basename(f)))
    log(f"[report] parquet files : {len(files)}")
    log(f"[report] observations  : {total_obs:,}")
    log(f"[report] distinct series (summed per-file): {total_series:,}")
    log(f"[report] on-disk size   : {total_bytes/1e9:.2f} GB")
    log("[report] 12 biggest files:")
    for n, name in sorted(biggest, reverse=True)[:12]:
        log(f"           {n:>13,}  {name}")
    return len(files), total_obs, total_series


# ----------------------------------------------------------------------------- driver
def main():
    args = sys.argv[1:]
    sess = make_session()

    if "--catalog" in args:
        download_catalog(sess)
        return

    if "--report" in args:
        report_disk()
        return

    dry = "--dry" in args
    dry_n = int(args[args.index("--dry") + 1]) if dry else None
    workers = int(args[args.index("--workers") + 1]) if "--workers" in args else 2
    only = args[args.index("--only") + 1] if "--only" in args else None
    start_at = int(args[args.index("--start") + 1]) if "--start" in args else 0
    if "--version-fallback" in args:
        global VERSION_FALLBACK
        VERSION_FALLBACK = True

    # refresh catalog if missing
    if not os.path.exists(os.path.join(RAW, "manifest.json")):
        download_catalog(sess)
    flows = load_manifest()

    if only:
        flows = [f for f in flows if f["id"] == only or f["id"].endswith(only)]
    if start_at:
        flows = flows[start_at:]
    if dry and dry_n:
        flows = flows[:dry_n]

    log(f"{'DRY-RUN' if dry else 'FULL'}: {len(flows)} dataflows, workers={workers}")

    results = []
    results_path = os.path.join(RAW, "ingest_results.jsonl")
    done_ids = set()
    # resume: skip flows whose parquet already exists (and is non-trivial) unless dry
    if not dry and "--no-resume" not in args:
        for f in flows:
            dest = os.path.join(OUT, flow_filename(f))
            if os.path.exists(dest) and os.path.getsize(dest) > 0:
                done_ids.add(f["id"])
        if done_ids:
            log(f"[resume] {len(done_ids)} dataflows already have parquet -> skipping")
    todo = [f for f in flows if f["id"] not in done_ids]

    # Order: do the cheap bulk FIRST so coverage builds fast; defer the known-giant /
    # split-prone flows (downscaled grids, bilateral trade, externals on other roots)
    # to the end where a few slow pulls won't starve the rest.
    HEAVY = ("DDOWN", "BIMTS", "_6D", "_4D", "TIVA", "DF_TRADE", "BTDIXE")

    def weight(f):
        fid = f["id"].upper()
        w = 0
        if any(k in fid for k in HEAVY):
            w += 100
        if f.get("external"):
            w += 50
        return w
    if not only:
        todo.sort(key=weight)

    t0 = time.time()
    n_done = 0
    n_obs_total = 0
    n_series_total = 0
    lock = threading.Lock()

    verbose = "--verbose" in args

    def work(flow):
        s = make_session()
        tw = time.time()
        if verbose:
            log(f"    -> START {flow['agency']}:{flow['id']} (v{flow['version']})")
        try:
            res = process_flow(s, flow, dry=dry)
        except Exception as e:  # noqa: BLE001
            res = {"status": "error", "n_obs": 0, "n_series": 0, "mode": "?",
                   "note": f"exception: {type(e).__name__}: {e}"}
        res["flow"] = f"{flow['agency']}:{flow['id']}"
        res["secs"] = round(time.time() - tw, 1)
        if verbose:
            log(f"    <- DONE  {flow['agency']}:{flow['id']} {res['status']} "
                f"{res['n_obs']:,} obs in {res['secs']}s")
        return res

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(work, f): f for f in todo}
        rf = None if dry else open(results_path, "a", encoding="utf-8")
        for fut in as_completed(futs):
            res = fut.result()
            with lock:
                n_done += 1
                n_obs_total += res["n_obs"]
                n_series_total += res["n_series"]
                results.append(res)
                if rf:
                    rf.write(json.dumps(res, ensure_ascii=False) + "\n")
                    rf.flush()
                if res["status"] not in ("ok", "full") or res["note"]:
                    log(f"  [{n_done}/{len(todo)}] {res['flow']:55} "
                        f"{res['status']:10} obs={res['n_obs']:>10,} "
                        f"ser={res['n_series']:>7,} {res['mode']:6} {res['note']}")
                elif n_done % 25 == 0:
                    el = time.time() - t0
                    log(f"  [{n_done}/{len(todo)}] ... {n_obs_total:,} obs so far "
                        f"({el:.0f}s)")
        if rf:
            rf.close()

    el = time.time() - t0
    # summary
    from collections import Counter
    by_status = Counter(r["status"] for r in results)
    log("")
    log(f"{'DRY' if dry else 'DONE'} in {el:.0f}s")
    log(f"  dataflows processed this run: {n_done}")
    log(f"  observations written this run: {n_obs_total:,}")
    log(f"  series written this run:       {n_series_total:,}")
    log(f"  status breakdown: {dict(by_status)}")


if __name__ == "__main__":
    main()
