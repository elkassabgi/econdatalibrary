#!/usr/bin/env python3
"""FULL-COVERAGE grouped ingest of the entire ECB Data Portal (SDMX 2.1).

Source: ECB Data Portal (data-api.ecb.europa.eu). license_id = ecb-attrib-nomodify
(from configs/sources.yaml: "Source: ECB -- available free at the ECB; values not
modified"). We do NOT modify values; the Clean tier stores the raw observation.

Strategy (verified live against data-api.ecb.europa.eu, June 2026):
  * Catalog = EVERY dataflow from /service/dataflow  (213 flows across agencies
    ECB, ECB.DISS, ESTAT, EUROSTAT, IMF -- ALL hosted at the ECB endpoint).
  * Data pulled as SDMX-CSV (?format=csvdata) with &detail=dataonly, which strips
    the ~25 bulky attribute columns (TITLE, OBS_COM, COLLECTION, ...) WITHOUT
    changing the observation count and is ~10-30x faster to transfer. The CSV
    header is then:  KEY, <dim1..dimN>, TIME_PERIOD, OBS_VALUE.
  * SERIES KEY = the CSV `KEY` column, which the ECB already emits pre-joined
    (e.g. "EXR.D.USD.EUR.SP00.A") -> stored verbatim as column `series_key`.
    FREQ (when present as a column) -> stored as `freq`.
  * Most flows pull whole in one request. The handful of GIANTS (EXR ~197MB,
    MIR ~124MB, YC ~11M rows, BSI, ICP/HICP, STS, SEC, IRS, FM, BOP, RPP, ...)
    are FREQUENCY-CHUNKED via the SDMX positional path key (the DSD dimension
    order, FREQ value + dots for the rest, e.g. EXR key "D.....").  If a single
    frequency chunk is still huge/fails, it is SUB-SPLIT by the next splitting
    dimension's codelist values (e.g. YC daily -> by INSTRUMENT_FM curve type).
    Each chunk is an independent, restart-safe request that lands in its own
    parquet part, so a process kill costs at most one chunk.

OUTPUT: grouped Parquet under data/clean_full/ecb/ ->
    <AGENCY>__<FLOW>.parquet                      (whole-flow pulls)
    <AGENCY>__<FLOW>__<FREQ>.parquet              (frequency chunks)
    <AGENCY>__<FLOW>__<FREQ>__<VAL>.parquet       (sub-split chunks)
  columns: series_key (str), obs_date (date32), value (float64), freq (str).
A sidecar _manifest.json records per-flow stats (NOT catalog.db, which is off-limits).

Memory is bounded: every HTTP body streams to a temp .csv.gz on disk, is parsed
row-by-row, and flushed to the Parquet writer in RecordBatches; no flow is held
whole in RAM. The distinct-series set per file is capped to bound memory.

Usage:
  python jobs/ingest_ecb.py --list                 # enumerate catalog, no data
  python jobs/ingest_ecb.py --only EXR,YC          # just these flows
  python jobs/ingest_ecb.py --workers 4            # FULL run, 4 concurrent flows
  python jobs/ingest_ecb.py --report               # re-read parquet, print true totals
  python jobs/ingest_ecb.py                        # FULL run (default workers=4)
"""
from __future__ import annotations

import csv
import datetime as dt
import gzip
import io
import json
import os
import re
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = r"D:/research/econfindatalibrary"
sys.path.insert(0, ROOT)

OUT = os.path.join(ROOT, "data", "clean_full", "ecb")
TMPDIR = os.path.join(ROOT, "data", "raw", "ecb", "_tmp")
MANIFEST = os.path.join(OUT, "_manifest.json")
CATALOG = os.path.join(OUT, "_catalog.json")
os.makedirs(OUT, exist_ok=True)
os.makedirs(TMPDIR, exist_ok=True)

UA = "Econ-Fin Data Library admin@hfdatalibrary.com"
BASE = "https://data-api.ecb.europa.eu/service"
LICENSE_ID = "ecb-attrib-nomodify"
STRUCT_MIME = "application/vnd.sdmx.structure+xml;version=2.1"

NS = {
    "mes": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
    "str": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
    "com": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
}

# Flows known/expected to be very large -> frequency-chunk them up front (avoids a
# 100-250MB single stream that a process kill would force us to restart from zero).
# (Sizes/observations confirmed or strongly indicated by live probing June 2026.)
GIANTS = {
    "EXR", "YC", "MIR", "BSI", "BSP", "ICP", "HICP", "STS", "SEC", "IRS", "FM",
    "BOP", "RPP", "IVF", "GST", "RTD", "MNA", "QSA", "CSEC", "BLS", "CBD2", "SHS", "SUP",
    "FVC", "OFI", "ICO", "ICB", "MMSR", "EMMS", "CES", "SAFE", "TRD", "PSS",
    "ESA", "DD", "SHI", "RESR", "SSI", "AME", "MPD", "EST", "EON",
    "BP6", "BPS", "RA6", "RAS", "ENA", "EDP", "GFS", "LCI", "JVS", "LFSI",
    "IEAQ", "IEAF", "ICPF", "IESS", "QSA_PUB", "MNA_PUB", "BSI_PUB", "EXR_PUB",
    "MIR_PUB", "ICP_PUB", "STS_PUB", "SEC_PUB", "FM_PUB", "YC_PUB", "GFS_PUB",
}

# MEGA = flows whose single-FREQUENCY chunk is itself enormous (hundreds of MB to
# >1GB). These skip the doomed whole-freq attempt and split DIRECTLY by
# freq x <named split dimension>. Value = the DSD dimension id to sub-split on
# (chosen low-cardinality so request count stays small). Verified live:
#   YC  whole = 1.23 GB / 43 min  -> split by INSTRUMENT_FM (only 3 curve types in
#                                    the data, so 3 freq-x-curve sub-chunks).
# (EXR at 197MB/5min and MIR at 124MB/32s are handled fine by plain freq-chunking
#  and a generous CHUNK_TIMEOUT, so they are NOT in MEGA.)
MEGA = {
    "YC": "INSTRUMENT_FM",
    # QSA = Quarterly Sector Accounts: 372k series, ~all quarterly, so the single Q
    # freq chunk is tens of millions of rows and never finishes before the host's
    # process reaper fires. Split by REF_AREA (33 reporters) -> ~11k series/chunk.
    "QSA": "REF_AREA",
    # CSEC = 738k series, ALL monthly (NA_SEC cube). Same problem as QSA; split by
    # REF_AREA (34 reporters).
    "CSEC": "REF_AREA",
}

# Flows whose per-(freq x split-dim) chunk is STILL too large to fetch in one stream
# (e.g. CSEC: a single country's monthly history is hundreds of MB and streams so
# slowly it never finishes before the process reaper). For these we go straight to a
# THIRD level -- split each (freq x split-value) by individual YEAR -- so every
# request is ~tens of MB and restart-safe. Value = the inclusive (first, last) year
# range the cube actually spans (verified live; probing far outside wastes requests
# because empty years return a slow 200-empty, not a fast 404).
FORCE_YEAR = {
    "CSEC": (2021, 2027),   # NA_SEC monthly cube; data starts 2021 (verified June 2026)
}

# Frequency code candidates ECB uses, ordered most->least common.
FREQ_CANDIDATES = ("D", "B", "M", "Q", "A", "W", "H", "S", "N", "E")

# Per-flow single-request timeout. ECB serves ~0.5-1MB/s; the biggest whole flows can
# run tens of minutes. Give the whole-flow attempt generous headroom; regular freq
# chunks a shorter cap; MEGA sub-chunks (still up to ~600MB) need a long cap.
FULL_TIMEOUT = 1800
CHUNK_TIMEOUT = 900
MEGA_CHUNK_TIMEOUT = 1500
# When a single (sub-)chunk still fails, split it by calendar decade via
# startPeriod/endPeriod (confirmed supported by the ECB API). These bound ANY series.
DECADES = [("1990-01-01", "1999-12-31"), ("2000-01-01", "2009-12-31"),
           ("2010-01-01", "2019-12-31"), ("2020-01-01", "2039-12-31"),
           ("1970-01-01", "1989-12-31")]
CONNECT_RETRIES = 5

csv.field_size_limit(min(sys.maxsize, 2_147_483_647))
_print_lock = threading.Lock()
_dsd_cache: dict[str, list[str]] = {}
_dsd_lock = threading.Lock()


def log(msg):
    with _print_lock:
        print(msg, flush=True)


# --------------------------------------------------------------------------- #
# Global throttle: space request STARTS so concurrent workers stay polite.
# --------------------------------------------------------------------------- #
MIN_INTERVAL = float(os.environ.get("ECB_MIN_INTERVAL", "0.6"))
_gate_lock = threading.Lock()
_next_allowed = [0.0]
_cooldown_until = [0.0]


def throttle():
    with _gate_lock:
        now = time.time()
        start = max(now, _next_allowed[0], _cooldown_until[0])
        _next_allowed[0] = start + MIN_INTERVAL
    wait = start - time.time()
    if wait > 0:
        time.sleep(wait)


def trip_cooldown(seconds):
    with _gate_lock:
        _cooldown_until[0] = max(_cooldown_until[0], time.time() + seconds)


# --------------------------------------------------------------------------- #
# HTTP helpers
# --------------------------------------------------------------------------- #
def http_get_bytes(url: str, accept: str | None = None, timeout: int = 180) -> bytes:
    """GET small structural responses fully into memory, with retry/backoff."""
    headers = {"User-Agent": UA, "Accept-Encoding": "gzip, deflate"}
    if accept:
        headers["Accept"] = accept
    last = None
    for attempt in range(CONNECT_RETRIES):
        throttle()
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                data = r.read()
                enc = (r.headers.get("Content-Encoding") or "").lower()
                if enc == "gzip":
                    data = gzip.decompress(data)
                elif enc == "deflate":
                    import zlib
                    data = zlib.decompress(data)
                return data
        except urllib.error.HTTPError as e:
            if e.code in (400, 404):
                raise
            last = e
            if e.code == 429:
                trip_cooldown(20.0)
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
            last = e
        time.sleep(3 * (attempt + 1))
    if last:
        raise last
    raise RuntimeError("unreachable")


def stream_query_to_tmp(path_key_url: str, timeout: int):
    """Stream one SDMX-CSV query to a temp .csv.gz. Returns (tmp_path, dl_bytes).

    Decompresses any transport gzip/deflate on the fly, re-compresses to the temp
    gz so the on-disk temp is always plain gzipped CSV regardless of transport.
    Raises HTTPError(400/404) for structural 'no data' so callers can mark empty.
    """
    headers = {"User-Agent": UA, "Accept-Encoding": "gzip, deflate"}
    last = None
    for attempt in range(CONNECT_RETRIES):
        throttle()
        try:
            req = urllib.request.Request(path_key_url, headers=headers)
            r = urllib.request.urlopen(req, timeout=timeout)
            enc = (r.headers.get("Content-Encoding") or "").lower()
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv.gz", dir=TMPDIR)
            nbytes = 0
            decompressor = None
            if enc == "gzip":
                import zlib
                decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
            elif enc == "deflate":
                import zlib
                decompressor = zlib.decompressobj()
            try:
                with gzip.open(tmp, "wb") as gz:
                    while True:
                        chunk = r.read(1 << 20)
                        if not chunk:
                            break
                        nbytes += len(chunk)
                        if decompressor is not None:
                            chunk = decompressor.decompress(chunk)
                        gz.write(chunk)
                    if decompressor is not None:
                        tail = decompressor.flush()
                        if tail:
                            gz.write(tail)
            finally:
                tmp.close()
                r.close()
            return tmp.name, nbytes
        except urllib.error.HTTPError as e:
            if e.code in (400, 404):
                raise
            last = e
            if e.code == 429:
                trip_cooldown(20.0)
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
            last = e
        time.sleep(4 * (attempt + 1))
    if last:
        raise last
    raise RuntimeError("unreachable")


# --------------------------------------------------------------------------- #
# Catalog + DSD structure
# --------------------------------------------------------------------------- #
def fetch_dataflows() -> list[dict]:
    """Enumerate EVERY dataflow at the ECB endpoint."""
    raw = http_get_bytes(f"{BASE}/dataflow", STRUCT_MIME, timeout=120)
    root = ET.fromstring(raw)
    out = []
    for f in root.findall(".//str:Dataflow", NS):
        did = f.get("id")
        nm = f.find("com:Name", NS)
        ref = f.find("str:Structure/Ref", NS)
        out.append({
            "agency": f.get("agencyID"),
            "id": did,
            "version": f.get("version") or "1.0",
            "dsd": ref.get("id") if ref is not None else None,
            "dsd_agency": ref.get("agencyID") if ref is not None else None,
            "dsd_version": ref.get("version") if ref is not None else None,
            "name": (nm.text if nm is not None else "") or "",
        })
    return out


def fetch_dim_order(flow: dict) -> list[str] | None:
    """Return the DSD dimension ids in key/position order (cached per DSD).
    TIME dimensions are excluded (they are not part of the series path key)."""
    dsd = flow.get("dsd")
    if not dsd:
        return None
    with _dsd_lock:
        if dsd in _dsd_cache:
            return _dsd_cache[dsd]
    agency = flow.get("dsd_agency") or "ECB"
    ver = flow.get("dsd_version") or "1.0"
    url = f"{BASE}/datastructure/{agency}/{dsd}/{ver}?references=none"
    try:
        raw = http_get_bytes(url, STRUCT_MIME, timeout=120)
        root = ET.fromstring(raw)
    except Exception:
        # try without explicit version
        try:
            url = f"{BASE}/datastructure/{agency}/{dsd}?references=none"
            raw = http_get_bytes(url, STRUCT_MIME, timeout=120)
            root = ET.fromstring(raw)
        except Exception:
            return None
    dims = []
    dl = root.find(".//str:DimensionList", NS)
    if dl is None:
        return None
    for dim in dl.findall("str:Dimension", NS):
        pos = dim.get("position")
        dims.append((int(pos) if pos else 999, dim.get("id")))
    dims.sort()
    ids = [d for _, d in dims]
    with _dsd_lock:
        _dsd_cache[dsd] = ids
    return ids


def fetch_used_dim_values(flow: dict, dim_index: int) -> list[str] | None:
    """Return the dimension values ACTUALLY PRESENT in the flow at key position
    dim_index, by pulling the (cheap) series-keys-only listing. Far better than the
    full codelist for sub-splitting MEGA flows: YC's INSTRUMENT_FM codelist has 257
    codes but the data uses only 3, so this yields 3 sub-chunks not 257.

    The serieskeysonly CSV `KEY` column is dataflow_id + the N dim values joined by
    '.', so the value for DSD dim i sits at split index i+1.
    Returns None on failure."""
    url = url_all(flow, detail="serieskeysonly")
    try:
        raw = http_get_bytes(url, timeout=300)
    except Exception:
        return None
    seen = []
    seenset = set()
    text = raw.decode("utf-8-sig", "replace")
    first = True
    for line in text.splitlines():
        if first:
            first = False
            continue
        if not line:
            continue
        key = line.split(",", 1)[0]
        parts = key.split(".")
        # parts[0] = dataflow id; DSD dim i -> parts[i+1]
        pi = dim_index + 1
        if pi < len(parts):
            v = parts[pi]
            if v and v not in seenset:
                seenset.add(v)
                seen.append(v)
    return seen or None


def fetch_codelist_values(flow: dict, dim_index: int) -> list[str] | None:
    """Return the codelist code ids for the dimension at position dim_index, via the
    DSD's referenced codelists. Used to sub-split a too-big frequency chunk.
    Returns None on any failure (caller then skips sub-splitting)."""
    dsd = flow.get("dsd")
    if not dsd:
        return None
    agency = flow.get("dsd_agency") or "ECB"
    ver = flow.get("dsd_version") or "1.0"
    url = f"{BASE}/datastructure/{agency}/{dsd}/{ver}?references=children"
    try:
        raw = http_get_bytes(url, STRUCT_MIME, timeout=180)
        root = ET.fromstring(raw)
    except Exception:
        return None
    # map dimension position -> codelist id
    dl = root.find(".//str:DimensionList", NS)
    if dl is None:
        return None
    dims = []
    for dim in dl.findall("str:Dimension", NS):
        pos = dim.get("position")
        cl_ref = dim.find("str:LocalRepresentation/str:Enumeration/Ref", NS)
        cl_id = cl_ref.get("id") if cl_ref is not None else None
        dims.append((int(pos) if pos else 999, cl_id))
    dims.sort()
    if dim_index >= len(dims):
        return None
    want_cl = dims[dim_index][1]
    if not want_cl:
        return None
    for cl in root.findall(".//str:Codelist", NS):
        if cl.get("id") == want_cl:
            vals = [c.get("id") for c in cl.findall("str:Code", NS) if c.get("id")]
            return vals or None
    return None


# --------------------------------------------------------------------------- #
# TIME_PERIOD parsing
# --------------------------------------------------------------------------- #
_Q = re.compile(r"^(\d{4})-Q([1-4])$")
_M = re.compile(r"^(\d{4})-(\d{2})$")
_D = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
_S = re.compile(r"^(\d{4})-S([12])$")
_H = re.compile(r"^(\d{4})-H([12])$")
_W = re.compile(r"^(\d{4})-W(\d{2})$")
_Y = re.compile(r"^(\d{4})$")


def parse_period(p: str):
    p = (p or "").strip()
    if not p:
        return None
    m = _D.match(p)
    if m:
        try:
            return dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None
    m = _M.match(p)
    if m:
        try:
            return dt.date(int(m.group(1)), int(m.group(2)), 1)
        except ValueError:
            return None
    m = _Q.match(p)
    if m:
        return dt.date(int(m.group(1)), (int(m.group(2)) - 1) * 3 + 1, 1)
    m = _Y.match(p)
    if m:
        return dt.date(int(m.group(1)), 12, 31)
    m = _S.match(p)
    if m:
        return dt.date(int(m.group(1)), 1 if m.group(2) == "1" else 7, 1)
    m = _H.match(p)
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
    if s is None or s == "":
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


# --------------------------------------------------------------------------- #
# Parquet writer (bounded memory, grouped)
# --------------------------------------------------------------------------- #
SCHEMA = pa.schema([
    ("series_key", pa.string()),
    ("obs_date", pa.date32()),
    ("value", pa.float64()),
    ("freq", pa.string()),
])
BATCH = 250_000
SERIES_CAP = 3_000_000


class BatchWriter:
    """Streams one or more SDMX-CSV temp files into ONE grouped parquet, flushing a
    row-group every BATCH rows. Peak RAM ~= one batch + the distinct-series set
    (capped). Writes to .part then atomically renames in finalize()."""

    def __init__(self, dest):
        self.dest = dest
        self.part = dest + ".part"
        self.writer = None
        self.n_obs = 0
        self.series = set()
        self.capped = False
        self.bad = 0
        self.dmin = None
        self.dmax = None
        self.freqs = set()
        self._reset()

    def _reset(self):
        self.bk, self.bd, self.bv, self.bf = [], [], [], []

    def _flush(self):
        if not self.bk:
            return
        if self.writer is None:
            self.writer = pq.ParquetWriter(self.part, SCHEMA, compression="zstd")
        batch = pa.record_batch([
            pa.array(self.bk, pa.string()),
            pa.array(self.bd, pa.date32()),
            pa.array(self.bv, pa.float64()),
            pa.array(self.bf, pa.string()),
        ], schema=SCHEMA)
        self.writer.write_batch(batch)
        self._reset()

    def add_from_tmp(self, tmp_path: str) -> int:
        """Parse one gzipped SDMX-CSV temp file. Returns rows added."""
        n = 0
        with gzip.open(tmp_path, "rt", encoding="utf-8-sig", newline="") as fh:
            rd = csv.reader(fh)
            hdr = next(rd, None)
            if not hdr:
                return 0
            if "TIME_PERIOD" not in hdr or "OBS_VALUE" not in hdr:
                return 0
            ki = hdr.index("KEY") if "KEY" in hdr else 0
            tpi = hdr.index("TIME_PERIOD")
            ovi = hdr.index("OBS_VALUE")
            fri = hdr.index("FREQ") if "FREQ" in hdr else (
                hdr.index("FREQUENCY") if "FREQUENCY" in hdr else -1)
            bk, bd, bv, bf = self.bk, self.bd, self.bv, self.bf
            seri = self.series
            for row in rd:
                if len(row) <= tpi or len(row) <= ovi:
                    continue
                val = to_float(row[ovi])
                if val is None:
                    continue
                od = parse_period(row[tpi])
                if od is None:
                    self.bad += 1
                    continue
                key = row[ki] if ki < len(row) else ""
                fr = row[fri] if (fri >= 0 and fri < len(row)) else ""
                bk.append(key)
                bd.append(od)
                bv.append(val)
                bf.append(fr)
                if not self.capped:
                    seri.add(key)
                    if len(seri) >= SERIES_CAP:
                        self.capped = True
                if fr:
                    self.freqs.add(fr)
                if self.dmin is None or od < self.dmin:
                    self.dmin = od
                if self.dmax is None or od > self.dmax:
                    self.dmax = od
                n += 1
                if len(bk) >= BATCH:
                    self._flush()
                    bk, bd, bv, bf = self.bk, self.bd, self.bv, self.bf
        self.n_obs += n
        return n

    def finalize(self):
        self._flush()
        if self.writer is None:
            _safe_rm(self.part)
            return 0, 0
        self.writer.close()
        self.writer = None
        try:
            if os.path.exists(self.dest):
                os.remove(self.dest)
        except OSError:
            pass
        os.replace(self.part, self.dest)
        return self.n_obs, len(self.series)

    def abort(self):
        try:
            if self.writer is not None:
                self.writer.close()
        except Exception:
            pass
        _safe_rm(self.part)


def _safe_rm(p):
    try:
        os.remove(p)
    except OSError:
        pass


# --------------------------------------------------------------------------- #
# URL builders
# --------------------------------------------------------------------------- #
def url_all(flow: dict, detail="dataonly") -> str:
    sel = flow["id"]
    return f"{BASE}/data/{flow['agency']},{sel},{flow['version']}?format=csvdata&detail={detail}"


def url_key(flow: dict, key: str, detail="dataonly", start=None, end=None) -> str:
    u = (f"{BASE}/data/{flow['agency']},{flow['id']},{flow['version']}/{key}"
         f"?format=csvdata&detail={detail}")
    if start:
        u += f"&startPeriod={start}"
    if end:
        u += f"&endPeriod={end}"
    return u


# --------------------------------------------------------------------------- #
# Ingest one flow
# --------------------------------------------------------------------------- #
def fname(flow: dict, suffix: str = "") -> str:
    safe = (flow["agency"] + "__" + flow["id"]).replace("/", "_").replace(":", "_")
    if suffix:
        safe += "__" + suffix.replace("/", "_").replace(":", "_").replace(".", "_")
    return safe + ".parquet"


def parts_exist(flow: dict) -> bool:
    """True if a whole-flow parquet OR any chunk part already exists."""
    import glob
    base = (flow["agency"] + "__" + flow["id"]).replace("/", "_").replace(":", "_")
    if os.path.exists(os.path.join(OUT, base + ".parquet")):
        return True
    return bool(glob.glob(os.path.join(OUT, base + "__*.parquet")))


def flow_has_series(flow: dict, attempts: int = 4) -> bool | None:
    """Verify whether a flow ACTUALLY has data, via the cheap series-keys listing,
    retrying a few times. The ECB API intermittently returns HTTP 404 on the full
    data pull under load; a bare 404 must therefore NOT be trusted as 'empty' until
    series-keys-only ALSO consistently 404s. Returns True (has >=1 series),
    False (consistently no data), or None (couldn't decide / network error)."""
    saw_404 = 0
    for i in range(attempts):
        try:
            raw = http_get_bytes(url_all(flow, detail="serieskeysonly"), timeout=300)
        except urllib.error.HTTPError as e:
            if e.code in (400, 404):
                saw_404 += 1
                if saw_404 >= 2:
                    return False  # consistently structural-empty
                time.sleep(2 * (i + 1))
                continue
            time.sleep(3 * (i + 1))
            continue
        except Exception:  # noqa: BLE001
            time.sleep(3 * (i + 1))
            continue
        # got a 200 body: any data rows?
        txt = raw.decode("utf-8-sig", "replace")
        nl = txt.find("\n")
        return nl >= 0 and len(txt) > nl + 1
    return None


def ingest_whole(flow: dict):
    """Pull a whole dataflow in one request -> one parquet. Returns stats dict."""
    dest = os.path.join(OUT, fname(flow))
    if os.path.exists(dest):
        try:
            n = pq.ParquetFile(dest).metadata.num_rows
        except Exception:
            n = 0
        return {"status": "have", "n_obs": n, "n_series": 0, "mode": "whole"}
    t0 = time.time()
    try:
        tmp, dlb = stream_query_to_tmp(url_all(flow), FULL_TIMEOUT)
    except urllib.error.HTTPError as e:
        if e.code in (400, 404):
            # A 404 on the full data pull may be a transient/under-load 404, NOT a
            # truly empty flow. Verify via the cheap series-keys listing before
            # accepting 'empty' -- otherwise we silently drop a populated flow.
            has = flow_has_series(flow)
            if has is True:
                return {"status": "failed", "n_obs": 0, "n_series": 0, "mode": "whole",
                        "note": f"HTTP {e.code} on data but flow HAS series "
                                f"(transient 404) -> needs chunking"}
            if has is False:
                return {"status": "empty", "n_obs": 0, "n_series": 0, "mode": "whole",
                        "note": f"HTTP {e.code} (verified no series)"}
            return {"status": "failed", "n_obs": 0, "n_series": 0, "mode": "whole",
                    "note": f"HTTP {e.code} (emptiness unverified)"}
        return {"status": "failed", "n_obs": 0, "n_series": 0, "mode": "whole",
                "note": f"HTTP {e.code}"}
    except Exception as e:  # noqa: BLE001
        return {"status": "failed", "n_obs": 0, "n_series": 0, "mode": "whole",
                "note": f"{type(e).__name__}: {e}"}
    w = BatchWriter(dest)
    try:
        w.add_from_tmp(tmp)
        nobs, nser = w.finalize()
    except Exception as e:  # noqa: BLE001
        w.abort()
        _safe_rm(tmp)
        return {"status": "failed", "n_obs": 0, "n_series": 0, "mode": "whole",
                "note": f"parse {type(e).__name__}: {e}"}
    _safe_rm(tmp)
    if nobs == 0:
        return {"status": "empty", "n_obs": 0, "n_series": 0, "mode": "whole",
                "note": "no parseable observations"}
    return {"status": "full", "n_obs": nobs, "n_series": nser, "mode": "whole",
            "start": str(w.dmin), "end": str(w.dmax), "freqs": sorted(w.freqs),
            "bytes": os.path.getsize(dest), "dl_bytes": dlb,
            "dur_s": round(time.time() - t0, 1)}


def _pull_chunk(flow, key, suffix, acc, start=None, end=None, timeout=CHUNK_TIMEOUT):
    """Pull ONE positional-key chunk into its own parquet part. Resumes if it exists.
    Returns 'have'|'ok'|'empty'|'err'."""
    dest = os.path.join(OUT, fname(flow, suffix))
    if os.path.exists(dest):
        try:
            acc["obs"] += pq.ParquetFile(dest).metadata.num_rows
        except Exception:
            pass
        return "have"
    try:
        tmp, _ = stream_query_to_tmp(url_key(flow, key, start=start, end=end), timeout)
    except urllib.error.HTTPError as e:
        return "empty" if e.code in (400, 404) else "err"
    except Exception:  # noqa: BLE001
        return "err"
    w = BatchWriter(dest)
    try:
        w.add_from_tmp(tmp)
        nobs, nser = w.finalize()
    except Exception:  # noqa: BLE001
        w.abort()
        _safe_rm(tmp)
        return "err"
    _safe_rm(tmp)
    if nobs == 0:
        return "empty"
    acc["obs"] += nobs
    acc["series"] += nser
    acc["bytes"] += os.path.getsize(dest)
    if acc["start"] is None or (w.dmin and str(w.dmin) < acc["start"]):
        acc["start"] = str(w.dmin)
    if acc["end"] is None or (w.dmax and str(w.dmax) > acc["end"]):
        acc["end"] = str(w.dmax)
    return "ok"


def _pull_chunk_decades(flow, key, suffix_base, acc, timeout):
    """Last-resort: split one positional-key chunk by calendar decade. Returns
    'ok' if any decade landed data, 'empty' if none, 'err' if all errored."""
    any_ok = False
    any_err = False
    for (s, e) in DECADES:
        tag = f"{suffix_base}__{s[:4]}"
        st = _pull_chunk(flow, key, tag, acc, start=s, end=e, timeout=timeout)
        if st in ("ok", "have"):
            any_ok = True
        elif st == "err":
            any_err = True
    if any_ok:
        return "ok"
    return "err" if any_err else "empty"


# Years to probe when YEAR-splitting a chunk. Wide enough to cover any ECB monthly
# cube's span; years with no data return a fast empty (404) and are skipped.
YEAR_SPAN = list(range(1990, 2028))


def _pull_chunk_years(flow, key, suffix_base, acc, timeout, years=YEAR_SPAN):
    """Split one positional-key chunk by individual YEAR (for cubes like CSEC whose
    whole-history-per-key chunk is hundreds of MB even after a dimension split, but
    which only span a handful of years -> each year is ~tens of MB). Restart-safe per
    year. Returns 'ok' if any year landed data, 'empty' if none, 'err' if all errored.

    Optimisation: ECB stores these cubes contiguously in time, so once we have seen
    data we stop after the first run of consecutive empty years (avoids probing 30
    empty years before the data starts)."""
    any_ok = False
    any_err = False
    seen_data = False
    empty_run = 0
    for y in years:
        tag = f"{suffix_base}__{y}"
        st = _pull_chunk(flow, key, tag, acc, start=f"{y}-01-01",
                         end=f"{y}-12-31", timeout=timeout)
        if st in ("ok", "have"):
            any_ok = True
            seen_data = True
            empty_run = 0
        elif st == "err":
            any_err = True
        else:  # empty
            if seen_data:
                empty_run += 1
                if empty_run >= 3:
                    break  # past the end of this cube's data
    if any_ok:
        return "ok"
    return "err" if any_err else "empty"


def ingest_chunked(flow: dict):
    """Frequency-chunk a giant flow; sub-split a too-big freq chunk by its first
    splittable dimension's codelist. Each chunk is restart-safe."""
    t0 = time.time()
    dims = fetch_dim_order(flow)
    if not dims:
        # cannot chunk without dimension order -> fall back to whole
        res = ingest_whole(flow)
        res["mode"] = "whole(fallback:no-dsd)"
        return res
    ndim = len(dims)
    fi = None
    for i, d in enumerate(dims):
        if d in ("FREQ", "FREQUENCY"):
            fi = i
            break
    if fi is None:
        res = ingest_whole(flow)
        res["mode"] = "whole(fallback:no-freq-dim)"
        return res

    acc = {"obs": 0, "series": 0, "bytes": 0, "start": None, "end": None}
    done, empty, err = [], [], []

    # Sub-split dimension. For MEGA flows the split dim is named explicitly; otherwise
    # we lazily pick the FIRST non-freq dim with a small-to-moderate codelist (<=60),
    # used only when a freq chunk actually fails.
    sub_idx = None
    sub_vals = None
    mega_dim = MEGA.get(flow["id"])
    if mega_dim and mega_dim in dims:
        sub_idx = dims.index(mega_dim)
        # use the values ACTUALLY in the data (cheap series-keys listing), not the
        # full codelist, so we issue only as many sub-requests as there are real values
        sub_vals = fetch_used_dim_values(flow, sub_idx) or fetch_codelist_values(flow, sub_idx)

    def ensure_sub():
        nonlocal sub_idx, sub_vals
        if sub_idx is not None or sub_vals is not None:
            return
        for ci in range(ndim):
            if ci == fi:
                continue
            vals = fetch_codelist_values(flow, ci)
            if vals and 2 <= len(vals) <= 60:
                sub_idx, sub_vals = ci, vals
                return
        sub_vals = []  # mark "tried, none usable"

    is_mega = bool(mega_dim and sub_idx is not None and sub_vals)
    mtimeout = MEGA_CHUNK_TIMEOUT if is_mega else CHUNK_TIMEOUT

    # For MEGA flows, restrict the frequency loop to the frequencies actually present
    # (from the cheap series-keys listing), so we don't fire <split-card> empty 404s
    # for every non-existent frequency. Falls back to all candidates if unknown.
    freq_loop = FREQ_CANDIDATES
    if is_mega:
        used_f = fetch_used_dim_values(flow, fi)
        if used_f:
            freq_loop = tuple(f for f in FREQ_CANDIDATES if f in set(used_f))

    for fv in freq_loop:
        parts = ["" for _ in range(ndim)]
        parts[fi] = fv
        if not is_mega:
            # try the whole-frequency chunk first
            st = _pull_chunk(flow, ".".join(parts), fv, acc, timeout=CHUNK_TIMEOUT)
            if st in ("ok", "have"):
                done.append(fv)
                continue
            if st == "empty":
                empty.append(fv)
                continue
            # st == 'err' -> too big / failed; sub-split it
            ensure_sub()
            if not sub_vals:
                # no split dim -> last resort decade split of the whole-freq key
                st_dec = _pull_chunk_decades(flow, ".".join(parts), fv, acc, CHUNK_TIMEOUT)
                (done if st_dec == "ok" else (empty if st_dec == "empty" else err)).append(fv)
                continue
        # MEGA, or a non-mega freq chunk that failed and HAS a split dim:
        any_ok = False
        any_err = False
        had_data = False
        year_range = FORCE_YEAR.get(flow["id"])
        for sv in sub_vals:
            p2 = ["" for _ in range(ndim)]
            p2[fi] = fv
            p2[sub_idx] = sv
            if year_range:
                # go straight to (freq x split-value x YEAR): each request is small
                yrs = list(range(year_range[0], year_range[1] + 1))
                st2 = _pull_chunk_years(flow, ".".join(p2), f"{fv}__{sv}", acc,
                                        mtimeout, years=yrs)
            else:
                st2 = _pull_chunk(flow, ".".join(p2), f"{fv}__{sv}", acc, timeout=mtimeout)
                if st2 == "err":
                    # this freq x value slice is itself too big -> decade-split it
                    st2 = _pull_chunk_decades(flow, ".".join(p2), f"{fv}__{sv}", acc, mtimeout)
            if st2 in ("ok", "have"):
                any_ok = True
                had_data = True
            elif st2 == "err":
                any_err = True
                err.append(f"{fv}/{sv}")
        if any_ok:
            done.append(fv)
        elif any_err:
            pass  # already recorded specific failures
        else:
            empty.append(fv)

    status = "full" if done and not err else ("partial" if done else (
        "empty" if not err else "failed"))
    return {
        "status": status, "mode": "chunked",
        "split_dim": (dims[sub_idx] if sub_idx is not None else None),
        "n_obs": acc["obs"], "n_series": acc["series"], "bytes": acc["bytes"],
        "freqs_done": done, "freqs_empty": empty, "chunks_err": err,
        "start": acc["start"], "end": acc["end"],
        "dur_s": round(time.time() - t0, 1),
    }


def ingest_flow(flow: dict):
    did = flow["id"]
    if did in GIANTS:
        res = ingest_chunked(flow)
        # If chunked produced nothing usable AND no error, the freq codes may not
        # match this flow; fall back to a whole pull as a safety net.
        if res["status"] in ("empty",) and not res.get("chunks_err"):
            wres = ingest_whole(flow)
            if wres["status"] in ("full", "have"):
                return wres
        return res
    # non-giant: try whole; if it fails (timeout/5xx), fall back to chunking
    res = ingest_whole(flow)
    if res["status"] == "failed":
        cres = ingest_chunked(flow)
        if cres["status"] in ("full", "partial"):
            return cres
        # keep whichever has more obs
        return cres if cres["n_obs"] >= res["n_obs"] else res
    return res


# --------------------------------------------------------------------------- #
# Report (re-read parquet -> TRUE totals)
# --------------------------------------------------------------------------- #
def report_disk():
    import glob
    import pyarrow.compute as pc
    files = sorted(glob.glob(os.path.join(OUT, "*.parquet")))
    tot_obs = tot_ser = tot_bytes = 0
    biggest = []
    flows = set()
    for f in files:
        try:
            pf = pq.ParquetFile(f)
            nrows = pf.metadata.num_rows
        except Exception as e:  # noqa: BLE001
            log(f"  [bad parquet] {os.path.basename(f)}: {e}")
            continue
        try:
            col = pf.read(columns=["series_key"])["series_key"]
            nser = pc.count_distinct(col).as_py()
        except Exception:
            nser = 0
        sz = os.path.getsize(f)
        tot_obs += nrows
        tot_ser += nser
        tot_bytes += sz
        biggest.append((nrows, os.path.basename(f)))
        base = os.path.basename(f).split("__")[0:2]
        flows.add("__".join(base).replace(".parquet", ""))
    log(f"[report] parquet files       : {len(files)}")
    log(f"[report] observations (rows) : {tot_obs:,}")
    log(f"[report] distinct series (sum per-file): {tot_ser:,}")
    log(f"[report] on-disk size        : {tot_bytes/1e9:.3f} GB")
    log("[report] 15 biggest files:")
    for n, name in sorted(biggest, reverse=True)[:15]:
        log(f"           {n:>13,}  {name}")
    return len(files), tot_obs, tot_ser


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    args = sys.argv[1:]

    if "--report" in args:
        report_disk()
        return

    log("Enumerating ECB dataflows ...")
    flows = fetch_dataflows()
    log(f"ECB published dataflows: {len(flows)} total")
    by_ag = {}
    for f in flows:
        by_ag[f["agency"]] = by_ag.get(f["agency"], 0) + 1
    log(f"  by agency: {by_ag}")
    with io.open(CATALOG, "w", encoding="utf-8") as fh:
        json.dump({"total": len(flows), "by_agency": by_ag, "flows": flows},
                  fh, ensure_ascii=False, indent=1)

    if "--list" in args:
        for f in sorted(flows, key=lambda x: (x["agency"], x["id"])):
            g = " [GIANT]" if f["id"] in GIANTS else ""
            log(f"  {f['agency']:10} {f['id']:34} DSD={str(f['dsd']):16} {f['name'][:42]}{g}")
        log(f"LIST: {len(flows)} dataflows")
        return

    targets = list(flows)
    if "--only" in args:
        only = set(args[args.index("--only") + 1].split(","))
        targets = [f for f in targets if f["id"] in only]

    skip_existing = "--no-resume" not in args
    workers = int(args[args.index("--workers") + 1]) if "--workers" in args else 4
    workers = max(1, min(workers, 6))

    # Order: non-giants first (banked fast), giants last; alpha within group.
    targets.sort(key=lambda f: (1 if f["id"] in GIANTS else 0, f["agency"], f["id"]))

    # resume: skip ONLY non-giant flows whose single whole-flow parquet is on disk.
    # Giants are ALWAYS re-entered: ingest_chunked() resumes internally by skipping
    # the freq/sub-chunk parts already written and pulling only the missing ones, so
    # a giant interrupted mid-chunking is correctly completed rather than skipped
    # (which would silently drop the frequencies it had not reached yet).
    def fully_done(f):
        if f["id"] in GIANTS:
            return False  # re-enter; internal per-chunk resume fills any gaps
        return os.path.exists(os.path.join(OUT, fname(f)))
    if skip_existing:
        pre = [f for f in targets if fully_done(f)]
        targets = [f for f in targets if not fully_done(f)]
        if pre:
            log(f"[resume] {len(pre)} whole-flow parquets present -> skipping; "
                f"giants re-entered for per-chunk resume")

    n_giant = sum(1 for f in targets if f["id"] in GIANTS)
    log(f"Pulling {len(targets)} flows ({len(targets)-n_giant} whole + "
        f"{n_giant} chunked giants), workers={workers}")

    results = []
    tot_obs = tot_ser = 0
    ok = empty = err = 0
    t_all = time.time()
    lock = threading.Lock()
    n_done = 0

    def work(flow):
        tw = time.time()
        try:
            res = ingest_flow(flow)
        except Exception as e:  # noqa: BLE001
            res = {"status": "error", "n_obs": 0, "n_series": 0, "mode": "?",
                   "note": f"exception: {type(e).__name__}: {e}"}
        res["agency"] = flow["agency"]
        res["id"] = flow["id"]
        res["name"] = flow["name"]
        res["secs"] = round(time.time() - tw, 1)
        return res

    def save_manifest(attempted):
        with io.open(MANIFEST, "w", encoding="utf-8") as fh:
            json.dump({
                "license_id": LICENSE_ID,
                "source_id": "ecb",
                "published_total_flows": len(flows),
                "by_agency": by_ag,
                "attempted": attempted,
                "ok": ok, "empty": empty, "error": err,
                "total_observations": tot_obs, "total_series": tot_ser,
                "results": results,
            }, fh, ensure_ascii=False, indent=1)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(work, f): f for f in targets}
        for fut in as_completed(futs):
            res = fut.result()
            with lock:
                n_done += 1
                results.append(res)
                st = res["status"]
                if st in ("full", "have", "partial"):
                    ok += 1
                    tot_obs += res.get("n_obs", 0)
                    tot_ser += res.get("n_series", 0)
                elif st == "empty":
                    empty += 1
                else:
                    err += 1
                extra = ""
                if res.get("mode") == "chunked":
                    extra = (f" freqs={res.get('freqs_done')}"
                             + (f" split={res.get('split_dim')}" if res.get('split_dim') else "")
                             + (f" ERR={res.get('chunks_err')}" if res.get('chunks_err') else ""))
                tag = f"{res['agency']}:{res['id']}"
                log(f"[{n_done}/{len(targets)}] {tag:28} {st:8} "
                    f"obs={res.get('n_obs',0):>10,} ser={res.get('n_series',0):>7,} "
                    f"{res.get('mode',''):10} {res.get('secs','?')}s{extra}"
                    + (f"  NOTE={res.get('note')}" if res.get('note') else ""))
                if n_done % 5 == 0 or n_done == len(targets):
                    save_manifest(n_done)

    save_manifest(len(targets))
    dur = time.time() - t_all
    log("=" * 72)
    log(f"DONE in {dur/60:.1f} min: ok={ok} empty={empty} error={err}")
    log(f"TOTAL (this run, from writers): {tot_ser:,} series / {tot_obs:,} obs")
    log(f"Manifest: {MANIFEST}")
    log("Re-reading parquet for TRUE on-disk totals ...")
    report_disk()


if __name__ == "__main__":
    main()
