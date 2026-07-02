"""S3 (sdmx_delta) fetcher — European Central Bank Data Portal (SDMX 2.1). No key.

Endpoint: https://data-api.ecb.europa.eu/service  (license_id = ecb-attrib-nomodify;
values are stored verbatim, never modified).

Layout (set by jobs/ingest_ecb.py): grouped Parquet under clean_full/ecb/ where the
FILENAME encodes the exact SDMX selection that produced it:
    <AGENCY>__<FLOW>.parquet                      whole-flow pull
    <AGENCY>__<FLOW>__<FREQ>.parquet              one frequency chunk
    <AGENCY>__<FLOW>__<FREQ>__<SPLIT>.parquet     freq x split-dim value
    <AGENCY>__<FLOW>__<FREQ>__<SPLIT>__<YEAR>.parquet   + calendar year
  columns (uniform across all 540 files): series_key (str), obs_date (date32),
  value (float64), freq (str). The CSV `KEY` (e.g. "EXR.D.USD.EUR.SP00.A") is the
  pre-joined series key = FLOW + the DSD dimension values dot-joined.

DELTA STRATEGY (date-tail; the WHOLE source's enumeration is the set of on-disk
files — each file IS a sub-unit with its own selection + max(obs_date)):
  For every parquet file under clean_full/ecb/:
    1. read its EXACT key columns (always series_key/obs_date/value/freq) and its
       max(obs_date) and the FREQ it holds,
    2. reconstruct the SDMX 2.1 positional path-key the ingester used. The ECB API
       REJECTS an under-sized key with HTTP 400, so the key MUST be padded to exactly
       (ndim) positions for that flow's DSD: FREQ at position 0, the split-dim value
       (from the filename) at its DSD position, dots elsewhere. ndim + the split-dim
       position are obtained by REUSING jobs/ingest_ecb.py's fetch_dim_order() (DSD
       dimension order, cached), exactly as the ingester did — not re-discovered here,
    3. request ONLY newer observations:  GET /data/<agency>,<flow>,<ver>/<key>
       ?format=csvdata&detail=dataonly&startPeriod=<max_obs advanced one period>
       (year-bounded files also pin &endPeriod=<year>-12-31 so a per-year part never
       absorbs another year's rows). startPeriod re-includes the boundary period so an
       in-place revision of the latest value is captured; merge dedups the overlap.
    4. parse with the SAME period/float parser as the ingester and MERGE only via
       merge.merge_and_write(path, tbl, mode='merge', dedup_keys=('series_key',
       'obs_date')) — never writing parquet directly, so the never-shrink invariant
       holds and existing data is always preserved.

HONEST STATUS (Tally + finalize): each file is a sub-unit.
  added_unit(n)     rows merged (n>0 new / n==0 already-current)
  empty_unit()      200 with no rows newer than the cursor (a quiet tail)
  transient_unit()  timeout / 5xx / 429 / network drop  -> WHOLE run becomes 'partial'
  structural_unit() 200 whose body parsed 0 rows from a real, non-empty response
                    (schema/structural break) -> finalize raises DefinitiveError
Per-file cursors are reported as series_cursors[<filename-stem>] = 'YYYY-MM-DD'.
empty_window_floor = (#files - 1): a perfectly healthy steady-state run can have many
files return "nothing newer" (most ECB cubes update monthly/quarterly), so only a
TRULY wholesale all-empty window (every file empty/404) is treated as a break.
"""
from __future__ import annotations

import datetime as dt
import glob
import gzip
import json
import os
import re
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ... import config, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Tally, finalize

# --------------------------------------------------------------------------- #
# Endpoint / constants — reused verbatim from jobs/ingest_ecb.py
# --------------------------------------------------------------------------- #
SOURCE = "ecb"
UA = "Econ-Fin Data Library admin@hfdatalibrary.com"
BASE = "https://data-api.ecb.europa.eu/service"
STRUCT_MIME = "application/vnd.sdmx.structure+xml;version=2.1"
DEDUP = ("series_key", "obs_date")
FREQ_LETTERS = set("D B M Q A W H S N E".split())
CONNECT_RETRIES = 4
DATA_TIMEOUT = 300
STRUCT_TIMEOUT = 120
YEAR_RE = re.compile(r"^(19|20)\d\d$")
# Min unparseable body rows from a 200 to call a sub-unit a structural break (a real
# schema break dumps the whole cube's worth of rows; a few suppressed boundary cells
# do not). finalize() raises DefinitiveError on the first structural unit, so keep this
# high enough that a tiny quiet/suppressed delta window can't trip it.
STRUCT_MIN_BODY = 50

NS = {
    "mes": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
    "str": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
    "com": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
}

# Per-flow split dimension the ingester sub-split MEGA flows on (jobs/ingest_ecb.py
# MEGA). Used to resolve which DSD position a filename's <SPLIT> token sits at. Keyed
# by FLOW id (agency-independent — QSA ships under ESTAT here, ECB elsewhere).
SPLIT_DIM_BY_FLOW = {
    "YC": "INSTRUMENT_FM",
    "QSA": "REF_AREA",
    "CSEC": "REF_AREA",
}

# Caches (per process run).
_dsd_cache: dict[str, list[str]] = {}


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #
def _http_get(url: str, accept: str | None = None, timeout: int = DATA_TIMEOUT) -> bytes:
    """GET into memory, decompressing transport gzip/deflate.

    Raises urllib.error.HTTPError for 400/404 (so callers can treat as 'no data').
    Raises TransientError after exhausting retries on timeout/5xx/429/network drops.
    """
    headers = {"User-Agent": UA, "Accept-Encoding": "gzip, deflate"}
    if accept:
        headers["Accept"] = accept
    last = None
    for attempt in range(CONNECT_RETRIES):
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
                raise  # structural 'no data' for this selection/window
            last = f"HTTP {e.code}"
            if e.code not in (429, 500, 502, 503, 504):
                # other hard 4xx -> still transient-ish for retry, but record
                pass
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
            last = f"{type(e).__name__}: {e}"
        import time
        time.sleep(2 * (attempt + 1))
    raise TransientError(f"ecb GET {url}: {last}")


# --------------------------------------------------------------------------- #
# DSD dimension order (reuses the ingester's structure call + caching)
# --------------------------------------------------------------------------- #
def _dim_order(flow: dict) -> list[str] | None:
    """DSD dimension ids in key/position order (TIME excluded), cached per DSD.
    Mirrors jobs/ingest_ecb.py fetch_dim_order()."""
    dsd = flow.get("dsd")
    if not dsd:
        return None
    if dsd in _dsd_cache:
        return _dsd_cache[dsd]
    agency = flow.get("dsd_agency") or "ECB"
    ver = flow.get("dsd_version") or "1.0"
    root = None
    for u in (f"{BASE}/datastructure/{agency}/{dsd}/{ver}?references=none",
              f"{BASE}/datastructure/{agency}/{dsd}?references=none"):
        try:
            root = ET.fromstring(_http_get(u, STRUCT_MIME, STRUCT_TIMEOUT))
            break
        except urllib.error.HTTPError:
            continue
        except TransientError:
            raise
    if root is None:
        return None
    dl = root.find(".//str:DimensionList", NS)
    if dl is None:
        return None
    dims = []
    for dim in dl.findall("str:Dimension", NS):
        pos = dim.get("position")
        dims.append((int(pos) if pos else 999, dim.get("id")))
    dims.sort()
    ids = [d for _, d in dims]
    _dsd_cache[dsd] = ids
    return ids


# --------------------------------------------------------------------------- #
# TIME_PERIOD parsing + period arithmetic (mirrors the ingester's parser)
# --------------------------------------------------------------------------- #
_Q = re.compile(r"^(\d{4})-Q([1-4])$")
_M = re.compile(r"^(\d{4})-(\d{2})$")
_D = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
_S = re.compile(r"^(\d{4})-S([12])$")
_H = re.compile(r"^(\d{4})-H([12])$")
_W = re.compile(r"^(\d{4})-W(\d{2})$")
_Y = re.compile(r"^(\d{4})$")


def _parse_period(p: str):
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


def _to_float(s):
    if s is None or s == "":
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _start_period(max_obs: dt.date, freq: str) -> str:
    """ISO startPeriod string for the boundary obs (INCLUSIVE — re-fetch the boundary
    so an in-place revision of the latest value is captured; merge dedups overlap).

    We send the boundary period in the freq's own granularity so the API doesn't
    silently drop sub-period rows. Daily uses the exact date; monthly YYYY-MM,
    quarterly YYYY-Qn, annual YYYY, half/semester YYYY-Hn / YYYY-Sn, weekly the date.
    """
    y = max_obs.year
    if freq in ("D", "B", "W"):
        return max_obs.isoformat()
    if freq == "M":
        return f"{y:04d}-{max_obs.month:02d}"
    if freq == "Q":
        q = (max_obs.month - 1) // 3 + 1
        return f"{y:04d}-Q{q}"
    if freq in ("H",):
        h = 1 if max_obs.month <= 6 else 2
        return f"{y:04d}-H{h}"
    if freq in ("S",):
        s = 1 if max_obs.month <= 6 else 2
        return f"{y:04d}-S{s}"
    if freq in ("A", "N", "E"):
        return f"{y:04d}"
    # unknown freq -> fall back to the date; ECB accepts an ISO date for any freq
    return max_obs.isoformat()


# --------------------------------------------------------------------------- #
# Filename -> SDMX selection
# --------------------------------------------------------------------------- #
def _decode_filename(stem: str) -> dict | None:
    """Parse '<AGENCY>__<FLOW>[__FREQ[__SPLIT[__YEAR]]]' into its parts.

    Returns {agency, flow, freq|None, split|None, year|None}. AGENCY may itself
    contain a dot (e.g. 'ECB.DISS'); FLOW may contain underscores ('EXR_PUB',
    'MOBILE_KEY_1', 'JDF_BSI_MFI_BALANCE_SHEET') but never '__' (the token sep)."""
    parts = stem.split("__")
    if len(parts) < 2:
        return None
    agency, flow = parts[0], parts[1]
    suf = parts[2:]
    freq = split = year = None
    if len(suf) == 1:
        freq = suf[0] if suf[0] in FREQ_LETTERS else None
        if freq is None:
            return None  # unexpected single non-freq token
    elif len(suf) == 2:
        if suf[0] not in FREQ_LETTERS:
            return None
        freq, split = suf
    elif len(suf) == 3:
        if suf[0] not in FREQ_LETTERS or not YEAR_RE.match(suf[2]):
            return None
        freq, split, year = suf[0], suf[1], int(suf[2])
    elif len(suf) > 3:
        return None
    return {"agency": agency, "flow": flow, "freq": freq, "split": split, "year": year}


def _split_position(flow: dict, decoded: dict, sample_keys: list[str]) -> int | None:
    """DSD position (0-indexed, FREQ=0) where the filename's <SPLIT> value sits.

    Primary: the ingester's MEGA split dim for this flow id, looked up in the DSD
    dimension order. Fallback: the position at which the split value is CONSTANT
    across all on-disk series keys (data is the ground truth)."""
    sv = decoded["split"]
    if sv is None:
        return None
    dims = None
    try:
        dims = _dim_order(flow)
    except TransientError:
        raise
    if dims:
        want = SPLIT_DIM_BY_FLOW.get(decoded["flow"])
        if want and want in dims:
            return dims.index(want)
    # fallback: find the constant position holding exactly sv in the existing keys
    parts = [k.split(".") for k in sample_keys if k]
    if not parts:
        return None
    ndim = max(len(x) for x in parts) - 1
    for i in range(ndim):
        if i == 0:
            continue  # FREQ position
        if all(len(x) > i + 1 and x[i + 1] == sv for x in parts):
            return i
    return None


def _build_path_key(flow: dict, decoded: dict, sample_keys: list[str]) -> str | None:
    """Build the FULLY dot-padded positional key (ndim positions) the ECB API
    requires. None for a whole-flow file (no path-key -> use the whole-flow URL)."""
    if decoded["freq"] is None:
        return None  # whole-flow file: no positional key
    dims = _dim_order(flow)
    if dims:
        ndim = len(dims)
    else:
        # derive ndim from the data if the DSD lookup failed
        parts = [k.split(".") for k in sample_keys if k]
        ndim = (max(len(x) for x in parts) - 1) if parts else 1
    pos = ["" for _ in range(ndim)]
    pos[0] = decoded["freq"]
    if decoded["split"] is not None:
        sp = _split_position(flow, decoded, sample_keys)
        if sp is None or sp >= ndim:
            # cannot place the split value safely; fall back to data-constant search
            return None
        pos[sp] = decoded["split"]
    return ".".join(pos)


def _data_url(flow: dict, path_key: str | None, start: str | None, end: str | None) -> str:
    sel = f"{flow['agency']},{flow['id']},{flow.get('version', '1.0')}"
    u = f"{BASE}/data/{sel}"
    if path_key:
        u += f"/{path_key}"
    u += "?format=csvdata&detail=dataonly"
    if start:
        u += f"&startPeriod={start}"
    if end:
        u += f"&endPeriod={end}"
    return u


# --------------------------------------------------------------------------- #
# CSV parse
# --------------------------------------------------------------------------- #
def _parse_csv(raw: bytes):
    """Parse SDMX-CSV (csvdata) -> (keys, dates, values, freqs, n_body_rows).
    n_body_rows counts non-header lines seen (to distinguish a real-but-unparseable
    body from a legitimately empty tail)."""
    import csv
    import io
    text = raw.decode("utf-8-sig", "replace")
    rd = csv.reader(io.StringIO(text))
    hdr = next(rd, None)
    if not hdr:
        return [], [], [], [], 0
    if "TIME_PERIOD" not in hdr or "OBS_VALUE" not in hdr:
        return [], [], [], [], 0
    ki = hdr.index("KEY") if "KEY" in hdr else 0
    tpi = hdr.index("TIME_PERIOD")
    ovi = hdr.index("OBS_VALUE")
    fri = hdr.index("FREQ") if "FREQ" in hdr else (
        hdr.index("FREQUENCY") if "FREQUENCY" in hdr else -1)
    keys, dates, vals, freqs = [], [], [], []
    n_body = 0
    for row in rd:
        n_body += 1
        if len(row) <= tpi or len(row) <= ovi:
            continue
        v = _to_float(row[ovi])
        if v is None:
            continue
        od = _parse_period(row[tpi])
        if od is None:
            continue
        keys.append(row[ki] if ki < len(row) else "")
        dates.append(od)
        vals.append(v)
        freqs.append(row[fri] if (0 <= fri < len(row)) else "")
    return keys, dates, vals, freqs, n_body


# --------------------------------------------------------------------------- #
# Per-file state
# --------------------------------------------------------------------------- #
def _file_max_and_keys(path: str):
    """(max_obs_date|None, freq|None, sample_distinct_series_keys[list])."""
    try:
        t = pq.read_table(path, columns=["series_key", "obs_date", "freq"])
    except Exception:
        return None, None, []
    if t.num_rows == 0:
        return None, None, []
    mx = pc.max(t.column("obs_date")).as_py()
    if isinstance(mx, dt.datetime):
        mx = mx.date()
    # dominant freq in the file
    fr = None
    fcol = t.column("freq").to_pylist()
    for x in fcol:
        if x:
            fr = x
            break
    keys = list({k for k in t.column("series_key").to_pylist() if k})
    return mx, fr, keys


# --------------------------------------------------------------------------- #
# Catalog (reuse the ingester's enumeration output if present; else fetch live)
# --------------------------------------------------------------------------- #
def _load_catalog(out_dir: str) -> dict:
    """{(agency, flow): {agency,id,version,dsd,dsd_agency,dsd_version}}.

    Prefer the on-disk _catalog.json the ingester already wrote (the SAME enumeration
    — do not re-discover). Fall back to a live /dataflow enumeration only if absent."""
    cat_path = os.path.join(out_dir, "_catalog.json")
    flows = None
    if os.path.exists(cat_path):
        try:
            flows = json.load(open(cat_path, encoding="utf-8")).get("flows")
        except (ValueError, OSError):
            flows = None
    if not flows:
        raw = _http_get(f"{BASE}/dataflow", STRUCT_MIME, STRUCT_TIMEOUT)
        root = ET.fromstring(raw)
        flows = []
        for f in root.findall(".//str:Dataflow", NS):
            ref = f.find("str:Structure/Ref", NS)
            flows.append({
                "agency": f.get("agencyID"), "id": f.get("id"),
                "version": f.get("version") or "1.0",
                "dsd": ref.get("id") if ref is not None else None,
                "dsd_agency": ref.get("agencyID") if ref is not None else None,
                "dsd_version": ref.get("version") if ref is not None else None,
            })
    out = {}
    for f in flows:
        out[(f["agency"], f["id"])] = f
    return out


# --------------------------------------------------------------------------- #
# Contract entry point
# --------------------------------------------------------------------------- #
def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    if not os.path.isdir(out_dir):
        raise DefinitiveError(f"ecb source dir missing: {out_dir}")

    pfiles = sorted(
        os.path.basename(p) for p in glob.glob(os.path.join(out_dir, "*.parquet"))
        if not os.path.basename(p).startswith("_"))
    if not pfiles:
        raise DefinitiveError(f"no ecb parquet files under {out_dir}")

    catalog = _load_catalog(out_dir)

    tally = Tally()
    total = 0
    maxd = None
    cursors: dict[str, str] = {}

    for fn in pfiles:
        path = os.path.join(out_dir, fn)
        stem = fn[:-len(".parquet")]
        before = 0
        try:
            before = pq.read_metadata(path).num_rows
        except Exception:
            pass

        decoded = _decode_filename(stem)
        max_obs, file_freq, sample_keys = _file_max_and_keys(path)

        # Seed the per-file cursor from the on-disk frontier so an untouched/empty
        # file still reports its real freshness (a frozen file can't hide).
        if max_obs is not None:
            cursors[stem] = max_obs.isoformat()

        if decoded is None or max_obs is None:
            # Unparseable filename or empty file: leave it untouched, count its rows.
            total += before
            tally.empty_unit()
            continue

        flow = catalog.get((decoded["agency"], decoded["flow"]))
        if flow is None:
            # Not in the enumeration (should not happen) -> leave untouched.
            total += before
            tally.empty_unit()
            continue

        freq = decoded["freq"] or file_freq or "D"

        # Build the request window. startPeriod = boundary (inclusive). Year-bounded
        # files pin endPeriod to the year so a per-year part never absorbs another
        # year. A year file already fully in the past (year < this year) and current
        # through its Dec is effectively closed, but we still re-request its boundary
        # so a late revision lands; that costs one small bounded request.
        start = _start_period(max_obs, freq)
        end = None
        if decoded["year"] is not None:
            end = f"{decoded['year']:04d}-12-31"
            # don't request a window that starts after the year ends
            ys = f"{decoded['year']:04d}-01-01"
            if start > end:
                start = ys
            elif start < ys:
                start = ys

        try:
            path_key = _build_path_key(flow, decoded, sample_keys)
        except TransientError:
            tally.transient_unit()
            total += before
            continue

        url = _data_url(flow, path_key, start, end)

        # Fetch the delta window.
        try:
            raw = _http_get(url, timeout=DATA_TIMEOUT)
        except urllib.error.HTTPError as e:
            if e.code in (400, 404):
                # No data in this newer window — a quiet tail. (An ECB 400/404 on a
                # startPeriod query past the last obs is the normal 'nothing new'.)
                tally.empty_unit()
                total += before
                continue
            tally.transient_unit()
            total += before
            continue
        except TransientError:
            tally.transient_unit()
            total += before
            continue

        keys, dates, vals, freqs, n_body = _parse_csv(raw)

        if not dates:
            # 200 but nothing parseable. finalize() raises DefinitiveError on the FIRST
            # structural sub-unit, so be conservative: only call it structural when a
            # SUBSTANTIAL body (>=STRUCT_MIN_BODY rows) parsed to zero — that is a real
            # schema/structure break. A tiny boundary window returning a few rows with
            # suppressed/blank values is a legitimately quiet tail, not a break, so it
            # must NOT nuke the whole source. (The all-empty floor still catches a
            # wholesale outage where EVERY file goes empty.)
            if n_body >= STRUCT_MIN_BODY:
                tally.structural_unit()
            else:
                tally.empty_unit()
            total += before
            continue

        # Restrict to rows at/after the file's stored max (defensive: the API can
        # round startPeriod down to a coarser granularity for some freqs).
        new_tbl = pa.table({
            "series_key": pa.array(keys, pa.string()),
            "obs_date":   pa.array(dates, pa.date32()),
            "value":      pa.array(vals, pa.float64()),
            "freq":       pa.array(freqs, pa.string()),
        })
        new_tbl = new_tbl.filter(pc.greater_equal(new_tbl.column("obs_date"),
                                                  pa.scalar(max_obs, pa.date32())))
        if new_tbl.num_rows == 0:
            tally.empty_unit()
            total += before
            continue

        try:
            n, md = merge.merge_and_write(path, new_tbl, mode="merge",
                                          dedup_keys=DEDUP)
        except (PermissionError, OSError) as e:
            # Transient OS-level publish contention (e.g. Windows os.replace WinError 5
            # when an AV scanner / another reader briefly holds the destination). The
            # existing file is left intact; record transient -> the WHOLE run reports
            # 'partial' and this file is retried next tick. NEVER let it crash the run.
            tally.transient_unit()
            total += before
            del e
            continue
        except DefinitiveError:
            # merge refused to publish (would shrink/empty/drop a column or break dedup)
            # for THIS file — a structural problem for this sub-unit. Keep the existing
            # data, record it, and keep going so one bad file can't strand the other 539.
            tally.structural_unit()
            total += before
            continue
        total += n
        # NET-DELTA -> ADDED: this file returned and parsed REAL rows this run
        # (new_tbl is non-empty here), so it is a data-bearing (added) sub-unit even
        # when the boundary re-fetch nets 0 new rows after dedup (merge returns the
        # new TOTAL, not a delta — a quiet steady-state cube re-returns its inclusive
        # boundary obs that dedups away). Counting that as empty_unit() would let a
        # fully-quiet healthy run trip the all-empty structural floor (false
        # DefinitiveError). Only a flow returning NO rows is empty. Mirrors bcb.py.
        tally.added_unit(new_tbl.num_rows)
        if md:
            cursors[stem] = md
            if maxd is None or md > maxd:
                maxd = md

    # A perfectly healthy run can have nearly every file return "nothing newer", so
    # only a TRULY wholesale all-empty/404 window should trip the structural floor.
    return finalize(tally, total, maxd, source=SOURCE, series_cursors=cursors,
                    empty_window_floor=len(pfiles) - 1)
