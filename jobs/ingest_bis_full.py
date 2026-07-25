#!/usr/bin/env python3
"""FULL-COVERAGE ingest of BIS statistics via the BIS SDMX RESTful API v1.

Enumerates EVERY BIS dataflow, then pulls each one fully via SDMX-CSV and writes
ONE grouped Parquet per dataflow (or per dataflow+frequency / +sub-key for the
giants) to data/clean_full/bis/<DATAFLOW_ID>[__<FREQ>[__<VAL>]].parquet with
columns:  series_key (the joined SDMX dimension values), obs_date, value, freq.

API facts (probed live 2026-06):
  * Catalog:  GET /api/v1/dataflow/BIS/all/latest  -> SDMX-ML structure listing
              every published dataflow (29 as of 2026-06).
  * Bulk:     GET /api/v1/data/{AGENCY},{FLOW},{VER}/all  with
              Accept: application/vnd.sdmx.data+csv  returns the ENTIRE dataflow
              (every series) in ONE streamed CSV. detail=dataonly trims metadata
              columns without changing the observation count.
  * CSV shape: first three columns are STRUCTURE,STRUCTURE_ID,ACTION; the DIMENSION
               columns follow (FREQ is always the first), then TIME_PERIOD,
               OBS_VALUE, then attribute columns. So dimensions = the columns
               between ACTION (idx 2) and TIME_PERIOD. series_key = their '.' join.

GIANT dataflows (consolidated/locational banking, debt securities, EER, total
credit, USD XR, property prices, OTC/XTD derivatives, long CPI) are
frequency-CHUNKED: read the DSD dimension order, locate FREQ, and issue one
positional-key request per frequency value (key '<f>.' style with the freq slot
filled and the rest wildcarded). Each chunk is a smaller, independent,
restart-safe request and lands in its own parquet part. MEGA cubes get a
second-level split on the lowest-cardinality non-freq dimension so each request
is only a few MB.

Memory is bounded: each HTTP body streams to a temp .gz, then parses row-by-row
and flushes to the Parquet writer in RecordBatches; no dataflow is held whole in
RAM.

License: bis-attrib-nc (re-serveable, NON-COMMERCIAL, attribution required).

Usage:
  python jobs/ingest_bis_full.py --list           # enumerate catalog, no data
  python jobs/ingest_bis_full.py --only WS_TC,WS_EER
  python jobs/ingest_bis_full.py --skip-existing  # full run, resume
  python jobs/ingest_bis_full.py                  # full run (all dataflows)
"""
from __future__ import annotations

import csv
import datetime as dt
import glob as _glob
import gzip
import io
import json
import os
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

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
sys.path.insert(0, ROOT)

OUT = os.path.join(ROOT, "data", "clean_full", "bis")
MANIFEST = os.path.join(OUT, "_manifest.json")
LOGDIR = os.path.join(ROOT, "data")

UA = "Econ-Fin Data Library admin@hfdatalibrary.com"
CSVMIME = "application/vnd.sdmx.data+csv"
XMLMIME = "application/vnd.sdmx.structure+xml;version=2.1"
BASE = "https://stats.bis.org/api/v1"
LICENSE_ID = "bis-attrib-nc"

NS = {
    "mes": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
    "str": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
    "com": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
}

# Dataflows expected to be large -> frequency-chunk them. (Banking locational /
# consolidated, intl debt securities, EER, USD XR, total credit, property
# prices, derivatives, long CPI, debt service, credit gap, GLI, national-accounts
# securities.) Anything not here is pulled whole in one request.
GIANTS = {
    "WS_LBS_D_PUB", "WS_CBS_PUB", "WS_DEBT_SEC2_PUB", "WS_EER", "WS_XRU",
    "WS_TC", "WS_SPP", "WS_DPP", "WS_CPP", "WS_OTC_DERIV2", "WS_DER_OTC_TOV",
    "WS_XTD_DERIV", "WS_LONG_CPI", "WS_DSR", "WS_CREDIT_GAP", "WS_GLI",
    "WS_NA_SEC_C3", "WS_NA_SEC_DSS",
}

# MEGA = giants whose single-frequency chunk is still huge -> second-level split
# on a MID-cardinality non-freq dimension so each request is only a few MB and
# completes fast (BIS throttles per-connection bandwidth on big extracts but
# serves small slices quickly). The split dimension is chosen at runtime; the
# explicit hints below name the natural high-cardinality slicer (reporting
# country / issuer / currency) so we get many small chunks instead of 2 huge ones.
MEGA = {
    "WS_LBS_D_PUB", "WS_CBS_PUB", "WS_DEBT_SEC2_PUB", "WS_TC",
    "WS_OTC_DERIV2", "WS_DER_OTC_TOV", "WS_XTD_DERIV",
}
SPLIT_HINT = {
    "WS_CBS_PUB": "L_REP_CTY",        # 33 reporting countries
    "WS_LBS_D_PUB": "L_REP_CTY",      # 50 reporting countries
    "WS_DEBT_SEC2_PUB": "ISSUE_CUR",  # 65 issue currencies
    "WS_TC": "BORROWERS_CTY",         # 48 borrower countries
    "WS_OTC_DERIV2": "DER_INSTR",     # 15 instruments
    "WS_DER_OTC_TOV": "DER_REP_CTY",  # 57 reporting countries
    "WS_XTD_DERIV": "ISSUE_CUR",      # 25 issue currencies
}
SPLIT_CAP = 90        # never explode a split into more than ~this many requests
CHUNK_WORKERS = 6     # polite parallelism for chunk downloads within one flow

csv.field_size_limit(min(sys.maxsize, 2_147_483_647))


# --------------------------------------------------------------------------- #
# HTTP helpers (retry/backoff, polite UA)
# --------------------------------------------------------------------------- #
def _open(url: str, accept: str, timeout: int = 120):
    """Open a URL with retry/backoff. `timeout` is the per-socket-operation
    timeout (connect AND each subsequent read), so a connection that BIS stalls
    mid-stream raises within `timeout` seconds instead of hanging for minutes."""
    last = None
    for attempt in range(5):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": accept})
            return urllib.request.urlopen(req, timeout=timeout)
        except urllib.error.HTTPError as e:
            # 404 = no results for that key (treat as empty); 400 = bad key.
            if e.code in (400, 404):
                raise
            last = e
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            last = e
        time.sleep(3 * (attempt + 1))
    if last:
        raise last
    raise RuntimeError("unreachable")


def fetch_dataflows() -> list[dict]:
    """Enumerate EVERY dataflow published by BIS, with its DSD reference."""
    url = f"{BASE}/dataflow/BIS/all/latest"
    with _open(url, XMLMIME) as r:
        raw = r.read()
    root = ET.fromstring(raw)
    out = []
    for f in root.findall(".//str:Dataflow", NS):
        did = f.get("id")
        nm = f.find("com:Name", NS)
        sref = f.find(".//str:Structure/Ref", NS)
        out.append({
            "agency": f.get("agencyID"),
            "id": did,
            "version": f.get("version"),
            "name": (nm.text if nm is not None else "") or "",
            "dsd_agency": sref.get("agencyID") if sref is not None else None,
            "dsd_id": sref.get("id") if sref is not None else None,
            "dsd_version": sref.get("version") if sref is not None else None,
        })
    return out


def fetch_dim_order(dsd_agency, dsd_id, dsd_ver):
    """Return (dim_ids_in_key_order, freq_index_or_None) from the DSD."""
    url = f"{BASE}/datastructure/{dsd_agency}/{dsd_id}/{dsd_ver}"
    try:
        with _open(url, XMLMIME) as r:
            raw = r.read()
        root = ET.fromstring(raw)
    except Exception:
        return None, None
    dims = []
    for dim in root.findall(".//str:DimensionList/str:Dimension", NS):
        pos = dim.get("position")
        did_ = dim.get("id")
        dims.append((int(pos) if pos else 999, did_))
    dims.sort()
    ids = [d for _, d in dims]
    fi = None
    for i, d in enumerate(ids):
        if d in ("FREQ", "FREQUENCY"):
            fi = i
            break
    return (ids or None), fi


def fetch_member_values(flow_agency, flow_id, flow_ver, dim_id):
    """List the code values actually in use for one dimension, via the
    availableconstraint endpoint. Returns None on any error."""
    url = (f"{BASE}/availableconstraint/{flow_agency},{flow_id},{flow_ver}"
           f"/all/all?mode=exact&references=none")
    try:
        with _open(url, XMLMIME, timeout=180) as r:
            raw = r.read()
        root = ET.fromstring(raw)
    except Exception:
        return None
    vals = []
    for kv in root.findall(".//com:KeyValue", NS) + root.findall(".//str:KeyValue", NS):
        if kv.get("id") == dim_id:
            for v in kv.findall("com:Value", NS) + kv.findall("str:Value", NS):
                if v.text:
                    vals.append(v.text.strip())
            break
    return vals or None


# --------------------------------------------------------------------------- #
# TIME_PERIOD parsing -- handles every SDMX period style BIS emits.
# --------------------------------------------------------------------------- #
def parse_period(tp: str):
    tp = (tp or "").strip()
    if not tp:
        return None, None
    try:
        if len(tp) == 4 and tp.isdigit():
            return dt.date(int(tp), 12, 31), "A"
        if "-Q" in tp:
            y, q = tp.split("-Q")
            return dt.date(int(y), (int(q) - 1) * 3 + 1, 1), "Q"
        if "-S" in tp or "-H" in tp:
            sep = "-S" if "-S" in tp else "-H"
            y, s = tp.split(sep)
            return dt.date(int(y), 1 if s.strip() == "1" else 7, 1), "S"
        if "-W" in tp:
            y, w = tp.split("-W")
            return dt.date.fromisocalendar(int(y), int(w), 1), "W"
        if "-M" in tp:
            y, m = tp.split("-M")
            return dt.date(int(y), int(m), 1), "M"
        if "-" in tp:
            parts = tp.split("-")
            if len(parts) == 3:
                return dt.date(int(parts[0]), int(parts[1]), int(parts[2])), "D"
            if len(parts) == 2:
                return dt.date(int(parts[0]), int(parts[1]), 1), "M"
    except (ValueError, TypeError, KeyError):
        return None, None
    return None, None


def to_float(v):
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


# --------------------------------------------------------------------------- #
# Download + parse one query (whole dataflow, or one chunk)
# --------------------------------------------------------------------------- #
SCHEMA = pa.schema([
    ("series_key", pa.string()),
    ("obs_date", pa.date32()),
    ("value", pa.float64()),
    ("freq", pa.string()),
])
BATCH = 250_000


def stream_query_to_tmp(agency, did, ver, key="all", max_tries=4):
    """Stream one SDMX-CSV query to a temp .gz. Returns (tmp_path, dl_bytes).

    The whole download (open + read loop) is retried: BIS sometimes stalls a
    connection mid-stream (no bytes, no error) -- the per-read socket timeout in
    _open turns that into a raised exception, and here we discard the partial
    temp and re-download from scratch. A 400/404 (genuinely no data for the key)
    propagates immediately so the caller records it as empty, not retried.
    """
    sel = f"{agency},{did},{ver}"
    url = f"{BASE}/data/{sel}/{key}?detail=dataonly"
    last = None
    for attempt in range(max_tries):
        tmp_path = None
        try:
            with _open(url, CSVMIME) as r:
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv.gz", dir=LOGDIR)
                tmp_path = tmp.name
                nbytes = 0
                with gzip.open(tmp, "wb") as gz:
                    while True:
                        chunk = r.read(1 << 20)
                        if not chunk:
                            break
                        nbytes += len(chunk)
                        gz.write(chunk)
                tmp.close()
            return tmp_path, nbytes
        except urllib.error.HTTPError:
            raise  # 400/404 -> empty; do not retry
        except Exception as e:  # noqa: BLE001  (timeout / reset / incomplete read)
            last = e
            if tmp_path:
                _cleanup(tmp_path)
            time.sleep(3 * (attempt + 1))
    raise last if last else RuntimeError("download failed")


def parse_tmp_to_parquet(tmp: str, out_path: str):
    """Parse a temp .gz SDMX-CSV into a grouped parquet. Returns stats dict.

    Dimension columns = everything between ACTION (col 2) and TIME_PERIOD;
    series_key = their join with '.'. FREQ is the first dimension column.
    """
    tmp_out = out_path + ".tmp"
    writer = None
    keys_buf, dates_buf, vals_buf, freq_buf = [], [], [], []
    n_obs = 0
    series_keys = set()
    bad_periods = 0
    freqs = set()
    dmin = dmax = None

    def flush():
        nonlocal keys_buf, dates_buf, vals_buf, freq_buf
        if not keys_buf:
            return
        batch = pa.record_batch(
            [pa.array(keys_buf, pa.string()),
             pa.array(dates_buf, pa.date32()),
             pa.array(vals_buf, pa.float64()),
             pa.array(freq_buf, pa.string())],
            schema=SCHEMA)
        writer.write_batch(batch)
        keys_buf, dates_buf, vals_buf, freq_buf = [], [], [], []

    with gzip.open(tmp, "rt", encoding="utf-8-sig", newline="") as fh:
        rd = csv.reader(fh)
        hdr = next(rd, None)
        if not hdr:
            return {"status": "empty", "note": "no header"}
        if "TIME_PERIOD" not in hdr or "OBS_VALUE" not in hdr:
            return {"status": "empty", "note": "no TIME_PERIOD/OBS_VALUE"}
        tpi = hdr.index("TIME_PERIOD")
        ovi = hdr.index("OBS_VALUE")
        # dims = columns AFTER the STRUCTURE/STRUCTURE_ID/ACTION prefix and
        # BEFORE TIME_PERIOD. Robustly skip the three known prefix columns.
        start = 0
        for pref in ("STRUCTURE", "STRUCTURE_ID", "ACTION"):
            if start < len(hdr) and hdr[start] == pref:
                start += 1
        dim_idx = [i for i in range(start, tpi) if i != ovi]
        freq_col = None
        for cand in ("FREQ", "FREQUENCY"):
            if cand in hdr:
                freq_col = hdr.index(cand)
                break
        writer = pq.ParquetWriter(tmp_out, SCHEMA, compression="zstd")
        for row in rd:
            if len(row) <= tpi or len(row) <= ovi:
                continue
            val = to_float(row[ovi])
            if val is None:
                continue
            d, finf = parse_period(row[tpi])
            if d is None:
                bad_periods += 1
                continue
            key = ".".join(row[i] if i < len(row) else "" for i in dim_idx)
            fr = (row[freq_col] if (freq_col is not None and freq_col < len(row)) else "") or finf or ""
            keys_buf.append(key)
            dates_buf.append(d)
            vals_buf.append(val)
            freq_buf.append(fr)
            series_keys.add(key)
            freqs.add(fr)
            if dmin is None or d < dmin:
                dmin = d
            if dmax is None or d > dmax:
                dmax = d
            n_obs += 1
            if len(keys_buf) >= BATCH:
                flush()
        flush()

    if writer is not None:
        writer.close()
    if n_obs == 0:
        try:
            os.unlink(tmp_out)
        except OSError:
            pass
        return {"status": "empty", "note": "no parseable observations"}
    os.replace(tmp_out, out_path)
    return {
        "status": "ok", "n_obs": n_obs, "n_series": len(series_keys),
        "parquet_bytes": os.path.getsize(out_path),
        "start": str(dmin), "end": str(dmax),
        "freqs": sorted(f for f in freqs if f), "bad_periods": bad_periods,
    }


def _cleanup(*paths):
    for p in paths:
        try:
            os.unlink(p)
        except OSError:
            pass


def ingest_simple(flow: dict, out_path: str):
    """Pull a whole dataflow in one request -> one parquet."""
    agency, did, ver = flow["agency"], flow["id"], flow["version"]
    t0 = time.time()
    try:
        tmp, dlb = stream_query_to_tmp(agency, did, ver, "all")
    except urllib.error.HTTPError as e:
        return {"id": did, "status": "empty" if e.code in (400, 404) else "error",
                "n_obs": 0, "n_series": 0, "note": f"HTTP {e.code}"}
    except Exception as e:  # noqa: BLE001
        return {"id": did, "status": "error", "n_obs": 0, "n_series": 0,
                "note": f"{type(e).__name__}: {e}"}
    try:
        res = parse_tmp_to_parquet(tmp, out_path)
    except Exception as e:  # noqa: BLE001
        _cleanup(tmp, out_path + ".tmp")
        return {"id": did, "status": "error", "n_obs": 0, "n_series": 0,
                "note": f"parse {type(e).__name__}: {e}"}
    _cleanup(tmp)
    res.update({"id": did, "agency": agency, "version": ver,
                "name": flow["name"], "dl_bytes": dlb,
                "dur_s": round(time.time() - t0, 1)})
    res.setdefault("n_obs", 0)
    res.setdefault("n_series", 0)
    return res


def _key_from_parts(parts):
    """positional dotted key, trailing empties trimmed (BIS accepts 'Q.' etc.)."""
    s = ".".join(parts)
    while s.endswith("."):
        s = s[:-1]
    return s or "all"


def _pull_one_chunk(agency, did, ver, key, out_path, acc, lock):
    """Download+parse ONE positional-key query into out_path. Resumes if the
    parquet already exists. Thread-safely updates the `acc` stats dict.
    Returns: 'have' | 'ok' | 'empty' | 'err'."""
    if os.path.exists(out_path):
        try:
            n = pq.ParquetFile(out_path).metadata.num_rows
        except Exception:
            n = 0
        with lock:
            acc["obs"] += n
        return "have"
    try:
        tmp, _ = stream_query_to_tmp(agency, did, ver, key)
    except urllib.error.HTTPError as e:
        return "empty" if e.code in (400, 404) else "err"
    except Exception:  # noqa: BLE001
        return "err"
    try:
        res = parse_tmp_to_parquet(tmp, out_path)
    except Exception:  # noqa: BLE001
        _cleanup(tmp, out_path + ".tmp")
        return "err"
    _cleanup(tmp)
    if res["status"] == "ok":
        with lock:
            acc["obs"] += res["n_obs"]
            acc["series"] += res["n_series"]
            acc["pq"] += res["parquet_bytes"]
            if acc["start"] is None or res["start"] < acc["start"]:
                acc["start"] = res["start"]
            if acc["end"] is None or res["end"] > acc["end"]:
                acc["end"] = res["end"]
        return "ok"
    return "empty"


def _choose_split(agency, did, ver, ids, fi):
    """Pick the second-level split dimension for a MEGA flow.

    Prefer the explicit hint; else the highest-cardinality non-freq dimension
    whose member count is in [2, SPLIT_CAP] (so chunks are small but we don't
    explode into hundreds of requests). Returns (idx, values) or (None, None).
    """
    ndim = len(ids)
    hint = SPLIT_HINT.get(did)
    if hint and hint in ids:
        ci = ids.index(hint)
        vals = fetch_member_values(agency, did, ver, ids[ci])
        if vals and 2 <= len(vals) <= SPLIT_CAP:
            return ci, vals
    best = (None, None, -1)
    for ci in range(ndim):
        if ci == fi:
            continue
        vals = fetch_member_values(agency, did, ver, ids[ci])
        n = len(vals) if vals else 0
        if vals and 2 <= n <= SPLIT_CAP and n > best[2]:
            best = (ci, vals, n)
    return best[0], best[1]


def ingest_chunked(flow, freqs_candidates=("A", "Q", "M", "D", "W", "S", "H", "B")):
    """Chunk a giant dataflow into smaller, independent, restart-safe requests.

    * Regular giants: one request per FREQ value -> <DID>__<FREQ>.parquet
      (these run sequentially -- only a handful of frequencies).
    * MEGA giants: two-level split (FREQ x mid-cardinality dim value) ->
      <DID>__<FREQ>__<VAL>.parquet, run with a small thread pool so BIS's fast
      small-extract serving is used in parallel (concurrency <= CHUNK_WORKERS).
    Every chunk part already on disk is skipped, so restart resumes exactly.
    """
    agency, did, ver = flow["agency"], flow["id"], flow["version"]
    t0 = time.time()
    ids, fi = fetch_dim_order(flow["dsd_agency"], flow["dsd_id"], flow["dsd_version"])
    if ids is None or fi is None:
        return ingest_simple(flow, os.path.join(OUT, did + ".parquet"))
    ndim = len(ids)

    # Only iterate the frequencies that ACTUALLY exist (avoids ~7x wasted 404
    # requests). Fall back to the full candidate list if the constraint is
    # unavailable, intersecting with it to preserve a sane request order.
    real_freqs = fetch_member_values(agency, did, ver, ids[fi])
    if real_freqs:
        order = [f for f in freqs_candidates if f in set(real_freqs)]
        # include any exotic codes not in our candidate order, just in case
        order += [f for f in real_freqs if f not in set(freqs_candidates)]
        freqs_candidates = tuple(order)

    acc = {"obs": 0, "series": 0, "pq": 0, "start": None, "end": None}
    lock = threading.Lock()
    done, empty, err = [], [], []

    split_idx = split_vals = None
    if did in MEGA:
        split_idx, split_vals = _choose_split(agency, did, ver, ids, fi)

    if split_idx is not None:
        # Build the full task list (freq x split-value), skipping any freq that
        # already has a whole-frequency parquet from an earlier (non-split) run.
        tasks = []
        for fv in freqs_candidates:
            freq_only = os.path.join(OUT, f"{did}__{fv}.parquet")
            if os.path.exists(freq_only):
                try:
                    with lock:
                        acc["obs"] += pq.ParquetFile(freq_only).metadata.num_rows
                except Exception:
                    pass
                done.append(fv)
                continue
            for sv in split_vals:
                safe_sv = sv.replace("/", "_").replace(":", "_").replace("\\", "_")
                out_path = os.path.join(OUT, f"{did}__{fv}__{safe_sv}.parquet")
                parts = ["" for _ in range(ndim)]
                parts[fi] = fv
                parts[split_idx] = sv
                tasks.append((fv, sv, _key_from_parts(parts), out_path))

        ok_by_fv = {}
        with ThreadPoolExecutor(max_workers=CHUNK_WORKERS) as ex:
            futs = {ex.submit(_pull_one_chunk, agency, did, ver, key, out_path, acc, lock):
                    (fv, sv) for (fv, sv, key, out_path) in tasks}
            for fut in as_completed(futs):
                fv, sv = futs[fut]
                try:
                    st = fut.result()
                except Exception:  # noqa: BLE001
                    st = "err"
                if st in ("ok", "have"):
                    ok_by_fv[fv] = True
                elif st == "err":
                    err.append(f"{fv}/{sv}")
        for fv in freqs_candidates:
            if fv in done:
                continue
            if ok_by_fv.get(fv):
                done.append(fv)
            elif any(t[0] == fv for t in tasks):
                empty.append(fv)
    else:
        for fv in freqs_candidates:
            out_path = os.path.join(OUT, f"{did}__{fv}.parquet")
            parts = ["" for _ in range(ndim)]
            parts[fi] = fv
            st = _pull_one_chunk(agency, did, ver, _key_from_parts(parts), out_path, acc, lock)
            if st in ("ok", "have"):
                done.append(fv)
            elif st == "err":
                err.append(fv)
            else:
                empty.append(fv)

    status = "ok" if done else ("error" if err else "empty")
    return {
        "id": did, "agency": agency, "version": ver, "name": flow["name"],
        "status": status, "chunked": True,
        "split_dim": (ids[split_idx] if split_idx is not None else None),
        "n_obs": acc["obs"], "n_series": acc["series"], "parquet_bytes": acc["pq"],
        "freqs": done, "chunks_done": done, "chunks_empty": empty, "chunks_err": err,
        "start": acc["start"], "end": acc["end"],
        "dur_s": round(time.time() - t0, 1),
    }


def already_done(did: str) -> bool:
    if os.path.exists(os.path.join(OUT, did + ".parquet")):
        return True
    return bool(_glob.glob(os.path.join(OUT, f"{did}__*.parquet")))


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    args = sys.argv[1:]
    os.makedirs(OUT, exist_ok=True)

    print("Enumerating BIS dataflows ...", flush=True)
    flows = fetch_dataflows()
    print(f"BIS published dataflows: {len(flows)} total", flush=True)

    with io.open(os.path.join(OUT, "_catalog.json"), "w", encoding="utf-8") as fh:
        json.dump({"total": len(flows), "dataflows": flows}, fh,
                  ensure_ascii=False, indent=1)

    if "--list" in args:
        for f in sorted(flows, key=lambda x: x["id"]):
            print(f"  {f['agency']:5} {f['id']:20} v{f['version']:6} "
                  f"dsd={f['dsd_id']:24} {f['name'][:50]}", flush=True)
        print(f"LIST: {len(flows)} dataflows", flush=True)
        return

    targets = list(flows)
    only = None
    if "--only" in args:
        only = set(args[args.index("--only") + 1].split(","))
        targets = [f for f in targets if f["id"] in only]
    skip_existing = "--skip-existing" in args

    # Order: non-giants first (banked fast), giants last; alpha within each group.
    targets.sort(key=lambda f: (1 if f["id"] in GIANTS else 0, f["id"]))

    n_giant = sum(1 for f in targets if f["id"] in GIANTS)
    print(f"Pulling {len(targets)} dataflows ({len(targets)-n_giant} simple + "
          f"{n_giant} freq-chunked giants)"
          + (f"; only={sorted(only)}" if only else "")
          + ("; skip-existing" if skip_existing else ""), flush=True)

    results = []
    tot_obs = tot_series = 0
    ok = empty = err = skipped = 0
    t_all = time.time()
    simple_exists = lambda d: os.path.exists(os.path.join(OUT, d + ".parquet"))

    for i, f in enumerate(targets, 1):
        did = f["id"]
        if skip_existing and (
            (did not in GIANTS and already_done(did))
            or (did in GIANTS and simple_exists(did))
        ):
            skipped += 1
            print(f"[{i}/{len(targets)}] {did}: SKIP (exists)", flush=True)
            continue
        if did in GIANTS:
            res = ingest_chunked(f)
        else:
            res = ingest_simple(f, os.path.join(OUT, did + ".parquet"))
        results.append(res)
        st = res["status"]
        if st == "ok":
            ok += 1
            tot_obs += res.get("n_obs", 0)
            tot_series += res.get("n_series", 0)
            extra = ""
            if res.get("chunked"):
                extra = f" chunks={res.get('chunks_done')} empty={res.get('chunks_empty')}"
                if res.get("chunks_err"):
                    extra += f" ERR={res.get('chunks_err')}"
                if res.get("split_dim"):
                    extra += f" split={res.get('split_dim')}"
            print(f"[{i}/{len(targets)}] {did}: ok series={res.get('n_series',0):,} "
                  f"obs={res.get('n_obs',0):,} {res.get('start')}..{res.get('end')} "
                  f"pq={res.get('parquet_bytes',0)/1e6:.1f}MB {res.get('dur_s')}s{extra}", flush=True)
        elif st == "empty":
            empty += 1
            print(f"[{i}/{len(targets)}] {did}: EMPTY ({res.get('note')})", flush=True)
        else:
            err += 1
            note = res.get("note") or res.get("chunks_err")
            print(f"[{i}/{len(targets)}] {did}: ERROR ({note})", flush=True)

        with io.open(MANIFEST, "w", encoding="utf-8") as fh:
            json.dump({
                "source_id": "bis", "license_id": LICENSE_ID,
                "published_total": len(flows),
                "attempted": i, "ok": ok, "empty": empty,
                "error": err, "skipped": skipped,
                "total_observations": tot_obs, "total_series": tot_series,
                "results": results,
            }, fh, ensure_ascii=False, indent=1)

    dur = time.time() - t_all
    print("=" * 70, flush=True)
    print(f"DONE in {dur/60:.1f} min: ok={ok} empty={empty} error={err} skipped={skipped}", flush=True)
    print(f"TOTAL: {tot_series:,} series / {tot_obs:,} observations", flush=True)
    print(f"Manifest: {MANIFEST}", flush=True)


if __name__ == "__main__":
    main()
