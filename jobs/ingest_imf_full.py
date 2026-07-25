#!/usr/bin/env python3
"""FULL-COVERAGE ingest of IMF Data via the new SDMX 2.1 REST API (api.imf.org).

Enumerates EVERY dataflow, then pulls each one fully via SDMX-CSV and writes ONE
grouped Parquet per dataflow (or per dataflow+frequency for the giants) to
data/clean_full/imf/<DATAFLOW_ID>[__<FREQ>].parquet with columns:
series_key (the joined SDMX dimension values), obs_date, value, freq.

Why SDMX-CSV + detail=dataonly + all-key:
  * The new API's JSON serializer throws JsonGenerationException on multi-series
    queries and SDMX-ML returns 500; SDMX-CSV (Accept application/vnd.sdmx.data+csv)
    is reliable.  (Confirmed against the legacy IMF connector + live probing.)
  * `/all?dimensionAtObservation=AllDimensions` returns the ENTIRE dataflow in one
    request (every country x indicator x frequency series).
  * `detail=dataonly` strips the bulky per-row metadata columns (FULL_DESCRIPTION,
    METHODOLOGY, ...) WITHOUT changing the observation count -- verified identical
    row counts (FDI: 70,848 obs both ways), ~28x smaller transfer.

GIANT dataflows (BOP, DIP, IIP, PIP, ...) are frequency-CHUNKED: we read the DSD
dimension order, locate FREQUENCY, and issue one positional-key request per
frequency value (e.g. BOP key '....A', '....Q', '....M'). Each chunk is a smaller,
independent, restart-safe request and lands in its own parquet part. (The IMF SDMX
server supports neither HTTP Range resume nor c[DIM]=val component filtering --
both verified to fail -- so positional keys are the chunking mechanism.)

Coverage policy:
  * Pull all 102 BASE (latest) dataflows = the current data.
  * SKIP the 91 *_VINTAGE dataflows: each is a point-in-time re-release snapshot of
    a base dataflow (e.g. BOP_2026_JAN_VINTAGE is a snapshot of BOP). Pulling every
    vintage would duplicate enormous amounts of identical series. --vintages adds them.

Ordering: small/medium dataflows first (banked quickly), known giants last, so a
process kill (the host OOM-kills the many concurrent ingest jobs periodically)
costs at most one chunk, never blocks the rest.

Memory is bounded: each HTTP body streams to a temp .gz, then parses row-by-row and
flushes to the Parquet writer in RecordBatches; no dataflow is held whole in RAM.

License: imf-terms (re-serveable; must disclose data is available free of charge).

Usage:
  python jobs/ingest_imf_full.py --list                 # enumerate catalog, no data
  python jobs/ingest_imf_full.py --only FDI,CPI         # just these dataflows
  python jobs/ingest_imf_full.py --skip-existing        # full run, resume
  python jobs/ingest_imf_full.py                        # full run (all 102 base)
  python jobs/ingest_imf_full.py --vintages             # also include vintage snapshots
"""
from __future__ import annotations

import csv
import datetime as dt
import gzip
import io
import json
import os
import sys
import tempfile
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
sys.path.insert(0, ROOT)

OUT = os.path.join(ROOT, "data", "clean_full", "imf")
MANIFEST = os.path.join(OUT, "_manifest.json")
LOGDIR = os.path.join(ROOT, "data")

UA = "Econ-Fin Data Library admin@hfdatalibrary.com"
CSVMIME = "application/vnd.sdmx.data+csv"
XMLMIME = "application/vnd.sdmx.structure+xml;version=2.1"
API = "https://api.imf.org/external/sdmx/2.1"
LICENSE_ID = "imf-terms"

NS = {
    "mes": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
    "str": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
    "com": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
}

# Dataflows known/expected to be very large -> frequency-chunk them.
# (Counterpart-economy positions, balance of payments / IIP, big monetary &
#  government-finance cubes, by-partner trade, big price/labor cubes.)
GIANTS = {
    "BOP", "BOP_AGG", "DIP", "IIP", "IIPCC", "PIP", "SPE", "IL", "IRFCL",
    "CPI", "PPI", "LS", "ITG", "IMTS", "FAS", "FSIBSIS", "FSIC", "FSICDM",
    "GFS_SOO", "GFS_BS", "GFS_COFOG", "GFS_SFCP", "GFS_SOEF", "GFS_SSUC",
    "QGFS", "MFS_CBS", "MFS_ODC", "MFS_OFC", "MFS_DC", "MFS_FC", "MFS_MA",
    "MFS_IR", "MFS_FMP", "QNEA", "ANEA", "NA_MAIN", "WEO", "EER", "ER",
    "GDD", "WORLD", "DIP", "PIP", "FA", "FD", "FAS", "NSDP", "SDG",
    "FSI_COUNTRY_METADATA_TABLE_2", "CCI", "CO2EEIEM", "AEA", "ANEA",
}

# MEGA = the truly huge cubes whose per-frequency chunk is still tens of MB.
# These get a SECOND-level split (frequency x low-cardinality dimension) so each
# request is only a few MB and completes before the host's process reaper fires.
MEGA = {
    "BOP", "BOP_AGG", "DIP", "IIP", "IIPCC", "PIP", "SPE", "IL", "IRFCL",
    "CPI", "PPI", "LS", "ITG", "IMTS", "FAS",
    "FSIBSIS", "FSIC", "FSICDM",
    "GFS_SOO", "GFS_BS", "GFS_COFOG", "GFS_SFCP", "GFS_SOEF", "GFS_SSUC", "QGFS",
    "MFS_CBS", "MFS_ODC", "MFS_OFC", "MFS_DC", "MFS_FC", "MFS_MA", "MFS_FMP",
    "QNEA", "NA_MAIN", "WEO", "EER", "ER", "GDD", "WORLD", "NSDP",
}

csv.field_size_limit(min(sys.maxsize, 2_147_483_647))


# --------------------------------------------------------------------------- #
# HTTP helpers (retry/backoff, polite UA)
# --------------------------------------------------------------------------- #
def _open(url: str, accept: str, timeout: int = 600):
    last = None
    for attempt in range(5):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": accept})
            return urllib.request.urlopen(req, timeout=timeout)
        except urllib.error.HTTPError as e:
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
    """Enumerate EVERY dataflow in the IMF SDMX registry."""
    url = f"{API}/dataflow"
    with _open(url, XMLMIME) as r:
        raw = r.read()
    root = ET.fromstring(raw)
    out = []
    for f in root.findall(".//str:Dataflow", NS):
        did = f.get("id")
        nm = f.find("com:Name", NS)
        out.append({
            "agency": f.get("agencyID"),
            "id": did,
            "version": f.get("version"),
            "name": (nm.text if nm is not None else "") or "",
            "is_vintage": "VINTAGE" in (did or "").upper(),
        })
    return out


def fetch_dim_order(agency: str, did: str, ver: str):
    """Return (dim_ids_in_key_order, freq_index_or_None) from the DSD."""
    url = f"{API}/dataflow/{agency}/{did}/{ver}?references=datastructure"
    try:
        with _open(url, XMLMIME) as r:
            raw = r.read()
    except Exception:
        return None, None
    try:
        root = ET.fromstring(raw)
    except ET.ParseError:
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
        if d in ("FREQUENCY", "FREQ"):
            fi = i
            break
    return ids, fi


def fetch_member_values(agency: str, did: str, ver: str, dim_index: int):
    """Return the list of code values actually present for one dimension of a
    dataflow, via the availableconstraint (cheap; lists members in use).

    Falls back to None on any error so the caller can skip sub-chunking.
    """
    url = (f"{API}/availableconstraint/{agency},{did},{ver}"
           f"?references=none&mode=exact")
    try:
        with _open(url, XMLMIME, timeout=120) as r:
            raw = r.read()
        root = ET.fromstring(raw)
    except Exception:
        return None
    # Find the KeyValue blocks; the one at position dim_index (by order) holds
    # this dimension's values. We match by the dimension id from the DSD order.
    ids, _ = fetch_dim_order(agency, did, ver)
    if not ids or dim_index >= len(ids):
        return None
    want_id = ids[dim_index]
    vals = []
    for kv in root.findall(".//com:KeyValue", NS) + root.findall(".//str:KeyValue", NS):
        if kv.get("id") == want_id:
            for v in kv.findall("com:Value", NS) + kv.findall("str:Value", NS):
                if v.text:
                    vals.append(v.text.strip())
            break
    return vals or None


# --------------------------------------------------------------------------- #
# TIME_PERIOD parsing -- handles every SDMX period style IMF emits.
# --------------------------------------------------------------------------- #
def parse_period(tp: str):
    tp = (tp or "").strip()
    if not tp:
        return None, None
    try:
        if len(tp) == 4 and tp.isdigit():
            return dt.date(int(tp), 12, 31), "A"
        if "-M" in tp:
            y, m = tp.split("-M")
            return dt.date(int(y), int(m), 1), "M"
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
# Download + parse one query (whole dataflow, or one frequency chunk)
# --------------------------------------------------------------------------- #
SCHEMA = pa.schema([
    ("series_key", pa.string()),
    ("obs_date", pa.date32()),
    ("value", pa.float64()),
    ("freq", pa.string()),
])
BATCH = 250_000


def stream_query_to_tmp(agency: str, did: str, ver: str, key: str = "all"):
    """Stream one SDMX-CSV query to a temp .gz. Returns (tmp_path, dl_bytes)."""
    sel = f"{agency},{did},{ver}"
    url = (f"{API}/data/{sel}/{key}"
           f"?dimensionAtObservation=AllDimensions&detail=dataonly")
    with _open(url, CSVMIME) as r:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv.gz", dir=LOGDIR)
        nbytes = 0
        with gzip.open(tmp, "wb") as gz:
            while True:
                chunk = r.read(1 << 20)
                if not chunk:
                    break
                nbytes += len(chunk)
                gz.write(chunk)
        tmp.close()
    return tmp.name, nbytes


def parse_tmp_to_parquet(tmp: str, out_path: str):
    """Parse a temp .gz SDMX-CSV into a grouped parquet. Returns stats dict.

    Generic: dimension columns = everything between DATAFLOW(col 0) and
    TIME_PERIOD; series_key = their join with '.'.
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
        dim_idx = [i for i in range(1, tpi) if i != ovi]
        freq_col = None
        for cand in ("FREQUENCY", "FREQ"):
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
        if e.code in (400, 404):
            return {"id": did, "status": "empty", "n_obs": 0, "n_series": 0,
                    "note": f"HTTP {e.code}"}
        return {"id": did, "status": "error", "n_obs": 0, "n_series": 0,
                "note": f"HTTP {e.code}"}
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
    res["id"] = did
    res["agency"] = agency
    res["version"] = ver
    res["name"] = flow["name"]
    res["dl_bytes"] = dlb
    res["dur_s"] = round(time.time() - t0, 1)
    res.setdefault("n_obs", 0)
    res.setdefault("n_series", 0)
    return res


def _pull_one_chunk(agency, did, ver, key, out_path, acc):
    """Download+parse ONE positional-key query into out_path. Resumes if the
    parquet already exists. Updates the `acc` stats dict in place.
    Returns one of: 'have' | 'ok' | 'empty' | 'err'.
    """
    if os.path.exists(out_path):
        try:
            acc["obs"] += pq.ParquetFile(out_path).metadata.num_rows
        except Exception:
            pass
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
        acc["obs"] += res["n_obs"]
        acc["series"] += res["n_series"]
        acc["pq"] += res["parquet_bytes"]
        if acc["start"] is None or res["start"] < acc["start"]:
            acc["start"] = res["start"]
        if acc["end"] is None or res["end"] > acc["end"]:
            acc["end"] = res["end"]
        return "ok"
    return "empty"


def ingest_chunked(flow: dict, freqs_candidates=("A", "Q", "M", "D", "W", "S", "H", "B")):
    """Chunk a giant dataflow into smaller, independent, restart-safe requests.

    * Regular giants: one request per FREQUENCY value -> <DID>__<FREQ>.parquet.
    * MEGA giants (huge counterpart/BOP/IIP cubes): two-level split, one request
      per FREQUENCY x <split-dim value> -> <DID>__<FREQ>__<VAL>.parquet, so each
      request is ~a few MB (survives the host's periodic process reaper, which
      kills long heavy downloads). The split-dim is the lowest-cardinality
      non-frequency dimension (looked up via availableconstraint), capped so we
      never explode into hundreds of requests.

    Every chunk part already on disk is skipped, so each restart resumes exactly
    where the last one was killed.
    """
    agency, did, ver = flow["agency"], flow["id"], flow["version"]
    t0 = time.time()
    ids, fi = fetch_dim_order(agency, did, ver)
    if ids is None or fi is None:
        return ingest_simple(flow, os.path.join(OUT, did + ".parquet"))
    ndim = len(ids)

    acc = {"obs": 0, "series": 0, "pq": 0, "start": None, "end": None}
    done, empty, err = [], [], []

    # Decide split dimension for MEGA dataflows.
    split_idx = None
    split_vals = None
    if did in MEGA:
        # candidate non-freq dimensions, prefer a known low-card one
        cand_order = [i for i in range(ndim) if i != fi]
        # try each candidate; pick the first whose member list is small (<=40)
        for ci in cand_order:
            vals = fetch_member_values(agency, did, ver, ci)
            if vals and 2 <= len(vals) <= 40:
                split_idx, split_vals = ci, vals
                break

    for fv in freqs_candidates:
        if split_idx is not None:
            # If a prior freq-only chunk exists for this frequency, keep it and
            # skip the sub-chunks (avoid duplicating that frequency's data).
            freq_only = os.path.join(OUT, f"{did}__{fv}.parquet")
            if os.path.exists(freq_only):
                try:
                    acc["obs"] += pq.ParquetFile(freq_only).metadata.num_rows
                except Exception:
                    pass
                done.append(fv)
                continue
            # two-level: FREQ x split value
            any_for_fv = False
            for sv in split_vals:
                safe_sv = sv.replace("/", "_").replace(":", "_")
                out_path = os.path.join(OUT, f"{did}__{fv}__{safe_sv}.parquet")
                parts = ["" for _ in range(ndim)]
                parts[fi] = fv
                parts[split_idx] = sv
                st = _pull_one_chunk(agency, did, ver, ".".join(parts), out_path, acc)
                if st in ("ok", "have"):
                    any_for_fv = True
                elif st == "err":
                    err.append(f"{fv}/{sv}")
            if any_for_fv:
                done.append(fv)
            else:
                empty.append(fv)
        else:
            out_path = os.path.join(OUT, f"{did}__{fv}.parquet")
            parts = ["" for _ in range(ndim)]
            parts[fi] = fv
            st = _pull_one_chunk(agency, did, ver, ".".join(parts), out_path, acc)
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
        "freqs": done, "chunks_done": done,
        "chunks_empty": empty, "chunks_err": err,
        "start": acc["start"], "end": acc["end"],
        "dur_s": round(time.time() - t0, 1),
    }


def already_done(did: str) -> bool:
    """True if a simple parquet OR any frequency-chunk parquet exists."""
    if os.path.exists(os.path.join(OUT, did + ".parquet")):
        return True
    import glob as _g
    return bool(_g.glob(os.path.join(OUT, f"{did}__*.parquet")))


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    args = sys.argv[1:]
    os.makedirs(OUT, exist_ok=True)

    print("Enumerating IMF dataflows ...", flush=True)
    flows = fetch_dataflows()
    base = [f for f in flows if not f["is_vintage"]]
    vint = [f for f in flows if f["is_vintage"]]
    print(f"IMF published dataflows: {len(flows)} total "
          f"= {len(base)} base + {len(vint)} vintage snapshots", flush=True)

    with io.open(os.path.join(OUT, "_catalog.json"), "w", encoding="utf-8") as fh:
        json.dump({"total": len(flows), "base": base, "vintage": vint}, fh,
                  ensure_ascii=False, indent=1)

    if "--list" in args:
        for f in sorted(base, key=lambda x: (x["agency"], x["id"])):
            print(f"  {f['agency']:10} {f['id']:34} v{f['version']:8} {f['name'][:50]}", flush=True)
        print(f"LIST: {len(base)} base dataflows (+{len(vint)} vintages skipped by default)", flush=True)
        return

    include_vint = "--vintages" in args
    targets = list(base) + (vint if include_vint else [])

    only = None
    if "--only" in args:
        only = set(args[args.index("--only") + 1].split(","))
        targets = [f for f in targets if f["id"] in only]

    skip_existing = "--skip-existing" in args

    # Order: non-giants first (banked fast), giants last; alpha within each group.
    def sort_key(f):
        return (1 if f["id"] in GIANTS else 0, f["id"])
    targets.sort(key=sort_key)

    n_giant = sum(1 for f in targets if f["id"] in GIANTS)
    print(f"Pulling {len(targets)} dataflows ({len(targets)-n_giant} simple + "
          f"{n_giant} freq-chunked giants); "
          f"{'incl' if include_vint else 'excl'} vintages"
          + (f"; only={sorted(only)}" if only else "")
          + ("; skip-existing" if skip_existing else ""), flush=True)

    results = []
    tot_obs = tot_series = 0
    ok = empty = err = skipped = 0
    t_all = time.time()

    simple_exists = lambda d: os.path.exists(os.path.join(OUT, d + ".parquet"))
    for i, f in enumerate(targets, 1):
        did = f["id"]
        # Skip if: (a) non-giant already done, or (b) giant that already has a
        # complete SIMPLE parquet from a prior whole-dataflow run. Giants without
        # a simple parquet fall through to chunked ingest, which itself resumes by
        # skipping chunk parts already on disk.
        if skip_existing and (
            (did not in GIANTS and already_done(did))
            or (did in GIANTS and simple_exists(did))
        ):
            skipped += 1
            print(f"[{i}/{len(targets)}] {did}: SKIP (exists)", flush=True)
            continue
        is_giant = did in GIANTS
        if is_giant:
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

        if i % 5 == 0 or i == len(targets):
            with io.open(MANIFEST, "w", encoding="utf-8") as fh:
                json.dump({
                    "published_total": len(flows),
                    "base_total": len(base),
                    "vintage_total": len(vint),
                    "included_vintages": include_vint,
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
