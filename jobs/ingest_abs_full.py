#!/usr/bin/env python3
"""FULL-COVERAGE grouped ingest of the Australian Bureau of Statistics (ABS) SDMX API.

Catalog  : https://data.api.abs.gov.au/rest/dataflow/ABS?detail=allstubs  (~1223 flows)
Data pull: https://data.api.abs.gov.au/rest/data/<FLOW>/all?format=csvfile (compact, codes only)

GROUPED storage (mirrors jobs/ingest_eurostat.py): ONE Parquet per dataflow under
    data/clean_full/abs/<FLOW>.parquet
with columns (series_key, obs_date, value) -- many series inside each file. The
series_key is the dotted SDMX data key: every dimension code column between the
DATAFLOW column and TIME_PERIOD, joined with '.'. This matches the existing
connectors/abs/connector.py key convention (e.g. "1.10001.10.50.Q").

The ABS compact CSV (format=csvfile) is:
    DATAFLOW, <dim1>, <dim2>, ..., TIME_PERIOD, OBS_VALUE, <attributes...>
Column positions vary per flow, so we locate TIME_PERIOD / OBS_VALUE by name and
treat every column strictly between DATAFLOW (col 0) and TIME_PERIOD as a dimension.

License = cc-by-4.0 (reservable id from configs/sources.yaml -> sources.abs.license).

Resilience: the ABS API is flaky. Each dataflow is streamed and parsed independently;
network errors retry with exponential backoff; a 404 / empty body is treated as
"no data for this flow" and skipped. Already-written .parquet files are skipped so
the job is fully resumable after a crash. A flow whose download exceeds a size guard
is split by its FREQ (or first) dimension and written as one Parquet per split value.

Usage:
  python jobs/ingest_abs_full.py --list           # just enumerate the catalog
  python jobs/ingest_abs_full.py --dry 5           # parse 5 flows, print, no writes
  python jobs/ingest_abs_full.py                   # full run (resumable)
  python jobs/ingest_abs_full.py --only CPI,LF     # restrict to named flows
"""
from __future__ import annotations

import csv
import datetime as dt
import os
import sys
import time
import xml.etree.ElementTree as ET
from calendar import monthrange
from typing import Iterator, Optional

import requests
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
sys.path.insert(0, ROOT)

OUT = os.path.join(ROOT, "data", "clean_full", "abs")
META_DIR = os.path.join(ROOT, "data", "clean_full")
UA = "Econ-Fin Data Library admin@hfdatalibrary.com"
BASE = "https://data.api.abs.gov.au/rest"
LICENSE_ID = "cc-by-4.0"

csv.field_size_limit(min(sys.maxsize, 2_147_483_647))

SDMX_NS = {
    "s": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
    "c": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
    "m": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
}


def session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    return s


def parse_period(p: str) -> Optional[dt.date]:
    """Map an SDMX TIME_PERIOD id to a period-END calendar date.

    Handles YYYY, YYYY-MM, YYYY-Qn, YYYY-Sn (semester), YYYY-Wnn (ISO week),
    and YYYY-MM-DD. Returns None for anything unrecognised.
    """
    p = p.strip()
    if not p:
        return None
    # SDMX time intervals carry a duration suffix, e.g. "2022-07-01/P1Y" or
    # "2022-07/P3M". Use the interval START instant (the part before '/').
    if "/" in p:
        p = p.split("/", 1)[0].strip()
        if not p:
            return None
    try:
        if "-Q" in p:
            y, q = p.split("-Q")
            m = int(q) * 3
            return dt.date(int(y), m, monthrange(int(y), m)[1])
        if "-S" in p:
            y, sx = p.split("-S")
            m = 6 if int(sx) == 1 else 12
            return dt.date(int(y), m, monthrange(int(y), m)[1])
        if "-W" in p:
            y, w = p.split("-W")
            return dt.date.fromisocalendar(int(y), int(w), 7)
        if "-" in p:
            parts = p.split("-")
            if len(parts) == 2:
                y, m = int(parts[0]), int(parts[1])
                return dt.date(y, m, monthrange(y, m)[1])
            if len(parts) == 3:
                return dt.date(int(parts[0]), int(parts[1]), int(parts[2]))
        if len(p) == 4 and p.isdigit():
            return dt.date(int(p), 12, 31)
    except (ValueError, IndexError):
        return None
    return None


def parse_value(cell: str) -> Optional[float]:
    v = cell.strip()
    if not v:
        return None
    try:
        return float(v)
    except ValueError:
        return None


def list_dataflows(s: requests.Session) -> list[dict]:
    """Enumerate the FULL ABS dataflow catalog -> [{id, version, name}, ...]."""
    for attempt in range(5):
        try:
            r = s.get(f"{BASE}/dataflow/ABS", params={"detail": "allstubs"}, timeout=180)
            r.raise_for_status()
            break
        except requests.RequestException as exc:
            if attempt == 4:
                raise
            print(f"  catalog fetch retry {attempt+1}: {exc}", flush=True)
            time.sleep(2 ** attempt + 1)
    root = ET.fromstring(r.content)
    flows = []
    for df in root.findall(".//s:Dataflow", SDMX_NS):
        fid = df.get("id")
        ver = df.get("version")
        name_el = df.find("c:Name", SDMX_NS)
        name = (name_el.text or "").strip() if name_el is not None else fid
        if fid:
            flows.append({"id": fid, "version": ver, "name": name})
    flows.sort(key=lambda d: d["id"])
    return flows


def stream_rows(s: requests.Session, flow: str, key: str = "all",
                params: Optional[dict] = None) -> Iterator[tuple[list[str], list[str]]]:
    """Yield (header, row) for a flow's compact CSV, streaming.

    `key` is the SDMX data key path segment ("all" or a dotted filter). Retries the
    initial connection; a 404 yields nothing (no data); other HTTP errors raise after
    retries. The first yielded item carries header==row==<header list> sentinel so the
    caller can read the header once; subsequent items are (header, datarow).
    """
    qp = dict(params or {})
    qp["format"] = "csvfile"
    url = f"{BASE}/data/{flow}/{key}"
    last_exc: Optional[Exception] = None
    for attempt in range(5):
        try:
            r = s.get(url, params=qp, stream=True, timeout=300)
        except requests.RequestException as exc:
            last_exc = exc
            time.sleep(2 ** attempt + 1)
            continue
        if r.status_code == 404:
            r.close()
            return
        if r.status_code in (429, 500, 502, 503, 504):
            last_exc = RuntimeError(f"HTTP {r.status_code}")
            r.close()
            time.sleep(2 ** attempt + 2)
            continue
        try:
            r.raise_for_status()
        except requests.RequestException as exc:
            r.close()
            last_exc = exc
            time.sleep(2 ** attempt + 1)
            continue
        # success -- stream line-by-line. iter_lines handles chunked/gzip decoding and
        # connection lifecycle correctly (wrapping r.raw in a TextIOWrapper races the
        # urllib3 connection release on large multi-chunk bodies -> "closed file").
        line_iter = r.iter_lines(decode_unicode=True, chunk_size=1024 * 1024)
        reader = csv.reader(line_iter)
        try:
            try:
                header = next(reader)
            except StopIteration:
                return
            yield header, header  # sentinel: header row
            for row in reader:
                yield header, row
        finally:
            r.close()
        return
    if last_exc:
        raise last_exc
    return


# Exceptions that mean "the HTTP stream broke mid-body" -- the whole pull must restart.
STREAM_BREAK = (
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ConnectionError,
    requests.exceptions.ReadTimeout,
    requests.exceptions.RequestException,
)


def collect(s: requests.Session, flow: str, key: str = "all",
            params: Optional[dict] = None):
    """Stream one (flow,key) compact CSV -> (keys, dates, vals) parallel lists.

    The ABS API frequently drops a connection mid-body (IncompleteRead). Because a
    partial parse is unusable, we retry the ENTIRE stream up to 5 times with backoff
    on any streaming break, rebuilding the lists from scratch each attempt.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(5):
        keys: list[str] = []
        dates: list[dt.date] = []
        vals: list[float] = []
        ti = vi = -1
        dimcols: list[int] = []
        first = True
        try:
            for hdr, row in stream_rows(s, flow, key, params):
                if first:
                    ti = hdr.index("TIME_PERIOD") if "TIME_PERIOD" in hdr else -1
                    vi = hdr.index("OBS_VALUE") if "OBS_VALUE" in hdr else -1
                    if ti < 0 or vi < 0:
                        return [], [], []          # unexpected shape -> nothing
                    dimcols = list(range(1, ti))   # between DATAFLOW(0) and TIME_PERIOD
                    first = False
                    continue
                if len(row) <= vi:
                    continue
                v = parse_value(row[vi])
                if v is None:
                    continue
                od = parse_period(row[ti])
                if od is None:
                    continue
                keys.append(".".join(row[c] for c in dimcols))
                dates.append(od)
                vals.append(v)
            return keys, dates, vals
        except STREAM_BREAK as exc:
            last_exc = exc
            time.sleep(2 ** attempt + 2)
            continue
        except Exception as exc:  # urllib3 ProtocolError etc. (not a requests subclass)
            if "IncompleteRead" in str(exc) or "Connection broken" in str(exc) or \
               "ProtocolError" in type(exc).__name__:
                last_exc = exc
                time.sleep(2 ** attempt + 2)
                continue
            raise
    if last_exc:
        raise last_exc
    return [], [], []


def dimension_codes(s: requests.Session, flow: str, version: Optional[str]):
    """Return (dim_ids, {dim_id: [codes]}) for a dataflow via its DSD.

    Used only to split oversized flows. Best-effort: on any failure returns ([], {}).
    """
    try:
        r = s.get(f"{BASE}/datastructure/ABS/{flow}", params={"references": "children"},
                  timeout=180)
        if r.status_code != 200:
            return [], {}
        root = ET.fromstring(r.content)
    except (requests.RequestException, ET.ParseError):
        return [], {}
    # dimension order
    dim_ids: list[str] = []
    for dl in root.findall(".//s:DimensionList", SDMX_NS):
        for d in dl:
            tag = d.tag.split("}")[-1]
            if tag in ("Dimension", "TimeDimension"):
                did = d.get("id")
                if did and did != "TIME_PERIOD":
                    dim_ids.append(did)
    # codelists
    codes: dict[str, list[str]] = {}
    cl_by_id: dict[str, list[str]] = {}
    for cl in root.findall(".//s:Codelist", SDMX_NS):
        clid = cl.get("id")
        vals = [c.get("id") for c in cl.findall("s:Code", SDMX_NS) if c.get("id")]
        cl_by_id[clid] = vals
    # map dimension -> codelist. The codelist reference sits at
    # Dimension/LocalRepresentation/Enumeration/Ref, and that <Ref> element carries NO
    # namespace prefix (unlike its structure-namespaced parents); its id is the
    # codelist id (e.g. CL_FREQ).
    for dl in root.findall(".//s:DimensionList", SDMX_NS):
        for d in dl:
            did = d.get("id")
            if not did or did == "TIME_PERIOD":
                continue
            ref = d.find(".//s:LocalRepresentation/s:Enumeration/Ref", SDMX_NS)
            if ref is None:
                ref = d.find(".//s:Enumeration/Ref", SDMX_NS)
            if ref is not None:
                clid = ref.get("id")
                if clid in cl_by_id:
                    codes[did] = cl_by_id[clid]
    return dim_ids, codes


def write_parquet(path: str, keys, dates, vals):
    tbl = pa.table({
        "series_key": pa.array(keys, type=pa.string()),
        "obs_date": pa.array(dates, type=pa.date32()),
        "value": pa.array(vals, type=pa.float64()),
    })
    pq.write_table(tbl, path, compression="zstd")


# If a whole-flow pull exceeds this many observations we re-fetch it split by a
# dimension, writing per-slice into a single combined Parquet, to bound peak memory.
# (Most ABS flows are < 1M obs; CPI ~ 0.32M. This guard only trips for huge tables.)
SPLIT_OBS = 8_000_000


def _split_fetch(s: requests.Session, flow: str, version: Optional[str], out_path: str):
    """Re-fetch an oversized flow split by FREQ (or first) dimension into one Parquet."""
    dim_ids, codes = dimension_codes(s, flow, version)
    split_dim = None
    if "FREQ" in dim_ids and codes.get("FREQ"):
        split_dim = "FREQ"
    else:
        for d in dim_ids:
            if codes.get(d):
                split_dim = d
                break
    if not split_dim:
        return 0, 0  # cannot split; give up on this flow rather than OOM
    pos = dim_ids.index(split_dim)
    writer = None
    n_obs = 0
    series: set[str] = set()
    try:
        for code in codes[split_dim]:
            key_parts = ["" for _ in dim_ids]
            key_parts[pos] = code
            k2, d2, v2 = collect(s, flow, ".".join(key_parts))
            if not k2:
                continue
            tbl = pa.table({
                "series_key": pa.array(k2, type=pa.string()),
                "obs_date": pa.array(d2, type=pa.date32()),
                "value": pa.array(v2, type=pa.float64()),
            })
            if writer is None:
                writer = pq.ParquetWriter(out_path, tbl.schema, compression="zstd")
            writer.write_table(tbl)
            n_obs += len(k2)
            series.update(k2)
    finally:
        if writer is not None:
            writer.close()
    return len(series), n_obs


def fetch_flow_grouped(s: requests.Session, flow: str, version: Optional[str],
                       out_path: str) -> tuple[int, int]:
    """Fetch one dataflow -> one grouped Parquet. Returns (n_series, n_obs).

    Streams the whole flow. If it turns out to be enormous (> SPLIT_OBS observations)
    we discard the in-memory lists and re-fetch split by a dimension, appending slices
    into a single Parquet so the on-disk layout stays one-file-per-flow.
    """
    keys, dates, vals = collect(s, flow, "all")
    if not keys:
        return 0, 0
    if len(keys) > SPLIT_OBS:
        del keys, dates, vals
        ns, no = _split_fetch(s, flow, version, out_path)
        if no:
            return ns, no
        # split unavailable/empty -> fall back to the whole pull we already validated
        keys, dates, vals = collect(s, flow, "all")
        if not keys:
            return 0, 0
    write_parquet(out_path, keys, dates, vals)
    return len(set(keys)), len(keys)


def main() -> None:
    args = sys.argv[1:]
    do_list = "--list" in args
    dry = "--dry" in args
    limit = None
    if dry:
        i = args.index("--dry")
        if i + 1 < len(args) and args[i + 1].isdigit():
            limit = int(args[i + 1])
    only = None
    if "--only" in args:
        only = set(args[args.index("--only") + 1].split(","))

    s = session()
    print("Enumerating ABS dataflow catalog ...", flush=True)
    flows = list_dataflows(s)
    print(f"Catalog: {len(flows)} dataflows", flush=True)
    if only:
        flows = [f for f in flows if f["id"] in only]
        print(f"Restricted to {len(flows)} flows: {sorted(only)}", flush=True)
    if do_list:
        for f in flows:
            print(f"  {f['id']:45} v{f['version']}  {f['name'][:60]}")
        return
    if limit:
        flows = flows[:limit]

    if not dry:
        os.makedirs(OUT, exist_ok=True)

    catalog_rows = []   # one coarse row per dataflow for a sidecar metadata file
    n_ds = n_series = n_obs = 0
    skipped = 0
    empty = 0
    errors: list[str] = []
    t_start = time.time()

    for idx, f in enumerate(flows, 1):
        flow = f["id"]
        out_path = os.path.join(OUT, f"{flow}.parquet")
        if not dry and os.path.exists(out_path):
            # resume: trust existing file, but record its stats for the catalog sidecar
            try:
                md = pq.read_metadata(out_path)
                rows = md.num_rows
            except Exception:
                rows = -1
            n_ds += 1
            n_obs += max(rows, 0)
            skipped += 1
            if idx % 50 == 0:
                print(f"[{idx}/{len(flows)}] (resume) {flow}: {rows} rows", flush=True)
            continue

        try:
            if dry:
                keys, dates, vals = collect(s, flow, "all")
                uniq = len(set(keys))
                if keys:
                    print(f"[{idx}] {flow:40} series={uniq:>7,} obs={len(keys):>9,} "
                          f"sample=({keys[0]}, {dates[0]}, {vals[0]})", flush=True)
                    n_series += uniq
                    n_obs += len(keys)
                    n_ds += 1
                else:
                    print(f"[{idx}] {flow:40} EMPTY/no-data", flush=True)
                    empty += 1
                continue

            ns, no = fetch_flow_grouped(s, flow, f["version"], out_path)
            if no == 0:
                empty += 1
                if idx % 25 == 0:
                    print(f"[{idx}/{len(flows)}] {flow}: empty", flush=True)
                continue
            n_ds += 1
            n_series += ns
            n_obs += no
            catalog_rows.append({"flow": flow, "name": f["name"],
                                 "version": f["version"], "n_series": ns, "n_obs": no})
            rate = n_obs / max(time.time() - t_start, 1)
            print(f"[{idx}/{len(flows)}] {flow:40} series={ns:>7,} obs={no:>9,} "
                  f"| total_obs={n_obs:,} ({rate:,.0f}/s)", flush=True)
        except Exception as exc:  # noqa: BLE001
            msg = f"{flow}: {type(exc).__name__}: {exc}"
            errors.append(msg)
            print(f"[{idx}/{len(flows)}] ERROR {msg}", flush=True)
            continue

    elapsed = time.time() - t_start
    print("=" * 70, flush=True)
    print(f"{'DRY' if dry else 'DONE'}: flows_written={n_ds:,} (skipped/resumed={skipped}) "
          f"empty={empty} errors={len(errors)} series={n_series:,} obs={n_obs:,} "
          f"in {elapsed/60:.1f} min", flush=True)
    if errors:
        print("ERRORS:", flush=True)
        for e in errors[:50]:
            print("  ", e, flush=True)

    if not dry and catalog_rows:
        # sidecar catalog (Parquet) summarising each dataflow group
        cat = pa.table({
            "flow": [r["flow"] for r in catalog_rows],
            "name": [r["name"] for r in catalog_rows],
            "version": [r["version"] for r in catalog_rows],
            "n_series": [r["n_series"] for r in catalog_rows],
            "n_obs": [r["n_obs"] for r in catalog_rows],
            "license_id": [LICENSE_ID] * len(catalog_rows),
        })
        pq.write_table(cat, os.path.join(META_DIR, "abs_catalog.parquet"))
        print(f"Wrote sidecar catalog: {len(catalog_rows)} flow rows", flush=True)


if __name__ == "__main__":
    main()
