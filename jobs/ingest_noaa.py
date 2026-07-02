#!/usr/bin/env python3
"""Full-coverage ingest of NOAA NCEI GSOM + GSOY climate summaries.

Source: NOAA NCEI bulk archive
  GSOM (Global Summary of the Month):  https://www.ncei.noaa.gov/data/gsom/access/
  GSOY (Global Summary of the Year):   https://www.ncei.noaa.gov/data/gsoy/access/
License: us-public-domain.  Attribution: "Source: NOAA (public domain)".

The bulk archive publishes ONE wide CSV per weather station (STATION, DATE,
LATITUDE, LONGITUDE, ELEVATION, NAME, then ~55 climate elements each paired
with an <ELEM>_ATTRIBUTES column).  Counts (enumerated 2026-06):
  GSOM: 127,905 station CSVs    GSOY: 86,018 station CSVs

GROUPED storage (anti-bloat -- NEVER one file per station):
  We melt each wide CSV to LONG form
      (dataset, station, series_key, obs_date, element, value, attributes)
  and accumulate rows into SHARDS keyed by the 2-char GHCN country/network
  prefix of the station id (US, AS, CA, ...).  Output:
      data/clean_full/noaa/gsom__<PREFIX>.parquet      (obs, many stations)
      data/clean_full/noaa/gsom__<PREFIX>__series.parquet  (series meta)
  ~250 prefixes x 2 datasets + sidecars => a few hundred files total.
  series_key = "<station>:<element>"  (one series per station x element).

Stages (resumable):
  --enumerate   (re)fetch the station-id manifests + station metadata
  --download    fetch every station CSV (skips files already on disk)
  --build       parse raw CSVs -> grouped long parquet shards + verify
  --verify      re-read shards and print obs/series/coverage report
Default (no flag) = --build.

Usage:
  python jobs/ingest_noaa.py --download
  python jobs/ingest_noaa.py --download --dataset gsoy --workers 6
  python jobs/ingest_noaa.py --build
  python jobs/ingest_noaa.py --verify
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import glob
import io
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = r"D:/research/econfindatalibrary"
RAW = os.path.join(ROOT, "data", "raw", "noaa")
OUT = os.path.join(ROOT, "data", "clean_full", "noaa")
UA = "Econ-Fin Data Library admin@hfdatalibrary.com"
LICENSE_ID = "us-public-domain"
ATTRIBUTION = "Source: NOAA (public domain)"
HOMEPAGE = "https://www.ncei.noaa.gov/cdo-web/"

DATASETS = {
    "gsom": {
        "access": "https://www.ncei.noaa.gov/data/gsom/access/",
        "freq": "M",
    },
    "gsoy": {
        "access": "https://www.ncei.noaa.gov/data/gsoy/access/",
        "freq": "A",
    },
}

# Non-element columns in every station CSV.
META_COLS = {"STATION", "DATE", "LATITUDE", "LONGITUDE", "ELEVATION", "NAME"}


# --------------------------------------------------------------------------- #
# session
# --------------------------------------------------------------------------- #
def make_session():
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    sess = requests.Session()
    retry = Retry(total=5, backoff_factor=1.5,
                  status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET"], respect_retry_after_header=True)
    ad = HTTPAdapter(max_retries=retry, pool_connections=16, pool_maxsize=16)
    sess.mount("https://", ad)
    sess.mount("http://", ad)
    sess.headers.update({"User-Agent": UA})
    return sess


# --------------------------------------------------------------------------- #
# enumerate
# --------------------------------------------------------------------------- #
def enumerate_ids(ds: str, sess) -> list[str]:
    import re
    url = DATASETS[ds]["access"]
    r = sess.get(url, timeout=180)
    r.raise_for_status()
    ids = sorted(set(re.findall(r'href="([^"/]+)\.csv"', r.text)))
    out = os.path.join(RAW, f"{ds}_station_ids.txt")
    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(ids) + "\n")
    print(f"  {ds}: {len(ids):,} station ids -> {out}", flush=True)
    return ids


def enumerate_all():
    os.makedirs(RAW, exist_ok=True)
    os.makedirs(os.path.join(RAW, "doc"), exist_ok=True)
    sess = make_session()
    counts = {}
    for ds in DATASETS:
        counts[ds] = len(enumerate_ids(ds, sess))
    # GHCN station metadata (lat/lon/elev/name) -- enrich series meta
    meta_url = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"
    try:
        r = sess.get(meta_url, timeout=180)
        r.raise_for_status()
        with open(os.path.join(RAW, "doc", "ghcnd-stations.txt"), "w", encoding="utf-8") as f:
            f.write(r.text)
        print(f"  ghcnd-stations.txt: {r.text.count(chr(10)):,} rows", flush=True)
    except Exception as e:  # noqa: BLE001
        print(f"  ghcnd-stations.txt fetch failed: {e}", flush=True)
    with open(os.path.join(RAW, "_manifest.json"), "w", encoding="utf-8") as f:
        json.dump({"counts": counts, "enumerated_at": dt.datetime.now().isoformat(timespec="seconds")},
                  f, indent=2)
    return counts


def load_ids(ds: str) -> list[str]:
    path = os.path.join(RAW, f"{ds}_station_ids.txt")
    if not os.path.exists(path):
        raise SystemExit(f"missing {path} -- run --enumerate first")
    with open(path, encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip()]


# --------------------------------------------------------------------------- #
# download (resumable, concurrent)
# --------------------------------------------------------------------------- #
_dl_lock = threading.Lock()
_dl_state = {"ok": 0, "skip": 0, "fail": 0, "bytes": 0, "done": 0}


def _dl_one(ds: str, sid: str, sess) -> tuple[str, str]:
    """Download one station CSV. Returns (status, sid). Skips if already present."""
    dest_dir = os.path.join(RAW, ds)
    dest = os.path.join(dest_dir, f"{sid}.csv")
    if os.path.exists(dest) and os.path.getsize(dest) > 0:
        with _dl_lock:
            _dl_state["skip"] += 1
            _dl_state["done"] += 1
        return ("skip", sid)
    url = DATASETS[ds]["access"] + f"{sid}.csv"
    tmp = dest + ".part"
    for attempt in range(4):
        try:
            r = sess.get(url, timeout=120)
            if r.status_code == 404:
                with _dl_lock:
                    _dl_state["fail"] += 1
                    _dl_state["done"] += 1
                return ("404", sid)
            r.raise_for_status()
            data = r.content
            with open(tmp, "wb") as f:
                f.write(data)
            os.replace(tmp, dest)
            with _dl_lock:
                _dl_state["ok"] += 1
                _dl_state["bytes"] += len(data)
                _dl_state["done"] += 1
            return ("ok", sid)
        except Exception:  # noqa: BLE001
            time.sleep(1.5 * (attempt + 1))
    if os.path.exists(tmp):
        try:
            os.remove(tmp)
        except OSError:
            pass
    with _dl_lock:
        _dl_state["fail"] += 1
        _dl_state["done"] += 1
    return ("fail", sid)


def download(datasets: list[str], workers: int):
    workers = max(1, min(workers, 6))
    for ds in datasets:
        ids = load_ids(ds)
        os.makedirs(os.path.join(RAW, ds), exist_ok=True)
        total = len(ids)
        for k in _dl_state:
            _dl_state[k] = 0
        print(f"DOWNLOAD {ds}: {total:,} stations, workers={workers} -> {os.path.join(RAW, ds)}",
              flush=True)
        t0 = time.time()
        failures = []
        # one session per worker (thread-safe-ish: requests.Session is OK for
        # concurrent GETs with a pooled adapter, but give each thread its own).
        local = threading.local()

        def task(sid, _ds=ds):
            if not hasattr(local, "sess"):
                local.sess = make_session()
            return _dl_one(_ds, sid, local.sess)

        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(task, sid) for sid in ids]
            for fut in as_completed(futs):
                status, sid = fut.result()
                if status in ("fail", "404"):
                    failures.append((sid, status))
                d = _dl_state["done"]
                if d % 2000 == 0 or d == total:
                    el = time.time() - t0
                    rate = d / el if el else 0
                    eta = (total - d) / rate if rate else 0
                    print(f"  {ds} {d:,}/{total:,}  ok={_dl_state['ok']:,} "
                          f"skip={_dl_state['skip']:,} fail={_dl_state['fail']:,} "
                          f"{_dl_state['bytes']/1e6:.0f}MB  {rate:.0f}/s  ETA {eta/60:.0f}m",
                          flush=True)
        # persist failures for retry
        fpath = os.path.join(RAW, f"_{ds}_failures.json")
        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(failures, f)
        el = time.time() - t0
        print(f"DONE {ds}: ok={_dl_state['ok']:,} skip={_dl_state['skip']:,} "
              f"fail={_dl_state['fail']:,} in {el/60:.1f}m  failures->{fpath}", flush=True)


# --------------------------------------------------------------------------- #
# build: melt wide CSV -> long shards by 2-char prefix
# --------------------------------------------------------------------------- #
def _parse_date(ds: str, s: str):
    """GSOM DATE='YYYY-MM' -> first of month; GSOY DATE='YYYY' -> Dec 31."""
    s = s.strip()
    try:
        if ds == "gsom":
            y, m = s.split("-")
            return dt.date(int(y), int(m), 1)
        # gsoy
        if len(s) >= 4 and s[:4].isdigit():
            return dt.date(int(s[:4]), 12, 31)
    except (ValueError, IndexError):
        return None
    return None


def _to_float(x: str):
    x = x.strip()
    if not x:
        return None
    try:
        return float(x)
    except ValueError:
        return None


class ShardWriter:
    """Buffered long-form Parquet writer, one ParquetWriter per shard (prefix).

    Flushes a RecordBatch every `batch_rows` so memory stays bounded across
    ~14M+ rows.  Tracks per-shard obs counts and per-series (station x element)
    metadata for the sidecar.
    """

    SCHEMA = pa.schema([
        ("dataset", pa.string()),
        ("station", pa.string()),
        ("series_key", pa.string()),
        ("obs_date", pa.date32()),
        ("element", pa.string()),
        ("value", pa.float64()),
        ("attributes", pa.string()),
    ])

    def __init__(self, ds: str, batch_rows: int = 400_000):
        self.ds = ds
        self.batch_rows = batch_rows
        self.writers: dict[str, pq.ParquetWriter] = {}
        self.buffers: dict[str, dict] = {}
        self.n_obs: dict[str, int] = {}
        # series meta accumulators: key -> dict (per shard, list of rows)
        self.series_rows: dict[str, list] = {}
        # track per-series date span + count without holding all obs
        self._series_span: dict[tuple, list] = {}  # (prefix,station,element)->[n,min,max]

    def _buf(self, prefix):
        b = self.buffers.get(prefix)
        if b is None:
            b = {"station": [], "series_key": [], "obs_date": [],
                 "element": [], "value": [], "attributes": []}
            self.buffers[prefix] = b
            self.n_obs[prefix] = 0
        return b

    def _writer(self, prefix):
        w = self.writers.get(prefix)
        if w is None:
            os.makedirs(OUT, exist_ok=True)
            path = os.path.join(OUT, f"{self.ds}__{prefix}.parquet")
            w = pq.ParquetWriter(path, self.SCHEMA, compression="zstd")
            self.writers[prefix] = w
        return w

    def add_obs(self, prefix, station, series_key, obs_date, element, value, attributes):
        b = self._buf(prefix)
        b["station"].append(station)
        b["series_key"].append(series_key)
        b["obs_date"].append(obs_date)
        b["element"].append(element)
        b["value"].append(value)
        b["attributes"].append(attributes)
        self.n_obs[prefix] += 1
        if len(b["station"]) >= self.batch_rows:
            self._flush(prefix)

    def bump_series(self, prefix, station, element, obs_date):
        k = (prefix, station, element)
        sp = self._series_span.get(k)
        if sp is None:
            self._series_span[k] = [1, obs_date, obs_date]
        else:
            sp[0] += 1
            if obs_date < sp[1]:
                sp[1] = obs_date
            if obs_date > sp[2]:
                sp[2] = obs_date

    def _flush(self, prefix):
        b = self.buffers.get(prefix)
        if not b or not b["station"]:
            return
        # Guard: truncate to the minimum length across all arrays to prevent
        # PyArrow length-mismatch errors if a CSV parse left one list 1-5 items
        # longer than the others (can happen on malformed stations).
        n = min(len(b[k]) for k in b)
        if n == 0:
            for key in b:
                b[key].clear()
            return
        batch = pa.record_batch({
            "dataset": pa.array([self.ds] * n, type=pa.string()),
            "station": pa.array(b["station"][:n], type=pa.string()),
            "series_key": pa.array(b["series_key"][:n], type=pa.string()),
            "obs_date": pa.array(b["obs_date"][:n], type=pa.date32()),
            "element": pa.array(b["element"][:n], type=pa.string()),
            "value": pa.array(b["value"][:n], type=pa.float64()),
            "attributes": pa.array(b["attributes"][:n], type=pa.string()),
        }, schema=self.SCHEMA)
        self._writer(prefix).write_batch(batch)
        for key in b:
            b[key].clear()

    def close(self, station_meta: dict):
        for prefix in list(self.buffers):
            self._flush(prefix)
        for w in self.writers.values():
            w.close()
        # write per-shard series sidecars
        shard_series: dict[str, list] = {}
        for (prefix, station, element), (n, dmin, dmax) in self._series_span.items():
            sm = station_meta.get(station, {})
            shard_series.setdefault(prefix, []).append({
                "dataset": self.ds,
                "series_key": f"{station}:{element}",
                "station": station,
                "element": element,
                "name": sm.get("name", ""),
                "latitude": sm.get("lat", ""),
                "longitude": sm.get("lon", ""),
                "elevation": sm.get("elev", ""),
                "country_code": prefix,
                "frequency": DATASETS[self.ds]["freq"],
                "n_obs": int(n),
                "start": str(dmin),
                "end": str(dmax),
            })
        for prefix, rows in shard_series.items():
            cols = {
                "dataset": pa.array([r["dataset"] for r in rows], pa.string()),
                "series_key": pa.array([r["series_key"] for r in rows], pa.string()),
                "station": pa.array([r["station"] for r in rows], pa.string()),
                "element": pa.array([r["element"] for r in rows], pa.string()),
                "name": pa.array([r["name"] for r in rows], pa.string()),
                "latitude": pa.array([r["latitude"] for r in rows], pa.string()),
                "longitude": pa.array([r["longitude"] for r in rows], pa.string()),
                "elevation": pa.array([r["elevation"] for r in rows], pa.string()),
                "country_code": pa.array([r["country_code"] for r in rows], pa.string()),
                "frequency": pa.array([r["frequency"] for r in rows], pa.string()),
                "n_obs": pa.array([r["n_obs"] for r in rows], pa.int64()),
                "start": pa.array([r["start"] for r in rows], pa.string()),
                "end": pa.array([r["end"] for r in rows], pa.string()),
            }
            pq.write_table(pa.table(cols),
                           os.path.join(OUT, f"{self.ds}__{prefix}__series.parquet"),
                           compression="zstd")
        return shard_series


def load_station_meta() -> dict:
    """Parse ghcnd-stations.txt (fixed width) -> {id: {lat,lon,elev,name}}."""
    path = os.path.join(RAW, "doc", "ghcnd-stations.txt")
    meta = {}
    if not os.path.exists(path):
        return meta
    with open(path, encoding="utf-8") as f:
        for line in f:
            if len(line) < 85:
                continue
            sid = line[0:11].strip()
            lat = line[12:20].strip()
            lon = line[21:30].strip()
            elev = line[31:37].strip()
            name = line[41:71].strip()
            meta[sid] = {"lat": lat, "lon": lon, "elev": elev, "name": name}
    return meta


def _melt_csv(ds: str, path: str, sid: str, prefix: str, sw: ShardWriter):
    """Parse one wide station CSV, emit long rows into the shard writer.

    Returns number of obs emitted for this station.
    """
    n = 0
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return 0
        # map column index -> element name; and element -> its _ATTRIBUTES index
        elem_idx = {}
        attr_idx = {}
        date_i = None
        for i, col in enumerate(header):
            c = col.strip()
            if c == "DATE":
                date_i = i
            elif c in META_COLS:
                continue
            elif c.endswith("_ATTRIBUTES"):
                attr_idx[c[:-len("_ATTRIBUTES")]] = i
            else:
                elem_idx[c] = i
        if date_i is None:
            return 0
        for row in reader:
            if len(row) <= date_i:
                continue
            od = _parse_date(ds, row[date_i])
            if od is None:
                continue
            for elem, ci in elem_idx.items():
                if ci >= len(row):
                    continue
                raw = row[ci]
                val = _to_float(raw)
                if val is None:
                    continue
                ai = attr_idx.get(elem)
                attrs = row[ai].strip() if (ai is not None and ai < len(row)) else ""
                sk = f"{sid}:{elem}"
                sw.add_obs(prefix, sid, sk, od, elem, val, attrs)
                sw.bump_series(prefix, sid, elem, od)
                n += 1
    return n


def build(datasets: list[str], limit: int | None = None):
    os.makedirs(OUT, exist_ok=True)
    station_meta = load_station_meta()
    print(f"BUILD: station_meta rows={len(station_meta):,}", flush=True)
    summary = {}
    for ds in datasets:
        files = sorted(glob.glob(os.path.join(RAW, ds, "*.csv")))
        if limit:
            files = files[:limit]
        n_files = len(files)
        print(f"BUILD {ds}: {n_files:,} station CSVs -> grouped long parquet", flush=True)
        sw = ShardWriter(ds)
        t0 = time.time()
        total_obs = 0
        n_done = 0
        n_empty = 0
        for path in files:
            sid = os.path.basename(path)[:-4]
            prefix = sid[:2].upper() if len(sid) >= 2 else "XX"
            try:
                got = _melt_csv(ds, path, sid, prefix, sw)
            except Exception as e:  # noqa: BLE001
                print(f"  parse error {sid}: {e}", flush=True)
                got = 0
            if got == 0:
                n_empty += 1
            total_obs += got
            n_done += 1
            if n_done % 5000 == 0 or n_done == n_files:
                el = time.time() - t0
                rate = n_done / el if el else 0
                eta = (n_files - n_done) / rate if rate else 0
                print(f"  {ds} {n_done:,}/{n_files:,} stations  obs={total_obs:,}  "
                      f"{rate:.0f} st/s  ETA {eta/60:.0f}m", flush=True)
        shard_series = sw.close(station_meta)
        n_series = sum(len(v) for v in shard_series.values())
        n_shards = len(shard_series)
        summary[ds] = {
            "n_stations_files": n_files,
            "n_stations_empty": n_empty,
            "n_obs_written": int(total_obs),
            "n_series": int(n_series),
            "n_shards": int(n_shards),
            "elapsed_min": round((time.time() - t0) / 60, 1),
        }
        print(f"DONE {ds}: stations={n_files:,} obs={total_obs:,} series={n_series:,} "
              f"shards={n_shards} in {summary[ds]['elapsed_min']}m", flush=True)
    with open(os.path.join(OUT, "_build_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    return summary


# --------------------------------------------------------------------------- #
# verify: re-read shards, count, build aggregate meta + coverage
# --------------------------------------------------------------------------- #
def verify(datasets: list[str]):
    report = {"source": "noaa", "license": LICENSE_ID, "attribution": ATTRIBUTION,
              "homepage": HOMEPAGE, "datasets": {}}
    grand_obs = 0
    grand_series = 0
    grand_files = 0
    for ds in datasets:
        obs_files = sorted(glob.glob(os.path.join(OUT, f"{ds}__*.parquet")))
        obs_files = [p for p in obs_files if not p.endswith("__series.parquet")]
        series_files = sorted(glob.glob(os.path.join(OUT, f"{ds}__*__series.parquet")))
        n_obs = 0
        n_series = 0
        stations = set()
        elements = set()
        dmin = None
        dmax = None
        for p in obs_files:
            pf = pq.ParquetFile(p)
            n_obs += pf.metadata.num_rows
        for p in series_files:
            t = pq.read_table(p, columns=["station", "element", "start", "end"])
            n_series += t.num_rows
            stations.update(t.column("station").to_pylist())
            elements.update(t.column("element").to_pylist())
            starts = [s for s in t.column("start").to_pylist() if s]
            ends = [e for e in t.column("end").to_pylist() if e]
            if starts:
                lo = min(starts)
                dmin = lo if dmin is None else min(dmin, lo)
            if ends:
                hi = max(ends)
                dmax = hi if dmax is None else max(dmax, hi)
        # published total stations for this dataset
        ids_path = os.path.join(RAW, f"{ds}_station_ids.txt")
        pub_total = 0
        if os.path.exists(ids_path):
            with open(ids_path, encoding="utf-8") as f:
                pub_total = sum(1 for ln in f if ln.strip())
        cov = (len(stations) / pub_total * 100) if pub_total else 0.0
        report["datasets"][ds] = {
            "n_obs": int(n_obs),
            "n_series": int(n_series),
            "n_stations_with_data": len(stations),
            "n_elements": len(elements),
            "elements": sorted(elements),
            "obs_parquet_files": len(obs_files),
            "series_parquet_files": len(series_files),
            "date_range": [dmin, dmax],
            "published_station_total": pub_total,
            "station_coverage_pct": round(cov, 2),
        }
        grand_obs += n_obs
        grand_series += n_series
        grand_files += len(obs_files) + len(series_files)
        print(f"VERIFY {ds}: obs={n_obs:,} series={n_series:,} "
              f"stations_with_data={len(stations):,}/{pub_total:,} "
              f"({cov:.1f}%) files={len(obs_files)+len(series_files)} "
              f"range={dmin}..{dmax}", flush=True)
    report["total_obs"] = int(grand_obs)
    report["total_series"] = int(grand_series)
    report["total_parquet_files"] = int(grand_files)
    with open(os.path.join(OUT, "noaa.meta.json"), "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    print(f"\nTOTAL: obs={grand_obs:,} series={grand_series:,} files={grand_files}", flush=True)
    return report


# --------------------------------------------------------------------------- #
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--enumerate", action="store_true")
    ap.add_argument("--download", action="store_true")
    ap.add_argument("--build", action="store_true")
    ap.add_argument("--verify", action="store_true")
    ap.add_argument("--dataset", choices=list(DATASETS) + ["all"], default="all")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--limit", type=int, default=None, help="build: cap #stations (debug)")
    args = ap.parse_args()

    datasets = list(DATASETS) if args.dataset == "all" else [args.dataset]

    did = False
    if args.enumerate:
        enumerate_all(); did = True
    if args.download:
        download(datasets, args.workers); did = True
    if args.build:
        build(datasets, args.limit); did = True
    if args.verify:
        verify(datasets); did = True
    if not did:
        build(datasets, args.limit)


if __name__ == "__main__":
    main()
