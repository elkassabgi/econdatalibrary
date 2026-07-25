#!/usr/bin/env python3
"""Full-coverage grouped ingest of Statistics Canada (WDS).

Enumerates the ENTIRE cube catalog via getAllCubesListLite (~8,200 cubes), then
for each cube pulls the full-table CSV bulk download (getFullTableDownloadCSV ->
zip URL on www150), streams the data CSV, and writes ONE Parquet per cube.

Parquet columns (grouped, one file per productId):
  series_key  : StatCan VECTOR id (e.g. "v41690973")  -- the canonical series key
  obs_date    : date32  (REF_DATE parsed: YYYY / YYYY-MM / YYYY-MM-DD)
  value       : float64 (null when suppressed/"..")
  geo         : geography label
  uom         : unit of measure
  coordinate  : StatCan dimension coordinate (e.g. "1.1.2")
  status      : STATUS flag (".."=unavailable, E=use w/ caution, F=too unreliable, etc.)

License: statcan-open (Statistics Canada Open Licence).

Memory is bounded: zips stream to a temp file, the CSV is parsed row-by-row, and
rows are flushed to the Parquet writer in batches (so even the 2 GB cube is fine).
Resumable: a cube with an existing .parquet + .done marker is skipped.

Usage:
  python jobs/ingest_statcan.py --dry 5      # enumerate + process 5 small cubes, print
  python jobs/ingest_statcan.py              # full run (all cubes)
  python jobs/ingest_statcan.py --workers 6  # set concurrency (default 6)
"""
import csv
import datetime as dt
import io
import json
import os
import re
import sys
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT = os.path.join(ROOT, "data", "clean_full", "statcan")
TMP = os.path.join(OUT, "_tmp")
MANIFEST = os.path.join(OUT, "_manifest.jsonl")
ERRLOG = os.path.join(OUT, "_errors.log")

UA = "Econ-Fin Data Library admin@hfdatalibrary.com"
HEADERS = {"User-Agent": UA}
LICENSE_ID = "statcan-open"

WDS_LIST = "https://www150.statcan.gc.ca/t1/wds/rest/getAllCubesListLite"
WDS_DL = "https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/{pid}/en"

# Structural columns present in every StatCan full-table CSV.
FLUSH_ROWS = 100_000   # row-group batch size (bounds memory on huge cubes)

csv.field_size_limit(1 << 24)

# Sandbox-safety: several concurrent LARGE streaming downloads reliably get the
# process killed here (network/IO watchdog), even though RAM stays tiny. So we use
# a size-aware download gate (readers/writers): many SMALL downloads may run at once
# (readers), but a LARGE download takes the gate EXCLUSIVELY (writer) -- never
# concurrent with any other download. Memory-heavy parses are also serialized.
HEAVY_ZIP_BYTES = 8_000_000          # zips >8MB compressed -> "large"
SMALL_DL_CONC = 4                    # max concurrent small downloads
SERIES_CAP = 2_000_000               # cap on exact distinct-series tracking (memory)
_heavy_parse = threading.Semaphore(1)     # global: one large parse in flight


class SizeAwareGate:
    """Allow up to `small_max` concurrent small downloads OR up to `large_max`
    concurrent large downloads (small and large never mix). Capping concurrent LARGE
    downloads is what keeps the sandbox from killing the process (it died at ~4
    simultaneous 220MB streams); 2 is a safe, faster-than-serial compromise."""
    def __init__(self, small_max, large_max=2):
        self.small_max = small_max
        self.large_max = large_max
        self.cv = threading.Condition()
        self.small_active = 0
        self.large_active = 0

    def acquire(self, large):
        with self.cv:
            if large:
                while self.small_active > 0 or self.large_active >= self.large_max:
                    self.cv.wait()
                self.large_active += 1
            else:
                while self.large_active > 0 or self.small_active >= self.small_max:
                    self.cv.wait()
                self.small_active += 1

    def release(self, large):
        with self.cv:
            if large:
                self.large_active -= 1
            else:
                self.small_active -= 1
            self.cv.notify_all()


_download_gate = SizeAwareGate(SMALL_DL_CONC, large_max=1)

_print_lock = threading.Lock()


def log(msg):
    with _print_lock:
        print(msg, flush=True)


def errlog(msg):
    with _print_lock:
        with open(ERRLOG, "a", encoding="utf-8") as f:
            f.write(msg + "\n")


def session():
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def get_json_retry(s, url, tries=5):
    for i in range(tries):
        try:
            r = s.get(url, timeout=120)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 502, 503, 504):
                time.sleep(2 * (i + 1) + 1)
                continue
            r.raise_for_status()   # 4xx (non-429) -> raise immediately, no retry
        except requests.RequestException:
            if i == tries - 1:
                raise
            time.sleep(2 * (i + 1) + 1)
    return None


def download_retry(s, url, dest, tries=5):
    for i in range(tries):
        try:
            with s.get(url, timeout=900, stream=True) as r:
                if r.status_code in (429, 502, 503, 504):
                    time.sleep(3 * (i + 1) + 1)
                    continue
                # permanent client errors (404 etc.) -> fail fast, do not retry
                if 400 <= r.status_code < 500 and r.status_code != 429:
                    r.raise_for_status()
                r.raise_for_status()
                with open(dest, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        if chunk:
                            f.write(chunk)
            return True
        except requests.HTTPError as e:
            code = e.response.status_code if e.response is not None else None
            if code and 400 <= code < 500 and code != 429:
                raise           # permanent -> bubble up now, no backoff loop
            if i == tries - 1:
                raise
            time.sleep(3 * (i + 1) + 1)
        except requests.RequestException:
            if i == tries - 1:
                raise
            time.sleep(3 * (i + 1) + 1)
    return False


def parse_refdate(p):
    """REF_DATE -> date. Handles YYYY, YYYY-MM, YYYY-MM-DD. Returns None if unparseable."""
    p = p.strip()
    if not p:
        return None
    try:
        n = len(p)
        if n == 4 and p.isdigit():
            return dt.date(int(p), 12, 31)            # annual -> year-end
        if n == 7:                                     # YYYY-MM (monthly/quarterly)
            y, m = p.split("-")
            return dt.date(int(y), int(m), 1)
        if n == 10:                                    # YYYY-MM-DD (daily/weekly)
            y, m, dd = p.split("-")
            return dt.date(int(y), int(m), int(dd))
        # rare: YYYY/YYYY (fiscal range) -> take first year
        if "/" in p:
            first = p.split("/")[0].strip()
            if first.isdigit() and len(first) == 4:
                return dt.date(int(first), 12, 31)
    except (ValueError, KeyError):
        return None
    return None


def parse_value(v):
    v = v.strip()
    if not v:
        return None
    try:
        return float(v)
    except ValueError:
        return None


SCHEMA = pa.schema([
    ("series_key", pa.string()),
    ("obs_date", pa.date32()),
    ("value", pa.float64()),
    ("geo", pa.string()),
    ("uom", pa.string()),
    ("coordinate", pa.string()),
    ("status", pa.string()),
])


def process_cube(c, dry=False):
    """Download + parse one cube. Returns dict stats or raises.

    Small requests (resolve URL + HEAD for size) run freely across workers; only the
    actual byte-download is size-gated: many small cubes download concurrently, but a
    large cube downloads EXCLUSIVELY (concurrent large downloads get the proc killed)."""
    pid = c["productId"]
    out_path = os.path.join(OUT, f"{pid}.parquet")
    done_path = os.path.join(OUT, f"{pid}.done")
    if os.path.exists(done_path) and os.path.exists(out_path):
        try:
            with open(done_path) as f:
                return json.load(f)
        except Exception:
            pass  # fall through and redo

    s = session()
    j = get_json_retry(s, WDS_DL.format(pid=pid))
    if not j or j.get("status") != "SUCCESS" or not j.get("object"):
        raise RuntimeError(f"no download url (resp={j})")
    zip_url = j["object"]

    # size via HEAD (tiny, not gated) -> decide small vs large download lane
    zip_size = 0
    try:
        h = s.head(zip_url, timeout=60, allow_redirects=True)
        zip_size = int(h.headers.get("Content-Length", 0))
    except (requests.RequestException, ValueError):
        zip_size = 0
    large = zip_size >= HEAVY_ZIP_BYTES

    zpath = os.path.join(TMP, f"{pid}.zip")
    _download_gate.acquire(large)
    try:
        download_retry(s, zip_url, zpath)
    finally:
        _download_gate.release(large)

    # Serialize the memory-heavy parse for large cubes (download done -> on disk).
    heavy = os.path.getsize(zpath) >= HEAVY_ZIP_BYTES
    if heavy:
        _heavy_parse.acquire()
    try:
        return _parse_and_write(c, pid, zpath, out_path, done_path)
    finally:
        if heavy:
            _heavy_parse.release()


VALCOL_RE = re.compile(r"\[(\d+)\]\s*$")   # trailing "[k]" marks a Census value column


def _iter_standard(rdr, idx, hdr):
    """Standard WDS long format: one row per (series, period). series_key = VECTOR."""
    i_ref = idx["REF_DATE"]; i_vec = idx["VECTOR"]; i_val = idx["VALUE"]
    i_geo = idx.get("GEO"); i_uom = idx.get("UOM")
    i_coord = idx.get("COORDINATE"); i_stat = idx.get("STATUS")
    ncol = len(hdr)
    for row in rdr:
        if not row or len(row) < ncol:
            continue
        od = parse_refdate(row[i_ref])
        if od is None:
            continue
        vec = row[i_vec].strip()
        if not vec:
            vec = row[i_coord].strip() if i_coord is not None else ""
            if not vec:
                continue
        val = parse_value(row[i_val])
        yield (vec, od,
               val,
               row[i_geo] if i_geo is not None else None,
               row[i_uom] if i_uom is not None else None,
               row[i_coord] if i_coord is not None else None,
               row[i_stat] if i_stat is not None else None)


def _iter_census(rdr, idx, hdr):
    """Census Program wide/pivoted layout. Each measure-member is its own column
    (header '...[k]') followed by a Symbol column. Emit one obs per value cell;
    series_key = '<Coordinate>.<k>' (stable, mirrors WDS coordinates)."""
    i_ref = idx["REF_DATE"]; i_geo = idx.get("GEO"); i_coord = idx["Coordinate"]
    ncol = len(hdr)
    # value columns: (col_index, member_k, label_without_suffix, symbol_col_index)
    valcols = []
    for ci, h in enumerate(hdr):
        m = VALCOL_RE.search(h)
        if not m:
            continue
        k = m.group(1)
        label = VALCOL_RE.sub("", h).strip().rstrip(":").strip()
        sym = ci + 1 if (ci + 1 < ncol and hdr[ci + 1] in ("Symbol", "Symbols")) else None
        valcols.append((ci, k, label, sym))
    for row in rdr:
        if not row or len(row) < ncol:
            continue
        od = parse_refdate(row[i_ref])
        if od is None:
            continue
        coord = row[i_coord].strip()
        geo = row[i_geo] if i_geo is not None else None
        for ci, k, label, sym in valcols:
            val = parse_value(row[ci])
            key = f"{coord}.{k}" if coord else f"c{k}"
            yield (key, od, val, geo, label, coord,
                   row[sym] if (sym is not None and sym < len(row)) else None)


def _parse_and_write(c, pid, zpath, out_path, done_path):
    n_obs = 0
    n_null = 0
    vectors = set()
    min_d = None
    max_d = None
    writer = None
    tmp_out = out_path + ".part"

    # batch buffers
    bk, bd, bv, bg, bu, bc, bs = [], [], [], [], [], [], []

    def flush():
        nonlocal writer, bk, bd, bv, bg, bu, bc, bs
        if not bk:
            return
        batch = pa.record_batch([
            pa.array(bk, type=pa.string()),
            pa.array(bd, type=pa.date32()),
            pa.array(bv, type=pa.float64()),
            pa.array(bg, type=pa.string()),
            pa.array(bu, type=pa.string()),
            pa.array(bc, type=pa.string()),
            pa.array(bs, type=pa.string()),
        ], schema=SCHEMA)
        if writer is None:
            writer = pq.ParquetWriter(tmp_out, SCHEMA, compression="zstd")
        writer.write_batch(batch)
        bk.clear(); bd.clear(); bv.clear(); bg.clear(); bu.clear(); bc.clear(); bs.clear()

    try:
        with zipfile.ZipFile(zpath) as z:
            data_names = [n for n in z.namelist() if not n.endswith("_MetaData.csv") and n.lower().endswith(".csv")]
            if not data_names:
                raise RuntimeError(f"no data csv in zip (names={z.namelist()})")
            data_name = data_names[0]
            with z.open(data_name) as raw:
                txt = io.TextIOWrapper(raw, encoding="utf-8-sig", errors="replace", newline="")
                rdr = csv.reader(txt)
                hdr = next(rdr)
                idx = {col: i for i, col in enumerate(hdr)}
                if "VECTOR" in idx and "VALUE" in idx:
                    rowgen = _iter_standard(rdr, idx, hdr)
                elif "Coordinate" in idx and any(VALCOL_RE.search(h) for h in hdr):
                    # Census Program wide/pivoted layout (no VECTOR): one column per
                    # measure-member, series_key = Coordinate + member index.
                    rowgen = _iter_census(rdr, idx, hdr)
                else:
                    raise RuntimeError(f"unrecognized layout in {data_name}; hdr={hdr[:12]}")
                for key, od, val, geo, uom, coord, stat in rowgen:
                    if val is None:
                        n_null += 1
                    bk.append(key); bd.append(od); bv.append(val)
                    bg.append(geo); bu.append(uom); bc.append(coord); bs.append(stat)
                    # bound the distinct-series set: on multi-GB cubes an exact set of
                    # tens of millions of keys exhausts RAM. Cap it; n_series becomes a
                    # lower bound (flagged via series_capped) past the cap.
                    if len(vectors) < SERIES_CAP:
                        vectors.add(key)
                    if min_d is None or od < min_d:
                        min_d = od
                    if max_d is None or od > max_d:
                        max_d = od
                    n_obs += 1
                    if len(bk) >= FLUSH_ROWS:
                        flush()
            flush()
    finally:
        if writer is not None:
            writer.close()
        # clean up temp zip immediately to bound disk
        try:
            os.remove(zpath)
        except OSError:
            pass

    if n_obs == 0:
        # empty cube -> still write an empty parquet so we have a record, mark done
        if writer is None:
            empty = pa.table({name: pa.array([], type=f.type)
                              for name, f in zip(SCHEMA.names, SCHEMA)}, schema=SCHEMA)
            pq.write_table(empty, tmp_out)

    # atomic-ish rename
    if os.path.exists(out_path):
        os.remove(out_path)
    os.replace(tmp_out, out_path)

    stats = {
        "productId": pid,
        "title": c.get("cubeTitleEn"),
        "cansimId": c.get("cansimId"),
        "frequencyCode": c.get("frequencyCode"),
        "archived": c.get("archived"),
        "subjectCode": c.get("subjectCode"),
        "n_series": len(vectors),
        "series_capped": len(vectors) >= SERIES_CAP,
        "n_obs": n_obs,
        "n_null": n_null,
        "start": min_d.isoformat() if min_d else None,
        "end": max_d.isoformat() if max_d else None,
        "license_id": LICENSE_ID,
        "file": os.path.basename(out_path),
        "file_bytes": os.path.getsize(out_path),
    }
    with open(done_path, "w", encoding="utf-8") as f:
        json.dump(stats, f)
    return stats


SIZECACHE = os.path.join(OUT, "_sizecache.json")


def fetch_sizes(todo, workers=6):
    """For each remaining cube, resolve its zip URL + Content-Length (HEAD).
    These are tiny requests so we run them concurrently (well under 25 req/s/IP
    once combined with retries). Cached to _sizecache.json for resume."""
    cache = {}
    if os.path.exists(SIZECACHE):
        try:
            cache = json.load(open(SIZECACHE))
        except Exception:
            cache = {}
    need = [c for c in todo if str(c["productId"]) not in cache]
    log(f"Size pre-pass: {len(cache):,} cached, fetching sizes for {len(need):,} cubes ...")
    if not need:
        return cache

    lock = threading.Lock()

    def one(c):
        pid = str(c["productId"])
        s = session()
        try:
            j = get_json_retry(s, WDS_DL.format(pid=pid), tries=4)
            if not j or j.get("status") != "SUCCESS" or not j.get("object"):
                return pid, {"url": None, "size": -1}
            url = j["object"]
            try:
                h = s.head(url, timeout=60, allow_redirects=True)
                size = int(h.headers.get("Content-Length", 0))
            except requests.RequestException:
                size = 0
            return pid, {"url": url, "size": size}
        except Exception:
            return pid, {"url": None, "size": -1}

    done = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for pid, info in ex.map(one, need):
            with lock:
                cache[pid] = info
                done += 1
                if done % 100 == 0:
                    json.dump(cache, open(SIZECACHE, "w"))
                    log(f"  sizes: {done:,}/{len(need):,}")
    json.dump(cache, open(SIZECACHE, "w"))
    log(f"Size pre-pass complete: {len(cache):,} cubes sized.")
    return cache


def main():
    argv = sys.argv[1:]
    dry = "--dry" in argv
    limit = int(argv[argv.index("--dry") + 1]) if dry else None
    workers = int(argv[argv.index("--workers") + 1]) if "--workers" in argv else 6
    workers = max(1, min(workers, 6))
    # time budget for one foreground pass (sec); 0 = unlimited. Stops submitting
    # new cubes once exceeded; in-flight cubes finish, then the pass exits cleanly.
    budget = float(argv[argv.index("--budget") + 1]) if "--budget" in argv else 0.0

    os.makedirs(OUT, exist_ok=True)
    os.makedirs(TMP, exist_ok=True)

    log(f"Enumerating StatCan cube catalog via getAllCubesListLite ...")
    cubes = get_json_retry(session(), WDS_LIST)
    total = len(cubes)
    active = sum(1 for c in cubes if str(c.get("archived")) == "2")
    archived = total - active
    log(f"CATALOG: {total:,} cubes total  ({active:,} active, {archived:,} archived)")

    if dry:
        cubes = sorted(cubes, key=lambda c: c["productId"])[:limit]
        log(f"DRY-RUN: processing {len(cubes)} cubes")

    # resume: pre-filter cubes already complete OR permanently failed (no table).
    cubes = sorted(cubes, key=lambda c: c["productId"])
    def settled(c):
        pid = c["productId"]
        return (os.path.exists(os.path.join(OUT, f"{pid}.done"))
                or os.path.exists(os.path.join(OUT, f"{pid}.fail")))
    done_before = sum(1 for c in cubes
                      if os.path.exists(os.path.join(OUT, f"{c['productId']}.done")))
    fail_before = sum(1 for c in cubes
                      if os.path.exists(os.path.join(OUT, f"{c['productId']}.fail")))
    todo = [c for c in cubes if not settled(c)]
    log(f"Resume: {done_before:,} done + {fail_before:,} no-data / {total:,}; "
        f"{len(todo):,} remaining{f'; budget={budget:.0f}s' if budget else ''}")

    # Optional size pre-pass (--presize) to schedule smallest-first. By default we
    # SKIP it (it's a slow blocking phase) and instead size each cube inline via a
    # cheap HEAD inside process_cube, gating only the byte-download by size. Small
    # cubes download concurrently; large cubes download exclusively (sandbox-safe).
    sizes = {}
    if "--presize" in argv:
        sizes = fetch_sizes(todo, workers=workers)
        if "--sizeonly" in argv:
            sized = sum(1 for c in todo if (sizes.get(str(c["productId"]), {}).get("size", 0) or 0) > 0)
            log(f"SIZEONLY done: {len(sizes):,} cached, {sized:,} sized. Exiting.")
            return
        def szof(c):
            s = sizes.get(str(c["productId"]), {}).get("size", 0)
            return s if s and s > 0 else 0
        todo.sort(key=szof)
        n_large = sum(1 for c in todo if szof(c) >= HEAVY_ZIP_BYTES)
        log(f"Scheduling smallest-first: {len(todo)-n_large:,} small + {n_large:,} large")

    t0 = time.time()
    n_ok = n_err = 0
    tot_obs = 0
    tot_series = 0
    manifest_f = open(MANIFEST, "a", encoding="utf-8")
    lock = threading.Lock()
    stop = {"flag": False}

    def work(c):
        return process_cube(c, dry=dry)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        it = iter(todo)
        futs = {}
        # prime the pool
        for _ in range(workers):
            c = next(it, None)
            if c is None:
                break
            futs[ex.submit(work, c)] = c
        while futs:
            for fut in as_completed(list(futs)):
                c = futs.pop(fut)
                pid = c["productId"]
                try:
                    st = fut.result()
                    with lock:
                        n_ok += 1
                        tot_obs += st["n_obs"]
                        tot_series += st["n_series"]
                        manifest_f.write(json.dumps(st) + "\n")
                        manifest_f.flush()
                        done = n_ok + n_err
                        if done % 50 == 0 or dry:
                            rate = done / max(time.time() - t0, 1e-9)
                            eta = (len(todo) - done) / max(rate, 1e-9) / 60
                            log(f"  [{done:,}/{len(todo):,} this-pass] ok={n_ok:,} err={n_err:,} "
                                f"obs={tot_obs:,} series={tot_series:,} "
                                f"{rate:.2f} cube/s ETA {eta:.0f}m  last={pid} ({st['n_obs']:,} obs)")
                except Exception as e:  # noqa: BLE001
                    with lock:
                        n_err += 1
                        errlog(f"{pid}\t{type(e).__name__}\t{e}")
                        if n_err <= 30:
                            log(f"  ERROR pid={pid}: {type(e).__name__}: {e}")
                    # mark permanent failures (no table available / not a zip) so we
                    # don't re-attempt them every pass. Transient errors are NOT marked.
                    msg = str(e)
                    permanent = ("404" in msg or isinstance(e, zipfile.BadZipFile)
                                 or "no download url" in msg or "no data csv" in msg)
                    if permanent:
                        try:
                            with open(os.path.join(OUT, f"{pid}.fail"), "w", encoding="utf-8") as f:
                                json.dump({"productId": pid, "error": f"{type(e).__name__}: {msg}"[:300]}, f)
                        except OSError:
                            pass
                # budget check -> stop submitting new work
                if budget and (time.time() - t0) > budget:
                    stop["flag"] = True
                # submit next cube to keep pool full (unless stopping)
                if not stop["flag"]:
                    nxt = next(it, None)
                    if nxt is not None:
                        futs[ex.submit(work, nxt)] = nxt
                break  # re-evaluate as_completed over the updated futs set
        # drain handled by while loop emptying futs

    manifest_f.close()
    dtm = time.time() - t0
    remaining_after = sum(1 for c in cubes
                          if not os.path.exists(os.path.join(OUT, f"{c['productId']}.done")))
    log("=" * 70)
    tag = "PASS-DONE (budget hit, more remain)" if (stop["flag"] and remaining_after) else "DONE"
    log(f"{tag} in {dtm/60:.1f} min")
    log(f"  source published total: {total:,} cubes")
    log(f"  processed this pass: ok={n_ok:,}  err={n_err:,}")
    log(f"  cubes complete overall: {total - remaining_after:,}/{total:,}  remaining: {remaining_after:,}")
    log(f"  observations written this pass: {tot_obs:,}")
    log(f"  output dir: {OUT}")
    log(f"  manifest: {MANIFEST}")
    if n_err:
        log(f"  errors logged to: {ERRLOG}")


if __name__ == "__main__":
    main()
