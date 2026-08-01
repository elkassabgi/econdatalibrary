"""Bulk per-series CSV derive for ONE tidy source — a streaming group-by instead of N scans.

WHY THIS EXISTS ALONGSIDE core/derive_csv.py. That tool resolves each series independently,
which is right for incremental work and quadratic for a backfill: every call runs a predicate
scan over the whole source parquet. Measured on cepii_gravity (93 MB, 69,666,545 rows,
1,143,250 series): 63 ms/series, i.e. 17.4 h serial, and 16 threads did not help because they
contend on the same file. Its 991,707 missing objects were not going to land that way.

WHAT THIS DOES INSTEAD. DuckDB streams the parquet ONCE in `ORDER BY series_key, obs_date`
(spilling to disk, so memory stays flat regardless of source size), and rows for a series are
flushed the moment the key changes. One pass, no per-series scan.

BYTE-EXACTNESS IS THE WHOLE CONTRACT, so it is verified rather than asserted: --verify N
compares this path's output against core.derive_csv._series_csv_bytes for N series sampled
across the FULL key range, and refuses to run unless every one matches exactly. The sample is
random, not the first N — a prefix sample cannot detect a divergence that only appears later,
which is the same reason a 5-key listing once nearly certified a 13%-complete derive (R167).

Usage:
  python tools/derive_csv_bulk.py --source cepii_gravity --verify 300 --dry-run
  python tools/derive_csv_bulk.py --source cepii_gravity --bucket econ-data --skip-existing
"""
from __future__ import annotations
import argparse
import csv
import io
import os
import queue
import random
import sys
import threading
import urllib.parse

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))

from core import r2_util  # noqa: E402

HEADER = ["series_id", "obs_date", "value"]


def csv_key(prefix: str, source: str, series_key: str) -> str:
    """The R2 key for one series' derived CSV. THE single definition of this layout.

    tools/verify_derive_parity.py imports it rather than re-deriving it: if the writer and
    the checker each spelled the encoding out, a drift in one could still show clean parity
    by being wrong the same way in both, which is precisely the failure a parity check is
    supposed to catch.
    """
    return f"{prefix}/{urllib.parse.quote(f'{source}:{series_key}', safe='')}.csv"


def csv_key_prefix(prefix: str, source: str) -> str:
    """The listing prefix covering every series of one source."""
    return f"{prefix}/{urllib.parse.quote(source + ':', safe='')}"


def _retry(fn, what, tries=8):
    """Call fn() with patient backoff. R2 answers ServiceUnavailable / SlowDown under load —
    'Reduce your concurrent request rate for the same object' killed a run in the
    skip-existing LISTING, which had no retry at all while the PUT path did. A resume step
    that dies on a throttle is worse than no resume: it forces a re-run that throttles harder.
    """
    import time as _t
    last = None
    for attempt in range(tries):
        try:
            return fn()
        except Exception as e:                               # noqa: BLE001
            last = e
            if attempt == tries - 1:
                break
            wait = min(60, 2 ** attempt)
            print(f"  {what} retry {attempt+1}/{tries} in {wait}s ({str(e)[:70]})", flush=True)
            _t.sleep(wait)
    raise last


def _csv_bytes(short_id: str, rows) -> bytes:
    """CSV for one series. Mirrors core.derive_csv._series_csv_bytes: same header, the SOURCE
    PREFIX STRIPPED from the id column, and lineterminator='\\n' so bytes match the Worker."""
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(HEADER)
    for d, v in rows:
        w.writerow([short_id, d, v])
    return buf.getvalue().encode("utf-8")


def _stream_one(con, path):
    """Yield (series_key, [(obs_date, value), ...]) in key order, one series at a time."""
    cur = con.execute(
        "SELECT series_key, obs_date, value FROM read_parquet(?) "
        "ORDER BY series_key, obs_date", [path])
    key = None
    acc = []
    while True:
        batch = cur.fetchmany(100_000)
        if not batch:
            break
        for k, d, v in batch:
            if k != key:
                if key is not None:
                    yield key, acc
                key, acc = k, []
            acc.append((d, v))
    if key is not None:
        yield key, acc


def _stream(con, paths):
    """Same, over a SHARDED source: one sorted scan PER SHARD, in sequence.

    Sharded stores are the norm above a certain size - noaa is 417 country-prefix shards over
    549,412,914 rows - and a single ORDER BY across all of them would sort the whole corpus to
    produce output that is already grouped. Per-shard sorting is bounded by the largest shard
    instead of by the source.

    THE PRECONDITION IS THAT SHARDS DO NOT SHARE A series_key, and it is CHECKED, not assumed:
    if two shards held the same key, each would flush its own CSV and the second would silently
    overwrite the first with a partial history. The check is a running set of keys already
    emitted, which costs one string per series and turns a silent truncation into a loud stop.
    """
    if isinstance(paths, str):
        paths = [paths]
    emitted: set[str] = set()
    for i, p in enumerate(paths, 1):
        if len(paths) > 1:
            print(f"  shard {i}/{len(paths)}: {os.path.basename(p)}", flush=True)
        for k, rows in _stream_one(con, p):
            if len(paths) > 1:
                if k in emitted:
                    raise SystemExit(
                        f"REFUSING to continue: series_key {k!r} appears in more than one shard "
                        f"({os.path.basename(p)} and an earlier one). Per-shard streaming would "
                        f"write this series twice and keep only the last, partial history. "
                        f"Derive this source with a single sorted scan instead.")
                emitted.add(k)
            yield k, rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True)
    ap.add_argument("--bucket")
    ap.add_argument("--prefix", default="series")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip-existing", action="store_true")
    ap.add_argument("--workers", type=int, default=24)
    ap.add_argument("--verify", type=int, default=300,
                    help="byte-compare this many RANDOM series against the resolver first")
    a = ap.parse_args()

    import duckdb
    src_dir = os.path.join(ROOT, "data", "clean_full", a.source)
    pqs = sorted(f for f in os.listdir(src_dir) if f.endswith(".parquet")
                 and not f.endswith("__series.parquet"))
    if not pqs:
        print(f"no parquet in {src_dir}")
        return 2
    paths = [os.path.join(src_dir, f) for f in pqs]
    # `path` is what the VERIFY step and the distinct-key count read. DuckDB's read_parquet
    # takes a list as happily as a string, so a sharded source is verified across all of its
    # shards rather than only the first - sampling one shard of 417 would certify a format
    # that the other 416 might not share.
    path = paths if len(paths) > 1 else paths[0]
    if len(paths) > 1:
        print(f"{len(paths)} shards in {src_dir}", flush=True)
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")

    # ---- byte-exactness gate -------------------------------------------------------
    if a.verify:
        from core.derive_csv import _series_csv_bytes
        keys = [r[0] for r in con.execute(
            "SELECT DISTINCT series_key FROM read_parquet(?) ORDER BY series_key",
            [path]).fetchall()]
        print(f"{len(keys):,} distinct series in the parquet", flush=True)
        rnd = random.Random(20260730)
        sample = rnd.sample(keys, min(a.verify, len(keys)))
        # ONE scan for the whole sample. A per-key query would re-scan all 69.6M rows each
        # time — 300 full passes to check 300 series, which is the very cost this tool exists
        # to remove, reintroduced inside its own test.
        want = set(sample)
        grouped: dict = {k: [] for k in sample}
        cur = con.execute(
            "SELECT series_key, obs_date, value FROM read_parquet(?) "
            "WHERE series_key IN (SELECT UNNEST(?)) ORDER BY series_key, obs_date",
            [path, sample])
        while True:
            batch = cur.fetchmany(50_000)
            if not batch:
                break
            for k, d, v in batch:
                if k in want:
                    grouped[k].append((d, v))
        bad = 0
        for k in sample:
            mine = _csv_bytes(k, grouped[k])
            theirs = _series_csv_bytes(f"{a.source}:{k}")
            if mine != theirs:
                bad += 1
                if bad <= 3:
                    print(f"  MISMATCH {k}\n    mine  : {mine[:120]!r}\n"
                          f"    theirs: {theirs[:120]!r}")
        print(f"verify: {len(sample) - bad}/{len(sample)} byte-identical", flush=True)
        if bad:
            print("REFUSING to run — output would not match the served contract.")
            return 1

    existing = set()
    s3 = None
    if not a.dry_run:
        if not a.bucket:
            ap.error("--bucket is required unless --dry-run")
        s3 = r2_util.client(write=True)
        if a.skip_existing:
            lp = csv_key_prefix(a.prefix, a.source)
            tok = None
            while True:
                kw = {"Bucket": a.bucket, "Prefix": lp, "MaxKeys": 1000}
                if tok:
                    kw["ContinuationToken"] = tok
                r = _retry(lambda: s3.list_objects_v2(**kw), "LIST")
                for o in r.get("Contents", []):
                    existing.add(o["Key"])
                if not r.get("IsTruncated"):
                    break
                tok = r["NextContinuationToken"]
            print(f"skip-existing: {len(existing):,} already in R2", flush=True)

    # ---- producer (single sorted scan) -> bounded queue -> PUT workers --------------
    q: queue.Queue = queue.Queue(maxsize=4000)
    counts = {"put": 0, "skip": 0, "err": 0}
    lock = threading.Lock()
    STOP = object()

    def worker():
        while True:
            item = q.get()
            if item is STOP:
                q.task_done()
                return
            key, body = item
            try:
                _retry(lambda: s3.put_object(Bucket=a.bucket, Key=key, Body=body,
                                             ContentType="text/csv"), "PUT")
                with lock:
                    counts["put"] += 1
                    if counts["put"] % 25_000 == 0:
                        print(f"  put {counts['put']:,} (skip {counts['skip']:,})", flush=True)
            except Exception as e:                           # noqa: BLE001
                with lock:
                    counts["err"] += 1
                    if counts["err"] <= 5:
                        print(f"  PUT FAILED {key}: {str(e)[:90]}", flush=True)
            finally:
                q.task_done()

    threads = []
    if not a.dry_run:
        for _ in range(a.workers):
            t = threading.Thread(target=worker, daemon=True)
            t.start()
            threads.append(t)

    seen = 0
    for k, rows in _stream(con, paths):
        seen += 1
        key = csv_key(a.prefix, a.source, k)
        if key in existing:
            counts["skip"] += 1
            continue
        if a.dry_run:
            if seen <= 3:
                print(f"  would PUT {key} ({len(_csv_bytes(k, rows))} B)")
            continue
        q.put((key, _csv_bytes(k, rows)))

    if not a.dry_run:
        q.join()
        for _ in threads:
            q.put(STOP)
        q.join()
    print(f"done: {seen:,} series streamed, put {counts['put']:,}, "
          f"skipped {counts['skip']:,}, errors {counts['err']:,}")
    return 1 if counts["err"] else 0


if __name__ == "__main__":
    sys.exit(main())
