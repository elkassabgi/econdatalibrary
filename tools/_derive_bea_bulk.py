"""One-shot: derive bea's 913,230 series CSVs in ONE pass, byte-matching the resolver.

WHY NOT tools/derive_csv_bulk.py. Its pre-flight refused bea at 32/300 byte-identical
(2026-08-10, _bea_derive.log) and its _stream would have refused anyway (bea replicates
series_key across table shards). The served contract for bea is _resolve_bea +
_DEDUP_ON["bea"]=(series_key, obs_date): read the WHOLE tree as one pyarrow dataset,
drop duplicate (key, date) rows KEEPING THE FIRST IN DATASET SCAN ORDER, then sort by
date. bulk's per-shard ORDER BY emits every duplicate row and, where the same key holds
DIFFERENT values across tables (#82's under-keyed class, e.g. 712:25023 at 2001-12-31 =
33755 vs 56736), a different survivor than the resolver keeps. Per-series resolver
derive is correct but ~0.4 s/series x 913k = ~4 days of full-tree predicate scans.

THIS tool replicates read_native exactly, once, for all keys:
  1. table = ds.dataset(<root>/bea).to_table()      -- the RESOLVER'S OWN construction
     (same discovery order, so the keep-first survivor is the same row).
  2. dictionary-encode keys, dedup (key, date) keep-first in row order (pandas
     drop_duplicates, the same call read_native makes per series).
  3. stable-sort (key, date); a filtered scan restricted to one key preserves relative
     row order, so per-key rows here == read_native's rows for that key.
  4. serialize per key with derive_csv_bulk._csv_bytes (proven byte-mirror of
     core.derive_csv._series_csv_bytes) using datetime.date objects + float values,
     exactly what native_to_tidy's itertuples hands csv.writer.

GATE: before ANY upload, byte-compare against core.derive_csv._series_csv_bytes (the
live resolver) on the SAME seed-20260730 300-sample bulk used, PLUS the three keys bulk
printed as mismatches. Anything short of 100% refuses the run.
"""
from __future__ import annotations

import datetime as dt
import os
import queue
import random
import sqlite3
import sys
import threading

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _REPO)
sys.path.insert(0, os.path.join(_REPO, "clients", "python"))

import numpy as np                     # noqa: E402
import pandas as pd                    # noqa: E402
import pyarrow as pa                   # noqa: E402
import pyarrow.compute as pc           # noqa: E402
import pyarrow.dataset as ds           # noqa: E402

from core import r2_util               # noqa: E402
from tools.derive_csv_bulk import _csv_bytes, _retry, csv_key, csv_key_prefix  # noqa: E402

ROOT = _REPO
SRC = "bea"
BUCKET = "econ-data"
PREFIX = "series"
STORE = os.path.join(ROOT, "data", "clean_full", SRC)
EPOCH = dt.date(1970, 1, 1)
# bulk's 3 printed mismatches: dup-date (830), different-value survivor (712, 200).
MUST_VERIFY = ["830:41033", "712:25023", "200:27173"]


def load_deduped_sorted() -> tuple[list[str], np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    print("scanning the tree exactly as the resolver does...", flush=True)
    table = ds.dataset(STORE).to_table()          # SAME call as read_native, no filter
    print(f"  {table.num_rows:,} raw rows", flush=True)
    keys = pc.dictionary_encode(table.column("series_key").combine_chunks())
    codes = keys.indices.to_numpy(zero_copy_only=False)
    dictionary = keys.dictionary.to_pylist()
    days = table.column("obs_date").cast(pa.int32()).to_numpy(zero_copy_only=False)
    vals = table.column("value").to_numpy(zero_copy_only=False)
    del table, keys
    df = pd.DataFrame({"k": codes, "d": days, "v": vals})
    df = df.drop_duplicates(subset=["k", "d"], keep="first")   # read_native's dedup, in scan order
    print(f"  {len(df):,} rows after (key,date) keep-first dedup", flush=True)
    df = df.sort_values(["k", "d"], kind="mergesort")
    karr = df["k"].to_numpy()
    darr = df["d"].to_numpy()
    varr = df["v"].to_numpy()
    del df
    bounds = np.flatnonzero(karr[1:] != karr[:-1]) + 1
    starts = np.concatenate(([0], bounds))
    ends = np.concatenate((bounds, [len(karr)]))
    print(f"  {len(starts):,} distinct series", flush=True)
    return dictionary, karr, darr, varr, np.stack([starts, ends], axis=1)


_DATE_CACHE: dict[int, dt.date] = {}


def _date(day: int) -> dt.date:
    d = _DATE_CACHE.get(day)
    if d is None:
        d = _DATE_CACHE[day] = EPOCH + dt.timedelta(days=int(day))
    return d


def series_bytes(key: str, darr, varr, s: int, e: int) -> bytes:
    rows = [(_date(darr[i]), varr[i]) for i in range(s, e)]
    return _csv_bytes(key, rows)


def main() -> int:
    dictionary, karr, darr, varr, spans = load_deduped_sorted()
    by_key = {dictionary[karr[s]]: (s, e) for s, e in spans}
    if len(by_key) != len(spans):
        print("FATAL: key/span mismatch"); return 1

    # ---- byte-exactness gate (resolver is the referee) --------------------------
    from core.derive_csv import _series_csv_bytes
    rnd = random.Random(20260730)
    sample = rnd.sample(sorted(by_key), min(300, len(by_key)))
    sample += [k for k in MUST_VERIFY if k not in set(sample)]
    bad = 0
    for i, k in enumerate(sample):
        s, e = by_key[k]
        mine = series_bytes(k, darr, varr, s, e)
        theirs = _series_csv_bytes(f"{SRC}:{k}")
        if mine != theirs:
            bad += 1
            if bad <= 3:
                print(f"  MISMATCH {k}\n    mine  : {mine[:120]!r}\n"
                      f"    theirs: {theirs[:120]!r}")
        if (i + 1) % 50 == 0:
            print(f"  verified {i+1}/{len(sample)}", flush=True)
    print(f"verify: {len(sample) - bad}/{len(sample)} byte-identical", flush=True)
    if bad:
        print("REFUSING to run — output would not match the served contract.")
        return 1

    # ---- only-catalogued (must be EXACT for bea: cataloguer proved ids == keys) -
    con = sqlite3.connect(f"file:{os.path.join(ROOT, 'data', 'catalog.db')}?mode=ro", uri=True)
    con.execute("PRAGMA busy_timeout = 180000")
    catalogued = {r[0] for r in con.execute(
        "select series_id from series where source_id=?", (SRC,))}
    con.close()
    uncat = [k for k in by_key if f"{SRC}:{k}" not in catalogued]
    print(f"catalogued {len(catalogued):,}; store keys {len(by_key):,}; "
          f"uncatalogued {len(uncat):,}", flush=True)
    if uncat:
        print(f"REFUSING: store/catalogue drifted since _cat_bea.py ran; first: {uncat[:3]}")
        return 1

    # ---- skip-existing + PUT workers -------------------------------------------
    s3 = r2_util.client(write=True)
    existing: set[str] = set()
    lp = csv_key_prefix(PREFIX, SRC)
    tok = None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": lp, "MaxKeys": 1000}
        if tok:
            kw["ContinuationToken"] = tok
        r = _retry(lambda: s3.list_objects_v2(**kw), "LIST")
        for o in r.get("Contents", []):
            existing.add(o["Key"])
        if not r.get("IsTruncated"):
            break
        tok = r.get("NextContinuationToken")
    print(f"skip-existing: {len(existing):,} already in R2", flush=True)

    q: queue.Queue = queue.Queue(maxsize=4000)
    counts = {"put": 0, "skip": 0, "err": 0}
    lock = threading.Lock()
    STOP = object()

    def worker():
        while True:
            item = q.get()
            if item is STOP:
                q.task_done(); return
            key, body = item
            try:
                _retry(lambda: s3.put_object(Bucket=BUCKET, Key=key, Body=body,
                                             ContentType="text/csv"), "PUT")
                with lock:
                    counts["put"] += 1
                    if counts["put"] % 25_000 == 0:
                        print(f"  put {counts['put']:,} (skip {counts['skip']:,})", flush=True)
            except Exception as e:                            # noqa: BLE001
                with lock:
                    counts["err"] += 1
                    if counts["err"] <= 5:
                        print(f"  PUT FAILED {key}: {str(e)[:90]}", flush=True)
            finally:
                q.task_done()

    threads = []
    for _ in range(24):
        t = threading.Thread(target=worker, daemon=True)
        t.start(); threads.append(t)

    for k, (s, e) in by_key.items():
        rkey = csv_key(PREFIX, SRC, k)
        if rkey in existing:
            with lock:
                counts["skip"] += 1
            continue
        q.put((rkey, series_bytes(k, darr, varr, s, e)))
    q.join()
    for _ in threads:
        q.put(STOP)
    q.join()
    print(f"done: {len(by_key):,} series, put {counts['put']:,}, "
          f"skipped {counts['skip']:,}, errors {counts['err']:,}")
    return 1 if counts["err"] else 0


if __name__ == "__main__":
    sys.exit(main())
