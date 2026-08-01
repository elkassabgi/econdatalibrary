"""Materialise istat at FLOW grain to R2 — one CSV per dataflow, giants split by a dimension.

WHY NOT PER SERIES. istat holds 398,619,720 observations across 43,564,079 series: 9.2
observations each. Hosting it per series means 43.5 million CSVs — four and a half times the
ENTIRE library's current 9,518,924 served series, for one source. Flow grain gives 1,226 units,
which is what ISTAT itself publishes and titles.

FLOW GRAIN ALONE IS NOT ENOUGH, and ROW COUNT is what says where — not series count. Measured:
110 flows exceed 500,000 rows, holding 178,762,007 rows between them, and 183_277 by itself is
52,957,388 rows. My first attempt split the ten flows with the most SERIES and missed most of
that: `111_263` is 7,332,532 rows, roughly half a gigabyte of CSV, and is nowhere near the top
ten by series.

So any flow over --max-rows is split, and its splitter is DISCOVERED per flow. istat keys are
colon-joined NAME=VALUE pairs,

    FREQ=A:ATECO_2007=68:ADDETTI=W0_9:FORMGIUR=1230:ITTER107=ITC48:CAR_ART=0:TIPO_DATO=AEN

so each dimension's cardinality is measured in that flow's own keys and the SMALLEST one that
still divides the flow below the bound wins — the coarsest split that works, giving fewer and
more meaningful units rather than maximal shredding. Where no single dimension suffices, a
hierarchical truncation of the widest is tried (ISTAT territory codes nest: IT > ITC > ITC1 >
ITC48, so a prefix is a real geography). A flow that neither can divide is REFUSED and named,
never quietly emitted as one enormous object.

DUPLICATE (series_key, obs_date) ROWS ARE COLLAPSED DETERMINISTICALLY AND COUNTED. Sorting puts
`value` last, so the survivor is the maximum rather than whichever row the sort happened to
leave last — the same non-determinism that would have shipped in the usda derive. The per-flow
count is printed and totalled, never silent.

    python tools/derive_istat_flows.py --dry-run --limit 5
    python tools/derive_istat_flows.py --bucket econ-data --skip-existing
"""
from __future__ import annotations

import argparse
import csv
import glob
import io
import json
import os
import queue
import re
import sys
import threading
import time
import urllib.parse

import duckdb
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import r2_util                                       # noqa: E402

SOURCE = "istat"
STORE = os.path.join(ROOT, "data", "clean_full", SOURCE)
HEADER = ["series_id", "obs_date", "value"]

# THE SPLIT IS CHOSEN PER FLOW, AT RUN TIME, FROM ROW COUNT — not from a hardcoded list.
#
# My first version hardcoded the ten flows with the most SERIES. That was the wrong measure:
# a CSV's size follows its ROW count, and measuring rows showed 110 flows above 500,000 rows
# that the series-based list missed, holding 178,762,007 rows between them. `111_263` alone is
# 7,332,532 rows — roughly half a gigabyte of CSV — and it is not in the top ten by series.
#
# So: any flow over --max-rows is split, and its splitter is discovered by measuring each named
# dimension's cardinality in that flow's own keys. Preference goes to the SMALLEST cardinality
# that still divides the flow below the bound, so the split is as coarse as it can be while
# still working — fewer, more meaningful units rather than maximal shredding.
#
# If no single dimension divides it enough, a hierarchical TRUNCATION of the widest dimension is
# tried (ISTAT territory codes nest: IT > ITC > ITC1 > ITC48, so a prefix is a real geography).
# If that still fails the flow is REFUSED and named, never silently emitted as one huge object.
MAX_ROWS_DEFAULT = 500_000


def choose_split(con, path: str, n_rows: int, max_rows: int):
    """-> (dimension, truncate_chars, n_parts) or (None, 0, 1) when no split is needed.

    CARDINALITY IS NOT BALANCE, and assuming it is produced a split that did not split. A first
    version picked the smallest dimension with at least ceil(n_rows/max_rows) distinct values,
    which is only sufficient if the rows are spread evenly across them. They are not: flow
    101_1015 has a DESTINATION_WINEGRAPES dimension with 4 values, comfortably "enough", whose
    parts came out at 775,206 / 191 / 81 rows. The big part was still over the bound, so the
    flow was split into an unusable object plus three trivia.

    So each candidate is CHECKED: group by the candidate and take the largest part. The first
    candidate whose largest part fits is used. Candidates are tried from lowest cardinality up,
    so the accepted split is the coarsest one that actually works.
    """
    if n_rows <= max_rows:
        return None, 0, 1
    sample = con.execute(
        f"select series_key from read_parquet('{path}') limit 1").fetchone()
    if not sample:
        return None, 0, 1
    dims = [p.split("=", 1)[0] for p in sample[0].split(":") if "=" in p]
    card = {}
    for d in dims:
        try:
            card[d] = con.execute(
                f"select count(distinct regexp_extract(series_key, '{d}=([^:]*)', 1)) "
                f"from read_parquet('{path}')").fetchone()[0]
        except Exception:                            # noqa: BLE001
            continue

    def largest_part(expr):
        return con.execute(
            f"select max(n) from (select count(*) n from read_parquet('{path}') "
            f"where value is not null and obs_date is not null group by {expr})").fetchone()[0]

    # plain dimensions, coarsest first
    for c, d in sorted((c, d) for d, c in card.items() if 2 <= c <= 2000):
        try:
            if largest_part(f"regexp_extract(series_key, '{d}=([^:]*)', 1)") <= max_rows:
                return d, 0, c
        except Exception:                            # noqa: BLE001
            continue
    # hierarchical truncations of the widest dimension (ISTAT codes nest)
    if card:
        widest = max(card, key=card.get)
        for t in (3, 4, 5, 6):
            expr = f"substr(regexp_extract(series_key, '{widest}=([^:]*)', 1), 1, {t})"
            try:
                c = con.execute(f"select count(distinct {expr}) "
                                f"from read_parquet('{path}')").fetchone()[0]
                if 2 <= c <= 2000 and largest_part(expr) <= max_rows:
                    return widest, t, c
            except Exception:                        # noqa: BLE001
                continue
    return "", 0, 0                                  # refused; the caller names it


def flow_id(stem: str, part: str | None = None) -> str:
    """`istat:<flow>` or `istat:<flow>#<dimension-value>` for a split giant.

    '#' separates the split because it cannot occur in an ISTAT flow id or dimension value, so
    an id round-trips unambiguously — unlike ':' or '|', which the keys already use.
    """
    return f"{SOURCE}:{stem}" + (f"#{part}" if part else "")


def csv_key(prefix: str, sid: str) -> str:
    return f"{prefix}/{urllib.parse.quote(sid, safe='')}.csv"


def _rows_csv(rows) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(HEADER)
    for sid, d, v in rows:
        w.writerow([sid, d, v])
    return buf.getvalue().encode("utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket")
    ap.add_argument("--prefix", default="series")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip-existing", action="store_true")
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--memory-limit", default="10GB")
    ap.add_argument("--max-rows", type=int, default=MAX_ROWS_DEFAULT,
                    help="split any flow larger than this many rows")
    a = ap.parse_args()
    if not a.dry_run and not a.bucket:
        ap.error("--bucket is required unless --dry-run")

    files = sorted(f.replace("\\", "/") for f in glob.glob(os.path.join(STORE, "*.parquet"))
                   if not f.endswith("__series.parquet"))
    if not files:
        raise SystemExit(f"no parquet under {STORE} — refusing to report an empty derive")
    print(f"{len(files)} dataflow file(s); splitting any over {a.max_rows:,} rows",
          flush=True)

    spill = os.path.join(ROOT, "logs", "_duckspill")
    os.makedirs(spill, exist_ok=True)

    existing = set()
    s3 = None
    if not a.dry_run:
        s3 = r2_util.client(write=True)
        if a.skip_existing:
            pref = f"{a.prefix}/{urllib.parse.quote(SOURCE + ':', safe='')}"
            tok = None
            while True:
                kw = {"Bucket": a.bucket, "Prefix": pref, "MaxKeys": 1000}
                if tok:
                    kw["ContinuationToken"] = tok
                r = s3.list_objects_v2(**kw)
                existing.update(o["Key"] for o in r.get("Contents", []))
                if not r.get("IsTruncated"):
                    break
                tok = r["NextContinuationToken"]
            print(f"skip-existing: {len(existing):,} already in R2", flush=True)

    q: queue.Queue = queue.Queue(maxsize=1000)
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
                s3.put_object(Bucket=a.bucket, Key=key, Body=body, ContentType="text/csv")
                with lock:
                    counts["put"] += 1
                    if counts["put"] % 200 == 0:
                        print(f"  put {counts['put']:,}", flush=True)
            except Exception as e:                             # noqa: BLE001
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

    t0 = time.time()
    n_units = 0
    dropped_total = 0
    refused = []
    for i, f in enumerate(sorted(files), 1):
        stem = os.path.splitext(os.path.basename(f))[0]
        con = duckdb.connect()
        con.execute(f"SET memory_limit='{a.memory_limit}'")
        con.execute(f"SET temp_directory='{spill}'")
        con.execute("SET preserve_insertion_order=false")
        n_rows_flow = pq.ParquetFile(f).metadata.num_rows
        dim, trunc, n_parts = choose_split(con, f, n_rows_flow, a.max_rows)
        if dim == "":
            refused.append((stem, n_rows_flow))
            print(f"  [{i}/{len(files)}] {stem}: REFUSED — {n_rows_flow:,} rows and no "
                  f"dimension divides it below {a.max_rows:,}; NOT emitted", flush=True)
            con.close()
            continue
        # `value` LAST in the ORDER BY so a collapsed duplicate is the MAXIMUM, deterministically.
        if dim:
            expr = f"regexp_extract(series_key, '{dim}=([^:]*)', 1)"
            if trunc:
                expr = f"substr({expr}, 1, {trunc})"
            sel = f"{expr} AS part, series_key, obs_date, value"
            order = "part, series_key, obs_date, value"
        else:
            sel = "'' AS part, series_key, obs_date, value"
            order = "series_key, obs_date, value"
        try:
            cur = con.execute(f"""
                SELECT {sel} FROM read_parquet('{f}')
                WHERE value IS NOT NULL AND obs_date IS NOT NULL
                ORDER BY {order}""")
        except Exception as e:                                 # noqa: BLE001
            print(f"  [{i}/{len(files)}] {stem}: SCAN FAILED {type(e).__name__} "
                  f"{str(e)[:70]}", flush=True)
            con.close()
            continue

        cur_part, rows, last, dropped = None, [], None, 0

        def flush(part):
            nonlocal n_units
            if not rows:
                return
            sid = flow_id(stem, part or None)
            n_units += 1
            if a.dry_run:
                if n_units <= 3:
                    print(f"  would PUT {csv_key(a.prefix, sid)} ({len(rows):,} rows)")
                return
            key = csv_key(a.prefix, sid)
            if key in existing:
                with lock:
                    counts["skip"] += 1
                return
            q.put((key, _rows_csv(rows)))

        while True:
            batch = cur.fetchmany(200_000)
            if not batch:
                break
            for part, k, d, v in batch:
                if part != cur_part:
                    flush(cur_part)
                    cur_part, rows, last = part, [], None
                if (k, d) == last:
                    dropped += 1
                    rows[-1] = (k, d.isoformat(), v)
                    continue
                last = (k, d)
                rows.append((k, d.isoformat(), v))
        flush(cur_part)
        con.close()
        dropped_total += dropped
        if i % 100 == 0 or i == len(files) or dim:
            print(f"  [{i}/{len(files)}] {stem}{' split by ' + dim if dim else ''}: "
                  f"{n_units:,} units so far, {dropped_total:,} dup rows collapsed, "
                  f"{time.time()-t0:,.0f}s", flush=True)
        if a.limit and i >= a.limit:
            break

    if not a.dry_run:
        for _ in threads:
            q.put(STOP)
        q.join()

    dt = time.time() - t0
    print(f"\nunits: {n_units:,}   put {counts['put']:,}   skipped {counts['skip']:,}   "
          f"errors {counts['err']:,}   {dt:,.0f}s")
    print(f"duplicate (series_key, obs_date) rows collapsed: {dropped_total:,}")
    if refused:
        print(f"REFUSED (too large, no usable splitter) — {len(refused)}:")
        for st, nr in refused:
            print(f"   {st:44s} {nr:>12,} rows")
    summary = os.path.join(ROOT, "logs", "istat_flows_summary.json")
    json.dump({"units": n_units, "put": counts["put"], "skipped": counts["skip"],
               "errors": counts["err"], "duplicates_collapsed": dropped_total,
               "seconds": round(dt)}, open(summary, "w"), indent=1)
    print(f"summary -> {summary}")
    return 1 if counts["err"] else 0


if __name__ == "__main__":
    sys.exit(main())
