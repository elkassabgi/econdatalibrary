"""Materialise one CSV per usda TABLE to R2 — the only grain this source can be served at.

WHY NOT PER SERIES. usda holds 57,629,841 observations across 15,534,339 series: 3.7
observations each. That is the CSO pathology — a publisher of many short cross-sections — and
hosting it per series means 15.5 million near-trivial CSVs. Measured alternatives:

    grain                              units      obs/unit
    series (as stored)            15,534,339           3.7
    SHORT_DESC alone                  36,066       1,597.9
    SHORT_DESC x agg level            69,488         829.3
    SHORT_DESC x agg x source         72,046         799.9   <- CHOSEN
    + domain category                603,877          95.4

TABLE = (SOURCE_DESC, SHORT_DESC, AGG_LEVEL_DESC). SHORT_DESC is USDA's own one-line name for
the measure ("MILK - FAT TEST, MEASURED IN PCT"), so the unit is one a human recognises, and
LOCATION_DESC / DOMAINCAT_DESC / REFERENCE_PERIOD_DESC stay as ROWS inside it. Nothing is lost
by not keying on them: each table becomes a tidy panel of ~800 observations. This is the cso
(7,896 tables) and insee_melodi (139 flows) precedent.

THE EMITTED ROW ID IS REPAIRED, NOT NATIVE, AND THAT IS DELIBERATE. The stored series_key OMITS
REFERENCE_PERIOD_DESC, which is a real dimension: Maryland winter wheat area harvested for 2020
carries SIX values under one key — MAY, JUN, JUL and AUG FORECAST, JUN ACREAGE, and the final
YEAR estimate. 213,135 (key, obs_date) groups conflict for this reason. Emitting the native key
would put six different numbers on the same id and date in one CSV, and a reader would have no
way to tell a forecast vintage from the final figure. So the row id carries it:

    <native series_key>|REFERENCE_PERIOD_DESC=<value>

matching the pipe-delimited shape the key already uses.

A RESIDUE REMAINS AND IS DISCLOSED, NOT HIDDEN. Even with the reference period and every
geography column in the parquet (STATE_ALPHA, COUNTY_CODE, ASD_DESC, WATERSHED_DESC,
CONGR_DISTRICT_CODE, ZIP_5, REGION_DESC), 2,062 groups still hold more than one value — e.g.
SHEEP OPERATIONS at ZIP CODE level in USDA's '99999' unknown-ZIP bucket, eight values with
nothing in the data to separate them. That is 0.0036% of the source. Those rows are deduped by
keeping the LAST value in sorted order, deterministically, and the count is printed and written
to the run summary. Reporting a clean key would be a lie about data I cannot disambiguate.

ONE SORTED PASS OVER EVERYTHING, NOT PER FILE — and this is not caution, it is measured.
61,644 of the 72,046 tables (86%) have rows in more than one parquet; ('CENSUS', 'HOGS - SALES,
MEASURED IN $', 'COUNTY') is spread across 11. A per-file PUT loop would therefore have written
each of those tables 11 times, each PUT replacing the last, leaving 86% of this source served as
a fragment with no error anywhere — the exact failure tools/derive_pxweb_flowgrain.py documents
and guards. Sorting by the table key makes the grouping correct regardless of how the store is
partitioned; DuckDB spills, so memory stays flat.

    python tools/derive_usda_tables.py --dry-run --limit 20     # inspect, no R2
    python tools/derive_usda_tables.py --bucket econ-data --skip-existing
"""
from __future__ import annotations

import argparse
import csv
import glob
import io
import json
import os
import queue
import sys
import threading
import time
import urllib.parse

import duckdb

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import r2_util                                       # noqa: E402

SOURCE = "usda"
STORE = os.path.join(ROOT, "data", "clean_full", SOURCE)
HEADER = ["series_id", "obs_date", "value"]
TABLE_COLS = ["SOURCE_DESC", "AGG_LEVEL_DESC", "SHORT_DESC"]


def table_id(source_desc, agg, short) -> str:
    """`usda:<SOURCE_DESC>|<AGG_LEVEL_DESC>|<SHORT_DESC>` — pipe-delimited, matching the shape
    usda's own series_key already uses, so an id reads as USDA describes its own data."""
    return f"{SOURCE}:{source_desc}|{agg}|{short}"


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
    ap.add_argument("--limit", type=int, default=0, help="stop after N tables (inspection)")
    ap.add_argument("--memory-limit", default="12GB")
    a = ap.parse_args()
    if not a.dry_run and not a.bucket:
        ap.error("--bucket is required unless --dry-run")

    files = sorted(f.replace("\\", "/")
                   for f in glob.glob(os.path.join(STORE, "**", "*.parquet"), recursive=True))
    if not files:
        raise SystemExit(f"no parquet under {STORE} — refusing to report an empty derive")
    print(f"{len(files)} parquet file(s)", flush=True)

    spill = os.path.join(ROOT, "logs", "_duckspill")
    os.makedirs(spill, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{a.memory_limit}'")
    con.execute(f"SET temp_directory='{spill}'")
    con.execute("SET preserve_insertion_order=false")
    lst = "[" + ",".join(f"'{f}'" for f in files) + "]"

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

    q: queue.Queue = queue.Queue(maxsize=2000)
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
                    if counts["put"] % 5_000 == 0:
                        print(f"  put {counts['put']:,} (skip {counts['skip']:,})", flush=True)
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

    # ONE sorted scan. Ordering by the table key groups a table's rows together no matter which
    # file they came from; ordering by (row id, date) inside it makes the CSV deterministic and
    # makes the duplicate check a comparison with the previous row rather than a set.
    cur = con.execute(f"""
        SELECT SOURCE_DESC, AGG_LEVEL_DESC, SHORT_DESC,
               series_key || '|REFERENCE_PERIOD_DESC=' || COALESCE(REFERENCE_PERIOD_DESC,'')
                 AS row_id,
               obs_date, value
        FROM read_parquet({lst})
        WHERE value IS NOT NULL AND obs_date IS NOT NULL
        ORDER BY SOURCE_DESC, AGG_LEVEL_DESC, SHORT_DESC, row_id, obs_date, value""")

    t0 = time.time()
    cur_key, rows, n_tables, dropped = None, [], 0, 0
    n_rows_written, n_empty = 0, 0
    last_pair = None

    def flush():
        nonlocal n_tables, n_rows_written, n_empty
        if cur_key is None:
            return
        if not rows:
            n_empty += 1
            return
        n_rows_written += len(rows)
        sid = table_id(*cur_key)
        n_tables += 1
        if a.dry_run:
            if n_tables <= 3:
                print(f"  would PUT {csv_key(a.prefix, sid)} "
                      f"({len(rows):,} rows, {len(_rows_csv(rows)):,} B)")
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
        for src, agg, short, rid, d, v in batch:
            k = (src, agg, short)
            if k != cur_key:
                flush()
                if a.limit and n_tables >= a.limit:
                    cur_key = None
                    break
                cur_key, rows, last_pair = k, [], None
            pair = (rid, d)
            if pair == last_pair:
                # Same row id AND date twice: the 2,062 residual conflicts no column in the
                # store can separate. Keeping "the last row" is only DETERMINISTIC because
                # `value` is the final ORDER BY term, so the survivor is the MAXIMUM value.
                # Without that tiebreak the sort leaves equal keys in an unspecified order and
                # a re-derive could publish a different number for the same id and date — a
                # difference nobody would ever see, on 2,062 rows, forever.
                dropped += 1
                rows[-1] = (rid, d.isoformat(), v)
                continue
            last_pair = pair
            rows.append((rid, d.isoformat(), v))
        if a.limit and n_tables >= a.limit:
            break
    flush()

    if not a.dry_run:
        for _ in threads:
            q.put(STOP)
        q.join()

    dt = time.time() - t0
    print(f"\ntables: {n_tables:,}   put {counts['put']:,}   skipped {counts['skip']:,}   "
          f"errors {counts['err']:,}   {dt:,.0f}s")
    # Percentage OF THE ROWS WRITTEN, which is the only denominator that means anything here.
    # The first version divided by (dropped + 1) and printed "99.9942%" for 17,299 collapsed
    # rows out of 53.5 million — a figure that would have read as "almost everything was a
    # duplicate" in a summary nobody would re-derive.
    pct = (100.0 * dropped / n_rows_written) if n_rows_written else 0.0
    print(f"rows written: {n_rows_written:,}")
    print(f"duplicate (row_id, obs_date) rows collapsed: {dropped:,} ({pct:.4f}% of rows "
          f"written) — the residual conflicts no column in the store can separate")
    print(f"tables with no usable row (every value or date NULL), so no CSV: {n_empty:,}")
    summary = os.path.join(ROOT, "logs", "usda_tables_summary.json")
    json.dump({"tables": n_tables, "rows_written": n_rows_written,
               "put": counts["put"], "skipped": counts["skip"], "errors": counts["err"],
               "duplicates_collapsed": dropped, "duplicate_pct": round(pct, 4),
               "tables_with_no_usable_row": n_empty,
               "seconds": round(dt)}, open(summary, "w"), indent=1)
    print(f"summary -> {summary}")
    return 1 if counts["err"] else 0


if __name__ == "__main__":
    sys.exit(main())
