"""Derive statcan CSVs at TABLE grain and put them in R2.

statcan is the largest source in the library: 8,207 files, 175.3 GB, 56,845,453,642 observations
across ~5,258,059,229 series — 10.81 observations per series. Series grain is arithmetically
impossible twice over. It would mean five BILLION CSVs averaging eleven rows each, and the
catalogue alone would not fit: D1 sits at ~6.9 GB of a 10 GB hard ceiling with 10.9M rows total,
so five billion rows is off by three orders of magnitude (#45).

THE UNIT IS THE TABLE, and it is the publisher's own unit. Every file is one StatCan Product ID
— 10100001.parquet, 11100058.parquet — which is exactly what Statistics Canada names, cites and
versions. 8,207 units is the same order as istat's 14,258 and ilostat's 3,225, both of which
came out of the identical measurement (9.2 and 10.0 obs per series).

SPLITTING USES REAL COLUMNS, as ilostat's does, because statcan carries them: geo, uom,
coordinate, status alongside series_key/obs_date/value. No regex over the key is needed. The
largest single table is 962,150,400 rows, so splitting is not optional — and a table that large
needs a PAIR of columns, which is why the pair search is here from the start rather than added
after it refused something (R219).

CARDINALITY IS NOT BALANCE: every candidate split is checked by measuring the LARGEST resulting
part, never by counting distinct values.

Not yet run at scale. --dry-run reports the split decisions without contacting R2, which is how
the splitter should be validated on the giant tables before a multi-day derive is committed to.
"""
from __future__ import annotations

import argparse
import csv
import glob
import gzip
import io
import json
import os
import queue
import sys
import threading
import time
import urllib.parse

import duckdb
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import r2_util                                        # noqa: E402

SOURCE = "statcan"
STORE = os.path.join(ROOT, "data", "clean_full", SOURCE)
HEADER = ["series_id", "obs_date", "value"]
MAX_ROWS_DEFAULT = 500_000
# Real dimension columns, coarsest-looking first. `status` is a per-observation quality flag and
# is NOT a splitter -- splitting on it would scatter one series across parts, the same defect
# that made insee_sdmx unusable (10.8M rows under 817 keys, all built from observation
# attributes). obs_date is excluded for the usual reason: splitting a time series by time makes
# every part useless alone.
DIM_COLS = ("uom", "geo", "coordinate")


def choose_split(con, path: str, n_rows: int, max_rows: int):
    """(column-or-pair, parts) needing a split; (None, 1) if it fits; ("", 0) if refused."""
    if n_rows <= max_rows:
        return None, 1
    cols = [c for c in DIM_COLS if c in set(pq.read_schema(path).names)]
    card = {}
    for c in cols:
        try:
            card[c] = con.execute(
                f'select count(distinct "{c}") from read_parquet(\'{path}\')').fetchone()[0]
        except Exception:                                       # noqa: BLE001
            continue

    def largest(expr: str):
        return con.execute(
            f"select max(n) from (select count(*) n from read_parquet('{path}') "
            f"where value is not null and obs_date is not null group by {expr})").fetchone()[0]

    for c, d in sorted((c, d) for d, c in card.items() if 2 <= c <= 2000):
        try:
            if largest(f'"{d}"') <= max_rows:
                return d, c
        except Exception:                                       # noqa: BLE001
            continue
    # HIERARCHICAL TRUNCATION OF `coordinate`, which is where statcan's giants actually divide.
    # Measured on 12100152 (427,009,412 rows): uom has ONE distinct value, status ONE, geo 14
    # (largest group 135,795,854 — far too coarse), and coordinate 17,732,442 (largest group 429
    # — far too fine to be a unit). There is nothing in between, which is why single columns and
    # pairs both refused it.
    #
    # But a coordinate is a dot-separated dimension tuple — '59.9.37.1.85.3.100.1400' — so
    # truncating it to k segments walks the publisher's own hierarchy from coarse to fine. Same
    # technique the istat derive uses for nested ISTAT territory codes and derive_census_tables
    # uses for HS commodity codes, applied to the column statcan actually nests.
    # ONE SCAN, THEN ROLL UP — not one scan per truncation level. The obvious loop issues a
    # count(distinct) and a group-by for each k, so seven levels is ~14 full passes over the
    # table; on 12100152 (427M rows) that ran 78 CPU-MINUTES without finishing, and the derive
    # has 8,207 tables to get through. Counting each FULL coordinate once gives a 17.7M-row
    # summary, and every truncation level is then a cheap group-by over that summary rather than
    # over the raw rows. Same answers, one pass.
    if "coordinate" in card:
        try:
            con.execute(f"""
                CREATE OR REPLACE TEMP TABLE _coord AS
                SELECT "coordinate" AS c, count(*) AS n
                FROM read_parquet('{path}')
                WHERE value IS NOT NULL AND obs_date IS NOT NULL
                GROUP BY 1""")
            ladder = []
            for k in range(1, 9):
                trunc = ("array_to_string(array_slice(string_split(c, '.'), 1, "
                         f"{k}), '.')")
                parts, biggest = con.execute(f"""
                    SELECT count(*), max(s) FROM (
                      SELECT {trunc} AS p, sum(n) AS s FROM _coord GROUP BY 1)""").fetchone()
                ladder.append((k, parts or 0, biggest or 0))
                if parts and 2 <= parts <= 20_000 and biggest and biggest <= max_rows:
                    return f"coordinate:{k}", parts
            # NO LEVEL SATISFIED BOTH BOUNDS — say so WITH the ladder, because "refused" alone
            # hides that this is a parameter choice and not a property of the data. Measured on
            # 12100152 (427,009,412 rows):
            #     k=4   6,514 parts   largest 2,172,577   <- fits the part cap, over the row bound
            #     k=5 317,350 parts   largest   339,545   <- fits the row bound, absurd part count
            # There is no k that fits both at max_rows=500,000, and the honest resolution is to
            # raise --max-rows for this source (k=4 becomes legal at 3,000,000, giving 6,514
            # parts of ~65 MB) rather than emit a third of a million objects for one table.
            print(f"      coordinate ladder (no level fits parts<=20,000 AND rows<={max_rows:,}):",
                  flush=True)
            for k, parts, biggest in ladder:
                fits = ("parts ok" if 2 <= parts <= 20_000 else "parts too many") + ", " + \
                       ("rows ok" if biggest <= max_rows else "rows too big")
                print(f"        k={k}  {parts:>12,} parts  largest {biggest:>14,}   {fits}",
                      flush=True)
        except Exception:                                       # noqa: BLE001
            pass

    usable = sorted((c, d) for d, c in card.items() if c >= 2)
    for prod, d1, d2 in sorted(
            (c1 * c2, n1, n2) for i, (c1, n1) in enumerate(usable)
            for c2, n2 in usable[i + 1:] if 2 <= c1 * c2 <= 20_000):
        try:
            if largest(f'"{d1}", "{d2}"') > max_rows:
                continue
            # '~' joins the two values in the part label, so it must be absent from both
            # vocabularies or the label cannot be split back apart. Checked, not assumed.
            if con.execute(f"select count(*) from read_parquet('{path}') "
                           f"where \"{d1}\" like '%~%' or \"{d2}\" like '%~%'").fetchone()[0]:
                continue
            return f"{d1}+{d2}", prod
        except Exception:                                       # noqa: BLE001
            continue
    return "", 0


def part_expr(dim: str) -> str:
    """ONE definition of the part label, imported by the catalogue and the resolver.

    Three shapes, and the catalogue MUST use this function rather than reimplement any of them —
    a split expression rebuilt elsewhere drifts into ids no object answers to:
        "geo"              a single column
        "uom+geo"          a pair, joined with '~' (verified absent from both vocabularies)
        "coordinate:3"     the first 3 dot-segments of the coordinate hierarchy
    """
    if dim.startswith("coordinate:"):
        k = int(dim.split(":", 1)[1])
        return (f"array_to_string(array_slice(string_split(\"coordinate\", '.'), 1, {k}), '.')")
    if "+" in dim:
        d1, d2 = dim.split("+", 1)
        return f"\"{d1}\" || '~' || \"{d2}\""
    return f'"{dim}"'


def unit_id(pid: str, part: str | None = None) -> str:
    return f"{SOURCE}:{pid}" + (f"#{part}" if part else "")


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
    ap.add_argument("--largest-first", action="store_true",
                    help="process the biggest tables first — use with --dry-run to validate the "
                         "splitter where it actually has to work")
    ap.add_argument("--memory-limit", default="8GB")
    ap.add_argument("--max-rows", type=int, default=MAX_ROWS_DEFAULT)
    ap.add_argument("--only", default="")
    a = ap.parse_args()
    if not a.dry_run and not a.bucket:
        ap.error("--bucket is required unless --dry-run")

    files = sorted(f.replace("\\", "/") for f in
                   glob.glob(os.path.join(STORE, "**", "*.parquet"), recursive=True)
                   if not f.endswith("__series.parquet"))
    if a.only:
        want = {s.strip() for s in a.only.split(",") if s.strip()}
        files = [f for f in files if os.path.splitext(os.path.basename(f))[0] in want]
        got = {os.path.splitext(os.path.basename(f))[0] for f in files}
        if got != want:
            raise SystemExit(f"--only names {len(want)}; {len(got)} exist. "
                             f"Missing: {', '.join(sorted(want - got))}")
    if not files:
        raise SystemExit(f"no parquet under {STORE} — refusing to report an empty derive")
    if a.largest_first:
        # SORT BY BYTES, NOT ROWS. Row count means opening 8,207 parquet footers, and a
        # 962M-row file's footer carries thousands of row-group entries — that sort alone burned
        # 40 CPU-MINUTES here without finishing. File size is free from the directory entry and
        # ranks the giants identically for this purpose. The distribution makes the point: the
        # largest table is 4,075 MB and the MEDIAN is 0.03 MB, so a handful of tables dominate
        # and everything else is trivial.
        files = [f for _n, f in sorted(((os.path.getsize(f), f) for f in files), reverse=True)]
    print(f"{len(files):,} table(s); splitting any over {a.max_rows:,} rows"
          f"{' — LARGEST FIRST' if a.largest_first else ''}", flush=True)

    # PER-PROCESS SPILL DIRECTORY. Every tool here used a shared logs/_duckspill, which is fine
    # until two of them spill at the same moment — then one deletes the other's temp storage and
    # both die: "IO Error: Failed to delete file duckdb_temp_storage_DEFAULT-0.tmp: The system
    # cannot find the file specified", and the sibling exits 139. Observed exactly that running
    # this probe alongside a measurement on the same table. DuckDB names its temp file after the
    # DATABASE, not the process, so the collision is silent until it isn't.
    spill = os.path.join(ROOT, "logs", "_duckspill", f"pid{os.getpid()}")
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

    # maxsize 64, not 1000: bodies are whole unit CSVs (up to ~100 MB raw). A
    # 1000-slot queue is an unbounded-in-BYTES buffer — with the census giants
    # producing units faster than 16 uploaders drained them, the 2026-08-19
    # relaunch died of MemoryError at giant 10/8207 while the box also hosted a
    # 63 GB imts finalize. 64 compressed bodies (~10-20 MB each after the gzip
    # below) bound the buffer to ~1 GB and give backpressure instead of death.
    q: queue.Queue = queue.Queue(maxsize=64)
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
                # GZIP AT REST (Ahmed 2026-08-18: "bring back statcan compressed").
                # This tool has its OWN uploader predating the fleet gzip writers —
                # the first statcan campaign uploaded 1.37 TB uncompressed because
                # of exactly this gap. Compression normally happens at ENQUEUE
                # (flush) so the queue buffers small bodies; the magic-byte check
                # keeps this path safe for both compressed and raw producers.
                if body[:2] != b"\x1f\x8b":
                    body = r2_util.gzip_bytes(body)
                s3.put_object(Bucket=a.bucket, Key=key, Body=body, ContentType="text/csv",
                              ContentEncoding="gzip")
                with lock:
                    counts["put"] += 1
                    if counts["put"] % 500 == 0:
                        print(f"  put {counts['put']:,}", flush=True)
            except Exception as e:                              # noqa: BLE001
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
    split_map: dict = {}
    for i, f in enumerate(files, 1):
        pid = os.path.splitext(os.path.basename(f))[0]
        con = duckdb.connect()
        con.execute(f"SET memory_limit='{a.memory_limit}'")
        con.execute(f"SET temp_directory='{spill}'")
        con.execute("SET preserve_insertion_order=false")
        con.execute("SET enable_progress_bar=false")
        n_rows = pq.ParquetFile(f).metadata.num_rows
        dim, n_parts = choose_split(con, f, n_rows, a.max_rows)
        if dim:
            split_map[pid] = {"dim": dim, "parts": n_parts, "rows": n_rows}
            # PERSIST THE DECISION IMMEDIATELY, not at the end of an 8,207-table run. Choosing a
            # split is the expensive half — ~7 minutes on a 427M-row cube — and a map written
            # only on a clean finish is lost entirely if the run is interrupted, which for a
            # multi-day job is the likely case, not the unlikely one. Same reasoning as
            # stat_slovenia's sweep offset: state that exists to survive a kill must be written
            # before the kill. --skip-existing then makes a restart cheap in BOTH halves.
            if not a.dry_run:
                try:
                    with open(os.path.join(STORE, "_split_map.json"), "w",
                              encoding="utf-8") as fh:
                        json.dump(split_map, fh, indent=1, sort_keys=True)
                except OSError:
                    pass    # a lost map costs re-deciding, never correctness
        if dim == "":
            refused.append((pid, n_rows))
            print(f"  [{i}/{len(files)}] {pid}: REFUSED — {n_rows:,} rows and no column pair "
                  f"divides it below {a.max_rows:,}; NOT emitted", flush=True)
            con.close()
            continue
        if a.dry_run:
            if dim or i <= 5 or i % 500 == 0:
                print(f"  [{i}/{len(files)}] {pid}: {n_rows:>12,} rows -> "
                      f"{('split by ' + dim + f' ({n_parts:,} parts)') if dim else 'whole'}"
                      f"   {time.time()-t0:,.0f}s", flush=True)
            con.close()
            n_units += n_parts if dim else 1
            if a.limit and i >= a.limit:
                break
            continue

        if dim:
            sel = f"{part_expr(dim)} AS part, series_key, obs_date, value"
            order = "part, series_key, obs_date, value"
        else:
            sel = "'' AS part, series_key, obs_date, value"
            order = "series_key, obs_date, value"
        try:
            cur = con.execute(f"""
                SELECT {sel} FROM read_parquet('{f}')
                WHERE value IS NOT NULL AND obs_date IS NOT NULL
                ORDER BY {order}""")
        except Exception as e:                                  # noqa: BLE001
            print(f"  [{i}/{len(files)}] {pid}: SCAN FAILED {type(e).__name__} "
                  f"{str(e)[:70]}", flush=True)
            con.close()
            continue

        cur_part, rows, last, dropped = None, [], None, 0

        def flush(part):
            nonlocal n_units
            if not rows:
                return
            sid = unit_id(pid, part or None)
            n_units += 1
            key = csv_key(a.prefix, sid)
            if key in existing:
                with lock:
                    counts["skip"] += 1
                return
            # Compress BEFORE enqueueing: the queue then buffers ~10-20 MB gzip
            # bodies instead of ~100 MB raw CSVs (measured 5.4-11x on statcan).
            # Same deterministic bytes as the worker path (mtime=0).
            q.put((key, r2_util.gzip_bytes(_rows_csv(rows))))

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
            print(f"  [{i}/{len(files)}] {pid}{' split by ' + dim if dim else ''}: "
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
            print(f"   {st:24s} {nr:>14,} rows")

    smap = os.path.join(STORE, "_split_map.json")
    if a.dry_run:
        print(f"(dry run: split map NOT written; this run chose {len(split_map)} split(s))")
    else:
        out_map = split_map
        if a.only:
            try:
                out_map = json.load(open(smap, encoding="utf-8"))
            except (OSError, ValueError):
                out_map = {}
            for st in {os.path.splitext(os.path.basename(x))[0] for x in files}:
                out_map.pop(st, None)
            out_map.update(split_map)
        with open(smap, "w", encoding="utf-8") as fh:
            json.dump(out_map, fh, indent=1, sort_keys=True)
        print(f"split map ({len(out_map):,} table(s)) -> {smap}")

    # Every terminal disposition gets a key (R219).
    summary = os.path.join(ROOT, "logs", "statcan_tables_summary.json")
    json.dump({"considered": len(files), "units": n_units, "put": counts["put"],
               "skipped": counts["skip"], "errors": counts["err"],
               "refused": [{"table": st, "rows": nr} for st, nr in refused],
               "refused_rows": sum(nr for _st, nr in refused),
               "duplicates_collapsed": dropped_total, "seconds": round(dt),
               # THE PARAMETER THE CATALOGUER MUST MATCH, RECORDED WHERE IT CAN READ IT
               # (R833). Persisted nowhere before: not here, and not in _split_map.json,
               # whose entries are {dim, parts, rows}. It was recoverable only by
               # inference from min(split rows), which bounds the cap from ABOVE and not
               # below - so a cataloguer run at the wrong cap read as a frozen pipeline.
               "max_rows": int(a.max_rows),
               "dry_run": bool(a.dry_run)}, open(summary, "w"), indent=1)
    print(f"summary -> {summary}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
