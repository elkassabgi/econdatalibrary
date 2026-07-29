"""tools/derive_pxweb_flowgrain.py — materialize one CSV per PxWeb table to R2.

For each flow-grain table (series_key prefix) in the 9 PxWeb sources, project the table's
rows to a long CSV and PUT it at series/<encodeURIComponent(series_id)>.csv so the Worker's
/v1/series/{id}.csv streams it. series_id = "<source>:<prefix>" (matches the flow-grain
catalog). CSV shape follows the contract series.ts expects:
    header:  series_id,obs_date,value          (series_id column = the native series_key)
    rows:    sorted by (series_id, obs_date)    (obs_date ISO -> lexical == chronological)
Reads each SUBJECT parquet once (memory-bounded), groups rows by prefix. INERT until D1 has
the catalog rows (the Worker 404s on an uncataloged id), so uploading is safe pre-flip.

  python tools/derive_pxweb_flowgrain.py --sample 5 --source stat_slovenia   # local, inspect
  python tools/derive_pxweb_flowgrain.py --dry-run                            # count, no R2
  python tools/derive_pxweb_flowgrain.py --bucket econ-data [--skip-existing] # full upload
"""
from __future__ import annotations
import argparse, csv, glob, io, os, sys, time, urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
import pyarrow.compute as pc
import pyarrow.parquet as pq

# DERIVED, NEVER HARDCODED — same dead-drive hazard the flow-grain cataloguer carried. The
# store moved off D:; globbing a directory that does not exist returns [], so this would have
# uploaded NOTHING while printing "0 tables" and exiting 0. A derive that silently publishes
# nothing is indistinguishable from one with nothing to do.
MAIN = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(MAIN, "data", "clean_full")
if not os.path.isdir(DATA):                    # fail loudly rather than "derive" nothing
    raise SystemExit(f"parquet store not found at {DATA} — refusing to report empty scans")
SOURCES = ["ssb", "stat_slovenia", "stat_latvia", "dst", "scb", "statfin", "hagstofa", "stat_estonia", "bfs"]
PREFIX_RE = r"^(?P<p>.*?):[^:=]*="
SAMPLE_DIR = r"D:/temp/claude/D--research-hfdatalibrary/5bda36f5-59a1-4804-b441-06c56c3755da/scratchpad/derive_sample"


def group_subject(path: str) -> dict:
    """{prefix -> [(native_key, obs_date_iso, value), ...]} for one subject parquet."""
    out: dict[str, list] = {}
    pf = pq.ParquetFile(path)
    for batch in pf.iter_batches(columns=["series_key", "obs_date", "value"], batch_size=500_000):
        keys = batch.column("series_key")
        p = pc.extract_regex(keys, pattern=PREFIX_RE).field("p")
        # null OR empty capture (a no-"=" time-only table) -> use the whole key as prefix
        usable = pc.and_(pc.invert(pc.is_null(p)), pc.not_equal(p, ""))
        pref = pc.if_else(usable, p, keys).to_pylist()
        kl = keys.to_pylist()
        dl = batch.column("obs_date").to_pylist()
        vl = batch.column("value").to_pylist()
        for p, k, d, v in zip(pref, kl, dl, vl):
            if d is None or v is None:
                continue
            out.setdefault(p, []).append((k, d.isoformat(), v))
    return out


def prefixes_split_across_files(files: list) -> set:
    """Prefixes whose rows live in MORE THAN ONE parquet — the ones a per-file PUT corrupts.

    The upload loop below reads one file at a time and PUTs `series/<source>:<prefix>.csv`
    per file. That is correct only while a table lives entirely inside one parquet. When it
    does not, the second file's PUT REPLACES the first's object, so the served CSV silently
    holds only the last file's slice of the table — no error, no short read, just a table
    missing rows nobody counted. cso surfaced it: 7,988 (file, prefix) pairs against 7,896
    distinct prefixes, so 92 tables were set up to be truncated on upload.

    One cheap pass over series_key only (no values, no dates) tells us which they are, so the
    common case stays streaming and only the genuinely split tables are buffered.
    """
    where: dict[str, set] = {}
    for f in files:
        fn = os.path.basename(f)
        pf = pq.ParquetFile(f)
        for batch in pf.iter_batches(columns=["series_key"], batch_size=500_000):
            keys = batch.column("series_key")
            p = pc.extract_regex(keys, pattern=PREFIX_RE).field("p")
            usable = pc.and_(pc.invert(pc.is_null(p)), pc.not_equal(p, ""))
            for x in pc.if_else(usable, p, keys).to_pylist():
                where.setdefault(x, set()).add(fn)
    return {k for k, v in where.items() if len(v) > 1}


def csv_bytes(rows: list) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["series_id", "obs_date", "value"])
    for k, d, v in sorted(rows):
        w.writerow([k, d, v])
    return buf.getvalue().encode("utf-8")


def r2_key(series_id: str) -> str:
    return f"series/{urllib.parse.quote(series_id, safe='')}.csv"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--sample", type=int, help="write N CSVs per source locally (no R2)")
    ap.add_argument("--source", action="append")
    ap.add_argument("--skip-existing", action="store_true")
    ap.add_argument("--threads", type=int, default=16)
    a = ap.parse_args()
    srcs = a.source or SOURCES

    s3 = None
    existing: set = set()
    if not a.dry_run and a.sample is None:
        if not a.bucket:
            ap.error("--bucket required for a real run")
        sys.path.insert(0, MAIN)
        from core import r2_util
        s3 = r2_util.client(write=True)
        if a.skip_existing:
            tok = None
            while True:
                kw = {"Bucket": a.bucket, "Prefix": "series/", "MaxKeys": 1000}
                if tok:
                    kw["ContinuationToken"] = tok
                resp = s3.list_objects_v2(**kw)
                for o in resp.get("Contents", []):
                    existing.add(o["Key"])
                if not resp.get("IsTruncated"):
                    break
                tok = resp.get("NextContinuationToken")
            print(f"skip-existing: {len(existing):,} objects already in R2", flush=True)

    def put(series_id: str, body: bytes):
        key = r2_key(series_id)
        for attempt in range(7):
            try:
                s3.put_object(Bucket=a.bucket, Key=key, Body=body, ContentType="text/csv")
                return
            except Exception as e:
                if attempt == 6:
                    raise
                time.sleep(2 ** attempt)

    grand_tables = grand_rows = grand_put = 0
    for src in srcs:
        t0 = time.time()
        files = sorted(glob.glob(os.path.join(DATA, src, "*.parquet")))
        n_tables = n_rows = n_put = 0
        sample_left = a.sample or 0
        if a.sample is not None:
            os.makedirs(os.path.join(SAMPLE_DIR, src), exist_ok=True)
        # Tables that straddle two parquets must be assembled BEFORE any PUT, or the second
        # file's object replaces the first and the table is served truncated (see
        # prefixes_split_across_files). Everything else still streams file-by-file.
        split = prefixes_split_across_files(files)
        if split:
            print(f"{src:16} {len(split):,} table(s) span >1 parquet — buffering those to "
                  f"PUT once, whole", flush=True)
        pending: dict[str, list] = {}

        for f in files:
            groups = group_subject(f)
            jobs = []
            for pref, rows in groups.items():
                if pref in split:            # accumulate; PUT after every file is read
                    pending.setdefault(pref, []).extend(rows)
                    continue
                sid = f"{src}:{pref}"
                body = csv_bytes(rows)
                n_tables += 1
                n_rows += len(rows)
                if a.sample is not None:
                    if sample_left > 0:
                        safe = urllib.parse.quote(sid, safe="") + ".csv"
                        with open(os.path.join(SAMPLE_DIR, src, safe), "wb") as fh:
                            fh.write(body)
                        sample_left -= 1
                    continue
                if a.dry_run:
                    continue
                if r2_key(sid) in existing:
                    continue
                jobs.append((sid, body))
            if jobs:
                with ThreadPoolExecutor(max_workers=a.threads) as ex:
                    futs = [ex.submit(put, sid, body) for sid, body in jobs]
                    for fu in as_completed(futs):
                        fu.result()
                        n_put += 1

        # Now every file has been read, so each split table is complete. One PUT each, with
        # ALL its rows — counted here so `tables=` is the distinct table count, not the
        # (file, prefix) pair count that first exposed the bug.
        if pending:
            jobs = []
            for pref, rows in pending.items():
                sid = f"{src}:{pref}"
                rows.sort(key=lambda r: (r[0], r[1]))     # contract order: (series_id, date)
                body = csv_bytes(rows)
                n_tables += 1
                n_rows += len(rows)
                if a.sample is not None:
                    if sample_left > 0:
                        safe = urllib.parse.quote(sid, safe="") + ".csv"
                        with open(os.path.join(SAMPLE_DIR, src, safe), "wb") as fh:
                            fh.write(body)
                        sample_left -= 1
                    continue
                if a.dry_run or r2_key(sid) in existing:
                    continue
                jobs.append((sid, body))
            if jobs:
                with ThreadPoolExecutor(max_workers=a.threads) as ex:
                    futs = [ex.submit(put, sid, body) for sid, body in jobs]
                    for fu in as_completed(futs):
                        fu.result()
                        n_put += 1
        print(f"{src:16} tables={n_tables:>7,} rows={n_rows:>11,} put={n_put:>7,} "
              f"{round(time.time()-t0,1)}s", flush=True)
        grand_tables += n_tables; grand_rows += n_rows; grand_put += n_put

    tag = "SAMPLE" if a.sample is not None else ("DRY-RUN" if a.dry_run else "UPLOAD")
    print(f"\n{tag} TOTAL: {grand_tables:,} tables, {grand_rows:,} rows, {grand_put:,} put")


if __name__ == "__main__":
    main()
