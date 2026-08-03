"""Materialize per-series CSV objects to R2 so /v1/series/{id}.csv is live on the Worker.

For each catalog series_id, project rows through the SAME econdl resolver the dev shim
uses (so the bytes are identical to the local /v1 response), and PUT them to R2 at
  <prefix>/series/<urlencoded series_id>.csv
The Worker then serves /v1/series/{id}.csv as a plain R2 GET (no parquet-in-Worker).

  python core/derive_csv.py --dry-run --limit 5     # derive locally + DIFF vs the dev shim
  python core/derive_csv.py --bucket econ-data       # full run (needs R2 write creds)

Tidy sources emit the canonical `series_id,obs_date,value`; relational/wide sources
(tidy_ok=False) emit their native columns verbatim — exactly as the contract specifies.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import os
import sqlite3
import sys
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "clients", "python"))

from . import r2_util  # noqa: E402

ROOT = r2_util.ROOT
CATALOG = os.path.join(ROOT, "data", "catalog.db")
DEFAULT_PREFIX = "series"


def _series_csv_bytes(series_id: str) -> bytes:
    """Project one series to CSV bytes via the econdl resolver (the contract shape)."""
    from econdl import _resolve
    res = _resolve.resolve(series_id)
    table = _resolve.read_native(res)
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")   # match the dev shim / Worker byte-for-byte
    if res.tidy_ok:
        df = _resolve.native_to_tidy(res, table)
        w.writerow(["series_id", "obs_date", "value"])
        for sid, _src, d, v in df[["series_id", "source", "obs_date", "value"]].itertuples(index=False):
            w.writerow([sid, d, v])
    else:
        cols = table.column_names
        w.writerow(cols)
        for row in table.to_pylist():
            w.writerow([row.get(c) for c in cols])
    return buf.getvalue().encode("utf-8")


def _catalog_ids(limit: int | None, source: list | None):
    conn = sqlite3.connect(f"file:{CATALOG}?mode=ro", uri=True)
    try:
        q = "SELECT series_id, source_id FROM series"
        if source:
            q += " WHERE source_id IN (%s)" % ",".join("?" * len(source))
        q += " ORDER BY source_id, series_id"
        if limit:
            q += f" LIMIT {int(limit)}"
        return conn.execute(q, source or []).fetchall()
    finally:
        conn.close()


def _put_with_backoff(s3, bucket, key, body) -> None:
    """PUT one object. R2 throws transient ServiceUnavailable/SlowDown throttles that outlast
    botocore's 5 built-in retries (that killed the 2026-07-02 run at 103k objects). Patient
    app-level backoff: 7 tries, ~2 min total, then re-raise loudly rather than lose the object."""
    import time as _time
    for attempt in range(7):
        try:
            s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="text/csv")
            return
        except Exception as e:                               # noqa: BLE001
            if attempt == 6:
                raise
            wait = 2 ** attempt                              # 1..64s
            print(f"  PUT retry {attempt+1}/7 in {wait}s ({str(e)[:70]})", flush=True)
            _time.sleep(wait)


def main() -> None:
    ap = argparse.ArgumentParser(description="Derive per-series CSV objects to R2")
    ap.add_argument("--bucket")
    ap.add_argument("--prefix", default=DEFAULT_PREFIX)
    ap.add_argument("--dry-run", action="store_true", help="derive locally, contact no R2")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--source", action="append")
    ap.add_argument("--verify-shim", help="base URL of a running dev shim to byte-diff against")
    ap.add_argument("--skip-newer-than", default=None,
                    help="ISO8601 UTC; skip series whose R2 object was last modified at or after "
                         "this instant. Makes a RE-derive resumable, where --skip-existing cannot "
                         "be (every key already exists, so it would skip everything).")
    ap.add_argument("--skip-existing", action="store_true",
                    help="list existing <prefix>/ keys once and skip them (resumable multi-day run)")
    ap.add_argument("--smallest-first", action="store_true",
                    help="process sources in ascending entry count so whole sources go live early")
    ap.add_argument("--workers", type=int, default=1,
                    help="parallel derive+PUT workers (default 1 = the original serial path). "
                         "Measured 2026-07-29: cepii_gravity derives at 63 ms/series, so its "
                         "991,707 remaining objects are 17.4 h serial. Both halves of the work "
                         "release the GIL (pyarrow read, then the HTTPS PUT), so threads help.")
    a = ap.parse_args()

    rows = _catalog_ids(a.limit, a.source)
    print(f"{len(rows):,} catalog series to derive")

    if a.dry_run:
        ok = miss = 0
        diffs = 0
        for sid, _src in rows:
            try:
                body = _series_csv_bytes(sid)
                ok += 1
            except Exception as e:  # store-coverage gaps error loudly, never silently skipped
                miss += 1
                print(f"  SKIP(unresolvable) {sid}: {str(e)[:80]}")
                continue
            if a.verify_shim:
                url = a.verify_shim.rstrip("/") + "/v1/series/" + urllib.parse.quote(sid, safe="") + ".csv"
                try:
                    shim = urllib.request.urlopen(url, timeout=15).read()
                    same = shim == body
                    diffs += 0 if same else 1
                    print(f"  {sid:42} {len(body):>8} B  shim-match={same}")
                except Exception as e:
                    print(f"  {sid:42} shim fetch failed: {str(e)[:60]}")
        print(f"DRY RUN: derived {ok}, unresolvable {miss}"
              + (f", shim byte-diffs {diffs}" if a.verify_shim else "")
              + " (no R2 contact)")
        return

    if not a.bucket:
        ap.error("--bucket is required for a real run")
    s3 = r2_util.client(write=True)

    if a.smallest_first:
        by_src: dict = {}
        for sid, src in rows:
            by_src.setdefault(src, []).append((sid, src))
        rows = [r for src in sorted(by_src, key=lambda s: len(by_src[s]))
                for r in by_src[src]]

    existing: set = set()
    if a.skip_newer_than:
        # RESUMABLE RE-DERIVE. --skip-existing is useless for a re-derive: the keys all exist
        # from the ORIGINAL derive, so it would skip everything and do nothing. But a re-derive
        # still has to survive an interruption — noaa's is ~14 hours over 3,135,873 series, and
        # the 2026-08-03 reboot threw away a third of one because there was no way to resume.
        #
        # The distinguishing fact is already on every object: LastModified. Anything rewritten
        # SINCE the campaign started is done; anything older still carries pre-restatement data.
        # Same single listing pass as --skip-existing, one extra comparison.
        cutoff = dt.datetime.fromisoformat(a.skip_newer_than.replace("Z", "+00:00"))
        listing_prefix = f"{a.prefix}/"
        if a.source and len(a.source) == 1:
            listing_prefix = f"{a.prefix}/{urllib.parse.quote(a.source[0] + ':', safe='')}"
        print(f"skip-newer-than {cutoff.isoformat()} scoped to {listing_prefix}", flush=True)
        tok = None
        seen = 0
        while True:
            kw = {"Bucket": a.bucket, "Prefix": listing_prefix, "MaxKeys": 1000}
            if tok:
                kw["ContinuationToken"] = tok
            resp = s3.list_objects_v2(**kw)
            for o in resp.get("Contents", []):
                seen += 1
                if o["LastModified"] >= cutoff:
                    existing.add(o["Key"])
            if not resp.get("IsTruncated"):
                break
            tok = resp.get("NextContinuationToken")
        print(f"skip-newer-than: {len(existing):,} of {seen:,} objects already re-derived "
              f"this campaign; the rest will be rewritten", flush=True)
    elif a.skip_existing:
        # Scope the listing to the source when exactly one is named. The unscoped
        # `series/` prefix spans every source (millions of objects), so a resume of one
        # source would spend its first many minutes paging through other sources' keys.
        listing_prefix = f"{a.prefix}/"
        if a.source and len(a.source) == 1:
            listing_prefix = f"{a.prefix}/{urllib.parse.quote(a.source[0] + ':', safe='')}"
            print(f"skip-existing scoped to {listing_prefix}", flush=True)
        tok = None
        while True:
            kw = {"Bucket": a.bucket, "Prefix": listing_prefix, "MaxKeys": 1000}
            if tok:
                kw["ContinuationToken"] = tok
            resp = s3.list_objects_v2(**kw)
            for o in resp.get("Contents", []):
                existing.add(o["Key"])
            if not resp.get("IsTruncated"):
                break
            tok = resp.get("NextContinuationToken")
        print(f"skip-existing: {len(existing):,} objects already in R2", flush=True)

    todo = []
    skip = 0
    for sid, src in rows:
        key = f"{a.prefix}/{urllib.parse.quote(sid, safe='')}.csv"
        if key in existing:
            skip += 1
            continue
        todo.append((sid, src, key))
    print(f"to derive: {len(todo):,}  (already present: {skip:,})", flush=True)

    up, miss = 0, 0
    if a.workers > 1:
        import threading
        from concurrent.futures import ThreadPoolExecutor, as_completed
        lock = threading.Lock()

        def work(item):
            sid, src, key = item
            try:
                body = _series_csv_bytes(sid)
            except Exception as e:                           # noqa: BLE001
                return ("miss", sid, str(e)[:80])
            _put_with_backoff(s3, a.bucket, key, body)
            return ("put", sid, None)

        # Chunked submission: 1M futures materialised at once would exhaust memory long
        # before the first one completed.
        CH = 20_000
        for start in range(0, len(todo), CH):
            chunk = todo[start:start + CH]
            with ThreadPoolExecutor(max_workers=a.workers) as ex:
                for fut in as_completed([ex.submit(work, it) for it in chunk]):
                    kind, sid, err = fut.result()
                    with lock:
                        if kind == "put":
                            up += 1
                        else:
                            miss += 1
                            print(f"  unresolvable {sid}: {err}", flush=True)
                        if (up + miss) % 5000 == 0:
                            print(f"  derived+put {up:,} (skip {skip:,}, miss {miss:,})...",
                                  flush=True)
    else:
        cur_src = None
        for sid, src, key in todo:
            if src != cur_src:
                if cur_src is not None:
                    print(f"  [source done] {cur_src} (running: put {up:,}, skip {skip:,})",
                          flush=True)
                cur_src = src
            try:
                body = _series_csv_bytes(sid)
            except Exception as e:                           # noqa: BLE001
                miss += 1
                print(f"  unresolvable {sid}: {str(e)[:80]}")
                continue
            _put_with_backoff(s3, a.bucket, key, body)
            up += 1
            if up % 500 == 0:
                print(f"  derived+put {up:,} (skip {skip:,}, miss {miss:,})...", flush=True)

    print(f"done: put {up:,} series CSVs, skipped {skip:,} existing, "
          f"{miss:,} unresolvable (store-coverage gaps)")


if __name__ == "__main__":
    main()
