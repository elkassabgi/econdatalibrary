"""Restore statcan's per-cube parquet store to R2, resumable and byte-verified.

WHY. statcan's ~8,207 cubes were deleted from R2 on 2026-08-18 in the cost cleanup (1,548.7 GB, 65 % of
the econ bucket, uncompressed). Ahmed's instruction the same day, superseding "later", was "bring back
statcan compressed and follow month to month", quoted then at ~257 GB / ~$3.85 a month. That restore
never completed, and because the fetcher only refreshes cubes it can already see — skipping the rest
silently — statcan has reported `no_change` over every publisher release since (measured: 337
cube-changes in the 15 days to 2026-09-05, none merged). The store-absent guard added the same day makes
that failure loud; this tool is the actual repair.

The parquets are already zstd-compressed by the writer, so "compressed" needs no re-encoding: the local
store measures 175.11 GB, well under the quote.

DISCIPLINE.
  * PRE-FLIGHT: a random sample of local files is opened and its parquet metadata read, so an unreadable
    store is never published. Any failure aborts before the first upload.
  * RESUMABLE: a remote object whose ContentLength already equals the local size is skipped, so an
    interrupted run costs nothing and the tool can be re-run freely.
  * VERIFIED: every upload is followed by a HEAD and a size comparison; a mismatch is counted, named and
    makes the run exit non-zero.
  * POLITE: modest concurrency by default, because the workstation's crawlers are using the link.

    python tools/upload_statcan_store.py [--dry-run] [--workers 3] [--sample 12] [--limit N]
"""
from __future__ import annotations

import argparse
import glob
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from core import r2_util   # noqa: E402

BUCKET = "econ-data"
PREFIX = "clean_full/statcan/"
LOCAL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "clean_full", "statcan")


def _utc() -> str:
    return time.strftime("%H:%M:%SZ", time.gmtime())


def preflight(files, n):
    """Open a random sample and read its parquet metadata. An unreadable store must never be published."""
    import random
    import pyarrow.parquet as pq
    sample = random.sample(files, min(n, len(files)))
    for p in sample:
        md = pq.ParquetFile(p).metadata
        if md.num_rows < 0 or md.num_columns <= 0:
            raise SystemExit(f"preflight: {os.path.basename(p)} has {md.num_rows} rows / {md.num_columns} cols")
    print(f"[{_utc()}] preflight: {len(sample)} of {len(files):,} files opened, parquet metadata readable, "
          f"{sum(pq.ParquetFile(p).metadata.num_rows for p in sample):,} rows in the sample", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--workers", type=int, default=3, help="kept low: the crawlers share this link")
    ap.add_argument("--sample", type=int, default=12)
    ap.add_argument("--limit", type=int, default=None)
    a = ap.parse_args()

    files = sorted(f for f in glob.glob(os.path.join(LOCAL, "*.parquet"))
                   if not os.path.basename(f).startswith("_"))
    if not files:
        print(f"no parquet files under {LOCAL} — nothing to restore"); return 2
    total = sum(os.path.getsize(f) for f in files)
    print(f"[{_utc()}] local store: {len(files):,} cubes, {total/1e9:.2f} GB -> r2://{BUCKET}/{PREFIX}", flush=True)
    preflight(files, a.sample)

    c = r2_util.client()
    # what is already there (resume): one listing, not 8,207 HEADs
    remote = {}
    tok = None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": PREFIX}
        if tok:
            kw["ContinuationToken"] = tok
        page = c.list_objects_v2(**kw)
        for o in page.get("Contents", []):
            remote[o["Key"]] = o["Size"]
        if not page.get("IsTruncated"):
            break
        tok = page.get("NextContinuationToken")
    print(f"[{_utc()}] already on R2 under this prefix: {len(remote):,} object(s)", flush=True)

    todo = []
    for f in files:
        key = PREFIX + os.path.basename(f)
        if remote.get(key) == os.path.getsize(f):
            continue
        todo.append((f, key))
    if a.limit:
        todo = todo[:a.limit]
    todo_bytes = sum(os.path.getsize(f) for f, _ in todo)
    print(f"[{_utc()}] to upload: {len(todo):,} cube(s), {todo_bytes/1e9:.2f} GB "
          f"(skipping {len(files)-len(todo):,} already present at the same size)", flush=True)
    if a.dry_run:
        print("(dry run — nothing uploaded)")
        return 0
    if not todo:
        print("nothing to do — the store is already restored"); return 0

    from boto3.s3.transfer import TransferConfig
    cfg = TransferConfig(multipart_threshold=64 * 1024 * 1024,
                         multipart_chunksize=64 * 1024 * 1024, max_concurrency=2)

    done = {"ok": 0, "bad": 0, "bytes": 0}
    bad_names = []

    def one(item):
        f, key = item
        size = os.path.getsize(f)
        c.upload_file(f, BUCKET, key, Config=cfg)
        got = c.head_object(Bucket=BUCKET, Key=key)["ContentLength"]
        return key, size, got

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=a.workers) as ex:
        futs = {ex.submit(one, it): it for it in todo}
        for i, fut in enumerate(as_completed(futs), 1):
            try:
                key, size, got = fut.result()
            except Exception as e:                                # noqa: BLE001
                done["bad"] += 1; bad_names.append(f"{futs[fut][1]}: {type(e).__name__}: {str(e)[:70]}")
                continue
            if got == size:
                done["ok"] += 1; done["bytes"] += size
            else:
                done["bad"] += 1; bad_names.append(f"{key}: remote {got} != local {size}")
            if i % 100 == 0 or i == len(todo):
                el = time.time() - t0
                print(f"[{_utc()}] {i:,}/{len(todo):,}  ok {done['ok']:,}  bad {done['bad']}  "
                      f"{done['bytes']/1e9:.2f} GB  {done['bytes']/1e6/max(el,1):.1f} MB/s", flush=True)

    print(f"[{_utc()}] DONE: uploaded {done['ok']:,} cube(s) / {done['bytes']/1e9:.2f} GB, {done['bad']} failed")
    for b in bad_names[:20]:
        print("   ", b)
    return 1 if done["bad"] else 0


if __name__ == "__main__":
    sys.exit(main())
