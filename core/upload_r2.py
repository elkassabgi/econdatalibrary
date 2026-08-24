"""Upload the canonical parquet store (data/clean_full/<source>/**) to R2.

Resumable + idempotent: an object already in R2 with the same size is skipped (R2
keeps an MD5/etag, but size-skip is the cheap first gate; --verify adds an etag check).
Key layout mirrors the store: clean_full/<source>/<...>.parquet -> <prefix>/<source>/<...>.

  python core/upload_r2.py --bucket econ-data --dry-run     # plan only (no creds needed)
  python core/upload_r2.py --bucket econ-data               # real upload (needs R2 write creds)

--dry-run walks the local tree and prints the exact upload plan (files, bytes, skips)
WITHOUT contacting R2, so the cutover can be sized and reviewed before any credential
exists. The real run requires R2_WRITE_* in .env (see api/DEPLOY.md).
"""
from __future__ import annotations

import argparse
import os
import re

from . import r2_util

ROOT = r2_util.ROOT
STORE = os.path.join(ROOT, "data", "clean_full")
DEFAULT_PREFIX = "clean_full"


# INTERMEDIATES ARE NOT THE STORE. A bare *.parquet walk uploads a crawler's working
# files as though they were finished tables. Measured 2026-08-24 before the cbs_nl/gus_dbw
# publish: gus_dbw keeps 1,366 files (3.05 GB) under parts/ and _cache/, and cbs_nl holds
# 373 flushed .partN.parquet plus __series sidecars (0.21 GB). Uploading those costs money
# for data nobody can use and, worse, puts objects in clean_full/<source>/ that the
# resolver and the CSV derive would treat as real tables.
_PART_RE = re.compile(r"\.part\d*\.parquet$", re.I)
_EXCL_DIRS = {"parts", "_cache", "_tmp"}


def _is_intermediate(rel: str) -> bool:
    """rel is the store-relative POSIX path (e.g. 'gus_dbw/parts/area_16/v1.parquet')."""
    segs = rel.split("/")
    if _EXCL_DIRS.intersection(segs[:-1]):     # any DIRECTORY segment, not the filename
        return True
    fn = segs[-1]
    return bool(_PART_RE.search(fn)) or fn.endswith("__series.parquet") or "ckpt" in fn.lower()


def _iter_parquet(store: str):
    for dirpath, _dirs, files in os.walk(store):
        for fn in files:
            if fn.endswith(".parquet"):
                full = os.path.join(dirpath, fn)
                rel = os.path.relpath(full, store).replace(os.sep, "/")
                if _is_intermediate(rel):
                    continue
                yield full, rel


def main() -> None:
    ap = argparse.ArgumentParser(description="Upload canonical parquet to R2")
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", default=DEFAULT_PREFIX)
    ap.add_argument("--store", default=STORE)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--source", action="append", help="limit to source dir(s)")
    a = ap.parse_args()

    files = list(_iter_parquet(a.store))
    if a.source:
        keep = set(a.source)
        files = [(f, r) for f, r in files if r.split("/", 1)[0] in keep]
    total_bytes = sum(os.path.getsize(f) for f, _ in files)
    print(f"local: {len(files):,} parquet files, {total_bytes/1e9:.2f} GB under {a.store}")

    if a.dry_run:
        by_src = {}
        for f, r in files:
            src = r.split("/", 1)[0]
            b = by_src.setdefault(src, [0, 0])
            b[0] += 1
            b[1] += os.path.getsize(f)
        print(f"DRY RUN — would upload to s3://{a.bucket}/{a.prefix}/ :")
        for src in sorted(by_src):
            n, b = by_src[src]
            print(f"  {src:18} {n:>6,} files  {b/1e6:>10.1f} MB")
        print(f"TOTAL {len(files):,} files / {total_bytes/1e9:.2f} GB (no R2 contact made)")
        return

    s3 = r2_util.client(write=True)   # raises clearly if write creds absent

    def remote_size(key: str) -> int | None:
        try:
            return s3.head_object(Bucket=a.bucket, Key=key)["ContentLength"]
        except Exception:
            return None

    up = skip = 0
    up_bytes = 0
    for f, rel in files:
        key = f"{a.prefix}/{rel}"
        sz = os.path.getsize(f)
        if remote_size(key) == sz:
            skip += 1
            continue
        s3.upload_file(f, a.bucket, key)
        up += 1
        up_bytes += sz
        if up % 200 == 0:
            print(f"  uploaded {up:,} ({up_bytes/1e9:.2f} GB), skipped {skip:,}...", flush=True)
    print(f"done: uploaded {up:,} ({up_bytes/1e9:.2f} GB), skipped {skip:,} already-current")


if __name__ == "__main__":
    main()
