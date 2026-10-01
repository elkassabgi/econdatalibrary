"""Copy objects from R2 `econ-data` into the self-hosted blob store, verified object by object.

docs/ECON_SELF_HOSTING_PLAN.md step 2. READ-ONLY on R2 (GetObject/ListObjectsV2 only). Each object is
checked before it is stored:
  * single-part etag  = MD5 of the bytes;
  * multipart etag    = MD5 of the concatenated part MD5s + "-N"; the part size is not recorded, so it is
                        recomputed with 8 MiB and 64 MiB parts (the sizes econ's uploaders use), then every
                        whole-MiB size that the part count allows. No match = a copy failure (review R1164).
Content-Encoding, Content-Type and custom metadata are kept, so the worker serves exactly what R2 served.

    python tools/selfhost/import_from_r2.py --root <blob store> --key series/<...>.csv [--key ...]
    python tools/selfhost/import_from_r2.py --root <blob store> --prefix _aqueduct/stats.json

BULK (plan step 2: ~14M objects, 798 GB): --workers N copies N objects at a time; --resume skips an object the
store already holds with the same etag AND size as the listing (so an interrupted run continues where it
stopped, and a re-run copies only what changed on R2 since); the listing is streamed page by page, never held
whole in memory; --quiet prints only FAIL lines; --progress FILE is rewritten every 30 s with the counts, bytes,
rate and the last listed key; --limit N stops after N objects are listed (a trial).

    python tools/selfhost/import_from_r2.py --root E:/econ_live/blobs --create --prefix series/ \\
        --workers 16 --resume --quiet --progress F:/econ_selfhost_probe/import/series.progress.json
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from blobstore import BlobStore  # noqa: E402

BUCKET = "econ-data"
MIB = 1 << 20


def multipart_etag(data: bytes, part_size: int) -> str:
    parts = [data[i:i + part_size] for i in range(0, len(data), part_size)] or [b""]
    digest = hashlib.md5(b"".join(hashlib.md5(p).digest() for p in parts)).hexdigest()
    return f"{digest}-{len(parts)}"


def etag_matches(data: bytes, etag: str) -> bool:
    etag = etag.strip('"')
    if "-" not in etag:
        return hashlib.md5(data).hexdigest() == etag
    n = int(etag.rsplit("-", 1)[1])
    tried = set()
    for ps in (8 * MIB, 64 * MIB):
        tried.add(ps)
        if multipart_etag(data, ps) == etag:
            return True
    # every whole-MiB part size that yields exactly n parts
    if n >= 1 and data:
        lo = max(1, -(-len(data) // n) // MIB)
        hi = (len(data) // max(1, n - 1)) // MIB if n > 1 else len(data) // MIB + 1
        for mib in range(lo, hi + 1):
            ps = mib * MIB
            if ps in tried:
                continue
            if -(-len(data) // ps) == n and multipart_etag(data, ps) == etag:
                return True
    return False


def copy_one(s3, store: BlobStore, key: str, overwrite: bool = False,
             restore_missing: bool = False) -> tuple[bool, str]:
    # AFTER T0 the store is what users are served and R2 is a frozen copy of the past:
    #   * the store's write rule applies (the live checkout and the single-writer lock - R1217 finding 3);
    #   * an object the store holds is NEWER than R2's: kept unless --overwrite (a deliberate repair);
    #   * an object the store does NOT hold may be missing on purpose - a licence removal deletes it, and
    #     importing it again put a retired source's CSVs back on sale (R1220 finding 1): refused unless
    #     --restore-missing, and a key under a logged licence removal is refused whatever the flags say.
    from core import cutover                                        # noqa: PLC0415
    if cutover.is_cut_over():
        from updater.blob import _refuse_or_own_store_write         # noqa: PLC0415
        from core.licence_targets import removed_csv_prefixes       # noqa: PLC0415
        _refuse_or_own_store_write(f"import {key} from the frozen R2 copy")
        gone = [p for p in removed_csv_prefixes() if key.startswith(p)]
        if gone:
            raise cutover.CutoverRefused(f"refused: {key} is under {gone[0]}, which a licence removal took out "
                                         f"of the store after T0 - it is never restored from R2 (R1220)")
        held = store.head(key) is not None
        if held and not overwrite:
            return True, f"{key} kept: the store already holds it, and after T0 it is newer than R2"
        if not held and not restore_missing:
            return False, (f"{key} NOT imported: the store does not hold it, and after T0 an absent object may "
                           f"have been removed on purpose - pass --restore-missing to put it back")
    r = s3.get_object(Bucket=BUCKET, Key=key)
    data = r["Body"].read()
    etag = r["ETag"].strip('"')
    if not etag_matches(data, etag):
        return False, f"etag mismatch for {key} ({len(data):,} bytes, etag {etag})"
    lm = r.get("LastModified")                       # boto3: a tz-aware datetime
    stored = lm.astimezone(dt.timezone.utc).isoformat(timespec="seconds") if lm is not None else None
    store.put(key, data, etag=etag, content_encoding=r.get("ContentEncoding"),
              content_type=r.get("ContentType"), custom_metadata=r.get("Metadata") or {}, stored_utc=stored)
    return True, f"{key} {len(data):,} bytes etag ok"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--key", action="append", default=[])
    ap.add_argument("--prefix", action="append", default=[])
    ap.add_argument("--create", action="store_true", help="create the blob store if it does not exist")
    ap.add_argument("--overwrite", action="store_true",
                    help="after T0, replace an object the store already holds (a repair; before T0 always)")
    ap.add_argument("--restore-missing", action="store_true",
                    help="after T0, import an object the store does not hold (never one a licence removal took)")
    ap.add_argument("--workers", type=int, default=1, help="objects copied at once (bulk)")
    ap.add_argument("--resume", action="store_true",
                    help="skip an object the store already holds with the listing's etag and size")
    ap.add_argument("--quiet", action="store_true", help="print only FAIL lines and the summary")
    ap.add_argument("--progress", help="rewrite this JSON file every 30 s with the run's counts")
    ap.add_argument("--limit", type=int, default=0, help="stop after this many listed objects (a trial)")
    a = ap.parse_args()
    from core import r2_util  # noqa: PLC0415
    s3 = r2_util.cloud_client()     # a named final-sync reader: keeps reading the cloud after T0
    store = BlobStore(a.root, create=a.create)
    if a.workers <= 1 and not a.resume and not a.progress and not a.limit:
        return _serial(s3, store, a)
    return _bulk(r2_util, s3, store, a)


def _serial(s3, store, a) -> int:
    keys = list(a.key)
    for p in a.prefix:
        tok = None
        while True:
            kw = {"Bucket": BUCKET, "Prefix": p}
            if tok:
                kw["ContinuationToken"] = tok
            resp = s3.list_objects_v2(**kw)
            keys += [o["Key"] for o in resp.get("Contents") or []]
            if not resp.get("IsTruncated"):
                break
            tok = resp["NextContinuationToken"]
    from core.cutover import CutoverRefused  # noqa: PLC0415
    bad = 0
    for k in keys:
        try:
            ok, msg = copy_one(s3, store, k, overwrite=a.overwrite, restore_missing=a.restore_missing)
        except CutoverRefused as e:
            # one refused key (under a licence removal, say) is a FAIL line, not the end of the batch with no
            # count (R1225); a refusal of the whole run (another checkout, another writer) repeats per key
            ok, msg = False, f"{k} REFUSED: {e}"
        bad += not ok
        print(("OK   " if ok else "FAIL ") + msg, flush=True)
    print(f"copied {len(keys) - bad} of {len(keys)}; failures {bad}")
    return 1 if bad else 0


def _listed(s3, a):
    """(key, etag, size) for every --key and every object under every --prefix, streamed page by page."""
    for k in a.key:
        h = s3.head_object(Bucket=BUCKET, Key=k)
        yield k, h["ETag"].strip('"'), h["ContentLength"]
    for p in a.prefix:
        tok = None
        while True:
            kw = {"Bucket": BUCKET, "Prefix": p}
            if tok:
                kw["ContinuationToken"] = tok
            resp = s3.list_objects_v2(**kw)
            for o in resp.get("Contents") or []:
                yield o["Key"], o["ETag"].strip('"'), o["Size"]
            if not resp.get("IsTruncated"):
                break
            tok = resp["NextContinuationToken"]


def _bulk(r2_util, s3, store, a) -> int:
    """Parallel, resumable copy. One R2 client per thread; BlobStore.put is serialised by its own lock."""
    import json as _json                                                       # noqa: PLC0415
    import threading                                                           # noqa: PLC0415
    import time                                                                # noqa: PLC0415
    from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait   # noqa: PLC0415
    from core.cutover import CutoverRefused                                    # noqa: PLC0415
    local = threading.local()
    lock = threading.Lock()
    c = {"listed": 0, "skipped_held": 0, "copied": 0, "failed": 0, "bytes": 0, "last_key": None}
    t0 = time.time()

    def client():
        if not hasattr(local, "s3"):
            local.s3 = r2_util.cloud_client()
        return local.s3

    def one(key, size):
        try:
            ok, msg = copy_one(client(), store, key, overwrite=a.overwrite, restore_missing=a.restore_missing)
        except CutoverRefused as e:
            ok, msg = False, f"{key} REFUSED: {e}"
        except Exception as e:                                                 # noqa: BLE001 - counted, never lost
            ok, msg = False, f"{key} ERROR {type(e).__name__}: {str(e)[:200]}"
        with lock:
            if ok:
                c["copied"] += 1
                c["bytes"] += size
            else:
                c["failed"] += 1
        if not ok or not a.quiet:
            print(("OK   " if ok else "FAIL ") + msg, flush=True)

    def report(final=False):
        if not a.progress:
            return
        el = max(time.time() - t0, 1e-9)
        with lock:
            snap = dict(c, seconds=round(el), objects_per_s=round(c["copied"] / el, 1),
                        mb_per_s=round(c["bytes"] / el / 1e6, 2), done=final,
                        utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
        tmp = a.progress + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            _json.dump(snap, fh, indent=1)
        os.replace(tmp, a.progress)

    last = time.time()
    pending = set()
    with ThreadPoolExecutor(max(1, a.workers)) as ex:
        for key, etag, size in _listed(s3, a):
            if a.limit and c["listed"] >= a.limit:
                break
            c["listed"] += 1
            c["last_key"] = key
            if a.resume:
                h = store.head(key)
                if h is not None and h["etag"] == etag and h["size"] == size:
                    c["skipped_held"] += 1
                    continue
            pending.add(ex.submit(one, key, size))
            if len(pending) >= a.workers * 4:                  # bounded: the listing never runs far ahead
                _done, pending = wait(pending, return_when=FIRST_COMPLETED)
            if time.time() - last >= 30:
                report()
                last = time.time()
        wait(pending)
    report(final=True)
    print(f"listed {c['listed']:,}; held already {c['skipped_held']:,}; copied {c['copied']:,} "
          f"({c['bytes'] / 1e9:.2f} GB); failures {c['failed']:,}")
    return 1 if c["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
