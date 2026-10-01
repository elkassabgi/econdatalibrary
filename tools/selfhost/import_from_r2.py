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
store already holds with the same etag, size AND LastModified (whole seconds) as the listing (so an interrupted
run continues where it stopped, and a re-run copies only what changed on R2 since); the listing is streamed page
by page, never held whole in memory; --quiet prints only FAIL lines; --progress FILE is rewritten every 30 s with
the counts, bytes, rate and the last listed key; --limit N stops after N objects are listed (a trial).

After the copy, each --prefix is listed again and merge-compared with the store: an object the store holds that
R2 no longer has (deleted or re-keyed on R2) is counted, written to --absent-out, and fails the run (rc 1);
--prune-absent deletes it from the store instead. Before T0 only - after T0 the store is newer than R2.

    python tools/selfhost/import_from_r2.py --root F:/econ_live/blobs --create --prefix series/ \\
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
    ap.add_argument("--absent-out", help="write the keys the store holds under --prefix that R2 no longer has")
    ap.add_argument("--prune-absent", action="store_true",
                    help="before T0, delete from the store what R2 no longer has under --prefix (else rc 1)")
    a = ap.parse_args()
    if a.prune_absent:
        from core import cutover                                                 # noqa: PLC0415
        if cutover.is_cut_over():
            ap.error("--prune-absent is refused after T0: the store is then newer than R2")
    from core import r2_util  # noqa: PLC0415
    s3 = r2_util.cloud_client()     # a named final-sync reader: keeps reading the cloud after T0
    store = BlobStore(a.root, create=a.create)
    # a --prefix always takes the bulk path: it alone checks for objects R2 no longer has
    if not a.prefix and a.workers <= 1 and not (a.resume or a.progress or a.limit):
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


def _utc_s(lm) -> str | None:
    """A boto3 LastModified as the store keeps it (ISO, UTC, whole seconds) - see copy_one."""
    return lm.astimezone(dt.timezone.utc).isoformat(timespec="seconds") if lm is not None else None


def _prefix_listing(s3, prefix):
    """(key, etag, size, last_modified) under one prefix, streamed page by page, in R2's (UTF-8 byte) key order."""
    tok = None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": prefix}
        if tok:
            kw["ContinuationToken"] = tok
        resp = s3.list_objects_v2(**kw)
        for o in resp.get("Contents") or []:
            yield o["Key"], o["ETag"].strip('"'), o["Size"], _utc_s(o.get("LastModified"))
        if not resp.get("IsTruncated"):
            break
        tok = resp["NextContinuationToken"]


def _listed(s3, a):
    """(key, etag, size, last_modified) for every --key and every object under every --prefix, streamed."""
    for k in a.key:
        h = s3.head_object(Bucket=BUCKET, Key=k)
        yield k, h["ETag"].strip('"'), h["ContentLength"], _utc_s(h.get("LastModified"))
    for p in a.prefix:
        yield from _prefix_listing(s3, p)


def absent_on_r2(s3, store, prefix):
    """Keys the STORE holds under `prefix` that R2 no longer lists (review AR-182 finding 1: a delete on R2 - a
    backup prune, a retire, a licence removal, a re-key - otherwise never reaches the store, and a resume reports
    success over it). A merge of two streams in the same byte order: neither side is held in memory."""
    theirs = (k for k, _e, _s, _m in _prefix_listing(s3, prefix))
    nxt = next(theirs, None)
    for mine in store.iter_keys(prefix):
        while nxt is not None and nxt.encode("utf-8") < mine.encode("utf-8"):
            nxt = next(theirs, None)
        if nxt != mine:
            yield mine


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
        for key, etag, size, modified in _listed(s3, a):
            if a.limit and c["listed"] >= a.limit:
                c["limited"] = True
                break
            c["listed"] += 1
            c["last_key"] = key
            if a.resume:
                h = store.head(key)
                # the SAME object: bytes (etag, size) and R2's LastModified (whole seconds, as copy_one keeps it).
                # A re-PUT of identical bytes moves LastModified, and a resume that skips it leaves the store
                # holding an older stored time than R2 (review AR-182 finding 2)
                if (h is not None and h["etag"] == etag and h["size"] == size
                        and (modified is None or h["stored_utc"] == modified)):
                    c["skipped_held"] += 1
                    continue
            pending.add(ex.submit(one, key, size))
            if len(pending) >= a.workers * 4:                  # bounded: the listing never runs far ahead
                _done, pending = wait(pending, return_when=FIRST_COMPLETED)
            if time.time() - last >= 30:
                report()
                last = time.time()
        wait(pending)
    absent = _absent_pass(s3, store, a, c)
    report(final=True)
    print(f"listed {c['listed']:,}; held already {c['skipped_held']:,}; copied {c['copied']:,} "
          f"({c['bytes'] / 1e9:.2f} GB); failures {c['failed']:,}; {c['absent_note']}")
    return 1 if c["failed"] or (absent and not a.prune_absent) else 0


def _absent_pass(s3, store, a, c) -> int:
    """After the copy: what the store holds under each --prefix that R2 no longer has. A fresh listing, so keys
    the copy itself wrote are never mistaken for deletions. Reported (count, and the keys to --absent-out) and
    rc 1; --prune-absent deletes them from the store instead. Not run on a --limit trial (it saw part of the
    prefix only) nor after T0 (the store is then newer than R2, and absence on R2 says nothing)."""
    from core import cutover                                                   # noqa: PLC0415
    c["absent"] = 0
    if not a.prefix:
        c["absent_note"] = "absent check: no --prefix"
        return 0
    if c.get("limited"):
        c["absent_note"] = "absent check NOT RUN: --limit saw part of the prefix only"
        return 0
    if cutover.is_cut_over():
        c["absent_note"] = "absent check NOT RUN: after T0 the store is newer than R2"
        return 0
    out = open(a.absent_out, "w", encoding="utf-8", newline="\n") if a.absent_out else None
    pruned = 0
    try:
        for p in a.prefix:
            for k in absent_on_r2(s3, store, p):
                c["absent"] += 1
                if out:
                    out.write(k + "\n")
                elif c["absent"] <= 20:
                    print(f"ABSENT {k}", flush=True)
                if a.prune_absent:
                    pruned += store.delete(k)
    finally:
        if out:
            out.close()
    c["pruned"] = pruned
    c["absent_note"] = (f"held but not on R2 {c['absent']:,}" + (f" (pruned {pruned:,})" if a.prune_absent else "")
                        + (f" -> {a.absent_out}" if a.absent_out and c["absent"] else ""))
    return c["absent"]


if __name__ == "__main__":
    raise SystemExit(main())
