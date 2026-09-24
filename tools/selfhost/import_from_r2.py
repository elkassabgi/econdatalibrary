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
    a = ap.parse_args()
    from core import r2_util  # noqa: PLC0415
    s3 = r2_util.cloud_client()     # a named final-sync reader: keeps reading the cloud after T0
    store = BlobStore(a.root, create=a.create)
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
    bad = 0
    for k in keys:
        ok, msg = copy_one(s3, store, k, overwrite=a.overwrite, restore_missing=a.restore_missing)
        bad += not ok
        print(("OK   " if ok else "FAIL ") + msg, flush=True)
    print(f"copied {len(keys) - bad} of {len(keys)}; failures {bad}")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
