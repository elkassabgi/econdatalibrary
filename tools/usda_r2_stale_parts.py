"""Remove usda part objects from R2 that the LIVE cubes no longer have. Run BY HAND, never wired.

WHY (2026-11-19, the atomic re-ingest in jobs/ingest_usda.py). A cube is now rebuilt whole and
swapped in, so the local store holds exactly the new vintage's parts. But the fetcher's publish
(blob.publish_file) only UPLOADS local files: when a new vintage has FEWER parts than the cube it
replaced, the higher part indexes (clean_full/usda/<cube>/part_NNN.parquet) stay on R2, and every
reader that lists the bucket prefix - the resolver over a mirror, the derives under
AQUEDUCT_BACKEND=r2, the fetcher's cursor walk - reads them as part of the cube.

WHAT IT DOES
  default   report only: local part keys, R2 part keys, MISSING (local, not on R2) and STALE
            (on R2 under a usda cube dir, not a live part). Writes nothing.
  --apply   refuses unless every live cube verifies (jobs.ingest_usda.verify_dir) and MISSING is
            empty (publish first - a key set that is short on R2 is not cleaned, it is fixed by
            the upload). Then, per stale key: updater.blob.backup_store_object to
            _backup/usda_stale_parts/<UTC stamp>/<key> (raises unless the copy is proved
            readable), then updater.blob.delete_store_object. Before and after, every live
            part's R2 object must have the local file's size AND ETag (byte_mismatches), and
            afterwards the R2 part-key set must equal the local one; exit 1 if not. It holds
            logs/usda_writer.lock for the run.

It never runs after T0 (core.cutover): the cloud copy is frozen then, and the served store is the
local one, where the swap already removed the old parts with the old cube.

Usage:
    python tools/usda_r2_stale_parts.py              # report
    python tools/usda_r2_stale_parts.py --apply      # back up + delete the stale keys (owner's yes first)
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

PREFIX = "clean_full/usda/"
# A part object directly under one cube dir. Anything else under the prefix is reported, not touched.
RE_PART = re.compile(r"^clean_full/usda/([A-Za-z0-9_.]+)/part_[0-9]{3}\.parquet$")


def _ingest():
    from jobs import ingest_usda as J                                    # noqa: PLC0415
    return J


def _r2_list(prefix: str) -> list:
    from updater import blob                                              # noqa: PLC0415
    return blob.R2Blob(write=False).list_keys(prefix)


def key_sets():
    """(local part keys, R2 part keys, other R2 keys under the prefix)."""
    from updater import blob                                              # noqa: PLC0415
    J = _ingest()
    local = {blob._path_to_key(p) for p in J.served_parts()}
    r2_all = _r2_list(PREFIX)
    r2_parts = {k for k in r2_all if RE_PART.match(k)}
    other = sorted(k for k in r2_all if k not in r2_parts and k.endswith(".parquet"))
    return local, r2_parts, other


MULTIPART_CHUNK = 8 * 1024 * 1024     # boto3 TransferConfig default, what R2Blob.put_file's upload_file uses


def _local_etag(path: str, multipart: bool) -> str:
    """The ETag R2 gives `path` uploaded whole (md5) or by upload_file in 8 MiB parts (md5-of-md5s-N)."""
    import hashlib                                                        # noqa: PLC0415
    if not multipart:
        h = hashlib.md5()
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()
    digests = []
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(MULTIPART_CHUNK), b""):
            digests.append(hashlib.md5(chunk).digest())
    return f"{hashlib.md5(b''.join(digests)).hexdigest()}-{len(digests)}"


def byte_mismatches(local_keys) -> list:
    """Live parts whose R2 object is NOT these bytes: size differs, or the ETag differs from the local
    file's (single-part md5, or the 8 MiB multipart form). A key-set match alone would pass an
    interrupted publish that left OLD bytes under a low-numbered key. Fails closed: an ETag this cannot
    reproduce (another chunk size) is a mismatch, not a pass."""
    from updater import blob                                              # noqa: PLC0415
    r2 = blob.R2Blob(write=False)
    bad = []
    for k in sorted(local_keys):
        path = _store_path(k)
        size, etag = r2.size(k), r2.etag(k)
        if size != os.path.getsize(path):
            bad.append(f"{k}: R2 {size} bytes, local {os.path.getsize(path)}")
            continue
        if not etag:
            bad.append(f"{k}: R2 gives no ETag")
            continue
        mine = _local_etag(path, multipart="-" in etag)
        if mine != etag:
            bad.append(f"{k}: R2 ETag {etag}, local {mine}")
    return bad


def _store_path(key: str) -> str:
    """The local store path whose R2 key is `key` (blob._path_to_key inverts it)."""
    J = _ingest()
    data = os.path.dirname(os.path.dirname(os.path.abspath(J.OUT)))     # .../data
    return os.path.join(data, *key.split("/"))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--apply", action="store_true", help="back up, then delete, the stale keys")
    a = ap.parse_args(argv)

    from core import cutover                                              # noqa: PLC0415
    if cutover.is_cut_over():
        print("after T0 the R2 copy is frozen and the local store is served - nothing to do here")
        return 2
    J = _ingest()
    bad = []
    for grp in J.SECTOR_FILES:
        ok, why, _rows = J.verify_dir(os.path.join(J.OUT, grp))
        if not ok:
            bad.append(f"{grp}: {why}")
    local, r2_parts, other = key_sets()
    missing = sorted(local - r2_parts)
    stale = sorted(r2_parts - local)
    print(f"local live parts {len(local):,}; R2 part objects {len(r2_parts):,}; "
          f"MISSING on R2 {len(missing):,}; STALE on R2 {len(stale):,}")
    for k in stale:
        print(f"  STALE   {k}")
    for k in missing:
        print(f"  MISSING {k}")
    for k in other:
        print(f"  OTHER (not a cube part, left alone) {k}")
    for b in bad:
        print(f"  UNVERIFIED live cube {b}")
    if not a.apply:
        print("report only - nothing changed (pass --apply, after the owner's yes, to clean)")
        return 0
    if bad:
        print("REFUSING --apply: a live cube does not verify, so the local key set is not a proof")
        return 1
    if missing:
        print("REFUSING --apply: R2 lacks live parts - publish them first, then re-run")
        return 1
    if not local:
        print("REFUSING --apply: no live parts at all")
        return 1
    # The usda store region is ours for the rest of this process: no rebuild may swap a cube while the
    # proof below is taken against it (updater/writer_lock.py; jobs/ingest_usda.py takes the same lock).
    from updater import writer_lock                                       # noqa: PLC0415
    writer_lock.hold("usda_writer", "tools/usda_r2_stale_parts.py --apply")
    differ = byte_mismatches(local)
    if differ:
        print(f"REFUSING --apply: {len(differ)} live part(s) on R2 are not the local bytes - "
              f"re-publish them first:")
        for d in differ[:20]:
            print(f"  DIFFERENT {d}")
        return 1

    from updater import blob                                              # noqa: PLC0415
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    for k in stale:
        path = _store_path(k)
        where = blob.backup_store_object(path, f"_backup/usda_stale_parts/{stamp}/{k}")
        blob.delete_store_object(path)
        print(f"  removed {k} (backup {where})")

    local2, r2_after, _ = key_sets()
    if local2 != r2_after:
        print(f"FAIL: after cleanup R2 part keys != local live parts "
              f"(extra {sorted(r2_after - local2)[:5]}, missing {sorted(local2 - r2_after)[:5]})")
        return 1
    differ = byte_mismatches(local2)
    if differ:
        print(f"FAIL: after cleanup {len(differ)} R2 part(s) are not the local bytes: {differ[:5]}")
        return 1
    print(f"PROVED: R2 part-key set == local live part set ({len(local2):,} keys), and every one has "
          f"the local size and ETag")
    return 0


if __name__ == "__main__":
    sys.exit(main())
