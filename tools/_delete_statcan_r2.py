r"""One-shot: delete statcan's R2 footprint (Ahmed 2026-08-18: serve compressed LATER).

EVIDENCE, verified before this script existed:
  - R2 measured 2026-08-18 (48-thread prefix scan): series/statcan%3A* holds
    1,373.6 GB / 421,531 objects of UNCOMPRESSED derived table CSVs (the derive
    was killed by Ahmed mid-run), clean_full/statcan/ holds 175.1 GB / 8,207+
    objects of parquet store — 1,548.7 GB total = 65% of the econ bucket =
    $23.23/month, and the source is NOT SERVED (no util.ts entry, no live
    listing), so nothing user-facing changes.
  - LOCAL store verified complete the same hour: 8,207 parquets / 164 GB at
    E:\research\econfindatalibrary\data\clean_full\statcan — the derive read
    from local, so local is the primary. Raw is re-crawlable from StatCan.
  - The future compressed serve re-derives from the local store through the
    gzip writers (core/derive_csv.py, 2026-08-18) at ~1/7th the footprint.

R2 DELETEs are free. Run:
  python tools/_delete_statcan_r2.py
"""
import os
import sys

import boto3

# Use the repo's own loader, not python-dotenv. The dependency preflight walks every module
# reachable from the fetchers and fails on anything imported but undeclared, because a
# runner missing it skips the source silently as "no adapter built". This file added
# `from dotenv import load_dotenv` on 2026-08-23 and turned main's preflight red; nothing
# else in the repo uses dotenv, and core.config.load_env already does the same job.
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(_ROOT)
sys.path.insert(0, _ROOT)

from core import config                                             # noqa: E402

config.load_env(".env.local")
config.load_env(".env")
s3 = boto3.client("s3", endpoint_url=os.environ["R2_WRITE_ENDPOINT"],
                  aws_access_key_id=os.environ["R2_WRITE_ACCESS_KEY_ID"],
                  aws_secret_access_key=os.environ["R2_WRITE_SECRET_ACCESS_KEY"])

total = 0
for prefix in ("series/statcan%3A", "clean_full/statcan/"):
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket="econ-data", Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", [])]
    print(f"{prefix}: deleting {len(keys):,} objects", flush=True)
    for i in range(0, len(keys), 1000):
        r = s3.delete_objects(Bucket="econ-data",
                              Delete={"Objects": [{"Key": k} for k in keys[i:i+1000]],
                                      "Quiet": True})
        total += len(keys[i:i+1000]) - len(r.get("Errors", []))
        if r.get("Errors"):
            print("ERRORS:", r["Errors"][:3], flush=True)
    left = s3.list_objects_v2(Bucket="econ-data", Prefix=prefix, MaxKeys=3)
    print(f"  remaining: {left.get('KeyCount', 0)}", flush=True)
print(f"TOTAL DELETED: {total:,} (expected ~429,738; ~1,548.7 GB ~= $23.23/mo freed)")
