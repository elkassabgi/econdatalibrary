"""Upload one source's clean_full parquet to R2, byte-verified.

Generalized from tools/_upload_biotrademerch_store.py (2026-08-15): a multi-day
pull's merged parquet must not live only on the workstation disk. Usage:

    python tools/_upload_clean_full_parquet.py <source_id>
"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from core import r2_util  # noqa: E402

from boto3.s3.transfer import TransferConfig  # noqa: E402

BUCKET = "econ-data"


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: _upload_clean_full_parquet.py <source_id>")
        return 2
    src = sys.argv[1]
    local = os.path.abspath(os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..",
        "data", "clean_full", src, f"{src}.parquet"))
    key = f"clean_full/{src}/{src}.parquet"
    size = os.path.getsize(local)
    print(f"[{time.strftime('%H:%M:%SZ', time.gmtime())}] uploading {size:,} B -> r2://{BUCKET}/{key}", flush=True)
    c = r2_util.client()
    cfg = TransferConfig(multipart_threshold=64 * 1024 * 1024,
                         multipart_chunksize=64 * 1024 * 1024,
                         max_concurrency=4)
    c.upload_file(local, BUCKET, key, Config=cfg)
    remote = c.head_object(Bucket=BUCKET, Key=key)["ContentLength"]
    print(f"[{time.strftime('%H:%M:%SZ', time.gmtime())}] remote {remote:,} local {size:,} match={remote == size}", flush=True)
    return 0 if remote == size else 1


if __name__ == "__main__":
    sys.exit(main())
