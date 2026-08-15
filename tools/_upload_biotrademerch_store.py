"""One-off: upload the merged unctad_biotrademerch clean_full parquet to R2.

The 2026-08-15 both-measure merge (2,291,982,918 obs, gates exact) produced an
18.8 GB parquet that existed ONLY on the workstation disk — no R2 copy at all
(verified: 0 objects under clean_full/unctad_biotrademerch/). This is the
durability step for a multi-day pull; it runs concurrently with the series
re-derive because both only READ the local parquet.
"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from core import r2_util  # noqa: E402

from boto3.s3.transfer import TransferConfig  # noqa: E402

LOCAL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..",
                     "data", "clean_full", "unctad_biotrademerch",
                     "unctad_biotrademerch.parquet")
KEY = "clean_full/unctad_biotrademerch/unctad_biotrademerch.parquet"
BUCKET = "econ-data"


def main() -> int:
    local = os.path.abspath(LOCAL)
    size = os.path.getsize(local)
    print(f"[{time.strftime('%H:%M:%SZ', time.gmtime())}] uploading {size:,} bytes -> r2://{BUCKET}/{KEY}", flush=True)
    c = r2_util.client()
    cfg = TransferConfig(multipart_threshold=64 * 1024 * 1024,
                         multipart_chunksize=64 * 1024 * 1024,
                         max_concurrency=4)
    t0 = time.time()
    done = [0]

    def cb(n):
        done[0] += n
        if done[0] % (1024 * 1024 * 1024) < 64 * 1024 * 1024:  # ~once per GB
            pct = 100.0 * done[0] / size
            mbps = done[0] / 1e6 / max(time.time() - t0, 1)
            print(f"[{time.strftime('%H:%M:%SZ', time.gmtime())}] {done[0]:,}/{size:,} ({pct:.1f}%) {mbps:.1f} MB/s", flush=True)

    c.upload_file(local, BUCKET, KEY, Config=cfg, Callback=cb)
    head = c.head_object(Bucket=BUCKET, Key=KEY)
    remote = head["ContentLength"]
    print(f"[{time.strftime('%H:%M:%SZ', time.gmtime())}] uploaded; remote size {remote:,} local {size:,} match={remote == size}", flush=True)
    return 0 if remote == size else 1


if __name__ == "__main__":
    sys.exit(main())
