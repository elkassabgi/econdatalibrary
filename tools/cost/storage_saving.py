"""How much storage would converting the plain CSV objects actually save?

R2 storage is $13.96/month and the second largest line on the bill. An unbiased census found
65% of objects are still PLAIN, ungzipped CSV from before the 2026-08-18 gzip-at-rest change.
Each must be uploaded once to convert - which the skip guard needs anyway before it can ever
skip them - so the conversion pays twice if the storage saving is real.

WHAT I DO NOT KNOW, and must not assume: object COUNT share is not object SIZE share. If the
plain objects happen to be the small ones, converting 65% of the count could move very little
of the 630 GB. This measures bytes, not counts.

METHOD. Sample series at random from the local catalogue by rowid (an index seek, not
`LIMIT 1 OFFSET n` which is O(offset) on 13.9M rows and did not finish 600 picks in 25 minutes).
For each: one ranged GET of 10 bytes to read the format, one HEAD for the stored size, and for
the plain ones a full GET so the bytes can be compressed and measured. R2 has no egress charge
and these objects are kilobytes; the whole run is a few MB and a few hundred class-B operations
against 10,000,000 included.

Nothing is written.
"""
import argparse
import random
import sqlite3
import os
import sys
from urllib.parse import quote

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from core import derive_csv as dc  # noqa: E402
from core import r2_util  # noqa: E402

BUCKET = "econ-data"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=250)
    ap.add_argument("--seed", type=int, default=20260903)
    a = ap.parse_args()

    con = sqlite3.connect("file:%s?mode=ro" % dc.CATALOG, uri=True)
    lo, hi = con.execute("SELECT MIN(rowid), MAX(rowid) FROM series").fetchone()
    print(f"catalogue rowids {lo:,}..{hi:,}; sampling {a.n} by rowid seek (seed {a.seed})")

    client = r2_util.client(write=False)
    rnd = random.Random(a.seed)

    plain_n = plain_stored = plain_gz = 0
    gz_n = gz_stored = 0
    absent = 0
    tried = 0

    while plain_n + gz_n < a.n and tried < a.n * 6:
        tried += 1
        row = con.execute("SELECT series_id FROM series WHERE rowid>=? LIMIT 1",
                          (rnd.randint(lo, hi),)).fetchone()
        if not row:
            continue
        key = "series/" + quote(row[0], safe="") + ".csv"
        try:
            head = client.get_object(Bucket=BUCKET, Key=key, Range="bytes=0-9")["Body"].read()
            size = client.head_object(Bucket=BUCKET, Key=key)["ContentLength"]
        except Exception:                                  # noqa: BLE001
            absent += 1
            continue
        if head[:2] == b"\x1f\x8b":
            gz_n += 1
            gz_stored += size
        else:
            body = client.get_object(Bucket=BUCKET, Key=key)["Body"].read()
            plain_n += 1
            plain_stored += len(body)
            plain_gz += len(r2_util.gzip_bytes(body))

    print(f"\nsampled {plain_n + gz_n} objects ({absent} absent/error, {tried} draws)")
    print(f"   already gzipped : {gz_n:>5}  {gz_stored/1e6:9.2f} MB stored")
    print(f"   still plain     : {plain_n:>5}  {plain_stored/1e6:9.2f} MB stored, "
          f"{plain_gz/1e6:9.2f} MB if gzipped")

    if not plain_n:
        print("no plain objects in the sample - nothing to estimate")
        return 0

    ratio = plain_stored / max(plain_gz, 1)
    plain_share_bytes = plain_stored / max(plain_stored + gz_stored, 1)
    print(f"\n   compression ratio on the plain ones : {ratio:5.2f}x")
    print(f"   plain objects' share of sampled BYTES: {100*plain_share_bytes:5.1f}%")

    ECON_GB = 630
    saved_gb = ECON_GB * plain_share_bytes * (1 - 1 / ratio)
    print(f"\nSCALED TO econ-data's {ECON_GB} GB:")
    print(f"   bytes in plain objects  ~ {ECON_GB * plain_share_bytes:6.1f} GB")
    print(f"   after conversion        ~ {ECON_GB * plain_share_bytes / ratio:6.1f} GB")
    print(f"   SAVED                   ~ {saved_gb:6.1f} GB "
          f"= ${saved_gb * 0.015:5.2f}/month of the $13.96 storage line")
    print("\nThe share is measured on a sample and econ-data's 630 GB includes buckets and")
    print("prefixes this sample never touched, so treat the GB figure as an order of")
    print("magnitude and the RATIO as the measured quantity.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
