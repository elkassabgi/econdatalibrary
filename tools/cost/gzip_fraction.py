"""What fraction of the served series CSVs is stored gzipped, and is that fraction moving?

econ-data fell from 649 GB to 630 GB over three days, after ADDING 97 GB the week before. A
19 GB drop is either the gzip-at-rest conversion landing - the intended outcome - or something
deleting data. Those must not be assumed apart; they look identical in a size series.

The distinguishing evidence is the objects themselves: a converted object is stored with
`ContentEncoding: gzip` and its bytes begin with the gzip magic. If the gzipped SHARE is rising
while the object COUNT holds, the fall is compression. If the count is falling, it is deletion.

Samples with a bounded number of list+head calls (class B, 10M included per month), and reports
the sample size rather than implying a census.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

from core import r2_util                                              # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefix", default="series/")
    ap.add_argument("--sample", type=int, default=300)
    a = ap.parse_args()

    s3 = r2_util.client()
    bucket = "econ-data"

    keys, token, listed = [], None, 0
    while len(keys) < a.sample:
        kw = {"Bucket": bucket, "Prefix": a.prefix, "MaxKeys": 1000}
        if token:
            kw["ContinuationToken"] = token
        r = s3.list_objects_v2(**kw)
        page = r.get("Contents") or []
        listed += len(page)
        keys.extend(o["Key"] for o in page)
        token = r.get("NextContinuationToken")
        if not token:
            break

    keys = keys[:a.sample]
    gz = plain = err = 0
    gz_bytes = plain_bytes = 0
    for k in keys:
        try:
            h = s3.head_object(Bucket=bucket, Key=k)
        except Exception:                                             # noqa: BLE001
            err += 1
            continue
        n = h.get("ContentLength", 0)
        if (h.get("ContentEncoding") or "").lower() == "gzip":
            gz += 1
            gz_bytes += n
        else:
            plain += 1
            plain_bytes += n

    seen = gz + plain
    print(f"bucket {bucket}, prefix {a.prefix!r}")
    print(f"sampled {seen} object(s) (listed {listed}); {err} head error(s)\n")
    print(f"  stored gzipped   {gz:>6}  {100 * gz / max(seen, 1):>5.1f}%   "
          f"mean {gz_bytes / max(gz, 1):>10,.0f} bytes")
    print(f"  stored plain     {plain:>6}  {100 * plain / max(seen, 1):>5.1f}%   "
          f"mean {plain_bytes / max(plain, 1):>10,.0f} bytes")
    print()
    print("A RISING gzipped share with a steady object count means the storage fall is")
    print("compression landing. A FALLING object count would mean deletion, which is a")
    print("different conversation - these two look identical in a size series alone.")
    print()
    print("Sample, not a census: the prefix holds far more than", a.sample, "objects, and")
    print("list order is lexicographic, so this describes the head of the keyspace.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
