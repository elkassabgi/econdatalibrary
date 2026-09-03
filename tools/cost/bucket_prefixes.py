"""Which top-level prefixes exist in econ-data, and which one could have shed 19 GB?

econ-data fell 649 -> 630 GB over three days. The series/ CSVs are only 1.2% gzipped in the head
of the keyspace, so the fall is NOT the compression conversion landing there.

This is deliberately CHEAP. Listing a 630 GB bucket's keys would be hundreds of thousands of
class-B operations, and the owner has just asked - fairly - what my diagnostics cost. A delimited
list returns the top-level prefixes in ONE call, and only prefixes small enough to matter are
then measured.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

from core import r2_util                                              # noqa: E402

BUCKET = "econ-data"
MAX_OBJECTS_TO_SIZE = 4000          # refuse to walk anything larger; report it as "large"


def main() -> int:
    s3 = r2_util.client()

    r = s3.list_objects_v2(Bucket=BUCKET, Delimiter="/")
    prefixes = [p["Prefix"] for p in (r.get("CommonPrefixes") or [])]
    roots = [o["Key"] for o in (r.get("Contents") or [])]
    print(f"{len(prefixes)} top-level prefix(es), {len(roots)} object(s) at the root\n")

    calls = 1
    for p in prefixes:
        n, total, token, truncated = 0, 0, None, False
        while True:
            kw = {"Bucket": BUCKET, "Prefix": p, "MaxKeys": 1000}
            if token:
                kw["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kw)
            calls += 1
            for o in resp.get("Contents") or []:
                n += 1
                total += o.get("Size", 0)
            token = resp.get("NextContinuationToken")
            if not token:
                break
            if n >= MAX_OBJECTS_TO_SIZE:
                truncated = True
                break
        size = f"{total / 1e9:>8.2f} GB" if not truncated else f"  >{total / 1e9:.1f} GB"
        note = "  (stopped early - large)" if truncated else ""
        print(f"  {p:<28}{n:>8,} objects {size}{note}")

    print(f"\n{calls} list call(s) total - class B, 10,000,000 included per month.")
    print("Prefixes marked large were not walked; sizing them would cost real operations.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
