"""How big is series/ really, and is re-compressing it worth the operations it would cost?

The deferred plan is to rewrite the served CSVs gzipped after the 8 September period boundary.
Before doing that it is worth knowing what it BUYS, because a 250-object sample put the mean
object at ~1,945 bytes, and a saving on two-kilobyte objects can be smaller than the cost of the
class-A writes that produce it.

Walking the prefix costs one list call per 1,000 objects - class B, against 10,000,000 included
per month - so a full census here is cheap in a way that walking clean_full/ would not be. The
call count is reported rather than hidden.

Read-only: lists sizes, writes nothing, and re-compresses nothing.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))


from core import r2_util                                              # noqa: E402

BUCKET, PREFIX = "econ-data", "series/"
CLASS_A_PER_MILLION = 4.50          # R2 write ops
STORAGE_USD_PER_GB_MONTH = 0.015
ASSUMED_RATIO = 4.0                 # conservative end of the 4-28x measured on this fleet


def main() -> int:
    s3 = r2_util.client()
    n = total = calls = 0
    token = None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": PREFIX, "MaxKeys": 1000}
        if token:
            kw["ContinuationToken"] = token
        r = s3.list_objects_v2(**kw)
        calls += 1
        for o in r.get("Contents") or []:
            n += 1
            total += o.get("Size", 0)
        token = r.get("NextContinuationToken")
        if not token:
            break
        if calls % 250 == 0:
            print(f"  ...{n:,} objects, {total / 1e9:.2f} GB so far", flush=True)

    gb = total / 1e9
    print(f"\n{PREFIX} holds {n:,} objects, {gb:.2f} GB")
    print(f"  mean object {total / max(n, 1):,.0f} bytes")
    print(f"  listed with {calls:,} class-B call(s) "
          f"({100 * calls / 10e6:.4f}% of the monthly allowance)\n")

    saved_gb = gb - gb / ASSUMED_RATIO
    saving = saved_gb * STORAGE_USD_PER_GB_MONTH
    write_cost = n / 1e6 * CLASS_A_PER_MILLION
    print(f"IF every object were re-compressed at a conservative {ASSUMED_RATIO:g}x:")
    print(f"  storage freed          {saved_gb:>8.2f} GB")
    print(f"  saving per month       ${saving:>8.2f}")
    print(f"  one-off class-A writes {n:,} = ${write_cost:.2f}")
    if saving <= 0.5:
        print(f"\n  The monthly saving is ${saving:.2f}. Against a ${write_cost:.2f} one-off and")
        print("  the risk of rewriting every served object, this is NOT worth doing for")
        print("  storage alone. If it is done, it should be for a different reason -")
        print("  smaller downloads for users - and argued on that.")
    else:
        months = write_cost / saving if saving else float("inf")
        print(f"\n  Pays back the one-off write cost in {months:.1f} month(s).")
    print("\nseries/ is only part of the 630 GB in econ-data; clean_full/ and clean_grouped/")
    print("hold parquet, which is already compressed. This sizes ONE prefix, not the bucket.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
