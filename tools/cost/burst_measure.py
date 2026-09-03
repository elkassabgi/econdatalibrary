"""How many distinct series would a quarterly upload actually push?

This is the one number that decides Ahmed's proposal, and every figure I have given him about it
so far has been a curve rather than a measurement.

WHAT THIS MEASURES, and what it does NOT. It walks the whole `series/` prefix and buckets every
object by its LastModified. That gives the count of distinct objects WRITTEN in the last 30, 60
and 90 days. That is an UPPER BOUND on the quarterly burst, not the burst itself, because
today's pipeline re-uploads series whose bytes did not change (292 of 295 fetchers re-derive
everything they touch, not only what changed). Under Ahmed's scheme those redundant writes
collapse into nothing.

An upper bound is still decisive in one direction: if even the upper bound is under 1,000,000
per quarter, the burst fits inside the monthly included allowance and the upload line is $0
whatever the true figure is.

FULL POPULATION, NOT A SAMPLE. 14,037,213 objects, every one of them. At 1,000 keys per call
that is ~14,038 ListObjects = about $0.06, against a period already 10.8M past its allowance, so
it does not move the whole-million rounding. It writes nothing.

Progress is printed as it goes, because a 20-minute silent job is indistinguishable from a hung
one (R562-R564).
"""
import datetime as dt
import os
import sys
import time
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from core import r2_util  # noqa: E402

BUCKET = "econ-data"
PREFIX = "series/"


def main():
    client = r2_util.client(write=False)
    now = dt.datetime.now(dt.timezone.utc)
    edges = [(30, now - dt.timedelta(days=30)),
             (60, now - dt.timedelta(days=60)),
             (90, now - dt.timedelta(days=90))]

    buckets = Counter()
    by_month = Counter()
    total = 0
    calls = 0
    token = None
    started = time.time()

    while True:
        kw = {"Bucket": BUCKET, "Prefix": PREFIX, "MaxKeys": 1000}
        if token:
            kw["ContinuationToken"] = token
        r = client.list_objects_v2(**kw)
        calls += 1
        for o in r.get("Contents", []):
            total += 1
            lm = o["LastModified"]
            by_month[lm.strftime("%Y-%m")] += 1
            for days, edge in edges:
                if lm >= edge:
                    buckets[days] += 1
        if calls % 250 == 0:
            el = time.time() - started
            print(f"   {total:>10,} objects, {calls:,} calls, {el/60:5.1f} min, "
                  f"90d so far {buckets[90]:,}", flush=True)
        if not r.get("IsTruncated"):
            break
        token = r["NextContinuationToken"]

    el = time.time() - started
    print(f"\nwalked {total:,} objects in {calls:,} calls, {el/60:.1f} min "
          f"(~${calls/1e6*4.50:.3f} of class-A)")
    print(f"\n{'window':<12}{'objects written':>18}{'share':>9}")
    for days, _ in edges:
        print(f"last {days:>3}d{'':<4}{buckets[days]:>18,}{100*buckets[days]/max(total,1):>8.1f}%")

    print(f"\nby calendar month written:")
    for m in sorted(by_month):
        print(f"   {m}   {by_month[m]:>12,}")

    q = buckets[90]
    print(f"\nUPPER BOUND ON A QUARTERLY BURST: {q:,} objects.")
    if q < 1_000_000:
        print("That is INSIDE the 1,000,000 monthly included allowance, so under Ahmed's")
        print("scheme the upload line would be $0 in every month of the year.")
    else:
        over = q - 1_000_000
        print(f"That EXCEEDS the 1,000,000 allowance by {over:,}, costing "
              f"${(over/1e6)*4.50:.2f} in a burst month, ${4*(over/1e6)*4.50:.2f} a year.")
    print("\nRemember this counts every write, including re-uploads of unchanged bytes, so the")
    print("true burst is smaller - possibly much smaller.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
