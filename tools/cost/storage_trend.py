"""Is stored data GROWING? My options table holds R2 storage flat at 928 GB and that is an
assumption, not a measurement.

`tools/billing_guard.py` lines 468-472 record the exact failure I am at risk of repeating: it
once priced R2 storage from a point-in-time snapshot and understated a checked invoice
(IN-74622130) by 62%, because R2 bills GB-MONTHS - the period mean, not today's size.

This period's mean is 1,762 GB against 928 GB today, because roughly 2 TB was deleted mid-period
(the 599 GB patent corpus and ~1.4 TB of statcan, both on Ahmed's 2026-08-18 cost order). So
928 GB is the right STARTING point for next period. The question this answers is whether it
STAYS there: the library ingests every day, and if storage climbs, my $13.92 is a floor and the
options table understates every row by the same amount.
"""
import datetime as dt
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from tools import billing_guard as bg  # noqa: E402

Q = """
query($acct: String!, $start: Date!, $end: Date!) {
  viewer { accounts(filter: {accountTag: $acct}) {
    r2StorageAdaptiveGroups(limit: 5000, filter: {date_geq: $start, date_leq: $end}) {
      dimensions { date bucketName } max { payloadSize metadataSize } } } } }"""

GB = 1000 ** 3          # Cloudflare bills in decimal GB


def main():
    tok, acct = bg._load_env_token(), bg._account_id()
    if not tok or not acct:
        print("no analytics token / account id")
        return 2
    end = dt.datetime.now(dt.timezone.utc).date()
    start = end - dt.timedelta(days=25)
    a = bg._graphql(tok, Q, {"acct": acct, "start": start.isoformat(), "end": end.isoformat()})
    if a is None:
        print("query returned nothing")
        return 2

    per = {}
    buckets = {}
    for r in bg._rows(a, "r2StorageAdaptiveGroups", 5000):
        d = r["dimensions"]["date"]
        b = r["dimensions"]["bucketName"]
        gb = (r["max"]["payloadSize"] + r["max"].get("metadataSize", 0)) / GB
        per[d] = per.get(d, 0.0) + gb
        buckets.setdefault(b, {})[d] = gb

    days = sorted(per)
    print(f"{'date':<12}{'total GB':>10}   by bucket")
    for d in days:
        parts = "  ".join(f"{b} {buckets[b][d]:,.0f}" for b in sorted(buckets)
                          if d in buckets[b])
        print(f"{d:<12}{per[d]:>10,.0f}   {parts}")

    if len(days) >= 8:
        recent = [per[d] for d in days[-7:]]
        older = [per[d] for d in days[-14:-7]]
        drift = (recent[-1] - recent[0]) / max(1, len(recent) - 1)
        print(f"\nlast 7 days: {recent[0]:,.0f} -> {recent[-1]:,.0f} GB "
              f"({drift:+,.1f} GB/day)")
        print(f"previous 7:  {older[0]:,.0f} -> {older[-1]:,.0f} GB")
        proj = recent[-1] + drift * 31
        print(f"\nAT THAT DRIFT, next period ends at {proj:,.0f} GB and its MEAN is about "
              f"{(recent[-1] + proj) / 2:,.0f} GB")
        print(f"   storage cost at today's size      $ {recent[-1] * 0.015:6.2f}")
        print(f"   storage cost at the drifted mean  $ {(recent[-1] + proj) / 2 * 0.015:6.2f}")
        print("\nThe options table uses today's size. The gap between those two lines is how"
              "\nmuch it understates, and it is the same GB-months error billing_guard records.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
