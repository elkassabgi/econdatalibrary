"""Which of Ahmed's libraries is spending the money?

THE QUESTION. Cloudflare's included allowances - 1,000,000 R2 class-A operations, 10,000,000
class-B, 25,000,000,000 D1 rows read, 50,000,000 rows written - are billed PER ACCOUNT, not per
bucket or per database. If econdatalibrary, hfdatalibrary and ipdatalibrary sit on one account
then they share one allowance, and every figure I have quoted for "the bill" already includes
all three whether or not I said so.

This splits the same numbers the billing guard prices by BUCKET and by DATABASE, so the answer
is measured rather than assumed. It also separates class-A (writes and listings, which is
maintenance) from class-B (reads, which is what a DOWNLOAD costs), because Ahmed's downloads are
mostly hf and my whole analysis so far has been about econ's writes.

Read-only. One GraphQL query per dataset, no D1 statements, nothing written.
"""
import datetime as dt
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from tools import billing_guard as bg  # noqa: E402

Q_R2 = """
query($acct: String!, $start: Date!, $end: Date!) {
  viewer { accounts(filter: {accountTag: $acct}) {
    r2OperationsAdaptiveGroups(limit: 5000, filter: {date_geq: $start, date_leq: $end}) {
      dimensions { bucketName actionType } sum { requests } } } } }"""

Q_D1 = """
query($acct: String!, $start: Date!, $end: Date!) {
  viewer { accounts(filter: {accountTag: $acct}) {
    d1AnalyticsAdaptiveGroups(limit: 5000, filter: {date_geq: $start, date_leq: $end}) {
      dimensions { databaseId } sum { rowsRead rowsWritten } } } } }"""


def main():
    tok, acct = bg._load_env_token(), bg._account_id()
    if not tok or not acct:
        print("no analytics token / account id")
        return 2
    end = dt.datetime.now(dt.timezone.utc).date()
    start = end - dt.timedelta(days=25)
    print(f"account {acct[:8]}...  window {start} .. {end}\n")

    a = bg._graphql(tok, Q_R2, {"acct": acct, "start": start.isoformat(), "end": end.isoformat()})
    cls_a, cls_b = defaultdict(int), defaultdict(int)
    for r in bg._rows(a or {}, "r2OperationsAdaptiveGroups", 5000):
        d = r["dimensions"]
        tgt = cls_b if d["actionType"] in bg._R2_CLASS_B else cls_a
        tgt[d["bucketName"]] += r["sum"]["requests"]

    buckets = sorted(set(cls_a) | set(cls_b))
    ta, tb = sum(cls_a.values()), sum(cls_b.values())
    print("R2 OPERATIONS BY BUCKET")
    print(f"{'bucket':<26}{'class-A (writes/lists)':>24}{'share':>8}"
          f"{'class-B (reads)':>18}{'share':>8}")
    for b in buckets:
        print(f"{b:<26}{cls_a[b]:>24,}{100*cls_a[b]/max(ta,1):>7.1f}%"
              f"{cls_b[b]:>18,}{100*cls_b[b]/max(tb,1):>7.1f}%")
    print(f"{'TOTAL':<26}{ta:>24,}{'':>8}{tb:>18,}")
    print(f"\nincluded per account: class-A {bg.R2_CLASS_A_INCLUDED:,}  "
          f"class-B {bg.R2_CLASS_B_INCLUDED:,}")
    print(f"class-A is {ta/bg.R2_CLASS_A_INCLUDED:.1f}x the allowance; "
          f"class-B is {tb/bg.R2_CLASS_B_INCLUDED:.2f}x it.")

    d = bg._graphql(tok, Q_D1, {"acct": acct, "start": start.isoformat(), "end": end.isoformat()})
    reads, writes = defaultdict(int), defaultdict(int)
    for r in bg._rows(d or {}, "d1AnalyticsAdaptiveGroups", 5000):
        did = r["dimensions"]["databaseId"]
        reads[did] += r["sum"]["rowsRead"]
        writes[did] += r["sum"]["rowsWritten"]
    tr = sum(reads.values())

    print("\nD1 BY DATABASE")
    print(f"{'database id':<40}{'rows read':>20}{'share':>8}{'rows written':>16}")
    for did in sorted(reads, key=lambda k: -reads[k]):
        print(f"{did[:38]:<40}{reads[did]:>20,}{100*reads[did]/max(tr,1):>7.1f}%"
              f"{writes[did]:>16,}")
    print(f"{'TOTAL':<40}{tr:>20,}")
    print(f"\nincluded per account: {bg.D1_READS_INCLUDED:,} rows read  "
          f"({tr/bg.D1_READS_INCLUDED:.1f}x used)")

    print("\nWHAT THIS ANSWERS. One account tag returned every bucket and every database above,")
    print("so the allowances are shared: a download from hf and an upload from econ draw on the")
    print("same free tier. Class-B is what a DOWNLOAD costs and class-A is what MAINTENANCE")
    print("costs, so compare their shares before deciding which library to change.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
