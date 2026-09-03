"""Which site is actually being used? Worker invocations per script.

WHY. Ahmed said "I have barely any downloads in econ, most of my downloads are in hf", and the
R2 class-B split says the opposite - econ-data 1,673,041 reads against hfdatalibrary-data
1,115,216. Those numbers are not in conflict: a class-B read is any GetObject, and the econ
worker fetches an object to SERVE it, so econ's count mixes user downloads with the worker's own
fetches and with whatever maintenance ran. It is not a measure of how many people used the site.

`workersInvocationsAdaptive` grouped by `scriptName` is closer to the question: one invocation is
one request a person or a client actually made. It still is not "downloads" - a page view and a
CSV fetch both count - but it separates the SITES, which the bucket totals cannot.

Read-only. One GraphQL query, no D1, nothing written.
"""
import datetime as dt
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from tools import billing_guard as bg  # noqa: E402

Q = """
query($acct: String!, $start: Date!, $end: Date!) {
  viewer { accounts(filter: {accountTag: $acct}) {
    workersInvocationsAdaptive(limit: 2000, filter: {date_geq: $start, date_leq: $end}) {
      dimensions { scriptName } sum { requests errors } } } } }"""


def main():
    tok, acct = bg._load_env_token(), bg._account_id()
    if not tok or not acct:
        print("no analytics token / account id")
        return 2
    end = dt.datetime.now(dt.timezone.utc).date()
    start = end - dt.timedelta(days=25)
    a = bg._graphql(tok, Q, {"acct": acct, "start": start.isoformat(), "end": end.isoformat()})
    if a is None:
        print("query returned nothing - scriptName may not be available on this plan")
        return 2

    per = defaultdict(lambda: [0, 0])
    for r in bg._rows(a, "workersInvocationsAdaptive", 2000):
        name = r["dimensions"].get("scriptName") or "(unnamed)"
        per[name][0] += r["sum"]["requests"]
        per[name][1] += r["sum"].get("errors", 0)

    total = sum(v[0] for v in per.values())
    print(f"WORKER INVOCATIONS {start} .. {end}")
    print(f"{'script':<38}{'requests':>14}{'share':>8}{'errors':>10}")
    for name in sorted(per, key=lambda k: -per[k][0]):
        req, err = per[name]
        print(f"{name[:36]:<38}{req:>14,}{100*req/max(total,1):>7.1f}%{err:>10,}")
    print(f"{'TOTAL':<38}{total:>14,}")
    print(f"\nincluded per account: {bg.WORKERS_INCLUDED:,} requests "
          f"({total/bg.WORKERS_INCLUDED:.2f}x used)")
    print("\nAn invocation is a REQUEST, not a download: a page view and a CSV fetch both count.")
    print("It separates the SITES, which the per-bucket read counts cannot, because the econ")
    print("worker's own GetObject calls land in econ-data alongside real user traffic.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
