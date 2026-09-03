"""Is ListObjects a DAILY habit or a few big days?

That decides whether it needs work at all. 2,416,305 list operations over 24 days averages
100,679 a day - about seven full walks of a 14-million-object bucket - but an average over a
window containing spikes says nothing about a normal day. The same mistake produced my $97.91
figure earlier tonight, which the medians corrected to $51.44.

If the listing is spiky, it is maintenance and the upload guard alone carries the saving. If it
is steady, it is a recurring $9 a month and worth chasing.
"""
import datetime as dt
import statistics
import os
import sys
from collections import defaultdict

sys.path.insert(0, r"E:\research\econfindatalibrary")

from tools import billing_guard as bg  # noqa: E402

Q = """
query($acct: String!, $start: Date!, $end: Date!) {
  viewer { accounts(filter: {accountTag: $acct}) {
    r2OperationsAdaptiveGroups(limit: 5000, filter: {date_geq: $start, date_leq: $end}) {
      dimensions { date actionType } sum { requests } } } } }"""


def main():
    tok, acct = bg._load_env_token(), bg._account_id()
    if not tok or not acct:
        print("no analytics token / account id")
        return 2
    end = dt.datetime.now(dt.timezone.utc).date()
    start = end - dt.timedelta(days=24)
    a = bg._graphql(tok, Q, {"acct": acct, "start": start.isoformat(),
                             "end": end.isoformat()})
    if a is None:
        print("query returned nothing")
        return 2

    per = defaultdict(lambda: defaultdict(int))
    for r in bg._rows(a, "r2OperationsAdaptiveGroups", 5000):
        d = r["dimensions"]
        if d["actionType"] in bg._R2_CLASS_B:
            continue
        per[d["date"]][d["actionType"]] += r["sum"]["requests"]

    days = sorted(per)
    print(f"{'date':<12}{'PutObject':>12}{'ListObjects':>13}{'other A':>10}")
    for d in days:
        row = per[d]
        put = row.get("PutObject", 0)
        lst = row.get("ListObjects", 0)
        other = sum(v for k, v in row.items() if k not in ("PutObject", "ListObjects"))
        print(f"{d:<12}{put:>12,}{lst:>13,}{other:>10,}")

    puts = [per[d].get("PutObject", 0) for d in days]
    lists = [per[d].get("ListObjects", 0) for d in days]
    print(f"\n{'':<12}{'median':>12}{'mean':>13}{'max':>12}")
    print(f"{'PutObject':<12}{statistics.median(puts):>12,.0f}"
          f"{statistics.mean(puts):>13,.0f}{max(puts):>12,}")
    print(f"{'ListObjects':<12}{statistics.median(lists):>12,.0f}"
          f"{statistics.mean(lists):>13,.0f}{max(lists):>12,}")

    ml = statistics.median(lists)
    mp = statistics.median(puts)
    print(f"\nA MONTH OF MEDIAN DAYS")
    print(f"   PutObject   {mp*30/1e6:6.2f} M -> ${max(0, mp*30-0)/1e6*4.50:6.2f} of class-A")
    print(f"   ListObjects {ml*30/1e6:6.2f} M -> ${ml*30/1e6*4.50:6.2f} of class-A")
    print("\n(The 1 M allowance applies to the class as a whole, so these are the shares of it,")
    print("not separate bills.)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
