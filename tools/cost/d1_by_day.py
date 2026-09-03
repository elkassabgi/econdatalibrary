"""D1 rows read per day. This is the line that is 61% of Ahmed's bill and I left it out entirely.

`tools/billing_guard.py` says 222,714,396,621 rows read over the 25 complete days since
2026-08-09, which is $200 against a 25-billion included allowance. But the same tool's 24-hour
panel says 285,913,827 rows a day across all three databases. Thirty-one days at that rate is
8.9 billion, which is INSIDE the allowance and therefore free.

Those two facts cannot both describe a normal day. One of them is a spike. This finds out which,
because the answer decides whether the $200 is a recurring cost or a thing that happened twice.

The project's own CLAUDE.md already claims the answer - "August billed ~$200 in D1 reads, 87% of
it on two days, and those two days were OUR catalogue maintenance, not users" - so this is a
check on a written claim, not a discovery.
"""
import datetime as dt
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from tools import billing_guard as bg  # noqa: E402

Q = """
query($acct: String!, $start: Date!, $end: Date!) {
  viewer { accounts(filter: {accountTag: $acct}) {
    d1AnalyticsAdaptiveGroups(limit: 5000, filter: {date_geq: $start, date_leq: $end}) {
      dimensions { date databaseId } sum { readQueries rowsRead rowsWritten } } } } }"""

INCLUDED_READS = 25_000_000_000
RATE_READ = 0.001 / 1e6          # $0.001 per million rows


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
    for r in bg._rows(a, "d1AnalyticsAdaptiveGroups", 5000):
        d = r["dimensions"]["date"]
        per[d] = per.get(d, 0) + r["sum"]["rowsRead"]

    days = sorted(per)
    total = sum(per.values())
    print(f"{'date':<12}{'rows read':>18}{'share':>8}")
    for d in days:
        print(f"{d:<12}{per[d]:>18,}{100*per[d]/total:>7.1f}%")

    ranked = sorted(per.items(), key=lambda kv: -kv[1])
    top2 = sum(v for _, v in ranked[:2])
    rest = total - top2
    n_rest = max(1, len(days) - 2)

    print(f"\ntotal over {len(days)} days   {total:>18,}")
    print(f"the two biggest days   {top2:>18,}   {100*top2/total:.1f}% of everything")
    for d, v in ranked[:2]:
        print(f"   {d}   {v:,}")
    print(f"every other day        {rest:>18,}   {rest/n_rest:,.0f} a day on average")

    print(f"\nWHAT A MONTH COSTS, at {INCLUDED_READS/1e9:.0f}B rows included:")
    for label, per_day in (("as billed, all days included", total / len(days)),
                           ("without those two days", rest / n_rest)):
        m = per_day * 31
        billable = max(0.0, m - INCLUDED_READS)
        print(f"   {label:<30}{m/1e9:8.1f} B/month   ${billable*RATE_READ:7.2f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
