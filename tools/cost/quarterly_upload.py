"""Ahmed's ACTUAL proposal, priced. I answered a different one twice and dismissed it both times.

WHAT I PRICED (wrong): check the slow sources once a quarter instead of daily. That saves the
FETCH, which is free, so I found it worth about $4 and then $0.

WHAT HE PROPOSED: keep fetching and merging DAILY on the desktop, so the local copy is always
current, and UPLOAD to Cloudflare on four days a year. The served data lags by up to a quarter;
the local data does not lag at all.

WHY THAT IS COMPLETELY DIFFERENT. The cost is per UPLOAD, and a series that changes every day is
uploaded 90 times a quarter today and ONCE under his scheme. The quarterly burst is the count of
series that changed AT LEAST ONCE in the quarter - not the sum of daily changes. Deduplication
over time is the entire saving, and it is the one thing my two answers never modelled.

AND THE ALLOWANCE IS MONTHLY. 1,000,000 class-A operations are included every month. Concentrate
the year's uploads into four days and eight months of the year carry almost none - so those
months are free outright, and a burst month is only charged on what exceeds 1,000,000.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from tools import billing_guard as bg  # noqa: E402

DAYS = 31

# ONE FIXED BLOCK, IMPORTED, NOT A SECOND COPY.
#
# Both models price the same account, and two copies is how they drift apart - this file
# carried 13.96 of storage and 1,111,156,496 reads/day as literals while options_v2 had already
# been changed to measure them. Importing also means the bucketName bug that made the storage
# query return the largest bucket instead of the total is fixed in one place, not two.
#
# D1 rows read belong in FIXED because they do not vary with upload volume. Leaving them out of
# this model is what led me to hand-add $9.45 - a PRE-tax figure - to an already-taxed column
# and understate option 4 by up to $0.62.
from tools.cost.options_v2 import fixed_block as _fixed_block  # noqa: E402

FIXED = sum(_fixed_block().values())
LIST_FLOOR = 7_500               # measured, after the noaa LIST leak stopped
ATTEMPTS_DAY = 110_000           # measured median of post-fix days with no backfill


def month_cost(class_a_ops):
    return bg.units(class_a_ops, bg.R2_CLASS_A_INCLUDED, 4.50)


def main():
    print("TODAY: uploads every day, no guard")
    ops = (ATTEMPTS_DAY + LIST_FLOOR) * DAYS
    c = month_cost(ops)
    print(f"   {ops/1e6:5.2f} M class-A a month -> ${c:6.2f}   "
          f"bill ${(FIXED + c) * bg.TAX_UPLIFT:6.2f}\n")

    print("AHMED'S SCHEME: fetch daily on the desktop, upload on 4 days a year.")
    print("The burst is the number of series that changed AT LEAST ONCE in the quarter.\n")
    print(f"{'quarterly burst':>18}{'burst month':>13}{'quiet month':>13}"
          f"{'YEAR of R2 ops':>16}")
    for burst in (100_000, 250_000, 500_000, 1_000_000, 2_000_000, 5_000_000, 10_000_000):
        quiet_ops = LIST_FLOOR * DAYS
        burst_ops = quiet_ops + burst
        c_burst = month_cost(burst_ops)
        c_quiet = month_cost(quiet_ops)
        year = 4 * c_burst + 8 * c_quiet
        print(f"{burst:>18,}{c_burst:>13.2f}{c_quiet:>13.2f}{year:>16.2f}")

    print("\nWHY THE ZEROES: the 1,000,000 class-A allowance is PER MONTH. A quiet month runs")
    print(f"{LIST_FLOOR * DAYS:,} operations, so it is free outright. A burst month is charged")
    print("only on what exceeds the allowance, and it gets a fresh allowance next month.")

    print("\nWHAT A YEAR COSTS ALL IN, at each burst size:")
    for burst in (250_000, 1_000_000, 2_000_000, 5_000_000):
        quiet_ops = LIST_FLOOR * DAYS
        year_ops_cost = 4 * month_cost(quiet_ops + burst) + 8 * month_cost(quiet_ops)
        year_fixed = 12 * FIXED
        total = (year_fixed + year_ops_cost) * bg.TAX_UPLIFT
        print(f"   burst {burst:>10,}   ${total:8.2f}/year   ${total/12:6.2f}/month average")

    print("\nTODAY, for comparison:")
    year_now = 12 * (FIXED + month_cost((ATTEMPTS_DAY + LIST_FLOOR) * DAYS))
    print(f"   ${year_now * bg.TAX_UPLIFT:8.2f}/year   "
          f"${year_now * bg.TAX_UPLIFT / 12:6.2f}/month")

    print("")
    print("THE BURST IS NOW MEASURED, and it turns this option down.")
    print("tools/cost/burst_measure.py walked all 13,978,094 objects: 100% were written within")
    print("90 days, 75.8% within 30. So the CEILING is the whole library, not the 250k-5M this")
    print("curve was built around. At the 70% write-redundancy measured by prove_plain_digest,")
    print("the burst is ~4.2M and option 4 costs about $39.47/month - WORSE than option 3's")
    print("$37.87, and it costs three-month-old served data on top. It wins only above ~90%")
    print("redundancy. See the proposal, section 8.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
