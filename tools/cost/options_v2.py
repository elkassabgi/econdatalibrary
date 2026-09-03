"""The options, re-priced after the adversarial review. Uses the meter's OWN functions.

WHAT THE REVIEW OVERTURNED, and both halves were mine:

  * "89.5% of series have changed" - FALSE. Every one of the 17 decompresses byte-identical to
    the local build. Zero data differences in 19 comparable objects spanning 2026-07-03 to
    08-31. The bucket is NOT stale. What differed was the ENCODING: 65% of stored objects are
    still plain ungzipped CSV from before the 2026-08-18 gzip-at-rest change, and `put_atomic`
    gzips before comparing, so a plain object can never match. Correctly - it must be uploaded
    once to convert.
  * "option 3 = 50,000 uploads/day = $27.04" - FALSE. That rate was copied from two quiet days
    (39,801 and 47,972) that had nothing to do with the guard and relabelled as the guard's
    effect. R500's error exactly.
  * My fixed block was $0.94 light: R2 storage priced a SNAPSHOT against a GB-month charge
    (the error NUMBERS.md row 58 already records), and D1 storage used a rate and allowance
    that appear nowhere in the meter.

THE FINDING THAT DECIDES THE RECOMMENDATION: R2 class-A bills in WHOLE MILLIONS, so the guard
returns nothing at all until its hit rate clears a threshold, then $4.50 in one step.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from tools import billing_guard as bg  # noqa: E402

DAYS = 31
R2_GB = 913           # measured 2026-09-03, flat for 7 days
D1_GB = 8.34          # NUMBERS.md rows 37/59
LIST_FLOOR = 7_500    # measured after the noaa LIST leak stopped


# D1 ROWS READ, measured over the 18 days after the 2026-08-15 serving fix. This module used to
# print "D1 reads $0" as though it were established; it was read off a 24-HOUR panel
# (285,913,827/day) treated as the rate, and the real average is 3.9x that. It does not vary
# with upload volume, so it belongs in the fixed block rather than being added by hand later.
D1_READS_DAY = 1_111_156_496


def live_inputs():
    """(stored GB, D1 GB, D1 rows read/day, source) measured now, or the constants if not.

    A cost tool whose inputs are frozen constants keeps printing the same answer while the
    account moves, which reads as live and is not. These three drift: stored bytes grow with
    ingestion, D1 read volume moves with both traffic and maintenance. The constants remain as
    a labelled fallback so the tool still runs with no credentials.
    """
    import datetime as _dt                                            # noqa: PLC0415
    try:
        tok, acct = bg._load_env_token(), bg._account_id()
        if not tok or not acct:
            return R2_GB, D1_GB, D1_READS_DAY, "constants (no analytics token)"
        end = _dt.datetime.now(_dt.timezone.utc).date()
        start = (end - _dt.timedelta(days=18)).isoformat()
        q_st = ("query($a:String!,$s:Date!,$e:Date!){viewer{accounts(filter:{accountTag:$a}){"
                "r2StorageAdaptiveGroups(limit:5000,filter:{date_geq:$s,date_leq:$e})"
                # bucketName IS LOAD-BEARING. Grouped by date alone the API returns ONE row per
                # date and `max` is the largest single bucket, not the total - it reported
                # 630 GB (econ-data) where the three buckets hold 913. A dimension you omit is
                # not summed for you; it is collapsed by the aggregate you asked for.
                "{dimensions{date bucketName}max{payloadSize metadataSize}}}}}")
        q_d1 = ("query($a:String!,$s:Date!,$e:Date!){viewer{accounts(filter:{accountTag:$a}){"
                "d1AnalyticsAdaptiveGroups(limit:5000,filter:{date_geq:$s,date_leq:$e})"
                "{dimensions{date}sum{rowsRead}}}}}")
        v = {"a": acct, "s": start, "e": end.isoformat()}
        st = bg._graphql(tok, q_st, v)
        d1 = bg._graphql(tok, q_d1, v)
        by_day = {}
        for r in bg._rows(st or {}, "r2StorageAdaptiveGroups", 5000):
            d = r["dimensions"]["date"]
            by_day[d] = by_day.get(d, 0.0) + (
                r["max"]["payloadSize"] + r["max"].get("metadataSize", 0)) / 1000 ** 3
        reads = {}
        for r in bg._rows(d1 or {}, "d1AnalyticsAdaptiveGroups", 5000):
            d = r["dimensions"]["date"]
            reads[d] = reads.get(d, 0) + r["sum"]["rowsRead"]
        # drop today: it is partial and would drag both figures down
        days = sorted(by_day)[:-1]
        rdays = sorted(reads)[:-1]
        gb = by_day[days[-1]] if days else R2_GB
        rpd = (sum(reads[d] for d in rdays) / len(rdays)) if rdays else D1_READS_DAY
        return gb, D1_GB, rpd, "measured over %d complete days" % max(len(rdays), 0)
    except Exception as exc:                                          # noqa: BLE001
        return R2_GB, D1_GB, D1_READS_DAY, "constants (%s)" % type(exc).__name__


def fixed_block():
    """Priced with the meter's own helpers, not re-derived rates."""
    gb, d1gb, rpd, src = live_inputs()
    r2 = bg.gb_months(gb, DAYS) * 0.015
    d1 = max(0.0, bg.gb_months(d1gb, DAYS) - 5.0) * 0.75
    reads = bg.units(rpd * DAYS, bg.D1_READS_INCLUDED, 0.001)
    print(f"   inputs: {gb:,.0f} GB stored, {rpd:,.0f} D1 rows read/day  [{src}]")
    return {"Workers plan": 5.00, "R2 storage": r2, "D1 storage": d1, "D1 rows read": reads}


def class_a(attempts_per_day, hit_rate):
    """Uploads ATTEMPTED per day; the guard skips `hit_rate` of them."""
    uploads = attempts_per_day * (1.0 - hit_rate)
    ops = (uploads + LIST_FLOOR) * DAYS
    return bg.units(ops, bg.R2_CLASS_A_INCLUDED, 4.50), ops


def main():
    fb = fixed_block()
    print("FIXED BLOCK, from billing_guard's own gb_months() and rates:")
    for k, v in fb.items():
        print(f"   {k:<22}$ {v:6.2f}")
    print(f"   {'':<22}$ {sum(fb.values()):6.2f}   "
          f"(I told Ahmed $20.87 with D1 reads at zero; it is ${sum(fb.values()):.2f})")

    print("\nD1 rows read are ABOVE the allowance and priced in the block above. D1 writes,")
    print("Workers requests and R2 class-B are all $0, inside theirs.")
    print("Reads were once called 49% of the allowance on a MEDIAN day. Over the 18 days")
    print("since the serving fix the AVERAGE is 1,111,156,496/day = 138% of it.")

    print("THE GUARD IS A STEP FUNCTION, not a smooth saving.")
    attempts = 110_000
    print(f"At {attempts:,} upload attempts a day:")
    prev = None
    for pct in range(0, 100, 2):
        cost, _ = class_a(attempts, pct / 100.0)
        if prev is None or cost != prev:
            print(f"   hit rate >= {pct:>3}%   class-A ${cost:6.2f}")
            prev = cost

    print("\nWHAT EACH OPTION COSTS, after Texas tax:")
    rows = [
        ("1. bulk re-uploads continue at this period's rate", 325_592, 0.0),
        ("2. no bulk re-uploads; daily updating only", 110_000, 0.0),
        ("3a. option 2 + guard at the MEASURED 70% skip rate", 110_000, 0.70),
        ("3b. option 2 + guard at the 89% on recently-written objects", 110_000, 0.89),
        ("4. quarterly uploads (see tools/cost/quarterly_upload.py)", 20_000, 0.89),
    ]
    print(f"{'option':<52}{'class-A':>9}{'TOTAL':>9}")
    for label, att, hit in rows:
        cost, _ = class_a(att, hit)
        total = (sum(fb.values()) + cost) * bg.TAX_UPLIFT
        print(f"{label:<52}{cost:>9.2f}{total:>9.2f}")

    print("\nNOTE: below a 20% skip rate the guard changes the bill by exactly nothing -")
    print("class-A bills in whole millions, so the saving arrives in $4.50 steps.")
    print("The guard has never run in production, so every hit rate above is a prediction.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
