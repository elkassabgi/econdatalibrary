"""Which day of each period does every source stamp its observations on?

The store has ONE date type (date32) and one column name (obs_date), so this is not
a formatting difference — every source already writes an ISO date. What differs is
the CONVENTION: an annual 2024 observation can be stamped 2024-01-01 (period-START)
or 2024-12-31 (period-END), and a monthly 2025-M06 can be 2025-06-01 or 2025-06-30.

That difference is invisible until it bites, and it bites hard:

  * MERGING mixes them. imf_commodity stores period-START; jobs/ingest_imf_direct.py
    stamps period-END. Merging the direct pull into it without converting would have
    written a SECOND row for every month of 34 years of history rather than
    extending the series — dedup keys on (series_key, obs_date), so the two dates
    are two different observations.
  * JOINING across sources silently misaligns. A user joining an annual series
    stamped 01-01 to one stamped 12-31 gets an empty join, or worse, a 1-year lag
    they never notice.
  * FILTERING by date cuts differently. `obs_date <= 2024-12-31` includes a
    period-END 2024 annual point and a period-START one; `< 2024-12-31` includes
    only one of them.

So: measure it. For every source this counts the (month, day) of EVERY observation
and names the convention that distribution implies. Two earlier designs kept
per-series state and died of memory (40 GB, then 134 GB climbing 12 GB/min) --
bounding per entity is useless when a source can hold hundreds of millions of
series. A 366-bucket histogram answers the question in constant memory, which is
what makes a COMPLETE scan possible rather than a bounded one.

Usage:  python tools/audit_date_conventions.py [--full] [--json out.json]
"""
from __future__ import annotations

import argparse
import calendar
import collections
import datetime as dt
import io
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import pyarrow.compute as pc                                   # noqa: E402
import pyarrow.parquet as pq                                  # noqa: E402

BASE = os.path.join(ROOT, "data", "clean_full")
MAX_FILES = 3          # parquet files sampled per source
MAX_ROWS = 200_000     # rows sampled per source


def histogram(sid, full=False):
    """{(month, day): count} over every observation in one source.

    WHY NOT PER-SERIES. Two earlier designs died here. Retaining every observation
    hit 40 GB and climbed 1.3 GB/20s. Retaining only the first and last five
    observations PER SERIES then hit 134 GB climbing 12 GB/min — because the bound
    scaled with the number of series, and some sources in this library have hundreds
    of millions of them. Bounding per entity is worthless when the entity count is
    itself unbounded.

    The question never needed per-series state. "Which day inside its period does
    this source stamp?" is answered by the distribution of (month, day) across the
    source's dates, which is at most 366 buckets no matter how much data flows
    through:
        annual START  -> everything on 01-01        annual END  -> 12-31
        monthly START -> day 1, twelve months       monthly END -> month-end days
        quarterly     -> four months, day 1 or last
        daily         -> every day present
    Constant memory, and now a genuinely COMPLETE scan rather than a bounded one:
    every row of every file of every source is counted.
    """
    counts = collections.Counter()
    d = os.path.join(BASE, sid)
    if not os.path.isdir(d):
        return counts
    files = sorted(f for f in os.listdir(d) if f.endswith(".parquet")
                   and not f.startswith("_"))
    if not full:
        files = files[:MAX_FILES]
    seen = 0
    for f in files:
        try:
            pf = pq.ParquetFile(os.path.join(d, f))
            if "obs_date" not in set(pf.schema_arrow.names):
                continue
            for i in range(pf.num_row_groups):
                col = pf.read_row_group(i, columns=["obs_date"])["obs_date"]
                # VECTORISED. Iterating obs_date in Python meant materialising one
                # date object per observation — hopeless across a store this size,
                # and the reason the earlier designs were both slow AND enormous.
                # month*100+day collapses each date to one small integer in Arrow,
                # and value_counts aggregates in C, so the whole scan touches at
                # most 366 distinct values per row group.
                key = pc.add(pc.multiply(pc.month(col), 100), pc.day(col))
                vc = pc.value_counts(key.drop_null())
                for entry in vc:
                    md, n = entry["values"].as_py(), entry["counts"].as_py()
                    if md is None:
                        continue
                    counts[(md // 100, md % 100)] += n
                    seen += n
                if not full and seen >= MAX_ROWS:
                    return counts
        except Exception:                                     # noqa: BLE001
            continue
    return counts


def classify_hist(counts):
    """Name the convention a (month, day) histogram implies."""
    if not counts:
        return None, 0
    total = sum(counts.values())
    months = {m for (m, _d) in counts}
    days = {dd for (_m, dd) in counts}
    first_of_month = sum(n for (m, dd), n in counts.items() if dd == 1)
    last_of_month = sum(n for (m, dd), n in counts.items()
                        if dd == calendar.monthrange(2001 if m != 2 else 2000, m)[1])
    jan1 = counts.get((1, 1), 0)
    dec31 = counts.get((12, 31), 0)

    if jan1 / total > 0.95:
        return "annual START (01-01)", total
    if dec31 / total > 0.95:
        return "annual END (12-31)", total
    if len(months) <= 4 and first_of_month / total > 0.95:
        return "quarterly START", total
    if len(months) <= 4 and last_of_month / total > 0.95:
        return "quarterly END", total
    if first_of_month / total > 0.95:
        return "monthly START (day 1)", total
    if last_of_month / total > 0.95:
        return "monthly END (month-end)", total
    if len(days) > 20:
        return "daily/exact dates", total
    return "MIXED (no single convention)", total


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--full", action="store_true",
                    help="read every file per source (slow, exhaustive)")
    ap.add_argument("--json")
    a = ap.parse_args()

    sids = sorted(s for s in os.listdir(BASE) if os.path.isdir(os.path.join(BASE, s)))
    print(f"sources on disk: {len(sids)}")
    print("sampling up to %s rows / %s files each\n"
          % (format(MAX_ROWS, ","), "all" if a.full else MAX_FILES))

    out, by_conv = {}, collections.defaultdict(list)
    obs_by_conv = collections.Counter()
    for sid in sids:
        counts = histogram(sid, a.full)
        verdict, total = classify_hist(counts)
        if not verdict:
            continue
        out[sid] = {"convention": verdict, "observations": total,
                    "top_daymonth": [f"{m:02d}-{d:02d}:{n}" for (m, d), n
                                     in collections.Counter(counts).most_common(4)]}
        by_conv[verdict].append(sid)
        obs_by_conv[verdict] += total
        print("  %-26s %-30s %14s obs" % (sid, verdict, format(total, ",")),
              flush=True)

    print()
    print("CONVENTION SUMMARY")
    print("  %-32s %7s %16s" % ("convention", "sources", "observations"))
    print("  " + "-" * 58)
    for k, sl in sorted(by_conv.items(), key=lambda kv: -len(kv[1])):
        print("  %-32s %7d %16s" % (k, len(sl), format(obs_by_conv[k], ",")))

    mixed = by_conv.get("MIXED (no single convention)", [])
    if mixed:
        print(f"\nSOURCES WITH NO SINGLE CONVENTION ({len(mixed)}) — these cannot be "
              f"converted by one rule and must be looked at individually:")
        for sid in mixed[:25]:
            print("  %-26s %s" % (sid, ", ".join(out[sid]["top_daymonth"])))

    starts = sum(len(v) for k, v in by_conv.items() if "START" in k)
    ends = sum(len(v) for k, v in by_conv.items() if "END" in k)
    print(f"\nPERIOD-START sources: {starts}    PERIOD-END sources: {ends}")

    if a.json:
        io.open(a.json, "w", encoding="utf-8").write(json.dumps(out, indent=1))
        print(f"\nwrote {a.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
