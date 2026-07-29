"""Re-stamp period-START observations onto period-END, safely and reversibly.

WHY. The library stores one `obs_date` per observation, but sources disagree about
WHICH day of the period it names: an annual 2024 point is `2024-01-01` in 100 sources
and `2024-12-31` in 59. Measured over the complete store (DATE_CONVENTIONS.md), 119
sources / 573,938,420 observations use period-START. Users joining across sources get
empty joins or silent one-year lags. Nothing has been downloaded yet, so the cost of
standardising is zero today and rises permanently from here.

Period-END is the target because it is where the data already is: annual END carries
8,036,676,385 observations against annual START's 29,812,857.

WHAT IS NOT TOUCHED, and why:
  * daily / exact-date sources (37) — the date IS the observation, there is no period.
  * MIXED sources (18) — several cadences share one source, so no single rule is safe.
  * Anything whose cadence cannot be inferred from its own spacing (see below).

CADENCE IS INFERRED PER SERIES, NOT PER SOURCE. A source classified "annual START"
can still hold monthly series, and converting those with an annual rule would map
twelve observations onto one date. So each series' own median gap decides its period.

THE COLLISION ASSERT IS THE POINT. If a cadence is misread, several observations map
to the same day; the store dedups on (series_key, obs_date), so the extra ones would
be silently DELETED — a data loss that looks like a successful run. Every series is
checked for uniqueness after mapping, and a file with any collision is written NOT AT
ALL. Row counts are compared before and after for the same reason.

Reversible: the original parquet is kept as <name>.parquet.prestamp until verified.

Usage:
  python tools/restamp_period_end.py --source unctad_ciocgeaia [--apply]
  python tools/restamp_period_end.py --all-start [--apply]
Without --apply it reports what WOULD change and writes nothing.
"""
from __future__ import annotations

import argparse
import calendar
import collections
import datetime as dt
import io
import json
import os
import shutil
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import pyarrow as pa                                          # noqa: E402
import pyarrow.parquet as pq                                  # noqa: E402

BASE = os.path.join(ROOT, "data", "clean_full")


def _month_end(y, m):
    return dt.date(y, m, calendar.monthrange(y, m)[1])


def period_end(d: dt.date, cadence: str):
    """Last day of the period that a period-START date opens."""
    if cadence == "annual":
        return dt.date(d.year, 12, 31)
    if cadence == "quarterly":
        q_end = ((d.month - 1) // 3) * 3 + 3
        return _month_end(d.year, q_end)
    if cadence == "monthly":
        return _month_end(d.year, d.month)
    return None


def cadence_of(dates):
    """Infer one series' cadence from its OWN spacing. None when unclear."""
    ds = sorted(set(dates))
    if len(ds) < 2:
        return None                       # a single point cannot reveal a period
    gaps = sorted((b - a).days for a, b in zip(ds, ds[1:]))
    med = gaps[len(gaps) // 2]
    if 350 <= med <= 380:
        return "annual"
    if 85 <= med <= 95:
        return "quarterly"
    if 28 <= med <= 31:
        return "monthly"
    return None


def single_point_cadence(d: dt.date, source_conv: str, file_cad=None):
    """Cadence for a ONE-OBSERVATION series, or None.

    Spacing cannot reveal a period from a single point, and many sources are entirely
    single-point (imf_gender_budgeting: all 288 series, unctad_mfbcoboa: all 155).
    Declining them outright would leave those sources still mixed after a migration
    meant to end mixing.

    The source's own measured convention supplies the missing cadence — but only when
    the date actually MATCHES that pattern, which is what keeps it honest. An
    annual-START source stamps 01-01; a point at 2024-03-01 in that source is not an
    annual stamp, so it is left alone rather than dragged to 2024-12-31.
    """
    # Prefer the cadence actually observed in THIS file over the source-level label.
    if file_cad:
        if file_cad == "annual" and (d.month, d.day) == (1, 1):
            return "annual"
        if file_cad == "quarterly" and d.day == 1 and d.month in (1, 4, 7, 10):
            return "quarterly"
        if file_cad == "monthly" and d.day == 1:
            return "monthly"
        return None
    if not source_conv or "START" not in source_conv:
        return None
    if source_conv.startswith("annual") and (d.month, d.day) == (1, 1):
        return "annual"
    if source_conv.startswith("quarterly") and d.day == 1 and d.month in (1, 4, 7, 10):
        return "quarterly"
    if source_conv.startswith("monthly") and d.day == 1:
        return "monthly"
    return None


def convert_table(t, source_conv=None):
    """Return (new_table, stats) or (None, stats) if anything is unsafe."""
    keys = t.column("series_key").to_pylist()
    dates = t.column("obs_date").to_pylist()
    by = collections.defaultdict(list)
    for i, (k, d) in enumerate(zip(keys, dates)):
        by[k].append(i)

    stats = collections.Counter()
    new_dates = list(dates)

    # The FILE's own dominant cadence, for single-point series. Using the SOURCE's
    # convention was wrong: ilostat is classified monthly at source level but holds
    # quarterly files (…_Q.parquet), so single-point series inside them were given a
    # MONTHLY period — 2020-01-01 -> 2020-01-31 instead of 2020-03-31. Cadence is a
    # property of the data in hand, not of the label on its parent.
    seen_cad = collections.Counter()
    for k, idx in by.items():
        c = cadence_of([dates[i] for i in idx])
        if c:
            seen_cad[c] += 1
    file_cad = seen_cad.most_common(1)[0][0] if seen_cad else None

    for k, idx in by.items():
        ds = [dates[i] for i in idx]
        cad = cadence_of(ds)
        if cad is None and len(set(ds)) == 1 and ds[0] is not None:
            cad = single_point_cadence(ds[0], source_conv, file_cad)
            if cad:
                stats["series_single_point_from_source"] += 1
        if cad is None:
            stats["series_cadence_unknown"] += 1
            continue                      # leave untouched rather than guess
        mapped = {}
        for i in idx:
            d = dates[i]
            if d is None:
                continue
            e = period_end(d, cad)
            if e is None:
                continue
            if e in mapped:
                # Two observations would share one date. The store dedups on
                # (series_key, obs_date), so proceeding would DELETE one of them.
                stats["COLLISION"] += 1
                return None, stats
            mapped[e] = i
            new_dates[i] = e
        stats[f"series_{cad}"] += 1

    out = pa.table({
        "series_key": t.column("series_key"),
        "obs_date": pa.array(new_dates, pa.date32()),
        "value": t.column("value"),
    })
    if out.num_rows != t.num_rows:
        stats["ROWCOUNT_CHANGED"] += 1
        return None, stats
    return out, stats


def period_start_profile(sid):
    """Do this source's ACTUAL stamps look like calendar period-STARTS?

    The audit's label is not sufficient authority, and trusting it nearly destroyed a
    source. un_wpp was labelled "quarterly START" because the classifier's
    `len(months) <= 4` gate is trivially satisfied by len(months) == 1 — and all
    27,756,924 of its observations sit on 07-01, UN MID-YEAR population estimates.
    Re-stamping those to 12-31 would move every value half a year, and the collision
    assert could never notice: one observation per year maps to one 12-31 per year,
    uniquely. A guard against collisions is not a guard against being semantically
    wrong.

    So read the real (month, day) histogram and require it to look like the start of a
    calendar period. A single mid-year month is a MID-PERIOD convention, not a start.
    """
    import pyarrow.compute as pc
    d = os.path.join(BASE, sid)
    counts = collections.Counter()
    for f in sorted(x for x in os.listdir(d)
                    if x.endswith(".parquet") and not x.startswith("_")):
        try:
            pf = pq.ParquetFile(os.path.join(d, f))
            if "obs_date" not in set(pf.schema_arrow.names):
                continue
            for i in range(pf.num_row_groups):
                col = pf.read_row_group(i, columns=["obs_date"])["obs_date"]
                key = pc.add(pc.multiply(pc.month(col), 100), pc.day(col))
                for e in pc.value_counts(key.drop_null()):
                    md = e["values"].as_py()
                    if md is not None:
                        counts[(md // 100, md % 100)] += e["counts"].as_py()
        except Exception as e:                                # noqa: BLE001
            # NOT swallowed: an unreadable file is a hole in the evidence this
            # decision rests on, and a silent `continue` is how 270 million bls rows
            # went missing from the audit that motivated this tool.
            return False, f"unreadable file {f} ({type(e).__name__})"
    if not counts:
        return False, "no dated observations"
    total = sum(counts.values())
    day1 = sum(n for (m, dd), n in counts.items() if dd == 1)
    months_at_1 = {m for (m, dd) in counts if dd == 1}
    if day1 / total < 0.90:
        return False, (f"only {100 * day1 / total:.1f}% of stamps are on day 1 — "
                       f"not a period-START source")
    if months_at_1 == {1}:
        return True, "annual starts (01-01)"
    if months_at_1 <= {1, 4, 7, 10} and len(months_at_1) >= 2:
        return True, f"quarterly starts {sorted(months_at_1)}"
    if len(months_at_1) >= 6:
        return True, f"monthly starts ({len(months_at_1)} months)"
    return False, (f"day-1 stamps occur only in month(s) {sorted(months_at_1)} — a "
                   f"MID-PERIOD convention, not a period start")


def do_source(sid, apply=False, source_conv=None):
    d = os.path.join(BASE, sid)
    if not os.path.isdir(d):
        print(f"  {sid}: no local directory"); return
    ok, why = period_start_profile(sid)
    if not ok:
        print(f"  {sid:<26} SKIPPED — {why}", flush=True)
        return 0
    files = sorted(f for f in os.listdir(d)
                   if f.endswith(".parquet") and not f.startswith("_"))
    tot = collections.Counter()
    changed_rows = files_ok = files_refused = 0
    for f in files:
        p = os.path.join(d, f)
        try:
            t = pq.read_table(p)
        except Exception as e:                                # noqa: BLE001
            print(f"  {sid}/{f}: unreadable ({type(e).__name__})", flush=True); continue
        if "obs_date" not in t.schema.names:
            continue
        out, stats = convert_table(t, source_conv)
        tot.update(stats)
        if out is None:
            files_refused += 1
            print(f"  {sid}/{f}: REFUSED — {dict(stats)}", flush=True)
            continue
        files_ok += 1
        before = t.column("obs_date").to_pylist()
        after = out.column("obs_date").to_pylist()
        changed_rows += sum(1 for a, b in zip(before, after) if a != b)
        if apply:
            shutil.copy2(p, p + ".prestamp")
            tmp = p + ".tmp"
            pq.write_table(out, tmp, compression="zstd")
            os.replace(tmp, p)
            chk = pq.read_table(p)
            assert chk.num_rows == t.num_rows, f"{p}: row count changed on disk"
    verb = "re-stamped" if apply else "would re-stamp"
    print(f"  {sid:<26} {verb} {changed_rows:>12,} rows across {files_ok} file(s)"
          + (f", {files_refused} REFUSED" if files_refused else "")
          + (f", {tot['series_cadence_unknown']:,} series left alone (cadence unclear)"
             if tot.get("series_cadence_unknown") else ""), flush=True)
    return changed_rows


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--source", action="append", default=[])
    ap.add_argument("--all-start", action="store_true",
                    help="every source the audit classified period-START")
    ap.add_argument("--audit", default=os.path.join(ROOT, "data", "_aqueduct",
                                                    "dateconv_full.json"))
    ap.add_argument("--apply", action="store_true", help="write; default is dry-run")
    a = ap.parse_args()

    srcs = list(a.source)
    if a.all_start:
        d = json.load(io.open(a.audit, encoding="utf-8"))
        srcs += sorted(k for k, v in d.items() if "START" in v["convention"])
    convs = {}
    try:
        convs = {k: v["convention"] for k, v in
                 json.load(io.open(a.audit, encoding="utf-8")).items()}
    except Exception:                                         # noqa: BLE001
        print("WARNING: no audit file — single-point series cannot be converted")
    if not srcs:
        print("nothing to do — pass --source or --all-start"); return 2
    print(f"{'APPLYING' if a.apply else 'DRY RUN'} over {len(srcs)} source(s)\n")
    total = 0
    for s in srcs:
        total += do_source(s, a.apply, convs.get(s)) or 0
    print(f"\ntotal rows {'re-stamped' if a.apply else 'that would change'}: {total:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
