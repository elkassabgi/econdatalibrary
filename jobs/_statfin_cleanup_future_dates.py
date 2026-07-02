"""One-off cleanup: remove StatFin rows with mis-parsed implausible obs_dates.

Root cause (fixed in ingest_statfin.py:is_time_dim + parse_jsonstat2): the old loose
value heuristic (^\\d{4}[MQKHW]?\\d*$) false-matched non-time PxWeb category/ContentsCode
numeric codes (sector codes, 4-digit municipality/industry codes like 2584/9610/9999,
8-digit codes), and the single-pass selector let such a dim beat the real time axis
("timeperiod_y", flagged `time: true` in PxWeb metadata). That wrote obs_dates across a
continuous garbage spread (year 11..9999, plus sub-1900 from low codes).

Cutoff DECISION (from the measured distribution): drop rows with obs_date year > 2100
OR year < 1900.
  * The garbage is a CONTINUOUS spread (11, 110, 1000, 1120, ... 9610, 9999) whose row
    counts match category-code cardinalities (recurring 660/758/880/220/102 signatures),
    NOT a clean annual series.
  * StatFin DOES publish a legitimate population PROJECTION (table 'vaenn',
    väestöennuste) as a clean annual band 2029..2075 — all <= 2100. The 2100 ceiling
    PRESERVES that product. Extending the cutoff down to current_year+2 would wrongly
    delete ~19k legitimate projection rows.
  * Residual: ntp/vtp each retain 51 garbage rows at exactly year 2100 (sector codes
    mis-parsed to 2100); year alone cannot distinguish those from a real 2100 projection,
    so the default cutoff leaves them (102 rows total, disclosed).

Rewrites each affected parquet ATOMICALLY (per-process tmp + os.replace, zstd). Never
writes a parquet to 0 rows — a 100%-artifact file is left in place and flagged.

Run:  python jobs/_statfin_cleanup_future_dates.py [--apply]
(default is a dry run; pass --apply to write.)
"""
from __future__ import annotations
import datetime as dt
import glob
import os
import sys

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

STATFIN_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "clean_full", "statfin")
LO = dt.date(1900, 1, 1)        # drop year < 1900
HI = dt.date(2100, 12, 31)      # drop year > 2100 (preserves legit projections to 2100)


def main():
    apply = "--apply" in sys.argv
    files = sorted(glob.glob(os.path.join(STATFIN_DIR, "*.parquet")))
    print(f"{'APPLY' if apply else 'DRY-RUN'} | keep {LO}..{HI} | {len(files)} statfin parquet files")
    total_removed = total_kept = 0
    flagged_all_artifacts = []
    for f in files:
        subject = os.path.basename(f).replace(".parquet", "")
        t = pq.read_table(f)
        if "obs_date" not in t.column_names:
            continue
        od = t.column("obs_date")
        keep_mask = pc.and_(pc.greater_equal(od, pa.scalar(LO)),
                            pc.less_equal(od, pa.scalar(HI)))
        kept = t.filter(keep_mask)
        removed = t.num_rows - kept.num_rows
        total_kept += kept.num_rows
        if removed == 0:
            continue
        total_removed += removed
        print(f"  {subject:12} removed {removed:>9,}  (kept {kept.num_rows:>10,} of {t.num_rows:>10,})")
        if kept.num_rows == 0:
            flagged_all_artifacts.append(subject)
            print(f"    !! {subject}: 100% artifacts — NOT writing (flag for re-ingest)")
            continue
        if apply:
            tmp = f"{f}.{os.getpid()}.cleanup.tmp"
            try:
                pq.write_table(kept, tmp, compression="zstd")
                os.replace(tmp, f)  # atomic
            finally:
                if os.path.exists(tmp):
                    os.remove(tmp)
    print(f"\n{'REMOVED' if apply else 'WOULD REMOVE'}: {total_removed:,} rows | kept: {total_kept:,}")
    if flagged_all_artifacts:
        print("FLAGGED (100% artifacts, left intact, re-ingest):", flagged_all_artifacts)
    if not apply:
        print("(dry run — re-run with --apply to write)")


if __name__ == "__main__":
    main()
