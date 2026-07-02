"""One-off cleanup: remove Statistics Estonia rows with mis-parsed obs_dates.

Root cause (fixed in ingest_stat_estonia.py: now uses the PxWeb `time: true` metadata
flag + a sane-date is_time_dim fallback + two-pass time-dim selection): a non-time
PxWeb category variable with numeric codes was sometimes treated as the time dimension,
so municipality / classification codes like 2101 / 4601 / 5001 / 9610 / 9999 (and low
codes like 1000 / 1881) were written as obs_date years.

Cutoff (evidence-based, NOT a blind > cur+2 drop):
  * The 2029-2085 future band is LEGITIMATE Statistics Estonia population projections
    (RV083 -> 2085, RV084 -> 2050, RV085 2025-2085). It is preserved.
  * The garbage is the continuous spread at year > 2100 (every year ~143 rows up to
    9999, plus a 9431-9434 spike) and the sub-1900 spread (years 37..1897).
  => Drop rows with obs_date year > 2100 OR year < 1900.

Each affected parquet is rewritten ATOMICALLY (per-process-unique tmp + os.replace,
zstd). A file that is 100% artifacts is left untouched and flagged (never write 0 rows).

Run:  python jobs/_stat_estonia_cleanup_future_dates.py [--apply]
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

EE_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "clean_full", "stat_estonia")
HI_CUTOFF = dt.date(2100, 12, 31)   # keep <= 2100 (preserves projections to 2085)
LO_CUTOFF = dt.date(1900, 1, 1)     # keep >= 1900-01-01


def main():
    apply = "--apply" in sys.argv
    files = sorted(glob.glob(os.path.join(EE_DIR, "**", "*.parquet"), recursive=True))
    print(f"{'APPLY' if apply else 'DRY-RUN'} | keep {LO_CUTOFF} .. {HI_CUTOFF} | {len(files)} parquet files")
    total_removed = total_kept = 0
    for f in files:
        subject = os.path.basename(f).replace(".parquet", "")
        t = pq.read_table(f)
        if "obs_date" not in t.column_names:
            continue
        col = t.column("obs_date")
        keep_mask = pc.and_(
            pc.less_equal(col, pa.scalar(HI_CUTOFF)),
            pc.greater_equal(col, pa.scalar(LO_CUTOFF)),
        )
        kept = t.filter(keep_mask)
        removed = t.num_rows - kept.num_rows
        total_kept += kept.num_rows
        if removed == 0:
            continue
        total_removed += removed
        print(f"  {subject:36} removed {removed:>9,}  (kept {kept.num_rows:>10,} of {t.num_rows:>10,})")
        if apply:
            if kept.num_rows == 0:
                print(f"    !! {subject}: ALL rows would be removed — skipping (100% artifacts, flag for re-ingest)")
                continue
            tmp = f"{f}.{os.getpid()}.cleanup.tmp"
            try:
                pq.write_table(kept, tmp, compression="zstd")
                os.replace(tmp, f)  # atomic
            finally:
                if os.path.exists(tmp):
                    os.remove(tmp)
    print(f"\n{'REMOVED' if apply else 'WOULD REMOVE'}: {total_removed:,} rows | kept: {total_kept:,}")
    if not apply:
        print("(dry run — re-run with --apply to write)")


if __name__ == "__main__":
    main()
