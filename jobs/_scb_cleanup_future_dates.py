"""One-off cleanup: remove SCB rows with mis-parsed far-future obs_dates.

Root cause (fixed in ingest_scb.py:is_time_dim): a non-time PxWeb category/Contents
variable with 4+-digit numeric codes was sometimes treated as the time dimension,
so codes like '2584' / '9610' / '9999' were written as obs_dates. This drops every
row whose obs_date is beyond (current_year + 2)-12-31 (verified to be a continuous
garbage spread to year 9999, not legitimate projections) and rewrites each affected
parquet ATOMICALLY (tmp + os.replace). Run:  python jobs/_scb_cleanup_future_dates.py [--apply]
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

SCB_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "clean_full", "scb")
CUTOFF = dt.date(dt.date.today().year + 2, 12, 31)


def main():
    apply = "--apply" in sys.argv
    files = sorted(glob.glob(os.path.join(SCB_DIR, "**", "*.parquet"), recursive=True))
    print(f"{'APPLY' if apply else 'DRY-RUN'} | cutoff = {CUTOFF} | {len(files)} scb parquet files")
    total_removed = total_kept = 0
    for f in files:
        subject = os.path.basename(f).replace(".parquet", "")
        t = pq.read_table(f)
        if "obs_date" not in t.column_names:
            continue
        keep_mask = pc.less_equal(t.column("obs_date"), pa.scalar(CUTOFF))
        kept = t.filter(keep_mask)
        removed = t.num_rows - kept.num_rows
        total_kept += kept.num_rows
        if removed == 0:
            continue
        total_removed += removed
        print(f"  {subject:8} removed {removed:>9,}  (kept {kept.num_rows:>10,} of {t.num_rows:>10,})")
        if apply:
            if kept.num_rows == 0:
                print(f"    !! {subject}: ALL rows would be removed — skipping (suspicious, not writing)")
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
