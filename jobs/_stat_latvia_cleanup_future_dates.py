"""One-off cleanup: remove Statistics Latvia (CSP) rows with mis-parsed obs_dates.

Root cause (fixed in ingest_stat_latvia.py): the old is_time_dim() value heuristic
matched ^\\d{4}[MQKHW]?\\d*$, so non-time PxWeb numeric codes (AREA municipality
codes 4601/5001/9610, ContentsCode/INDICATOR numeric codes, 9999 totals) were
treated as the time dimension and written as obs_dates. Ingester now locks onto the
PxWeb metadata `time: true` flag.

Evidence for the cutoff (measured 2026-06-24 on data/clean_full/stat_latvia/):
  * Legit data is a dense annual/quarterly/monthly series ending cleanly at 2026
    (2026: 61,687 rows; 2025: 371,652; 2024: 529,950 ...).
  * Then a COMPLETE gap: ZERO rows in 2027..2099.
  * Garbage resumes at 2100 and spreads uniformly (~22 rows / "year") to 9999, plus
    a low-side spread of 3-digit/4-digit codes parsed as years 100..1896.
  * Latvia (CSP) publishes no long-horizon population projection in this universe,
    so there is NO legitimate band to preserve. The year-2100 entries are themselves
    part of the garbage spread (code "2100" mis-parsed as a year).
  => Cutoff: keep 1900 <= year <= current_year+2 (2028). Removes every sentinel
     including the 2100 band; preserves all real data (which tops out at 2026).

Rewrites each affected parquet ATOMICALLY (per-process-unique tmp + os.replace,
compression="zstd"). NEVER writes a parquet to 0 rows.

Run:  python jobs/_stat_latvia_cleanup_future_dates.py [--apply]
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

LV_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "clean_full", "stat_latvia")
HI_CUTOFF = dt.date(dt.date.today().year + 2, 12, 31)  # 2028-12-31
LO_CUTOFF = dt.date(1900, 1, 1)


def main():
    apply = "--apply" in sys.argv
    files = sorted(glob.glob(os.path.join(LV_DIR, "*.parquet")))
    print(f"{'APPLY' if apply else 'DRY-RUN'} | keep {LO_CUTOFF} .. {HI_CUTOFF} | {len(files)} latvia parquet files")
    total_removed = total_kept = 0
    for f in files:
        subject = os.path.basename(f).replace(".parquet", "")
        t = pq.read_table(f)
        if "obs_date" not in t.column_names:
            continue
        col = t.column("obs_date")
        keep_mask = pc.and_(
            pc.greater_equal(col, pa.scalar(LO_CUTOFF)),
            pc.less_equal(col, pa.scalar(HI_CUTOFF)),
        )
        kept = t.filter(keep_mask)
        removed = t.num_rows - kept.num_rows
        total_kept += kept.num_rows
        if removed == 0:
            continue
        total_removed += removed
        print(f"  {subject:22} removed {removed:>9,}  (kept {kept.num_rows:>10,} of {t.num_rows:>10,})")
        if apply:
            if kept.num_rows == 0:
                print(f"    !! {subject}: ALL rows would be removed - skipping (suspicious, flag for re-ingest)")
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
        print("(dry run - re-run with --apply to write)")


if __name__ == "__main__":
    main()
