"""One-off cleanup: remove Statistics Iceland (hagstofa) rows with mis-parsed obs_dates.

Root cause (fixed in ingest_hagstofa.py): the old is_time_dim heuristic matched the
loose value pattern ^\\d{4}[MQKHW]?\\d*$, which false-matched non-time PxWeb numeric
category codes and positional-index axes (and even a non-time variable literally named
'Year' whose values were indices). Those got treated as the time dimension, writing
garbage obs_dates — a CONTINUOUS sentinel spread reaching year 9999 (Atvinnuvegir max
8722, Samfelag 9897, Ibuar 9999) and sub-1900 values (years 10/111/1000 from low codes),
with row counts matching category-code cardinalities.

The fixed ingester now prefers the PxWeb metadata `time: true` flag, so future re-ingests
are clean. This script purges the existing artifacts.

CUTOFF (evidence-based, NOT a blind year>cur+2 drop): keep 1900 <= year <= 2100.
Justification from the measured distribution:
  * Statistics Iceland publishes a LEGITIMATE population projection (MAN09xxx) as a clean,
    contiguous annual band 2027..2074 at a CONSTANT 1,734 rows/year (+ a 2100 tail of 29
    rows) in Ibuar.parquet — exactly the projection band the default cutoff must preserve.
  * Efnahagur has a small contiguous 2029..2031 band (23 rows/yr) — plausible short-term
    forecasts; kept.
  * Everything > 2100 (2200, 2300 ... 9999) and < 1900 (10, 111, 1000 ...) is the sentinel
    garbage spread, with no constant-cardinality annual structure. Dropped.

Rewrites each affected parquet ATOMICALLY (per-process-unique tmp + os.replace,
compression='zstd'). Never writes a file to 0 rows; a 100%-artifact file is left intact
and flagged for re-ingest.

Run:  python jobs/_hagstofa_cleanup_future_dates.py [--apply]
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

HAG_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "clean_full", "hagstofa")
YEAR_HI = 2100   # keep population/long-term projections up to 2100
YEAR_LO = 1900
CUTOFF_HI = dt.date(YEAR_HI, 12, 31)
CUTOFF_LO = dt.date(YEAR_LO, 1, 1)


def main():
    apply = "--apply" in sys.argv
    files = sorted(glob.glob(os.path.join(HAG_DIR, "**", "*.parquet"), recursive=True))
    print(f"{'APPLY' if apply else 'DRY-RUN'} | keep {YEAR_LO}..{YEAR_HI} | {len(files)} hagstofa parquet files")
    total_removed = total_kept = 0
    flagged = []
    for f in files:
        subject = os.path.basename(f).replace(".parquet", "")
        t = pq.read_table(f)
        if "obs_date" not in t.column_names:
            continue
        col = t.column("obs_date")
        keep_mask = pc.and_(
            pc.greater_equal(col, pa.scalar(CUTOFF_LO)),
            pc.less_equal(col, pa.scalar(CUTOFF_HI)),
        )
        kept = t.filter(keep_mask)
        removed = t.num_rows - kept.num_rows
        total_kept += kept.num_rows
        if removed == 0:
            print(f"  {subject:14} clean (0 removed, {t.num_rows:,} rows)")
            continue
        total_removed += removed
        print(f"  {subject:14} removed {removed:>9,}  (kept {kept.num_rows:>10,} of {t.num_rows:>10,})")
        if apply:
            if kept.num_rows == 0:
                print(f"    !! {subject}: ALL rows are artifacts — leaving intact, FLAG for re-ingest")
                flagged.append(subject)
                continue
            tmp = f"{f}.{os.getpid()}.cleanup.tmp"
            try:
                pq.write_table(kept, tmp, compression="zstd")
                os.replace(tmp, f)  # atomic
            finally:
                if os.path.exists(tmp):
                    os.remove(tmp)
    print(f"\n{'REMOVED' if apply else 'WOULD REMOVE'}: {total_removed:,} rows | kept: {total_kept:,}")
    if flagged:
        print("FLAGGED (100% artifacts, not written, re-ingest needed):", flagged)
    if not apply:
        print("(dry run — re-run with --apply to write)")


if __name__ == "__main__":
    main()
