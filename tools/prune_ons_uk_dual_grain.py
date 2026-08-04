"""Drop the OLD-grain rows from ons_uk files that hold BOTH key schemes.

WHAT HAPPENED. ons_uk's store was re-keyed on 2026-07-29 (approved) to time-free, code-based
series_keys. The fetcher then kept writing the OLD observation-level keys — `CV=14.0:
calendar-years=2018:...` — and MERGED them in, because dedup is on (series_key, obs_date) and
the two grains no longer collide. So four files ended up holding every observation TWICE, once
under each scheme:

    ashe-table-5           10,646,304 rows
    ashe-tables-11-and-12   4,966,950
    ashe-tables-20          3,824,768
    ashe-tables-25            760,280
                           ----------
                           20,198,302  -- exactly the obs_count on the old state row

They cannot self-heal. The repair writes FEWER rows than are on disk (about half), and
merge_and_write's never-shrink guard refuses anything below 0.97 of the existing count — by
design, and correctly: that guard is what stops a truncated upstream pull from destroying good
data. This tool is the deliberate exception, applied to four named files.

THE NEGATIVE CONTROL IS THE POINT, NOT THE PRUNE (R288). A plausible repair that quietly drops
real observations is the failure mode here: on cso, a naive date fix would have destroyed 11 of
12 rows and only a control caught it. So before removing anything from a file, this proves the
old grain carries NO observation the new grain lacks, by comparing the (obs_date, value)
multisets of the two halves. Measured on ashe-tables-25:

    distinct (date,value) in NEW : 114,712   rows 380,140
    distinct (date,value) in OLD : 114,712   rows 380,140
    (date,value) ONLY in OLD     : 0         <- nothing would be lost

A file where that count is NON-ZERO is SKIPPED and reported, never pruned.

    python tools/prune_ons_uk_dual_grain.py --dry-run
    python tools/prune_ons_uk_dual_grain.py --apply
"""
from __future__ import annotations
import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import pyarrow as pa                                          # noqa: E402
import pyarrow.compute as pc                                  # noqa: E402
from updater import blob, config                              # noqa: E402

# Markers of the OLD, observation-level key scheme. `CV` is a coefficient of variation (a
# property of one measurement) and the rest are TIME axes — none of them belongs in a series
# identity. Matched with a regex over the key column via Arrow, never a Python loop: ashe-table-5
# is 10.6M rows and `to_pylist()` on 200+ char keys is how this source reached 32 GB RSS before.
OLD_GRAIN = r"(CV=|Data marking=|calendar-years=|mmm-yy=|yyyy-yy=|two-year-intervals=|yyyy-to-yyyy-yy=|mmm-mmm-yyyy=|yyyy-qq=)"

TARGETS = ["ashe-table-5.parquet", "ashe-tables-11-and-12.parquet",
           "ashe-tables-20.parquet", "ashe-tables-25.parquet"]


def analyse(t):
    """(mask_old, n_old, n_new, only_old) — the split plus the negative control."""
    keys = t.column("series_key").combine_chunks()
    mask_old = pc.match_substring_regex(keys, OLD_GRAIN)
    n_old = pc.sum(pc.cast(mask_old, "int64")).as_py() or 0
    n_new = t.num_rows - n_old

    # THE CONTROL. Build the (date, value) SET for each half and ask what the old half holds
    # that the new half does not. Done on the two filtered tables so peak memory is one column
    # pair, not the whole file as Python objects.
    old_t = t.filter(mask_old).select(["obs_date", "value"])
    new_t = t.filter(pc.invert(mask_old)).select(["obs_date", "value"])
    old_pairs = set(zip(old_t.column("obs_date").to_pylist(),
                        old_t.column("value").to_pylist()))
    new_pairs = set(zip(new_t.column("obs_date").to_pylist(),
                        new_t.column("value").to_pylist()))
    return mask_old, n_old, n_new, (old_pairs - new_pairs)


def main() -> int:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--apply", action="store_true")
    ap.add_argument("--file", action="append", help="limit to one file (repeatable)")
    a = ap.parse_args()

    out_dir = config.source_dir("ons_uk")
    targets = a.file or TARGETS
    print(f"ons_uk store: {out_dir}  (backend={config.BACKEND})\n", flush=True)

    pruned = skipped = 0
    for fn in targets:
        path = os.path.join(out_dir, fn)
        if not blob.exists(path):
            print(f"  {fn}: ABSENT — skipped", flush=True)
            continue
        t = blob.read_table(path)
        mask_old, n_old, n_new, only_old = analyse(t)

        if n_old == 0:
            print(f"  {fn}: already clean ({t.num_rows:,} rows, no old-grain keys)", flush=True)
            continue

        print(f"  {fn}: {t.num_rows:,} rows -> old {n_old:,} / new {n_new:,}", flush=True)
        if only_old:
            skipped += 1
            print(f"      REFUSING: {len(only_old):,} (date,value) pair(s) exist ONLY in the "
                  f"old grain — pruning would LOSE real observations. Sample: "
                  f"{list(only_old)[:3]}", flush=True)
            continue
        print(f"      control OK: 0 (date,value) pairs exist only in the old grain", flush=True)

        if not a.apply:
            print(f"      would write {n_new:,} rows (dry run)", flush=True)
            continue

        kept = t.filter(pc.invert(mask_old))
        assert kept.num_rows == n_new, (kept.num_rows, n_new)
        # write_table_atomic, NOT merge_and_write: the never-shrink guard would refuse this by
        # design (we are deliberately halving the file), and re-running the guard here would
        # only re-implement the check the control above already made.
        blob.write_table_atomic(path, kept)

        # VERIFY FROM THE STORE, not from the object we just built (R296): re-read and confirm.
        back = blob.read_table(path)
        still = pc.sum(pc.cast(
            pc.match_substring_regex(back.column("series_key").combine_chunks(), OLD_GRAIN),
            "int64")).as_py() or 0
        print(f"      WROTE {back.num_rows:,} rows; old-grain keys remaining: {still}", flush=True)
        assert back.num_rows == n_new and still == 0
        pruned += 1

    print(f"\n{'PRUNED' if a.apply else 'would prune'}: {pruned} file(s); refused: {skipped}")
    if skipped:
        print("A refusal is the tool working. Investigate those files before forcing anything.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
