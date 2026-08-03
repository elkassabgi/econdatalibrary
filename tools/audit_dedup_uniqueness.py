"""Is a store's dedup key actually a KEY? Run this before tailing a source incrementally.

WHY. merge_and_write dedups on (series_key, obs_date). If a store already holds several rows
sharing that pair, the first incremental merge collapses them — the file does not gain the
tail, it loses most of itself. never-shrink refuses the write, so the data survives, but the
symptom is a baffling "refusing shrink 5,910->15" that looks like a fetcher bug and is nothing
of the kind: the STORE was never uniquely keyed.

FOUND THE HARD WAY, TWICE. comtrade had to be re-keyed before it could ever auto-update
(task #16). Then on 2026-08-03, while adding census families to the date tail, bds looked like
an easy win — measurably behind (2022 stored, 2023 published), fetches cleanly, and every one
of the 5,910 rows it returns maps to a key the store already holds. It also holds 5,910 rows
under FIFTEEN distinct (series_key, obs_date) pairs. Enabling it would have tried to collapse
99.7% of the file. Nothing in the fetch or the key mapping hinted at it; the only tell was
counting the pairs.

So this is the check that should precede "just add the family". It is cheap relative to being
wrong: two columns, one group_by per file.

    python tools/audit_dedup_uniqueness.py census
    python tools/audit_dedup_uniqueness.py census --prefix intltrade__
    python tools/audit_dedup_uniqueness.py comtrade bds --quiet-ok
"""
from __future__ import annotations
import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import pyarrow.compute as pc                                       # noqa: E402
from updater import blob, config                                   # noqa: E402

KEY_COLS = ("series_key", "obs_date")


def audit_file(path: str) -> tuple:
    """(rows, distinct_keys, distinct_pairs) or None when the file has no dedup columns."""
    try:
        schema = blob.read_schema(path)
    except Exception:                                              # noqa: BLE001
        return None
    if not all(c in schema.names for c in KEY_COLS):
        return None
    t = blob.read_table(path, columns=list(KEY_COLS))
    if t.num_rows == 0:
        return (0, 0, 0)
    keys = pc.count_distinct(t.column("series_key")).as_py()
    pairs = t.group_by(list(KEY_COLS)).aggregate([]).num_rows
    return (t.num_rows, keys, pairs)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("sources", nargs="+")
    ap.add_argument("--prefix", default="", help="only files whose name starts with this")
    ap.add_argument("--quiet-ok", action="store_true", help="print only the under-keyed files")
    a = ap.parse_args()

    bad_total = 0
    for source in a.sources:
        d = config.source_dir(source)
        try:
            files = [f for f in blob.list_parquets(d, recursive=True)
                     if not os.path.basename(f).startswith("_")]
        except Exception as e:                                     # noqa: BLE001
            print(f"{source}: cannot list ({type(e).__name__}: {e})")
            continue
        files = [f for f in files if os.path.basename(f).startswith(a.prefix)]
        print(f"\n{source}: {len(files)} file(s)"
              + (f" matching {a.prefix!r}" if a.prefix else ""))
        checked = skipped = bad = 0
        for rel in sorted(files):
            r = audit_file(os.path.join(d, rel))
            if r is None:
                skipped += 1
                continue
            rows, keys, pairs = r
            checked += 1
            if pairs < rows:
                bad += 1
                bad_total += 1
                print(f"  UNDER-KEYED  {rel}")
                print(f"      rows={rows:,}  distinct series_key={keys:,}  "
                      f"distinct (series_key,obs_date)={pairs:,}")
                print(f"      -> a merge dedup would collapse {rows - pairs:,} row(s) "
                      f"({(rows - pairs) / max(rows, 1) * 100:.1f}% of the file). "
                      f"Re-key before tailing this incrementally.")
            elif not a.quiet_ok:
                print(f"  ok           {rel}  rows={rows:,}  keys={keys:,}")
        print(f"  checked {checked}, under-keyed {bad}"
              + (f", skipped {skipped} without {'/'.join(KEY_COLS)}" if skipped else ""))

    # A NON-ZERO EXIT so this can gate a change rather than merely inform one. The whole point
    # is to be run BEFORE enabling a tail, and a check nobody can wire into a script is a check
    # that gets skipped.
    print(f"\n{bad_total} under-keyed file(s) across {len(a.sources)} source(s)")
    return 1 if bad_total else 0


if __name__ == "__main__":
    raise SystemExit(main())
