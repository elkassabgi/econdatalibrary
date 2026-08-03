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

DEFAULT_KEY_COLS = ("series_key", "obs_date")

# SOURCES WHOSE DEDUP KEY IS COMPUTED PER FILE, so it cannot be read off a module constant.
# treasury builds tuple(_identity_keys(["series_key","obs_date"] + out_cols)) — series_key there
# is the ENDPOINT PATH and is constant within a file, so the identity is the dimension columns.
# Auditing it against the default pair says 166 of its 181 files are under-keyed, and every one
# of those is a FALSE POSITIVE. Skipped loudly rather than mis-measured.
_COMPUTED_KEY = {"treasury"}


def dedup_key_for(source: str) -> tuple:
    """The dedup key THIS source actually passes to merge_and_write.

    Hardcoding ("series_key","obs_date") is what made the first version of this tool wrong. Of
    18 live extend_by_date sources, three differ: treasury computes its key per file, ofr uses
    ("series_id","obs_date"), and worldbank_esg uses ("country","obs_date") because it has no
    series_key column at all. Read the constant from the module; do not assume it.
    """
    import importlib
    try:
        mod = importlib.import_module(f"updater.strategies.fetchers.{source}")
    except Exception:                                              # noqa: BLE001
        return DEFAULT_KEY_COLS
    d = getattr(mod, "DEDUP", None)
    return tuple(d) if d else DEFAULT_KEY_COLS


def audit_file(path: str, key_cols: tuple) -> tuple:
    """(rows, distinct_first_col, distinct_key_tuples) or None when the file lacks the columns."""
    try:
        schema = blob.read_schema(path)
    except Exception:                                              # noqa: BLE001
        return None
    if not all(c in schema.names for c in key_cols):
        return None
    t = blob.read_table(path, columns=list(key_cols))
    if t.num_rows == 0:
        return (0, 0, 0)
    keys = pc.count_distinct(t.column(key_cols[0])).as_py()
    pairs = t.group_by(list(key_cols)).aggregate([]).num_rows
    return (t.num_rows, keys, pairs)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("sources", nargs="+")
    ap.add_argument("--prefix", default="", help="only files whose name starts with this")
    ap.add_argument("--quiet-ok", action="store_true", help="print only the under-keyed files")
    a = ap.parse_args()

    bad_total = 0
    for source in a.sources:
        if source in _COMPUTED_KEY:
            print(f"\n{source}: SKIPPED — its dedup key is computed per file "
                  f"(series_key is the endpoint path and is constant within a file, so the "
                  f"identity is the dimension columns). Auditing it against "
                  f"{'/'.join(DEFAULT_KEY_COLS)} reports every file under-keyed and every one "
                  f"of those is a false positive.")
            continue
        key_cols = dedup_key_for(source)
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
            r = audit_file(os.path.join(d, rel), key_cols)
            if r is None:
                skipped += 1
                continue
            rows, keys, pairs = r
            checked += 1
            if pairs < rows:
                bad += 1
                bad_total += 1
                print(f"  UNDER-KEYED  {rel}")
                print(f"      rows={rows:,}  distinct {key_cols[0]}={keys:,}  "
                      f"distinct {key_cols}={pairs:,}")
                print(f"      -> a merge dedup would collapse {rows - pairs:,} row(s) "
                      f"({(rows - pairs) / max(rows, 1) * 100:.1f}% of the file). "
                      f"Re-key before tailing this incrementally.")
            elif not a.quiet_ok:
                print(f"  ok           {rel}  rows={rows:,}  keys={keys:,}")
        print(f"  checked {checked}, under-keyed {bad}"
              + (f", skipped {skipped} without {'/'.join(key_cols)}" if skipped else ""))

    # A NON-ZERO EXIT so this can gate a change rather than merely inform one. The whole point
    # is to be run BEFORE enabling a tail, and a check nobody can wire into a script is a check
    # that gets skipped.
    print(f"\n{bad_total} under-keyed file(s) across {len(a.sources)} source(s)")
    return 1 if bad_total else 0


if __name__ == "__main__":
    raise SystemExit(main())
