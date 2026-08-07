"""Seed cso's `_held.json` — which matrices the STORE actually holds.

WHY THIS EXISTS (measured 2026-08-07). cso picks each run's work as
    changed = [m for m, u in cur_upd.items() if stored.get(m) != u]
    changed.sort(key=revision_date, reverse=True)
and takes the first MAX_TABLES=60. The cursor (`_collupd.json`) was being written to the
runner's local disk under the r2 backend, so it was thrown away every run; that was fixed
2026-08-03 and the cursor restarted EMPTY (61 entries on 08-03, 120 on 08-06) while the
store already held 7,613 matrices.

The consequence is not a stall, it is a mis-ordering: with only 120 timestamps stored,
12,788 of the publisher's 12,908 matrices look "changed", and newest-revision-first happily
re-pulls matrices we ALREADY HAVE while 290 catalogued matrices with NO rows in the store
wait their turn. A cso run takes ~34 min for 60 matrices (2,017.8 s measured), which is
already at its time budget, so raising MAX_TABLES cannot help — only ordering can.
At 60/run those holes are ~213 runs away.

`_held.json` is the missing fact: the set of matrices the store can actually serve. The
fetcher sorts unheld-first on it, so real holes are filled before re-pulls, and adds each
run's successfully-pulled matrices to it. This tool seeds it once from the current store.

It asserts nothing about FRESHNESS — "held" is not "current". A held matrix still gets
re-pulled when its publisher revision differs from the cursor; it just yields priority to a
matrix that has no rows at all. Claiming currency we cannot verify is what would freeze
stale data silently.

  python tools/seed_cso_held.py            # report only
  python tools/seed_cso_held.py --apply    # write _held.json through the blob layer
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def held_from_store(store_dir: str) -> set[str]:
    """Distinct matrix codes present in the store, read from series_key prefixes.

    cso keys are `CSO:<MATRIX>:<dim>=<code>:...`, so the matrix is the SECOND
    ':'-segment. Reading only the series_key column keeps this to a metadata-ish scan.
    """
    import duckdb
    files = [f.replace(os.sep, "/") for f in
             glob.glob(os.path.join(store_dir, "*.parquet"))]
    if not files:
        raise SystemExit(f"no parquet files under {store_dir!r} — refusing to write an "
                         f"empty _held.json, which would claim the store holds nothing")
    lst = ", ".join(f"'{f}'" for f in files)
    q = duckdb.connect()
    rows = q.execute(f"""
        select distinct string_split(series_key, ':')[2]
        from read_parquet([{lst}])""").fetchall()
    return {r[0] for r in rows if r[0]}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    store_dir = os.path.join(ROOT, "data", "clean_full", "cso")
    held = held_from_store(store_dir)
    print(f"store holds {len(held):,} distinct matrices")

    cur = os.path.join(store_dir, "_collupd.json")
    n_cur = len(json.load(open(cur, encoding="utf-8"))) if os.path.exists(cur) else 0
    print(f"revision cursor (_collupd.json) has {n_cur:,} entries — the gap between these "
          f"two numbers is what mis-orders the queue")

    if not a.apply:
        print("(report only — pass --apply to write _held.json)")
        return 0

    from updater import blob
    path = os.path.join(store_dir, "_held.json")
    blob.write_bytes_atomic(
        path, json.dumps(sorted(held), separators=(",", ":")).encode("utf-8"))
    back = blob.read_bytes(path)
    n = len(json.loads(back.decode("utf-8"))) if back else 0
    print(f"wrote _held.json through the blob layer; read back {n:,} matrices "
          f"({'OK' if n == len(held) else 'MISMATCH'})")
    return 0 if n == len(held) else 1


if __name__ == "__main__":
    sys.exit(main())
