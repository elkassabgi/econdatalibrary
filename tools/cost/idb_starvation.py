"""How many idb resources are permanently stuck, and do they fill the whole run budget?

THE SHAPE. `updater/strategies/fetchers/idb.py`:

    :118   if sidecar.get(rid) == lm and blob.exists(path):   -> skip, already have it
    :151   tally.empty_unit(); sidecar[rid] = lm              -> empty/over-cap: NO parquet written
    :164   tally.empty_unit(); sidecar[rid] = lm              -> no date+value pattern: NO parquet
    :188   sidecar[rid] = lm                                  -> advance ONLY after a clean publish

The gate needs BOTH halves. Lines 151 and 164 satisfy the first and can never satisfy the second,
so every resource that reaches them re-enters `todo` on every future run, forever, and consumes
one of the MAX_PER_RUN = 40 slots each time. `tally.empty_unit()` takes no label, so nothing in
the log ever names them.

This counts them from the sidecar and the store, locally. Nothing fetched, nothing written.
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, r"E:\research\econfindatalibrary")

from core import r2_util                                              # noqa: E402

STORE = os.path.join(r2_util.ROOT, "data", "clean_full", "idb")
SIDECAR = os.path.join(STORE, "_bulk_vintages.json")
MAX_PER_RUN = 40


def main() -> int:
    if not os.path.exists(SIDECAR):
        print("no sidecar at", SIDECAR)
        return 1
    sidecar = json.load(open(SIDECAR, encoding="utf-8"))

    # the store names files <pkg_slug>__<rid[:8]>.parquet, so a resource is "held" when some
    # file carries its first 8 characters
    held = {f.rsplit("__", 1)[-1][:-8] for f in os.listdir(STORE) if f.endswith(".parquet")}

    stuck, ok = [], []
    for rid in sidecar:
        (ok if rid[:8] in held else stuck).append(rid)

    print(f"sidecar entries              {len(sidecar):>6}")
    print(f"  with a parquet in the store{len(ok):>6}   <- the gate at :118 skips these")
    print(f"  with NO parquet            {len(stuck):>6}   <- re-enter todo on EVERY run, forever")
    print()
    print(f"MAX_PER_RUN                  {MAX_PER_RUN:>6}")
    if len(stuck) >= MAX_PER_RUN:
        print(f"  the stuck set ALONE fills every slot: {len(stuck)} >= {MAX_PER_RUN}")
        print("  -> no resource that is not already stuck can ever be reached")
    else:
        free = MAX_PER_RUN - len(stuck)
        print(f"  slots left for real work each run: {free}")
        print(f"  -> a backlog of N resources needs N/{free} runs, not N/{MAX_PER_RUN}")
    print()
    print(f"parquet files in the store   {len(held):>6}")
    print(f"resources the sidecar knows  {len(sidecar):>6}")
    print("  the store PREDATES this sidecar - the two are not the same population, and")
    print("  a file with no sidecar entry is fetched again on the next run that reaches it.")
    if stuck:
        print("\nstuck resource ids (first 12):")
        for r in stuck[:12]:
            print("   ", r)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
