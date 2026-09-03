"""HOW MANY SERVED idb SERIES CARRY CONTRADICTORY VALUES? Count series, not rows.

Earlier I measured rows and distinct (series_key, obs_date) pairs, which sizes the DEFECT. This
counts the SERIES a user could download and find two different numbers on one date, which is the
only figure that can justify a disclosure.

The store is one parquet per resource; a series key can appear in several. So the count is done
across the whole store at once, keyed by series_key: a series is AFFECTED when some date under it
carries more than one DISTINCT value. Two rows with the same value on the same date are a
duplicate, not a contradiction, and are counted separately - conflating them would overstate.

Local and free. Nothing fetched, nothing written.
"""
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

import pyarrow.parquet as pq                                          # noqa: E402

from core import r2_util                                             # noqa: E402

STORE = os.path.join(r2_util.ROOT, "data", "clean_full", "idb")


def main() -> int:
    files = sorted(f for f in os.listdir(STORE) if f.endswith(".parquet"))
    # series_key -> obs_date -> set of values.  Memory: ~15M rows, but the value sets stay tiny.
    seen: dict[str, dict[object, set]] = defaultdict(lambda: defaultdict(set))

    for i, f in enumerate(files, 1):
        try:
            t = pq.read_table(os.path.join(STORE, f),
                              columns=["series_key", "obs_date", "value"])
        except Exception:                                             # noqa: BLE001
            continue
        for k, d, v in zip(t.column("series_key").to_pylist(),
                           t.column("obs_date").to_pylist(),
                           t.column("value").to_pylist()):
            seen[k][d].add(v)
        if i % 100 == 0:
            print(f"  ...{i}/{len(files)} files, {len(seen):,} series so far", flush=True)

    total = len(seen)
    contradictory = sum(1 for dates in seen.values() if any(len(v) > 1 for v in dates.values()))
    worst_key, worst_n = None, 0
    for k, dates in seen.items():
        n = max((len(v) for v in dates.values()), default=0)
        if n > worst_n:
            worst_key, worst_n = k, n

    print()
    print(f"series keys in the local store          {total:>8,}")
    print(f"  carrying two or more DISTINCT values")
    print(f"  on one date - a contradiction         {contradictory:>8,}   "
          f"{100 * contradictory / max(total, 1):.1f}%")
    print(f"  clean                                 {total - contradictory:>8,}")
    print()
    if worst_key:
        print(f"worst single series: {worst_n} different values on one date")
        print(f"   {worst_key[:100]}")
    print()
    print("NOTE: this counts the LOCAL store. The catalogue advertises 18,838 idb series; the")
    print("store and the catalogue are not the same population and must not be mixed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
