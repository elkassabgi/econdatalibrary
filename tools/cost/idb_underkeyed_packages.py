"""Is the under-keying repair really confined to ONE cc-by dataset?

DATABASE_LICENSES_VERBATIM.md, the file that gates licence decisions, states:

    "MATERIAL FOR THE 2026-08-28 REPAIR: all 379 under-keyed files belong to ONE dataset,
     social-indicators-of-latin-america-and-the-caribbean, whose licence is cc-by
     (CC BY 4.0 - derivatives permitted with attribution). The under-keying repair is therefore
     licence-clear on its own terms and does not touch any ND dataset."

That conclusion is load-bearing: it is the reason a re-key can proceed without reopening the
NoDerivatives question on the 16 cc-by-nc-nd datasets. If under-keyed files also sit in ND
packages, the repair is NOT licence-clear and the sentence is doing real harm.

DEFINITION, stated because "under-keyed" is not self-evident: a file is under-keyed when its rows
outnumber its distinct (series_key, obs_date) pairs - i.e. at least one series-date carries more
than one row. The 97% threshold the reviewer used is a stricter variant; both are reported so the
answer does not depend on which one the original claim meant.

Local and free.
"""
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

import pyarrow.compute as pc                                          # noqa: E402
import pyarrow.parquet as pq                                          # noqa: E402

from core import r2_util                                             # noqa: E402

STORE = os.path.join(r2_util.ROOT, "data", "clean_full", "idb")

# from the 2026-08-28 per-dataset pass recorded in DATABASE_LICENSES_VERBATIM.md
CC_BY = "cc-by"
ND = "cc-by-nc-nd"


def main() -> int:
    files = sorted(f for f in os.listdir(STORE) if f.endswith(".parquet"))
    any_stack: dict[str, int] = defaultdict(int)      # pkg -> files with ANY duplicate pair
    below97: dict[str, int] = defaultdict(int)        # pkg -> files under the 97% threshold
    total: dict[str, int] = defaultdict(int)

    for f in files:
        pkg = f.rsplit("__", 1)[0] if "__" in f else f[:-8]
        total[pkg] += 1
        try:
            t = pq.read_table(os.path.join(STORE, f), columns=["series_key", "obs_date"])
        except Exception:                                             # noqa: BLE001
            continue
        if not t.num_rows:
            continue
        combo = pc.binary_join_element_wise(
            t.column("series_key").cast("string"),
            pc.cast(t.column("obs_date"), "string"), "|")
        distinct = len(pc.unique(combo))
        if distinct < t.num_rows:
            any_stack[pkg] += 1
        if distinct < 0.97 * t.num_rows:
            below97[pkg] += 1

    print(f"{len(files)} files across {len(total)} packages\n")
    for label, d in (("ANY duplicated (series_key, obs_date)", any_stack),
                     ("below the 97% distinct threshold", below97)):
        n_files = sum(d.values())
        print(f"{label}: {n_files} files in {len(d)} package(s)")
        for pkg, n in sorted(d.items(), key=lambda kv: -kv[1]):
            print(f"    {n:>4}  {pkg[:66]}")
        print()

    others = {p for p in below97 if p != "social-indicators-of-latin-america-and-the-caribbean"}
    print("THE CLAIM UNDER TEST: 'all 379 under-keyed files belong to ONE dataset'")
    if others:
        print(f"  FALSE on this measurement - {len(others)} other package(s) also hold "
              f"under-keyed files:")
        for p in sorted(others):
            print(f"     {p}")
        print("  Each of those carries its own licence. The 2026-08-28 pass found 16 of the 21")
        print("  served datasets are cc-by-nc-nd, where NoDerivatives is exactly the unresolved")
        print("  question - so a repair touching them is NOT licence-clear on its own terms.")
    else:
        print("  holds on this measurement: only social-indicators is under-keyed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
