"""WHICH idb datasets hold the contradictory series? Scope, before any fetch.

WHY THIS DECIDES THE PLAN. `DATABASE_LICENSES_VERBATIM.md` marks idb DISPUTED / NEEDS HUMAN
REVIEW. A per-dataset pass on 2026-08-28 read `license_id` live for every one of the 21 datasets
the catalogue actually serves and found 16 cc-by-nc-nd, 5 cc-by, 0 unlicensed - so the
portal-wide "~86% carry NO declared licence" finding that drove the DISPUTED verdict "does not
touch what we serve". It does not touch what we serve BECAUSE WE SERVE 21 OF 1,591 PACKAGES.

A re-pull that also ingests the 1,377 never-fetched resources would walk straight into that 86%.
So the licence-clear job and the expansion job are different jobs, and this measures which
datasets the CORRECTNESS problem actually lives in.

The store is one parquet per resource named `<pkg_slug>__<rid[:8]>.parquet`, so the package is
readable from the filename. Multiplicity = rows / distinct (series_key, obs_date): 1.0 means one
value per series-date, anything above means values stacked under one id.

Local and free. Nothing written, nothing fetched.
"""
import os
import sys
from collections import defaultdict

sys.path.insert(0, r"E:\research\econfindatalibrary")

import pyarrow.compute as pc  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

from core import r2_util  # noqa: E402

STORE = os.path.join(r2_util.ROOT, "data", "clean_full", "idb")


def main():
    files = sorted(f for f in os.listdir(STORE) if f.endswith(".parquet"))
    per = defaultdict(lambda: [0, 0, 0])          # pkg -> [rows, distinct pairs, files]
    for f in files:
        pkg = f.rsplit("__", 1)[0] if "__" in f else f[:-8]
        try:
            t = pq.read_table(os.path.join(STORE, f), columns=["series_key", "obs_date"])
        except Exception:                                             # noqa: BLE001
            continue
        rows = t.num_rows
        if not rows:
            per[pkg][2] += 1
            continue
        # distinct (key, date) without materialising a python set of every row
        combo = pc.binary_join_element_wise(
            t.column("series_key").cast("string"),
            pc.cast(t.column("obs_date"), "string"), "|")
        distinct = len(pc.unique(combo))
        per[pkg][0] += rows
        per[pkg][1] += distinct
        per[pkg][2] += 1

    rank = sorted(per.items(), key=lambda kv: -(kv[1][0] - kv[1][1]))
    tot_rows = sum(v[0] for v in per.values())
    tot_dist = sum(v[1] for v in per.values())

    print(f"{len(per)} datasets, {len(files)} files, {tot_rows:,} rows, "
          f"{tot_dist:,} distinct (series,date) pairs")
    print(f"overall multiplicity {tot_rows/max(tot_dist,1):.1f}x\n")
    print(f"{'dataset':<52}{'files':>6}{'rows':>12}{'distinct':>11}{'mult':>7}")
    for pkg, (rows, dist, nf) in rank[:12]:
        m = rows / max(dist, 1)
        print(f"{pkg[:50]:<52}{nf:>6}{rows:>12,}{dist:>11,}{m:>6.1f}x")

    stacked = [(p, v) for p, v in per.items() if v[0] > v[1]]
    print(f"\ndatasets with ANY stacking: {len(stacked)} of {len(per)}")
    if rank:
        top, (r, d, _) = rank[0]
        print(f"the worst is {top}")
        print(f"  {r:,} rows over {d:,} distinct pairs = {r-d:,} rows sharing an id with another")
        print(f"  that is {100*(r-d)/max(tot_rows-tot_dist,1):.1f}% of all the stacking in idb")
    return 0


if __name__ == "__main__":
    sys.exit(main())
