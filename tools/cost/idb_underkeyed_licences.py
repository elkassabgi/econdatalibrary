"""Which LICENCE do the under-keyed idb files actually sit under?

DATABASE_LICENSES_VERBATIM.md says the under-keying repair "does not touch any ND dataset", and
that sentence is the reason a re-key could proceed without reopening NoDerivatives on the 16
cc-by-nc-nd datasets. The previous measurement showed under-keyed files in 17 packages, not one.
This attaches each package's licence, from the 2026-08-28 per-dataset pass, so the question stops
being "how many packages" and becomes the one that matters: how much of the repair lands on ND.

Local and free. Uses the recorded pass, not a fresh crawl.
"""
import json
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

import pyarrow.compute as pc                                          # noqa: E402
import pyarrow.parquet as pq                                          # noqa: E402

from core import r2_util                                             # noqa: E402

STORE = os.path.join(r2_util.ROOT, "data", "clean_full", "idb")
PASS = r"D:\temp\claude\idb_licence_pass.json"


def licences() -> dict:
    raw = json.load(open(PASS, encoding="utf-8"))
    out = {}
    def walk(o):
        if isinstance(o, dict):
            slug = o.get("slug") or o.get("name") or o.get("package")
            lic = o.get("license_id") or o.get("licence") or o.get("license")
            if isinstance(slug, str) and isinstance(lic, (str, type(None))):
                out[slug] = lic
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(raw)
    return out


def main() -> int:
    lic = licences()
    print(f"licence pass covers {len(lic)} datasets\n")

    per = defaultdict(lambda: [0, 0, 0])          # pkg -> [files, underkeyed files, extra rows]
    for f in sorted(os.listdir(STORE)):
        if not f.endswith(".parquet"):
            continue
        pkg = f.rsplit("__", 1)[0] if "__" in f else f[:-8]
        per[pkg][0] += 1
        try:
            t = pq.read_table(os.path.join(STORE, f), columns=["series_key", "obs_date"])
        except Exception:                                             # noqa: BLE001
            continue
        if not t.num_rows:
            continue
        combo = pc.binary_join_element_wise(
            t.column("series_key").cast("string"),
            pc.cast(t.column("obs_date"), "string"), "|")
        d = len(pc.unique(combo))
        if d < t.num_rows:
            per[pkg][1] += 1
            per[pkg][2] += t.num_rows - d

    rows = [(p, v, lic.get(p, "NOT IN PASS")) for p, v in per.items() if v[1]]
    rows.sort(key=lambda r: -r[1][2])

    print(f"{'licence':<14}{'files':>6}{'u-keyed':>8}{'extra rows':>12}  package")
    tot = defaultdict(lambda: [0, 0])
    for pkg, (nf, nu, extra), L in rows:
        print(f"{str(L):<14}{nf:>6}{nu:>8}{extra:>12,}  {pkg[:52]}")
        tot[str(L)][0] += nu
        tot[str(L)][1] += extra

    print()
    print(f"{'licence':<14}{'u-keyed files':>15}{'extra rows':>14}")
    for L, (nu, extra) in sorted(tot.items(), key=lambda kv: -kv[1][1]):
        print(f"{L:<14}{nu:>15,}{extra:>14,}")

    nd = tot.get("cc-by-nc-nd", [0, 0])
    print()
    print("THE CLAIM: 'the under-keying repair ... does not touch any ND dataset'")
    if nd[0]:
        allx = sum(v[1] for v in tot.values())
        print(f"  FALSE: {nd[0]} under-keyed files sit in cc-by-nc-nd packages, carrying")
        print(f"  {nd[1]:,} stacked rows - {100 * nd[1] / max(allx, 1):.1f}% of the total.")
        print("  Small in share, but NoDerivatives is a yes/no question, not a proportion:")
        print("  repairing those files is making a derivative of ND-licensed data.")
    else:
        print("  holds: no under-keyed file sits in a cc-by-nc-nd package")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
