"""Has any company's STORE lost most of the facts its own catalogue row records?

WHY THIS EXISTS AND WHY footer_diff COULD NOT DO IT. `tools/footer_diff.py` compares the local
mirror against R2. That finds a store regression only while the mirror still holds the old data —
XOM was caught exactly that way, because its mirror happened to be three months stale.

XPRO was not. Its ticker was re-pointed at a new CIK (Expro Group Holdings N.V. 1575828 ->
Expro Ltd 2126198, 19,399 facts -> 4), the refresher replaced the store with the successor's four
facts, and the mirror was overwritten with the same four. Both sides agreed, so footer_diff
reported SAME and every mirror check in the fleet passed. The store had lost 19,395 rows and no
instrument that compares the two copies could ever say so.

What DID say so is the catalogue: `series.metadata.n_obs` records how many observations the last
successful catalogue write saw. That is an independent third witness, written at a different time
by a different code path, and it survives when both copies of the data are clobbered together.
XPRO's row still said 19,399 while the parquet held 4.

So this compares the store against the CATALOGUE, not against the mirror. A source whose store
has fallen below half of its recorded size is either a re-keyed publisher identifier or a genuine
truncation; both need a human, and neither shows up anywhere else.

    python tools/audit_store_vs_catalogued_size.py                # sec_edgar (17,274 companies)
    python tools/audit_store_vs_catalogued_size.py --threshold 0.9
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="sec_edgar")
    ap.add_argument("--root", default="clean_grouped")
    ap.add_argument("--threshold", type=float, default=0.5,
                    help="flag when the store holds less than this fraction of the "
                         "catalogued n_obs (default 0.5)")
    a = ap.parse_args()

    import pyarrow.parquet as pq
    d = os.path.join(ROOT, "data", a.root, a.source)
    con = sqlite3.connect(
        f"file:{os.path.join(ROOT, 'data', 'catalog.db')}?mode=ro", uri=True)
    con.execute("PRAGMA busy_timeout = 180000")
    rows = con.execute("select series_id, metadata from series where source_id=?",
                       (a.source,)).fetchall()

    shrunk, checked, no_file, no_count = [], 0, 0, 0
    for sid, md in rows:
        ident = sid.split(":", 1)[1]
        p = os.path.join(d, ident.replace("/", "_").replace(":", "_") + ".parquet")
        if not os.path.exists(p):
            no_file += 1
            continue
        try:
            meta = json.loads(md or "{}")
        except Exception:                                          # noqa: BLE001
            meta = {}
        recorded = meta.get("n_obs")
        if not recorded:
            no_count += 1
            continue
        checked += 1
        now = pq.read_metadata(p).num_rows
        if now < recorded * a.threshold:
            shrunk.append((ident, recorded, now, meta.get("cik")))

    print(f"{a.source}: {checked:,} compared against their catalogued n_obs "
          f"({no_file:,} with no store file, {no_count:,} with no recorded count)")
    print(f"STORE BELOW {a.threshold:.0%} OF THE CATALOGUED SIZE: {len(shrunk)}")
    for ident, rec, now, cik in sorted(shrunk, key=lambda t: -(t[1] - t[2])):
        print(f"   {ident:14s} catalogued {rec:>9,} -> store {now:>9,}   "
              f"(cik recorded {cik})")
    if shrunk:
        print("\nFor a ticker-keyed store this is usually a re-assigned publisher id: fetch the "
              "RECORDED cik's payload and union it into the file (that is how XPRO went 4 -> "
              "19,403 and XOM 274 -> 20,903). Archive the object first.")
    return 0 if not shrunk else 1


if __name__ == "__main__":
    sys.exit(main())
