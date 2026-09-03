"""The idb serving surface as it stands TODAY, before any re-pull touches it.

WHY BEFORE. Ahmed authorised re-pulling idb with a wider series key on 2026-09-03. That renames
every catalogued id, so the migration needs a baseline it can be checked against afterwards -
and two earlier idb remedies were refuted under review precisely for reasoning from numbers
nobody had taken (R501 authorised on n=1, R527 on a miscounted collision count).

CHEAP BY CONSTRUCTION. `source_counts` is a one-row lookup, not a scan. The R2 side is a
prefix LIST anchored on the ENCODED COLON - `series/idb%3A` - because `series/idb` would also
match any source whose id starts with "idb" (R129, R462). The local store is read from disk.

Nothing is written.
"""
import os
import sqlite3
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, r"E:\research\econfindatalibrary")

from core import derive_csv as dc  # noqa: E402
from core import r2_util  # noqa: E402

BUCKET = "econ-data"
PREFIX = "series/idb%3A"


def main():
    print("=== LOCAL CATALOGUE ===")
    con = sqlite3.connect("file:%s?mode=ro" % dc.CATALOG, uri=True)
    n_cat = con.execute("SELECT COUNT(*) FROM series WHERE source_id='idb'").fetchone()[0]
    print(f"catalogued idb series (local catalog.db): {n_cat:,}")
    shapes = Counter()
    for (sid,) in con.execute("SELECT series_id FROM series WHERE source_id='idb'"):
        shapes[sid.count(":")] += 1
    print("colons per id (the key's depth):")
    for k in sorted(shapes):
        print(f"   {k} colons: {shapes[k]:,}")

    print("\n=== LOCAL STORE ===")
    store = os.path.join(r2_util.ROOT, "data", "clean_full", "idb")
    if os.path.isdir(store):
        files = [f for f in os.listdir(store) if f.endswith(".parquet")]
        total = sum(os.path.getsize(os.path.join(store, f)) for f in files)
        print(f"{len(files):,} parquet files, {total/1e9:.2f} GB")
        try:
            import pyarrow.parquet as pq
            sch = pq.read_schema(os.path.join(store, files[0]))
            print(f"schema of {files[0]}: {sch.names}")
        except Exception as exc:                                      # noqa: BLE001
            print("schema read failed:", type(exc).__name__, exc)
    else:
        print("no local store directory")

    print("\n=== SERVED OBJECTS (R2) ===")
    client = r2_util.client(write=False)
    n_obj, total_bytes, tok, calls = 0, 0, None, 0
    while True:
        kw = {"Bucket": BUCKET, "Prefix": PREFIX, "MaxKeys": 1000}
        if tok:
            kw["ContinuationToken"] = tok
        r = client.list_objects_v2(**kw)
        calls += 1
        for o in r.get("Contents", []):
            n_obj += 1
            total_bytes += o["Size"]
        if not r.get("IsTruncated"):
            break
        tok = r["NextContinuationToken"]
    print(f"{n_obj:,} objects under {PREFIX!r} in {calls} calls, {total_bytes/1e6:.1f} MB")

    print("\n=== THE GAP THAT MATTERS FOR A RENAME ===")
    print(f"catalogued {n_cat:,} vs served {n_obj:,}  -> difference {n_cat - n_obj:,}")
    print("Every one of those objects is keyed by the id the re-pull would CHANGE. A rename")
    print("that publishes new ids without retiring the old ones leaves both live, and the")
    print("catalogue would advertise one set while the bucket holds two.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
