"""Rebuild store rows for cso matrices CSO has RETIRED, from the CSVs we already serve.

THE SITUATION IT FIXES (2026-08-07). 27 of cso's catalogued matrices are gone from the
publisher's ReadCollection entirely, so no fetch can ever restore them. Their CSVs are on
R2 with real data (58,982 rows), but the parquet store had no rows for them, so
`/v1/series/<id>.csv` answered while the parquet download did not — one product giving two
answers for the same id.

Deleting them was not an option: the data is NOT re-crawlable, and "delete data that cannot
be re-crawled" is Ahmed's decision, not a cleanup. Reconstructing the store rows from the
CSVs we ALREADY publish is non-destructive, invents nothing, and makes the two paths agree.

Written as a normal subject-style parquet (`999_Retired_Upstream.parquet`) so the resolver's
plain `*.parquet` glob and the flow-grain prefix rule pick it up with no special-casing, and
so the cso fetcher — which only ever writes the subject files of matrices it pulled — cannot
overwrite it. These matrices are never pulled again by construction.

Every row is checked against its own id before it is written: a CSV whose keys do not start
with `CSO:<matrix>` aborts rather than quietly filing rows under the wrong matrix.

  python tools/rebuild_cso_retired_from_csv.py            # build locally, report
  python tools/rebuild_cso_retired_from_csv.py --upload   # also PUT to R2
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import os
import sys
import urllib.parse

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

# Measured 2026-08-07 as catalogued-but-absent from PxStat.Data.Cube_API.ReadCollection.
# Hardcoded deliberately: this is a historical fact about one moment, not a live query —
# re-deriving it later against a changed collection would silently change what gets rebuilt.
RETIRED = [
    "A0207", "A0208", "A0209", "B0207", "B0208", "B0209", "B0212", "C0424", "C0427",
    "C0429", "C0438", "CD820", "E1004", "E1018", "E1033", "E1036", "E1037", "E1038",
    "E1039", "E1042", "E1043", "E7043", "NAA02", "NAA03", "NAA04", "NQQ34", "NQQ38",
]
OUT_NAME = "999_Retired_Upstream.parquet"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--upload", action="store_true")
    a = ap.parse_args()

    import pyarrow as pa
    import pyarrow.parquet as pq
    from core import r2_util

    c = r2_util.client()
    keys, dates, vals = [], [], []
    for m in RETIRED:
        k = "series/" + urllib.parse.quote(f"cso:CSO:{m}", safe="") + ".csv"
        body = c.get_object(Bucket="econ-data", Key=k)["Body"].read().decode("utf-8")
        rdr = csv.reader(io.StringIO(body))
        hdr = next(rdr)
        if hdr != ["series_id", "obs_date", "value"]:
            raise SystemExit(f"{m}: unexpected CSV header {hdr!r}")
        n0 = len(keys)
        for row in rdr:
            if len(row) != 3 or not row[0]:
                continue
            if not (row[0].startswith(f"CSO:{m}:") or row[0] == f"CSO:{m}"):
                raise SystemExit(f"{m}: CSV holds a key for another matrix: {row[0]!r}")
            try:
                v = float(row[2])
            except ValueError:
                continue                      # publisher blank/suppressed cell
            keys.append(row[0])
            dates.append(dt.date.fromisoformat(row[1]))
            vals.append(v)
        print(f"  {m}: {len(keys) - n0:,} rows")

    t = pa.table({"series_key": pa.array(keys, pa.string()),
                  "obs_date": pa.array(dates, pa.date32()),
                  "value": pa.array(vals, pa.float64())})
    out = os.path.join(ROOT, "data", "clean_full", "cso", OUT_NAME)
    pq.write_table(t, out, compression="zstd")
    n_mat = len({k.split(":")[1] for k in keys})
    print(f"\nwrote {out}: {t.num_rows:,} rows / {len(set(keys)):,} keys / {n_mat} matrices")
    if n_mat != len(RETIRED):
        raise SystemExit(f"expected {len(RETIRED)} matrices, got {n_mat} — refusing to "
                         f"upload a partial rebuild")

    if a.upload:
        key = f"clean_full/cso/{OUT_NAME}"
        r2_util.client(write=True).upload_file(out, "econ-data", key)
        got = c.head_object(Bucket="econ-data", Key=key)["ContentLength"]
        want = os.path.getsize(out)
        print(f"uploaded {key}: {got:,} B (local {want:,} B) "
              f"-> {'MATCH' if got == want else 'MISMATCH'}")
        return 0 if got == want else 1
    print("(local only — pass --upload to PUT it to R2)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
