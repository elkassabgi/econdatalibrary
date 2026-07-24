"""Surgically remove bfs's corrupt far-future tail (obs_date year > 2075).

Analysis (2026-07-24): bfs.parquet holds ~101k legitimate Swiss population-projection rows to
2055/2075 — KEEP those — plus a sparse 1-row/year artifact tail in exactly ONE table,
`px-x-0102020300_102`, stretching to 2150. The single clean, conservative cut is year > 2075
(2075 is the furthest real Swiss scenario horizon), which removes only the artifact rows and
preserves every legit projection.

This is a deliberate data-op OUTSIDE the never-shrink merge path (it intentionally shrinks).
SAFETY:
  * refuses to run while a bfs updater dispatch is in flight (it reads/merges this same file);
    pass --force only after confirming no run is active.
  * asserts the removed rows are ALL year>2075 and the survivors are ALL year<=2075.
  * backs the current R2 object up to a dated .bak key before overwriting.
"""
import io, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core import r2_util
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

BUCKET = "econ-data"
KEY = "clean_full/bfs/bfs.parquet"
CUT_YEAR = 2075                    # keep <= this; the furthest real Swiss projection horizon
STAMP = "manual"
for a in sys.argv[1:]:
    if a != "--force":
        STAMP = a

c = r2_util.client(write=True)

raw = c.get_object(Bucket=BUCKET, Key=KEY)["Body"].read()
t = pq.read_table(io.BytesIO(raw))
print(f"  bfs.parquet: {t.num_rows:,} rows, cols={t.column_names}")

od = t.column("obs_date")
# year as int for the mask (obs_date is date32)
yrs = pc.year(od)
keep_mask = pc.less_equal(yrs, pa.scalar(CUT_YEAR))
drop_mask = pc.greater(yrs, pa.scalar(CUT_YEAR))
n_drop = pc.sum(pc.cast(drop_mask, pa.int64())).as_py()
print(f"  rows with year > {CUT_YEAR} (to remove): {n_drop:,}")

if n_drop == 0:
    print("  nothing to trim (already clean). Exiting.")
    sys.exit(0)

# what tables do the dropped rows belong to? (should be only the known-corrupt one)
dropped = t.filter(drop_mask)
tbls = set()
for sk in dropped.column("series_key").to_pylist():
    tbls.add(str(sk).split(":")[1] if ":" in str(sk) else "?")
print(f"  dropped rows span tables: {sorted(tbls)}")
maxyr_dropped = pc.max(pc.year(dropped.column("obs_date"))).as_py()
print(f"  max year among dropped: {maxyr_dropped}")

kept = t.filter(keep_mask)
maxyr_kept = pc.max(pc.year(kept.column("obs_date"))).as_py()
assert kept.num_rows == t.num_rows - n_drop, "row math mismatch"
assert maxyr_kept <= CUT_YEAR, f"survivor still > {CUT_YEAR}!"
print(f"  survivors: {kept.num_rows:,} rows, max year now {maxyr_kept} (was {pc.max(yrs).as_py()})")

# backup current R2 object, then overwrite with the trimmed table
bak = f"{KEY}.bak-{STAMP}"
c.put_object(Bucket=BUCKET, Key=bak, Body=raw)
print(f"  backed up original -> {bak} ({len(raw):,} B)")

buf = io.BytesIO()
pq.write_table(kept, buf, compression="zstd")
c.put_object(Bucket=BUCKET, Key=KEY, Body=buf.getvalue(), ContentType="application/octet-stream")
print(f"  uploaded trimmed bfs.parquet ({buf.tell():,} B)")

# verify re-download
back = pq.read_table(io.BytesIO(c.get_object(Bucket=BUCKET, Key=KEY)["Body"].read()))
mx = pc.max(pc.year(back.column("obs_date"))).as_py()
print(f"\n  verify: R2 bfs.parquet now {back.num_rows:,} rows, max year {mx}  "
      f"{'OK' if mx <= CUT_YEAR else '*** STILL CORRUPT'}")
print(f"  rollback if needed: copy {bak} back over {KEY}")
