"""Verify the DeFiLlama clean_full output by re-reading every Parquet."""
import glob
import os

import pyarrow.parquet as pq

OUT = r"D:/research/econfindatalibrary/data/clean_full/defillama"

files = sorted(glob.glob(os.path.join(OUT, "*.parquet")))
total_obs = 0
total_series = 0
by_family = {}
print(f"{'file':52} {'rows':>12} {'cols'}")
print("-" * 90)
for f in files:
    md = pq.read_metadata(f)
    rows = md.num_rows
    name = os.path.basename(f)
    # series count: read just the key column
    keycol = "series_key"
    schema_names = [field.name for field in md.schema.to_arrow_schema()]
    nseries = ""
    if keycol in schema_names:
        t = pq.read_table(f, columns=[keycol])
        nseries = len(set(t.column(0).to_pylist()))
    print(f"{name:52} {rows:>12,} {schema_names}")
    if not name.startswith("_catalog") and "snapshot" not in name:
        total_obs += rows
        if isinstance(nseries, int):
            total_series += nseries
    fam = name.split("_")[0] if "_" in name else name.replace(".parquet", "")
    by_family.setdefault(fam, [0, 0])
    by_family[fam][0] += rows
    if isinstance(nseries, int):
        by_family[fam][1] += nseries

print("-" * 90)
print(f"TOTAL files: {len(files)}")
print(f"TOTAL observation rows (excl catalog/snapshot): {total_obs:,}")
print(f"TOTAL distinct series (sum over files): {total_series:,}")
print("\nBy family (rows, series):")
for k in sorted(by_family):
    print(f"  {k:24} rows={by_family[k][0]:>13,} series~={by_family[k][1]:,}")

# disk
sz = sum(os.path.getsize(f) for f in files)
print(f"\nTotal Parquet size on disk: {sz/1e9:.2f} GB ({sz/1e6:.0f} MB)")
