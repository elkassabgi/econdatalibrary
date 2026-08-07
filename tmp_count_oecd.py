import pyarrow.parquet as pq
import os, glob

root = r'D:/research/econfindatalibrary/data/clean_full'

# Count OECD rows
odir = os.path.join(root, 'oecd')
files = glob.glob(odir + '/*.parquet')
total = 0
for f in files:
    try: total += pq.read_metadata(f).num_rows
    except: pass
print(f"OECD: {len(files)} files, {total:,} rows")