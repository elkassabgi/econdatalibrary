import pyarrow.parquet as pq
import os, glob

root = r'D:/research/econfindatalibrary/data/clean_full'

results = {}
errors = []

for entry in sorted(os.scandir(root), key=lambda e: e.name.lower()):
    if not entry.is_dir():
        continue
    src = entry.name
    n = 0
    # Walk recursively (BEA has subdirs, etc.)
    for dirpath, dirnames, filenames in os.walk(entry.path):
        for fname in filenames:
            if fname.endswith('.parquet') and 'checkpoint' not in fname.lower() and 'ckpt' not in fname.lower():
                fpath = os.path.join(dirpath, fname)
                try:
                    n += pq.read_metadata(fpath).num_rows
                except Exception as e:
                    errors.append(f'{src}/{fname}: {e}')
    if n > 0:
        results[src] = n

grand_total = sum(results.values())
print(f'GRAND TOTAL: {grand_total:,} observations across {len(results)} sources')
print()
print(f'{"Source":<40} {"Obs":>20}')
print('-'*62)
for src, n in sorted(results.items(), key=lambda x: -x[1]):
    print(f'{src:<40} {n:>20,}')

if errors:
    print(f'\nErrors ({len(errors)}):')
    for e in errors[:10]:
        print(f'  {e}')