"""Verify the ingest: re-read EVERY Parquet under data/clean_full/treasury/ and report
actual row counts vs source-published totals. This is the honesty check."""
import glob, json, os
import pyarrow.parquet as pq

# Derived from this file, never a drive letter (R330). This script calls itself "the honesty
# check", and a stale root makes it glob an absent directory: 0 files, no error, and a report
# that reads as a clean verification of nothing.
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(_ROOT, "data", "clean_full", "treasury")
if not os.path.isdir(OUT):
    raise SystemExit(f"_treasury_verify_written: {OUT!r} does not exist — refusing to report "
                     f"a verification over zero files.")
catalog = json.load(open(os.path.join(_ROOT, "data", "_treasury_catalog_final.json")))

files = sorted(glob.glob(os.path.join(OUT, "*.parquet")))
print(f"Parquet files written: {len(files)}")

# map file -> endpoint via manifest
ep_of_file = {}
mpath = os.path.join(OUT, "_manifest.jsonl")
if os.path.exists(mpath):
    for line in open(mpath, encoding="utf-8"):
        try:
            r = json.loads(line)
            ep_of_file[r["file"]] = r["endpoint"]
        except Exception:
            pass

total_written = 0
total_size = 0
per = []
for f in files:
    md = pq.read_metadata(f)
    rows = md.num_rows
    size = os.path.getsize(f)
    total_written += rows
    total_size += size
    fn = os.path.basename(f)
    ep = ep_of_file.get(fn, "?")
    expected = (catalog.get(ep, {}) or {}).get("total")
    per.append((fn, ep, rows, expected, size))

# known source-published total (sum of catalog 'total' where known)
known_pub = sum((v.get("total") or 0) for v in catalog.values())

print(f"\nTOTAL rows written (re-read from Parquet): {total_written:,}")
print(f"TOTAL parquet size: {total_size/1e9:.3f} GB  ({total_size/1e6:.1f} MB)")
print(f"avg rows/file: {total_written//max(len(files),1):,}   avg MB/file: {total_size/1e6/max(len(files),1):.2f}")
print(f"sum of KNOWN source-published totals (catalog): {known_pub:,}")

# coverage on endpoints with a known expected count
covered = miss = 0
shortfalls = []
for fn, ep, rows, expected, size in per:
    if expected:
        if rows >= expected:
            covered += expected
        else:
            covered += rows
            shortfalls.append((ep, rows, expected))
print(f"\nendpoints with known expected: coverage where comparable")
if shortfalls:
    print("SHORTFALLS (rows < expected):")
    for ep, rows, expected in shortfalls:
        print(f"   {ep:60} {rows:>12,} / {expected:>12,}  ({100.0*rows/expected:.1f}%)")
else:
    print("  none -- every endpoint with a known count met or exceeded it.")

# endpoints that were runtime-counted (catalog total None): show what they wrote
print("\nruntime-counted endpoints (no pre-count; wrote N rows):")
for fn, ep, rows, expected, size in per:
    if not expected:
        print(f"   {ep:60} wrote {rows:>12,}")

# zero-row files (possible problems)
zero = [(fn, ep) for fn, ep, rows, expected, size in per if rows == 0]
if zero:
    print(f"\nZERO-ROW files ({len(zero)}):")
    for fn, ep in zero:
        print("   ", ep, fn)

# largest files
print("\nlargest 8 files by rows:")
for fn, ep, rows, expected, size in sorted(per, key=lambda x: -x[2])[:8]:
    print(f"   {rows:>12,}  {size/1e6:7.1f}MB  {fn}")
