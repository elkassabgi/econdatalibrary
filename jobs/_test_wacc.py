import requests, sys, importlib
sys.path.insert(0, "D:/research/econfindatalibrary/jobs")
import ingest_damodaran as d
importlib.reload(d)

UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
BASE = "https://pages.stern.nyu.edu/~adamodar/pc/datasets"

r = requests.get(f"{BASE}/wacc.xls", headers=UA, timeout=30)
data = r.content
print(f"Downloaded {len(data)} bytes")

# Use parse_dataset which is the actual entry point
k, ds, v = d.parse_dataset(data, "costequity", f"{BASE}/wacc.xls", None)
print(f"\nResult: {len(v)} obs")
if len(v) > 0:
    for i in range(min(5, len(k))):
        print(f"  {k[i]} | {ds[i]} | {v[i]:.4f}")
else:
    # Debug: show what _parse_rows produces for sheet
    rows = d._get_rows_xls(data, "Industry Averages")
    print(f"Total rows: {len(rows)}")
    # Find header
    header_idx = -1
    for ri, row in enumerate(rows[:35]):
        if d._is_metadata_row(row):
            continue
        non_null = [c for c in (row or []) if c is not None]
        if not non_null or not isinstance(non_null[0], str) or not non_null[0].strip():
            print(f"  R{ri}: SKIP (non-string first cell: {repr(non_null[0])[:25] if non_null else 'empty'})")
            continue
        if len(non_null) >= 3:
            header_idx = ri
            print(f"  R{ri}: HEADER FOUND ({non_null[0]}), non_null={len(non_null)}")
            break
        print(f"  R{ri}: too few non-null ({len(non_null)}), first={repr(non_null[0])[:25]}")
