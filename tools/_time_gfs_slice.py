"""Time ONE decade slice of the largest GFS flow, to size the resume-based schedule.

With _pull_sliced now resumable, a flow no longer has to complete inside the 45-minute
unit deadline — each SLICE does, and successive runs converge. So the number that
decides whether imf_gfs* can be scheduled is per-slice wall time, not whole-flow time.
The registry's 482 s/observation figure is an older measurement of a different thing.
"""
import os, sys, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from jobs import ingest_imf_direct as ing

FLOW, AGENCY, SRC = "GFS_SOO", "IMF.STA", "imf_gfssoo_direct"
WINDOW = ("2010", "2019")          # a dense decade, not a sparse early one
part = os.path.join(os.environ.get("TMPDIR", "."), "_gfs_slice_probe.parquet")
url = f"{ing.BASE}/data/{AGENCY},{FLOW}/all?startPeriod={WINDOW[0]}&endPeriod={WINDOW[1]}"
print(f"GET {url}", flush=True)
t0 = time.time()
try:
    n = ing._pull_streamed(url, FLOW, AGENCY, SRC, part, 0)
    dt = time.time() - t0
    size = os.path.getsize(part) if os.path.exists(part) else 0
    print(f"\nRESULT slice {WINDOW[0]}-{WINDOW[1]}: {n:,} rows in {dt:,.0f} s "
          f"({dt/60:.1f} min), parquet {size:,} B")
    print(f"10 windows at this rate -> {10*dt/60:.1f} min total; "
          f"slice fits 45-min deadline: {dt < 45*60}")
finally:
    for p in (part, part + ".sdmx.tmp"):
        if os.path.exists(p):
            try: os.remove(p)
            except OSError: pass
