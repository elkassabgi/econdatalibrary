"""Peak RSS of the REAL merge path on a source's largest file - can the cloud run it?

WHY THIS REPLACES THE EARLIER ESTIMATE. tools/audit_cloud_capacity.py multiplied row counts
by an assumed 70 bytes/row and called anything over ~2 GB cloud-infeasible. That was built
on the belief that the crashes were memory exhaustion. They were not: _dedup's group_by was
overflowing Arrow's 2 GiB int32 string offsets and killing the process outright, which is now
fixed (see updater/merge.py). So the estimate needs replacing with a MEASUREMENT of what the
fixed code actually costs.

Each file is processed in its own child so a death kills only that child, and RSS is sampled
by the parent - a process that aborts cannot report its own peak.

KNOWN UNDERSTATEMENT, MEASURED 2026-07-31 - READ THIS BEFORE TRUSTING A VERDICT.
This runs _dedup + _sort on the EXISTING file only. A real update also downloads and parses
the NEW data and merges existing+new, so the true peak is on the COMBINED table and is
materially higher. Acting on this tool's numbers alone, bls (11.1 GB here) and cepii_gravity
(11.3 GB here) were routed to a 16 GB runner and BOTH had their runners destroyed in
production, at 15.8 and 15.5 GB. insee_sirene (11.0 GB here) survived, so the gap is not a
constant that can be corrected with a multiplier.

Treat a result here as a LOWER BOUND. A source measuring within ~4 GB of the runner budget
must be proven by an actual isolated run before being called cloud-capable.

Usage:
    python tools/measure_merge_peak.py                 # every run_location: local source
    python tools/measure_merge_peak.py --source bis
    python tools/measure_merge_peak.py --runner-gb 16  # headroom verdict against this size
"""
from __future__ import annotations
import argparse
import io
import os
import subprocess
import sys
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

CHILD = r'''
import sys, os
sys.path.insert(0, r"{root}")
import pyarrow.parquet as pq
from updater import merge

path = r"{path}"
t = pq.read_table(path)
print("rows=%d" % t.num_rows, flush=True)
# exactly what merge_and_write does to an existing table: concat with itself is not needed -
# the dominant cost is dedup + sort over the existing rows, which is what crashed.
d = merge._dedup(t, ("series_key", "obs_date"))
s = merge._sort(d, ("series_key", "obs_date"))
print("out_rows=%d" % s.num_rows, flush=True)
'''


def peak_rss_of(cmd) -> tuple[int, float, str]:
    """Run cmd, sampling the child's RSS. Returns (returncode, peak_mb, stdout)."""
    try:
        import psutil
    except Exception:                                        # noqa: BLE001
        p = subprocess.run(cmd, capture_output=True, text=True)
        return p.returncode, -1.0, (p.stdout or "")

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    ps = psutil.Process(proc.pid)
    peak = 0.0
    stop = threading.Event()

    def sample():
        nonlocal peak
        while not stop.is_set():
            try:
                peak = max(peak, ps.memory_info().rss / 1e6)
            except Exception:                                # noqa: BLE001
                return
            time.sleep(0.25)

    th = threading.Thread(target=sample, daemon=True)
    th.start()
    out, _err = proc.communicate()
    stop.set()
    th.join(timeout=1)
    return proc.returncode, peak, (out or "")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", action="append")
    ap.add_argument("--runner-gb", type=float, default=16.0)
    ap.add_argument("--baseline-gb", type=float, default=1.5,
                    help="RSS the runner already holds before the merge starts")
    args = ap.parse_args()

    import yaml
    reg = yaml.safe_load(io.open(os.path.join(ROOT, "updater", "registry.yaml"),
                                 encoding="utf-8"))
    local = [e["source_id"] for e in reg["sources"] if e.get("run_location") == "local"]
    targets = args.source or local
    store = os.path.join(ROOT, "data", "clean_full")

    import pyarrow.parquet as pq
    budget = (args.runner_gb - args.baseline_gb) * 1000.0

    print(f"runner {args.runner_gb:.0f} GB minus {args.baseline_gb:.1f} GB baseline "
          f"= {budget:,.0f} MB available for one merge\n")
    print(f"{'source':16s} {'largest file rows':>18s} {'peak MB':>10s} {'rc':>6s}  verdict")
    rows_out = []
    for src in targets:
        d = os.path.join(store, src)
        if not os.path.isdir(d):
            print(f"{src:16s} {'(no local store)':>18s}")
            continue
        biggest, name = 0, None
        for f in os.listdir(d):
            if f.endswith(".parquet"):
                try:
                    r = pq.read_metadata(os.path.join(d, f)).num_rows
                except Exception:                            # noqa: BLE001
                    continue
                if r > biggest:
                    biggest, name = r, f
        if not name:
            continue
        code = CHILD.format(root=ROOT.replace("\\", "/"),
                            path=os.path.join(d, name).replace("\\", "/"))
        rc, peak, out = peak_rss_of([sys.executable, "-u", "-c", code])
        if rc != 0:
            verdict = "DIED - local only"
        elif peak > budget:
            verdict = f"LOCAL - needs {peak/1000:.1f} GB"
        else:
            verdict = f"CLOUD OK - {(budget-peak)/1000:.1f} GB spare"
        print(f"{src:16s} {biggest:18,d} {peak:10,.0f} {rc:>6}  {verdict}")
        rows_out.append((src, biggest, peak, rc, verdict))

    ok = [r for r in rows_out if r[3] == 0 and r[2] <= budget]
    print(f"\nCAN RETURN TO THE CLOUD: {len(ok)} of {len(rows_out)} measured")
    for src, _b, peak, _rc, _v in ok:
        print(f"    {src} (peak {peak/1000:.1f} GB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
