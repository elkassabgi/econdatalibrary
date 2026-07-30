"""Which databases CANNOT be updated on a 16 GB CI runner, and must run on the workstation?

THE OWNER'S STANDING RULE: anything too big for the cloud to process is updated locally, and
EVERY database is to be assessed for it — not just whichever one broke last.

WHY THE LARGEST FILE IS THE MEASURE, NOT THE TOTAL. updater.merge.merge_and_write reads the
whole existing parquet, concatenates the new rows, deduplicates (which allocates an index
column, a group-by hash table and an is_in value set) and sorts — all live at once, one FILE
at a time. So a source of 400 small parquets is safe however many rows it holds, while a
single 262-million-row file is fatal on its own. Row counts come from parquet FOOTERS
(metadata only, no column scan).

CALIBRATION IS OBSERVED, NOT ASSUMED. Four real data points from CI runs on 2026-07-30:

    abs      survived, process peak 12,888 MB   (largest file 3,050,209 rows)
    adb      survived, process peak 12,888 MB
    bis      DESTROYED the runner at 15,806 MB  (largest file 36,379,671 rows, ~2.5 GB)
    bls      DESTROYED the runner at 15,886 MB  (largest file 66,161,839 rows, ~4.6 GB)

So a largest-file of ~2.5 GB decoded has been OBSERVED to kill a 16 GB runner. The threshold
below is set from that observation, deliberately conservative, and the estimate column is
labelled an estimate because the true multiplier varies with dtype mix and batch size.

Usage:  python tools/audit_cloud_capacity.py [--bytes-per-row 70] [--runner-gb 16]
"""
from __future__ import annotations
import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

# Decoded Arrow cost per row for the common (series_key str, obs_date date32, value float64)
# shape. Conservative: wide sources with several string columns cost more.
DEFAULT_BYTES_PER_ROW = 70

# Observed: bis died with a 2.5 GB largest file. Anything at or above this is treated as
# cloud-infeasible rather than merely risky.
LOCAL_REQUIRED_GB = 2.0
WATCH_GB = 1.0


def scan(store_root: str, bpr: int):
    import pyarrow.parquet as pq
    out = []
    for name in sorted(os.listdir(store_root)):
        d = os.path.join(store_root, name)
        if not os.path.isdir(d):
            continue
        biggest = 0
        biggest_name = ""
        total = 0
        files = 0
        for dirpath, _dirs, names in os.walk(d):
            for f in names:
                if not f.endswith(".parquet"):
                    continue
                try:
                    r = pq.read_metadata(os.path.join(dirpath, f)).num_rows
                except Exception:                            # noqa: BLE001
                    continue
                files += 1
                total += r
                if r > biggest:
                    biggest, biggest_name = r, f
        if files:
            out.append({
                "source": name, "files": files, "total_rows": total,
                "largest_rows": biggest, "largest_file": biggest_name,
                "largest_gb": biggest * bpr / 1e9,
            })
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bytes-per-row", type=int, default=DEFAULT_BYTES_PER_ROW)
    ap.add_argument("--runner-gb", type=float, default=16.0)
    ap.add_argument("--store", default=None)
    args = ap.parse_args()

    try:
        from updater import config
        root = args.store or os.path.dirname(config.source_dir("abs"))
    except Exception:                                        # noqa: BLE001
        root = args.store or os.path.join(ROOT, "data", "clean_full")
    if not os.path.isdir(root):
        print(f"store root not found: {root}")
        return 1

    rows = scan(root, args.bytes_per_row)
    rows.sort(key=lambda r: -r["largest_gb"])

    local = [r for r in rows if r["largest_gb"] >= LOCAL_REQUIRED_GB]
    watch = [r for r in rows if WATCH_GB <= r["largest_gb"] < LOCAL_REQUIRED_GB]

    print(f"store: {root}")
    print(f"assessed {len(rows)} source store(s); {args.bytes_per_row} B/row decoded; "
          f"runner {args.runner_gb:.0f} GB\n")

    print(f"{'source':18s} {'largest file rows':>18s} {'~GB':>7s} {'files':>7s} {'total rows':>16s}  verdict")
    for r in rows:
        if r["largest_gb"] < WATCH_GB:
            continue
        if r["largest_gb"] >= args.runner_gb:
            v = "LOCAL — one file exceeds the WHOLE runner"
        elif r["largest_gb"] >= LOCAL_REQUIRED_GB:
            v = "LOCAL — at/above the size observed to kill a runner"
        else:
            v = "watch"
        print(f"{r['source']:18s} {r['largest_rows']:18,d} {r['largest_gb']:7.1f} "
              f"{r['files']:7,d} {r['total_rows']:16,d}  {v}")

    print(f"\n=== MUST RUN LOCALLY: {len(local)} source(s) ===")
    for r in local:
        note = "  (exceeds the entire runner)" if r["largest_gb"] >= args.runner_gb else ""
        print(f"    {r['source']:16s} {r['largest_file']}  {r['largest_rows']:,} rows "
              f"~{r['largest_gb']:.1f} GB{note}")
    print(f"\n=== WATCH ({WATCH_GB:.0f}-{LOCAL_REQUIRED_GB:.0f} GB): {len(watch)} source(s) ===")
    for r in watch:
        print(f"    {r['source']:16s} {r['largest_rows']:,} rows ~{r['largest_gb']:.1f} GB")
    print(f"\n=== CLOUD-OK: {len(rows) - len(local) - len(watch)} source(s) below "
          f"{WATCH_GB:.0f} GB ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
