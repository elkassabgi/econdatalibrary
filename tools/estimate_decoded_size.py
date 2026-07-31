"""Decoded size of a parquet's biggest file, read from the FOOTER - no data loaded.

WHY NOT JUST RUN THE MERGE. For most sources we do (tools/measure_merge_peak.py). But the
five largest - oecd 1,792,000,000 rows in one file, statcan 962,150,400, gus_dbw 358,524,120,
noaa 262,514,152, cepii_baci 242,914,764 - are exactly the ones where loading to find out
could thrash the machine. So read what parquet already records: total_uncompressed_byte_size
per column, summed over row groups. That is the on-the-wire decoded size of the column data,
which is the dominant term in what Arrow will hold.

WHY THE EARLIER ESTIMATE WAS NOT GOOD ENOUGH. It multiplied rows by an assumed 70 B/row, and
row count turned out to predict almost nothing: bis needs 43.8 GB for 36,379,671 rows while
bls needs 11.1 GB for 66,161,839, because bis's series_key column alone holds 13.2 GB of
text. Bytes are the thing to measure, not rows.

The multiplier from decoded bytes to PEAK RSS is calibrated against the sources actually
measured, and printed, so the extrapolation is visible rather than buried.
"""
from __future__ import annotations
import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

# Measured with tools/measure_merge_peak.py on this machine (peak RSS of _dedup + _sort).
MEASURED_PEAK_GB = {
    "abs": 4.6, "bls": 11.1, "insee_sirene": 11.0, "cepii_gravity": 11.3, "vdem": 12.8,
    "wid": 17.3, "faostat": 20.7, "istat": 21.6, "eia": 26.7, "cbs_nl": 39.7, "bis": 43.8,
}


def biggest_file(d):
    import pyarrow.parquet as pq
    best = (0, None, None)
    for f in os.listdir(d):
        if not f.endswith(".parquet"):
            continue
        p = os.path.join(d, f)
        try:
            md = pq.read_metadata(p)
        except Exception:                                    # noqa: BLE001
            continue
        if md.num_rows > best[0]:
            best = (md.num_rows, p, md)
    return best


def decoded_gb(md) -> tuple[float, dict]:
    """Sum total_uncompressed_byte_size across every column chunk in every row group."""
    total = 0
    per_col = {}
    for rg in range(md.num_row_groups):
        g = md.row_group(rg)
        for c in range(g.num_columns):
            col = g.column(c)
            name = col.path_in_schema
            b = col.total_uncompressed_size
            total += b
            per_col[name] = per_col.get(name, 0) + b
    return total / 1e9, {k: v / 1e9 for k, v in per_col.items()}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", action="append", required=True)
    ap.add_argument("--machine-gb", type=float, default=382.0)
    args = ap.parse_args()

    store = os.path.join(ROOT, "data", "clean_full")

    # calibrate: measured peak RSS / decoded footer bytes, over the sources we DID measure
    ratios = []
    for src, peak in MEASURED_PEAK_GB.items():
        d = os.path.join(store, src)
        if not os.path.isdir(d):
            continue
        rows, path, md = biggest_file(d)
        if not md:
            continue
        gb, _ = decoded_gb(md)
        if gb > 0:
            ratios.append((src, peak / gb, gb, peak))
    if ratios:
        vals = sorted(r[1] for r in ratios)
        lo, hi = vals[0], vals[-1]
        mid = vals[len(vals) // 2]
        print("CALIBRATION - measured peak RSS divided by decoded footer bytes:")
        for src, r, gb, peak in sorted(ratios, key=lambda x: -x[1]):
            print(f"    {src:15s} decoded {gb:7.1f} GB -> peak {peak:5.1f} GB   ratio {r:4.1f}x")
        print(f"  ratio range {lo:.1f}x - {hi:.1f}x, median {mid:.1f}x\n")
    else:
        lo = mid = hi = float("nan")

    print(f"{'source':14s} {'rows':>16s} {'decoded GB':>12s}   projected peak (median / worst)")
    for src in args.source:
        d = os.path.join(store, src)
        if not os.path.isdir(d):
            print(f"{src:14s} (no local store)")
            continue
        rows, path, md = biggest_file(d)
        if not md:
            print(f"{src:14s} (no parquet)")
            continue
        gb, per_col = decoded_gb(md)
        proj_mid, proj_hi = gb * mid, gb * hi
        verdict = ("fits this machine" if proj_hi < args.machine_gb
                   else "OVER even at the median" if proj_mid > args.machine_gb
                   else "borderline - depends on the ratio")
        print(f"{src:14s} {rows:16,d} {gb:12.1f}   {proj_mid:6.0f} GB / {proj_hi:6.0f} GB  {verdict}")
        big = sorted(per_col.items(), key=lambda kv: -kv[1])[:3]
        print("               columns: " + ", ".join(f"{k}={v:.1f}GB" for k, v in big))
    print(f"\nmachine: {args.machine_gb:.0f} GB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
