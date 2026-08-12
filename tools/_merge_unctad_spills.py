"""Assemble a unctad store parquet DIRECTLY from cached spill files.

WHY this exists (2026-08-12, US.BiotradeMerch): the ingest's resume walk must
re-issue every INTERMEDIATE size-cap probe over the network to rediscover the
split tree — only leaf 200-responses are spilled; the 400-cap responses that
shaped the descent are not. With ~100k such probes and the publisher's CDN
drip-throttling the campaign IP, a resume costs ~2 days to re-read data that
is already complete on disk. This tool skips the tree entirely: the leaf
files ARE the dataset — read them all, build rows with the SAME semantics as
jobs/ingest_unctad_ds.pull_rows (imported, never duplicated), write the same
parquet the ingest would have written.

Discriminating gate: --expect-obs <measure>=<count> refuses to write unless
the assembled per-measure observation count matches EXACTLY. For biotrademerch
the running ingest's own log line is ground truth from the identical files:
  M4023: 1,063,192,830 obs
A tool that writes a store nobody counted is R420's disease; the gate is the
control.

Usage:
  python tools/_merge_unctad_spills.py US.BiotradeMerch \
      --expect-obs M4023=1063192830 [--out data/clean_full/<src>/<src>.parquet]

Memory: rows flush to Arrow chunks every FLUSH_FILES files (~10x smaller than
Python lists); peak well under the in-process ingest's 220+ GB profile.
"""
from __future__ import annotations

import argparse
import csv
import io
import os
import re
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "jobs"))
from ingest_unctad_ds import (  # noqa: E402 — reuse, never duplicate (parity rule)
    SCHEMA, ROOT, parse_time, source_id_for, report_metadata, dataset_layout,
)

FLUSH_FILES = 2000
_MEASURE_COL = re.compile(r"^M(\w+)_Value$")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("ds_name", help="e.g. US.BiotradeMerch")
    ap.add_argument("--expect-obs", action="append", default=[],
                    metavar="MCODE=COUNT",
                    help="refuse to write unless this measure's obs count matches "
                         "exactly (repeatable); e.g. M4023=1063192830")
    ap.add_argument("--out", help="output parquet (default: the ingest's own path)")
    args = ap.parse_args()

    expects = {}
    for spec in args.expect_obs:
        m, _, c = spec.partition("=")
        expects[m if m.startswith("M") else "M" + m] = int(c)

    src = source_id_for(args.ds_name)
    meta = report_metadata(args.ds_name)
    kfields, tfield, is_year, period_axis, measures = dataset_layout(meta)
    if period_axis:
        raise SystemExit("period-coded time axes need parse_period_code handling — "
                         "this tool currently supports plain time axes only")
    print(f"{args.ds_name} -> {src}: key dims {kfields}, time {tfield}, "
          f"measures {['M' + c for c in measures]}, version {meta.get('version')}",
          flush=True)

    spill_dir = os.path.join(ROOT, "data", "_unctad_spill", src)
    files = [f for f in os.listdir(spill_dir)
             if f.endswith(".csv") and not f.endswith(".tmp")]
    print(f"spill files: {len(files):,}", flush=True)

    code_cols = [f"{f}_Code" for f in kfields]
    chunks: list[pa.Table] = []
    rows_k: list[str] = []
    rows_d: list = []
    rows_v: list[float] = []
    per_measure: dict[str, int] = {}
    skipped_rows = 0
    empty_files = 0
    t0 = time.time()

    def _flush():
        nonlocal rows_k, rows_d, rows_v
        if rows_k:
            chunks.append(pa.table({"series_key": pa.array(rows_k, pa.string()),
                                    "obs_date": pa.array(rows_d, pa.date32()),
                                    "value": pa.array(rows_v, pa.float64())},
                                   schema=SCHEMA))
            rows_k, rows_d, rows_v = [], [], []

    for i, fn in enumerate(sorted(files)):
        with open(os.path.join(spill_dir, fn), encoding="utf-8") as fh:
            text = fh.read()
        if not text.strip() or text.strip() == "0":
            empty_files += 1
            continue
        rdr = csv.DictReader(io.StringIO(text))
        mcol = next((c for c in (rdr.fieldnames or []) if _MEASURE_COL.match(c)), None)
        if mcol is None:
            empty_files += 1
            continue
        mcode = mcol[:-len("_Value")]           # 'M4023_Value' -> 'M4023'
        for rec in rdr:
            vals = [rec.get(c, "") for c in code_cols]
            tv = rec.get(tfield) or rec.get(f"{tfield}_Code", "")
            vv = rec.get(mcol, "")
            d = parse_time(tv, is_year)
            if d is None or vv in ("", None):
                skipped_rows += 1
                continue
            try:
                v = float(vv)
            except ValueError:
                skipped_rows += 1
                continue
            rows_k.append(".".join(vals + [mcode]))
            rows_d.append(d)
            rows_v.append(v)
            per_measure[mcode] = per_measure.get(mcode, 0) + 1
        if len(rows_k) >= 0 and (i + 1) % FLUSH_FILES == 0:
            _flush()
            done = i + 1
            rate = done / max(1e-9, time.time() - t0)
            total = sum(per_measure.values())
            print(f"  [{done:,}/{len(files):,}] files  {total:,} obs  "
                  f"({rate:.0f} files/s, eta {(len(files) - done) / rate / 60:.0f} min)",
                  flush=True)
    _flush()

    total = sum(per_measure.values())
    n_series_est = None  # computed from the table below
    print(f"parsed {total:,} obs from {len(files) - empty_files:,} data files "
          f"({empty_files} empty, {skipped_rows} rows skipped)", flush=True)
    for m, c in sorted(per_measure.items()):
        print(f"  {m}: {c:,} obs", flush=True)

    failures = [f"{m}: expected {expects[m]:,}, got {per_measure.get(m, 0):,}"
                for m in expects if per_measure.get(m, 0) != expects[m]]
    if failures:
        print("EXPECTATION GATE FAILED — refusing to write:", flush=True)
        for f in failures:
            print(f"  {f}", flush=True)
        return 1

    tbl = pa.concat_tables(chunks)
    del chunks
    out = args.out or os.path.join(ROOT, "data", "clean_full", src, f"{src}.parquet")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    pq.write_table(tbl, out, compression="zstd")
    n = pq.read_metadata(out).num_rows
    print(f"WROTE {out}: {n:,} obs", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
