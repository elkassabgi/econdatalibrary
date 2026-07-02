#!/usr/bin/env python3
"""IBGE Brazil — municipal (N6) wave. Additive companion to ingest_ibge.py.

Ahmed's 2026-07-02 decision: include municipal data, start now. The base
ingester is skip-if-exists per aggregate, so flipping INCLUDE_MUNI there would
skip everything already built. This pass instead writes SEPARATE per-aggregate
files ({agg_id}_n6.parquet) containing only the N6 (5,570-municipality) level:
  * purely additive — never touches the existing N1/N3 files
  * resumable — skips aggregates whose _n6 file exists
  * aggregates where N6 yields nothing get a marker in _n6_none/ so re-runs
    don't hammer the API forever (delete the marker to force a retry)

Run: python jobs/ingest_ibge_muni.py
"""
from __future__ import annotations
import os, sys, time

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ingest_ibge import (  # noqa: E402
    OUT, get_aggregates, get_aggregate_meta, get_periods, fetch_data, log,
)

NONE_DIR = os.path.join(OUT, "_n6_none")


def ingest_aggregate_n6(agg_id: int, agg_name: str) -> int:
    out_path = os.path.join(OUT, f"{agg_id}_n6.parquet")
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"  skip agg {agg_id} n6 ({n:,} rows)")
        return n
    if os.path.exists(os.path.join(NONE_DIR, str(agg_id))):
        return 0

    meta = get_aggregate_meta(agg_id)
    variables = (meta or {}).get("variaveis", [])
    var_ids = [str(v["id"]) for v in variables if v.get("id")]
    if not var_ids:
        open(os.path.join(NONE_DIR, str(agg_id)), "w").close()
        return 0
    periods = get_periods(agg_id)
    if not periods:
        open(os.path.join(NONE_DIR, str(agg_id)), "w").close()
        return 0

    CHUNK = 50
    all_keys, all_dates, all_vals = [], [], []
    for i in range(0, len(periods), CHUNK):
        period_str = "|".join(periods[i:i + CHUNK])
        for k, d, v in fetch_data(agg_id, var_ids, period_str, "N6"):
            all_keys.append(k); all_dates.append(d); all_vals.append(v)
        time.sleep(0.5)

    if not all_vals:
        # N6 level not published for this aggregate — mark so we don't loop on it
        open(os.path.join(NONE_DIR, str(agg_id)), "w").close()
        log(f"  agg {agg_id} ({agg_name[:40]}): no N6 data")
        return 0

    tbl = pa.table({
        "series_key": pa.array(all_keys, pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals, pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"  agg {agg_id} ({agg_name[:40]}): {n:,} N6 obs")
    return n


def main() -> None:
    os.makedirs(OUT, exist_ok=True)
    os.makedirs(NONE_DIR, exist_ok=True)
    log("IBGE municipal (N6) wave — fetching aggregate catalog...")
    aggregates = get_aggregates()
    log(f"{len(aggregates)} aggregates to process at N6")
    total = 0
    for i, agg in enumerate(aggregates, 1):
        agg_id = int(agg.get("id", 0))
        if not agg_id:
            continue
        if i % 100 == 0:
            log(f"[{i}/{len(aggregates)}] running total {total:,} N6 obs")
        total += ingest_aggregate_n6(agg_id, agg.get("nome", str(agg_id)))
    log(f"DONE: {total:,} municipal observations")


if __name__ == "__main__":
    main()
