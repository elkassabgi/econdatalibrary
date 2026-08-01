"""Re-key the stored noaa parquet to dataset:station:element, in R2 and the local mirror.

WHY. `<station>:<element>` alone names two different series. Measured over all 417 shards:
1,046,291 of 2,089,582 ids appear in BOTH gsom (monthly) and gsoy (yearly), so one id serves a
monthly series with its own annual aggregates mixed into it. There is no (key, obs_date)
COLLISION - gsom stamps month-start, gsoy stamps year-end - which is exactly why a duplicate
check reports the store clean. The defect only appears when a human reads the series and finds
six spikes in a monthly line.

jobs/ingest_noaa.py already emits the qualified key, so NEW data is correct. This fixes the
data written before that change, and it has to happen BEFORE the catalogue is built: after
publication the same repair costs 3,135,873 renamed ids and every download link that used one.

WHAT IT DOES. Rewrites one column. `dataset` is already stored alongside `series_key` in both
the observation shards and the __series sidecars, so the new key is a string concat of columns
that are already in the file - nothing is re-derived, re-fetched or inferred.

IDEMPOTENT. A file whose keys already start with "<dataset>:" is left untouched and counted as
skipped, so an interrupted run resumes by re-running. Station ids are GHCN codes (ACW00011604)
and can never equal "gsom"/"gsoy", so the prefix test cannot produce a false positive.

MEMORY: THIS IS A WORKSTATION TOOL, NOT A RUNNER TOOL. It reads each shard whole, and
gsom__US.parquet is 262,514,152 rows - about 18 GB decoded into Arrow, plus a second 262M-element
string column for the new key. Observed peak here was ~27 GB resident on that one file. Every
other shard is an order of magnitude smaller. A streaming row-group rewrite would avoid that, but
this is a ONE-OFF repair of data written before the ingest was fixed: the fetcher publishes
correctly-keyed shards from then on, so the complexity would buy nothing. Do not run it on a
16 GB machine.

WRITES GO THROUGH updater.blob. With AQUEDUCT_BACKEND=r2 that publishes the object AND keeps
the local mirror, which is what makes this repair reach what the API actually serves - a local
rewrite alone would leave R2 (the source of truth) still holding the ambiguous keys.

    python tools/rekey_noaa_store.py            # dry run: report, change nothing
    python tools/rekey_noaa_store.py --apply
"""
from __future__ import annotations

import argparse
import os
import sys
import time

import pyarrow as pa
import pyarrow.compute as pc

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def rekey(tbl: pa.Table) -> tuple[pa.Table, bool]:
    """-> (table, changed). Prefixes series_key with its own dataset column."""
    ds = tbl.column("dataset")
    sk = tbl.column("series_key")
    if tbl.num_rows == 0:
        return tbl, False
    # already done? test against the file's OWN dataset values, not a hardcoded list
    already = pc.starts_with(sk, pattern=ds[0].as_py() + ":")
    if pc.all(already).as_py():
        return tbl, False
    new = pc.binary_join_element_wise(pc.cast(ds, pa.string()),
                                      pc.cast(sk, pa.string()), ":")
    i = tbl.schema.get_field_index("series_key")
    return tbl.set_column(i, tbl.schema.field(i), new.cast(pa.string())), True


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    if os.environ.get("AQUEDUCT_BACKEND", "local").strip().lower() != "r2":
        print("AQUEDUCT_BACKEND is not r2. R2 is the source of truth for this store, so a "
              "local-only rewrite would leave the served objects ambiguous.\n"
              "Re-run with AQUEDUCT_BACKEND=r2.")
        return 1

    import pyarrow.parquet as pq

    from updater import blob, config

    d = config.source_dir("noaa")
    names = sorted(blob.list_parquets(d))
    print(f"{len(names)} parquet object(s) under {d}")

    t0 = time.time()
    done = skipped = rows = 0
    for n, name in enumerate(names, 1):
        path = os.path.join(d, name)
        # read LOCAL when the mirror is present: identical bytes, no egress, much faster.
        tbl = pq.read_table(path) if os.path.exists(path) else blob.read_table(path)
        out, changed = rekey(tbl)
        rows += out.num_rows
        if not changed:
            skipped += 1
        elif a.apply:
            blob.write_table_atomic(path, out)
            done += 1
        else:
            done += 1
        if n % 50 == 0 or n == len(names):
            print(f"  [{n}/{len(names)}] rewritten {done:,}  already-qualified {skipped:,}  "
                  f"rows {rows:,}  {time.time() - t0:,.0f}s", flush=True)

    print(f"\n{'REWROTE' if a.apply else 'WOULD REWRITE'} {done:,} object(s); "
          f"{skipped:,} already qualified; {rows:,} rows seen")
    if not a.apply:
        print("(dry run - pass --apply to write)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
