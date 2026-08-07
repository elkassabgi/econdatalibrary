"""tools/derive_unsdg_flows.py — materialize one CSV per SDG series code to R2.

Flow grain for unsdg: one CSV per SDG series code (396 held of 713 published), each holding
every key of that code — `AG_LND_DGRD:AFG`, `AG_LND_DGRD:AFG|Sex=FEMALE`, ... The catalogue
built by tools/catalog_unsdg_flows.py has one row per code, and clients/python/econdl/
_resolve.py serves a code by the `_FLOW_GRAIN` prefix rule (`starts_with(series_key,
"<code>:")`), so the CSV here and the parquet resolver must agree on that same boundary.

WHY NOT tools/derive_pxweb_flowgrain.py: its prefix regex is `^(?P<p>.*?):[^:=]*=`, which
requires a `=` in the key. unsdg's undimensioned keys (`AG_LND_DGRD:AFG`) have none, so that
tool falls through to "whole key is the prefix" and would emit one CSV per key — 227,955
objects at series grain instead of 396 at flow grain, silently the wrong product.

CSV contract is identical to every other deriver (series.ts depends on it):
    header:  series_id,obs_date,value      (series_id column = the native store key)
    rows:    sorted by (series_id, obs_date)
    R2 key:  series/<encodeURIComponent("unsdg:<CODE>")>.csv
Uploading is INERT until D1 carries the catalogue rows — the Worker 404s an uncatalogued id.

  python tools/derive_unsdg_flows.py --dry-run                 # count, touch nothing
  python tools/derive_unsdg_flows.py --sample 3                # write 3 CSVs locally
  python tools/derive_unsdg_flows.py --bucket econ-data        # real upload
"""
from __future__ import annotations

import argparse
import csv
import io
import os
import sys
import time
import urllib.parse

import pyarrow.parquet as pq

MAIN = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# DERIVED, never hardcoded (the R330 dead-drive class): a glob of a directory that does not
# exist returns nothing while exiting 0, so a derive that publishes NOTHING looks identical
# to one with nothing to do. Fail loudly instead.
STORE = os.path.join(MAIN, "data", "clean_full", "unsdg", "unsdg.parquet")
SAMPLE_DIR = os.path.join(MAIN, "logs", "derive_samples", "unsdg_flows")


def code_of(series_key: str) -> str:
    """The SDG series code — everything before the FIRST ':'. Mirrors the resolver's
    prefix rule exactly; if these two ever disagree the catalogue advertises ids whose
    CSV holds different rows than the parquet download."""
    return series_key.split(":", 1)[0]


def csv_bytes(rows: list) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["series_id", "obs_date", "value"])
    for k, d, v in sorted(rows):
        w.writerow([k, d, v])
    return buf.getvalue().encode("utf-8")


def r2_key(series_id: str) -> str:
    return f"series/{urllib.parse.quote(series_id, safe='')}.csv"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--sample", type=int, help="write N CSVs locally instead of uploading")
    ap.add_argument("--threads", type=int, default=16)
    a = ap.parse_args()

    if not os.path.exists(STORE):
        raise SystemExit(f"store not found at {STORE} — refusing to report an empty derive")

    groups: dict[str, list] = {}
    n_rows = n_skipped = 0
    pf = pq.ParquetFile(STORE)
    for batch in pf.iter_batches(columns=["series_key", "obs_date", "value"],
                                 batch_size=500_000):
        for k, d, v in zip(batch.column("series_key").to_pylist(),
                           batch.column("obs_date").to_pylist(),
                           batch.column("value").to_pylist()):
            if d is None or v is None or k is None:
                n_skipped += 1
                continue
            groups.setdefault(code_of(k), []).append(
                (k, d.isoformat() if hasattr(d, "isoformat") else str(d), v))
            n_rows += 1

    print(f"{len(groups)} codes / {n_rows:,} rows"
          f"{f' ({n_skipped:,} null rows skipped)' if n_skipped else ''}")

    # RECONCILE AGAINST THE CATALOGUE BEFORE PUBLISHING (R364): whr PUT 1,927 CSVs for a
    # 1,749-row catalogue because nothing compared the two counts, and 178 objects from a
    # retired provenance landed on R2. A mismatch here means the catalogue and the store
    # disagree about what exists -- stop and look, do not upload the difference.
    import sqlite3
    con = sqlite3.connect(os.path.join(MAIN, "data", "catalog.db"), timeout=120)
    cat = {r[0].split(":", 1)[1] for r in
           con.execute("SELECT series_id FROM series WHERE source_id='unsdg'")}
    con.close()
    only_store, only_cat = set(groups) - cat, cat - set(groups)
    print(f"catalogue rows: {len(cat)}  |  store codes: {len(groups)}  |  "
          f"store-only: {len(only_store)}  catalogue-only: {len(only_cat)}")
    if only_store or only_cat:
        print(f"  store-only     (would be orphan CSVs): {sorted(only_store)[:8]}")
        print(f"  catalogue-only (would 404 for users) : {sorted(only_cat)[:8]}")
        raise SystemExit("catalogue/store mismatch — re-run tools/catalog_unsdg_flows.py "
                         "--apply first so every published CSV has a listing and vice versa")

    if a.sample is not None:
        os.makedirs(SAMPLE_DIR, exist_ok=True)
        for code in sorted(groups)[:a.sample]:
            p = os.path.join(SAMPLE_DIR, f"unsdg_{code}.csv")
            with open(p, "wb") as fh:
                fh.write(csv_bytes(groups[code]))
            print(f"  wrote {p} ({len(groups[code]):,} rows)")
        return 0

    if a.dry_run:
        print(f"(dry run — would PUT {len(groups)} CSVs to series/unsdg%3A<CODE>.csv)")
        return 0

    if not a.bucket:
        ap.error("--bucket required for a real run")
    sys.path.insert(0, MAIN)
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from core import r2_util
    s3 = r2_util.client(write=True)

    def put(code: str) -> int:
        body = csv_bytes(groups[code])
        key = r2_key(f"unsdg:{code}")
        for attempt in range(7):
            try:
                s3.put_object(Bucket=a.bucket, Key=key, Body=body, ContentType="text/csv")
                return len(body)
            except Exception:
                if attempt == 6:
                    raise
                time.sleep(2 ** attempt)
        return 0

    done = total_bytes = 0
    with ThreadPoolExecutor(max_workers=a.threads) as ex:
        futs = {ex.submit(put, c): c for c in groups}
        for f in as_completed(futs):
            total_bytes += f.result()
            done += 1
            if done % 50 == 0 or done == len(groups):
                print(f"  {done}/{len(groups)} CSVs uploaded", flush=True)
    print(f"DONE: {done} CSVs / {total_bytes:,} bytes to r2://{a.bucket}/series/")
    print("NEXT: refresh_r2_catalog, sync_catalog_d1, un-gate, util.ts, deploy, verify.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
