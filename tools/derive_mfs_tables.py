"""Materialize one CSV per imf_mfs*_direct table (COUNTRY x FREQ, key parts 1-2) to R2.

Contract identical to derive_dip_tables.py; the table dims are the key PREFIX (position
mapping proven per flow against the codelists in catalog_mfs_tables.py):
    header series_id,obs_date,value · series_id column = the native store key ·
    rows sorted (series_id, obs_date) · key series/<urlencode(catalog_id)>.csv
One sorted DuckDB pass cuts table boundaries in stream order (R88/R169). Reads the LOCAL
mirror; INERT until the worker flip.
"""
from __future__ import annotations

import argparse
import csv
import io
import os
import sys
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from tools.catalog_mfs_tables import FLOWS  # noqa: E402  — single source of flow config


def csv_bytes(rows) -> bytes:
    buf = io.StringIO(newline="")
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["series_id", "obs_date", "value"])
    for k, d, v in rows:
        w.writerow([k, d.isoformat(), repr(v) if v == v else ""])
    return buf.getvalue().encode("utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("source", choices=sorted(FLOWS))
    ap.add_argument("--bucket")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--threads", type=int, default=16)
    a = ap.parse_args()
    cfg = FLOWS[a.source]
    flow = cfg["flow"]
    pc_, pf_ = cfg.get("pos_country", 1), cfg.get("pos_freq", 2)

    import duckdb
    from core import r2_util
    from updater import config

    store = os.path.join(config.source_dir(a.source), f"{a.source}.parquet")
    if not os.path.exists(store):
        raise SystemExit(f"local mirror missing at {store} — pull from R2 first; a derive "
                         f"from nothing is indistinguishable from one with nothing to do")

    q = duckdb.connect()
    q.execute("PRAGMA threads=6")
    cur = q.execute(f"""
        SELECT '{flow}:' || split_part(split_part(series_key,':',2),'.',{pc_}) || '.' ||
               split_part(split_part(series_key,':',2),'.',{pf_})   AS tbl,
               series_key, obs_date, value
        FROM read_parquet('{store.replace(chr(92), '/')}')
        ORDER BY 1, 2, 3
    """)

    client = r2_util.client(write=True) if (a.bucket and not a.dry_run) else None
    pool = ThreadPoolExecutor(max_workers=a.threads) if client else None
    futures = []

    def put(key: str, body: bytes):
        client.put_object(Bucket=a.bucket, Key=key, Body=body,
                          ContentType="text/csv; charset=utf-8")

    n_tables = n_rows = n_put = errors = 0
    cur_tbl, rows = None, []
    t0 = time.time()

    def flush():
        nonlocal n_tables
        if cur_tbl is None:
            return
        n_tables += 1
        cid = f"{a.source}:{cur_tbl}"
        body = csv_bytes(rows)
        if a.dry_run:
            if n_tables <= 3:
                print(f"  would PUT series/{urllib.parse.quote(cid, safe='')}.csv "
                      f"({len(body):,} B, {len(rows):,} rows)")
        elif client:
            futures.append(pool.submit(put,
                                       f"series/{urllib.parse.quote(cid, safe='')}.csv",
                                       body))

    while True:
        batch = cur.fetchmany(200_000)
        if not batch:
            break
        for tbl, key, d, v in batch:
            if tbl != cur_tbl:
                flush()
                cur_tbl, rows = tbl, []
            rows.append((key, d, v))
            n_rows += 1
    flush()

    if pool:
        for f in futures:
            try:
                f.result()
                n_put += 1
            except Exception as e:                               # noqa: BLE001
                errors += 1
                if errors <= 3:
                    print(f"  PUT failed: {type(e).__name__} {str(e)[:80]}")
        pool.shutdown()

    print(f"done: {n_tables:,} tables / {n_rows:,} rows in {time.time()-t0:.0f}s"
          + (f", put {n_put:,}, errors {errors}" if client else " (no upload)"))
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
