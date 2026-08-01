"""Materialise census at TABLE grain to R2 — one CSV per table, large tables split.

WHY TABLE GRAIN. census is 80 WIDE tables (11 to 238 columns, the measures ARE the columns) over
44,242,170 rows. At cell grain it would be 1,202,396,034 nominal series. Measured, 70 of the 80
are genuine PANELS (more than 3 periods) and only 8 are single-period censuses — the opposite of
what two early samples suggested, and the reason this is worth serving at all rather than
treating as a pile of cross-sections.

IT CALLS THE RESOLVER, IT DOES NOT REIMPLEMENT IT. Every other derive in this repo builds the
CSV itself and is then checked for byte-parity against core.derive_csv — a check that has caught
a real defect every single time it was run today (a missing null filter, an unqualified id, a
non-deterministic tiebreak). census has only ~300 units, so the scan-per-unit cost that forces
the other sources to hand-roll their own writer simply does not apply here. Producing the bytes
THROUGH the reference implementation makes parity structural instead of tested: there is one
construction, so there is nothing to diverge.

SPLITTING large tables reuses the same evidence-based rule as istat: measure each named
dimension in the table's own pipe-delimited keys, verify the LARGEST resulting part fits the
bound (cardinality is not balance — that mistake produced a 775,206-row "part" for istat), and
record the choice in _split_map.json so the resolver can reproduce it.

    python tools/derive_census_tables.py --dry-run
    python tools/derive_census_tables.py --bucket econ-data --skip-existing
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sys
import time
import urllib.parse

import duckdb
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))

from core import r2_util                                       # noqa: E402
from core.derive_csv import _series_csv_bytes                  # noqa: E402

SOURCE = "census"
STORE = os.path.join(ROOT, "data", "clean_full", SOURCE)
MAX_ROWS_DEFAULT = 500_000


def table_sid(table: str, part: str | None = None) -> str:
    return f"{SOURCE}:{table}" + (f"#{part}" if part else "")


def csv_key(prefix: str, sid: str) -> str:
    return f"{prefix}/{urllib.parse.quote(sid, safe='')}.csv"


def choose_split(con, path: str, n_rows: int, max_rows: int):
    """-> (dimension, parts) or (None, []) when the table needs no split.

    Keys are pipe-delimited NAME=VALUE fragments. Candidates are tried coarsest-first and each
    is CHECKED by grouping — the largest part must fit, because a dimension can have plenty of
    distinct values and still put almost every row in one of them.
    """
    if n_rows <= max_rows:
        return None, []
    row = con.execute(f"select series_key from read_parquet('{path}') limit 1").fetchone()
    if not row:
        return None, []
    dims = [p.split("=", 1)[0] for p in row[0].split("|") if "=" in p]
    card = {}
    for d in dims:
        try:
            card[d] = con.execute(
                f"select count(distinct regexp_extract(series_key, '{d}=([^|]*)', 1)) "
                f"from read_parquet('{path}')").fetchone()[0]
        except Exception:                                      # noqa: BLE001
            continue
    for c, d in sorted((c, d) for d, c in card.items() if 2 <= c <= 2000):
        expr = f"regexp_extract(series_key, '{d}=([^|]*)', 1)"
        try:
            biggest = con.execute(
                f"select max(n) from (select count(*) n from read_parquet('{path}') "
                f"where obs_date is not null group by {expr})").fetchone()[0]
            if biggest and biggest <= max_rows:
                parts = [r[0] for r in con.execute(
                    f"select distinct {expr} from read_parquet('{path}') "
                    f"where obs_date is not null order by 1").fetchall()]
                return d, parts
        except Exception:                                      # noqa: BLE001
            continue
    return "", []                                              # refused


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket")
    ap.add_argument("--prefix", default="series")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip-existing", action="store_true")
    ap.add_argument("--max-rows", type=int, default=MAX_ROWS_DEFAULT)
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()
    if not a.dry_run and not a.bucket:
        ap.error("--bucket is required unless --dry-run")

    files = sorted(f.replace("\\", "/") for f in glob.glob(os.path.join(STORE, "*.parquet"))
                   if not f.endswith("__series.parquet"))
    if not files:
        raise SystemExit(f"no parquet under {STORE} — refusing to report an empty derive")
    print(f"{len(files)} table(s); splitting any over {a.max_rows:,} rows", flush=True)

    spill = os.path.join(ROOT, "logs", "_duckspill")
    os.makedirs(spill, exist_ok=True)

    existing = set()
    s3 = None
    if not a.dry_run:
        s3 = r2_util.client(write=True)
        if a.skip_existing:
            pref = f"{a.prefix}/{urllib.parse.quote(SOURCE + ':', safe='')}"
            tok = None
            while True:
                kw = {"Bucket": a.bucket, "Prefix": pref, "MaxKeys": 1000}
                if tok:
                    kw["ContinuationToken"] = tok
                r = s3.list_objects_v2(**kw)
                existing.update(o["Key"] for o in r.get("Contents", []))
                if not r.get("IsTruncated"):
                    break
                tok = r["NextContinuationToken"]
            print(f"skip-existing: {len(existing):,} already in R2", flush=True)

    split_map, refused, ids = {}, [], []
    for i, f in enumerate(files, 1):
        table = os.path.splitext(os.path.basename(f))[0]
        try:
            n_rows = pq.ParquetFile(f).metadata.num_rows
            cols = set(pq.read_schema(f).names)
        except Exception as e:                                 # noqa: BLE001
            refused.append((table, f"unreadable {type(e).__name__}"))
            continue
        if "series_key" not in cols or "obs_date" not in cols:
            # pseo__earnings / pseo__flows are entity-level and have no obs_date; named, not
            # silently dropped, because "80 tables" minus two with no explanation is a gap.
            refused.append((table, f"no series_key/obs_date (cols={len(cols)})"))
            continue
        con = duckdb.connect()
        con.execute("SET memory_limit='6GB'")
        con.execute(f"SET temp_directory='{spill}'")
        con.execute("SET preserve_insertion_order=false")
        dim, parts = choose_split(con, f, n_rows, a.max_rows)
        con.close()
        if dim == "":
            refused.append((table, f"{n_rows:,} rows, no dimension divides it"))
            print(f"  [{i}/{len(files)}] {table}: REFUSED — {n_rows:,} rows", flush=True)
            continue
        if dim:
            split_map[table] = {"dim": dim, "parts": len(parts), "rows": n_rows}
            ids += [table_sid(table, p) for p in parts if p]
            print(f"  [{i}/{len(files)}] {table}: {n_rows:,} rows split by {dim} "
                  f"-> {len(parts)} parts", flush=True)
        else:
            ids.append(table_sid(table))
        if a.limit and i >= a.limit:
            break

    print(f"\n{len(ids):,} unit(s) to write; {len(refused)} table(s) refused", flush=True)
    if not a.dry_run:
        with open(os.path.join(STORE, "_split_map.json"), "w", encoding="utf-8") as fh:
            json.dump(split_map, fh, indent=1, sort_keys=True)

    t0, put, skip, err = time.time(), 0, 0, 0
    for n, sid in enumerate(ids, 1):
        key = csv_key(a.prefix, sid)
        if key in existing:
            skip += 1
            continue
        try:
            body = _series_csv_bytes(sid)                      # THE reference construction
        except Exception as e:                                 # noqa: BLE001
            err += 1
            if err <= 5:
                print(f"  BUILD FAILED {sid}: {type(e).__name__} {str(e)[:90]}", flush=True)
            continue
        if a.dry_run:
            if n <= 3:
                print(f"  would PUT {key} ({len(body):,} B)")
            continue
        try:
            s3.put_object(Bucket=a.bucket, Key=key, Body=body, ContentType="text/csv")
            put += 1
            if put % 25 == 0:
                print(f"  put {put:,}/{len(ids):,}  {time.time()-t0:,.0f}s", flush=True)
        except Exception as e:                                 # noqa: BLE001
            err += 1
            if err <= 5:
                print(f"  PUT FAILED {key}: {str(e)[:90]}", flush=True)

    print(f"\nunits {len(ids):,}   put {put:,}   skipped {skip:,}   errors {err:,}   "
          f"{time.time()-t0:,.0f}s")
    if refused:
        print(f"REFUSED — {len(refused)}:")
        for t, why in refused:
            print(f"   {t:44s} {why}")
    return 1 if err else 0


if __name__ == "__main__":
    sys.exit(main())
