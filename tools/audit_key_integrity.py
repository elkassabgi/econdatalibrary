"""Does each source's series_key actually IDENTIFY an observation?

THE QUESTION: for how many (series_key, obs_date) pairs does the store hold more than one
DISTINCT value? Zero means the key identifies the observation. Anything else means the key is
UNDER-SPECIFIED - two genuinely different series share an id, and publishing them as one picks
a winner silently.

This has now bitten three times and I found it by accident every time:

  comtrade  19,932 conflicting rows; the key omitted server-side dimensions
  noaa      1,046,291 ids named BOTH a monthly and an annual series. No (key, date) conflict at
            all, because gsom stamps month-start and gsoy year-end - which is why this check
            alone is not sufficient and the sibling question "does one key span two datasets?"
            is asked too, where a `dataset` column exists
  usda      213,135 conflicting groups; REFERENCE_PERIOD_DESC is a real dimension the key omits,
            so Maryland winter wheat 2020 carries six values - MAY/JUN/JUL/AUG FORECAST, JUN
            ACREAGE, and the final YEAR estimate - stacked under one id. Serving that as a time
            series would plot a mixture of forecast vintages as if it were history.

Three accidents is a class, so it becomes a standing check rather than a thing I remember to do.

BOUNDED AND INCREMENTAL (R212): memory_limit, a spill directory, one source per connection
closed after use, results printed as they land and appended to --out so an interrupted run
resumes with --resume. --max-gb skips oversized stores and NAMES them rather than reporting
silence as coverage.

    python tools/audit_key_integrity.py --max-gb 8 --out logs/key_integrity.tsv
    python tools/audit_key_integrity.py --resume --out logs/key_integrity.tsv
"""
from __future__ import annotations

import argparse
import glob
import os
import sys
import time

import duckdb
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STORE = os.path.join(ROOT, "data", "clean_full")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(ROOT, "logs", "key_integrity.tsv"))
    ap.add_argument("--max-gb", type=float, default=0.0)
    ap.add_argument("--memory-limit", default="6GB")
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--source", action="append",
                    help="limit to these sources (repeatable)")
    a = ap.parse_args()

    done = set()
    if a.resume and os.path.exists(a.out):
        for line in open(a.out, encoding="utf-8"):
            done.add(line.split("\t", 1)[0])
        print(f"resuming: {len(done)} source(s) already audited")

    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    fh = open(a.out, "a" if a.resume else "w", encoding="utf-8")
    if not a.resume:
        fh.write("source\tconflicting_groups\tspan_datasets\tnote\n")

    names = sorted(d for d in os.listdir(STORE) if os.path.isdir(os.path.join(STORE, d)))
    if a.source:
        names = [n for n in names if n in set(a.source)]
    skipped, bad = [], []
    for i, d in enumerate(names, 1):
        if d in done:
            continue
        files = [f for f in glob.glob(os.path.join(STORE, d, "**", "*.parquet"),
                                      recursive=True)
                 if not f.endswith("__series.parquet")]
        if not files:
            continue
        gb = sum(os.path.getsize(f) for f in files) / 1e9
        if a.max_gb and gb > a.max_gb:
            skipped.append((d, gb))
            print(f"[{i}/{len(names)}] {d:24s} SKIPPED — {gb:,.1f} GB > --max-gb {a.max_gb}",
                  flush=True)
            continue
        try:
            cols = set(pq.read_schema(files[0]).names)
        except Exception as e:                                 # noqa: BLE001
            print(f"[{i}/{len(names)}] {d:24s} unreadable {type(e).__name__}", flush=True)
            continue
        if not {"series_key", "obs_date", "value"} <= cols:
            fh.write(f"{d}\t\t\tnot a long series store\n"); fh.flush()
            print(f"[{i}/{len(names)}] {d:24s} not a long {{series_key,obs_date,value}} store",
                  flush=True)
            continue

        lst = "[" + ",".join(f"'{f}'".replace("\\", "/") for f in files) + "]"
        con = duckdb.connect()
        t0 = time.time()
        try:
            con.execute(f"SET memory_limit='{a.memory_limit}'")
            spill = os.path.join(ROOT, "logs", "_duckspill")
            os.makedirs(spill, exist_ok=True)   # DuckDB will not create it and errors if absent
            con.execute(f"SET temp_directory='{spill}'")
            con.execute("SET preserve_insertion_order=false")
            # EXPRESSED SO IT SPILLS. `count(distinct value)` inside a GROUP BY keeps a
            # per-group distinct set in memory and DuckDB cannot spill it: cepii_gravity
            # (69,666,545 rows) died with "failed to allocate 8.0 KiB (6.9 GiB/6.0 GiB)" even
            # with memory_limit and a temp_directory set. A hash-DISTINCT followed by a plain
            # COUNT(*) asks the identical question in two operators that both spill to disk.
            conf = con.execute(f"""
                select count(*) from (
                  select series_key, obs_date from (
                    select distinct series_key, obs_date, value
                    from read_parquet({lst}, union_by_name=true))
                  group by 1, 2 having count(*) > 1)""").fetchone()[0]
            # THE SIBLING QUESTION. noaa had ZERO (key,date) conflicts and was still badly
            # under-keyed, because its two datasets stamp different days of the period. Where a
            # `dataset` column exists, ask whether one key spans more than one of them.
            span = ""
            if "dataset" in cols:
                span = con.execute(f"""
                    select count(*) from (
                      select series_key from (
                        select distinct series_key, dataset
                        from read_parquet({lst}, union_by_name=true))
                      group by 1 having count(*) > 1)""").fetchone()[0]
        except Exception as e:                                 # noqa: BLE001
            fh.write(f"{d}\t\t\tscan failed {type(e).__name__}\n"); fh.flush()
            print(f"[{i}/{len(names)}] {d:24s} SCAN FAILED {type(e).__name__}: "
                  f"{str(e)[:70]}", flush=True)
            continue
        finally:
            con.close()

        flag = "UNDER-KEYED" if (conf or (span or 0)) else "ok"
        if flag != "ok":
            bad.append((d, conf, span))
        fh.write(f"{d}\t{conf}\t{span}\t{flag}\n"); fh.flush()
        print(f"[{i}/{len(names)}] {d:24s} conflicting {conf:>10,}  "
              f"span-datasets {str(span):>10s}  {flag:12s} {gb:,.1f} GB "
              f"{time.time()-t0:,.0f}s", flush=True)

    fh.close()
    print(f"\nUNDER-KEYED sources: {len(bad)}")
    for d, c, s in bad:
        print(f"   {d:24s} conflicting (key,date) groups {c:,}   keys spanning datasets {s}")
    if skipped:
        print(f"\nNOT AUDITED — {len(skipped)} store(s) over --max-gb {a.max_gb}:")
        for d, gb in sorted(skipped, key=lambda x: -x[1]):
            print(f"   {d:24s} {gb:,.1f} GB")
    print(f"\nfull results: {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
