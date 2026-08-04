"""Which sources hold data we do not catalogue, and which do we catalogue without holding?

WHY THIS EXISTS. noaa (3,135,873 series) and census (440,414) both turned out to be sitting in
R2 with ZERO catalogue rows - hosted, downloadable by id, invisible to search - and I found both
by accident while chasing something else. A reported example is one instance of a class, so the
whole surface gets swept rather than the two I tripped over.

Three outcomes matter:
  UNCATALOGUED  data present, no catalogue row      -> hosted and invisible
  PARTIAL       catalogue covers part of the store
  ORPHAN        catalogue rows with no store key    -> listed and undownloadable, which is worse

THE FIRST VERSION OF THIS TOOL WAS THE DEFECT IT LOOKS FOR. It ran `count(distinct series_key)`
over every store in one DuckDB connection with no memory limit and printed nothing until the
final sort. Two hours in it held 128 GB of RAM, had produced not one line of output, and was
starving the three jobs that actually mattered. So:

  * ONE CONNECTION PER SOURCE, closed after use, with memory_limit and a temp_directory so
    DuckDB spills instead of growing. A distinct-count over hundreds of millions of string keys
    builds a hash table of every distinct value; unbounded across ~140 stores that is the whole
    machine.
  * PRINTS EACH SOURCE AS IT FINISHES, flushed. A long audit that reports only at the end is
    indistinguishable from a hung one - the same defect I had just fixed in the orchestrator.
  * --max-gb skips stores bigger than a bound and SAYS which it skipped, so a cheap pass is
    honest about what it did not cover rather than reporting silence as a clean bill.
  * --resume reads the output file and skips sources already done, so an interrupted run
    continues instead of restarting.

    python tools/audit_store_vs_catalog.py --max-gb 5 --out logs/store_audit.tsv
    python tools/audit_store_vs_catalog.py --resume --out logs/store_audit.tsv
"""
from __future__ import annotations

import argparse
import glob
import os
import sqlite3
import sys
import time

import duckdb
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STORE = os.path.join(ROOT, "data", "clean_full")


def dir_gb(files) -> float:
    return sum(os.path.getsize(f) for f in files) / 1e9


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(ROOT, "logs", "store_audit.tsv"))
    ap.add_argument("--max-gb", type=float, default=0.0,
                    help="skip stores larger than this (0 = no bound)")
    ap.add_argument("--memory-limit", default="6GB")
    ap.add_argument("--resume", action="store_true")
    a = ap.parse_args()

    con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=180.0)
    con.execute("PRAGMA busy_timeout = 180000")
    counts = dict(con.execute("select source_id, count(*) from series group by 1").fetchall())
    con.close()

    done = set()
    if a.resume and os.path.exists(a.out):
        for line in open(a.out, encoding="utf-8"):
            done.add(line.split("\t", 1)[0])
        print(f"resuming: {len(done)} source(s) already audited")

    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    fh = open(a.out, "a" if a.resume else "w", encoding="utf-8")
    if not a.resume:
        fh.write("source\tin_store\tcatalogued\tgap\tnote\n")

    skipped_big, unc, orph = [], 0, 0
    names = sorted(d for d in os.listdir(STORE) if os.path.isdir(os.path.join(STORE, d)))
    for i, d in enumerate(names, 1):
        if d in done:
            continue
        files = [f for f in glob.glob(os.path.join(STORE, d, "*.parquet"))
                 if not f.endswith("__series.parquet")]
        if not files:
            continue
        cat = counts.get(d, 0)
        gb = dir_gb(files)
        if a.max_gb and gb > a.max_gb:
            skipped_big.append((d, gb))
            print(f"[{i}/{len(names)}] {d:24s} SKIPPED — {gb:,.1f} GB > --max-gb {a.max_gb}",
                  flush=True)
            continue
        try:
            if "series_key" not in pq.read_schema(files[0]).names:
                line = f"{d}\t\t{cat}\t\tnot a series store"
                fh.write(line + "\n"); fh.flush()
                print(f"[{i}/{len(names)}] {d:24s} not a series store "
                      f"(catalogued {cat:,})", flush=True)
                continue
        except Exception as e:                                 # noqa: BLE001
            fh.write(f"{d}\t\t{cat}\t\tunreadable {type(e).__name__}\n"); fh.flush()
            print(f"[{i}/{len(names)}] {d:24s} UNREADABLE {type(e).__name__}", flush=True)
            continue

        t0 = time.time()
        qualified = False        # set when the shard-qualified recount was the better answer
        q = duckdb.connect()
        try:
            q.execute(f"SET memory_limit='{a.memory_limit}'")
            q.execute(f"SET temp_directory='{os.path.join(ROOT, 'logs', '_duckspill')}'")
            lst = "[" + ",".join(f"'{f}'".replace("\\", "/") for f in files) + "]"
            n = q.execute(f"select count(distinct series_key) from "
                          f"read_parquet({lst}, union_by_name=true)").fetchone()[0]
            # SHARD-QUALIFIED RETRY, only when the bare count says ORPHAN.
            #
            # Some sources key their CATALOGUE by shard - fed_board:CHGDEL:STFBAILB_XEOP_MA_N.Q,
            # fhfa:annual_cbsa:01 - which is exactly what derive_csv_bulk's --qualify-with-shard
            # exists for. A bare `count(distinct series_key)` then UNDERCOUNTS them: one key
            # appearing in two shards is two catalogue rows but one distinct value, so the source
            # reports as ORPHAN ("listed and undownloadable") when nothing is wrong at all.
            #
            # Measured 2026-08-04: this produced fed_board -29 and fhfa -2,021, i.e. 2,050 of the
            # run's 2,408 reported orphans. All 2,050 were phantom -- re-counted with the shard
            # qualifier both sources are EXACTLY coherent (52,322 == 52,322, 89,706 == 89,706),
            # and the other 358 real orphans (unhcr 303, noaa 55) were every one downloadable
            # in R2. A false ORPHAN is expensive: it is the one verdict that says users are being
            # offered something that 404s, so it gets chased first.
            #
            # Only recomputed on the ORPHAN branch, so the common path costs nothing.
            if n < cat:
                # parse_filename(path, true) strips directory AND extension. Deliberately not a
                # regex: the obvious `regexp_replace(filename, '^.*[/\\]', '')` has to survive
                # Python-string escaping and then DuckDB's, and my first attempt reached DuckDB
                # as the invalid class `[/\]` and threw. A builtin has no escaping to get wrong.
                qn = q.execute(
                    f"select count(distinct (parse_filename(filename, true) || ':' || "
                    f"series_key)) from read_parquet({lst}, union_by_name=true, "
                    f"filename=true)").fetchone()[0]
                if abs(qn - cat) < abs(n - cat):
                    n, qualified = qn, True
        except Exception as e:                                 # noqa: BLE001
            fh.write(f"{d}\t\t{cat}\t\tscan failed {type(e).__name__}\n"); fh.flush()
            print(f"[{i}/{len(names)}] {d:24s} SCAN FAILED {type(e).__name__}: "
                  f"{str(e)[:80]}", flush=True)
            continue
        finally:
            q.close()

        gap = n - cat
        if cat == 0 and n > 0:
            note, unc = "UNCATALOGUED", unc + n
        elif gap > 0:
            note, unc = "partial", unc + gap
        elif gap < 0:
            note, orph = "ORPHAN", orph - gap
        else:
            note = "ok"
        # Say WHICH count is being reported. A number that silently changed meaning is how the
        # fed_board/fhfa false orphans read as real in the first place.
        tag = "  [shard-qualified]" if qualified else ""
        fh.write(f"{d}\t{n}\t{cat}\t{gap}\t{note}{tag}\n"); fh.flush()
        print(f"[{i}/{len(names)}] {d:24s} store {n:>12,}  cat {cat:>12,}  {gap:>+12,}  "
              f"{note:14s} {gb:,.1f} GB {time.time()-t0:,.0f}s{tag}", flush=True)

    fh.close()
    print(f"\nhosted but not catalogued : {unc:,} series")
    print(f"catalogued but not hosted : {orph:,} series")
    if skipped_big:
        # DISCLOSED, never silent: a bounded pass that did not say what it skipped reads as
        # full coverage.
        print(f"\nNOT AUDITED — {len(skipped_big)} store(s) over --max-gb {a.max_gb}:")
        for d, gb in sorted(skipped_big, key=lambda x: -x[1]):
            print(f"   {d:24s} {gb:,.1f} GB")
        print("Re-run without --max-gb (or with a larger one) to cover these.")
    print(f"\nfull results: {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
