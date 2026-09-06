"""Which sources hold data we do not catalogue, and which do we catalogue without holding?

WHY THIS EXISTS. noaa (3,135,873 series) and census (440,414) both turned out to be sitting in
R2 with ZERO catalogue rows - hosted, downloadable by id, invisible to search - and I found both
by accident while chasing something else. A reported example is one instance of a class, so the
whole surface gets swept rather than the two I tripped over.

Three outcomes matter:
  UNCATALOGUED  data present, no catalogue row      -> hosted and invisible
  PARTIAL       catalogue covers part of the store
  ORPHAN        catalogue rows with no LOCAL STORE KEY

ORPHAN DOES NOT MEAN "404", AND THIS FILE USED TO SAY IT DID (corrected 2026-09-06, R825). The
old line on ORPHAN asserted that such series were listed but could not be downloaded, and called
that the worse of the three outcomes. That is not what this tool measures:
every comparison here is the CATALOGUE against the LOCAL PARQUET STORE, and neither of those is
what a user receives — the worker serves pre-derived CSVs from R2. Measured on fed_board, whose
638 orphans are the largest set this tool has ever reported: **60 of 60 sampled had a live CSV**,
with a present control at 20/20 and a fabricated id correctly 404ing. They are series the store
can no longer regenerate, not dead links — the same shape already recorded for cso's 290.

AND THE NUMBER IS A FLOOR, because `gap` is a NET. A source with as many uncatalogued store keys
as uncatalogued catalogue rows reports gap 0 and never reaches the ORPHAN branch at all.
fed_board's true split is 638 catalogue ids with no store key against 406 store keys with no
catalogue row; this tool can only ever show their difference, 232 — a number matching neither
side, whose two halves have opposite fixes. Getting the split requires the actual key SETS, which
is the expensive thing this tool exists to avoid; when you need it, compute it per source.

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
        # RECURSIVE. A one-level glob undercounts every nested store and drops four of them
        # entirely — measured 2026-09-03: bea 1 -> 592, gus_dbw 194 -> 868, eia 30 -> 60, and
        # edgar_insider / edgar_13f / edgar_pointers / usda all 0 -> hundreds. bea's flat count
        # made it report `store 17,699 / cat 913,230 / ORPHAN`, an artifact of 591 unseen files.
        # R261/R389/R390's flat-listing trap, which R390 names usda for by name.
        files = [os.path.join(root, f)
                 for root, _dirs, fs in os.walk(os.path.join(STORE, d))
                 for f in fs
                 if f.endswith(".parquet") and not f.endswith("__series.parquet")]
        if not files:
            # NEVER a silent continue. R390: "a guard is the LAST place to tolerate a silent
            # skip, so any branch where it cannot evaluate must print and be read as UNCHECKED,
            # never as clean" — a dropped store is indistinguishable from a clean one.
            line = f"{d}\t\t{counts.get(d, 0)}\t\tno parquet (UNCHECKED)"
            fh.write(line + "\n"); fh.flush()
            print(f"[{i}/{len(names)}] {d:24s} no parquet under it — UNCHECKED, not clean "
                  f"(catalogue holds {counts.get(d, 0):,})", flush=True)
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
            # reports as ORPHAN (catalogued with no store key) when nothing is wrong at all.
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
            # HLL FALLBACK, the method tools/series_census.py::_distinct_keys already uses:
            # exact where it completes, approximate only when it cannot, and SAY WHICH.
            # Without this the five biggest stores have no figure at all, so the fleet total
            # silently excludes the library's two largest sources.
            try:
                n = q.execute(f"select approx_count_distinct(series_key) from "
                              f"read_parquet({lst}, union_by_name=true)").fetchone()[0]
                approx = True
            except Exception as e2:                            # noqa: BLE001
                fh.write(f"{d}\t\t{cat}\t\tscan failed {type(e).__name__}; "
                         f"approx also failed {type(e2).__name__}\n"); fh.flush()
                print(f"[{i}/{len(names)}] {d:24s} SCAN FAILED {type(e).__name__}, "
                      f"approx too ({type(e2).__name__})", flush=True)
                continue
            gap = n - cat
            # AN ESTIMATE MAY NOT ASSERT ORPHAN. Measured HLL error is +19.3% to -14.0%
            # (series_census docstring), so a negative gap on a giant is as likely to be the
            # estimator as the data — and ORPHAN is the verdict that claims users get a 404.
            if cat == 0 and n > 0:
                note, unc = "UNCATALOGUED", unc + n
            elif gap > 0:
                note, unc = "partial", unc + gap
            else:
                note = "inconclusive"
            fh.write(f"{d}\t{n}\t{cat}\t{gap}\t{note}  [approx +19/-14%]\n"); fh.flush()
            print(f"[{i}/{len(names)}] {d:24s} store ~{n:>11,}  cat {cat:>12,}  {gap:>+12,}  "
                  f"{note:14s} {gb:,.1f} GB  [approx: exact OOM'd]", flush=True)
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
    print(f"\nhosted but not catalogued          : {unc:,} series")
    # NOT "not hosted", and not a 404 count (R825). Say what was compared, in the line itself:
    # someone reading only the summary must not be able to take this for user impact.
    print(f"catalogued with no LOCAL STORE KEY : {orph:,} series"
          f"   <- a FLOOR, and NOT a count of 404s")
    if orph:
        print("   These are compared against the LOCAL parquet store, not against what users get")
        print("   (the worker serves pre-derived CSVs from R2). Measured 2026-09-06 on fed_board:")
        print("   60 of 60 sampled such ids HAD a live CSV — series the store can no longer")
        print("   regenerate, not dead links. FLOOR because `gap` is a net: a source with as many")
        print("   uncatalogued store keys as uncatalogued rows reports 0 here. fed_board's real")
        print("   split is 638 / 406, which nets to the 232 this line would otherwise show alone.")
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
