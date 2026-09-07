"""Which sources hold data we do not catalogue, and which do we catalogue without holding?

WHY THIS EXISTS. noaa (3,135,873 series) and census (440,414) both turned out to be sitting in
R2 with ZERO catalogue rows - hosted, downloadable by id, invisible to search - and I found both
by accident while chasing something else. A reported example is one instance of a class, so the
whole surface gets swept rather than the two I tripped over.

Three outcomes matter:
  UNCATALOGUED  data present LOCALLY, no catalogue row
  PARTIAL       catalogue covers part of the local store
  ORPHAN        catalogue rows with no LOCAL STORE KEY

"UNCATALOGUED" DOES NOT MEAN "HOSTED AND INVISIBLE", and this file used to say it did
(corrected 2026-09-06). Every comparison here is CATALOGUE vs LOCAL PARQUET STORE, and
neither is R2. Listing R2 under series/<source>%3A for every source with a large positive
gap in the run then in flight:

    source     R2 objects     catalogue rows     local store keys
    abs                18                 18          376,333,085
    bls                 9                  9          154,190,127
    bis                49                 49            1,521,257
    ember              60                 60              255,898
    ecb                35                 35            3,733,574
    census          2,993              2,993              440,414
    gus                 0                  0              151,236

R2 and the catalogue agree EXACTLY in all seven. So the gap is data that was never derived
to R2 at all - HELD, not hosted. It is still a real coverage gap, because a user cannot get
it; but the fix is "derive and publish", not "add a catalogue row", and the two are not the
same job. This is the R825 correction in the opposite direction: there the file wrongly said
ORPHAN meant a 404, here it wrongly said a gap meant hosted. Both came from describing the
SERVED system with a LOCAL measurement.

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
import io
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


def _predicate_shape(pred: str, key_col: str) -> str:
    """WHOLE-FILE / GROUP / SERIES, from the predicate the resolver actually returns.

    The column matters, not the operator: `(flow == "X")` is an equality that selects an entire
    flow, and reading it as "one series" is how insee_melodi was misclassified.
    """
    import re                                                        # noqa: PLC0415
    if "starts_with" in pred or "is_in" in pred or "match" in pred or "ends_with" in pred:
        return "GROUP"
    eqs = re.findall(r"\(?\s*([A-Za-z_][A-Za-z0-9_]*)\s*==", pred)
    if eqs:
        return "SERIES" if all(c == key_col for c in eqs) else "GROUP"
    if "is_valid" in pred:
        return "WHOLE-FILE"
    return "GROUP"          # unrecognised: treat as coarse and let the declarations disagree


def _grain_from_resolver(_resolve) -> dict:
    """{source: 'file'|'group'} for every source we can resolve one real catalogue id for.

    A source with ZERO catalogue rows has no id to resolve and simply does not appear - absence
    of measurement, not evidence of series grain. The declaration lists cover that case.
    Resolving builds a path and an expression; NO parquet data is read.
    """
    import sqlite3                                                   # noqa: PLC0415
    out: dict[str, str] = {}
    con = sqlite3.connect(
        f"file:{os.path.join(ROOT, 'data', 'catalog.db')}?mode=ro", uri=True, timeout=60.0)
    try:
        srcs = [r[0] for r in con.execute("SELECT DISTINCT source_id FROM series")]
        for s in srcs:
            row = con.execute(
                "SELECT series_id FROM series WHERE source_id=? LIMIT 1", (s,)).fetchone()
            if not row:
                continue
            try:
                res = _resolve.resolve(row[0], STORE)
            except Exception:                                        # noqa: BLE001, PERF203
                continue                                             # unresolvable: leave to lists
            shape = _predicate_shape(str(getattr(res, "predicate", "")),
                                     str(getattr(res, "key_col", "")))
            if shape == "WHOLE-FILE":
                out[s] = "file"
            elif shape == "GROUP":
                out[s] = "group"
    finally:
        con.close()
    return out


def grain_index() -> dict:
    """Which sources are NOT served at series grain — taken from the RESOLVER'S OWN sets.

    THE SUBTRACTION THIS TOOL PRINTS IS ONLY A COVERAGE MEASURE WHEN ONE CATALOGUE ROW
    MEANS ONE STORE KEY. For a source served at flow, table or file grain that is false by
    design: one row deliberately stands for thousands of keys, so `gap` measures the GRAIN,
    not missing coverage. Unqualified, abs reads as 376,333,067 hosted-and-invisible series
    against 18 catalogue rows, and bls as 154,190,118 against 9 — figures that would
    dominate the fleet total and are not defects at all.

    Read from `clients/python/econdl/_resolve.py` rather than re-listed here, so the audit
    and the resolver agree BY CONSTRUCTION and not by both being edited — the same reason
    the key-column candidates are shared with core/broaden_catalog.py::_key_col.
    """
    # BOTH paths. Run as a script, sys.path[0] is tools/, so neither the client package nor
    # the repo root is importable - and the orchestrate import below then fails, which
    # (correctly) refuses the whole run. Caught by the guard itself, on the first live run:
    # the tests passed because pytest puts the repo root on sys.path and the script does not.
    sys.path.insert(0, os.path.join(ROOT, "clients", "python"))
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    from econdl import _resolve                                   # noqa: PLC0415
    out: dict[str, str] = {}
    for s in getattr(_resolve, "_FLOW_GRAIN", ()):
        out[s] = "flow"
    for s in getattr(_resolve, "_DOT_TABLE_GRAIN", ()):
        out[s] = "dot-table"
    # THE SIXTH HOLDER, and it overlaps neither of the two above. Measured 2026-09-06:
    #   _resolve._FLOW_GRAIN 11 | _resolve._DOT_TABLE_GRAIN 18 | orchestrate._TABLE_GRAIN 14
    #   in orchestrate but in NEITHER _resolve set: all 14 (every imf_*_direct)
    #   in _resolve's sets but not in orchestrate  : all 29
    # tests/test_table_grain_mapping.py pins it, and says why: those stores are at SERIES
    # grain while their catalogue is at TABLE grain, so an unmapped count equal to the key
    # count is "GRAIN mismatch, not a missing catalogue" - this tool's exact question,
    # already answered elsewhere on a set it did not consult.
    try:
        import importlib                                            # noqa: PLC0415
        for s in getattr(importlib.import_module("updater.orchestrate"),
                         "_TABLE_GRAIN", ()):
            out[s] = "table"
    except Exception as e:                                          # noqa: BLE001
        # NOT SILENT. Raising is right: without it 14 declared table-grain sources would be
        # downgraded to "unestablished" and inflate the headline with a designed difference,
        # which is the mirror of the bug this file exists to fix.
        raise RuntimeError(
            f"updater.orchestrate._TABLE_GRAIN unreadable ({type(e).__name__}: {e}); "
            f"14 declared table-grain sources would be misclassified") from e
    # ---- ASK THE RESOLVER, not the lists -------------------------------------------------
    # A catalogue id's grain is decided by the predicate `resolve()` returns, so read that
    # rather than six declaration lists that drift. Measured 2026-09-06: the lists are a strict
    # SUBSET of truth - 48 declared, 62 measured, ZERO declared-but-wrong, 15 coarse but
    # declared nowhere (eurostat, statcan, istat, usda, census, sec_edgar, treasury, faostat,
    # fed_board, ilostat, eia, worldbank*). Counting those 15 as gaps inflated the headline by
    # 1,012,069,333 keys.
    out.update(_grain_from_resolver(_resolve))

    file_grain = getattr(_resolve, "_resolve_file_grain", None)
    for s, fn in getattr(_resolve, "_RESOLVERS", {}).items():
        # "custom" IS NOT A CLAIM THAT THE SOURCE IS TABLE-GRAIN. A bespoke resolver may
        # exist for a layout reason and still be series grain. All it establishes is that
        # THIS tool has not established the grain — which is a third answer, not a licence
        # to excuse the gap. Reported separately and never as clean.
        out.setdefault(s, "file" if fn is file_grain else "custom")
    return out


DECLARED_GRAINS = ("flow", "dot-table", "file", "table", "group")


def denylisted() -> set:
    """Sources deliberately GATED: they answer 451 and are hidden from the catalogue.

    A gated source holds data, has zero catalogue rows and zero R2 objects BY DECISION - the
    2026-07-22/23 licence purge. Reporting one as UNCATALOGUED without saying so invites the
    reader to conclude data was forgotten, which is the error this function exists to stop:
    on 2026-09-06 I wrote that `gus` was "a dead store directory nothing owns", from a
    registry lookup, when it has a working fetcher (jobs/ingest_gus_bdl.py) and sits in this
    very set - twenty minutes after a reviewer told me the same about `cow` and `cboe`
    (R838). A rule broken by hand twice belongs in the instrument.
    """
    import re                                                    # noqa: PLC0415
    p = os.path.join(ROOT, "api", "worker", "src", "denylist.ts")
    m = re.search(r"NON_REDISTRIBUTABLE[^=]*=\s*new Set(?:<[^>]*>)?\(\[(.*?)\]\)",
                  io.open(p, encoding="utf-8").read(), re.S)
    if not m:
        raise RuntimeError(f"could not parse NON_REDISTRIBUTABLE in {p}")
    ids = set(re.findall(r'"([a-z0-9_]+)"', m.group(1)))
    if len(ids) < 10:
        # FAIL CLOSED: a near-empty denylist would report every gated source as forgotten.
        raise RuntimeError(f"parsed only {len(ids)} denylist ids, implausibly few")
    return ids


def summarise(path: str) -> int:
    """Re-classify a finished run's rows. Reads the TSV, never the store."""
    try:
        grain, grain_ok = grain_index(), True
    except Exception as e:                                     # noqa: BLE001
        grain, grain_ok = {}, False
        print(f"GRAIN INDEX UNAVAILABLE ({type(e).__name__}: {e}) — nothing below is "
              f"qualified.")
    try:
        deny = denylisted()
    except Exception as e:                                     # noqa: BLE001
        deny = set()
        print(f"DENYLIST UNAVAILABLE ({type(e).__name__}: {e}) — gated sources will be "
              f"reported as forgotten ones. Do not act on the bucket below.")
    unc = graingap = unkgap = zerogap = gategap = 0
    grainsrc, unkgrain, zerocat, gatedsrc, orph, unread = [], [], [], [], 0, []
    rows = 0
    with io.open(path, encoding="utf-8") as fh:
        for line in fh:
            f = line.rstrip("\n").split("\t")
            if len(f) < 5 or f[0] == "source":
                continue
            d, in_store, cat = f[0], f[1], f[2]
            rows += 1
            if not in_store:
                # no parquet / no key column / unreadable — NOT MEASURED, never clean
                unread.append((d, f[4]))
                continue
            n, c = int(in_store), int(cat or 0)
            gap, g = n - c, grain.get(d)
            if d in deny and gap > 0:
                gategap += gap
                gatedsrc.append((d, n, c))
            elif c == 0 and n > 0:
                zerogap, unc = zerogap + n, unc + n
                zerocat.append((d, n, g))
            elif gap > 0 and g in DECLARED_GRAINS:
                graingap += gap
                grainsrc.append((d, n, c, g))
            elif gap > 0 and g:
                unkgap, unc = unkgap + gap, unc + gap
                unkgrain.append((d, n, c, g))
            elif gap > 0:
                unc += gap
            elif gap < 0 and "[approx" not in f[4]:
                # AN ESTIMATE MAY NOT ASSERT ORPHAN. main() refuses this deliberately
                # (measured HLL error +19.3%/-14.0%), and summarise must agree with it or a
                # giant that OOM'd into the HLL path invents an orphan count out of noise.
                orph -= gap
    print(f"re-read {rows:,} row(s) from {path} — MEASURED NOTHING, only re-classified")
    print(f"\nheld locally but not catalogued    : {unc:,} series"
          f"   <- SERIES-GRAIN sources only; NOT a claim about R2")
    if not grain_ok:
        print("   WARNING: grain index missing — this total is UNQUALIFIED. Do not report "
              "it.")
    print(f"catalogued with no LOCAL STORE KEY : {orph:,} series"
          f"   <- a FLOOR, and NOT a count of 404s")
    if gatedsrc:
        print(f"\nGATED WITH A GAP - {len(gatedsrc)} denylisted source(s), "
              f"{gategap:,} keys, EXCLUDED from the total above")
        print("   They answer 451 and are hidden from the catalogue, so a difference "
              "between\n   their store and their catalogue is not a coverage gap at "
              "any row count.")
        for d, n, c in sorted(gatedsrc, key=lambda x: -x[1])[:10]:
            print(f"   {d:24s} store {n:>13,}  cat {c:>10,}   denylisted")
    if zerocat:
        # THE LOUDEST BUCKET, because grain cannot excuse it: a source with no catalogue
        # row at all reaches nobody, whatever grain it would be served at.
        # NOT split by gate any more: the gate is checked BEFORE this branch, so nothing
        # here is denylisted by construction. A dead "GATED BY DECISION - 0" heading reads
        # as a measurement and is worse than none.
        print(f"\nNO CATALOGUE ROWS AT ALL - {len(zerocat)} source(s), {zerogap:,} "
              f"store keys - INCLUDED in the total above")
        print("   Not a grain question. An id absent from the catalogue 404s "
              "(api/worker/src/series.ts),\n   so every key here is held and "
              "reachable by nobody, and none of it is gated - gated sources are "
              "reported above,\n   excluded. The grain shown says how many "
              "catalogue ROWS a fix needs, not how many series are unreachable.")
        for d, n, g in sorted(zerocat, key=lambda x: -x[1])[:15]:
            print(f"   {d:24s} store {n:>13,}  grain:{g or 'series (default)'}")
        if len(zerocat) > 15:
            print(f"   ... and {len(zerocat) - 15} more")
    if grainsrc:
        print(f"\nNOT COMPARABLE — {len(grainsrc)} source(s) at a DECLARED non-series "
              f"grain, {graingap:,} store keys")
        for d, n, c, g in sorted(grainsrc, key=lambda x: -x[1])[:15]:
            print(f"   {d:24s} store {n:>13,}  cat {c:>10,}  grain:{g}")
        if len(grainsrc) > 15:
            print(f"   ... and {len(grainsrc) - 15} more")
    if unkgrain:
        print(f"\nGRAIN UNESTABLISHED — {len(unkgrain)} source(s) with a bespoke resolver, "
              f"{unkgap:,} store keys — INCLUDED in the total above (unknown fails LOUD)")
        for d, n, c, g in sorted(unkgrain, key=lambda x: -x[1])[:15]:
            print(f"   {d:24s} store {n:>13,}  cat {c:>10,}  resolver:{g}")
        if len(unkgrain) > 15:
            print(f"   ... and {len(unkgrain) - 15} more")
    if unread:
        print(f"\nNOT MEASURED — {len(unread)} source(s) the run could not count:")
        for d, why in sorted(unread):
            print(f"   {d:24s} {why}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(ROOT, "logs", "store_audit.tsv"))
    ap.add_argument("--max-gb", type=float, default=0.0,
                    help="skip stores larger than this (0 = no bound)")
    ap.add_argument("--memory-limit", default="6GB")
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--summarise", metavar="TSV",
                    help="re-read a FINISHED tsv through the current grain index and print "
                         "the summary only. Measures nothing; the numbers are the ones that "
                         "run already produced. Use it when the classification changed but "
                         "the measurement did not, instead of spending hours re-scanning.")
    a = ap.parse_args()

    if a.summarise:
        return summarise(a.summarise)

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

    # FAIL CLOSED (R503): if the grain index cannot be built, the tool must NOT fall back to
    # treating every source as series grain — that is precisely the false total this exists
    # to prevent. It reports UNKNOWN and refuses to print the split.
    try:
        deny = denylisted()
    except Exception as e:                                     # noqa: BLE001
        deny = set()
        print(f"DENYLIST UNAVAILABLE ({type(e).__name__}: {e}) — gated sources will be "
              f"reported as forgotten ones. Do not act on the NO CATALOGUE ROWS bucket.",
              flush=True)
    try:
        grain = grain_index()
        grain_ok = True
    except Exception as e:                                     # noqa: BLE001
        grain, grain_ok = {}, False
        print(f"GRAIN INDEX UNAVAILABLE ({type(e).__name__}: {e}) — every gap below is\n"
              f"  UNQUALIFIED and the fleet total is NOT trustworthy. Fix the import before\n"
              f"  believing any figure this run prints.", flush=True)

    skipped_big, unc, orph = [], 0, 0
    graingap, grainsrc = 0, []   # DECLARED flow/dot-table/file grain: apart, never in `unc`
    unkgap, unkgrain = 0, []     # bespoke resolver: grain UNESTABLISHED, apart from BOTH
    zerogap, zerocat = 0, []     # ZERO catalogue rows: unreachable at ANY grain, in `unc`
    gategap, gatedsrc = 0, []    # DENYLISTED at any cat: 451, never a coverage gap
    nokey = []               # stores with no recognised key column — reported, never silent
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
            # WRITTEN TO THE TSV, not just printed: without a row, --summarise cannot see
            # that the run was bounded, and a bounded pass reads as full coverage.
            fh.write(f"{d}\t\t{cat}\t\tSKIPPED {gb:,.1f} GB > --max-gb\n"); fh.flush()
            print(f"[{i}/{len(names)}] {d:24s} SKIPPED — {gb:,.1f} GB > --max-gb {a.max_gb}",
                  flush=True)
            continue
        try:
            # THE KEY COLUMN IS NOT ALWAYS `series_key`, and assuming it was silently excluded
            # whole sources from this audit (R825/R821). bls keys on `series_id` and holds
            # 154,190,127 distinct series; eia likewise, at 3,862,801. Both were booked "not a
            # series store" and vanished from every total this tool printed - 157,784,417 series
            # of real gap, larger than most of what it did report. The candidate list is the one
            # core/broaden_catalog.py::_key_col also uses. NOTE, corrected after review: this
            # is a HAND-COPIED literal, so the two agree only by both being edited - exactly
            # the arrangement this file criticises for the grain sets. Sharing it for real
            # means importing one of them; until that is done, treat this as a duplicate.
            cols = set(pq.read_schema(files[0]).names)
            key = next((c for c in ("series_key", "series_id", "idbank") if c in cols), None)
            if key is None:
                nokey.append((d, cat, sorted(cols)[:6]))
                fh.write(f"{d}\t\t{cat}\t\tno key column\n"); fh.flush()
                print(f"[{i}/{len(names)}] {d:24s} NO KEY COLUMN — not measured "
                      f"(catalogued {cat:,}; columns {sorted(cols)[:6]})", flush=True)
                continue
            if key != "series_key":
                print(f"[{i}/{len(names)}] {d:24s} keyed on {key!r}, not series_key", flush=True)
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
            n = q.execute(f'select count(distinct "{key}") from '
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
                    f'"{key}")) from read_parquet({lst}, union_by_name=true, '
                    f"filename=true)").fetchone()[0]
                if abs(qn - cat) < abs(n - cat):
                    n, qualified = qn, True
        except Exception as e:                                 # noqa: BLE001
            # HLL FALLBACK, the method tools/series_census.py::_distinct_keys already uses:
            # exact where it completes, approximate only when it cannot, and SAY WHICH.
            # Without this the five biggest stores have no figure at all, so the fleet total
            # silently excludes the library's two largest sources.
            try:
                n = q.execute(f'select approx_count_distinct("{key}") from '
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
            g = grain.get(d)
            if d in deny and gap > 0:
                note = "GATED - 451, not a coverage gap"
                gategap += gap
                gatedsrc.append((d, n, cat))
            elif cat == 0 and n > 0:
                # Same rule as the exact path: zero rows is unreachable at any grain.
                note = f"UNCATALOGUED — 0 catalogue rows{f'  grain:{g}' if g else ''}"
                zerogap, unc = zerogap + n, unc + n
                zerocat.append((d, n, g))
            elif gap > 0 and g in DECLARED_GRAINS:
                note = f"grain:{g} — NOT a coverage gap"
                graingap += gap
                grainsrc.append((d, n, cat, g))
            elif gap > 0 and g:
                note = f"grain UNESTABLISHED ({g} resolver) — COUNTED as a gap"
                unkgap, unc = unkgap + gap, unc + gap
                unkgrain.append((d, n, cat, g))
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
        g = grain.get(d)
        if d in deny and gap > 0:
            # GATED FIRST, at any cat. A source that answers 451 is not a coverage gap, and
            # the zero-catalogue split alone missed the case measured on the real data: a
            # denylisted source WITH catalogue rows (26) whose 233-key gap printed as
            # unexplained. The gate outranks the grain.
            note = "GATED - 451, not a coverage gap"
            gategap += gap
            gatedsrc.append((d, n, cat))
        elif cat == 0 and n > 0:
            # ZERO CATALOGUE ROWS IS NOT A GRAIN QUESTION. Whatever the grain, a source with
            # no catalogue row at all reaches nobody - `series.ts` 404s an id absent from the
            # catalogue - so every one of these keys is held and unreachable, and belongs in
            # the headline. Booking them as "design" is how ilo (file grain, 29,447,518 keys,
            # 0 rows) read as intentional, and how imf and owid vanished from the total.
            # Listed in its OWN bucket so the two gap buckets keep partitioning cleanly.
            note = f"UNCATALOGUED — 0 catalogue rows{f'  grain:{g}' if g else ''}"
            zerogap, unc = zerogap + n, unc + n
            zerocat.append((d, n, g))
        elif gap > 0 and g in DECLARED_GRAINS:
            # A DESIGNED DIFFERENCE, NOT A DEFECT. Kept visible with its real number, but
            # never added to the coverage total, which would otherwise be dominated by it.
            note = f"grain:{g} — NOT a coverage gap"
            graingap += gap
            grainsrc.append((d, n, cat, g))
        elif gap > 0 and g:
            # A BESPOKE RESOLVER IS NOT A GRAIN CLAIM, AND UNKNOWN MUST FAIL LOUD. abs, bls
            # and bis all have one and are SERIES grain (exact-key predicates; their ids are
            # single series). Excusing them hid 532,044,393 reachable-by-nobody series, which
            # api/worker/src/catalog.ts:30 already names as a caught error, and R525 records.
            # So the gap COUNTS toward the headline; the listing below says it is unqualified.
            note = f"grain UNESTABLISHED ({g} resolver) — COUNTED as a gap"
            unkgap, unc = unkgap + gap, unc + gap
            unkgrain.append((d, n, cat, g))
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

    # CATALOGUED SOURCES WITH NO STORE DIRECTORY AT ALL. `names` comes from os.listdir(STORE),
    # so a source that is catalogued but has no directory under clean_full is never visited and
    # can never be reported — it is invisible to every verdict this tool prints, including
    # ORPHAN. Measured 2026-09-06: exactly one, sec_edgar at 17,467 catalogue rows, which is 75x
    # the orphan total the tool did report.
    #
    # It is NOT an orphan, and saying so would be the R289 error: serving reads clean_grouped/,
    # not clean_full/, so "prefix empty" is a false darkness signal for exactly this source. The
    # honest output is a NAMED omission that says which tree was searched.
    grouped_dir = os.path.join(ROOT, "data", "clean_grouped")
    grouped = set(os.listdir(grouped_dir)) if os.path.isdir(grouped_dir) else set()
    nostore = sorted((s, c) for s, c in counts.items() if c and s not in set(names))
    if nostore:
        print(f"\nNOT MEASURED — {len(nostore)} catalogued source(s) with no directory under "
              f"data/clean_full:")
        for s, c in nostore:
            where = ("present under data/clean_grouped — serving reads THAT tree (R289), so this "
                     "is not missing data" if s in grouped else
                     "absent from clean_grouped too — genuinely no local store")
            print(f"   {s:24s} catalogued {c:>10,}   {where}")

    fh.close()
    print(f"\nheld locally but not catalogued    : {unc:,} series"
          f"   <- SERIES-GRAIN sources only; NOT a claim about R2")
    if not grain_ok:
        print("   WARNING: the grain index failed to build, so this total is UNQUALIFIED "
              "and may be dominated by designed grain differences. Do not report it.")
    if gatedsrc:
        print(f"\nGATED WITH A GAP - {len(gatedsrc)} denylisted source(s), "
              f"{gategap:,} keys, EXCLUDED from the total above")
        print("   They answer 451 and are hidden from the catalogue, so a difference "
              "between\n   their store and their catalogue is not a coverage gap at "
              "any row count.")
        for d, n, c in sorted(gatedsrc, key=lambda x: -x[1])[:10]:
            print(f"   {d:24s} store {n:>13,}  cat {c:>10,}   denylisted")
    if zerocat:
        # THE LOUDEST BUCKET, because grain cannot excuse it: a source with no catalogue
        # row at all reaches nobody, whatever grain it would be served at.
        # NOT split by gate any more: the gate is checked BEFORE this branch, so nothing
        # here is denylisted by construction. A dead "GATED BY DECISION - 0" heading reads
        # as a measurement and is worse than none.
        print(f"\nNO CATALOGUE ROWS AT ALL - {len(zerocat)} source(s), {zerogap:,} "
              f"store keys - INCLUDED in the total above")
        print("   Not a grain question. An id absent from the catalogue 404s "
              "(api/worker/src/series.ts),\n   so every key here is held and "
              "reachable by nobody, and none of it is gated - gated sources are "
              "reported above,\n   excluded. The grain shown says how many "
              "catalogue ROWS a fix needs, not how many series are unreachable.")
        for d, n, g in sorted(zerocat, key=lambda x: -x[1])[:15]:
            print(f"   {d:24s} store {n:>13,}  grain:{g or 'series (default)'}")
        if len(zerocat) > 15:
            print(f"   ... and {len(zerocat) - 15} more")
    if grainsrc:
        print(f"\nNOT COMPARABLE — {len(grainsrc)} source(s) served at a NON-SERIES grain, "
              f"{graingap:,} store keys")
        print("   One catalogue row here deliberately stands for many store keys (flow, "
              "dot-table\n   or file grain, per clients/python/econdl/_resolve.py), so "
              "store-keys minus rows\n   measures the GRAIN and not coverage. To audit "
              "these, compare catalogue rows\n   against FLOWS / TABLES / FILES — never "
              "against keys.")
        for d, n, cat, g in sorted(grainsrc, key=lambda x: -x[1])[:15]:
            print(f"   {d:24s} store {n:>13,}  cat {cat:>10,}  grain:{g}")
        if len(grainsrc) > 15:
            print(f"   ... and {len(grainsrc) - 15} more (all rows are in the TSV)")
    if unkgrain:
        # THE ONE THAT MUST NOT READ AS CLEAN. These are not excused and not counted;
        # they are the work list. Resolving one means reading its resolver and deciding
        # whether its catalogue row means a key or a file.
        print(f"\nGRAIN UNESTABLISHED — {len(unkgrain)} source(s) with a bespoke resolver, "
              f"{unkgap:,} store keys — INCLUDED in the total above")
        print("   A bespoke resolver is NOT evidence of table grain, so these are counted "
              "as gaps\n   until someone shows otherwise: unknown must fail LOUD. abs, bls "
              "and bis all have\n   a bespoke resolver and are SERIES grain (exact-key "
              "predicates, ids naming one\n   series each) — excusing them would hide "
              "532,044,393 unreachable series, the error\n   api/worker/src/catalog.ts:30 "
              "already names and R525 records. Confirm each with\n   R525's positive test "
              "(do the catalogue rows carry a scalar frequency and geography?).")
        for d, n, cat, g in sorted(unkgrain, key=lambda x: -x[1])[:15]:
            print(f"   {d:24s} store {n:>13,}  cat {cat:>10,}  resolver:{g}")
        if len(unkgrain) > 15:
            print(f"   ... and {len(unkgrain) - 15} more (all rows are in the TSV)")
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
    if nokey:
        # DISCLOSED, never silent — the same rule --max-gb already follows. A guard keyed on one
        # column name once removed bls (154,190,127 series) and eia (3,862,801) from every total
        # this tool printed, without a line saying so.
        print(f"\nNOT MEASURED — {len(nokey)} store(s) with no recognised key column "
              f"(tried series_key, series_id, idbank):")
        for d, cat, cols in sorted(nokey):
            print(f"   {d:24s} catalogued {cat:>10,}   columns {cols}")
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
