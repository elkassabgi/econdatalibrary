#!/usr/bin/env python3
"""The third leg: how many OBJECTS does R2 hold per source, against the catalogue?

WHY THIS EXISTS. `tools/audit_store_vs_catalog.py` compares the CATALOGUE against the LOCAL
PARQUET STORE. Neither of those is what a user receives, and the tool cannot see R2 at all - which
is exactly how it came to print "hosted but not catalogued" about data that had never been
published (corrected 2026-09-06, R834). Its ORPHAN verdict had the mirror-image defect a day
earlier (R825, "ORPHAN is not a 404 count").

Three quantities, three different answers:

    local store keys   what we HOLD          data/clean_full/<source>/*.parquet
    R2 objects         what we HOST          series/<source>%3A...
    catalogue rows     what we LIST          data/catalog.db - THE LOCAL COPY.
                                             D1 is what SERVES users and the two
                                             disagree (measured 2026-09-07: D1 held
                                             +21 fed_board and +61 fhfa rows the
                                             local file did not), so a difference
                                             below is a question for D1, not a
                                             finding about what users can reach.

A gap between the first and the third is "held, not published" - the fix is a derive.
A gap between the second and the third is "published but unlisted", or "listed but the bytes
are missing" - the fix is a catalogue write, or a derive, or a delist. Those are different
jobs with different costs, and the words are not interchangeable.

AND THE STATUS CODE MATTERS. A catalogue row with no object does NOT 404. The worker pins an
honest-status tree in api/worker/src/series.ts and implements it:

    1. id not in catalog   -> 404 not_found
    4. R2 object ABSENT    -> 502 data_unavailable ("the object isn't published yet; loud +
                              actionable, never an empty 200")

404 means we never listed it; 502 means we listed it and have not published the bytes. This
file called the second one a 404 until 2026-09-07 - the R825 class of error, a served-system
claim made from a local measurement, with the wrong code attached.

MEASURED 2026-09-06, the run that prompted this tool:

    source     R2 objects     catalogue rows     local store keys
    abs                18                 18          376,333,085
    bls                 9                  9          154,190,127
    bis                49                 49            1,521,257
    ember              60                 60              255,898
    ecb                35                 35            3,733,574
    census          2,993              2,993              440,414
    gus                 0                  0              151,236

R2 and the catalogue agreed EXACTLY in all seven, which is what settled the question.

COST. `list_objects_v2` at 1,000 keys per page, so roughly one Class A call per 1,000 objects -
about $4.50 per million calls. Listing statcan's 466,341 keys is ~470 calls, well under a cent.
It touches D1 not at all. `--max` bounds a runaway prefix and SAYS it stopped rather than
reporting a truncated count as a total.

    python tools/audit_r2_vs_catalog.py abs bls bis
    python tools/audit_r2_vs_catalog.py --all          # every source with catalogue rows
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import urllib.parse

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def count_prefix(s3, bucket: str, prefix: str, cap: int) -> tuple[int, bool]:
    """Objects under `prefix`. Returns (count, truncated). Truncated is NEVER silent."""
    n, token = 0, None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kw["ContinuationToken"] = token
        r = s3.list_objects_v2(**kw)
        n += r.get("KeyCount", 0)
        token = r.get("NextContinuationToken")
        if not token:
            return n, False
        if cap and n >= cap:
            return n, True


def catalogue_counts() -> dict:
    con = sqlite3.connect(
        f"file:{os.path.join(ROOT, 'data', 'catalog.db')}?mode=ro", uri=True)
    try:
        return dict(con.execute("SELECT source_id, count(*) FROM series GROUP BY 1"))
    finally:
        con.close()


def store_only_sources(counts: dict) -> list:
    """Sources with a STORE directory but zero LOCAL catalogue rows.

    `--all` enumerates `catalogue_counts()`, which reads the local `catalog.db`. A source with no
    local row therefore never enters the run - so this audit cannot see it in EITHER direction,
    and prints nothing to say so. Measured 2026-09-07: `worldbank_pink` has 26 rows in D1 and 0
    locally, and was silently outside every `--all` run.

    Named rather than counted, and reported under NOT MEASURED, because the honest statement is
    "this run did not look", not "there is nothing there".
    """
    d = os.path.join(ROOT, "data", "clean_full")
    try:
        dirs = {n for n in os.listdir(d) if os.path.isdir(os.path.join(d, n))}
    except OSError:
        return []
    return sorted(n for n in dirs if not counts.get(n))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("sources", nargs="*")
    ap.add_argument("--all", action="store_true",
                    help="every source holding catalogue rows (hundreds of prefixes; slow)")
    ap.add_argument("--bucket", default="econ-data")
    ap.add_argument("--prefix", default="series")
    ap.add_argument("--max", type=int, default=2_000_000,
                    help="stop counting a single prefix past this and SAY SO, rather than "
                         "reporting a truncated figure as a total")
    a = ap.parse_args()

    counts = catalogue_counts()
    names = a.sources or (sorted(counts) if a.all else [])
    if not names:
        print("name at least one source, or pass --all")
        return 2

    from core import r2_util                                   # noqa: PLC0415  (boto3 late)
    s3 = r2_util.client()

    print(f"{'source':<24}{'R2 objects':>14}{'catalogue rows':>16}{'difference':>13}  verdict")
    agree = disagree = 0
    trunc_srcs, unchecked = [], []      # NOT MEASURED, and never silent in the total
    for src in names:
        pre = f"{a.prefix}/{urllib.parse.quote(src + ':', safe='')}"
        try:
            n, truncated = count_prefix(s3, a.bucket, pre, a.max)
        except Exception as e:                                 # noqa: BLE001
            # NEVER a silent skip. An unlistable prefix is UNCHECKED, not clean (R390).
            print(f"  {src:<22}{'UNCHECKED':>14}{counts.get(src, 0):>16,}"
                  f"{'':>13}  LIST FAILED {type(e).__name__}")
            unchecked.append((src, f"{type(e).__name__}"))
            continue
        cat = counts.get(src, 0)
        if truncated:
            print(f"  {src:<22}{n:>13,}+{cat:>16,}{'':>13}  STOPPED at --max — not a total")
            trunc_srcs.append((src, n, cat))
            continue
        d = n - cat
        if d == 0:
            verdict, agree = "agree", agree + 1
        elif d > 0:
            # "no LOCAL catalogue row". Measured 2026-09-07: fed_board's 21 and fhfa's 61 were
            # reported here as published-but-unlisted, and ALL 82 turned out to be present in
            # D1 - the local copy was simply behind. A difference here is a QUESTION for D1,
            # not an answer about users.
            verdict, disagree = ("OBJECTS WITH NO LOCAL CATALOGUE ROW — verify against D1 "
                                 "before calling them unlisted"), disagree + 1
        else:
            verdict, disagree = ("LOCAL CATALOGUE ROWS WITH NO OBJECT — if D1 lists them too, "
                                 "a user gets 502 data_unavailable"), disagree + 1
        print(f"  {src:<22}{n:>14,}{cat:>16,}{d:>+13,}  {verdict}")

    print()
    print(f"  {agree} source(s) where R2 and the catalogue agree; {disagree} where they do "
          f"not.")
    store_only = store_only_sources(counts) if a.all else []
    if store_only:
        # NEVER SILENT, and this one is invisible by construction: `--all` iterates the LOCAL
        # catalogue, so a source with no local row is not in the run at all. worldbank_pink has
        # 26 rows in D1 and 0 locally (measured 2026-09-07).
        print(f"  NOT MEASURED, and NOT in the totals above: {len(store_only)} source(s) hold a "
              f"store directory but ZERO local catalogue rows, so `--all` never enumerated them. "
              f"A source can be live in D1 with no local row - name it explicitly to audit it:")
        for sname in store_only[:40]:
            print(f"     {sname}")
        if len(store_only) > 40:
            print(f"     ... and {len(store_only) - 40} more")
    if trunc_srcs or unchecked:
        # NEVER SILENT. agree + disagree is NOT the number of sources asked about, and a
        # summary that omits the difference reads as full coverage - the exact failure
        # --max exists to prevent, one level up.
        print(f"  NOT MEASURED: {len(trunc_srcs)} stopped at --max, {len(unchecked)} could "
              f"not be listed. {agree + disagree} of "
              f"{agree + disagree + len(trunc_srcs) + len(unchecked)} sources were actually "
              f"compared.")
        for s, n, c in trunc_srcs:
            print(f"     {s:<22}stopped at {n:,}+ objects (catalogue {c:,}) — re-run with "
                  f"a larger --max")
        for s, why in unchecked:
            print(f"     {s:<22}listing failed: {why}")
    if disagree:
        # THE DISCLOSURE THAT WAS MISSING, and it cost two wrong entries in NUMBERS.md.
        # `catalogue_counts()` reads data/catalog.db. Users are served from D1. The two
        # disagree whenever a source is written straight to D1 (sec_edgar's refresher) or
        # synced from a machine this one has not caught up with - and when they disagree the
        # difference lands here looking exactly like a publishing gap.
        print()
        print("  THE `catalogue rows` COLUMN IS THE LOCAL data/catalog.db, NOT D1. A non-zero")
        print("  difference is a QUESTION, not a finding: check the ids against D1 by primary")
        print("  key (an index seek, ~2 rows read per id, free) before calling anything")
        print("  unlisted. On 2026-09-07 all 82 of fed_board's 21 and fhfa's 61 'unlisted'")
        print("  objects were present in D1; the local copy was behind. Two istat objects were")
        print("  genuinely absent from D1, and two eurostat ids were genuinely 502.")
    print()
    print("  This says nothing about the LOCAL STORE — a source can agree here and still hold")
    print("  hundreds of millions of unpublished keys locally. That is "
          "tools/audit_store_vs_catalog.py.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
