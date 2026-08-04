"""Remove the WRONG-GRAIN rows from named tables, at TABLE grain, without touching the rest.

WHY NOT `repull_file.py`. That tool retires a whole store FILE so the next tick re-pulls it, and
it is the right answer when the file is small or wholly damaged. It is the wrong answer for
`scb/BE.parquet`: 1,553,817 rows of which 26,206 are affected. Retiring it re-pulls 264 tables
through MAX_CELLS-limited tailing queries and leaves a served database thin for many runs to
repair 1.7% of it. Same reasoning as `tools/cso_repull_matrix.py`, which exists because the
subject-grain route would have deleted 742 matrices to fix 60.

WHAT THE DAMAGE IS. A table whose real time axis could not be parsed had a CODE dimension read as
the year instead, and the real time dimension baked into the series_key. Measured 2026-08-04:

    statfin  tyonv 12tc.px   BAD  ...:timeperiod_m=2020M01:...   obs_date 3011-12-31
                             SANE ...:Koulutus=SSS:...           obs_date 2009-01-01
    hagstofa Umhverfi        BAD  ...UMH11150.px:Mælistöð=0:...  obs_date 3001-12-31
    scb      BE/HE           BAD  ...:Tid=1998-2002              obs_date 0114-12-31

3011 and 3001 are an education code and a measuring-station code; 0114 is a Swedish municipality.
The parsers are FIXED (R331/R333), so the correct-grain rows are already arriving alongside these.

THE CONTROL IS NOT ons_uk's. There (task #42) the two grains held byte-identical observations, so
the control could prove the old half was redundant and the prune lost nothing. Here the bad rows
carry REAL VALUES under FABRICATED DATES: deleting them removes observations that only a re-pull
can restore. So this tool proves something different and states it plainly —

    1. every row it removes is out of the sane date band (never a judgement call), and
    2. the table RETAINS sane-dated rows afterwards, so the prune cannot silently empty a table
       (an empty table is classified `empty`, which holds a whole source at `partial` forever),
    3. and it reports the observation count the source must re-fetch, rather than implying none.

A table that would be left with ZERO sane rows is REFUSED: that is not a prune, it is a deletion,
and it needs `repull_file.py` plus a dispatched backfill instead.

    python tools/prune_bad_grain_rows.py --dry-run
    python tools/prune_bad_grain_rows.py --apply --only scb
"""
from __future__ import annotations
import argparse
import datetime as dt
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import pyarrow.compute as pc                                  # noqa: E402
from updater import blob, config                              # noqa: E402

SANE_LO = dt.date(1500, 1, 1)
SANE_HI = dt.date(2200, 1, 1)

# (source, store file, series_key prefix identifying the table). Derived from
# tools/audit_impossible_dates.py --r2 plus a per-table breakdown; each entry was confirmed
# against the PUBLISHER's own dimension list before being put here.
TARGETS = [
    ("statfin", "tyonv.parquet", "tyonv:12tc.px"),
    ("hagstofa", "Umhverfi.parquet", "ICE:Umhverfi:1_natturufar:2_vedurfar:UMH11150.px"),
    ("hagstofa", "Umhverfi.parquet", "ICE:Umhverfi:1_natturufar:2_vedurfar:UMH11140.px"),
    ("hagstofa", "Umhverfi.parquet", "ICE:Umhverfi:1_natturufar:2_vedurfar:UMH11130.px"),
    ("scb", "HE.parquet", "HE:HE0110:HE0110H:TABIRH3"),
    ("scb", "HE.parquet", "HE:HE0110:HE0110H:TABIRH4"),
    ("scb", "HE.parquet", "HE:HE0110:HE0110H:TABIRH5"),
    ("scb", "BE.parquet", "BE:BE0101:BE0101I:DodaVeckaRegionCKM"),
    ("scb", "BE.parquet", "BE:BE0101:BE0101I:Medellivsl"),
]


def _masks(t, prefix):
    """(in_table, is_bad) boolean arrays over the whole table.

    Matches on `prefix + ":"`, not the bare prefix: a series_key is `<table>:<dim>=<val>:...`,
    so the separator is what makes the match a whole table id rather than a string prefix.
    Without it, a target of `...TABIRH3` would also select a sibling named `...TABIRH30`.
    Verified to select the identical row counts on today's store — this is protection against
    a future table name, not a correction of the numbers below.
    """
    keys = t.column("series_key").combine_chunks()
    dates = t.column("obs_date").combine_chunks()
    in_table = pc.starts_with(keys, pattern=prefix + ":")
    out_of_band = pc.or_(pc.less(dates, SANE_LO), pc.greater_equal(dates, SANE_HI))
    return in_table, pc.and_(in_table, out_of_band)


def main() -> int:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--apply", action="store_true")
    ap.add_argument("--only", action="append", help="limit to a source id (repeatable)")
    a = ap.parse_args()

    print(f"backend={config.BACKEND}   sane band {SANE_LO}..{SANE_HI}\n", flush=True)
    by_file: dict[tuple[str, str], list[str]] = {}
    for src, fn, pref in TARGETS:
        if a.only and src not in a.only:
            continue
        by_file.setdefault((src, fn), []).append(pref)

    pruned = refused = 0
    for (src, fn), prefixes in by_file.items():
        path = os.path.join(config.source_dir(src), fn)
        if not blob.exists(path):
            print(f"{src}/{fn}: ABSENT — skipped", flush=True)
            continue
        t = blob.read_table(path)
        print(f"{src}/{fn}: {t.num_rows:,} rows", flush=True)

        drop = None
        ok = True
        for pref in prefixes:
            in_table, is_bad = _masks(t, pref)
            n_tab = pc.sum(pc.cast(in_table, "int64")).as_py() or 0
            n_bad = pc.sum(pc.cast(is_bad, "int64")).as_py() or 0
            n_keep = n_tab - n_bad
            print(f"    {pref}", flush=True)
            print(f"        rows {n_tab:,}  out-of-band {n_bad:,}  would keep {n_keep:,}",
                  flush=True)
            if n_bad == 0:
                print(f"        already clean", flush=True)
                continue
            if n_keep == 0:
                ok = False
                refused += 1
                print(f"        REFUSING: pruning would leave this table EMPTY. That is a "
                      f"deletion, not a prune — an `empty` table holds the whole source at "
                      f"`partial`. Use tools/repull_file.py and dispatch a backfill instead.",
                      flush=True)
                continue
            print(f"        control OK: {n_keep:,} sane-dated row(s) remain, table stays alive",
                  flush=True)
            print(f"        NOTE: {n_bad:,} observation(s) carry real values under fabricated "
                  f"dates; they are NOT recoverable from this file and must be re-fetched.",
                  flush=True)
            drop = is_bad if drop is None else pc.or_(drop, is_bad)

        if drop is None or not ok:
            if not ok:
                print(f"    -> file left untouched because a table was refused\n", flush=True)
            else:
                print(f"    -> nothing to do\n", flush=True)
            continue

        n_drop = pc.sum(pc.cast(drop, "int64")).as_py() or 0
        kept = t.filter(pc.invert(drop))
        print(f"    total to drop: {n_drop:,} -> {kept.num_rows:,} rows remain", flush=True)
        if not a.apply:
            print(f"    (dry run — nothing written)\n", flush=True)
            continue

        # write_table_atomic, NOT merge_and_write: never-shrink would refuse this by design, and
        # re-running the guard here would only re-implement the checks above.
        blob.write_table_atomic(path, kept)
        # VERIFY FROM THE STORE (R296), not from the object just built.
        back = blob.read_table(path)
        still = 0
        for pref in prefixes:
            _, is_bad = _masks(back, pref)
            still += pc.sum(pc.cast(is_bad, "int64")).as_py() or 0
        print(f"    WROTE {back.num_rows:,} rows; out-of-band rows remaining: {still}", flush=True)
        assert back.num_rows == kept.num_rows and still == 0
        pruned += 1
        print(flush=True)

    print(f"{'PRUNED' if a.apply else 'would prune'}: {pruned} file(s); refused: {refused}")
    if refused:
        print("A refusal is the tool working. Those tables need a re-pull, not a prune.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
