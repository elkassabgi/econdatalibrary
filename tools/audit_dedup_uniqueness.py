"""Is a store's dedup key actually a KEY? Run this before tailing a source incrementally.

WHY. merge_and_write dedups on (series_key, obs_date). If a store already holds several rows
sharing that pair, the first incremental merge collapses them — the file does not gain the
tail, it loses most of itself. never-shrink refuses the write, so the data survives, but the
symptom is a baffling "refusing shrink 5,910->15" that looks like a fetcher bug and is nothing
of the kind: the STORE was never uniquely keyed.

FOUND THE HARD WAY, TWICE. comtrade had to be re-keyed before it could ever auto-update
(task #16). Then on 2026-08-03, while adding census families to the date tail, bds looked like
an easy win — measurably behind (2022 stored, 2023 published), fetches cleanly, and every one
of the 5,910 rows it returns maps to a key the store already holds. It also holds 5,910 rows
under FIFTEEN distinct (series_key, obs_date) pairs. Enabling it would have tried to collapse
99.7% of the file. Nothing in the fetch or the key mapping hinted at it; the only tell was
counting the pairs.

So this is the check that should precede "just add the family". It is cheap relative to being
wrong: two columns, one group_by per file.

    python tools/audit_dedup_uniqueness.py census
    python tools/audit_dedup_uniqueness.py census --prefix intltrade__
    python tools/audit_dedup_uniqueness.py comtrade bds --quiet-ok
"""
from __future__ import annotations
import argparse
import collections
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import blob, config                                   # noqa: E402

DEFAULT_KEY_COLS = ("series_key", "obs_date")

# SOURCES WHOSE DEDUP KEY IS COMPUTED PER FILE, so it cannot be read off a module constant.
# treasury builds tuple(_identity_keys(["series_key","obs_date"] + out_cols)) — series_key there
# is the ENDPOINT PATH and is constant within a file, so the identity is the dimension columns.
# Auditing it against the default pair says 166 of its 181 files are under-keyed, and every one
# of those is a FALSE POSITIVE. Skipped loudly rather than mis-measured.
_COMPUTED_KEY = {"treasury"}


def dedup_key_for(source: str) -> tuple:
    """(key_cols, provenance) — provenance is "declared", "assumed" or "unimportable".

    Hardcoding ("series_key","obs_date") is what made the first version of this tool wrong. Of
    18 live extend_by_date sources, three differ: treasury computes its key per file, ofr uses
    ("series_id","obs_date"), and worldbank_esg uses ("country","obs_date") because it has no
    series_key column at all. Read the constant from the module; do not assume it.

    THE PROVENANCE IS PART OF THE ANSWER (R281's second half, closed 2026-08-30 via R527).
    The old version fell back to the default SILENTLY when a fetcher declared no DEDUP —
    `eia.py` and `usda.py` declare none, and eia's store has no `series_key` column at all, so
    every eia file skipped the column check and the tool printed "0 under-keyed" while having
    measured NOTHING. That zero then fed a collision census, which fed an authorisation request
    to Ahmed built on the wrong numbers. A fallback that can absorb the whole answer must
    announce itself, and the caller must treat "assumed" differently from "declared".
    """
    import importlib
    try:
        mod = importlib.import_module(f"updater.strategies.fetchers.{source}")
    except Exception:                                              # noqa: BLE001
        return DEFAULT_KEY_COLS, "unimportable"
    d = getattr(mod, "DEDUP", None)
    return (tuple(d), "declared") if d else (DEFAULT_KEY_COLS, "assumed")


# Distinct pairs are counted exactly, in memory, so the cost scales with the DISTINCT count.
# A set of packed ints runs about 60-70 bytes an entry once CPython's set overhead is included,
# so 50M pairs is roughly 3.5 GB. Past this the file is reported UNMEASURED rather than allowed
# to consume the machine: refusing to answer is a result, being killed is not.
MAX_EXACT_PAIRS = 50_000_000
_PACK_BASE = 1 << 32           # per-column ordinal space; no real column comes near 4.3e9

Audit = collections.namedtuple("Audit", "rows keys pairs capped")


def audit_file(path: str, key_cols: tuple, batch_size: int = 1_000_000):
    """Audit -> (rows, distinct_first_col, distinct_key_tuples, capped), or None if no columns.

    THIS STREAMS, AND THAT IS THE WHOLE POINT (R806). The previous version read the entire table
    and called `t.group_by(list(key_cols)).aggregate([]).num_rows`. pyarrow's hash aggregate
    FAST-FAILS the process on a large table — exit 0xC0000409 (STATUS_STACK_BUFFER_OVERRUN), no
    exception, no traceback, and because stdout is buffered when redirected, no output at all.
    Measured 2026-09-06: it survives imf's largest file at 6,300,194 rows and dies on cso's at
    29,760,740 and vdem's at 77,371,121, while `pc.count_distinct` over the same column of the
    same table succeeds. That crash made 17 of 379 stores unmeasurable in the first fleet sweep —
    statcan, eurostat, cbs_nl, oecd, istat and gus_dbw among them — and a crash with no output
    is indistinguishable from a tool that simply printed nothing.

    `blob.iter_batches` exists for precisely this case; its own docstring says "use this for any
    scan whose result is an AGGREGATE rather than the table itself". It is R2-routed like every
    other read here, so this keeps working under AQUEDUCT_BACKEND=r2.

    EXACT, NEVER APPROXIMATE. Each column's values are mapped to ordinals and the row's key is
    packed into one integer, so the set holds ints rather than tuples. An approximate distinct
    count would be cheaper and is exactly the class of answer R330 exists to forbid: this tool's
    verdict decides whether a store can be tailed incrementally at all.
    """
    try:
        schema = blob.read_schema(path)
    except Exception:                                              # noqa: BLE001
        return None
    if not all(c in schema.names for c in key_cols):
        return None

    rows = 0
    ordinals = [dict() for _ in key_cols]      # value -> ordinal, per key column
    seen = set()
    capped = False
    # `batch_size` is a parameter ONLY so a test can force more than one batch. At the default
    # 1,000,000 `iter_batches` coalesces small row groups into a single batch, so a two-row-group
    # fixture yields ONE batch and a per-batch-aggregate mutant passes a test named for catching
    # exactly that. Production never passes this.
    for batch in blob.iter_batches(path, columns=list(key_cols), batch_size=batch_size):
        rows += batch.num_rows
        if capped:
            # ROW COUNT ONLY. `to_pylist()` used to run before this check, so a capped file
            # still decoded every remaining batch to no purpose: measured on cso's
            # 29,760,740-row file, 80.4 s against 7.2 s for the same answer, and about 39
            # minutes wasted on statcan's largest cube.
            continue
        cols = [batch.column(c).to_pylist() for c in key_cols]
        for vals in zip(*cols):
            packed = 0
            for v, table in zip(vals, ordinals):
                o = table.get(v)
                if o is None:
                    o = table[v] = len(table)
                packed = packed * _PACK_BASE + o
            seen.add(packed)
        if len(seen) > MAX_EXACT_PAIRS:
            capped = True
            # BOTH structures, because `seen` is not the larger one. Measured at 5M entries and
            # scaled: `seen` is ~74.8 B/entry (3.48 GB at the cap) while `ordinals[0]` at idb's
            # real key width (mean 89 chars) is ~201.2 B/entry — 9.37 GB, and capping only the
            # set left the bigger dict growing unbounded.
            seen.clear()
            for t in ordinals:
                t.clear()
    if rows == 0:
        return Audit(0, 0, 0, False)
    if capped:
        return Audit(rows, -1, -1, True)        # neither count survived; say so, do not imply 0
    # `pc.count_distinct` (the previous implementation) EXCLUDES nulls and `len(ordinals[0])`
    # does not, so a null first key column would have silently changed this number by one.
    keys = len(ordinals[0]) - (1 if None in ordinals[0] else 0)
    return Audit(rows, keys, len(seen), False)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("sources", nargs="+")
    ap.add_argument("--prefix", default="", help="only files whose name starts with this")
    ap.add_argument("--quiet-ok", action="store_true", help="print only the under-keyed files")
    ap.add_argument("--key", default=None,
                    help="comma-separated key columns, overriding the fetcher's DEDUP. For a "
                         "source that declares none and whose schema lacks the default pair "
                         "(eia keys on series_id and stores the hour in `period`), this is the "
                         "only way to measure it at all: --key series_id,obs_date,period")
    a = ap.parse_args()

    bad_total = 0
    measured_nothing = 0
    unmeasured_total = 0
    for source in a.sources:
        if source in _COMPUTED_KEY:
            print(f"\n{source}: SKIPPED — its dedup key is computed per file "
                  f"(series_key is the endpoint path and is constant within a file, so the "
                  f"identity is the dimension columns). Auditing it against "
                  f"{'/'.join(DEFAULT_KEY_COLS)} reports every file under-keyed and every one "
                  f"of those is a false positive.")
            continue
        if a.key:
            key_cols, provenance = tuple(c.strip() for c in a.key.split(",")), "explicit"
        else:
            key_cols, provenance = dedup_key_for(source)
        if provenance in ("assumed", "unimportable"):
            print(f"\n{source}: WARNING — fetcher declares no DEDUP ({provenance}); auditing "
                  f"against the ASSUMED default {'/'.join(key_cols)}. A verdict under an "
                  f"assumed key is a hypothesis, not a measurement; pass --key to be explicit.")
        d = config.source_dir(source)
        try:
            files = [f for f in blob.list_parquets(d, recursive=True)
                     if not os.path.basename(f).startswith("_")]
        except Exception as e:                                     # noqa: BLE001
            print(f"{source}: cannot list ({type(e).__name__}: {e})")
            continue
        files = [f for f in files if os.path.basename(f).startswith(a.prefix)]
        print(f"\n{source}: {len(files)} file(s)"
              + (f" matching {a.prefix!r}" if a.prefix else ""))
        checked = skipped = bad = unmeasured = 0
        for rel in sorted(files):
            r = audit_file(os.path.join(d, rel), key_cols)
            if r is None:
                skipped += 1
                continue
            rows, keys, pairs, capped = r
            if capped:
                # The distinct count was abandoned, so this file has NO verdict. Counted
                # separately from `skipped`, which means "the key does not apply here" — a
                # different statement about a different thing.
                unmeasured += 1
                print(f"  UNMEASURED   {rel}")
                print(f"      rows={rows:,} — more than {MAX_EXACT_PAIRS:,} distinct key "
                      f"tuples, which will not fit in memory to be counted exactly. "
                      f"UNKNOWN, not clean.")
                continue
            checked += 1
            if pairs < rows:
                bad += 1
                bad_total += 1
                print(f"  UNDER-KEYED  {rel}")
                print(f"      rows={rows:,}  distinct {key_cols[0]}={keys:,}  "
                      f"distinct {key_cols}={pairs:,}")
                print(f"      -> a merge dedup would collapse {rows - pairs:,} row(s) "
                      f"({(rows - pairs) / max(rows, 1) * 100:.1f}% of the file). "
                      f"Re-key before tailing this incrementally.")
            elif not a.quiet_ok:
                print(f"  ok           {rel}  rows={rows:,}  keys={keys:,}")
        print(f"  checked {checked}, under-keyed {bad}"
              + (f", UNMEASURED {unmeasured}" if unmeasured else "")
              + (f", skipped {skipped} without {'/'.join(key_cols)}" if skipped else ""))

        # "0 DEFECTS IN 0 FILES EXAMINED IS NOT A RESULT" (R330, and how eia reported clean).
        # When every file skipped the column check, the key does not describe this store at
        # all, and the audit measured nothing — that is a failure of the AUDIT, and it must
        # not exit 0 wearing the same face as a genuine pass. eia: 30 files, all lacking
        # `series_key`, previously summarised as "checked 0, under-keyed 0" -> exit 0.
        # THE COUNTER HAS TO LEAVE THE LOOP. `unmeasured` was local, so a store whose every file
        # exceeded the cap printed "0 under-keyed file(s)" and exited 0 — turning R806's loud
        # crash into a silent pass, which is worse. 16 of the 18 stores R806/R809 name hold a
        # file above the cap (statcan 962,150,400 rows; oecd 1,792,000,000; cbs_nl
        # 1,886,692,500; unctad_biotrademerch 2,291,982,918), so this was almost all of them.
        unmeasured_total += unmeasured
        if files and checked == 0 and unmeasured == 0:
            measured_nothing += 1
            print(f"  *** MEASURED NOTHING: all {len(files)} file(s) lack "
                  f"{'/'.join(key_cols)}. The key is wrong for this store, not the store "
                  f"clean. Declare DEDUP in the fetcher or re-run with --key.")
        elif files and checked == 0 and unmeasured:
            # They do NOT lack the columns — saying so would send the reader to the fetcher to
            # declare a DEDUP that is already there (R800: a driver read this tool's prose).
            print(f"  *** NO VERDICT: all {len(files)} file(s) exceeded the "
                  f"{MAX_EXACT_PAIRS:,}-pair exact-count cap. The columns are present; the "
                  f"store is too large to count in memory. UNKNOWN, not clean.")

    # A NON-ZERO EXIT so this can gate a change rather than merely inform one. The whole point
    # is to be run BEFORE enabling a tail, and a check nobody can wire into a script is a check
    # that gets skipped.
    print(f"\n{bad_total} under-keyed file(s) across {len(a.sources)} source(s)"
          + (f"; {measured_nothing} source(s) MEASURED NOTHING" if measured_nothing else "")
          + (f"; {unmeasured_total} file(s) UNMEASURED (over the "
             f"{MAX_EXACT_PAIRS:,}-pair cap)" if unmeasured_total else ""))
    return 1 if (bad_total or measured_nothing or unmeasured_total) else 0


if __name__ == "__main__":
    raise SystemExit(main())
