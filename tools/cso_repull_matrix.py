"""Re-pull ONLY the cso matrices whose time axis was swapped — not the subjects that contain them.

WHY THIS EXISTS BESIDE cso_repull_subject.py. That tool is correct and its ordering (cursor first,
delete second) is the part worth keeping. Its GRAIN is wrong for this defect. It removes a whole
subject parquet, and the defect is not subject-shaped: measured on the store 2026-08-03, 290 of
7,892 matrices carry the swap, holding 754,780 of 49,204,621 rows (1.5%). Fixing the 60 bad
matrices inside 10_Census_2016 by the subject route would delete 742 matrices and 3,274,801 rows,
of which 2,970,636 are correct and already served — discarding 97% of a file to repair 3% of it,
and taking the whole subject offline for the ~14 runs the re-pull needs.

WHAT THE DEFECT IS. cso's ingester picked the time axis by first-match, so on matrices where a
classification axis precedes the real TLIST axis the two were SWAPPED: the classification code
landed in obs_date and the year was baked into the series_key. The signature is exact and needs no
threshold — a series_key containing `TLIST(A1)=1991` is definitionally wrong, because time varies
per observation and therefore cannot be part of a series identity.

    CSO:AAA01:STATISTIC=AAA01:C02196V02652=-:TLIST(A1)=1991   obs_date 0111-12-31 .. 0322-12-31
              ^ classification value lost, placeholder '-'    ^ 12 crop codes, not 12 dates
                                          ^ the year, in the key

DO NOT "REPAIR" THIS BY READING THE YEAR OUT OF THE KEY. It looks trivially fixable and it is a
data-loss trap: those 12 rows are 12 distinct series at ONE year, so setting obs_date := 1991 for
all of them collapses 12 rows onto one (series_key, obs_date) pair and dedup keeps ONE. Eleven of
twelve rows are destroyed, every remaining date is sane, and every instrument reports success. The
negative control that caught this: across the store, 34,179 rows whose key carries TLIST DISAGREE
with their own obs_date year against 1,217 that agree — so the key's year is not the row's year.

The parser is already fixed (jobs/ingest_cso_ireland.py resolves an explicitly named time axis —
TLIST*, role.time, TIME/YEAR/PERIOD/TID — in a first pass, ahead of the >=60% heuristic). A
re-pull therefore produces authoritative keys. Reconstruction here would only produce inferred
ones, and R22 is explicit: a parser SELECTION fix changes series_keys, so it needs a clean re-pull,
never a merge.

ORDER IS LOAD-BEARING, and is the one thing taken unchanged from the subject tool. The cursor is
pruned and VERIFIED FIRST, the rows are removed second. The other order loses data permanently: a
matrix whose rows are gone while `_collupd.json` still lists it as current is never re-fetched, and
nothing anywhere would report it missing.

    python tools/cso_repull_matrix.py              # dry run: what would change
    python tools/cso_repull_matrix.py --apply
"""
from __future__ import annotations
import argparse
import datetime as dt
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import pyarrow as pa                                               # noqa: E402
from updater import blob, config                                   # noqa: E402
# IMPORTED, not re-implemented. R5 says the store is single-writer and this tool mutates both the
# cursor and the parquets, so it needs the same in-flight guard the subject tool has. A second copy
# of that check is a second thing to go stale, and the one that goes stale is always the copy.
from tools.cso_repull_subject import runs_in_flight                # noqa: E402
from tools._store_banner import banner                             # noqa: E402

SOURCE = "cso"
# A key holding an explicit time value. This is the defect signature itself, not a proxy for it:
# time cannot be part of a series identity, so any key matching this was written under the swap.
TLIST_IN_KEY = re.compile(r"TLIST\([AQMH]\d\)=")


def matrix_of(key: str) -> str:
    """CSO:<matrix>:<dims...> -> <matrix>."""
    parts = (key or "").split(":")
    return parts[1] if len(parts) > 1 else ""


def scan(out_dir):
    """{file: (bad_matrices, rows_to_drop, total_rows)} for every file holding a swapped matrix."""
    found = {}
    for fn in blob.list_parquets(out_dir):
        t = blob.read_table(os.path.join(out_dir, fn), columns=["series_key"])
        keys = t.column("series_key").to_pylist()
        if not keys:
            continue
        bad = {matrix_of(k) for k in keys if k and TLIST_IN_KEY.search(k)}
        bad.discard("")
        if not bad:
            continue
        # Every row of a swapped matrix goes, not just the rows with an impossible date. The
        # swap applies to the matrix as a whole; a row whose classification code happens to look
        # like a plausible year is corrupt in exactly the same way, just undetectably so.
        drop = sum(1 for k in keys if matrix_of(k) in bad)
        found[fn] = (bad, drop, len(keys))
    return found


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="make the changes (default: dry run)")
    a = ap.parse_args()

    out_dir = config.source_dir(SOURCE)
    # Say which store this is about to read and (with --apply) rewrite. R296: the same tool run
    # against the local mirror produces numbers that look right and describe the wrong world.
    banner(SOURCE, out_dir)
    found = scan(out_dir)
    if not found:
        print("no swapped matrices on the store — nothing to do")
        return 0

    all_bad, tot_drop, tot_rows = set(), 0, 0
    print(f"{'file':<42}{'rows':>11}{'to drop':>11}{'matrices':>11}")
    for fn, (bad, drop, rows) in sorted(found.items(), key=lambda kv: -kv[1][1]):
        all_bad |= bad
        tot_drop += drop
        tot_rows += rows
        print(f"{fn:<42}{rows:>11,}{drop:>11,}{len(bad):>11,}")
    print(f"\n{len(all_bad):,} swapped matrices across {len(found)} file(s)")
    print(f"{tot_drop:,} rows removed; {tot_rows - tot_drop:,} rows in these files stay served")

    # AVAILABILITY COST, stated up front — the subject tool's habit, kept. The matrices below are
    # unavailable from when their rows are removed until the fetcher re-pulls them. Everything
    # else in the same file stays downloadable throughout, which is the whole point of this grain.
    print(f"\nAVAILABILITY: {len(all_bad):,} matrices go dark until re-pulled. The fetcher walks "
          f"changed matrices per run, so this is a handful of runs, not one per subject.")

    if not a.apply:
        print("\nDRY RUN — nothing written. Re-run with --apply.")
        return 0

    # R5: single-writer. Both mutations below race a live run — the cursor prune can be overwritten
    # by the run's own write, and a parquet rewritten under an in-flight merge can lose either side.
    # Checked only on --apply so the dry run stays usable while the daily job is running.
    busy = runs_in_flight()
    if busy:
        print("\nREFUSING to apply — the store is not idle:")
        for w in busy:
            print(f"    {w}")
        print("Re-run when the run finishes. Nothing was written.")
        return 1

    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    # ---- 1. CURSOR FIRST. If this fails, the store is untouched and nothing is lost. --------
    cur_path = os.path.join(out_dir, "_collupd.json")
    stored = {}
    if blob.exists(cur_path):
        stored = json.loads((blob.read_bytes(cur_path) or b"{}").decode("utf-8"))
    pruned = {k: v for k, v in stored.items() if k not in all_bad}
    dropped = len(stored) - len(pruned)
    blob.write_bytes_atomic(os.path.join(out_dir, f"_collupd.backup-{stamp}.json"),
                            json.dumps(stored).encode("utf-8"))
    blob.write_bytes_atomic(cur_path, json.dumps(pruned).encode("utf-8"))
    check = json.loads((blob.read_bytes(cur_path) or b"{}").decode("utf-8"))
    still = sorted(all_bad & set(check))
    if still:
        print(f"  ABORT: {len(still)} target matrices still in the cursor after the write "
              f"({still[:5]}). Refusing to remove any rows.")
        return 1
    print(f"\ncursor: {dropped} of {len(stored)} entries dropped, verified absent. "
          f"(Matrices not yet cursored already read as changed.)")

    # ---- 2. ROWS SECOND, each file backed up and the backup proved readable before writing. --
    for fn, (bad, drop, rows) in sorted(found.items()):
        path = os.path.join(out_dir, fn)
        r2 = blob.R2Blob()
        key = blob._path_to_key(path)
        backup = f"_backup/cso_repull_matrix/{stamp}/{fn}"
        r2.client.copy_object(Bucket=r2.bucket, Key=backup,
                              CopySource={"Bucket": r2.bucket, "Key": key})
        if not r2.exists(backup):
            print(f"  ABORT on {fn}: backup not readable after copy — original untouched.")
            return 1
        t = blob.read_table(path)
        keep_mask = [matrix_of(k) not in bad for k in t.column("series_key").to_pylist()]
        kept = t.filter(pa.array(keep_mask, pa.bool_()))
        blob.write_table_atomic(path, kept)
        after = blob.row_count(path)
        if after != rows - drop:
            print(f"  WARNING {fn}: expected {rows - drop:,} rows after removal, store has "
                  f"{after:,} — inspect before trusting this file.")
        else:
            print(f"  {fn}: {rows:,} -> {after:,} ({drop:,} removed), backup r2://{backup}")

    print(f"\nDONE. {len(all_bad):,} matrices queued for a clean re-pull by the normal fetcher.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
