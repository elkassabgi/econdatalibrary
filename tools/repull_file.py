"""Retire a store file so the next run re-pulls it cleanly — the R22 operation, made reversible.

WHY A CLEAN RE-PULL AND NOT A MERGE. When a parser's time-axis SELECTION changes, the keys
change with it: the dimension that was wrongly treated as time moves out of the series_key and
the real one moves in. Merging then writes the corrected rows ALONGSIDE the old wrong ones —
both are "new" to a dedup on (series_key, obs_date) — and merge's never-shrink guard cannot see
the duplication because the file only grows. R22: delete and re-ingest, never merge.

WHAT THIS IS FOR. Measured 2026-08-03 by tools/audit_impossible_dates.py, seven sources hold
observations dated past 2200. Two groups need exactly this operation:
  * cso — 11 files, 434,408 rows, parser fixed this session (it was the only ingester of eleven
    not routing through core/pxweb.resolve_time_dim);
  * cbs_nl / statfin / stat_slovenia / hagstofa — parsers ALREADY correct since 2026-07-21;
    the rows are legacy and cannot age out, because merge never shrinks.

REVERSIBLE BY CONSTRUCTION. The object is COPIED to a dated `_backup/` key before it is removed,
server-side, so nothing depends on this machine holding a copy. That is what makes this safe to
run without it being a reserved decision: the data is both re-crawlable AND retained.

    python tools/repull_file.py cso 10_Census_2016.parquet
    python tools/repull_file.py cso 10_Census_2016.parquet --apply
"""
from __future__ import annotations
import argparse
import datetime as dt
import json
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import pyarrow.compute as pc                                  # noqa: E402
from updater import blob, config                              # noqa: E402

IMPOSSIBLE_AFTER = dt.date(2200, 1, 1)
# The LOW side matters at least as much: a counter-as-year starts at 1, so a future-only test
# misses most fabrication. It reported 273,980 rows when the real figure was ~637,000 (R322).
IMPOSSIBLE_BEFORE = dt.date(1500, 1, 1)


def runs_in_flight() -> list:
    why = []
    lock = os.path.join(ROOT, "logs", "local_heavy.lock")
    if os.path.exists(lock):
        why.append(f"workstation lock held ({lock})")
    try:
        out = subprocess.run(
            ["gh", "run", "list", "--workflow=updater-daily.yml", "--limit", "5",
             "--json", "status,databaseId"],
            capture_output=True, text=True, timeout=60, cwd=ROOT)
        if out.returncode == 0:
            for r in json.loads(out.stdout or "[]"):
                if r.get("status") in ("in_progress", "queued", "requested", "waiting"):
                    why.append(f"CI updater-daily {r['databaseId']} is {r['status']}")
    except Exception as e:                                     # noqa: BLE001
        why.append(f"could not check CI ({type(e).__name__}) — refusing to assume it is idle")
    return why


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("source")
    ap.add_argument("filename", help="name RELATIVE to the source dir, e.g. 10_Census_2016.parquet")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--force-unsafe", action="store_true")
    ap.add_argument("--cursor-cleared", action="store_true",
                    help="for a CHANGE-DRIVEN source: confirm you have already dropped this "
                         "file's entries from the fetcher's cursor sidecar, without which the "
                         "delete is permanent")
    a = ap.parse_args()

    blockers = runs_in_flight()
    if blockers and not a.force_unsafe:
        print("REFUSING — a writer may be active (R5, single-writer store):")
        for b in blockers:
            print(f"  - {b}")
        return 2

    # DELETING A FILE ONLY REBUILDS IT IF THE FETCHER NOTICES IT IS GONE. That is true of a
    # snapshot fetcher, which re-pulls whatever the publisher currently offers, and FALSE of a
    # change-driven one, which pulls only what the publisher reports as CHANGED since a stored
    # cursor. cso is the measured case: `changed = [m for m,u in cur_upd.items() if
    # stored.get(m) != u]`. Delete a subject parquet and its cursor still says "we hold the
    # current version of every matrix", so nothing is re-fetched — the rows are simply gone
    # until CSO happens to revise each table. That would have been 3.9M rows deleted and not
    # returned, from a tool whose whole premise is that they come back.
    #
    # So: refuse unless the caller has cleared the cursor too, and name the file to clear.
    _CURSOR_DRIVEN = {
        "cso": "_collupd.json",       # per-matrix LastUpdated; entries must be dropped as well
    }
    if a.source in _CURSOR_DRIVEN and not a.cursor_cleared:
        print(f"REFUSING — {a.source} is a CHANGE-DRIVEN fetcher. It pulls what the publisher "
              f"reports as changed, not what is missing locally, so deleting this file does "
              f"NOT make it come back.")
        print(f"  Clear the matching entries from {_CURSOR_DRIVEN[a.source]} in the store first, "
              f"then re-run with --cursor-cleared.")
        print(f"  Without that the rows are gone until the publisher happens to revise each "
              f"table — which is not a re-pull, it is a deletion.")
        return 2

    d = config.source_dir(a.source)
    path = os.path.join(d, a.filename)
    if not blob.exists(path):
        print(f"REFUSING — {path} does not exist. Nothing to retire; check the name against "
              f"`blob.list_parquets` rather than assuming the store layout.")
        return 2

    # State the CONSEQUENCE, not just the selection (R263): how much goes away, how much of it
    # was the thing we are fixing, and therefore how much good data rides along.
    t = blob.read_table(path, columns=["obs_date"])
    total = t.num_rows
    col = t.column("obs_date").combine_chunks()
    bad_hi = pc.sum(pc.cast(pc.greater(col, IMPOSSIBLE_AFTER), "int64")).as_py() or 0
    bad_lo = pc.sum(pc.cast(pc.less(col, IMPOSSIBLE_BEFORE), "int64")).as_py() or 0
    bad = bad_hi + bad_lo
    print(f"{a.source}/{a.filename}")
    print(f"  rows in file            : {total:,}")
    print(f"  rows dated past {IMPOSSIBLE_AFTER.year}    : {bad_hi:,} ({bad_hi/max(total,1)*100:.1f}%)")
    print(f"  rows dated before {IMPOSSIBLE_BEFORE.year}  : {bad_lo:,} ({bad_lo/max(total,1)*100:.1f}%)")
    # NOT "rows that are FINE". A range test cannot certify the rest, and saying it can is how
    # 05W got called 42% damaged when it was 99.7% damaged: its fabricated dates are SETTLEMENT
    # CODES, and codes 1500..6152 land inside any sane calendar window. Measured on stat_slovenia
    # 05W — 506,605 rows, this line once read "291,830 FINE", the true figure was 1,463 (the ten
    # tables carrying a real LETO/year axis; the other 23 were settlement counters end to end).
    # A counter that starts at 1 walks THROUGH the plausible band on its way out of it, so
    # in-range is not evidence of correctness. Say what was tested, and only that. R322/R329.
    print(f"  rows OUTSIDE the impossible bands           : {total - bad:,}")
    print(f"      ^ IN-RANGE ONLY — not a clean bill of health. A code-as-year counter passes")
    print(f"        through 1500..2200; confirm per TABLE against the publisher's dimensions")
    print(f"        before believing any of these are real observations.")
    if not bad:
        print("\n  This file has no impossible dates. If you are retiring it for another "
              "reason, say so explicitly — this tool's checks are about THIS defect.")

    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = f"_backup/repull/{a.source}/{stamp}/{a.filename}"
    print(f"\n  backup -> r2://{backup}")
    print(f"  then DELETE the live object, so the next run re-pulls it with the corrected parser")

    if not a.apply:
        print("\n--dry run: nothing copied, nothing deleted. Re-run with --apply.")
        return 0

    r2 = blob.R2Blob()
    key = blob._path_to_key(path)
    # Server-side copy: the backup must not depend on this machine holding the bytes.
    r2.client.copy_object(Bucket=r2.bucket, Key=backup,
                          CopySource={"Bucket": r2.bucket, "Key": key})
    if not r2.exists(backup):
        print("  ABORT: backup not readable after copy — refusing to delete the original.")
        return 1
    print(f"  backup verified ({backup})")

    r2.client.delete_object(Bucket=r2.bucket, Key=key)
    if blob.exists(path):
        print("  ABORT: object still present after delete — investigate before re-running.")
        return 1
    print(f"  deleted {key}")
    print(f"\n  NEXT: dispatch the fetcher, e.g.\n"
          f"    gh workflow run updater-daily.yml -f source={a.source} -f force=true\n"
          f"  then re-run tools/audit_impossible_dates.py --r2 --source {a.source} "
          f"and expect zero.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
