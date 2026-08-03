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
    a = ap.parse_args()

    blockers = runs_in_flight()
    if blockers and not a.force_unsafe:
        print("REFUSING — a writer may be active (R5, single-writer store):")
        for b in blockers:
            print(f"  - {b}")
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
    bad = pc.sum(pc.cast(pc.greater(t.column("obs_date").combine_chunks(),
                                    IMPOSSIBLE_AFTER), "int64")).as_py() or 0
    print(f"{a.source}/{a.filename}")
    print(f"  rows in file            : {total:,}")
    print(f"  rows dated past {IMPOSSIBLE_AFTER.year}    : {bad:,} ({bad/max(total,1)*100:.1f}%)")
    print(f"  rows that are FINE and will be re-fetched too: {total - bad:,}")
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
