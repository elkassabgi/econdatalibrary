"""Re-pull ONE cso subject parquet correctly — clear its matrices' cursor entries, then retire
the file.

WHY A DEDICATED TOOL, AND WHY tools/repull_file.py REFUSES cso. cso is CHANGE-DRIVEN. Its
update() computes

    changed = [m for m, u in cur_upd.items() if stored.get(m) != u]

against `_collupd.json`, a {matrix: LastUpdated} map in the STORE. It fetches what the publisher
reports as REVISED, never what is missing locally. So deleting a subject parquet does not make
its rows come back: the cursor still says "we hold the current version of every matrix", nothing
is re-fetched, and the rows are simply gone until CSO happens to revise each table on its own
schedule. That is a deletion, not a re-pull — measured at 3.9M rows for the eleven files with
impossible dates.

THE CORRECT ORDER, and it is the whole point of this file:
  1. map matrices -> subject via `_matrix_subject_map()` (the same function the fetcher uses, so
     the mapping cannot drift from it);
  2. DROP exactly that subject's matrices from `_collupd.json` and write it back THROUGH blob —
     they then read as changed on the next tick;
  3. only then back up and delete the parquet.
Cursor first. If step 3 ran first and step 2 failed, the file would be gone with nothing queued
to rebuild it.

DO NOT DELETE `_collupd.json` WHOLESALE. Every one of ~12.7k matrices would read as changed, and
update() pulls at most MAX_TABLES per run — so the store would churn for hundreds of runs while
the file you actually wanted stays missing.

THE AVAILABILITY COST IS REAL AND IS PRINTED. Between the delete and the last re-fetch the
subject serves nothing, and the fetcher is bounded per run, so recovery takes
ceil(len(matrices) / MAX_TABLES) runs — stated before anything is touched, because "re-pullable"
does not mean "back in a minute".

    python tools/cso_repull_subject.py --list
    python tools/cso_repull_subject.py 10_Census_2016
    python tools/cso_repull_subject.py 10_Census_2016 --apply
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

from updater import blob, config                                   # noqa: E402
from tools._store_banner import banner                             # noqa: E402
from updater.strategies.fetchers import cso as C                   # noqa: E402


def runs_in_flight() -> "list[str]":
    """R5: the store is single-writer. Never mutate a cursor under a live run."""
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
    except Exception as e:                                         # noqa: BLE001
        why.append(f"could not check CI ({type(e).__name__}) — refusing to assume it is idle")
    return why


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("subject", nargs="?", help="subject file stem, WITHOUT .parquet")
    ap.add_argument("--list", action="store_true", help="show subjects and their matrix counts")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--force-unsafe", action="store_true")
    a = ap.parse_args()

    m2s = C._matrix_subject_map()
    if not m2s:
        print("REFUSING — the matrix->subject map is empty. _catalog.json is missing from the "
              "store and could not be rebuilt from CSO's Search API; without it a matrix cannot "
              "be routed to its parquet and this would clear the wrong cursor entries.")
        return 2

    by_subject: dict[str, list[str]] = {}
    for mtr, subj in m2s.items():
        by_subject.setdefault(subj, []).append(mtr)

    if a.list or not a.subject:
        print(f"{len(m2s):,} matrices across {len(by_subject):,} subjects "
              f"(MAX_TABLES={C.MAX_TABLES}/run)")
        for subj, ms in sorted(by_subject.items(), key=lambda kv: -len(kv[1])):
            runs = -(-len(ms) // max(1, C.MAX_TABLES))
            print(f"  {len(ms):>5} matrices  ~{runs:>3} run(s) to rebuild   {subj}")
        return 0

    matrices = by_subject.get(a.subject)
    if not matrices:
        print(f"REFUSING — no matrices map to subject {a.subject!r}. Check --list; the stem is "
              f"the parquet name without .parquet.")
        return 2

    blockers = runs_in_flight()
    if blockers and not a.force_unsafe:
        print("REFUSING — a writer may be active (R5, single-writer store):")
        for b in blockers:
            print(f"  - {b}")
        return 2

    out_dir = C._out_dir()
    # Name the store before deleting anything from it. R296: run against the local mirror by
    # accident and every number still looks right, because the mirror has the same shape.
    banner("cso", out_dir)
    parquet = os.path.join(out_dir, f"{a.subject}.parquet")
    cur_path = C._cursor_path()

    raw = blob.read_bytes(cur_path)
    stored = {}
    if raw:
        try:
            stored = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            stored = {}
    present = [m for m in matrices if m in stored]

    rows = blob.row_count(parquet) if blob.exists(parquet) else 0
    runs_needed = -(-len(matrices) // max(1, C.MAX_TABLES))
    print(f"subject          : {a.subject}")
    print(f"matrices          : {len(matrices):,}  ({len(present):,} currently in the cursor)")
    print(f"parquet           : {parquet}")
    print(f"rows that go away : {rows:,}")
    print(f"cursor entries to drop: {len(present):,} of {len(stored):,} total")
    print(f"AVAILABILITY COST : this subject serves NOTHING until the re-fetch completes — "
          f"~{runs_needed} run(s) at MAX_TABLES={C.MAX_TABLES} per run.")

    if not a.apply:
        print("\n--dry run: nothing written, nothing deleted. Re-run with --apply.")
        return 0

    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    # ---- 1) CURSOR FIRST. If this fails, the parquet is still intact. ----
    blob.write_bytes_atomic(os.path.join(out_dir, f"_collupd.backup-{stamp}.json"),
                            json.dumps(stored).encode("utf-8"))
    pruned = {m: u for m, u in stored.items() if m not in set(matrices)}
    blob.write_bytes_atomic(cur_path, json.dumps(pruned).encode("utf-8"))
    check = json.loads((blob.read_bytes(cur_path) or b"{}").decode("utf-8"))
    still = [m for m in matrices if m in check]
    if still:
        print(f"  ABORT: {len(still)} target matrices are STILL in the cursor after the write "
              f"— refusing to delete the parquet, since nothing would re-fetch it.")
        return 1
    print(f"  cursor pruned and verified: {len(stored):,} -> {len(check):,} entries")

    # ---- 2) then retire the parquet, reversibly ----
    if not blob.exists(parquet):
        print("  parquet absent — cursor cleared, nothing to delete. Next runs will re-fetch.")
        return 0
    r2 = blob.R2Blob()
    key = blob._path_to_key(parquet)
    backup = f"_backup/cso_repull/{stamp}/{a.subject}.parquet"
    r2.client.copy_object(Bucket=r2.bucket, Key=backup,
                          CopySource={"Bucket": r2.bucket, "Key": key})
    if not r2.exists(backup):
        print("  ABORT: backup not readable after copy — refusing to delete the original.")
        return 1
    r2.client.delete_object(Bucket=r2.bucket, Key=key)
    print(f"  backed up -> r2://{backup}, deleted {key}")
    print(f"\n  NEXT: let the daily run proceed (~{runs_needed} run(s)), then re-check with\n"
          f"    python tools/audit_impossible_dates.py --r2 --source cso")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
