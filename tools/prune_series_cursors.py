"""Drop stale series_cursor rows left behind by a cursor-GRAIN change.

WHY THIS EXISTS. `series_cursor` is keyed (source_id, series_key) and is only ever upserted —
nothing prunes it. So when a fetcher's cursor grain changes, every key from the OLD grain stays
for ever. Measured 2026-08-03: the table holds 14,349,462 rows and **ons_uk alone accounts for
10,099,151 of them (70%)**, against 40 that are actually current. Those keys average 389 bytes
(269-525), i.e. ~3.9 GB of key text, which is most of why state.db is 9.35 GB uncompressed /
376 MB on R2 — an object that EVERY run on BOTH routes pulls and pushes. Tonight's workstation
pass spent ~2 minutes on the pull alone before fetching anything.

The stale ons_uk keys are whole ONS dimension combinations minted per DATA ROW, e.g.
    CV=.:calendar-years=2021:administrative-geography=E12000001:Geography=North East:...
while the current fetcher writes one cursor per DATASET (`cursors[ds_id] = iso`).

WHAT IS AND IS NOT REPAIRED. Cursors are bookkeeping, fully re-derivable from the store, so
nothing here destroys data — this is not the reserved "delete something not re-crawlable" case.
It also does NOT fix ons_uk's on-disk keys, which carry the same wrong grain; ons_uk.py says so
itself ("the on-disk keys are still wrong and that remains a re-ingest, not a fetcher patch").

THE KEEP SET IS THE SIDECAR, NOT A LENGTH RULE. The authority for "which cursors are current"
is the source's own vintage sidecar (ds_id -> version), read from the store. Length is used
ONLY as a cross-check: if any key we mean to KEEP is long, or any key we mean to DELETE is
short, the two disagree and this ABORTS rather than guessing (R10 — an absurd sentinel must
stop a destructive step, and the delete-set is printed BEFORE anything is written, never after).

SINGLE WRITER. The state store is compare-and-swap on one ETag (R5). This refuses to run while
a CI updater is in flight or the workstation holds its lock, because a prune that loses the CAS
is wasted and a prune that WINS it destroys the in-flight run's whole bookkeeping.

    python tools/prune_series_cursors.py ons_uk              # dry run, prints the delete set
    python tools/prune_series_cursors.py ons_uk --apply
"""
from __future__ import annotations
import argparse
import json
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import config                                    # noqa: E402
from updater.state import StateStore                          # noqa: E402

# A current cursor key is a dataset slug; a legacy one is a full dimension combination. The
# observed gap is 45 vs 269 characters, so this bound sits in open space between them and is a
# TRIPWIRE, not the rule. If it ever fires, the sidecar and the table disagree about grain and a
# human needs to look.
MAX_CURRENT_KEY = 64
MIN_LEGACY_KEY = 120


def keep_set(source: str) -> set:
    """Current cursor keys, from the source's OWN sidecar — no network, no guessing."""
    if source == "ons_uk":
        from updater.strategies.fetchers import ons_uk
        return set(ons_uk._load_sidecar(config.source_dir(source)))
    raise SystemExit(f"{source}: no keep-set rule defined. Add one that reads THAT source's "
                     f"own record of what it currently writes — never infer it from the "
                     f"cursors being pruned, which is the thing under suspicion.")


def runs_in_flight() -> list:
    """Reasons it is not safe to touch the shared state right now."""
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
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--force-unsafe", action="store_true",
                    help="skip the in-flight check (you must have another reason to be sure)")
    ap.add_argument("--allow-delete-all", action="store_true",
                    help="permit removing EVERY cursor for the source, leaving it with none. "
                         "Only when a cursor-less state is intended — see the guard below.")
    a = ap.parse_args()

    blockers = runs_in_flight()
    if blockers and not a.force_unsafe:
        print("REFUSING — the state store is single-writer (R5):")
        for b in blockers:
            print(f"  - {b}")
        print("\nRe-run when idle, or pass --force-unsafe if you have independent proof.")
        return 2

    keep = keep_set(a.source)
    if not keep:
        print(f"{a.source}: keep-set is EMPTY — that would delete every cursor. Refusing; "
              f"an empty sidecar means 'I could not look', not 'nothing is current'.")
        return 2

    store = StateStore()
    cur = store.series_cursors(a.source)
    doomed = [k for k in cur if k not in keep]
    kept = [k for k in cur if k in keep]

    print(f"{a.source}: {len(cur):,} cursor(s) in state | sidecar says {len(keep):,} current")
    print(f"  KEEP   {len(kept):,}")
    print(f"  DELETE {len(doomed):,}")
    if not doomed:
        print("\nnothing to prune.")
        return 0

    # Cross-check BEFORE printing samples, so a disagreement stops the run loudly.
    bad_keep = [k for k in kept if len(k) > MAX_CURRENT_KEY]
    bad_del = [k for k in doomed if len(k) < MIN_LEGACY_KEY]
    if bad_keep or bad_del:
        print("\nABORT — the sidecar and the cursor table disagree about grain:")
        for k in bad_keep[:5]:
            print(f"  keep-but-long   ({len(k)}) {k[:100]}")
        for k in bad_del[:5]:
            print(f"  delete-but-short({len(k)}) {k[:100]}")
        print("Resolve by hand. Not guessing which side is right.")
        return 2

    # DELETING EVERY CURSOR IS NOT A PRUNE. Measured on ons_uk 2026-08-03: the sidecar lists 40
    # current ds_ids and NOT ONE of them has a cursor row, because the source has not completed
    # a run since its grain was fixed. So "prune the stale ones" would have removed all
    # 10,099,151 and left the source with ZERO cursors — and every check above would have
    # passed, including the post-condition, since the intended keep-set was empty too.
    #
    # A source with no cursors reports no per-series freshness at all (the defect task #32
    # exists for), so this is a state to enter deliberately or not at all. The safe ORDER is:
    # let the source complete one run, which writes the current cursors, and prune after that —
    # then the delete-set is genuinely the stale remainder and the source is never blind.
    if not kept:
        print(f"\nREFUSING — this would delete ALL {len(doomed):,} of {a.source}'s cursors and "
              f"leave it with none.")
        print(f"  The sidecar lists {len(keep):,} current key(s), but NOT ONE has a cursor row "
              f"yet, which means the source has not completed a run since its grain changed.")
        print(f"  Let it run once, then prune: the delete-set becomes the stale remainder "
              f"rather than everything, and the source is never left without freshness data.")
        print(f"  Override with --allow-delete-all only if a cursor-less state is intended.")
        if not getattr(a, "allow_delete_all", False):
            return 2

    kl = [len(k) for k in doomed]
    print(f"\n  delete-set key length min/avg/max: {min(kl)}/{sum(kl)//len(kl)}/{max(kl)}")
    print(f"  approx key bytes reclaimed: {sum(kl)/1e9:.2f} GB")
    print("  sample of what will be DELETED:")
    for k in doomed[:3]:
        print(f"    {k[:110]}...")
    print("  sample of what will be KEPT:")
    for k in sorted(kept)[:5]:
        print(f"    {k}")

    if not a.apply:
        print("\n--dry run: nothing written. Re-run with --apply.")
        return 0

    con = store.db
    con.executemany("DELETE FROM series_cursor WHERE source_id=? AND series_key=?",
                    [(a.source, k) for k in doomed])
    con.commit()
    after = store.series_cursors(a.source)
    print(f"\n  deleted; {a.source} now holds {len(after):,} cursor(s)")
    if set(after) != set(kept):
        print("  WARNING: post-state does not equal the intended keep-set — investigate.")
        return 1
    con.execute("VACUUM")
    print(f"  VACUUM done. Push with: python -m updater.run --push-state")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
