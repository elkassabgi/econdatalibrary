"""Retire a legacy source whose publisher-direct successor is live — the CLEAN removal.

AUTHORIZED by Ahmed 2026-08-06 ("no bookmarks, no one has even seen the data.. refresh to
match publisher.. I need a clean database"): legacy relay-era ids retire in favor of their
proven *_direct successors. The full plan (Class A/B1/B2) lives in
.claude/skills/econ-updater/references/50-queue.md.

Pipeline per source (each step verified, dry-run by default):
  1. ARCHIVE the primary parquet(s) to archive/retired/<source>/ — cheap
     insurance, same pattern as purge_unpermitted_r2.py.
  2. the catalogue: DELETE series + source rows.
  3. D1: DELETE series + source rows (the worker requires BOTH a source row and >=1 series
     for /v1/sources, so after this the id vanishes there).
  4. purge: series/<urlencode('<source>:')>-prefixed CSVs + clean_full/<source>/ store.
     Prefixes TERMINATED ('imf_fsi%3A', 'clean_full/imf_fsi/') so the *_direct successors
     sharing the name stem can NEVER be swept (imf_fsi vs imf_fsi[bsis]_direct is exactly
     the R112/R129 unanchored-substring trap; the '%3A'/'/' terminators kill it).
  5. Caller then: remove the id from util.ts SUPPORTED_SOURCES, retire any registry entry
     (+count bump SAME commit, R347), wrangler deploy, live /v1/sources absence check,
     refresh_r2_catalog, coverage re-measure. Those are deliberate manual/reviewed steps.

WHERE each step acts is core/licence_targets.py's job (docs/ECON_SELF_HOSTING_PLAN.md, change 5): the R2
bucket, the checkout's catalogue and D1 before T0, exactly as always; the self-hosted blob store, the store
files and the one catalogue build (under the single-writer lock) after T0, with D1 left frozen.

Usage:
  python tools/retire_source.py imf_psbsfad                 # dry run: counts only
  python tools/retire_source.py imf_psbsfad --apply         # execute steps 1-4
"""
from __future__ import annotations

import argparse
import contextlib
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core.licence_targets import Targets, csv_prefix, store_prefix  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("source")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args(argv)
    src = a.source
    if src.endswith("_direct"):
        print(f"REFUSING: {src} is a publisher-direct SUCCESSOR, never a retirement target")
        return 1

    t = Targets(apply=a.apply)
    if t.selfhosted and a.apply:                    # a dry run only reads, so it takes no lock (AR-153)
        from core.catalog_path import writer_lock                           # noqa: PLC0415
        lock = writer_lock()
    else:
        lock = contextlib.nullcontext()
    with lock:
        return _retire(src, a.apply, t)


def _retire(src: str, apply: bool, t: Targets) -> int:
    con = t.catalogue(write=apply)
    n_series = con.execute("SELECT COUNT(*) FROM series WHERE source_id=?", (src,)).fetchone()[0]
    n_source = con.execute("SELECT COUNT(*) FROM source WHERE source_id=?", (src,)).fetchone()[0]

    # TERMINATED prefixes — the ':' urlencodes to %3A; the store dir ends with '/'.
    cp, sp = csv_prefix(src), store_prefix(src)
    csv_keys = [k for k, _ in t.list(cp)]
    store_objs = t.list(sp)
    parquets = [(k, s) for k, s in store_objs if k.endswith(".parquet")]

    where = "self-hosted" if t.selfhosted else "R2"
    print(f"{src}: catalog series={n_series:,} source_row={n_source}  "
          f"{where} csvs={len(csv_keys):,}  store objects={len(store_objs):,} "
          f"(parquets to archive: {len(parquets)})")

    if not apply:
        print("(dry run — pass --apply to retire)")
        return 0

    # 0. LOG FIRST, then change (R1222 finding 3): logged after the deletes, a retirement that stopped half way
    # left no record, and import_from_r2 --restore-missing put its deleted CSVs back. A logged removal that
    # then fails is the safe way round: nothing is restored under it until the log is looked at.
    t.record_removal("retire_source", src, "retired: catalogue rows, series CSVs and store objects")

    # 1. archive primary parquets
    for k, _ in parquets:
        t.archive(k, f"archive/retired/{src}/{os.path.basename(k)}")
    print(f"  archived {len(parquets)} parquet(s) -> archive/retired/{src}/")

    # 2. the catalogue (after T0 also the tables D1 used to carry - see Targets.remove_source_rows)
    left = t.remove_source_rows(con, src)
    print(f"  catalogue: deleted; residual rows={left} (must be 0)")

    # 3. D1
    # source_counts MUST go with them (R709). It is what /v1/catalog serves as `total`
    # (sql.ts:246) and what /v1/stats sums (index.ts:121). Deleting `series` and `source`
    # while leaving the count row behind produces the ilo symptom: the source vanishes from
    # /v1/sources, still contributes its old count to the fleet total, and still advertises
    # `total: N` over an empty result set. Retiring a source removes its count; it does not
    # recompute it to 0, because the source no longer exists.
    # AND THE FRESHNESS PROJECTION, for the same reason one table over. /v1/last-updates selects
    # from unit_state with NO join to `source` and no denylist filter (sql.ts LAST_UPDATES,
    # lastUpdates.ts), so a source retired without these rows disappears from /v1/sources and goes
    # on publishing its id, unit id, status, last_updated, last_obs_date and obs_count for ever.
    # Measured 2026-09-21: 15 ids were being served that way, 11 of them gated. source_data_through
    # belongs with them - it is read by the two LEFT JOINs in sql.ts and is emitted by the same
    # freshness sync.
    # (A D1 error report that crashed on the console's encoding once left a source deleted locally but
    # live in D1 - R363/R234; core.d1_remote.execute_wrangler keeps that fix.)
    if not t.skip_d1():
        if not t.d1_execute(src):
            return 1

    # 4. purge (batched; every key re-checked against the terminated prefixes)
    n = t.delete(csv_keys, (cp, sp))
    print(f"  {where}: deleted {n:,} series CSV(s)")
    n = t.delete([k for k, _ in store_objs], (cp, sp))
    print(f"  {where}: deleted {n:,} store object(s)")

    from core import cutover                                           # noqa: PLC0415
    print(f"{src}: RETIRED (data plane). " + cutover.next_steps(
        "Now: util.ts removal + registry retire/count bump + deploy + live absence check + refresh_r2_catalog."))
    return 0


if __name__ == "__main__":
    sys.exit(main())
