"""WU-3 step 1: clear the sidecar vintages of every ksh table whose stored keys carry U+FFFD.

WHY THIS EXISTS. The decode fix shipped (ksh_stadat.py: strict utf-8-sig -> cp1250 fallback,
commit 7b8ac3900) but it CANNOT re-fetch anything on its own: `update()` skips a table whose
sidecar vintage already matches (ksh_stadat.py:151), and the mojibake tables' vintages were
advanced when their themes merged. Without this step the fix is inert for ever — the review's
own addendum, and the reason WU-3's exit gate was never reachable.

THE SIDECAR LIVES ON R2, NOT LOCALLY. Measured 2026-08-31: under AQUEDUCT_BACKEND=local the
sidecar reads 0 entries; under r2 it holds 840. A local-backend run of this tool would find
nothing to clear and report success — so the tool REFUSES to run under any backend but r2.
(Same family as R533's lesson about store-adjacent sidecars, one file over.)

THE AFFECTED SET IS RECOMPUTED, NEVER HARDCODED. The table list comes from the store itself
each run (distinct series_key containing U+FFFD, split to its table id), so it cannot drift
from the data the way a pasted list would (R191/R192).

WHAT HAPPENS NEXT, AND THE PART THE SPEC DOES NOT COVER. Cleared tables re-fetch at
MAX_PER_RUN=60 against a deliberately slow host, so ~181 tables drain over ~4 ticks. merge is
never-shrink, so after the re-fetch the store holds BOTH the clean keys and the OLD mojibake
keys: the catalogued ids become servable (that IS the user-facing exit gate), but 76,134
orphan mojibake rows remain in the store. The spec's purge phase names only the CURSOR rows.
That store residue is a separate, deliberate decision — do not let this tool's success be read
as having removed it.

Usage:
  AQUEDUCT_BACKEND=r2 py tools/clear_ksh_mojibake_vintages.py            # dry run
  AQUEDUCT_BACKEND=r2 py tools/clear_ksh_mojibake_vintages.py --apply
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import blob, config  # noqa: E402

SIDECAR_NAME = "_bulk_vintages.json"
FFFD = chr(0xFFFD)
FEFF = chr(0xFEFF)


def affected_tables() -> tuple[set[str], dict]:
    """Table ids whose STORE keys carry U+FFFD or U+FEFF, recomputed from the parquets."""
    import duckdb

    store = config.source_dir("ksh_stadat")
    files = sorted(glob.glob(os.path.join(store, "*.parquet")))
    if not files:
        raise SystemExit(f"no ksh parquets under {store} — nothing to measure")
    sel = "', '".join(f.replace("\\", "/") for f in files)
    con = duckdb.connect()
    rows_total, keys_fffd, keys_feff = con.execute(f"""
        SELECT COUNT(*),
               COUNT(DISTINCT CASE WHEN contains(series_key, chr(65533))
                                   THEN series_key END),
               COUNT(DISTINCT CASE WHEN contains(series_key, chr(65279))
                                   THEN series_key END)
        FROM read_parquet(['{sel}'])""").fetchone()
    tabs = con.execute(f"""
        SELECT split_part(series_key, ':', 2) AS table_id,
               COUNT(DISTINCT series_key) AS keys,
               COUNT(*) AS rows
        FROM read_parquet(['{sel}'])
        WHERE contains(series_key, chr(65533)) OR contains(series_key, chr(65279))
        GROUP BY 1 ORDER BY 2 DESC""").fetchall()
    con.close()
    stats = {"store_files": len(files), "store_rows": rows_total,
             "distinct_keys_fffd": keys_fffd, "distinct_keys_feff": keys_feff,
             "affected_tables": len(tabs),
             "affected_rows": sum(r[2] for r in tabs)}
    return {t for t, _k, _r in tabs}, {**stats, "tables": [
        {"table_id": t, "keys": k, "rows": r} for t, k, r in tabs]}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    if config.BACKEND != "r2":
        print(f"REFUSED: backend is {config.BACKEND!r}. The authoritative ksh sidecar lives "
              f"on R2 (840 entries measured 2026-08-31); the local copy is EMPTY, so a "
              f"local run would clear nothing and report success. "
              f"Re-run with AQUEDUCT_BACKEND=r2.")
        return 2

    tables, stats = affected_tables()
    print(json.dumps({k: v for k, v in stats.items() if k != "tables"}, indent=1))

    path = os.path.join(config.source_dir("ksh_stadat"), SIDECAR_NAME)
    raw = blob.read_bytes(path)
    sidecar = json.loads(raw.decode("utf-8")) if raw else {}
    print(f"sidecar entries before: {len(sidecar):,}")
    if not sidecar:
        print("REFUSED: sidecar is empty — nothing to clear, and an empty read here means "
              "the backend or path is wrong, not that the work is done.")
        return 2

    present = sorted(t for t in tables if t in sidecar)
    missing = sorted(t for t in tables if t not in sidecar)
    print(f"affected tables in store : {len(tables):,}")
    print(f"  ... present in sidecar : {len(present):,}  (these get cleared -> re-fetched)")
    print(f"  ... absent from sidecar: {len(missing):,}  (already unpinned; nothing to do)")
    if missing[:5]:
        print(f"      e.g. {missing[:5]}")

    if not a.apply:
        print("(dry run — pass --apply to clear and publish)")
        return 0
    if not present:
        print("nothing to clear")
        return 0

    backup = os.path.join(ROOT, "data", "_aqueduct",
                          "ksh_bulk_vintages.before_wu3_clear.json")
    os.makedirs(os.path.dirname(backup), exist_ok=True)
    with open(backup, "w", encoding="utf-8") as f:
        json.dump(sidecar, f, indent=1, sort_keys=True)
    print("backed up the pre-clear sidecar to", backup)

    for t in present:
        sidecar.pop(t, None)
    blob.write_bytes_atomic(path, json.dumps(sidecar, sort_keys=True).encode("utf-8"))

    verify = blob.read_bytes(path)
    after = json.loads(verify.decode("utf-8")) if verify else {}
    still = [t for t in present if t in after]
    print(f"sidecar entries after: {len(after):,} (expected {len(sidecar):,})")
    if still or len(after) != len(sidecar):
        print(f"MISMATCH after read-back: {len(still)} cleared tables still present. "
              f"Restore from {backup} before any re-fetch.")
        return 1
    print(f"cleared {len(present):,} table vintage(s); the next ksh runs will re-fetch them "
          f"at MAX_PER_RUN=60, so expect ~{-(-len(present) // 60)} ticks to drain.")
    print("REMINDER: the re-fetch adds CLEAN keys; merge is never-shrink, so the "
          f"{stats['affected_rows']:,} old mojibake rows STAY in the store until a separate, "
          f"deliberate purge. Do not report this step as having removed them.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
