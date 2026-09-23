"""Seed ksh_stadat/_backfill_vintages.json: the vintage the June 2026 desktop backfill stored, for every
table the store holds rows of that the updater has never fetched (review R1134).

WHY. The fetcher measures how long a reader has waited for KSH's newer release from the vintage the
store holds. For a table the updater fetched, that is the sidecar (_bulk_vintages.json). The 802 tables
it never fetched had no stored vintage, so 658 served tables - 231 of them behind a KSH release more
than 45 days old - were invisible to the drain signal and to the 45-day ATTENTION limit.

THE SOURCE, AND ITS ONE ERROR. data/clean_full/ksh_stadat/_catalog.json is the toc.json snapshot the
desktop ingester (jobs/ingest_ksh_stadat.py load_catalog) saved and then fetched from. It is a DESKTOP
file: the store on R2 does not hold it. It was saved BEFORE the tables were fetched, so a table KSH
updated in between is stored newer than the snapshot says. That error makes a wait read LONGER, never
shorter - toward ATTENTION, not toward a hidden backlog.

WHAT IS WRITTEN. {tid: "updatedAt|correctedAt"} for each snapshot table that (1) is not in the sidecar
and (2) has at least one row 'KSH:<tid>:...' in its theme parquet or in a '_' side file on the STORE
(R2 under AQUEDUCT_BACKEND=r2; the local copies differ). A table with no stored rows is left out, so it
stays on the fetcher's "nothing stored" side and is fetched as new.

ONE-TIME. It refuses to replace an existing file (--force overrides). The sidecar wins once the updater
fetches a table, so the file only ever shrinks in meaning.

PLANT. A table the sidecar holds and the store has rows of (kkr0049 by default) must read as held,
so "no rows" can never be a reader failure.

Usage:
  AQUEDUCT_BACKEND=r2 py tools/seed_ksh_backfill_vintages.py            # dry run: counts only
  AQUEDUCT_BACKEND=r2 py tools/seed_ksh_backfill_vintages.py --apply    # write + read back
"""
from __future__ import annotations

import argparse
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import blob, config  # noqa: E402
from updater.strategies.fetchers import ksh_stadat as K  # noqa: E402


def held_tables(out_dir, tids) -> set:
    """The tids with at least one stored row, reading each theme parquet and each side file ONCE."""
    import pyarrow.compute as pc
    names = set(blob.list_parquets(out_dir))
    side = sorted(n for n in names if n.startswith("_"))
    want = set(tids)
    held: set = set()
    by_theme: dict = {}
    for t in want:
        by_theme.setdefault(f"{t[:3].lower()}.parquet", []).append(t)
    for fn in sorted(set(by_theme) & names) + side:
        keys = blob.read_table(os.path.join(out_dir, fn), columns=["series_key"]).column("series_key")
        # a theme file can only hold its own theme's tables; a side file can hold any
        for t in (by_theme[fn] if fn in by_theme else sorted(want - held)):
            if t not in held and pc.any(pc.starts_with(keys, f"KSH:{t}:")).as_py():
                held.add(t)
    return held


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--catalog", default=os.path.join("E:/research/econfindatalibrary/data/clean_full",
                                                      "ksh_stadat", "_catalog.json"))
    ap.add_argument("--plant", default="kkr0049")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args(argv)

    out_dir = config.source_dir(K.SOURCE)
    print(f"backend={config.BACKEND} store={out_dir}")
    snap = json.load(open(a.catalog, encoding="utf-8-sig"))
    snap = {K._table_id(e): e for e in snap if K._table_id(e)}
    sidecar = K._load_sidecar(out_dir)
    if not sidecar:
        print("REFUSED: the sidecar read empty - cannot tell never-fetched tables from fetched ones")
        return 2
    never = sorted(t for t in snap if t not in sidecar)
    held = held_tables(out_dir, never + [a.plant])
    if a.plant not in held:
        print(f"REFUSED: the plant {a.plant} (in the sidecar, stored) did not read as held - the reader is broken")
        return 2
    seed = {t: K._vintage(snap[t]) for t in never if t in held}
    print(f"snapshot tables {len(snap)}; sidecar {len(sidecar)}; never fetched {len(never)}; "
          f"with stored rows {len(seed)}; without {len(never) - len(seed)}; plant {a.plant} held")
    if not a.apply:
        print("dry run - nothing written")
        return 0
    path = os.path.join(out_dir, K.BACKFILL)
    if blob.read_bytes(path) is not None and not a.force:
        print(f"REFUSED: {K.BACKFILL} already exists (one-time seed; --force to replace)")
        return 2
    body = json.dumps(seed, sort_keys=True).encode("utf-8")
    blob.write_bytes_atomic(path, body)
    back = blob.read_bytes(path)
    ok = back == body and K._load_backfill(out_dir) == seed
    print(f"wrote {len(seed)} vintages ({len(body)} bytes); read back {'IDENTICAL' if ok else 'DIFFERENT'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
