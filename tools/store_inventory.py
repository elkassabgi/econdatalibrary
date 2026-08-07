"""What does the STORE hold for a source? Answered from R2, not the workstation disk.

WHY THIS IS A TOOL AND NOT A RULE. "Measure a cloud-backend source against R2" is written in
the ledger three times — R366 (unsdg: 715 codes locally, 396 on R2), R371 (insee_melodi: "55
rows with no data" that R2 held all of), R374 (insee_melodi again: 84 local files vs 139 on
R2, which turned a 9-flow gap into a published "64"). Each time the local glob was simply the
cheapest thing to type, and each time it answered a different question confidently. A rule
broken three times in one day is not a rule. Use this instead:

  python tools/store_inventory.py insee_melodi
  python tools/store_inventory.py cso --names        # also list the file/flow stems

It prints R2, LOCAL and CATALOGUE side by side precisely so a divergence is impossible to
miss, and it never reports a local count alone. If R2 cannot be reached it says so and exits
non-zero rather than falling back to local — a fallback is how this mistake happens.
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def r2_store_files(source: str) -> set[str]:
    from core import r2_util
    c = r2_util.client()
    out, tok = set(), None
    while True:
        kw = dict(Bucket="econ-data", Prefix=f"clean_full/{source}/", MaxKeys=1000)
        if tok:
            kw["ContinuationToken"] = tok
        r = c.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            b = os.path.basename(o["Key"])
            if b.endswith(".parquet"):
                out.add(b[: -len(".parquet")])
        if not r.get("IsTruncated"):
            break
        tok = r.get("NextContinuationToken")
    return out


def local_store_files(source: str) -> set[str]:
    d = os.path.join(ROOT, "data", "clean_full", source)
    if not os.path.isdir(d):
        return set()
    return {f[: -len(".parquet")] for f in os.listdir(d) if f.endswith(".parquet")}


def catalogue_ids(source: str) -> set[str]:
    db = os.path.join(ROOT, "data", "catalog.db")
    if not os.path.exists(db):
        return set()
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        return {r[0].split(":", 1)[1] for r in con.execute(
            "SELECT series_id FROM series WHERE source_id=?", (source,))}
    finally:
        con.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("source")
    ap.add_argument("--names", action="store_true", help="list the stems, not just counts")
    a = ap.parse_args()

    try:
        r2 = r2_store_files(a.source)
    except Exception as e:                                          # noqa: BLE001
        print(f"R2 unreachable ({type(e).__name__}: {str(e)[:120]}).")
        print("REFUSING to answer from the local disk — that is the mistake this tool exists "
              "to prevent (ledger R366/R371/R374). Fix the credentials and re-run.")
        return 2

    loc = local_store_files(a.source)
    cat = catalogue_ids(a.source)
    print(f"{a.source}")
    print(f"  R2 store files    : {len(r2):>7,}   <- THE STORE")
    print(f"  local disk files  : {len(loc):>7,}   <- scratch/mirror, NOT the store")
    print(f"  catalogue ids     : {len(cat):>7,}")
    if loc and loc != r2:
        only_r2, only_loc = len(r2 - loc), len(loc - r2)
        print(f"  DIVERGENT: {only_r2:,} on R2 only, {only_loc:,} local only — any count taken "
              f"from the local tree is wrong by that much")
    # Compare catalogue ids to file stems ONLY where that comparison means something.
    # For file-grain sources (ons_uk, insee_melodi) one catalogue id IS one file. For
    # flow-grain (cso, unsdg) or series-grain (worldbank_esg) sources the ids are keys
    # INSIDE the files, so a set difference reports every id as "missing" — 7,896 of them
    # for cso on the first run of this tool. A confidently wrong number is the failure this
    # tool exists to prevent, so it must not produce one itself.
    if cat:
        overlap = len(cat & r2)
        if overlap:
            missing = cat - r2
            print(f"  catalogued ids with NO R2 store file: {len(missing):,}"
                  + (f"  {sorted(missing)[:8]}" if missing else ""))
        else:
            print(f"  (catalogue ids are not file stems for this source — {len(cat):,} ids "
                  f"live INSIDE the files; use the source's own resolver to check coverage, "
                  f"not a filename set difference)")
    if a.names:
        print(f"\n  R2 stems ({len(r2)}): {sorted(r2)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
