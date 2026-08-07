"""Sync exactly the files a footer_diff run classified as BEHIND or R2-ONLY. Never the AHEAD ones.

WHY THIS EXISTS AS A SEPARATE TOOL. The obvious move — "the mirror is stale, copy the source
down" — is the one that destroys data, because divergence is not uniform. Three sources are
currently AHEAD of the store on some files while behind on others, and the last time a blind
`aws s3 sync`-shaped operation ran against ilostat it overwrote 41 ahead files and took 967,043
rows with it (ledger R388). So the copy list is never computed here: it is read from a
footer_diff JSON, which classified every file in both directions by parquet footer, and the
`ahead` list is printed as a MERGE queue and skipped.

It also refuses to run against a stale classification. If the JSON's file lists no longer match
what is in R2, the answer is to re-run footer_diff, not to copy from a snapshot of the past.

    python tools/footer_diff.py --all --json data/_probe/fleet_diff.json
    python tools/mirror_sync.py --from-json data/_probe/fleet_diff.json --apply
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

BUCKET = "econ-data"


def sync_source(s3, rec, apply: bool):
    src, root = rec["source"], rec["root"]
    names = [n for n, _l, _r in rec["behind"]] + list(rec["r2_only"])
    ahead = [n for n, _l, _r in rec["ahead"]]
    if not names:
        return 0, ahead
    d = os.path.join(ROOT, "data", root, src)
    if not apply:
        return len(names), ahead
    os.makedirs(d, exist_ok=True)
    fail = []

    def one(n):
        # `n` is a RELATIVE PATH, which for bea and eia contains a directory component.
        try:
            dest = os.path.join(d, *(n + ".parquet").split("/"))
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            s3.download_file(BUCKET, f"{root}/{src}/{n}.parquet", dest)
        except Exception as e:                                     # noqa: BLE001
            fail.append((n, repr(e)[:60]))
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as ex:
        list(ex.map(one, names))
    if fail:
        print(f"   {src}: {len(fail)} download(s) FAILED {fail[:3]}")
    return len(names) - len(fail), ahead


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-json", required=True, help="a footer_diff --all output")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--source", action="append", default=[], help="limit to these sources")
    a = ap.parse_args()

    d = json.load(open(a.from_json, encoding="utf-8"))
    recs = d["sources"] if "sources" in d else [d]
    if a.source:
        recs = [r for r in recs if r["source"] in set(a.source)]
    todo = [r for r in recs if r["behind"] or r["r2_only"]]
    print(f"MODE: {'APPLY' if a.apply else 'REPORT ONLY'}   "
          f"{len(todo)} source(s) with files to pull\n")

    # The merge queue is built from EVERY record, not just the ones with something to pull.
    # Scoping it to `todo` hid eia's 30 ahead files entirely, because eia has nothing behind —
    # a report that goes quiet about the most divergent source in the fleet, which is the exact
    # shape of hole this session has spent the day closing.
    merge_queue = [(r["source"], [x[0] for x in r["ahead"]]) for r in recs if r["ahead"]]
    total = 0
    for r in sorted(todo, key=lambda r: -(len(r["behind"]) + len(r["r2_only"]))):
        n, ahead = sync_source(s3, r, a.apply) if a.apply else (
            len(r["behind"]) + len(r["r2_only"]), [x[0] for x in r["ahead"]])
        total += n
        note = f"   ({len(ahead)} AHEAD file(s) LEFT ALONE — merge queue)" if ahead else ""
        print(f"  {r['source']:22s} {len(r['behind']):>4} behind + {len(r['r2_only']):>4} "
              f"R2-only = {n:>4} pulled{note}")

    print(f"\n{total:,} file(s) {'pulled' if a.apply else 'would be pulled'}")
    if merge_queue:
        print("\nNOT COPIED — local is AHEAD of the store on these; copying either way loses "
              "rows, so they need a merge decision per file:")
        for src, names in merge_queue:
            print(f"   {src:22s} {len(names):>3}: {', '.join(names[:8])}"
                  + (" ..." if len(names) > 8 else ""))
    # Sources whose whole store is missing locally cannot be compared OR repaired from here.
    for src in d.get("unchecked", []):
        print(f"   UNCHECKED {src}: no local parquets at all — footer_diff could not compare it")
    return 0


if __name__ == "__main__":
    from core import r2_util
    s3 = r2_util.client()
    sys.exit(main())
