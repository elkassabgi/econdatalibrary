"""Does IDB's permission-required dataset backlink still resolve for every idb series we serve?

IDB's written permission (2026-07-15) carries condition (3): a clear, permanent link back to the
original dataset page. `api/worker/src/series.ts` builds it from the series id positionally, since
ids are shaped `idb:IDB:<dataset-slug>:<indicator>:<country>`:

    https://data.iadb.org/dataset/<slug>

That link is only as permanent as the publisher's slug. IDB renames datasets, and a rename 404s
the old name with no redirect - so a link that was correct at ingest silently stops meeting the
permission. Nothing in the pipeline would notice: the id is still well-formed, the CSV still
serves, and only the footer link is dead.

WHY THIS ASKS THE API AND NOT THE PAGE. The obvious check - GET the dataset page and look for 200
- does not work from a script. After about twenty requests data.iadb.org answers every request
from the IP with `HTTP 202` and a zero-byte body, indefinitely, regardless of user agent. A sweep
that does not know this reports every remaining dataset as broken; mine did, claiming 16 dead
links over 336 series, and the giveaway was that a URL which had returned 200 minutes earlier
returned 202 on the retry. The instrument was measuring its own throttle state.

`package_show` is a different endpoint, is not throttled the same way, and answers the question
that actually matters: does this slug still name a dataset at the publisher?

Read-only and paced. Nothing is fetched beyond the package metadata, nothing is written.
"""
import argparse
import json
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from core import derive_csv as dc                                     # noqa: E402

BASE = "https://data.iadb.org/api/3/action"
UA = {"User-Agent": "econdatalibrary/1.0 (+https://econdatalibrary.com)"}
PACE = 0.5

# Mirrors IDB_RENAMED in api/worker/src/series.ts. The worker rewrites these before building the
# link, so a slug listed here is EXPECTED not to resolve under its old name.
RENAMED = {
    "center-for-learning-improvement-information-cima-regional-indicators-2007-2": "cima-indicators",
}


def resolves(slug: str) -> object:
    """True when `slug` names a dataset at the publisher, else an error token."""
    try:
        req = urllib.request.Request(f"{BASE}/package_show?id={slug}", headers=UA)
        j = json.load(urllib.request.urlopen(req, timeout=90))
    except urllib.error.HTTPError as e:
        return e.code
    except Exception as e:                                            # noqa: BLE001
        return type(e).__name__
    if not j.get("success"):
        return "not-success"
    return True if j["result"].get("name") == slug else "renamed:" + j["result"].get("name", "?")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="idb")
    a = ap.parse_args()

    con = sqlite3.connect("file:%s?mode=ro" % dc.CATALOG, uri=True)
    slugs: dict[str, int] = {}
    for (sid,) in con.execute("SELECT series_id FROM series WHERE source_id=?", (a.source,)):
        parts = sid.split(":")
        if len(parts) >= 3:
            slugs[parts[2]] = slugs.get(parts[2], 0) + 1

    print(f"{len(slugs)} distinct dataset slugs across {sum(slugs.values()):,} served "
          f"{a.source} series\n")
    print(f"{'slug':<58}{'series':>8}  status")

    broken = []
    for slug, n in sorted(slugs.items(), key=lambda kv: -kv[1]):
        target = RENAMED.get(slug, slug)
        st = resolves(target)
        time.sleep(PACE)
        note = "ok" if st is True else str(st)
        if slug in RENAMED:
            note += f"  (worker rewrites -> {target})"
        if st is not True:
            broken.append((slug, n, st))
            note += "   <-- BACKLINK DEAD"
        print(f"{slug[:56]:<58}{n:>8,}  {note}")

    print()
    if broken:
        print("IDB PERMISSION CONDITION (3) IS NOT MET for "
              f"{sum(n for _, n, _ in broken):,} served series:")
        for slug, n, st in broken:
            print(f"   {st}  {slug}  ({n:,} series)")
        print("\nFix by adding the new slug to IDB_RENAMED in api/worker/src/series.ts AND to")
        print("RENAMED above, then deploying the worker - `npx wrangler deploy` is MANUAL (R345),")
        print("so an edit alone changes nothing a user can reach.")
        return 1
    print(f"All {len(slugs)} dataset backlinks resolve at the publisher.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
