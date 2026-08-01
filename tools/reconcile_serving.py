"""Three-way reconcile: catalogued (searchable) x SUPPORTED_SOURCES (downloadable) x registry.

Three files, seconds to run, and it catches most of what a full store scan would - which is the
point. The scan version of this question held 128 GB for two hours and printed nothing (R212);
this reads catalog.db, api/worker/src/util.ts and updater/registry.yaml and answers immediately.

The two failure modes it exists for:

  CATALOGUED BUT NOT IN SUPPORTED_SOURCES
      Searchable on the site, and every download answers 501 not_migrated. This stranded
      cepii_gravity's 1,143,250 series: catalogued, indexed, advertised, and undownloadable.

  IN SUPPORTED_SOURCES BUT NOT CATALOGUED
      Downloadable by id and invisible to search. This is what noaa (3,135,873 series) and
      census (440,414) were, and both were found by accident rather than by looking.

It also flags entries in SUPPORTED_SOURCES with NO STORE AT ALL - a promise to serve data that
does not exist anywhere, which answers 404 rather than 501 and is the least visible of the three.

    python tools/reconcile_serving.py
"""
from __future__ import annotations

import glob
import os
import re
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def supported_sources() -> set[str]:
    """The worker's list, with its commentary stripped.

    Parsed from the source rather than from a deploy: this must answer what the NEXT deploy
    will serve, and reading the live worker would only report the last one.
    """
    p = os.path.join(ROOT, "api", "worker", "src", "util.ts")
    ts = open(p, encoding="utf-8").read()
    blk = ts.split("SUPPORTED_SOURCES: readonly string[] = [", 1)[1].split("];", 1)[0]
    return set(re.findall(r'"([a-z0-9_]+)"', re.sub(r"//[^\n]*", "", blk)))


def store_size(source_id: str) -> tuple[int, float]:
    """(parquet files, MB) across every data tier — clean_full, clean, and any other."""
    files = []
    for d in glob.glob(os.path.join(ROOT, "data", "*", source_id)):
        if os.path.isdir(d):
            files += glob.glob(os.path.join(d, "*.parquet"))
    return len(files), sum(os.path.getsize(f) for f in files) / 1e6


def main() -> int:
    supported = supported_sources()
    con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=180.0)
    con.execute("PRAGMA busy_timeout = 180000")
    cat = dict(con.execute("select source_id, count(*) from series group by 1").fetchall())
    con.close()

    from updater import registry
    reg = {s["source_id"]: s for s in registry.load()["sources"]}
    live = {k for k, v in reg.items() if v.get("live") is True}

    print(f"SUPPORTED_SOURCES : {len(supported)}")
    print(f"catalogued sources: {len(cat)}  ({sum(cat.values()):,} series)")
    print(f"registry sources  : {len(reg)}  ({len(live)} live:true)")

    a = sorted(s for s in cat if s not in supported)
    print(f"\nCATALOGUED BUT NOT DOWNLOADABLE — every series answers 501 "
          f"({len(a)} sources, {sum(cat[s] for s in a):,} series)")
    for s in a:
        print(f"   {s:24s} {cat[s]:>10,}   registry={s in reg}  live={s in live}")

    b = sorted(s for s in supported if cat.get(s, 0) == 0)
    hosted, phantom = [], []
    for s in b:
        n, mb = store_size(s)
        (hosted if n else phantom).append((s, n, mb))
    print(f"\nHOSTED BUT NOT SEARCHABLE — data present, zero catalogue rows ({len(hosted)})")
    for s, n, mb in hosted:
        print(f"   {s:24s} {n:>4} parquet {mb:>9,.1f} MB   registry={s in reg}")
    print(f"\nPROMISED BUT ABSENT — in SUPPORTED_SOURCES with no store anywhere ({len(phantom)})")
    for s, _, _ in phantom:
        print(f"   {s:24s} registry={s in reg}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
