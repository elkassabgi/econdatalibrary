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

AND IT CATCHES PARTIAL CATALOGUING, which a zero-test cannot. census has 22 catalogue rows over
an 80-table, 44,939,061-row store; noaa had 10 over 3,135,873 series. Both are the same defect as
"zero rows" and both pass a test that only looks for zero. Where a source ships
`*__series.parquet` sidecars the exact series count is in the parquet FOOTER, so the comparison
costs a metadata read and no scan. Sources without sidecars are reported as "needs a scan"
rather than silently omitted - see tools/audit_store_vs_catalog.py for that.

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


def sidecar_series(source_id: str) -> int | None:
    """DISTINCT series count from the `*__series.parquet` sidecars, or None if there are none.

    Counts distinct series_key, NOT sidecar rows. Rows overstate it wherever a source
    cross-lists a series: fed_board publishes IP.B50001.A under three presentation groupings
    (IP_MAJOR_INDUSTRY_GROUPS, IP_MARKET_GROUPS, IP_SPECIAL_AGGREGATES) though its observations
    live only in G17, and a row count reports 52,519 where there are 52,293 series. fhfa is
    worse: 89,706 rows, 87,685 series. Reporting the inflated figure would manufacture a
    permanent phantom gap against a catalogue that was actually complete.

    One string column of a metadata sidecar - kilobytes, not the observation store - so this
    stays cheap enough to run every time even though it is no longer a pure footer read.
    """
    files = []
    for d in glob.glob(os.path.join(ROOT, "data", "*", source_id)):
        files += glob.glob(os.path.join(d, "*__series.parquet"))
    if not files:
        return None
    import pyarrow.parquet as pq
    keys: set = set()
    try:
        for f in files:
            keys.update(pq.read_table(f, columns=["series_key"])
                        .column("series_key").to_pylist())
    except Exception:                                          # noqa: BLE001
        return None
    return len(keys)


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

    # --- partial cataloguing, from parquet footers only ------------------------------
    part = []
    for s in sorted(supported):
        if cat.get(s, 0) == 0:
            continue                                           # already reported above
        n = sidecar_series(s)
        if n is None or n <= cat[s]:
            continue
        part.append((s, n, cat[s]))
    print(f"\nPARTIALLY CATALOGUED — the store's own sidecars list more series than the "
          f"catalogue does ({len(part)})")
    for s, n, c in part:
        print(f"   {s:24s} store {n:>12,}  catalogue {c:>12,}  missing {n - c:>12,}")
    print("   (sources with no __series.parquet sidecar cannot be judged from metadata; "
          "use tools/audit_store_vs_catalog.py)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
