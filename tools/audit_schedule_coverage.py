"""How much of what we SERVE actually auto-updates — the one number that must reach 100%.

WHY THIS IS A TOOL AND NOT AN INLINE QUERY. This measurement was re-derived by hand every
cycle, and hand-derivation is exactly how it went wrong: once as an unfiltered `GROUP BY`
labelled "served sources" and believed (ledger R143), once as a cadence-filtered list that
hid 10 fetcher-ready sources including three shipped that same day (R157), and once as a
gap-check that would have reported CLEAN on exactly 10 missing sources (R142). A number that
decides what to build next has to come from one auditable place.

EVERY INPUT IS PARSED FROM THE FILE THAT OWNS IT — nothing here is a hardcoded list, because
a second copy of a list is a second thing to go stale (R159):

  SERVED     catalogued (a row in catalog.db `series`) AND resolvable by the worker
             (present in SUPPORTED_SOURCES in api/worker/src/util.ts). Catalogued but not
             resolvable = a 501; resolvable but not catalogued = invisible in search. A
             source is only genuinely served when it is BOTH.
  SCHEDULED  registry.yaml `live: true`  UNION  the updater-heavy.yml matrix literal
             UNION  the sec_edgar source owned by sec-edgar-daily.yml. The heavy matrix and
             sec-edgar are separate workflows, so a registry-only definition under-counts
             and would send me to "fix" sources that already run.

The gap — served but NOT scheduled — is the work queue, printed largest-series-first so the
next build is chosen by how much data it keeps current, not by which was easiest to reach.

SCHEDULED BUT NOT SERVED is printed too, and is not automatically a bug: a source can be
deliberately gated (licence) while still being refreshed on disk. It IS worth seeing, because
the other reading is that we are paying to refresh something no one can reach.

Usage:  python tools/audit_schedule_coverage.py [--verbose]
"""
from __future__ import annotations
import argparse
import json
import os
import re
import sqlite3
import sys

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG = os.path.join(ROOT, "data", "catalog.db")
UTIL_TS = os.path.join(ROOT, "api", "worker", "src", "util.ts")
REGISTRY = os.path.join(ROOT, "updater", "registry.yaml")
HEAVY = os.path.join(ROOT, ".github", "workflows", "updater-heavy.yml")
SEC = os.path.join(ROOT, ".github", "workflows", "sec-edgar-daily.yml")


def supported_sources() -> set:
    """Parse SUPPORTED_SOURCES out of util.ts. Comments are stripped FIRST — the array is
    heavily commented and several entries sit on lines after a `//` note, so a naive
    quoted-string scan would harvest words out of prose (the R137 failure shape)."""
    src = open(UTIL_TS, encoding="utf-8").read()
    m = re.search(r"SUPPORTED_SOURCES\s*:\s*readonly\s+string\[\]\s*=\s*\[(.*?)\]\s*;",
                  src, re.S)
    if not m:
        raise SystemExit("could not locate SUPPORTED_SOURCES in util.ts — resolve by hand")
    body = re.sub(r"//[^\n]*", "", m.group(1))          # drop line comments, keep the entries
    body = re.sub(r"/\*.*?\*/", "", body, flags=re.S)
    return set(re.findall(r'"([^"]+)"', body))


# Strategies the orchestrator resolves through a per-source fetcher module. A source with one
# of these and NO module is filed as "PENDING — no adapter built" and skipped forever, however
# it is scheduled. Must match orchestrate._has_adapter.
FETCHER_BACKED = {"extend_by_date", "overwrite_if_changed", "sdmx_delta",
                  "manual_vintage", "bulk_snapshot_if_changed"}


def _adapter_missing(entry) -> bool:
    """True when this entry can never run: fetcher-backed strategy with no fetcher module.

    MEASURED 2026-07-30, which is why this check exists. updater-heavy ran green with all four
    matrix jobs reporting "0 unit(s) processed" — and two of them printed
    "PENDING <src> — no adapter built": cepii_gravity and eia are in the matrix, in the
    registry, and have NO fetcher. Counting matrix membership as "scheduled" therefore
    OVERSTATED coverage: a source can be scheduled on paper and structurally incapable of
    running. Scheduled has to mean "will actually run".
    """
    if entry.get("strategy") not in FETCHER_BACKED:
        return False
    sid = entry.get("source_id")
    return not os.path.exists(os.path.join(ROOT, "updater", "strategies", "fetchers",
                                           f"{sid}.py"))


def scheduled_sources() -> tuple:
    """(set, {source: reason}) — every mechanism that actually runs something.

    Excludes entries with no adapter; they are returned separately by no_adapter().
    """
    why = {}
    reg = yaml.safe_load(open(REGISTRY, encoding="utf-8"))
    by_id = {e["source_id"]: e for e in reg["sources"]}
    stranded = {}
    for s in reg["sources"]:
        if s.get("live"):
            if _adapter_missing(s):
                stranded[s["source_id"]] = f"registry live ({s.get('cadence','?')})"
                continue
            why[s["source_id"]] = f"registry live ({s.get('cadence', '?')})"

    heavy = open(HEAVY, encoding="utf-8").read()
    m = re.search(r"ALL='(\[[^']*\])'", heavy)
    if not m:
        raise SystemExit("could not locate the ALL=[...] matrix literal in updater-heavy.yml")
    for s in json.loads(m.group(1)):
        e = by_id.get(s)
        if e is not None and _adapter_missing(e):
            stranded.setdefault(s, "updater-heavy matrix")
            continue
        why.setdefault(s, "updater-heavy matrix")

    sec = open(SEC, encoding="utf-8").read()
    for sid in sorted(set(re.findall(r"\bsec_edgar(?:_xbrl)?\b", sec))):
        why.setdefault(sid, "sec-edgar-daily")

    # FOURTH MECHANISM: the workstation route. tools/run_local_heavy.ps1 asks
    # tools/_list_local_sources.py which sources to run, and that selects on
    # `run_location == "local"` REGARDLESS of `live` — so a source can be refreshed every
    # ~20h without ever setting live:true. Omitting it here under-counted coverage by up to
    # 17 sources, among them istat (14,267 catalogued series), census (2,993), bis, bls, eia,
    # oecd, statcan, faostat and vdem — every one of which this file reported as NOT
    # scheduled while the workstation was in fact updating them.
    #
    # Not hypothetical: the 2026-08-03T00:58Z pass logged "registry routes 17 source(s) to
    # this machine: bea, bis, bls, cbs_nl, census, cepii_gravity, comtrade, eia, faostat,
    # gus_dbw, istat, noaa, oecd, ons_uk, statcan, vdem, wid" and merged +8,457 rows into
    # census on that very run.
    #
    # Same adapter caution as the matrix above, for the same measured reason: membership in a
    # schedule is not the ability to run. A routed source with no fetcher is STRANDED, not
    # scheduled, and counting it would overstate coverage exactly as matrix membership once
    # did.
    for s in reg["sources"]:
        if (s.get("run_location") or "cloud") != "local":
            continue
        sid = s["source_id"]
        if _adapter_missing(s):
            stranded.setdefault(sid, "workstation route (run_location: local)")
            continue
        why.setdefault(sid, "workstation route (run_location: local)")

    scheduled_sources.stranded = stranded
    return set(why), why


def catalog_counts() -> dict:
    if not os.path.exists(CATALOG):
        raise SystemExit(f"no catalog at {CATALOG}")
    con = sqlite3.connect(f"file:{CATALOG}?mode=ro", uri=True)
    # catalog.db has concurrent writers (the derive jobs hold it for minutes at a time), so even
    # a read-only connection fails with "database is locked" the moment one of them runs — which
    # is exactly when this audit gets run. Six sibling tools already open it this way
    # (audit_store_vs_catalog, audit_license_coverage, the catalog_* family); this one was the
    # outlier and crashed instead of waiting, so the ONE number the procedure says to report was
    # unobtainable whenever the workstation was busy. R210.
    con.execute("PRAGMA busy_timeout = 180000")
    rows = con.execute("SELECT source_id, COUNT(*) FROM series GROUP BY source_id").fetchall()
    con.close()
    return {r[0]: r[1] for r in rows if r[0]}


def discontinued():
    """{source_id: entry} for publishers that have retired a dataset we still serve.

    Loaded, not hardcoded, so the evidence lives next to the claim. Returns {} if the file is
    absent — a missing file must never silently shrink the work queue.
    """
    import yaml
    p = os.path.join(ROOT, "updater", "discontinued.yaml")
    if not os.path.exists(p):
        return {}
    d = yaml.safe_load(open(p, encoding="utf-8")) or {}
    return {e["source_id"]: e for e in (d.get("sources") or [])}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--verbose", action="store_true",
                    help="also list the scheduled sources and their mechanism")
    args = ap.parse_args()

    counts = catalog_counts()
    supported = supported_sources()
    sched, why = scheduled_sources()
    stranded = getattr(scheduled_sources, "stranded", {})

    served = {s for s in counts if s in supported}
    served_series = sum(counts[s] for s in served)

    covered = served & sched
    gap = served - sched
    covered_series = sum(counts[s] for s in covered)
    gap_series = sum(counts[s] for s in gap)

    print(f"catalogued sources      {len(counts):>6,}")
    print(f"resolvable (util.ts)    {len(supported):>6,}")
    print(f"SERVED  (both)          {len(served):>6,}   {served_series:>12,} series")
    print()
    print(f"SCHEDULED of served     {len(covered):>6,}   {covered_series:>12,} series")
    # SPLIT THE GAP INTO WORK AND ARCHIVAL. Both are genuinely NOT auto-updating — that total is
    # unchanged and still printed first — but a dataset the publisher has RETIRED cannot be fixed
    # by building a fetcher, so leaving it at the top of the work queue makes the largest apparent
    # gap in the library a permanently un-closable one. imf_ifs alone is 100,706 series, 24% of
    # the queue, and IMF publishes no IFS dataflow at all (measured 2026-08-07: 222 dataflows, 0
    # matching). Entries must carry their own measurement — see updater/discontinued.yaml.
    archival = discontinued()
    arch = {s: archival[s] for s in gap if s in archival}
    arch_series = sum(counts[s] for s in arch)
    gap = gap - set(arch)
    gap_series -= arch_series
    print(f"NOT scheduled           {len(gap) + len(arch):>6,}   "
          f"{gap_series + arch_series:>12,} series")
    if arch:
        print(f"   ARCHIVAL (retired)   {len(arch):>6,}   {arch_series:>12,} series"
              f"   — frozen by the publisher, no fetcher is possible")
        print(f"   ACTIONABLE work      {len(gap):>6,}   {gap_series:>12,} series")
    pct_s = 100.0 * len(covered) / len(served) if served else 0.0
    pct_o = 100.0 * covered_series / served_series if served_series else 0.0
    print(f"\n>>> {len(covered)} of {len(served)} sources / {covered_series:,} of "
          f"{served_series:,} series scheduled  ({pct_s:.1f}% of sources, {pct_o:.1f}% of series)")

    # WHAT THIS NUMBER DOES NOT MEAN, said here because it has been read as more than it is.
    # "Scheduled" is answered from the registry: the source is live, has an adapter, and the
    # orchestrator will offer it a turn. It says NOTHING about whether the fetcher, once running,
    # reaches every sub-unit it owns.
    #
    # The difference is not hypothetical. worldbank_esg counted inside this figure for months
    # while 32 of its 71 indicators sat frozen at their first-pass ingest date, and adb likewise
    # with 44 of 54 flows — both bounded over a fixed order with no rotation, both reporting an
    # honest `partial` the whole time (R190, fixed 2026-08-03). Neither could ever have shown up
    # here, because both were scheduled, and were.
    #
    # Sub-unit coverage is a different measurement against a different source of truth — the
    # store's write times, not the registry — and it lives in tools/audit_untouched_files.py.
    # Run BOTH before saying a source auto-updates.
    print("    NOTE: 'scheduled' is a registry fact — live, adapter built, offered a turn. It is\n"
          "    NOT sub-unit coverage: a scheduled source can have most of its store frozen and\n"
          "    still be counted here (worldbank_esg 32/71, adb 44/54, both fixed 2026-08-03).\n"
          "    For that question run tools/audit_untouched_files.py --live.")

    # Catalogued but NOT resolvable — a live 501 for anyone who asks. Distinct from the gap.
    unresolvable = {s: counts[s] for s in counts if s not in supported}
    if unresolvable:
        tot = sum(unresolvable.values())
        print(f"\nCATALOGUED BUT NOT RESOLVABLE  {len(unresolvable)} sources / {tot:,} series"
              f"  (searchable, but the worker 501s on them)")
        for s, n in sorted(unresolvable.items(), key=lambda kv: -kv[1])[:15]:
            print(f"    {n:>12,}  {s}")

    if gap:
        print(f"\nWORK QUEUE — served, licence-cleared enough to be listed, NOT auto-updating"
              f"  ({len(gap)} sources / {gap_series:,} series):")
        for s, n in sorted(((s, counts[s]) for s in gap), key=lambda kv: -kv[1]):
            print(f"    {n:>12,}  {s}")

    if arch:
        print("")
        print(f"ARCHIVAL — the publisher retired these; they stay served and FROZEN "
              f"({len(arch)} sources / {arch_series:,} series). Counted as not auto-updating, "
              f"because they are not; listed apart from the work queue because no fetcher can "
              f"close them:")
        for s, e in sorted(arch.items(), key=lambda kv: -counts.get(kv[0], 0)):
            print(f"    {counts.get(s, 0):>12,}  {s:<22s} measured {e.get('measured')}")
            print(f"                  {str(e.get('finding', '')).strip()[:150]}")

    if stranded:
        tot = sum(counts.get(s, 0) for s in stranded)
        print(f"\nSCHEDULED ON PAPER, CANNOT RUN ({len(stranded)} sources / {tot:,} series) — a "
              f"fetcher-backed strategy with NO fetcher module. The orchestrator files these as "
              f"'PENDING — no adapter built' and skips them forever, however they are scheduled:")
        for s, r in sorted(stranded.items(), key=lambda kv: -counts.get(kv[0], 0)):
            print(f"    {counts.get(s, 0):>12,}  {s:<22s} {r}")

    orphan = sched - served
    if orphan:
        print(f"\nSCHEDULED BUT NOT SERVED ({len(orphan)}) — refreshed on disk, reaches nobody."
              f" Gated-by-licence is a legitimate reason; verify each:")
        for s in sorted(orphan):
            print(f"    {s:<24s} {why[s]}  (catalog rows: {counts.get(s, 0):,})")

    if args.verbose:
        print("\nSCHEDULED, by mechanism:")
        for s in sorted(sched):
            print(f"    {s:<24s} {why[s]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
