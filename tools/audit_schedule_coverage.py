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


def scheduled_sources() -> tuple:
    """(set, {source: reason}) — the union of every mechanism that actually runs something."""
    why = {}
    reg = yaml.safe_load(open(REGISTRY, encoding="utf-8"))
    for s in reg["sources"]:
        if s.get("live"):
            why[s["source_id"]] = f"registry live ({s.get('cadence', '?')})"

    heavy = open(HEAVY, encoding="utf-8").read()
    m = re.search(r"ALL='(\[[^']*\])'", heavy)
    if not m:
        raise SystemExit("could not locate the ALL=[...] matrix literal in updater-heavy.yml")
    for s in json.loads(m.group(1)):
        why.setdefault(s, "updater-heavy matrix")

    sec = open(SEC, encoding="utf-8").read()
    for sid in sorted(set(re.findall(r"\bsec_edgar(?:_xbrl)?\b", sec))):
        why.setdefault(sid, "sec-edgar-daily")
    return set(why), why


def catalog_counts() -> dict:
    if not os.path.exists(CATALOG):
        raise SystemExit(f"no catalog at {CATALOG}")
    con = sqlite3.connect(f"file:{CATALOG}?mode=ro", uri=True)
    rows = con.execute("SELECT source_id, COUNT(*) FROM series GROUP BY source_id").fetchall()
    con.close()
    return {r[0]: r[1] for r in rows if r[0]}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--verbose", action="store_true",
                    help="also list the scheduled sources and their mechanism")
    args = ap.parse_args()

    counts = catalog_counts()
    supported = supported_sources()
    sched, why = scheduled_sources()

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
    print(f"NOT scheduled           {len(gap):>6,}   {gap_series:>12,} series")
    pct_s = 100.0 * len(covered) / len(served) if served else 0.0
    pct_o = 100.0 * covered_series / served_series if served_series else 0.0
    print(f"\n>>> {len(covered)} of {len(served)} sources / {covered_series:,} of "
          f"{served_series:,} series scheduled  ({pct_s:.1f}% of sources, {pct_o:.1f}% of series)")

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
