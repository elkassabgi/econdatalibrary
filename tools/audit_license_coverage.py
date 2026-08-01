"""Which sources have NO licence verdict at all?

DATABASE_LICENSES_VERBATIM.md is the canonical record: every database's terms quoted verbatim
and adversarially verified. A source ABSENT from it has not been cleared and has not been
refused - it has never been asked. That is a different state from "restricted", and it is the
easier one to miss, because nothing anywhere says no.

The rule this enforces: a catalogue row is an offer to serve, so a source with no verdict must
not have one. Serving is the thing that needs positive evidence; not-serving is the default.

Reported in three buckets, because the risk differs by an order of magnitude:
  SERVED + UNASSESSED    catalogue rows exist and no verdict does -> stop serving, or assess now
  SCHEDULED + UNASSESSED refreshing data nobody can reach; correct today, but assess it
  STORED + UNASSESSED    data on disk only

    python tools/audit_license_coverage.py
"""
from __future__ import annotations

import glob
import os
import re
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

AUDIT = os.path.join(ROOT, "DATABASE_LICENSES_VERBATIM.md")


def assessed() -> dict[str, str]:
    """-> {source_id: decision tier or verdict}.

    THE AUDIT RECORDS CLEARANCES IN TWO FORMATS, and reading only one is how this tool first
    reported 30 served-but-unassessed sources when the real figure is far smaller:

      1. a per-provider section with `**Databases (n):** `a`, `b`` and `**Decision tier:**`;
      2. a GROUP TABLE - "National statistical offices + UN SDG + WHR - verified & un-gated
         2026-07-21" clears nine PxWeb NSOs as `| scb | CC0 1.0 | CLEARED (open) | ...`, with
         the source id as the first cell and no `**Databases**` line anywhere.

    Every one of those nine came out "unassessed" on the first pass. A tool that reports a
    licence gap has to be right about it: the number drives whether data gets withdrawn.

    Table rows are only trusted when the first cell looks like a source id (lowercase, digits,
    underscore). Other tables in the file are keyed by PROVIDER NAME - "World Trade
    Organization (WTO)", "Deutsche Bundesbank time series" - and those must not be mistaken for
    ids, so the shape test is what separates them.
    """
    txt = open(AUDIT, encoding="utf-8").read()
    out: dict[str, str] = {}
    for sec in re.split(r"\n#+ ", txt):
        m = re.search(r"\*\*Databases[^:]*:\*\*\s*(.+)", sec)
        if not m:
            continue
        tier = re.search(r"\*\*Decision tier:\*\*\s*(.+)", sec)
        for sid in re.findall(r"`([a-z0-9_]+)`", m.group(1)):
            out[sid] = (tier.group(1).strip() if tier else "(no tier stated)")

    for line in txt.splitlines():
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 3 or not re.fullmatch(r"[a-z0-9_]+", cells[0]):
            continue
        verdict = next((c for c in cells[1:] if "CLEARED" in c or "RESTRICT" in c
                        or "REVIEW" in c), cells[2])
        out.setdefault(cells[0], f"{verdict} (group table)")
    return out


def main() -> int:
    known = assessed()
    print(f"licence audit covers {len(known)} source id(s)")

    con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=180.0)
    con.execute("PRAGMA busy_timeout = 180000")
    cat = dict(con.execute("select source_id, count(*) from series group by 1").fetchall())
    con.close()

    from updater import registry
    reg = {s["source_id"]: s for s in registry.load()["sources"]}
    live = {k for k, v in reg.items() if v.get("live") is True}

    stored = set()
    for d in glob.glob(os.path.join(ROOT, "data", "*", "*")):
        if os.path.isdir(d) and glob.glob(os.path.join(d, "*.parquet")):
            stored.add(os.path.basename(d))

    # WHAT LICENCE IS EACH SOURCE ACTUALLY SERVED UNDER? A source id absent from the audit is
    # not automatically unassessed: most of the remainder are per-flow variants of an assessed
    # publisher (the nine imf_*_direct sources are served under `imf-terms`, unesco_natmon and
    # unesco_sdg under the publisher-wide UIS terms). Reporting those as "no verdict" would
    # manufacture a compliance panic and bury the ones that genuinely have none.
    #
    # It is NOT a clean bill either: the inheritance is asserted in scattered code comments
    # rather than recorded in the audit, so nothing checks it. Print the licence and whether it
    # is reservable, and let the two columns be read together.
    con2 = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=180.0)
    con2.execute("PRAGMA busy_timeout = 180000")
    lic = dict(con2.execute(
        "select source_id, license_id from source").fetchall())
    resv = dict(con2.execute("select license_id, reservable from license").fetchall())
    con2.close()

    served_un = sorted(s for s in cat if s not in known)
    sched_un = sorted(s for s in live if s not in known and s not in cat)
    stored_un = sorted(s for s in stored if s not in known and s not in cat and s not in live)

    print(f"\nSERVED, NOT NAMED IN THE AUDIT BY ID ({len(served_un)}) — read the licence column: "
          f"a reservable licence means a verdict exists for the PUBLISHER, just not for this id")
    print(f"   {'source':26s} {'rows':>10s}  live   {'license_id':28s} reservable")
    naked = []
    for s in served_un:
        lid = lic.get(s) or "(none)"
        r = resv.get(lid)
        flag = "" if r else "   <-- NO reservable licence either"
        if not r:
            naked.append(s)
        print(f"   {s:26s} {cat[s]:>10,}  {str(s in live):5s}  {lid:28s} "
              f"{'yes' if r else 'NO':10s}{flag}")
    if naked:
        print(f"\n   {len(naked)} of these have NEITHER an audit entry NOR a reservable "
              f"licence: {', '.join(naked)}")
    print(f"\nSCHEDULED + UNASSESSED — refreshing, reaches nobody ({len(sched_un)})")
    for s in sched_un:
        n = sum(1 for _ in glob.glob(os.path.join(ROOT, "data", "*", s, "*.parquet")))
        print(f"   {s:26s} {n:>3} parquet file(s)")
    print(f"\nSTORED + UNASSESSED — on disk only ({len(stored_un)})")
    for s in stored_un:
        print(f"   {s}")
    print(f"\ntotals: served+unassessed {len(served_un)}, scheduled+unassessed "
          f"{len(sched_un)}, stored-only+unassessed {len(stored_un)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
