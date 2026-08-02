"""How many databases are FULLY DONE — hosted AND auto-updating — and how many are left?

Two independent dimensions, because all of this session's failures came from one
being true while the other quietly was not:

  SERVABLE  a user can find and download it
            = catalog rows > 0  AND  source in the worker's SUPPORTED_SOURCES
  UPDATING  it refreshes itself without human intervention
            = in updater/registry.yaml with live: true  AND  health == OK

"health" is NOT a definition invented here — it is updater.health.assess(), the
project's own SLA/data-recency classifier (RED-SLA = job has not succeeded within
tolerance, RED-DATA = job succeeds but the newest observation has gone stale,
RED-UNRUN = never succeeded, PENDING = adapter not built yet).

The interesting number is neither total alone: a source can be downloadable and
frozen, or updating nightly and invisible (boe was serving 21 of 30,674 series this
morning while its fetcher ran daily). FULLY DONE requires both.
"""
from __future__ import annotations

import os
import re
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import health, registry                        # noqa: E402


def supported_sources() -> set:
    ts = open(os.path.join(ROOT, "api", "worker", "src", "util.ts"),
              encoding="utf-8").read()
    m = re.search(r"SUPPORTED_SOURCES[^=]*=\s*(?:new Set\()?\[(.*?)\]", ts, re.S)
    return set(re.findall(r'"([a-z0-9_]+)"', m.group(1))) if m else set()


def main() -> int:
    sup = supported_sources()
    cat = {r[0]: r[1] for r in sqlite3.connect(
        os.path.join(ROOT, "data", "catalog.db")).execute(
        "SELECT source_id, COUNT(*) FROM series GROUP BY source_id")}
    reg = {e["source_id"]: e for e in registry.load().get("sources", [])}
    rep = health.assess()
    hz = {r["source"]: r["health"] for r in rep["sources"]}

    universe = sorted(set(reg) | set(cat))
    rows = []
    for sid in universe:
        e = reg.get(sid)
        live = bool(e and e.get("live"))
        h = hz.get(sid, "NOT-IN-REGISTRY")
        servable = cat.get(sid, 0) > 0 and sid in sup
        updating = live and h == "OK"
        rows.append((sid, cat.get(sid, 0), bool(e), live, h, servable, updating))

    done = [r for r in rows if r[5] and r[6]]
    serv_only = [r for r in rows if r[5] and not r[6]]
    upd_only = [r for r in rows if r[6] and not r[5]]
    neither = [r for r in rows if not r[5] and not r[6]]

    print("=" * 74)
    print("DATABASE COMPLETION — servable (downloadable) x updating (auto-refreshing)")
    print("=" * 74)
    print(f"total distinct databases (registry OR catalogued) : {len(rows)}")
    print(f"  registry sources (the auto-update fleet)        : {len(reg)}")
    print(f"  catalogued sources (what users can search)      : {len(cat)}")
    print()
    print(f"FULLY DONE   servable AND updating                : {len(done)}")
    print(f"servable, NOT updating                            : {len(serv_only)}")
    print(f"updating, NOT servable                            : {len(upd_only)}")
    print(f"neither                                           : {len(neither)}")
    print()

    print("--- servable but NOT updating (users can download; nothing refreshes it) ---")
    by_reason: dict = {}
    for sid, n, inreg, live, h, s, u in sorted(serv_only, key=lambda r: -r[1]):
        reason = ("not in updater registry" if not inreg
                  else ("registry entry but live=false" if not live else f"live but health={h}"))
        by_reason.setdefault(reason, []).append((sid, n))
    for reason, items in sorted(by_reason.items(), key=lambda x: -sum(i[1] for i in x[1])):
        tot = sum(i[1] for i in items)
        print(f"  {reason}: {len(items)} source(s), {tot:,} series")
        for sid, n in items[:8]:
            print(f"      {sid:<24} {n:>9,} series")
        if len(items) > 8:
            print(f"      ... and {len(items) - 8} more")

    print()
    print("--- updating but NOT servable (refreshing data nobody can download) ---")
    for sid, n, inreg, live, h, s, u in sorted(upd_only, key=lambda r: r[0]):
        why = "no catalog rows" if n == 0 else "not in SUPPORTED_SOURCES"
        print(f"      {sid:<24} {n:>9,} series   ({why})")
    if not upd_only:
        print("      none")

    print()
    print("--- the auto-update fleet, by health ---")
    fleet: dict = {}
    for sid, e in reg.items():
        key = ("live" if e.get("live") else "not-live") + " / " + hz.get(sid, "?")
        fleet[key] = fleet.get(key, 0) + 1
    for k, v in sorted(fleet.items(), key=lambda x: -x[1]):
        print(f"      {k:<28} {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
