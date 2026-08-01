"""Where does each served source update, and has it PROVEN it — cloud, local, or pending?

Three questions the coverage audit does not separate:

  WHERE      run_location: `local` means the workstation heavy job; anything else runs in CI.
  STATE      four outcomes, not two. `last_success_utc` alone is the wrong test: the
             honest-status contract deliberately WITHHOLDS it on a partial run, and a partial
             run still merged the data it got.
               OK       a clean sweep has completed
               PARTIAL  ran and merged what it got; some sub-units retry next tick
               FAILED   ran and failed outright
               NEVER    no attempt has ever been recorded -- a source can be live,
                        adapter-present and correctly configured and still never have executed.
                        33 units were in exactly that state until stalest-first ordering landed.
  PENDING    served to users but not scheduled at all: it will never refresh.

SERVED means catalogued AND resolvable by the worker, because a source that fails either is not
reaching anyone whatever its update status.
"""
from __future__ import annotations

import os
import re
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def supported() -> set:
    ts = open(os.path.join(ROOT, "api", "worker", "src", "util.ts"), encoding="utf-8").read()
    blk = ts.split("SUPPORTED_SOURCES: readonly string[] = [", 1)[1].split("];", 1)[0]
    return set(re.findall(r'"([a-z0-9_]+)"', re.sub(r"//[^\n]*", "", blk)))


def main() -> int:
    from updater import registry
    from updater.state import StateStore

    sup = supported()
    con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=180.0)
    con.execute("PRAGMA busy_timeout = 180000")
    cat = dict(con.execute("select source_id, count(*) from series group by 1").fetchall())
    con.close()

    reg = {s["source_id"]: s for s in registry.load()["sources"]}
    units = {u.source_id: u for u in registry.all_units()}
    store = StateStore()

    served = {s: n for s, n in cat.items() if s in sup and n}
    STATES = ("ok", "partial", "failed", "never")
    buckets: dict = {f"{w}_{k}": [] for w in ("cloud", "local") for k in STATES}
    buckets["pending"] = []
    for s, n in served.items():
        entry = reg.get(s)
        live = bool(entry and entry.get("live") is True)
        if not live:
            buckets["pending"].append((s, n))
            continue
        loc = (units[s].config or {}).get("run_location") if s in units else None
        st = store.get_unit(s, "_all") or {}
        # FOUR states, not two. `last_success_utc` alone is the wrong test: the honest-status
        # contract deliberately WITHHOLDS it on a partial run, and a partial run still merged
        # the data it got. unesco_sdg and unesco_natmon each ran ~50 minutes in the 2026-08-01
        # CI pass and came out partial; calling them "no successful run" would report them as
        # not updating when they are.
        if st.get("last_success_utc"):
            state = "ok"
        elif not st.get("last_attempt_utc"):
            state = "never"
        elif (st.get("status") or "") == "partial":
            state = "partial"
        else:
            state = "failed"
        where = "local" if loc == "local" else "cloud"
        buckets[f"{where}_{state}"].append((s, n))

    def line(label, key):
        rows = sorted(buckets[key], key=lambda r: -r[1])
        print(f"{label:52s} {len(rows):>4} sources {sum(r[1] for r in rows):>12,} series")
        return rows

    print(f"SERVED (catalogued AND resolvable): {len(served)} sources, "
          f"{sum(served.values()):,} series\n")
    line("CLOUD  — clean run completed (OK)", "cloud_ok")
    line("CLOUD  — updating, last run PARTIAL", "cloud_partial")
    cfail = line("CLOUD  — last run FAILED", "cloud_failed")
    cnev = line("CLOUD  — never attempted", "cloud_never")
    line("LOCAL  — clean run completed (OK)", "local_ok")
    line("LOCAL  — updating, last run PARTIAL", "local_partial")
    lfail = line("LOCAL  — last run FAILED", "local_failed")
    lnev = line("LOCAL  — never attempted", "local_never")
    pd = line("PENDING — served but NOT scheduled at all", "pending")

    for label, rows in (("CLOUD failed", cfail), ("CLOUD never attempted", cnev),
                        ("LOCAL failed", lfail), ("LOCAL never attempted", lnev)):
        if rows:
            print(f"\n{label}:")
            for s, n in rows:
                print(f"   {s:26s} {n:>12,}")
    print("\nPENDING, largest first:")
    for s, n in pd[:18]:
        print(f"   {s:26s} {n:>12,}")
    if len(pd) > 18:
        print(f"   … and {len(pd)-18} more")
    return 0


if __name__ == "__main__":
    sys.exit(main())
