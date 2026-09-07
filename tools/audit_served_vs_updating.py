#!/usr/bin/env python3
"""How many databases are served and updating, served and stale, and not served at all?

WHY THIS EXISTS. Ahmed asked exactly this on 2026-09-06 and answering it took composing four
separate helpers by hand. It is a standing question - the kind that gets asked again - and a
question answered by hand is answered differently each time.

EVERY POPULATION COMES FROM AN EXISTING SOURCE OF TRUTH, never a fresh definition (R262 - the
measuring instrument is a claim too, and it decays):

    served      tools/audit_schedule_coverage.py::supported_sources()
                MINUS api/worker/src/denylist.ts::NON_REDISTRIBUTABLE
                The denylist is the gate that answers 451. SUPPORTED_SOURCES alone only means the
                worker can RESOLVE an id, so subtracting the gate is what makes the count "served".
    updating    data/_aqueduct/state.db `source_state`, judged against each source's OWN cadence,
                with the same generous budgets tools/cost/source_staleness.py uses. A source is
                only called stale when it is well past its own cadence, so this answers "which
                have clearly stopped", not "which ran late once".
    registered  updater/registry.yaml

THE TRAP THIS TOOL EXISTS TO AVOID (R838). `source_state` is the CLOUD updater's table. Sources on
the LOCAL route - the big desktop crawlers - never write a row in it, so judging them by it reports
"never scheduled" about crawlers that are running right now, whose PIDs you can see. Those land in
a NO STATE ROW bucket that is explicitly NOT a verdict, and the only honest way to resolve one is
to read that source's own log or store, not this table.

Local and free: no D1, no R2, no network.

    python tools/audit_served_vs_updating.py
    python tools/audit_served_vs_updating.py --list-no-state
"""
from __future__ import annotations

import argparse
import datetime as dt
import io
import os
import re
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
STATE = os.path.join(ROOT, "data", "_aqueduct", "state.db")

# the same generous multiples tools/cost/source_staleness.py uses: a source is called stale only
# when it is well past its own cadence
BUDGET_DAYS = {"daily": 7, "weekly": 21, "biweekly": 35, "monthly": 75,
               "quarterly": 200, "annual": 500, "irregular": 180}
DEFAULT_DAYS = 90


def denylisted() -> set:
    """The gate that answers 451. Fails CLOSED: an unparseable or implausibly short list would
    OVERSTATE what is served, which is the one direction that matters here."""
    p = os.path.join(ROOT, "api", "worker", "src", "denylist.ts")
    m = re.search(r"NON_REDISTRIBUTABLE[^=]*=\s*new Set(?:<[^>]*>)?\(\[(.*?)\]\)",
                  io.open(p, encoding="utf-8").read(), re.S)
    if not m:
        raise RuntimeError(f"could not parse NON_REDISTRIBUTABLE in {p}")
    ids = set(re.findall(r'"([a-z0-9_]+)"', m.group(1)))
    if len(ids) < 10:
        raise RuntimeError(f"parsed only {len(ids)} denylist ids, implausibly few")
    return ids


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--list-no-state", action="store_true",
                    help="name the served sources with no source_state row (NOT a failure list)")
    a = ap.parse_args()

    import yaml                                                      # noqa: PLC0415
    from tools.audit_schedule_coverage import supported_sources      # noqa: PLC0415

    supported = set(supported_sources())
    deny = denylisted()
    served = supported - deny

    reg = yaml.safe_load(io.open(os.path.join(ROOT, "updater", "registry.yaml"),
                                 encoding="utf-8"))
    registered = {s["source_id"] for s in reg["sources"]}

    con = sqlite3.connect(f"file:{STATE}?mode=ro", uri=True)
    rows = list(con.execute(
        "SELECT source_id, cadence, status, last_success_utc FROM source_state"))
    con.close()
    now = dt.datetime.now(dt.timezone.utc)

    state = {}
    for sid, cadence, status, last in rows:
        days = None
        if last:
            try:
                days = (now - dt.datetime.fromisoformat(last.replace("Z", "+00:00"))).days
            except Exception:                                        # noqa: BLE001
                days = None
        state[sid] = {"cadence": cadence, "status": status, "days": days,
                      "budget": BUDGET_DAYS.get(str(cadence), DEFAULT_DAYS)}

    current = [s for s in served
               if s in state and state[s]["days"] is not None
               and state[s]["days"] <= state[s]["budget"]]
    stale = [s for s in served
             if s in state and (state[s]["days"] is None
                                or state[s]["days"] > state[s]["budget"])]
    nostate = sorted(s for s in served if s not in state)
    unserved = sorted(registered - served - deny)

    print(f"SUPPORTED_SOURCES (worker can resolve an id) : {len(supported):>5}")
    print(f"  gated - answer 451, hidden from catalogue  : {len(deny):>5}"
          f"   ({len(supported & deny)} of them in SUPPORTED_SOURCES)")
    print(f"SERVED = supported minus gated               : {len(served):>5}")
    print(f"registered in updater/registry.yaml          : {len(registered):>5}")
    print(f"rows in source_state                         : {len(state):>5}")
    print()
    print(f"(a) SERVED and current for its own cadence   : {len(current):>5}")
    print(f"(b) SERVED but PAST its cadence budget       : {len(stale):>5}")
    for s in sorted(stale):
        st = state[s]
        d = "never" if st["days"] is None else f"{st['days']} days"
        print(f"      {s:<28}{str(st['cadence']):<11}{d} (budget {st['budget']})")
    print(f"(c) REGISTERED, not gated, but NOT served    : {len(unserved):>5}")
    for s in unserved:
        print(f"      {s}")
    print(f"(d) GATED - hosting nothing, on purpose      : {len(deny):>5}")
    print()
    print(f"NOT A VERDICT - {len(nostate)} served source(s) have NO source_state row.")
    print("   `source_state` is the CLOUD updater's table. Sources on the LOCAL route never")
    print("   write it, so it reports 'never scheduled' about crawlers that are running (R838).")
    print("   Resolve one by reading ITS OWN log or store, never this table.")
    print(f"   of those, in registry.yaml : {len(set(nostate) & registered)}")
    print(f"   of those, not in registry  : {len(set(nostate) - registered)}")
    if a.list_no_state:
        for s in nostate:
            print(f"      {s:<28}{'in registry' if s in registered else 'not in registry'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
