"""Split the daily digest's ATTENTION list into PERMANENT-BY-CONSTRUCTION and ACTIONABLE.

The digest names 37 sources needing attention every single morning. A list that is always 37 long
cannot distinguish a new problem from a known one, which is the same failure as a cost guard that
refuses free runs or a staleness check that always names two retired sources: the reader stops
reading it, and then there is no alert.

Two causes are structural and can never clear on their own:

  SUBSET      the source serves a curated subset of what it fetches, so changed keys legitimately
              have no catalogue mapping. `_classify_zero_mapped` demotes unless the source
              DECLARES `catalog_scope: subset` - and even then refuses the exception when the
              changed-set is cursor-cap-saturated, because truncated evidence is not evidence
              (R497, which caught a version of this that would have frozen 598 served eia tables).
  BUDGET      the source has more sub-units than a run's budget allows, so it returns `partial`
              with "deferred by budget" forever and never stamps last_success (R303 makes the
              deferral non-failing, but the STATUS still never goes green).

Everything else is a real event a person should look at.

Reads the state store; sends nothing and changes nothing.
"""
import os
import re
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

STATE = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))), "data", "_aqueduct", "state.db")

BUCKETS = [
    ("SUBSET  (permanent until catalog_scope is settled)", re.compile(
        r"csv coherence unmet|csv coverage note", re.I)),
    ("BUDGET  (permanent until the unit fits a window)", re.compile(
        r"deferred by budget", re.I)),
    ("SHRINK GUARD  (refusing to publish a smaller file)", re.compile(
        r"refusing shrink", re.I)),
    ("SCHEMA BREAK  (200 but parsed 0 rows)", re.compile(
        r"returned 200 but parsed 0 rows", re.I)),
    ("MIGRATION  (a one-time re-key not finished)", re.compile(
        r"migration has not completed", re.I)),
    ("TRANSIENT  (will retry - a real event)", re.compile(
        r"transient-failed|UNEXPECTED:|UnitTimeout", re.I)),
]


def main() -> int:
    con = sqlite3.connect("file:%s?mode=ro" % STATE, uri=True)
    con.execute("PRAGMA busy_timeout=8000")
    rows = con.execute(
        "SELECT source_id, status, COALESCE(last_error,'') FROM unit_state "
        "WHERE status NOT IN ('ok','no_change')").fetchall()

    seen, out = set(), {}
    for sid, status, err in rows:
        if sid in seen:
            continue
        seen.add(sid)
        label = "OTHER"
        for name, pat in BUCKETS:
            if pat.search(err or ""):
                label = name
                break
        out.setdefault(label, []).append((sid, status, (err or "")[:60]))

    total = sum(len(v) for v in out.values())
    print(f"{total} source(s) not in ok/no_change\n")

    order = [b[0] for b in BUCKETS] + ["OTHER"]
    structural = 0
    for label in order:
        items = out.get(label)
        if not items:
            continue
        if label.startswith(("SUBSET", "BUDGET")):
            structural += len(items)
        print(f"{label} — {len(items)}")
        for sid, status, err in sorted(items):
            print(f"    {sid[:34]:<36}{status:<16}{err}")
        print()

    print(f"PERMANENT BY CONSTRUCTION: {structural} of {total} "
          f"({100 * structural / max(total, 1):.0f}%)")
    print("Those cannot clear without a decision - declaring a catalog_scope, or giving a")
    print("unit a window it fits. Until then the morning email says ATTENTION every day and")
    print("a genuinely new failure has to be spotted inside a list that never shrinks.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
