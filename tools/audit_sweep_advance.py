"""Does a budgeted sweep actually ADVANCE, or re-walk the same prefix forever?

Reads the run history in state.db and, for every source whose runs report budget deferrals,
compares the SORTED POSITION of each run's first-deferred sub-unit. A sweep that resumes
moves that position across the list; a truncated one hovers in the same region regardless of
what the sub-unit is called.

WHY POSITION AND NOT NAME. The obvious check — "does the first-deferred sub-unit differ
between runs?" — gives a FALSE ALL-CLEAR. Measured 2026-08-07: ecb's first-deferred file was
DIFFERENT in all seven of its recorded runs, which a name-based check reads as healthy. Every
one of those names was inside the same `ECB__CSEC__M__*` block — positions 191..297 of 540 —
so the sweep stopped at a slightly different point in the SAME prefix each run depending on
upstream speed, and indices 297-539 went untouched. Compare `abs`, whose three runs deferred
from genuinely different regions; that is what advancing looks like.

This complements tests/test_budget_needs_resumption.py rather than duplicating it. That test
is static and asks "is a resumption mechanism present"; this one is empirical and asks "did
the last few runs actually make progress" — so it catches a mechanism that exists but does
not work, which is the failure mode R377 was written about ("persists something" is not
"resumes").

  python tools/audit_sweep_advance.py               # all sources with deferral notes
  python tools/audit_sweep_advance.py --source ecb
"""
from __future__ import annotations

import argparse
import collections
import os
import re
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

# A run's note lists the deferred sub-units; the FIRST is where the budget bit.
_FIRST = re.compile(r"\[([^,\]]+)")
# How much of the list the first-deferred position must range over before we call it
# advancing. Deliberately generous: a source that genuinely rotates moves a long way.
_SPAN_FRACTION = 0.35


def _sorted_units(source: str) -> list[str]:
    """The sweep's ordering, best-effort: the store's sorted parquet stems, which is what
    blob.list_parquets returns and therefore what most of these loops iterate."""
    try:
        from tools.store_inventory import r2_store_files
        return sorted(r2_store_files(source))
    except Exception:                                            # noqa: BLE001
        return []


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source")
    a = ap.parse_args()

    db = os.path.join(ROOT, "data", "_aqueduct", "state.db")
    if not os.path.exists(db):
        print(f"no state store at {db}")
        return 2
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    cols = [r[1] for r in con.execute("PRAGMA table_info(runs)")]
    seen = collections.defaultdict(list)
    for r in con.execute("SELECT * FROM runs ORDER BY rowid"):
        d = dict(zip(cols, r))
        note = d.get("note") or ""
        if "deferred" not in note and "budget" not in note.lower():
            continue
        if a.source and d["source_id"] != a.source:
            continue
        m = _FIRST.search(note)
        if m:
            seen[d["source_id"]].append(m.group(1).strip())

    if not seen:
        print("no runs report budget deferrals — nothing to judge")
        return 0

    bad = []
    for src, firsts in sorted(seen.items()):
        firsts = firsts[-8:]
        units = _sorted_units(src)
        pos = []
        for f in firsts:
            stem = f.split(":")[0].strip()
            for cand in (stem, stem[: -len(".parquet")] if stem.endswith(".parquet") else stem):
                if cand in units:
                    pos.append(units.index(cand))
                    break
        print(f"\n{src}: {len(firsts)} run(s) with deferrals, list length {len(units) or '?'}")
        if len(pos) < 2:
            print("   positions unresolved (sub-unit names are not store stems) — "
                  "judge this one by reading the loop")
            continue
        span = max(pos) - min(pos)
        frac = span / len(units) if units else 0
        print(f"   first-deferred positions: {pos}")
        print(f"   span {span} of {len(units)} ({frac:.0%})  max reached {max(pos)}")
        if units and frac < _SPAN_FRACTION:
            bad.append(src)
            print(f"   *** HOVERING — the sweep re-walks the same prefix; roughly "
                  f"{len(units) - max(pos)} sub-unit(s) beyond index {max(pos)} were never "
                  f"reached in these runs")
        else:
            print("   advancing")

    print(f"\n{'SUSPECT: ' + ', '.join(bad) if bad else 'no hovering sweeps detected'}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
