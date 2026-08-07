"""Does a budgeted sweep advance, and is anything actually left uncovered?

TWO DIFFERENT QUESTIONS, and the first version of this tool conflated them — which is how it
reproduced a claim I had to retract the same day (ledger R379):

  1. WASTE — do the budget-capped runs re-walk the same prefix? Measured from the sorted
     POSITION of each capped run's first-deferred sub-unit. Hovering means each capped run
     redoes work the previous one already did.
  2. COVERAGE — is anything therefore never fetched? This does NOT follow from (1). A source
     may also get UNCAPPED runs that cover everything. ecb has both: seven capped runs
     hovering at positions 191-297 of 540, AND four runs of ~4,000 s with no deferral at all
     (+7.17M / +6.20M / +5.85M / +5.87M rows) that cover the whole list. Its tail is stale
     between full passes, not unfetched — and only 15 of its 540 store objects are more than
     30 days old.

So this tool reads the FULL run history, never a subset filtered to deferral notes. Filtering
`runs` to rows mentioning the symptom is precisely the bug: it cannot see the runs that
disprove the conclusion.

It also refuses to answer the coverage question from run notes at all. Run notes prove a
sweep was truncated; only the STORE proves what was never fetched — a zero-row sub-unit, or
an object whose age is far past the source's cadence. Use tools/store_inventory.py and the
object timestamps for that.

  python tools/audit_sweep_advance.py
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

_FIRST = re.compile(r"\[([^,\]]+)")
_DEFER = re.compile(r"defer|budget", re.I)
_SPAN_FRACTION = 0.35
# "Not budget-capped" is NOT the same as "covered the list". insee_melodi has a run whose
# note mentions neither defer nor budget — because it reads "129/144 sub-unit(s)
# transient-failed". Counting that as coverage would repeat R379's overclaim in the opposite
# direction, exonerating a source on the strength of a run that mostly failed. A run only
# counts as a full pass if it neither deferred NOR failed sub-units en masse.
_FAILED = re.compile(r"(\d+)\s*/\s*(\d+)\s+sub-unit", re.I)


def _is_full_pass(d: dict) -> bool:
    note = d.get("note") or ""
    if (d.get("dur_s") or 0) <= 0:
        return False
    if _DEFER.search(note):
        return False
    m = _FAILED.search(note)
    if m:
        failed, total = int(m.group(1)), int(m.group(2))
        if total and failed / total > 0.10:      # a mostly-failed run covered nothing
            return False
    return True


def _sorted_units(source: str) -> list[str]:
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

    # THE WHOLE history per source. Never filtered to the symptom.
    runs = collections.defaultdict(list)
    for r in con.execute("SELECT * FROM runs ORDER BY rowid"):
        d = dict(zip(cols, r))
        if a.source and d["source_id"] != a.source:
            continue
        runs[d["source_id"]].append(d)

    wasteful = []
    for src, rs in sorted(runs.items()):
        capped = [d for d in rs if _DEFER.search(d.get("note") or "")]
        if not capped:
            continue                                   # never budget-capped: nothing to judge
        full = [d for d in rs if _is_full_pass(d)]
        units = _sorted_units(src)
        pos = []
        for d in capped[-8:]:
            m = _FIRST.search(d.get("note") or "")
            if not m:
                continue
            stem = m.group(1).split(":")[0].strip()
            for cand in (stem, stem[:-8] if stem.endswith(".parquet") else stem):
                if cand in units:
                    pos.append(units.index(cand))
                    break

        print(f"\n{src}: {len(rs)} run(s) total — {len(capped)} budget-capped, "
              f"{len(full)} not capped; list length {len(units) or '?'}")
        if len(pos) < 2:
            print("   sub-unit labels are not store stems — judge the WASTE question by "
                  "reading the loop")
        else:
            span = max(pos) - min(pos)
            frac = span / len(units) if units else 0
            print(f"   capped runs' first-deferred positions: {pos}  (span {frac:.0%})")
            if units and frac < _SPAN_FRACTION:
                wasteful.append(src)
                print(f"   *** WASTEFUL — capped runs re-walk the same prefix, so each one "
                      f"redoes the previous one's work. Wire rotate_after/save_rotation.")
            else:
                print("   capped runs advance across the list")

        # COVERAGE is a separate question and is NOT answered here.
        if full:
            print(f"   coverage: {len(full)} uncapped run(s) exist "
                  f"(longest {max((d.get('dur_s') or 0) for d in full):.0f}s) — the tail is "
                  f"reached by those, so it is STALE between them, not unfetched")
        else:
            print("   coverage: NO uncapped run recorded — the tail may genuinely never be "
                  "fetched, but CONFIRM AGAINST THE STORE (zero-row sub-units / object ages), "
                  "never from these notes alone")

    print(f"\n{'WASTEFUL (capped runs redo a prefix): ' + ', '.join(wasteful) if wasteful else 'no hovering sweeps detected'}")
    print("Coverage claims require store evidence — see tools/store_inventory.py. R379.")
    return 1 if wasteful else 0


if __name__ == "__main__":
    sys.exit(main())
