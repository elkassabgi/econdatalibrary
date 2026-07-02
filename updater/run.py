"""CLI runner for the Aqueduct orchestrator.

  python -m updater.run --dry                      # show what's due, do nothing
  python -m updater.run --source bcrp              # update one source
  python -m updater.run --strategy extend_by_date  # all S2 sources
  python -m updater.run --cadence daily            # everything on the daily cadence
  python -m updater.run --source treasury --force  # force a run regardless of cadence
"""
from __future__ import annotations
import argparse

from . import orchestrate


def main():
    ap = argparse.ArgumentParser(description="Aqueduct continuous-update runner")
    ap.add_argument("--source", action="append", help="limit to source_id (repeatable)")
    ap.add_argument("--strategy", action="append", help="limit to strategy (repeatable)")
    ap.add_argument("--cadence", action="append", help="limit to cadence (repeatable)")
    ap.add_argument("--force", action="store_true", help="ignore cadence + change-detection")
    ap.add_argument("--dry", action="store_true", help="report what's due, make no changes")
    a = ap.parse_args()

    res = orchestrate.run_once(sources=a.source, strategies=a.strategy, cadences=a.cadence,
                               force=a.force, dry=a.dry)
    print(f"\n=== {len(res)} unit(s) processed ===")
    for k, s in res:
        print(f"  {s:16} {k}")


if __name__ == "__main__":
    main()
