"""State hygiene: purge qog's inert series_cursor rows from the authoritative store.

WHY. QoG's licensing office REFUSED redistribution in writing (received 2026-08-31,
recorded verbatim in DATABASE_LICENSES_VERBATIM.md §qog) and the holdings were deleted under
that decision's adversarial review. R481's lesson is that a series lives in SIX
places, and the state store is the sixth: 227,814 series_cursor rows (measured by the
2026-08-31 cursor-grain audit: qog cursors=227,814, catalogue rows=0) still reference
the deleted source. They are pure bookkeeping for data that no longer exists anywhere
we serve — inert, but they inflate every full-store scan and read as live tracking.

SCOPE — deliberately narrow:
  * ONLY `series_cursor WHERE source_id='qog'`. The registry entry and runs history
    are records of what happened, not tracking of what exists — untouched.
  * The other 13 UNCATALOGUED-in-audit sources (cboe, famafrench, owid, shiller, ...)
    are NOT deleted sources — catalogue absence is not source deletion — untouched.
  * source_state/unit_state rows for qog are REPORTED below but not deleted here;
    if the reviewer rules they go too, that is a scope bump to make explicitly.

MECHANICS: same guarded pull -> purge (exact predicate + expected-count refusal) ->
push (CAS) envelope as tools/purge_ilostat_dead_retries.py. CI-idle window (R5).

Usage: py tools/purge_qog_state_cursors.py [--apply]
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

EXPECTED = 227_814


def run_module(*args):
    p = subprocess.run([sys.executable, "-m", "updater.run", *args], cwd=ROOT,
                       capture_output=True, text=True, encoding="utf-8", errors="replace",
                       timeout=1800)
    for ln in (p.stdout or "").strip().splitlines()[-3:]:
        print("   ", ln)
    return p.returncode


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    print("pull-state ...")
    if run_module("--pull-state") != 0:
        raise SystemExit("pull-state failed — NOT proceeding (would purge a stale copy)")

    from updater.state import StateStore
    st = StateStore()
    n = st.db.execute(
        "SELECT COUNT(*) FROM series_cursor WHERE source_id='qog'").fetchone()[0]
    other = {
        "source_state": st.db.execute(
            "SELECT COUNT(*) FROM source_state WHERE source_id='qog'").fetchone()[0],
        "unit_state": st.db.execute(
            "SELECT COUNT(*) FROM unit_state WHERE source_id='qog'").fetchone()[0],
        "runs(history, kept by design)": st.db.execute(
            "SELECT COUNT(*) FROM runs WHERE source_id='qog'").fetchone()[0],
        "csv_retry_queue": st.db.execute(
            "SELECT COUNT(*) FROM csv_retry_queue WHERE source_id='qog'").fetchone()[0],
    }
    print("qog series_cursor rows:", format(n, ","))
    print("qog rows elsewhere (reported, NOT touched):", other)

    if n != EXPECTED:
        raise SystemExit(
            "REFUSING: %s rows match, expected exactly %s (the audit's measurement). "
            "Re-measure and re-scope." % (format(n, ","), format(EXPECTED, ",")))

    if not a.apply:
        print("(dry run — pass --apply to purge and push)")
        return 0

    st.db.execute("DELETE FROM series_cursor WHERE source_id='qog'")
    st.db.commit()
    left = st.db.execute(
        "SELECT COUNT(*) FROM series_cursor WHERE source_id='qog'").fetchone()[0]
    print("purged; qog series_cursor rows remaining:", left)

    print("push-state ...")
    if run_module("--push-state") != 0:
        raise SystemExit(
            "push-state failed — the purge is LOCAL ONLY and will be resurrected by "
            "the next pull. Re-run when the store is quiet.")
    print("purge committed to the authoritative store.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
