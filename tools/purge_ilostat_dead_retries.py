"""One-time purge of ilostat's 50,000 structurally-dead csv_retry_queue rows.

SCOPE, fixed by the adversarial review of 2026-08-31 (which REFUTED the wider purge):
  * The rows were enqueued by the csv-fence swallow bug (R353's class, fixed the same day):
    an entire CURSOR_CAP batch booked as "csv_derive crashed: UnitTimeout(...)".
  * They are STORE-GRAIN ids that never had CSVs at all — the reviewer HEAD'd 104 sampled
    ids across the residue: 104/104 are 404 on R2, and 0/100 exist in catalog.db. They can
    never resolve on ANY backend.
  * abs (100,000), imf_qgfs_direct (20,502) and scb (2,682) carry the same residue but are
    UNPREFIXED store keys, which the drain's 02005b9c8 auto-purge removes loudly on each
    source's next run — leave them to it (verify the purge lines in those run logs).
  * ilostat's 50,000 alone are `ilostat:`-prefixed, so they PASS the drain's prefix filter
    and would instead be re-attempted 20,000/run, refail on ResolveError, and burn derive
    budget forever. These need the manual purge.
  * usda's 48,047 are designed budget DEFERRALS ("none failed") and MUST stay.

MECHANICS (R263/R481's resurrection guard): the authoritative queue lives in the R2-hosted
state store; the local file is a working copy. A local-only purge is resurrected by the next
push. So: pull-state → purge with the EXACT predicate (refusing on any count but 50,000) →
push-state, with the CAS refusing if a writer raced us. Run only while CI is idle (R5).

Usage: py tools/purge_ilostat_dead_retries.py [--apply]
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

PREDICATE = ("source_id = 'ilostat' AND "
             "last_error LIKE 'csv_derive crashed: UnitTimeout%'")
EXPECTED = 50_000


def run_module(*args):
    p = subprocess.run([sys.executable, "-m", "updater.run", *args], cwd=ROOT,
                       capture_output=True, text=True, encoding="utf-8", errors="replace",
                       timeout=1800)
    tail = (p.stdout or "").strip().splitlines()[-3:]
    for ln in tail:
        print("   ", ln)
    return p.returncode


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    print("pull-state (authoritative queue lives on R2; local is a working copy) ...")
    if run_module("--pull-state") != 0:
        raise SystemExit("pull-state failed — NOT proceeding (would purge a stale copy)")

    from updater.state import StateStore
    st = StateStore()
    n = st.db.execute(
        "SELECT COUNT(*) FROM csv_retry_queue WHERE " + PREDICATE).fetchone()[0]
    others = st.db.execute(
        "SELECT source_id, COUNT(*) FROM csv_retry_queue GROUP BY source_id").fetchall()
    print("matching the predicate:", format(n, ","))
    print("full queue by source  :", ", ".join("%s=%s" % (s, format(c, ",")) for s, c in others))

    if n != EXPECTED:
        raise SystemExit(
            "REFUSING: predicate matches %s rows, expected exactly %s. The queue moved "
            "since the review measured it — re-measure and re-scope before purging."
            % (format(n, ","), format(EXPECTED, ",")))

    if not a.apply:
        print("(dry run — pass --apply to purge and push)")
        return 0

    st.db.execute("DELETE FROM csv_retry_queue WHERE " + PREDICATE)
    st.db.commit()
    left = st.db.execute(
        "SELECT COUNT(*) FROM csv_retry_queue WHERE source_id='ilostat'").fetchone()[0]
    print("purged; ilostat rows remaining:", left)

    print("push-state (CAS refuses if a writer raced us) ...")
    rc = run_module("--push-state")
    if rc != 0:
        raise SystemExit(
            "push-state failed (rc=%d) — the purge is LOCAL ONLY and will be resurrected "
            "by the next pull. Re-run this tool when the store is quiet." % rc)
    print("purge committed to the authoritative store.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
