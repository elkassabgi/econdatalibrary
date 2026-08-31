"""WU-2a + WU-6: the one guarded migration session that purges dead series_cursor rows.

WHY A BUNDLE. Every purge in WU-2a/WU-6 is the same mechanism against the same 11.28 GB
object, and that object is single-writer by ETag compare-and-swap (R5). Nine separate
pull->edit->push cycles would be nine chances to lose a CAS and nine 11 GB round trips.
One session, one pull, one push, per-source receipts.

THE EXPECTATIONS ARE DATA, NOT CODE. Every count this tool enforces is read from a receipts
JSON produced by the pre-measurement pass, so nothing is transcribed by hand between the
measurement and the enforcement (transcription is exactly how a wrong number becomes an
authorised number — R500/R506). A source is touched ONLY if its receipt says decision=="GO".

ALL-OR-NOTHING PRE-FLIGHT. Every source's pre-count and match-count is verified BEFORE any
DELETE runs. One disagreement aborts the whole session with nothing written: the spec's rule
is "receipts match predictions exactly; any deviation = stop, reviewer investigates", and a
half-applied migration is the worst of both states.

THE PREDICATE CANNOT ESCAPE ITS SOURCE. The receipt supplies only the row-selecting clause;
this tool always wraps it as
    DELETE FROM series_cursor WHERE source_id = ? AND (<clause>)
so a malformed or hostile receipt cannot reach another source or another table. Clauses are
additionally screened for statement-breaking tokens.

NOT IN SCOPE, DELIBERATELY: runs history (a record of what happened, not tracking of what
exists), source_state/unit_state, and ssb — whose WU-6 line is a MAPPER TIER, not a purge.

    py tools/purge_state_cursors_bundle.py --receipts data/_aqueduct/wu6_receipts.json
    py tools/purge_state_cursors_bundle.py --receipts ... --apply
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.state import StateStore                              # noqa: E402
# The in-flight check is IMPORTED, never retyped: it already encodes the workstation lock,
# the CI-run query, and the refuse-if-unknowable rule (R191/R192).
from tools.prune_series_cursors import runs_in_flight             # noqa: E402

# Statement-breaking / scope-escaping tokens. The clause is a WHERE fragment, nothing else.
_FORBIDDEN = re.compile(
    r"(;|--|/\*|\bDROP\b|\bATTACH\b|\bDETACH\b|\bPRAGMA\b|\bINSERT\b|\bUPDATE\b|"
    r"\bDELETE\b|\bCREATE\b|\bALTER\b|\bUNION\b|\bsource_id\b)", re.I)


def run_module(*args) -> int:
    p = subprocess.run([sys.executable, "-m", "updater.run", *args], cwd=ROOT,
                       capture_output=True, text=True, encoding="utf-8",
                       errors="replace", timeout=3600)
    for ln in (p.stdout or "").strip().splitlines()[-4:]:
        print("   ", ln)
    if p.returncode != 0:
        for ln in (p.stderr or "").strip().splitlines()[-4:]:
            print("  !", ln)
    return p.returncode


def load_receipts(path: str) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        doc = json.load(f)
    rows = doc.get("receipts") if isinstance(doc, dict) else doc
    if not rows:
        raise SystemExit(f"{path}: no receipts found — an empty plan is not an empty purge")
    out = []
    for r in rows:
        need = ("source_id", "predicate", "pre_count", "match_count", "post_count", "decision")
        missing = [k for k in need if k not in r]
        if missing:
            raise SystemExit(f"receipt for {r.get('source_id', '?')} missing {missing}")
        if r["decision"] != "GO":
            print(f"  skip {r['source_id']}: decision={r['decision']!r}"
                  + (f" ({r.get('change_required')})" if r.get("change_required") else ""))
            continue
        if _FORBIDDEN.search(r["predicate"]):
            raise SystemExit(
                f"REFUSED: {r['source_id']}'s predicate contains a forbidden token "
                f"(statement break or source_id override): {r['predicate']!r}")
        if r["pre_count"] - r["match_count"] != r["post_count"]:
            raise SystemExit(
                f"REFUSED: {r['source_id']}'s own receipt does not balance: "
                f"{r['pre_count']:,} - {r['match_count']:,} != {r['post_count']:,}")
        if r["match_count"] <= 0:
            raise SystemExit(f"REFUSED: {r['source_id']} matches 0 rows — nothing to purge, "
                             f"and a zero match means the predicate missed, not that the "
                             f"work is done")
        out.append(r)
    if not out:
        raise SystemExit("no source reached decision=GO — nothing authorised to run")
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--receipts", required=True)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--force-unsafe", action="store_true",
                    help="skip the in-flight check (independent proof required)")
    a = ap.parse_args()

    plan = load_receipts(a.receipts)
    print(f"{len(plan)} source(s) authorised GO: {[r['source_id'] for r in plan]}")

    blockers = runs_in_flight()
    if blockers and not a.force_unsafe:
        print("REFUSING — the state store is single-writer (R5):")
        for b in blockers:
            print(f"  - {b}")
        return 2

    print("pull-state ...")
    if run_module("--pull-state") != 0:
        raise SystemExit("pull-state failed — NOT proceeding (would purge a stale copy)")

    st = StateStore()

    # ---- PRE-FLIGHT: verify EVERY source before mutating ANY of them ----------------
    print("\npre-flight (all sources verified before the first DELETE):")
    disagreements = []
    live = {}
    for r in plan:
        sid = r["source_id"]
        pre = st.db.execute(
            "SELECT COUNT(*) FROM series_cursor WHERE source_id=?", (sid,)).fetchone()[0]
        match = st.db.execute(
            f"SELECT COUNT(*) FROM series_cursor WHERE source_id=? AND ({r['predicate']})",
            (sid,)).fetchone()[0]
        live[sid] = (pre, match)
        ok = pre == r["pre_count"] and match == r["match_count"]
        print(f"  {sid:<22} pre {pre:>8,} (want {r['pre_count']:>8,})  "
              f"match {match:>8,} (want {r['match_count']:>8,})  {'OK' if ok else 'MISMATCH'}")
        if not ok:
            disagreements.append(
                f"{sid}: live pre={pre:,} match={match:,} vs receipt "
                f"pre={r['pre_count']:,} match={r['match_count']:,}")

    if disagreements:
        print("\nABORT — the store disagrees with the measured receipts. NOTHING was written.")
        for d in disagreements:
            print(f"  - {d}")
        print("\nThe receipts were measured before this run; a drift means the population "
              "changed underneath the authorisation (R500). Re-measure, re-review, re-run.")
        return 2

    if not a.apply:
        print("\n(dry run — every count agrees; pass --apply to purge and push)")
        return 0

    # ---- MUTATE ---------------------------------------------------------------------
    print("\napplying:")
    receipts_out = []
    for r in plan:
        sid = r["source_id"]
        pre, match = live[sid]
        st.db.execute(
            f"DELETE FROM series_cursor WHERE source_id=? AND ({r['predicate']})", (sid,))
        st.db.commit()
        post = st.db.execute(
            "SELECT COUNT(*) FROM series_cursor WHERE source_id=?", (sid,)).fetchone()[0]
        ok = post == r["post_count"]
        print(f"  {sid:<22} {pre:,} -> {post:,} (want {r['post_count']:,}) "
              f"{'OK' if ok else 'MISMATCH'}")
        receipts_out.append({"source_id": sid, "pre": pre, "deleted": match, "post": post,
                             "expected_post": r["post_count"], "ok": ok})
        if not ok:
            print("\nSTOP — a post-count missed its prediction. The state is LOCAL ONLY and "
                  "has NOT been pushed; the authoritative store is untouched. Investigate "
                  "before pushing; discarding the local copy costs one pull.")
            return 1

    out = os.path.join(ROOT, "data", "_aqueduct", "wu6_purge_receipts_applied.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump({"receipts": receipts_out}, f, indent=1)
    print("\nwrote", out)

    print("push-state ...")
    if run_module("--push-state") != 0:
        raise SystemExit(
            "push-state failed — the purge is LOCAL ONLY and will be resurrected by the "
            "next pull. Re-run when the store is quiet.")
    print("purge committed to the authoritative store.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
