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

# Upper bound on how many rows one run can insert for a source — the same cap the
# fetchers report under. A pre_count jump larger than this is not "a source ran".
_CURSOR_CAP = 50_000


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
    #
    # WHAT IS PINNED, AND WHY ONLY THAT (the verifiers' required change). match_count is
    # the ONLY invariant number here: every predicate selects a legacy key shape that
    # current code cannot emit any more, and put_series_cursors is INSERT..ON CONFLICT
    # DO UPDATE (state.py:141) — upsert-only, never deleting — so a matched row can be
    # re-dated but never created or destroyed by a normal run. It is also the number
    # that carries the safety property: it IS the delete set.
    #
    # pre_count and post_count are NOT invariant: any run of the source inserts new
    # (prefixed) rows between the measurement and the execution — fed_board alone could
    # add up to +44,967 on republish. Pinning them would abort a correct migration for a
    # legitimate change, and this is an ALL-OR-NOTHING session, so one such abort blocks
    # the other eight. They are therefore REPORTED with their drift, and the arithmetic
    # identity live_post == live_pre - live_match is enforced after the delete instead.
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
        ok = match == r["match_count"]
        drift = pre - r["pre_count"]
        print(f"  {sid:<22} match {match:>8,} (want {r['match_count']:>8,}) "
              f"{'OK' if ok else 'MISMATCH'}   pre {pre:>8,} "
              f"(measured {r['pre_count']:>8,}, drift {drift:+,})")
        if not ok:
            disagreements.append(
                f"{sid}: live match={match:,} vs authorised match={r['match_count']:,}")
        # pre_count is a MONOTONE-UP band, not free drift. Cursors are upsert-only, so a
        # source that ran can only ADD rows; a DECREASE means some other writer deleted
        # from this table — that is a different store, not drift. And no single run can
        # insert more than CURSOR_CAP, so a bigger jump is also unexplained.
        if drift < 0:
            disagreements.append(
                f"{sid}: pre_count FELL {abs(drift):,} ({r['pre_count']:,} -> {pre:,}). "
                f"Cursors are upsert-only (state.py:141) and the only DELETEs live in the "
                f"purge tools — someone else deleted rows, or this is a different store.")
        elif drift > _CURSOR_CAP:
            disagreements.append(
                f"{sid}: pre_count JUMPED {drift:,}, more than one run can insert "
                f"(CURSOR_CAP={_CURSOR_CAP:,}). Unexplained growth — investigate.")
        elif drift:
            print(f"      note: {drift:,} row(s) appeared since the measurement — expected "
                  f"for a source that ran; the delete set itself is unchanged.")

    # ---- THE GATE THAT ACTUALLY AUTHORISES THE DELETES -----------------------------
    # Counting proves the set is the one we measured. THIS proves the set is safe to lose:
    # every key about to be deleted must map to NOTHING in the catalogue, so no served
    # series can lose its only cursor. It is invariant to drift, which is why loosening
    # pre_count costs nothing.
    #
    # BACKEND MUST BE FORCED TO r2. Under the default local backend the derive-all
    # fallback (_DERIVE_ALL_CAP, orchestrate.py) returns EVERY id of a small-catalogue
    # source with unmapped=[], so a set of provably dead keys reads as 100% MAPPED. That
    # artefact was hit twice while measuring this very migration (worldbank_wdi's 10,255
    # "mapping" all 1,486 ids; whr's 178 "mapping" all 1,749) and is already twice in the
    # ledger. A gate that fails open is not a gate (R503).
    if not disagreements:
        from updater import config as _cfg
        from updater import orchestrate as _orc
        _saved = _cfg.BACKEND
        _cfg.BACKEND = "r2"
        try:
            print("\nmapper gate (r2 semantics — every doomed key must map to NOTHING):")
            for r in plan:
                sid = r["source_id"]
                keys = [row[0] for row in st.db.execute(
                    f"SELECT series_key FROM series_cursor WHERE source_id=? "
                    f"AND ({r['predicate']})", (sid,))]
                ids, unmapped = _orc._catalog_ids_for(sid, keys)
                print(f"  {sid:<22} {len(keys):>8,} doomed keys -> {len(ids):,} catalogue id(s)"
                      f"   {'OK' if not ids else 'REFUSES'}")
                if ids:
                    disagreements.append(
                        f"{sid}: {len(ids):,} of the {len(keys):,} rows to delete MAP to "
                        f"catalogue ids (e.g. {ids[:3]}) — those series would lose their "
                        f"cursor. This is not dead state.")
        finally:
            _cfg.BACKEND = _saved

    if disagreements:
        print("\nABORT — the delete set is not the one that was authorised. NOTHING written.")
        for d in disagreements:
            print(f"  - {d}")
        print("\nmatch_count is the authorised population (R500: an authorisation is only as "
              "good as the facts it was given). Re-measure, re-review, re-run.")
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
        # The IDENTITY, not the stale literal: post must be exactly what this store had
        # minus what we just deleted. Comparing against the receipt's post_count would
        # fail on legitimate drift (see the pre-flight note); comparing against the
        # arithmetic catches the thing that actually matters — a DELETE that removed a
        # different number of rows than the one we counted and authorised.
        ok = post == pre - match
        print(f"  {sid:<22} {pre:,} -> {post:,} (identity {pre:,}-{match:,}="
              f"{pre - match:,}) {'OK' if ok else 'MISMATCH'}"
              f"   [receipt predicted {r['post_count']:,}]")
        receipts_out.append({"source_id": sid, "pre": pre, "deleted": match, "post": post,
                             "identity_expected": pre - match,
                             "receipt_predicted_post": r["post_count"], "ok": ok})
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
