"""Sample a source's served bytes; re-derive ONLY if the sample proves it stale.

THE BACKLOG THIS DRAINS. Until 2026-08-07 the orchestrator re-derived a series' CSV only on a
run whose status was exactly `ok` (ledger R380). Chronically partial sources never return ok,
so their served CSVs froze while their parquet advanced — users downloading a "current"
series got superseded values, not merely a short tail. The gate is fixed, which repairs FUTURE
runs, but a series whose last change predates its last derive and which never changes again
stays frozen at the stale bytes. That residue is what this walks.

WHY SAMPLE FIRST INSTEAD OF JUST RE-DERIVING EVERYTHING. The true stale rate varies enormously
and is not predictable from run status — every source below is chronically partial:

    dst              9 of 15 sampled objects stale   (60%)
    worldbank_esg   14 of 40                         (35%)
    hagstofa         2 of 25                         ( 8%)
    stat_slovenia    1 of 25                         ( 4%)
    ember, ilostat   0 of 15                         ( 0%)
    statfin, scb     0 of 25                         ( 0%)

A blanket re-derive would rewrite millions of objects to fix a few, and R2 PUTs are billed
Class A operations. Sampling costs a handful of GETs per source and answers the question.

AND WHY NOT THE CHEAP METADATA SCREEN. `tools/audit_csv_staleness.py` compares R2 LastModified
and has ~zero precision on the dirty side — it flagged statfin 1,539/1,539 and scb 2,550/2,550
when both are byte-identical — because a merge rewrites a whole parquet for a one-row change.
Only the byte-compare answers it, so that is what this uses.

SAMPLING IS NOT PROOF OF CLEANLINESS, and the output says so: a clean sample of K bounds the
stale rate loosely, it does not establish zero. This tool is a prioritiser for a backlog, not
a certification — `tools/verify_source_served.py` remains the per-source verdict.

WHICH SOURCES TO WALK: `--at-risk` = anything with a `partial` SINCE its last `ok`. My first
filter was "never returned ok" and it MISSED REAL BREAKAGE — ecb has 1 ok against 12 partials,
so it fell outside that set, and it byte-compared 0 of 25 identical (100% of served objects
stale) immediately after a 2-hour pass rewrote 523 of its 540 store files. treasury was missed
the same way at 3 of 14. See _at_risk().

    python tools/repair_stale_csvs.py --at-risk            # sample only, report
    python tools/repair_stale_csvs.py --at-risk --apply    # re-derive the proven-stale
"""
from __future__ import annotations

import argparse
import os
import random
import sqlite3
import subprocess
import sys
import urllib.parse

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))

BUCKET = "econ-data"


def _at_risk() -> set[str]:
    """Sources that have had a `partial` SINCE their last `ok`.

    "Never returned ok" was my first filter and it was WRONG — it is a special case, not the
    rule. A source that returned ok once long ago and has gone partial ever since is in
    exactly the same position: everything merged after that last ok was never derived. ecb
    proved it the hard way (12 partials against 1 ok, sitting OUTSIDE a never-ok sweep) and
    byte-compared 0 of 25 identical — 100% of its served objects stale — right after a
    2-hour pass rewrote 523 of its 540 store files. treasury, also missed, was 3 of 14 stale.

    So the predicate is positional, not a set membership: find the last ok, and ask whether
    any partial follows it.
    """
    db = os.path.join(ROOT, "data", "_aqueduct", "state.db")
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    cols = [r[1] for r in con.execute("PRAGMA table_info(runs)")]
    hist: dict[str, list[str]] = {}
    for row in con.execute("SELECT * FROM runs ORDER BY rowid"):
        d = dict(zip(cols, row))
        hist.setdefault(d["source_id"], []).append(d["status"])
    out = set()
    for src, st in hist.items():
        if "ok" not in st:
            out.add(src)                       # nothing ever derived
            continue
        last_ok = len(st) - 1 - st[::-1].index("ok")
        if any(x == "partial" for x in st[last_ok + 1:]):
            out.add(src)                       # merged since the last derive
    return out


def _sample_ids(src: str, k: int, seed: int) -> list[str]:
    """Random across the FULL key range, never a prefix — a head sample once nearly
    certified a 13%-complete derive (R167)."""
    cat = os.path.join(ROOT, "data", "catalog.db")
    con = sqlite3.connect(f"file:{cat}?mode=ro", uri=True)
    ids = [r[0] for r in con.execute(
        "SELECT series_id FROM series WHERE source_id=?", (src,))]
    if not ids:
        return []
    rnd = random.Random(seed)
    return rnd.sample(ids, min(k, len(ids)))


def _stale_count(s3, ids: list[str]) -> tuple[int, int, list[str]]:
    """(mismatches, compared, examples). A series missing from R2 is NOT counted stale —
    that is the MISSING class verify_source_served reports, a different defect."""
    from core.derive_csv import _series_csv_bytes
    bad, n, examples = 0, 0, []
    for sid in ids:
        key = "series/" + urllib.parse.quote(sid, safe="") + ".csv"
        try:
            served = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        except Exception:                                             # noqa: BLE001
            continue
        try:
            fresh = _series_csv_bytes(sid)
        except Exception:                                             # noqa: BLE001
            continue
        n += 1
        if served != fresh:
            bad += 1
            if len(examples) < 3:
                examples.append(sid)
    return bad, n, examples


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", action="append")
    ap.add_argument("--at-risk", action="store_true",
                    help="sources with a partial since their last ok — see _at_risk()")
    ap.add_argument("--never-ok", action="store_true", help="deprecated alias for --at-risk")
    ap.add_argument("--sample", type=int, default=12)
    ap.add_argument("--seed", type=int, default=20260807)
    ap.add_argument("--apply", action="store_true", help="re-derive the proven-stale sources")
    ap.add_argument("--max-series", type=int, default=250_000,
                    help="skip sources larger than this; a full re-derive of a giant is its "
                         "own decision, not a side effect of a sweep")
    a = ap.parse_args()
    print(f"MODE: {'APPLY (will re-derive proven-stale sources)' if a.apply else 'REPORT ONLY'}")

    from core import r2_util
    cat = sqlite3.connect(f"file:{os.path.join(ROOT,'data','catalog.db')}?mode=ro", uri=True)
    sizes = {r[0]: r[1] for r in cat.execute(
        "SELECT source_id, count(*) FROM series GROUP BY source_id")}

    targets = a.source or sorted(sizes)
    if a.at_risk or a.never_ok:
        n = _at_risk()
        targets = [t for t in targets if t in n]
    skipped = [t for t in targets if sizes.get(t, 0) > a.max_series]
    targets = [t for t in targets if sizes.get(t, 0) <= a.max_series]
    print(f"{len(targets)} source(s) to sample at k={a.sample}; "
          f"{len(skipped)} skipped as too large: {sorted(skipped)}\n")

    s3 = r2_util.client()
    stale, clean, empty = [], [], []
    for src in targets:
        ids = _sample_ids(src, a.sample, a.seed)
        if not ids:
            empty.append(src)
            continue
        bad, n, ex = _stale_count(s3, ids)
        if n == 0:
            empty.append(src)
            continue
        if bad:
            stale.append((src, bad, n, sizes.get(src, 0)))
            print(f"  STALE  {src:24s} {bad:>3}/{n:<3} sampled differ  "
                  f"({sizes.get(src,0):,} series)  e.g. {ex[0] if ex else ''}")
        else:
            clean.append((src, n))
            print(f"  clean  {src:24s} {n} sampled identical")

    print(f"\nPROVEN STALE: {len(stale)}   sample-clean: {len(clean)}   "
          f"unsampleable: {len(empty)}")
    print("NOTE: a clean sample of k bounds the stale rate loosely — it is NOT proof of zero. "
          "Use tools/verify_source_served.py for a per-source verdict.")
    if not stale:
        return 0
    if not a.apply:
        print("\nreport only — re-run with --apply to re-derive:")
        for src, bad, n, sz in stale:
            print(f"   {src}  ({sz:,} series)")
        return 1

    for src, bad, n, sz in stale:
        print(f"\n=== re-deriving {src} ({sz:,} series) ===", flush=True)
        env = dict(os.environ, AQUEDUCT_BACKEND="r2", PYTHONIOENCODING="utf-8")
        r = subprocess.run([sys.executable, "-m", "core.derive_csv", "--source", src,
                            "--bucket", BUCKET, "--workers", "12"],
                           cwd=ROOT, env=env, capture_output=True, text=True)
        tail = [ln for ln in (r.stdout or "").splitlines() if ln.strip()][-2:]
        print("\n".join(tail) or (r.stderr or "")[-300:], flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
