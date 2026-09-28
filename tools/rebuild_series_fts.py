"""Rebuild D1's series_fts from its own `series` table — the Phase 2 (W4) repair.

Design + adversarial review + probes: docs/briefs/PHASE2_FTS_DESIGN.md. Executes ONLY the
reviewed v2 plan:

  * chunked server-side `INSERT INTO series_fts_new SELECT … FROM series WHERE series_id >= ?
    AND < ?` — reads ride the PK (no scans), sources over CHUNK_ROWS split by boundaries taken
    from the local catalogue (boundaries need only PARTITION the range, not match D1 exactly);
  * per-chunk journal with cumulative expected counts; on any error the driver STOPS —
    a retry into fts5 is NOT idempotent, so a human reconciles `SELECT COUNT(*)` against the
    journal before resuming (--resume-from N skips the first N chunks);
  * verification against the same-day D1 `series` count BEFORE the swap;
  * the swap is ONE atomic --file batch (DROP + RENAME) — probed 2026-08-31: a failing
    statement rolls back the whole file, RENAME works on fts5, MATCH works after;
  * the noaa shard is SKIPPED (measured 1.0000 ratio, 42 surplus rows of 3.14M).

PRECONDITIONS the operator asserts before running (the driver checks what it can):
  * updater-daily, updater-heavy and sec-edgar-daily are DISABLED (they write series_fts by
    name mid-window) and no run is in flight;
  * the reading gate's receipts exist (this is consequential D1 work);
  * after the swap: run `python -m core.sync_catalog_d1` (or the daily run) once, then the
    live acceptance table in the design doc decides pass/fail.

Cost, probed: ~1.0 rows_written per row (13.5M total, ≈$13.50 ceiling), reads ~13.5M PK-range
(≈$0.014), 100k-row chunk = 244 ms SQL. Wall time is wrangler round-trips, ~4 s × ~170 calls.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
# The plan phase reads ~13.5M rows; on the contended USB store drive (two first-pass
# ingesters writing) that wedged past 15 minutes. AQUEDUCT_PLAN_CATALOG points it at a fast
# local COPY -- safe because chunk boundaries only need to PARTITION each PK range, which any
# snapshot of the catalogue does regardless of drift.
CATALOG = os.environ.get("AQUEDUCT_PLAN_CATALOG") or os.path.join(ROOT, "data", "catalog.db")
JOURNAL = os.path.join(ROOT, "data", "_fts_rebuild_journal.jsonl")

DB = "econ-catalog"
NEW = "series_fts_new"
CHUNK_ROWS = 100_000          # probed: 244 ms per 100k; far inside every limit


def d1(sql: str) -> dict:
    """One remote statement; returns the parsed result block. Raises SystemExit on failure.

    Through core.d1_remote (plan step 1): the pinned wrangler before T0, refused after it for anything but
    one plain read. It retries ONLY wrangler's auth error 10000. The old loop here retried EVERY failure
    three times, including a chunk INSERT the server may already have taken - fts5 has no key, so that
    duplicated rows (R1185/R1191; the count check before the swap would have refused, after the cost)."""
    from core import d1_remote                                          # noqa: PLC0415
    try:
        block = d1_remote.run_json(DB, sql, timeout=600)[0]
    except (RuntimeError, ValueError) as e:                            # D1Unreachable is a RuntimeError
        raise SystemExit("D1 statement failed: %s\nSQL: %s" % (e, sql[:200])) from None
    if not block.get("success", True):
        raise SystemExit("D1 statement failed: %r\nSQL: %s" % (block, sql[:200]))
    return block


def chunk_plan():
    """[(label, lo, hi)] partitioning every source's PK range, big sources split locally."""
    con = sqlite3.connect("file:%s?mode=ro" % CATALOG.replace("\\", "/"), uri=True)
    rows = con.execute(
        "SELECT source_id, COUNT(*) FROM series GROUP BY source_id ORDER BY source_id"
    ).fetchall()
    plan = []
    for src, n in rows:
        lo, hi = src + ":", src + ";"
        if n <= CHUNK_ROWS:
            plan.append((src, lo, hi))
            continue
        n_chunks = (n + CHUNK_ROWS - 1) // CHUNK_ROWS
        step = (n // n_chunks) or 1
        # Boundary ids via PK-RANGE seeks, never a source_id filter: the local catalogue has
        # no index on source_id BACK THEN, so the first version's window query full-scanned
        # 13.5M rows (ix_series_source_id exists now, verified 2026-09-22)
        # PER BIG SOURCE (~10 of them) over a USB drive — the ~4-min plan phase wedged past
        # 15, and a control run alongside made two full scans thrash one disk. R492's lesson
        # in local form: the cost is the predicate's access path, not the row count.
        # Boundaries only need to PARTITION [lo, hi): every D1 row falls in exactly one
        # [b_i, b_{i+1}) regardless of local/D1 drift.
        bounds = []
        for i in range(1, n_chunks):
            r = con.execute(
                "SELECT series_id FROM series WHERE series_id >= ? AND series_id < ? "
                "ORDER BY series_id LIMIT 1 OFFSET ?", (lo, hi, step * i)).fetchone()
            if r:
                bounds.append(r[0])
        edges = [lo] + sorted(set(bounds)) + [hi]
        for i in range(len(edges) - 1):
            plan.append(("%s[%d/%d]" % (src, i + 1, len(edges) - 1), edges[i], edges[i + 1]))
    con.close()
    return plan


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="without it: print the plan only")
    ap.add_argument("--resume-from", type=int, default=0,
                    help="skip the first N chunks (after reconciling the journal by hand)")
    a = ap.parse_args()

    plan = chunk_plan()
    total_local = sum(1 for _ in plan)
    print("chunk plan: %d statements (CHUNK_ROWS=%s)" % (total_local, format(CHUNK_ROWS, ",")))
    if not a.apply:
        for lbl, lo, hi in plan[:8]:
            print("   %-28s [%s .. %s)" % (lbl, lo[:40], hi[:40]))
        print("   ... (--apply to execute)")
        return 0

    # Same-day authority: the count the finished table must equal.
    series_n = d1("SELECT COUNT(*) AS n FROM series")["results"][0]["n"]
    print("same-day D1 series count: %s" % format(series_n, ","))

    d1("CREATE VIRTUAL TABLE IF NOT EXISTS %s USING fts5"
       "(series_id UNINDEXED, title, geography)" % NEW)
    if a.resume_from == 0:
        n0 = d1("SELECT COUNT(*) AS n FROM %s" % NEW)["results"][0]["n"]
        if n0:
            raise SystemExit(
                "REFUSING: %s already holds %s rows and this is not --resume-from. A retry "
                "into fts5 duplicates; reconcile the journal, then resume or DROP the table."
                % (NEW, format(n0, ",")))

    cum = 0
    t0 = time.time()
    with open(JOURNAL, "a", encoding="utf-8") as jf:
        for i, (lbl, lo, hi) in enumerate(plan):
            if i < a.resume_from:
                continue
            block = d1(
                "INSERT INTO %s(series_id, title, geography) "
                "SELECT series_id, title, geography FROM series "
                "WHERE series_id >= '%s' AND series_id < '%s'"
                % (NEW, lo.replace("'", "''"), hi.replace("'", "''")))
            w = block["meta"]["rows_written"]
            cum += w
            jf.write(json.dumps({"i": i, "label": lbl, "written": w, "cum": cum,
                                 "t": time.time()}) + "\n")
            jf.flush()
            if i % 20 == 0 or w > CHUNK_ROWS:
                print("  [%3d/%3d] %-28s +%s (cum %s) %.0fs"
                      % (i + 1, len(plan), lbl, format(w, ","), format(cum, ","),
                         time.time() - t0), flush=True)

    new_n = d1("SELECT COUNT(*) AS n FROM %s" % NEW)["results"][0]["n"]
    print("built: %s rows; same-day series: %s -> %s"
          % (format(new_n, ","), format(series_n, ","),
             "MATCH" if new_n == series_n else "MISMATCH"))
    if new_n != series_n:
        raise SystemExit("REFUSING TO SWAP: built table does not equal the series count. "
                         "Reconcile the journal; the old index is still serving.")

    # Sanity MATCH on the new table before the swap (must find real titles).
    probe = d1("SELECT COUNT(*) AS n FROM %s WHERE %s MATCH 'disposable'"
               % (NEW, NEW))["results"][0]["n"]
    print("pre-swap MATCH 'disposable' on the new table: %s" % format(probe, ","))
    if probe == 0:
        raise SystemExit("REFUSING TO SWAP: the new index matches nothing — built empty of "
                         "titles? Old index still serving.")

    # THE SWAP — one atomic file (probed: a failing statement rolls the file back).
    swap = os.path.join(ROOT, "data", "_fts_swap.sql")
    # The swap adapts to whether the OLD table still exists. In the 2026-08-31 run the DB hit
    # D1's 10 GB ceiling mid-build (the old 23.7M-row index + 7.06M new rows) and the old
    # table was dropped EARLY to reclaim space (9.99 GB -> 5.95 GB; the LIKE fallback carried
    # search, measured live at the clean predictions). A DROP of a now-absent table inside
    # the atomic file would roll the RENAME back with it.
    old_exists = d1("SELECT COUNT(*) AS n FROM sqlite_master WHERE name = 'series_fts'"
                    )["results"][0]["n"] > 0
    with open(swap, "w", encoding="utf-8") as fh:
        if old_exists:
            fh.write("DROP TABLE series_fts;\nALTER TABLE %s RENAME TO series_fts;\n" % NEW)
        else:
            fh.write("ALTER TABLE %s RENAME TO series_fts;\n" % NEW)
    from core import d1_remote                                          # noqa: PLC0415
    try:
        d1_remote.execute_file(DB, swap, timeout=600)                   # once: never retried (tries=1)
        ok = True
    except RuntimeError as e:
        ok, why = False, e
    print("SWAP:", "ok" if ok else "FAILED — old index likely still present; investigate "
          "before ANY retry")
    if not ok:
        print(str(why)[-400:], (getattr(why, "stderr", "") or "")[-400:])
        return 1

    final = d1("SELECT COUNT(*) AS n FROM series_fts")["results"][0]["n"]
    print("post-swap series_fts count: %s (want %s)" % (format(final, ","),
                                                        format(series_n, ",")))
    print("NEXT: python -m core.sync_catalog_d1 once, re-enable the workflows, then the "
          "acceptance table in docs/briefs/PHASE2_FTS_DESIGN.md against the LIVE endpoint.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
