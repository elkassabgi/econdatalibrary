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
import re
import sqlite3
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORKER = os.path.join(ROOT, "api", "worker")
CATALOG = os.path.join(ROOT, "data", "catalog.db")
JOURNAL = os.path.join(ROOT, "data", "_fts_rebuild_journal.jsonl")

DB = "econ-catalog"
NEW = "series_fts_new"
CHUNK_ROWS = 100_000          # probed: 244 ms per 100k; far inside every limit


def d1(sql: str, retries: int = 3) -> dict:
    """One remote statement; returns the parsed result block. Raises on final failure."""
    last = None
    for attempt in range(1, retries + 1):
        p = subprocess.run(
            ["npx", "wrangler", "d1", "execute", DB, "--remote", "--json",
             "--command", sql],
            cwd=WORKER, capture_output=True, text=True, encoding="utf-8",
            errors="replace", timeout=600, shell=(os.name == "nt"))
        m = re.search(r"\[\s*\{.*\}\s*\]", p.stdout or "", re.S)
        if m:
            block = json.loads(m.group(0))[0]
            if block.get("success"):
                return block
            last = block
        else:
            last = (p.stdout or "")[-300:] + (p.stderr or "")[-300:]
        # 7403/7500 transients: R363/R222 — re-probe before believing
        time.sleep(10 * attempt)
    raise SystemExit("D1 statement failed after %d attempts: %r\nSQL: %s"
                     % (retries, last, sql[:200]))


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
        # Boundary ids from the local catalogue. They only need to PARTITION [lo, hi):
        # every D1 row falls in exactly one [b_i, b_{i+1}) regardless of local/D1 drift.
        bounds = [r[0] for r in con.execute(
            "SELECT series_id FROM (SELECT series_id, ROW_NUMBER() OVER (ORDER BY series_id)"
            " AS rn FROM series WHERE source_id = ?) WHERE rn % ? = 0",
            (src, (n // n_chunks) or 1)).fetchall()][: n_chunks - 1]
        edges = [lo] + bounds + [hi]
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
    with open(swap, "w", encoding="utf-8") as fh:
        fh.write("DROP TABLE series_fts;\nALTER TABLE %s RENAME TO series_fts;\n" % NEW)
    p = subprocess.run(["npx", "wrangler", "d1", "execute", DB, "--remote", "--json",
                        "--file", swap.replace("\\", "/")],
                       cwd=WORKER, capture_output=True, text=True, encoding="utf-8",
                       errors="replace", timeout=600, shell=(os.name == "nt"))
    ok = '"success": true' in (p.stdout or "") or '"success":true' in (p.stdout or "")
    print("SWAP:", "ok" if ok else "FAILED — old index likely still present; investigate "
          "before ANY retry")
    if not ok:
        print((p.stdout or "")[-400:])
        return 1

    final = d1("SELECT COUNT(*) AS n FROM series_fts")["results"][0]["n"]
    print("post-swap series_fts count: %s (want %s)" % (format(final, ","),
                                                        format(series_n, ",")))
    print("NEXT: python -m core.sync_catalog_d1 once, re-enable the workflows, then the "
          "acceptance table in docs/briefs/PHASE2_FTS_DESIGN.md against the LIVE endpoint.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
