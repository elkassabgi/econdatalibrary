#!/usr/bin/env python3
"""One-time offline dedup of legacy-inflated BLS survey parquets.  PROPOSAL.

STATUS: written 2026-07-19 as a reviewable fix proposal — DO NOT RUN --apply
without Ahmed's explicit approval. Default mode is a read-only dry run.

WHY THIS EXISTS (diagnosed 2026-07-19)
--------------------------------------
The retired jobs/ingest_bls_full.py wrote 39 of the 63 BLS survey parquets in
data/clean_full/bls/ with massive EXACT-REPEAT row inflation (overlapping cuts
ingested repeatedly; headline CPI rows appear 4x). Scan:
logs/_bls_dup_scan_0719.json — worst: cb 53.1M repeat rows (50%), la 30.0M
(66%), sm 21.9M (69%), cu/CPI 1.6M (49%). Consequence: the incremental
updater's clean Current-cut merge produces ~0.3-0.8x of the inflated on-disk
row count, so merge.merge_and_write's never-shrink guard (min_ratio=0.97)
CORRECTLY refuses — those surveys are FROZEN (cu at 2026-04-01) and surface as
`partial` every daily tick. bls.py:49-58 prescribes exactly this offline dedup.

THE ROW IDENTITY IS 3 COLUMNS, NOT 2 (verified 2026-07-19)
----------------------------------------------------------
(series_id, obs_date) is NOT unique in legitimate BLS data: quarterly Q03 and
semiannual S02/S03 periods can map to the SAME derived obs_date with DIFFERENT
true values (e.g. cu CUUS0300SACL1E 1984-07-01 S02=105.8 vs S03=104.8). The
true identity is (series_id, obs_date, period); logs/_bls_key3_check_0719.json
proves this 3-col key is value-conflict-free in 39/40 dup-affected surveys
(cu keeps 97,452 legitimate collision rows a 2-col dedup would DESTROY; ln's
192 apparent dups are ALL legitimate collisions — no dedup needed).
Residual: ee has 2,152 keys (0.11%) conflicting even on 3 cols (mixed-vintage
legacy writes) — resolved keep-LAST (mirrors merge.py new-wins), with an audit
CSV of both values exported to the backup dir first.

COMPANION CODE CHANGE (required, separate approval; NOT applied by this script):
updater/strategies/fetchers/bls.py must merge on the same 3-col identity, or
the next incremental merge would collapse the legitimate collision pairs and
re-trip never-shrink (cu would shrink ~5.5% > 3% tolerance -> frozen again):
    -DEDUP = ("series_id", "obs_date")    # line ~93
    +DEDUP = ("series_id", "obs_date", "period")
and _preexisting_dups() (line ~222) should group by the same 3 columns so its
legacy-dup counter does not misreport legitimate collision rows as dups.

WHAT THIS SCRIPT DOES
---------------------
  dry run (default):  re-measure every survey (rows / 3-col unique / true
                      repeats / 3-col value conflicts), print the exact plan,
                      write nothing. Safe anywhere, anytime.
  --apply:            for each survey with TRUE repeats (rows > 3-col unique),
                      local files only:
                        1. if 3-col value conflicts exist, export them to
                           <backup_dir>/<sv>_value_conflicts.csv (audit trail)
                        2. dedup with duckdb streaming: keep the LAST physical
                           occurrence per (series_id, obs_date, period)
                        3. VERIFY: schema byte-identical (pyarrow equality),
                           out rows == 3-col unique count, zero remaining
                           3-col repeats, min/max obs_date preserved
                        4. BACKUP original to
                           data/_backup_bls_dedup_<UTCDATE>/<sv>.parquet
                        5. atomic os.replace()
                      Any verification failure aborts THAT survey (original
                      untouched) and continues with the rest.
  --only cu,sm        restrict to named surveys.

WHAT IT DELIBERATELY DOES NOT DO (follow-up steps, separate approvals):
  * apply the bls.py DEDUP diff above (code change, reviewed separately);
  * R2: the R2 copies of these parquets (updater r2 backend) carry the same
    repeats and must be replaced with the deduped files (backup first);
  * regenerate derived per-series CSVs + catalog metadata for BLS;
  * run the updater / touch the state plane (no --push-state);
  * NEVER runs jobs/ingest_bls_full.py (it caused the inflation; QCEW
    double-count — do not resurrect it).

KNOWN QUIRK LEFT IN PLACE (documented, not "fixed"): the legacy ingest mapped
S03 to Jul-1 while the current parser maps S03 to Dec-31, so post-fix S03 rows
will accumulate at Dec-31 while historical ones sit at Jul-1. Remapping
historical obs_date from period is a separate data-op decision.

SAFETY PROPERTIES
  * The updater must NOT be running against this store during --apply
    (single-writer; also respect the CI window rule — do not run 05:40-06:45Z).
  * Per-survey atomic: tmp file + os.replace, original backed up first; a
    crash mid-run leaves every survey either fully old or fully new.
  * Idempotent: re-running on an already-deduped survey is a no-op.
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import shutil
import sys

import duckdb
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BLS_DIR = os.path.join(ROOT, "data", "clean_full", "bls")
SPILL = "D:/temp/claude/duckdb_spill"
KEY = "series_id, obs_date, period"          # the TRUE row identity (3 cols)


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='3GB'")   # polite: crawlers share this box
    con.execute("PRAGMA threads=2")
    con.execute("SET preserve_insertion_order=false")
    os.makedirs(SPILL, exist_ok=True)
    con.execute(f"SET temp_directory='{SPILL}'")
    return con


def q(path: str) -> str:
    return path.replace("'", "''")


def measure(con, path: str) -> dict:
    """Read-only per-file stats on the 3-col identity."""
    rows, k3, k3v = con.execute(f"""
        SELECT
          (SELECT COUNT(*) FROM read_parquet('{q(path)}')),
          (SELECT COUNT(*) FROM (SELECT DISTINCT {KEY} FROM read_parquet('{q(path)}'))),
          (SELECT COUNT(*) FROM (SELECT DISTINCT {KEY}, value FROM read_parquet('{q(path)}')))
    """).fetchone()
    lo, hi = con.execute(
        f"SELECT MIN(obs_date), MAX(obs_date) FROM read_parquet('{q(path)}')").fetchone()
    return {"rows": rows, "k3": k3, "repeats": rows - k3,
            "value_conflicts": k3v - k3, "min_obs": str(lo), "max_obs": str(hi)}


def export_conflicts(con, src: str, dest_csv: str) -> int:
    """Audit trail: write every 3-col key that carries >1 distinct value."""
    con.execute(f"""
        COPY (
          SELECT t.series_id, t.obs_date, t.period, t.value,
                 t.file_row_number AS physical_row
          FROM read_parquet('{q(src)}', file_row_number=true) t
          JOIN (SELECT {KEY} FROM read_parquet('{q(src)}')
                GROUP BY {KEY} HAVING COUNT(DISTINCT value) > 1) c
          USING (series_id, obs_date, period)
          ORDER BY t.series_id, t.obs_date, t.period, t.file_row_number
        ) TO '{q(dest_csv)}' (FORMAT csv, HEADER)
    """)
    return con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{q(dest_csv)}')").fetchone()[0]


def dedup_one(con, sv: str, backup_dir: str) -> tuple[bool, str]:
    """Dedup one survey parquet in place (audit + backup + verify + atomic swap).
    Returns (changed, message). Never leaves a partially-written prod file."""
    src = os.path.join(BLS_DIR, f"{sv}.parquet")
    before = measure(con, src)
    if before["repeats"] == 0:
        return False, f"{sv}: clean on 3-col key ({before['rows']:,} rows) — skipped"

    os.makedirs(backup_dir, exist_ok=True)
    if before["value_conflicts"]:
        csv = os.path.join(backup_dir, f"{sv}_value_conflicts.csv")
        n = export_conflicts(con, src, csv)
        print(f"      audit: {before['value_conflicts']:,} conflicted keys "
              f"({n:,} rows) -> {csv}", flush=True)

    in_schema = pq.ParquetFile(src).schema_arrow
    tmp = src + ".dedup.tmp"
    if os.path.exists(tmp):
        os.remove(tmp)

    # Keep the LAST physical occurrence per (series_id, obs_date, period):
    # mirrors merge.py's keep-last/new-wins rule. file_row_number = physical order.
    cols = ", ".join(f'"{f.name}"' for f in in_schema)
    con.execute(f"""
        COPY (
          SELECT {cols} FROM (
            SELECT *, row_number() OVER (
                PARTITION BY {KEY}
                ORDER BY file_row_number DESC) AS _rn
            FROM read_parquet('{q(src)}', file_row_number=true)
          ) WHERE _rn = 1
        ) TO '{q(tmp)}' (FORMAT parquet)
    """)

    # ---- verification gauntlet (all must pass or we abort this survey) ----
    problems = []
    out_schema = pq.ParquetFile(tmp).schema_arrow
    if not out_schema.equals(in_schema):
        problems.append(f"schema drift: {in_schema} -> {out_schema}")
    after = measure(con, tmp)
    if after["rows"] != before["k3"]:
        problems.append(f"row count {after['rows']:,} != expected 3-col unique {before['k3']:,}")
    if after["repeats"] != 0:
        problems.append(f"output still has {after['repeats']:,} repeats")
    if (after["min_obs"], after["max_obs"]) != (before["min_obs"], before["max_obs"]):
        problems.append(f"obs_date range changed: {before['min_obs']}..{before['max_obs']}"
                        f" -> {after['min_obs']}..{after['max_obs']}")
    if problems:
        os.remove(tmp)
        return False, f"{sv}: ABORTED (original untouched): " + "; ".join(problems)

    # ---- backup, then atomic swap ----
    bak = os.path.join(backup_dir, f"{sv}.parquet")
    if not os.path.exists(bak):          # never overwrite an earlier backup
        shutil.copy2(src, bak)
    os.replace(tmp, src)
    note = (f" [{before['value_conflicts']:,} value-conflict keys resolved keep-last]"
            if before["value_conflicts"] else "")
    return True, (f"{sv}: {before['rows']:,} -> {after['rows']:,} rows "
                  f"(-{before['repeats']:,} repeats){note}; backup: {bak}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--apply", action="store_true",
                    help="actually dedup (default: read-only dry run)")
    ap.add_argument("--only", default="",
                    help="comma-separated survey codes to restrict to")
    args = ap.parse_args()

    surveys = sorted(fn[:-8] for fn in os.listdir(BLS_DIR)
                     if fn.endswith(".parquet") and not fn.startswith("_"))
    if args.only:
        want = {s.strip() for s in args.only.split(",") if s.strip()}
        missing = want - set(surveys)
        if missing:
            print(f"unknown surveys: {sorted(missing)}", file=sys.stderr)
            return 2
        surveys = [s for s in surveys if s in want]

    con = connect()
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d")
    backup_dir = os.path.join(ROOT, "data", f"_backup_bls_dedup_{stamp}")

    if not args.apply:
        print(f"DRY RUN — measuring {len(surveys)} surveys on the 3-col identity "
              f"({KEY}); no writes\n")
        total_repeats = 0
        plan = []
        for sv in surveys:
            m = measure(con, os.path.join(BLS_DIR, f"{sv}.parquet"))
            total_repeats += m["repeats"]
            flag = "  <- WILL DEDUP" if m["repeats"] else ""
            if m["repeats"]:
                plan.append(sv)
            print(f"  {sv:4s} rows={m['rows']:>12,} repeats={m['repeats']:>12,} "
                  f"conflicts={m['value_conflicts']:>7,} "
                  f"range {m['min_obs']}..{m['max_obs']}{flag}", flush=True)
        print(f"\nPLAN: dedup {len(plan)} surveys, removing {total_repeats:,} "
              f"true repeat rows (legitimate period-collision rows preserved).")
        print(f"Backups would go to {backup_dir}")
        print("Run with --apply (after approval) to execute. Remember the "
              "companion bls.py DEDUP 3-col change (separate approval).")
        return 0

    print(f"APPLY — deduping {len(surveys)} surveys on ({KEY}); "
          f"backups -> {backup_dir}\n")
    changed = aborted = 0
    for sv in surveys:
        ok, msg = dedup_one(con, sv, backup_dir)
        print("  " + msg, flush=True)
        if ok:
            changed += 1
        elif "ABORTED" in msg:
            aborted += 1
    print(f"\nDONE: {changed} deduped, {aborted} aborted, "
          f"{len(surveys) - changed - aborted} already clean.")
    print("NEXT (separate approvals): the bls.py 3-col DEDUP diff + R2 object "
          "replacement + per-series CSV regeneration + a --source bls updater "
          "tick to confirm surveys advance.")
    return 0 if aborted == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
