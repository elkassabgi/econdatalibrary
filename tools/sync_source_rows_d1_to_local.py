"""Copy catalogue rows that exist on D1 but not in the local catalog.db, for ONE source - by primary key.

WHY (2026-09-05). sec_edgar's CI refresher writes D1 only (a runner has no local catalogue), so
new registrants catalogued by CI since 2026-08-22 - and the retained legacy CIK-keyed ids of the
two-products history (task #81) - exist on D1 and are served, while the curated local copy lacks
them: 17,467 rows on D1 vs 17,306 locally after today's catch-up. verify_source_served compares
R2 with the LOCAL catalogue and reports every such row as "orphaned", which is the local mirror
lagging, not a served defect - but a lagging mirror is exactly how the R2 coherence copy ends up
missing rows the next time it is refreshed (R245/R250). This tool closes that gap the cheap way:
one D1 primary-key-RANGE read of the source (an index range, free class - never `WHERE
source_id=`, never a series_fts predicate), one local PK-range read, INSERT OR IGNORE of the
difference in chunks of a few hundred rows per IMMEDIATE transaction (R734: catalog.db is
journal_mode=delete and shared with the crawlers; one big COMMIT held them off for 25 minutes),
plus the FTS row for each inserted id (INSERT only - series_fts is fts5(series_id UNINDEXED, ...),
so any DELETE by id is a full scan).

Direction is D1 -> local ONLY, rows the local copy does not have. Nothing is updated, nothing is
deleted, D1 is never written. Dry run by default.

    python tools/sync_source_rows_d1_to_local.py --source sec_edgar [--apply] [--chunk 400]
"""
from __future__ import annotations
import argparse
import datetime as dt
import json
import os
import sqlite3
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, "data", "catalog.db")
WRANGLER = os.path.join(ROOT, "api", "worker", "node_modules", ".bin", "wrangler.cmd" if os.name == "nt" else "wrangler")


def d1_rows(source: str):
    """Every D1 `series` row of the source, by primary-key range. One statement."""
    sql = (f"SELECT * FROM series WHERE series_id >= '{source}:' AND series_id < '{source};'")
    last = None
    for attempt in range(3):
        r = subprocess.run([WRANGLER, "d1", "execute", "econ-catalog", "--remote", "--json", "--command", sql],
                           cwd=os.path.join(ROOT, "api", "worker"), capture_output=True, text=True,
                           encoding="utf-8", errors="replace", timeout=900)
        if r.returncode == 0:
            lines = r.stdout.splitlines()
            start = next((i for i, ln in enumerate(lines) if ln.strip() == "["), None)
            if start is not None:
                res = json.loads("\n".join(lines[start:]))
                rows = [row for e in res for row in (e.get("results") or [])]
                rows_read = sum(int((e.get("meta") or {}).get("rows_read") or 0) for e in res)
                return rows, rows_read
        last = (r.stderr or "")[-600:] + " | " + (r.stdout or "")[-300:]
        if "code: 10000" in last and attempt < 2:
            print(f"   wrangler auth error 10000 (attempt {attempt + 1}/3) - retrying in 10 s", flush=True)
            time.sleep(10)
            continue
        break
    raise RuntimeError(f"D1 read failed: {last}")


def local_ids(source: str) -> set:
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=120)
    con.execute("PRAGMA busy_timeout=120000")
    try:
        return {r[0] for r in con.execute("SELECT series_id FROM series WHERE series_id >= ? AND series_id < ?",
                                          (source + ":", source + ";"))}
    finally:
        con.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--source", required=True)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true",
                    help="explicit no-write run (already the default without --apply); named in ARGV so the D1 cost "
                         "guard can tell a free run from a charged one (R323)")
    ap.add_argument("--chunk", type=int, default=400)
    a = ap.parse_args()
    if not os.path.exists(DB):
        print(f"no local catalogue at {DB}"); return 2
    t0 = dt.datetime.now(dt.timezone.utc)
    rows, rows_read = d1_rows(a.source)
    have = local_ids(a.source)
    missing = [r for r in rows if r.get("series_id") and r["series_id"] not in have]
    print(f"{t0:%H:%M:%SZ} {a.source}: D1 rows {len(rows):,} (rows_read {rows_read:,}); local rows {len(have):,}; "
          f"on D1 and NOT local: {len(missing):,}", flush=True)
    for r in missing[:12]:
        print(f"   {r['series_id']:<28} {str(r.get('start_date')):10} {str(r.get('end_date')):10} {str(r.get('title'))[:60]}")
    if len(missing) > 12:
        print(f"   ... {len(missing) - 12:,} more")
    receipt = {"utc": t0.isoformat(), "source": a.source, "d1_rows": len(rows), "d1_rows_read": rows_read,
               "local_rows_before": len(have), "missing": [r["series_id"] for r in missing], "apply": a.apply}
    rpath = os.path.join("D:/temp/claude" if os.path.isdir("D:/temp/claude") else ROOT,
                         f"sync_rows_{a.source}_{t0:%Y%m%dT%H%M%SZ}.json")
    if not a.apply or not missing:
        json.dump(receipt, open(rpath, "w", encoding="utf-8"), indent=1)
        print(f"dry run - nothing written; receipt {rpath}" if not a.apply else f"nothing to insert; receipt {rpath}")
        return 0

    con = sqlite3.connect(DB, timeout=120, isolation_level=None)
    con.execute("PRAGMA busy_timeout=120000")
    local_cols = [c[1] for c in con.execute("PRAGMA table_info(series)")]
    d1_cols = [c for c in missing[0].keys() if c in local_cols]
    absent = [c for c in local_cols if c not in d1_cols]
    print(f"  columns copied: {d1_cols}; local-only columns left NULL: {absent}")
    ins = f"INSERT OR IGNORE INTO series ({', '.join(d1_cols)}) VALUES ({', '.join('?' for _ in d1_cols)})"
    inserted = fts = 0
    chunks = [missing[i:i + a.chunk] for i in range(0, len(missing), a.chunk)]
    for k, part in enumerate(chunks, 1):
        for attempt in range(12):
            try:
                con.execute("BEGIN IMMEDIATE")
                for r in part:
                    cur = con.execute(ins, [r.get(c) for c in d1_cols])
                    if cur.rowcount == 1:
                        inserted += 1
                        con.execute("INSERT INTO series_fts (series_id, title, geography) VALUES (?,?,?)",
                                    (r["series_id"], r.get("title"), r.get("geography")))
                        fts += 1
                con.execute("COMMIT")
                break
            except sqlite3.OperationalError as e:
                print(f"   chunk {k} attempt {attempt + 1}: {e} - retrying in 20 s", flush=True)
                try:
                    con.execute("ROLLBACK")
                except Exception:              # noqa: BLE001
                    pass
                time.sleep(20)
        else:
            receipt.update({"inserted": inserted, "fts_inserted": fts, "stopped_at_chunk": k})
            json.dump(receipt, open(rpath, "w", encoding="utf-8"), indent=1)
            print(f"chunk {k} never committed after {inserted:,} row(s); receipt {rpath}"); return 1
        print(f"   chunk {k}/{len(chunks)}: {inserted:,} row(s) inserted so far", flush=True)
    after = len(local_ids(a.source))
    receipt.update({"inserted": inserted, "fts_inserted": fts, "local_rows_after": after})
    json.dump(receipt, open(rpath, "w", encoding="utf-8"), indent=1)
    print(f"DONE: inserted {inserted:,} row(s) + {fts:,} FTS row(s); local {a.source} rows {len(have):,} -> {after:,} "
          f"(D1 {len(rows):,}); receipt {rpath}")
    con.close()
    return 0 if after == len(rows) else 1


if __name__ == "__main__":
    sys.exit(main())
