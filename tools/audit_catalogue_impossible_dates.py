# -*- coding: utf-8 -*-
"""Census of CATALOGUE rows (not store rows) carrying impossible dates, by source.

tools/audit_impossible_dates.py audits the STORES (parquet row-group statistics). It passed while 23
stat_slovenia catalogue rows advertised year 0001 and 23 served CSVs carried them (2026-09-04): the
catalogue is what metadata.json and browse show, the served CSV is what users download, and neither
is a store. This tool reads the catalogue.

  python tools/audit_catalogue_impossible_dates.py DBPATH        # one pass: SUM(...) GROUP BY source_id
  python tools/audit_catalogue_impossible_dates.py DBPATH --indexed  # per-source, safe on the LIVE file
  python tools/audit_catalogue_impossible_dates.py DBPATH --list [SRC ...]   # also list the rows

--indexed EXISTS BECAUSE THE DEFAULT SCAN IS WHY NOBODY RUNS THIS. The GROUP BY below reads every
row, so it needs a snapshot of an 11.9 GB database and starves against the live file. `series.source_id`
is indexed (ix_series_source_id), so asking each source separately answers the SAME question without
scanning the table: measured 2026-09-22, 321 sources in 9.5 s against the LIVE file with crawlers
running, versus the hours the full scan took on the same disk. Same predicate, same totals, same
allowlist - only the access path differs. Prefer it unless you already hold a snapshot.

DBPATH: for the DEFAULT mode, give a SNAPSHOT or a local COPY, not the live data/catalog.db while a
crawler runs. The live
file is rollback-journal with writers attached; this full scan took under a minute on a copy on an
idle disk (2026-09-05: 51 s for 12,376,196 rows) and starved for hours against the live file on
the crawlers' disk. After a run, confirm each named source against live D1 with ONE bounded
statement: WHERE source_id IN (...) AND (start_date < '1500-01-01' OR end_date > '2200-01-01')
GROUP BY source_id (rows_read = the sum of those sources' rows; 144,302 for ten sources).

Known legitimate hits (do not "fix"): ggdc, maddison, gapminder start at year 1 / 730 (deep
economic history); eurostat uses 9999-12-31 as a publisher sentinel. Everything else is a defect:
a cross-tabulation catalogued as a time series (delist) or a period-parser error (repair).
"""
from __future__ import annotations
import argparse, sqlite3, sys, time

LEGIT = {"ggdc": "deep history (Maddison lineage) — starts year 1", "maddison": "deep history — starts year 1",
         "gapminder": "deep history — starts 730", "eurostat": "publisher sentinel 9999-12-31"}
Q = ("SELECT source_id, "
     "  SUM(CASE WHEN start_date < '1500-01-01' THEN 1 ELSE 0 END) AS early_start, "
     "  SUM(CASE WHEN end_date   > '2200-01-01' THEN 1 ELSE 0 END) AS late_end, "
     "  SUM(CASE WHEN start_date < '1500-01-01' OR end_date > '2200-01-01' THEN 1 ELSE 0 END) AS any_bad, "
     "  COUNT(*) AS n FROM series GROUP BY source_id HAVING any_bad > 0 ORDER BY any_bad DESC")

# Same predicate as Q, one source at a time, driven by ix_series_source_id.
QS = ("SELECT "
      "  SUM(CASE WHEN start_date < '1500-01-01' THEN 1 ELSE 0 END), "
      "  SUM(CASE WHEN end_date   > '2200-01-01' THEN 1 ELSE 0 END), "
      "  SUM(CASE WHEN start_date < '1500-01-01' OR end_date > '2200-01-01' THEN 1 ELSE 0 END), "
      "  COUNT(*) FROM series WHERE source_id = ?")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("db"); ap.add_argument("--list", nargs="*", default=None, help="list rows (all named sources, or these)")
    ap.add_argument("--indexed", action="store_true",
                    help="sweep per source using the source_id index instead of one full scan; safe on the live catalogue")
    a = ap.parse_args()
    if a.db.replace("\\", "/").endswith("data/catalog.db") and not a.indexed:
        print("WARNING: this is the live catalogue; with a crawler writing, this scan blocks and is blocked. Prefer --indexed, or a copy.", file=sys.stderr)
    con = sqlite3.connect("file:%s?mode=ro" % a.db.replace("\\", "/"), uri=True, timeout=60)
    t0 = time.time()
    if a.indexed:
        # One bounded, index-backed query per source. Never touches a row whose
        # source_id was not asked for, so it cannot degenerate into a table scan.
        srcs = [r[0] for r in con.execute(
            "SELECT DISTINCT source_id FROM series ORDER BY source_id")]
        rows, n_total = [], 0
        for s in srcs:
            e1, e2, c, n = con.execute(QS, (s,)).fetchone()
            n_total += n
            if c:
                rows.append((s, e1, e2, c, n))
        rows.sort(key=lambda r: -r[3])
        mode = "indexed sweep of %d sources" % len(srcs)
    else:
        n_total, = con.execute("SELECT COUNT(*) FROM series").fetchone()
        rows = con.execute(Q).fetchall()
        mode = "full scan"
    print("rows: %d   %s %.1f s\n" % (n_total, mode, time.time() - t0))
    print("%-22s %10s %10s %10s %12s  %s" % ("source", "start<1500", "end>2200", "any", "of rows", "note"))
    tot = 0
    for s, e1, e2, c, n in rows:
        tot += c
        print("%-22s %10d %10d %10d %12d  %s" % (s, e1, e2, c, n, LEGIT.get(s, "")))
    print("\nsources: %d   rows with impossible dates: %d   (legitimate-by-allowlist rows counted; see note column)" % (len(rows), tot))
    if a.list is not None:
        want = set(a.list) or {r[0] for r in rows}
        for s in sorted(want):
            rs = con.execute("SELECT series_id, start_date, end_date, title FROM series WHERE source_id=? AND "
                             "(start_date < '1500-01-01' OR end_date > '2200-01-01') ORDER BY series_id", (s,)).fetchall()
            print("\n=== %s: %d rows" % (s, len(rs)))
            for sid, st, en, ti in rs:
                print("  %-60s %s..%s  %s" % (sid[:60], st, en, (ti or "")[:60]))
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
