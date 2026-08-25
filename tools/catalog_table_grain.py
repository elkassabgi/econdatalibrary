#!/usr/bin/env python3
"""Catalogue a FILE-GRAIN source at TABLE grain: one catalog row per store file.

WHY THIS EXISTS. tools/catalog_complete.py catalogues at series-key grain — one row per
distinct key. That is right for a source whose keys are the thing users ask for, and wrong
for cbs_nl and gus_dbw: cbs_nl holds 9,063,913,608 rows across 5,156 tables, so series grain
would put billions of rows into a 9 GB catalogue. Both sources are registered file-grain in
clients/python/econdl/_resolve.py (`_resolve_file_grain`), meaning one catalog id IS one
store file and the rows inside carry dimension-only keys. The catalogue must match that
grain or the ids it lists will not resolve.

This is the same decision imf_imts_direct made ("2,937 table rows serve 472,234 partner
series") and for the same reason: the catalogue is a finding aid, not a copy of the store.

TITLES ARE THE PUBLISHER'S, NOT INVENTED. cbs_nl titles come from CBS's own OData catalogue
(opendata.cbs.nl/ODataCatalog/Tables), cached to data/_cbs_titles.json. A table with no
published title keeps its native id rather than a guess.

DATES ARE MEASURED from each parquet's obs_date column statistics — footer metadata only, no
data read. A file whose statistics are absent gets NULL dates rather than a fabricated range.

--apply writes; without it this prints what it would do and changes nothing.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys

import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG = os.path.join(ROOT, "data", "catalog.db")
STORE = os.path.join(ROOT, "data", "clean_full")

_PART_RE = re.compile(r"\.part\d*\.parquet$", re.I)
_EXCL_DIRS = {"parts", "_cache", "_tmp"}

# source -> (licence id, title-map file or None, id builder)
SOURCES = {
    "cbs_nl":  ("cc-by-4.0",   "data/_cbs_titles.json"),
    "gus_dbw": ("gus-pl-open", "data/_gus_titles.json"),
    # ilo: CC BY 4.0 (ILO organization-wide grant, DATABASE_LICENSES_VERBATIM 2026-08-24).
    # No title map - the SDMX dataflow id IS the published name, so it stands as the title.
    "ilo": ("cc-by-4.0", "data/_ilo_titles.json"),
}


def _tables(source: str):
    """(table_id, abs_path) for every REAL table file of this source."""
    root = os.path.join(STORE, source)
    out = []
    for dp, _dirs, files in os.walk(root):
        rel = os.path.relpath(dp, root).replace(os.sep, "/")
        if _EXCL_DIRS.intersection(rel.split("/")):
            continue
        for fn in files:
            if not fn.endswith(".parquet"):
                continue
            if _PART_RE.search(fn) or fn.endswith("__series.parquet") or "ckpt" in fn.lower():
                continue
            out.append((fn[:-len(".parquet")], os.path.join(dp, fn)))
    return sorted(out)


def _date_range(path: str):
    """(min, max) of obs_date from column statistics — footer only, no data read."""
    try:
        md = pq.read_metadata(path)
        idx = md.schema.names.index("obs_date") if "obs_date" in md.schema.names else None
        if idx is None:
            return None, None
        lo = hi = None
        for rg in range(md.num_row_groups):
            st = md.row_group(rg).column(idx).statistics
            if st is None or not st.has_min_max:
                continue
            a, b = str(st.min)[:10], str(st.max)[:10]
            lo = a if lo is None or a < lo else lo
            hi = b if hi is None or b > hi else hi
        return lo, hi
    except Exception:
        return None, None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("sources", nargs="+", choices=sorted(SOURCES))
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    con = sqlite3.connect(CATALOG, timeout=180)
    con.execute("PRAGMA busy_timeout=180000")
    total_new = 0

    for src in a.sources:
        lic, title_file = SOURCES[src]
        have_lic = con.execute("SELECT COUNT(*) FROM license WHERE license_id=?", (lic,)).fetchone()[0]
        if not have_lic:
            print(f"  {src}: licence {lic!r} has NO row in the license table — record its "
                  f"terms before cataloguing. Refusing.")
            return 2
        titles = {}
        if title_file:
            p = os.path.join(ROOT, title_file.replace("/", os.sep))
            if os.path.exists(p):
                titles = json.load(open(p, encoding="utf-8"))
            else:
                print(f"  {src}: title map {title_file} missing — ids will be their own titles.")

        tables = _tables(src)
        existing = {r[0] for r in con.execute(
            "SELECT series_id FROM series WHERE source_id=?", (src,))}
        rows, skipped = [], 0
        for tid, path in tables:
            sid = f"{src}:{tid}"
            if sid in existing:
                skipped += 1
                continue
            lo, hi = _date_range(path)
            rows.append((sid, src, titles.get(tid, tid), None, None, None, None, lic, lo, hi, None, "{}"))
        print(f"  {src:9} tables={len(tables):>6,}  already catalogued={skipped:>6,}  "
              f"to insert={len(rows):>6,}  licence={lic}"
              + (f"  titled={sum(1 for t,_ in tables if t in titles):,}" if titles else ""))
        if rows[:1]:
            r = rows[0]
            print(f"            e.g. {r[0]}  title={r[2][:58]!r}  dates={r[8]}..{r[9]}")
        if a.apply and rows:
            con.executemany(
                "INSERT OR IGNORE INTO series (series_id,source_id,title,frequency,unit,"
                "geography,category,license_id,start_date,end_date,last_updated,metadata) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows)
            try:
                # DELETE FIRST. `series` above is INSERT OR IGNORE, but an FTS5 virtual table
                # has NO unique constraint, so OR IGNORE has no equivalent here: an id that
                # `series` ignores still gets a SECOND row in the index. That mismatch is the
                # shape that put 8.00 copies of every boc series in the live index.
                con.executemany("DELETE FROM series_fts WHERE series_id=?",
                                [(r[0],) for r in rows])
                con.executemany("INSERT INTO series_fts(series_id,title,geography) VALUES (?,?,?)",
                                [(r[0], r[2], None) for r in rows])
            except sqlite3.OperationalError:
                pass
            con.commit()
            print(f"            -> inserted {len(rows):,} rows")
        total_new += len(rows)

    if not a.apply:
        print(f"\n  DRY RUN — {total_new:,} rows would be inserted. Re-run with --apply.")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
