"""Catalogue usda at TABLE grain — one row per (SOURCE_DESC, AGG_LEVEL_DESC, SHORT_DESC).

Pairs with tools/derive_usda_tables.py, which writes the CSVs; the id shape must match it
exactly or the catalogue lists ids whose objects do not exist. Both build the id through
`table_id()` imported from that module, so there is ONE definition and they cannot drift.

WHY TABLE GRAIN AT ALL (measured, see the derive's docstring): 57,629,841 observations across
15,534,339 series is 3.7 observations each — the CSO pathology. 72,046 tables of ~800
observations is the honest unit, and SHORT_DESC is USDA's own one-line name for the measure.

TITLES come from USDA's own vocabulary, never invented:
    MILK - FAT TEST, MEASURED IN PCT — State — Survey
i.e. SHORT_DESC, then the aggregation level, then whether the figures are CENSUS or SURVEY.
That last part matters and is not decoration: USDA's Census of Agriculture and its Surveys are
different collections with different methods, and a title that merged them would present two
distinct measurements as one series.

The 25 pre-existing rows are slug-style ids (`usda:corn_grain_production_measured_in_$`) from a
hand-curated starter set. They are REPLACED; their R2 objects are left in place so any existing
link still resolves, the same treatment fed_board and fhfa got.

--apply writes; without it this prints what it would do and changes nothing.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sqlite3
import sys

import duckdb

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools"))

from derive_usda_tables import SOURCE, STORE, table_id         # noqa: E402

LICENSE_ID = "us-public-domain"
BATCH = 20_000
SOURCE_WORD = {"CENSUS": "Census of Agriculture", "SURVEY": "Survey"}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--memory-limit", default="12GB")
    a = ap.parse_args()

    con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=180.0)
    con.execute("PRAGMA busy_timeout = 180000")
    lic = con.execute("select reservable, name from license where license_id=?",
                      (LICENSE_ID,)).fetchone()
    if not lic or not lic[0]:
        print(f"licence {LICENSE_ID!r} missing or not reservable — refusing to create rows")
        return 1
    print(f"licence {LICENSE_ID}: reservable={lic[0]}  ok to catalogue")

    files = sorted(f.replace("\\", "/")
                   for f in glob.glob(os.path.join(STORE, "**", "*.parquet"), recursive=True))
    if not files:
        raise SystemExit(f"no parquet under {STORE} — refusing to catalogue an empty store")
    spill = os.path.join(ROOT, "logs", "_duckspill")
    os.makedirs(spill, exist_ok=True)
    q = duckdb.connect()
    q.execute(f"SET memory_limit='{a.memory_limit}'")
    q.execute(f"SET temp_directory='{spill}'")
    q.execute("SET preserve_insertion_order=false")
    lst = "[" + ",".join(f"'{f}'" for f in files) + "]"

    print(f"aggregating {len(files)} file(s) — one pass", flush=True)
    rows_in = q.execute(f"""
        SELECT SOURCE_DESC, AGG_LEVEL_DESC, SHORT_DESC,
               min(obs_date)::VARCHAR AS d0, max(obs_date)::VARCHAR AS d1,
               count(*) AS n_obs,
               any_value(UNIT_DESC) AS unit,
               any_value(FREQ_DESC) AS freq
        FROM read_parquet({lst})
        WHERE value IS NOT NULL AND obs_date IS NOT NULL
        GROUP BY 1, 2, 3""").fetchall()
    print(f"{len(rows_in):,} tables", flush=True)

    meta = json.dumps({
        "citation_short": "U.S. Department of Agriculture, National Agricultural Statistics "
                          "Service (NASS).",
        "citation_long": ("U.S. Department of Agriculture, National Agricultural Statistics "
                          "Service, Quick Stats. Compiled and redistributed by the Elkassabgi "
                          "Data Library."),
        "description_processing": ("Retrieved from USDA NASS Quick Stats and stored as zstd "
                                   "Parquet. Served at TABLE grain — one CSV per (source, "
                                   "aggregation level, measure) — because the source averages "
                                   "3.7 observations per series. Each row carries its location, "
                                   "domain category and reference period, so a forecast vintage "
                                   "is distinguishable from a final estimate."),
    }, ensure_ascii=False)

    out = []
    for src, agg, short, d0, d1, n_obs, unit, freq in rows_in:
        who = SOURCE_WORD.get((src or "").strip(), src)
        title = f"{short} — {(agg or '').title()} — {who}"
        out.append((table_id(src, agg, short), SOURCE, title,
                    None, (unit or None), (agg or None), "Agriculture",
                    LICENSE_ID, d0, d1, meta))

    print(f"rows to write: {len(out):,}")
    for r in out[:4]:
        print(f"   {r[0][:96]}")
        print(f"      {r[2][:104]}   {r[8]}..{r[9]}")

    if not a.apply:
        print("\n(dry run — pass --apply to write)")
        return 0

    for i in range(0, len(out), BATCH):
        con.executemany(
            """INSERT OR REPLACE INTO series
               (series_id,source_id,title,frequency,unit,geography,category,license_id,
                start_date,end_date,metadata) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            out[i:i + BATCH])
    con.commit()

    keep = {r[0] for r in out}
    legacy = [r[0] for r in con.execute(
        "select series_id from series where source_id=?", (SOURCE,)).fetchall()
        if r[0] not in keep]
    con.executemany("delete from series where series_id=?", [(s,) for s in legacy])
    con.commit()
    n = con.execute("select count(*) from series where source_id=?", (SOURCE,)).fetchone()[0]
    print(f"\nremoved {len(legacy)} legacy slug-id row(s); their R2 objects are kept")
    print(f"catalogue rows for {SOURCE}: {n:,}")
    try:
        con.execute("INSERT INTO series_fts(series_fts) VALUES('rebuild')")
        con.commit()
        print("series_fts rebuilt")
    except sqlite3.Error as e:
        print(f"series_fts rebuild skipped: {e}")
    print("\nNEXT: add 'usda' handling to the resolver (table grain, not uniform-long), verify "
          "against R2, then sync D1.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
