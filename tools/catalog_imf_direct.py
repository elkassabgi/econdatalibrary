"""Build catalogue rows for an imf_*_direct source, with decoded titles and real coverage.

The six GFS direct sources hold data in R2 and have ZERO catalogue rows, so every series is
downloadable by id and invisible to search - the gap core/sync_catalog_d1.py exists to close,
and the one that stranded 31,259 series before it was measured.

WHAT THIS WRITES THAT THE EXISTING _direct ROWS LACK. The seven already-catalogued direct
sources carry title == the raw key, start_date/end_date NULL and metadata {}. This writes:
  * a title decoded from IMF's own codelists (tools/imf_direct_titles.py), so
    `GFS_SSUC:AFG.A.BI.CIOA_TCB_CAB..S13.POGDP_PT` reads as "Afghanistan, Islamic Republic of
    - Annual - Balancing Items - Cash inflow from operating activities ... - Percent of GDP";
  * real start/end per series, computed from the store rather than left null;
  * producer-first citation metadata matching the rest of the catalogue.

LICENCE IS CHECKED BEFORE ANYTHING IS WRITTEN, not assumed from a sibling source. A row in the
catalogue is an offer to serve, so a source whose licence is not reservable must not get one.

--apply writes; without it this prints what it would do and changes nothing.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools"))

import imf_direct_titles as T                                    # noqa: E402

CITATION_SHORT = "International Monetary Fund (IMF)."
TERMS = "https://www.imf.org/en/about/copyright-and-terms"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True)
    ap.add_argument("--flow", required=True)
    ap.add_argument("--agency", default="IMF.STA")
    ap.add_argument("--name", required=True, help="human name for the source row")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    from updater import blob, config
    store = os.path.join(config.source_dir(a.source), f"{a.source}.parquet")
    if not blob.exists(store):
        print(f"no store for {a.source} at {store}")
        return 1

    # busy_timeout, because catalog.db has OTHER writers. The updater's CSV derive and its
    # catalogue sync both touch it, and a run can hold the write lock for minutes. Without a
    # timeout sqlite raises "database is locked" the instant it collides - which is exactly
    # what happened on the first batch: 4 of 7 sources failed and, because the loop grepped
    # stdout for a success line, the tracebacks went to stderr and the failures looked like
    # silence. Wait for the lock instead of losing the work.
    con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=120.0)
    con.execute("PRAGMA busy_timeout = 120000")

    # --- licence gate, before anything else -------------------------------------------
    lic = con.execute("select reservable, name from license where license_id=?",
                      ("imf-terms",)).fetchone()
    if not lic:
        print("licence 'imf-terms' is not in the catalogue - refusing to create rows")
        return 1
    if not lic[0]:
        print(f"licence 'imf-terms' is NOT reservable ({lic[1]}) - a catalogue row is an offer "
              f"to serve, so refusing to create one")
        return 1
    print(f"licence imf-terms: reservable={lic[0]}  ok to catalogue")

    # --- coverage per series, from the store -------------------------------------------
    tbl = blob.read_table(store, columns=["series_key", "obs_date"])
    print(f"store: {tbl.num_rows:,} rows")
    ks = tbl.column("series_key").to_pylist()
    ds = tbl.column("obs_date").to_pylist()
    span: dict[str, list] = {}
    for k, d in zip(ks, ds):
        if not k or d is None:
            continue
        iso = d.isoformat()
        cur = span.get(k)
        if cur is None:
            span[k] = [iso, iso]
        else:
            if iso < cur[0]:
                cur[0] = iso
            if iso > cur[1]:
                cur[1] = iso
    print(f"series: {len(span):,}")

    # --- titles ------------------------------------------------------------------------
    dsd_dims, dim_codes = T.load_structure(a.flow, a.agency)
    order = T.load_dims(a.source, store) or T.infer_dims(
        list(span)[:12], dsd_dims, dim_codes)
    if not order:
        print("could not establish the key order - refusing to write titles that may be wrong")
        return 1
    print(f"key order: {order}")

    meta = json.dumps({
        "citation_short": CITATION_SHORT,
        "citation_long": (f"International Monetary Fund - {a.name}. Retrieved directly from "
                          f"the IMF SDMX API (api.imf.org). Compiled and redistributed by the "
                          f"Elkassabgi Data Library."),
        "description_processing": ("Retrieved first-hand from api.imf.org (SDMX 2.1), "
                                   "normalized to a long {series_key, obs_date, value} schema, "
                                   "de-duplicated, and stored as zstd Parquet."),
    }, ensure_ascii=False)

    rows, unresolved = [], 0
    for key, (start, end) in span.items():
        title, hit, tot = T.title_for(key, order, dim_codes)
        if tot and hit < tot:
            unresolved += 1
        rows.append((f"{a.source}:{key}", a.source, title, None, None, None, None,
                     "imf-terms", start, end, meta))

    print(f"rows to write: {len(rows):,}   with an unresolved part: {unresolved:,}")
    for r in rows[:3]:
        print(f"   {r[0]}")
        print(f"      {r[2][:120]}   {r[8]}..{r[9]}")

    if not a.apply:
        print("\n(dry run - pass --apply to write)")
        return 0

    con.execute(
        "INSERT OR REPLACE INTO source(source_id,name,homepage,license_id,attribution,terms_url)"
        " VALUES(?,?,?,?,?,?)",
        (a.source, f"International Monetary Fund — {a.name} (direct from api.imf.org)",
         "https://www.imf.org/en/data", "imf-terms",
         f"Source: International Monetary Fund, {a.name}. Retrieved directly from the IMF "
         f"SDMX API (api.imf.org).", TERMS))
    con.executemany(
        """INSERT OR REPLACE INTO series
           (series_id,source_id,title,frequency,unit,geography,category,license_id,
            start_date,end_date,metadata) VALUES(?,?,?,?,?,?,?,?,?,?,?)""", rows)
    con.commit()
    n = con.execute("select count(*) from series where source_id=?", (a.source,)).fetchone()[0]
    print(f"\nwritten: {n:,} catalogue rows for {a.source}")
    try:
        con.execute("INSERT INTO series_fts(series_fts) VALUES('rebuild')")
        con.commit()
        print("series_fts rebuilt")
    except sqlite3.Error as e:
        print(f"series_fts rebuild skipped: {e}")
    print("\nNEXT, and required before these are usable: derive their CSVs "
          "(tools/derive_csv_bulk.py), add the source id to api/worker/src/util.ts "
          "SUPPORTED_SOURCES, and sync to D1 (core/sync_catalog_d1.py --source ...). "
          "A catalogue row without a CSV is a listed series that will not download.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
