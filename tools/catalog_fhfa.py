"""Build catalogue rows for fhfa from the store's own series sidecars.

WHY. 89,706 series sit in R2 and fhfa has 61 catalogue rows. It is one of five
hosted-but-invisible sources tools/reconcile_serving.py surfaced on 2026-08-01.

THE ID IS `fhfa:<dataset>:<series_key>`, one shape for all ten store files. The existing 61
use `fhfa:<flavor>:<freq>:<place_id>`, which cannot name most of this source: it only ever
reaches hpi_master.parquet (1,178 of 89,706 series) and it hardcodes hpi_type `traditional`,
so the 65 non-metro / distress-free / manufactured / developmental series in that same file
are inexpressible. _resolve_fhfa now accepts both, choosing by whether the second segment
names a real store file.

THE 61 LEGACY ROWS ARE REPLACED, THEIR CSVs ARE NOT. Deleting those objects would 404 every
existing link for no gain; the resolver still answers the legacy id, so old links keep working
while search offers the general one.

TITLES ARE COMPOSED, NOT COPIED FROM ONE COLUMN. The ten sidecars do not share a schema -
annual_cbsa names the place in `name`, annual_county splits it across `state` and `county`,
annual_zip5 has `zip`, annual_tract has only a constant `level`, hpi_master has `place_name` -
and taking the first populated column produces titles that do not identify anything: 376 county
names are shared across states, and all 63,930 tract series would have been titled
"census-tract". place_of() composes instead, and the result is checked: 89,706 series yield
88,626 distinct place labels, with every per-dataset file 1:1.

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

SOURCE = "fhfa"
LICENSE_ID = "us-public-domain"
BATCH = 20_000

FREQ_CODE = {"monthly": "M", "quarterly": "Q", "annual": "A"}
# Every column that could name a place, so the reader can be checked against the schemas.
PLACE_COLS = ["place_name", "name", "county", "state", "zip", "zip3", "cbsa", "fips",
              "state_abbr", "abbr", "level"]


def place_of(d: dict) -> str:
    """A human place label that IDENTIFIES the place, composed where one column cannot.

    Picking the first populated column is wrong twice over here:

      * annual_county stores 'Autauga' in `county` and 'AL' in `state`, and 376 county names
        are shared by more than one state - "Washington" alone names several different series.
      * annual_tract, which is 63,930 of this source's 89,706 series, has exactly ONE distinct
        value in its only descriptive column: `level` = 'census-tract'. Every one of those
        titles would have been identical, which is worse than no title at all: search would
        return 63,930 indistinguishable hits. Its identifier is the tract FIPS in series_key,
        so that is what the title has to carry.

    So the key is passed in and used wherever the descriptive columns cannot separate rows.
    """
    def g(c):
        v = d.get(c)
        return str(v).strip() if v not in (None, "") and str(v).strip() else ""

    key = g("series_key")
    if g("county"):
        return f"{g('county')} County, {g('state')}" if g("state") else g("county")
    if g("level") in ("census-tract", "Census Tract"):
        return f"Census Tract {key}" + (f", {g('state_abbr')}" if g("state_abbr") else "")
    for c in ("place_name", "name"):
        if g(c):
            return g(c)
    if g("state") and g("abbr"):
        return f"{g('state')} ({g('abbr')})"
    if g("zip"):
        return f"ZIP {g('zip')}"
    if g("zip3"):
        return f"3-digit ZIP {g('zip3')}" + (f", {g('state_abbr')}" if g("state_abbr") else "")
    for c in ("state", "cbsa", "fips", "abbr"):
        if g(c):
            return g(c)
    # A geography column that is constant across the dataset identifies nothing on its own;
    # pair it with the key rather than emitting the same title for every series.
    if g("state_abbr"):
        return f"{key}, {g('state_abbr')}"
    if g("level"):
        return f"{g('level')} {key}"
    return key


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=180.0)
    con.execute("PRAGMA busy_timeout = 180000")
    lic = con.execute("select reservable, name from license where license_id=?",
                      (LICENSE_ID,)).fetchone()
    if not lic or not lic[0]:
        print(f"licence {LICENSE_ID!r} missing or not reservable — refusing to create rows")
        return 1
    print(f"licence {LICENSE_ID}: reservable={lic[0]}  ok to catalogue")

    store = os.path.join(ROOT, "data", "clean_full", SOURCE)
    side = [f.replace("\\", "/") for f in glob.glob(os.path.join(store, "*__series.parquet"))]
    if not side:
        print(f"no sidecars under {store}")
        return 1
    q = duckdb.connect()
    q.execute("SET memory_limit='4GB'")
    lst = "[" + ",".join(f"'{f}'" for f in side) + "]"

    have = set(q.execute(f"describe select * from read_parquet({lst}, union_by_name=true)"
                         ).df()["column_name"])
    place = [c for c in PLACE_COLS if c in have]
    print(f"{len(side)} sidecars; place columns present: {place}")

    cols = ", ".join(f'"{c}"' for c in
                     ["dataset", "series_key", "hpi_type", "hpi_flavor", "frequency",
                      "start", "end"] + place)
    rows_in = q.execute(
        f'select {cols} from read_parquet({lst}, union_by_name=true)').fetchall()
    names = ["dataset", "series_key", "hpi_type", "hpi_flavor", "frequency",
             "start", "end"] + place
    print(f"{len(rows_in):,} sidecar rows")

    citation = json.dumps({
        "citation_short": "U.S. Federal Housing Finance Agency (FHFA).",
        "citation_long": ("U.S. Federal Housing Finance Agency, House Price Index (HPI). "
                          "Compiled and redistributed by the Elkassabgi Data Library."),
        "description_processing": ("Retrieved from FHFA's published HPI datasets, normalized "
                                   "to a long {series_key, obs_date, ...} schema and stored "
                                   "as zstd Parquet, one file per FHFA dataset. Index columns "
                                   "are kept side by side as published rather than melted."),
    }, ensure_ascii=False)

    seen, out, no_place = set(), [], 0
    for r in rows_in:
        d = dict(zip(names, r))
        sid = f"{SOURCE}:{d['dataset']}:{d['series_key']}"
        if sid in seen:
            continue
        seen.add(sid)
        who = place_of(d)  # d carries series_key, which place_of needs for tracts
        if not who:
            no_place += 1
            who = str(d["series_key"])
        bits = [who]
        if d.get("hpi_flavor"):
            bits.append(str(d["hpi_flavor"]))
        if d.get("hpi_type") and str(d["hpi_type"]) != "traditional":
            bits.append(str(d["hpi_type"]))
        title = "FHFA House Price Index — " + " — ".join(bits)
        if d.get("frequency"):
            title += f" ({d['frequency']})"
        out.append((sid, SOURCE, title, FREQ_CODE.get(str(d.get("frequency")), None), "Index",
                    who or None, "Housing", LICENSE_ID, d.get("start"), d.get("end"),
                    citation))

    print(f"rows to write: {len(out):,}   with no place column populated: {no_place:,}")
    for r in out[:4]:
        print(f"   {r[0]}")
        print(f"      {r[2][:110]}   {r[8]}..{r[9]}")

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

    keep = set(seen)
    legacy = [r[0] for r in con.execute(
        "select series_id from series where source_id=?", (SOURCE,)).fetchall()
        if r[0] not in keep]
    con.executemany("delete from series where series_id=?", [(s,) for s in legacy])
    con.commit()
    n = con.execute("select count(*) from series where source_id=?", (SOURCE,)).fetchone()[0]
    print(f"\nremoved {len(legacy)} legacy row(s); their R2 CSVs are kept so old links live")
    print(f"catalogue rows for {SOURCE}: {n:,}")
    try:
        con.execute("INSERT INTO series_fts(series_fts) VALUES('rebuild')")
        con.commit()
        print("series_fts rebuilt")
    except sqlite3.Error as e:
        print(f"series_fts rebuild skipped: {e}")
    print("\nNEXT: derive the CSVs (tools/derive_csv_bulk.py --source fhfa) and sync D1.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
