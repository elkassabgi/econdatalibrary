"""Catalogue every PWT 11.0 series the ingester actually landed — not a curated 60.

THE GAP. `jobs/ingest_penn_world_table.py` publishes "the 42 NUMERIC variables" of pwt110.xlsx
and the store holds 7,163 series (42 files, 418,397 rows, newest obs 2023-12-31). The CATALOGUE
holds 60, because `connectors/penn_world_table/connector.py` hard-codes VARIABLES (6) x ECONOMIES
(10) — a deliberate early curation that the ingester long outgrew.

WHY THAT MATTERS RATHER THAN BEING A TIDY-UP. The SUPERSEDED vintage is fully catalogued: source
id `pwt` carries 7,159 series (43 indicators x 183 geos) whose data ends 2019-12-31. Both ids are
in SUPPORTED_SOURCES. So a browsing user finds 7,159 series of a four-year-stale vintage and 60
of the current one — the fresh data is in R2 and effectively invisible. This does not add data;
it stops hiding data we already host.

NOTHING HERE IS INVENTED. Every field comes from PWT itself:
  * definition + unit  -> jobs.ingest_penn_world_table.VAR_DEFS, whose comment records them as
                          "verbatim from the workbook's Legend sheet"
  * country name       -> the workbook's own Data sheet (185 countrycode/country pairs)
  * start / end dates  -> measured per series from the parquet in the store
  * licence            -> cc-by-4.0, matching the 60 rows already present
A series whose variable is missing from VAR_DEFS is REPORTED AND SKIPPED, never given a
placeholder title — a made-up definition on an economics series is worse than an absent one.

ORDER IS LOAD-BEARING. Catalog rows written here are INERT: the serving layer reads D1, not this
file, so nothing becomes visible until a later D1 sync. Derive the CSVs and verify them present
in R2 BEFORE that sync. Cataloguing first and deriving later is how harvard_atlas ended up with
255,217 series served to nobody and the four IEP sources with Download buttons that 404 — both
recorded in api/worker/src/util.ts.

    python tools/catalog_penn_world_table.py --dry-run
    python tools/catalog_penn_world_table.py --apply
"""
from __future__ import annotations
import argparse
import json
import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import pyarrow.compute as pc                                  # noqa: E402
from updater import blob, config                              # noqa: E402

SOURCE = "penn_world_table"
LICENSE = "cc-by-4.0"
CATEGORY = "macro"
FREQ = "A"


def _country_names() -> dict:
    """ISO3 -> country name, from the workbook's own Data sheet."""
    import pandas as pd
    xlsx = os.path.join(ROOT, "data", "raw", SOURCE, "pwt110.xlsx")
    if not os.path.exists(xlsx):
        raise SystemExit(f"pwt110.xlsx not found at {xlsx!r}. It is the ONLY authority for "
                         f"country names here; refusing to guess them.")
    df = pd.read_excel(xlsx, sheet_name="Data", engine="openpyxl",
                       usecols=["countrycode", "country"]).drop_duplicates()
    return {str(c).strip(): str(n).strip() for c, n in df.itertuples(index=False, name=None)}


def _store_series() -> dict:
    """{(var, geo): (start_date, end_date)} for everything actually in the store."""
    d = config.source_dir(SOURCE)
    out = {}
    for rel in blob.list_parquets(d, recursive=True):
        var = os.path.basename(rel)[:-len(".parquet")]
        t = blob.read_table(os.path.join(d, rel)).select(["series_key", "obs_date"])
        keys = t.column("series_key").to_pylist()
        dates = t.column("obs_date").to_pylist()
        for k, dt_ in zip(keys, dates):
            # native key is '<var>|<geo>' — see clients/python/econdl/_resolve.py::_resolve_pwt
            if "|" not in k:
                continue
            kvar, geo = k.split("|", 1)
            cur = out.get((kvar, geo))
            if cur is None:
                out[(kvar, geo)] = [dt_, dt_]
            else:
                if dt_ < cur[0]:
                    cur[0] = dt_
                if dt_ > cur[1]:
                    cur[1] = dt_
        del t, keys, dates
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    import jobs.ingest_penn_world_table as ING
    defs = ING.VAR_DEFS
    names = _country_names()
    series = _store_series()
    print(f"backend={config.BACKEND}")
    print(f"VAR_DEFS: {len(defs)} variables (verbatim from the Legend sheet)")
    print(f"country names from the workbook: {len(names)}")
    print(f"series in the store: {len(series):,}\n")

    con = sqlite3.connect(config.CATALOG_DB if hasattr(config, "CATALOG_DB")
                          else os.path.join(ROOT, "data", "catalog.db"), timeout=300)
    have = {r[0] for r in con.execute(
        "SELECT series_id FROM series WHERE source_id=?", (SOURCE,))}
    print(f"already catalogued: {len(have):,}")

    rows, skipped_var, skipped_geo = [], set(), set()
    for (var, geo), (d0, d1) in sorted(series.items()):
        if var not in defs:
            skipped_var.add(var)
            continue
        if geo not in names:
            skipped_geo.add(geo)
            continue
        sid = f"{SOURCE}:{var}:{geo}"
        if sid in have:
            continue
        definition, unit = defs[var]
        rows.append((
            sid, SOURCE, f"{definition} - {names[geo]}", FREQ, unit, geo, CATEGORY, LICENSE,
            d0.isoformat(), d1.isoformat(), None,
            json.dumps({"variable": var, "definition": definition, "country": names[geo]},
                       ensure_ascii=False),
        ))

    print(f"NEW rows to write: {len(rows):,}")
    if skipped_var:
        print(f"  SKIPPED — {len(skipped_var)} variable(s) absent from VAR_DEFS, so no "
              f"authoritative definition exists: {sorted(skipped_var)}")
    if skipped_geo:
        print(f"  SKIPPED — {len(skipped_geo)} geo(s) absent from the workbook: "
              f"{sorted(skipped_geo)}")
    if rows:
        print(f"  sample: {rows[0][0]}  |  {rows[0][2]}  |  {rows[0][8]}..{rows[0][9]}")

    if not a.apply:
        print("\n(dry run — nothing written)")
        con.close()
        return 0

    con.executemany(
        "INSERT OR IGNORE INTO series(series_id,source_id,title,frequency,unit,geography,"
        "category,license_id,start_date,end_date,last_updated,metadata) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    con.commit()
    n = con.execute("SELECT COUNT(*) FROM series WHERE source_id=?", (SOURCE,)).fetchone()[0]
    con.close()
    print(f"\nWROTE {len(rows):,}; {SOURCE} now has {n:,} catalog rows")
    print("These are INERT until a D1 sync. Derive the CSVs and verify them in R2 FIRST:")
    print("    python -m core.derive_csv --source penn_world_table --dry-run")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
