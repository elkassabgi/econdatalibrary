"""Catalogue whr — BUILT AND TESTED, BUT DO NOT RUN --apply YET. See PROVENANCE below.

PROVENANCE BLOCKER (found 2026-08-01, before publishing anything). This data did not come from
the World Happiness Report. jobs/ingest_whr.py tries WHR's own S3 XLS panels and several CSV
mirrors first; the ingest log shows every one of them returning 403 or 404, and the run fell
through to `https://ourworldindata.org/grapher/happiness-cantril-ladder.csv`. The store proves
it: 14 of its 178 geography codes are OWID's own aggregate codes (OWID_WRL, OWID_HIC,
OWID_AFR...), which the World Happiness Report does not publish.

That matters three ways, and each one alone is enough to hold publication:
  * the citation below would name WHR and Gallup for data actually retrieved from a third party;
  * the written permission on file is from Gallup/WHR, and data obtained via OWID is governed by
    OWID's terms (CC BY), which is a different licence question the audit never asked;
  * the owner's standing instruction is to take data from the source, not from an aggregator -
    the same correction that removed two DBnomics-backed fetchers.

The store is also partial: ingest_whr lists eight indicators and only "Self-reported life
satisfaction" is present.

transparency_ti and gpi are in the same family - transparency_ti fetches OWID as its PRIMARY url
with TI's own CDN as a "frequently 403" fallback, and gpi lists an OWID grapher CSV last among
its candidates. Both are already live and cited to their primary publisher.

Everything below is finished and dry-run clean (178 rows, 164 country names resolved from
pycountry, 14 OWID aggregate codes left as raw codes). Run it once the data comes from WHR.

---

Catalogue whr — 178 series that are hosted, cleared by written grant, and listed nowhere.

LICENCE, in full, because this one is a GRANT rather than a public licence and the distinction
matters. The World Happiness Report publishes NO licence: no CC mark, no terms-of-use document
addressing redistribution, and the audit's adversarial verifier searched four official surfaces
and found "no explicit redistribution ban, but crucially also no redistribution GRANT" -- the
site's "available to download for free" is a download permission, not a re-hosting one, and the
underlying Gallup World Poll is proprietary. On the public terms alone this source would stay
gated, and it was.

What clears it is a WRITTEN PERMISSION on file: Gallup/WHR granted redistribution in writing on
2026-07-09, recorded in DATABASE_LICENSES_VERBATIM.md as "CLEARED (NC + attrib)". So the licence
row created here is NON-COMMERCIAL and ATTRIBUTION-REQUIRED, and it is a transcription of that
recorded verdict, not a fresh judgement about the terms.

The licence row has to be created because none exists: neither `whr-granted` nor `whr` is in the
license table, so the catalogue's own gate refuses -- correctly, since a series row references a
licence and there was nothing to reference.

--apply writes; without it this prints what it would do and changes nothing.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys

import duckdb

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

SOURCE = "whr"
LICENSE_ID = "whr-granted"
TERMS = "https://www.worldhappiness.report/data-sharing/"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=180.0)
    con.execute("PRAGMA busy_timeout = 180000")

    store = os.path.join(ROOT, "data", "clean_full", SOURCE, "whr.parquet")
    if not os.path.exists(store):
        print(f"no store at {store}")
        return 1
    q = duckdb.connect()
    p = store.replace("\\", "/")
    rows_in = q.execute(f'''
        select series_key,
               min(obs_date)::VARCHAR, max(obs_date)::VARCHAR, count(*)
        from read_parquet('{p}') group by 1 order by 1''').fetchall()
    print(f"{len(rows_in):,} series in the store")

    meta = json.dumps({
        "citation_short": "World Happiness Report (Wellbeing Research Centre, University of "
                          "Oxford), powered by the Gallup World Poll.",
        "citation_long": ("World Happiness Report, published by the Wellbeing Research Centre "
                          "at the University of Oxford in partnership with Gallup and the UN "
                          "Sustainable Development Solutions Network. Data powered by the "
                          "Gallup World Poll. Redistributed by the Elkassabgi Data Library "
                          "under written permission granted 2026-07-09; non-commercial use, "
                          "attribution required."),
        "description_processing": ("Retrieved from the World Happiness Report's published "
                                   "data, normalized to a long {series_key, obs_date, value} "
                                   "schema and stored as zstd Parquet."),
    }, ensure_ascii=False)

    # ISO 3166-1 alpha-3 -> country name, from pycountry (the ISO register), NOT hand-written.
    # A title of "Self-reported life satisfaction — AFG" is searchable by nobody who does not
    # already know the code, and inventing names from memory is exactly the thing not to do.
    try:
        import pycountry
        def cname(iso):
            c = pycountry.countries.get(alpha_3=iso)
            return c.name if c else None
    except ImportError:
        def cname(iso):
            return None
    unresolved = []

    out = []
    for key, start, end, n in rows_in:
        parts = key.split(":")
        indicator = parts[1] if len(parts) > 2 else key
        iso = parts[-1]
        who = cname(iso)
        if who is None:
            unresolved.append(iso)
            who = iso                                          # never invent a country name
        out.append((f"{SOURCE}:{key}", SOURCE,
                    f"{indicator} — {who}", "A", None, who, "Wellbeing",
                    LICENSE_ID, start, end, meta))

    print(f"rows to write: {len(out):,}")
    if unresolved:
        print(f"codes with no ISO 3166-1 alpha-3 entry, left as the raw code "
              f"({len(unresolved)}): {sorted(set(unresolved))}")
    for r in out[:3]:
        print(f"   {r[0]}")
        print(f"      {r[2]}   {r[8]}..{r[9]}")

    if not a.apply:
        print("\n(dry run — pass --apply to write)")
        return 0

    # NON-COMMERCIAL + ATTRIBUTION, per the recorded grant. reservable=1 is what makes the
    # series downloadable at all; commercial_ok=0 is not decoration, it is the condition the
    # permission was given under.
    con.execute(
        "INSERT OR REPLACE INTO license"
        "(license_id,name,reservable,commercial_ok,attribution_required,no_modify,url) "
        "VALUES(?,?,?,?,?,?,?)",
        (LICENSE_ID, "whr-granted (written permission, non-commercial, attribution)",
         1, 0, 1, 0, TERMS))
    con.execute(
        "INSERT OR REPLACE INTO source(source_id,name,homepage,license_id,attribution,terms_url)"
        " VALUES(?,?,?,?,?,?)",
        (SOURCE, "World Happiness Report", "https://www.worldhappiness.report/",
         LICENSE_ID,
         "Source: World Happiness Report (Wellbeing Research Centre, University of Oxford), "
         "powered by the Gallup World Poll. Redistributed under written permission; "
         "non-commercial use only.", TERMS))
    con.executemany(
        """INSERT OR REPLACE INTO series
           (series_id,source_id,title,frequency,unit,geography,category,license_id,
            start_date,end_date,metadata) VALUES(?,?,?,?,?,?,?,?,?,?,?)""", out)
    con.commit()
    n = con.execute("select count(*) from series where source_id=?", (SOURCE,)).fetchone()[0]
    print(f"\nlicence {LICENSE_ID} created (reservable=1, commercial_ok=0, attribution=1)")
    print(f"catalogue rows for {SOURCE}: {n:,}")
    try:
        con.execute("INSERT INTO series_fts(series_fts) VALUES('rebuild')")
        con.commit()
        print("series_fts rebuilt")
    except sqlite3.Error as e:
        print(f"series_fts rebuild skipped: {e}")
    print("\nNEXT: derive the CSVs, verify both directions, sync D1.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
