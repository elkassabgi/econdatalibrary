"""Catalogue cepii_baci's pair-grain series with real titles and the licence gate first.

One row per BACI:tv/tq:<EXP>:<IMP> series in the pairs projection, titled from the publisher's
OWN country names (country_codes_*.csv inside the vintage zip), with per-series start/end
computed from the projection. The product dimension is aggregated away and every title says so
— a derived total presented as native grain would misrepresent the data.

LICENCE GATE BEFORE ANYTHING (etalab-2.0): the licence requires "the date of the last update of
the data is mentioned when known". The metadata records the dataset version V-string CEPII
itself publishes (V202601 = 2026-01) — a publisher-stated marker, month precision, with no
fabricated day (the same reasoning cepii_gravity's gate closure used to REFUSE inventing
"2022-11-01" from V202211). --lastmod upgrades it to the exact file Last-Modified when CEPII is
reachable to observe one.

--apply writes; without it, a dry run prints what would happen and changes nothing.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sqlite3
import sys
import zipfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

SOURCE = "cepii_baci"
LIC = "etalab-2.0"
MEASURE = {"tv": ("Total trade value", "1000 current USD"),
           "tq": ("Total trade quantity", "metric tons")}


def country_names(zip_path: str) -> dict[str, str]:
    """ISO3 -> display name, from the vintage's own country_codes csv."""
    with zipfile.ZipFile(zip_path) as z:
        name = [n for n in z.namelist() if "country_codes" in n.lower()][0]
        with z.open(name) as f:
            rows = list(csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")))
    return {r["country_iso3"].strip(): r["country_name"].strip()
            for r in rows if r.get("country_iso3")}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--vintage", required=True, help="publisher V-string, e.g. V202601")
    ap.add_argument("--lastmod", default=None,
                    help="observed Last-Modified of the vintage file (upgrades the month-"
                         "precision version date; never invent one)")
    ap.add_argument("--zip", default=os.path.join(ROOT, "data", "raw", "cepii_baci",
                                                  "HS96.zip"))
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    os.environ.setdefault("AQUEDUCT_BACKEND", "r2")
    from updater import blob, config
    from updater.strategies.fetchers.cepii_baci import PAIRS_BASENAME

    con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=120.0)
    con.execute("PRAGMA busy_timeout = 120000")

    # --- licence gate FIRST -----------------------------------------------------------
    lic = con.execute("select reservable, name from license where license_id=?",
                      (LIC,)).fetchone()
    if not lic:
        print(f"licence {LIC!r} not in the catalogue — refusing to create rows")
        return 1
    if not lic[0]:
        print(f"licence {LIC!r} is NOT reservable ({lic[1]}) — a catalogue row is an offer "
              f"to serve; refusing")
        return 1
    print(f"licence {LIC}: reservable=1  ok")

    ver = a.vintage
    upd = a.lastmod or f"{ver[1:5]}-{ver[5:7]} (publisher version string {ver}; " \
                       f"month precision — no observed file date yet)"
    print(f"last-update statement: {upd}")

    names = country_names(a.zip)
    print(f"country names: {len(names)}")

    # --- per-series coverage from the projection --------------------------------------
    store = os.path.join(config.source_dir(SOURCE), PAIRS_BASENAME)
    tbl = blob.read_table(store, columns=["series_key", "obs_date"])
    print(f"projection rows: {tbl.num_rows:,}")
    span: dict[str, list] = {}
    for k, d in zip(tbl.column("series_key").to_pylist(),
                    tbl.column("obs_date").to_pylist()):
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

    meta = json.dumps({
        "citation_short": "CEPII, BACI database.",
        "citation_long": (f"CEPII — BACI: International Trade Database at the Product-Level, "
                          f"{ver}. Aggregated to country-pair totals (HS6 product dimension "
                          f"summed) by the Elkassabgi Data Library. Licence Ouverte / Open "
                          f"Licence 2.0 (Etalab). Last update: {upd}."),
        "description_processing": ("Country-pair totals derived from BACI HS96 by summing "
                                   "value (1000 current USD) and quantity (metric tons) over "
                                   "all HS6 products per exporter-importer-year. NOT BACI's "
                                   "native product-level grain."),
        "dataset_version": ver,
        "last_update": upd,
    }, ensure_ascii=False)

    rows, unnamed = [], 0
    for key, (start, end) in sorted(span.items()):
        # key = BACI:tv:EXP:IMP
        _, m, exp, imp = key.split(":")
        mname, unit = MEASURE[m]
        en = names.get(exp)
        iname = names.get(imp)
        if not en or not iname:
            unnamed += 1
            en, iname = en or exp, iname or imp
        title = (f"{mname} — {en} ({exp}) → {iname} ({imp}) — annual, {unit}, "
                 f"all HS6 products aggregated — BACI HS96 {ver}")
        rows.append((f"{SOURCE}:{key}", SOURCE, title, "A", unit, f"{exp}→{imp}", None,
                     LIC, start, end, meta))
    print(f"rows to write: {len(rows):,}   pairs lacking a publisher name: {unnamed}")
    for r in rows[:2] + rows[-1:]:
        print(f"   {r[0]}\n      {r[2][:120]}   {r[8]}..{r[9]}")

    if not a.apply:
        print("\n(dry run — pass --apply to write)")
        return 0

    con.execute(
        "INSERT OR REPLACE INTO source(source_id,name,homepage,license_id,attribution,"
        "terms_url) VALUES(?,?,?,?,?,?)",
        (SOURCE, "CEPII — BACI: International Trade Database (country-pair totals)",
         "https://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele_item.asp?id=37", LIC,
         f"Source: CEPII, BACI database {ver}, Licence Ouverte / Open Licence 2.0 (Etalab). "
         f"Last update: {upd}.",
         "https://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele_item.asp?id=37"))
    con.executemany(
        """INSERT OR REPLACE INTO series
           (series_id,source_id,title,frequency,unit,geography,category,license_id,
            start_date,end_date,metadata) VALUES(?,?,?,?,?,?,?,?,?,?,?)""", rows)
    con.commit()
    n = con.execute("select count(*) from series where source_id=?", (SOURCE,)).fetchone()[0]
    print(f"\nwritten: {n:,} catalogue rows for {SOURCE}")
    try:
        con.execute("INSERT INTO series_fts(series_fts) VALUES('rebuild')")
        con.commit()
        print("series_fts rebuilt")
    except sqlite3.Error as e:
        print(f"series_fts rebuild skipped: {e}")
    print("\nNEXT (per the skill's serving pipeline): derive CSVs (tools/derive_csv_bulk.py "
          "--source cepii_baci --verify 300), verify_source_served, refresh_r2_catalog, "
          "sync_catalog_d1 --source cepii_baci, util.ts, wrangler deploy, live /v1/sources.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
