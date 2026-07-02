# -*- coding: utf-8 -*-
"""
Deterministic titler for source `ei_statreview`.

Data provenance: ingested from OWID energy-data (NOT the Energy Institute
Statistical Review workbook directly). Titles are therefore built from OWID's
official, machine-readable codebook.

Catalog keys: ei_statreview:EISR:<owid_var>:<ISO3>

OWID codebook columns (verbatim from the file): column, title, description, unit, source
  - `title`       : the official human-readable subject label for the variable
                    (e.g. "Primary energy consumption from biofuels")
  - `description` : almost always a measurement/methodology note
                    (e.g. "Measured in terawatt-hours.") and is EMPTY for 12 vars
  - `unit`        : measurement unit (e.g. "terawatt-hours (TWh)", "%")

We title each series as:
    "<title> (<unit>) - <country>"   when unit is non-empty
    "<title> - <country>"            when unit is empty

Rationale (honest official labelling):
  The task brief assumed the codebook `description` carried the subject label.
  In the actual OWID codebook, `description` is a measurement note that is
  identical across many variables (e.g. 24 vars all say "Measured in
  terawatt-hours.") and is empty for 12 variables. Using it would make distinct
  series indistinguishable and, for 12 vars, produce no label at all. The field
  that carries the genuine official subject label, verbatim, for ALL 127 vars is
  `title`. The verbatim `unit` is appended to disambiguate the 4 title pairs that
  otherwise collide (the _change_pct vs _change_twh variants). Everything emitted
  is verbatim from the OWID codebook; nothing is paraphrased or invented.

Country names: ISO3 -> country resolved from OWID's own owid-energy-data.csv
(`iso_code` -> `country`), i.e. OWID's official naming, not a third-party list.
"""

import csv
import io
import json
import os
import sqlite3
import sys
import urllib.request

CATALOG_DB = r"D:\research\econfindatalibrary\data\catalog.db"
OUT_PATH = r"D:\research\econfindatalibrary\dist\titles\ei_statreview.json"
SOURCE_ID = "ei_statreview"

CODEBOOK_URL = "https://raw.githubusercontent.com/owid/energy-data/master/owid-energy-codebook.csv"
ENERGYDATA_URL = "https://raw.githubusercontent.com/owid/energy-data/master/owid-energy-data.csv"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=180) as r:
        return r.read().decode("utf-8")


def load_codebook(text):
    cb = {}
    for row in csv.DictReader(io.StringIO(text)):
        cb[row["column"]] = {
            "title": (row.get("title") or "").strip(),
            "description": (row.get("description") or "").strip(),
            "unit": (row.get("unit") or "").strip(),
        }
    return cb


def load_iso2country(text):
    iso2c = {}
    for row in csv.DictReader(io.StringIO(text)):
        iso = (row.get("iso_code") or "").strip()
        country = (row.get("country") or "").strip()
        if iso and country and iso not in iso2c:
            iso2c[iso] = country
    return iso2c


def main():
    codebook = load_codebook(fetch(CODEBOOK_URL))
    iso2country = load_iso2country(fetch(ENERGYDATA_URL))

    con = sqlite3.connect(CATALOG_DB)
    series_ids = [r[0] for r in con.execute(
        "SELECT series_id FROM series WHERE source_id=? ORDER BY series_id",
        (SOURCE_ID,)).fetchall()]
    con.close()

    titles = {}
    unresolved = []
    for sid in series_ids:
        parts = sid.split(":")
        if len(parts) != 4:
            unresolved.append((sid, "malformed_key"))
            continue
        _, _prefix, owid_var, iso3 = parts
        cb = codebook.get(owid_var)
        country = iso2country.get(iso3)
        if cb is None:
            unresolved.append((sid, "var_not_in_codebook"))
            continue
        if not country:
            unresolved.append((sid, "iso_not_in_owid"))
            continue
        label = cb["title"]
        if not label:
            unresolved.append((sid, "empty_title"))
            continue
        unit = cb["unit"]
        if unit:
            titles[sid] = f"{label} ({unit}) - {country}"
        else:
            titles[sid] = f"{label} - {country}"

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(titles, f, ensure_ascii=False, indent=2, sort_keys=True)

    print(f"total series:   {len(series_ids)}")
    print(f"titled:         {len(titles)}")
    print(f"unresolved:     {len(unresolved)}")
    for sid, why in unresolved[:20]:
        print(f"  {why}: {sid}")
    print(f"written to:     {OUT_PATH}")


if __name__ == "__main__":
    sys.exit(main())
