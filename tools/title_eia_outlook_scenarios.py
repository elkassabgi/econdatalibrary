#!/usr/bin/env python3
"""Title the eia AEO/IEO rows that are SCENARIOS, not series.

EIA's bulk archive has no AEO.zip, so tools/title_eia_from_bulk.py cannot reach these and
they keep a bare code for a title: 'AEO.2014.ALTLOWNUC14', 'IEO.2017.HIGHMACRO'.

They are not series ids. The store holds AEO.<year>.<SCENARIO>.<SERIES> (357,775 distinct
keys in AEO.2014 alone); the catalogue lists the <year>.<SCENARIO> prefix - a projection
CASE, one grain up. EIA names those cases on the v2 API's scenario facet, which is keyed
LOWERCASE while our ids are upper:

    GET https://api.eia.gov/v2/{aeo|ieo}/{year}/facet/scenario

so 'altlownuc14' -> 'Low nuclear' and 'co2fee10' -> 'Greenhouse gas $10'. Matched
case-insensitively; a scenario EIA does not name is left untitled.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import sys
import time

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CODED = re.compile(r"^[0-9A-Z_.\-]+$")
OUTLOOK = {"AEO": "aeo", "IEO": "ieo"}


def _key() -> str:
    for line in open(os.path.join(ROOT, ".env"), encoding="utf-8"):
        if line.startswith("EIA_API_KEY="):
            return line.split("=", 1)[1].strip().strip('"')
    raise SystemExit("EIA_API_KEY not in .env")


def main() -> int:
    write = "--write" in sys.argv
    key = _key()
    con = sqlite3.connect("file:%s?mode=ro" % os.path.join(ROOT, "data", "catalog.db").replace("\\", "/"),
                          uri=True, timeout=180)
    try:
        rows = con.execute("SELECT series_id, title FROM series WHERE source_id='eia'").fetchall()
    finally:
        con.close()
    targets = []
    for sid, t in rows:
        body = sid.split(":", 1)[1]
        if not (str(t) == body or (CODED.match(str(t) or "") and " " not in str(t or ""))):
            continue
        parts = body.split(".")
        if len(parts) == 3 and parts[0] in OUTLOOK and parts[1].isdigit():
            targets.append((sid, parts[0], parts[1], parts[2]))
    print("eia outlook scenario rows needing a title: %s" % format(len(targets), ","), flush=True)

    cache: dict = {}
    titles, unnamed = {}, 0
    for sid, fam, year, scen in targets:
        ck = (fam, year)
        if ck not in cache:
            url = "https://api.eia.gov/v2/%s/%s/facet/scenario" % (OUTLOOK[fam], year)
            try:
                r = requests.get(url, params={"api_key": key}, timeout=90)
                facets = ((r.json() or {}).get("response") or {}).get("facets") or [] if r.status_code == 200 else []
            except Exception:                                    # noqa: BLE001
                facets = []
            cache[ck] = {str(f.get("id", "")).lower(): (f.get("name") or f.get("description"))
                         for f in facets}
            time.sleep(0.2)
        name = cache[ck].get(scen.lower())
        if name and len(str(name).strip()) > 2:
            label = "Annual Energy Outlook" if fam == "AEO" else "International Energy Outlook"
            titles[sid] = "%s %s - %s" % (label, year, str(name).strip())
        else:
            unnamed += 1
    print("titled %s of %s (%s scenarios EIA does not name)"
          % (format(len(titles), ","), format(len(targets), ","), format(unnamed, ",")), flush=True)
    for k in list(titles)[:3]:
        print("   e.g. %-28s %s" % (k.split(":", 1)[1][:28], titles[k]), flush=True)
    if write and titles:
        p = os.path.join(ROOT, "dist", "titles", "eia_outlook.json")
        json.dump(titles, open(p, "w", encoding="utf-8"), ensure_ascii=False, indent=0, sort_keys=True)
        print("wrote %s" % p, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
