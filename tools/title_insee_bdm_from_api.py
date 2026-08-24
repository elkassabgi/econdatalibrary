#!/usr/bin/env python3
"""Compose dist/titles/insee_bdm.json from INSEE's OWN BDM series attributes.

1,085 insee_bdm catalogue rows carry a bare IDBANK for a title ('011793884'), so none is
findable by name. INSEE's BDM API is keyless and returns the series' own TITLE_EN /
TITLE_FR attributes on the <Series> element:

    GET https://api.insee.fr/series/BDM/V1/data/SERIES_BDM/<id>+<id>+...

ENGLISH ONLY, DELIBERATELY. Many series carry TITLE_EN="." - a placeholder, not a title -
while TITLE_FR has real content. Measured over 200 of the coded ids: 139 have a substantive
TITLE_EN, 59 have neither, and only 2 have French alone. Writing French into the default
(English) title field to gain those 2 would mix languages in one column for a rounding
error's worth of coverage, so a series without a real TITLE_EN is left untitled. The i18n
surface is where translations belong.

Batched: the endpoint accepts '+'-joined ids; 40 per request is proven (40 ids -> 38
Series elements, the shortfall being ids INSEE returns nothing for).
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
URL = "https://api.insee.fr/series/BDM/V1/data/SERIES_BDM/%s"
CODED = re.compile(r"^[0-9A-Z_.\-]+$")
BATCH = 40


def main() -> int:
    write = "--write" in sys.argv
    con = sqlite3.connect("file:%s?mode=ro" % os.path.join(ROOT, "data", "catalog.db").replace("\\", "/"),
                          uri=True, timeout=180)
    try:
        rows = con.execute("SELECT series_id, title FROM series WHERE source_id='insee_bdm'").fetchall()
    finally:
        con.close()
    coded = [sid for sid, t in rows
             if str(t) == sid.split(":", 1)[1] or (CODED.match(str(t) or "") and " " not in str(t or ""))]
    print("insee_bdm rows: %s ; coded: %s" % (format(len(rows), ","), format(len(coded), ",")), flush=True)

    by_id = {sid.split(":", 1)[1]: sid for sid in coded}
    ids = sorted(by_id)
    titles, placeholder, absent = {}, 0, 0
    seen = set()
    for i in range(0, len(ids), BATCH):
        batch = ids[i:i + BATCH]
        try:
            r = requests.get(URL % "+".join(batch), timeout=180,
                             headers={"User-Agent": "econdatalibrary/1.0"})
        except Exception as e:                                   # noqa: BLE001
            print("  batch %d failed: %s" % (i // BATCH, str(e)[:70]), flush=True)
            continue
        if r.status_code != 200:
            print("  batch %d HTTP %s" % (i // BATCH, r.status_code), flush=True)
            continue
        for el in re.findall(r"<Series\b[^>]*>", r.text):
            m = re.search(r'IDBANK="([^"]+)"', el)
            if not m or m.group(1) not in by_id:
                continue
            seen.add(m.group(1))
            te = re.search(r'TITLE_EN="([^"]*)"', el)
            en = (te.group(1).strip() if te else "")
            if len(en) > 3:
                titles[by_id[m.group(1)]] = en
            else:
                placeholder += 1
        if (i // BATCH) % 8 == 0:
            print("  %5d/%d ids  titled=%s" % (i + len(batch), len(ids), format(len(titles), ",")), flush=True)
        time.sleep(0.2)
    absent = len(ids) - len(seen)
    print("titled %s of %s coded rows (%s had only a '.' placeholder, %s returned no Series)"
          % (format(len(titles), ","), format(len(ids), ","), format(placeholder, ","), format(absent, ",")), flush=True)
    if write and titles:
        p = os.path.join(ROOT, "dist", "titles", "insee_bdm.json")
        json.dump(titles, open(p, "w", encoding="utf-8"), ensure_ascii=False, indent=0, sort_keys=True)
        print("wrote %s" % p, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
