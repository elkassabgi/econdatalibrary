#!/usr/bin/env python3
"""Compose dist/titles/boc.json from the Bank of Canada's OWN Valet series list.

739 boc catalogue rows carry a bare Valet code for a title ('A.BCPI', 'BA.CDN.30D.MID',
'ACTUAL'), so none is findable by name.

ONE REQUEST, NOT 739. https://www.bankofcanada.ca/valet/lists/series/json returns every
series the Valet API publishes - 15,922 of them - each with a `description`. The per-series
endpoint (/valet/series/<code>/json) returns the same string under `seriesDetails`
(PLURAL - `seriesDetail` singular is absent and reads as None, which is how a first pass
concluded the labels were missing).

Descriptions are taken verbatim. A code the Valet list does not describe is left untitled
rather than given an invented name.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import sys

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LIST_URL = "https://www.bankofcanada.ca/valet/lists/series/json"
CODED = re.compile(r"^[0-9A-Z_.\-]+$")


def main() -> int:
    write = "--write" in sys.argv
    con = sqlite3.connect("file:%s?mode=ro" % os.path.join(ROOT, "data", "catalog.db").replace("\\", "/"),
                          uri=True, timeout=180)
    try:
        rows = con.execute("SELECT series_id, title FROM series WHERE source_id='boc'").fetchall()
    finally:
        con.close()
    coded = [sid for sid, t in rows
             if str(t) == sid.split(":", 1)[1] or (CODED.match(str(t) or "") and " " not in str(t or ""))]
    print("boc catalogue rows: %s ; coded: %s" % (format(len(rows), ","), format(len(coded), ",")), flush=True)

    r = requests.get(LIST_URL, timeout=300, headers={"User-Agent": "econdatalibrary/1.0"})
    r.raise_for_status()
    series = (r.json() or {}).get("series") or {}
    print("valet publishes %s series" % format(len(series), ","), flush=True)

    titles, missing = {}, 0
    for sid in coded:
        code = sid.split(":", 1)[1]
        d = str((series.get(code) or {}).get("description") or "").strip()
        # A DESCRIPTION IS NOT AUTOMATICALLY A TITLE. Valet answers '-' for ECUCAA01 and a
        # bare 'GDP' for every MPR_<vintage>_AARGG_CAN_ series - 42 of the 543 matches.
        # Replacing a code with '-' is worse than leaving the code, and one 'GDP' shared by
        # a dozen distinct Monetary Policy Report vintages is actively misleading. Require
        # something that reads as a phrase: 8+ characters and not itself code-shaped.
        if len(d) >= 8 and not (CODED.match(d) and " " not in d):
            titles[sid] = d
        else:
            missing += 1
    print("titled %s of %s coded rows (%s not described by Valet)"
          % (format(len(titles), ","), format(len(coded), ","), format(missing, ",")), flush=True)
    if write and titles:
        p = os.path.join(ROOT, "dist", "titles", "boc.json")
        json.dump(titles, open(p, "w", encoding="utf-8"), ensure_ascii=False, indent=0, sort_keys=True)
        print("wrote %s" % p, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
