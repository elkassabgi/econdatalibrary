# -*- coding: utf-8 -*-
"""
Deterministic titler for source 'freedomhouse'.

Uses ONLY official Freedom House (Freedom in the World) provider labels, verbatim.

Official indicator labels (from freedomhouse.org research methodology and the
"Country and Territory Ratings and Statuses, FIW 1973-2025" dataset column headers):
  - political_rights -> "Political Rights"   (PR rating)
  - civil_liberties  -> "Civil Liberties"    (CL rating)
  - freedom_status   -> "Status"             (Free / Partly Free / Not Free)

Country/Territory entity names are taken verbatim from the catalog series_id,
which were ingested directly from the FH dataset's official Country/Territory
column (e.g. "Cote d'Ivoire", "Congo (Brazzaville)", "Germany, E.").

Title pattern: "<Official indicator label> - <Country/Territory>".

Any series whose indicator segment is NOT one of the three official mappings is
OMITTED (left untitled) per the VERBATIM/OMIT rule.
"""
import sqlite3
import json
import os


# Repo root derived from this file, never a drive letter. The store moved D: -> E: in
# the workstation cutover; a stale root here silently writes into, or reports on, a
# tree that is not there. R330.
def _RD(*parts):
    _r = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(_r, *parts) if parts else _r

CATALOG = _RD('data', 'catalog.db')
OUT = _RD('dist', 'titles', 'freedomhouse.json')

# Official, verbatim Freedom House indicator labels.
INDICATOR_LABELS = {
    "political_rights": "Political Rights",
    "civil_liberties": "Civil Liberties",
    "freedom_status": "Status",
}


def main():
    conn = sqlite3.connect(CATALOG)
    rows = [r[0] for r in conn.execute(
        "SELECT series_id FROM series WHERE source_id='freedomhouse' ORDER BY series_id"
    )]
    conn.close()

    titles = {}
    omitted = []
    for sid in rows:
        parts = sid.split(":", 2)
        # Expect "freedomhouse:<indicator>:<Country/Territory>"
        if len(parts) != 3 or parts[0] != "freedomhouse":
            omitted.append(sid)
            continue
        indicator, entity = parts[1], parts[2]
        label = INDICATOR_LABELS.get(indicator)
        if label is None or not entity:
            omitted.append(sid)
            continue
        titles[sid] = f"{label} - {entity}"

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(titles, f, ensure_ascii=False, indent=1, sort_keys=True)

    print(f"total_series={len(rows)} titled={len(titles)} omitted={len(omitted)}")
    if omitted:
        print("OMITTED:", omitted[:20])


if __name__ == "__main__":
    main()
