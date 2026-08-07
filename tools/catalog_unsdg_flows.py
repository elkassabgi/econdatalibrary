"""Catalogue unsdg at FLOW grain — one row per SDG series code (~715), titled by the publisher.

The #45 arithmetic: the store holds ~353k distinct (code:geo|dims) keys — series grain would
eat most of the remaining D1 headroom, while the SDG database's own natural unit is the
SERIES CODE (713 listed). One catalog row per code = the ilostat/PxWeb flow-grain pattern;
the resolver serves a code's whole table via the _FLOW_GRAIN prefix rule.

Titles come from the SDG API's Series/List `description` field — the publisher's own words,
never fabricated. Licence: UNdata Terms of Use, CLEARED (DATABASE_LICENSES_VERBATIM.md:3106,
"may be copied freely, duplicated and further distributed provided that UNdata is cited as
the reference") — attribution required, no NC clause. Creates the `undata-terms` licence row
and the unsdg source row if absent.

  python tools/catalog_unsdg_flows.py            # dry run
  python tools/catalog_unsdg_flows.py --apply
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import urllib.request

import duckdb

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STORE = os.path.join(ROOT, "data", "clean_full", "unsdg", "unsdg.parquet")
LICENSE_ID = "undata-terms"
TERMS = "https://data.un.org/Host.aspx?Content=UNdataUse"
UA = {"User-Agent": "Econ-Fin Data Library admin@econdatalibrary.com"}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    if not os.path.exists(STORE):
        print(f"no store at {STORE}")
        return 1

    q = duckdb.connect()
    p = STORE.replace("\\", "/")
    rows_in = q.execute(f"""
        select split_part(series_key, ':', 1) code,
               min(obs_date)::VARCHAR, max(obs_date)::VARCHAR,
               count(*) n, count(distinct series_key) ks
        from read_parquet('{p}') group by 1 order by 1""").fetchall()
    print(f"{len(rows_in)} distinct series codes in the store "
          f"({sum(r[3] for r in rows_in):,} rows / {sum(r[4] for r in rows_in):,} keys)")

    # Publisher titles from Series/List (description field).
    d = json.loads(urllib.request.urlopen(
        urllib.request.Request("https://unstats.un.org/SDGAPI/v1/sdg/Series/List",
                               headers=UA), timeout=120).read())
    titles = {s.get("code"): (s.get("description") or "").strip() for s in d if s.get("code")}
    untitled = [c for c, *_ in rows_in if not titles.get(c)]
    print(f"publisher titles resolved for {len(rows_in) - len(untitled)}/{len(rows_in)}; "
          f"raw-code fallbacks: {len(untitled)}", untitled[:5])

    meta = json.dumps({
        "citation_short": "United Nations Statistics Division, SDG Indicators Database (UNdata).",
        "citation_long": ("United Nations Statistics Division, Sustainable Development Goal "
                          "Indicators Database, via the SDG API (unstats.un.org/SDGAPI). "
                          "Redistributed by the Elkassabgi Data Library under the UNdata "
                          "Terms of Use; UNdata is cited as the reference."),
        "description_processing": ("Retrieved per series code from the UN SDG API, normalized "
                                   "to a long {series_key, obs_date, value} schema (key = "
                                   "code:geoArea with all non-trivial dimensions) and stored "
                                   "as zstd Parquet. This id downloads the code's full table."),
    }, ensure_ascii=False)

    out = []
    for code, start, end, n, ks in rows_in:
        title = titles.get(code) or code           # honest fallback, never invented
        out.append((f"unsdg:{code}", "unsdg", title, "A", None, "World", "SDG",
                    LICENSE_ID, start, end, meta))
    for r in out[:3]:
        print("  ", r[0], "—", r[2][:70])

    if not a.apply:
        print("(dry run — pass --apply to write)")
        return 0

    con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=180)
    con.execute("PRAGMA busy_timeout=180000")
    con.execute(
        "INSERT OR REPLACE INTO license"
        "(license_id,name,reservable,commercial_ok,attribution_required,no_modify,url) "
        "VALUES(?,?,?,?,?,?,?)",
        (LICENSE_ID, "UNdata Terms of Use (attribution)", 1, 1, 1, 0, TERMS))
    con.execute(
        "INSERT OR REPLACE INTO source(source_id,name,homepage,license_id,attribution,terms_url)"
        " VALUES(?,?,?,?,?,?)",
        ("unsdg", "UN SDG Indicators Database", "https://unstats.un.org/sdgs/dataportal",
         LICENSE_ID, "Source: United Nations Statistics Division, SDG Indicators Database "
         "(UNdata cited as the reference).", TERMS))
    con.execute("DELETE FROM series WHERE source_id='unsdg'")
    con.executemany(
        """INSERT OR REPLACE INTO series
           (series_id,source_id,title,frequency,unit,geography,category,license_id,
            start_date,end_date,metadata) VALUES(?,?,?,?,?,?,?,?,?,?,?)""", out)
    con.commit()
    n = con.execute("select count(*) from series where source_id='unsdg'").fetchone()[0]
    print(f"catalogue rows for unsdg: {n:,}")
    try:
        con.execute("INSERT INTO series_fts(series_fts) VALUES('rebuild')")
        con.commit()
        print("series_fts rebuilt")
    except sqlite3.Error as e:
        print(f"series_fts rebuild skipped: {e}")
    print("NEXT: derive flow CSVs, _FLOW_GRAIN + util.ts, refresh, D1, un-gate, deploy, verify.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
