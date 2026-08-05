"""Catalogue imf_pip_direct at TABLE grain: one row per COUNTRY x FREQUENCY x INDICATOR.

WHY TABLE GRAIN (task #105, decided by arithmetic 2026-08-05): the store holds 42,925,901 obs
across 3,126,127 series — series grain would be ~6-7x the library's ENTIRE remaining D1 budget
(9.31 GB of 10 measured). The 8,876 tables cost ~1.9% and serve every series inside its
table's CSV, the imf_imts_direct/census/usda/statcan precedent.

KEY SHAPE (7 parts, dims alphabetical — measured, not assumed):
    PIP:<ACCOUNTING_ENTRY>.<COUNTERPART_COUNTRY>.<COUNTERPART_SECTOR>.<COUNTRY>.<FREQ>.<INDICATOR>.<SECTOR>
Table dims sit MID-KEY at positions 4-6. Catalog id: imf_pip_direct:PIP:<COUNTRY>.<FREQ>.<IND>
(3 key parts — cannot collide with a 7-part store key).

NOT the World Bank's `pip` — never abbreviate this source id.

LICENCE GATE FIRST (imf-terms). Titles from IMF's own codelists, loaded live.
--apply writes; default dry run prints and changes nothing.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools"))

SOURCE = "imf_pip_direct"
FLOW, AGENCY = "PIP", "IMF.STA"
LIC = "imf-terms"
TERMS = "https://www.imf.org/en/about/copyright-and-terms"
FREQ_LABEL = {"A": "annual", "S": "semiannual", "Q": "quarterly", "M": "monthly"}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    os.environ.setdefault("AQUEDUCT_BACKEND", "r2")
    import duckdb
    import imf_direct_titles as T
    from updater import config

    store = os.path.join(config.source_dir(SOURCE), f"{SOURCE}.parquet")
    if not os.path.exists(store):
        print(f"local mirror missing at {store} — pull from R2 first")
        return 1

    con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=120.0)
    con.execute("PRAGMA busy_timeout = 120000")

    lic = con.execute("select reservable, name from license where license_id=?",
                      (LIC,)).fetchone()
    if not lic or not lic[0]:
        print(f"licence {LIC!r} missing or not reservable — refusing to catalogue")
        return 1
    print(f"licence {LIC}: reservable=1  ok")

    _dims, codes = T.load_structure(FLOW, AGENCY)
    cnames = codes.get("COUNTRY", {})
    inames = codes.get("INDICATOR", {})
    print(f"codelists: {len(cnames)} countries, {len(inames)} indicators")

    q = duckdb.connect()
    rows_db = q.execute(f"""
        SELECT split_part(series_key, '.', 4) AS country,
               split_part(series_key, '.', 5) AS freq,
               split_part(series_key, '.', 6) AS ind,
               COUNT(DISTINCT series_key)     AS n_series,
               MIN(obs_date)                  AS d0,
               MAX(obs_date)                  AS d1,
               COUNT(*)                       AS n_obs
        FROM read_parquet('{store.replace(chr(92), '/')}')
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
    """).fetchall()
    print(f"tables measured: {len(rows_db):,}")
    tot_obs = sum(r[6] for r in rows_db)
    tot_series = sum(r[3] for r in rows_db)
    print(f"universe check: {tot_obs:,} obs (expect 42,925,901), "
          f"{tot_series:,} distinct series (expect 3,126,127)")

    meta_base = {
        "citation_short": "International Monetary Fund (IMF).",
        "citation_long": ("International Monetary Fund — Portfolio Investment Positions by "
                          "Counterpart Economy (PIP, formerly the Coordinated Portfolio "
                          "Investment Survey, CPIS). Retrieved directly from the IMF SDMX "
                          "API (api.imf.org). Compiled and redistributed by the Elkassabgi "
                          "Data Library."),
        "description_processing": ("TABLE-grain listing: one catalogue entry per country x "
                                   "frequency x indicator; the CSV carries every series of "
                                   "the table (all counterpart economies, counterpart "
                                   "sectors, holder sectors and accounting entries) in long "
                                   "form, series_id column = the native 7-part PIP key."),
        "dataset_version": "PIP:5.0.0",
        "grain": "table:COUNTRY.FREQ.INDICATOR",
    }

    out, unnamed = [], 0
    for country, freq, ind, n_series, d0, d1, n_obs in rows_db:
        cn = cnames.get(country)
        iname = inames.get(ind)
        if not cn or not iname:
            unnamed += 1
            cn, iname = cn or country, iname or ind
        title = (f"Portfolio investment positions — {cn} ({country}) — {iname} — "
                 f"{FREQ_LABEL.get(freq, freq)}, by counterpart economy and sector "
                 f"({n_series} series) — PIP (formerly CPIS)")
        md = dict(meta_base, n_table_series=n_series, n_observations=n_obs)
        out.append((f"{SOURCE}:{FLOW}:{country}.{freq}.{ind}", SOURCE, title,
                    freq, "US dollars", country, None, LIC,
                    d0.isoformat(), d1.isoformat(), json.dumps(md, ensure_ascii=False)))
    print(f"rows to write: {len(out):,}   lacking a codelist name: {unnamed}")
    for r in out[:2] + out[-1:]:
        print(f"   {r[0]}\n      {r[2][:130]}   {r[8]}..{r[9]}")

    if not a.apply:
        print("\n(dry run — pass --apply to write)")
        return 0

    con.execute(
        "INSERT OR REPLACE INTO source(source_id,name,homepage,license_id,attribution,"
        "terms_url) VALUES(?,?,?,?,?,?)",
        (SOURCE, "International Monetary Fund — Portfolio Investment Positions by "
                 "Counterpart Economy (PIP, formerly CPIS; direct from api.imf.org)",
         "https://www.imf.org/en/data", LIC,
         "Source: International Monetary Fund, Portfolio Investment Positions by Counterpart "
         "Economy (PIP). Retrieved directly from the IMF SDMX API (api.imf.org).", TERMS))
    con.executemany(
        """INSERT OR REPLACE INTO series
           (series_id,source_id,title,frequency,unit,geography,category,license_id,
            start_date,end_date,metadata) VALUES(?,?,?,?,?,?,?,?,?,?,?)""", out)
    con.commit()
    n = con.execute("select count(*) from series where source_id=?", (SOURCE,)).fetchone()[0]
    print(f"\nwritten: {n:,} catalogue rows for {SOURCE}")
    try:
        con.execute("INSERT INTO series_fts(series_fts) VALUES('rebuild')")
        con.commit()
        print("series_fts rebuilt")
    except sqlite3.Error as e:
        print(f"series_fts rebuild skipped: {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
