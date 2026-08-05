"""Catalogue imf_dip_direct at TABLE grain: one row per COUNTRY x FREQUENCY x INDICATOR.

WHY TABLE GRAIN (cycle 4, decided by the #45 D1 arithmetic 2026-08-05): the store holds
8,548,096 obs; the measured table count is 5,180 — ~1.1% of the remaining D1 budget, the
imf_imts_direct/imf_pip_direct/census/usda/statcan precedent. Series grain would burn the
budget for no serving gain: every series is reachable inside its table's CSV.

KEY SHAPE (5 parts, dims alphabetical — measured at the proof run, not assumed):
    DIP:<COUNTERPART_COUNTRY>.<COUNTRY>.<DV_TYPE>.<FREQUENCY>.<INDICATOR>
ALL keys are exactly 5 parts (part-count histogram {5: 8548096} — no METHODOLOGY tail,
unlike IMTS). Table dims sit MID-KEY: COUNTRY at position 2, FREQUENCY at 4, INDICATOR
at 5, with the counterpart economy FIRST. Catalog id: imf_dip_direct:DIP:<COUNTRY>.<FREQ>.<IND>
(3 key parts — cannot collide with a 5-part store key).

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

SOURCE = "imf_dip_direct"
FLOW, AGENCY = "DIP", "IMF.STA"
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
        SELECT split_part(series_key, '.', 2) AS country,
               split_part(series_key, '.', 4) AS freq,
               split_part(series_key, '.', 5) AS ind,
               COUNT(DISTINCT series_key)     AS n_series,
               MIN(obs_date)                  AS d0,
               MAX(obs_date)                  AS d1,
               COUNT(*)                       AS n_obs
        FROM read_parquet('{store.replace(chr(92), '/')}')
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
    """).fetchall()
    print(f"tables measured: {len(rows_db):,} (expect 5,180)")
    tot_obs = sum(r[6] for r in rows_db)
    tot_series = sum(r[3] for r in rows_db)
    print(f"universe check: {tot_obs:,} obs (expect 8,548,096), "
          f"{tot_series:,} distinct series (measured here — record in the runbook)")

    meta_base = {
        "citation_short": "International Monetary Fund (IMF).",
        "citation_long": ("International Monetary Fund — Direct Investment Positions by "
                          "Counterpart Economy (DIP, formerly the Coordinated Direct "
                          "Investment Survey, CDIS). Retrieved directly from the IMF SDMX "
                          "API (api.imf.org). Compiled and redistributed by the Elkassabgi "
                          "Data Library."),
        "description_processing": ("TABLE-grain listing: one catalogue entry per country x "
                                   "frequency x indicator; the CSV carries every series of "
                                   "the table (all counterpart economies and derived-value "
                                   "types) in long form, series_id column = the native "
                                   "5-part DIP key."),
        "dataset_version": "DIP:12.0.1",
        "grain": "table:COUNTRY.FREQ.INDICATOR",
    }

    out, unnamed = [], 0
    for country, freq, ind, n_series, d0, d1, n_obs in rows_db:
        cn = cnames.get(country)
        iname = inames.get(ind)
        if not cn or not iname:
            unnamed += 1
            cn, iname = cn or country, iname or ind
        title = (f"Direct investment positions — {cn} ({country}) — {iname} — "
                 f"{FREQ_LABEL.get(freq, freq)}, by counterpart economy and DV type "
                 f"({n_series} series) — DIP (formerly CDIS)")
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
        (SOURCE, "International Monetary Fund — Direct Investment Positions by "
                 "Counterpart Economy (DIP, formerly CDIS; direct from api.imf.org)",
         "https://www.imf.org/en/data", LIC,
         "Source: International Monetary Fund, Direct Investment Positions by Counterpart "
         "Economy (DIP). Retrieved directly from the IMF SDMX API (api.imf.org).", TERMS))
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
