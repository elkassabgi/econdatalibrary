"""Catalogue imf_imts_direct at TABLE grain: one row per COUNTRY x FREQUENCY x INDICATOR.

WHY TABLE GRAIN (decided by arithmetic, task #104, 2026-08-05): the store holds 71,668,759
rows / 472,234 distinct series, and D1 measures 9.31 GB of its ~10 GB ceiling — series grain
would consume the library's ENTIRE remaining catalogue budget on one source. Table grain costs
2,937 rows (<1%) and serves every one of the 472,234 series inside its table's CSV, following
the census/usda/statcan/PxWeb flow-grain precedent.

Catalog id: imf_imts_direct:IMTS:<COUNTRY>.<FREQ>.<INDICATOR>  (3 key parts — cannot collide
with a store key, which always has 5). Each table's CSV carries its partner series in long
form (series_id column = the native 5-part store key), derived by tools/derive_imts_tables.py
and resolved by the bespoke two-condition predicate in econdl._resolve (prefix + suffix; the
counterpart dimension sits mid-key, so the PxWeb pure-prefix _FLOW_GRAIN mechanism does not
fit).

LICENCE GATE FIRST (imf-terms), same as every imf_* cataloguer. Titles come from IMF's own
codelists (COUNTRY and INDICATOR names, FREQUENCY labels), loaded live — never guessed.

--apply writes; the default dry run prints what would happen and changes nothing.
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

SOURCE = "imf_imts_direct"
FLOW, AGENCY = "IMTS", "IMF.STA"
LIC = "imf-terms"
TERMS = "https://www.imf.org/en/about/copyright-and-terms"

FREQ_LABEL = {"A": "annual", "M": "monthly", "Q": "quarterly"}


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
        print(f"local mirror missing at {store} — pull it from R2 first "
              f"(blob.read_bytes); this tool reads the SAME bytes the store serves")
        return 1

    con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=120.0)
    con.execute("PRAGMA busy_timeout = 120000")

    # --- licence gate BEFORE anything -------------------------------------------------
    lic = con.execute("select reservable, name from license where license_id=?",
                      (LIC,)).fetchone()
    if not lic or not lic[0]:
        print(f"licence {LIC!r} missing or not reservable — refusing to catalogue")
        return 1
    print(f"licence {LIC}: reservable=1  ok")

    # --- codelists for titles ----------------------------------------------------------
    _dims, codes = T.load_structure(FLOW, AGENCY)
    cnames = codes.get("COUNTRY", {})
    inames = codes.get("INDICATOR", {})
    print(f"codelists: {len(cnames)} countries, {len(inames)} indicators")

    # --- one aggregate pass over the store ----------------------------------------------
    q = duckdb.connect()
    rows_db = q.execute(f"""
        SELECT split_part(series_key, '.', 1) AS c0,        -- 'IMTS:<COUNTRY>'
               split_part(series_key, '.', 3) AS freq,
               split_part(series_key, '.', 4) AS ind,
               COUNT(DISTINCT series_key)     AS n_series,
               MIN(obs_date)                  AS d0,
               MAX(obs_date)                  AS d1,
               COUNT(*)                       AS n_obs
        FROM read_parquet('{store.replace(chr(92), '/')}')
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
    """).fetchall()
    print(f"tables measured: {len(rows_db):,}")
    total_series = sum(r[3] for r in rows_db)
    print(f"series covered: {total_series:,}  (universe check: must be 472,234 for the "
          f"2026-08-05 vintage; a different vintage may legitimately differ)")

    meta_base = {
        "citation_short": "International Monetary Fund (IMF).",
        "citation_long": ("International Monetary Fund — International Trade in Goods by "
                          "partner country (IMTS, formerly Direction of Trade Statistics). "
                          "Retrieved directly from the IMF SDMX API (api.imf.org). Compiled "
                          "and redistributed by the Elkassabgi Data Library."),
        "description_processing": ("TABLE-grain listing: one catalogue entry per country x "
                                   "frequency x indicator; the CSV carries every partner-"
                                   "country series of the table in long form (series_id "
                                   "column = the native IMTS key)."),
        "dataset_version": "IMTS:1.0.0",
        "grain": "table:COUNTRY.FREQ.INDICATOR",
    }

    out = []
    unnamed = 0
    for c0, freq, ind, n_series, d0, d1, n_obs in rows_db:
        country = c0.split(":", 1)[1]
        cn = cnames.get(country)
        iname = inames.get(ind)
        if not cn or not iname:
            unnamed += 1
            cn, iname = cn or country, iname or ind
        title = (f"International trade in goods — {cn} ({country}) — {iname} — "
                 f"{FREQ_LABEL.get(freq, freq)}, by partner country ({n_series} partner "
                 f"series) — IMTS (formerly DOTS)")
        md = dict(meta_base, n_partner_series=n_series, n_observations=n_obs)
        out.append((f"{SOURCE}:{FLOW}:{country}.{freq}.{ind}", SOURCE, title,
                    freq, "US dollars", country, None, LIC,
                    d0.isoformat(), d1.isoformat(), json.dumps(md, ensure_ascii=False)))
    print(f"rows to write: {len(out):,}   tables lacking a codelist name: {unnamed}")
    for r in out[:2] + out[-1:]:
        print(f"   {r[0]}\n      {r[2][:130]}   {r[8]}..{r[9]}")

    if not a.apply:
        print("\n(dry run — pass --apply to write)")
        return 0

    con.execute(
        "INSERT OR REPLACE INTO source(source_id,name,homepage,license_id,attribution,"
        "terms_url) VALUES(?,?,?,?,?,?)",
        (SOURCE, "International Monetary Fund — International Trade in Goods by partner "
                 "country (IMTS, formerly DOTS; direct from api.imf.org)",
         "https://www.imf.org/en/data", LIC,
         "Source: International Monetary Fund, International Trade in Goods by partner "
         "country (IMTS). Retrieved directly from the IMF SDMX API (api.imf.org).", TERMS))
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
    print("\nNEXT (Checklist B): tools/derive_imts_tables.py --dry-run, then the standard "
          "pipeline: refresh_r2_catalog, sync_catalog_d1 --source imf_imts_direct, resolver "
          "entry + util.ts, typecheck, wrangler deploy, live /v1/sources, "
          "verify_source_served exit 0.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
