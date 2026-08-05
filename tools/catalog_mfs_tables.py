"""Catalogue the imf_mfs*_direct family at TABLE grain: one row per COUNTRY x FREQUENCY.

WHY THIS GRAIN (cycle 5, decided by the #45 D1 arithmetic 2026-08-05): MFS_DC measured
36,506 distinct series against 35,949 COUNTRY x FREQ x INDICATOR combos — the C.F.I grain
of IMTS/PIP/DIP saves NOTHING here (TYPE_OF_TRANSFORMATION averages 1.02 values per combo).
COUNTRY x FREQ cuts the same universe into 539 tables (largest ~23k obs ≈ 1 MB CSV), i.e.
~0.1% of the remaining D1 headroom instead of ~8% per flow, with eia (#37) and bea (#65)
still queued against that headroom. The CSV carries every indicator and transformation of
the country-frequency pair; per-indicator discovery lives inside the file, not the catalogue.

KEY SHAPE (measured per flow, never assumed — position-to-dim mapping proven by testing
every sampled position value against the flow's own codelists):
    MFS_DC: <COUNTRY>.<FREQ>.true.<INDICATOR>.<TYPE_OF_TRANSFORMATION>  (5 parts; position
    3 is a literal 'true' across all 4,494,366 rows — an attribute the data carries and the
    DSD omits, the exact class imf_direct_titles.py warns about)
Catalog id: imf_mfsdc_direct:MFS_DC:<COUNTRY>.<FREQ> (2 key parts — cannot collide with a
5-part store key). Table dims are the key PREFIX, so the serving resolver is a plain
starts_with — no mid-key regex needed for this family.

Flows are opted in via FLOWS below ONLY after their store is measured (part histogram,
position fit, universe). LICENCE GATE FIRST (imf-terms). Titles from IMF's own codelists.
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

LIC = "imf-terms"
TERMS = "https://www.imf.org/en/about/copyright-and-terms"
FREQ_LABEL = {"A": "annual", "S": "semiannual", "Q": "quarterly", "M": "monthly"}

# Per-flow config, added ONLY once the store is measured at its proof run.
FLOWS = {
    "imf_mfsdc_direct": {
        "flow": "MFS_DC", "version": "MFS_DC:8.0.0",
        "sub": "depository corporations survey",
        "expect_obs": 4_494_366, "expect_series": 36_506, "expect_tables": 539,
    },
    # MFS_MA measured 2026-08-05: same 5-part shape (phantom 'true' at position 3 across
    # all 344,652 rows), positions 1/2 vocabulary-proven COUNTRY/FREQUENCY, tail
    # INDICATOR.UNIT.
    "imf_mfsma_direct": {
        "flow": "MFS_MA", "version": "MFS_MA:10.0.1",
        "sub": "monetary aggregates",
        "expect_obs": 344_652, "expect_series": 3_016, "expect_tables": 468,
    },
    # MFS_OFC measured 2026-08-05: same 5-part shape (phantom 'true' at position 3 across
    # all 348,519 rows), positions 1/2 vocabulary-proven COUNTRY/FREQUENCY, tail
    # INDICATOR.TYPE_OF_TRANSFORMATION; 76 reporting countries.
    "imf_mfsofc_direct": {
        "flow": "MFS_OFC", "version": "MFS_OFC:7.0.0",
        "sub": "other financial corporations survey",
        "expect_obs": 348_519, "expect_series": 4_704, "expect_tables": 231,
    },
    # MFS_FMP measured 2026-08-05: same 5-part shape (phantom 'true' at position 3 across
    # all 55,623 rows), positions 1/2 vocabulary-proven COUNTRY/FREQUENCY; 69 countries.
    "imf_mfsfmp_direct": {
        "flow": "MFS_FMP", "version": "MFS_FMP:3.0.0",
        "sub": "financial markets and positions",
        "expect_obs": 55_623, "expect_series": 276, "expect_tables": 207,
    },
    # MFS_IR measured 2026-08-05: 4-PART keys — the flow has only 3 dims (COUNTRY,
    # FREQUENCY, INDICATOR) plus the family's phantom 'true' at position 3 (all 537,710
    # rows); no tail dim. Positions 1/2 vocabulary-proven, so the C.F cut is unchanged.
    "imf_mfsir_direct": {
        "flow": "MFS_IR", "version": "MFS_IR:9.0.0",
        "sub": "interest rates",
        "expect_obs": 537_710, "expect_series": 3_382, "expect_tables": 510,
    },
    # BOP_AGG measured 2026-08-05 (cycle 6): 6-PART keys — COUNTRY.FREQ.<phantom true OR
    # EMPTY>.INDICATOR.BPM6.<TYPE_OF_TRANSFORMATION> (methodology constant at 5; phantom
    # attribute sometimes absent -> empty part). Positions 1/2 vocabulary-proven: 206 of
    # 208 position-1 codes in the COUNTRY codelist, the 2 misfits are GX010/GX205, the
    # DIP-class publisher aggregates. Annual only.
    "imf_bopagg_direct": {
        "flow": "BOP_AGG", "version": "BOP_AGG:9.0.1",
        "sub": "headline aggregates",
        "family": "BOP and IIP statistics",
        "family_long": ("BOP and IIP Statistics aggregates ({flow}, formerly BOPAGG)"),
        "expect_obs": 140_907, "expect_series": 7_839, "expect_tables": 208,
    },
    # PSBS measured 2026-08-05 (cycle 7): clean 5-part keys, NO phantom —
    # COUNTRY.FREQ.INDICATOR.SECTOR.UNIT, all five dims codelisted (position 2 is 'A'
    # only, ambiguous by vocabulary but fixed by the alphabetical dim order and by
    # position 3 fitting INDICATOR with 144 values). Distinct series = 14,018 =
    # EXACTLY the legacy imf_psbsfad count (the R75 same-dataset proof, now in the
    # store itself). Agency IMF.FAD.
    "imf_psbs_direct": {
        "flow": "PSBS", "version": "PSBS:2.0.0", "agency": "IMF.FAD",
        "sub": "stocks of assets and liabilities",
        "family": "Public sector balance sheet",
        "family_long": ("Public Sector Balance Sheet ({flow}, formerly PSBSFAD)"),
        "expect_obs": 209_229, "expect_series": 14_018, "expect_tables": 86,
    },
}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("source", choices=sorted(FLOWS))
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    cfg = FLOWS[a.source]
    flow = cfg["flow"]
    fam = cfg.get("family", "Monetary and financial statistics")
    fam_long = cfg.get("family_long",
                       "Monetary and Financial Statistics ({flow}), one of the five flows "
                       "the former MFS dataset was split into").format(flow=flow)

    os.environ.setdefault("AQUEDUCT_BACKEND", "r2")
    import duckdb
    import imf_direct_titles as T
    from updater import config

    store = os.path.join(config.source_dir(a.source), f"{a.source}.parquet")
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

    _dims, codes = T.load_structure(flow, cfg.get("agency", "IMF.STA"))
    cnames = codes.get("COUNTRY", {})
    print(f"codelists: {len(cnames)} countries")

    q = duckdb.connect()
    sp = store.replace(chr(92), "/")
    rows_db = q.execute(f"""
        SELECT split_part(split_part(series_key, ':', 2), '.', 1) AS country,
               split_part(split_part(series_key, ':', 2), '.', 2) AS freq,
               COUNT(DISTINCT series_key)     AS n_series,
               MIN(obs_date)                  AS d0,
               MAX(obs_date)                  AS d1,
               COUNT(*)                       AS n_obs
        FROM read_parquet('{sp}')
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).fetchall()
    print(f"tables measured: {len(rows_db):,} (expect {cfg['expect_tables']:,})")
    tot_obs = sum(r[5] for r in rows_db)
    tot_series = sum(r[2] for r in rows_db)
    print(f"universe check: {tot_obs:,} obs (expect {cfg['expect_obs']:,}), "
          f"{tot_series:,} distinct series (expect {cfg['expect_series']:,})")
    if (len(rows_db), tot_obs, tot_series) != (cfg["expect_tables"], cfg["expect_obs"],
                                               cfg["expect_series"]):
        print("UNIVERSE DOES NOT CLOSE against the measured expectations — re-measure "
              "before cataloguing; the store may have moved since the proof run")
        return 1

    meta_base = {
        "citation_short": "International Monetary Fund (IMF).",
        "citation_long": (f"International Monetary Fund — {fam_long}. Retrieved "
                          "directly from the IMF SDMX API (api.imf.org). Compiled and "
                          "redistributed by the Elkassabgi Data Library."),
        "description_processing": ("TABLE-grain listing: one catalogue entry per country x "
                                   "frequency; the CSV carries every series of the pair "
                                   "(all indicators and transformations) in long form, "
                                   "series_id column = the native store key."),
        "dataset_version": cfg["version"],
        "grain": "table:COUNTRY.FREQ",
    }

    out, unnamed = [], 0
    for country, freq, n_series, d0, d1, n_obs in rows_db:
        cn = cnames.get(country)
        if not cn:
            unnamed += 1
            cn = country
        title = (f"{fam} — {cfg['sub']} — {cn} ({country}) — "
                 f"{FREQ_LABEL.get(freq, freq)} ({n_series} series) — {flow}")
        md = dict(meta_base, n_table_series=n_series, n_observations=n_obs)
        out.append((f"{a.source}:{flow}:{country}.{freq}", a.source, title,
                    freq, None, country, None, LIC,
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
        (a.source, f"International Monetary Fund — {fam}, "
                   f"{cfg['sub']} ({flow}; direct from api.imf.org)",
         "https://www.imf.org/en/data", LIC,
         f"Source: International Monetary Fund, {fam} ({flow})."
         " Retrieved directly from the IMF SDMX API (api.imf.org).", TERMS))
    con.executemany(
        """INSERT OR REPLACE INTO series
           (series_id,source_id,title,frequency,unit,geography,category,license_id,
            start_date,end_date,metadata) VALUES(?,?,?,?,?,?,?,?,?,?,?)""", out)
    con.commit()
    n = con.execute("select count(*) from series where source_id=?",
                    (a.source,)).fetchone()[0]
    print(f"\nwritten: {n:,} catalogue rows for {a.source}")
    try:
        con.execute("INSERT INTO series_fts(series_fts) VALUES('rebuild')")
        con.commit()
        print("series_fts rebuilt")
    except sqlite3.Error as e:
        print(f"series_fts rebuild skipped: {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
