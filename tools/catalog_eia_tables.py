"""Catalogue eia at TABLE grain — one catalog id per dot-prefix table, per-dataset depth.

WHY. eia serves 7 hand-picked series while its store holds 3,888,368 (60 parquets, 168M rows);
series-grain cataloguing would eat 2.6x the ENTIRE remaining D1 headroom (#45), so the source
sat dark. Ahmed's 2026-08-07 D1 order: eia FIRST at table grain, then wid, bea deferred.

THE GRAIN IS PER-DATASET, MEASURED, NOT GUESSED (full store, _eia_grain_measure.log +
_eia_table_rows.log, 2026-08-08): a uniform depth is wrong — AEO vintages only group at
depth3 (their depth2 is one 3-14M-row blob), SEDS/COAL/STEO/TOTAL only at depth2 (their
depth3 is a flood of 6-65-row fragments), ELEC's depth2 has a 50M-row table. The map below
encodes the measured choice; every dataset not named here raises rather than guessing.

IEO.parquet IS A REDUNDANT UNION — measured identical (41,382 ids == the union of the four
IEO.<year>.parquet id sets) — so it is SKIPPED: cataloguing both representations would serve
every IEO series under two table ids. AEO.IEO2 overlaps neither (0 ids shared) and is kept.

Table id = 'eia:<prefix>' where <prefix> is the first `depth` dot-segments of the native
series_id. The resolver serves a table id with the predicate
    (series_id == prefix) | starts_with(series_id, prefix + '.')
which also keeps the 7 legacy series-grain ids resolving unchanged (an EIA id ends in a
frequency segment; nothing nests below a full id).

SAFE: INSERT OR IGNORE for eia only; never deletes, never rewrites existing rows. Licence is
copied from the eia source row (CLEARED redistributable_attribution) with the same
announce-the-terms print as tools/catalog_complete.py (R117).

    python tools/catalog_eia_tables.py --dry-run
    python tools/catalog_eia_tables.py --apply
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys

import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

CAT = os.path.join(ROOT, "data", "catalog.db")
STORE = os.path.join(ROOT, "data", "clean_full", "eia")

# dataset file (basename, no .parquet) -> prefix depth. MEASURED — see module docstring.
DEPTH = {
    "AEO.2014": 3, "AEO.2015": 3, "AEO.2016": 3, "AEO.2017": 3, "AEO.2018": 3,
    "AEO.2019": 3, "AEO.2020": 3, "AEO.2021": 3, "AEO.2022": 3, "AEO.2023": 3,
    "AEO.2025": 3, "AEO.2026": 3, "AEO.IEO2": 3,
    "ELEC": 3, "IEO.2017": 3, "IEO.2019": 3, "IEO.2021": 3, "IEO.2023": 3,
    "NUC_STATUS": 3,
    "COAL": 2, "EBA": 2, "EMISS": 2, "INTL": 2, "NG": 2, "PET": 2,
    "PET_IMPORTS": 2, "SEDS": 2, "STEO": 2, "TOTAL": 2,
}
SKIP = {"IEO"}  # redundant union of IEO.<year> (identical id sets, measured)


def table_prefixes(path: str, depth: int) -> set[str]:
    out: set[str] = set()
    pf = pq.ParquetFile(path)
    for batch in pf.iter_batches(columns=["series_id"], batch_size=1_000_000):
        for s in pc.unique(batch.column(0)).to_pylist():
            if s:
                out.add(".".join(s.split(".")[:depth]))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    files = sorted(f for f in os.listdir(STORE) if f.endswith(".parquet"))
    names = {f[:-len(".parquet")] for f in files}
    unmapped = names - set(DEPTH) - SKIP
    if unmapped:
        # A new bulk dataset appeared since the measurement. Refuse to guess its grain —
        # measure it (rows per prefix at both depths) and extend DEPTH deliberately.
        print(f"REFUSING: {len(unmapped)} dataset file(s) not in the grain map: "
              f"{sorted(unmapped)}. Measure their rows-per-table and extend DEPTH.")
        return 2

    con = sqlite3.connect(CAT)
    existing = {r[0] for r in con.execute(
        "SELECT series_id FROM series WHERE source_id='eia'")}
    lic = None
    row = con.execute("SELECT license_id FROM source WHERE source_id='eia'").fetchone()
    if row and row[0]:
        lic = row[0]
    if lic is None:
        print("REFUSING: no licence on the eia source row — record it first.")
        return 2
    flags = con.execute("SELECT commercial_ok, no_modify, attribution_required, name "
                        "FROM license WHERE license_id=?", (lic,)).fetchone()
    if not flags:
        print(f"REFUSING: licence {lic!r} has no row in the license table.")
        return 2
    comm, nomod, attrib, lname = flags
    print(f"eia: applying licence {lic!r} ({lname}) — commercial_ok={comm} "
          f"no_modify={nomod} attribution={attrib}")
    if comm == 1:
        print("   NOTE: rows will be published as COMMERCIALLY USABLE. eia's verdict in "
              "DATABASE_LICENSES_VERBATIM.md is CLEARED (attribution) — if that ever "
              "changes, stop here.")

    total_new = 0
    for fn in files:
        name = fn[:-len(".parquet")]
        if name in SKIP:
            print(f"  {name}: SKIP (redundant union of IEO.<year>)")
            continue
        prefixes = table_prefixes(os.path.join(STORE, fn), DEPTH[name])
        new = sorted(p for p in prefixes if f"eia:{p}" not in existing)
        print(f"  {name:12} depth={DEPTH[name]} tables={len(prefixes):>7,} "
              f"new={len(new):>7,}")
        total_new += len(new)
        if a.apply and new:
            # Same 12-column shape catalog_complete.py inserts (title = the native key;
            # a later broaden_catalog pass backfills real titles).
            rows = [(f"eia:{p}", "eia", p, None, None, None, None, lic,
                     None, None, None, "{}") for p in new]
            con.executemany(
                "INSERT OR IGNORE INTO series (series_id, source_id, title, frequency, "
                "unit, geography, category, license_id, start_date, end_date, "
                "last_updated, metadata) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows)
            con.commit()

    print(f"\ntotal new table ids: {total_new:,}")
    if a.apply:
        n = con.execute("SELECT COUNT(*) FROM series WHERE source_id='eia'").fetchone()[0]
        print(f"eia catalog rows now: {n:,}")
        print("NEXT: derive table CSVs (one-sorted-pass), sync_catalog_d1 --source eia, "
              "refresh_r2_catalog, verify_source_served --source eia")
    else:
        print("--dry-run: nothing written.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
