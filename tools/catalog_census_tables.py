"""Catalogue census at TABLE grain — one row per unit written by derive_census_tables.

Pairs with tools/derive_census_tables.py and imports its id builder and split map, so the two
cannot drift into listing ids whose objects do not exist. The split is decided at DERIVE time
from each table's own data, so the map is the only record of it; this reads the same file the
resolver does.

TITLES come from the store's own naming, not from anything invented. A census table id is a
path-ish stem (`intltrade__exports__hs`, `eits__mrtsadv`, `aies__basic`) whose segments are the
Census Bureau's own programme and dataset names, so the title is those segments spelled out,
plus the split part where there is one:

    International Trade — Exports — HS — commodity chapter 551
    Economic Indicators — Monthly Retail Trade Survey (advance)

WHERE A SEGMENT HAS NO KNOWN EXPANSION IT IS LEFT AS PUBLISHED. Guessing what `sitcexport`
stands for would put an invention in front of a reader as if it were the Bureau's own words.

--apply writes; without it this prints what it would do and changes nothing.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sqlite3
import sys

import duckdb
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools"))

from derive_census_tables import SOURCE, STORE, table_sid      # noqa: E402

LICENSE_ID = "us-public-domain"
BATCH = 5_000

# Census Bureau programme names, from the Bureau's own product titles. A stem segment with no
# entry here is passed through as published rather than guessed at.
PROGRAMME = {
    "intltrade": "International Trade",
    "eits": "Economic Indicators",
    "aies": "Annual Integrated Economic Survey",
    "asm": "Annual Survey of Manufactures",
    "idb": "International Data Base",
    "healthins": "Health Insurance",
    "poverty": "Poverty",
    "bds": "Business Dynamics Statistics",
    "hhpulse": "Household Pulse Survey",
    "hps": "Household Pulse Survey",
    "pseo": "Post-Secondary Employment Outcomes",
    "exports": "Exports",
    "imports": "Imports",
}


def title_for(table: str, part: str | None, dims) -> str:
    words = [PROGRAMME.get(seg, seg) for seg in table.split("__")]
    t = " — ".join(words)
    if part:
        label = "+".join(d.split(":")[0] for d in dims) if dims else "part"
        t += f" — {label} {part}"
    return t


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=180.0)
    con.execute("PRAGMA busy_timeout = 180000")
    lic = con.execute("select reservable, name from license where license_id=?",
                      (LICENSE_ID,)).fetchone()
    if not lic or not lic[0]:
        print(f"licence {LICENSE_ID!r} missing or not reservable — refusing to create rows")
        return 1
    print(f"licence {LICENSE_ID}: reservable={lic[0]}  ok to catalogue")

    smap_path = os.path.join(STORE, "_split_map.json")
    try:
        smap = json.load(open(smap_path, encoding="utf-8"))
    except (OSError, ValueError) as e:
        print(f"cannot read {smap_path} ({e!r}) — run the derive first; the split is decided "
              f"there and nothing else records it")
        return 1
    print(f"split map: {len(smap):,} split table(s)")

    spill = os.path.join(ROOT, "logs", "_duckspill")
    os.makedirs(spill, exist_ok=True)

    meta = json.dumps({
        "citation_short": "U.S. Census Bureau.",
        "citation_long": ("U.S. Census Bureau. Compiled and redistributed by the Elkassabgi "
                          "Data Library."),
        "description_processing": ("Retrieved from U.S. Census Bureau APIs and stored as zstd "
                                   "Parquet, one file per dataset. Served at TABLE grain — the "
                                   "measures are columns, so each CSV is the table as "
                                   "published; large tables are split on one of their own "
                                   "classification dimensions."),
    }, ensure_ascii=False)

    rows = []
    files = sorted(f.replace("\\", "/") for f in glob.glob(os.path.join(STORE, "*.parquet"))
                   if not f.endswith("__series.parquet"))
    for i, f in enumerate(files, 1):
        table = os.path.splitext(os.path.basename(f))[0]
        cols = set(pq.read_schema(f).names)
        if "obs_date" not in cols or "series_key" not in cols:
            continue
        entry = smap.get(table)
        q = duckdb.connect()
        q.execute("SET memory_limit='6GB'")
        q.execute(f"SET temp_directory='{spill}'")
        q.execute("SET preserve_insertion_order=false")
        try:
            if entry:
                dims = entry.get("dims") or []
                sep = entry.get("sep", "~")
                exprs = []
                for d in dims:
                    name, _, tr = d.partition(":")
                    e = f"regexp_extract(series_key, '{name}=([^|]*)', 1)"
                    exprs.append(f"substr({e}, 1, {tr})" if tr else e)
                pexpr = f" || '{sep}' || ".join(exprs) if len(exprs) > 1 else exprs[0]
                got = q.execute(f"""
                    select {pexpr} p, min(obs_date)::VARCHAR, max(obs_date)::VARCHAR, count(*)
                    from read_parquet('{f}') where obs_date is not null
                    group by 1 order by 1""").fetchall()
                for p, d0, d1, n in got:
                    rows.append((table_sid(table, p), SOURCE,
                                 title_for(table, p, dims), None, None, None,
                                 "Economy", LICENSE_ID, d0, d1, meta))
            else:
                d0, d1, n = q.execute(
                    f"select min(obs_date)::VARCHAR, max(obs_date)::VARCHAR, count(*) "
                    f"from read_parquet('{f}') where obs_date is not null").fetchone()
                if n:
                    rows.append((table_sid(table), SOURCE, title_for(table, None, None),
                                 None, None, None, "Economy", LICENSE_ID, d0, d1, meta))
        except Exception as e:                                 # noqa: BLE001
            print(f"  {table}: FAILED {type(e).__name__} {str(e)[:70]}")
        finally:
            q.close()
        if i % 20 == 0 or i == len(files):
            print(f"  [{i}/{len(files)}] {len(rows):,} unit(s)", flush=True)

    print(f"\nrows to write: {len(rows):,}")
    for r in rows[:4]:
        print(f"   {r[0][:88]}")
        print(f"      {r[2][:100]}   {r[8]}..{r[9]}")

    if not a.apply:
        print("\n(dry run — pass --apply to write)")
        return 0

    for i in range(0, len(rows), BATCH):
        con.executemany(
            """INSERT OR REPLACE INTO series
               (series_id,source_id,title,frequency,unit,geography,category,license_id,
                start_date,end_date,metadata) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            rows[i:i + BATCH])
    con.commit()
    n = con.execute("select count(*) from series where source_id=?", (SOURCE,)).fetchone()[0]
    print(f"\ncatalogue rows for {SOURCE}: {n:,}")
    print("NOTE: the 22 pre-existing EITS series-grain rows are KEPT — their ids resolve "
          "through the older branch and their objects exist. Table grain is additive here.")
    try:
        con.execute("INSERT INTO series_fts(series_fts) VALUES('rebuild')")
        con.commit()
        print("series_fts rebuilt")
    except sqlite3.Error as e:
        print(f"series_fts rebuild skipped: {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
