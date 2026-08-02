"""Catalogue statcan at TABLE grain, with titles from Statistics Canada's own cube metadata.

Pairs with tools/derive_statcan_tables.py: it imports that module's id builder and split
expression and reads the same _split_map.json the resolver reads, so the catalogue, the objects
and the resolver share ONE definition of a unit.

TITLES ARE ALREADY ON DISK — no network call, and nothing invented. The ingest writes a `.done`
sidecar per Product ID carrying StatCan's own `cubeTitleEn` plus cansimId, frequencyCode,
archived, subjectCode, start, end and license_id. All 8,207 sidecars have a title; the survey
found zero blanks. So a statcan unit reads

    statcan:10100001  "Federal public sector employment reconciliation of Treasury Board of
                       Canada Secretariat, Public Service Commission of Canada and Statistics
                       Canada statistical universes, as at December 31"

rather than the bare Product ID, which is the difference between a searchable catalogue and
8,207 opaque numbers.

DATES COME FROM THE DATA FOR SPLIT TABLES, not from the sidecar. The sidecar's start/end describe
the WHOLE cube; a part covers a slice of it, so each part's range is computed from its own rows.
An unsplit table can take the sidecar's range directly, which is why the common case costs
nothing.

LICENCE: statcan is CONFIRMED "redistributable_attribution / CLEARED - re-host OK (attribution)"
in DATABASE_LICENSES_VERBATIM.md, and the `statcan-open` licence row is reservable. Checked
before any row is written, because a catalogue row is an offer to serve.

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

from derive_statcan_tables import (SOURCE, STORE, MAX_ROWS_DEFAULT,   # noqa: E402
                                   part_expr, unit_id)

LICENSE_ID = "statcan-open"
BATCH = 10_000
# StatCan frequencyCode -> the catalogue's single-letter frequency. Codes seen in the store:
# 18 (2,919), 12 (2,673), 6 (859), 9 (621), 13 (406), 16 (319), 15 (132), 11 (81). Anything not
# listed stays NULL rather than being guessed — a wrong frequency is worse than an absent one.
FREQ = {1: "A", 2: "A", 4: "Q", 6: "M", 7: "M", 9: "A", 11: "A", 12: "A", 13: "A",
        14: "D", 15: "W", 16: "D", 17: "Q", 18: "M", 19: "M", 20: "Q", 21: "A"}


def sidecars() -> dict:
    """{productId: sidecar dict} — StatCan's own cube metadata, written at ingest time."""
    out = {}
    for f in glob.glob(os.path.join(STORE, "*.done")):
        try:
            j = json.load(open(f, encoding="utf-8"))
        except (OSError, ValueError):
            continue
        pid = str(j.get("productId") or os.path.splitext(os.path.basename(f))[0])
        out[pid] = j
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--max-rows", type=int, default=MAX_ROWS_DEFAULT,
                    help="must match the value the derive ran with, or the completeness guard "
                         "below compares against the wrong set of oversized tables")
    a = ap.parse_args()

    con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=180.0)
    con.execute("PRAGMA busy_timeout = 180000")
    lic = con.execute("select reservable from license where license_id=?",
                      (LICENSE_ID,)).fetchone()
    if not lic or not lic[0]:
        print(f"licence {LICENSE_ID!r} missing or not reservable — refusing to create rows")
        return 1

    files = sorted(f.replace("\\", "/") for f in
                   glob.glob(os.path.join(STORE, "**", "*.parquet"), recursive=True)
                   if not f.endswith("__series.parquet"))
    if not files:
        print(f"no parquet under {STORE}")
        return 1
    try:
        smap = json.load(open(os.path.join(STORE, "_split_map.json"), encoding="utf-8"))
    except (OSError, ValueError) as e:
        print(f"cannot read the split map ({e!r}) — run the derive first")
        return 1

    # THE MAP MUST COVER EVERY OVERSIZED TABLE, or this emits ONE id for a table whose objects
    # were written as N parts: the whole-table id 404s and every part stays invisible. State the
    # discrepancy and list the causes rather than asserting one (R219).
    big = {os.path.splitext(os.path.basename(f))[0]: pq.ParquetFile(f).metadata.num_rows
           for f in files}
    big = {k: v for k, v in big.items() if v > a.max_rows}
    absent = {k: v for k, v in big.items() if k not in smap}
    if absent:
        try:
            ref = {r["table"] for r in json.load(open(
                os.path.join(ROOT, "logs", "statcan_tables_summary.json"),
                encoding="utf-8")).get("refused", [])}
        except (OSError, ValueError, TypeError):
            ref = set()
        print(f"REFUSING: {len(big):,} table(s) exceed {a.max_rows:,} rows but {len(absent):,} "
              f"have no split-map entry. Missing:")
        for k, v in sorted(absent.items(), key=lambda kv: -kv[1])[:20]:
            why = ("REFUSED by the derive — no splitter found" if k in ref
                   else "not seen by the derive — new, grown, or the derive is still running")
            print(f"   {k:16s} {v:>14,} rows   {why}")
        if len(absent) > 20:
            print(f"   … and {len(absent) - 20:,} more")
        return 1

    meta_cubes = sidecars()
    print(f"{len(files):,} table(s); split map {len(smap):,}; sidecars {len(meta_cubes):,}")

    spill = os.path.join(ROOT, "logs", "_duckspill", f"pid{os.getpid()}")
    os.makedirs(spill, exist_ok=True)
    base_meta = {
        "citation_short": "Statistics Canada.",
        "citation_long": ("Statistics Canada. Reproduced and distributed on an 'as is' basis "
                          "with the permission of Statistics Canada. Compiled and redistributed "
                          "by the Elkassabgi Data Library."),
        "description_processing": (
            "Retrieved from Statistics Canada's Web Data Service and stored as zstd Parquet, one "
            "file per Product ID. Served at TABLE grain because the source averages 10.8 "
            "observations per series across 5.26 billion series; large tables are split on one "
            "of their own dimension columns or on the coordinate hierarchy."),
    }

    rows, untitled = [], 0
    for i, f in enumerate(files, 1):
        pid = os.path.splitext(os.path.basename(f))[0]
        sc = meta_cubes.get(pid) or {}
        title = (sc.get("title") or "").strip()
        if not title:
            untitled += 1
            title = pid                                        # never invented
        freq = FREQ.get(sc.get("frequencyCode"))
        meta = dict(base_meta)
        for k, src in (("cansim_id", "cansimId"), ("archived", "archived"),
                       ("subject_code", "subjectCode")):
            if sc.get(src) not in (None, "", []):
                meta[k] = sc[src]
        meta_json = json.dumps(meta, ensure_ascii=False)

        entry = smap.get(pid)
        if not entry:
            # Unsplit: the sidecar's own start/end describe the whole cube, so no scan needed.
            d0, d1 = sc.get("start"), sc.get("end")
            if d0 and d1:
                rows.append((unit_id(pid), SOURCE, title, freq, None, "Canada", None,
                             LICENSE_ID, d0, d1, meta_json))
            else:
                q = duckdb.connect()
                q.execute(f"SET temp_directory='{spill}'")
                q.execute("SET enable_progress_bar=false")
                try:
                    d0, d1, n = q.execute(
                        f"select min(obs_date)::VARCHAR, max(obs_date)::VARCHAR, count(*) "
                        f"from read_parquet('{f}') "
                        f"where value is not null and obs_date is not null").fetchone()
                    if n:
                        rows.append((unit_id(pid), SOURCE, title, freq, None, "Canada", None,
                                     LICENSE_ID, d0, d1, meta_json))
                except Exception as e:                          # noqa: BLE001
                    print(f"  {pid}: FAILED {type(e).__name__} {str(e)[:60]}")
                finally:
                    q.close()
        else:
            # Split: each part covers a slice, so the sidecar's whole-cube range would be wrong
            # for every one of them. Compute each part's own range from its rows.
            dim = entry["dim"]
            q = duckdb.connect()
            q.execute("SET memory_limit='6GB'")
            q.execute(f"SET temp_directory='{spill}'")
            q.execute("SET preserve_insertion_order=false")
            q.execute("SET enable_progress_bar=false")
            try:
                got = q.execute(f"""
                    select {part_expr(dim)} p, min(obs_date)::VARCHAR, max(obs_date)::VARCHAR
                    from read_parquet('{f}') where value is not null and obs_date is not null
                    group by 1 order by 1""").fetchall()
                for p, d0, d1 in got:
                    if p is None or p == "":
                        continue
                    rows.append((unit_id(pid, p), SOURCE, f"{title} — {dim} {p}", freq, None,
                                 "Canada", None, LICENSE_ID, d0, d1, meta_json))
            except Exception as e:                              # noqa: BLE001
                print(f"  {pid}: SPLIT SCAN FAILED {type(e).__name__} {str(e)[:60]}")
            finally:
                q.close()
        if i % 500 == 0 or i == len(files):
            print(f"  [{i}/{len(files)}] {len(rows):,} unit(s)", flush=True)

    print(f"\nrows to write: {len(rows):,}   tables with no published title: {untitled:,}")
    for r in rows[:4]:
        print(f"   {r[0][:60]}\n      {r[2][:100]}   {r[8]}..{r[9]}")

    if not a.apply:
        print("\n(dry run — pass --apply to write)")
        return 0

    con.execute(
        "INSERT OR REPLACE INTO source(source_id,name,homepage,license_id,attribution,terms_url)"
        " VALUES(?,?,?,?,?,?)",
        (SOURCE, "Statistics Canada", "https://www150.statcan.gc.ca/", LICENSE_ID,
         "Source: Statistics Canada. Reproduced and distributed on an 'as is' basis with the "
         "permission of Statistics Canada.",
         "https://www.statcan.gc.ca/en/reference/licence"))
    for i in range(0, len(rows), BATCH):
        con.executemany(
            """INSERT OR REPLACE INTO series
               (series_id,source_id,title,frequency,unit,geography,category,license_id,
                start_date,end_date,metadata) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            rows[i:i + BATCH])
    con.commit()
    n = con.execute("select count(*) from series where source_id=?", (SOURCE,)).fetchone()[0]
    print(f"\ncatalogue rows for {SOURCE}: {n:,}")
    try:
        con.execute("INSERT INTO series_fts(series_fts) VALUES('rebuild')")
        con.commit()
        print("series_fts rebuilt")
    except sqlite3.Error as e:
        print(f"series_fts rebuild skipped: {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
