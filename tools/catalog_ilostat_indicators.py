"""Catalogue ilostat at INDICATOR grain, with titles from ILO's own table of contents.

Pairs with tools/derive_ilostat_indicators.py: it imports that module's id builder and split
expression and reads the same _split_map.json the resolver reads, so the catalogue, the objects
and the resolver share ONE definition of a unit.

TITLES COME FROM THE PUBLISHER. ILO ships data/raw/ilostat/toc_indicator_en.csv, whose `id`
column is exactly the parquet file stem (`SDG_0111_SEX_AGE_RT_A`) and whose `indicator.label` is
a real English title ("SDG indicator 1.1.1: Working poverty rate by sex and age (%)"). The same
row carries freq, data.start/data.end and the subject and database labels. An indicator missing
from the TOC keeps its ID as the title and is COUNTED, never given an invented one.

THE 80 LEGACY ROWS ARE KEPT. `ilostat:<flow>:<classif1>:<geo>` (4 segments) still resolves and
still works; this adds `ilostat:<stem>` and `ilostat:<stem>#<part>` (2 segments) alongside it.
The two shapes coexist the way census's table and EITS-series ids do, and segment count tells
them apart by rule — every one of the 1,947 stems matches [A-Za-z0-9_]+, so a stem cannot
introduce a third colon.

--apply writes; without it this prints what it would do and changes nothing.
"""
from __future__ import annotations

import argparse
import csv
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

from derive_ilostat_indicators import (SOURCE, STORE, MAX_ROWS_DEFAULT,   # noqa: E402
                                       part_expr, unit_id)

LICENSE_ID = "cc-by-4.0"                 # already present and reservable; ilostat already uses it
TOC = os.path.join(ROOT, "data", "raw", "ilostat", "toc_indicator_en.csv")
BATCH = 10_000


def toc_rows() -> dict:
    """{file stem: TOC row}. The publisher's own catalogue, read from the copy the fetcher
    downloads each run."""
    if not os.path.exists(TOC):
        return {}
    out = {}
    with open(TOC, encoding="utf-8-sig", newline="") as fh:
        for r in csv.DictReader(fh):
            if r.get("id"):
                out[r["id"]] = r
    return out


def refused_set(sum_obj, key):
    """(ids, provenance) from a derive summary's `refused` list. provenance is one of
    "full" | "partial" | "unreadable".

    A REFUSAL LIST IS EVIDENCE ONLY IF THE RUN THAT WROTE IT COVERED THE STORE. The derives write
    their summary unconditionally - `--dry-run`, `--only` and `--limit` runs included - and each
    cataloguer prints `--only <ids>` as the remedy for its own refusal, so following that
    instruction is precisely what leaves a scoped record behind (R843 addendum).

    Both directions matter, and they fail differently:
      * an EMPTY list from a scoped run makes "not seen by the derive" an assertion nobody
        checked - R219's single confident cause;
      * a NON-EMPTY list from a scoped run is worse: it can mark a table "correctly NOT
        catalogued" that a full run would have split without trouble.

    "unreadable" is kept distinct from "partial" so the operator is told WHICH it was; collapsing
    them is the fail-quiet shape of R503. A caller must treat anything but "full" as UNKNOWN -
    never as empty.
    """
    if not isinstance(sum_obj, dict):
        return set(), "unreadable"
    lst = sum_obj.get("refused")
    if not isinstance(lst, list):
        return set(), "unreadable"
    # `refused_scope` is the list's own provenance; `scope` describes the CAP and is accepted
    # only for back-compatibility with summaries written before the list had its own key.
    scope = sum_obj.get("refused_scope") or ("full" if sum_obj.get("scope") == "full" else None)
    ids = {r.get(key) for r in lst if isinstance(r, dict) and r.get(key) is not None}
    return ids, ("full" if scope == "full" else "partial")


def summary_coverage(sum_obj, n_store_now):
    """One line saying what the summary actually covers - the cheapest guard of all.

    `considered: 11` against a store of 2,442 makes the scope error self-evident with no tag to
    interpret. Printed unconditionally wherever the summary is read.
    """
    if not isinstance(sum_obj, dict):
        return "summary: UNREADABLE"
    # NOT `a or b or c`: a legitimate `processed: 0` is falsy and would fall through
    # to `considered`, reporting a run that processed NOTHING as having covered
    # everything - the fail-open this whole line exists to prevent.
    con = None
    for _k in ("processed", "processed_tables", "considered"):
        if sum_obj.get(_k) is not None:
            con = sum_obj[_k]
            break
    store = sum_obj.get("store_files") or sum_obj.get("store_shards")
    bits = ["scope=%s" % (sum_obj.get("scope") or "UNRECORDED"),
            "refused_scope=%s" % (sum_obj.get("refused_scope") or "UNRECORDED")]
    if con is not None:
        bits.append("covered %s of %s at the time" % (f"{con:,}", f"{store:,}" if store else "?"))
    bits.append("store holds %s now" % f"{n_store_now:,}")
    return "summary: " + ", ".join(bits)

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=180.0)
    con.execute("PRAGMA busy_timeout = 180000")
    lic = con.execute("select reservable from license where license_id=?",
                      (LICENSE_ID,)).fetchone()
    if not lic or not lic[0]:
        print(f"licence {LICENSE_ID!r} missing or not reservable — refusing to create rows")
        return 1

    files = sorted(f.replace("\\", "/") for f in glob.glob(os.path.join(STORE, "*.parquet"))
                   if not f.endswith("__series.parquet"))
    if not files:
        print(f"no parquet under {STORE}")
        return 1
    try:
        smap = json.load(open(os.path.join(STORE, "_split_map.json"), encoding="utf-8"))
    except (OSError, ValueError) as e:
        print(f"cannot read the split map ({e!r}) — run the derive first")
        return 1

    # THE MAP MUST COVER EVERY OVERSIZED FILE OR THIS CATALOGUES IDS NO OBJECT ANSWERS TO: one id
    # for an indicator whose objects were written as N parts means the whole-indicator id 404s
    # and every part stays invisible. State the discrepancy, list the causes — never assert one
    # (R219).
    big = {os.path.splitext(os.path.basename(f))[0]: pq.ParquetFile(f).metadata.num_rows
           for f in files}
    big = {k: v for k, v in big.items() if v > MAX_ROWS_DEFAULT}
    absent = {k: v for k, v in big.items() if k not in smap}
    if absent:
        try:
            _sum = json.load(open(
                os.path.join(ROOT, "logs", "ilostat_indicators_summary.json"),
                encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            _sum = None
        ref, ref_prov = refused_set(_sum, "indicator")
        print(f"REFUSING: {len(big):,} indicator(s) exceed {MAX_ROWS_DEFAULT:,} rows but "
              f"{len(absent):,} have no split-map entry. Missing:")
        print("   " + summary_coverage(_sum, len(files)))
        if ref_prov != "full":
            print(f"   the derive's refusal list is {ref_prov.upper()}, so NO cause below is "
                  f"asserted from it")
        for k, v in sorted(absent.items(), key=lambda kv: -kv[1]):
            if ref_prov != "full":
                why = ("cause NOT ESTABLISHED — new, grown, refused by a run whose record "
                       "is not store-wide, or still running")
            else:
                why = ("REFUSED by the derive — no splitter found" if k in ref
                       else "not seen by the derive — new or grown since that run")
            print(f"   {k:48s} {v:>12,} rows   {why}")
        print(f"\nRe-run:  python tools/derive_ilostat_indicators.py --bucket <b> "
              f"--only {','.join(sorted(absent))}")
        return 1

    toc = toc_rows()
    print(f"{len(files):,} indicator file(s); split map {len(smap):,}; TOC {len(toc):,} rows")

    spill = os.path.join(ROOT, "logs", "_duckspill", f"pid{os.getpid()}")
    os.makedirs(spill, exist_ok=True)
    meta_base = {
        "citation_short": "ILOSTAT, International Labour Organization.",
        "citation_long": ("International Labour Organization, ILOSTAT. Licensed CC BY 4.0. "
                          "Compiled and redistributed by the Elkassabgi Data Library."),
        "description_processing": (
            "Retrieved from ILO's bulk indicator service and stored as zstd Parquet, one file "
            "per indicator and frequency. Served at INDICATOR grain because the source averages "
            "10.0 observations per series; indicators over 500,000 rows are split on one of "
            "their own dimension columns."),
    }

    rows, unnamed = [], 0
    for i, f in enumerate(files, 1):
        stem = os.path.splitext(os.path.basename(f))[0]
        t = toc.get(stem) or {}
        name = (t.get("indicator.label") or "").strip()
        if not name:
            unnamed += 1
            name = stem                                         # never invented
        freq = (t.get("freq") or "").strip() or None
        meta = dict(meta_base)
        for k, src in (("subject", "subject.label"), ("database", "database.label")):
            if (t.get(src) or "").strip():
                meta[k] = t[src].strip()
        meta_json = json.dumps(meta, ensure_ascii=False)

        entry = smap.get(stem)
        q = duckdb.connect()
        q.execute("SET memory_limit='6GB'")
        q.execute(f"SET temp_directory='{spill}'")
        q.execute("SET preserve_insertion_order=false")
        q.execute("SET enable_progress_bar=false")
        try:
            if entry:
                dim = entry["dim"]
                got = q.execute(f"""
                    select {part_expr(dim)} p, min(obs_date)::VARCHAR, max(obs_date)::VARCHAR
                    from read_parquet('{f}') where value is not null and obs_date is not null
                    group by 1 order by 1""").fetchall()
                for p, d0, d1 in got:
                    if p is None or p == "":
                        continue
                    rows.append((unit_id(stem, p), SOURCE, f"{name} — {dim} {p}",
                                 freq, None, None, None, LICENSE_ID, d0, d1, meta_json))
            else:
                d0, d1, n = q.execute(
                    f"select min(obs_date)::VARCHAR, max(obs_date)::VARCHAR, count(*) "
                    f"from read_parquet('{f}') "
                    f"where value is not null and obs_date is not null").fetchone()
                if n:
                    rows.append((unit_id(stem), SOURCE, name, freq, None, None, None,
                                 LICENSE_ID, d0, d1, meta_json))
        except Exception as e:                                  # noqa: BLE001
            print(f"  {stem}: FAILED {type(e).__name__} {str(e)[:70]}")
        finally:
            q.close()
        if i % 200 == 0 or i == len(files):
            print(f"  [{i}/{len(files)}] {len(rows):,} unit(s)", flush=True)

    print(f"\nrows to write: {len(rows):,}   indicators with no published title: {unnamed:,}")
    for r in rows[:4]:
        print(f"   {r[0][:70]}\n      {r[2][:96]}   {r[8]}..{r[9]}")

    if not a.apply:
        print("\n(dry run — pass --apply to write)")
        return 0

    con.execute(
        "INSERT OR REPLACE INTO source(source_id,name,homepage,license_id,attribution,terms_url)"
        " VALUES(?,?,?,?,?,?)",
        (SOURCE, "ILOSTAT (International Labour Organization)", "https://ilostat.ilo.org/",
         LICENSE_ID, "Source: ILOSTAT (CC BY 4.0)", "https://www.ilo.org/rights-and-permissions"))
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
