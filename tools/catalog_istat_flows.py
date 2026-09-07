"""Catalogue istat at FLOW grain, with titles from ISTAT's own dataflow catalogue.

Pairs with tools/derive_istat_flows.py: it imports that module's id builder and reads the same
_split_map.json the resolver reads, so the catalogue, the objects and the resolver share one
definition of a unit.

TITLES COME FROM THE PUBLISHER, FETCHED LIVE. istat flow ids are opaque — `101_1015`,
`183_277`, `164_164_DF_DASH_DCIS_RICPOPRES2011_24` — and 2,400 units titled with those would be
searchable by nobody. ISTAT publishes the names in its own SDMX catalogue:

    GET https://esploradati.istat.it/SDMXWS/rest/dataflow/IT1?detail=allstubs
    -> 4,899 dataflows, English names: 101_1015 "Crops",
       101_1015_DF_DCSP_COLTIVAZIONI_10 "Sowing forecast"

Cached to _dataflow_names.json in the store so a catalogue rebuild does not depend on the host
being up — it is flaky enough that the ingest carries a two-host fallback for exactly this
reason. A flow with no name in the catalogue keeps its ID as the title and is COUNTED, never
given an invented one.

--apply writes; without it this prints what it would do and changes nothing.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sqlite3
import sys
import urllib.request
import xml.etree.ElementTree as ET

import duckdb

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools"))

from derive_istat_flows import SOURCE, STORE, flow_id, part_expr   # noqa: E402

LICENSE_ID = "cc-by-4.0-istat"
BATCH = 10_000
NAMES_CACHE = os.path.join(STORE, "_dataflow_names.json")
# must match derive_istat_flows' MAX_ROWS_DEFAULT; used only to detect a
# half-written split map, not to make a splitting decision here.
MAX_ROWS_HINT = 500_000
HOSTS = ("https://esploradati.istat.it/SDMXWS/rest/", "https://sdmx.istat.it/SDMXWS/rest/")
UA = {"User-Agent": "Econ-Fin Data Library admin@econdatalibrary.com",
      "Accept": "application/vnd.sdmx.structure+xml;version=2.1"}


def dataflow_names(refresh: bool = False) -> dict:
    """{flow_id: English name}, from cache unless --refresh-names."""
    if not refresh and os.path.exists(NAMES_CACHE):
        try:
            return json.load(open(NAMES_CACHE, encoding="utf-8"))
        except ValueError:
            pass
    for base in HOSTS:
        try:
            req = urllib.request.Request(base + "dataflow/IT1?detail=allstubs", headers=UA)
            root = ET.fromstring(urllib.request.urlopen(req, timeout=300).read())
        except Exception as e:                                 # noqa: BLE001
            print(f"  {base}: {type(e).__name__} {str(e)[:70]}")
            continue
        out = {}
        for el in root.iter():
            if el.tag.split("}")[-1] != "Dataflow" or not el.get("id"):
                continue
            name = ""
            for c in el:
                if c.tag.split("}")[-1] != "Name":
                    continue
                lang = c.get("{http://www.w3.org/XML/1998/namespace}lang")
                if lang == "en":
                    name = (c.text or "").strip()
                    break
                if not name:
                    name = (c.text or "").strip()
            if name:
                out[el.get("id")] = name
        if out:
            json.dump(out, open(NAMES_CACHE, "w", encoding="utf-8"),
                      indent=1, sort_keys=True, ensure_ascii=False)
            print(f"  {len(out):,} dataflow name(s) from {base} -> {NAMES_CACHE}")
            return out
    return {}


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
    con = sum_obj.get("processed") or sum_obj.get("processed_tables") or sum_obj.get("considered")
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
    ap.add_argument("--refresh-names", action="store_true")
    a = ap.parse_args()

    con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=180.0)
    con.execute("PRAGMA busy_timeout = 180000")
    lic = con.execute("select reservable from license where license_id=?",
                      (LICENSE_ID,)).fetchone()
    if not lic:
        # ISTAT was assessed 2026-08-01: CC BY 4.0, CLEARED, quoted verbatim from both the
        # English legal notice and the Italian original. Creating the row transcribes that
        # recorded verdict; it does not make a new one.
        if not a.apply:
            print(f"licence {LICENSE_ID!r} does not exist yet — --apply would create it "
                  f"(CC BY 4.0, commercial ok, attribution required)")
        else:
            con.execute(
                "INSERT OR REPLACE INTO license"
                "(license_id,name,reservable,commercial_ok,attribution_required,no_modify,url)"
                " VALUES(?,?,?,?,?,?,?)",
                (LICENSE_ID, "cc-by-4.0", 1, 1, 1, 0,
                 "https://creativecommons.org/licenses/by/4.0/"))
            con.commit()
            print(f"licence {LICENSE_ID} created (CC BY 4.0, reservable)")
    elif not lic[0]:
        print(f"licence {LICENSE_ID!r} is NOT reservable — refusing to create rows")
        return 1

    names = dataflow_names(a.refresh_names)
    print(f"dataflow names available: {len(names):,}")

    files_all = sorted(f.replace("\\", "/")
                       for f in glob.glob(os.path.join(STORE, "*.parquet"))
                       if not f.endswith("__series.parquet"))
    try:
        smap = json.load(open(os.path.join(STORE, "_split_map.json"), encoding="utf-8"))
    except (OSError, ValueError) as e:
        print(f"cannot read the split map ({e!r}) — run the derive first")
        return 1
    print(f"split map: {len(smap):,} split flow(s)")

    # THE MAP MUST BE COMPLETE OR THIS CATALOGUES IDS THAT DO NOT EXIST. The derive decides the
    # split per flow and writes the map as it goes, so a map written by a --limit run describes
    # only the flows it reached. Building against that would emit ONE id for a flow whose
    # objects were actually written as N parts: the whole-flow id 404s and every part is
    # invisible. Compare against the flows that are large enough to need splitting and refuse
    # if the map is plainly short.
    import pyarrow.parquet as _pq
    big = {os.path.splitext(os.path.basename(f))[0]: _pq.ParquetFile(f).metadata.num_rows
           for f in files_all}
    big = {k: v for k, v in big.items() if v > MAX_ROWS_HINT}
    absent = {k: v for k, v in big.items() if k not in smap}
    if absent:
        # STATE THE DISCREPANCY, LIST THE CAUSES (R219). The first version of this asserted one
        # cause — "written by a partial derive run" — which was wrong: the derive had run to
        # completion and REFUSED three flows it could not split. A guard that names a single
        # confident cause sends the reader past the real one.
        try:
            _sum = json.load(
                open(os.path.join(ROOT, "logs", "istat_flows_summary.json"),
                     encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            _sum = None
        ref, ref_prov = refused_set(_sum, "flow")
        print(f"REFUSING: {len(big):,} flow(s) exceed {MAX_ROWS_HINT:,} rows but {len(absent):,} "
              f"of them have no split-map entry, so cataloguing now would emit ONE id for a flow "
              f"whose objects were written as N parts — the whole-flow id 404s and every part "
              f"stays invisible. Missing:")
        print("   " + summary_coverage(_sum, len(files_all)))
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
            print(f"   {k:44s} {v:>12,} rows   {why}")
        print(f"\nRe-run:  python tools/derive_istat_flows.py --bucket <b> "
              f"--only {','.join(sorted(absent))}")
        return 1

    spill = os.path.join(ROOT, "logs", "_duckspill", f"pid{os.getpid()}")
    os.makedirs(spill, exist_ok=True)
    meta = json.dumps({
        "citation_short": "Istat (Istituto nazionale di statistica).",
        "citation_long": ("Istat, Italian National Institute of Statistics. Licensed CC BY 4.0. "
                          "Compiled and redistributed by the Elkassabgi Data Library."),
        "description_processing": ("Retrieved from Istat's SDMX API and stored as zstd Parquet, "
                                   "one file per dataflow. Served at FLOW grain because the "
                                   "source averages 9.2 observations per series; large flows "
                                   "are split on one of their own named dimensions."),
    }, ensure_ascii=False)

    files = files_all
    rows, unnamed = [], 0
    for i, f in enumerate(files, 1):
        stem = os.path.splitext(os.path.basename(f))[0]
        name = names.get(stem)
        if not name:
            unnamed += 1
            name = stem                                        # never invented
        entry = smap.get(stem)
        q = duckdb.connect()
        q.execute("SET memory_limit='6GB'")
        q.execute(f"SET temp_directory='{spill}'")
        q.execute("SET preserve_insertion_order=false")
        try:
            if entry:
                # ONE definition of the part expression, imported from the derive — a catalogue
                # that reimplements it drifts into ids no object answers to.
                dim, tr = entry["dim"], entry.get("trunc") or 0
                e = part_expr(dim, tr)
                got = q.execute(f"""
                    select {e} p, min(obs_date)::VARCHAR, max(obs_date)::VARCHAR
                    from read_parquet('{f}') where value is not null and obs_date is not null
                    group by 1 order by 1""").fetchall()
                for p, d0, d1 in got:
                    if not p:
                        continue
                    rows.append((flow_id(stem, p), SOURCE, f"{name} — {dim} {p}",
                                 None, None, None, "Italy", LICENSE_ID, d0, d1, meta))
            else:
                d0, d1, n = q.execute(
                    f"select min(obs_date)::VARCHAR, max(obs_date)::VARCHAR, count(*) "
                    f"from read_parquet('{f}') "
                    f"where value is not null and obs_date is not null").fetchone()
                if n:
                    rows.append((flow_id(stem), SOURCE, name, None, None, None,
                                 "Italy", LICENSE_ID, d0, d1, meta))
        except Exception as e:                                 # noqa: BLE001
            print(f"  {stem}: FAILED {type(e).__name__} {str(e)[:70]}")
        finally:
            q.close()
        if i % 200 == 0 or i == len(files):
            print(f"  [{i}/{len(files)}] {len(rows):,} unit(s)", flush=True)

    print(f"\nrows to write: {len(rows):,}   flows with no published name: {unnamed:,}")
    for r in rows[:4]:
        print(f"   {r[0][:70]}")
        print(f"      {r[2][:96]}   {r[8]}..{r[9]}")

    if not a.apply:
        print("\n(dry run — pass --apply to write)")
        return 0

    con.execute(
        "INSERT OR REPLACE INTO source(source_id,name,homepage,license_id,attribution,terms_url)"
        " VALUES(?,?,?,?,?,?)",
        (SOURCE, "Istat (Italian National Institute of Statistics)", "https://www.istat.it/",
         LICENSE_ID, "Source: Istat, licensed CC BY 4.0.",
         "https://www.istat.it/en/legal-notice/"))
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
