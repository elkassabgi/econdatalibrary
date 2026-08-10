"""Enrich bea's 913,230 code-titles with real BEA descriptions (task #65 follow-up).

The full-tree cataloguer (tools/_cat_bea.py) shipped code-titles because
jobs/ingest_bea_full.py discards LineDescription at ingest. This tool rebuilds
titles from BEA's own metadata, then only REPLACES titles that still equal the
native key (the 240 pre-existing real titles are never touched).

PROVENANCE RULE. The store dedups (key, date) keep-first in pyarrow dataset scan
order (see tools/_derive_bea_bulk.py), so when the same key appears in several
datasets/tables the SERVED rows come from the first. Titles follow the same rule:
datasets are processed in the dataset-directory scan order and the first title
set for a key wins, so a title always describes the rows the key actually serves.
Regional (796,716 keys — 87% of the source) gets this at table grain: the first
Regional table (alphabetical file order == scan order) that contains a key names
its LineCode.

SOURCES OF TRUTH:
  * offline — data/raw/bea/catalog_manifest.json param_values (ITA indicators,
    IIP investment types, IntlServTrade service types, industry codelists,
    MNE series ids, Regional GeoFips names, table descriptions);
  * API (rate-limited via jobs.ingest_bea_full.call) —
      - GetParameterValuesFiltered LineCode per Regional table (105 calls),
      - one-recent-year GetData per NIPA/NIUnderlyingDetail table+freq and per
        FixedAssets table, for SeriesCode -> LineDescription (~1,040 calls).
    Year ladder 2024 -> 2023 -> 2019 -> 2015; a table with no data in any of
    those keeps code-titles and is COUNTED, not silently dropped.

Coverage is MEASURED and printed per dataset at the end. D1 is NOT synced here —
run core/sync_catalog_d1.py --source bea afterwards (R401).
"""
from __future__ import annotations

import glob
import json
import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import pyarrow.parquet as pq  # noqa: E402

from jobs import ingest_bea_full as ing  # noqa: E402  (call, load_manifest, _pk)

STORE = os.path.join(ROOT, "data", "clean_full", "bea")
CATALOG = os.path.join(ROOT, "data", "catalog.db")
YEARS = ["2024", "2023", "2019", "2015"]


def _desc(entry: dict, key: str) -> str | None:
    """The human description field of a GetParameterValues entry, whatever its name."""
    for fld in ("Description", "Desc", "description", "desc", "LineDescription",
                "TableDescription", "IndustrYDescription", "IndustryDescription"):
        v = entry.get(fld)
        if isinstance(v, str) and v.strip() and v.strip() != key:
            return v.strip()
    best = None
    for v in entry.values():
        if isinstance(v, str) and v.strip() and v.strip() != key:
            if best is None or len(v.strip()) > len(best):
                best = v.strip()
    return best


def _lookup(pv_list) -> dict[str, str]:
    out = {}
    for e in pv_list:
        k = ing._pk(e)
        if k is None:
            continue
        d = _desc(e, k)
        if d:
            out.setdefault(k, d)
    return out


def _distinct_keys_in_order(dirname: str) -> list[tuple[str, list[str]]]:
    """[(table_file_stem, distinct keys in that file)] in filename (=scan) order."""
    out = []
    for f in sorted(glob.glob(os.path.join(STORE, dirname, "*.parquet"))):
        pf = pq.ParquetFile(f)
        if "series_key" not in pf.schema_arrow.names:
            continue
        seen: set[str] = set()
        for b in pf.iter_batches(columns=["series_key"], batch_size=500_000):
            seen.update(b.column(0).to_pylist())
        out.append((os.path.splitext(os.path.basename(f))[0], sorted(seen)))
    return out


def _getdata_linedesc(dataset: str, table_param: str, table: str,
                      extra: dict) -> dict[str, str]:
    """SeriesCode -> LineDescription from one recent-year GetData, with year ladder."""
    for y in YEARS:
        rows = ing.call("GetData", DatasetName=dataset, **{table_param: table},
                        Year=y, **extra)
        if rows:
            out = {}
            for r in rows:
                c = str(r.get("SeriesCode", "")).strip()
                d = str(r.get("LineDescription", "")).strip()
                if c and d:
                    out.setdefault(c, d)
            if out:
                return out
    return {}


def build() -> dict[str, dict[str, str]]:
    """Per-dataset {native_key: title}; apply order = dataset scan order."""
    M = ing.load_manifest()
    pv = M["param_values"]
    titles: dict[str, dict[str, str]] = {}

    ind_map = {}
    for ds_name in ("GDPbyIndustry", "UnderlyingGDPbyIndustry"):
        ind_map.update(_lookup(pv[ds_name]["Industry"]))

    # ---- FixedAssets (API): key = SeriesCode --------------------------------
    fa = {}
    tabs = [ing._pk(e) for e in pv["FixedAssets"]["TableName"]]
    for i, t in enumerate(t for t in tabs if t):
        for code, d in _getdata_linedesc("FixedAssets", "TableName", t, {}).items():
            fa.setdefault(code, d)
        if (i + 1) % 25 == 0:
            print(f"  FixedAssets {i+1}/{len(tabs)} tables", flush=True)
    titles["FixedAssets"] = fa

    # ---- GDPbyIndustry / UnderlyingGDPbyIndustry (offline): TableID:Ind:freq
    for ds_name in ("GDPbyIndustry", "UnderlyingGDPbyIndustry"):
        tmap = _lookup(pv[ds_name]["TableID"])
        d = {}
        for stem, keys in _distinct_keys_in_order(ds_name):
            for k in keys:
                parts = k.split(":")
                if len(parts) != 3:
                    continue
                tid, indus, fq = parts
                tdesc = tmap.get(tid.lstrip("T"), tmap.get(tid))
                idesc = ind_map.get(indus)
                if tdesc and idesc:
                    d.setdefault(k, f"{tdesc} — {idesc} ({fq})")
                elif idesc:
                    d.setdefault(k, f"{ds_name} {tid} — {idesc} ({fq})")
        titles[ds_name] = d

    # ---- IIP (offline): TypeOfInvestment:Component:freq ---------------------
    toi = _lookup(pv["IIP"]["TypeOfInvestment"])
    comp = _lookup(pv["IIP"]["Component"])
    d = {}
    for _stem, keys in _distinct_keys_in_order("IIP"):
        for k in keys:
            parts = k.split(":")
            if len(parts) != 3:
                continue
            t, c, fq = parts
            if toi.get(t):
                d.setdefault(k, f"{toi[t]} — {comp.get(c, c)} ({fq})")
    titles["IIP"] = d

    # ---- ITA (offline): Indicator:AreaOrCountry -----------------------------
    ind = _lookup(pv["ITA"]["Indicator"])
    area = _lookup(pv["ITA"]["AreaOrCountry"])
    d = {}
    for _stem, keys in _distinct_keys_in_order("ITA"):
        for k in keys:
            parts = k.split(":")
            if len(parts) != 2:
                continue
            i, a = parts
            if ind.get(i):
                d.setdefault(k, f"{ind[i]} — {area.get(a, a)}")
    titles["ITA"] = d

    # ---- InputOutput (offline, partial): row|col via industry codelists -----
    d = {}
    for _stem, keys in _distinct_keys_in_order("InputOutput"):
        for k in keys:
            parts = k.split("|")
            if len(parts) != 2:
                continue
            r, c = parts
            rd, cd = ind_map.get(r), ind_map.get(c)
            if rd or cd:
                d.setdefault(k, f"Input-Output: {rd or r} × {cd or c}")
    titles["InputOutput"] = d

    # ---- IntlServSTA (offline): Channel:Ownership:Industry:Country ----------
    sta_ind = _lookup(pv["IntlServSTA"]["Industry"])
    sta_area = _lookup(pv["IntlServSTA"]["AreaOrCountry"])
    d = {}
    for _stem, keys in _distinct_keys_in_order("IntlServSTA"):
        for k in keys:
            parts = k.split(":")
            if len(parts) != 4:
                continue
            ch, own, indus, ctry = parts
            if sta_ind.get(indus) or sta_area.get(ctry):
                d.setdefault(k, f"{sta_ind.get(indus, indus)} — "
                                f"{sta_area.get(ctry, ctry)} ({ch}, {own})")
    titles["IntlServSTA"] = d

    # ---- IntlServTrade (offline): TypeOfService:Direction:Affiliation:Area --
    tos = _lookup(pv["IntlServTrade"]["TypeOfService"])
    tr_area = _lookup(pv["IntlServTrade"]["AreaOrCountry"])
    d = {}
    for _stem, keys in _distinct_keys_in_order("IntlServTrade"):
        for k in keys:
            parts = k.split(":")
            if len(parts) != 4:
                continue
            t, direc, aff, a = parts
            if tos.get(t):
                d.setdefault(k, f"{tos[t]} — {direc} — {aff} — {tr_area.get(a, a)}")
    titles["IntlServTrade"] = d

    # ---- MNE (offline, partial): SeriesID:row:col ---------------------------
    sid = _lookup(pv["MNE"]["SeriesID"])
    d = {}
    for _stem, keys in _distinct_keys_in_order("MNE"):
        for k in keys:
            parts = k.split(":")
            if len(parts) != 3:
                continue
            s, r, c = parts
            if sid.get(s):
                d.setdefault(k, f"{sid[s]} ({r}:{c})")
    titles["MNE"] = d

    # ---- NIPA / NIUnderlyingDetail (API): SeriesCode:freq -------------------
    for ds_name in ("NIPA", "NIUnderlyingDetail"):
        d = {}
        tabs = [t for t in (ing._pk(e) for e in pv[ds_name]["TableName"]) if t]
        for i, t in enumerate(tabs):
            for fq in ("A", "Q", "M"):
                for code, desc in _getdata_linedesc(
                        ds_name, "TableName", t, {"Frequency": fq}).items():
                    d.setdefault(f"{code}:{fq}", f"{desc} ({fq})")
            if (i + 1) % 25 == 0:
                print(f"  {ds_name} {i+1}/{len(tabs)} tables", flush=True)
        titles[ds_name] = d

    # ---- Regional (API linecodes + offline geo): LineCode:GeoFips -----------
    geo = _lookup(pv["Regional"]["GeoFips"])
    line_by_table: dict[str, dict[str, str]] = {}
    rtabs = [t for t in (ing._pk(e) for e in pv["Regional"]["TableName"]) if t]
    for i, t in enumerate(rtabs):
        vals = ing.call("GetParameterValuesFiltered", DatasetName="Regional",
                        TargetParameter="LineCode", TableName=t)
        line_by_table[t] = _lookup(vals) if vals else {}
        if (i + 1) % 25 == 0:
            print(f"  Regional linecodes {i+1}/{len(rtabs)} tables", flush=True)
    d = {}
    for stem, keys in _distinct_keys_in_order("Regional"):
        lmap = line_by_table.get(stem, {})
        for k in keys:
            if k in d:
                continue          # first table wins — provenance rule
            parts = k.split(":")
            if len(parts) != 2:
                continue
            lc, gf = parts
            g = geo.get(gf)
            ld = lmap.get(lc)
            if ld and g:
                d[k] = f"{ld} — {g}"
            elif g:
                d[k] = f"{g} — {stem} line {lc}"
    titles["Regional"] = d

    return titles


def main() -> int:
    titles = build()
    # apply in dataset scan order, first-set-wins; root bea.parquet keys are
    # NIPA-shaped and covered by the NIPA/NIUD maps applied at their turn.
    order = ["FixedAssets", "GDPbyIndustry", "IIP", "ITA", "InputOutput",
             "IntlServSTA", "IntlServTrade", "MNE", "NIPA", "NIUnderlyingDetail",
             "Regional", "UnderlyingGDPbyIndustry"]
    merged: dict[str, str] = {}
    for ds_name in order:
        for k, t in titles.get(ds_name, {}).items():
            merged.setdefault(k, t)

    con = sqlite3.connect(CATALOG, timeout=7200)
    con.execute("PRAGMA busy_timeout=7200000")
    cur = con.execute(
        "SELECT series_id, title FROM series WHERE source_id='bea'")
    rows = cur.fetchall()
    updates = []
    kept_real = 0
    for sid, title in rows:
        native = sid.split(":", 1)[1]
        if title != native:            # a real title already — never clobber
            kept_real += 1
            continue
        t = merged.get(native)
        if t:
            updates.append((t, sid))
    con.executemany("UPDATE series SET title=? WHERE series_id=?", updates)
    # LOCAL series_fts: catalog.db has no triggers and _cat_bea.py never inserted
    # bea fts rows at all — rebuild the source's slice from `series` so local
    # search matches what D1 will serve after the re-sync.
    con.execute("DELETE FROM series_fts WHERE series_id LIKE 'bea:%'")
    con.execute("INSERT INTO series_fts (series_id, title, geography) "
                "SELECT series_id, title, geography FROM series "
                "WHERE source_id='bea'")
    con.commit()

    total = len(rows)
    print(f"\ncatalogue rows      : {total:,}")
    print(f"pre-existing titles : {kept_real:,} (untouched)")
    print(f"titles APPLIED      : {len(updates):,}")
    print(f"still code-titled   : {total - kept_real - len(updates):,}")
    for ds_name in order:
        print(f"  {ds_name:26s} map size {len(titles.get(ds_name, {})):>8,}")
    print("\nNOW RUN: python core/sync_catalog_d1.py --source bea   (R401)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
