#!/usr/bin/env python3
"""Compose dist/titles/<source>.json for served unctad_* sources from UNCTAD'S OWN API.

WHY. The currently-served unctad_* sources carry machine-code titles - `008.M1900` is a
catalogue row's entire title - so roughly 700,000 series cannot be found by name. The
titles that DO read well live in the legacy DBnomics-era unctad stores, and that mirror is
BANNED as a source here (econ CLAUDE.md section 0, ledger R251), so it is not a lawful
label source.

It is also not needed. Everything required is on UNCTAD's own KEYLESS endpoints:

  report title    GET /api/reportMetadata/{DS}/en   -> `title`
  measure label   same document                     -> defaults.observations[].measures[] {code,label}
  dimension label GET /datamart-api/{DS}/{version}/{DimTable}?$orderby=Order&culture=en
                                                    -> [{Code, Label, ...}]

That last one is the same endpoint jobs/ingest_unctad_ds.py already calls for chunked pulls
(`dim_codes`), which reads only `Code` and discards `Label`.

KEY LAYOUT IS TAKEN FROM THE INGEST, NOT RE-DERIVED. A series_key is `<dim>...<dim>.M<measure>`
("076.M1900"; "001.364.M6700" when a dataset has two key dims). Which dims, and in what order,
comes from `dataset_layout(meta)` - the same function that BUILT the keys - so the two cannot
drift apart. A key whose segment count does not match the layout is skipped and counted, never
guessed at.

Titles read "<report title> - <dim labels> (<measure label>)", the shape UNCTAD itself uses.
Any code with no published label leaves that series untitled rather than inventing one.
"""
from __future__ import annotations

import argparse
import glob
import importlib.util
import json
import os
import re
import sys

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

_spec = importlib.util.spec_from_file_location("iu", os.path.join(ROOT, "jobs", "ingest_unctad_ds.py"))
iu = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(iu)

DATAMART = "https://unctadstat-api.unctad.org/datamart-api"


_DATACENTER = "https://unctadstat-api.unctad.org/api/datacenter/en"
_REPORTS: list = []


def _advertised_reports() -> list:
    """Every reportName UNCTAD's own datacenter advertises (cached for the process)."""
    if not _REPORTS:
        try:
            j = requests.get(_DATACENTER, headers=iu.UA, timeout=180).json()
        except Exception:                                        # noqa: BLE001
            return _REPORTS
        def walk(o):
            if isinstance(o, dict):
                if o.get("reportName"):
                    _REPORTS.append(o["reportName"])
                for v in o.values():
                    walk(v)
            elif isinstance(o, list):
                for v in o:
                    walk(v)
        walk(j)
    return _REPORTS


def _ds_for(source_id: str, reg: dict):
    vs = str(((reg.get(source_id) or {}).get("adapter") or {}).get("vintage_signal", ""))
    m = re.search(r"reportMetadata/([A-Za-z0-9_.]+)/", vs)
    if m:
        return m.group(1)
    # FALL BACK TO UNCTAD'S OWN CATALOGUE WHEN THE REGISTRY NAMES NO DATASET. Some entries
    # describe their vintage signal without a reportMetadata URL, so there is nothing to
    # regex - and the source then silently titles nothing. unctad_tradeservcatbypartner
    # (9,243 rows) and unctad_biotrademerch (6,666) were exactly that. Their ids ARE the
    # dataset name modulo case and underscores, so match against what UNCTAD advertises and
    # accept ONLY an unambiguous hit; a source that matches two datasets is skipped rather
    # than guessed at.
    key = source_id[len("unctad_"):].replace("_", "").lower() if source_id.startswith("unctad_") else ""
    if not key:
        return None
    hits = [r for r in _advertised_reports()
            if r.lower().replace("us.", "", 1).replace("_", "") == key]
    return hits[0] if len(hits) == 1 else None


def _dim_labels(ds: str, version, table: str) -> dict:
    r = requests.get(f"{DATAMART}/{ds}/{version}/{table}",
                     params={"$orderby": "Order", "culture": "en"},
                     headers=iu.UA, timeout=180)
    if r.status_code != 200:
        return {}
    body = r.json()
    vals = body.get("value", body) if isinstance(body, dict) else body
    return {str(x["Code"]): x.get("Label") for x in vals
            if x.get("Code") is not None and x.get("Label")}


def _catalogued(source_id: str) -> set:
    """The ids this source actually HAS a catalogue row for.

    The store holds far more series keys than the catalogue lists (oceantrade: a 2.4 GB
    title file against ~32k catalogued rows), and apply_title_wave loads each file whole,
    so an unfiltered file is both useless weight and an out-of-memory risk. Titling what is
    not listed changes nothing a user can see.
    """
    import sqlite3
    db = os.path.join(ROOT, "data", "catalog.db").replace("\\", "/")
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=120)
    try:
        return {r[0] for r in con.execute(
            "SELECT series_id FROM series WHERE source_id=?", (source_id,))}
    finally:
        con.close()


def titles_for(source_id: str, ds: str):
    meta = iu.report_metadata(ds)
    ver = meta.get("version")
    kfields, _tf, _isy, _pax, _measures = iu.dataset_layout(meta)
    dims = [d for axe in ("rowAxe", "colAxe", "pageAxe") for d in (meta["defaults"].get(axe) or [])]
    by_field = {d.get("field"): d for d in dims}
    tables = [by_field.get(f, {}).get("name") for f in kfields]
    labels = [(_dim_labels(ds, ver, t) if t else {}) for t in tables]

    mlab = {}
    for obs in meta["defaults"].get("observations") or []:
        for m in obs.get("measures") or []:
            if m.get("code") is not None and m.get("label"):
                mlab[str(m["code"])] = m["label"]

    report = (meta.get("title") or ds).strip()
    files = [f for f in glob.glob(os.path.join(ROOT, "data", "clean_full", source_id, "*.parquet"))
             if not f.endswith("__series.parquet")]
    if not files:
        return {}, {"reason": "no store file"}

    # DO NOT SCAN THE STORE WHEN THE CATALOGUE ALREADY NAMES THE TARGETS. This query
    # materialises every distinct series_key in the file, and the giants have hundreds of
    # millions: on unctad_biotrademerch it took the process to a 79 GB resident set before
    # it was killed, to build a list that is then thrown away, because the loop below
    # iterates the CATALOGUE ids. The store is only needed when a source has no catalogue
    # rows at all, which is the --write-a-new-source case.
    keep = _catalogued(source_id)
    keys = []
    if not keep:
        import duckdb
        con = duckdb.connect()
        con.execute("SET temp_directory='%s'" %
                    os.path.join(ROOT, "data", "_duckdb_spill", "titles").replace("\\", "/"))
        keys = [r[0] for r in con.execute(
            "SELECT DISTINCT series_key FROM read_parquet(%r)" % files[0]).fetchall()]
        con.close()

    # TITLE WHAT THE CATALOGUE LISTS, NOT WHAT THE STORE HOLDS. Filtering store keys by the
    # catalogue works only while the two share a grain. Sixteen sources catalogue COARSER
    # than their store - oceantrade lists "0000.01.O_A" (Economy/Flow/Product, Partner summed
    # out) against store keys like "0000.01.O_A.004.M5040" - so every store key was filtered
    # away and the source titled zero rows. The catalogue ids ARE the thing being titled.
    targets = sorted(c.split(":", 1)[1] for c in keep) if keep else sorted(str(k) for k in keys)
    out, skipped = {}, 0
    for k in targets:
        ks = str(k)
        # PARSE FROM THE RIGHT, AND DO NOT ASSUME "." SEPARATES DIMENSIONS. A dimension CODE
        # can itself contain a dot - US.CommodityPrice_A keys read "090100.01.M7110", where
        # "090100.01" is ONE commodity code - so splitting on "." over-segments the key and
        # threw away every series in those datasets. Only the trailing ".M<measure>" is a
        # reliable delimiter; the remaining prefix is resolved against the published code
        # lists, longest match first, so a code with a dot is matched as the code it is.
        # TWO KEY SHAPES, BOTH REAL. Most sources catalogue at full grain and end in the
        # measure ("076.M1900"); the coarser sixteen carry only leading dimensions and no
        # measure at all ("A01.0000" = Category/Economy, Partner and Flow summed out).
        # Requiring the ".M" suffix skipped every one of the latter.
        i = ks.rfind(".M")
        ml = mlab.get(ks[i + 2:]) if i >= 0 else None
        prefix = ks[:i] if ml else ks
        n_dims = min(len(kfields), prefix.count(".") + 1)
        parts, rest, ok = [], prefix, True
        for di in range(n_dims):
            lut = labels[di]
            if di == n_dims - 1:                  # last PRESENT dim takes the whole remainder
                lab = lut.get(rest)
                if not lab:
                    ok = False
                    break
                parts.append(lab)
                rest = ""
                break
            cand = None
            for c in lut:                          # longest published code that prefixes rest
                if rest.startswith(c + ".") and (cand is None or len(c) > len(cand)):
                    cand = c
            if cand is None:
                ok = False
                break
            parts.append(lut[cand])
            rest = rest[len(cand) + 1:]
        if not ok or rest:
            skipped += 1
            continue
        title = f"{report} - {', '.join(parts)}" + (f" ({ml})" if ml else "")
        out[f"{source_id}:{k}"] = title
    return out, {"keys": len(keys), "skipped": skipped, "dims": tables, "report": report}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("sources", nargs="+")
    ap.add_argument("--write", action="store_true")
    a = ap.parse_args()
    import yaml
    reg = {x["source_id"]: x for x in yaml.safe_load(
        open(os.path.join(ROOT, "updater", "registry.yaml"), encoding="utf-8"))["sources"]}
    total = 0
    for sid in a.sources:
        ds = _ds_for(sid, reg)
        if not ds:
            print(f"  {sid:34} no UNCTAD dataset in its registry vintage_signal - skipped")
            continue
        try:
            t, info = titles_for(sid, ds)
        except Exception as e:                                   # noqa: BLE001
            print(f"  {sid:34} FAILED {type(e).__name__}: {str(e)[:70]}")
            continue
        print(f"  {sid:34} ds={ds:28} titled={len(t):>7,} skipped={info.get('skipped',0):>6,}")
        if t:
            ex = next(iter(t.items()))
            print(f"       e.g. {ex[0][:44]} -> {ex[1][:96]}")
        if a.write and t:
            p = os.path.join(ROOT, "dist", "titles", f"{sid}.json")
            json.dump(t, open(p, "w", encoding="utf-8"), ensure_ascii=False, indent=0, sort_keys=True)
        total += len(t)
    print(f"  total titles composed: {total:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
