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


def _ds_for(source_id: str, reg: dict):
    vs = str(((reg.get(source_id) or {}).get("adapter") or {}).get("vintage_signal", ""))
    m = re.search(r"reportMetadata/([A-Za-z0-9_.]+)/", vs)
    return m.group(1) if m else None


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

    import duckdb
    con = duckdb.connect()
    con.execute("SET temp_directory='%s'" %
                os.path.join(ROOT, "data", "_duckdb_spill", "titles").replace("\\", "/"))
    keys = [r[0] for r in con.execute(
        "SELECT DISTINCT series_key FROM read_parquet(%r)" % files[0]).fetchall()]
    con.close()

    out, skipped = {}, 0
    for k in keys:
        segs = str(k).split(".")
        if len(segs) != len(kfields) + 1 or not segs[-1].startswith("M"):
            skipped += 1
            continue
        parts, ok = [], True
        for i, seg in enumerate(segs[:-1]):
            lab = labels[i].get(seg)
            if not lab:
                ok = False
                break
            parts.append(lab)
        ml = mlab.get(segs[-1][1:])
        if not ok or not ml:
            skipped += 1
            continue
        out[f"{source_id}:{k}"] = f"{report} - {', '.join(parts)} ({ml})"
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
