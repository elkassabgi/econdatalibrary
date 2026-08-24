#!/usr/bin/env python3
"""Compose dist/titles/fao_*.json from FAO's OWN bulk code lists.

162,769 fao rows carry their id for a title - fao_fo 68,508 of 85,211, fao_qcl 59,077 of
79,315, fao_pp 35,184 of 40,016. Those ids read `FAO_FO:5510.1.1600`, which my
"machine-coded" regex could not see because it has a colon in it (ledger R474); the rows
were untitled the whole time.

THE KEY IS element.area.item, AND THE FORMAT IS ALREADY SET BY THE ROWS THAT ARE TITLED.
`FAO_QCL:5111.1.1016` is titled "Stocks, Goats - Armenia": element 5111 = Stocks, area 1 =
Armenia, item 1016 = Goats. So the shape is "<Element>, <Item> - <Area>", and this fills
the gaps in the same shape rather than inventing a second convention for one source.

The names come from the same bulk archive the fetcher already tracks
(bulks-faostat.fao.org/production/datasets_E.json -> FileLocation), whose normalized CSV
carries Area Code / Area / Item Code / Item / Element Code / Element on every row. A code
FAO does not name leaves that series untitled.
"""
from __future__ import annotations

import csv
import io
import json
import os
import sqlite3
import sys
import zipfile

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE = os.environ.get("FAO_BULK_CACHE") or r"D:\temp\claude\fao"
MANIFEST = "https://bulks-faostat.fao.org/production/datasets_E.json"
# fao_et added 2026-08-24: its 13 untitled rows are element 6078 (Standard Deviation)
# against area codes absent from the maps built for the other domains. FAO's bulk
# manifest lists ET as "Land, Inputs and Sustainability: Temperature change on land".
SOURCES = {"fao_fo": "FO", "fao_qcl": "QCL", "fao_pp": "PP", "fao_et": "ET"}


def _file_location(code: str):
    j = requests.get(MANIFEST, timeout=180, headers={"User-Agent": "econdatalibrary/1.0"}).json()
    out = []
    def walk(o):
        if isinstance(o, dict):
            if o.get("DatasetCode"): out.append(o)
            for v in o.values(): walk(v)
        elif isinstance(o, list):
            for v in o: walk(v)
    walk(j)
    hit = [d for d in out if str(d.get("DatasetCode")) == code]
    return hit[0].get("FileLocation") if hit else None


def _maps(code: str):
    """(element, area, item) code -> name, from FAO's own normalized bulk CSV."""
    os.makedirs(CACHE, exist_ok=True)
    p = os.path.join(CACHE, code + ".zip")
    if not os.path.exists(p):
        url = _file_location(code)
        if not url:
            return None
        r = requests.get(url, timeout=1800, stream=True, headers={"User-Agent": "econdatalibrary/1.0"})
        r.raise_for_status()
        with open(p, "wb") as fh:
            for chunk in r.iter_content(1 << 20):
                fh.write(chunk)
    z = zipfile.ZipFile(p)
    member = [m for m in z.namelist() if m.lower().endswith(".csv") and "normalized" in m.lower()]
    if not member:
        member = [m for m in z.namelist() if m.lower().endswith(".csv")]
    el, ar, it = {}, {}, {}
    with z.open(member[0]) as fh:
        rd = csv.DictReader(io.TextIOWrapper(fh, encoding="utf-8-sig", errors="replace"))
        for row in rd:
            if row.get("Element Code") and row.get("Element"): el.setdefault(row["Element Code"].strip(), row["Element"].strip())
            if row.get("Area Code") and row.get("Area"): ar.setdefault(row["Area Code"].strip(), row["Area"].strip())
            if row.get("Item Code") and row.get("Item"): it.setdefault(row["Item Code"].strip(), row["Item"].strip())
    return el, ar, it


def main() -> int:
    write = "--write" in sys.argv
    want = [a for a in sys.argv[1:] if not a.startswith("--")] or sorted(SOURCES)
    con = sqlite3.connect("file:%s?mode=ro" % os.path.join(ROOT, "data", "catalog.db").replace("\\", "/"),
                          uri=True, timeout=300)
    grand = 0
    for sid_src in want:
        code = SOURCES.get(sid_src)
        if not code:
            print("  %-9s not a known fao bulk source" % sid_src, flush=True); continue
        rows = con.execute(
            "SELECT series_id FROM series WHERE source_id=? AND title = substr(series_id, instr(series_id,':')+1)",
            (sid_src,)).fetchall()
        if not rows:
            print("  %-9s nothing untitled" % sid_src, flush=True); continue
        m = _maps(code)
        if not m:
            print("  %-9s no bulk file location in the manifest" % sid_src, flush=True); continue
        el, ar, it = m
        titles, unnamed = {}, 0
        for (sid,) in rows:
            key = sid.split(":", 2)[-1]            # after "<source>:<PREFIX>:"
            parts = key.split(".")
            # TWO KEY SHAPES, because not every FAO domain has an Item dimension. FO/QCL/PP are
            # element.area.item; ET (Temperature change on land) has no Item column at all, so
            # its keys are element.area - 6078.186. Rejecting anything that is not 3 parts made
            # all 13 fao_et rows report as unnamed_codes while FAO named every one of them:
            # element 6078 is Standard Deviation, area 274 is Guernsey, area 283 is Jersey.
            if len(parts) == 3:
                e, a, i = (el.get(parts[0]), ar.get(parts[1]), it.get(parts[2]))
                if e and a and i:
                    titles[sid] = "%s, %s - %s" % (e, i, a)
                else:
                    unnamed += 1
            elif len(parts) == 2:
                e, a = (el.get(parts[0]), ar.get(parts[1]))
                if e and a:
                    titles[sid] = "%s - %s" % (e, a)      # matches the titled rows exactly
                else:
                    unnamed += 1
            else:
                unnamed += 1
        print("  %-9s untitled=%-8s titled=%-8s unnamed_codes=%s  (element/area/item maps: %d/%d/%d)"
              % (sid_src, format(len(rows), ","), format(len(titles), ","), format(unnamed, ","),
                 len(el), len(ar), len(it)), flush=True)
        if titles:
            k = next(iter(titles))
            print("       e.g. %-34s %s" % (k.split(":", 1)[1][:34], titles[k][:70]), flush=True)
        if write and titles:
            json.dump(titles, open(os.path.join(ROOT, "dist", "titles", sid_src + ".json"), "w",
                                   encoding="utf-8"), ensure_ascii=False, indent=0, sort_keys=True)
        grand += len(titles)
    con.close()
    print("  total fao titles composed: %s" % format(grand, ","), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
