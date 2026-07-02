#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Deterministic titler for source imf_hpdd (IMF Historical Public Debt Database).

Labels are taken VERBATIM from the DBnomics faithful mirror of IMF/HPDD
(dataset.dimensions_values_labels), which matches our legacy ingest vintage.
The live IMF API uses different codes, so it is NOT used here.

SDMX key layout (dimensions_codes_order): FREQ.REF_AREA.INDICATOR
Title composition: "<INDICATOR label> - <REF_AREA label>"
  - lead with the indicator label
  - append " - <area>"
  - FREQ is omitted
  - no UNIT/sector/counterpart dimension exists in this dataset, so none is added
VERBATIM RULE: every code-derived token is the exact official label. If a code
has no official label, the series is OMITTED (left raw); nothing is paraphrased,
translated, or invented.

Output: {series_id: title}, UTF-8, ensure_ascii=False, sorted by series_id.
"""
import json
import sqlite3
import urllib.request

SOURCE_ID = "imf_hpdd"
CATALOG_DB = r"D:\research\econfindatalibrary\data\catalog.db"
OUT_PATH = r"D:\research\econfindatalibrary\dist\titles\imf_hpdd.json"
DATASET_URL = "https://api.db.nomics.world/v22/datasets/IMF/HPDD"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")


def fetch_dimensions_values_labels():
    req = urllib.request.Request(DATASET_URL, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    ds = data["datasets"]["docs"][0]
    order = ds["dimensions_codes_order"]  # ['FREQ', 'REF_AREA', 'INDICATOR']
    dvl = ds["dimensions_values_labels"]
    return order, dvl


def load_series_ids():
    con = sqlite3.connect(CATALOG_DB)
    try:
        rows = con.execute(
            "SELECT series_id FROM series WHERE source_id=?;", (SOURCE_ID,)
        ).fetchall()
    finally:
        con.close()
    return [r[0] for r in rows]


def main():
    order, dvl = fetch_dimensions_values_labels()
    # Map dimension position -> dimension code per official order.
    pos = {i: dim for i, dim in enumerate(order)}

    series_ids = load_series_ids()
    titles = {}
    for sid in series_ids:
        # id = imf_hpdd:<PROVIDER_CODE>:<seg1>.<seg2>...  key is dot-part after 2nd colon
        parts = sid.split(":", 2)
        if len(parts) != 3:
            continue
        key = parts[2]
        segs = key.split(".")
        if len(segs) != len(order):
            continue

        # Resolve each dimension code to its official label (verbatim).
        labels = {}
        ok = True
        for i, code in enumerate(segs):
            dim = pos[i]
            label = dvl.get(dim, {}).get(code)
            if label is None:
                # No official label for this code -> omit series (leave raw).
                ok = False
                break
            labels[dim] = label
        if not ok:
            continue

        indicator = labels["INDICATOR"]
        area = labels["REF_AREA"]
        # FREQ omitted by design.
        title = f"{indicator} - {area}"
        titles[sid] = title

    titles = dict(sorted(titles.items(), key=lambda kv: kv[0]))
    import os
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(titles, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")

    print(f"series in catalog: {len(series_ids)}")
    print(f"titled: {len(titles)}")
    print(f"omitted: {len(series_ids) - len(titles)}")
    print(f"wrote: {OUT_PATH}")


if __name__ == "__main__":
    main()
