#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Deterministic titler for IMF SDMX source `imf_mcdreo`.

Labels are sourced ONLY from the DBnomics faithful mirror of IMF/MCDREO
(dimensions_values_labels), which matches our legacy ingest vintage. The live
IMF API uses different codes, so it is intentionally NOT consulted.

SDMX dimension order (from DBnomics dimensions_codes_order):
    FREQ . REF_AREA . INDICATOR

Series id format in catalog:
    imf_mcdreo:IMF_MCDREO:<FREQ>.<REF_AREA>.<INDICATOR>
The SDMX key is the dot-part after the 2nd colon.

Composition (matching confirmed IMF SDMX siblings):
    lead with the INDICATOR label (which already embeds the unit in prose,
    e.g. "billions of US dollars", "in percent of GDP", "percent change"),
    then append " - <REF_AREA label>". FREQ is omitted.

VERBATIM RULE: every code-derived token is the EXACT official DBnomics label.
If any code lacks an official label, that series is left untitled (omitted).
Nothing is paraphrased, translated, or invented.
"""
import json
import sqlite3

CATALOG_DB = r"D:\research\econfindatalibrary\data\catalog.db"
DATASET_JSON = r"D:\research\econfindatalibrary\data\clean_full\imf_mcdreo\_dbnomics_dataset.json"
OUT_PATH = r"D:\research\econfindatalibrary\dist\titles\imf_mcdreo.json"
SOURCE_ID = "imf_mcdreo"


def load_labels(dataset_json_path):
    with open(dataset_json_path, encoding="utf-8") as fh:
        doc = json.load(fh)
    d = doc["datasets"]["docs"][0]
    dvl = d["dimensions_values_labels"]
    order = d["dimensions_codes_order"]  # ["FREQ","REF_AREA","INDICATOR"]
    return order, dvl


def main():
    order, dvl = load_labels(DATASET_JSON)
    assert order == ["FREQ", "REF_AREA", "INDICATOR"], order
    area_lbl = dvl["REF_AREA"]
    ind_lbl = dvl["INDICATOR"]

    conn = sqlite3.connect(CATALOG_DB)
    series_ids = sorted(
        r[0]
        for r in conn.execute(
            "SELECT series_id FROM series WHERE source_id=?", (SOURCE_ID,)
        ).fetchall()
    )
    conn.close()

    titles = {}
    skipped = []
    for sid in series_ids:
        parts = sid.split(":")
        if len(parts) < 3:
            skipped.append(sid)
            continue
        key = parts[2]
        segs = key.split(".")
        if len(segs) != 3:
            skipped.append(sid)
            continue
        _freq, area, ind = segs  # FREQ omitted per spec
        # VERBATIM: require official labels for both qualifier dims; else omit
        if area not in area_lbl or ind not in ind_lbl:
            skipped.append(sid)
            continue
        title = "{indicator} - {area}".format(
            indicator=ind_lbl[ind], area=area_lbl[area]
        )
        titles[sid] = title

    titles = dict(sorted(titles.items()))
    with open(OUT_PATH, "w", encoding="utf-8") as fh:
        json.dump(titles, fh, ensure_ascii=False, indent=2, sort_keys=True)
        fh.write("\n")

    print("series in catalog:", len(series_ids))
    print("titled:", len(titles))
    print("skipped (no official label):", len(skipped))
    print("wrote:", OUT_PATH)


if __name__ == "__main__":
    main()
