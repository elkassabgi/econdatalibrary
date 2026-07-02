#!/usr/bin/env python3
"""Deterministic titler for source `unctad_tabmscioeaiopa`.

Dataset: UNCTAD / TABMSCIOEAIOPA
  "Trade and Biodiversity - Market structural change indices of exports and
   imports of products, annual"

All labels are taken VERBATIM from the DBnomics faithful mirror of UNCTADstat
labels (dimensions_values_labels), the exact vintage our data was ingested from.

Catalog series_id format:
    unctad_tabmscioeaiopa:UNCTAD_TABMSCIOEAIOPA:<freq>.<flow>.<measure>.<product>

DBnomics dimensions_codes_order: ['frequency', 'product', 'flow', 'measure']
Key segment order (as stored):   frequency . flow . measure . product

Title style (mirrors sibling dist/titles/unctad_rfia.json):
    lead with the main indicator/measure label, then append the varying
    dimension labels joined by " - ":
        "<Measure> - <Flow> - <Product>"
There is no unit dimension in this dataset.

VERBATIM rule: every code-derived token is the exact official label. No
paraphrase, no separator normalization, no invention. If any code in a series
has no official label, that series is OMITTED (left raw).
"""

import json
import sqlite3
from pathlib import Path

CATALOG_DB = Path(r"D:/research/econfindatalibrary/data/catalog.db")
DBN_JSON = Path(
    r"D:/temp/claude/D--research-hfdatalibrary/"
    r"b9dda646-3b45-4dc7-96e1-c9b3efa36387/scratchpad/dbn.json"
)
OUT_PATH = Path(r"D:/research/econfindatalibrary/dist/titles/unctad_tabmscioeaiopa.json")
SOURCE_ID = "unctad_tabmscioeaiopa"


def load_labels():
    """Return per-dimension {code: label} dicts from the DBnomics mirror."""
    with DBN_JSON.open(encoding="utf-8") as fh:
        doc = json.load(fh)["datasets"]["docs"][0]
    assert doc["code"] == "TABMSCIOEAIOPA", doc["code"]
    dvl = doc["dimensions_values_labels"]
    return {
        "frequency": dict(dvl["frequency"]),
        "flow": dict(dvl["flow"]),
        "measure": dict(dvl["measure"]),
        "product": dict(dvl["product"]),
    }


def load_series_ids():
    con = sqlite3.connect(str(CATALOG_DB))
    try:
        rows = con.execute(
            "SELECT series_id FROM series WHERE source_id=? ORDER BY series_id",
            (SOURCE_ID,),
        ).fetchall()
    finally:
        con.close()
    return [r[0] for r in rows]


def main():
    labels = load_labels()
    freq_l = labels["frequency"]
    flow_l = labels["flow"]
    meas_l = labels["measure"]
    prod_l = labels["product"]

    titles = {}
    omitted = 0
    for sid in load_series_ids():
        parts = sid.split(":")
        if len(parts) < 3:
            omitted += 1
            continue
        key = parts[2]
        segs = key.split(".")
        if len(segs) != 4:
            omitted += 1
            continue
        freq, flow, meas, prod = segs

        # every code must resolve to an official verbatim label, else omit
        if freq not in freq_l or flow not in flow_l or meas not in meas_l or prod not in prod_l:
            omitted += 1
            continue

        # lead with the main indicator (measure), then varying dims: flow, product
        title = f"{meas_l[meas]} - {flow_l[flow]} - {prod_l[prod]}"
        titles[sid] = title

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    # deterministic: sort keys
    ordered = {k: titles[k] for k in sorted(titles)}
    with OUT_PATH.open("w", encoding="utf-8") as fh:
        json.dump(ordered, fh, ensure_ascii=False, indent=1)
        fh.write("\n")

    print(f"titled={len(ordered)} omitted={omitted} -> {OUT_PATH}")


if __name__ == "__main__":
    main()
