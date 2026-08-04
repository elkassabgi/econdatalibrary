# -*- coding: utf-8 -*-
"""
Deterministic titler for IMF SDMX source `imf_apdreo`
(Regional Economic Outlook: Asia and Pacific).

Titles are built ONLY from official IMF labels mirrored faithfully by DBnomics
(dataset IMF/APDREO), matching our legacy ingest vintage. The live IMF API uses
different codes, so we deliberately use DBnomics.

Key structure (after the 2nd colon in our series_id):
    FREQ . REF_AREA . INDICATOR        e.g.  A.AU.BCA_GDP_BP6

Composition (sibling convention):
    "<INDICATOR label> - <REF_AREA label>"
    - FREQ is omitted.
    - There is NO separate unit/sector/counterpart dimension in APDREO; the unit
      is already embedded verbatim inside the INDICATOR label, so no parenthetical
      is added.
    - Every code-derived token is the EXACT official label (verbatim). Only outer
      whitespace is normalized. No paraphrasing / translation / invention.
    - If any component lacks an official label, the series is OMITTED (left raw).
"""
import json
import sqlite3
import urllib.request

CATALOG_DB = r"D:\research\econfindatalibrary\data\catalog.db"
OUT_PATH = r"D:\research\econfindatalibrary\dist\titles\imf_apdreo.json"
SOURCE_ID = "imf_apdreo"
DBNOMICS_DATASET = "https://api.db.nomics.world/v22/datasets/IMF/APDREO"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")


def fetch_labels():
    """Return dict: dim -> {code: official_label} from DBnomics (faithful mirror)."""
    req = urllib.request.Request(DBNOMICS_DATASET, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    doc = data["datasets"]["docs"][0]
    order = doc["dimensions_codes_order"]
    assert order == ["FREQ", "REF_AREA", "INDICATOR"], f"unexpected dim order: {order}"
    return doc["dimensions_values_labels"]


def read_keys():
    """Return list of SDMX keys (dot-part after the 2nd colon)."""
    con = sqlite3.connect(CATALOG_DB)
    try:
        rows = con.execute(
            "SELECT series_id FROM series WHERE source_id=? ORDER BY series_id",
            (SOURCE_ID,),
        ).fetchall()
    finally:
        con.close()
    return [r[0] for r in rows]


def main():
    labels = fetch_labels()
    ind_lbl = labels["INDICATOR"]
    area_lbl = labels["REF_AREA"]

    series_ids = read_keys()
    titles = {}
    omitted = []

    for sid in series_ids:
        key = sid.split(":", 2)[2]          # FREQ.REF_AREA.INDICATOR
        parts = key.split(".")
        if len(parts) < 3:
            omitted.append((sid, "unexpected key shape"))
            continue
        freq, area = parts[0], parts[1]
        indicator = ".".join(parts[2:])     # INDICATOR is the remainder (it never
                                            # contains dots, but be defensive)

        ind = ind_lbl.get(indicator)
        area_name = area_lbl.get(area)
        # VERBATIM RULE: omit any series whose code lacks an official label.
        if ind is None or area_name is None:
            omitted.append((sid, f"missing label ind={ind is None} area={area_name is None}"))
            continue

        title = f"{ind.strip()} - {area_name.strip()}"
        titles[sid] = title

    # Write deterministic, UTF-8, ensure_ascii=False, sorted keys.
    titles = {k: titles[k] for k in sorted(titles)}
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(titles, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")

    print(f"series in catalog : {len(series_ids)}")
    print(f"titled            : {len(titles)}")
    print(f"omitted           : {len(omitted)}")
    for sid, why in omitted:
        print("  OMIT", sid, "->", why)
    print(f"wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()
