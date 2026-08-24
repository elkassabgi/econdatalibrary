#!/usr/bin/env python3
"""Title Riksbank's 47 SEK exchange-rate series from Riksbank's own descriptions.

The untitled rows are all `SEK<CUR>PMI` — SEKCZKPMI, SEKEURPMI, SEKDKKPMI. Riksbank's Series
endpoint names every one of them, but not in the field the existing titles came from:

    seriesId          shortDescription   midDescription                  longDescription
    SECBREPOEFF       "Policy rate"      "Policy rate"                   "The policy rate is ..."
    SEKCZKPMI         "CZK"              "CZK Czech Republic, koruna"    "Czech koruna"

The already-titled rba-style rows used `shortDescription`, which is right for "Policy rate" and
useless for an exchange rate — it yields the bare code "CZK". `midDescription` carries the
country and the currency, so it is used here.

THE CURRENCY PAIR IS READ FROM RIKSBANK'S OWN ID, not supplied by me. `SEKCZKPMI` is SEK against
CZK, so the title leads with SEK/CZK and then gives Riksbank's description:

    SEK/CZK — CZK Czech Republic, koruna

A series the API does not list keeps its key, and the run reports how many.

--apply writes; without it nothing changes.
"""
from __future__ import annotations

import argparse
import io
import json
import os
import re
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG = os.path.join(ROOT, "data", "catalog.db")
CACHE = os.path.join(ROOT, "data", "_riksbank_series.json")
API = "https://api.riksbank.se/swea/v1/Series"

PAIR = re.compile(r"^SEK([A-Z]{3})PMI$")


def series_meta() -> dict:
    if os.path.exists(CACHE):
        return json.load(io.open(CACHE, encoding="utf-8"))
    import requests
    r = requests.get(API, timeout=180)
    r.raise_for_status()
    rows = r.json()
    out = {str(x["seriesId"]): x for x in rows if x.get("seriesId")}
    json.dump(out, io.open(CACHE, "w", encoding="utf-8"), indent=0, sort_keys=True,
              ensure_ascii=False)
    print(f"  Riksbank lists {len(out)} series -> {CACHE}")
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    meta = series_meta()
    con = sqlite3.connect(CATALOG, timeout=600)
    con.execute("PRAGMA busy_timeout=600000")
    rows = con.execute("""SELECT series_id FROM series WHERE source_id='riksbank'
        AND (title IS NULL OR title='' OR title=series_id
          OR title = substr(series_id, instr(series_id,':')+1))""").fetchall()
    ids = [r[0] for r in rows]
    print(f"  untitled riksbank rows: {len(ids):,}")

    updates, unlisted, nodesc = [], [], []
    for sid in ids:
        code = sid.split(":")[-1]
        m = meta.get(code)
        if not m:
            unlisted.append(code)
            continue
        desc = (m.get("midDescription") or m.get("longDescription")
                or m.get("shortDescription") or "").strip()
        if not desc:
            nodesc.append(code)
            continue
        p = PAIR.match(code)
        title = f"SEK/{p.group(1)} — {desc}" if p else desc
        updates.append((title, sid))

    print(f"  titles to write : {len(updates):,}")
    if unlisted:
        print(f"  not listed by the API: {len(unlisted)}  {sorted(set(unlisted))[:8]}")
    if nodesc:
        print(f"  listed but undescribed: {len(nodesc)}")
    for t, sid in updates[:4]:
        print(f"    {sid.split(':')[-1]:<14} -> {t[:66]!r}")

    if a.apply and updates:
        con.executemany("UPDATE series SET title=? WHERE series_id=?", updates)
        con.commit()
        print(f"  APPLIED {len(updates):,} titles")
    elif not a.apply:
        print("  DRY RUN — re-run with --apply")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
