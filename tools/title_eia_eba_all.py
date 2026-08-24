#!/usr/bin/env python3
"""Title EIA's 82 `EBA.<BA>-ALL` interchange series with EIA's own respondent names.

The titled siblings set the template exactly:

    EBA.AEC-MISO  ->  Actual Net Interchange for PowerSouth Energy Cooperative (AEC) to
                      Midcontinent Independent System Operator, Inc. (MISO), hourly

so `EBA.<BA>-<OTHER>` is interchange between two balancing authorities, and the 82 untitled rows
are the `-ALL` form: one per balancing authority, the interchange against all counterparties
rather than one.

BOTH NAMES COME FROM EIA. /v2/electricity/rto/region-data/facet/respondent/ maps every code to
its name — AEC is PowerSouth Energy Cooperative, AECI is Associated Electric Cooperative, Inc.,
CISO is California Independent System Operator. EIA's `type` facet for the same dataset names
the aggregate concept "Total interchange" (TI), and the dataset itself is described as
"interchange by balancing authority", which is what "all balancing authorities" renders below.

    Actual Net Interchange for PowerSouth Energy Cooperative (AEC) to all balancing
    authorities, hourly

A respondent code EIA does not name keeps its key.

--apply writes; without it nothing changes.
"""
from __future__ import annotations

import argparse
import io
import json
import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG = os.path.join(ROOT, "data", "catalog.db")
CACHE = os.path.join(ROOT, "data", "_eia_eba_respondents.json")
API = "https://api.eia.gov/v2/electricity/rto/region-data/facet/respondent/"


def _key() -> str:
    for f in (".env", ".env.local"):
        p = os.path.join(ROOT, f)
        if not os.path.exists(p):
            continue
        for line in open(p, encoding="utf-8"):
            if line.startswith("EIA_API_KEY="):
                return line.split("=", 1)[1].strip().strip('"')
    raise SystemExit("EIA_API_KEY not found")


def respondents() -> dict:
    if os.path.exists(CACHE):
        return json.load(io.open(CACHE, encoding="utf-8"))
    import requests
    r = requests.get(API, params={"api_key": _key()}, timeout=180)
    r.raise_for_status()
    facets = (r.json().get("response") or {}).get("facets") or []
    out = {str(f["id"]): str(f["name"]).strip() for f in facets if f.get("id") and f.get("name")}
    json.dump(out, io.open(CACHE, "w", encoding="utf-8"), indent=0, sort_keys=True,
              ensure_ascii=False)
    print(f"  EIA names {len(out)} balancing authorities -> {CACHE}")
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    names = respondents()
    con = sqlite3.connect(CATALOG, timeout=600)
    con.execute("PRAGMA busy_timeout=600000")
    rows = con.execute("""SELECT series_id FROM series WHERE source_id='eia'
        AND series_id LIKE 'eia:EBA.%-ALL'
        AND (title IS NULL OR title='' OR title=series_id
          OR title = substr(series_id, instr(series_id,':')+1))""").fetchall()
    ids = [r[0] for r in rows]
    print(f"  untitled EBA -ALL rows: {len(ids):,}")

    updates, unnamed = [], []
    for sid in ids:
        key = sid.split(":", 1)[1]              # EBA.AEC-ALL
        ba = key[len("EBA."):].rsplit("-ALL", 1)[0]
        nm = names.get(ba)
        if not nm:
            unnamed.append(ba)
            continue
        updates.append((f"Actual Net Interchange for {nm} ({ba}) to all balancing "
                        f"authorities, hourly", sid))

    print(f"  titles to write : {len(updates):,}")
    if unnamed:
        print(f"  EIA does not name: {sorted(set(unnamed))}")
    for t, sid in updates[:3]:
        print(f"    {sid.split(':',1)[1]:<18} -> {t[:78]!r}")

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
