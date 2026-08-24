#!/usr/bin/env python3
"""Title EIA's nuclear-status series with EIA's own facility names and column labels.

522 rows read as their key — `NUC_STATUS.CAP.1060`. The key is
`NUC_STATUS.<metric>.<facility>[-<generator>]`, and EIA publishes both halves:

  * FACILITY — /v2/nuclear-outages/generator-nuclear-outages/facet/facility/ maps the numeric id
    to a plant name. 1060 is Duane Arnold, 204 is Clinton Power Station, 1590 is Pilgrim Nuclear
    Power Station.
  * METRIC — the same dataset declares its data columns as `capacity`, `outage` and
    `percentOutage`, which are exactly the three codes in our keys (CAP, OUT, OUT_PCT), 174 of
    each.

So a title is EIA's plant name and EIA's column label, and the generator suffix is carried as
written:

    Duane Arnold — Capacity
    Duane Arnold, generator 1 — Capacity
    Pilgrim Nuclear Power Station — Percent outage

A facility id EIA does not name keeps its id, and the run reports how many.

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
CACHE = os.path.join(ROOT, "data", "_eia_nuclear_facilities.json")
API = "https://api.eia.gov/v2/nuclear-outages/generator-nuclear-outages/facet/facility/"

# EIA's own data-column names for this dataset, in EIA's own words.
METRIC = {"CAP": "Capacity", "OUT": "Outage", "OUT_PCT": "Percent outage"}


def _key() -> str:
    for f in (".env", ".env.local"):
        p = os.path.join(ROOT, f)
        if not os.path.exists(p):
            continue
        for line in open(p, encoding="utf-8"):
            if line.startswith("EIA_API_KEY="):
                return line.split("=", 1)[1].strip().strip('"')
    raise SystemExit("EIA_API_KEY not found")


def facilities() -> dict:
    if os.path.exists(CACHE):
        return json.load(io.open(CACHE, encoding="utf-8"))
    import requests
    r = requests.get(API, params={"api_key": _key()}, timeout=180)
    r.raise_for_status()
    facets = (r.json().get("response") or {}).get("facets") or []
    out = {str(f.get("id")): str(f.get("name")).strip()
           for f in facets if f.get("id") is not None and f.get("name")}
    json.dump(out, io.open(CACHE, "w", encoding="utf-8"), indent=0, sort_keys=True,
              ensure_ascii=False)
    print(f"  EIA names {len(out)} nuclear facilities -> {CACHE}")
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    names = facilities()
    con = sqlite3.connect(CATALOG, timeout=600)
    con.execute("PRAGMA busy_timeout=600000")
    rows = con.execute("""SELECT series_id FROM series WHERE source_id='eia'
        AND series_id LIKE 'eia:NUC_STATUS%'
        AND (title IS NULL OR title='' OR title=series_id
          OR title = substr(series_id, instr(series_id,':')+1))""").fetchall()
    ids = [r[0] for r in rows]
    print(f"  untitled NUC_STATUS rows: {len(ids):,}")

    updates, no_fac, no_met = [], set(), set()
    for sid in ids:
        key = sid.split(":", 1)[1]
        parts = key.split(".")
        if len(parts) < 3:
            continue
        metric, fac = parts[1], ".".join(parts[2:])
        gen = ""
        if "-" in fac:
            fac, gen = fac.split("-", 1)
        label = METRIC.get(metric)
        plant = names.get(fac)
        if not plant:
            no_fac.add(fac)
            continue
        if not label:
            no_met.add(metric)
            continue
        who = f"{plant}, generator {gen}" if gen else plant
        updates.append((f"{who} — {label}", sid))

    print(f"  titles to write : {len(updates):,}")
    if no_fac:
        print(f"  EIA does not name {len(no_fac)} facility id(s): {sorted(no_fac)[:8]}")
    if no_met:
        print(f"  unknown metric code(s): {sorted(no_met)}")
    for t, sid in updates[:4]:
        print(f"    {sid.split(':',1)[1]:<26} -> {t[:62]!r}")

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
