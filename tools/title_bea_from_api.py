#!/usr/bin/env python3
"""Give BEA's remaining coded series their published names, from BEA's own API.

2,103 of bea's 913,230 catalogue rows carry their series code as their title — `bea:A134RA:A`
titled "A134RA:A". They are not mysterious: `jobs/ingest_bea_full.py` reads `SeriesCode` out of
every NIPA/NIUnderlyingDetail/FixedAssets row and discards the `LineDescription` sitting beside
it, so the name was in hand at ingest time and thrown away. This asks BEA for it again.

WHY A SINGLE YEAR PER TABLE, NOT Year=ALL. The description is a property of the LINE, not of
the observation, so one year of a table names every code that table carries. T10105 for 2023 is
26 rows; the same call with Year=ALL is thousands, for identical descriptions. Codes that are
absent from the probe year — discontinued lines — are retried across a spread of older years
rather than by pulling every year of every table.

NOTHING IS INVENTED. A code BEA does not name keeps its code as its title, and the run reports
how many those are. --apply writes; without it nothing changes.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG = os.path.join(ROOT, "data", "catalog.db")
CACHE = os.path.join(ROOT, "data", "_bea_line_descriptions.json")
API = "https://apps.bea.gov/api/data"

DATASETS = {
    "NIPA": ("A", "Q", "M"),
    "NIUnderlyingDetail": ("A", "Q", "M"),
    "FixedAssets": (None,),
}

# Probe years, newest first. A current line is named by the first; the older years exist for
# lines BEA has since discontinued, which is exactly what an untitled code tends to be.
PROBE_YEARS = ("2023", "2010", "1995", "1975")

SLEEP = 0.4          # BEA allows 100 requests/minute; this sits comfortably inside it


def _key() -> str:
    for f in (".env", ".env.local"):
        p = os.path.join(ROOT, f)
        if not os.path.exists(p):
            continue
        for line in open(p, encoding="utf-8"):
            if line.startswith("BEA_API_KEY="):
                return line.split("=", 1)[1].strip()
    raise SystemExit("BEA_API_KEY not found in .env / .env.local")


def _get(key: str, **params):
    params.update({"UserID": key, "ResultFormat": "JSON"})
    for attempt in range(4):
        try:
            r = requests.get(API, params=params, timeout=180)
            if r.status_code == 200:
                res = r.json().get("BEAAPI", {}).get("Results", {})
                if isinstance(res, dict) and res.get("Error"):
                    return []
                return res.get("Data") or res.get("ParamValue") or []
        except requests.RequestException:
            pass
        time.sleep(2 ** attempt)
    return []


def _tables(key: str, dataset: str) -> list[str]:
    vals = _get(key, method="GetParameterValues", datasetname=dataset, ParameterName="TableName")
    out = []
    for v in vals:
        t = v.get("TableName") or v.get("Key")
        if t:
            out.append(str(t))
    return sorted(set(out))


def _untitled(con) -> set[str]:
    """Series codes whose catalogue title is still just the code.

    Keyed on `title == the id, or the id minus its source prefix` — no character class, because
    a metric built from what codes 'usually look like' misses the ones that do not (R474).
    """
    rows = con.execute("""
        SELECT series_id FROM series
        WHERE source_id='bea' AND (title IS NULL OR title='' OR title=series_id
           OR title = substr(series_id, instr(series_id,':')+1))
    """).fetchall()
    return {r[0] for r in rows}


def _code_of(series_id: str) -> str:
    """bea:A134RA:A -> A134RA ; bea:XYZ -> XYZ"""
    rest = series_id.split(":", 1)[1] if ":" in series_id else series_id
    return rest.split(":", 1)[0]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--max-tables", type=int, default=0, help="0 = every table")
    a = ap.parse_args()

    key = _key()
    con = sqlite3.connect(CATALOG, timeout=180)
    con.execute("PRAGMA busy_timeout=180000")

    untitled = _untitled(con)
    want = {_code_of(s) for s in untitled}
    print(f"  untitled bea rows: {len(untitled):,}  distinct codes: {len(want):,}")

    names: dict[str, str] = {}
    if os.path.exists(CACHE):
        names = json.load(open(CACHE, encoding="utf-8"))
        print(f"  cache: {len(names):,} code->description already harvested")

    calls = 0
    for dataset, freqs in DATASETS.items():
        tables = _tables(key, dataset)
        if a.max_tables:
            tables = tables[:a.max_tables]
        print(f"  {dataset}: {len(tables)} tables")
        for ti, table in enumerate(tables, 1):
            if not (want - set(names)):
                break
            for year in PROBE_YEARS:
                if not (want - set(names)):
                    break
                for fr in freqs:
                    p = dict(method="GetData", datasetname=dataset, TableName=table, Year=year)
                    if fr:
                        p["Frequency"] = fr
                    rows = _get(key, **p)
                    calls += 1
                    time.sleep(SLEEP)
                    got = 0
                    for row in rows:
                        code = row.get("SeriesCode")
                        desc = (row.get("LineDescription") or "").strip()
                        if code and desc and code not in names:
                            names[str(code)] = desc
                            got += 1
                    if got:
                        break        # this year named the table's lines; older years unneeded
            if ti % 25 == 0:
                remaining = len(want - set(names))
                print(f"    {dataset} {ti}/{len(tables)}  harvested={len(names):,}  "
                      f"still-unnamed targets={remaining:,}  calls={calls}")
                json.dump(names, open(CACHE, "w", encoding="utf-8"), indent=0, sort_keys=True)

    json.dump(names, open(CACHE, "w", encoding="utf-8"), indent=0, sort_keys=True)
    hit = want & set(names)
    print(f"  harvested {len(names):,} descriptions in {calls} calls")
    print(f"  of the {len(want):,} untitled codes, BEA names {len(hit):,} "
          f"({len(want) - len(hit):,} it does not name — those keep their code)")

    updates = []
    for sid in sorted(untitled):
        d = names.get(_code_of(sid))
        if not d:
            continue
        freq = sid.split(":")[2] if sid.count(":") >= 2 else ""
        updates.append((f"{d} ({freq})" if freq else d, sid))
    print(f"  would update {len(updates):,} catalogue rows")
    for t, sid in updates[:5]:
        print(f"    {sid:<28} -> {t[:66]!r}")

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
