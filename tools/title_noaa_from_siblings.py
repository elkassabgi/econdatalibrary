#!/usr/bin/env python3
"""Title NOAA's remaining 330 station-series from names we already hold, plus 8 API lookups.

A noaa key is `<dataset>:<station>:<datatype>` and a titled row reads
`TABLE ROCK RSVR — Cooling Degree Days — Monthly`. So the catalogue itself already knows what
almost every station and datatype is called: 3,137,829 titled rows teach 127,927 station names
and 120 datatype names. 274 of the 330 untitled rows are therefore composable with no network
access at all — the names were already in the building.

The remaining 56 rows belong to 8 stations that appear ONLY in untitled rows, so there is
nothing to learn them from. Those are fetched from NOAA's own search service, whose
`results[].stations[].name` carries the official name ("GRAND LAKE 1.7 W, CO US"). Eight
requests, cached.

WHY BORROW RATHER THAN RE-FETCH EVERYTHING. Re-deriving 127,927 station names from the API
would be thousands of requests to reproduce strings we are already serving. Reading them back
out of our own titled rows is exact, because those titles were built from the same NOAA
metadata — and if a title is wrong, re-fetching would not tell us, since it would agree.

A station NOAA does not name is left alone rather than given a placeholder.

--apply writes; without it nothing changes.
"""
from __future__ import annotations

import argparse
import io
import json
import os
import sqlite3
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG = os.path.join(ROOT, "data", "catalog.db")
CACHE = os.path.join(ROOT, "data", "_noaa_station_names.json")
SEARCH = "https://www.ncei.noaa.gov/access/services/search/v1/data"
DATASET_FOR = {"gsom": "global-summary-of-the-month", "gsoy": "global-summary-of-the-year"}


def _untitled(series_id: str, title) -> bool:
    if title is None or title == "":
        return True
    bare = series_id.split(":", 1)[1] if ":" in series_id else series_id
    return title == series_id or title == bare


def fetch_station_name(station: str, dataset: str) -> str | None:
    """NOAA's own name for a station, or None if it does not name it."""
    import requests
    ds = DATASET_FOR.get(dataset, "global-summary-of-the-month")
    try:
        r = requests.get(SEARCH, params={"dataset": ds, "stations": station, "limit": 1},
                         timeout=120)
        if r.status_code != 200:
            return None
        for res in (r.json().get("results") or []):
            for st in (res.get("stations") or []):
                if st.get("id") == station and st.get("name"):
                    return str(st["name"])
    except Exception:                                        # noqa: BLE001
        return None
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    con = sqlite3.connect(CATALOG, timeout=300)
    con.execute("PRAGMA busy_timeout=300000")
    rows = con.execute("SELECT series_id, title FROM series WHERE source_id='noaa'").fetchall()

    station_name: dict[str, str] = {}
    datatype_name: dict[str, str] = {}
    dataset_freq: dict[str, str] = {}
    untitled: list[tuple[str, str, str, str]] = []

    for sid, title in rows:
        key = sid.split(":", 1)[1] if ":" in sid else sid
        parts = key.split(":")
        if len(parts) != 3:
            continue
        dataset, station, dtype = parts
        if _untitled(sid, title):
            untitled.append((sid, dataset, station, dtype))
            continue
        seg = [x.strip() for x in str(title).split("—")]
        if len(seg) >= 2:
            station_name.setdefault(station, seg[0])
            datatype_name.setdefault(dtype, seg[1])
        if len(seg) >= 3:
            dataset_freq.setdefault(dataset, seg[2])

    print(f"  noaa rows {len(rows):,}   untitled {len(untitled):,}")
    print(f"  learned from titled siblings: {len(station_name):,} stations, "
          f"{len(datatype_name)} datatypes, {len(dataset_freq)} dataset labels")

    cache = json.load(io.open(CACHE, encoding="utf-8")) if os.path.exists(CACHE) else {}
    need = sorted({(ds, st) for _sid, ds, st, _dt in untitled
                   if st not in station_name and st not in cache})
    if need:
        print(f"  {len(need)} station(s) appear only in untitled rows — asking NOAA:")
        for ds, st in need:
            nm = fetch_station_name(st, ds)
            cache[st] = nm or ""
            print(f"    {st:<14} -> {nm!r}")
            time.sleep(0.4)
        json.dump(cache, io.open(CACHE, "w", encoding="utf-8"),
                  indent=0, sort_keys=True, ensure_ascii=False)

    updates, skipped = [], 0
    for sid, dataset, station, dtype in untitled:
        sname = station_name.get(station) or cache.get(station) or ""
        dname = datatype_name.get(dtype)
        if not sname or not dname:
            skipped += 1
            continue
        freq = dataset_freq.get(dataset)
        title = f"{sname} — {dname}" + (f" — {freq}" if freq else "")
        updates.append((title, sid))

    print(f"  titles to write : {len(updates):,}")
    print(f"  left alone (no name from either source) : {skipped:,}")
    for t, sid in updates[:5]:
        print(f"    {sid:<34} -> {t[:70]!r}")

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
