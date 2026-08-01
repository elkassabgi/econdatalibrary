"""Build catalogue rows for noaa from the store's own series sidecars.

WHY. 3,135,873 series sit in R2, downloadable by id and invisible to search, because noaa has
never been catalogued. What IS in the catalogue is worse than nothing: ten hand-curated demo
rows keyed `noaa:GSOM:USW00012960:PRCP` - uppercase GSOM, a key format the store has never
used - so all ten are listed and will not download. They also understate what is there, claiming
New York Central Park starts in 1990 when the store holds it from 1869.

EVERYTHING COMES FROM THE STORE, NOT FROM A SECOND FETCH. The `<ds>__<PREFIX>__series.parquet`
sidecars already carry series_key, station, element, station name, lat/lon/elevation, country
code, frequency, n_obs and the real start/end. So the catalogue is a projection of what is
actually published - it cannot describe a series the data does not contain, which is the failure
mode that leaves a search result pointing at a 404.

TITLES DECODE THE ELEMENT CODE, and every label is quoted from a NOAA document (see
tools/noaa_elements.py). Measured over the whole store: 120 distinct codes, 100% resolved. An
unresolvable code would keep its raw code and be counted in the report, never guessed at.

--apply writes; without it this prints what it would do and changes nothing.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sqlite3
import sys
import urllib.request

import duckdb

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools"))

import noaa_elements as E                                      # noqa: E402

SOURCE = "noaa"
LICENSE_ID = "us-public-domain"
COUNTRIES_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-countries.txt"
UA = {"User-Agent": "Econ-Fin Data Library admin@econdatalibrary.com"}
FREQ_WORD = {"M": "Monthly", "A": "Annual"}
BATCH = 50_000


def countries() -> dict:
    """FIPS 2-letter -> country name, from NOAA's own list. Cached under data/raw/noaa."""
    cache = os.path.join(ROOT, "data", "raw", "noaa", "ghcnd-countries.txt")
    txt = None
    if os.path.exists(cache):
        txt = open(cache, encoding="utf-8", errors="replace").read()
    else:
        try:
            txt = urllib.request.urlopen(
                urllib.request.Request(COUNTRIES_URL, headers=UA), timeout=180
            ).read().decode("utf-8", "replace")
            os.makedirs(os.path.dirname(cache), exist_ok=True)
            open(cache, "w", encoding="utf-8").write(txt)
        except Exception as e:                                 # noqa: BLE001
            print(f"country list unavailable ({e!r}) — geography will carry the FIPS code")
            return {}
    out = {}
    for line in txt.splitlines():
        if len(line) > 3:
            out[line[:2].strip()] = line[2:].strip()
    return out


def title_for(name, element, freq, station) -> tuple[str, bool]:
    label, ok = E.label_for(element)
    # A station with no name in ghcnd-stations.txt still needs an identifiable title, so fall
    # back to its GHCN id rather than emitting a title that starts with " — ".
    who = (name or "").strip() or station
    return f"{who} — {label} — {FREQ_WORD.get(freq, freq)}", ok


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=180.0)
    con.execute("PRAGMA busy_timeout = 180000")

    # LICENCE GATE FIRST. A catalogue row is an offer to serve, so it must not exist for a
    # licence that is not reservable - and that is checked, never assumed from the source row.
    lic = con.execute("select reservable, name from license where license_id=?",
                      (LICENSE_ID,)).fetchone()
    if not lic:
        print(f"licence {LICENSE_ID!r} is not in the catalogue — refusing to create rows")
        return 1
    if not lic[0]:
        print(f"licence {LICENSE_ID!r} is NOT reservable ({lic[1]}) — refusing to create rows")
        return 1
    print(f"licence {LICENSE_ID}: reservable={lic[0]}  ok to catalogue")

    cc = countries()
    print(f"country names: {len(cc)}")

    d = os.path.join(ROOT, "data", "clean_full", SOURCE)
    sidecars = [f.replace("\\", "/") for f in glob.glob(os.path.join(d, "*__series.parquet"))]
    if not sidecars:
        print(f"no series sidecars under {d}")
        return 1
    lst = "[" + ",".join(f"'{f}'" for f in sidecars) + "]"
    q = duckdb.connect()
    total = q.execute(f"select count(*) from read_parquet({lst})").fetchone()[0]
    print(f"{len(sidecars)} sidecars, {total:,} series")

    # sqlite LIKE is case-insensitive for ASCII, so 'noaa:gsom:%' also matches 'noaa:GSOM:%'.
    # Compare with substr instead, or the broken demo rows count as valid and survive.
    stale = con.execute(
        "select count(*) from series where source_id=? "
        "and substr(series_id,1,10) not in ('noaa:gsom:','noaa:gsoy:')", (SOURCE,)).fetchone()[0]
    print(f"existing rows with a key the store does not use: {stale:,} (these will be removed)")

    meta = json.dumps({
        "citation_short": "NOAA National Centers for Environmental Information (NCEI).",
        "citation_long": ("NOAA National Centers for Environmental Information, Global Summary "
                          "of the Month / Global Summary of the Year (GSOM/GSOY), derived from "
                          "the Global Historical Climatology Network-Daily dataset. Retrieved "
                          "from the NCEI bulk archive and redistributed by the Elkassabgi Data "
                          "Library."),
        "description_processing": ("Retrieved from NCEI's bulk archive (gsom-latest.tar.gz / "
                                   "gsoy-latest.tar.gz), melted from one wide CSV per station "
                                   "to a long {series_key, obs_date, value} schema, and stored "
                                   "as zstd Parquet sharded by GHCN country prefix."),
    }, ensure_ascii=False)

    cur = q.execute(f'''select series_key, station, element, name, frequency,
                               "start", "end", country_code, latitude, longitude, elevation
                        from read_parquet({lst})''')

    written = unresolved = 0
    samples = []
    while True:
        chunk = cur.fetchmany(BATCH)
        if not chunk:
            break
        rows = []
        for (sk, station, element, name, freq, start, end, ccode, lat, lon, elev) in chunk:
            title, ok = title_for(name, element, freq, station)
            if not ok:
                unresolved += 1
            geo = cc.get((ccode or "").strip(), (ccode or "").strip())
            rows.append((f"{SOURCE}:{sk}", SOURCE, title, freq, None, geo, "Climate",
                         LICENSE_ID, start, end, meta))
            if len(samples) < 4:
                samples.append((f"{SOURCE}:{sk}", title, geo, start, end, lat, lon, elev))
        if a.apply:
            con.executemany(
                """INSERT OR REPLACE INTO series
                   (series_id,source_id,title,frequency,unit,geography,category,license_id,
                    start_date,end_date,metadata) VALUES(?,?,?,?,?,?,?,?,?,?,?)""", rows)
        written += len(rows)
        if written % 500_000 < BATCH:
            print(f"  {written:,}/{total:,}", flush=True)

    print(f"\nrows {'written' if a.apply else 'that would be written'}: {written:,}")
    print(f"element codes left as a raw code: {unresolved:,}")
    for s in samples:
        print(f"   {s[0]}")
        print(f"      {s[1]}")
        print(f"      {s[2]} | {s[3]}..{s[4]} | {s[5]},{s[6]} elev {s[7]}")

    if not a.apply:
        print("\n(dry run — pass --apply to write)")
        return 0

    # Remove the broken demo rows AFTER the good ones are in, so the source is never
    # momentarily less catalogued than it started.
    n_del = con.execute(
        "delete from series where source_id=? "
        "and substr(series_id,1,10) not in ('noaa:gsom:','noaa:gsoy:')", (SOURCE,)).rowcount
    con.commit()
    n = con.execute("select count(*) from series where source_id=?", (SOURCE,)).fetchone()[0]
    print(f"\nremoved {n_del} broken demo row(s) keyed noaa:GSOM:… (the store has no such key)")
    print(f"catalogue rows for {SOURCE}: {n:,}")
    try:
        con.execute("INSERT INTO series_fts(series_fts) VALUES('rebuild')")
        con.commit()
        print("series_fts rebuilt")
    except sqlite3.Error as e:
        print(f"series_fts rebuild skipped: {e}")
    print("\nNEXT, and required before these are usable: derive their CSVs "
          "(tools/derive_csv_bulk.py --source noaa), add 'noaa' to api/worker/src/util.ts "
          "SUPPORTED_SOURCES, and sync to D1. A catalogue row without a CSV is a listed series "
          "that will not download — exactly what the ten demo rows were.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
