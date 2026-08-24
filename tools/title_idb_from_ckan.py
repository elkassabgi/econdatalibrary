#!/usr/bin/env python3
"""Title IDB's remaining 738 series with IDB's official dataset names.

These keys read as themselves — `idb:IDB:population-and-housing-censuses-indicators-of-latin-
america-and-the-caribbe:Ninis_2_PHC:ARG`. They carry one segment more than their titled
siblings, because this shape is dataset:indicator:country while the titled ones are
dataset:country, and the slug in the key is TRUNCATED by the ingest.

WHAT IDB PUBLISHES, AND WHAT IT DOES NOT. data.iadb.org is a CKAN portal with no API key. Its
`package_show` gives the full official dataset title, so the truncated slug can be resolved to
IDB's own words. It does NOT name the indicators: `Ninis_2_PHC` and `prangoedad_76_90_PHC` are
values in an `indicator` COLUMN, the resource carries no label column beside them, the package
`notes` field is empty and `extras` is empty. Checked all three, 2026-08-24.

So the indicator code and the ISO-3 country code are carried VERBATIM, in brackets, and the
official dataset title supplies the readable part:

    Population and Housing Censuses Indicators of Latin America and the Caribbean — ARG (Ninis_2_PHC)

Expanding "Ninis" to "young people not in employment, education or training" would be me
supplying a definition IDB has not published, which is the same call made for UNCTAD's SPAN and
V-Dem's suffixes. The country code stays a code for the same reason: IDB gives `isoalpha3` and
no country name, and borrowing another publisher's country table would put V-Dem's or UNHCR's
naming into IDB's titles.

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
CACHE = os.path.join(ROOT, "data", "_idb_dataset_titles.json")
BASE = "https://data.iadb.org/api/3/action"


def _en(title) -> str:
    """CKAN returns multilingual titles as {'en': ..., 'es': ...} or a plain string."""
    if isinstance(title, dict):
        return str(title.get("en") or title.get("es") or next(iter(title.values()), "")).strip()
    return str(title or "").strip()


def resolve_titles(slugs: list[str]) -> dict:
    """truncated-slug -> IDB's official dataset title, cached."""
    cache = json.load(io.open(CACHE, encoding="utf-8")) if os.path.exists(CACHE) else {}
    todo = [s for s in slugs if s not in cache]
    if todo:
        import requests
        r = requests.get(BASE + "/package_list", timeout=180)
        r.raise_for_status()
        packages = r.json().get("result") or []
        for slug in todo:
            # our slug is truncated, so match by prefix; refuse an ambiguous match rather
            # than pick one, because the wrong dataset title is worse than none.
            hits = [p for p in packages if p.startswith(slug)]
            if len(hits) != 1:
                print(f"  {slug[:52]}: {len(hits)} package(s) match — left alone")
                cache[slug] = ""
                continue
            d = requests.get(BASE + "/package_show", params={"id": hits[0]}, timeout=180)
            cache[slug] = _en((d.json().get("result") or {}).get("title"))
            print(f"  {slug[:46]}... -> {cache[slug][:60]!r}")
        json.dump(cache, io.open(CACHE, "w", encoding="utf-8"),
                  indent=0, sort_keys=True, ensure_ascii=False)
    return cache


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    con = sqlite3.connect(CATALOG, timeout=600)
    con.execute("PRAGMA busy_timeout=600000")
    rows = con.execute("""SELECT series_id FROM series WHERE source_id='idb'
        AND (title IS NULL OR title='' OR title=series_id
          OR title = substr(series_id, instr(series_id,':')+1))""").fetchall()
    ids = [r[0] for r in rows]
    print(f"  untitled idb rows: {len(ids):,}")

    slugs = sorted({s.split(":")[2] for s in ids if s.count(":") >= 4})
    titles = resolve_titles(slugs)

    updates, skipped = [], 0
    for sid in ids:
        parts = sid.split(":")
        if len(parts) < 5:
            skipped += 1
            continue
        slug, indicator, cc = parts[2], parts[3], parts[4]
        ds = titles.get(slug)
        if not ds:
            skipped += 1
            continue
        updates.append((f"{ds} — {cc} ({indicator})", sid))

    print(f"  titles to write : {len(updates):,}")
    print(f"  left alone      : {skipped:,}")
    for t, sid in updates[:4]:
        print(f"    {t[:96]!r}")

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
