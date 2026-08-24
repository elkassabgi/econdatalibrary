#!/usr/bin/env python3
"""Title UNHCR's remaining 332 population series, matching the convention already in the rows.

A unhcr key is `population:<type>:<origin>:<asylum>` and a titled row reads

    refugees — Country of origin: Unknown ; Country of asylum: Australia

so the TYPE is already carried as UNHCR's own raw token, not an expansion of it. That settles
the only interpretive question here: 29 of the untitled rows are type `hst`, which appears in no
titled row, and the temptation is to write "Host community". The convention in the data says
otherwise, and `hst` is a real UNHCR category — the population endpoint returns it alongside
refugees/asylum_seekers/idps with 26,095,474 people for 2023 — so the token stands as written,
exactly as `refugees` and `ooc` do.

Country names come from titled sibling rows first (217 codes, and they are the same strings we
already serve, so borrowing keeps the source internally consistent), then from UNHCR's own
countries endpoint for codes that appear only in untitled rows. A code neither source names is
left alone rather than guessed.

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
COUNTRIES = os.path.join(ROOT, "data", "_unhcr_countries.json")

# The full shape is
#   <type> — Country of origin: <origin> ; Country of asylum: <asylum> (Population figures)
# and that trailing parenthetical belongs to the TITLE, not to the asylum country. A regex ending
# at `$` swallows it, so `learned["UGA"]` became "Uganda (Population figures)" and every composed
# title then read "Country of origin: Uganda (Population figures) ; ...". Caught in the dry run
# by reading the output rather than the count.
SUFFIX = " (Population figures)"
PAIR_RE = re.compile(r"Country of origin:\s*(.+?)\s*;\s*Country of asylum:\s*(.+?)\s*$")


def _split_tail(asylum: str) -> str:
    return asylum[:-len(SUFFIX)] if asylum.endswith(SUFFIX) else asylum


def _untitled(series_id: str, title) -> bool:
    if title is None or title == "":
        return True
    bare = series_id.split(":", 1)[1] if ":" in series_id else series_id
    return title == series_id or title == bare


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    api_names = {}
    if os.path.exists(COUNTRIES):
        api_names = json.load(io.open(COUNTRIES, encoding="utf-8"))

    con = sqlite3.connect(CATALOG, timeout=300)
    con.execute("PRAGMA busy_timeout=300000")
    rows = con.execute("SELECT series_id,title FROM series WHERE source_id='unhcr'").fetchall()

    learned: dict[str, str] = {}
    untitled = []
    for sid, title in rows:
        bare = sid.split(":", 1)[1] if ":" in sid else sid
        parts = bare.split(":")
        if len(parts) != 4:
            continue
        _ds, typ, org, asy = parts
        if _untitled(sid, title):
            untitled.append((sid, typ, org, asy))
            continue
        m = PAIR_RE.search(str(title))
        if m:
            learned.setdefault(org, m.group(1))
            learned.setdefault(asy, _split_tail(m.group(2)))

    print(f"  unhcr rows {len(rows):,}   untitled {len(untitled):,}")
    print(f"  country names: {len(learned)} learned from titled rows, "
          f"{len(api_names)} from UNHCR's API")

    def cname(code: str):
        return learned.get(code) or api_names.get(code)

    updates, skipped = [], []
    for sid, typ, org, asy in untitled:
        o, s = cname(org), cname(asy)
        if not o or not s:
            skipped.append((sid, org if not o else asy))
            continue
        updates.append((f"{typ} — Country of origin: {o} ; Country of asylum: {s}" + SUFFIX, sid))

    print(f"  titles to write : {len(updates):,}")
    if skipped:
        codes = sorted({c for _s, c in skipped})
        print(f"  left alone      : {len(skipped):,} (no name for {codes})")
    for t, sid in updates[:5]:
        print(f"    {sid:<34} -> {t[:74]!r}")

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
