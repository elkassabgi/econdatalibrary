#!/usr/bin/env python3
"""Title RBA's remaining series from the Title row RBA ships in its own CSVs.

338 rba rows read as their series id — `ALDOMOCP`, `AOMOLDWAR`. RBA publishes the name in the
same file the data came from. Every statistical table at rba.gov.au/statistics/tables/ has a
`Title` row and a `Series ID` row, and they align by COLUMN:

    Title,     Bond Issuer, Coupon Rate, Maturity,  Face Value, Average Purchase Rate, ...
    Series ID, ALDOMOISS,   ALDOMOCP,    ALDOMOMD,  ALDOMOFVD,  AOMOLDWAR,             ...

So `ALDOMOCP` is "Coupon Rate". The ingest reads the Series ID row and drops the Title row, the
same shape as the BEA LineDescription and the NOAA station names: the label was in hand and
thrown away.

DISAMBIGUATED BY TABLE, and that is a deliberate departure. The existing titled rba rows carry
the bare label ("Notes on issue"), which works while labels are distinctive and fails badly for
this set — "Coupon Rate", "Maturity" and "Face Value" are meaningless alone and RBA reuses them
across tables. The table name is RBA's own words from row 0 of the same file, so adding it
invents nothing and makes the title identify a series:

    Coupon Rate — A3 Monetary Policy Operations – Long-Dated Open Market Operations

A series id RBA does not name is left as its id.

--apply writes; without it nothing changes.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import sqlite3
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG = os.path.join(ROOT, "data", "catalog.db")
CACHE = os.path.join(ROOT, "data", "_rba_series_titles.json")
BASE = "https://www.rba.gov.au"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
      "Accept": "text/html,text/csv,application/csv,*/*"}


def _titlecase_table(s: str) -> str:
    """RBA writes row 0 in caps. Lower it, but keep short all-caps tokens (A3, RBA, OMO)."""
    s = s.replace("﻿", "").strip()
    out = []
    for w in s.split():
        if len(w) <= 3 and w.isupper():
            out.append(w)
        elif w.isupper():
            out.append(w.capitalize())
        else:
            out.append(w)
    return " ".join(out)


def harvest() -> dict:
    """series id -> 'Label — Table name', from every RBA statistical table CSV."""
    if os.path.exists(CACHE):
        return json.load(io.open(CACHE, encoding="utf-8"))
    import requests
    r = requests.get(BASE + "/statistics/tables/", headers=UA, timeout=180)
    r.raise_for_status()
    links = sorted(set(re.findall(r'href="([^"]+\.csv)"', r.text)))
    print(f"  {len(links)} table CSV(s) listed")
    out: dict[str, str] = {}
    for n, u in enumerate(links, 1):
        full = u if u.startswith("http") else BASE + u
        try:
            txt = requests.get(full, headers=UA, timeout=180).content.decode("utf-8", "replace")
        except Exception:                                    # noqa: BLE001
            continue
        rows = list(csv.reader(io.StringIO(txt)))
        if not rows:
            continue
        table = _titlecase_table(rows[0][0] if rows[0] else "")
        title_row = next((r for r in rows[:14] if r and r[0].strip().lower() == "title"), None)
        id_row = next((r for r in rows[:14] if r and r[0].strip().lower() == "series id"), None)
        if not title_row or not id_row:
            continue
        for i in range(1, min(len(title_row), len(id_row))):
            sid, lab = id_row[i].strip(), title_row[i].strip()
            if sid and lab and sid not in out:
                out[sid] = f"{lab} — {table}" if table else lab
        if n % 40 == 0:
            print(f"    {n}/{len(links)} tables, {len(out):,} ids named")
        time.sleep(0.15)
    json.dump(out, io.open(CACHE, "w", encoding="utf-8"), indent=0, sort_keys=True,
              ensure_ascii=False)
    print(f"  harvested {len(out):,} series labels -> {CACHE}")
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    names = harvest()
    con = sqlite3.connect(CATALOG, timeout=600)
    con.execute("PRAGMA busy_timeout=600000")
    rows = con.execute("""SELECT series_id FROM series WHERE source_id='rba'
        AND (title IS NULL OR title='' OR title=series_id
          OR title = substr(series_id, instr(series_id,':')+1))""").fetchall()
    ids = [r[0] for r in rows]
    print(f"  untitled rba rows: {len(ids):,}")

    updates, unnamed = [], []
    for sid in ids:
        key = sid.split(":", 1)[1] if ":" in sid else sid
        lab = names.get(key)
        if not lab:
            unnamed.append(key)
            continue
        updates.append((lab, sid))

    print(f"  titles to write : {len(updates):,}")
    print(f"  RBA does not name: {len(unnamed):,}" + (f"  e.g. {unnamed[:6]}" if unnamed else ""))
    for t, sid in updates[:4]:
        print(f"    {sid.split(':',1)[1]:<14} -> {t[:78]!r}")

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
