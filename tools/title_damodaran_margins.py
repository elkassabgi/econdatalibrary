#!/usr/bin/env python3
"""Title damodaran's margins series from Damodaran's own column headers and industry names.

These 384 series used to be untitleable, and `data/clean_full/damodaran/_build_titles.py`
records why, correctly: margin.xls has a merged category band row above the real header, the
ingest header-finder latched onto the band, the band labels became column slugs and the actual
margin CELLS spilled into the entity position. The identity was gone from the key, so no title
could be honest, and the whole group was OMITTED on principle.

That diagnosis named a code path, and the code is now fixed (jobs/ingest_damodaran.py tests
row[0] rather than the first non-null cell). The same sheet yields 1,728 series keyed
DAMODARAN:margins:<metric>:<industry>, every one identifiable — so the omission is obsolete and
these can be titled the way every other damodaran series is: from the publisher's labels,
verbatim.

NO REVERSE-ENGINEERING OF THE SLUG. The key carries a lossy slug ("Bank__Money_Center"), and
un-escaping it would be guesswork — the double underscore could be "(", " (", "/" or several
other things. Instead the ORIGINAL strings are read from margin.xls and the ingest's own slug
rule is applied FORWARD to build slug -> official label:

    col_slug = re.sub(r'[^a-zA-Z0-9_]', '_', header)[:25].strip('_')
    entity   = re.sub(r'[^a-zA-Z0-9_]', '_', industry)[:30].strip('_')

A slug that two official labels collide onto is left alone, for the same reason the divfcfe
group is: no single label is true for all of its rows.

--apply writes; without it nothing changes.
"""
from __future__ import annotations

import argparse
import collections
import os
import re
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG = os.path.join(ROOT, "data", "catalog.db")
MARGIN_URL = "https://pages.stern.nyu.edu/~adamodar/pc/datasets/margin.xls"


def col_slug(header: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", str(header))[:25].strip("_")


def entity_slug(industry: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", str(industry))[:30].strip("_")


def load_sheet(path: str):
    import xlrd
    sh = xlrd.open_workbook(path).sheet_by_name("Industry Averages")
    return [[sh.cell_value(r, c) if sh.cell_value(r, c) != "" else None
             for c in range(sh.ncols)] for r in range(sh.nrows)]


def _fetch(path: str) -> str:
    if os.path.exists(path):
        return path
    import requests
    r = requests.get(MARGIN_URL, headers={"User-Agent": "Econ-Fin Data Library"}, timeout=180)
    r.raise_for_status()
    with open(path, "wb") as f:
        f.write(r.content)
    return path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--xls", default=os.path.join(ROOT, "data", "_margin.xls"))
    a = ap.parse_args()

    rows = load_sheet(_fetch(a.xls))

    # Reuse the INGEST's own metadata-row filter rather than restating the rule. Restating it
    # is how this first ran: I copied the "column 0 is a non-empty string" test and left out
    # _is_metadata_row, so it picked row 2, "What is this data?", found 2 columns instead of 18
    # and titled nothing. The rule that produced the keys is the only rule that can decode them,
    # so it has to be imported, not paraphrased (R276).
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "dam_ingest", os.path.join(ROOT, "jobs", "ingest_damodaran.py"))
    ingest = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ingest)

    hidx = -1
    for i, r in enumerate(rows[:35]):
        if ingest._is_metadata_row(r):
            continue
        if isinstance(r[0], str) and r[0].strip() and len([c for c in r if c is not None]) >= 3:
            hidx = i
            break
    if hidx < 0:
        print("  could not find the header row in margin.xls")
        return 2
    header = rows[hidx]
    print(f"  header row {hidx}: {[str(h)[:22] for h in header[:5]]}")

    col_by_slug = collections.defaultdict(set)
    for h in header[1:]:
        if h:
            col_by_slug[col_slug(h)].add(str(h).strip())
    ent_by_slug = collections.defaultdict(set)
    for r in rows[hidx + 1:]:
        if isinstance(r[0], str) and r[0].strip():
            ent_by_slug[entity_slug(r[0])].add(r[0].strip())

    collisions = {k for k, v in col_by_slug.items() if len(v) > 1} | \
                 {k for k, v in ent_by_slug.items() if len(v) > 1}
    print(f"  official labels: {len(col_by_slug)} columns, {len(ent_by_slug)} industries"
          + (f"; {len(collisions)} slug collision(s) LEFT ALONE" if collisions else ""))

    con = sqlite3.connect(CATALOG, timeout=600)
    con.execute("PRAGMA busy_timeout=600000")
    cur = con.execute("SELECT series_id, title FROM series WHERE source_id='damodaran' "
                      "AND series_id LIKE 'damodaran:DAMODARAN:margins:%'")
    updates, unmatched = [], collections.Counter()
    for sid, title in cur.fetchall():
        parts = sid.split(":")
        if len(parts) < 5:
            continue
        cslug, eslug = parts[3], parts[4]
        if cslug in collisions or eslug in collisions:
            unmatched["slug collision"] += 1
            continue
        cols, ents = col_by_slug.get(cslug), ent_by_slug.get(eslug)
        if not cols or not ents:
            unmatched["no official label" if not cols else "no official industry"] += 1
            continue
        new = f"{next(iter(cols))} - {next(iter(ents))}"
        if title != new:
            updates.append((new, sid))

    print(f"  titles to write : {len(updates):,}")
    if unmatched:
        print(f"  left alone      : {dict(unmatched)}")
    for t, sid in updates[:5]:
        print(f"    {sid:<56} -> {t[:60]!r}")

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
