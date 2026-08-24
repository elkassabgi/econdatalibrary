#!/usr/bin/env python3
"""Title UNCTAD's SPAN variants from the base series UNCTAD already named.

1,146 `unctad_trademerchgr` rows read as their own key — `0000.01.M4017|SPAN=3Y`. They are not
unknown series: each is a qualified form of a base key UNCTAD DOES name, and that base is
already titled in our catalogue. `0000.01.M4017` is "Merchandise: Total trade growth rates,
annual - World, Imports"; the `|SPAN=3Y` row is the same indicator carrying an extra qualifier.

NO API CALL, AND NO INTERPRETATION. The title becomes the base's published title followed by the
qualifier EXACTLY as UNCTAD writes it in the key:

    Merchandise: Total trade growth rates, annual - World, Imports — SPAN=3Y

"SPAN=3Y" plainly suggests a three-year averaging window, and UNCTAD's report metadata
(reportMetadata/US.TradeMerchGR/bundle, fetched 2026-08-24) does not define the dimension — it
carries no `dimensions` array and the string SPAN appears nowhere in it. So writing "(3-year
average growth rate)" would be me supplying a definition the publisher has not, on 1,146 rows,
in a field users read as authoritative. The token is preserved instead. The readability that was
actually missing is the base title, and that is the publisher's own words.

A SPAN row whose base is NOT titled is left alone — there is nothing to inherit, and a partial
title is worse than an honest key.

--apply writes; without it nothing changes.
"""
from __future__ import annotations

import argparse
import os
import re
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG = os.path.join(ROOT, "data", "catalog.db")

SPAN_RE = re.compile(r"^(?P<base>.+)\|(?P<qual>SPAN=\d+Y)$")


def _is_untitled(series_id: str, title) -> bool:
    """title == the id (with or without the source prefix), or empty.

    Keyed on equality, not on a character class: a regex built from what codes "usually look
    like" is how 169,722 rows once stayed invisible (R474). `title == id` needs no alphabet.
    """
    if title is None or title == "":
        return True
    bare = series_id.split(":", 1)[1] if ":" in series_id else series_id
    return title == series_id or title == bare


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    con = sqlite3.connect(CATALOG, timeout=180)
    con.execute("PRAGMA busy_timeout=180000")

    titled: dict[str, str] = {}
    untitled: list[str] = []
    for sid, title in con.execute("SELECT series_id, title FROM series"):
        if _is_untitled(sid, title):
            untitled.append(sid)
        else:
            titled[sid] = title

    updates, no_base = [], []
    for sid in untitled:
        prefix, _, key = sid.partition(":")
        m = SPAN_RE.match(key)
        if not m:
            continue
        base_id = f"{prefix}:{m.group('base')}"
        base_title = titled.get(base_id)
        if not base_title:
            no_base.append(sid)
            continue
        updates.append((f"{base_title} — {m.group('qual')}", sid))

    print(f"  catalogue rows scanned : {len(titled) + len(untitled):,}")
    print(f"  SPAN rows with a titled base : {len(updates):,}")
    print(f"  SPAN rows whose base is ALSO untitled (left alone) : {len(no_base):,}")
    for t, sid in updates[:5]:
        print(f"    {sid:<44} -> {t[:78]!r}")

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
