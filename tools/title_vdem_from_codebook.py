#!/usr/bin/env python3
"""Title V-Dem's 783,100 series from V-Dem's own codebook and country table.

Every vdem key is `VDEM:<variable>:<country_text_id>` — `VDEM:v2elpubfin_codehigh:AFG` — and
catalogue rows are inserted with the key as their title, which is useless to a reader. Both
halves are published by V-Dem itself, so nothing here is invented:

  * VARIABLE — `data/codebook.RData` in the vdeminstitute/vdemdata package maps 781 `tag` values
    to their `name` ("v2elpubfin" -> "Public campaign finance").
  * COUNTRY — `country_text_id` -> `country_name` taken from V-Dem's own dataset. Do NOT
    substitute an ISO-3166 table: 202 codes look like ISO3 and are not all ISO3. `BDN` is Baden,
    a 19th-century German state. An ISO lookup would silently mislabel or drop the historical
    polities that are much of the point of V-Dem.

SUFFIX STRIPPING IS WHAT MAKES THIS COMPLETE. The codebook names 658 of the 4,574 variables in
the store (14.4%); the other 3,916 are suffixed forms of those same variables — `_codehigh`,
`_codelow`, `_sd`, `_osp`, `_ord`, `_nr`, `_mean` and combinations. Stripping trailing `_tokens`
until a tag matches resolves ALL of them: measured 658 direct + 3,916 stripped + 0 unresolved.

The suffix is carried VERBATIM, in parentheses. `_codehigh` is the upper bound of the
measurement model's credible interval and `_osp` is the original-scale posterior prediction, but
the codebook does not define the suffix convention in a field this tool reads, so spelling them
out would be me supplying definitions the publisher has not — across hundreds of thousands of
rows, in a field users read as authoritative. Same call as the UNCTAD SPAN titles.

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
COUNTRIES = os.path.join(ROOT, "data", "_vdem_countries.json")
CODEBOOK_JSON = os.path.join(ROOT, "data", "_vdem_codebook.json")
CODEBOOK_URL = "https://raw.githubusercontent.com/vdeminstitute/vdemdata/master/data/codebook.RData"


def load_codebook() -> dict:
    """tag -> published variable name, cached as JSON after the first read."""
    if os.path.exists(CODEBOOK_JSON):
        return json.load(io.open(CODEBOOK_JSON, encoding="utf-8"))
    import tempfile
    import requests
    import pyreadr
    r = requests.get(CODEBOOK_URL, timeout=300)
    r.raise_for_status()
    fd, tmp = tempfile.mkstemp(suffix=".RData")
    os.close(fd)
    try:
        with open(tmp, "wb") as f:
            f.write(r.content)
        cb = pyreadr.read_r(tmp)["codebook"]
        tags = {str(t): str(n) for t, n in zip(cb["tag"], cb["name"])
                if str(t) not in ("nan", "") and str(n) not in ("nan", "")}
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass
    json.dump(tags, io.open(CODEBOOK_JSON, "w", encoding="utf-8"),
              indent=0, sort_keys=True, ensure_ascii=False)
    return tags


def resolve_variable(var: str, tags: dict):
    """(published name, suffix) for a variable, or None when the codebook cannot name it.

    Longest base first: `v2x_polyarchy_codehigh` must resolve against `v2x_polyarchy`, not
    against a shorter prefix that happens to be a tag.
    """
    if var in tags:
        return tags[var], ""
    parts = var.split("_")
    for cut in range(len(parts) - 1, 0, -1):
        base = "_".join(parts[:cut])
        if base in tags:
            return tags[base], "_".join(parts[cut:])
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    if not os.path.exists(COUNTRIES):
        print(f"  missing {COUNTRIES} — extract country_text_id -> country_name from V-Dem's "
              f"own vdem.RData first; do NOT substitute an ISO-3166 table (see the header).")
        return 2
    countries = json.load(io.open(COUNTRIES, encoding="utf-8"))
    tags = load_codebook()
    print(f"  codebook variables: {len(tags):,}   country codes: {len(countries):,}")

    con = sqlite3.connect(CATALOG, timeout=300)
    con.execute("PRAGMA busy_timeout=300000")
    rows = con.execute("SELECT series_id, title FROM series WHERE source_id='vdem'").fetchall()
    print(f"  vdem catalogue rows: {len(rows):,}")

    updates = []
    no_var = no_country = malformed = already = 0
    unresolved_vars = set()
    for sid, title in rows:
        key = sid.split(":", 1)[1] if ":" in sid else sid
        parts = key.split(":")
        if len(parts) != 3 or parts[0] != "VDEM":
            malformed += 1
            continue
        _, var, cc = parts
        res = resolve_variable(var, tags)
        if not res:
            no_var += 1
            unresolved_vars.add(var)
            continue
        cname = countries.get(cc)
        if not cname:
            no_country += 1
            continue
        name, suffix = res
        new = f"{name} — {cname}" + (f" ({suffix})" if suffix else "")
        if title == new:
            already += 1
            continue
        updates.append((new, sid))

    print(f"  titles to write            : {len(updates):,}")
    print(f"  already correct            : {already:,}")
    print(f"  variable not in codebook   : {no_var:,} ({len(unresolved_vars)} distinct)")
    print(f"  country code not in V-Dem  : {no_country:,}")
    print(f"  malformed keys             : {malformed:,}")
    if unresolved_vars:
        print(f"    e.g. {sorted(unresolved_vars)[:8]}")
    for t, sid in updates[:5]:
        print(f"    {sid:<38} -> {t[:74]!r}")

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
