#!/usr/bin/env python3
"""Title the World Bank indicator series carried inside unesco_dem, using World Bank names.

unesco_dem's untitled rows are two different families, and they need two different publishers:

  * 164 rows are WORLD BANK indicator codes with a country and a frequency —
    `DT.TDS.DECT.GN.ZS.IRQ.A`, `NY.GDP.DEFL.ZS.FRO.A`, `PA.NUS.ATLS.MAF.A`. The World Bank's
    open API (no key) names both halves: DT.TDS.DECT.GN.ZS is "Total debt service (% of GNI)"
    and it names 71 of the 75 country codes we need.
  * 20 rows are UNESCO's own numeric indicators — `200101.GGY.A`. Their INDICATOR is already
    known from titled siblings ("Total population (thousands)"); only the country name is
    missing. Those are NOT touched here, because using the World Bank's country naming inside a
    UNESCO-indicator title would mix two publishers' vocabularies in one string. They need
    UNESCO's own country table.

So this tool handles the World Bank family only, and says how many it left.

    Total debt service (% of GNI) — Iraq (Annually)

matching the shape of the already-titled unesco_dem rows. A code neither the indicator API nor
the country API names keeps its key; GGY, JEY, TKL and ZZA are known to be unnamed by the World
Bank.

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
CACHE = os.path.join(ROOT, "data", "_wb_names.json")
WB = "https://api.worldbank.org/v2"

FREQ = {"A": "Annually", "Q": "Quarterly", "M": "Monthly"}
# A World Bank indicator code is dotted uppercase; UNESCO's own are numeric.
WB_CODE = re.compile(r"^[A-Z][A-Z0-9]*(\.[A-Z0-9]+)+$")


def names(codes: list[str]) -> dict:
    cache = json.load(io.open(CACHE, encoding="utf-8")) if os.path.exists(CACHE) else {}
    cache.setdefault("indicator", {})
    cache.setdefault("country", {})
    import requests
    if not cache["country"]:
        r = requests.get(WB + "/country", params={"format": "json", "per_page": 400}, timeout=180)
        d = r.json()
        for c in (d[1] if len(d) > 1 else []):
            if c.get("id") and c.get("name"):
                cache["country"][c["id"]] = c["name"]
        print(f"  World Bank names {len(cache['country'])} countries")
    todo = [c for c in codes if c not in cache["indicator"]]
    for c in todo:
        try:
            r = requests.get(f"{WB}/indicator/{c}", params={"format": "json"}, timeout=120)
            d = r.json()
            item = (d[1] or [{}])[0] if len(d) > 1 else {}
            cache["indicator"][c] = str(item.get("name") or "")
        except Exception:                                    # noqa: BLE001
            cache["indicator"][c] = ""
        print(f"    {c:<24} {cache['indicator'][c][:54]!r}")
    json.dump(cache, io.open(CACHE, "w", encoding="utf-8"), indent=0, sort_keys=True,
              ensure_ascii=False)
    return cache


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    con = sqlite3.connect(CATALOG, timeout=600)
    con.execute("PRAGMA busy_timeout=600000")
    ids = [r[0] for r in con.execute("""SELECT series_id FROM series WHERE source_id='unesco_dem'
        AND (title IS NULL OR title='' OR title=series_id
          OR title = substr(series_id, instr(series_id,':')+1))""")]
    print(f"  untitled unesco_dem rows: {len(ids):,}")

    parsed, skipped_unesco = [], 0
    for sid in ids:
        key = sid.split(":", 2)[-1]
        parts = key.split(".")
        if len(parts) < 3:
            skipped_unesco += 1
            continue
        ind, cc, fr = ".".join(parts[:-2]), parts[-2], parts[-1]
        if not WB_CODE.match(ind):
            skipped_unesco += 1
            continue
        parsed.append((sid, ind, cc, fr))
    print(f"  World Bank-coded rows: {len(parsed):,}")
    print(f"  UNESCO-coded rows left for UNESCO's own country table: {skipped_unesco:,}")

    cache = names(sorted({p[1] for p in parsed}))
    ind_n, cty_n = cache["indicator"], cache["country"]

    updates, no_ind, no_cty = [], set(), set()
    for sid, ind, cc, fr in parsed:
        i, c = ind_n.get(ind), cty_n.get(cc)
        if not i:
            no_ind.add(ind); continue
        if not c:
            no_cty.add(cc); continue
        f = FREQ.get(fr, fr)
        updates.append((f"{i} — {c} ({f})", sid))

    print(f"  titles to write : {len(updates):,}")
    if no_ind:
        print(f"  indicator unnamed: {sorted(no_ind)}")
    if no_cty:
        print(f"  country unnamed  : {sorted(no_cty)}")
    for t, sid in updates[:4]:
        print(f"    {sid.split(':',2)[-1]:<26} -> {t[:60]!r}")

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
