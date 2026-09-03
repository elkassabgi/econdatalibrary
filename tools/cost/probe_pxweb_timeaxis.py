"""Why did a PxWeb source refuse these tables? Ask the publisher, per table.

PxWeb sources refuse a table when its time axis parses to no dates, and that refusal is CORRECT:
R331/R334 record that falling through to another dimension is what fabricates years. So the
question is never "why is the parser broken" but "what is the publisher's metadata actually
doing", and there are at least two answers:

  NAME-FIRST        every variable's `values` are ordinal codes ['0','1','2',...] and the real
                    labels live in `valueTexts`, where the year variable reads
                    ['2010','2011',...]. Often NO variable is flagged `time: true` at all.
                    Measured on hagstofa SJA02203.px, 2026-09-03.

  MISLABELLED AXIS  the publisher flags the WRONG variable as the time axis. Measured on
                    stat_slovenia 1012308S: `ORGANIZACIJSKA OBLIKA` (organisational form) is
                    `time: true` with values ['0','1','2'], while `LETO` - year - holds ['2012']
                    and is flagged `time: null`.

The two need different fixes, and a blanket fall-through would be the fabrication the guard
exists to prevent. This tool tells them apart with evidence rather than by inspection.

It reads the failing table ids from `unit_state.last_error`, which names them because
tests/test_stat_slovenia_failure_naming.py made naming a regression gate. Read-only and paced.
"""
from __future__ import annotations

import argparse
import io
import json
import os
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.request

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
STATE = os.path.join(ROOT, "data", "_aqueduct", "state.db")
UA = {"User-Agent": "econdatalibrary/1.0 (+https://econdatalibrary.com)"}
YEAR = re.compile(r"^(19|20|21)\d{2}")
PACE = 0.5

# base URL and, where the tree is split, the databases to try in turn
SOURCES = {
    "hagstofa": ("https://px.hagstofa.is/pxen/api/v1/en",
                 ["Atvinnuvegir", "Efnahagur", "Ibuar", "Samfelag", "Umhverfi"]),
    "stat_slovenia": ("https://pxweb.stat.si/SiStatData/api/v1/en/Data", [""]),
}


def failing_tables(source: str) -> list[str]:
    """Table ids named inside the source's own last_error, in [a, b, c] form."""
    con = sqlite3.connect("file:%s?mode=ro" % STATE, uri=True)
    con.execute("PRAGMA busy_timeout=8000")
    row = con.execute("SELECT last_error FROM unit_state WHERE source_id=? "
                      "AND last_error IS NOT NULL LIMIT 1", (source,)).fetchone()
    if not row or not row[0]:
        return []
    m = re.search(r"\[([^\]]+)\]", row[0])
    if not m:
        return []
    out = []
    for part in m.group(1).split(","):
        p = part.strip()
        p = p.split(":")[0].strip() if ": " in p else p
        if p:
            out.append(p)
    return out


def fetch(base: str, dbs: list[str], path: str):
    for db in dbs:
        url = f"{base}/{db}/{path}" if db else f"{base}/{path}"
        try:
            r = urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=90)
            return db, json.load(r)
        except urllib.error.HTTPError:
            pass
        except Exception:                                             # noqa: BLE001
            pass
        time.sleep(0.2)
    return None, None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("source", choices=sorted(SOURCES))
    ap.add_argument("--limit", type=int, default=6)
    a = ap.parse_args()

    base, dbs = SOURCES[a.source]
    tables = failing_tables(a.source)[:a.limit]
    if not tables:
        print(f"{a.source}: no table ids in unit_state.last_error - either the source is "
              f"healthy or its note does not name them")
        return 0

    print(f"{a.source}: probing {len(tables)} refused table(s)\n")
    name_first = mislabelled = unknown = 0

    for path in tables:
        db, d = fetch(base, dbs, path if path.endswith(".px") else path + ".px")
        time.sleep(PACE)
        if d is None:
            print(f"--- {path}\n    not retrievable from the publisher (retired, or a path "
                  f"shape this tool does not build)\n")
            unknown += 1
            continue
        print(f"--- {(db + '/' if db else '') + path}")
        print(f"    {str(d.get('title', ''))[:66]}")
        flagged_dateless = False
        any_flagged = False
        text_years = False
        for v in d.get("variables", []):
            vals = v.get("values") or []
            texts = v.get("valueTexts") or []
            dv = sum(1 for x in vals if YEAR.match(str(x)))
            dt = sum(1 for x in texts if YEAR.match(str(x)))
            if v.get("time"):
                any_flagged = True
                flagged_dateless = dv == 0
            if texts and dt == len(texts):
                text_years = True
            note = ""
            if v.get("time"):
                note = "  <- FLAGGED time=true"
            elif vals and dv == len(vals):
                note = "  <- values are all years"
            elif texts and dt == len(texts):
                note = "  <- valueTexts are all years"
            print(f"      {str(v.get('code'))[:22]:<24}time={str(v.get('time')):<5}"
                  f"n={len(vals):<5}values_dates={dv:<5}texts_dates={dt:<5}{note}")

        if not any_flagged and text_years:
            name_first += 1
            print("      => NAME-FIRST: nothing is flagged as the time axis and the years are "
                  "in valueTexts")
        elif any_flagged and flagged_dateless and text_years:
            mislabelled += 1
            print("      => MISLABELLED AXIS: the flagged axis holds no dates while another "
                  "variable holds only years")
        else:
            unknown += 1
            print("      => neither known shape - look at this one by hand")
        print()

    print(f"name-first {name_first}   mislabelled-axis {mislabelled}   unclassified {unknown}")
    print()
    print("A fix must be GUARDED, never a blind fall-through: adopt another axis only when the")
    print("flagged one parses to ZERO dates AND the candidate's values are ALL dates. R331/R334")
    print("record that the unguarded version fabricates years.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
