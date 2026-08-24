#!/usr/bin/env python3
"""Compose dist/titles/boe.json from the Bank of England's OWN series descriptions.

WHY. 30,653 of boe's 30,670 catalogue rows carry a title identical to their id - a bare
IADB series code like `CFMB2CX` - so none of them can be found by name.

THE SOURCE. The IADB serves a CSV of SERIES,DESCRIPTION for a list of codes:

    GET https://www.bankofengland.co.uk/boeapps/database/_iadb-fromshowcolumns.asp
        ?csv.x=yes&SeriesCodes=<comma list>&UsingCodes=Y&CSVF=TT&VPD=Y&VFD=N
        &Datefrom=01/Jan/2024&Dateto=31/Dec/2024

Note the LEADING UNDERSCORE. `fromshowcolumns.asp` (no underscore) answers 200 with an HTML
page for the same query - a silent wrong answer, not an error - so the underscore form is
the one to use and the Content-Type is checked on every response rather than assumed.

BATCH SIZE IS MEASURED, NOT GUESSED: 50 codes per request returns `application/csv`; 200
overruns the URL and answers 404 with HTML. Descriptions are taken verbatim; a code the
IADB does not describe is left untitled rather than given an invented name.
"""
from __future__ import annotations

import csv
import glob
import io
import json
import os
import sys
import time

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
URL = "https://www.bankofengland.co.uk/boeapps/database/_iadb-fromshowcolumns.asp"
UA = {"User-Agent": "Mozilla/5.0 econdatalibrary/1.0 (research data library)"}
BATCH = 50


def store_codes() -> list:
    import duckdb
    fs = [f for f in glob.glob(os.path.join(ROOT, "data", "clean_full", "boe", "*.parquet"))
          if not f.endswith("__series.parquet")]
    con = duckdb.connect()
    con.execute("SET temp_directory='%s'" %
                os.path.join(ROOT, "data", "_duckdb_spill", "boe_titles").replace("\\", "/"))
    out = set()
    for f in fs:
        out |= {r[0] for r in con.execute(
            "SELECT DISTINCT series_key FROM read_parquet(%r)" % f).fetchall()}
    con.close()
    return sorted(c for c in out if c)


def fetch(batch: list) -> dict:
    r = requests.get(URL, params={"csv.x": "yes", "Datefrom": "01/Jan/2024",
                                  "Dateto": "31/Dec/2024", "SeriesCodes": ",".join(batch),
                                  "CSVF": "TT", "UsingCodes": "Y", "VPD": "Y", "VFD": "N"},
                     timeout=180, headers=UA)
    if r.status_code != 200 or "csv" not in r.headers.get("Content-Type", "").lower():
        return {}
    out = {}
    for row in csv.reader(io.StringIO(r.text)):
        if len(row) >= 2 and row[0] and row[0] != "SERIES" and row[1].strip():
            out[row[0].strip()] = row[1].strip()
    return out


def main() -> int:
    write = "--write" in sys.argv
    codes = store_codes()
    print("boe store codes: %s" % format(len(codes), ","), flush=True)
    titles, missed, failed = {}, 0, 0
    for i in range(0, len(codes), BATCH):
        batch = codes[i:i + BATCH]
        try:
            got = fetch(batch)
        except Exception as e:                                   # noqa: BLE001
            got = {}
            print("  batch %d failed: %s" % (i // BATCH, str(e)[:70]), flush=True)
        if not got:
            failed += len(batch)
        for c in batch:
            d = got.get(c)
            if d:
                titles["boe:" + c] = d
            else:
                missed += 1
        if (i // BATCH) % 40 == 0:
            print("  %6d/%d codes  titled=%s  undescribed=%s  failed_batches=%s"
                  % (i + len(batch), len(codes), format(len(titles), ","),
                     format(missed, ","), format(failed, ",")), flush=True)
        time.sleep(0.3)
    print("titled %s of %s codes (undescribed %s, in failed batches %s)"
          % (format(len(titles), ","), format(len(codes), ","),
             format(missed - failed, ","), format(failed, ",")), flush=True)
    if write and titles:
        p = os.path.join(ROOT, "dist", "titles", "boe.json")
        json.dump(titles, open(p, "w", encoding="utf-8"), ensure_ascii=False, indent=0, sort_keys=True)
        print("wrote %s" % p, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
