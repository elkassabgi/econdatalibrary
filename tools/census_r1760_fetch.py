"""Fetch, from the Census API, the four intltrade IMPORTS value columns that R1760 found holding the national
TOTAL in every detail row up to 2026-02 - at DETAIL grain, for a later reviewed store patch. FETCH ONLY.

WHY. jobs/ingest_census.pull_slice merged a variable chunk that carried no commodity column onto every commodity
row of the month (keys [time, YEAR, MONTH]; fixed in #235). The rows from 2026-03 come from the tail fetcher in
one request and are clean. This asks for exactly the bad columns WITH the row's identity
(commodity, COMM_LVL, SUMMARY_LVL2, DISTRICT), so each value can be joined back to the one row it belongs to.
Design and sizing: R1760 findings (F:/work/r1760/FINDINGS.md on the workstation).

WHAT IT NEVER DOES. It writes nothing to the store, R2 or D1. It writes raw responses, each with its md5 and the
request URL WITHOUT the key, into --out (a scratch folder), plus a manifest. The key is read in-process by the
fetchers' own updater/strategies/fetchers/_common.api_key and is never printed or written (R1685).

    python tools/census_r1760_fetch.py --calibrate --out F:/work/r1760/fetch     # R0: 4 requests
    python tools/census_r1760_fetch.py --table enduse --year 2015 --out ...      # one year of one table
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sys
import time
from urllib.parse import quote, quote_plus

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import requests  # noqa: E402

BASE = "https://api.census.gov/data/timeseries/intltrade/imports/"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
GAP_S = 5.0
TIMEOUT_S = 300
# SUMMARY_LVL is asked for too: the store holds only 'DET' and the tail fetcher pins it; R0 must PROVE the API
# answers CTY_CODE='-' at that level rather than assume it (review AR-440).
IDENTITY = ["SUMMARY_LVL", "COMM_LVL", "SUMMARY_LVL2", "DISTRICT"]
# Clean columns, asked for in the same request so the patch can require fetched == stored on every row of a month:
# a publisher revision changes them too, and the bad columns alone cannot show it (review AR-442 5a, R1819).
GATE = ["GEN_VAL_MO", "CON_VAL_MO", "AIR_VAL_MO"]
TABLES = {
    # table: (commodity column, the columns R1760 measured as copied, first affected month in the store)
    "enduse": ("I_ENDUSE", ["CON_CHA_YR", "DUT_VAL_YR", "DUT_VAL_MO", "AIR_VAL_YR"], "2010-01"),
    "naics": ("NAICS", ["CON_CHA_YR", "DUT_VAL_YR", "DUT_VAL_MO", "AIR_VAL_YR"], "2019-01"),
    "sitc": ("SITC", ["CON_CHA_YR", "DUT_VAL_YR", "DUT_VAL_MO", "AIR_VAL_YR"], "2024-01"),
    "hitech": ("HITECH", ["DUT_VAL_YR", "DUT_VAL_MO", "AIR_VAL_YR"], "2010-01"),   # its CON_CHA_YR sat in chunk 0
}
LAST_BAD = "2026-02"                       # R1760: copied on every date up to here, on none from 2026-03
# an old month and a clean one as the control, for the widest table too
CALIBRATE = [("enduse", "2015-06"), ("enduse", "2026-03"), ("naics", "2019-06"), ("naics", "2026-03")]
# NOT covered here (separate steps): asm/area2012 and asm/area2017 (findings section 7), imports/usda (needs a
# full re-pull: its chunk 1 carried the commodity code itself).


def _key() -> str:
    from updater.strategies.fetchers._common import api_key
    k = api_key("CENSUS_API_KEY")
    if not k:
        raise SystemExit("CENSUS_API_KEY is not set (environment or .env): nothing fetched")
    return k


def request(table: str, time_clause: str) -> "tuple[str, dict]":
    """(url without key, params without key) for one table and a time= clause (YYYY-MM or 'from+..+to+..')."""
    comm, cols, _first = TABLES[table]
    params = {"get": ",".join([comm] + IDENTITY + cols + GATE), "CTY_CODE": "-", "time": time_clause}
    return BASE + table, params


def fetch(table: str, time_clause: str, out_dir: str, key: str, sess=None) -> dict:
    """One request; the raw body and a manifest line go to out_dir. Returns the manifest record."""
    sess = sess or requests.Session()
    url, params = request(table, time_clause)
    stem = f"{table}_{time_clause.replace('+', '_')}"
    t0 = time.monotonic()
    try:
        r = sess.get(url, params={**params, "key": key}, headers=UA, timeout=TIMEOUT_S)
    except requests.RequestException as e:
        # requests' ConnectionError/SSLError carry urllib3's message WITH the full query string, key included
        # (review AR-440, demonstrated): the message is scrubbed and the traceback suppressed (`from None`).
        msg = str(e)
        for form in {key, quote(key, safe=""), quote_plus(key)}:          # raw and URL-encoded forms
            msg = msg.replace(form, "<key>")
        raise SystemExit(f"{stem}: {type(e).__name__}: " + msg[:300]) from None
    body = r.content
    secs = round(time.monotonic() - t0, 2)
    ctype = (r.headers.get("Content-Type") or "").lower()
    rec = {"table": table, "time": time_clause, "url": url, "params": params, "status": r.status_code,
           "content_type": ctype, "bytes": len(body), "md5": hashlib.md5(body).hexdigest(), "seconds": secs,
           "fetched_utc": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    # A missing or bad key is an HTTP 200 HTML page (census.py): record it as what it is, not as data.
    rec["kind"] = ("html" if ("html" in ctype or body.lstrip()[:6].lower() == b"<html") else
                   "json" if r.status_code == 200 else "empty" if r.status_code == 204 else "error")
    if rec["kind"] == "json":
        try:
            js = json.loads(body.decode("utf-8"))
        except ValueError as e:
            js, rec["kind"], rec["error"] = None, "unparseable", str(e)[:120]
        if rec["kind"] == "json":
            if not isinstance(js, list) or not all(isinstance(row, list) for row in js):
                rec["kind"], rec["error"] = "unparseable", "the body is not a list of rows"
            else:
                rec["rows"] = max(0, len(js) - 1)
                rec["header"] = js[0] if js else []
                # The API answers the asked columns, then the predicate columns (review AR-443).
                if js and rec["header"] != params["get"].split(",") + ["CTY_CODE", "time"]:
                    rec["kind"] = "bad_header"
    if key.encode() in body:                                 # checked BEFORE anything is written
        raise SystemExit(f"{stem}: the response echoes the key; nothing written")
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, stem + ".raw"), "wb") as fh:
        fh.write(body)
    with open(os.path.join(out_dir, "manifest.jsonl"), "a", encoding="utf-8", newline="\n") as fh:
        fh.write(json.dumps(rec, sort_keys=True) + "\n")
    return rec


def plan(table: str, year: int) -> list:
    """Per-month time= clauses for one year, inside the table's affected range (first stored month..LAST_BAD).
    Per month, as census.py splits intltrade, because a whole year of naics timed out."""
    first = TABLES[table][2]
    return [ym for ym in (f"{year}-{m:02d}" for m in range(1, 13)) if first <= ym <= LAST_BAD]


def _inside(path: str, root: str) -> bool:
    p = os.path.normcase(os.path.realpath(path))
    r = os.path.normcase(os.path.realpath(root))
    return p == r or p.startswith(r.rstrip("\\/") + os.sep)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--out", required=True, help="scratch folder for raw responses + manifest.jsonl")
    ap.add_argument("--calibrate", action="store_true", help="R0: one old month and one clean month")
    ap.add_argument("--table", choices=sorted(TABLES))
    ap.add_argument("--year", type=int)
    a = ap.parse_args(argv)
    if _inside(a.out, ROOT):
        raise SystemExit("--out must be outside the checkout (a scratch folder)")
    if a.calibrate:
        work = CALIBRATE
    elif a.table and a.year:
        work = [(a.table, ym) for ym in plan(a.table, a.year)]
    else:
        ap.error("--calibrate, or --table with --year")
    key = _key()
    sess = requests.Session()
    for i, (t, ym) in enumerate(work):
        if i:
            time.sleep(GAP_S)
        rec = fetch(t, ym, a.out, key, sess)
        print(f"{t:7} {ym}  {rec['status']} {rec['kind']:5} rows={rec.get('rows', '-')} "
              f"bytes={rec['bytes']:,} {rec['seconds']}s", flush=True)
        if rec["kind"] != "json" or not rec.get("rows"):
            # An HTML page (a missing or bad key) or an error answers every later request the same way: stop.
            # No rows stops too, whether a 204 or a 200 with a header only: every month asked for is one the
            # store holds, so no rows is a gap the patch would silently skip (reviews AR-441 C2, AR-443).
            print(f"stopping: {t} {ym} answered {rec['kind']} rows={rec.get('rows', 0)} "
                  f"(HTTP {rec['status']})", flush=True)
            return 2
    # Re-runs append manifest lines while each .raw is overwritten: readers take the LAST line per stem.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
