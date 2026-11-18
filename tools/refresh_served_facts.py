#!/usr/bin/env python3
"""Measure what the public API serves and write catalog/served_facts.json - the FIRST step of every site build.

    python tools/refresh_served_facts.py --dry-run          # measure, print the diff, write nothing
    python tools/refresh_served_facts.py                    # measure and write
    python tools/refresh_served_facts.py --source statcan   # measure ONE source and print it (never writes)
    python catalog/gen_site.py                              # then the build, which reads the file

catalog/gen_site.py caps every page's series count at what the API serves (load_served_facts), and since
2026-11-18 REFUSES to build when the file is missing, older than gen_site.SERVED_FACTS_MAX_AGE_HOURS, or has
no entry for a source that gets a page. This tool is the one writer of that file.

WHAT IS MEASURED. Exactly the sources that get a page, decided by CALLING gen_site.page_source_ids (the
predicate gen_site's own refusal uses): a `source` row whose licence is reservable, not in the worker's
denylist (gen_site.DENYLISTED - those answer 451 and get no page), with at least one catalogued series. The catalogue is opened READ-ONLY
through core.catalog_path. For each one:

    GET https://econdl-api.elkassabgi.workers.dev/v1/catalog?source=<id>&limit=1  ->  total

The worker answers from its cached per-source count (a carve-out source takes one bounded count) behind a
6 h edge cache, so a full run is cheap on D1. Requests are SEQUENTIAL, 0.5 s apart, with a generic
User-Agent. A source gets at most TWO tries, and the second only after a network error or a 5xx; a 4xx is
not retried (it is an answer, not an outage). There is no cache-buster.

WHAT IS RECORDED. `n_series` per source (0 is a measurement - the page then says "not yet served"), and
`edge_cache` per source: the CF-Cache-Status and Age response headers (age_s null when absent). An
edge-cached answer is as old as its Age, so `measured_utc` is the EARLIEST (fetch time - Age) over all
sources: the oldest serving state any count in the file can describe, which is what gen_site's age limit
is judged against. An answer with no cache header was computed at fetch time; a cache status with no Age
is charged the cache's full lifetime (EDGE_MAX_AGE_S) - see observed_at. The `frequencies`
block is hand-measured (not by this tool) and is carried over unchanged.

ALL OR NOTHING. If any source cannot be measured, the tool names every failure, exits 1 and writes NOTHING:
a partial file would leave the failed sources' pages uncapped (gen_site refuses a missing entry anyway).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

API = "https://econdl-api.elkassabgi.workers.dev"
INSTRUMENT = f"GET {API}/v1/catalog?source=<id>&limit=1 -> total"
FACTS_PATH = os.path.join(ROOT, "catalog", "served_facts.json")
USER_AGENT = "econdl-served-facts/1.0"
PAUSE_S = 0.5
TRIES = 2
TIMEOUT_S = 60
# The longest a /v1/catalog answer can sit in the edge cache: s-maxage=21600 (api/worker/src/index.ts, the
# /v1/catalog branch). Charged to a cached answer that carries no Age (observed_at), so measured_utc never
# claims a fresher serving state than the cache can guarantee.
EDGE_MAX_AGE_S = 21600
ABOUT = ("What the public API serves, measured. gen_site.py never lets a page claim more series than "
         "n_series here, and uses `frequencies` in place of the local catalogue's for a listed source. "
         "Refresh n_series by GET /v1/catalog?source=<id>&limit=1 -> total for every source.")
STAMP = "%Y-%m-%dT%H:%M:%SZ"


class MeasureError(Exception):
    pass


# ---- the HTTP layer (tests replace it) -------------------------------------------------------------------
def http_get(url: str) -> tuple[int, dict, bytes]:
    """(status, headers lower-cased, body). Raises OSError on a network failure. Never follows a cache-buster."""
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_S) as r:
            return r.status, {k.lower(): v for k, v in r.headers.items()}, r.read()
    except urllib.error.HTTPError as e:
        return e.code, {k.lower(): v for k, v in (e.headers or {}).items()}, e.read() or b""


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def measure_one(sid: str, get=http_get, now=utcnow, sleep=time.sleep) -> dict:
    """{"n": total, "status": CF-Cache-Status, "age_s": Age, "fetched_utc": ...} or MeasureError.
    At most TRIES attempts; only a network error or a 5xx earns the second one."""
    url = f"{API}/v1/catalog?source={sid}&limit=1"
    last = None
    for attempt in range(1, TRIES + 1):
        t0 = now()
        try:
            status, headers, body = get(url)
        except OSError as e:                                     # network: may be retried once
            last = f"{type(e).__name__}: {e}"
            if attempt < TRIES:
                sleep(PAUSE_S)
            continue
        if status >= 500:
            last = f"HTTP {status}"
            if attempt < TRIES:
                sleep(PAUSE_S)
            continue
        if status != 200:
            raise MeasureError(f"{sid}: HTTP {status} (not retried: a 4xx is an answer)")
        try:
            doc = json.loads(body)
            total = doc["total"]
        except (ValueError, KeyError, TypeError) as e:
            raise MeasureError(f"{sid}: HTTP 200 without a readable `total` ({type(e).__name__})") from None
        if isinstance(total, bool) or not isinstance(total, int) or total < 0:
            raise MeasureError(f"{sid}: `total` is {total!r}, not a count")
        try:
            age = max(0, int(headers["age"])) if headers.get("age") not in (None, "") else None
        except ValueError:
            age = None
        return {"n": total, "status": headers.get("cf-cache-status", ""), "age_s": age,
                "fetched_utc": t0.strftime(STAMP), "_t": t0}
    raise MeasureError(f"{sid}: {last} on all {TRIES} tries")


# ---- which sources -------------------------------------------------------------------------------------
def page_sources(con, gen) -> tuple[list, int]:
    """(sorted page source ids, number of would-be pages skipped as denylisted), decided by gen_site's OWN
    predicate `gen.page_source_ids` - the same call gen_site's refusal uses, so the two cannot drift. Its
    inputs are read cheaply: the source and license tables, and one primary-key range probe per source for
    "has a series" (page_source_ids needs only truthiness there, not gen_site's 13.9M-row rollup)."""
    licenses = {r[0]: {"reservable": r[1]} for r in con.execute("SELECT license_id, reservable FROM license")}
    sources = {r[0]: {"license_id": r[1]} for r in con.execute("SELECT source_id, license_id FROM source")}
    has_series = {sid: True for sid in sources
                  if con.execute("SELECT 1 FROM series WHERE series_id >= ? AND series_id < ? LIMIT 1",
                                 (sid + ":", sid + ";")).fetchone()}
    pages = gen.page_source_ids(sources, licenses, has_series)
    denied = {s for s in has_series if s in gen.DENYLISTED
              and bool((licenses.get(sources[s]["license_id"]) or {}).get("reservable", 0))}
    return sorted(pages), len(denied)


def open_catalogue(db: str | None):
    from core import catalog_path
    return catalog_path.connect_path(db or catalog_path.under(ROOT), write=False)


def load_gen_site():
    """catalog/gen_site.py as a module (it is a script, not a package member). Importing it reads only repo
    files (the denylist, withheld list, registry); it opens no catalogue and makes no request."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("gen_site_for_refresh", os.path.join(ROOT, "catalog", "gen_site.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---- the run -------------------------------------------------------------------------------------------
def measure_all(sids, get=http_get, now=utcnow, sleep=time.sleep, log=print):
    """({sid: result}, {sid: error}). Sequential, PAUSE_S apart."""
    ok, errors = {}, {}
    for i, sid in enumerate(sids):
        if i:
            sleep(PAUSE_S)
        try:
            ok[sid] = measure_one(sid, get=get, now=now, sleep=sleep)
        except MeasureError as e:
            errors[sid] = str(e)
            log(f"  FAILED {e}")
    return ok, errors


def observed_at(r: dict) -> datetime:
    """When the count in this answer was read from D1: fetch time minus its Age.

    Measured 2026-11-18: an answer the worker computed fresh carries NEITHER CF-Cache-Status NOR Age (bcrp,
    cold, 11:45:04Z), and the same URL served from the cache two minutes later carried CF-Cache-Status HIT
    and Age 125. So no cache header at all = computed at fetch time (age 0). A cache status WITHOUT an Age
    is not explained by that measurement, so it is charged the cache's whole lifetime (EDGE_MAX_AGE_S)."""
    if r["age_s"] is not None:
        age = r["age_s"]
    else:
        age = EDGE_MAX_AGE_S if r["status"] else 0
    return r["_t"] - timedelta(seconds=age)


def build_doc(results: dict, previous: dict | None) -> dict:
    observed = [observed_at(r) for r in results.values()]
    fetched = [r["_t"] for r in results.values()]
    return {
        "_about": ABOUT,
        "measured_utc": min(observed).strftime(STAMP),
        "fetched_utc": {"first": min(fetched).strftime(STAMP), "last": max(fetched).strftime(STAMP)},
        "instrument": INSTRUMENT,
        "n_series": {s: results[s]["n"] for s in sorted(results)},
        "edge_cache": {s: {"cf_cache_status": results[s]["status"], "age_s": results[s]["age_s"]}
                       for s in sorted(results)},
        "frequencies": (previous or {}).get("frequencies") or {},
    }


def diff(old: dict, new: dict) -> dict:
    o, n = (old or {}).get("n_series", {}), new["n_series"]
    return {"added": {s: n[s] for s in sorted(set(n) - set(o))},
            "removed": {s: o[s] for s in sorted(set(o) - set(n))},
            "changed": {s: (o[s], n[s]) for s in sorted(set(o) & set(n)) if o[s] != n[s]},
            "unchanged": sum(1 for s in set(o) & set(n) if o[s] == n[s])}


def print_diff(d: dict, old: dict | None, new: dict) -> None:
    print(f"  measured_utc: {(old or {}).get('measured_utc')} -> {new['measured_utc']}")
    print(f"  sources: {len((old or {}).get('n_series', {}))} -> {len(new['n_series'])}; unchanged {d['unchanged']}, "
          f"changed {len(d['changed'])}, added {len(d['added'])}, removed {len(d['removed'])}")
    for s, (a, b) in d["changed"].items():
        print(f"    changed  {s}: {a} -> {b} ({b - a:+d})")
    for s, v in d["added"].items():
        print(f"    added    {s}: {v}")
    for s, v in d["removed"].items():
        print(f"    removed  {s}: {v}")
    zero = [s for s, v in new["n_series"].items() if v == 0]
    if zero:
        print(f"  served 0 (pages will say 'not yet served'): {zero}")
    stat = {}
    for e in new["edge_cache"].values():
        stat[e["cf_cache_status"] or "(none)"] = stat.get(e["cf_cache_status"] or "(none)", 0) + 1
    ages = [e["age_s"] for e in new["edge_cache"].values() if e["age_s"] is not None]
    print(f"  CF-Cache-Status: {stat}; Age header on {len(ages)} of {len(new['edge_cache'])} answers"
          + (f", max {max(ages)} s" if ages else ""))


def write_atomic(path: str, doc: dict) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(doc, fh, indent=1, ensure_ascii=False)
        fh.write("\n")
    os.replace(tmp, path)


def read_previous(path: str) -> dict | None:
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def run(argv=None, *, get=http_get, now=utcnow, sleep=time.sleep, sources=None, out_path=FACTS_PATH) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--dry-run", action="store_true", help="measure and print the diff; write nothing")
    ap.add_argument("--source", action="append", default=[],
                    help="measure only this source (repeatable); prints, never writes")
    ap.add_argument("--db", help="the catalogue to read the page sources from (default: this checkout's)")
    a = ap.parse_args(argv)

    if a.source:
        sids = sorted(set(a.source))
    elif sources is not None:
        sids = sorted(sources)
    else:
        con = open_catalogue(a.db)
        try:
            sids, skipped = page_sources(con, load_gen_site())
        finally:
            con.close()
        print(f"  page sources: {len(sids)} (denylisted, no page, not measured: {skipped})")
    if not sids:
        print("FAILED: no source to measure - refusing to write an empty file")
        return 1

    started = time.monotonic()
    results, errors = measure_all(sids, get=get, now=now, sleep=sleep)
    print(f"  measured {len(results)} of {len(sids)} in {time.monotonic() - started:.1f} s; failed {len(errors)}")
    if a.source:
        for s in sids:
            r = results.get(s)
            print(f"    {s}: " + (f"total {r['n']}, CF-Cache-Status {r['status'] or '(none)'}, "
                                  f"Age {'(none)' if r['age_s'] is None else str(r['age_s']) + ' s'}"
                                  if r else errors[s]))
        return 1 if errors else 0
    if errors:
        print(f"FAILED: {len(errors)} source(s) could not be measured - {out_path} NOT written:")
        for s, e in errors.items():
            print(f"    {e}")
        return 1

    previous = read_previous(out_path)
    doc = build_doc(results, previous)
    print_diff(diff(previous, doc), previous, doc)
    if a.dry_run:
        print(f"  --dry-run: {out_path} NOT written")
        return 0
    write_atomic(out_path, doc)
    print(f"  wrote {out_path}: {len(doc['n_series'])} sources, measured_utc {doc['measured_utc']}")
    return 0


if __name__ == "__main__":
    sys.exit(run())
