"""Is every catalogued series of a source actually downloadable, and vice versa?

BOTH DIRECTIONS, because each one alone hides a different failure:
  MISSING  catalogued with no R2 object -> the site lists a series whose download 404s
  ORPHANED an R2 object with no catalogue row -> paid-for storage nobody can find

A presence count is not enough either. "52,322 objects exist" passes while every one of them
holds the wrong series, so this also byte-compares a random sample against the resolver - the
same contract derive_csv_bulk gates on, checked after the upload rather than before it.

    python tools/verify_source_served.py --source fed_board --sample 40
"""
from __future__ import annotations

import argparse
import os
import random
import sqlite3
import sys
import urllib.parse

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))


API_BASE = os.environ.get("ECONDL_API", "https://econdl-api.elkassabgi.workers.dev")


def _listed_live(source: str):
    """Is this source visible on the LIVE API? True / False / None (unchecked).

    ASK THE RUNNING SYSTEM, NOT THE SOURCE FILE. This used to parse SUPPORTED_SOURCES out of
    api/worker/src/util.ts and call the result "the worker's SUPPORTED_SOURCES". It is not: that
    constant takes effect only once the worker is DEPLOYED, and nothing in .github/workflows
    deploys the worker. The check therefore flipped to 'yes' the instant I edited a text file,
    and stayed 'yes' while the live worker — last deployed 2026-08-02 — could not serve those ids
    at all. It reported my own intent back to me and I read it as a verdict. Cost: 425,462 series
    across three tasks called "SERVED" and "live" while unreachable (R345). The identical lesson
    is already written into the D1 leg below (R224); this leg was never held to it.

    WHY /v1/sources AND NOT A CSV PROBE. My first attempt fetched /v1/series/<id>.csv and treated
    "not 501" as supported. Measured against the live API, that discriminates NOTHING: auth runs
    BEFORE the migration gate, so a fabricated id returns the same 401 as a real one —

        zillow:ZHVI_US -> 401   not_a_real_source:abc -> 401   imf_cpi_direct:<real> -> 401

    A check that returns True for `not_a_real_source` is worse than no check. /v1/sources is
    unauthenticated and discriminates for real (verified: imf_cpi_direct listed, zillow and ksh
    absent).

    NOTE THE SCOPE HONESTLY: this proves the source is DISCOVERABLE on the deployed API — it
    needs a `source` row, >=1 series row in D1, and a worker that answers. It does not by itself
    isolate SUPPORTED_SOURCES membership; no unauthenticated endpoint exposes that today, and
    inventing a green light for it is what caused R345.
    """
    import json
    import urllib.request
    # EXPLICIT User-Agent: urllib's default ("Python-urllib/3.x") is refused with 403 by the
    # edge, while curl gets 200 for the same URL. Without this the probe returns None for every
    # source — "unchecked" for all of them, which is at least honest but useless.
    req = urllib.request.Request(f"{API_BASE}/v1/sources",
                                 headers={"User-Agent": "econdl-verify/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            d = json.loads(r.read().decode("utf-8"))
        rows = d if isinstance(d, list) else d.get("sources", d.get("data", []))
        ids = {(x.get("source") or x.get("source_id") or x.get("id"))
               for x in rows if isinstance(x, dict)}
        return source in ids
    except Exception:                                          # noqa: BLE001
        return None                                            # UNCHECKED, never "fine"


def _d1_count(source: str):
    """(rows_in_D1, None) or (0, reason). D1 is what the worker reads to answer a request, so a
    source absent from it 404s no matter how coherent the local catalogue and R2 are."""
    import json
    import subprocess
    exe = os.path.join(ROOT, "api", "worker", "node_modules", ".bin", "wrangler.cmd")
    if not os.path.exists(exe):
        exe = os.path.join(ROOT, "api", "worker", "node_modules", ".bin", "wrangler")
    if not os.path.exists(exe):
        return 0, "wrangler not found"
    try:
        p = subprocess.run(
            [exe, "d1", "execute", "econ-catalog", "--remote", "--json", "--command",
             f"select count(*) n from series where source_id='{source}'"],
            cwd=os.path.join(ROOT, "api", "worker"), capture_output=True, text=True,
            timeout=300)
        if p.returncode != 0:
            return 0, f"wrangler exit {p.returncode}"
        txt = p.stdout[p.stdout.index("["):]
        return int(json.loads(txt)[0]["results"][0]["n"]), None
    except Exception as e:                                     # noqa: BLE001
        return 0, f"{type(e).__name__}: {str(e)[:50]}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True)
    ap.add_argument("--bucket", default="econ-data")
    ap.add_argument("--prefix", default="series")
    ap.add_argument("--sample", type=int, default=40,
                    help="byte-compare this many RANDOM served objects against the resolver")
    a = ap.parse_args()

    con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=180.0)
    con.execute("PRAGMA busy_timeout = 180000")
    cat = {r[0] for r in con.execute(
        "select series_id from series where source_id=?", (a.source,))}
    print(f"catalogue rows : {len(cat):,}")

    from core import r2_util
    s3 = r2_util.client()
    pref = f"{a.prefix}/{urllib.parse.quote(a.source + ':', safe='')}"
    keys, tok = set(), None
    while True:
        kw = {"Bucket": a.bucket, "Prefix": pref, "MaxKeys": 1000}
        if tok:
            kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            k = o["Key"]
            if k.endswith(".csv"):
                keys.add(urllib.parse.unquote(k[len(a.prefix) + 1:-4]))
        if not r.get("IsTruncated"):
            break
        tok = r["NextContinuationToken"]
    print(f"R2 objects     : {len(keys):,}")

    missing = sorted(cat - keys)
    orphan = sorted(keys - cat)
    print(f"MISSING  (catalogued, no object): {len(missing):,}")
    for s in missing[:5]:
        print(f"   {s}")

    # NOT ALL ORPHANS ARE THE SAME, and treating them alike makes this tool cry wolf.
    # When a source's id shape changes, the old objects are deliberately KEPT so existing
    # links keep working (fed_board went from a bare key to a flow-qualified one and 21 legacy
    # objects were retained on purpose). Those still RESOLVE. An object whose id resolves to
    # nothing is the real defect: storage nobody can reach by any id.
    from econdl import _resolve
    retained, junk = [], []
    for s in orphan:
        try:
            _resolve.resolve(s)
            retained.append(s)
        except Exception:                                      # noqa: BLE001
            junk.append(s)
    print(f"ORPHANED (object, no catalogue row): {len(orphan):,}"
          f"  — {len(retained):,} still resolve (retained legacy ids), {len(junk):,} unreachable")
    for s in junk[:5]:
        print(f"   UNREACHABLE {s}")
    for s in retained[:3]:
        print(f"   retained    {s}")

    # WHAT THE BYTE-COMPARE BELOW CANNOT SEE — stated here because it was quoted as proof and
    # should not have been (ledger R385). `_series_csv_bytes` resolves through the LOCAL mirror
    # under data/clean_full/. If that mirror is behind R2, the served object and the "expected"
    # bytes come from the SAME wrong copy, so a clean result establishes served == local and
    # says nothing about the store. An adversarial audit measured 1,379 local files behind R2
    # (ilostat 952, eurostat 124, owid 58) while this tool was printing 25/25 identical, and two
    # of those "clean" sources were live regressions — ons_uk/weekly-deaths-age-sex served
    # 31,878 rows against a 37,950-row store parquet.
    #
    # So check the mirror FIRST and withhold the byte verdict when it is behind. A withheld
    # verdict is useful; a false "identical" is worse than no check at all.
    if a.sample:
        try:
            from core.derive_csv import _mirror_behind_store
            stale = _mirror_behind_store([a.source])
        except Exception as e:                                 # noqa: BLE001
            stale = []
            print(f"   (mirror-vs-R2 check unavailable: {e!r} — read the byte-compare as "
                  f"CONSISTENCY ONLY, not verification)")
        if stale:
            for _s, detail in stale:
                print(f"MIRROR BEHIND R2 : {detail}")
            print("byte-compare   : WITHHELD — the local mirror is behind the store, so "
                  "comparing served bytes against it could only prove served==local. Sync "
                  "this source's parquets from R2 and re-run.")
            a.sample = 0

    # byte-compare a random sample of what is actually served
    bad = 0
    both = sorted(cat & keys)
    if a.sample and both:
        from core.derive_csv import _series_csv_bytes
        rnd = random.Random(20260801)
        for sid in rnd.sample(both, min(a.sample, len(both))):
            key = f"{a.prefix}/{urllib.parse.quote(sid, safe='')}.csv"
            got = s3.get_object(Bucket=a.bucket, Key=key)["Body"].read()
            try:
                want = _series_csv_bytes(sid)
            except Exception as e:                             # noqa: BLE001
                bad += 1
                print(f"   UNRESOLVABLE {sid}: {type(e).__name__} {str(e)[:80]}")
                continue
            if got != want:
                bad += 1
                if bad <= 3:
                    print(f"   MISMATCH {sid}\n      served: {got[:110]!r}\n"
                          f"      resolver: {want[:110]!r}")
        print(f"byte-compare   : {min(a.sample, len(both)) - bad}/"
              f"{min(a.sample, len(both))} identical")

    # THE THIRD LEG. A series is reachable only if it is in D1 *and* its source is in
    # SUPPORTED_SOURCES *and* its object is in R2. This tool used to check catalogue<->R2 and
    # then print "SERVED", which is a claim about all three. noaa passed it with 3,135,873
    # catalogue rows and 3,135,873 objects while D1 held TEN — every other series would have
    # 404'd (R224). Local artefacts agreeing with each other is not evidence that a request
    # succeeds.
    d1_n, d1_err = _d1_count(a.source)
    # Probe the DEPLOYED worker with a real id from this source, not the local util.ts.
    in_sup = _listed_live(a.source)
    if d1_err:
        print(f"D1             : UNCHECKED ({d1_err})")
    else:
        gap = len(cat) - d1_n
        print(f"D1             : {d1_n:,} row(s)"
              + (f"  — {gap:,} CATALOGUED BUT NOT IN D1: those ids 404 at the API"
                 if gap > 0 else "  — matches the catalogue"))
    print(f"LIVE /v1/sources : {'listed — discoverable on the deployed API'
                                if in_sup else 'NOT LISTED — invisible to anyone browsing'
                                if in_sup is False else 'unchecked (probe failed)'}")

    coherent = not missing and not junk and not bad
    reachable = (d1_err is None and d1_n >= len(cat)) and in_sup is not False
    if coherent and reachable:
        # Say what was actually verified. "ORPHANED 0" would be false here — there are 21
        # retained legacy objects — and a summary line that overstates is how a check stops
        # being worth reading.
        note = ("SERVED — MISSING 0, 0 unreachable objects, sample byte-identical, "
                "D1 in step, source supported"
                + (f"; {len(retained):,} retained legacy id(s)" if retained else ""))
    elif coherent:
        note = ("STORE COHERENT BUT NOT REACHABLE — catalogue and R2 agree, but "
                + ("D1 is behind" if d1_err is None and d1_n < len(cat) else "")
                + (" and " if (d1_err is None and d1_n < len(cat)) and in_sup is False else "")
                + ("the source is absent from SUPPORTED_SOURCES" if in_sup is False else "")
                + ". Users cannot fetch these ids yet.")
    else:
        note = (f"NOT CLEAN — missing {len(missing):,}, unreachable {len(junk):,}, "
                f"byte-mismatch {bad}")
    print(f"\n{a.source}: {note}")
    return 0 if (coherent and reachable) else 1


if __name__ == "__main__":
    sys.exit(main())
