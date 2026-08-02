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


def _supported():
    """The worker's SUPPORTED_SOURCES, or None if it cannot be read (UNCHECKED, never 'fine')."""
    try:
        import re
        ts = open(os.path.join(ROOT, "api", "worker", "src", "util.ts"),
                  encoding="utf-8").read()
        blk = ts.split("SUPPORTED_SOURCES: readonly string[] = [", 1)[1].split("];", 1)[0]
        return set(re.findall(r'"([a-z0-9_]+)"', re.sub(r"//[^\n]*", "", blk)))
    except Exception:                                          # noqa: BLE001
        return None


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
    sup = _supported()
    in_sup = a.source in sup if sup is not None else None
    if d1_err:
        print(f"D1             : UNCHECKED ({d1_err})")
    else:
        gap = len(cat) - d1_n
        print(f"D1             : {d1_n:,} row(s)"
              + (f"  — {gap:,} CATALOGUED BUT NOT IN D1: those ids 404 at the API"
                 if gap > 0 else "  — matches the catalogue"))
    print(f"SUPPORTED_SOURCES: {'yes' if in_sup else 'NO — every id answers 501 not_migrated'
                               if in_sup is False else 'unchecked'}")

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
