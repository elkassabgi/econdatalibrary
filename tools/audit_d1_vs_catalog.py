"""Does D1 — the thing that actually answers requests — agree with the local catalogue?

verify_source_served answers this one source at a time and costs a wrangler round trip each. For
the whole surface that is 200+ invocations, so nobody runs it and the drift goes unseen. This
pulls every source's D1 count in ONE query and compares the lot.

THREE GAPS, and they are NOT the same problem:

  D1 BEHIND the catalogue   ids catalogued locally that D1 has never heard of. They 404 at the
                            API however healthy the local artefacts look. This is the noaa case:
                            3,135,873 catalogue rows, 3,135,873 R2 objects, and TEN rows in D1
                            (R224). Fix by running core/sync_catalog_d1.py.

  D1 AHEAD of the catalogue AMBIGUOUS, and the reason this tool exists. It can mean stale rows
                            advertising withdrawn ids — or perfectly good RETAINED LEGACY ids
                            whose objects exist and whose local rows a later re-catalogue
                            dropped. Both look identical from counts alone.

  IN D1, NOT SERVED         a source absent from SUPPORTED_SOURCES or from the local catalogue
                            entirely, still searchable in production. This is where the zillow
                            breach was found: a RESTRICTED source, withdrawn from the catalogue
                            and from R2, whose 52 D1 rows were never deleted and stayed
                            searchable with titles.

SO IT PROBES BEFORE IT JUDGES. For every ahead/orphan gap it FETCHES a sample from the live API
and reports what actually happened, because I once had 102 ids isolated and labelled "stale rows
advertising removed ids" on the strength of the count alone; all six I sampled returned HTTP 200
with real data — 470, 6,251 and 205 rows — and deleting them would have destroyed working series
(R227). zillow presented the identical symptom and WAS a breach. Only the fetch separates them.

Read-only. It deletes nothing and changes nothing; it prints what is true and what to do.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

API = "https://econdl-api.elkassabgi.workers.dev"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126 Safari/537.36")


def _wrangler() -> str | None:
    for name in ("wrangler.cmd", "wrangler"):
        p = os.path.join(ROOT, "api", "worker", "node_modules", ".bin", name)
        if os.path.exists(p):
            return p
    return None


def d1_counts() -> dict:
    exe = _wrangler()
    if not exe:
        raise SystemExit("wrangler not found under api/worker/node_modules/.bin")
    p = subprocess.run(
        [exe, "d1", "execute", "econ-catalog", "--remote", "--json", "--command",
         "select source_id, count(*) n from series group by 1"],
        cwd=os.path.join(ROOT, "api", "worker"), capture_output=True, text=True, timeout=900)
    if p.returncode != 0:
        raise SystemExit(f"wrangler exit {p.returncode}: {p.stderr[-300:]}")
    txt = p.stdout[p.stdout.index("["):]
    return {r["source_id"]: r["n"] for r in json.loads(txt)[0]["results"]}


def supported() -> set:
    ts = open(os.path.join(ROOT, "api", "worker", "src", "util.ts"), encoding="utf-8").read()
    blk = ts.split("SUPPORTED_SOURCES: readonly string[] = [", 1)[1].split("];", 1)[0]
    return set(re.findall(r'"([a-z0-9_]+)"', re.sub(r"//[^\n]*", "", blk)))


def api_key() -> str | None:
    try:
        m = re.search(r"^EDL_API_KEY=(.+)$",
                      open(os.path.join(ROOT, ".env.local"), encoding="utf-8").read(), re.M)
        return m.group(1).strip() if m else None
    except OSError:
        return None


def probe(series_id: str, key: str | None):
    """(status, rows) from the LIVE api. The only thing that tells an orphan from a survivor."""
    url = f"{API}/v1/series/{urllib.parse.quote(series_id, safe='')}.csv"
    hdr = {"User-Agent": UA}
    if key:
        hdr["X-API-Key"] = key
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=hdr), timeout=90) as r:
            body = r.read().decode(errors="replace")
            rows = [ln for ln in body.split("\n") if ln and not ln.startswith("#")]
            return r.status, max(0, len(rows) - 1)
    except urllib.error.HTTPError as e:
        return e.code, 0
    except Exception as e:                                     # noqa: BLE001
        return type(e).__name__, 0


def d1_ids(source: str) -> set:
    exe = _wrangler()
    p = subprocess.run(
        [exe, "d1", "execute", "econ-catalog", "--remote", "--json", "--command",
         f"select series_id from series where source_id='{source}'"],
        cwd=os.path.join(ROOT, "api", "worker"), capture_output=True, text=True, timeout=900)
    if p.returncode != 0:
        return set()
    txt = p.stdout[p.stdout.index("["):]
    return {r["series_id"] for r in json.loads(txt)[0]["results"]}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=3,
                    help="ids to FETCH per gap before calling it anything")
    a = ap.parse_args()

    sup = supported()
    con = sqlite3.connect(f"file:{os.path.join(ROOT, 'data', 'catalog.db')}?mode=ro",
                          uri=True, timeout=180.0)
    cat = dict(con.execute("select source_id, count(*) from series group by 1").fetchall())
    served = {s: n for s, n in cat.items() if s in sup and n}
    d1 = d1_counts()
    key = api_key()
    print(f"D1 sources {len(d1)} · served sources {len(served)} · "
          f"api key {'present' if key else 'ABSENT (probes may 403)'}\n")

    behind = sorted(((s, n, d1.get(s, 0)) for s, n in served.items() if d1.get(s, 0) < n),
                    key=lambda r: r[2] - r[1])
    ahead = sorted(((s, n, d1[s]) for s, n in served.items() if d1.get(s, 0) > n),
                   key=lambda r: r[1] - r[2])
    orphan = sorted(((s, m) for s, m in d1.items() if s not in served and m),
                    key=lambda r: -r[1])

    print(f"D1 BEHIND the catalogue — those ids 404 at the API ({len(behind)}):")
    for s, n, m in behind:
        print(f"   {s:24s} catalogue {n:>10,}  D1 {m:>10,}  gap {n - m:>10,}"
              f"   -> python -m core.sync_catalog_d1 --source {s}")
    if not behind:
        print("   none")

    print(f"\nD1 AHEAD of the catalogue ({len(ahead)}) — PROBED, because the count alone cannot "
          f"tell a retained legacy id from a stale one:")
    for s, n, m in ahead:
        extra = sorted(d1_ids(s) - {r[0] for r in con.execute(
            "select series_id from series where source_id=?", (s,))})
        verdicts = [(sid, *probe(sid, key)) for sid in extra[:a.sample]]
        alive = sum(1 for _sid, st, _rows in verdicts if st == 200)
        print(f"   {s:24s} catalogue {n:>10,}  D1 {m:>10,}  extra {m - n:>6,}"
              f"   probed {alive}/{len(verdicts)} ALIVE")
        for sid, st, rows in verdicts:
            print(f"      {sid[:52]:52s} HTTP {st}  {rows:>8,} rows")
        print("      -> RETAINED LEGACY, leave alone" if alive == len(verdicts) and verdicts
              else "      -> mixed or dead; check each before touching anything")
    if not ahead:
        print("   none")

    print(f"\nIN D1 BUT NOT SERVED ({len(orphan)}) — searchable in production, source not in "
          f"SUPPORTED_SOURCES or not catalogued locally:")
    for s, m in orphan:
        row = con.execute("select series_id from series where source_id=? limit 1", (s,)).fetchone()
        sid = row[0] if row else next(iter(d1_ids(s)), None)
        st, rows = probe(sid, key) if sid else ("n/a", 0)
        print(f"   {s:24s} D1 {m:>8,}   sample {str(sid)[:40]} -> HTTP {st} ({rows:,} rows)")
        print("      -> serves: it is UNLISTED, not broken — decide list-or-delist"
              if st == 200 else
              "      -> does NOT serve: searchable and undownloadable. If the source is GATED "
              "this is a licence exposure — delete the D1 rows.")
    if not orphan:
        print("   none")
    return 0


if __name__ == "__main__":
    sys.exit(main())
