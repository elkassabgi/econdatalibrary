"""Make SEC fundamentals findable by TICKER, not just by company name.

THE DEFECT. `sec_edgar:GOOGL` serves Alphabet's full XBRL fundamentals, and
`sec_edgar:GOOG` returns 404 — even though SEC maps BOTH tickers (plus GOOGM and
GOOGN) to the same registrant, CIK 1652044. Ahmed hit exactly this: "it's goog not
googl". Measured against SEC's own company_tickers.json, that generalises to 2,050
valid tickers across 1,470 multi-ticker CIKs whose company we already serve under a
different ticker.

Worse, the data was effectively unfindable by ticker at all: series_fts indexes
`title` and `geography`, and 5,905 of our 6,722 ticker-keyed titles are a bare
company name with no ticker in them ("Alphabet Inc."). So searching GOOGL — the id we
DO serve — returned only the 1-minute bars series, and searching GOOG returned
nothing. A reasonable person concludes the library has no company fundamentals. One
did.

THE FIX HERE is deliberately the cheap half: fold every ticker SEC maps to that CIK
into the title, so all of them are searchable and the metadata names the id that
works. "Alphabet Inc." becomes "Alphabet Inc. (GOOGL, GOOG, GOOGM, GOOGN)". No data
is duplicated, no worker deploy is needed, and nothing about the served CSVs changes.

WHAT THIS DOES NOT FIX: `sec_edgar:GOOG` still 404s on a direct id lookup. Making
alternate ids resolve needs an alias table plus worker resolution, which is a serving
change and a separate step. Search now leads you to the id that works, which is the
difference between "we don't have it" and "it's under another name".

Both stores are written, because they disagree by design: the local catalog.db is the
curated source of truth and D1 is what the worker actually reads. A licence fix
earlier in this rollout was inert for exactly this reason (R96), and series_fts is a
standalone FTS5 table — not external-content — so it does NOT track series.title and
must be updated too or search keeps the old text.

Usage:  python tools/enrich_sec_edgar_tickers.py            (local only, prints plan)
        python tools/enrich_sec_edgar_tickers.py --d1       (also writes D1)
"""
from __future__ import annotations

import collections
import io
import json
import os
import re
import sqlite3
import subprocess
import sys
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, "data", "catalog.db")
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
CHUNK = 400          # statements per wrangler invocation


def sec_ticker_map():
    """{cik: {tickers}}, {ticker: cik} straight from SEC's own mapping file."""
    d = json.load(urllib.request.urlopen(
        urllib.request.Request(TICKERS_URL, headers=UA), timeout=180))
    rows = list(d.values()) if isinstance(d, dict) else d
    bycik, byticker = collections.defaultdict(set), {}
    for r in rows:
        bycik[r["cik_str"]].add(r["ticker"])
        byticker[r["ticker"]] = r["cik_str"]
    return bycik, byticker


def plan():
    bycik, byticker = sec_ticker_map()
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    rows = con.execute(
        "SELECT series_id, title FROM series WHERE source_id='sec_edgar'").fetchall()
    out = []
    for sid, title in rows:
        tick = sid.split(":", 1)[1]
        if tick.startswith("CIK"):
            continue                      # no ticker to advertise
        cik = byticker.get(tick)
        if cik is None:
            continue                      # ticker SEC no longer lists; leave alone
        alls = [tick] + sorted(bycik[cik] - {tick})
        base = re.sub(r"\s*\([A-Z0-9\.\-,\s]+\)\s*$", "", str(title or "")).strip()
        if not base:
            base = tick
        new = f"{base} ({', '.join(alls)})"
        # EVERY ticker-keyed series is emitted, not just the ones whose LOCAL title
        # differs. The first version diffed against catalog.db and wrote it first, so
        # a follow-up `--d1` run recomputed the plan against its own completed work,
        # found nothing to do, and left D1 — the store the worker actually reads —
        # untouched while reporting success. Two stores that can disagree cannot share
        # one diff; the plan is now the DESIRED state and both writers are idempotent
        # UPDATEs, so re-running is safe and D1 is never skipped because local is
        # already correct.
        out.append((sid, new, title))
    return out


def write_local(changes):
    con = sqlite3.connect(DB)
    con.executemany("UPDATE series SET title=? WHERE series_id=?",
                    [(n, s) for s, n, _ in changes])
    con.commit()
    return con.total_changes


def d1_sql(changes):
    """UPDATEs for `series` only. series_fts is rebuilt separately — see below."""
    def esc(s):
        return s.replace("'", "''")
    return [f"UPDATE series SET title='{esc(new)}' WHERE series_id='{esc(sid)}';"
            for sid, new, _ in changes]


# series_fts is a STANDALONE fts5 table (`fts5(series_id UNINDEXED, title,
# geography)`) — it does not mirror series.title, so leaving it alone keeps search on
# the OLD text. But it cannot be updated row-by-row either: `series_id` is UNINDEXED,
# so `UPDATE series_fts ... WHERE series_id = ?` is a FULL SCAN. Measured on D1: one
# such statement read 2,071,107 rows to write 1. At 6,595 rows that is ~13.7 billion
# row reads, which is why batching them 400-per-call failed outright rather than
# merely being slow.
#
# One delete and one insert-select replace the whole source's index entries in two
# passes instead of 6,595 scans.
FTS_REBUILD = [
    "DELETE FROM series_fts WHERE series_id LIKE 'sec_edgar:%';",
    "INSERT INTO series_fts (series_id, title, geography) "
    "SELECT series_id, title, geography FROM series WHERE source_id='sec_edgar';",
]


def run_d1(stmts):
    wdir = os.path.join(ROOT, "api", "worker")
    tmp = os.path.join(ROOT, "data", "_sec_ticker_titles.sql")
    done = 0
    for i in range(0, len(stmts), CHUNK):
        io.open(tmp, "w", encoding="utf-8").write("\n".join(stmts[i:i + CHUNK]))
        # encoding/errors are REQUIRED here: text=True alone decodes with the
        # Windows ANSI codepage, and wrangler's output contains bytes cp1252 cannot
        # map — which raises UnicodeDecodeError from inside subprocess and kills the
        # run for a reason that has nothing to do with the SQL. Then `r.stderr` is
        # None, so a handler doing `(r.stderr or r.stdout)[-400:]` crashes on top of
        # the real error and hides it.
        r = subprocess.run(
            ["npx", "wrangler", "d1", "execute", "econ-catalog", "--remote",
             "--file", tmp, "-y"],
            cwd=wdir, capture_output=True, text=True,
            encoding="utf-8", errors="replace", shell=(os.name == "nt"))
        if r.returncode != 0:
            msg = (r.stderr or "") + (r.stdout or "") or "(no output captured)"
            print(f"  FAILED at statement {i}: {msg[-500:]}", flush=True)
            return done
        done += len(stmts[i:i + CHUNK])
        print(f"  applied {done:,}/{len(stmts):,} statements", flush=True)
    if os.path.exists(tmp):
        os.remove(tmp)
    return done


def main():
    changes = plan()
    differing = [c for c in changes if c[1] != c[2]]
    print(f"sec_edgar ticker-keyed series: {len(changes):,}  "
          f"(local titles currently differing: {len(differing):,})")
    for s, n, old in (differing or changes)[:6]:
        print(f"   {s:<24} {str(old)[:34]:<34} -> {n[:56]}")
    if not changes:
        return 0
    n = write_local(changes)
    print(f"local catalog.db rows updated: {n:,}")
    if "--d1" in sys.argv:
        stmts = d1_sql(changes)
        print(f"applying {len(stmts):,} D1 series UPDATEs ({CHUNK} per call) ...")
        applied = run_d1(stmts)
        print(f"D1 series statements applied: {applied:,}")
        if applied == len(stmts):
            # Only after `series` is fully correct — the rebuild reads FROM series,
            # so running it against a half-applied table would index stale titles.
            print("rebuilding series_fts entries for sec_edgar (delete + "
                  "insert-select, 2 statements) ...")
            n = run_d1(FTS_REBUILD)
            print(f"series_fts rebuild statements applied: {n}")
        else:
            print("SKIPPING series_fts rebuild — `series` is only partially applied, "
                  "and the rebuild copies FROM series, so it would index stale text.")
    else:
        print("D1 NOT written (pass --d1). The worker reads D1, so a local-only "
              "change is invisible to users.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
