# -*- coding: utf-8 -*-
"""Is the number every visitor sees on a source-browse page actually true?

WHY THIS EXISTS. `/v1/catalog?source=X` does not count anything. It reads one cached row:

    api/worker/src/sql.ts:246
      BROWSE_SOURCE_COUNT_CACHED = `SELECT n FROM source_counts WHERE source_id = ?`

and `api/worker/src/catalog.ts:200-217` takes that value verbatim for every source WITHOUT a
series-level carve-out — no validation, no bound, no fallback once the row exists. The cache was
the right fix for the 2026-08-15 incident (a live COUNT(*) per page view read 42.2B rows in a
day), but it introduced a number that can silently diverge from the data it claims to describe.

`core/sync_catalog_d1.py:274-279` refreshes it only for sources present in the rows being
INSERTED (`for src in sorted({r["source_id"] for r in rows})`). Any path that removes series
rows without a following insert-sync for that source leaves the count high forever.

That is not hypothetical. Measured 2026-09-04 on econ-catalog-climate:

    source_counts.n for noaa   3,138,201   <- served to users, stable across two days
    COUNT(*) WHERE source_id   3,138,159
    COUNT(*) over the PK range 3,138,159   <- what browseSourceSql can actually page

Both real populations agree; the cache alone was 42 high. A fleet sweep the same day found noaa
was the ONLY one of 321 cached sources that disagreed — but nothing would have caught it, and
nothing would catch the next one. Hence this file.

COST. The default mode is free: it reads `source_counts` (one row per source, an indexed lookup
the d1_cost_guard does not count) and compares against the LOCAL catalog.db, per the
decide-locally policy in CLAUDE.md. That answers "has anything drifted?" for $0.

`--remote-truth` answers the authoritative question — cache vs D1's own `series` — with ONE
`GROUP BY source_id` per database. That is a full scan each (~10.3M + ~3.1M rows, ~$0.013 total)
and it WILL count against the scan budget, so it is opt-in. Prefer it before writing a
correction; the local mode is for routine drift detection.

EXIT CODES.  0 = every cached count agrees.  1 = at least one disagrees.  2 = could not look
(the distinction R704 exists to preserve: a check that scanned nothing must never read as a pass).

USAGE
    python tools/audit_d1_source_counts.py                  # free, local truth
    python tools/audit_d1_source_counts.py --remote-truth   # authoritative, 2 scans
    python tools/audit_d1_source_counts.py --fix            # print the repair SQL (writes nothing)
"""
from __future__ import annotations

import argparse
import io
import json
import os
import shutil
import sqlite3
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOCAL = os.path.join(ROOT, "data", "catalog.db")
WORKER_DIR = os.path.join(ROOT, "api", "worker")

# Both catalogue databases. types.ts:17 binds CATALOG_CLIMATE; util.ts:510 SHARDED_SOURCES and
# util.ts:513 dbFor() route a source to one of them. Auditing only the primary understates the
# fleet by the whole climate shard — that is R702, booked 2026-09-04 for exactly this mistake.
DBS = ("econ-catalog", "econ-catalog-climate")


def _wrangler() -> str:
    """Prefer the repo-local binary. `npx wrangler` re-resolves the package and collides with
    any concurrent wrangler in the shared npm cache (observed: EBUSY on miniflare during the
    statcan push)."""
    for c in (os.path.join(WORKER_DIR, "node_modules", ".bin", "wrangler.cmd"),
              os.path.join(WORKER_DIR, "node_modules", ".bin", "wrangler"),
              os.path.join(ROOT, "node_modules", ".bin", "wrangler.cmd"),
              os.path.join(ROOT, "node_modules", ".bin", "wrangler")):
        if os.path.exists(c):
            return c
    w = shutil.which("wrangler")
    if w:
        return w
    raise SystemExit("FATAL: no wrangler binary found; cannot reach D1")


def d1(db: str, sql: str) -> tuple[list, int]:
    res = subprocess.run(
        [_wrangler(), "d1", "execute", db, "--remote", "--json", "--command", sql],
        cwd=WORKER_DIR, capture_output=True, text=True,
        encoding="utf-8", errors="replace", timeout=900)
    out = res.stdout or ""
    i = out.find("[")
    if res.returncode != 0 or i < 0:
        raise RuntimeError(f"{db}: wrangler rc={res.returncode}: {(res.stderr or out)[-300:]}")
    rows, read = [], 0
    for b in json.loads(out[i:]):
        rows.extend(b.get("results") or [])
        read += (b.get("meta") or {}).get("rows_read") or 0
    return rows, read


def local_counts() -> dict:
    """GROUP BY over the local catalogue.

    This is SLOW and says so before it starts: `series` has no index on source_id, so this is a
    full scan of an ~11.9 GB table and takes minutes. The first version of this file printed
    nothing until it finished, so a 900 s timeout killed it with an empty log and no clue which
    step was slow — R706's shape exactly. Announce the expensive call BEFORE making it.
    """
    if not os.path.exists(LOCAL):
        raise RuntimeError(f"local catalogue not found at {LOCAL}")
    gb = os.path.getsize(LOCAL) / 1e9
    print(f"  scanning local catalogue ({gb:.2f} GB, no source_id index -- expect minutes) ...",
          flush=True)
    con = sqlite3.connect(f"file:{LOCAL.replace(os.sep, '/')}?mode=ro", uri=True)
    try:
        out = {s: n for s, n in con.execute(
            "SELECT source_id, COUNT(*) FROM series GROUP BY source_id") if isinstance(s, str)}
        print(f"  local scan done: {len(out):,} sources", flush=True)
        return out
    finally:
        con.close()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--remote-truth", action="store_true",
                    help="compare against D1's own series (2 full scans, ~$0.013) instead of local")
    ap.add_argument("--fix", action="store_true",
                    help="print the repair SQL for each mismatch; writes NOTHING")
    a = ap.parse_args(argv)

    truth_label = "D1 series" if a.remote_truth else "local catalog.db"
    print(f"source_counts audit -- cache vs {truth_label}")

    cache, truth, read = {}, {}, 0
    try:
        for db in DBS:
            print(f"  reading source_counts from {db} ...", flush=True)
            rows, r = d1(db, "SELECT source_id, n FROM source_counts")
            read += r
            for row in rows:
                cache[(db, row["source_id"])] = row["n"]
            print(f"    {len(rows):,} cached row(s)", flush=True)
            if a.remote_truth:
                print(f"  GROUP BY series on {db} (FULL SCAN, spends the budget) ...", flush=True)
                rows, r = d1(db, "SELECT source_id, COUNT(*) AS n FROM series GROUP BY source_id")
                read += r
                for row in rows:
                    # Keyed by (db, source_id), NOT source_id alone. A source that exists in BOTH
                    # databases -- which is precisely the state a botched shard migration leaves --
                    # would otherwise have the second database silently overwrite the first, and
                    # the audit would report agreement for the one case it most needs to catch.
                    truth[(db, row["source_id"])] = row["n"]
                print(f"    {len(rows):,} source(s), rows_read={r:,}", flush=True)
        if not a.remote_truth:
            truth = local_counts()
    except Exception as e:                                    # noqa: BLE001
        # EXIT 2, never 0. A check that could not look is not a check that passed (R704).
        print(f"  COULD NOT LOOK: {e}", file=sys.stderr)
        return 2

    if not cache:
        print("  COULD NOT LOOK: source_counts returned no rows", file=sys.stderr)
        return 2

    # A source must live in exactly one database (util.ts:513 dbFor routes it). Two cache rows for
    # one source is a finding in its own right -- it double-counts in /v1/stats -- and it is the
    # case the earlier source_id-only keying would have hidden, so report it before anything else.
    homes: dict = {}
    for (db, src) in cache:
        homes.setdefault(src, []).append(db)
    dupes = {s: d for s, d in homes.items() if len(d) > 1}

    bad = []
    if a.remote_truth:
        for key in sorted(set(cache) | set(truth)):
            cv, tv = cache.get(key), truth.get(key)
            if cv != tv:
                bad.append((key[0], key[1], cv, tv))
    else:
        # Local truth is per-source, so compare per source. Summing the cache is correct only
        # because a source should have ONE home; `dupes` above reports it when that fails.
        cache_by_src: dict = {}
        for (db, src), n in cache.items():
            cache_by_src[src] = cache_by_src.get(src, 0) + n
        for src in sorted(set(cache_by_src) | set(truth)):
            cv, tv = cache_by_src.get(src), truth.get(src)
            if cv != tv:
                bad.append(("/".join(homes.get(src, [])) or None, src, cv, tv))

    print(f"  cached sources : {len(homes):,}")
    print(f"  truth sources  : {len({k[1] for k in truth} if a.remote_truth else truth):,}")
    print(f"  rows_read      : {read:,}")
    if dupes:
        print(f"\n  *** {len(dupes)} source(s) cached in MORE THAN ONE database "
              f"(double-counted by /v1/stats):")
        for s, d in sorted(dupes.items()):
            print(f"      {s}: {', '.join(d)}")

    if not bad:
        print("\n  PASS -- every cached count equals its true count")
        return 0

    print(f"\n  {len(bad)} MISMATCH(ES) -- these numbers are served to users:\n")
    print(f"  {'source':<26}{'cached':>14}{'true':>14}{'delta':>10}   database")
    for db, src, cv, tv in sorted(bad, key=lambda x: -abs((x[2] or 0) - (x[3] or 0))):
        print(f"  {src:<26}{('-' if cv is None else format(cv, ',')):>14}"
              f"{('-' if tv is None else format(tv, ',')):>14}"
              f"{(cv or 0) - (tv or 0):>+10,}   {db or '?'}")

    if a.fix:
        print("\n  repair SQL (the statement sync_catalog_d1.py:277-279 already uses --"
              "\n  it recomputes inside D1, so it cannot be wrong about the current state):\n")
        for db, src, cv, tv in bad:
            if tv is None and cv is not None:
                print(f"  -- {src}: cached but no series rows; verify the source is not mid-push")
                continue
            print(f"  wrangler d1 execute {db or DBS[0]} --remote --command \\\n"
                  f"    \"INSERT OR REPLACE INTO source_counts(source_id, n) "
                  f"SELECT '{src}', COUNT(*) FROM series WHERE source_id = '{src}';\"")
        if not a.remote_truth:
            print("\n  NOTE: this ran against the LOCAL catalogue. Re-run with --remote-truth")
            print("  before applying anything -- local and D1 can legitimately differ mid-sync.")

    return 1


if __name__ == "__main__":
    sys.exit(main())
