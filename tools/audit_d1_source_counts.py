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
import datetime as dt
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
    # RuntimeError, NOT SystemExit. SystemExit derives from BaseException, so main()'s
    # `except Exception` would not catch it and the process would exit 1 -- which this file
    # defines as "at least one cached count disagrees". A missing binary would then report as a
    # FINDING instead of as "could not look", inverting the very trichotomy the docstring
    # promises. That path is live in CI: wrangler is installed by the `npm ci` inside the
    # "Sync freshness to D1" step, which is skipped whenever the state push fails.
    raise RuntimeError("no wrangler binary found; cannot reach D1")


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


def local_counts(sources) -> dict:
    """Count each source in the local catalogue, using the PRIMARY KEY range.

    The obvious form -- `SELECT source_id, COUNT(*) FROM series GROUP BY source_id` -- is a full
    scan of an ~11.9 GB table with no index on source_id. It took over 900 s and was killed by a
    timeout with an empty log, which is why this function announces itself before doing work
    (R706: never put a slow call at the start of a job without a print in front of it).

    Counting `series_id >= 'src:' AND series_id < 'src;'` instead rides sqlite's implicit index on
    the primary key, so ~322 bounded counts finish in seconds where the single GROUP BY did not
    finish at all. It is the same trick the worker uses (sql.ts:216-231) and the same predicate
    `--refresh-counts` writes with, so the three agree by construction.

    The source set is the UNION of the cached sources and the local `source` table, not the
    cached sources alone. Both directions are findings and each is invisible from the other side:

      * cached, no local rows  -> the cache over-reports (noaa's +42 was this shape);
      * local rows, NOT cached -> the worker falls through to a live COUNT(*) on EVERY page view.
        That second class is the expensive one and it is on record: ECONLIB_COMPLETION_PLAN.md:77
        has `vdem` with no cache row costing a live COUNT(*) of 783,100 rows per page view -- the
        rebuilt $82/day shape. Taking sources from the cache alone made that class structurally
        unreportable, which would have been a coverage REGRESSION against the reconciliation this
        check replaces (whose superseded WORKLOG line reads "drifted 0, uncached 0").
    """
    if not os.path.exists(LOCAL):
        raise RuntimeError(f"local catalogue not found at {LOCAL}")
    gb = os.path.getsize(LOCAL) / 1e9
    age_d = (dt.datetime.now() - dt.datetime.fromtimestamp(os.path.getmtime(LOCAL))).days
    con = sqlite3.connect(f"file:{LOCAL.replace(os.sep, '/')}?mode=ro", uri=True, timeout=300.0)
    con.execute("PRAGMA busy_timeout = 300000")   # crawlers hold this file continuously
    try:
        # STALENESS IS THE FAILURE MODE OF THIS WHOLE MODE, so say the age out loud. In CI the
        # local catalogue is a snapshot pulled from R2, and that object is refreshed only by a
        # hand-run tools/refresh_r2_catalog.py. A snapshot older than the thing it is auditing
        # reports AGREEMENT for any source catalogued since -- so a source at cache 0 and local
        # 0 looks fine while D1 actually holds rows. Use --remote-truth when this warns.
        if age_d >= 2:
            print(f"  *** WARNING: local catalogue is {age_d} day(s) old. Any source catalogued"
                  f"\n      since then will compare 0-vs-0 and report AGREEMENT. Prefer"
                  f"\n      --remote-truth when this file is not current.", flush=True)
        local_srcs = {r[0] for r in con.execute("SELECT source_id FROM source")
                      if isinstance(r[0], str)}
        sources = set(sources) | local_srcs
        print(f"  counting {len(sources):,} source(s) in the local catalogue "
              f"({gb:.2f} GB, {age_d}d old) by PK range ...", flush=True)
        out = {}
        for src in sorted(sources):
            out[src] = con.execute(
                "SELECT COUNT(*) FROM series WHERE series_id >= ? AND series_id < ?",
                (src + ":", src + ";")).fetchone()[0]
        print(f"  local counts done: {len(out):,} sources", flush=True)
        return out
    finally:
        con.close()


def diff_counts(cache: dict, truth: dict, homes: dict, remote: bool) -> list:
    """Which cached counts disagree with the truth? Returns [(db, source, cached, true), ...].

    Extracted from main() so it can be tested without D1 or an 11.9 GB sqlite file. Two rules
    here were wrong when this shipped, and both were found by RUNNING it rather than reading it:

    ABSENT AND ZERO ARE THE SAME THING. A registered source with no series rows and no cache row
    has simply not been ingested; comparing `cv != tv` made `None != 0` a mismatch and produced 27
    false positives (central_banks, cftc, gii, pxweb ...) that buried the four real findings on
    the first run. `(cv or 0)` collapses them while KEEPING the case that matters — an absent
    cache row over a source that does have rows is the vdem shape, a live COUNT(*) per page view.

    REMOTE AND LOCAL MODES KEY DIFFERENTLY. Remote truth is per (database, source), so a source
    present in both databases must not collapse — that is the state a botched shard migration
    leaves, and source_id alone would let the second database silently overwrite the first.
    Local truth is per source, so the cache is summed across databases; that is sound only
    because a source should have ONE home, which `homes` reports when it fails.
    """
    bad = []
    if remote:
        for key in sorted(set(cache) | set(truth)):
            cv, tv = cache.get(key), truth.get(key)
            if (cv or 0) != (tv or 0):
                bad.append((key[0], key[1], cv, tv))
        return bad
    cache_by_src: dict = {}
    for (_db, src), n in cache.items():
        cache_by_src[src] = cache_by_src.get(src, 0) + n
    for src in sorted(set(cache_by_src) | set(truth)):
        cv, tv = cache_by_src.get(src), truth.get(src)
        if (cv or 0) != (tv or 0):
            bad.append(("/".join(homes.get(src, [])) or None, src, cv, tv))
    return bad


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
            truth = local_counts({src for (_db, src) in cache})
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

    bad = diff_counts(cache, truth, homes, a.remote_truth)

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

    # WHAT A MISMATCH MEANS DEPENDS ON THE MODE, and conflating the two would misdiagnose every
    # finding. --remote-truth compares the cache against D1's OWN series, so a mismatch there is
    # cache drift and nothing else. The default compares against the LOCAL catalogue, where a
    # mismatch has two possible causes: the cache drifted, OR D1 legitimately holds rows local
    # does not. That second case is real and routine -- measured 2026-09-04, sec_edgar +161,
    # fhfa +61 and fed_board +21 all reconcile to the known 243-row D1-ahead-of-local residual,
    # and their caches match D1 exactly. Reporting those as "wrong numbers served to users" would
    # send the next reader chasing three non-problems.
    if a.remote_truth:
        print(f"\n  {len(bad)} MISMATCH(ES) -- the cache disagrees with D1's own `series`, so "
              f"these numbers ARE wrong for users:\n")
    else:
        print(f"\n  {len(bad)} MISMATCH(ES) vs the LOCAL catalogue. This mode cannot tell cache "
              f"drift from\n  D1 legitimately being ahead of local -- re-run with --remote-truth "
              f"to separate them:\n")
    print(f"  {'source':<26}{'cached':>14}{'true':>14}{'delta':>10}   database")
    for db, src, cv, tv in sorted(bad, key=lambda x: -abs((x[2] or 0) - (x[3] or 0))):
        print(f"  {src:<26}{('-' if cv is None else format(cv, ',')):>14}"
              f"{('-' if tv is None else format(tv, ',')):>14}"
              f"{(cv or 0) - (tv or 0):>+10,}   {db or '?'}")

    if a.fix:
        # Prints the SUPPORTED repair command, not raw SQL. The earlier version emitted a
        # hand-run `wrangler d1 execute ... WHERE source_id = '<src>'`, which was wrong three
        # ways: it is the full-scan predicate (~10.3M rows a time) where the range form is a
        # bounded read; it is precisely the by-hand patch that falsifies the documented
        # "source_counts has exactly one writer" invariant (state-baseline.md:25,
        # protocols.md:38); and it guessed the database, so for an UNCACHED source on the
        # climate shard it would have named the primary and silently written n=0 there -- the
        # failure the noaa repair used a negative control to rule out.
        #
        # --refresh-counts routes through the single writer and resolves the shard itself via
        # CATALOG_SHARD_FOR, so none of the three applies.
        srcs = sorted({src for _db, src, _cv, _tv in bad})
        print("\n  repair (routes through the one writer; it resolves the shard itself):\n")
        print("      python core/sync_catalog_d1.py --refresh-counts " + ",".join(srcs))
        for db, src, cv, tv in bad:
            if tv in (None, 0) and cv:
                print(f"\n  -- {src}: cached {cv:,} but no rows found. If a push is in flight,"
                      f" let it finish;\n     refreshing now would publish a partial count.")
            elif cv is None and tv:
                # NO ROW AT ALL. catalog.ts:216-218 falls through to a live COUNT(*) for this
                # source on every page view -- the expensive class (vdem, 783,100 rows/view).
                print(f"\n  -- {src}: {tv:,} rows and NO cache row -- the worker runs a LIVE"
                      f" COUNT(*) for this\n     source on every page view until it is set"
                      f" (ECONLIB_COMPLETION_PLAN.md:77, the vdem shape).")
            elif cv == 0 and tv:
                # A ROW EXISTING WITH VALUE 0 IS A DIFFERENT BUG, and saying "falls back to a
                # live count" here would be wrong: the row exists, so the cached branch is taken
                # and 0 is served verbatim beside a non-empty page. Cheap, and incoherent.
                print(f"\n  -- {src}: {tv:,} rows but the cache row says 0 -- served as"
                      f" `total: 0` next to a\n     non-empty page. Usual cause: an interrupted"
                      f" push (source_counts is written only at the END of a sync, R709).")
        if not a.remote_truth:
            print("\n  NOTE: this ran against the LOCAL catalogue, which in CI is an R2 snapshot")
            print("  refreshed only by hand. Re-run with --remote-truth before applying anything.")

    return 1


if __name__ == "__main__":
    sys.exit(main())
