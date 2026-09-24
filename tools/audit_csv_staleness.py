"""Which served CSVs were written BEFORE the parquet they are derived from?

WHY THIS EXISTS. Until 2026-08-07 the orchestrator re-derived a series' CSV only on a run
whose status was exactly `ok` (ledger R380). Sources that are chronically `partial` — one
flaky sub-unit out of eighty, every run — never return ok, so their parquet advanced in R2
while the CSV a user downloads stayed frozen. worldbank_esg served 2023 values, including
superseded revisions, from series the system recorded as fresh. The gate is fixed, but the
fix only repairs FUTURE runs: objects already stale stay stale until something re-derives
them, and this names which sources those are.

THE SIGNAL, and its limits. Comparing R2 LastModified of each `series/<src>%3A...csv` against
the newest parquet under `clean_full/<src>/` is pure listing metadata — no downloads, no
parquet parsing, so it runs over sources holding millions of series. What it proves is
ONE-DIRECTIONAL and that asymmetry is the whole point:

  CSV newer than every parquet  -> PROVABLY NOT STALE. Nothing has written the store since.
  CSV older than a parquet      -> CANDIDATE ONLY. The parquet rewrite may not have touched
                                   that particular series (a merge rewrites the whole file
                                   even when one row changed), so this is an upper bound on
                                   staleness, never a count of it.

So a clean verdict here is trustworthy and a dirty one is NOT EVEN A WORK LIST. Measured the
day this was written, against sources whose true state was already known by byte-compare:

    statfin        flagged 1,539 of 1,539    byte-compare 25/25 identical  -> 0% truly stale
    scb            flagged 2,550 of 2,550    byte-compare 25/25 identical  -> 0% truly stale
    worldbank_esg  clean (just re-derived)   byte-compare 60/60 identical  -> correct

That is ~zero precision on the dirty side, and the reason is structural, not a tuning problem:
a merge rewrites the WHOLE parquet even when one row changed, so for any actively-updated
source the newest parquet is newer than every CSV derived before it — the predicate fires on
all of them. Do NOT read the candidate count as a backlog size; it is closer to "every source
that has ingested anything since its last derive".

What this tool is actually good for is the ONE-DIRECTIONAL half: a source in the PROVABLY NOT
STALE list needs no further checking, which is worth having cheaply over millions of objects.
For everything else the only answer is `tools/verify_source_served.py --source <sid> --sample N`,
whose byte-compare is what caught the original defect and what corrected this tool's own
over-reporting.

NO SILENT CAPS: `--max-objects` bounds the listing per source, and any source whose listing
was truncated is reported as PARTIAL SCAN with the number seen. A bounded scan that printed
"clean" would be the same class of lie this tool exists to expose.

    python tools/audit_csv_staleness.py                      # every live+served source
    python tools/audit_csv_staleness.py --source worldbank_esg
    python tools/audit_csv_staleness.py --never-ok-only      # the R380 risk set
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import urllib.parse

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import r2_util  # noqa: E402

BUCKET = "econ-data"


def _never_ok_sources() -> set[str]:
    db = os.path.join(ROOT, "data", "_aqueduct", "state.db")
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)  # plain-open: the updater's state.db, read-only
    ok, seen = set(), set()
    for src, status in con.execute("SELECT source_id, status FROM runs"):
        seen.add(src)
        if status == "ok":
            ok.add(src)
    return seen - ok


def _served_sources() -> dict[str, int]:
    from core import catalog_path                                     # noqa: PLC0415 - plan step 1
    con = catalog_path.connect()
    return {r[0]: r[1] for r in con.execute(
        "SELECT source_id, count(*) FROM series GROUP BY source_id")}


def _newest_parquet(s3, src: str):
    """LastModified of the most recently written parquet in the source's store dir."""
    newest = None
    p = s3.get_paginator("list_objects_v2")
    for page in p.paginate(Bucket=BUCKET, Prefix=f"clean_full/{src}/"):
        for o in page.get("Contents", []):
            if not o["Key"].endswith(".parquet"):
                continue
            if newest is None or o["LastModified"] > newest:
                newest = o["LastModified"]
    return newest


def _csv_ages(s3, src: str, cutoff, max_objects: int):
    """(older_than_cutoff, total_seen, truncated). Anchored on the encoded colon so
    `imf_fsi` does not also match `imf_fsire` (R129)."""
    prefix = "series/" + urllib.parse.quote(src + ":", safe="")
    older = total = 0
    p = s3.get_paginator("list_objects_v2")
    for page in p.paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            total += 1
            if o["LastModified"] < cutoff:
                older += 1
            if total >= max_objects:
                return older, total, True
    return older, total, False


def _newest_parquet_local(src: str):
    """AFTER T0: the newest parquet write time in the live store (a UTC datetime), recursive like the R2 prefix."""
    import datetime as dt                                              # noqa: PLC0415
    import glob                                                        # noqa: PLC0415
    files = [f for f in glob.glob(os.path.join(ROOT, "data", "clean_full", src, "**", "*.parquet"), recursive=True)]
    if not files:
        return None
    return max(dt.datetime.fromtimestamp(os.path.getmtime(f), tz=dt.timezone.utc) for f in files)


def _csv_ages_selfhost(store, src: str, cutoff, max_objects: int):
    """AFTER T0: _csv_ages over the self-hosted store's stored_utc (whole seconds - a CSV stored in the parquet's
    own second counts as older, the cautious side, as make_servable rounds)."""
    prefix = "series/" + urllib.parse.quote(src + ":", safe="")
    older = total = 0
    for _k, stored in store.list_modified(prefix):
        total += 1
        if stored < cutoff:
            older += 1
        if total >= max_objects:
            return older, total, True
    return older, total, False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", action="append")
    ap.add_argument("--never-ok-only", action="store_true",
                    help="restrict to sources that have never returned ok (the R380 set)")
    ap.add_argument("--max-objects", type=int, default=200_000)
    a = ap.parse_args()

    from core import cutover                                          # noqa: PLC0415
    if cutover.is_cut_over():
        # AFTER T0 the catalogue is the LIVE build: refuse outside the live checkout BEFORE reading it, and count it
        # in short primary-key chunks - one GROUP BY holds the build's read lock and the writer waits (R1249)
        from updater import blob                                      # noqa: PLC0415
        blob.refuse_unless_live_checkout("audit_csv_staleness (after T0 it judges the live store)")
        import collections                                            # noqa: PLC0415
        from core import catalog_path                                 # noqa: PLC0415
        _con = catalog_path.connect()
        try:
            served = dict(collections.Counter(s for (s,) in catalog_path.iter_series(_con, ("source_id",))))
        finally:
            _con.close()
    else:
        served = _served_sources()
    targets = a.source or sorted(served)
    if a.never_ok_only:
        never = _never_ok_sources()
        targets = [t for t in targets if t in never]
    print(f"screening {len(targets)} source(s); cap {a.max_objects:,} objects each\n")

    if cutover.is_cut_over():
        # AFTER T0 (plan step 6d): the parquets are the live local store and the served CSVs the self-hosted
        # store (R2 is a frozen copy) - the live-checkout refusal ran above, before the catalogue was read
        from updater import blob                                      # noqa: PLC0415
        _store = blob.SelfhostBlob()

        def newest_of(src):
            return _newest_parquet_local(src)

        def ages_of(src, cutoff):
            return _csv_ages_selfhost(_store, src, cutoff, a.max_objects)
    else:
        s3 = r2_util.client()

        def newest_of(src):
            return _newest_parquet(s3, src)

        def ages_of(src, cutoff):
            return _csv_ages(s3, src, cutoff, a.max_objects)
    stale, clean, partial_scan, nodata = [], [], [], []
    for src in targets:
        try:
            newest = newest_of(src)
        except Exception as e:                                        # noqa: BLE001
            print(f"  {src:24s} ERROR listing store: {e}")
            continue
        if newest is None:
            nodata.append(src)
            continue
        older, total, trunc = ages_of(src, newest)
        if total == 0:
            nodata.append(src)
            continue
        tag = "PARTIAL SCAN" if trunc else ""
        if older:
            stale.append((src, older, total, trunc))
            print(f"  {src:24s} {older:>8,} of {total:>8,} CSVs predate the newest "
                  f"parquet ({newest:%Y-%m-%d})  {tag}")
        else:
            (partial_scan if trunc else clean).append(src)

    print(f"\nPROVABLY NOT STALE (every CSV newer than every parquet): {len(clean)}")
    print(f"  {sorted(clean)}")
    if partial_scan:
        print(f"\nCLEAN SO FAR BUT SCAN TRUNCATED — verdict withheld: {sorted(partial_scan)}")
    if nodata:
        print(f"\nno store parquets or no CSVs, nothing to compare: {sorted(nodata)}")
    print(f"\nCANDIDATES — NOT a stale count and NOT a work list: {len(stale)}. Measured "
          f"precision on this signal is ~0 (statfin flagged 1,539/1,539 and scb 2,550/2,550 "
          f"while both byte-compared 25/25 identical), because a merge rewrites the whole "
          f"parquet. Only verify_source_served.py --sample N answers it.")
    for src, older, total, trunc in sorted(stale, key=lambda x: -x[1]):
        print(f"  {src:24s} up to {older:,}/{total:,}" + ("  [PARTIAL SCAN]" if trunc else ""))
    return 1 if stale else 0


if __name__ == "__main__":
    sys.exit(main())
