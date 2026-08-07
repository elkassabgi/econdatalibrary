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

So a clean verdict here is trustworthy and a dirty one is a work list, not a finding. Confirm
candidates with `tools/verify_source_served.py --source <sid> --sample N`, which byte-compares
served bytes against the resolver — that is the authoritative check, and the one that caught
the original defect.

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
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    ok, seen = set(), set()
    for src, status in con.execute("SELECT source_id, status FROM runs"):
        seen.add(src)
        if status == "ok":
            ok.add(src)
    return seen - ok


def _served_sources() -> dict[str, int]:
    cat = os.path.join(ROOT, "data", "catalog.db")
    con = sqlite3.connect(f"file:{cat}?mode=ro", uri=True)
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


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", action="append")
    ap.add_argument("--never-ok-only", action="store_true",
                    help="restrict to sources that have never returned ok (the R380 set)")
    ap.add_argument("--max-objects", type=int, default=200_000)
    a = ap.parse_args()

    served = _served_sources()
    targets = a.source or sorted(served)
    if a.never_ok_only:
        never = _never_ok_sources()
        targets = [t for t in targets if t in never]
    print(f"screening {len(targets)} source(s); cap {a.max_objects:,} objects each\n")

    s3 = r2_util.client()
    stale, clean, partial_scan, nodata = [], [], [], []
    for src in targets:
        try:
            newest = _newest_parquet(s3, src)
        except Exception as e:                                        # noqa: BLE001
            print(f"  {src:24s} ERROR listing store: {e}")
            continue
        if newest is None:
            nodata.append(src)
            continue
        older, total, trunc = _csv_ages(s3, src, newest, a.max_objects)
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
    print(f"\nCANDIDATES (upper bound, NOT a stale count — confirm with "
          f"verify_source_served.py): {len(stale)}")
    for src, older, total, trunc in sorted(stale, key=lambda x: -x[1]):
        print(f"  {src:24s} up to {older:,}/{total:,}" + ("  [PARTIAL SCAN]" if trunc else ""))
    return 1 if stale else 0


if __name__ == "__main__":
    sys.exit(main())
