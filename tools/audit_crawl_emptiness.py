"""Catch the "fetching for weeks, writing nothing" failure BEFORE it costs weeks.

THE INCIDENT THIS EXISTS FOR (ledger M-20260727 family, cbs_nl, twice):
  * `parse_cbs_period` returned None for period formats CBS actually publishes (`SJ`,
    `X0`, plain `YYYYMMDD`), and the row loop `continue`d past the WHOLE row. Table
    71493ned fetched 144,000,000 rows over 60 hours and wrote ZERO observations. 23
    tables were in that state; the fingerprint was 40 checkpoints reading `written=0`.
  * CBS names its time dimension after what it measures (`JaarVanImmigratie`), the
    exact-match detector missed it, and ~59,000,000 more fetched rows were discarded.

Both looked perfectly healthy from outside: process alive, log scrolling, CPU ticking,
row counters climbing. The ONLY early signal was inside the checkpoints — rows fetched
with nothing written — and nobody was reading it. This tool reads it, mechanically.

WHAT IT CHECKS (per long-running crawl, from the crawler's OWN checkpoint/state files):
  cbs_nl    data/clean_full/cbs_nl/<table>.ckpt.json  {skip, parts, written, pidx}
            skip  = rows fetched so far (OData offset)      written = obs written
            DEFECT: skip > 0 and written == 0  -> fetching into the void.
  gus_dbw   parquet parts + their row counts (its parts ARE its progress record)
  statcan   parquet files + row counts

Exit 1 if ANY table shows the fetch-without-write signature, so a guard/CI step can
fail on it instead of a human noticing in week three.

  python tools/audit_crawl_emptiness.py            # human-readable table
  python tools/audit_crawl_emptiness.py --json     # machine-readable
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data", "clean_full")
if not os.path.isdir(DATA):                # fail loudly rather than audit nothing (R330)
    raise SystemExit(f"store not found at {DATA} — refusing to report an empty audit")


def cbs_nl_report() -> dict:
    """cbs_nl keeps a per-table checkpoint; that file IS the fetched-vs-written record."""
    cks = sorted(glob.glob(os.path.join(DATA, "cbs_nl", "*.ckpt.json")))
    writing, empty, idle, unreadable = [], [], [], []
    for p in cks:
        name = os.path.basename(p)[: -len(".ckpt.json")]
        try:
            d = json.load(open(p, encoding="utf-8"))
        except Exception as e:             # a torn checkpoint is a finding, not a skip
            unreadable.append((name, type(e).__name__))
            continue
        skip, written = int(d.get("skip", 0) or 0), int(d.get("written", 0) or 0)
        if written > 0:
            writing.append((name, skip, written))
        elif skip > 0:
            empty.append((name, skip, written))    # THE incident signature
        else:
            idle.append((name, skip, written))
    return {"source": "cbs_nl", "unit": "table", "writing": writing, "empty": empty,
            "idle": idle, "unreadable": unreadable}


def parquet_report(source: str) -> dict:
    """For crawls whose progress record is the parquet set itself: a file with zero rows
    is the same disease one level down (fetched, parsed to nothing, written as empty)."""
    import pyarrow.parquet as pq
    files = sorted(glob.glob(os.path.join(DATA, source, "**", "*.parquet"), recursive=True))
    writing, empty, unreadable = [], [], []
    for f in files:
        base = os.path.basename(f)
        if base.startswith("_"):
            continue
        try:
            n = pq.ParquetFile(f).metadata.num_rows
        except Exception as e:
            unreadable.append((base, type(e).__name__))
            continue
        (writing if n > 0 else empty).append((base, None, n))
    return {"source": source, "unit": "parquet", "writing": writing, "empty": empty,
            "idle": [], "unreadable": unreadable}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()

    reports = [cbs_nl_report(), parquet_report("gus_dbw"), parquet_report("statcan")]
    if a.json:
        print(json.dumps(reports, indent=1))
    else:
        print("=== crawl emptiness audit — 'fetching but writing nothing' ===")
        for r in reports:
            print(f"\n{r['source']} ({r['unit']}s)")
            print(f"  writing        : {len(r['writing']):>6,}")
            print(f"  EMPTY/DEFECT   : {len(r['empty']):>6,}"
                  + ("   <-- fetch-without-write" if r["empty"] else ""))
            if r["idle"]:
                print(f"  not started    : {len(r['idle']):>6,}")
            if r["unreadable"]:
                print(f"  unreadable     : {len(r['unreadable']):>6,}  {r['unreadable'][:3]}")
            for name, skip, written in sorted(r["empty"], key=lambda x: -(x[1] or 0))[:10]:
                fetched = f"{skip:,}" if skip else "?"
                print(f"    DEFECT {name:20s} fetched={fetched:>14s} written={written}")
            for name, skip, written in sorted(r["writing"], key=lambda x: -x[2])[:3]:
                print(f"    ok     {name:20s} written={written:,}")

    total_empty = sum(len(r["empty"]) for r in reports)
    total_unread = sum(len(r["unreadable"]) for r in reports)
    # In --json mode stdout carries ONLY the JSON document, because a consumer parses it
    # (the guard heartbeat does). Mixing the human summary in made json.loads fail with
    # "Extra data" — which the heartbeat correctly surfaced as "audit did not run",
    # never as a false pass, but the audit still has to be parseable to be useful.
    out = sys.stderr if a.json else sys.stdout
    print(f"\nTOTAL fetch-without-write units: {total_empty}"
          f" | unreadable checkpoints: {total_unread}", file=out)
    if total_empty:
        print("FAIL: at least one unit is fetching and writing nothing — the cbs_nl "
              "incident class. Read that unit's parser BEFORE letting the crawl continue.",
              file=out)
        return 1
    print("PASS: every unit that has fetched anything has also written rows.", file=out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
