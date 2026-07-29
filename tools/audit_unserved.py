"""Which sources hold DATA that reaches nobody — and why, per source.

WHY THIS EXISTS. Twice in one day a source turned out to be sitting on disk, fully
paid for, serving nobody: unesco_natmon (1,876,322 obs) and unesco_sdg (734,662) had
no catalog rows, no R2 objects, a denylist entry and no registry unit — so nothing
could even report them as missing. A source the orchestrator never iterates is not
failing; it is INVISIBLE, and invisible is the one state no per-run check can find.
Two SEC companies reached the same state hours later through a different mechanism.

The only thing that finds this class is auditing the POPULATION rather than a run.

WHAT IT WILL NOT DO is hand you a headline number. The raw count is ~7.2 billion
observations across 54 sources, and quoting that would be worse than saying nothing,
because the overwhelming majority is correctly unserved:

  GATED           the worker's denylist covers it — deliberately not redistributable
  NO SOURCE ROW   no licence has been assessed at all; hosting is not yet permitted
  NEEDS-REVIEW    licence explicitly unverified (reservable=0) — the conservative default
  IN-FLIGHT       a first-pass crawl is still writing it; uncatalogued is expected
  RELATIONAL      wide tables (13F filings, business registers, LEI records) that are
                  not series_key/obs_date/value and cannot simply be catalogued

Only what survives ALL of those is a real finding: series-shaped, licence-cleared,
idle, and catalogued nowhere. Each bucket is printed so the number carries its own
caveats instead of being quoted without them.

Usage:  python tools/audit_unserved.py [--idle-hours 6]
"""
from __future__ import annotations

import argparse
import collections
import datetime as dt
import glob
import io
import os
import re
import sqlite3
import sys

import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, "data", "catalog.db")
DENY = os.path.join(ROOT, "api", "worker", "src", "denylist.ts")


def load_context():
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    catalogued = {r[0] for r in con.execute("SELECT DISTINCT source_id FROM series")}
    srcrow = {r[0]: r[1] for r in con.execute("SELECT source_id, license_id FROM source")}
    lic = {r[0]: (r[1], r[2]) for r in con.execute(
        "SELECT license_id, reservable, name FROM license")}
    deny = set()
    if os.path.exists(DENY):
        deny = set(re.findall(r'"([a-z0-9_]+)"', io.open(DENY, encoding="utf-8").read()))
    return catalogued, srcrow, lic, deny


def scan(idle_hours):
    catalogued, srcrow, lic, deny = load_context()
    now = dt.datetime.now()
    buckets = collections.defaultdict(list)
    for base in ("data/clean_full", "data/clean_grouped"):
        d0 = os.path.join(ROOT, base)
        if not os.path.isdir(d0):
            continue
        for name in sorted(os.listdir(d0)):
            p = os.path.join(d0, name)
            if not os.path.isdir(p) or name in catalogued:
                continue
            files = glob.glob(p + "/**/*.parquet", recursive=True)
            if not files:
                continue
            try:
                obs = sum(pq.read_metadata(f).num_rows for f in files)
            except Exception:                                 # noqa: BLE001
                buckets["UNREADABLE"].append((name, 0, 0.0))
                continue
            if obs <= 0:
                continue
            age_h = (now - dt.datetime.fromtimestamp(
                max(os.path.getmtime(f) for f in files))).total_seconds() / 3600.0
            rec = (name, obs, age_h)

            if name in deny:
                buckets["GATED (worker denylist)"].append(rec)
                continue
            lid = srcrow.get(name)
            if lid is None:
                buckets["NO SOURCE ROW (licence never assessed)"].append(rec)
                continue
            reservable = (lic.get(lid) or (None, None))[0]
            if reservable == 0:
                buckets["licence reservable=0 (NEEDS-REVIEW / restricted)"].append(rec)
                continue
            if age_h < idle_hours:
                buckets["IN-FLIGHT (still being written)"].append(rec)
                continue
            cols = pq.read_schema(files[0]).names
            shaped = (("series_key" in cols or "series_id" in cols)
                      and "obs_date" in cols and "value" in cols)
            buckets["SERVABLE NOW (series-shaped, cleared, idle)" if shaped
                    else "RELATIONAL (needs a transform, not just cataloguing)"].append(rec)
    return buckets


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--idle-hours", type=float, default=6.0,
                    help="written more recently than this = a live crawl, not a gap")
    a = ap.parse_args()

    buckets = scan(a.idle_hours)
    order = ["SERVABLE NOW (series-shaped, cleared, idle)",
             "RELATIONAL (needs a transform, not just cataloguing)",
             "IN-FLIGHT (still being written)",
             "NO SOURCE ROW (licence never assessed)",
             "licence reservable=0 (NEEDS-REVIEW / restricted)",
             "GATED (worker denylist)", "UNREADABLE"]
    total = sum(o for v in buckets.values() for _, o, _ in v)
    print(f"sources holding data with NO catalog rows: "
          f"{sum(len(v) for v in buckets.values())}   "
          f"({total:,} observations in total)")
    print("The total is NOT a finding — most of it is correctly unserved. By reason:")
    print()
    for k in order:
        v = buckets.get(k)
        if not v:
            continue
        print(f"{k}: {len(v)} source(s), {sum(o for _, o, _ in v):,} obs")
        for name, obs, age in sorted(v, key=lambda x: -x[1])[:10]:
            when = f"{age:.1f}h" if age < 48 else f"{age / 24:.0f}d"
            print(f"    {name:<20} {obs:>16,}   last write {when} ago")
        if len(v) > 10:
            print(f"    ... +{len(v) - 10} more")
        print()
    key = buckets.get("SERVABLE NOW (series-shaped, cleared, idle)") or []
    print("=" * 72)
    print(f"ACTIONABLE: {len(key)} source(s), {sum(o for _, o, _ in key):,} observations "
          f"are series-shaped, licence-cleared, idle, and reach nobody.")
    print("Each still needs its own check before hosting — a licence row saying "
          "reservable=1 is a claim, and an idle crawl may simply be unfinished.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
