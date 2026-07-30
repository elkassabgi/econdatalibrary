"""Catalog vs R2 parity for one source's derived per-series CSVs — BOTH directions.

A derive is only done when the two sets agree, and a one-directional check cannot say that:

  MISSING   catalogued, no CSV in R2  -> the site offers a download that 404s
  ORPHANED  CSV in R2, not catalogued -> bytes nobody can reach, and a sign the key
                                         encoding or the catalog id set has drifted

Counting alone is not enough either: |catalog| == |R2| is satisfied by any set with the
same size, so this compares the actual id SETS. And it lists the full prefix rather than
sampling — a 5-key sample once nearly certified a derive that was 13% complete with
991,707 objects missing (R167).

Keys must be formed EXACTLY as tools/derive_csv_bulk.py forms them
(`<prefix>/<urlquoted "source:series_key">.csv`); deriving that independently here would
let an encoding drift pass parity by being wrong in both places, so it is imported.

Usage:
  python tools/verify_derive_parity.py --source cepii_gravity [--bucket econ-data]
Exit 1 if either direction is non-empty.
"""
from __future__ import annotations
import argparse
import os
import sqlite3
import sys
import urllib.parse

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))

from core import r2_util  # noqa: E402
from tools.derive_csv_bulk import csv_key_prefix  # noqa: E402  THE writer's own layout


def catalog_ids(db: str, source: str) -> set:
    con = sqlite3.connect(db)
    try:
        rows = con.execute(
            "select series_id from series where source_id = ?", (source,)).fetchall()
    except sqlite3.OperationalError:
        rows = con.execute(
            "select id from series where source_id = ?", (source,)).fetchall()
    finally:
        con.close()
    return {r[0] for r in rows if r[0]}


def r2_keys(bucket: str, source: str, prefix: str) -> set:
    """Every derived CSV id under this source's prefix — the FULL listing, paginated."""
    cl = r2_util.client()
    lp = csv_key_prefix(prefix, source)
    out = set()
    token = None
    pages = 0
    while True:
        kw = {"Bucket": bucket, "Prefix": lp, "MaxKeys": 1000}
        if token:
            kw["ContinuationToken"] = token
        resp = cl.list_objects_v2(**kw)
        for o in resp.get("Contents", []) or []:
            k = o["Key"]
            if not k.endswith(".csv"):
                continue
            ident = urllib.parse.unquote(k[len(prefix) + 1:-len(".csv")])
            out.add(ident)
        pages += 1
        if pages % 50 == 0:
            print(f"  ...{len(out):,} keys listed", flush=True)
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True)
    ap.add_argument("--bucket", default="econ-data")
    ap.add_argument("--prefix", default="series")
    ap.add_argument("--catalog", default=os.environ.get("ECONDL_CATALOG")
                    or os.path.join(ROOT, "data", "catalog.db"))
    ap.add_argument("--show", type=int, default=10)
    args = ap.parse_args()

    cat = catalog_ids(args.catalog, args.source)
    print(f"catalog {args.source}: {len(cat):,} series  ({args.catalog})", flush=True)
    if not cat:
        print("catalog has no rows for this source — nothing to verify against")
        return 1

    print(f"listing r2://{args.bucket}/{args.prefix}/{args.source}:* ...", flush=True)
    got = r2_keys(args.bucket, args.source, args.prefix)
    print(f"R2: {len(got):,} derived CSV(s)")

    # The ids in R2 carry the "source:" prefix; catalog ids may or may not. Compare on
    # whichever form the catalog uses, rather than assuming (a wrong assumption here would
    # report a clean parity between two differently-shaped id spaces).
    sample = next(iter(cat))
    if not sample.startswith(args.source + ":"):
        got = {g.split(":", 1)[1] if g.startswith(args.source + ":") else g for g in got}

    missing = cat - got
    orphaned = got - cat
    print(f"\nMISSING  (catalogued, no CSV): {len(missing):,}")
    for m in sorted(missing)[:args.show]:
        print(f"    {m}")
    print(f"ORPHANED (CSV, not catalogued): {len(orphaned):,}")
    for o in sorted(orphaned)[:args.show]:
        print(f"    {o}")

    ok = not missing and not orphaned
    print(f"\nPARITY: {'OK — both directions empty' if ok else 'FAILED'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
