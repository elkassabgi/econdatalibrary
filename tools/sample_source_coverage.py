"""What fraction of a source's catalogued series are actually downloadable? — for the big ones.

WHY THIS EXISTS BESIDE verify_source_served.py. That tool lists every R2 object and diffs it
against every catalogue row, which is exactly right and gives MISSING/ORPHANED exactly. It is
also O(objects), and some sources are too large for that to terminate in a working session:

    penn_world_table      7,163 series   verify_source_served finishes in ~2 min
    fed_board            52,322          fine
    noaa              3,135,873          `series/noaa` passed 400,000 objects on a PARTIAL
                                         listing and the run was abandoned (2026-08-04)

So for the large ones this answers the cheaper question — "roughly what share is present, and is
that share 31% or 97%?" — which is usually the only question actually blocking a decision.

IT IS A SAMPLE, AND IT SAYS SO. Every line reports the DENOMINATOR and a Wilson 95% interval,
because a coverage figure without the number examined is not a measurement (R330). It cannot find
ORPHANS (objects with no catalogue row) — that direction genuinely needs the full listing, and the
output says so rather than letting silence imply zero.

RANDOM, NEVER A PREFIX. Sampling the first N ids sorted by series_id concentrates on one part of
the key space; a derive that died 13% in once passed a 5-key check for exactly that reason (R167).
`ORDER BY RANDOM()` in SQLite over the catalogue is uniform across the whole source.

    python tools/sample_source_coverage.py --source noaa --sample 400
    python tools/sample_source_coverage.py --source noaa --sample 400 --workers 16
"""
from __future__ import annotations
import argparse
import concurrent.futures as cf
import math
import os
import sqlite3
import sys
import urllib.parse

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import r2_util                                      # noqa: E402

DEFAULT_PREFIX = "series"
BUCKET = "econ-data"


def _wilson(k: int, n: int, z: float = 1.96):
    """95% interval for a proportion. Normal approximation breaks exactly where it matters —
    near 0 and near 1 — and those are the two answers we most need to trust."""
    if n == 0:
        return (0.0, 0.0)
    p = k / n
    d = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / d
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (max(0.0, centre - half), min(1.0, centre + half))


def _key_for(series_id: str, prefix: str) -> str:
    # Mirrors core/derive_csv's layout: <prefix>/<url-encoded series_id>.csv
    return f"{prefix}/{urllib.parse.quote(series_id, safe='')}.csv"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True)
    ap.add_argument("--sample", type=int, default=400)
    ap.add_argument("--prefix", default=DEFAULT_PREFIX)
    ap.add_argument("--bucket", default=BUCKET)
    ap.add_argument("--workers", type=int, default=12)
    a = ap.parse_args()

    cat = os.path.join(ROOT, "data", "catalog.db")
    con = sqlite3.connect(f"file:{cat}?mode=ro", uri=True, timeout=300)
    total = con.execute("SELECT COUNT(*) FROM series WHERE source_id=?", (a.source,)).fetchone()[0]
    if total == 0:
        print(f"{a.source}: 0 catalogued series — nothing to sample.")
        return 0
    n = min(a.sample, total)
    ids = [r[0] for r in con.execute(
        "SELECT series_id FROM series WHERE source_id=? ORDER BY RANDOM() LIMIT ?",
        (a.source, n))]
    con.close()

    s3 = r2_util.client()

    def present(sid: str) -> bool:
        try:
            s3.head_object(Bucket=a.bucket, Key=_key_for(sid, a.prefix))
            return True
        except Exception:                                     # noqa: BLE001
            return False

    with cf.ThreadPoolExecutor(max_workers=a.workers) as ex:
        hits = list(ex.map(present, ids))
    k = sum(hits)
    lo, hi = _wilson(k, len(ids))

    print(f"{a.source}")
    print(f"  catalogued        : {total:,}")
    print(f"  SAMPLED           : {len(ids):,}  (random over the whole key space, not a prefix)")
    print(f"  present in R2     : {k:,}")
    print(f"  coverage          : {k/len(ids):.1%}   95% CI [{lo:.1%}, {hi:.1%}]")
    print(f"  implies missing   : ~{round(total*(1-hi)):,} to ~{round(total*(1-lo)):,} series")
    print()
    print("  This is a SAMPLE, and it cannot see ORPHANS (objects with no catalogue row) —")
    print("  that direction needs the full listing in tools/verify_source_served.py.")
    if k < len(ids):
        miss = [s for s, h in zip(ids, hits) if not h][:5]
        print(f"  examples missing  : {miss}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
