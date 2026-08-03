"""Backfill start_date/end_date for catalogued series that have neither.

Run 2026-08-02: took the catalog from 4,625,655 dateless series (42.6% of 10.9M) down to
1,694 (0.016%). The remaining two sources have no local parquet to read from -- hf_equities
(1,391) and unhcr (303, whose local file holds population:refugees:* while the catalogue
holds population:asylum_seekers:*).

WHY IT MATTERED: the download search renders a coverage column per row, and a series with
neither bound showed an em dash -- for 42.6% of the catalogue. The dates were never missing
from the DATA, only from the catalogue: every affected source already had an obs_date column
in data/clean_full/<source>/, which is exactly what this reads.

After running this, push the affected sources to the serving catalogue or the change stays
invisible to users:  python core/sync_catalog_d1.py --source <name>
Set PYTHONIOENCODING=utf-8 when doing so -- at least one series title contains an emoji
(U+1FAB5) that crashes printing on a cp1252 console mid-run.

42.6% of the catalog (4.6M of 10.9M series) carries no coverage dates, which is why the
download search renders an em dash for most rows. The data to fix it is local: every affected
source's parquet in data/clean_full/<source>/ has an obs_date column keyed by series_key.

Read-only over the parquet; the ONLY write is an UPDATE of start_date/end_date on rows where
both are currently NULL. Never touches a series that already has dates.

Usage:  python backfill_coverage.py <source> [--apply]
Without --apply it reports what it would change and writes nothing.
"""
import glob
import os
import sqlite3
import sys
import time

import pyarrow.parquet as pq

REPO = r"E:\research\econfindatalibrary"
DB = os.path.join(REPO, "data", "catalog.db")


def scan(source):
    """{series_key: (min_iso, max_iso)} over every parquet file for this source."""
    files = sorted(glob.glob(os.path.join(REPO, "data", "clean_full", source, "**", "*.parquet"),
                             recursive=True))
    if not files:
        return None, 0
    # The key column is NOT always "series_key": insee_bdm calls it "idbank", and assuming
    # otherwise made the whole source fail with a schema error rather than a clear message.
    # Detect it per file from the schema, preferring the conventional names.
    def key_col(path):
        names = list(pq.read_schema(path).names)
        for cand in ("series_key", "idbank", "key", "series_id"):
            if cand in names:
                return cand
        rest = [n for n in names if n not in ("obs_date", "value", "dataflow")]
        return rest[0] if rest else None

    agg = {}
    for f in files:
        kc = key_col(f)
        if kc is None:
            continue
        t = pq.read_table(f, columns=[kc, "obs_date"])
        keys = t.column(kc).to_pylist()
        dates = t.column("obs_date").to_pylist()
        for k, d in zip(keys, dates):
            if k is None or d is None:
                continue
            ds = d.isoformat() if hasattr(d, "isoformat") else str(d)
            a = agg.get(k)
            if a is None:
                agg[k] = [ds, ds]
            else:
                if ds < a[0]:
                    a[0] = ds
                if ds > a[1]:
                    a[1] = ds
    return agg, len(files)


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    source = sys.argv[1]
    apply_ = "--apply" in sys.argv

    t0 = time.time()
    agg, nfiles = scan(source)
    if agg is None:
        print("  %s: no parquet files found" % source)
        return 1
    print("  %s: scanned %d file(s), %s distinct series keys in %.1fs"
          % (source, nfiles, format(len(agg), ","), time.time() - t0))

    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT series_id FROM series WHERE source_id=? AND start_date IS NULL AND end_date IS NULL",
        (source,)).fetchall()
    print("  %s: %s catalogued series currently have NO dates" % (source, format(len(rows), ",")))

    prefix = source + ":"
    matched, unmatched = [], 0
    for r in rows:
        sid = r["series_id"]
        key = sid[len(prefix):] if sid.startswith(prefix) else sid
        hit = agg.get(key)
        if hit is None:
            unmatched += 1
            continue
        matched.append((hit[0], hit[1], sid))

    print("  %s: %s can be filled, %s have no matching key in the data"
          % (source, format(len(matched), ","), format(unmatched, ",")))
    if matched:
        lo = min(m[0] for m in matched)
        hi = max(m[1] for m in matched)
        print("  %s: resulting coverage spans %s .. %s" % (source, lo, hi))
        for s, e, sid in matched[:3]:
            print("      e.g. %-52s %s .. %s" % (sid[:52], s, e))

    if not apply_:
        print("  DRY RUN - nothing written. Re-run with --apply to write.")
        return 0

    con.executemany(
        "UPDATE series SET start_date=?, end_date=? "
        "WHERE series_id=? AND start_date IS NULL AND end_date IS NULL", matched)
    con.commit()
    left = con.execute(
        "SELECT COUNT(*) FROM series WHERE source_id=? AND start_date IS NULL AND end_date IS NULL",
        (source,)).fetchone()[0]
    print("  %s: APPLIED. %s rows updated; %s still dateless."
          % (source, format(len(matched), ","), format(left, ",")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
