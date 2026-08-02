"""Refresh the updater's coherence reference: upload the curated local catalog.db to R2.

The CI pulls `_aqueduct/catalog.db.zst` read-only and the derive/coherence step maps each
changed store series_key to a catalog series_id. When that R2 copy lags the local catalogue,
the map fails and the source merges its rows and then demotes to "csv coherence unmet" —
forever, because a `partial` never sets last_success_utc and so can never trip RED-SLA either.

Measured 2026-08-02: the R2 copy held 4,605,291 series against 10,853,209 local — 57.6% of the
catalogue missing, including noaa (10 rows on R2 vs 3,135,873 local) and cepii_gravity (0 vs
1,143,250). 28 sources reported "no catalog mapping" every run because of it.

STREAMING, not slurping. This script used to `f.read()` the whole database, compress it in
memory, then decompress the re-download in memory as well — written when catalog.db was ~0.5 GB
and its comments still said so. At 8.5 GB that is ~17 GB of peak RSS to move one file, i.e. the
tool would die on the very database whose growth made running it urgent. Everything below is
chunked through disk instead, so cost is flat in the catalogue's size.

SUPERSET GUARD. Losing a source here is silent and total: the next run simply cannot map it.
The old header recorded a superset check done BY HAND, once, for that day's upload — which is
exactly the kind of check that is not repeated the next time. It is now enforced in code, per
source, and the upload ABORTS on any shrink unless --allow-shrink names it deliberate.

Usage:
    python tools/refresh_r2_catalog.py <stamp> [--against <decompressed-r2-catalog.db>]
                                               [--allow-shrink src1,src2] [--dry-run]

Reversible: the current R2 object is server-side copied to a dated .bak key before any write.
"""
import argparse, os, sqlite3, sys, tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core import r2_util
import zstandard

BUCKET = "econ-data"
KEY = "_aqueduct/catalog.db.zst"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOCAL = os.path.join(ROOT, "data", "catalog.db")


def source_counts(db_path: str) -> dict:
    con = sqlite3.connect(f"file:{db_path.replace(os.sep, '/')}?mode=ro", uri=True)
    try:
        return dict(con.execute("SELECT source_id, COUNT(*) FROM series GROUP BY source_id"))
    finally:
        con.close()


def stream_decompress(client, key: str, dest: str) -> None:
    """R2 object -> decompressed file on disk, without ever holding it in memory."""
    body = client.get_object(Bucket=BUCKET, Key=key)["Body"]
    with open(dest, "wb") as out:
        zstandard.ZstdDecompressor().copy_stream(body, out)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("stamp", nargs="?", default="manual",
                    help="date stamp for the .bak key (no Date.now in scripts)")
    ap.add_argument("--against", default=None,
                    help="already-decompressed copy of the CURRENT R2 catalog, to skip re-downloading it")
    ap.add_argument("--allow-shrink", default="",
                    help="comma-separated source_ids that are DELIBERATELY dropped/reduced")
    ap.add_argument("--dry-run", action="store_true", help="run every check, write nothing")
    a = ap.parse_args()
    allow = {s.strip() for s in a.allow_shrink.split(",") if s.strip()}

    if not os.path.exists(LOCAL):
        print(f"ERROR: {LOCAL} not found", file=sys.stderr)
        return 1
    # A torn sqlite file uploads just as happily as a good one and fails only later, on the
    # runner, as an unexplained mapping miss. Check it here where the fix is free.
    con = sqlite3.connect(f"file:{LOCAL.replace(os.sep, '/')}?mode=ro", uri=True)
    qc = con.execute("PRAGMA quick_check(1)").fetchone()[0]
    con.close()
    if qc != "ok":
        print(f"ERROR: local catalog.db failed quick_check: {qc}", file=sys.stderr)
        return 1
    print(f"  local catalog.db quick_check: ok  ({os.path.getsize(LOCAL):,} B)")

    c = r2_util.client(write=True)
    tmpdir = tempfile.mkdtemp(prefix="refresh_cat_")

    # ---- superset guard -------------------------------------------------------------
    cur_db = a.against
    if cur_db and os.path.exists(cur_db):
        print(f"  comparing against supplied copy of current R2 catalog: {cur_db}")
    else:
        cur_db = os.path.join(tmpdir, "current_r2_catalog.db")
        print(f"  downloading current R2 catalog for the superset check -> {cur_db}")
        stream_decompress(c, KEY, cur_db)

    old, new = source_counts(cur_db), source_counts(LOCAL)
    shrink = sorted((old[s] - new.get(s, 0), s) for s in old if new.get(s, 0) < old[s])
    gained = sum(new[s] - old.get(s, 0) for s in new if new[s] > old.get(s, 0))
    print(f"\n  current R2 : {sum(old.values()):,} series across {len(old)} sources")
    print(f"  local      : {sum(new.values()):,} series across {len(new)} sources")
    print(f"  gained     : +{gained:,} series")
    if shrink:
        print(f"  SHRINK     : {len(shrink)} source(s) lose series:")
        blocked = []
        for d, s in shrink:
            tag = "allowed (declared deliberate)" if s in allow else "*** BLOCKS THE UPLOAD"
            if s not in allow:
                blocked.append(s)
            print(f"     {s:<26} {old[s]:>10,} -> {new.get(s, 0):>10,}   -{-d if d < 0 else d:,}  {tag}")
        if blocked:
            print("\n  ABORTED: the upload would drop series for the source(s) above. If that is "
                  "intended, re-run with --allow-shrink " + ",".join(blocked), file=sys.stderr)
            return 2
    else:
        print("  SHRINK     : none — clean superset")

    if a.dry_run:
        print("\n  --dry-run: no write performed.")
        return 0

    # ---- backup (server-side copy: no download, no memory) ---------------------------
    bak = f"{KEY}.bak-{a.stamp}"
    c.copy_object(Bucket=BUCKET, Key=bak, CopySource={"Bucket": BUCKET, "Key": KEY})
    print(f"\n  backed up current R2 catalog -> {bak}")

    # ---- compress to disk, then upload from disk -------------------------------------
    zpath = os.path.join(tmpdir, "catalog.db.zst")
    with open(LOCAL, "rb") as ifh, open(zpath, "wb") as ofh:
        zstandard.ZstdCompressor(level=10).copy_stream(
            ifh, ofh, size=os.path.getsize(LOCAL))
    print(f"  compressed -> {zpath}  ({os.path.getsize(zpath):,} B)")

    with open(zpath, "rb") as fh:
        c.upload_fileobj(fh, BUCKET, KEY, ExtraArgs={"ContentType": "application/zstd"})
    print(f"  uploaded new {KEY}")

    # ---- verify the object that is actually there now --------------------------------
    vpath = os.path.join(tmpdir, "verify_catalog.db")
    stream_decompress(c, KEY, vpath)
    # quick_check the object that is NOW LIVE, not just the one we read. Another process
    # can write catalog.db while this script streams it, and a torn upload passes every
    # count check here only to fail on the runner as an unexplained mapping miss.
    vcon = sqlite3.connect(f"file:{vpath.replace(os.sep, '/')}?mode=ro", uri=True)
    vqc = vcon.execute("PRAGMA quick_check(1)").fetchone()[0]
    vcon.close()
    print(f"  uploaded object quick_check: {vqc}")
    if vqc != "ok":
        print(f"  *** UPLOADED OBJECT IS CORRUPT — roll back with {bak}", file=sys.stderr)
        return 1
    back = source_counts(vpath)
    tot = sum(back.values())
    print(f"\n  verify re-download: {tot:,} series across {len(back)} sources")
    ok = True
    for s in ("noaa", "cepii_gravity", "adb", "fhfa", "usda", "fed_board", "istat"):
        print(f"    {s:16}{back.get(s, 0):>12,}")
    for s in ("cow", "sipri", "polity"):
        n = back.get(s, 0)
        print(f"    purged {s:9}{n:>12}  ({'OK gone' if n == 0 else '*** still present'})")
    if tot != sum(new.values()):
        print(f"  *** MISMATCH: uploaded {sum(new.values()):,} but read back {tot:,}", file=sys.stderr)
        ok = False
    print(f"\n  {'DONE' if ok else 'FAILED'}. Rollback: copy {bak} back over {KEY}.")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
