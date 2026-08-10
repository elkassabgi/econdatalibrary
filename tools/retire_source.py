"""Retire a legacy source whose publisher-direct successor is live — the CLEAN removal.

AUTHORIZED by Ahmed 2026-08-06 ("no bookmarks, no one has even seen the data.. refresh to
match publisher.. I need a clean database"): legacy relay-era ids retire in favor of their
proven *_direct successors. The full plan (Class A/B1/B2) lives in
.claude/skills/econ-updater/references/50-queue.md.

Pipeline per source (each step verified, dry-run by default):
  1. ARCHIVE the primary parquet(s) to r2://<bucket>/archive/retired/<source>/ — cheap
     insurance, same pattern as purge_unpermitted_r2.py.
  2. catalog.db: DELETE series + source rows.
  3. D1: DELETE series + source rows (wrangler d1 execute; the worker requires BOTH a
     source row and >=1 series for /v1/sources, so after this the id vanishes there).
  4. R2 purge: series/<urlencode('<source>:')>-prefixed CSVs + clean_full/<source>/ store.
     Prefixes TERMINATED ('imf_fsi%3A', 'clean_full/imf_fsi/') so the *_direct successors
     sharing the name stem can NEVER be swept (imf_fsi vs imf_fsi[bsis]_direct is exactly
     the R112/R129 unanchored-substring trap; the '%3A'/'/' terminators kill it).
  5. Caller then: remove the id from util.ts SUPPORTED_SOURCES, retire any registry entry
     (+count bump SAME commit, R347), wrangler deploy, live /v1/sources absence check,
     refresh_r2_catalog, coverage re-measure. Those are deliberate manual/reviewed steps.

Usage:
  python tools/retire_source.py imf_psbsfad                 # dry run: counts only
  python tools/retire_source.py imf_psbsfad --apply         # execute steps 1-4
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import subprocess
import sys
import urllib.parse

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import r2_util  # noqa: E402

BUCKET = "econ-data"
D1_NAME = "econ-catalog"


def walk(client, prefix):
    tok = None
    while True:
        kw = dict(Bucket=BUCKET, Prefix=prefix, MaxKeys=1000)
        if tok:
            kw["ContinuationToken"] = tok
        r = client.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            yield o["Key"], o["Size"]
        if not r.get("IsTruncated"):
            return
        tok = r.get("NextContinuationToken")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("source")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    src = a.source
    if src.endswith("_direct"):
        print(f"REFUSING: {src} is a publisher-direct SUCCESSOR, never a retirement target")
        return 1

    read = r2_util.client()
    con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=120)
    con.execute("PRAGMA busy_timeout=120000")

    n_series = con.execute("SELECT COUNT(*) FROM series WHERE source_id=?",
                           (src,)).fetchone()[0]
    n_source = con.execute("SELECT COUNT(*) FROM source WHERE source_id=?",
                           (src,)).fetchone()[0]

    # TERMINATED prefixes — the ':' urlencodes to %3A; the store dir ends with '/'.
    csv_prefix = "series/" + urllib.parse.quote(f"{src}:", safe="")
    store_prefix = f"clean_full/{src}/"
    csv_keys = [k for k, _ in walk(read, csv_prefix)]
    store_objs = list(walk(read, store_prefix))
    parquets = [(k, s) for k, s in store_objs if k.endswith(".parquet")]

    print(f"{src}: catalog series={n_series:,} source_row={n_source}  "
          f"R2 csvs={len(csv_keys):,}  store objects={len(store_objs):,} "
          f"(parquets to archive: {len(parquets)})")

    if not a.apply:
        print("(dry run — pass --apply to retire)")
        return 0

    write = r2_util.client(write=True)

    # 1. archive primary parquets
    for k, _ in parquets:
        dst = f"archive/retired/{src}/{os.path.basename(k)}"
        write.copy_object(Bucket=BUCKET, CopySource={"Bucket": BUCKET, "Key": k}, Key=dst)
    print(f"  archived {len(parquets)} parquet(s) -> archive/retired/{src}/")

    # 2. catalog.db
    con.execute("DELETE FROM series WHERE source_id=?", (src,))
    con.execute("DELETE FROM source WHERE source_id=?", (src,))
    con.commit()
    left = con.execute("SELECT COUNT(*) FROM series WHERE source_id=?", (src,)).fetchone()[0]
    print(f"  catalog.db: deleted; residual rows={left} (must be 0)")

    # 3. D1
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    for stmt in (f"DELETE FROM series WHERE source_id='{src}';",
                 f"DELETE FROM source WHERE source_id='{src}';"):
        r = subprocess.run(["npx", "wrangler", "d1", "execute", D1_NAME, "--remote",
                            "--command", stmt],
                           cwd=os.path.join(ROOT, "api", "worker"),
                           capture_output=True, text=True, encoding="utf-8",
                           errors="replace", env=env, shell=(os.name == "nt"))
        ok = r.returncode == 0
        print(f"  D1: {stmt.split(' WHERE')[0]} -> {'ok' if ok else 'FAILED'}")
        if not ok:
            # The subprocess is captured as utf-8, but THIS console is cp1252 and
            # wrangler's output carries emoji — so printing the error raised
            # UnicodeEncodeError and destroyed the only report of why D1 failed,
            # leaving the source deleted from catalog.db but still live in D1.
            # An error path that can itself crash is worse than no error path.
            # (R363/R234, one organ further along: there it was the subprocess
            # decode, here it is the print encode.)
            detail = (r.stderr or r.stdout or "")[-600:]
            sys.stdout.write(detail.encode(sys.stdout.encoding or "utf-8",
                                           "replace").decode(sys.stdout.encoding
                                                             or "utf-8", "replace"))
            sys.stdout.write("\n")
            return 1

    # 4. R2 purge (batched deletes; re-assert the terminated prefix on every batch)
    def purge(keys, label):
        n = 0
        for i in range(0, len(keys), 1000):
            batch = keys[i:i + 1000]
            assert all(k.startswith((csv_prefix, store_prefix)) for k in batch)
            write.delete_objects(Bucket=BUCKET,
                                 Delete={"Objects": [{"Key": k} for k in batch],
                                         "Quiet": True})
            n += len(batch)
        print(f"  R2: deleted {n:,} {label}")

    purge(csv_keys, "series CSV(s)")
    purge([k for k, _ in store_objs], "store object(s)")

    print(f"{src}: RETIRED (data plane). Now: util.ts removal + registry retire/count bump "
          f"+ deploy + live absence check + refresh_r2_catalog.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
