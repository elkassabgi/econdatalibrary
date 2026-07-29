"""Turn MERGED data into SERVED data: sync store -> catalogue -> derive -> verify.

A successful merge is not a served source. Twice today a repaired fetcher published
its parquet and left most of the source impossible to download:

    yale_epi   77,240 in the store,  21,300 catalogued  ->  55,940 invisible
    fao_fo     85,211 in the store,  16,703 catalogued  ->  68,508 invisible
    fao_pp     40,016 in the store,   4,832 catalogued  ->  35,184 invisible

Nothing errors in that state. The run is green, the data is genuinely hosted, and
the series simply cannot be found or fetched — which is the "host it fully or don't
list it" rule broken silently. Doing the repair by hand once per source invites
missing a step, so it lives here as one sequence:

  1. SYNC the published parquet down from R2. The CSV resolver reads the LOCAL
     store, and under the r2 backend that holds only what the local process wrote —
     so deriving without this step fails every new series with "zero rows matched"
     (observed on yale_epi). Never-shrink is asserted before overwriting: a local
     file LARGER than the published one means something is wrong, and the copy is
     refused rather than losing rows.
  2. CATALOGUE the keys that have no row, inheriting the source's existing licence
     (tools/catalog_complete.py refuses outright if it cannot find one).
  3. DERIVE the missing CSVs concurrently.
  4. VERIFY against R2 by listing it, not by trusting the loop's own counter.

D1 sync is deliberately NOT run here — it is a write to the serving catalogue and
belongs in an explicit step (core/sync_catalog_d1.py --source X).

Usage:  python tools/make_servable.py fao_fo fao_pp fao_et
"""
from __future__ import annotations

import io
import os
import shutil
import sqlite3
import subprocess
import sys
import time
import urllib.parse

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
sys.path.insert(0, ROOT)
os.environ["AQUEDUCT_BACKEND"] = "r2"
os.environ.setdefault("AQUEDUCT_DERIVE_WORKERS", "12")

import pyarrow.parquet as pq                                  # noqa: E402
from core import r2_util                                      # noqa: E402
from updater import derive, blob as bm                        # noqa: E402

BUCKET = "econ-data"


def r2_stamps(client, source):
    """{series_id: LastModified} for every CSV of a source — one full pagination.

    PRESENCE IS NOT CURRENCY, and the difference is not academic. fao_oa had all
    1,388 CSVs present, so this returned them all, "to derive" was 0, and the verify
    printed OK — while 69.6% of its served observations differed from the published
    parquet by up to 460%, because the CSVs predated a republish by 26 days. A stale
    VINTAGE carries the same dates as a fresh one, so nothing date-based can see it;
    only the write TIME distinguishes them. Treating a CSV older than the parquet as
    absent is what makes the gap self-healing.
    """
    out, tok = {}, None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": f"series/{source}", "MaxKeys": 1000}
        if tok:
            kw["ContinuationToken"] = tok
        r = client.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            if o["Key"].endswith(".csv"):
                out[urllib.parse.unquote(o["Key"][len("series/"):-4])] = o["LastModified"]
        if not r.get("IsTruncated"):
            break
        tok = r["NextContinuationToken"]
    return out


def parquet_mtime(client, source):
    """Newest publish time among a source's parquets, or None."""
    newest, tok = None, None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": f"clean_full/{source}/", "MaxKeys": 1000}
        if tok:
            kw["ContinuationToken"] = tok
        r = client.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            if o["Key"].endswith(".parquet") and (newest is None
                                                  or o["LastModified"] > newest):
                newest = o["LastModified"]
        if not r.get("IsTruncated"):
            break
        tok = r["NextContinuationToken"]
    return newest


def sync_parquet(client, source):
    """Bring the local store into line with what is PUBLISHED, or refuse."""
    key = f"clean_full/{source}/{source}.parquet"
    dst = os.path.join(ROOT, "data", "clean_full", source, f"{source}.parquet")
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    raw = client.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    pub = pq.read_table(io.BytesIO(raw))
    if os.path.exists(dst):
        loc = pq.read_table(dst)
        if loc.num_rows > pub.num_rows:
            print(f"  REFUSING sync: local {loc.num_rows:,} rows > published "
                  f"{pub.num_rows:,}. Investigate before overwriting.", flush=True)
            return None
        shutil.copy2(dst, dst + ".bak")
    io.open(dst, "wb").write(raw)
    print(f"  store synced: {pub.num_rows:,} rows, "
          f"{len(set(pub['series_key'].to_pylist())):,} series", flush=True)
    return pub


def main(sources):
    client = r2_util.client()
    blob = bm.from_env()
    for src in sources:
        print(f"\n=== {src} ===", flush=True)
        if sync_parquet(client, src) is None:
            continue

        r = subprocess.run([sys.executable, "tools/catalog_complete.py", src],
                           capture_output=True, text=True, encoding="utf-8",
                           errors="replace")
        for line in (r.stdout or "").strip().splitlines():
            if line.strip():
                print("  " + line.strip(), flush=True)

        con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"))
        ids = [x[0] for x in con.execute(
            "SELECT series_id FROM series WHERE source_id=?", (src,))]
        # List R2 ONCE. Written as `[i for i in ids if i not in r2_csvs(client, src)]`
        # this re-listed the entire prefix per id — 40,016 full listings for fao_pp,
        # which burned 14 minutes without writing a single object while looking
        # perfectly busy (20% CPU, memory flat). fao_et survived it only by being
        # 574 ids long. A function call inside a comprehension's condition is
        # evaluated every iteration; when that call is an S3 listing, the loop is
        # quadratic in network round-trips.
        pmt = parquet_mtime(client, src)
        # ONE listing, both facts. Computing the stale count with a second
        # r2_csvs() call doubled the pagination for no information gain — the same
        # class of waste as the quadratic listing this file already warns about.
        stamps = r2_stamps(client, src)
        have = {k for k, m in stamps.items() if pmt is None or m >= pmt}
        todo = [i for i in ids if i not in have]
        n_stale = sum(1 for i in ids if i in stamps and i not in have)
        print(f"  catalog {len(ids):,} | to derive {len(todo):,}"
              + (f" ({n_stale:,} of them present but OLDER than the parquet)"
                 if n_stale > 0 else ""), flush=True)
        if todo:
            t0 = time.time()
            res = derive.derive_and_put(todo, blob)
            el = time.time() - t0
            print(f"  derived put={res['put']:,} failed={len(res['failed']):,} "
                  f"in {el / 60:.1f} min ({res['put'] / max(el, 1e-9):.1f}/s)",
                  flush=True)
            for f in res["failed"][:5]:
                print(f"     FAIL {f}", flush=True)

        # Verify by LISTING R2, never by trusting the counter above — once. The
        # freshness filter applies here too: a verify that counts stale files as
        # present is the check that declared fao_oa OK while it served 26-day-old
        # values.
        after = {k for k, m in r2_stamps(client, src).items()
                 if pmt is None or m >= pmt}
        missing = [i for i in ids if i not in after]
        print(f"  VERIFY: catalog {len(ids):,}  csv_in_r2 {len(ids) - len(missing):,}"
              f"  MISSING {len(missing):,}"
              + ("  <-- still not downloadable" if missing else "  OK"), flush=True)
        if missing:
            print("     e.g. " + ", ".join(missing[:3]), flush=True)
        print(f"  NEXT: python core/sync_catalog_d1.py --source {src}", flush=True)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        raise SystemExit(2)
    main(sys.argv[1:])
