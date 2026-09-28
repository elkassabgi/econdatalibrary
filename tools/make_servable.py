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

import glob
import io
import os
import shutil
import sqlite3
import subprocess
import sys
import time
import urllib.parse

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from core import cutover                                      # noqa: E402


def _prepare_process():
    """The process settings this tool runs with, set when it RUNS, not when it is imported (an import used to
    chdir and switch the whole interpreter's store backend - a test importing it moved 41 unrelated tests onto
    R2). Before T0 the published store is R2; after T0 the self-hosted one this machine serves."""
    os.chdir(ROOT)
    os.environ["AQUEDUCT_BACKEND"] = "selfhost" if cutover.is_cut_over() else "r2"
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
    # ANCHOR ON THE COLON. `series/{source}` is a PREFIX match, so it also returns
    # every longer source id that starts with this one: listing `imf_fsi` swept in all
    # 18,620 `imf_fsire` objects and the VERIFY reported them as ORPHANED — a
    # fabricated finding about a healthy sibling source. There are 50 such pairs in the
    # catalog (imf_fsi/imf_fsire, imf/imf_*, fao_q*/...), so this is not a one-off.
    #
    # It fails in the dangerous direction too: MISSING is computed against this set, so
    # a source could appear to already have CSVs that in fact belong to its
    # longer-named sibling, and the derive would SKIP files that were never written.
    # Keys are `series/<urlencoded source:id>.csv`, so anchoring on the encoded colon
    # (%3A) makes the prefix exact.
    prefix = "series/" + urllib.parse.quote(f"{source}:", safe="")
    out, tok = {}, None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": prefix, "MaxKeys": 1000}
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
    """Bring the local store into line with what is PUBLISHED, or refuse.

    NOT EVERY SOURCE IS ONE FILE. This assumed `clean_full/<src>/<src>.parquet` and
    died with a bare botocore NoSuchKey on the first multi-file source it met — adb
    publishes 54 per-flow parquets (EGELC.parquet, EGELC_EG.parquet, ...) and un_wpp
    two (indicators_medium/other). The traceback named the missing key but not the
    source, and it killed the whole batch before any of the four was derived.

    For those layouts there is no single object to pull, so the never-shrink comparison
    this function exists to perform cannot be made. Skip the sync and SAY SO — the
    local store is then taken as authoritative, which is correct only because no
    fetcher has republished these sources (they are not in the live tier). Deriving
    from a stale local copy against a newer published one is the fao_oa failure, so
    this must stay loud rather than becoming a silent fallback.
    """
    key = f"clean_full/{source}/{source}.parquet"
    dst = os.path.join(ROOT, "data", "clean_full", source, f"{source}.parquet")
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    try:
        raw = client.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    except client.exceptions.NoSuchKey:
        n = len(glob.glob(os.path.join(ROOT, "data", "clean_full", source,
                                       "**", "*.parquet"), recursive=True))
        if n == 0:
            print(f"  NO published {key} and NO local parquet — nothing to serve.",
                  flush=True)
            return None
        print(f"  multi-file layout: no single {source}.parquet on R2; SKIPPING the "
              f"never-shrink sync and deriving from the {n} local file(s). Valid only "
              f"because no fetcher republishes this source — verify if that changes.",
              flush=True)
        return "multi-file"
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


def _verify_line(ids, stamps_after, pmt, derived_now=frozenset()):
    """The VERIFY lists: missing (catalogued, no current CSV) and orphaned (a CSV the catalogue does not list). A
    CSV is current when stored at or after the newest parquet, or derived by this run and present in the store."""
    after = {k for k, m in stamps_after.items() if pmt is None or m >= pmt or k in derived_now}
    missing = [i for i in ids if i not in after]
    orphans = sorted(set(stamps_after) - set(ids))
    return missing, orphans


def main_selfhosted(sources):
    """AFTER T0 (plan step 1): the local store IS the served store and the series CSVs live in the self-hosted
    blob store the origin serves. So: no sync (nothing is published elsewhere to pull down); the catalogue,
    the derive and the verify run in THIS process under the single-writer lock for the whole run - the
    cataloguer is called in-process, because a child would be refused the lock this process holds; the
    verify lists the self-hosted store with the same freshness rule (a CSV older than the source's newest
    parquet counts as absent). Refused outside the live checkout. No R2, no D1."""
    from core import catalog_path                                # noqa: PLC0415
    from tools import catalog_complete                           # noqa: PLC0415
    import datetime as dt                                        # noqa: PLC0415
    bm.refuse_unless_live_checkout("make_servable (it writes the served store and the catalogue)")
    os.environ["AQUEDUCT_BACKEND"] = "selfhost"                  # the cataloguer's store reads: the local store
    store = bm.SelfhostBlob()
    failed_sources = []
    with catalog_path.write_session():
        for src in sources:
            print(f"\n=== {src} ===", flush=True)
            try:
                if not _serve_one_selfhosted(src, store, catalog_path, catalog_complete, dt):
                    failed_sources.append(src)
            except cutover.CutoverRefused:
                raise                                # a refusal is never one source's problem
            except Exception as e:                   # noqa: BLE001 - one bad source must not stop the batch
                # (R1241: in-process, a corrupt parquet in the first source raised and the rest were never served)
                print(f"  FAIL {src}: {type(e).__name__}: {e}", flush=True)
                failed_sources.append(src)
    if failed_sources:
        print(f"\nNOT SERVED CLEANLY: {failed_sources}", flush=True)
        return 1
    return 0


def _serve_one_selfhosted(src, store, catalog_path, catalog_complete, dt) -> bool:
    """One source after T0. True when it verifies with nothing missing."""
    files = [f for f in glob.glob(os.path.join(ROOT, "data", "clean_full", src, "**", "*.parquet"),
                                  recursive=True) if not os.path.basename(f).startswith("_")]
    if not files:
        print("  NO parquet in the store - nothing to serve.", flush=True)
        return True
    print(f"  store: {len(files):,} parquet file(s); after T0 the local store IS the served store (no sync)",
          flush=True)
    catalog_complete.main([src])
    con = catalog_path.connect()
    ids = [x[0] for x in con.execute("SELECT series_id FROM series WHERE source_id=?", (src,))]
    con.close()
    # The NEWEST parquet, ROUNDED UP to the store's whole seconds (stored_utc has no fraction): a CSV stored in
    # the same second as the parquet's last write, or earlier, counts as stale - R1241 found the floor let a CSV
    # half a second older pass as current. What THIS run writes is current by construction (below).
    newest = max(dt.datetime.fromtimestamp(os.path.getmtime(f), tz=dt.timezone.utc) for f in files)
    pmt = newest.replace(microsecond=0) + (dt.timedelta(seconds=1) if newest.microsecond else dt.timedelta(0))
    prefix = "series/" + urllib.parse.quote(f"{src}:", safe="")                # anchored on the colon
    stamps = {urllib.parse.unquote(k[len("series/"):-4]): m
              for k, m in store.list_modified(prefix) if k.endswith(".csv")}
    have = {k for k, m in stamps.items() if m >= pmt}
    todo = [i for i in ids if i not in have]
    n_stale = sum(1 for i in ids if i in stamps and i not in have)
    print(f"  catalog {len(ids):,} | to derive {len(todo):,}"
          + (f" ({n_stale:,} of them present but OLDER than the parquet)" if n_stale else ""), flush=True)
    derived_now = set()
    if todo:
        t0 = time.time()
        res = derive.derive_and_put(todo, store)
        el = time.time() - t0
        print(f"  derived put={res['put']:,} (already current, not re-written: "
              f"{res.get('skipped_identical', 0):,}) failed={len(res['failed']):,} in {el / 60:.1f} min", flush=True)
        for f in res["failed"][:5]:
            print(f"     FAIL {f}", flush=True)
        # derived THIS run and in the store: current, whether written or found identical (R1241: a skipped
        # identical write keeps its old stored_utc, and the time rule alone then called it MISSING)
        derived_now = set(todo) - set(res["failed"])
    stamps_after = {urllib.parse.unquote(k[len("series/"):-4]): m
                    for k, m in store.list_modified(prefix) if k.endswith(".csv")}
    missing, orphans = _verify_line(ids, stamps_after, pmt, derived_now)
    print(f"  VERIFY: catalog {len(ids):,}  csv_in_store {len(ids) - len(missing):,}"
          f"  MISSING {len(missing):,}  ORPHANED {len(orphans):,}"
          + ("  <-- still not downloadable" if missing else
             "  <-- serving ids the catalog does not list" if orphans else "  OK"), flush=True)
    if missing:
        print("     missing e.g. " + ", ".join(missing[:3]), flush=True)
    if orphans:
        print("     orphaned e.g. " + ", ".join(orphans[:3]), flush=True)
    print("  " + cutover.next_steps(f"NEXT: python core/sync_catalog_d1.py --source {src}"), flush=True)
    return not missing


def main(sources):
    _prepare_process()
    if cutover.is_cut_over():
        return main_selfhosted(sources)
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

        con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"))  # plain-open: BEFORE T0 only, the checkout's catalogue as this tool always read it (after T0 main_selfhosted uses catalog_path.connect)
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
            # `put` counts CSVs HANDLED, not bytes sent: R2Blob.put_atomic compares the
            # gzipped body against the stored ETag and returns without uploading when they
            # match. Naming the skipped share here keeps this line from counting one thing
            # while saying another (R666, and R628/R652 before it).
            _skipped = res.get("skipped_identical", 0)
            print(f"  derived put={res['put']:,} "
                  f"(already current, not re-uploaded: {_skipped:,}) "
                  f"failed={len(res['failed']):,} "
                  f"in {el / 60:.1f} min ({res['put'] / max(el, 1e-9):.1f}/s)",
                  flush=True)
            for f in res["failed"][:5]:
                print(f"     FAIL {f}", flush=True)

        # Verify by LISTING R2, never by trusting the counter above — once. The
        # freshness filter applies here too: a verify that counts stale files as
        # present is the check that declared fao_oa OK while it served 26-day-old
        # values.
        stamps_after = r2_stamps(client, src)
        after = {k for k, m in stamps_after.items() if pmt is None or m >= pmt}
        missing = [i for i in ids if i not in after]
        # BOTH DIRECTIONS. `missing` answers "is every catalogued series downloadable"
        # and stops there — it is structurally blind to the opposite failure: a CSV
        # still sitting in R2 under an id the catalog no longer lists. That is not
        # hypothetical here. fao_qa's catalog went from ~79,000 series to 3,182 when
        # the QCL superset it had absorbed was restricted away; had the CSVs not been
        # purged with it, ~76,000 objects would have gone on being served under a
        # prefix that no longer claims them, and this verify would have printed OK.
        # The set difference is free — R2 is already listed above — so the only reason
        # it was ever one-directional is that nobody asked the other question.
        orphans = sorted(set(stamps_after) - set(ids))
        print(f"  VERIFY: catalog {len(ids):,}  csv_in_r2 {len(ids) - len(missing):,}"
              f"  MISSING {len(missing):,}  ORPHANED {len(orphans):,}"
              + ("  <-- still not downloadable" if missing else
                 "  <-- serving ids the catalog does not list" if orphans else "  OK"),
              flush=True)
        if missing:
            print("     missing e.g. " + ", ".join(missing[:3]), flush=True)
        if orphans:
            print("     orphaned e.g. " + ", ".join(orphans[:3]), flush=True)
        print(f"  NEXT: python core/sync_catalog_d1.py --source {src}", flush=True)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        raise SystemExit(2)
    raise SystemExit(main(sys.argv[1:]) or 0)      # after T0 a source that did not serve cleanly exits 1
