"""Materialize per-series CSV objects to R2 so /v1/series/{id}.csv is live on the Worker.

For each catalog series_id, project rows through the SAME econdl resolver the dev shim
uses (so the bytes are identical to the local /v1 response), and PUT them to R2 at
  <prefix>/series/<urlencoded series_id>.csv
The Worker then serves /v1/series/{id}.csv as a plain R2 GET (no parquet-in-Worker).

  python core/derive_csv.py --dry-run --limit 5     # derive locally + DIFF vs the dev shim
  python core/derive_csv.py --bucket econ-data       # full run (needs R2 write creds)

Tidy sources emit the canonical `series_id,obs_date,value`; relational/wide sources
(tidy_ok=False) emit their native columns verbatim — exactly as the contract specifies.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import os
import sqlite3
import sys
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "clients", "python"))

from . import r2_util  # noqa: E402

ROOT = r2_util.ROOT
CATALOG = os.path.join(ROOT, "data", "catalog.db")
DEFAULT_PREFIX = "series"


def _series_csv_bytes(series_id: str) -> bytes:
    """Project one series to CSV bytes via the econdl resolver (the contract shape)."""
    from econdl import _resolve
    res = _resolve.resolve(series_id)
    table = _resolve.read_native(res)
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")   # match the dev shim / Worker byte-for-byte
    if res.tidy_ok:
        df = _resolve.native_to_tidy(res, table)
        w.writerow(["series_id", "obs_date", "value"])
        for sid, _src, d, v in df[["series_id", "source", "obs_date", "value"]].itertuples(index=False):
            w.writerow([sid, d, v])
    else:
        cols = table.column_names
        w.writerow(cols)
        for row in table.to_pylist():
            w.writerow([row.get(c) for c in cols])
    return buf.getvalue().encode("utf-8")


def _catalog_ids(limit: int | None, source: list | None):
    conn = sqlite3.connect(f"file:{CATALOG}?mode=ro", uri=True)
    try:
        q = "SELECT series_id, source_id FROM series"
        if source:
            q += " WHERE source_id IN (%s)" % ",".join("?" * len(source))
        q += " ORDER BY source_id, series_id"
        if limit:
            q += f" LIMIT {int(limit)}"
        return conn.execute(q, source or []).fetchall()
    finally:
        conn.close()


def _put_with_backoff(s3, bucket, key, body) -> None:
    """PUT one object, gzip-compressed. R2 throws transient ServiceUnavailable/SlowDown
    throttles that outlast botocore's 5 built-in retries (that killed the 2026-07-02 run at
    103k objects). Patient app-level backoff: 7 tries, ~2 min total, then re-raise loudly
    rather than lose the object.

    GZIP AT REST (cost plan 2026-08-18): numeric CSVs compress 5-10x and R2 storage is the
    bill's dominant line. ContentEncoding='gzip' on the object is the marker the worker's
    reader keys on (api/worker/src/series.ts decompresses before its date-window/citation
    processing, so clients see byte-identical responses). mtime=0 in the gzip header keeps
    the bytes deterministic — verify_source_served byte-compares served objects against
    freshly-derived expectations, and a timestamp in the header would break equality for
    identical CSV content."""
    import gzip as _gzip
    import time as _time
    if isinstance(body, str):
        body = body.encode()
    body = _gzip.compress(body, mtime=0)
    for attempt in range(7):
        try:
            s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="text/csv",
                          ContentEncoding="gzip")
            return
        except Exception as e:                               # noqa: BLE001
            if attempt == 6:
                raise
            wait = 2 ** attempt                              # 1..64s
            print(f"  PUT retry {attempt+1}/7 in {wait}s ({str(e)[:70]})", flush=True)
            _time.sleep(wait)


def _mirror_behind_store(sources, sample: int = 0):
    """[(source, detail)] for sources whose LOCAL parquets hold less than R2's.

    Compared by row count and max observation date only — see the note at the call site for
    why timestamps and hashes are both wrong here. A single behind file is enough to refuse,
    because we cannot know which series it feeds. Any error reading either side is treated as
    "cannot prove it is safe" and reported.

    SAMPLE SIZE SCALES WITH THE SOURCE, and says what it checked. A fixed 4-file sample was
    the first version and it is close to meaningless on statcan (8,207 files) — it would clear
    a source after inspecting 0.05% of it, which is the kind of bounded check that reads as
    coverage and is not (R190's disease, applied to a guard). Now: min(64, max(6, 5% of files)),
    and the count is printed so a thin check cannot pass for a thorough one. Still a SAMPLE —
    it can miss a behind file — so a clean result means "no evidence of drift in N files",
    not "the mirror is current". Passing an explicit `sample` overrides the scaling.
    """
    import glob
    import random
    import tempfile

    out = []
    try:
        import duckdb
        from core import r2_util
        s3 = r2_util.client()
    except Exception as e:                                            # noqa: BLE001
        print(f"[preflight] cannot reach R2 to check the mirror ({e!r}) — not blocking")
        return out

    # NOT ONE STORE ROOT. This assumed data/clean_full and therefore returned [] for every
    # source that lives elsewhere — silently CLEARING them. sec_edgar's 17,276 parquets are
    # under data/clean_grouped/, so the guard added today would not have blocked its re-derive
    # at all, and ~2,000 of its served CSVs were written from a stale mirror while the
    # preflight reported nothing. A guard that cannot find a source's files must not read as
    # "this source is fine".
    ROOTS = ("clean_full", "clean_grouped")

    # NOR ONE DIRECTORY LEVEL. `os.listdir` sees only the top of the tree, and two stores are
    # nested: bea is data/clean_full/bea/<Dataset>/<Table>.parquet and usda
    # data/clean_full/usda/<theme>/part_NNN.parquet. For those this printed "NO local parquets
    # under any of ('clean_full', 'clean_grouped') — UNCHECKED" while 60 usda parquets sat
    # one level down, so the guard skipped the exact source it was meant to check. Walk, and
    # carry the RELATIVE path so the R2 key matches (tools/footer_diff.py made the same mistake
    # keying on the basename and reported 30 phantom AHEAD files for eia — ledger R389).
    # NOT updater.blob.list_parquets(recursive=True), which does exactly this walk but is
    # R2-ROUTED: under AQUEDUCT_BACKEND=r2 it lists the BUCKET. Using it here would make the
    # "local" side of a local-vs-R2 comparison come from R2, so the guard would compare the
    # store against itself and pass every time — a far worse failure than the one being fixed.
    def _rel_parquets(d):
        out_ = []
        for dirpath, _dirs, fs in os.walk(d):
            rel = os.path.relpath(dirpath, d).replace(os.sep, "/")
            out_ += [f if rel == "." else f"{rel}/{f}" for f in fs if f.endswith(".parquet")]
        return out_

    def _dir_for(src):
        for r in ROOTS:
            d = os.path.join(ROOT, "data", r, src)
            if os.path.isdir(d) and _rel_parquets(d):
                return d, r
        return None, None

    if sources:
        names = list(sources)
    else:
        names = []
        for r in ROOTS:
            names += [os.path.basename(p)
                      for p in glob.glob(os.path.join(ROOT, "data", r, "*"))]
        names = sorted(set(names))
    q = duckdb.connect()
    tmp = tempfile.mkdtemp()

    def stats(path):
        p = path.replace(os.sep, "/")
        cols = [r[0] for r in q.execute(
            f"describe select * from read_parquet('{p}')").fetchall()]
        dc = [c for c in cols if "date" in c.lower()]
        n = q.execute(f"select count(*) from read_parquet('{p}')").fetchone()[0]
        mx = q.execute(
            f"select max({dc[0]})::VARCHAR from read_parquet('{p}')").fetchone()[0] if dc else None
        return n, mx

    for src in names:
        d, store_root = _dir_for(src)
        if d is None:
            # SAY SO. Returning quietly here is what made sec_edgar invisible: no directory
            # found reads downstream as "nothing wrong with this source".
            if sources:
                print(f"[preflight] {src}: NO local parquets under any of {ROOTS} — cannot "
                      f"compare against R2, so this source is UNCHECKED, not clean", flush=True)
            continue
        files = _rel_parquets(d)
        if not files:
            continue
        k = sample or min(64, max(6, len(files) // 20))
        k = min(k, len(files))
        print(f"[preflight] {src}: comparing {k} of {len(files)} parquet(s) under "
              f"data/{store_root}/ against R2 by row count and max obs date", flush=True)
        for f in random.Random(0).sample(files, k):
            rp = os.path.join(tmp, "r.parquet")
            try:
                # Same root on both sides — a clean_grouped source must be compared against
                # r2://econ-data/clean_grouped/, not clean_full, or every object 404s and the
                # `except: continue` below turns the whole check into a silent pass.
                s3.download_file("econ-data", f"{store_root}/{src}/{f}", rp)
                ln, lmx = stats(os.path.join(d, *f.split("/")))
                rn, rmx = stats(rp)
            except Exception:                                         # noqa: BLE001
                continue
            if rn > ln or (rmx and lmx and str(rmx) > str(lmx)):
                out.append((src, f"{f}: local {ln:,} rows/{lmx} vs R2 {rn:,} rows/{rmx}"))
                break
            if ln > rn or (rmx and lmx and str(lmx) > str(rmx)):
                # THE OTHER DIRECTION, which the first version of this guard could not see.
                # An adversarial audit measured 79 clean_full files (+6 sec_edgar) with MORE
                # rows locally than on R2 — ilostat 41, cbs_nl 10, edgar_13f 7, gus_dbw 7 — and
                # six sources (abs, cso, ember, fed_board, ilostat, usda) diverging in BOTH
                # directions at once. That is the store missing data, not the mirror lagging.
                #
                # It does NOT refuse: deriving from the richer local copy is not the danger
                # here, and blocking would stop legitimate work over a store-side problem. But
                # it must never be silent, because "local has rows R2 lacks" means an upload
                # was lost and nobody has noticed.
                #
                # DO NOT "fix" this by copying local over R2. Spot checks show the divergence
                # is two-directional on the same files: abs/LF_HOURS has 160 rows on R2 that
                # are NOT local; zillow/State_zhvi differs in every row (revised values);
                # sec_edgar/XOM is local 20,629 vs R2 274 yet R2's max date is NEWER. A blind
                # push would destroy data. It is a MERGE queue.
                print(f"[preflight] WARNING {src}/{f}: the LOCAL mirror is AHEAD of R2 "
                      f"(local {ln:,} rows/{lmx} vs R2 {rn:,} rows/{rmx}) — the store is "
                      f"missing rows this machine holds. Do not push local over R2; the "
                      f"divergence is often two-directional. Investigate before trusting "
                      f"either side.", flush=True)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Derive per-series CSV objects to R2")
    ap.add_argument("--bucket")
    ap.add_argument("--prefix", default=DEFAULT_PREFIX)
    ap.add_argument("--dry-run", action="store_true", help="derive locally, contact no R2")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--source", action="append")
    ap.add_argument("--verify-shim", help="base URL of a running dev shim to byte-diff against")
    ap.add_argument("--skip-newer-than", default=None,
                    help="ISO8601 UTC; skip series whose R2 object was last modified at or after "
                         "this instant. Makes a RE-derive resumable, where --skip-existing cannot "
                         "be (every key already exists, so it would skip everything).")
    ap.add_argument("--skip-existing", action="store_true",
                    help="list existing <prefix>/ keys once and skip them (resumable multi-day run)")
    ap.add_argument("--smallest-first", action="store_true",
                    help="process sources in ascending entry count so whole sources go live early")
    ap.add_argument("--workers", type=int, default=1,
                    help="parallel derive+PUT workers (default 1 = the original serial path). "
                         "Measured 2026-07-29: cepii_gravity derives at 63 ms/series, so its "
                         "991,707 remaining objects are 17.4 h serial. Both halves of the work "
                         "release the GIL (pyarrow read, then the HTTPS PUT), so threads help.")
    ap.add_argument("--allow-stale-mirror", action="store_true",
                    help="derive even if the local parquet mirror is BEHIND R2. Only for a "
                         "deliberate rebuild from an older vintage — see the guard below.")
    a = ap.parse_args()

    # PREFLIGHT (ledger R383). This tool WRITES to R2 but READS through the econdl resolver,
    # which reads data/clean_full/ — the LOCAL mirror. Under AQUEDUCT_BACKEND=r2 that is a
    # scratch copy of whatever this machine last ran, so deriving from it can overwrite correct
    # served objects with OLDER data. Not hypothetical: on 2026-08-07 a re-derive of
    # stat_slovenia and hagstofa did exactly that (local 2,629 rows to 2024 against R2's 2,771
    # to 2025; local 1,884,485 rows against 2,222,916) while trying to FIX a staleness bug.
    #
    # The rule existed in prose and did not stop it, so it lives here, where it refuses.
    # Judged by CONTENT — row count and max observation date — never LastModified (upload time,
    # not change time) and never md5 (a re-encoded parquet differs with identical data). Both
    # of those proxies produced false verdicts the same day.
    if not a.dry_run and not a.allow_stale_mirror:
        behind = _mirror_behind_store(a.source)
        if behind:
            print("\nREFUSING TO DERIVE — the local parquet mirror is BEHIND R2 for:")
            for src, detail in behind:
                print(f"    {src}: {detail}")
            print("\nDeriving from it would overwrite correct served objects with older data "
                  "(R383).\nSync those parquets from R2 first, or pass --allow-stale-mirror if "
                  "you genuinely mean to publish the older vintage.")
            raise SystemExit(2)

    rows = _catalog_ids(a.limit, a.source)
    print(f"{len(rows):,} catalog series to derive")

    if a.dry_run:
        ok = miss = 0
        diffs = 0
        for sid, _src in rows:
            try:
                body = _series_csv_bytes(sid)
                ok += 1
            except Exception as e:  # store-coverage gaps error loudly, never silently skipped
                miss += 1
                print(f"  SKIP(unresolvable) {sid}: {str(e)[:80]}")
                continue
            if a.verify_shim:
                url = a.verify_shim.rstrip("/") + "/v1/series/" + urllib.parse.quote(sid, safe="") + ".csv"
                try:
                    shim = urllib.request.urlopen(url, timeout=15).read()
                    same = shim == body
                    diffs += 0 if same else 1
                    print(f"  {sid:42} {len(body):>8} B  shim-match={same}")
                except Exception as e:
                    print(f"  {sid:42} shim fetch failed: {str(e)[:60]}")
        print(f"DRY RUN: derived {ok}, unresolvable {miss}"
              + (f", shim byte-diffs {diffs}" if a.verify_shim else "")
              + " (no R2 contact)")
        return

    if not a.bucket:
        ap.error("--bucket is required for a real run")
    s3 = r2_util.client(write=True)

    if a.smallest_first:
        by_src: dict = {}
        for sid, src in rows:
            by_src.setdefault(src, []).append((sid, src))
        rows = [r for src in sorted(by_src, key=lambda s: len(by_src[s]))
                for r in by_src[src]]

    existing: set = set()
    if a.skip_newer_than:
        # RESUMABLE RE-DERIVE. --skip-existing is useless for a re-derive: the keys all exist
        # from the ORIGINAL derive, so it would skip everything and do nothing. But a re-derive
        # still has to survive an interruption — noaa's is ~14 hours over 3,135,873 series, and
        # the 2026-08-03 reboot threw away a third of one because there was no way to resume.
        #
        # The distinguishing fact is already on every object: LastModified. Anything rewritten
        # SINCE the campaign started is done; anything older still carries pre-restatement data.
        # Same single listing pass as --skip-existing, one extra comparison.
        cutoff = dt.datetime.fromisoformat(a.skip_newer_than.replace("Z", "+00:00"))
        listing_prefix = f"{a.prefix}/"
        if a.source and len(a.source) == 1:
            listing_prefix = f"{a.prefix}/{urllib.parse.quote(a.source[0] + ':', safe='')}"
        print(f"skip-newer-than {cutoff.isoformat()} scoped to {listing_prefix}", flush=True)
        tok = None
        seen = 0
        while True:
            kw = {"Bucket": a.bucket, "Prefix": listing_prefix, "MaxKeys": 1000}
            if tok:
                kw["ContinuationToken"] = tok
            resp = s3.list_objects_v2(**kw)
            for o in resp.get("Contents", []):
                seen += 1
                if o["LastModified"] >= cutoff:
                    existing.add(o["Key"])
            if not resp.get("IsTruncated"):
                break
            tok = resp.get("NextContinuationToken")
        print(f"skip-newer-than: {len(existing):,} of {seen:,} objects already re-derived "
              f"this campaign; the rest will be rewritten", flush=True)
    elif a.skip_existing:
        # Scope the listing to the source when exactly one is named. The unscoped
        # `series/` prefix spans every source (millions of objects), so a resume of one
        # source would spend its first many minutes paging through other sources' keys.
        # One scoped listing PER named source, not one unscoped listing when there are
        # several. The single-source case was already scoped; passing two --source flags
        # fell through to the bare `series/` prefix and paged all ~12.9M objects in the
        # bucket at 1,000 a call. Measured 2026-08-24: fifteen minutes in, zero CSVs
        # written, because it was still walking other sources' keys.
        prefixes = ([f"{a.prefix}/{urllib.parse.quote(src + ':', safe='')}" for src in a.source]
                    if a.source else [f"{a.prefix}/"])
        for listing_prefix in prefixes:
            print(f"skip-existing scoped to {listing_prefix}", flush=True)
            tok = None
            while True:
                kw = {"Bucket": a.bucket, "Prefix": listing_prefix, "MaxKeys": 1000}
                if tok:
                    kw["ContinuationToken"] = tok
                resp = s3.list_objects_v2(**kw)
                for o in resp.get("Contents", []):
                    existing.add(o["Key"])
                if not resp.get("IsTruncated"):
                    break
                tok = resp.get("NextContinuationToken")
        print(f"skip-existing: {len(existing):,} objects already in R2", flush=True)

    todo = []
    skip = 0
    for sid, src in rows:
        key = f"{a.prefix}/{urllib.parse.quote(sid, safe='')}.csv"
        if key in existing:
            skip += 1
            continue
        todo.append((sid, src, key))
    print(f"to derive: {len(todo):,}  (already present: {skip:,})", flush=True)

    up, miss = 0, 0
    if a.workers > 1:
        import threading
        from concurrent.futures import ThreadPoolExecutor, as_completed
        lock = threading.Lock()

        def work(item):
            sid, src, key = item
            try:
                body = _series_csv_bytes(sid)
            except Exception as e:                           # noqa: BLE001
                return ("miss", sid, str(e)[:80])
            _put_with_backoff(s3, a.bucket, key, body)
            return ("put", sid, None)

        # Chunked submission: 1M futures materialised at once would exhaust memory long
        # before the first one completed.
        CH = 20_000
        for start in range(0, len(todo), CH):
            chunk = todo[start:start + CH]
            with ThreadPoolExecutor(max_workers=a.workers) as ex:
                for fut in as_completed([ex.submit(work, it) for it in chunk]):
                    kind, sid, err = fut.result()
                    with lock:
                        if kind == "put":
                            up += 1
                        else:
                            miss += 1
                            print(f"  unresolvable {sid}: {err}", flush=True)
                        if (up + miss) % 5000 == 0:
                            print(f"  derived+put {up:,} (skip {skip:,}, miss {miss:,})...",
                                  flush=True)
    else:
        cur_src = None
        for sid, src, key in todo:
            if src != cur_src:
                if cur_src is not None:
                    print(f"  [source done] {cur_src} (running: put {up:,}, skip {skip:,})",
                          flush=True)
                cur_src = src
            try:
                body = _series_csv_bytes(sid)
            except Exception as e:                           # noqa: BLE001
                miss += 1
                print(f"  unresolvable {sid}: {str(e)[:80]}")
                continue
            _put_with_backoff(s3, a.bucket, key, body)
            up += 1
            if up % 500 == 0:
                print(f"  derived+put {up:,} (skip {skip:,}, miss {miss:,})...", flush=True)

    print(f"done: put {up:,} series CSVs, skipped {skip:,} existing, "
          f"{miss:,} unresolvable (store-coverage gaps)")


if __name__ == "__main__":
    main()
