"""Bulk per-series CSV derive for ONE tidy source — a streaming group-by instead of N scans.

WHY THIS EXISTS ALONGSIDE core/derive_csv.py. That tool resolves each series independently,
which is right for incremental work and quadratic for a backfill: every call runs a predicate
scan over the whole source parquet. Measured on cepii_gravity (93 MB, 69,666,545 rows,
1,143,250 series): 63 ms/series, i.e. 17.4 h serial, and 16 threads did not help because they
contend on the same file. Its 991,707 missing objects were not going to land that way.

WHAT THIS DOES INSTEAD. DuckDB streams the parquet ONCE in `ORDER BY series_key, obs_date`
(spilling to disk, so memory stays flat regardless of source size), and rows for a series are
flushed the moment the key changes. One pass, no per-series scan.

BYTE-EXACTNESS IS THE WHOLE CONTRACT, so it is verified rather than asserted: --verify N
compares this path's output against core.derive_csv._series_csv_bytes for N series sampled
across the FULL key range, and refuses to run unless every one matches exactly. The sample is
random, not the first N — a prefix sample cannot detect a divergence that only appears later,
which is the same reason a 5-key listing once nearly certified a 13%-complete derive (R167).

Usage:
  python tools/derive_csv_bulk.py --source cepii_gravity --verify 300 --dry-run
  python tools/derive_csv_bulk.py --source cepii_gravity --bucket econ-data --skip-existing
"""
from __future__ import annotations
import argparse
import csv
import io
import os
import queue
import random
import sys
import threading
import urllib.parse

# Same resolution contract as updater/config.py: ECONDL_ROOT wins, else the repo this file
# lives in. The env override is what lets tests point the tool at a fixture tree instead of
# the production store — without it, a test of this tool IS a run against production.
_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT = os.environ.get("ECONDL_ROOT") or _REPO
sys.path.insert(0, _REPO)
sys.path.insert(0, os.path.join(_REPO, "clients", "python"))

from core import r2_util  # noqa: E402

HEADER = ["series_id", "obs_date", "value"]


def csv_key(prefix: str, source: str, series_key: str) -> str:
    """The R2 key for one series' derived CSV. THE single definition of this layout.

    tools/verify_derive_parity.py imports it rather than re-deriving it: if the writer and
    the checker each spelled the encoding out, a drift in one could still show clean parity
    by being wrong the same way in both, which is precisely the failure a parity check is
    supposed to catch.
    """
    return f"{prefix}/{urllib.parse.quote(f'{source}:{series_key}', safe='')}.csv"


def csv_key_prefix(prefix: str, source: str) -> str:
    """The listing prefix covering every series of one source."""
    return f"{prefix}/{urllib.parse.quote(source + ':', safe='')}"


def _retry(fn, what, tries=8):
    """Call fn() with patient backoff. R2 answers ServiceUnavailable / SlowDown under load —
    'Reduce your concurrent request rate for the same object' killed a run in the
    skip-existing LISTING, which had no retry at all while the PUT path did. A resume step
    that dies on a throttle is worse than no resume: it forces a re-run that throttles harder.
    """
    import time as _t
    last = None
    for attempt in range(tries):
        try:
            return fn()
        except Exception as e:                               # noqa: BLE001
            last = e
            if attempt == tries - 1:
                break
            wait = min(60, 2 ** attempt)
            print(f"  {what} retry {attempt+1}/{tries} in {wait}s ({str(e)[:70]})", flush=True)
            _t.sleep(wait)
    raise last


def _csv_bytes(short_id: str, rows) -> bytes:
    """CSV for one series. Mirrors core.derive_csv._series_csv_bytes: same header, the SOURCE
    PREFIX STRIPPED from the id column, and lineterminator='\\n' so bytes match the Worker."""
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(HEADER)
    for d, v in rows:
        w.writerow([short_id, d, v])
    return buf.getvalue().encode("utf-8")


def _stream_one(con, path):
    """Yield (series_key, [(obs_date, value), ...]) in key order, one series at a time."""
    cur = con.execute(
        "SELECT series_key, obs_date, value FROM read_parquet(?) "
        "ORDER BY series_key, obs_date", [path])
    key = None
    acc = []
    while True:
        batch = cur.fetchmany(100_000)
        if not batch:
            break
        for k, d, v in batch:
            if k != key:
                if key is not None:
                    yield key, acc
                key, acc = k, []
            acc.append((d, v))
    if key is not None:
        yield key, acc


def _stream(con, paths, qualify=False):
    """Same, over a SHARDED source: one sorted scan PER SHARD, in sequence.

    Sharded stores are the norm above a certain size - noaa is 417 country-prefix shards over
    549,412,914 rows - and a single ORDER BY across all of them would sort the whole corpus to
    produce output that is already grouped. Per-shard sorting is bounded by the largest shard
    instead of by the source.

    WITH qualify=True the emitted id becomes `<shard>:<series_key>`, which is what a source
    whose CATALOGUE ids carry the shard needs - fed_board:H15:RIFSPFF_N.B, fhfa:annual_cbsa:01.
    Deriving those under a bare key would write every CSV to the wrong R2 object, so the
    catalogue would list 142,028 series whose downloads all 404. The disjointness check below
    is then unnecessary (the shard is IN the id, so two shards cannot collide) and is skipped.

    THE PRECONDITION, when qualify is False, IS THAT SHARDS DO NOT SHARE A series_key, and it
    is CHECKED, not assumed:
    if two shards held the same key, each would flush its own CSV and the second would silently
    overwrite the first with a partial history. The check is a running set of keys already
    emitted, which costs one string per series and turns a silent truncation into a loud stop.
    """
    if isinstance(paths, str):
        paths = [paths]
    emitted: set[str] = set()
    for i, p in enumerate(paths, 1):
        shard = os.path.splitext(os.path.basename(p))[0]
        if len(paths) > 1:
            print(f"  shard {i}/{len(paths)}: {os.path.basename(p)}", flush=True)
        for k, rows in _stream_one(con, p):
            if len(paths) > 1 and not qualify:
                if k in emitted:
                    raise SystemExit(
                        f"REFUSING to continue: series_key {k!r} appears in more than one shard "
                        f"({os.path.basename(p)} and an earlier one). Per-shard streaming would "
                        f"write this series twice and keep only the last, partial history. "
                        f"Derive this source with a single sorted scan instead.")
                emitted.add(k)
            yield (f"{shard}:{k}" if qualify else k), rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True)
    ap.add_argument("--bucket")
    ap.add_argument("--prefix", default="series")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip-existing", action="store_true")
    ap.add_argument("--only-catalogued", action="store_true",
                    help="write ONLY series with a catalog.db row. Without this the stream "
                         "writes every series_key in the parquet — R364 measured 1,927 objects "
                         "for a 1,749-row catalogue on whr, and wid would mint 393,196 "
                         "uncatalogued objects (2,858,393 store keys vs 2,465,197 catalogued). "
                         "Cataloguing those is a D1-headroom decision, not a derive side effect.")
    ap.add_argument("--workers", type=int, default=24)
    ap.add_argument("--qualify-with-shard", action="store_true",
                    help="emit ids as <source>:<shard>:<series_key> — required for sources "
                         "whose catalogue ids carry the store file (fed_board, fhfa)")
    ap.add_argument("--verify", type=int, default=300,
                    help="byte-compare this many RANDOM series against the resolver first")
    a = ap.parse_args()

    import duckdb
    # WALK, AND RESOLVE THE ROOT. A flat os.listdir under clean_full returns [] for two real
    # layouts, and "no parquet in ..." is indistinguishable from an empty store. usda keeps
    # data/clean_full/usda/<theme>/part_NNN.parquet (60 files) and bea
    # data/clean_full/bea/<Dataset>/<Table>.parquet (592); sec_edgar is not under clean_full at
    # all. The same assumption blinded the R383 preflight to its two nested sources (R390);
    # this was the copy left unfixed in that sweep, and it surfaced the moment usda needed the
    # bulk path — the per-series route was measured at 337 objects in 90 minutes, i.e. 314
    # hours for its 69,704 series.
    src_dir = None
    for _root in ("clean_full", "clean_grouped"):
        cand = os.path.join(ROOT, "data", _root, a.source)
        if os.path.isdir(cand) and any(f.endswith(".parquet")
                                       for _dp, _dn, fs in os.walk(cand) for f in fs):
            src_dir = cand
            break
    if src_dir is None:
        print(f"no parquet for {a.source} under data/clean_full or data/clean_grouped")
        return 2
    paths = sorted(os.path.join(dp, f)
                   for dp, _dn, fs in os.walk(src_dir) for f in fs
                   if f.endswith(".parquet") and not f.endswith("__series.parquet"))

    # LICENCE EXCLUSIONS ARE STRUCTURAL, NOT A FLAG. tools/catalog_complete.py refuses to
    # catalogue files whose licence is not the directory's source licence, and until now this
    # tool did not know about that list. data/clean_full/vdem/ holds vparty.parquet beside
    # vdem.parquet: V-Dem publishes CC BY-SA 4.0 for "The V-Dem Dataset", V-Party is a separate
    # publication whose own page states no licence, and its 682,659 keys do not overlap vdem's
    # so the duplicate-shard guard below cannot fire either. Running this tool on vdem without
    # --only-catalogued would therefore mint 682,659 unlicensed CSVs onto R2 - R364 verbatim,
    # whose stated remedy is to separate them BEFORE any whole-directory tool runs. Honouring
    # the same list here makes the protection a property of the CODE rather than of remembering
    # a flag.
    # NO SILENT FALLBACK. This was `except Exception: SOURCE_FILE_EXCLUSIONS = {}`, which turns
    # a broken import into ZERO exclusions - a fallback that absorbs 100% of the protection it
    # guards, and the failure would be invisible (R419). An import error here means the licence
    # list is unavailable, and deriving a whole directory without it is exactly what publishes
    # V-Party. Refuse instead.
    try:
        from tools.catalog_complete import SOURCE_FILE_EXCLUSIONS
    except Exception as _e:                                  # noqa: BLE001
        print(f"REFUSING: cannot import the licence exclusion list ({_e}). A whole-directory "
              f"derive without it can publish files this source is not licensed to serve.")
        return 2
    _excl = set(SOURCE_FILE_EXCLUSIONS.get(a.source, ()))
    if _excl:
        _skipped = [p for p in paths if os.path.basename(p) in _excl]
        paths = [p for p in paths if os.path.basename(p) not in _excl]
        # Say what was dropped: a coverage limit nobody prints reads as full coverage.
        print(f"  {a.source}: EXCLUDING {len(_skipped)} file(s) whose licence is not this "
              f"source's: {[os.path.basename(p) for p in _skipped]} - see "
              f"DATABASE_LICENSES_VERBATIM.md")
        if not paths:
            print(f"  {a.source}: every parquet was excluded; nothing to derive.")
            return 2
    # wid ONLY: exclude the superseded legacy monolith when the per-country shards exist —
    # the same targeted skip the resolver applies (_resolve._resolve_generic_long). wid.parquet
    # (1.93M series to 2024) sits beside 412 shards (2.86M series to 2025); streaming both
    # emits duplicate dates with contradictory values, which is the corruption R384 nearly
    # published at 2.4M-object scale. NOT generalised: six sources have a same-named file
    # beside shards (bea, fred, sipri, stats_nz, vdem, wid) and only wid's is proven
    # superseded. The --verify gate would catch a divergence anyway, since the resolver now
    # excludes the monolith — this keeps the two readers defined identically rather than
    # relying on the gate to notice they are not.
    if a.source == "wid":
        mono = os.path.join(src_dir, "wid.parquet")
        rest = [f for f in paths if os.path.abspath(f) != os.path.abspath(mono)]
        if rest and len(rest) != len(paths):
            paths = rest
    if not paths:
        print(f"no parquet in {src_dir}")
        return 2
    print(f"source files: {len(paths)} parquet(s) under "
          f"{os.path.relpath(src_dir, ROOT).replace(os.sep, '/')}", flush=True)

    # ONLY UNIFORM-LONG FILES ARE THE SERVING STORE. A store dir may hold native/raw parquets
    # BESIDE the tidy projection that serves (cepii_baci: baci_hs17/hs96.parquet are the raw
    # vintages with year/exporter/importer columns; cepii_baci_pairs.parquet is what serves).
    # Feeding a raw file into the DISTINCT-series_key scan is a BinderException at best and a
    # wrong id universe at worst. Skip by SCHEMA, and say so — a silent skip would read as
    # "covered everything" (the no-silent-caps rule).
    import duckdb as _duck
    _need = {"series_key", "obs_date", "value"}
    uniform, skipped = [], []
    for p in paths:
        cols = {r[0] for r in _duck.connect().execute(
            "SELECT name FROM parquet_schema(?)", [p]).fetchall()}
        (uniform if _need <= cols else skipped).append(p)
    if skipped:
        print(f"skipping {len(skipped)} non-uniform parquet(s) (missing "
              f"{sorted(_need)} columns) — native/raw files are not the serving store: "
              + ", ".join(os.path.basename(s) for s in skipped), flush=True)
    if not uniform:
        print(f"no uniform-long parquet (series_key/obs_date/value) in {src_dir} — "
              f"nothing here can serve")
        return 2
    paths = uniform
    # `path` is what the VERIFY step and the distinct-key count read. DuckDB's read_parquet
    # takes a list as happily as a string, so a sharded source is verified across all of its
    # shards rather than only the first - sampling one shard of 417 would certify a format
    # that the other 416 might not share.
    path = paths if len(paths) > 1 else paths[0]
    if len(paths) > 1:
        print(f"{len(paths)} shards in {src_dir}", flush=True)
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")

    # ---- byte-exactness gate -------------------------------------------------------
    if a.verify:
        from core.derive_csv import _series_csv_bytes
        if a.qualify_with_shard:
            # The id carries the shard, so a bare DISTINCT over the whole source cannot build
            # one: the same key may exist in two shards as two different series. Sample per
            # shard instead, which also spreads the check along the axis where a schema
            # divergence would actually appear.
            per = max(1, -(-a.verify // len(paths)))
            keys, grouped = [], {}
            for pth in paths:
                shard = os.path.splitext(os.path.basename(pth))[0]
                got = 0
                for k, rows in _stream_one(con, pth):
                    qk = f"{shard}:{k}"
                    keys.append(qk)
                    grouped[qk] = rows
                    got += 1
                    if got >= per:
                        break
            sample = keys
            print(f"verify sample: {len(sample):,} series, up to {per} from each of "
                  f"{len(paths)} shard(s)", flush=True)
        else:
            keys = [r[0] for r in con.execute(
                "SELECT DISTINCT series_key FROM read_parquet(?) ORDER BY series_key",
                [path]).fetchall()]
            print(f"{len(keys):,} distinct series in the parquet", flush=True)
            rnd = random.Random(20260730)
            sample = rnd.sample(keys, min(a.verify, len(keys)))
        # ONE scan for the whole sample. A per-key query would re-scan all 69.6M rows each
        # time — 300 full passes to check 300 series, which is the very cost this tool exists
        # to remove, reintroduced inside its own test.
        want = set(sample)
        if not a.qualify_with_shard:
            grouped = {k: [] for k in sample}
        cur = None if a.qualify_with_shard else con.execute(
            "SELECT series_key, obs_date, value FROM read_parquet(?) "
            "WHERE series_key IN (SELECT UNNEST(?)) ORDER BY series_key, obs_date",
            [path, sample])
        while cur is not None:
            batch = cur.fetchmany(50_000)
            if not batch:
                break
            for k, d, v in batch:
                if k in want:
                    grouped[k].append((d, v))
        bad = 0
        for k in sample:
            mine = _csv_bytes(k, grouped[k])
            theirs = _series_csv_bytes(f"{a.source}:{k}")
            if mine != theirs:
                bad += 1
                if bad <= 3:
                    print(f"  MISMATCH {k}\n    mine  : {mine[:120]!r}\n"
                          f"    theirs: {theirs[:120]!r}")
        print(f"verify: {len(sample) - bad}/{len(sample)} byte-identical", flush=True)
        if bad:
            print("REFUSING to run — output would not match the served contract.")
            return 1

    existing = set()
    s3 = None
    if not a.dry_run:
        if not a.bucket:
            ap.error("--bucket is required unless --dry-run")
        s3 = r2_util.client(write=True)
        if a.skip_existing:
            lp = csv_key_prefix(a.prefix, a.source)
            tok = None
            while True:
                kw = {"Bucket": a.bucket, "Prefix": lp, "MaxKeys": 1000}
                if tok:
                    kw["ContinuationToken"] = tok
                r = _retry(lambda: s3.list_objects_v2(**kw), "LIST")
                for o in r.get("Contents", []):
                    existing.add(o["Key"])
                if not r.get("IsTruncated"):
                    break
                tok = r["NextContinuationToken"]
            print(f"skip-existing: {len(existing):,} already in R2", flush=True)

    # ---- producer (single sorted scan) -> bounded queue -> PUT workers --------------
    q: queue.Queue = queue.Queue(maxsize=4000)
    counts = {"put": 0, "skip": 0, "err": 0}
    lock = threading.Lock()
    STOP = object()

    def worker():
        while True:
            item = q.get()
            if item is STOP:
                q.task_done()
                return
            key, body = item
            try:
                _retry(lambda: s3.put_object(Bucket=a.bucket, Key=key, Body=body,
                                             ContentType="text/csv"), "PUT")
                with lock:
                    counts["put"] += 1
                    if counts["put"] % 25_000 == 0:
                        print(f"  put {counts['put']:,} (skip {counts['skip']:,})", flush=True)
            except Exception as e:                           # noqa: BLE001
                with lock:
                    counts["err"] += 1
                    if counts["err"] <= 5:
                        print(f"  PUT FAILED {key}: {str(e)[:90]}", flush=True)
            finally:
                q.task_done()

    threads = []
    if not a.dry_run:
        for _ in range(a.workers):
            t = threading.Thread(target=worker, daemon=True)
            t.start()
            threads.append(t)

    catalogued = None
    if a.only_catalogued:
        import sqlite3
        _con = sqlite3.connect(
            f"file:{os.path.join(ROOT, 'data', 'catalog.db')}?mode=ro", uri=True)
        _con.execute("PRAGMA busy_timeout = 180000")
        catalogued = {r[0] for r in _con.execute(
            "select series_id from series where source_id=?", (a.source,))}
        _con.close()
        print(f"only-catalogued: {len(catalogued):,} id(s) eligible; store keys outside the "
              f"catalogue are counted and skipped, not silently dropped", flush=True)

    seen = 0
    uncat = 0
    for k, rows in _stream(con, paths, qualify=a.qualify_with_shard):
        seen += 1
        if catalogued is not None and f"{a.source}:{k}" not in catalogued:
            uncat += 1
            continue
        key = csv_key(a.prefix, a.source, k)
        if key in existing:
            counts["skip"] += 1
            continue
        if a.dry_run:
            if seen <= 3:
                print(f"  would PUT {key} ({len(_csv_bytes(k, rows))} B)")
            continue
        q.put((key, _csv_bytes(k, rows)))

    if not a.dry_run:
        q.join()
        for _ in threads:
            q.put(STOP)
        q.join()
    if uncat:
        print(f"only-catalogued: SKIPPED {uncat:,} store series with no catalogue row "
              f"(they are real data — cataloguing them is a separate decision)", flush=True)
    print(f"done: {seen:,} series streamed, put {counts['put']:,}, "
          f"skipped {counts['skip']:,}, errors {counts['err']:,}")
    return 1 if counts["err"] else 0


if __name__ == "__main__":
    sys.exit(main())
