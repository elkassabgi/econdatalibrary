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


class TooLarge(Exception):
    """This series' native store is too big to project in memory at the current limit."""


# Set from --max-rows. 0 disables. A series whose STORE FILE holds more rows than this is
# skipped and NAMED, never attempted.
#
# WHY: _series_csv_bytes builds the whole CSV in a StringIO before returning bytes, so peak
# memory is the Arrow table plus a pandas frame plus the full CSV string. Measured
# 2026-08-24 on the cbs_nl/gus_dbw publish: one worker on gus_dbw's 358M-row area_16 held
# 118 GB RSS, roughly six times the CSV it was producing. cbs_nl's 37824 is 1,886,692,500
# rows - about 600 GB by that ratio, on a 382 GB machine, with four workers able to start
# several giants at once. That does not fail politely; it thrashes the box and takes the
# crawlers with it.
#
# 11 tables (0.2%) are over 100M rows. Skipping them costs 0.2% of the tables and protects
# the 99.8% that derive cleanly, which is the opposite trade from letting the run die.
_MAX_ROWS = 0
_ALLOW_STREAM = False   # see _derive_and_put
_STREAM_MAX = 0        # upper bound for the streaming path; 0 = none



def _duck_spill_dir() -> str:
    """A spill directory private to this process (see the collision note in the sorter).

    Lives beside the store rather than in %TEMP%: an external sort of a billion-row table
    can spill tens of GB, and %TEMP% is on the small drive.
    """
    base = os.environ.get("ECONDL_DUCKDB_TMP")
    if not base:
        repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        base = os.path.join(repo, "data", "_duckdb_spill")
    d = os.path.join(base, "pid%d" % os.getpid())
    os.makedirs(d, exist_ok=True)
    return d.replace("\\", "/").replace("'", "''")


def _series_csv_to_file_sorted(series_id: str, out_path: str) -> int:
    """Write one FILE-GRAIN series to a gzipped CSV, globally sorted, bounded memory.

    DuckDB does the whole job in C++: read the parquet, ORDER BY (spilling to disk), and
    WRITE THE CSV. Nothing crosses into Python, which matters twice over:

      SPEED  cbs_nl:71892ned (599,257,804 bytes) takes 4s here against 50s when the same
             sorted rows were pulled through Arrow and formatted row-by-row in Python.
      SAFETY the Arrow path SEGFAULTED (exit 139) on large sorts - it killed five of six
             derive processes and a standalone test before this replaced it. COPY never
             materialises a batch on the Python side, so there is nothing to crash.

    The CSV is then gzipped in a streaming pass with mtime=0 and no filename in the header,
    so the object matches `core.r2_util.gzip_bytes(csv)` exactly.

    It used to say "matches gzip.compress(csv, mtime=0)", and that was portable only by
    accident. GzipFile writes the header's OS byte as 255 on every Python; gzip.compress leaves
    it at zlib's build platform on 3.11 (3 on the Linux runners) and forces 255 on 3.14. So
    this streaming path and the in-memory path produced DIFFERENT objects for the same CSV
    whenever CI wrote one of them. `gzip_bytes` normalises that byte, so the two agree again.

    FILE-GRAIN ONLY: res.predicate is a pyarrow Expression that cannot be handed to SQL. For
    file-grain the whole file is the series set, so "key IS NOT NULL" is the same filter.
    Anything else keeps the in-memory path rather than have its predicate silently dropped.
    """
    from econdl import _resolve                                      # noqa: PLC0415
    import duckdb as _ddb                                            # noqa: PLC0415
    import gzip as _gzip                                             # noqa: PLC0415
    import shutil as _shutil                                         # noqa: PLC0415
    import tempfile as _tf                                           # noqa: PLC0415
    res = _resolve.resolve(series_id)
    if res.dedup_on or res.stamp_id or not res.tidy_ok:
        raise ValueError(f"{series_id}: not eligible for sorted streaming")
    src = _duck_source(res)          # one quoted path, or a DuckDB list of them
    key = res.key_col.replace('"', '""')
    fd, plain = _tf.mkstemp(suffix=".csv", prefix="ddb_")
    os.close(fd)
    plain_q = plain.replace("\\", "/").replace("'", "''")
    try:
        con = _ddb.connect()
        # Deliberately small so many sorts can run at once without their limits summing
        # past the machine, and so each spills to disk rather than pushing its in-memory
        # sort as far as it can.
        con.execute("SET memory_limit='3GB'")
        con.execute("PRAGMA threads=2")
        con.execute("SET preserve_insertion_order=false")
        # Every connection MUST get its OWN spill directory. DuckDB names its external-sort
        # spill file `duckdb_temp_storage_DEFAULT-<n>.tmp`, and that name is identical in
        # every process, so pointing them all at one shared temp dir makes concurrent sorts
        # fight over the same file: the loser raises "Access is denied" on Windows, and the
        # same collision inside one multi-threaded process is a native crash with no
        # traceback. That crash is what cost hours of wrong guesses at memory limits, the
        # S3 client and the table itself - none of which were ever the cause.
        con.execute("SET temp_directory='%s'" % _duck_spill_dir())
        con.execute(
            f'COPY (SELECT CAST("{key}" AS VARCHAR) AS series_id, obs_date, value '
            f"FROM read_parquet({src}) WHERE \"{key}\" IS NOT NULL "
            f"ORDER BY series_id, obs_date) "
            f"TO '{plain_q}' (FORMAT CSV, HEADER, DELIMITER ',')")
        con.close()
        if os.path.getsize(plain) == 0:
            raise _ResolveZero(f"{series_id}: zero rows")
        with open(plain, "rb") as fin, open(out_path, "wb") as raw,                 _gzip.GzipFile(fileobj=raw, mode="wb", mtime=0, filename="") as gz:
            _shutil.copyfileobj(fin, gz, length=8 * 1024 * 1024)
    finally:
        try:
            os.remove(plain)
        except OSError:
            pass
    return os.path.getsize(out_path)


def _series_csv_to_file(series_id: str, out_path: str) -> int:
    """Stream one series' CSV to a file. Returns the byte size written.

    THE MEMORY-BOUNDED PATH. _series_csv_bytes materialises the Arrow table, a pandas
    frame AND the whole CSV string before it can return bytes, so peak RSS runs about six
    times the finished CSV — measured 2026-08-24 at 118 GB for a 358M-row table. That
    makes the biggest tables unservable on any machine: cbs_nl's 37824 is 1,886,692,500
    rows, and 11 tables holding 4.42 BILLION rows sit above the safe in-memory bound.
    Dropping them would mean discarding half the data this publish exists to serve.

    Reading in record batches and writing each before fetching the next keeps peak memory
    at one batch, so size stops mattering. Same writer, same lineterminator and column
    order as the in-memory path, so the bytes are identical to what the Worker and the dev
    shim expect.

    NOT for sources needing dedup: res.dedup_on drops duplicates ACROSS the whole table,
    which a per-batch pass cannot see. Those keep the in-memory path.
    """
    from econdl import _resolve
    import pyarrow as pa                                             # noqa: PLC0415
    import pyarrow.dataset as pads                                   # noqa: PLC0415
    res = _resolve.resolve(series_id)
    if res.dedup_on:
        raise ValueError(f"{series_id}: dedup_on set; cannot stream (needs the whole table)")
    if res.stamp_id:
        raise ValueError(f"{series_id}: stamp_id set; cannot stream (identity is per-file)")
    # Written already-GZIPPED. _put_with_backoff compresses in memory, which would undo
    # the whole point of streaming; mtime=0 and the default level keep the bytes identical
    # to gzip.compress(csv, mtime=0), which verify_source_served byte-compares against.
    import gzip as _gzip                                             # noqa: PLC0415
    rows_seen = 0
    # filename="" matters: GzipFile(path) stores the FNAME in the header and
    # gzip.compress() does not, a consistent 18-19 byte difference that would fail the
    # byte-compare in verify_source_served even though the CSV inside is identical.
    with open(out_path, "wb") as raw,             _gzip.GzipFile(fileobj=raw, mode="wb", mtime=0, filename="") as gz,             io.TextIOWrapper(gz, encoding="utf-8", newline="") as fh:
        w = csv.writer(fh, lineterminator='\n')
        header = False
        # ORDER MUST MATCH THE IN-MEMORY PATH. dataset.to_batches() reads fragments in
        # parallel, so its row order differs from to_table()'s - the streamed CSV then holds
        # exactly the same LINES in a different sequence. Measured 2026-08-24 on
        # cbs_nl:71892ned: identical byte count (599,257,804), identical line count
        # (5,536,832), set(lines) equal, and the two files still differ from line 1. The data
        # is right and the bytes are wrong, which is the shape that quietly fails
        # verify_source_served's byte-compare forever.
        _scanner = pads.Scanner.from_dataset(
            pads.dataset(res.parquet_path), filter=res.predicate,
            batch_size=131_072, use_threads=False)
        for batch in _scanner.to_batches():
            if batch.num_rows == 0:
                continue
            tbl = pa.Table.from_batches([batch])
            if res.tidy_ok:
                df = _resolve.native_to_tidy(res, tbl)
                if not header:
                    w.writerow(["series_id", "obs_date", "value"]); header = True
                for sid, _src, d, v in df[["series_id", "source", "obs_date", "value"]].itertuples(index=False):
                    w.writerow([sid, d, v])
            else:
                cols = tbl.column_names
                if not header:
                    w.writerow(cols); header = True
                for row in tbl.to_pylist():
                    w.writerow([row.get(c) for c in cols])
            rows_seen += batch.num_rows
    if rows_seen == 0:
        raise _ResolveZero(f"{series_id}: zero rows matched")
    return os.path.getsize(out_path)


class _ResolveZero(Exception):
    """Streamed a series and found no rows — same meaning as read_native's zero-row error."""


def _derive_and_put(s3, bucket: str, key: str, series_id: str) -> None:
    """Derive one series and PUT it, choosing the memory-safe path by table size.

    Small tables go through _series_csv_bytes, which is what every other source has
    always used. Anything above _MAX_ROWS streams to a temp file and uploads the file
    handle, so peak memory is one record batch instead of six times the finished CSV.

    Measured on gus_dbw's 358M-row area_16: 118 GB resident in memory versus 1.8 GB
    streamed, byte-for-byte the same output (verified on three tables before this was
    wired in). That difference is what makes cbs_nl's 1,886,692,500-row 37824 servable
    at all rather than something the machine dies on.
    """
    # STREAMING IS OFF BY DEFAULT AND MUST STAY OFF UNTIL IT SORTS.
    # native_table_to_tidy ends with .sort_values(["series_id","obs_date"]), so the contract
    # CSV is globally sorted. A per-batch writer sorts each batch alone, which yields the
    # same LINES in a different sequence: measured 2026-08-24 on cbs_nl:71892ned, identical
    # 599,257,804 bytes, identical 5,536,832 lines, set(lines) equal, and still different
    # from line 1. Data correct, bytes wrong - the shape that fails verify_source_served's
    # byte-compare forever while looking fine.
    # use_threads=False does NOT fix it; the reordering is the missing GLOBAL sort, not
    # fragment parallelism. Streaming needs an external sort (DuckDB ORDER BY spilling to
    # disk) before it can be trusted, so until then a too-large table is SKIPPED and named,
    # never silently mis-ordered.
    try:
        body = _series_csv_bytes(series_id)
    except TooLarge:
        if not _ALLOW_STREAM:
            raise                                # skipped and NAMED by the caller
        if _STREAM_MAX:
            import pyarrow.parquet as _pq2                           # noqa: PLC0415
            from econdl import _resolve as _rs                       # noqa: PLC0415
            _p = _rs.resolve(series_id).parquet_path
            if isinstance(_p, str):
                _n = _pq2.read_metadata(_p).num_rows
                if _n > _STREAM_MAX:
                    raise TooLarge(f"{_n:,} rows > --stream-max-rows {_STREAM_MAX:,}; "
                                   f"a single CSV this size is not a usable download")
        # SORTED streaming only. _series_csv_to_file (unsorted, per-batch) writes the right
        # rows in the wrong order (R466); _series_csv_to_file_sorted pushes the ORDER BY into
        # DuckDB, which spills to disk, so the batches arrive already globally sorted.
        # Byte-verified against _series_csv_bytes on cbs_nl:71892ned (599 MB),
        # 70962ned (862 MB) and 81455NED (2.87 GB).
        import tempfile                                              # noqa: PLC0415
        fd, tmp = tempfile.mkstemp(suffix=".csv.gz", prefix="derive_")
        os.close(fd)
        try:
            _series_csv_to_file_sorted(series_id, tmp)
            _put_gzip_file_with_backoff(s3, bucket, key, tmp)
        finally:
            try:
                os.remove(tmp)
            except OSError:
                pass
        return
    _put_with_backoff(s3, bucket, key, body)


def resolved_paths(res) -> list:
    """Every parquet file a resolution covers, as a list, whatever shape it arrived in.

    ONE ANSWER FOR THREE CALLERS. `Resolution.parquet_path` may be a file, a directory or (for
    a partitioned source) a list, and each consumer used to decide for itself: DuckDB got
    `str(path)`, pyarrow got the raw value, and the MAX_ROWS ceiling checked `isinstance(...,
    str)` and silently skipped anything else. Three readings of one field is how a ceiling
    stops applying without anyone noticing (R658).
    """
    p = res.parquet_path
    if isinstance(p, (list, tuple)):
        return [str(x) for x in p]
    p = str(p)
    if os.path.isdir(p):
        import glob as _glob                                          # noqa: PLC0415
        return sorted(_glob.glob(os.path.join(p, "**", "*.parquet"), recursive=True))
    return [p]


def _duck_source(res) -> str:
    """The `read_parquet(...)` argument for a resolution: one quoted path, or a LIST of them.

    DuckDB accepts `read_parquet(['a','b'])`, so a partitioned series needs no glob and no
    temporary view - but it does need the list built here rather than by str() on a Python
    list, which yields `['a', 'b']` with Python quoting and fails to parse.
    """
    paths = [p.replace("\\", "/").replace("'", "''") for p in resolved_paths(res)]
    if len(paths) == 1:
        return "'%s'" % paths[0]
    return "[%s]" % ", ".join("'%s'" % p for p in paths)


def _series_csv_bytes(series_id: str) -> bytes:
    """Project one series to CSV bytes via the econdl resolver (the contract shape)."""
    from econdl import _resolve
    res = _resolve.resolve(series_id)
    if _MAX_ROWS:
        # SUM THE PARTS. The old gate was `isinstance(res.parquet_path, str)`, so a list or a
        # directory skipped the ceiling altogether - the check stopped applying at exactly the
        # shape that means MORE data (R658 F3). An unreadable footer still contributes 0, so an
        # unknown size cannot block a series that would otherwise derive.
        n = 0
        try:
            import pyarrow.parquet as _pq
            for _p in resolved_paths(res):
                n += _pq.read_metadata(_p).num_rows
        except Exception:
            n = n or 0
        if n > _MAX_ROWS:
            raise TooLarge(f"{n:,} rows > {_MAX_ROWS:,}; use the streaming path")
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


# How many uploads the identical-object check avoided this process. Printed by callers that
# report a total, so the saving is observed rather than assumed.
_SKIPPED_IDENTICAL = [0]



def _file_md5(path: str) -> str:
    """MD5 of a file, streamed. Matches R2's ETag for a single-part object."""
    import hashlib                                                   # noqa: PLC0415
    h = hashlib.md5()                                                # noqa: S324
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(4 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def object_is_identical(s3, bucket: str, key: str, path: str) -> bool:
    """True only when R2 already holds EXACTLY these bytes.

    WHY THIS IS SAFE TO TRUST. `sorted_csv_gz` gzips with mtime=0 and no filename, and every
    in-memory writer goes through `core.r2_util.gzip_bytes`, so the same data gives the same
    bytes ON EVERY MACHINE - hence the same MD5, which R2 reports as the ETag of a single-part
    object.

    "On every machine" is the part that was not true until 2026-09-02. The header's OS byte
    came from zlib's build platform under Python 3.11, which CI pins, so a key written by a
    Linux runner could never be recognised as identical by the desktop, and the two took turns
    re-uploading it. Nothing that decompresses ever noticed.

    WHY IT REFUSES TO GUESS. A multipart ETag looks like `<hex>-<n>` and is a digest of part
    digests, not of the content, so it cannot be compared and those objects upload. So does any
    head that errors, lacks an ETag, or disagrees on size. Every uncertain case falls toward
    UPLOADING: a wasted class-A operation costs $0.0000045, and a stale served object costs a
    user the wrong answer.

    MEASURED REASON TO EXIST: 7,686,397 of 10.8 M class-A operations in 24 days are PutObject on
    econ-data, ~320,000 a day, on a publish path that never compared anything.
    """
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
    except Exception:                                                # noqa: BLE001
        return False                                                 # absent, or cannot ask
    etag = str(head.get("ETag") or "").strip('"')
    if not etag or "-" in etag:
        return False                                                 # multipart: not comparable
    try:
        if head.get("ContentLength") != os.path.getsize(path):
            return False                                             # cheap disagreement first
        return etag == _file_md5(path)
    except OSError:
        return False

def _put_gzip_file_with_backoff(s3, bucket, key, path, metadata=None,
                                skip_identical=True) -> None:
    """PUT an ALREADY-GZIPPED file by streaming it, same backoff as the in-memory put.

    The file is reopened on every attempt. Passing one handle would upload zero bytes on
    any retry, because the first attempt leaves it at EOF - a silent empty object, which
    is worse than the transient error that caused the retry.

    SKIPS AN IDENTICAL OBJECT. 71% of this account's class-A operations are PutObject on
    econ-data - 7,686,397 of 10.8 M over 24 days - on a path that never compared anything, so
    a source that marks every series changed republishes identical bytes daily. The check is
    an exact MD5-vs-ETag match and refuses to guess; see `object_is_identical`. Pass
    skip_identical=False to force the write.
    """
    import time as _time                                             # noqa: PLC0415
    if skip_identical and object_is_identical(s3, bucket, key, path):
        _SKIPPED_IDENTICAL[0] += 1
        return
    for attempt in range(7):
        try:
            with open(path, "rb") as fh:
                s3.put_object(Bucket=bucket, Key=key, Body=fh, Metadata=(metadata or {}), ContentType="text/csv",
                              ContentEncoding="gzip")
            return
        except Exception as e:                               # noqa: BLE001
            if attempt == 6:
                raise
            wait = 2 ** attempt
            print(f"  PUT(stream) retry {attempt+1}/7 in {wait}s ({str(e)[:70]})", flush=True)
            _time.sleep(wait)


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
    import time as _time
    from .r2_util import gzip_bytes
    if isinstance(body, str):
        body = body.encode()
    # gzip_bytes, not gzip.compress: on Python 3.11 (what CI pins) the header's OS byte comes
    # from zlib's build platform and is 3 on Linux; on 3.14 (the desktop) gzip.compress forces
    # it to 255. The same CSV therefore becomes two different objects depending on where this
    # ran, which defeats every digest comparison downstream.
    body = gzip_bytes(body)
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


def content_fingerprint_sql(cols, path: str) -> str:
    """DuckDB query returning a DATA-level fingerprint of a parquet file.

    Order-independent (an aggregate over per-row hashes) and independent of how the file was
    WRITTEN, which is the whole point: the desktop writes with pyarrow 23.0.0 and CI with
    25.0.1, so byte comparison and md5 both flag a pure re-encode as a difference while missing
    a real one. R383 rejected hashes for that reason; this fingerprints the values instead.

    `sum` rather than `bit_xor` because XOR cancels in pairs — two identical duplicate rows
    would disappear from the fingerprint entirely.

    `list_value`, NOT `concat_ws`. DuckDB's concat_ws SKIPS nulls, so it flattens distinct rows
    onto one string: concat_ws('|','a',NULL,'b'), concat_ws('|','a','b',NULL) and
    concat_ws('|','a|b',NULL) all produce 'a|b'. That is not theoretical — on the live
    wikidata/companies.parquet, swapping the two nullable columns `inception` and `website`
    changes 4,233 of 19,219 rows and left the concat_ws fingerprint IDENTICAL. Several sources
    (ember, faostat, fhfa, penn_world_table) also carry '|' inside series_key, which was the
    delimiter. Hashing a LIST keeps nulls as elements and needs no delimiter at all.
    """
    expr = ", ".join('"%s"::VARCHAR' % c.replace('"', '""') for c in cols)
    return (f"select sum(hash(list_value({expr}))::HUGEINT) "
            f"from read_parquet('{path}')")


def _mirror_behind_store(sources, sample: int = 0):
    """[(source, detail)] for sources whose LOCAL parquets hold less than R2's.

    Compared by row count and max observation date, and — when those two TIE — by a data-level
    content fingerprint, because a publisher revision rewrites values while leaving both
    unchanged and is invisible to any shape test (R549: three eurostat flows served superseded
    values, one of them headline real GDP growth). See the note at the call site for why
    timestamps and byte hashes are both wrong here. A single behind file is enough to refuse,
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

    # CONTENT FINGERPRINT, for the case row count and max date CANNOT see: a publisher
    # REVISION, which rewrites values in place. On 2026-09-01 three eurostat flows were served
    # at a superseded vintage — TEC00115 (real GDP growth) had 11 revised values with identical
    # size, identical row count and identical max date, so every shape-based test cleared it
    # while users downloaded the old numbers (R549).
    #
    # Data-level, not byte-level, which is what makes it safe here: the desktop writes with
    # pyarrow 23.0.0 and CI with 25.0.1, so a pure re-encode of identical data changes the
    # file's bytes and its md5 while changing nothing that matters. R383 rejected timestamps
    # and hashes for exactly that reason and settled on rows+dates; this keeps R383's objection
    # satisfied and closes the gap it left.
    #
    # sum() rather than bit_xor(): XOR cancels in pairs, so two identical duplicate rows would
    # vanish from the fingerprint.
    FP_MAX_ROWS = 5_000_000

    def fingerprint(path, n_rows):
        if n_rows > FP_MAX_ROWS:
            return None
        p = path.replace(os.sep, "/")
        cols = [r[0] for r in q.execute(
            f"describe select * from read_parquet('{p}')").fetchall()]
        return q.execute(content_fingerprint_sql(cols, p)).fetchone()[0]

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
        unreadable = 0
        for f in random.Random(0).sample(files, k):
            rp = os.path.join(tmp, "r.parquet")
            try:
                # Same root on both sides — a clean_grouped source must be compared against
                # r2://econ-data/clean_grouped/, not clean_full, or every object 404s and this
                # whole check becomes a silent pass.
                s3.download_file("econ-data", f"{store_root}/{src}/{f}", rp)
                ln, lmx = stats(os.path.join(d, *f.split("/")))
                rn, rmx = stats(rp)
            except Exception as _e:                                   # noqa: BLE001
                # NEVER SILENTLY. This was a bare `continue`, in the guard that authorises a
                # WRITE: a source whose objects all 404 (wrong root, renamed prefix, expired
                # credentials) produced a clean preflight having compared nothing at all. Say
                # which file and why, count it, and refuse below if the sample was mostly
                # unreadable — a check that could not read its own sample has not checked.
                unreadable += 1
                print(f"[preflight] {src}/{f}: NOT COMPARED ({type(_e).__name__}: "
                      f"{str(_e)[:70]})", flush=True)
                continue
            if rn > ln or (rmx and lmx and str(rmx) > str(lmx)):
                out.append((src, f"{f}: local {ln:,} rows/{lmx} vs R2 {rn:,} rows/{rmx}"))
                break
            if rn == ln and str(rmx) == str(lmx):
                # Same shape on both sides. That is where a REVISION hides, so this is the one
                # case worth paying a content read for (R549).
                fp_err = None
                try:
                    lfp = fingerprint(os.path.join(d, *f.split("/")), ln)
                    rfp = fingerprint(rp, rn)
                except Exception as _e:                               # noqa: BLE001
                    lfp = rfp = None
                    fp_err = _e
                if fp_err is not None:
                    # FAIL CLOSED, and say what actually happened. This branch used to fall
                    # through to the cap message — so a schema the query could not read (a
                    # source with no date column raised IndexError) printed "over the
                    # 5,000,000-row cap", which was false, and then let the derive PROCEED.
                    # The tool that gates a WRITE must not be the one that fails open.
                    out.append((src, f"{f}: content check FAILED "
                                     f"({type(fp_err).__name__}: {str(fp_err)[:60]}) — cannot "
                                     f"prove the mirror is level"))
                    break
                if lfp is None or rfp is None:
                    # NEVER silently. A file too large to fingerprint is a file this guard did
                    # not fully check, and saying so is the difference between a bounded check
                    # and one that reads as coverage it does not have.
                    print(f"[preflight] {src}/{f}: {ln:,} rows — same shape on both sides but "
                          f"NOT content-checked (over the {FP_MAX_ROWS:,}-row fingerprint cap); "
                          f"a value revision here would not be detected", flush=True)
                elif lfp != rfp:
                    out.append((src, f"{f}: same shape ({ln:,} rows to {lmx}) but the VALUES "
                                     f"differ — R2 holds a revision the mirror does not"))
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
        if unreadable:
            # A sample this guard could not READ is not a sample it PASSED. One transient R2
            # error should not block legitimate work, but if most of the sample failed the
            # check established nothing and must not read as a clean preflight.
            print(f"[preflight] {src}: {unreadable} of {k} sampled file(s) could not be "
                  f"compared", flush=True)
            if unreadable * 2 > k:
                out.append((src, f"{unreadable} of {k} sampled files were UNREADABLE — this "
                                 f"preflight compared almost nothing and cannot clear the "
                                 f"mirror"))
    return out


def _apply_only(rows, path):
    """Restrict the work list to the ids named in ``path`` (one per line, # comments).

    Two failure shapes this must not have, both of which would report a tidy success:

      * a requested id that is NOT in the catalogue is NAMED, not silently dropped — the run
        would otherwise derive a smaller set than the operator believes they selected;
      * a selection of zero EXITS NONZERO rather than completing instantly with nothing done,
        which is what a mistyped path or a stale id list looks like.
    """
    want = set()
    with open(path, encoding="utf-8") as fh:
        for ln in fh:
            # A '#' is only a comment at the START of a line. Splitting on it anywhere
            # TRUNCATED every id that contains one — ilostat's split-part ids look like
            # `ilostat:CPI_XCPI_COI_RT_M#COI_COICOP_CP01`, so 52 of them silently became
            # `ilostat:CPI_XCPI_COI_RT_M`, matched nothing, and were reported as absent from
            # the catalogue when the real id was right there.
            if ln.lstrip().startswith("#"):
                continue
            ln = ln.strip()
            if ln:
                want.add(ln)
    before = len(rows)
    kept = [r for r in rows if r[0] in want]
    missing = want - {r[0] for r in kept}
    print(f"--only: selected {len(kept):,} of {before:,} catalog series "
          f"({len(want):,} ids requested)", flush=True)
    if missing:
        print(f"--only: {len(missing):,} REQUESTED IDS ARE NOT IN THE CATALOGUE and will not "
              f"be derived, e.g. {sorted(missing)[:5]}", flush=True)
    if not kept:
        print("--only selected nothing — refusing a no-op run that would exit 0 having "
              "derived nothing.", flush=True)
        raise SystemExit(2)
    return kept


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
    ap.add_argument("--stream-max-rows", type=int, default=0,
                    help="upper bound for the streaming path; a table above this is skipped "
                         "and named rather than attempted. 0 = no bound. Two cbs_nl tables "
                         "(1.89B and 1.06B rows) would each produce a single CSV of 20.8 GB "
                         "and 11.6 GB - not a usable download at any speed - and a crash "
                         "there takes a whole shard's queue with it.")
    ap.add_argument("--allow-stream", action="store_true",
                    help="derive tables above --max-rows by streaming them through a DuckDB "
                         "external ORDER BY instead of skipping them. Off by default: an "
                         "earlier unsorted streamer wrote the right rows in the wrong order "
                         "(R466), so this path is opt-in and byte-verified.")
    ap.add_argument("--shard", default=None, metavar="I/N",
                    help="process only every Nth series (shard I of N, I from 0). Lets several "
                         "independent PROCESSES split one source: the CSV projection is a "
                         "pure-Python row loop, so threads serialise on the GIL and adding "
                         "workers past a handful buys nothing (measured 2026-08-24: 11.7 MB/20s "
                         "with 6 threads vs 10.0 MB/20s with 25). Separate processes do scale.")
    ap.add_argument("--max-rows", type=int, default=0,
                    help="skip (and name) any series whose store file exceeds this many "
                         "rows; the CSV is built in memory, so a giant table can exhaust "
                         "the machine. 0 disables.")
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
    ap.add_argument("--only", metavar="PATH",
                    help="file of catalog series ids, one per line (lines starting with # are comments); derive "
                         "ONLY these. For a targeted REBUILD, where --skip-existing would skip "
                         "every key and --skip-newer-than would walk the whole source to reach a "
                         "handful. Written 2026-09-01 for the 68 eurostat flows whose served CSV "
                         "was built from a local mirror that had fallen behind R2, so the CSV "
                         "served an older vintage than the store held (tec00108 served 5,328 "
                         "rows against R2's 5,415).")
    a = ap.parse_args()
    global _MAX_ROWS, _ALLOW_STREAM
    _MAX_ROWS = a.max_rows
    _ALLOW_STREAM = a.allow_stream
    global _STREAM_MAX
    _STREAM_MAX = a.stream_max_rows

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

    if a.only:
        rows = _apply_only(rows, a.only)

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

    if a.shard:
        try:
            _i, _n = (int(x) for x in a.shard.split("/", 1))
        except ValueError:
            print(f"--shard expects I/N, got {a.shard!r}"); return 2
        if not (0 <= _i < _n):
            print(f"--shard {a.shard}: need 0 <= I < N"); return 2
        before = len(rows)
        # Stable, order-independent split so every shard sees a disjoint set no matter what
        # order the catalogue returned. Hash the id, not the index.
        import zlib                                                  # noqa: PLC0415
        rows = [r for r in rows if zlib.crc32(r[0].encode()) % _n == _i]
        print(f"shard {_i}/{_n}: {len(rows):,} of {before:,} series", flush=True)

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
                _derive_and_put(s3, a.bucket, key, sid)
            except Exception as e:                           # noqa: BLE001
                return ("miss", sid, str(e)[:80])
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
                _derive_and_put(s3, a.bucket, key, sid)
            except Exception as e:                           # noqa: BLE001
                miss += 1
                print(f"  unresolvable {sid}: {str(e)[:80]}")
                continue
            up += 1
            if up % 500 == 0:
                print(f"  derived+put {up:,} (skip {skip:,}, miss {miss:,})...", flush=True)

    # SAY WHAT THE CHECK SAVED. `_SKIPPED_IDENTICAL` counted avoided uploads from the first
    # commit and nothing printed it, so the saving could only be predicted. 71% of this
    # account's class-A operations are PutObject on econ-data - 7,686,397 of 10.8 M over 24
    # days - so this number is the one that says whether that line is coming down.
    ident = _SKIPPED_IDENTICAL[0]
    print(f"done: put {up:,} series CSVs, skipped {skip:,} existing, "
          f"{miss:,} unresolvable (store-coverage gaps)")
    if ident:
        print(f"      {ident:,} upload(s) avoided - R2 already held those exact bytes "
              f"(~${ident / 1e6 * 4.50:.2f} of class-A operations not spent)")


if __name__ == "__main__":
    main()
