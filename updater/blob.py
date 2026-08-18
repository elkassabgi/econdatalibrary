"""Blob (object) accessor — filesystem primitives + the cloud-portable Blob interface.

Two layers live here on purpose (UPDATER_BUILD_PLAN.md §1.1 step 3, §1.3):

1. Module-level path functions (exists / read_table / write_table_atomic /
   row_count): what the ~75 strategy fetchers call against store paths. With
   AQUEDUCT_BACKEND=local they are the original filesystem primitives
   (atomic publish = per-process unique ``.tmp`` + ``os.replace``); with
   AQUEDUCT_BACKEND=r2 they route to the bucket via path→key translation
   (see the Layer-1 banner below) so every fetcher is CI-capable unedited.

2. The Blob interface (``LocalBlob`` / ``R2Blob`` + ``from_env``): a uniform
   ``get/put_atomic/etag/exists`` handle so the SAME invariant-guarded publish
   (merge.merge_and_write) runs against local files at home and R2 objects in
   CI. Backend selection is the env var ``AQUEDUCT_BACKEND``: ``local``
   (default, filesystem) or ``r2`` (boto3 via core.r2_util, bucket
   ``econ-data``). ``cloud`` — the D1-native StateStore of the original design
   — is an explicit v1 non-goal (plan §7) and is rejected loudly rather than
   left half-working.

WHY ``put_atomic`` is a plain PUT on R2: R2 PUTs are atomic per key (plan D-3)
— a reader sees the old object or the new object, never a torn write — so no
temp-key + CopyObject dance is needed. Objects here are well under the 5 GB
single-PUT limit (state.db.zst ~tens of MB, per-source parquet ≤ ~4.6 GB;
giants never publish through CI at all, plan §3.4).
"""
from __future__ import annotations
import hashlib
import io
import os
import time
import shutil
import uuid

import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Layer 1 — path primitives used directly by the ~75 fetchers.
#
# BACKEND ROUTING (the one choke point that makes every fetcher CI-capable
# without editing any of them): with AQUEDUCT_BACKEND=r2 these four functions
# translate the local store path (…/data/clean_full/<src>/x.parquet) to the R2
# object key (clean_full/<src>/x.parquet) and read/publish against the bucket —
# R2 is the source of truth in CI. Writes ALSO keep the local file at the
# original path: that mirror is what plan §1.1-step-5 needs so $ECONDL_DATA can
# point at the runner scratch store for the CSV derive (bytes already in hand,
# no re-download). With AQUEDUCT_BACKEND=local (default) behavior is
# byte-identical to the original filesystem-only implementation.
# ---------------------------------------------------------------------------


def _r2_routed():
    """The active R2Blob when AQUEDUCT_BACKEND=r2, else None (= local mode)."""
    if os.environ.get("AQUEDUCT_BACKEND", "local").strip().lower() == "r2":
        return from_env("r2")
    return None


def _path_to_key(path: str) -> str:
    """Map a local store path to its R2 object key: everything after the last
    /data/ segment. Refuses loudly on paths outside the store — guessing a key
    could publish data to the wrong object."""
    norm = str(path).replace("\\", "/")
    i = norm.rfind("/data/")
    if i == -1:
        raise ValueError(
            f"cannot derive an R2 key from {path!r}: no /data/ segment. "
            "R2-routed blob helpers only accept store paths (…/data/<tier>/…).")
    return norm[i + len("/data/"):]


def exists(path: str) -> bool:
    r2 = _r2_routed()
    if r2 is not None:
        return r2.exists(_path_to_key(path))
    return os.path.exists(path)


def read_table(path: str, columns=None):
    """A stored parquet as an Arrow table, R2-routed. `columns` projects the
    read (same semantics as pq.read_table's columns=) so a fetcher learning each
    series' last obs_date can pull just the two columns it needs. columns=None
    (the default) reads every column, so existing callers are unchanged.

    THIS is the CI-safe read: a raw pq.read_table(path) reads the local path,
    which does not exist on a GitHub runner (AQUEDUCT_BACKEND=r2) even though the
    parquet is in R2 -> the fetcher silently ingests nothing (ledger R36)."""
    r2 = _r2_routed()
    if r2 is not None:
        data = r2.get(_path_to_key(path))
        if data is None:
            raise FileNotFoundError(f"R2 object absent for {path!r}")
        return pq.read_table(io.BytesIO(data), columns=columns)
    return pq.read_table(path, columns=columns)


def iter_batches(path: str, columns=None, batch_size: int = 1_000_000):
    """Yield a stored parquet as Arrow RecordBatches, R2-routed like read_table.

    WHY THIS EXISTS (2026-07-30). read_table materialises the WHOLE table, and `columns=`
    only narrows the row WIDTH — it does not bound the peak. statcan's largest cube,
    98100435.parquet, is 962,150,400 rows: reading every column decodes to roughly 67 GB
    on a 16 GB runner, and projecting to the four string columns one caller needs still
    lands near 56 GB. Narrowing a fatal read to a slightly smaller fatal read is not a fix.

    Iterating row groups keeps DECODED memory to one batch. Under the R2 backend the
    object still has to come over the wire in full (~2 GB compressed for that cube), but
    the compressed bytes are the floor, not the ~56 GB decode.

    Use this for any scan whose result is an AGGREGATE (a max, a map, a count) rather than
    the table itself. See tools/audit_cursor_blowup.py, CLASS 2.
    """
    r2 = _r2_routed()
    if r2 is not None:
        data = r2.get(_path_to_key(path))
        if data is None:
            raise FileNotFoundError(f"R2 object absent for {path!r}")
        pf = pq.ParquetFile(io.BytesIO(data))
    else:
        pf = pq.ParquetFile(path)
    for batch in pf.iter_batches(batch_size=batch_size, columns=columns):
        yield batch


def read_schema(path: str):
    """The Arrow schema of a stored parquet, R2-routed like read_table.
    Replaces a raw pq.ParquetFile(path).schema_arrow, which reads a local path
    absent on a CI runner under AQUEDUCT_BACKEND=r2 (ledger R36)."""
    r2 = _r2_routed()
    if r2 is not None:
        data = r2.get(_path_to_key(path))
        if data is None:
            raise FileNotFoundError(f"R2 object absent for {path!r}")
        return pq.read_schema(io.BytesIO(data))
    return pq.read_schema(path)


def read_metadata(path: str):
    """The parquet FileMetaData of a stored parquet, R2-routed like read_schema.

    Exists so an aggregate that parquet already knows (a column min/max held in the
    row-group statistics) can be answered from the FOOTER instead of decoding data.
    Locally that is a couple of seeks; there is no decode at any file size.

    NOTE the honest limit under the R2 backend: the object still has to come over the
    wire in full before its footer can be read, so this saves the DECODE, not the GET.
    That is the difference between "expensive" and "impossible" — oecd's largest flow
    file is 1,792,000,000 rows and cannot be decoded on any runner we own.
    """
    r2 = _r2_routed()
    if r2 is not None:
        data = r2.get(_path_to_key(path))
        if data is None:
            raise FileNotFoundError(f"R2 object absent for {path!r}")
        return pq.read_metadata(io.BytesIO(data))
    return pq.read_metadata(path)


def write_table_atomic(path: str, table, compression: str = "zstd") -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Per-process+per-call unique temp name so two concurrent writers (different
    # runners/threads) can never clobber each other's in-progress temp file.
    tmp = f"{path}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp"
    try:
        pq.write_table(table, tmp, compression=compression)
        # RETRY THE REPLACE. os.replace onto an existing file is atomic on POSIX but can fail
        # on Windows with PermissionError(13) whenever anything holds a transient handle on
        # the target - an antivirus scanning the file we just wrote, an indexer, or a reader
        # that has not yet released it. It is a RACE, not a permission problem: the same call
        # succeeds moments later.
        #
        # This is not hypothetical. cepii_gravity streams a 1.25 GB CSV and re-publishes the
        # same parquet every batch, so it performs this replace hundreds of times in one run;
        # on 2026-08-01 it lost the race after 20,000,000 rows and the whole source
        # transient-failed with "Access is denied" having merged nothing. A source that dies
        # 20 million rows in because of a momentary file lock is a source that never finishes.
        #
        # Bounded and loud: six attempts over ~3 s, then the original error propagates. POSIX
        # is unaffected - the first attempt succeeds and the loop costs nothing.
        for attempt in range(6):
            try:
                os.replace(tmp, path)  # atomic publish — readers never see a half-written file
                break
            except PermissionError:
                if attempt == 5:
                    raise
                time.sleep(0.2 * (2 ** attempt))
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass
    r2 = _r2_routed()
    if r2 is not None:
        # R2 is the publish target in CI; the local file written above doubles
        # as the scratch-store mirror for the same-run CSV derive ($ECONDL_DATA).
        with open(path, "rb") as fh:
            r2.put_atomic(_path_to_key(path), fh.read())


def list_parquets(dir_path: str, recursive: bool = False) -> list[str]:
    """Sorted parquet names inside a store dir, R2-routed.

    Replaces ``os.listdir(out_dir)`` / ``glob.glob(out_dir/*.parquet)`` in fetchers:
    the local store dir is absent on a CI runner (AQUEDUCT_BACKEND=r2) — a raw
    listdir either trips the fetcher's "source dir missing" DefinitiveError or,
    worse, silently yields zero flows (ledger R36, same class as raw reads).

    Default (``recursive=False``) returns BASENAMES ONLY, so existing callers keep their
    ``os.path.join(out_dir, fn)`` shape and a nested key can never masquerade as a top-level
    flow. That default is deliberate and unchanged.

    ``recursive=True`` returns names RELATIVE to dir_path, so nested layouts come back as
    ``"Regional/CAINC5S.parquet"``. Needed because not every store is flat: bea's 591 files
    live at ``clean_full/bea/<Dataset>/<Table>.parquet``, and a non-recursive listing of that
    directory returns an empty list — indistinguishable from an empty store. The caller still
    joins onto dir_path, and the separator is normalised so a Windows join produces the same
    key an R2 listing did.
    """
    r2 = _r2_routed()
    if r2 is not None:
        prefix = _path_to_key(dir_path).rstrip("/") + "/"
        names = []
        paginator = r2.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=r2.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                name = obj["Key"][len(prefix):]
                if not name or not name.endswith(".parquet"):
                    continue
                if recursive or "/" not in name:
                    names.append(name)
        return sorted(names)
    if not os.path.isdir(dir_path):
        return []
    if not recursive:
        return sorted(f for f in os.listdir(dir_path)
                      if f.endswith(".parquet")
                      and os.path.isfile(os.path.join(dir_path, f)))
    out = []
    for root, _dirs, files in os.walk(dir_path):
        for f in files:
            if f.endswith(".parquet"):
                rel = os.path.relpath(os.path.join(root, f), dir_path)
                out.append(rel.replace(os.sep, "/"))   # match the R2 key spelling
    return sorted(out)


def read_bytes(path: str) -> bytes | None:
    """Raw bytes of a store-adjacent non-parquet sidecar (…/data/<tier>/<src>/_x.json),
    R2-routed like read_table; None when absent. A fetcher that keeps per-survey
    state beside the data (e.g. bls's _vintages.json) must read it through here —
    a plain open(path) sees nothing on a CI runner and the state resets every run."""
    r2 = _r2_routed()
    if r2 is not None:
        return r2.get(_path_to_key(path))
    if not os.path.exists(path):
        return None
    with open(path, "rb") as fh:
        return fh.read()


def write_bytes_atomic(path: str, data: bytes) -> None:
    """Atomic publish of a non-parquet sidecar, R2-routed (tmp + os.replace locally,
    single PUT on R2; the local file doubles as the scratch-store mirror, same
    contract as write_table_atomic)."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp"
    try:
        with open(tmp, "wb") as fh:
            fh.write(data)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass
    r2 = _r2_routed()
    if r2 is not None:
        r2.put_atomic(_path_to_key(path), data)


def publish_file(path: str) -> int:
    """Publish an ALREADY-WRITTEN local store file to R2 by STREAMING it from disk.

    For the one case write_table_atomic cannot serve: a reused production ingest that
    writes its own parquet with a raw pq.ParquetWriter. Those bytes are already correct
    on disk at the store path — they simply never reached R2, because blob is the only
    writer that knows about R2 (ledger R36). Reading such a file back with pq.read_table
    just to hand the table to write_table_atomic would materialise it whole in RAM;
    fed_board's Z.1 release alone is a ~590MB zip, which is how a CI runner OOMs.

    Returns bytes published, 0 if the file is absent. Under the local backend the file
    is already AT its store path, so this is a no-op that just reports the size.
    """
    if not os.path.exists(path):
        return 0
    r2 = _r2_routed()
    if r2 is not None:
        r2.put_file(_path_to_key(path), path)
    return os.path.getsize(path)


def row_count(path: str) -> int:
    r2 = _r2_routed()
    if r2 is not None:
        data = r2.get(_path_to_key(path))
        if data is None:
            return 0
        try:
            return pq.read_metadata(io.BytesIO(data)).num_rows
        except Exception:
            return 0
    if not os.path.exists(path):
        return 0
    try:
        return pq.read_metadata(path).num_rows
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Layer 2 — the Blob interface (local | r2), selected by AQUEDUCT_BACKEND
# ---------------------------------------------------------------------------

R2_BUCKET = "econ-data"


class LocalBlob:
    """Filesystem Blob. Keys are paths — absolute, or joined onto ``root``.

    ``etag()`` is the md5 hexdigest of the file bytes: the same value a
    single-part S3/R2 PUT reports as its ETag, so compare-and-swap logic can be
    exercised (and unit-tested) with no cloud at all.
    """

    def __init__(self, root: str | None = None):
        self.root = root

    def _path(self, key: str) -> str:
        if self.root and not os.path.isabs(key):
            return os.path.join(self.root, key)
        return key

    def get(self, key: str) -> bytes | None:
        p = self._path(key)
        if not os.path.exists(p):
            return None
        with open(p, "rb") as f:
            return f.read()

    def put_atomic(self, key: str, data: bytes) -> None:
        p = self._path(key)
        d = os.path.dirname(p)
        if d:
            os.makedirs(d, exist_ok=True)
        tmp = f"{p}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp"
        try:
            with open(tmp, "wb") as f:
                f.write(data)
            os.replace(tmp, p)  # atomic publish, same guarantee as write_table_atomic
        finally:
            if os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except OSError:
                    pass

    def put_file(self, key: str, src_path: str) -> None:
        """Stream a file into the store. A no-op when src is already the store path."""
        p = self._path(key)
        if os.path.abspath(p) == os.path.abspath(src_path):
            return
        d = os.path.dirname(p)
        if d:
            os.makedirs(d, exist_ok=True)
        shutil.copyfile(src_path, p)

    def etag(self, key: str) -> str | None:
        data = self.get(key)
        if data is None:
            return None
        return hashlib.md5(data).hexdigest()

    def exists(self, key: str) -> bool:
        return os.path.exists(self._path(key))


# Extension -> ContentType for R2 PUTs. Only types the Worker actually serves
# raw need an entry; everything else falls back to S3's application/octet-stream.
# text/csv matches core/derive_csv.py's backfill PUTs exactly.
_CONTENT_TYPES = {
    ".csv": "text/csv",
    ".json": "application/json",
}


def _is_404(exc) -> bool:
    """True when a botocore ClientError means 'object does not exist'."""
    resp = getattr(exc, "response", None) or {}
    code = str(resp.get("Error", {}).get("Code", ""))
    status = resp.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return code in ("404", "NoSuchKey", "NotFound") or status == 404


class R2Blob:
    """R2-backed Blob. Keys are object keys inside the ``econ-data`` bucket.

    The boto3 client comes from ``core.r2_util.client(write=True)`` — creds
    resolve env-first with ``.env`` fallback, so this works identically on a
    laptop and headless in CI (GitHub secrets arrive as the same R2_* env
    names). The client is built lazily: importing this module never requires
    boto3 or credentials, only actually touching R2 does.
    """

    def __init__(self, bucket: str = R2_BUCKET):
        self.bucket = bucket
        self._client = None

    @property
    def client(self):
        if self._client is None:
            from core import r2_util  # lazy — only R2 runs need boto3 + creds
            self._client = r2_util.client(write=True)
        return self._client

    def get(self, key: str) -> bytes | None:
        from botocore.exceptions import ClientError
        try:
            resp = self.client.get_object(Bucket=self.bucket, Key=key)
        except ClientError as e:
            if _is_404(e):
                return None
            raise
        return resp["Body"].read()

    def put_atomic(self, key: str, data: bytes) -> None:
        # Single PUT — atomic per key on R2 (plan D-3); botocore already retries
        # transient failures (r2_util config: 5 attempts, standard mode).
        # ContentType by extension: the Worker serves series CSVs via plain R2 GET,
        # and core/derive_csv.py's backfill PUTs set text/csv — a re-derived CSV
        # must not silently downgrade to application/octet-stream (A3 handoff note).
        kw = {}
        ct = _CONTENT_TYPES.get(os.path.splitext(key)[1].lower())
        if ct:
            kw["ContentType"] = ct
        # GZIP AT REST, series CSVs ONLY (cost plan 2026-08-18, mirrors
        # core/derive_csv.py's writer): ContentEncoding='gzip' is the marker the
        # worker's reader decompresses on; mtime=0 keeps bytes deterministic for
        # the verifier's byte-compare. Manifests/JSON stay plain — their readers
        # (including this module's own get paths) expect raw bytes.
        if key.startswith("series/") and key.endswith(".csv"):
            import gzip as _gzip
            data = _gzip.compress(data, mtime=0)
            kw["ContentEncoding"] = "gzip"
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data, **kw)

    def put_file(self, key: str, src_path: str) -> None:
        # upload_file streams and switches to multipart above the threshold, so a
        # multi-GB parquet never has to exist in memory. Same ContentType rule as
        # put_atomic — a CSV must not silently downgrade to octet-stream.
        kw = {}
        ct = _CONTENT_TYPES.get(os.path.splitext(key)[1].lower())
        if ct:
            kw["ContentType"] = ct
        self.client.upload_file(src_path, self.bucket, key,
                                ExtraArgs=kw or None)

    def etag(self, key: str) -> str | None:
        from botocore.exceptions import ClientError
        try:
            resp = self.client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as e:
            if _is_404(e):
                return None
            raise
        return (resp.get("ETag") or "").strip('"') or None

    def size(self, key: str) -> int | None:
        """Stored byte length, or None if the object does not exist.

        Used by push_state's shrink guard: "is what I am about to overwrite
        substantial?" is a question only the remote can answer, and judging the
        local file against fixed thresholds instead got every legitimate seed
        refused (R407 follow-up).
        """
        from botocore.exceptions import ClientError
        try:
            resp = self.client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as e:
            if _is_404(e):
                return None
            raise
        return resp.get("ContentLength")

    def exists(self, key: str) -> bool:
        return self.etag(key) is not None

    def list_keys(self, prefix: str) -> list[str]:
        """All keys under a prefix. Used by push_state's backup retention;
        _aqueduct/backups/ holds tens of keys, so no pagination concerns beyond
        what the paginator already handles."""
        keys: list[str] = []
        for page in self.client.get_paginator("list_objects_v2").paginate(
                Bucket=self.bucket, Prefix=prefix):
            keys += [o["Key"] for o in page.get("Contents", [])]
        return keys

    def delete(self, key: str) -> None:
        """Delete one object. Deletes are free on R2; a 404 is already-gone,
        which is the goal state, so no error mapping is needed."""
        self.client.delete_object(Bucket=self.bucket, Key=key)


def from_env(backend: str | None = None) -> LocalBlob | R2Blob:
    """Build the Blob selected by AQUEDUCT_BACKEND (explicit arg overrides env).

    'local' (or unset) -> LocalBlob (keys are filesystem paths, today's behavior)
    'r2'               -> R2Blob   (keys are econ-data object keys)
    """
    b = (backend or os.environ.get("AQUEDUCT_BACKEND", "local")).strip().lower()
    if b in ("", "local"):
        return LocalBlob()
    if b == "r2":
        return R2Blob()
    if b == "cloud":
        raise ValueError(
            "AQUEDUCT_BACKEND=cloud (the D1-native StateStore) is not implemented and "
            "is a v1 non-goal (UPDATER_BUILD_PLAN.md §7). Use AQUEDUCT_BACKEND=r2 for "
            "the R2 object backend, or 'local' for the filesystem.")
    raise ValueError(f"unknown AQUEDUCT_BACKEND {b!r}; expected 'local' or 'r2'")
