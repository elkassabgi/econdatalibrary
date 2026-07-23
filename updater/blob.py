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


def write_table_atomic(path: str, table, compression: str = "zstd") -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Per-process+per-call unique temp name so two concurrent writers (different
    # runners/threads) can never clobber each other's in-progress temp file.
    tmp = f"{path}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp"
    try:
        pq.write_table(table, tmp, compression=compression)
        os.replace(tmp, path)  # atomic publish — readers never see a half-written file
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
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data, **kw)

    def etag(self, key: str) -> str | None:
        from botocore.exceptions import ClientError
        try:
            resp = self.client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as e:
            if _is_404(e):
                return None
            raise
        return (resp.get("ETag") or "").strip('"') or None

    def exists(self, key: str) -> bool:
        return self.etag(key) is not None


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
