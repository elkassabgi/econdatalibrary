"""Blob (object) accessor — filesystem primitives + the cloud-portable Blob interface.

Two layers live here on purpose (UPDATER_BUILD_PLAN.md §1.1 step 3, §1.3):

1. Module-level path functions (exists / read_table / write_table_atomic /
   row_count): the original filesystem primitives. ~40 strategy fetchers call
   these against local paths; their behavior is UNCHANGED — atomic publish is
   still a per-process unique ``.tmp`` + ``os.replace``.

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
import os
import uuid

import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Layer 1 — filesystem path primitives (pre-existing; used directly by fetchers)
# ---------------------------------------------------------------------------


def exists(path: str) -> bool:
    return os.path.exists(path)


def read_table(path: str):
    return pq.read_table(path)


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


def row_count(path: str) -> int:
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
