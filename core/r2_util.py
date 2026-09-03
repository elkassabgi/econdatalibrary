"""Shared R2 (S3-compatible) client + helpers for the cutover scripts.

Reads credentials from the gitignored .env. Prefers WRITE keys (R2_WRITE_*) and
falls back to READ keys (R2_READ_*). Never prints secrets. A client is only built
when real creds are present; placeholder values (e.g. '...') are treated as absent
so a dry-run works with nothing configured and the real run fails loudly if asked
to write without write creds.
"""
from __future__ import annotations

import gzip as _gzip
import hashlib as _hashlib
import os

_THIS = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(_THIS, ".."))
ENV = os.path.join(ROOT, ".env")

# The gzip header's OS byte, forced. See gzip_bytes() below for why this constant exists.
_GZIP_OS_UNKNOWN = 0xFF


def gzip_bytes(data: bytes, compresslevel: int = 9) -> bytes:
    """Gzip for R2, normalised so THIS repo's two writers agree with each other.

    WHAT IT FIXES. `core/derive_csv.py` writes some objects with `GzipFile` and others with
    `gzip.compress`, and those disagreed on Python 3.11: `GzipFile._write_gzip_header` writes
    b"\\377" unconditionally, while 3.11's `gzip.compress` returns
    `zlib.compress(data, level, wbits=31)` untouched when mtime == 0, leaving zlib's build
    platform in the header's OS byte (3 on Linux). Python 3.14 ends the same function with
    `struct.pack("<4sLBB", gzip_data, int(mtime), gzip_data[8], 255)`, forcing 255. Forcing it
    here makes the two paths byte-identical on every interpreter tested.

    WHAT IT DOES NOT FIX, and this must not be overstated. It does NOT make output portable
    across machines. The DEFLATE STREAM itself differs: Python 3.14 on the desktop links
    zlib-ng ("1.3.1.zlib-ng"), the 3.11 runners link stock zlib 1.3.1, and the same input at
    level 9 produced 787,922 bytes against 788,191. Measured on 90 real bucket objects written
    since the 2026-08-18 gzip cutover, each population is reproducible only by the compressor
    that made it:

        stored OS=3   (n=45): 45/45 reproducible on 3.11, 23/45 on 3.14
        stored OS=255 (n=44): 44/44 reproducible on 3.14,  9/44 on 3.11

    With this normalisation the desktop's skip rate on those objects rises from 49.4% to 75.3%
    and CI's from 50.6% to 60.7%. Roughly a quarter still re-uploads on every pass, forever,
    and no header edit can reach that.

    THE REAL FIX IS NOT BYTE-IDENTITY. A compressed stream is not a portable invariant, and a
    matching Python version would not save it either - 3.14 bundles zlib-ng on Windows and
    links system zlib on Linux. The durable comparison is the digest of the PRE-COMPRESSION
    CSV, carried in object metadata. `core/derive_csv.py:428` already passes `Metadata=`, and
    the skip guard already pays a `head_object` that returns it. This helper is a prerequisite,
    not the cure. Same lesson as R383, which rejected byte hashes for parquet because the
    desktop and CI run different pyarrow versions.
    """
    out = _gzip.compress(data, compresslevel=compresslevel, mtime=0)
    if len(out) > 9 and out[9] != _GZIP_OS_UNKNOWN:
        out = out[:9] + bytes([_GZIP_OS_UNKNOWN]) + out[10:]
    return out


def load_env() -> dict:
    out = {}
    if os.path.exists(ENV):
        for ln in open(ENV, encoding="utf-8"):
            ln = ln.strip()
            if "=" in ln and not ln.startswith("#"):
                k, v = ln.split("=", 1)
                out[k.strip()] = v.strip().strip('"').strip("'")
    # Real environment variables win over the .env file: in CI there is no .env —
    # GitHub secrets arrive as env vars (same R2_* names), so this is what makes
    # the updater runnable headless. Locally .env still fills anything unset.
    for k, v in os.environ.items():
        if k.startswith(("R2_READ_", "R2_WRITE_")) and v:
            out[k] = v
    return out


def _real(v: str | None) -> str | None:
    """A credential value that is actually set (not empty / not a '...' placeholder)."""
    if not v:
        return None
    if set(v) <= {"."} or v.lower() in ("changeme", "todo", "xxx"):
        return None
    return v


def creds(write: bool = False) -> dict | None:
    """Return {endpoint, key, secret, mode} or None if (real) creds are absent."""
    e = load_env()
    if write:
        ep, ak, sk = (_real(e.get("R2_WRITE_ENDPOINT")), _real(e.get("R2_WRITE_ACCESS_KEY_ID")),
                      _real(e.get("R2_WRITE_SECRET_ACCESS_KEY")))
        if ep and ak and sk:
            return {"endpoint": ep, "key": ak, "secret": sk, "mode": "write"}
        return None
    # read (or write keys if read absent)
    ep = _real(e.get("R2_READ_ENDPOINT")) or _real(e.get("R2_WRITE_ENDPOINT"))
    ak = _real(e.get("R2_READ_ACCESS_KEY_ID")) or _real(e.get("R2_WRITE_ACCESS_KEY_ID"))
    sk = _real(e.get("R2_READ_SECRET_ACCESS_KEY")) or _real(e.get("R2_WRITE_SECRET_ACCESS_KEY"))
    if ep and ak and sk:
        return {"endpoint": ep, "key": ak, "secret": sk, "mode": "read"}
    return None


def client(write: bool = False):
    """Build a boto3 S3 client for R2, or raise a clear error if creds are missing."""
    c = creds(write=write)
    if c is None:
        raise RuntimeError(
            f"R2 {'write' if write else 'read'} credentials are not set in {ENV} "
            f"(need R2_{'WRITE' if write else 'READ'}_ENDPOINT/ACCESS_KEY_ID/SECRET_ACCESS_KEY). "
            "Add real values — current ones are absent or placeholders.")
    import boto3
    from botocore.config import Config
    return boto3.client(
        "s3", endpoint_url=c["endpoint"], aws_access_key_id=c["key"],
        aws_secret_access_key=c["secret"], region_name="auto",
        config=Config(signature_version="s3v4", retries={"max_attempts": 5, "mode": "standard"}))


# ---------------------------------------------------------------------------
# SERIES CSV OBJECTS: one definition of how they are stored, shared by every writer.
#
# WHY THIS LIVES HERE. `updater/blob.py::put_atomic` and nine tools under `tools/` all write
# `series/<id>.csv` into the same bucket, and until 2026-09-03 they disagreed: the updater
# gzipped and the tools wrote plain, so any id both touched alternated encodings and neither
# side's skip check could ever match. Two implementations of "how a series CSV is stored" is
# the defect; this is the one.
# ---------------------------------------------------------------------------

PLAIN_MD5_KEY = "csvmd5"
"""Object-metadata key holding the MD5 of the CSV BEFORE compression.

Lowercase and separator-free on purpose: S3 metadata keys travel as HTTP header suffixes and
boto3 lowercases them on the way back, so anything else invites a case mismatch that nobody
notices until a guard silently stops matching.
"""


def series_csv_put_args(csv: bytes) -> tuple[bytes, dict, str]:
    """(body to store, put_object kwargs, the CSV's own digest).

    The digest is taken BEFORE compression, and that is the point. Compressed bytes are not a
    portable identity: the desktop's Python 3.14 links zlib-ng and the 3.11 runners link stock
    zlib, so the same CSV at level 9 deflates to 787,922 bytes on one and 788,191 on the other.
    The CSV's own MD5 is the same number on every machine, Python and zlib.
    """
    # ALREADY-GZIPPED INPUT IS RETURNED AS-IS. Some producers compress at enqueue and hand the
    # compressed body straight here; gzipping it again would store a double-gzipped object
    # served as text/csv, which is R560 - about 188 objects shipped that way from exactly this
    # mistake. tools/derive_statcan_tables.py:285 already carries this magic-byte check
    # privately; the shared helper must not be the one place that lacks it.
    #
    # The digest is then of the COMPRESSED bytes, not the CSV, so it cannot be compared against
    # a digest taken before compression. Returning None says so rather than offering a number
    # that looks comparable and is not.
    if csv[:2] == b"\x1f\x8b":
        return csv, {"ContentType": "text/csv", "ContentEncoding": "gzip"}, None

    digest = _hashlib.md5(csv).hexdigest()                            # noqa: S324
    return gzip_bytes(csv), {
        "ContentType": "text/csv",
        "ContentEncoding": "gzip",
        "Metadata": {PLAIN_MD5_KEY: digest},
    }, digest


def r2_holds_csv(s3, bucket: str, key: str, digest: str, body: bytes | None = None) -> bool:
    """True only when R2 provably holds this same CSV. Every uncertain case returns False.

    Two comparisons, in order of how much they prove:

      1. `x-amz-meta-csvmd5` against `digest`. Durable across machines and compressors.
      2. The ETag against the MD5 of `body`, when given. Only for objects written before the
         metadata existed, so the fleet converges instead of re-uploading everything at once.
         It answers correctly when the same compressor wrote the object and stays silent
         otherwise - a false NEGATIVE, which costs one upload and never a wrong skip.

    A multipart ETag (`<hex>-<n>`) is a digest of digests and is never comparable. An error
    asking is not evidence of anything.
    """
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
    except Exception:                                                 # noqa: BLE001
        return False
    meta = {k.lower(): v for k, v in (head.get("Metadata") or {}).items()}
    stored = meta.get(PLAIN_MD5_KEY)
    # `digest` may be empty when the caller had no pre-compression digest to offer (an
    # already-gzipped body). Comparing a real stored digest against "" would answer False and
    # force an upload - safe, but wasteful, and it would look like a mismatch rather than a
    # question that was never asked.
    if stored and digest:
        return stored == digest
    if body is None:
        return False
    tag = (head.get("ETag") or "").strip('"')
    if not tag or "-" in tag:
        return False
    return tag == _hashlib.md5(body).hexdigest()                      # noqa: S324


PUT_TRIES = 7
"""App-level PUT attempts, on top of boto's own 5.

NOT REDUNDANT WITH BOTO. `updater/derive.py:50-57` records why: "R2 can throw transient
ServiceUnavailable/SlowDown throttles that outlast boto's built-in retries (killed the
2026-07-02 bulk run at 103k objects)". Four of the nine tools that write series CSVs carry a
private 6-or-7-try loop for this reason and five carry none at all; a shared helper without a
loop would silently demote the four to boto's 5 while looking like consolidation.
"""


def put_series_csv(s3, bucket: str, key: str, csv: bytes, *, skip_identical: bool = True):
    """PUT one series CSV the way every writer should. Returns "put" or "skipped".

    Compresses via `series_csv_put_args` (which leaves an already-gzipped body alone), records
    the pre-compression digest so the next writer can recognise it from any machine, skips the
    upload when R2 provably holds the same CSV, and retries transient R2 throttles.
    """
    import time as _time                                              # noqa: PLC0415

    body, kw, digest = series_csv_put_args(csv)
    if skip_identical and digest and r2_holds_csv(s3, bucket, key, digest, body):
        return "skipped"
    for attempt in range(PUT_TRIES):
        try:
            s3.put_object(Bucket=bucket, Key=key, Body=body, **kw)
            return "put"
        except Exception:                                             # noqa: BLE001
            if attempt == PUT_TRIES - 1:
                raise
            _time.sleep(2 ** attempt)
    raise AssertionError("unreachable")
