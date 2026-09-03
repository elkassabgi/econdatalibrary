"""Shared R2 (S3-compatible) client + helpers for the cutover scripts.

Reads credentials from the gitignored .env. Prefers WRITE keys (R2_WRITE_*) and
falls back to READ keys (R2_READ_*). Never prints secrets. A client is only built
when real creds are present; placeholder values (e.g. '...') are treated as absent
so a dry-run works with nothing configured and the real run fails loudly if asked
to write without write creds.
"""
from __future__ import annotations

import gzip as _gzip
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
