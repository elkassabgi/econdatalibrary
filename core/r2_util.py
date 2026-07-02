"""Shared R2 (S3-compatible) client + helpers for the cutover scripts.

Reads credentials from the gitignored .env. Prefers WRITE keys (R2_WRITE_*) and
falls back to READ keys (R2_READ_*). Never prints secrets. A client is only built
when real creds are present; placeholder values (e.g. '...') are treated as absent
so a dry-run works with nothing configured and the real run fails loudly if asked
to write without write creds.
"""
from __future__ import annotations

import os

_THIS = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(_THIS, ".."))
ENV = os.path.join(ROOT, ".env")


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
