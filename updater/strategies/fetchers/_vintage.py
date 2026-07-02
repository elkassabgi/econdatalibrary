"""Shared vintage-detection for S1 (overwrite_if_changed) fetchers.

A "vintage" is a cheap token that changes iff upstream data changed — an HTTP
ETag / Last-Modified / Content-Length, a content hash, a GitHub commit SHA, a
Dataverse version id, a catalog 'updated' timestamp, etc. The S1 strategy skips
the (expensive) re-fetch when the stored vintage equals the current one, and only
re-pulls + overwrites when it moves. A fetcher's current_vintage() returns None
when it can't cheaply determine a vintage — the strategy then fetches anyway
(cadence-gated), which is safe because merge_and_write dedups + never shrinks.
"""
from __future__ import annotations
import hashlib
import time

import requests

from ...errors import TransientError

UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}


def http_vintage(url, session=None, tries=3):
    """ETag/Last-Modified/Content-Length token via HEAD (no body). Returns None if
    the server exposes none or HEAD is unsupported (caller then fetches anyway).
    Returns None (not raise) on transient errors here — detection failing should
    not fail the run; the actual fetch in update() handles transients properly."""
    s = session or requests
    for a in range(tries):
        try:
            r = s.head(url, headers=UA, timeout=60, allow_redirects=True)
        except (requests.Timeout, requests.ConnectionError):
            if a == tries - 1:
                return None
            time.sleep(2 ** a)
            continue
        if r.status_code in (429, 500, 502, 503, 504):
            if a == tries - 1:
                return None
            time.sleep(2 ** a)
            continue
        h = r.headers
        return h.get("ETag") or h.get("Last-Modified") or h.get("Content-Length")
    return None


def github_sha(repo, path=None, session=None):
    """Latest commit SHA for repo (optionally a path) — vintage for raw.githubusercontent mirrors."""
    s = session or requests
    params = {"per_page": 1}
    if path:
        params["path"] = path
    try:
        r = s.get(f"https://api.github.com/repos/{repo}/commits", headers=UA, params=params, timeout=60)
    except (requests.Timeout, requests.ConnectionError) as e:
        raise TransientError(f"github_sha {repo}: {e}")
    if r.status_code == 200 and r.json():
        return r.json()[0].get("sha")
    if r.status_code in (429, 500, 502, 503, 504):
        raise TransientError(f"github_sha {repo} HTTP {r.status_code}")
    return None


def content_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()[:16]
