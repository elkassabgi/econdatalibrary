"""S5 bulk fetcher — UCDP/PRIO Armed Conflict Dataset (ACD). Single annual bulk zip → one parquet.

CC BY 4.0 (UCDP), no key for the bulk CSV path. Single parquet clean_full/ucdp/acd.parquet, schema
(series_key, obs_date, value); series_key = "UCDP:ACD:{variable}:{id}" (raw conflict-year vars keyed by
conflict_id, recomputed country-year aggregates keyed by gwno_loc). obs_date = Dec-31 of the conflict year.

UCDP ships ONE version-pinned zip per annual release (ucdp-prio-acd-<ver>-csv.zip, ver = <YY>1). There is
no server-side date filter and no manifest, but the downloads listing names exactly one current ACD file,
so the VINTAGE is (latest zip filename + its Last-Modified). A version bump (241→251→261…) or a new
Last-Modified means a new release; unchanged means skip the whole re-download. Because it is a SINGLE
dataset, the unit-level vintage is the entire gate — no per-domain sidecar is needed (unlike faostat).

The parse REUSES the production parser jobs.ingest_ucdp.parse_acd, so the emitted series_key is
byte-identical to what is already on disk — a re-merge dedups on (series_key, obs_date) and updates revised
conflict-years without duplicating or shrinking (the duplication invariant). Store I/O via blob (R36); the
whole file is re-parsed each vintage (country-year aggregates are rebuilt from the full file).

HONEST-STATUS: listing/HEAD/download failing after retries -> TransientError (partial, retried, data kept);
a 200 zip that parses to ZERO rows -> structural (kept, flagged); parsed rows -> added.
"""
from __future__ import annotations
import os
import re
import time

import pyarrow as pa
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Tally, finalize
from jobs import ingest_ucdp as ig   # reuse the production parser (byte-identical series_key)

SOURCE = "ucdp"
DOWNLOADS = "https://ucdp.uu.se/downloads/"
BASE = "https://ucdp.uu.se/downloads/ucdpprio/"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
PARQUET = "acd.parquet"
DEDUP = ("series_key", "obs_date")
_TRANSIENT_HTTP = (429, 500, 502, 503, 504)
_ACD_RE = re.compile(r'ucdp-prio-acd-(\d+)-csv\.zip')


def _discover(sess):
    """Scrape the downloads listing for the newest ACD zip. Returns (url, vintage_token).
    vintage_token = '<zipname>|<Last-Modified>'. Raises TransientError on a retryable failure."""
    try:
        r = sess.get(DOWNLOADS, headers=UA, timeout=60)
    except (requests.Timeout, requests.ConnectionError) as e:
        raise TransientError(f"ucdp: listing {e}")
    if r.status_code in _TRANSIENT_HTTP:
        raise TransientError(f"ucdp: listing HTTP {r.status_code}")
    if r.status_code != 200:
        raise TransientError(f"ucdp: listing HTTP {r.status_code}")
    versions = _ACD_RE.findall(r.text)
    if not versions:
        raise TransientError("ucdp: no ACD zip in listing")
    latest = max(versions, key=lambda v: int(v))
    zipname = f"ucdp-prio-acd-{latest}-csv.zip"
    url = BASE + zipname
    # HEAD for Last-Modified (best-effort; version string alone is a sufficient change signal)
    lastmod = ""
    try:
        h = sess.head(url, headers=UA, timeout=30, allow_redirects=True)
        if h.status_code == 200:
            lastmod = h.headers.get("Last-Modified", "")
    except (requests.Timeout, requests.ConnectionError):
        pass
    return url, f"{zipname}|{lastmod}"


def current_vintage(unit) -> str | None:
    """Cheap probe: the (latest ACD zip filename + Last-Modified) token. None if undeterminable."""
    try:
        _, token = _discover(requests.Session())
        return token
    except TransientError:
        return None


def _download(sess, url, tries=4) -> bytes:
    for a in range(tries):
        try:
            r = sess.get(url, headers=UA, timeout=180)
        except (requests.Timeout, requests.ConnectionError) as e:
            if a == tries - 1:
                raise TransientError(f"ucdp: download {e}")
            time.sleep(min(5 * (a + 1), 30)); continue
        if r.status_code == 200:
            return r.content
        if r.status_code in _TRANSIENT_HTTP:
            if a == tries - 1:
                raise TransientError(f"ucdp: download HTTP {r.status_code}")
            time.sleep(min(5 * (a + 1), 30)); continue
        raise TransientError(f"ucdp: download HTTP {r.status_code}")
    raise TransientError("ucdp: download retry budget exhausted")


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, PARQUET)
    sess = requests.Session()
    tally = Tally()

    url, token = _discover(sess)          # TransientError on failure -> partial, data kept
    before = blob.row_count(path) if blob.exists(path) else 0

    try:
        data = _download(sess, url)
    except TransientError:
        tally.transient_unit()
        return finalize(tally, before, since or None, source=SOURCE)

    keys, dates, vals = ig.parse_acd(data)
    if not keys:
        # a real 200 zip that parses to nothing is a structural break; keep existing data.
        tally.structural_unit()
        return finalize(tally, before, since or None, source=SOURCE)

    tbl = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date": pa.array(dates, pa.date32()),
        "value": pa.array(vals, pa.float64()),
    })
    try:
        n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    except DefinitiveError:
        # a >3% shrink guard tripped on a revision — keep data, surface partial, don't seal vintage.
        tally.transient_unit()
        return finalize(tally, before, since or None, source=SOURCE)

    tally.added_unit(max(0, n - before))
    # series_cursors: the CSV-coherence step maps each changed series_key -> catalog id, so a
    # bulk source that merges rows MUST report which series changed (else "no series_cursors for
    # N merged obs" -> partial). All ucdp series are catalogued, so this maps cleanly.
    res = finalize(tally, n, md, source=SOURCE, series_cursors=_series_maxes(tbl))
    # single-dataset bulk gate token — let the strategy persist it (skip next unchanged tick).
    if res.status in ("ok", "no_change"):
        res.new_vintage = token
    return res


def _series_maxes(tbl):
    out = {}
    for k, d in zip(tbl.column("series_key").to_pylist(), tbl.column("obs_date").to_pylist()):
        if d is None:
            continue
        if k not in out or d > out[k]:
            out[k] = d
    return {k: v.isoformat() for k, v in out.items()}
