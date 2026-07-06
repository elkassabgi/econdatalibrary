"""Shared machinery for the IEP (Institute for Economics & Peace) fetchers:
gpi (kept separate — multi-URL fallback), gti, ppi, etr.

All four IEP public-release datasets were granted to the Elkassabgi Data Library
for non-commercial re-hosting under CC BY-NC-SA 4.0 (2026-07-06, via IEP's
data-licensing confirmation, info@economicsandpeace.org). Attribute the Institute
for Economics & Peace and apply ShareAlike; no commercial use.

Each concrete fetcher module defines SOURCE, URLS, and a parse(xlsx_bytes) ->
(keys, dates, vals) function, then delegates current_vintage()/update() here so the
honest-status contract (Tally/finalize), atomic dedup/never-shrink merge, and
per-series cursors are identical across the family (mirrors _giant.py's role for
the giants).
"""
from __future__ import annotations
import os

import pyarrow as pa
import requests

from ... import config, blob, merge
from ..base import Result
from ._common import Tally, finalize
from ._vintage import UA

DEDUP = ("series_key", "obs_date")
_TRANSIENT_HTTP = (429, 500, 502, 503, 504)


def probe_vintage(urls):
    """Cheap change-signal: ETag/Last-Modified of the first URL that returns 200.
    A 404 (stale versioned URL) is skipped so a dead candidate never yields a junk
    vintage. Returns None if none respond 200 (strategy then fetches anyway; the
    merge dedups + never-shrinks, so that is safe)."""
    for url in urls:
        try:
            r = requests.head(url, headers=UA, timeout=60, allow_redirects=True)
        except (requests.Timeout, requests.ConnectionError):
            continue
        if r.status_code != 200:
            continue
        v = r.headers.get("ETag") or r.headers.get("Last-Modified") or r.headers.get("Content-Length")
        if v:
            return v
    return None


def _series_maxes(tbl):
    out = {}
    if tbl.num_rows == 0:
        return out
    for k, d in zip(tbl.column("series_key").to_pylist(), tbl.column("obs_date").to_pylist()):
        if d is None:
            continue
        if k not in out or d > out[k]:
            out[k] = d
    return {k: v.isoformat() for k, v in out.items()}


def run_update(source: str, urls, parse_fn) -> Result:
    """Try each URL in order; the first that parses >0 rows wins. Publish ONLY via
    the atomic dedup/revision-wins/never-shrink merge. Honest Tally: network/5xx/429
    -> transient; 200 that parses 0 rows (or a hard 4xx / unparseable body) ->
    structural (finalize raises DefinitiveError; existing data kept)."""
    out_dir = config.source_dir(source)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{source}.parquet")
    before = blob.row_count(path)
    tally = Tally()

    keys, dates, vals = [], [], []
    got_data = False
    for url in urls:
        try:
            r = requests.get(url, headers=UA, timeout=120, allow_redirects=True)
        except (requests.Timeout, requests.ConnectionError):
            tally.transient_unit()
            continue
        if r.status_code in _TRANSIENT_HTTP:
            tally.transient_unit()
            continue
        if r.status_code != 200 or len(r.content) < 1000:
            tally.structural_unit()
            continue
        try:
            k, d, v = parse_fn(r.content)
        except Exception:
            tally.structural_unit()  # 200 with a body we couldn't parse -> schema break
            continue
        if v:
            keys, dates, vals = k, d, v
            got_data = True
            break
        tally.structural_unit()  # 200, real body, parsed 0 rows -> structural

    if not got_data:
        return finalize(tally, before, None, source=source)

    tbl = pa.table({"series_key": pa.array(keys, pa.string()),
                    "obs_date": pa.array(dates, pa.date32()),
                    "value": pa.array(vals, pa.float64())})
    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    tally.added_unit(max(0, n - before))
    return finalize(tally, n, md, source=source, series_cursors=_series_maxes(tbl))
