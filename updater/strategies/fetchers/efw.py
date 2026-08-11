"""S1 fetcher — Economic Freedom of the World (Fraser Institute; annual, 165 juris.).

PERMISSION: written grant on file (DATABASE_LICENSES_VERBATIM.md, 2026-08-10): NC
re-host, attribution, link-back to efotw.org, ANNUAL refresh cadence. This fetcher
honours that cadence: registry polls irregular/annual, and each poll touches only
the one published API the dataset page itself loads.

Store: clean_full/efw/efw.parquet [series_key, obs_date, value], series_key
"<ISO3>:<measure>" (13 measures/jurisdiction), obs_date Dec-31. Built by
jobs/ingest_efw.py (probe-verified endpoint, 58,486 obs / 2,145 series, 1970..2023).

VINTAGE: the endpoint serves the ENTIRE dataset as one JSON body with no useful
HTTP validators to rely on, so the vintage is a content hash of the body. The GET
is 2.8 MB — cheap at the registry's cadence — and a hash only moves when Fraser
actually revises data, which is exactly the bulk_snapshot_if_changed contract.
OVERWRITE, NOT MERGE: annual editions restate history in place (same reason as
fed_board's overwrite note); never-shrink is enforced by comparing row counts.
"""
from __future__ import annotations

import hashlib
import os

import requests

from ... import config, blob
from ..base import Result
from ._common import Tally, finalize
from ._vintage import UA

URL = "https://efotw.org/api/v1/ftw_get_all_data"
SOURCE = "efw"


def _get(timeout=300):
    return requests.get(URL, headers=UA, timeout=timeout)


def current_vintage(unit):
    try:
        r = _get()
    except (requests.Timeout, requests.ConnectionError):
        return None
    if r.status_code != 200 or not r.content:
        return None
    return hashlib.sha1(r.content).hexdigest()


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "efw.parquet")
    before = blob.row_count(path)
    tally = Tally()

    try:
        r = _get()
    except (requests.Timeout, requests.ConnectionError):
        tally.transient_unit()
        return finalize(tally, before, None, source=SOURCE)
    if r.status_code in (429, 500, 502, 503, 504):
        tally.transient_unit()
        return finalize(tally, before, None, source=SOURCE)
    if r.status_code != 200:
        tally.structural_unit()
        return finalize(tally, before, None, source=SOURCE)

    from jobs.ingest_efw import build_table
    import json as _json
    try:
        data = r.json()
    except _json.JSONDecodeError:
        tally.structural_unit()   # 200 with a non-JSON body is a schema break
        return finalize(tally, before, None, source=SOURCE)
    tbl, _countries, _labels, _skipped = build_table(data)
    if tbl.num_rows == 0:
        tally.structural_unit()   # real body that parses to nothing
        return finalize(tally, before, None, source=SOURCE)
    if tbl.num_rows < before:
        # never-shrink: a smaller snapshot is a publisher regression or a partial
        # body — keep the store and flag, don't overwrite.
        tally.structural_unit()
        return finalize(tally, before, None, source=SOURCE)

    import pyarrow.parquet as pq
    pq.write_table(tbl, path)
    tally.added_unit(max(0, tbl.num_rows - before))
    maxes = {}
    for k, d in zip(tbl.column("series_key").to_pylist(),
                    tbl.column("obs_date").to_pylist()):
        if k not in maxes or d > maxes[k]:
            maxes[k] = d
    return finalize(tally, tbl.num_rows, None, source=SOURCE,
                    series_cursors={k: v.isoformat() for k, v in maxes.items()})
