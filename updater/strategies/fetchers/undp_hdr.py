"""S1 fetcher — UNDP Human Development Report composite indices (annual, 190+ countries).

License CC BY 3.0 IGO. Single grouped parquet clean_full/undp_hdr/undp_hdr.parquet,
schema (series_key, obs_date, value); keys are {indicator}:{ISO3} (e.g. hdi:AFG) with
obs_date = Dec 31 of the reference year. UNDP ships one composite "complete time series"
CSV per HDR edition that carries the full 1990-present panel for ~40 indicators, so a
per-obs delta is meaningless: we detect a new EDITION (or a Last-Modified bump on the
current edition) and re-fetch the whole CSV, then MERGE (dedup series_key+obs_date,
new wins on revision, never-shrink). One sub-unit (the composite CSV); a 200 that
parses 0 rows from a real body is structural.

Edition handling: the registry's hardcoded ingester points at the 2023-24 edition
(data -> 2022). UNDP has since published the HDR 2025 edition
(.../2025_HDR/HDR25_Composite_indices_complete_time_series.csv), which extends the
panel to 2023. We prefer the newest live edition and fall back down the list, so a
new edition is picked up without hand-editing. Parse logic is reused verbatim from
jobs/ingest_undp_hdr.ingest_composite_csv (handles both long and wide CSV layouts).
"""
from __future__ import annotations
import datetime as dt
import os

import pyarrow as pa
import requests

from ... import config, blob, merge
from ._common import Tally, finalize
from ._vintage import http_vintage, UA

SOURCE = "undp_hdr"
DEDUP = ("series_key", "obs_date")

# Composite "complete time series" CSV per edition, newest first. Each one carries the
# full 1990-present panel; we use the first that responds 200 (preferring newer editions
# which extend the panel by a year). vintage_signal: edition-parameterized composite URL
# + Last-Modified on the chosen edition.
COMPOSITE_URLS = [
    "https://hdr.undp.org/sites/default/files/2025_HDR/HDR25_Composite_indices_complete_time_series.csv",
    "https://hdr.undp.org/sites/default/files/2023-24_HDR/HDR23-24_Composite_indices_complete_time_series.csv",
]


def _pick_live_url(session=None):
    """Return (url, vintage_token) for the newest edition that responds (HEAD 200),
    or (None, None) if none answer. vintage = ETag/Last-Modified/Content-Length so the
    token changes both on a new edition (different URL/token) and on a same-edition
    revision (Last-Modified bump)."""
    s = session or requests
    for url in COMPOSITE_URLS:
        try:
            r = s.head(url, headers=UA, timeout=60, allow_redirects=True)
        except (requests.Timeout, requests.ConnectionError):
            continue
        if r.status_code == 200:
            h = r.headers
            tok = h.get("ETag") or h.get("Last-Modified") or h.get("Content-Length")
            # Tag the vintage with the edition so a new edition always moves the token,
            # even if a server happened to reuse a Last-Modified value.
            edition = url.split("/files/")[-1].split("/")[0]
            return url, (f"{edition}:{tok}" if tok else edition)
    return None, None


def current_vintage(unit):
    """Cheap probe: HEAD the newest live composite CSV; token = edition + ETag/Last-Modified.
    None if no edition answers (strategy then fetches anyway, which is safe)."""
    _, tok = _pick_live_url()
    return tok


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


def update(unit, since):
    from jobs.ingest_undp_hdr import ingest_composite_csv  # reuse existing parse logic

    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "undp_hdr.parquet")
    before = blob.row_count(path)
    tally = Tally()

    url, _ = _pick_live_url()
    if url is None:
        # No edition responded to HEAD — treat as transient (network/5xx); existing data kept.
        tally.transient_unit()
        return finalize(tally, before, None, source=SOURCE)

    try:
        r = requests.get(url, headers=UA, timeout=120)
    except (requests.Timeout, requests.ConnectionError):
        tally.transient_unit()
        return finalize(tally, before, None, source=SOURCE)
    if r.status_code in (429, 500, 502, 503, 504):
        tally.transient_unit()
        return finalize(tally, before, None, source=SOURCE)
    if r.status_code != 200 or len(r.content) <= 1000:
        # A non-200 (or a tiny body where a real CSV is expected) is a structural break:
        # the hardcoded edition URL has moved/404'd. Surface it, don't fake success.
        tally.structural_unit()
        return finalize(tally, before, None, source=SOURCE)

    keys, dates, vals = [], [], []
    ingest_composite_csv(r.content, keys, dates, vals)

    tbl = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date":   pa.array(dates, pa.date32()),
        "value":      pa.array(vals, pa.float64()),
    })
    if tbl.num_rows == 0:
        tally.structural_unit()  # 200 with a real body but parsed nothing -> schema break
        return finalize(tally, before, None, source=SOURCE)

    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    tally.added_unit(max(0, n - before))
    return finalize(tally, n, md, source=SOURCE, series_cursors=_series_maxes(tbl))
