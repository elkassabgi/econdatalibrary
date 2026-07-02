"""S1 fetcher — World Bank World Governance Indicators (annual, 1996-present).

CC BY 4.0. Single grouped parquet clean_full/wgi/wgi.parquet, schema
(series_key, obs_date, value). series_key = WGI:{indicator_code}:{iso3}
(e.g. WGI:GOV_WGI_CC.EST:AIA). The World Bank re-estimates the whole WGI history
each annual release, so we re-fetch the entire workbook and MERGE (dedup
series_key+obs_date, new wins on revision, never-shrink). One sub-unit (the
workbook); a 200 that parses 0 rows from a real body is structural.

Upstream is a single all-years Excel inside a ZIP — no per-year endpoint — so the
only correct refresh is a full re-download gated on a cheap vintage probe (the
databank ZIP exposes a stable ETag/Last-Modified). The doc-page WGIData.xlsx URL
is dead (404); the live, current workbook (Last-Modified 2026-03-19, data through
2024) is the databank WGI_EXCEL.zip — the exact URL the existing ingest used.
"""
from __future__ import annotations
import os

import pyarrow as pa
import requests

from ... import config, blob, merge
from ..base import Result
from ._common import Tally, finalize
from ._vintage import http_vintage, UA

# Reuse the existing ingester's URL list + parse logic verbatim so output matches
# the published parquet byte-for-byte in shape (same keys, same value mapping).
from jobs.ingest_wgi import WGI_URLS, parse_wgi_xlsx

SOURCE = "wgi"
DEDUP = ("series_key", "obs_date")
# The databank ZIP is the candidate that actually serves a live workbook; probe it
# first for the cheap vintage signal and try it first on the full fetch.
PRIMARY_URL = "https://databank.worldbank.org/data/download/WGI_EXCEL.zip"


def current_vintage(unit):
    # Cheap HEAD probe (ETag/Last-Modified/Content-Length) on the live ZIP.
    return http_vintage(PRIMARY_URL)


def _fetch(url, retries=3):
    """GET bytes; returns (data, kind) where kind is 'ok'|'transient'|'structural'|None.
    'transient' = timeout/429/5xx/network (retry next tick); 'structural' = 403/404 or
    a 200 with a tiny body; None = exhausted retries with only transient errors."""
    last = None
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=120, allow_redirects=True, stream=True)
        except (requests.Timeout, requests.ConnectionError):
            last = "transient"
            continue
        if r.status_code == 200:
            chunks = []
            for chunk in r.iter_content(chunk_size=1 << 20):
                if chunk:
                    chunks.append(chunk)
            data = b"".join(chunks)
            if len(data) > 10_000:
                return data, "ok"
            last = "structural"  # 200 but trivially small body
            continue
        if r.status_code in (429, 500, 502, 503, 504):
            last = "transient"
            continue
        if r.status_code in (403, 404):
            return None, "structural"  # dead/moved URL — try next candidate
        last = "structural"
    return None, last


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


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "wgi.parquet")
    before = blob.row_count(path)
    tally = Tally()

    # Try the live primary first, then the remaining registry candidates.
    urls = [PRIMARY_URL] + [u for u in WGI_URLS if u != PRIMARY_URL]
    data = None
    saw_transient = False
    for url in urls:
        d, kind = _fetch(url)
        if kind == "ok" and d:
            data = d
            break
        if kind == "transient":
            saw_transient = True
        # structural/None on this candidate -> fall through to the next URL

    if data is None:
        # No candidate yielded a workbook. If any attempt was transient, surface it as
        # partial (retry next tick); otherwise every live candidate is gone (structural).
        if saw_transient:
            tally.transient_unit()
        else:
            tally.structural_unit()
        return finalize(tally, before, None, source=SOURCE)

    keys, dates, vals = parse_wgi_xlsx(data)

    tbl = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date":   pa.array(dates, pa.date32()),
        "value":      pa.array(vals, pa.float64()),
    })
    if tbl.num_rows == 0:
        tally.structural_unit()  # 200 + real workbook but parsed nothing -> schema break
        return finalize(tally, before, None, source=SOURCE)

    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    tally.added_unit(max(0, n - before))
    return finalize(tally, n, md, source=SOURCE, series_cursors=_series_maxes(tbl))
