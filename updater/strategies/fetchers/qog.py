"""S1 fetcher — Quality of Government (QoG) Standard time-series dataset.

CC BY 4.0 (University of Gothenburg). One grouped parquet
clean_full/qog/qog.parquet, schema (series_key, obs_date, value) where
series_key='QOG:<variable>:<ccode>' and obs_date=Dec-31 of the country-year.

QoG re-releases the WHOLE wide country-year CSV once per vintage (tagged
'jan<yy>'); there is no date filter, so we re-fetch the whole file and MERGE
(dedup series_key+obs_date, new wins on revision, never-shrink). One sub-unit
(the workbook): a 200 that parses 0 rows from a real body is structural.

The hardcoded ingester pins jan25/jan24; this fetcher AUTO-DISCOVERS the newest
'jan<yy>' vintage on qogdata.pol.gu.se (probing a small window of years) so it
picks up jan26+ without code edits, and gates the (large) re-fetch on the
discovered vintage's ETag/Last-Modified via http_vintage.
"""
from __future__ import annotations
import csv
import datetime as dt
import io
import os

import pyarrow as pa
import requests

from ... import config, merge, blob
from ..base import Result
from ._common import Tally, finalize
from ._vintage import http_vintage, UA

SOURCE = "qog"
DEDUP = ("series_key", "obs_date")
BASE = "https://www.qogdata.pol.gu.se/data/qog_std_ts_{tag}.csv"
MIN_BYTES = 100_000  # a real vintage CSV is tens of MB; anything smaller is an error page

# Columns that identify the observation (not data variables) — mirror jobs/ingest_qog.py
ID_COLS = frozenset({
    "cname_qog", "cname", "year", "ccodecow", "ccodealp", "ccodealp_year",
    "ccode_qog", "cname_year", "ccode", "version", "date", "ccowhist",
    "ccowhist_year",
})


def _candidate_tags():
    """Newest-first 'jan<yy>' vintage tags to probe. QoG publishes a January
    vintage tagged jan<yy> (e.g. jan26 appeared 2026-02). We look one year ahead
    of today (in case the new vintage drops early) down through recent years so a
    fresh release is found without editing this file."""
    yy = dt.date.today().year
    years = [yy + 1, yy, yy - 1, yy - 2, yy - 3]
    return [f"jan{y % 100:02d}" for y in years]


def _resolve_vintage_url(session=None):
    """Return (url, vintage_token) for the newest live vintage, or (None, None).

    HEAD each candidate newest-first; the first 200 whose Content-Length looks
    like a real CSV (>100KB) wins. vintage_token = its ETag/Last-Modified/CL."""
    s = session or requests
    for tag in _candidate_tags():
        url = BASE.format(tag=tag)
        try:
            r = s.head(url, headers=UA, timeout=60, allow_redirects=True)
        except (requests.Timeout, requests.ConnectionError):
            continue
        if r.status_code != 200:
            continue
        cl = r.headers.get("Content-Length")
        if cl is not None:
            try:
                if int(cl) < MIN_BYTES:
                    continue
            except ValueError:
                pass
        token = r.headers.get("ETag") or r.headers.get("Last-Modified") or cl
        return url, (token or tag)
    return None, None


def current_vintage(unit):
    """Cheap probe: newest live vintage's ETag/Last-Modified (changes iff data
    moved). None if no vintage URL responds cheaply (strategy then fetches anyway)."""
    url, token = _resolve_vintage_url()
    if url is None:
        return None
    # Prefer the standard http_vintage token for the resolved URL; fall back to
    # the token gathered during discovery.
    return http_vintage(url) or token


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


def _parse_csv(content: bytes):
    """Wide-to-long melt mirroring jobs/ingest_qog.py. Returns (keys, dates, vals)."""
    try:
        text = content.decode("utf-8", errors="replace")
    except Exception:
        text = content.decode("latin-1", errors="replace")

    reader = csv.DictReader(io.StringIO(text))
    headers = reader.fieldnames or []
    data_cols = [h for h in headers if h not in ID_COLS and h]

    keys, dates, vals = [], [], []
    seen: set[tuple] = set()
    for row in reader:
        yr_raw = row.get("year", "")
        if not yr_raw:
            continue
        try:
            yr = int(float(yr_raw))
            if not (1800 <= yr <= 2030):
                continue
        except (ValueError, TypeError):
            continue
        obs_date = dt.date(yr, 12, 31)

        ccode = row.get("ccodealp", "").strip()  # ISO3 preferred
        if not ccode:
            ccode = row.get("cname", "").strip()[:30]
        if not ccode:
            continue

        for col in data_cols:
            v_raw = row.get(col, "")
            if not v_raw or v_raw in ("", "NA", "N/A", ".", ".."):
                continue
            try:
                v = float(v_raw)
                if v != v:  # NaN
                    continue
            except (ValueError, TypeError):
                continue
            series_key = f"QOG:{col}:{ccode}"
            tok = (series_key, obs_date)
            if tok in seen:
                continue
            seen.add(tok)
            keys.append(series_key)
            dates.append(obs_date)
            vals.append(v)
    return keys, dates, vals


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "qog.parquet")
    before = blob.row_count(path)
    tally = Tally()

    url, _token = _resolve_vintage_url()
    if url is None:
        # No live vintage URL — hardcoded host moved / all candidates 404. Surface
        # as transient (retry next tick); do NOT launder into no_change.
        tally.transient_unit()
        return finalize(tally, before, None, source=SOURCE)

    try:
        r = requests.get(url, headers=UA, timeout=300)
    except (requests.Timeout, requests.ConnectionError):
        tally.transient_unit()
        return finalize(tally, before, None, source=SOURCE)
    if r.status_code in (429, 500, 502, 503, 504):
        tally.transient_unit()
        return finalize(tally, before, None, source=SOURCE)
    if r.status_code != 200 or len(r.content) < MIN_BYTES:
        # A 200 with a tiny body (error page) or a 4xx is a structural break for a
        # source whose real file is tens of MB.
        tally.structural_unit()
        return finalize(tally, before, None, source=SOURCE)

    keys, dates, vals = _parse_csv(r.content)
    tbl = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date": pa.array(dates, pa.date32()),
        "value": pa.array(vals, pa.float64()),
    })
    if tbl.num_rows == 0:
        tally.structural_unit()  # 200 with a real body but parsed nothing -> schema break
        return finalize(tally, before, None, source=SOURCE)

    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    tally.added_unit(max(0, n - before))
    return finalize(tally, n, md, source=SOURCE, series_cursors=_series_maxes(tbl))
