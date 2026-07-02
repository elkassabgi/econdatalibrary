"""S1 fetcher — IMF Primary Commodity Prices (PCPS) via DBnomics.

Public IMF PCPS data mirrored on DBnomics (api.db.nomics.world/v22), ~1,236
series (energy, metals, food, beverages, agricultural raw materials; monthly/
quarterly/annual, 1980-present). Single grouped parquet
clean_full/imf_commodity/imf_commodity.parquet, schema
(series_key, obs_date, value). series_key = "IMF_COMMODITY:<series_code>".

IMF revises history each release, so we re-fetch the whole dataset and MERGE
(dedup series_key+obs_date, new wins on revision, never-shrink). One logical unit
(the whole PCPS dataset, paged); a 200 that parses 0 rows from a real body is
structural. Vintage is the DBnomics dataset metadata (dir_hash + indexed_at +
nb_series), a cheap GET with observations=0 — it moves iff the dataset moved.
"""
from __future__ import annotations
import datetime as dt
import os
import time

import pyarrow as pa
import requests

from ... import config, blob, merge
from ..base import Result
from ._common import Tally, finalize
from ._vintage import UA

SOURCE = "imf_commodity"
DEDUP = ("series_key", "obs_date")

DBNOMICS = "https://api.db.nomics.world/v22"
PAGE_SIZE = 1000  # max series per request
# Cheap vintage probe: dataset metadata only (observations=0, one series doc).
VINTAGE_URL = f"{DBNOMICS}/series/IMF/PCPS?observations=0&limit=1"


def current_vintage(unit):
    """Cheap probe: DBnomics dataset vintage (dir_hash + indexed_at + nb_series).

    dir_hash is a content hash of the dataset directory that changes iff the
    underlying data changed; we combine it with indexed_at and nb_series so a
    re-conversion or series-count change is also caught. Returns None on any
    transient/undeterminable condition (strategy then fetches anyway, which is
    safe under merge dedup + never-shrink)."""
    try:
        r = requests.get(VINTAGE_URL, headers=UA, timeout=60)
    except (requests.Timeout, requests.ConnectionError):
        return None
    if r.status_code != 200:
        return None
    try:
        ds = r.json().get("dataset", {})
    except ValueError:
        return None
    parts = [str(ds.get("dir_hash") or ""),
             str(ds.get("indexed_at") or ds.get("updated_at") or ""),
             str(ds.get("nb_series") or "")]
    token = "|".join(p for p in parts if p)
    return token or None


def _fetch_page(offset: int):
    """Fetch one page of PCPS series with observations. Returns (data, transient).
    data is the parsed JSON dict on 200, else None. transient=True when the
    failure is retryable (timeout/5xx/429/network) so the caller can mark partial
    rather than laundering it into success."""
    url = (f"{DBNOMICS}/series/IMF/PCPS"
           f"?observations=1&limit={PAGE_SIZE}&offset={offset}")
    transient = False
    for attempt in range(3):
        try:
            r = requests.get(url, headers=UA, timeout=180)
        except (requests.Timeout, requests.ConnectionError):
            transient = True
            time.sleep(2 ** attempt)
            continue
        if r.status_code == 200:
            try:
                return r.json(), False
            except ValueError:
                transient = True
                time.sleep(2 ** attempt)
                continue
        if r.status_code in (429, 500, 502, 503, 504):
            transient = True
            time.sleep(2 ** attempt)
            continue
        # 400/404/other -> not retryable; treat as a structural/definitive break
        return None, False
    return None, transient


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
    path = os.path.join(out_dir, "imf_commodity.parquet")
    before = blob.row_count(path)
    tally = Tally()

    keys, dates, vals = [], [], []
    offset = 0
    total = None
    transient_hit = False
    saw_any_page = False

    while True:
        data, transient = _fetch_page(offset)
        if data is None:
            if transient:
                transient_hit = True
            else:
                # Non-retryable HTTP error on the very first page = structural
                # (dataset moved/removed). Mid-run we stop and merge what we have.
                if not saw_any_page:
                    tally.structural_unit()
                    return finalize(tally, before, None, source=SOURCE)
            break

        saw_any_page = True
        series_obj = data.get("series", {})
        docs = series_obj.get("docs", [])
        total = series_obj.get("num_found", total or 0)
        if not docs:
            break

        for series in docs:
            series_code = series.get("series_code", "")
            periods = series.get("period_start_day", [])
            values = series.get("value", [])
            if not series_code or not periods:
                continue
            skey = f"IMF_COMMODITY:{series_code}"
            for period_str, v in zip(periods, values):
                if v is None:
                    continue
                try:
                    obs_d = dt.date.fromisoformat(period_str)
                    fv = float(v)
                except (ValueError, TypeError):
                    continue
                if fv != fv:  # NaN
                    continue
                keys.append(skey)
                dates.append(obs_d)
                vals.append(fv)

        offset += len(docs)
        if offset >= (total or 0):
            break
        time.sleep(1.0)  # polite rate limit

    tbl = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date": pa.array(dates, pa.date32()),
        "value": pa.array(vals, pa.float64()),
    })

    # Nothing parsed at all.
    if tbl.num_rows == 0:
        if transient_hit:
            tally.transient_unit()
        elif saw_any_page:
            # A real 200 body that yielded 0 rows -> structural break.
            tally.structural_unit()
        else:
            tally.transient_unit()
        return finalize(tally, before, None, source=SOURCE)

    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    tally.added_unit(max(0, n - before))
    if transient_hit:
        # Got partial data (some pages timed out); record the transient so the
        # orchestrator does NOT stamp last_success and re-runs next tick.
        tally.transient_unit()
    return finalize(tally, n, md, source=SOURCE, series_cursors=_series_maxes(tbl))
