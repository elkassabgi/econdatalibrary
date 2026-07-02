"""S1 fetcher — Transparency International Corruption Perceptions Index (CPI).

License: CC BY-ND 4.0 (TI) / CC BY 4.0 (OWID). ~180 countries, annual, 2012-present.
Single grouped parquet clean_full/transparency_ti/transparency_ti.parquet, schema
(series_key, obs_date, value) with series_key = "TI_CPI:cpi_score:{iso3}" and
obs_date = Dec-31 of the survey year.

Vintage: OWID's grapher CSV returns ALL years on every call, so a per-obs delta is
meaningless — a new edition = a newer max year. The OWID host serves the CSV
dynamically (Last-Modified == request time, no ETag/Content-Length), so HEAD-based
http_vintage is useless here. Instead we cheaply GET the small (~67KB) CSV and use
its MAX YEAR as the vintage token (registry vintage_signal: "max-year compare vs
stored parquet; rewrite only if a newer year appears"). We re-fetch the whole table
each time and MERGE (dedup series_key+obs_date, new wins on a CPI revision, never
shrink). A 200 that parses 0 rows from a real body is a structural break.
"""
from __future__ import annotations
import csv
import datetime as dt
import io
import os

import pyarrow as pa
import requests

from ... import config, blob, merge
from ..base import Result
from ._common import Tally, finalize
from ._vintage import UA

# OWID mirrors TI CPI — confirmed working 2026-06 (reliably public, CC BY).
URL = "https://ourworldindata.org/grapher/ti-corruption-perception-index.csv"
# TI CDN xlsx fallback — derived from current year (CPI20XX), frequently 403.
TI_CDN_TMPL = "https://files.transparencycdn.org/images/CPI{yr}-Results-and-trends.xlsx"
SOURCE = "transparency_ti"
DEDUP = ("series_key", "obs_date")


def _parse_owid_csv(data: bytes):
    """OWID grapher CSV: Entity, Code, Year, Corruption Perceptions Index, ...
    Returns (keys, dates, vals, max_year)."""
    text = data.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    headers = reader.fieldnames or []
    code_col = next((h for h in headers if h.strip() in ("Code", "iso3", "ISO3", "code")), None)
    entity_col = next((h for h in headers if h.strip().lower() in ("entity", "country", "name")), None)
    year_col = next((h for h in headers if h.strip().lower() in ("year", "yr")), None)
    score_col = next((h for h in headers if "corruption" in h.lower() or "cpi" in h.lower()
                      or "perception" in h.lower()), None)
    if not year_col or not score_col:
        return [], [], [], None

    keys, dates, vals = [], [], []
    max_year = None
    for row in reader:
        iso3 = (row.get(code_col) or "").strip() if code_col else ""
        if not iso3 and entity_col:
            iso3 = (row.get(entity_col) or "").strip().replace(" ", "_")[:30]
        if not iso3:
            continue
        yr_raw = (row.get(year_col) or "").strip()
        try:
            yr = int(float(yr_raw))
            obs_d = dt.date(yr, 12, 31)
        except (ValueError, TypeError):
            continue
        v_raw = (row.get(score_col) or "").strip()
        if not v_raw or v_raw in ("NA", "N/A", "nan"):
            continue
        try:
            v = float(v_raw)
        except (ValueError, TypeError):
            continue
        if v != v:  # NaN
            continue
        keys.append(f"TI_CPI:cpi_score:{iso3}")
        dates.append(obs_d)
        vals.append(v)
        if max_year is None or yr > max_year:
            max_year = yr
    return keys, dates, vals, max_year


def current_vintage(unit):
    """Cheap probe: GET the small OWID CSV and return its max year as the vintage.
    Changes iff a newer CPI edition appears. None if it can't be determined cheaply
    (the strategy then fetches anyway, which is safe)."""
    try:
        r = requests.get(URL, headers=UA, timeout=60, allow_redirects=True)
    except (requests.Timeout, requests.ConnectionError):
        return None
    if r.status_code != 200 or len(r.content) < 200:
        return None
    _, _, _, max_year = _parse_owid_csv(r.content)
    return f"owid:maxyear:{max_year}" if max_year is not None else None


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
    path = os.path.join(out_dir, "transparency_ti.parquet")
    before = blob.row_count(path)
    tally = Tally()

    try:
        r = requests.get(URL, headers=UA, timeout=120, allow_redirects=True)
    except (requests.Timeout, requests.ConnectionError):
        tally.transient_unit()
        return finalize(tally, before, None, source=SOURCE)
    if r.status_code in (429, 500, 502, 503, 504):
        tally.transient_unit()
        return finalize(tally, before, None, source=SOURCE)
    if r.status_code != 200 or len(r.content) < 200:
        tally.structural_unit()
        return finalize(tally, before, None, source=SOURCE)

    keys, dates, vals, _ = _parse_owid_csv(r.content)
    tbl = pa.table({"series_key": pa.array(keys, pa.string()),
                    "obs_date": pa.array(dates, pa.date32()),
                    "value": pa.array(vals, pa.float64())})
    if tbl.num_rows == 0:
        tally.structural_unit()  # 200 but parsed nothing -> schema break, not a quiet day
        return finalize(tally, before, None, source=SOURCE)

    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    tally.added_unit(max(0, n - before))
    return finalize(tally, n, md, source=SOURCE, series_cursors=_series_maxes(tbl))
