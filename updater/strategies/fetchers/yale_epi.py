"""S1 fetcher — Yale Environmental Performance Index (EPI), biennial country index.

CC BY 4.0 (Yale Center for Environmental Law & Policy). Single grouped parquet
clean_full/yale_epi/yale_epi.parquet, schema (series_key, obs_date, value),
series_key 'EPI:{variable}:{iso}'. The EPI results CSV is re-estimated/re-published
each release (currently epi2024results.csv, a wide format: code/iso/country columns
plus ~146 indicator columns). We re-fetch the WHOLE table and MERGE (dedup
series_key+obs_date, new wins on revision, never-shrink). One sub-unit (the CSV);
a 200 that parses 0 rows from a real body is structural.

vintage_signal: the registry asks to scrape the downloads page for a new
epiYYYYresults.csv link. The hardcoded results URL is served with a stable ETag /
Last-Modified / Content-Length, so http_vintage(URL) is the cheap probe that moves
iff Yale re-publishes (a new release year would also change the file at that URL or
ship a new URL; see open_question). No catalog/version endpoint exists.
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
from ._vintage import http_vintage, UA

SOURCE = "yale_epi"
DEDUP = ("series_key", "obs_date")

# Confirmed-working URL from https://epi.yale.edu/downloads (mirrors jobs/ingest_yale_epi.py).
RESULT_URLS = [
    ("https://epi.yale.edu/downloads/epi2024results.csv", 2024),
]


def current_vintage(unit):
    # Cheap HEAD probe: ETag / Last-Modified / Content-Length on the results CSV.
    # Moves iff Yale re-publishes the file. None if the server exposes none (the
    # strategy then fetches anyway, which merge dedups + never-shrinks).
    return http_vintage(RESULT_URLS[0][0])


def _parse_epi_csv(data: bytes, default_year: int):
    """Parse EPI results CSV — wide: iso column + many indicator columns.
    Byte-for-byte the same logic as jobs/ingest_yale_epi.parse_epi_csv."""
    text = data.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    headers = reader.fieldnames or []

    iso3_col = next((h for h in headers if h.lower() in
                     ("iso", "iso3", "iso_code", "country_iso3", "code")), None)
    year_col = next((h for h in headers if h.lower() in ("year", "yr")), None)
    if not iso3_col:
        return [], [], []

    skip = {(iso3_col or "").lower(), (year_col or "").lower(),
            "country", "region", "continent", "rank", "tier", "country.name"}

    keys, dates, vals = [], [], []
    for row in reader:
        iso3 = (row.get(iso3_col) or "").strip()
        if not iso3 or len(iso3) != 3:
            continue

        if year_col and row.get(year_col):
            try:
                yr = int(float(row[year_col]))
            except (ValueError, TypeError):
                yr = default_year
        else:
            yr = default_year
        obs_d = dt.date(yr, 12, 31)

        for col, raw in row.items():
            if col is None or col.lower().strip() in skip or not col:
                continue
            if not raw or str(raw).strip() in ("", "NA", "N/A", "nan", "#N/A", "-"):
                continue
            try:
                v = float(str(raw).replace(",", ""))
                if v != v:
                    continue
                keys.append(f"EPI:{col.strip()}:{iso3}")
                dates.append(obs_d)
                vals.append(v)
            except (TypeError, ValueError):
                pass

    return keys, dates, vals


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
    path = os.path.join(out_dir, "yale_epi.parquet")
    before = blob.row_count(path)
    tally = Tally()

    all_keys, all_dates, all_vals = [], [], []
    for url, yr in RESULT_URLS:
        try:
            r = requests.get(url, headers=UA, timeout=120, allow_redirects=True)
        except (requests.Timeout, requests.ConnectionError):
            tally.transient_unit()
            continue
        if r.status_code in (429, 500, 502, 503, 504):
            tally.transient_unit()
            continue
        if r.status_code != 200 or len(r.content) <= 500:
            # A wholesale 404 / tiny body on a hardcoded vintage URL is a structural
            # break (Yale moved/renamed the file), not a quiet day.
            tally.structural_unit()
            continue
        k, d, v = _parse_epi_csv(r.content, default_year=yr)
        if not v:
            tally.structural_unit()  # 200 with a real body but parsed nothing -> schema break
            continue
        all_keys.extend(k)
        all_dates.extend(d)
        all_vals.extend(v)

    tbl = pa.table({
        "series_key": pa.array(all_keys, pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals, pa.float64()),
    })

    if tbl.num_rows == 0:
        # Nothing parsed across all URLs; Tally already recorded transient/structural.
        return finalize(tally, before, None, source=SOURCE)

    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    tally.added_unit(max(0, n - before))
    return finalize(tally, n, md, source=SOURCE, series_cursors=_series_maxes(tbl))
