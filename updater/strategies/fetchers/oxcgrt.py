"""S1 fetcher — Oxford COVID-19 Government Response Tracker (OxCGRT).

Public, CC BY 4.0 (University of Oxford via GitHub raw). The national-level CSV is a
single closed bulk file (Jan 2020-Dec 2022; project ended, last commit 2023-06). We
re-fetch the WHOLE table by reusing jobs/ingest_oxcgrt.py's URL list + parse logic,
build a pyarrow table, and MERGE (dedup series_key+obs_date, never-shrink). One logical
sub-unit (the CSV); a 200 that parses 0 rows from a real body is structural.

Existing parquet: data/clean_full/oxcgrt/oxcgrt.parquet
  schema (series_key: str, obs_date: date32, value: double)
  series_key = "OXCGRT:{indicator}:{iso3}"

Vintage signal (registry adapter.vintage_signal): GitHub commit SHA / raw ETag for the
OxCGRT raw CSV — only rebuild if it moved. In practice the project ended, so this never
fires.
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
from ._vintage import UA, github_sha, http_vintage

SOURCE = "oxcgrt"
DEDUP = ("series_key", "obs_date")

# Reuse the ingester's URL list (first reachable wins). The first is the active
# covid-policy-dataset compact national CSV that built the published parquet.
URLS = [
    "https://raw.githubusercontent.com/OxCGRT/covid-policy-dataset/main/data/OxCGRT_compact_national_v1.csv",
    "https://raw.githubusercontent.com/OxCGRT/covid-policy-tracker/master/data/OxCGRT_latest.csv",
    "https://raw.githubusercontent.com/OxCGRT/covid-policy-tracker/master/data/OxCGRT_nat_latest.csv",
]

# (repo, path) pairs matching URLS, for the GitHub commit-SHA vintage probe.
GH_SOURCES = [
    ("OxCGRT/covid-policy-dataset", "data/OxCGRT_compact_national_v1.csv"),
    ("OxCGRT/covid-policy-tracker", "data/OxCGRT_latest.csv"),
    ("OxCGRT/covid-policy-tracker", "data/OxCGRT_nat_latest.csv"),
]


def current_vintage(unit):
    """Cheap probe that moves iff upstream data changed. Prefer the GitHub commit SHA
    for the active CSV path; fall back to the raw ETag/Last-Modified. Return None if
    nothing is cheaply determinable (strategy then fetches anyway — safe)."""
    for repo, path in GH_SOURCES:
        try:
            sha = github_sha(repo, path)
        except Exception:
            sha = None
        if sha:
            return f"gh:{sha}"
    for url in URLS:
        v = http_vintage(url)
        if v:
            return f"http:{v}"
    return None


def _parse_date(date_raw: str):
    if not date_raw:
        return None
    try:
        if len(date_raw) == 8 and date_raw.isdigit():       # YYYYMMDD
            return dt.date(int(date_raw[:4]), int(date_raw[4:6]), int(date_raw[6:8]))
        return dt.date.fromisoformat(date_raw[:10])
    except (ValueError, TypeError):
        return None


def _series_maxes(keys, dates):
    out = {}
    for k, d in zip(keys, dates):
        if d is None:
            continue
        if k not in out or d > out[k]:
            out[k] = d
    return {k: v.isoformat() for k, v in out.items()}


def _fetch(url):
    """GET the whole CSV. Returns (status, bytes|None) where status is one of
    'ok' / 'transient' / 'structural' so update() can tally honestly."""
    try:
        r = requests.get(url, headers=UA, timeout=180)
    except (requests.Timeout, requests.ConnectionError):
        return "transient", None
    if r.status_code in (429, 500, 502, 503, 504):
        return "transient", None
    if r.status_code != 200:
        return "miss", None              # 404 etc — try next candidate URL
    data = r.content
    if len(data) <= 10_000:
        return "structural", None        # 200 but a stub body
    return "ok", data


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "oxcgrt.parquet")
    before = blob.row_count(path)
    tally = Tally()

    data = None
    saw_transient = False
    for url in URLS:
        status, body = _fetch(url)
        if status == "ok":
            data = body
            break
        if status == "transient":
            saw_transient = True
        # 'miss' / 'structural' -> try the next candidate URL

    if data is None:
        # No URL yielded a usable body. If any candidate was a transient (timeout/5xx/
        # 429), treat the unit as transient (retry next tick); otherwise every candidate
        # 404'd/stubbed -> the whole mirror moved (structural break) -> DefinitiveError.
        if saw_transient:
            tally.transient_unit()
        else:
            tally.structural_unit()
        return finalize(tally, before, None, source=SOURCE)

    text = data.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    headers = reader.fieldnames or []

    iso3_col = next((h for h in headers if h.lower() in ("countrycode", "country_code", "iso")), None)
    date_col = next((h for h in headers if h.lower() in ("date",)), None)

    if not iso3_col or not date_col:
        tally.structural_unit()          # 200 with a real body but the columns moved
        return finalize(tally, before, None, source=SOURCE)

    skip = {iso3_col.lower(), date_col.lower(),
            "countryname", "country_name", "jurisdiction", "regioncode", "regionname"}
    value_cols = [h for h in headers if h.lower() not in skip and h.lower() not in ("", "m_flag")]

    keys, dates, vals = [], [], []
    for row in reader:
        iso3 = (row.get(iso3_col) or "").strip()
        obs_d = _parse_date((row.get(date_col) or "").strip())
        if not iso3 or obs_d is None:
            continue
        for col in value_cols:
            raw = (row.get(col) or "").strip()
            if not raw or raw in ("NA", "N/A"):
                continue
            try:
                v = float(raw)
            except (TypeError, ValueError):
                continue
            if v != v:  # NaN
                continue
            keys.append(f"OXCGRT:{col}:{iso3}")
            dates.append(obs_d)
            vals.append(v)

    tbl = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date": pa.array(dates, pa.date32()),
        "value": pa.array(vals, pa.float64()),
    })
    if tbl.num_rows == 0:
        tally.structural_unit()          # 200, real body, parsed nothing -> schema break
        return finalize(tally, before, None, source=SOURCE)

    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    tally.added_unit(max(0, n - before))
    return finalize(tally, n, md, source=SOURCE, series_cursors=_series_maxes(keys, dates))
