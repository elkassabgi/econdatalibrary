"""S1 fetcher — World Happiness Report (annual country panel).

CC BY 4.0 (Gallup / SDSN). Single grouped parquet clean_full/whr/whr.parquet,
schema (series_key, obs_date, value), keys WHR:{variable}:{iso3|country}. WHR is
re-published yearly (and history is revised), so we re-fetch the whole table and
MERGE (dedup series_key+obs_date, new wins on revision, never-shrink).

COVERAGE BLOCKER (see registry adapter.open_question): the full 6-factor panel
lives in S3 XLSX appendices that return 403 even with a worldhappiness.report
Referer, and 6/7 OWID grapher factor CSV slugs now 404. Only the OWID Cantril-
ladder CSV (life satisfaction) currently serves — verified live 2026-06. So this
fetcher publishes the ladder slice that the existing parquet already holds and
keeps the same series_key shape; it does not fabricate the missing factors. If a
human supplies a working S3 Referer/URL or the OWID factor slugs return, add them
to URLS below — the merge will fold them in without shrinking.

A 200 that parses 0 rows from a real body -> structural; timeout/5xx/429 ->
transient; the ladder CSV being the only live endpoint is the documented blocker,
not a failure of this run.
"""
from __future__ import annotations
import csv
import datetime as dt
import io
import os

import pyarrow as pa
import hashlib
import requests

from ... import config, blob, merge
from ..base import Result
from ._common import Tally, finalize
from ._vintage import http_vintage, UA

SOURCE = "whr"
DEDUP = ("series_key", "obs_date")

# Vintage probe + primary table: the OWID Cantril-ladder grapher CSV is the only
# WHR endpoint that currently serves (others 403/404 — see module docstring).
VINTAGE_URL = "https://ourworldindata.org/grapher/happiness-cantril-ladder.csv"

# All candidate WHR tables (reuse the ingester's working URL set). Each CSV that
# returns 200 contributes its variable column(s); dead ones are skipped (they are
# the documented blocker, not a per-run failure). Listed best-first.
URLS = [
    "https://ourworldindata.org/grapher/happiness-cantril-ladder.csv",
    # The factor CSVs below currently 404 but are kept so a single re-enable on
    # OWID's side is captured automatically; they are NOT counted as failures.
    "https://ourworldindata.org/grapher/log-gdp-per-capita-whr.csv",
    "https://ourworldindata.org/grapher/social-support-whr.csv",
    "https://ourworldindata.org/grapher/healthy-life-expectancy-whr.csv",
    "https://ourworldindata.org/grapher/freedom-to-make-life-choices.csv",
    "https://ourworldindata.org/grapher/generosity-whr.csv",
    "https://ourworldindata.org/grapher/perceptions-of-corruption-whr.csv",
]


def current_vintage(unit):
    # ETag/Last-Modified on the one live WHR table — changes iff the panel is
    # re-published. None if undeterminable (strategy then fetches anyway; safe).
    # NOT http_vintage — MEASURED 2026-07-30, and it cannot work on this url.
    # ourworldindata.org serves the grapher CSV from a CDN with NO ETag and NO Content-Length,
    # and its Last-Modified is the CACHE-FILL time, not the content date: probed at 03:26 it
    # returned "Thu, 30 Jul 2026 03:26:17 GMT" and at 07:33 "Thu, 30 Jul 2026 07:33:58 GMT",
    # each within seconds of the request, with Age: 59 / Age: 79 confirming a fresh cache fill.
    # Stable inside one TTL window, different on every daily run — so the gate could never
    # match and this source re-downloaded and re-merged forever while looking cached. That is
    # the fed_board defect (R164); it hid from the stability sweep because the mover's period
    # is the CDN TTL, not seconds.
    # The BODY is the honest signal: this CSV is small, so hash it.
    try:
        r = requests.get(VINTAGE_URL, headers=UA, timeout=120, allow_redirects=True)
        if r.status_code != 200 or not r.content:
            return None
    except (requests.Timeout, requests.ConnectionError):
        return None                                          # unknown -> cadence decides
    return "whr:" + hashlib.sha256(r.content).hexdigest()[:16]


def _parse_whr_csv(data: bytes):
    """Reuse ingest_whr.parse_whr_csv shape: country/iso3 + year + value columns.
    Returns (keys, dates, vals)."""
    text = data.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    headers = reader.fieldnames or []

    ctry_col = next((h for h in headers if h.lower().strip() in
                     ("country.name", "country_name", "country name", "country",
                      "entity", "name")), None)
    iso3_col = next((h for h in headers if h.lower().strip() in
                     ("iso_code", "iso3", "code", "country_code")), None)
    year_col = next((h for h in headers if h.lower().strip() in ("year", "yr")), None)

    if not (ctry_col or iso3_col) or not year_col:
        return [], [], []

    skip = {(ctry_col or "").lower(), (iso3_col or "").lower(),
            (year_col or "").lower(), "country", "entity", "region"}

    keys, dates, vals = [], [], []
    for row in reader:
        iso3 = ""
        if iso3_col:
            iso3 = (row.get(iso3_col) or "").strip()
        if not iso3 and ctry_col:
            iso3 = (row.get(ctry_col) or "").strip().replace(" ", "_")[:30]
        if not iso3:
            continue

        yr_raw = (row.get(year_col) or "").strip()
        try:
            yr = int(float(yr_raw))
            obs_d = dt.date(yr, 12, 31)
        except (ValueError, TypeError):
            continue

        for col, raw in row.items():
            if not col or col.lower().strip() in skip:
                continue
            if not raw or str(raw).strip() in ("", "NA", "N/A", "nan", "#N/A"):
                continue
            try:
                v = float(str(raw).replace(",", ""))
            except (TypeError, ValueError):
                continue
            if v != v:  # NaN
                continue
            keys.append(f"WHR:{col.strip()}:{iso3}")
            dates.append(obs_d)
            vals.append(v)

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
    path = os.path.join(out_dir, "whr.parquet")
    before = blob.row_count(path)
    tally = Tally()

    all_keys, all_dates, all_vals = [], [], []
    any_200_with_rows = False
    primary_ok = False  # did the live (primary) table return a real body?

    for url in URLS:
        is_primary = (url == VINTAGE_URL)
        try:
            r = requests.get(url, headers=UA, timeout=120, allow_redirects=True)
        except (requests.Timeout, requests.ConnectionError):
            if is_primary:
                tally.transient_unit()
            continue
        if r.status_code in (429, 500, 502, 503, 504):
            if is_primary:
                tally.transient_unit()
            continue
        if r.status_code != 200 or len(r.content) <= 500:
            # Dead factor slugs (404/short body) are the documented blocker, not a
            # per-run failure — only the primary table's death counts structurally.
            if is_primary:
                tally.structural_unit()
            continue

        k, d, v = _parse_whr_csv(r.content)
        if is_primary:
            primary_ok = True
        if v:
            any_200_with_rows = True
            all_keys.extend(k)
            all_dates.extend(d)
            all_vals.extend(v)
        elif is_primary:
            # Primary returned a real body but parsed nothing -> schema break.
            tally.structural_unit()

    # If a transient/structural already fired on the primary, surface it honestly.
    if tally.transient or tally.structural:
        return finalize(tally, before, None, source=SOURCE)

    if not primary_ok:
        # Primary endpoint never returned a usable 200 (and wasn't a clean
        # transient/structural above) -> treat as transient so the unit retries.
        tally.transient_unit()
        return finalize(tally, before, None, source=SOURCE)

    tbl = pa.table({
        "series_key": pa.array(all_keys, pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals, pa.float64()),
    })
    if tbl.num_rows == 0 or not any_200_with_rows:
        tally.structural_unit()
        return finalize(tally, before, None, source=SOURCE)

    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    tally.added_unit(max(0, n - before))
    return finalize(tally, n, md, source=SOURCE, series_cursors=_series_maxes(tbl))
