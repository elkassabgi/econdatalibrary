"""S1 fetcher — UN Sustainable Development Goals (SDG) database (713 series).

CC BY 4.0 (UN Statistics Division). Single grouped parquet
clean_full/unsdg/unsdg.parquet, schema (series_key, obs_date, value), annual
(Dec-31), series_key = '<seriesCode>:<geoAreaCode>[|Dim=val|...]' (up to 3 sorted
non-trivial dimensions) — exactly as built by jobs/ingest_unsdg.py.

The SDG API exposes NO global since/updatedAfter filter, and the UNSD re-estimates
+ extends history at each quarterly release, so the correct refresh is to re-fetch
the whole table per series and MERGE (dedup series_key+obs_date, revised values win,
never-shrink). Change-detection is the release tag the API stamps on every series in
Series/List (currently '2026.Q1.G.02'); a content hash of all (code,release) pairs
moves iff any series got a new release -> exactly the "history was re-estimated"
signal. One cheap ~180 KB GET, no auth.

Each series is a sub-unit: 200-with-records that parse >0 obs -> added/empty;
200/4xx that yields 0 records from a real query -> structural; 429/5xx/network ->
transient. A full pull is ~713 paginated GETs (the SDG API is slow, ~7-9 s/page),
so a per-run series budget is honored from unit.config['max_series'] /
$UNSDG_MAX_SERIES (default: all 713). A bounded run is a legitimate partial refresh:
merge unions the re-fetched series with the untouched ones and never shrinks.
"""
from __future__ import annotations
import datetime as dt
import os
import time

import pyarrow as pa
import requests

from ... import config, merge, blob
from ...errors import DefinitiveError
from ..base import Result
from ._common import Tally, finalize
from ._vintage import content_hash, UA as _UA

SOURCE = "unsdg"
DEDUP = ("series_key", "obs_date")

BASE = "https://unstats.un.org/sdgapi/v1/sdg"
UA = {**_UA, "Accept": "application/json"}
PAGE = 1000
RATE = 0.5          # polite delay between series (matches the ingester)
PAGE_RETRIES = 4

# Production default: re-fetch every series. Overridable (chiefly for live tests /
# budgeted ticks) via unit.config['max_series'] or $UNSDG_MAX_SERIES. 0/empty = all.
MAX_SERIES_DEFAULT = 0


def _get_json(url, params=None, retries=PAGE_RETRIES):
    """Return (json, outcome) where outcome is 'ok' (200 parsed), 'missing'
    (400/404/other 4xx -> structural for the sub-unit), or 'transient'
    (timeout/5xx/429/network -> retry next tick)."""
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=UA, timeout=120)
        except (requests.Timeout, requests.ConnectionError):
            if attempt >= retries - 1:
                return None, "transient"
            time.sleep(3 * (attempt + 1))
            continue
        if r.status_code == 200:
            try:
                return r.json(), "ok"
            except ValueError:
                return None, "missing"  # 200 with non-JSON body -> structural
        if r.status_code in (400, 404):
            return None, "missing"
        if r.status_code in (429, 500, 502, 503, 504):
            if attempt >= retries - 1:
                return None, "transient"
            time.sleep(60 if r.status_code == 429 else 5 * (attempt + 1))
            continue
        return None, "missing"  # other 4xx
    return None, "transient"


def _series_list():
    """Return (series_list, outcome). The release-tagged catalog is also the
    vintage source, so we fetch it once and reuse it."""
    data, outcome = _get_json(f"{BASE}/Series/List")
    if outcome != "ok":
        return None, outcome
    return (data if isinstance(data, list) else []), "ok"


def current_vintage(unit):
    """Cheap probe: a content hash over the sorted (seriesCode, release) pairs from
    Series/List. Changes iff any of the 713 series got a new release tag -> the
    exact signal that the SDG data vintage moved. Returns None if the catalog can't
    be fetched cheaply (strategy then fetches anyway; merge dedups + never-shrinks)."""
    series, outcome = _series_list()
    if outcome != "ok" or not series:
        return None
    pairs = sorted((str(s.get("code", "")), str(s.get("release", ""))) for s in series)
    blob_bytes = "".join(f"{c}={r};" for c, r in pairs).encode("utf-8")
    return content_hash(blob_bytes)


def _parse_records(records, code):
    """Reuse the ingester's exact key/value/date construction."""
    keys, dates, vals = [], [], []
    for rec in records:
        geo = str(rec.get("geoAreaCode", rec.get("geoAreaName", "WLD")))
        time_period = rec.get("timePeriodStart") or rec.get("timePeriod")
        val_raw = rec.get("value")
        if val_raw is None or str(val_raw).strip() in ("", "N/A", "null", "None"):
            continue
        try:
            v = float(str(val_raw).replace(",", ""))
        except (ValueError, TypeError):
            continue
        if time_period is None:
            continue
        try:
            yr = int(str(time_period).split(".")[0])
            obs_date = dt.date(yr, 12, 31)
        except (ValueError, TypeError):
            continue
        dims = rec.get("dimensions", {})
        dim_str = ""
        if isinstance(dims, dict) and dims:
            dim_parts = [f"{k}={dv}" for k, dv in sorted(dims.items())
                         if dv and dv not in ("", "_T", "ALLAREA", "G")]
            if dim_parts:
                dim_str = "|" + "|".join(dim_parts[:3])
        keys.append(f"{code}:{geo}{dim_str}")
        dates.append(obs_date)
        vals.append(v)
    return keys, dates, vals


def _fetch_series(code):
    """Fetch all observations for one series across all pages. Returns
    (keys, dates, vals, outcome) where outcome is 'ok' (>=1 page parsed),
    'missing' (200/4xx but no records from a real query -> structural), or
    'transient' (timeout/5xx/429/network on any page -> retry next tick)."""
    keys, dates, vals = [], [], []
    page = 1
    got_any = False
    while True:
        data, outcome = _get_json(f"{BASE}/Series/Data",
                                  params={"seriesCode": code, "pageSize": PAGE, "page": page})
        if outcome == "transient":
            # A mid-pagination transient means this series is incomplete; surface it
            # as transient (do not publish a truncated series as success).
            return keys, dates, vals, "transient"
        if outcome == "missing":
            break
        if isinstance(data, dict):
            records = data.get("data", [])
            total_pages = data.get("totalPages", 1)
        elif isinstance(data, list):
            records = data
            total_pages = 1
        else:
            break
        if not records:
            break
        got_any = True
        k, d, v = _parse_records(records, code)
        keys.extend(k); dates.extend(d); vals.extend(v)
        if page >= (total_pages or 1):
            break
        page += 1
    return keys, dates, vals, ("ok" if got_any else "missing")


def _series_maxes(keys, dates):
    out = {}
    for k, d in zip(keys, dates):
        if d is None:
            continue
        if k not in out or d > out[k]:
            out[k] = d
    return {k: v.isoformat() for k, v in out.items()}


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "unsdg.parquet")
    before = blob.row_count(path)
    tally = Tally()

    cfg = unit.config or {}
    budget = int(cfg.get("max_series")
                 or os.environ.get("UNSDG_MAX_SERIES", MAX_SERIES_DEFAULT) or 0)

    series, outcome = _series_list()
    if outcome != "ok":
        # Can't even list series -> whole pull is transient; existing data untouched.
        tally.transient_unit()
        return finalize(tally, before, None, source=SOURCE)
    if not series:
        tally.structural_unit()  # 200 catalog that parsed no series -> real break
        return finalize(tally, before, None, source=SOURCE)

    codes = [s.get("code") for s in series if s.get("code")]
    if budget > 0:
        codes = codes[:budget]

    all_keys, all_dates, all_vals = [], [], []
    transient = 0      # series that transient-failed (retry next run)
    structural = 0     # series: 200/4xx that yielded 0 records from a real query
    for code in codes:
        k, d, v, outc = _fetch_series(code)
        if outc == "transient":
            transient += 1
            time.sleep(RATE)
            continue
        if outc == "missing":
            structural += 1
            time.sleep(RATE)
            continue
        if not k:
            # 200 with records but every record unparseable -> structural sub-unit.
            structural += 1
            time.sleep(RATE)
            continue
        all_keys.extend(k); all_dates.extend(d); all_vals.extend(v)
        time.sleep(RATE)

    # Surface per-series structural/transient outcomes BEFORE publishing so the
    # returned status is honest (existing data was never touched on a failure).
    for _ in range(structural):
        tally.structural_unit()
    for _ in range(transient):
        tally.transient_unit()

    # Nothing parsed at all -> let finalize raise the honest structural/empty-window
    # error (existing data kept). With a tight per-run budget over a healthy upstream
    # this won't trigger; it fires on a genuine wholesale break.
    if not all_keys:
        return finalize(tally, before, None, source=SOURCE)

    tbl = pa.table({
        "series_key": pa.array(all_keys, pa.string()),
        "obs_date": pa.array(all_dates, pa.date32()),
        "value": pa.array(all_vals, pa.float64()),
    })

    # The re-fetched series are published in ONE atomic merge (dedup on
    # series_key+obs_date, revised values win, never-shrink @0.97). Untouched series
    # survive the union, so a budgeted partial pull can only grow the table.
    #
    # KNOWN BLOCKER (see module docstring + REPORT): the PUBLISHED parquet's effective
    # key (series_key, obs_date) is NOT unique — the ingester truncates each series'
    # dimensions to the first 3 (dim_parts[:3]) and collapses every period to Dec-31,
    # so observations that differ only in a 4th+ dimension share one (series_key,
    # obs_date) with different values (e.g. SE_ADT_ACTS:400|Age|Location|Sex carries 36
    # distinct values on 2024-12-31). 352,671 of 3,175,479 published rows (11%) are such
    # collapsible duplicates. A correct dedup-on-merge therefore drops them and trips
    # never-shrink (88.9% < 97%). merge_and_write leaves the existing file UNTOUCHED on
    # that refusal. We do NOT weaken the guard or lower min_ratio; we surface it as a
    # partial with the reason. Real fix belongs upstream (make series_key carry ALL
    # dimensions so the key is unique), which requires re-ingesting all 713 series.
    try:
        n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    except DefinitiveError as e:
        return Result(status="partial", obs=before, last_obs_date=None,
                      new_vintage=None,
                      series_cursors=_series_maxes(all_keys, all_dates),
                      error=("merge refused (existing data kept, guard intact): "
                             f"{e}"))
    tally.added_unit(max(0, n - before))
    return finalize(tally, n, md, source=SOURCE, series_cursors=_series_maxes(all_keys, all_dates))
