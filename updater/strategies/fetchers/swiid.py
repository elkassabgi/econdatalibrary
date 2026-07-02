"""S1 fetcher — SWIID (Standardized World Income Inequality Database).

Frederick Solt's SWIID, CC BY 4.0, mirrored on GitHub (fsolt/swiid). Small annual
full-table CSV that is RE-ESTIMATED end-to-end each release (historical Gini values
change between versions), so we re-fetch the whole summary CSV and MERGE (dedup
series_key+obs_date, new wins on revision, never-shrink). One sub-unit (the summary
workbook); a 200 that parses 0 rows from a real body is structural.

Vintage signal (registry): GitHub latest-commit SHA for data/swiid_summary.csv on
fsolt/swiid (cheap, changes iff a new release was committed). Single grouped parquet
clean_full/swiid/swiid.parquet, schema (series_key, obs_date, value).
series_key = SWIID:{variable}:{country_name_30char}, obs_date = Dec-31 of year.
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
from ._vintage import github_sha, UA

SOURCE = "swiid"
REPO = "fsolt/swiid"
SUMMARY_PATH = "data/swiid_summary.csv"
SUMMARY_URL = "https://raw.githubusercontent.com/fsolt/swiid/master/data/swiid_summary.csv"
DEDUP = ("series_key", "obs_date")

# Numeric columns to extract from the summary CSV (mirrors jobs/ingest_swiid.py)
VALUE_COLS = [
    "gini_disp", "gini_disp_se",   # disposable income Gini + SE
    "gini_mkt",  "gini_mkt_se",    # market income Gini + SE
    "abs_red",   "rel_red",         # absolute/relative redistribution
]


def current_vintage(unit):
    """Cheap probe: latest commit SHA touching the summary CSV. Changes iff a new
    SWIID release was committed. github_sha may raise TransientError on 429/5xx —
    swallow it (detection failing must not fail the run; update() re-checks)."""
    try:
        return github_sha(REPO, SUMMARY_PATH)
    except Exception:
        return None


def _parse_swiid(data: bytes):
    """Parse SWIID summary CSV into (keys, dates, vals), reusing ingest_swiid.py logic.

    Columns: country, year, gini_disp, gini_disp_se, gini_mkt, gini_mkt_se,
             abs_red, abs_red_se, rel_red, rel_red_se, ...
    country is the country NAME (not ISO3); year is integer; obs_date = Dec-31.
    """
    text = data.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    headers = [h.strip() for h in (reader.fieldnames or [])]

    ctry_col = next((h for h in headers if h.lower() in ("country", "iso", "iso3", "code")), None)
    year_col = next((h for h in headers if h.lower() in ("year", "yr")), None)
    if not ctry_col or not year_col:
        return [], [], []

    val_cols = [c for c in VALUE_COLS if c in headers]
    extra = [h for h in headers if h not in (ctry_col, year_col) and h not in val_cols
             and h.lower() not in ("country", "year", "iso", "iso3")]
    val_cols.extend(extra)

    keys, dates, vals = [], [], []
    for rec in reader:
        ctry = (rec.get(ctry_col) or "").strip()
        if not ctry:
            continue
        yr_raw = rec.get(year_col, "")
        try:
            yr = int(float(str(yr_raw).strip()))
            obs_d = dt.date(yr, 12, 31)
        except (TypeError, ValueError):
            continue
        for col in val_cols:
            raw = rec.get(col, "")
            if not raw or str(raw).strip() in ("", "NA", "NaN", "nan"):
                continue
            try:
                v = float(str(raw).strip())
                if v != v:  # NaN
                    continue
                safe_ctry = ctry.replace(":", "_").replace("/", "_")[:30]
                keys.append(f"SWIID:{col}:{safe_ctry}")
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
    path = os.path.join(out_dir, "swiid.parquet")
    before = blob.row_count(path)
    tally = Tally()

    try:
        r = requests.get(SUMMARY_URL, headers=UA, timeout=120, allow_redirects=True)
    except (requests.Timeout, requests.ConnectionError):
        tally.transient_unit()
        return finalize(tally, before, None, source=SOURCE)
    if r.status_code in (429, 500, 502, 503, 504):
        tally.transient_unit()
        return finalize(tally, before, None, source=SOURCE)
    if r.status_code != 200 or len(r.content) <= 100:
        tally.structural_unit()  # dead/moved URL or trivial body -> surface, do not fake success
        return finalize(tally, before, None, source=SOURCE)

    keys, dates, vals = _parse_swiid(r.content)
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
