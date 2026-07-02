"""S1 fetcher — Kenneth French Data Library factor panels (Dartmouth).

Eight full-history factor zips (3- & 5-factor monthly+daily, momentum monthly+daily,
short- & long-term reversal monthly), each published WHOLE every month and revised
back through history. One grouped parquet per dataset at
clean_full/famafrench/<name>.parquet with schema (series_key, obs_date, value),
series_key = "ff:{name}:{factor}". We re-fetch all eight, parse, and MERGE each into
its own parquet (dedup series_key+obs_date, new wins on revision, never-shrink).

Each dataset is a Tally sub-unit:
  fresh 200 that merged rows / had no new rows -> added/empty
  200 that parsed 0 rows from a real zip body  -> structural (schema break)
  timeout / 5xx / 429 / network / BadZip-as-net -> transient (retry next run)

vintage_signal (registry): HTTP Last-Modified / ETag on each Dartmouth zip. The
host returns usable ETag+Last-Modified and supports HEAD, so current_vintage()
concatenates a cheap per-zip token across all eight (changes iff any zip moves).
License: Copyright Fama & French — research/educational use only.
"""
from __future__ import annotations
import csv
import datetime as dt
import io
import os
import zipfile

import pyarrow as pa
import requests

from ... import blob, config, merge
from ..base import Result
from ._common import Tally, finalize
from ._vintage import http_vintage, UA

SOURCE = "famafrench"
DEDUP = ("series_key", "obs_date")
BASE = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp"

# (local_name, zip_filename) — mirrors jobs/ingest_famafrench.py DATASETS.
DATASETS = {
    "factors_3_monthly": "F-F_Research_Data_Factors_CSV.zip",
    "factors_3_daily":   "F-F_Research_Data_Factors_daily_CSV.zip",
    "factors_5_monthly": "F-F_Research_Data_5_Factors_2x3_CSV.zip",
    "factors_5_daily":   "F-F_Research_Data_5_Factors_2x3_daily_CSV.zip",
    "momentum_monthly":  "F-F_Momentum_Factor_CSV.zip",
    "momentum_daily":    "F-F_Momentum_Factor_daily_CSV.zip",
    "st_reversal_monthly": "F-F_ST_Reversal_Factor_CSV.zip",
    "lt_reversal_monthly": "F-F_LT_Reversal_Factor_CSV.zip",
}

_TRANSIENT_STATUS = (429, 500, 502, 503, 504)


def current_vintage(unit):
    """Cheap probe: per-zip ETag/Last-Modified concatenated into one token.

    Changes iff ANY of the eight zips moves. Returns None if no zip exposed a
    usable header (the strategy then fetches anyway, which is safe under merge).
    """
    s = requests.Session()
    parts = []
    for name, fname in DATASETS.items():
        tok = http_vintage(f"{BASE}/{fname}", session=s)
        if tok:
            parts.append(f"{name}={tok}")
    return "|".join(parts) if parts else None


def parse_csv(text, name, is_daily):
    """Reused from jobs/ingest_famafrench.py: skip preamble, read factor rows,
    stop at the annual section. Returns list of (series_key, date, value)."""
    rows = []
    lines = text.replace("\r", "").splitlines()
    hdr_i = None
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped and stripped[0].isdigit():
            for j in range(i - 1, -1, -1):
                if lines[j].strip():
                    hdr_i = j
                    break
            break
    if hdr_i is None:
        return []

    reader = csv.reader(lines[hdr_i:])
    hdr = [h.strip() for h in next(reader)]
    factor_cols = [h for h in hdr[1:] if h]  # skip date column
    for row in reader:
        if not row or not row[0].strip():
            continue
        date_str = row[0].strip()
        if not date_str.isdigit():
            break  # hit the annual section or end
        try:
            if is_daily and len(date_str) == 8:
                d = dt.date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
            elif len(date_str) == 6:  # YYYYMM
                d = dt.date(int(date_str[:4]), int(date_str[4:6]), 1)
            elif len(date_str) == 4:  # YYYY
                d = dt.date(int(date_str), 12, 31)
            else:
                continue
        except (ValueError, IndexError):
            continue
        for i, col in enumerate(factor_cols, 1):
            if i >= len(row):
                break
            v = row[i].strip()
            if not v or v in ("", " ", "99.99"):
                continue
            try:
                fv = float(v)
            except ValueError:
                continue
            rows.append((f"ff:{name}:{col}", d, fv))
    return rows


def _series_maxes(rows, out):
    """Accumulate {series_key: max obs_date isoformat} across datasets."""
    for k, d, _ in rows:
        cur = out.get(k)
        if cur is None or d.isoformat() > cur:
            out[k] = d.isoformat()


def _fetch_dataset(session, name, fname, tally, cursors):
    """Fetch+parse+merge ONE dataset zip. Records exactly one Tally sub-unit and
    returns the dataset's row count after the merge (0 if it left data untouched)."""
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{name}.parquet")
    url = f"{BASE}/{fname}"

    try:
        r = session.get(url, headers=UA, timeout=120)
    except (requests.Timeout, requests.ConnectionError):
        tally.transient_unit()
        return None
    if r.status_code in _TRANSIENT_STATUS:
        tally.transient_unit()
        return None
    if r.status_code != 200:
        # hard non-200 (e.g. moved/404) -> structural for this dataset
        tally.structural_unit()
        return None

    try:
        z = zipfile.ZipFile(io.BytesIO(r.content))
        csvs = [m for m in z.namelist() if m.lower().endswith(".csv")]
        if not csvs:
            tally.structural_unit()
            return None
        text = z.read(csvs[0]).decode("utf-8", errors="replace")
    except zipfile.BadZipFile:
        # a truncated/garbled body is most likely a transient transfer issue
        tally.transient_unit()
        return None

    is_daily = "daily" in name
    rows = parse_csv(text, name, is_daily)
    if not rows:
        tally.structural_unit()  # 200 + real zip but parsed 0 rows -> schema break
        return None

    tbl = pa.table({
        "series_key": pa.array([row[0] for row in rows], pa.string()),
        "obs_date":   pa.array([row[1] for row in rows], pa.date32()),
        "value":      pa.array([row[2] for row in rows], pa.float64()),
    })
    prev = blob.row_count(path)
    n, _md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    tally.added_unit(max(0, n - prev))
    _series_maxes(rows, cursors)
    return n


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)

    def total_rows():
        n = 0
        for name in DATASETS:
            n += blob.row_count(os.path.join(out_dir, f"{name}.parquet"))
        return n

    tally = Tally()
    cursors: dict = {}
    session = requests.Session()
    for name, fname in DATASETS.items():
        _fetch_dataset(session, name, fname, tally, cursors)

    after = total_rows()
    last_obs = max(cursors.values()) if cursors else None
    return finalize(tally, after, last_obs, source=SOURCE, series_cursors=cursors)
