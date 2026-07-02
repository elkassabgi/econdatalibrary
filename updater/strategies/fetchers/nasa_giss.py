"""S1 fetcher — NASA GISS Surface Temperature Analysis (GISTEMP v4).

Public domain (NASA). Single grouped parquet clean_full/nasa_giss/giss_temp.parquet,
schema (series_key, obs_date, value); series_key format 'GISS:{table}:{period}'.
GISS re-publishes the four full wide CSV tables (global / NH / SH / zonal) monthly
and revises history each release, so we re-fetch all four and MERGE (dedup
series_key+obs_date, new wins on revision, never-shrink). Four sub-units (one per
CSV); a 200 that parses 0 rows from a real body is structural.

Reuses the ingester's URLs + parse logic from jobs/ingest_nasa_giss.py.
Vintage signal (registry): HTTP Last-Modified/ETag on the four CSVs — if any moved,
re-download all four. Vintage = combined ETag/Last-Modified of the four files.
"""
from __future__ import annotations
import datetime as dt
import os

import pyarrow as pa
import requests

from ... import config, blob, merge
from ..base import Result
from ._common import Tally, finalize
from ._vintage import UA

BASE = "https://data.giss.nasa.gov/gistemp/tabledata_v4"
SOURCE = "nasa_giss"
DEDUP = ("series_key", "obs_date")

# (filename, series_label) — mirrors jobs/ingest_nasa_giss.py TABLES
TABLES = [
    ("GLB.Ts+dSST.csv", "global"),
    ("NH.Ts+dSST.csv", "north_hemisphere"),
    ("SH.Ts+dSST.csv", "south_hemisphere"),
    ("ZonAnn.Ts+dSST.csv", "zonal"),
]

MONTH_MAP = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

_TRANSIENT = (429, 500, 502, 503, 504)


def current_vintage(unit):
    """Cheap probe: combined ETag/Last-Modified of the four GISTEMP CSVs (HEAD, no
    body). Changes iff any CSV upstream changed. Returns None if none of the four
    expose a usable header (strategy then fetches anyway, which is safe)."""
    parts = []
    for fname, _ in TABLES:
        url = f"{BASE}/{fname}"
        try:
            r = requests.head(url, headers=UA, timeout=60, allow_redirects=True)
        except (requests.Timeout, requests.ConnectionError):
            continue
        if r.status_code in _TRANSIENT or r.status_code != 200:
            continue
        h = r.headers
        tok = h.get("ETag") or h.get("Last-Modified") or h.get("Content-Length")
        if tok:
            parts.append(f"{fname}={tok}")
    if not parts:
        return None
    return "|".join(parts)


def _parse_giss_csv(data: bytes, label: str):
    """Parse a GISS wide table (year in col0, months + seasons in other cols).
    Returns (keys, dates, vals). Mirrors jobs/ingest_nasa_giss.parse_giss_csv."""
    text = data.decode("utf-8", errors="replace")
    lines = [l for l in text.splitlines() if l.strip() and not l.startswith("*")]
    if not lines:
        return [], [], []

    keys, dates, vals = [], [], []

    header = None
    data_start = 0
    for i, line in enumerate(lines):
        parts = [p.strip() for p in line.split(",")]
        if parts and parts[0].lower() in ("year", ""):
            header = parts
            data_start = i + 1
            break

    if header is None:
        return [], [], []

    col_map = {}  # col_idx -> (period_label, month or None)
    for ci, col in enumerate(header):
        col = col.strip()
        if ci == 0 or col.lower() == "year":
            continue
        if col in MONTH_MAP:
            col_map[ci] = (col, MONTH_MAP[col])
        elif col in ("J-D", "J.D"):
            col_map[ci] = ("annual", None)
        elif col in ("D-N", "D.N"):
            col_map[ci] = ("dec_nov", None)
        elif col.upper() in ("DJF", "MAM", "JJA", "SON"):
            col_map[ci] = (col.upper(), None)
        elif col and col not in ("", "Year"):
            col_map[ci] = (col.replace(" ", "_"), None)

    for line in lines[data_start:]:
        parts = [p.strip() for p in line.split(",")]
        if not parts or not parts[0]:
            continue
        try:
            yr = int(parts[0])
        except ValueError:
            continue

        for ci, (period, month) in col_map.items():
            if ci >= len(parts):
                continue
            raw = parts[ci].strip()
            if raw in ("", "****", "***", "NA", "N/A"):
                continue
            try:
                v = float(raw)
            except ValueError:
                continue
            if v != v:  # NaN guard
                continue

            if month is not None:
                obs_d = dt.date(yr, month, 1)
            else:
                obs_d = dt.date(yr, 12, 31)

            keys.append(f"GISS:{label}:{period}")
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
    path = os.path.join(out_dir, "giss_temp.parquet")
    before = blob.row_count(path)
    tally = Tally()

    all_keys, all_dates, all_vals = [], [], []
    parsed_ok = 0  # sub-units (CSVs) that fetched 200 AND parsed >0 rows

    # Four sub-units (one CSV each). Each is tallied honestly so a per-file
    # transient/structural failure cannot launder into a fresh "ok". Files that
    # parse rows are deferred and tallied after the single combined merge below.
    for fname, label in TABLES:
        url = f"{BASE}/{fname}"
        try:
            r = requests.get(url, headers=UA, timeout=120)
        except (requests.Timeout, requests.ConnectionError):
            tally.transient_unit()
            continue
        if r.status_code in _TRANSIENT or r.status_code == 404:
            tally.transient_unit()
            continue
        if r.status_code != 200:
            # hard non-200 (not 404/429) — body unavailable, treat as structural
            tally.structural_unit()
            continue

        body = r.content or b""
        k, d, v = _parse_giss_csv(body, label)
        if not v:
            if body.strip():
                tally.structural_unit()  # real body, parsed nothing -> schema break
            else:
                tally.empty_unit()       # genuinely empty body
            continue

        all_keys.extend(k)
        all_dates.extend(d)
        all_vals.extend(v)
        parsed_ok += 1

    if not all_vals:
        # nothing parsed from any file — let finalize decide (transient/structural
        # via the already-recorded sub-unit outcomes).
        return finalize(tally, before, None, source=SOURCE)

    tbl = pa.table({
        "series_key": pa.array(all_keys, pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals, pa.float64()),
    })

    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)

    # Attribute net-new rows across the parse-successful CSVs: the merge is one
    # combined publish, so the added rows land on the first ok sub-unit and the
    # remaining ok sub-units count as empty (revisions only / no new rows).
    added = max(0, n - before)
    tally.added_unit(added)
    for _ in range(parsed_ok - 1):
        tally.empty_unit()

    return finalize(tally, n, md, source=SOURCE, series_cursors=_series_maxes(tbl))
