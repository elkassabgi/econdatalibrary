"""S1 fetcher — Robert Shiller US stock market & CAPE (monthly, 1871-present).

Public domain (Yale). Single grouped parquet clean_full/shiller/shiller.parquet,
schema (series_key, obs_date, value). Shiller revises history each release, so we
re-fetch the whole xls and MERGE (dedup series_key+obs_date, new wins on revision,
never-shrink). One sub-unit (the workbook); a 200 that parses 0 rows is structural.
"""
from __future__ import annotations
import datetime as dt
import os

import pyarrow as pa
import requests

from ... import config, blob, merge
from ...errors import TransientError
from ..base import Result
from ._common import Tally, finalize
from ._vintage import http_vintage, UA

# Shiller migrated off the Yale econ page (that URL froze at 2023-09); the live data
# now lives on shillerdata.com (served from this wsimg blob, currently to 2024-09).
URL = "https://img1.wsimg.com/blobby/go/e5e77e0b-59d1-44d9-ab25-4763ac982e53/downloads/ie_data.xls"
URL_FALLBACK = "http://www.econ.yale.edu/~shiller/data/ie_data.xls"
SOURCE = "shiller"
DEDUP = ("series_key", "obs_date")
COLUMNS = [(1, "sp500_price"), (2, "sp500_dividend"), (3, "sp500_earnings"), (4, "cpi"),
           (6, "long_rate"), (7, "real_price"), (8, "real_dividend"), (9, "real_earnings"),
           (10, "cape")]


def current_vintage(unit):
    return http_vintage(URL)


def _parse_date(val):
    if val is None:
        return None
    try:
        s = str(val).strip()
        if "." in s:
            yr = int(s.split(".")[0])
            frac = s.split(".")[1] if "." in s else "01"
            mon = int(frac[:2] if len(frac) >= 2 else frac.ljust(2, "0"))
            return dt.date(yr, min(max(mon, 1), 12), 1)
        if len(s) == 7 and "-" in s:
            return dt.date.fromisoformat(s + "-01")
        if s.isdigit() and len(s) == 6:
            return dt.date(int(s[:4]), int(s[4:6]), 1)
    except (ValueError, TypeError, IndexError):
        pass
    return None


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
    path = os.path.join(out_dir, "shiller.parquet")
    before = blob.row_count(path)
    tally = Tally()

    try:
        r = requests.get(URL, headers=UA, timeout=120)
    except (requests.Timeout, requests.ConnectionError) as e:
        tally.transient_unit()
        return finalize(tally, before, None, source=SOURCE)
    if r.status_code in (429, 500, 502, 503, 504):
        tally.transient_unit()
        return finalize(tally, before, None, source=SOURCE)
    if r.status_code != 200:
        tally.structural_unit()
        return finalize(tally, before, None, source=SOURCE)

    import xlrd
    wb = xlrd.open_workbook(file_contents=r.content)
    ws = next((wb.sheet_by_name(n) for n in wb.sheet_names() if "data" in n.lower()), wb.sheet_by_index(0))
    start = 0
    for i in range(ws.nrows):
        v = ws.row_values(i)
        try:
            if v and 1870 <= float(str(v[0]).strip() or 0) <= 1880:
                start = i
                break
        except (ValueError, TypeError):
            pass
    keys, dates, vals = [], [], []
    for i in range(start, ws.nrows):
        row = ws.row_values(i)
        if not row or row[0] in (None, ""):
            break
        od = _parse_date(row[0])
        if od is None:
            continue
        for ci, name in COLUMNS:
            if ci >= len(row) or row[ci] in (None, ""):
                continue
            try:
                fv = float(row[ci])
            except (ValueError, TypeError):
                continue
            if fv != fv:  # NaN
                continue
            keys.append(name); dates.append(od); vals.append(fv)

    tbl = pa.table({"series_key": pa.array(keys, pa.string()),
                    "obs_date": pa.array(dates, pa.date32()),
                    "value": pa.array(vals, pa.float64())})
    if tbl.num_rows == 0:
        tally.structural_unit()  # 200 but parsed nothing -> schema break, not a quiet day
        return finalize(tally, before, None, source=SOURCE)

    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    tally.added_unit(max(0, n - before))
    return finalize(tally, n, md, source=SOURCE, series_cursors=_series_maxes(tbl))
