"""S1 fetcher — Deutsche Bundesbank SDMX REST (keyless, DL-DE-BY-2.0).

Seven medium/large SDMX flows, one grouped parquet per flow under
clean_full/bundesbank/<flow>.parquet, schema (series_key, obs_date, value).
The API IGNORES startPeriod/endPeriod (and lastNObservations) for the XML format,
so every pull is the WHOLE flow; we re-fetch all flows and MERGE each into its own
parquet (dedup series_key+obs_date, new wins on revision, never-shrink). A 200 that
parses 0 rows from a real body is a structural break; timeout/5xx/429 is transient.

Vintage: the per-flow endpoint exposes NO ETag / Last-Modified / Content-Length
(verified live: HEAD returns 200 with none of them), and lastNObservations/detail
do NOT shrink the large bodies (BBEX3 is ~1.3 GB even with lastNObservations=1).
So there is no cheap, *complete* change probe — current_vintage returns None and the
S1 strategy fetches anyway on the monthly cadence (safe: merge dedups + never shrinks).
This matches the registry strategy_reason ("only 7 medium flows, re-fetch full XML").
"""
from __future__ import annotations
import datetime as dt
import io
import os
import xml.etree.ElementTree as ET

import pyarrow as pa
import requests

from ... import config, blob, merge
from ..base import Result
from ._common import Tally, finalize

SOURCE = "bundesbank"
BASE = "https://api.statistiken.bundesbank.de/rest/data"
DEDUP = ("series_key", "obs_date")
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
      "Accept": "application/xml"}

# Flows confirmed live via HTTP probe (one grouped parquet per flow).
FLOWS = ["BBEX3", "BBNZ1", "BBDP1", "BBSIS", "BBFI1", "BBFI3", "BBBP1"]

# SDMX 2.1 generic-data namespace (the format this API serves).
NS_GENERIC = "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic"
_TAG_SERIES = f"{{{NS_GENERIC}}}Series"
_TAG_SERIESKEY = f"{{{NS_GENERIC}}}SeriesKey"
_TAG_VALUE = f"{{{NS_GENERIC}}}Value"
_TAG_OBS = f"{{{NS_GENERIC}}}Obs"
_TAG_OBSDIM = f"{{{NS_GENERIC}}}ObsDimension"
_TAG_OBSVAL = f"{{{NS_GENERIC}}}ObsValue"

_TRANSIENT = (429, 500, 502, 503, 504)


def current_vintage(unit):
    """No cheap, complete vintage signal exists for this source.

    The per-flow endpoint returns no ETag/Last-Modified/Content-Length, and the API
    ignores lastNObservations for the large flows (BBEX3 is ~1.3 GB regardless), so
    any probe cheap enough to run here would only reflect one small flow and would
    falsely report 'no change' on days a different flow moved. Returning None makes
    the S1 strategy fetch on cadence (monthly) — safe because merge dedups + never
    shrinks. See registry adapter.open_question for this source.
    """
    return None


def _parse_bbk_date(s: str):
    """Parse a Bundesbank SDMX time period to a date (period start, year-end for annual)."""
    s = (s or "").strip()
    try:
        if len(s) == 4 and s.isdigit():                          # Annual: 2023
            return dt.date(int(s), 12, 31)
        if len(s) == 7 and s[4] == "-" and s[5:].isdigit():      # Monthly: 2023-01
            return dt.date(int(s[:4]), int(s[5:7]), 1)
        if len(s) == 7 and s[4] == "-" and s[5] == "Q":          # Quarterly: 2023-Q1
            yr, q = int(s[:4]), int(s[6])
            return dt.date(yr, (q - 1) * 3 + 1, 1)
        if len(s) == 10 and s[4] == "-" and s[7] == "-":         # Daily: 2023-01-15
            return dt.date.fromisoformat(s)
        if len(s) == 8 and s[4] == "-" and s[5] == "W":          # Weekly: 2023-W01
            return dt.date.fromisocalendar(int(s[:4]), int(s[6:8]), 1)
    except (ValueError, IndexError):
        pass
    return None


def _parse_flow(xml_bytes: bytes, flow_id: str):
    """Iterparse SDMX generic XML into (keys, dates, vals) for one flow."""
    keys, dates, vals = [], [], []
    current_key = ""
    current_obs_date = None
    for event, elem in ET.iterparse(io.BytesIO(xml_bytes), events=("start", "end")):
        if event == "start":
            if elem.tag == _TAG_SERIES:
                current_key = ""
            elif elem.tag == _TAG_OBS:
                current_obs_date = None
            elif elem.tag == _TAG_OBSDIM:
                current_obs_date = _parse_bbk_date(elem.get("value", ""))
            elif elem.tag == _TAG_OBSVAL:
                if current_obs_date is not None:
                    try:
                        v = float(elem.get("value", "nan"))
                    except (ValueError, TypeError):
                        continue
                    if v == v:  # not NaN
                        keys.append(f"BBK:{flow_id}:{current_key}")
                        dates.append(current_obs_date)
                        vals.append(v)
        elif event == "end":
            if elem.tag == _TAG_SERIESKEY:
                current_key = ":".join(
                    f"{v.get('id', '')}={v.get('value', '')}"
                    for v in elem if v.tag == _TAG_VALUE
                )
            elif elem.tag == _TAG_SERIES:
                elem.clear()  # free memory between series
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
    tally = Tally()
    cursors: dict = {}
    total_rows = 0
    last_obs = None

    for flow_id in FLOWS:
        path = os.path.join(out_dir, f"{flow_id}.parquet")
        before = blob.row_count(path)
        url = f"{BASE}/{flow_id}"

        # --- fetch (one sub-unit per flow) ---
        try:
            r = requests.get(url, headers=UA, timeout=600, stream=True)
            if r.status_code in _TRANSIENT:
                tally.transient_unit()
                total_rows += before
                continue
            if r.status_code != 200:
                # 4xx other than transient -> structural (flow vanished / id changed)
                tally.structural_unit()
                total_rows += before
                continue
            chunks = []
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    chunks.append(chunk)
            xml_bytes = b"".join(chunks)
        except (requests.Timeout, requests.ConnectionError, requests.RequestException):
            tally.transient_unit()
            total_rows += before
            continue

        # --- parse ---
        try:
            keys, dates, vals = _parse_flow(xml_bytes, flow_id)
        except ET.ParseError:
            # 200 with an unparseable body from a non-trivial response -> structural
            if len(xml_bytes) > 256:
                tally.structural_unit()
            else:
                tally.transient_unit()
            total_rows += before
            continue

        tbl = pa.table({
            "series_key": pa.array(keys, pa.string()),
            "obs_date": pa.array(dates, pa.date32()),
            "value": pa.array(vals, pa.float64()),
        })
        if tbl.num_rows == 0:
            # 200 but parsed nothing from a real body -> structural break (schema change)
            tally.structural_unit()
            total_rows += before
            continue

        # --- publish (atomic, dedup, never-shrink) ---
        n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
        tally.added_unit(max(0, n - before))
        total_rows += n
        if md and (last_obs is None or md > last_obs):
            last_obs = md
        cursors.update(_series_maxes(tbl))

    return finalize(tally, total_rows, last_obs, source=SOURCE, series_cursors=cursors)
