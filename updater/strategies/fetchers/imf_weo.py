"""S1 fetcher — IMF DataMapper (WEO / IFS / regional outlooks, annual, ~1980-present).

Free public IMF DataMapper API, no key. Single grouped parquet
clean_full/imf_weo/imf_weo.parquet, schema (series_key, obs_date, value) where
series_key = '{indicator}:{country}' and obs_date is the Dec-31 annual stamp.

DataMapper serves whole country x year matrices per indicator with no since
filter; WEO is fully re-estimated each release (Apr & Oct rounds), so we re-pull
EVERY indicator and MERGE (dedup series_key+obs_date, new wins on revision,
never-shrink). The cheap vintage is a content hash of the /indicators metadata
response — that body carries each indicator's edition tag ("World Economic
Outlook (April 2026)") and a 'last-modified' timestamp, so the hash moves iff an
upstream edition advanced. Re-pull only happens when it moves.

One sub-unit per indicator (~132). A wholesale loss of the indicator list, or
every indicator GET transient-failing, surfaces honestly (transient/structural);
a 200 indicator list that yields 0 parsed obs across all indicators is structural.
"""
from __future__ import annotations
import datetime as dt
import os
import time

import pyarrow as pa
import requests

from ... import config, blob, merge
from ..base import Result
from ._common import Tally, finalize
from ._vintage import content_hash, UA as _UA

SOURCE = "imf_weo"
BASE = "https://www.imf.org/external/datamapper/api/v1"
DEDUP = ("series_key", "obs_date")
RATE = 0.5
# JSON Accept on top of the shared UA (matches the ingester's headers).
UA = {**_UA, "Accept": "application/json"}


def _get_json(url, retries=4):
    """Mirror the ingester's get_json: 200 -> json; 400/404 -> None (gone);
    429 -> sleep+retry; other/network -> retry; give up -> None."""
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=60)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 404):
                return None
            if r.status_code == 429:
                time.sleep(min(60, 5 * (attempt + 1)))
                continue
        except (requests.Timeout, requests.ConnectionError, ValueError):
            pass
        time.sleep(min(15, 5 * (attempt + 1)))
    return None


def current_vintage(unit):
    """Cheap probe: hash the /indicators metadata body, which embeds every
    indicator's edition tag + last-modified. Changes iff an upstream edition
    advanced. None if the list can't be fetched (strategy then fetches anyway)."""
    try:
        r = requests.get(f"{BASE}/indicators", headers=UA, timeout=60)
    except (requests.Timeout, requests.ConnectionError):
        return None
    if r.status_code != 200 or not r.content:
        return None
    return content_hash(r.content)


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
    path = os.path.join(out_dir, "imf_weo.parquet")
    before = blob.row_count(path)
    tally = Tally()

    # Indicator list — wholesale gate. If this can't be obtained, nothing else
    # can be attempted; treat as transient (retry next tick) and keep old data.
    meta = _get_json(f"{BASE}/indicators")
    if not meta or "indicators" not in meta:
        tally.transient_unit()
        return finalize(tally, before, None, source=SOURCE)

    indicators = meta["indicators"]
    keys, dates, vals = [], [], []

    # One sub-unit per indicator (reuse the ingester's URL + parse shape exactly).
    for code in indicators:
        data = _get_json(f"{BASE}/{code}")
        if data is None:
            # network/non-200 after retries -> transient for this indicator
            tally.transient_unit()
            time.sleep(RATE)
            continue
        if "values" not in data:
            # 200 with a real body but no values block -> genuinely empty sub-unit
            tally.empty_unit()
            time.sleep(RATE)
            continue

        # Structure: {"values": {"<code>": {"<country>": {"<year>": <val>, ...}}}}
        ind_vals = data["values"].get(code, {})
        n_added = 0
        for country_code, year_map in ind_vals.items():
            for yr_str, val in year_map.items():
                if val is None:
                    continue
                try:
                    v = float(val)
                    yr = int(yr_str)
                except (TypeError, ValueError):
                    continue
                keys.append(f"{code}:{country_code}")
                dates.append(dt.date(yr, 12, 31))  # Dec-31 annual stamp
                vals.append(v)
                n_added += 1
        tally.added_unit(n_added)  # n>0 counts as added; 0 (empty matrix) counts as empty
        time.sleep(RATE)

    tbl = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date":   pa.array(dates, pa.date32()),
        "value":      pa.array(vals, pa.float64()),
    })

    # 200 indicator list but 0 obs parsed across all indicators from real bodies
    # -> structural break, not a quiet day. finalize() raises DefinitiveError on
    # tally.structural, so flag it explicitly here.
    if tbl.num_rows == 0:
        tally.structural_unit()
        return finalize(tally, before, None, source=SOURCE)

    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    # Re-base the added count to actual NEW rows in the published file (merge
    # dedups revisions away); keep transient/empty tallies for honest status.
    tally.added = max(0, n - before)
    return finalize(tally, n, md, source=SOURCE, series_cursors=_series_maxes(tbl))
