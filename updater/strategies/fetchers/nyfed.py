"""S2 fetcher — NY Fed reference rates via FRED's public API. US public domain.

The NY Fed publishes SOFR / OBFR / TGCR and the SOFR averages+index to FRED; we mirror the
same daily series the ingester (jobs/ingest_nyfed.py) seeded. Layout: one parquet per rate,
clean_full/nyfed/<name>.parquet, schema (series_key, obs_date, value), series_key = "nyfed:<name>".

Date-tail: each on-disk series is refreshed from a revision-lookback window behind its stored max
obs_date (FRED observation_start=), so same-day revisions and a lagging publish are captured;
merge.merge_and_write dedups the overlap and never shrinks. Only series ALREADY on disk are
refreshed (new rates are a re-ingest concern). Store reads/writes go through blob (CI-safe, R36).

HONEST-STATUS: timeout/5xx/429 -> TransientError -> transient_unit (retried next tick, data kept).
A 200 whose body is not the documented JSON on a non-empty response -> transient (never a silent
no_change). A 4xx on one series (e.g. a retired FRED id) -> that series is empty for this run.
"""
from __future__ import annotations
import datetime as dt
import os
import time

import pyarrow as pa
import pyarrow.compute as pc
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Tally, finalize, revision_since

SOURCE = "nyfed"
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
DEDUP = ("series_key", "obs_date")
EARLIEST = "2000-01-01"

# on-disk parquet name -> FRED series id (from jobs/ingest_nyfed.py; bgcr is not on FRED)
FRED_MAP = {
    "sofr": "SOFR", "obfr": "OBFR",
    "sofr30d": "SOFR30DAYAVG", "sofr90d": "SOFR90DAYAVG", "sofr180d": "SOFR180DAYAVG",
    "sofridx": "SOFRINDEX", "tgcr": "TGCRRATE", "tgcrvol": "TGCRVOLUME",
}


def _api_key() -> str:
    k = os.environ.get("FRED_API_KEY", "").strip()
    if not k:
        try:
            from core.config import load_env
            load_env()
            k = os.environ.get("FRED_API_KEY", "").strip()
        except Exception:
            pass
    if not k:
        # a missing key is an environment/config fault, not a data break: surface it loudly so
        # the run is partial (retried) rather than silently reporting no_change.
        raise DefinitiveError("nyfed: FRED_API_KEY not set (env or .env)")
    return k


def _last(path) -> dt.date | None:
    if not blob.exists(path):
        return None
    t = blob.read_table(path, columns=["obs_date"])
    if t.num_rows == 0:
        return None
    d = pc.max(t.column("obs_date")).as_py()
    return d if isinstance(d, dt.date) else None


def _parse_date(s):
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return dt.datetime.strptime((s or "").strip(), fmt).date()
        except ValueError:
            pass
    return None


def _fetch(sess, fred_id, start, key, tries=5):
    params = {"series_id": fred_id, "api_key": key, "file_type": "json",
              "observation_start": start, "sort_order": "asc"}
    for a in range(tries):
        try:
            r = sess.get(FRED_BASE, params=params, headers=UA, timeout=90)
        except (requests.Timeout, requests.ConnectionError) as e:
            if a == tries - 1:
                raise TransientError(f"nyfed: {e}")
            time.sleep(min(2 ** a, 30)); continue
        if r.status_code == 200:
            try:
                return r.json().get("observations", [])
            except ValueError:
                if a == tries - 1:
                    raise TransientError("nyfed: 200 body not JSON")
                time.sleep(min(2 ** a, 30)); continue
        if r.status_code in (429, 500, 502, 503, 504):
            if a == tries - 1:
                raise TransientError(f"nyfed HTTP {r.status_code}")
            time.sleep(min(2 ** a, 30)); continue
        # 4xx (e.g. a retired/invalid FRED id): no data for this series this run, not a break.
        return []
    raise TransientError("nyfed: retry budget exhausted")


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    key = _api_key()
    pfiles = blob.list_parquets(out_dir)
    if not pfiles:
        raise DefinitiveError(f"no nyfed parquet files under {out_dir}")

    sess = requests.Session()
    tally = Tally()
    cursors: dict[str, str] = {}
    maxd: dt.date | None = None
    total = 0

    for fn in pfiles:
        name = fn[:-len(".parquet")]
        path = os.path.join(out_dir, fn)
        fred_id = FRED_MAP.get(name)
        if not fred_id:
            total += blob.row_count(path)
            tally.empty_unit()
            continue

        last = _last(path)
        start = revision_since(last, unit).isoformat() if last else EARLIEST
        try:
            obs = _fetch(sess, fred_id, start, key)
        except TransientError:
            total += blob.row_count(path)
            tally.transient_unit()
            time.sleep(0.4)
            continue

        keys, dates, vals = [], [], []
        for o in obs:
            d = _parse_date(o.get("date", ""))
            v = (o.get("value") or "").strip()
            if d is None or v in ("", ".", "NA"):
                continue
            try:
                fv = float(v)
            except ValueError:
                continue
            keys.append(f"nyfed:{name}"); dates.append(d); vals.append(fv)

        if not keys:
            total += blob.row_count(path)
            tally.empty_unit()
            time.sleep(0.4)
            continue

        tbl = pa.table({
            "series_key": pa.array(keys, pa.string()),
            "obs_date": pa.array(dates, pa.date32()),
            "value": pa.array(vals, pa.float64()),
        })
        n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
        total += n
        if n > 0:
            tally.added_unit(n)
            cursors[f"nyfed:{name}"] = md
        else:
            tally.empty_unit()
        if md:
            md_d = dt.date.fromisoformat(md) if isinstance(md, str) else md
            if maxd is None or md_d > maxd:
                maxd = md_d
        time.sleep(0.4)

    last_obs = maxd.isoformat() if maxd else (since or None)
    return finalize(tally, total, last_obs, source=SOURCE, series_cursors=cursors,
                    empty_window_floor=len(pfiles) + 1)
