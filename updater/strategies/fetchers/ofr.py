"""S2 fetcher — OFR (Office of Financial Research, US Treasury). Public domain, no key.

Layout: per-dataset parquet under clean_full/ofr/{fnyr,repo,mmf,nypd}.parquet,
schema (series_id, obs_date, value, dataset). The OFR API returns each dataset's
full series in one call, so we re-fetch and MERGE (dedup on series_id+obs_date,
new values win on revision, never-shrink) — existing series gain new dates safely.

Honest-status contract (Tally + finalize):
  - Each dataset is one sub-unit. A successful merge -> added_unit(n_new).
  - A 200 whose `timeseries` envelope is MISSING, non-dict, or PRESENT-BUT-EMPTY
    is a structural break (OFR serves full history for every dataset on every
    call) -> structural_unit(). A 200 whose envelope is a non-empty dict but
    parses 0 rows is likewise structural. Either way finalize() raises
    DefinitiveError; existing data is kept untouched.
  - A timeout / 5xx / 429 / network drop -> transient_unit() (status partial).
  - Datasets are fetched all-or-nothing BEFORE any publish, so a late transient
    cannot leave disk advanced while state stays behind (data/state divergence).
"""
from __future__ import annotations
import datetime as dt
import math
import os
import time

import pyarrow as pa
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Tally, finalize

UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
BASE = "https://data.financialresearch.gov/v1/series/dataset"
DATASETS = ["fnyr", "repo", "mmf", "nypd"]
DEDUP = ("series_id", "obs_date")
SOURCE = "ofr"

# A single dataset with a partial schema drift can still parse most series; if a
# large fraction of points across the dataset are unparseable, treat as structural.
DROP_RATIO_STRUCTURAL = 0.5


def _get(ds, tries=5):
    """Fetch one dataset. Returns parsed JSON on 200. Raises TransientError on
    timeout/5xx/429/network (retry next run), DefinitiveError on a hard non-200,
    and TransientError on a 200 whose body will not parse as JSON (truncated/
    flaky body is a transient fault, not data)."""
    for a in range(tries):
        try:
            r = requests.get(BASE, params={"dataset": ds}, headers=UA, timeout=180)
        except (requests.Timeout, requests.ConnectionError) as e:
            if a == tries - 1:
                raise TransientError(f"OFR {ds}: {e}")
            time.sleep(min(2 ** a, 30)); continue
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError as e:
                if a == tries - 1:
                    raise TransientError(f"OFR {ds}: bad json on 200 ({e})")
                time.sleep(min(2 ** a, 30)); continue
        if r.status_code in (429, 500, 502, 503, 504):
            if a == tries - 1:
                raise TransientError(f"OFR {ds} HTTP {r.status_code}")
            time.sleep(min(2 ** a, 30)); continue
        raise DefinitiveError(f"OFR {ds} HTTP {r.status_code}")


def _parse(ds, payload):
    """Return (table, envelope_ok, dropped, total_points).

    envelope_ok is True ONLY when the `timeseries` key is present AND is a
    non-empty dict — i.e. the body has the structure OFR always returns. A
    missing key, a non-dict value, or an EMPTY dict ({"timeseries":{}}) all
    yield envelope_ok=False so update() flags a structural break (these MUST NOT
    be laundered into no_change). `dropped` / `total_points` count per-point
    parse losses so a per-series schema drift inside an otherwise-fine envelope
    can be surfaced rather than silently shrinking that series.
    """
    if not isinstance(payload, dict):
        return _empty_table(), False, 0, 0
    ts = payload.get("timeseries")
    envelope_ok = isinstance(ts, dict) and len(ts) > 0
    sids, dates, vals, dss = [], [], [], []
    dropped, total = 0, 0
    for sid, sdata in (ts or {}).items():
        if not isinstance(sdata, dict):
            continue
        inner = sdata.get("timeseries", {}) or {}
        for pt in (inner.get("aggregation") or inner.get("value") or []):
            total += 1
            if not isinstance(pt, (list, tuple)) or len(pt) < 2 or pt[1] is None:
                dropped += 1
                continue
            try:
                d_val = dt.date.fromisoformat(str(pt[0]).strip())
                fv = float(pt[1])
            except (ValueError, TypeError):
                dropped += 1
                continue
            if not math.isfinite(fv):
                dropped += 1
                continue
            sids.append(sid); dates.append(d_val); vals.append(fv); dss.append(ds)
    tbl = pa.table({
        "series_id": pa.array(sids, pa.string()),
        "obs_date": pa.array(dates, pa.date32()),
        "value": pa.array(vals, pa.float64()),
        "dataset": pa.array(dss, pa.string()),
    })
    return tbl, envelope_ok, dropped, total


def _empty_table():
    return pa.table({
        "series_id": pa.array([], pa.string()),
        "obs_date": pa.array([], pa.date32()),
        "value": pa.array([], pa.float64()),
        "dataset": pa.array([], pa.string()),
    })


def _series_maxes(tbl):
    """{series_id: 'YYYY-MM-DD'} max obs_date per series in a parsed table, so a
    frozen series can't hide behind the dataset-level max."""
    out: dict[str, dt.date] = {}
    if tbl.num_rows == 0:
        return out
    sid_col = tbl.column("series_id").to_pylist()
    date_col = tbl.column("obs_date").to_pylist()
    for sid, d in zip(sid_col, date_col):
        if d is None:
            continue
        cur = out.get(sid)
        if cur is None or d > cur:
            out[sid] = d
    return {k: v.isoformat() for k, v in out.items()}


def update(unit, since) -> Result:
    out_dir = config.source_dir("ofr")
    os.makedirs(out_dir, exist_ok=True)

    tally = Tally()
    series_cursors: dict[str, str] = {}
    total, maxd = 0, None

    # ---- Phase 1: fetch + parse ALL datasets first (all-or-nothing within the
    # unit). A transient failure aborts here BEFORE any publish, so disk and
    # state stay consistent (no data-vs-state divergence on a mid-loop failure).
    fetched = []  # list of (ds, path, before, tbl, envelope_ok, dropped, total_points)
    for ds in DATASETS:
        path = os.path.join(out_dir, f"{ds}.parquet")
        before = blob.row_count(path)
        try:
            payload = _get(ds)  # TransientError -> propagate (whole unit becomes partial)
        except TransientError:
            # Record the transient sub-unit and surface partial WITHOUT publishing
            # anything (nothing has been written yet in this run).
            tally.transient_unit()
            cur_total = sum(blob.row_count(os.path.join(out_dir, f"{d}.parquet")) for d in DATASETS)
            last_db = None
            for d in DATASETS:
                p = os.path.join(out_dir, f"{d}.parquet")
                if blob.exists(p):
                    m = merge._max_obs_date(blob.read_table(p))
                    if m and (last_db is None or m > last_db):
                        last_db = m
            return finalize(tally, cur_total, last_db, source=SOURCE,
                            series_cursors=series_cursors)
        tbl, envelope_ok, dropped, total_points = _parse(ds, payload)
        fetched.append((ds, path, before, tbl, envelope_ok, dropped, total_points))

    # ---- Phase 2: classify + publish. No network here, so no late transient.
    for ds, path, before, tbl, envelope_ok, dropped, total_points in fetched:
        # Structural: envelope missing/non-dict/empty, OR a non-empty envelope
        # that parsed 0 rows, OR a dataset that previously had data now parsing
        # 0 rows, OR a large fraction of points unparseable (partial drift).
        if not envelope_ok:
            tally.structural_unit()
            continue
        if tbl.num_rows == 0:
            # Non-empty envelope but 0 usable rows -> schema break (OFR always
            # serves history). before>0 reinforces it but isn't required.
            tally.structural_unit()
            continue
        if total_points > 0 and (dropped / total_points) >= DROP_RATIO_STRUCTURAL:
            tally.structural_unit()
            continue
        # Healthy dataset: merge + publish.
        n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
        new_rows = max(0, n - before)
        tally.added_unit(new_rows)
        total += n
        if md and (maxd is None or md > maxd):
            maxd = md
        series_cursors.update(_series_maxes(tbl))

    # finalize: structural -> DefinitiveError; transient -> partial; else ok/no_change.
    return finalize(tally, total, maxd, source=SOURCE, series_cursors=series_cursors)
