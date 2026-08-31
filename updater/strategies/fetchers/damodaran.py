"""S1 fetcher — Damodaran Online Financial Data (NYU Stern, Aswath Damodaran).

Annual cross-sectional snapshots (country risk premiums, betas, margins, multiples,
cost of capital, etc.), re-estimated each January and re-posted at pages.stern.nyu.edu.
Single grouped parquet clean_full/damodaran/damodaran.parquet, schema
(series_key, obs_date, value); series_key = DAMODARAN:<dataset>:<col>:<entity>.

S1 (overwrite_if_changed): each release re-publishes the whole workbooks, so we
re-fetch every dataset by REUSING jobs/ingest_damodaran.py's URL list + parse logic,
build one pyarrow table, and MERGE (dedup series_key+obs_date, new wins on revision,
never-shrink). The cheap vintage is the country-premium workbook's HEAD token
(Last-Modified/ETag) — the canonical annual-release marker per the registry hint.

A 200 that parses 0 rows from a real body is structural; a timeout/5xx/429/network
failure is transient; all datasets 404 (URLs rotated) -> all-empty window -> structural.
"""
from __future__ import annotations
import datetime as dt
import importlib.util
import os

import pyarrow as pa
import requests

from ... import config, blob, merge
from ..base import Result
from ._common import Tally, finalize
from ._vintage import http_vintage, UA

SOURCE = "damodaran"
DEDUP = ("series_key", "obs_date")

# The country-premium workbook is the canonical annual-release marker (rotates name each
# January, e.g. ctrypremApr26.xlsx). HEAD Last-Modified/ETag on it is the cheap vintage.
VINTAGE_URL = "https://pages.stern.nyu.edu/~adamodar/pc/datasets/ctrypremApr26.xlsx"

# Transient HTTP statuses (retry next tick) vs definitive (treat as not-found / skip dataset).
_TRANSIENT_STATUS = (429, 500, 502, 503, 504)


def _ingest_mod():
    """Load jobs/ingest_damodaran.py as a module to reuse ANNUAL_DATASETS + parse_dataset.

    The ingester is a standalone script (not a package), so import it by path rather
    than copy its 20-dataset table / multi-sheet parse logic."""
    path = os.path.join(config.JOBS_DIR, "ingest_damodaran.py")
    spec = importlib.util.spec_from_file_location("_ingest_damodaran", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def current_vintage(unit):
    """Cheap probe: HEAD token of the country-premium workbook (annual-release marker)."""
    return http_vintage(VINTAGE_URL)


def _fetch(url):
    """Re-fetch one workbook. Returns (bytes | None, outcome) where outcome is one of
    'ok', 'transient', 'notfound' so update() can tally honestly."""
    try:
        r = requests.get(url, headers=UA, timeout=120, allow_redirects=True)
    except (requests.Timeout, requests.ConnectionError):
        return None, "transient"
    if r.status_code in _TRANSIENT_STATUS:
        return None, "transient"
    if r.status_code == 200 and len(r.content) > 500:
        return r.content, "ok"
    # 403/404/other or a too-small body -> treat as this workbook missing/rotated.
    return None, "notfound"


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
    path = os.path.join(out_dir, "damodaran.parquet")
    before = blob.row_count(path)
    tally = Tally()

    ing = _ingest_mod()

    all_keys, all_dates, all_vals = [], [], []
    for entry in ing.ANNUAL_DATASETS:
        if len(entry) == 4:
            dataset, url, _entity_type, specific_sheets = entry
        else:
            dataset, url, _entity_type = entry
            specific_sheets = None

        data, outcome = _fetch(url)
        if outcome == "transient":
            tally.transient_unit()
            continue
        if outcome == "notfound" or not data:
            # A rotated/missing workbook on a normally-multi-file source: count as empty
            # (genuine 404 for one of ~22 files). If EVERY file 404s, finalize's
            # all-empty-window guard escalates to a structural break (URLs rotated).
            tally.empty_unit()
            continue

        try:
            k, d, v = ing.parse_dataset(data, dataset, url, specific_sheets)
        except Exception:
            # 200 returned a body but it failed to parse -> structural/schema break.
            tally.structural_unit()
            continue

        if v:
            all_keys.extend(k); all_dates.extend(d); all_vals.extend(v)
            tally.attempted += 1            # parsed-ok; real new-row count assigned after merge
        else:
            # 200 with a real (>500B) body but 0 rows parsed -> structural break.
            tally.structural_unit()

    # If nothing parsed (all transient, or guards not yet tripped), don't write.
    if not all_vals:
        return finalize(tally, before, None, source=SOURCE)

    tbl = pa.table({
        "series_key": pa.array(all_keys, pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals, pa.float64()),
    })

    # Publish ONLY via merge (atomic, dedup, never-shrink). The honest new-row count is
    # the merge delta (n-before), not parsed rows — an unchanged release merges to 0 new
    # rows and must read as no_change, not ok.
    #
    # min_ratio=0.92 (vs default 0.97): the legacy ingester wrote damodaran.parquet WITHOUT
    # deduping. CORRECTED TWICE 2026-08-31 ("the four"; R527 then R535 — the first
    # correction of the false "0 distinct observations lost" itself shipped a false
    # "that collapse is history"): the store TODAY still holds every duplicate —
    # 26,536 rows / 24,687 distinct (series_key,obs_date), 1,842 dup groups, 721 of
    # them carrying DIFFERING values (measured; dups surviving under this merge-dedup
    # fetcher PROVE no merge has ever completed here). The first merge that completes
    # will collapse those ~1,849 rows and PICK one of two real numbers for each of
    # the 721 (last-in-sort wins) — a PENDING lossy event, disclosed here, not
    # history. Also pending: ONE mangled-shaped key survives outside the margins
    # class (histretSP:Real_Estate:328_442, R480's disease; the margins-class rows
    # themselves measure 0 in-store). 0.92 admits that collapse yet
    # still catches a real truncation — e.g. a pull missing even one ~1400-obs workbook
    # (~21900 rows) falls below the 0.92 floor (23177) and is refused. After the first run
    # the file is dup-free and stays >= its distinct count. This is an explicit per-call
    # min_ratio (sanctioned by merge_and_write), NOT a weakening of the shared guard.
    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP, min_ratio=0.92)
    new_rows = max(0, n - before)
    if new_rows:
        tally.added += new_rows
    else:
        tally.empty += 1
    return finalize(tally, n, md, source=SOURCE, series_cursors=_series_maxes(all_keys, all_dates))
