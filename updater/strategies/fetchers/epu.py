"""S1 fetcher — Economic Policy Uncertainty (EPU), Baker/Bloom/Davis.

Public (free for academic use). Single grouped parquet clean_full/epu/epu.parquet,
schema (series_key, obs_date, value). Each policyuncertainty.com workbook is
republished as a FULL Year/Month history (back-revisions included, no since param),
and ~22 workbooks feed one combined file — so we re-fetch every workbook and MERGE
(dedup series_key+obs_date, new wins on revision, never-shrink). The parse logic is
reused verbatim from jobs/ingest_epu.py (do not re-discover the workbook list/format).

current_vintage probes the primary multi-country workbook (All_Country_Data.xlsx)
via HEAD ETag/Last-Modified — it carries ~30 countries and is republished whenever
the panel refreshes, so its tag is a faithful cheap signal for the whole source.

HONEST-STATUS CONTRACT (Tally + finalize): each workbook URL is a sub-unit.
  added_unit(n)    rows merged for the workbook (n>0 new, n==0 nothing new)
  empty_unit()     a 403/404 workbook (a country file can legitimately move/retire —
                   the ingester already treats these as fatal-skip-per-URL)
  structural_unit()a 200 with a real body that parsed 0 rows (schema/format break)
  transient_unit() timeout/5xx/429/network on the workbook (retry next run)
finalize() returns 'ok'/'no_change' only when nothing transient/structural-failed;
'partial' on any transient; and raises DefinitiveError if the WHOLE set came back
empty/404 (a wholesale outage, not a quiet month).
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

SOURCE = "epu"
DEDUP = ("series_key", "obs_date")

# Primary cheap vintage signal: the multi-country workbook (the dominant data source,
# ~30 countries) carries an ETag/Last-Modified and is republished on each panel refresh.
VINTAGE_URL = "https://www.policyuncertainty.com/media/All_Country_Data.xlsx"

# Workbooks whose layout the ingester's GENERIC Year/Month parser has NEVER read
# (they ship a single "Date" column as M/YYYY strings or datetimes, not Year+Month):
# EPU_KOR and EPU_NLD. Their data is absent from the published baseline and these
# return 200-with-a-real-body that parses 0 rows on EVERY run — a long-standing
# parser gap, NOT a fresh schema break. Treating them as structural would make this
# otherwise-healthy S1 source DefinitiveError forever, so they count as empty (a
# workbook this ingester can't capture). Any OTHER 200-but-0-rows IS structural.
KNOWN_UNPARSEABLE = {"EPU_KOR", "EPU_NLD"}


def _ingest():
    """Load jobs/ingest_epu.py so its workbook list + parsers stay single-sourced."""
    path = os.path.join(config.JOBS_DIR, "ingest_epu.py")
    spec = importlib.util.spec_from_file_location("_epu_ingest", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def current_vintage(unit):
    # ETag/Last-Modified of the All_Country workbook; None if the server exposes
    # none (strategy then fetches anyway — safe, merge dedups + never-shrinks).
    return http_vintage(VINTAGE_URL)


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
    job = _ingest()
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "epu.parquet")
    before = blob.row_count(path)
    tally = Tally()

    keys, dates, vals, seen = [], [], [], set()

    for label, url, code in job.EPU_SOURCES:
        # Per-workbook fetch: distinguish 403/404 (empty/retired) from
        # timeout/5xx/network (transient) without laundering either into success.
        data = None
        transient = False
        for attempt in range(3):
            try:
                r = requests.get(url, headers=UA, timeout=60, allow_redirects=True)
            except (requests.Timeout, requests.ConnectionError):
                transient = True
                continue
            if r.status_code == 200 and len(r.content) > 1000:
                data = r.content
                transient = False
                break
            if r.status_code in (403, 404):
                transient = False
                break  # workbook genuinely gone — empty sub-unit
            if r.status_code in (429, 500, 502, 503, 504):
                transient = True
                continue
            # any other non-200 -> treat as empty (no body to parse)
            transient = False
            break

        if data is None:
            if transient:
                tally.transient_unit()
            else:
                tally.empty_unit()
            continue

        try:
            if label == "EPU_global":
                k, d, v = job.parse_all_country_xlsx(data)
            else:
                k, d, v = job.parse_epu_xlsx(data, code)
        except Exception:
            # a 200 with a real body we could not parse
            if label in KNOWN_UNPARSEABLE:
                tally.empty_unit()
            else:
                tally.structural_unit()  # fresh schema/format break
            continue

        if not v:
            # 200, non-trivial body, but parsed 0 rows
            if label in KNOWN_UNPARSEABLE:
                tally.empty_unit()       # long-standing layout this parser can't read
            else:
                tally.structural_unit()  # a workbook that used to parse now doesn't -> break
            continue

        added = 0
        for ki, di, vi in zip(k, d, v):
            token = (ki, di)
            if token in seen:
                continue
            seen.add(token)
            keys.append(ki); dates.append(di); vals.append(vi)
            added += 1
        tally.added_unit(added)

    tbl = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date":   pa.array(dates, pa.date32()),
        "value":      pa.array(vals, pa.float64()),
    })

    # Nothing usable came back at all -> let finalize raise (wholesale outage) or
    # surface partial; never publish 0 rows (merge would refuse anyway).
    if tbl.num_rows == 0:
        return finalize(tally, before, None, source=SOURCE)

    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)

    # Report the rows actually NEW to the PUBLISHED file (not the in-memory dedup
    # count): on a quiet month every workbook re-states its existing history, so
    # n-before is the true delta. We re-state the Tally's added/empty from this
    # delta while PRESERVING transient/structural counts (the honest-failure bits).
    delta = max(0, n - before)
    tally.added = delta
    # We just published a healthy, non-shrinking table from real parsed rows, so the
    # source is demonstrably alive — a 0-delta here is a genuine quiet month, never
    # the wholesale-outage case the all-empty guard is meant to catch. Clearing the
    # empty count (workbooks that parsed but added nothing new) keeps that guard from
    # mis-firing on a healthy quiet month while leaving transient/structural intact.
    tally.empty = 0
    return finalize(tally, n, md, source=SOURCE, series_cursors=_series_maxes(tbl))
