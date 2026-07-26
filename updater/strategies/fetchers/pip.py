"""S1 fetcher — World Bank Poverty & Inequality Platform (PIP).

CC BY 4.0 (World Bank). Single grouped parquet clean_full/pip/pip.parquet,
schema (series_key, obs_date, value), series_key='PIP:<indicator>:<cc>:<povline_label>'
(annual, Dec-31). PIP has no observation-level since= filter (queries are
country=all / year=all) and it RE-ESTIMATES history every release, so we gate the
full rebuild on the PIP version catalog (/pip/v1/versions) and MERGE the re-fetched
whole table (dedup series_key+obs_date, revised values win, never-shrink).

PPP lineage note: the published parquet's poverty-line labels (100/190/215/.../2170)
are 2017-PPP $/day thresholds (e.g. 215 = $2.15/day in 2017 PPP). The PIP API now
defaults to the 2021-PPP product, under which the SAME label would carry a DIFFERENT
real threshold/value and corrupt the series. So we PIN every query to the current
2017-PPP PROD release (the lineage the 470k existing rows are built on); the vintage
token is exactly that pinned release string, so a new 2017-PPP release re-triggers.

Ten sub-units (one bulk all-countries x all-years call per poverty line); a 200 that
parses 0 rows from a real body is structural; 429/5xx/network is transient.
"""
from __future__ import annotations
import datetime as dt
import os
import time

import pyarrow as pa
import requests

from ... import config, merge, blob
from ..base import Result
from ._common import Tally, finalize
from ._vintage import UA

SOURCE = "pip"
DEDUP = ("series_key", "obs_date")

VERSIONS_URL = "https://api.worldbank.org/pip/v1/versions"
API = "https://api.worldbank.org/pip/v1/pip"

# Pin to the 2017-PPP product line — the published parquet's labels are 2017-PPP $/day.
PPP_VERSION = "2017"

# Poverty lines in 2017 PPP $/day -> the label embedded in series_key (from the ingester).
POV_LINES = [1.00, 1.90, 2.15, 3.20, 3.65, 5.50, 6.85, 10.00, 15.00, 21.70]
POV_LABELS = {1.00: "100", 1.90: "190", 2.15: "215", 3.20: "320", 3.65: "365",
              5.50: "550", 6.85: "685", 10.00: "1000", 15.00: "1500", 21.70: "2170"}

DIST_INDICATORS = [
    "mean", "median", "mld", "gini", "polarization",
    "decile1", "decile2", "decile3", "decile4", "decile5",
    "decile6", "decile7", "decile8", "decile9", "decile10",
]
POV_INDICATORS = ["headcount", "poverty_gap", "poverty_severity", "watts"]

RATE = 1.0  # seconds between poverty-line calls (polite; API is generous)


def _current_release_for_ppp(ppp=PPP_VERSION, session=None):
    """Return the latest PROD release string for the pinned PPP line, e.g.
    '20260324_2017_01_02_PROD', or None if it can't be determined cheaply."""
    s = session or requests
    for a in range(3):
        try:
            r = s.get(VERSIONS_URL, headers=UA, timeout=60)
        except (requests.Timeout, requests.ConnectionError):
            if a == 2:
                return None
            time.sleep(2 ** a)
            continue
        if r.status_code in (429, 500, 502, 503, 504):
            if a == 2:
                return None
            time.sleep(2 ** a)
            continue
        if r.status_code != 200:
            return None
        try:
            rows = r.json()
        except ValueError:
            return None
        if not isinstance(rows, list):
            return None
        cands = [e for e in rows
                 if isinstance(e, dict)
                 and str(e.get("ppp_version")) == str(ppp)
                 and str(e.get("identity", "")).upper() == "PROD"
                 and e.get("version")]
        if not cands:
            return None
        # release_version is YYYYMMDD; newest wins.
        cands.sort(key=lambda e: str(e.get("release_version", "")), reverse=True)
        return cands[0]["version"]
    return None


def current_vintage(unit):
    # The pinned-PPP release string changes iff World Bank publishes a new 2017-PPP
    # estimate -> exactly the signal that history was re-estimated. None is safe
    # (strategy then fetches anyway; merge dedups + never-shrinks).
    return _current_release_for_ppp()


def _get_rows(version, pline, retries=4):
    """Bulk all-countries x all-years for one poverty line, pinned to `version`.
    Returns (rows, ok): ok=False means a transient failure for this sub-unit."""
    params = {
        "country": "all", "year": "all",
        "povline": pline, "fill_gaps": "false",
        "welfare_type": "all", "reporting_level": "national",
        "format": "json", "version": version,
    }
    for attempt in range(retries):
        try:
            r = requests.get(API, params=params, headers=UA, timeout=120)
        except (requests.Timeout, requests.ConnectionError):
            if attempt >= retries - 1:
                return [], "transient"
            time.sleep(3 * (attempt + 1))
            continue
        if r.status_code == 200:
            try:
                j = r.json()
            except ValueError:
                return [], "structural"   # 200 with non-JSON body -> schema/structural break
            if isinstance(j, list):
                return j, "ok"            # 200 JSON list (possibly empty = discontinued line)
            # 200 JSON OBJECT = PIP's error envelope, {"ok":[false],"error":[...]}.
            # NOT automatically structural. PIP answers its own server-side timeout this
            # way: povline 21.7 (the widest line, so the heaviest query) returns
            #   {"ok":[false],"error":["Request exceeded timeout of 180 seconds"],...}
            # while the other nine lines each return 2,475 rows (measured 2026-07-25).
            # Calling that a permanent schema break turned a CAPACITY problem into a
            # discontinued-series verdict, demoted the source to partial, and skipped
            # the retry that would actually have fixed it.
            err = " ".join(str(x) for x in (j.get("error") or [])) if isinstance(j, dict) else ""
            if any(w in err.lower() for w in ("timeout", "timed out", "too large",
                                              "try again", "unavailable", "busy")):
                if attempt >= retries - 1:
                    return [], "transient"
                time.sleep(10 * (attempt + 1))   # heavier backoff: the server is loaded
                continue
            return [], "structural"       # any other error envelope = real schema break
        if r.status_code in (429, 500, 502, 503, 504):
            if attempt >= retries - 1:
                return [], "transient"
            time.sleep(30 if r.status_code == 429 else 3 * (attempt + 1))
            continue
        # other hard 4xx (e.g. the pinned version was removed) -> definitive/structural
        return [], "structural"
    return [], "transient"


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
    path = os.path.join(out_dir, "pip.parquet")
    before = blob.row_count(path)
    tally = Tally()

    version = _current_release_for_ppp()
    if version is None:
        # Can't determine the pinned-PPP release cheaply -> treat the whole pull as
        # transient (retry next tick); existing data left untouched.
        tally.transient_unit()
        return finalize(tally, before, None, source=SOURCE)

    keys, dates, vals = [], [], []
    transient = 0          # poverty-line sub-units that transient-failed (retry next run)
    empty = 0              # poverty-line sub-units: 200 with an empty list (line not served)
    structural = 0         # poverty-line sub-units: 200 with records but all-unparseable
    parsed_lines = 0       # poverty-line sub-units that yielded >0 obs
    for pline in POV_LINES:
        label = POV_LABELS[pline]
        rows, st = _get_rows(version, pline)
        if st == "transient":
            transient += 1
            time.sleep(RATE)
            continue
        if st == "structural":
            # non-JSON 200, a JSON error object, or a hard 4xx (e.g. the pinned PIP
            # version was removed). This is a DEFINITIVE break, not a quiet day — count
            # it structural so finalize raises DefinitiveError -> unit 'partial' -> ATTENTION.
            structural += 1
            time.sleep(RATE)
            continue
        if not rows:
            # 200 with an empty JSON list: PIP serves no records for this poverty line
            # under the pinned product/release (e.g. a 2021-PPP line under the 2017-PPP
            # product). Historical rows are preserved by the merge. Honest empty sub-unit.
            empty += 1
            time.sleep(RATE)
            continue
        line_obs = 0
        for rec in rows:
            ctry = (rec.get("country_code") or "").strip()
            yr = rec.get("reporting_year") or rec.get("year")
            if not ctry or not yr:
                continue
            try:
                obs_d = dt.date(int(yr), 12, 31)
            except (TypeError, ValueError):
                continue
            for ind in POV_INDICATORS + DIST_INDICATORS:
                v = rec.get(ind)
                if v is None:
                    continue
                try:
                    fv = float(v)
                except (TypeError, ValueError):
                    continue
                if fv != fv:  # NaN
                    continue
                keys.append(f"PIP:{ind}:{ctry}:{label}")
                dates.append(obs_d)
                vals.append(fv)
                line_obs += 1
        if line_obs:
            parsed_lines += 1
        else:
            structural += 1  # 200 with records but every indicator unparseable -> structural
        time.sleep(RATE)

    # Surface per-line outcomes BEFORE publishing so the status is honest. A line whose
    # 200 body had records but none parsed is a real structural break (raises). A line
    # that returned an empty list (discontinued/not-served line, history kept by merge)
    # is an honest empty sub-unit. Transients downgrade the whole run to 'partial'.
    for _ in range(structural):
        tally.structural_unit()
    for _ in range(empty):
        tally.empty_unit()
    for _ in range(transient):
        tally.transient_unit()

    # If nothing parsed at all, let finalize raise the honest structural/empty-window error.
    if not keys:
        return finalize(tally, before, None, source=SOURCE, empty_window_floor=len(POV_LINES) - 1)

    tbl = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date": pa.array(dates, pa.date32()),
        "value": pa.array(vals, pa.float64()),
    })

    # The whole re-fetched table is published in ONE atomic merge (dedup on
    # series_key+obs_date, revised values win, never-shrink). Count real NEW rows.
    #
    # min_ratio=0.95 (vs the default 0.97): the ORIGINAL streaming-writer ingest left
    # ~16.7k duplicate (series_key,obs_date) rows in the published file (470,188 raw vs
    # 453,468 DISTINCT keys, a 3.56% duplicate fraction). merge_and_write rightly dedups
    # them, so the first clean merge dips to ~453,474 — that is MORE than the distinct
    # existing keys (no series lost; older survey-year estimates the current release
    # dropped are still kept by the union), but ~3.6% under the RAW count. 0.95 admits
    # this one-time de-duplication with margin while still blocking a real truncation.
    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP, min_ratio=0.95)
    tally.added_unit(max(0, n - before))
    return finalize(tally, n, md, source=SOURCE, series_cursors=_series_maxes(keys, dates),
                    empty_window_floor=len(POV_LINES) - 1)
