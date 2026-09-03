"""S1 fetcher — Harvard Growth Lab, Atlas of Economic Complexity.

Three Harvard Dataverse datasets (by DOI), nine long-format parquet files under
clean_full/harvard_atlas, all schema (series_key, obs_date, value), dedup on
(series_key, obs_date). The Atlas re-publishes whole annual CSV snapshots each
release, so we re-fetch the WHOLE table(s) and MERGE (new wins on revision,
never-shrink @0.97).

Vintage signal (registry.adapter.vintage_signal): the Dataverse dataset API by
DOI — compare versionNumber/versionMinorNumber + lastUpdateTime. The original
ingester PINNED numeric datafile IDs, so a new Atlas release would be silently
ignored; here we resolve datafile IDs BY FILENAME from the latest version, so a
re-publish (new IDs) is picked up automatically. A 200 that parses 0 rows from a
real CSV body is structural; timeout/5xx/429/network is transient.
"""
from __future__ import annotations
import csv
import datetime as dt
import io
import os
import time

import pyarrow as pa
import requests

from ... import config, blob, merge
from ...errors import DefinitiveError
from ._common import Tally, finalize
from ._vintage import UA

SOURCE = "harvard_atlas"
DEDUP = ("series_key", "obs_date")

DV_VERSIONS = "https://dataverse.harvard.edu/api/datasets/:persistentId/versions/:latest"
DV_ACCESS = "https://dataverse.harvard.edu/api/access/datafile"

# DOI of each of the three Atlas datasets.
DOI_ECI = "doi:10.7910/DVN/XTAQMC"   # Growth Projections / Complexity Rankings
DOI_SVC = "doi:10.7910/DVN/NDDMSN"   # International Trade Data — Services
DOI_HS12 = "doi:10.7910/DVN/YAVJDF"  # International Trade Data — HS12 goods

# Each sub-unit: (output parquet, DOI, upstream filename, parse-kind, key prefix, label, min_ratio).
# Parse kinds mirror jobs/ingest_harvard_atlas.py exactly. min_ratio is the per-call
# never-shrink floor passed to merge_and_write (default 0.97). It is lowered ONLY for
# hs12_country_year.parquet, whose EXISTING published file is corrupt: the original
# ingester emitted bilateral rows with no reporter/partner identity, so 1,651,952
# physical rows dedup to just 52 distinct (series_key, obs_date). The corrected parse
# below yields ~826k genuinely-distinct bilateral series — a legitimate "shrink" that
# replaces garbage with clean data. The merge contract names exactly this case
# ("sources that legitimately shrink more must pass an explicit lower min_ratio"); we
# use the API's documented per-call knob and do NOT weaken the shared guard.
SUBUNITS = [
    # output parquet,                  doi,      upstream csv filename,                              kind,    prefix,        label,  min_ratio
    ("eci_rankings.parquet",           DOI_ECI,  "growth_proj_eci_rankings.csv",                     "eci",   "ATLAS:ECI",   None,   0.97),
    ("services_country_year.parquet",  DOI_SVC,  "services_unilateral_country_year.csv",             "svccy", "ATLAS:SVC",   None,   0.97),
    ("services_cp_1.parquet",          DOI_SVC,  "services_unilateral_country_product_year_1.csv",   "svccp", "ATLAS:SVC",   "1",    0.97),
    ("services_cp_2.parquet",          DOI_SVC,  "services_unilateral_country_product_year_2.csv",   "svccp", "ATLAS:SVC",   "2",    0.97),
    ("services_cp_4.parquet",          DOI_SVC,  "services_unilateral_country_product_year_4.csv",   "svccp", "ATLAS:SVC",   "4",    0.97),
    ("services_cp_6.parquet",          DOI_SVC,  "services_unilateral_country_product_year_6.csv",   "svccp", "ATLAS:SVC",   "6",    0.97),
    ("hs12_country_year.parquet",      DOI_HS12, "hs12_country_country_year.csv",                     "hs12cy", "ATLAS:HS12", None,   0.40),
    ("hs12_cp_hs1.parquet",            DOI_HS12, "hs12_country_product_year_1.csv",                   "hs12cp", "ATLAS:HS12", "hs1",  0.97),
    ("hs12_cp_hs2.parquet",            DOI_HS12, "hs12_country_product_year_2.csv",                   "hs12cp", "ATLAS:HS12", "hs2",  0.97),
]

DOIS = (DOI_ECI, DOI_SVC, DOI_HS12)
TIMEOUT = 180


# --------------------------------------------------------------------------- vintage

def _dataset_version(doi, session=None):
    """Return (versionNumber, versionMinorNumber, lastUpdateTime) for the latest
    released version, or None on transient/unavailable. Cheap (metadata only)."""
    s = session or requests
    try:
        r = s.get(DV_VERSIONS, headers=UA, params={"persistentId": doi}, timeout=60)
    except (requests.Timeout, requests.ConnectionError):
        return None
    if r.status_code != 200:
        return None
    try:
        d = r.json()["data"]
    except (ValueError, KeyError):
        return None
    return (d.get("versionNumber"), d.get("versionMinorNumber"), d.get("lastUpdateTime"))


def current_vintage(unit):
    """Composite token across the three Atlas DOIs: each dataset's
    version + lastUpdateTime. Changes iff any of the three re-publishes. None if
    none of the three can be probed (strategy then fetches anyway — safe)."""
    s = requests.Session()
    parts = []
    seen = False
    for doi in DOIS:
        v = _dataset_version(doi, session=s)
        if v is None:
            parts.append("?")
            continue
        seen = True
        major, minor, updated = v
        parts.append(f"{major}.{minor}@{updated}")
    if not seen:
        return None
    return "|".join(parts)


def _file_id_index(doi, session=None):
    """Resolve {filename: datafile_id} for the latest version of `doi`. Returns
    None on transient/unavailable so the caller can record a transient sub-unit."""
    s = session or requests
    try:
        r = s.get(DV_VERSIONS, headers=UA, params={"persistentId": doi}, timeout=60)
    except (requests.Timeout, requests.ConnectionError):
        return None
    if r.status_code in (429, 500, 502, 503, 504):
        return None
    if r.status_code != 200:
        return None
    try:
        files = r.json()["data"].get("files", [])
    except (ValueError, KeyError):
        return None
    out = {}
    for f in files:
        df = f.get("dataFile", {})
        fn = df.get("filename")
        fid = df.get("id")
        if fn and fid is not None:
            out[fn] = fid
    return out


# --------------------------------------------------------------------------- parse

def _read_csv(data: bytes):
    text = data.decode("utf-8-sig", errors="replace")
    rows = list(csv.reader(io.StringIO(text)))
    if not rows:
        return [], []
    return rows[0], rows[1:]


def _col_idx(headers, candidates):
    lh = [h.lower().strip() for h in headers]
    for c in candidates:
        if c.lower() in lh:
            return lh.index(c.lower())
    return None


def _obs_date(row, year_i):
    try:
        yr = int(float(row[year_i]))
    except (ValueError, TypeError, IndexError):
        return None
    return dt.date(yr, 12, 31)


_NA = ("", ".", "NA", "N/A", "#N/A")


def _parse(kind, prefix, label, data: bytes):
    """Mirror jobs/ingest_harvard_atlas.py parse logic exactly, returning
    (keys, dates, vals)."""
    headers, rows = _read_csv(data)
    year_i = _col_idx(headers, ["year"])
    if year_i is None:
        return [], [], []

    if kind == "eci":
        ctry_i = _col_idx(headers, ["country", "country_id", "iso3"])
        skip = {"year", "country", "country_id", "iso3", "region", "income_group", ""}
        num_idx = [(h.strip(), i) for i, h in enumerate(headers)
                   if h.lower().strip() not in skip and i != ctry_i]
        keys, dates, vals = [], [], []
        for row in rows:
            od = _obs_date(row, year_i)
            if od is None:
                continue
            ctry = row[ctry_i].strip() if ctry_i is not None and ctry_i < len(row) else ""
            for col, ci in num_idx:
                if ci >= len(row):
                    continue
                raw = row[ci].strip()
                if raw in _NA:
                    continue
                try:
                    v = float(raw)
                except (ValueError, TypeError):
                    continue
                key = f"{prefix}:{col}:{ctry}" if ctry else f"{prefix}:{col}"
                keys.append(key); dates.append(od); vals.append(v)
        return keys, dates, vals

    if kind == "svccy":
        ctry_i = _col_idx(headers, ["country_id", "country", "iso3"])
        skip = {"year", "country_id", "country", "iso3", "country_name", ""}
        num_idx = [(h.strip(), i) for i, h in enumerate(headers)
                   if h.lower().strip() not in skip and i not in [year_i, ctry_i]]
        keys, dates, vals = [], [], []
        for row in rows:
            od = _obs_date(row, year_i)
            if od is None:
                continue
            ctry = row[ctry_i].strip() if ctry_i is not None and ctry_i < len(row) else ""
            for col, ci in num_idx:
                if ci >= len(row):
                    continue
                raw = row[ci].strip()
                if raw in _NA:
                    continue
                try:
                    v = float(raw)
                except (ValueError, TypeError):
                    continue
                key = f"{prefix}:{col}:{ctry}" if ctry else f"{prefix}:{col}"
                keys.append(key); dates.append(od); vals.append(v)
        return keys, dates, vals

    if kind == "svccp":
        ctry_i = _col_idx(headers, ["country_id", "country", "iso3"])
        product_i = _col_idx(headers, ["service_id", "product_id", "product", "sitc"])
        skip = {"year", "country_id", "country", "iso3", "country_name",
                "service_id", "product_id", "product", "sitc", ""}
        num_idx = [(h.strip(), i) for i, h in enumerate(headers)
                   if h.lower().strip() not in skip and i not in [year_i, ctry_i, product_i]]
        keys, dates, vals = [], [], []
        for row in rows:
            od = _obs_date(row, year_i)
            if od is None:
                continue
            ctry = row[ctry_i].strip() if ctry_i is not None and ctry_i < len(row) else ""
            prod = row[product_i].strip() if product_i is not None and product_i < len(row) else ""
            for col, ci in num_idx:
                if ci >= len(row):
                    continue
                raw = row[ci].strip()
                if raw in _NA:
                    continue
                try:
                    v = float(raw)
                except (ValueError, TypeError):
                    continue
                parts = [x for x in [col, ctry, prod] if x]
                keys.append(f"{prefix}:" + ":".join(parts)); dates.append(od); vals.append(v)
        return keys, dates, vals

    if kind == "hs12cy":
        # Bilateral file header is: country_id, country_iso3_code, partner_country_id,
        # partner_iso3_code, year, export_value, import_value. The original ingester
        # looked for "exporter"/"importer" columns (which don't exist), so it emitted
        # keys with NO bilateral identity and every pair for a year collapsed onto the
        # same (series_key, obs_date) — 1.65M rows but only 52 unique dedup keys. We
        # resolve the reporter (country_id) and partner (partner_country_id) that
        # actually exist so each bilateral observation is a distinct series.
        exp_i = _col_idx(headers, ["exporter", "exporter_id", "exp_country", "country_id"])
        imp_i = _col_idx(headers, ["importer", "importer_id", "imp_country", "partner_country_id"])
        skip = {"year", "exporter", "exporter_id", "exp_country", "importer", "importer_id",
                "imp_country", "exporter_name", "importer_name",
                "country_id", "country_iso3_code", "partner_country_id", "partner_iso3_code", ""}
        num_idx = [(h.strip(), i) for i, h in enumerate(headers)
                   if h.lower().strip() not in skip and i not in [year_i, exp_i, imp_i]]
        keys, dates, vals = [], [], []
        for row in rows:
            od = _obs_date(row, year_i)
            if od is None:
                continue
            exp = row[exp_i].strip() if exp_i is not None and exp_i < len(row) else ""
            imp = row[imp_i].strip() if imp_i is not None and imp_i < len(row) else ""
            for col, ci in num_idx:
                if ci >= len(row):
                    continue
                raw = row[ci].strip()
                if raw in _NA:
                    continue
                try:
                    v = float(raw)
                except (ValueError, TypeError):
                    continue
                parts = [x for x in [col, exp, imp] if x]
                keys.append(f"{prefix}:" + ":".join(parts)); dates.append(od); vals.append(v)
        return keys, dates, vals

    if kind == "hs12cp":
        ctry_i = _col_idx(headers, ["country_id", "country", "iso3"])
        product_i = _col_idx(headers, ["hs_product_code", "product_id", "hs"])
        skip = {"year", "country_id", "country", "iso3", "country_name",
                "hs_product_code", "product_id", "hs", "hs_product_name", ""}
        num_idx = [(h.strip(), i) for i, h in enumerate(headers)
                   if h.lower().strip() not in skip and i not in [year_i, ctry_i, product_i]]
        keys, dates, vals = [], [], []
        for row in rows:
            od = _obs_date(row, year_i)
            if od is None:
                continue
            ctry = row[ctry_i].strip() if ctry_i is not None and ctry_i < len(row) else ""
            prod = row[product_i].strip() if product_i is not None and product_i < len(row) else ""
            for col, ci in num_idx:
                if ci >= len(row):
                    continue
                raw = row[ci].strip()
                if raw in _NA:
                    continue
                try:
                    v = float(raw)
                except (ValueError, TypeError):
                    continue
                parts = [x for x in [col, ctry, prod] if x]
                keys.append(f"{prefix}:" + ":".join(parts)); dates.append(od); vals.append(v)
        return keys, dates, vals

    return [], [], []


def _series_maxes(keys, dates):
    out = {}
    for k, d in zip(keys, dates):
        if d is None:
            continue
        if k not in out or d > out[k]:
            out[k] = d
    return {k: v.isoformat() for k, v in out.items()}


# --------------------------------------------------------------------------- update

def update(unit, since):
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    tally = Tally()
    total_rows = 0
    last_obs = None
    cursors = {}

    s = requests.Session()
    # Resolve current datafile IDs by filename per DOI (so a re-publish is picked up).
    id_index = {}
    for doi in DOIS:
        idx = _file_id_index(doi, session=s)
        id_index[doi] = idx  # None means transient/unavailable for this DOI

    for basename, doi, filename, kind, prefix, label, min_ratio in SUBUNITS:
        path = os.path.join(out_dir, basename)
        before = blob.row_count(path)
        idx = id_index.get(doi)
        if idx is None:
            # couldn't resolve the dataset listing this run
            tally.transient_unit(f"{basename}: dataset listing for DOI {doi} unresolved")
            continue
        file_id = idx.get(filename)
        if file_id is None:
            # The expected file vanished from the latest version -> structural change.
            tally.structural_unit(f"{basename}: {filename} is gone from the latest version")
            continue

        url = f"{DV_ACCESS}/{file_id}"
        content = None
        for attempt in range(4):
            try:
                r = s.get(url, headers=UA, timeout=TIMEOUT)
            except (requests.Timeout, requests.ConnectionError):
                time.sleep(2 ** attempt)
                continue
            if r.status_code == 200:
                content = r.content
                break
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(2 ** attempt)
                continue
            # hard 4xx (incl. 404 on a previously-listed id) -> stop retrying
            break

        if content is None:
            tally.transient_unit(f"{basename}: download of {filename} returned nothing")
            continue

        keys, dates, vals = _parse(kind, prefix, label, content)
        if not keys:
            # 200 with a real CSV body that parsed 0 numeric rows -> structural break.
            tally.structural_unit(
                f"{basename}: real CSV body parsed 0 numeric rows over {before:,} stored")
            continue

        tbl = pa.table({
            "series_key": pa.array(keys, pa.string()),
            "obs_date": pa.array(dates, pa.date32()),
            "value": pa.array(vals, pa.float64()),
        })
        try:
            n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP,
                                          min_ratio=min_ratio)
        except DefinitiveError:
            # A never-shrink / column-drop refusal on ONE file: the existing data is
            # kept untouched (merge publishes nothing). Surface as structural so the
            # whole-source status is honest; do not abort the other sub-units.
            tally.structural_unit(
                f"{basename}: merge refused over {before:,} stored rows (min_ratio "
                f"{min_ratio})")
            total_rows += before
            continue
        total_rows += n
        if md is not None and (last_obs is None or md > last_obs):
            last_obs = md
        cursors.update(_series_maxes(keys, dates))
        tally.added_unit(max(0, n - before))

    return finalize(tally, total_rows, last_obs, source=SOURCE, series_cursors=cursors)
