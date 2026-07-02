"""S1 fetcher — Global Carbon Budget (GCB), annual, re-estimated each vintage.

GCB is re-estimated yearly with whole-series back-revisions, so the correct
semantics is a full re-fetch + MERGE (dedup series_key+obs_date, new wins on
revision, never-shrink). Four grouped parquets under clean_full/gcb/, all sharing
schema (series_key: string, obs_date: date32 [Dec-31], value: float64):

    gcb_budget.parquet          global/historical budget aggregates + sinks
    gcb_national_fossil.parquet territorial / consumption / transfers by country
    gcb_luc.parquet             land-use-change by model by country
    gcb_fossil_flat.parquet     GCP fossil CO2 flat CSV (country x year x fuel)

Vintage signal (registry adapter.vintage_signal): the Zenodo concept-DOI 'latest
version' record id — concept record 5569234 (concept DOI 10.5281/zenodo.5569234)
resolves to the current vintage's numeric record id (e.g. 14106218 -> 2024v18,
17417124 -> 2025v15). That id changes iff a new GCB vintage is published, so it is
the cheap probe. The three globalcarbonbudget.org /download/NNNN/ links are stable
redirects that already serve the *current* vintage's XLSX, so they are reused as-is.
The Zenodo flat CSV filename is version-stamped, so we resolve it dynamically from
the concept-latest record's file list (the hardcoded 2024v18 name would freeze).

Each component is its own sub-unit in the Tally: a 200 that parses 0 rows from a
real body -> structural; timeout/5xx/429/network -> transient; genuine empty ->
empty. Publish is ONLY via merge.merge_and_write; never write parquet directly.
"""
from __future__ import annotations

import datetime as dt
import io
import os

import pyarrow as pa
import requests

from ... import config, merge, blob
from ..base import Result
from ._common import Tally, finalize
from ._vintage import UA, http_vintage

# Reuse the existing ingester's parse logic + sheet maps (import by package path;
# jobs/ resolves as a namespace package under the repo root on the sys.path).
from jobs import ingest_gcb as ig

SOURCE = "gcb"
DEDUP = ("series_key", "obs_date")

# Stable redirecting download links (auto-serve the current vintage XLSX).
GCB_BUDGET_URL = ig.GCB_BUDGET_URL
GCB_FOSSIL_URL = ig.GCB_FOSSIL_URL
GCB_LUC_URL = ig.GCB_LUC_URL

# Zenodo concept record for the GCB fossil dataset (stable across all versions).
ZENODO_CONCEPT = "5569234"
ZENODO_LATEST_API = f"https://zenodo.org/api/records/{ZENODO_CONCEPT}/versions/latest"

_TRANSIENT_HTTP = (429, 500, 502, 503, 504)


def current_vintage(unit):
    """Cheap probe: the Zenodo concept-latest record id (changes per vintage).

    Falls back to the budget XLSX HTTP vintage if Zenodo is unreachable, and
    returns None if neither can be determined (strategy then fetches anyway,
    which is safe under merge_and_write's dedup + never-shrink)."""
    try:
        r = requests.get(ZENODO_LATEST_API, headers=UA, timeout=60)
        if r.status_code == 200:
            j = r.json()
            rid = j.get("id")
            ver = (j.get("metadata") or {}).get("version")
            if rid is not None:
                return f"zenodo:{rid}:{ver}" if ver else f"zenodo:{rid}"
    except (requests.Timeout, requests.ConnectionError, ValueError):
        pass
    # Fallback: ETag/Last-Modified on the budget XLSX redirect.
    return http_vintage(GCB_BUDGET_URL)


def _zenodo_flat_url():
    """Resolve the *_MtCO2_flat.csv download URL from the concept-latest record.

    Returns (url, status) where status is 'ok', 'transient', or 'structural'.
    The filename is version-stamped (GCB2025v15_MtCO2_flat.csv), so we cannot
    hardcode it; we pick the file whose key matches *_MtCO2_flat.csv exactly."""
    try:
        r = requests.get(ZENODO_LATEST_API, headers=UA, timeout=60)
    except (requests.Timeout, requests.ConnectionError):
        return None, "transient"
    if r.status_code in _TRANSIENT_HTTP:
        return None, "transient"
    if r.status_code != 200:
        return None, "structural"
    try:
        j = r.json()
    except ValueError:
        return None, "structural"
    for f in j.get("files", []):
        key = f.get("key", "")
        if key.endswith("_MtCO2_flat.csv") and "metadata" not in key:
            link = (f.get("links") or {}).get("self")
            if link:
                return link, "ok"
    return None, "structural"


def _fetch(url):
    """GET with the ingester's UA. Returns (content_bytes, status) where status
    is 'ok' | 'transient' | 'structural'."""
    try:
        r = requests.get(url, headers=UA, timeout=120, allow_redirects=True)
    except (requests.Timeout, requests.ConnectionError):
        return None, "transient"
    if r.status_code in _TRANSIENT_HTTP:
        return None, "transient"
    if r.status_code != 200:
        return None, "structural"
    return r.content, "ok"


def _table(keys, dates, vals):
    return pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date": pa.array(dates, pa.date32()),
        "value": pa.array(vals, pa.float64()),
    })


def _series_maxes(tbl, acc):
    """Accumulate {series_key: max obs_date isoformat} into acc."""
    if tbl.num_rows == 0:
        return acc
    for k, d in zip(tbl.column("series_key").to_pylist(), tbl.column("obs_date").to_pylist()):
        if d is None:
            continue
        if k not in acc or d > acc[k]:
            acc[k] = d
    return acc


def _parse_xlsx_sheets(content, sheet_specs, parser):
    """Open an XLSX from bytes and run `parser` over each (sheet_name, prefix[, ...])
    spec, concatenating (keys, dates, vals). Returns (keys, dates, vals)."""
    import openpyxl
    wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    all_k, all_d, all_v = [], [], []
    for spec in sheet_specs:
        sname, prefix = spec[0], spec[1]
        if sname not in wb.sheetnames:
            continue
        k, d, v = parser(wb[sname], prefix)
        all_k.extend(k); all_d.extend(d); all_v.extend(v)
    return all_k, all_d, all_v


def _component(tally, cursors, status, content, path, build_fn, min_ratio=0.97):
    """Handle one component: fetch-status -> Tally, parse, merge_and_write.

    `status` is the fetch status; `content` the bytes (or None); `build_fn(content)`
    returns (keys, dates, vals). `min_ratio` is the per-component never-shrink floor
    passed through to merge_and_write (default 0.97; relaxed only where a measured,
    legitimate dedup-driven shrink is expected — see the budget component below).
    Returns rows in the published file after this component (or the existing count
    on a failure)."""
    before = blob.row_count(path)
    if status == "transient":
        tally.transient_unit()
        return before
    if status == "structural":
        tally.structural_unit()
        return before
    keys, dates, vals = build_fn(content)
    tbl = _table(keys, dates, vals)
    if tbl.num_rows == 0:
        # 200 with a real body but parsed nothing -> structural break.
        tally.structural_unit()
        return before
    n, _ = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP, min_ratio=min_ratio)
    tally.added_unit(max(0, n - before))
    _series_maxes(tbl, cursors)
    return n


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    p_budget = os.path.join(out_dir, "gcb_budget.parquet")
    p_fossil = os.path.join(out_dir, "gcb_national_fossil.parquet")
    p_luc = os.path.join(out_dir, "gcb_luc.parquet")
    p_flat = os.path.join(out_dir, "gcb_fossil_flat.parquet")

    before_total = (blob.row_count(p_budget) + blob.row_count(p_fossil)
                    + blob.row_count(p_luc) + blob.row_count(p_flat))

    tally = Tally()
    cursors: dict = {}

    # --- 1. Global budget aggregates (XLSX) ---
    # The "Land-Use Change Emissions" global sheet has several model columns that
    # share a column LABEL (e.g. multiple "Net" columns), so parse_global_sheet
    # emits multiple rows per (series_key, obs_date). The ORIGINAL ingester wrote
    # these undeduplicated (8254 raw rows -> 6759 distinct); merge_and_write
    # correctly collapses them, which against that inflated baseline looks like an
    # ~18% shrink (measured ratio 0.819). This is a legitimate dedup of a known
    # un-deduplicated baseline, not a truncated upstream — so this ONE component
    # gets a relaxed never-shrink floor (0.80). The other three keep the default
    # 0.97 guard. (We do NOT weaken merge itself; min_ratio is its sanctioned
    # per-source escape hatch.)
    content, st = _fetch(GCB_BUDGET_URL)
    _component(tally, cursors, st, content, p_budget,
               lambda c: _parse_xlsx_sheets(c, ig.GLOBAL_SHEETS, ig.parse_global_sheet),
               min_ratio=0.80)

    # --- 2. National fossil emissions (XLSX) ---
    content, st = _fetch(GCB_FOSSIL_URL)
    _component(tally, cursors, st, content, p_fossil,
               lambda c: _parse_xlsx_sheets(c, ig.FOSSIL_SHEETS, ig.parse_wide_country_sheet))

    # --- 3. National LUC emissions (XLSX) ---
    content, st = _fetch(GCB_LUC_URL)
    _component(tally, cursors, st, content, p_luc,
               lambda c: _parse_xlsx_sheets(c, ig.LUC_SHEETS, ig.parse_wide_country_sheet))

    # --- 4. GCP fossil flat CSV (Zenodo concept-latest, version-stamped) ---
    flat_url, flat_st = _zenodo_flat_url()
    if flat_st == "ok":
        content, st = _fetch(flat_url)
    else:
        content, st = None, flat_st
    _component(tally, cursors, st, content, p_flat, _build_flat)

    after_total = (blob.row_count(p_budget) + blob.row_count(p_fossil)
                   + blob.row_count(p_luc) + blob.row_count(p_flat))

    last_obs = max(cursors.values()).isoformat() if cursors else None
    cursors_iso = {k: v.isoformat() for k, v in cursors.items()}
    # finalize's empty_window_floor default (10) is irrelevant here — only 4 sub-units —
    # but the structural/transient guards are what matter for an honest status.
    return finalize(tally, after_total, last_obs, source=SOURCE, series_cursors=cursors_iso)


def _build_flat(content):
    """Parse the GCP fossil flat CSV bytes into (keys, dates, vals), reusing the
    ingester's column-detection logic (year col, name col, value cols, skip set)."""
    import csv as _csv
    text = content.decode("utf-8", errors="replace")
    reader = _csv.DictReader(io.StringIO(text))
    headers = reader.fieldnames or []
    year_col = next((h for h in headers if h.lower() in ("year", "yr")), None)
    name_col = next((h for h in headers if h.lower() in ("country", "name", "nation")), None)
    skip_cols = {year_col, name_col, "isocode", "iso3", "Country.Code",
                 "country_code", "country_id", "ISO3166_1_Alpha_3"}
    val_cols = [h for h in headers if h not in skip_cols and h and h.strip()]
    if year_col is None:
        return [], [], []
    keys, dates, vals = [], [], []
    for row in reader:
        try:
            yr = int(float(row[year_col]))
            obs_d = dt.date(yr, 12, 31)
        except (ValueError, TypeError, KeyError):
            continue
        country = str(row.get(name_col, "") or "").strip() if name_col else ""
        for vc in val_cols:
            raw = str(row.get(vc, "") or "").strip()
            if not raw or raw in ("", "NA", "N/A", "NaN", "nan", "None"):
                continue
            try:
                v = float(raw)
                if v != v:
                    continue
                label = vc.replace(".", "_").replace(" ", "_")
                k = f"GCB:fossil_flat:{label}:{country}" if country else f"GCB:fossil_flat:{label}"
                keys.append(k); dates.append(obs_d); vals.append(v)
            except (TypeError, ValueError):
                pass
    return keys, dates, vals
