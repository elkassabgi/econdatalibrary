"""S1 fetcher — Global Power Plant Database (WRI, ~35k plants, 165 countries).

Public (CC BY 4.0). Single grouped parquet clean_full/gppd/gppd.parquet, schema
(series_key, obs_date, value). GPPD is effectively frozen at WRI v1.3.0 (2021):
the WRI dataverse ZIP URL 404s over time, so the live source is the GitHub master
mirror of output_database/global_power_plant_database.csv. We re-fetch the whole
CSV and MERGE (dedup series_key+obs_date, never-shrink). Vintage = the master CSV's
commit SHA (registry's preferred signal), with the raw ETag as a cheap fallback.

series_key = GPPD:{variable}:{gppd_idnr}; one (series_key, obs_date) per plant×var×year:
  capacity_mw       -> baseline 2020-12-31
  generation_gwh    -> year-tagged (generation_gwh_YYYY columns), obs_date = YYYY-12-31
  estimated_generation_gwh (only if no per-year gen columns) -> 2017-12-31
A 200 that parses 0 rows from a real body is a structural break, not a quiet day.
"""
from __future__ import annotations
import csv
import datetime as dt
import io
import os
import zipfile

import pyarrow as pa
import requests

from ... import config, blob, merge
from ..base import Result
from ._common import Tally, finalize
from ._vintage import github_sha, http_vintage, UA

SOURCE = "gppd"
DEDUP = ("series_key", "obs_date")

# GitHub master mirror is the live source; WRI dataverse ZIP 404s over time (kept last).
URLS = [
    "https://github.com/wri/global-power-plant-database/raw/master/output_database/global_power_plant_database.csv",
    "https://raw.githubusercontent.com/wri/global-power-plant-database/master/output_database/global_power_plant_database.csv",
    "https://datasets.wri.org/dataset/540dcf46-f287-47ac-985d-269b04bea4c6/resource/c240ed2e-1190-4d7e-b1da-c66b72e08858/download/globalpowerplantdatabasev130.zip",
]
GH_REPO = "wri/global-power-plant-database"
GH_PATH = "output_database/global_power_plant_database.csv"
RAW_URL = "https://raw.githubusercontent.com/wri/global-power-plant-database/master/output_database/global_power_plant_database.csv"

GEN_PATTERN = "generation_gwh_"
_TRANSIENT_HTTP = (429, 500, 502, 503, 504)


def current_vintage(unit):
    """Cheap probe: the master CSV's commit SHA (registry's preferred signal). Falls
    back to the raw ETag if the GitHub API is unavailable/rate-limited, then None."""
    try:
        sha = github_sha(GH_REPO, GH_PATH)
        if sha:
            return sha
    except Exception:
        pass
    return http_vintage(RAW_URL)


def _parse_gppd_csv(data: bytes):
    """Reuse ingest_gppd.parse_gppd_csv shape. Returns (keys, dates, vals)."""
    text = data.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    headers = reader.fieldnames or []

    id_col = next((h for h in headers if h.lower() in ("gppd_idnr", "id", "plant_id")), None)
    if not id_col:
        return [], [], []

    gen_cols = [(h, int(h[len(GEN_PATTERN):]))
                for h in headers if h.startswith(GEN_PATTERN) and h[len(GEN_PATTERN):].isdigit()]
    est_col = next((h for h in headers if h.lower().startswith("estimated_generation_gwh")), None)

    keys, dates, vals = [], [], []
    for row in reader:
        plant_id = (row.get(id_col) or "").strip()
        if not plant_id:
            continue

        cap = row.get("capacity_mw") or row.get("Capacity (MW)") or ""
        if cap:
            try:
                v = float(cap.replace(",", ""))
                if v > 0:
                    keys.append(f"GPPD:capacity_mw:{plant_id}")
                    dates.append(dt.date(2020, 12, 31))
                    vals.append(v)
            except (ValueError, TypeError):
                pass

        for col, yr in gen_cols:
            raw = (row.get(col) or "").strip()
            if not raw or raw in ("", "NA", "N/A"):
                continue
            try:
                v = float(raw)
                if v > 0:
                    keys.append(f"GPPD:generation_gwh:{plant_id}")
                    dates.append(dt.date(yr, 12, 31))
                    vals.append(v)
            except (ValueError, TypeError):
                pass

        if est_col and not gen_cols:
            raw = (row.get(est_col) or "").strip()
            if raw and raw not in ("", "NA"):
                try:
                    v = float(raw)
                    if v > 0:
                        keys.append(f"GPPD:estimated_generation_gwh:{plant_id}")
                        dates.append(dt.date(2017, 12, 31))
                        vals.append(v)
                except (ValueError, TypeError):
                    pass

    return keys, dates, vals


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
    path = os.path.join(out_dir, "gppd.parquet")
    before = blob.row_count(path)
    tally = Tally()

    data, transient = None, False
    for url in URLS:
        try:
            r = requests.get(url, headers=UA, timeout=120, allow_redirects=True)
        except (requests.Timeout, requests.ConnectionError):
            transient = True
            continue
        if r.status_code in _TRANSIENT_HTTP:
            transient = True
            continue
        if r.status_code == 200 and len(r.content) > 1000:
            data = r.content
            break
        # other non-200 (404 on the stale dataverse URL etc.) -> try next mirror

    if data is None:
        # No mirror returned a usable body. If anything timed out / 5xx'd, this is
        # transient (retry next tick); otherwise all mirrors are dead -> structural.
        if transient:
            tally.transient_unit()
        else:
            tally.structural_unit()
        return finalize(tally, before, None, source=SOURCE)

    keys, dates, vals = [], [], []
    if data[:2] == b"PK":  # ZIP
        z = zipfile.ZipFile(io.BytesIO(data))
        csv_files = [m for m in z.namelist() if m.endswith(".csv") and "metadata" not in m.lower()]
        for cf in csv_files:
            k, d, v = _parse_gppd_csv(z.read(cf))
            keys.extend(k); dates.extend(d); vals.extend(v)
    else:
        keys, dates, vals = _parse_gppd_csv(data)

    if not vals:
        tally.structural_unit()  # 200 with a real body but parsed nothing -> schema break
        return finalize(tally, before, None, source=SOURCE)

    tbl = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date":   pa.array(dates, pa.date32()),
        "value":      pa.array(vals, pa.float64()),
    })

    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    tally.added_unit(max(0, n - before))
    return finalize(tally, n, md, source=SOURCE, series_cursors=_series_maxes(keys, dates))
