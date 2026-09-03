"""S1 fetcher — Barro-Lee Educational Attainment (5-year intervals, 1950-2020).

Static academic dataset of 15 hardcoded GitHub Pages CSVs rebuilt into one grouped
parquet clean_full/barro_lee/barro_lee.parquet, schema (series_key, obs_date, value).
A 'delta' is meaningless: Barro-Lee re-publish a whole new vintage at once, so we
re-fetch every CSV and MERGE (dedup series_key+obs_date, new wins on revision,
never-shrink). Each CSV is one sub-unit — a 200 that parses 0 rows from a real body
is a structural break; timeouts/5xx/429 are transient (re-run next tick).

Vintage signal (registry adapter.vintage_signal): the GitHub commit SHA of the
barrolee.github.io Pages repo (barrolee/BarroLeeDataSet) — one cheap GET that moves
iff any of the 15 CSVs change. Falls back to None (strategy fetches anyway, safe).

series_key: BARRO_LEE:{variable}:{agefrom}_{ageto}:{sex}:{iso3}
  e.g. BARRO_LEE:yr_sch:25_64:MF:DZA
"""
from __future__ import annotations
import csv
import datetime as dt
import io
import os

import pyarrow as pa
import requests

from ... import config, blob, merge
from ..base import Result
from ._common import Tally, finalize
from ._vintage import github_sha, UA

SOURCE = "barro_lee"
DEDUP = ("series_key", "obs_date")
REPO = "barrolee/BarroLeeDataSet"

BASE = "https://barrolee.github.io/BarroLeeDataSet/BLData"

# 15 files: v3 (5-year intervals 1950-2020, 146 countries) + v2.2 (1950-2010).
BL_FILES = [
    f"{BASE}/BL_v3_MF.csv",
    f"{BASE}/BL_v3_F.csv",
    f"{BASE}/BL_v3_M.csv",
    f"{BASE}/BL_v3_MF1564.csv",
    f"{BASE}/BL_v3_F1564.csv",
    f"{BASE}/BL_v3_M1564.csv",
    f"{BASE}/BL_v3_MF2564.csv",
    f"{BASE}/BL_v3_F2564.csv",
    f"{BASE}/BL_v3_M2564.csv",
    f"{BASE}/BL2013_MF1599_v2.2.csv",
    f"{BASE}/BL2013_F1599_v2.2.csv",
    f"{BASE}/BL2013_M1599_v2.2.csv",
    f"{BASE}/BL2013_MF2599_v2.2.csv",
    f"{BASE}/BL2013_F2599_v2.2.csv",
    f"{BASE}/BL2013_M2599_v2.2.csv",
]

# Value columns to extract (each becomes its own series_key variable).
VAL_COLS = ["lu", "lp", "lpc", "ls", "lsc", "lh", "lhc",
            "yr_sch", "yr_sch_pri", "yr_sch_sec", "yr_sch_ter"]

_TRANSIENT = (429, 500, 502, 503, 504)


def current_vintage(unit):
    """Cheap probe: GitHub commit SHA of the Pages repo. Moves iff a CSV changed.
    github_sha raises TransientError on network/5xx; swallow to None (detection
    failing must not fail the run — update() handles transients honestly)."""
    try:
        return github_sha(REPO)
    except Exception:
        return None


def _parse_bl_csv(data: bytes):
    """Parse one Barro-Lee CSV -> (keys, dates, vals). Mirrors jobs/ingest_barro_lee.py."""
    text = data.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    headers = list(reader.fieldnames or [])

    iso_col = next((h for h in headers if h in ("WBcode", "iso3c", "ISO3")), None)
    if iso_col is None:
        iso_col = next((h for h in headers if h.lower() in ("country", "name")), None)
    year_col = next((h for h in headers if h.lower() == "year"), None)
    sex_col = next((h for h in headers if h.lower() == "sex"), None)
    agefrom_col = next((h for h in headers
                        if h.lower() in ("agefrom", "age_from", "agefr")), None)
    ageto_col = next((h for h in headers if h.lower() in ("ageto", "age_to")), None)

    if not iso_col or not year_col:
        return [], [], []
    available_val_cols = [c for c in VAL_COLS if c in headers]
    if not available_val_cols:
        return [], [], []

    keys, dates, vals = [], [], []
    for rec in reader:
        iso3 = (rec.get(iso_col) or "").strip()
        if not iso3 or iso3.lower() in ("nan", "none", ""):
            continue
        try:
            yr = int(float((rec.get(year_col) or "").strip()))
            obs_d = dt.date(yr, 12, 31)
        except (ValueError, TypeError):
            continue
        sex = (rec.get(sex_col) or "MF").strip() or "MF"
        try:
            af = int(float(rec.get(agefrom_col) or 0)) if agefrom_col else 0
            at = int(float(rec.get(ageto_col) or 99)) if ageto_col else 99
            age_s = f"{af}_{at}"
        except (ValueError, TypeError):
            age_s = "all"
        entity = iso3[:15]
        for col in available_val_cols:
            raw = rec.get(col, "")
            if not raw or str(raw).strip() in ("", "NA", ".", "nan"):
                continue
            try:
                v = float(str(raw).strip())
            except (ValueError, TypeError):
                continue
            if v != v:  # NaN
                continue
            keys.append(f"BARRO_LEE:{col}:{age_s}:{sex}:{entity}")
            dates.append(obs_d)
            vals.append(v)
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
    path = os.path.join(out_dir, "barro_lee.parquet")
    before = blob.row_count(path)
    tally = Tally()

    all_keys, all_dates, all_vals = [], [], []
    seen = set()  # in-run dedup across overlapping MF/F/M aggregate files

    for url in BL_FILES:
        try:
            r = requests.get(url, headers=UA, timeout=120, allow_redirects=True)
        except (requests.Timeout, requests.ConnectionError) as e:
            tally.transient_unit(f"{url[-52:]}: {type(e).__name__}")
            continue
        if r.status_code in _TRANSIENT:
            tally.transient_unit(f"{url[-52:]}: HTTP {r.status_code}")
            continue
        if r.status_code != 200 or len(r.content) <= 100:
            # 404 / moved / truncated body — not a transient; treat as structural so
            # a stale/renamed file list surfaces instead of laundering to no_change.
            tally.structural_unit(
                f"{url[-52:]}: HTTP {r.status_code}, {len(r.content):,} bytes")
            continue

        k, d, v = _parse_bl_csv(r.content)
        if not v:
            # 200 with a real body that parsed nothing -> schema/structural break.
            tally.structural_unit(f"{url[-52:]}: real body parsed 0 rows")
            continue

        for ki, di, vi in zip(k, d, v):
            dk = (ki, di)
            if dk in seen:
                continue
            seen.add(dk)
            all_keys.append(ki)
            all_dates.append(di)
            all_vals.append(vi)

    # The 15 CSVs are parts of ONE whole-table publish, so for honest STATUS we account
    # the merge as a single logical sub-unit (avoids the all-empty-window floor=10 guard
    # firing on a legitimate no-change refresh). Structural/transient per-file tallies are
    # preserved below so finalize raises on a stale/renamed file or returns partial.
    if not all_vals:
        # Nothing parsed from any file: finalize classifies (structural ->
        # DefinitiveError, all-transient -> partial, else empty).
        return finalize(tally, before, None, source=SOURCE)

    tbl = pa.table({
        "series_key": pa.array(all_keys, pa.string()),
        "obs_date": pa.array(all_dates, pa.date32()),
        "value": pa.array(all_vals, pa.float64()),
    })

    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    # One success sub-unit for the whole-table merge: net_new>0 -> ok, else no_change.
    tally.added_unit(max(0, n - before))
    return finalize(tally, n, md, source=SOURCE,
                    series_cursors=_series_maxes(all_keys, all_dates))
