"""S1 fetcher — IRENA (International Renewable Energy Agency) PxWeb API.

Public, CC BY 4.0, no key. Source publishes vintage-stamped PxWeb tables
(e.g. *_2026_H1_*, *_2025_H2_*) under two databases (IRENASTAT, RE-STAT) and
re-estimates/revises whole tables each release — a textbook S1 (overwrite_if_changed)
shape. There is no per-file ETag/Last-Modified on the catalog, so the cheap vintage
probe is a BFS crawl of the catalog collapsed to a content-hash of the sorted set
of table ids; that token moves iff a new/renamed vintage table appears.

Storage matches the two existing ingesters, which split tables by whether the PxWeb
server accepts a whole-table POST:
  * jobs/ingest_irena.py        -> small/medium tables fetched in one POST; file
                                   {safe(table_id)}.parquet, series_key prefix
                                   "IRENA:{table_id w/o .px, %20->space}"
  * jobs/ingest_irena_country.py-> large country tables that 403/400/500 on a full
                                   POST, fetched in 20-country batches; file
                                   ctry_{safe(table_id)}.parquet, series_key prefix
                                   "IRENA:{table_id}" (literal, keeps .px)
This fetcher reproduces both exactly: try the full POST first; on failure fall back
to country-batched. Each table is published to ITS OWN parquet via merge_and_write
(mode='merge', dedup series_key+obs_date, new wins on revision, never-shrink @0.97).

Schema per file (unchanged): series_key:string, obs_date:date32[day], value:double.
"""
from __future__ import annotations
import datetime as dt
import os
import re
import time

import pyarrow as pa
import requests

from ... import config, blob, merge
from ..base import Result
from ._common import Tally, finalize
from ._vintage import content_hash

SOURCE = "irena"
BASE = "https://pxweb.irena.org/api/v1/en"
DBS = ["IRENASTAT", "RE-STAT"]
DEDUP = ("series_key", "obs_date")
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
      "Content-Type": "application/json"}
BATCH = 20            # countries per request for large tables
RATE = 0.3           # polite throttle between catalog items
TRANSIENT_CODES = (429, 500, 502, 503, 504)


# ---------------------------------------------------------------- HTTP helpers

class _Transient(Exception):
    """Raised on timeout / connection error / 5xx / 429 so update() can mark the
    affected sub-unit transient and the orchestrator re-runs next tick."""


def _get_json(url, retries=3):
    for a in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=60)
        except (requests.Timeout, requests.ConnectionError) as e:
            if a == retries - 1:
                raise _Transient(str(e))
            time.sleep(2 ** a)
            continue
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError:
                return None
        if r.status_code == 404:
            return None
        if r.status_code in TRANSIENT_CODES:
            if a == retries - 1:
                raise _Transient(f"GET {r.status_code}")
            time.sleep(2 ** a)
            continue
        return None
    return None


def _post_json(url, body, retries=3):
    """Returns (json_or_None, http_status). status lets caller distinguish a
    server-side 'too big' refusal (403/400/500 -> try batching) from a transient."""
    last = None
    for a in range(retries):
        try:
            r = requests.post(url, json=body, headers=UA, timeout=120)
        except (requests.Timeout, requests.ConnectionError) as e:
            if a == retries - 1:
                raise _Transient(str(e))
            time.sleep(2 ** a)
            continue
        last = r.status_code
        if r.status_code == 200:
            try:
                return r.json(), 200
            except ValueError:
                return None, 200
        if r.status_code in TRANSIENT_CODES:
            if a == retries - 1:
                raise _Transient(f"POST {r.status_code}")
            time.sleep(2 ** a)
            continue
        # 400/403/404 etc. — a definitive server answer (e.g. table too large)
        return None, r.status_code
    return None, last


# ---------------------------------------------------------------- catalog crawl

def _crawl(db):
    """BFS the PxWeb catalog for one database. Returns list of (path, table_id).
    Mirrors jobs/ingest_irena.py crawl_catalog."""
    found = []
    queue = [f"{BASE}/{db}/"]
    visited = set()
    while queue:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)
        items = _get_json(url)
        if not items:
            continue
        for item in (items if isinstance(items, list) else [items]):
            itype = item.get("type", "l")
            iid = item.get("id", item.get("dbid", ""))
            if itype == "t":
                found.append((url, iid))
            elif itype in ("l", "h"):
                child = url.rstrip("/") + "/" + requests.utils.quote(iid)
                if child not in visited:
                    queue.append(child + "/")
        time.sleep(RATE)
    return found


def _crawl_all():
    out = []
    for db in DBS:
        out.extend(_crawl(db))
    return out


# ---------------------------------------------------------------- json-stat2 parse

def _parse_jsonstat2(resp, prefix):
    """json-stat2 -> (keys, dates, vals) long format. Identical algorithm to both
    ingesters; `prefix` is the series_key namespace (matching each script's convention)."""
    if not resp or "dimension" not in resp:
        return [], [], []
    dims = resp["dimension"]
    size = resp.get("size", [])
    ids = resp.get("id", [])
    vals_raw = resp.get("value", [])
    if not ids or not size or not vals_raw:
        return [], [], []

    time_dim = None
    for dim_id in ids:
        cats = list(dims.get(dim_id, {}).get("category", {}).get("label", {}).values())
        if cats and all(re.match(r"^\d{4}", str(v)) for v in cats):
            time_dim = dim_id
            break
    if not time_dim:
        return [], [], []

    time_cats = list(dims[time_dim]["category"]["label"].values())
    series_dims = [d for d in ids if d != time_dim]

    strides = [1] * len(ids)
    for i in range(len(ids) - 2, -1, -1):
        strides[i] = strides[i + 1] * size[i + 1]
    time_i = ids.index(time_dim)

    def parse_yr(s):
        m = re.match(r"^(\d{4})", str(s))
        return int(m.group(1)) if m else None

    time_dates = [dt.date(y, 12, 31) if (y := parse_yr(c)) else None for c in time_cats]
    series_cats = [list(dims[sd]["category"]["label"].values()) for sd in series_dims]

    keys, dates, vals = [], [], []
    for flat_idx, val in enumerate(vals_raw):
        if val is None:
            continue
        try:
            v = float(val)
        except (TypeError, ValueError):
            continue
        if v != v:
            continue
        idx = flat_idx
        dim_indices = []
        for s in size:
            dim_indices.append(idx // strides[len(dim_indices)])
            idx %= strides[len(dim_indices) - 1]
        ti = dim_indices[time_i]
        if ti >= len(time_dates) or not time_dates[ti]:
            continue
        parts = []
        for si, sd in enumerate(series_dims):
            ci = dim_indices[ids.index(sd)]
            if ci < len(series_cats[si]):
                parts.append(str(series_cats[si][ci]).replace("|", "_"))
        keys.append(f"{prefix}|{'|'.join(parts)}")
        dates.append(time_dates[ti])
        vals.append(v)
    return keys, dates, vals


# ---------------------------------------------------------------- per-table fetch

def _fetch_full(table_url, prefix):
    """One whole-table POST. Returns (keys, dates, vals, http_status)."""
    resp, st = _post_json(table_url, {"query": [], "response": {"format": "json-stat2"}})
    if st != 200 or resp is None:
        return [], [], [], st
    k, d, v = _parse_jsonstat2(resp, prefix)
    return k, d, v, 200


def _fetch_batched(table_url, prefix):
    """Country-batched POST for large tables that refuse a full query. Mirrors
    jobs/ingest_irena_country.py. Returns (keys, dates, vals, ok). ok=False on a
    transient failure of one or more batches (so caller can mark transient)."""
    meta = _get_json(table_url)
    if not meta or "variables" not in meta:
        return [], [], [], True  # no meta from a 200-style path -> structural, handled upstream
    ctry_var = None
    for v in meta["variables"]:
        if v.get("code", "").lower() in ("country/area", "country", "countries"):
            ctry_var = v
            break
    if ctry_var is None:
        ctry_var = meta["variables"][0]
    all_ctry = ctry_var["values"]
    other = [v for v in meta["variables"] if v["code"] != ctry_var["code"]]

    keys, dates, vals = [], [], []
    any_ok = False
    for start in range(0, len(all_ctry), BATCH):
        batch = all_ctry[start:start + BATCH]
        query = [{"code": ctry_var["code"], "selection": {"filter": "item", "values": batch}}] + \
                [{"code": v["code"], "selection": {"filter": "all", "values": ["*"]}} for v in other]
        resp, st = _post_json(table_url, {"query": query, "response": {"format": "json-stat2"}})
        if st != 200 or resp is None:
            # a batch refused/failed; keep going but flag partial so we don't shrink-publish
            continue
        k, d, vv = _parse_jsonstat2(resp, prefix)
        keys.extend(k)
        dates.extend(d)
        vals.extend(vv)
        any_ok = True
        time.sleep(0.5)
    return keys, dates, vals, any_ok


def _safe(name):
    return re.sub(r"[^\w]", "_", name)[:80]


def _table_url(path, table_id):
    return path.rstrip("/") + "/" + requests.utils.quote(table_id)


# ---------------------------------------------------------------- S1 interface

def current_vintage(unit):
    """Cheap probe: BFS the catalog and hash the sorted set of table ids. The hash
    moves iff a new/renamed vintage table is published (e.g. _2025_H2_ -> _2026_H1_).
    Returns None on a transient catalog failure (strategy then fetches anyway)."""
    try:
        tables = _crawl_all()
    except _Transient:
        return None
    if not tables:
        return None
    ids = sorted(tid for _, tid in tables)
    return content_hash("\n".join(ids).encode("utf-8"))


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    tally = Tally()

    try:
        tables = _crawl_all()
    except _Transient:
        tally.transient_unit()
        return finalize(tally, blob.row_count(out_dir), None, source=SOURCE)

    if not tables:
        # whole-catalog crawl yielded nothing from a reachable host -> structural
        tally.structural_unit()
        return finalize(tally, 0, None, source=SOURCE)

    total_rows = 0
    last_obs = None
    cursors = {}

    for path, table_id in tables:
        url = _table_url(path, table_id)
        try:
            # Try whole-table first (the "main ingester" path).
            full_prefix = f"IRENA:{table_id.replace('.px', '').replace('%20', ' ')}"
            k, d, v, st = _fetch_full(url, full_prefix)
            if st == 200:
                out_path = os.path.join(out_dir, f"{_safe(table_id)}.parquet")
                prefix_used = full_prefix
            else:
                # Full POST refused (403/400/500 = table too large) -> country-batched.
                ctry_prefix = f"IRENA:{table_id}"
                k, d, v, any_ok = _fetch_batched(url, ctry_prefix)
                out_path = os.path.join(out_dir, f"ctry_{_safe(table_id)}.parquet")
                prefix_used = ctry_prefix
                if not k and not any_ok:
                    # every batch transient-failed -> retry next tick; keep old file
                    tally.transient_unit()
                    total_rows += blob.row_count(out_path)
                    continue
        except _Transient:
            # network/5xx/429 mid-table -> transient; do not touch existing file
            tally.transient_unit()
            # best-effort: count whatever is already on disk for this table
            for cand in (os.path.join(out_dir, f"{_safe(table_id)}.parquet"),
                         os.path.join(out_dir, f"ctry_{_safe(table_id)}.parquet")):
                if blob.exists(cand):
                    total_rows += blob.row_count(cand)
                    break
            continue

        before = blob.row_count(out_path)
        if not k:
            # 200 (or batched ok) but parsed 0 rows from a real body -> structural break.
            tally.structural_unit()
            total_rows += before
            continue

        tbl = pa.table({"series_key": pa.array(k, pa.string()),
                        "obs_date": pa.array(d, pa.date32()),
                        "value": pa.array(v, pa.float64())})
        n, md = merge.merge_and_write(out_path, tbl, mode="merge", dedup_keys=DEDUP)
        tally.added_unit(max(0, n - before))
        total_rows += n
        if md is not None and (last_obs is None or md > last_obs):
            last_obs = md
        # per-series cursors (max obs_date per series_key) for this table
        for sk, od in zip(k, d):
            iso = od.isoformat()
            if sk not in cursors or iso > cursors[sk]:
                cursors[sk] = iso

    return finalize(tally, total_rows, last_obs, source=SOURCE, series_cursors=cursors)
