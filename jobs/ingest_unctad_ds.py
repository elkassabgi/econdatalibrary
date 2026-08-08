#!/usr/bin/env python3
"""Generic UNCTADstat dataset ingest — the publisher's own documented data API.

One source per CURRENT dataset (successor family to the 38 retired DBnomics-era
unctad_* slugs; upstream re-coded all dataset ids, 0 of 38 match — see #70). The full
contract was read from the app's own generated code sample and PROVEN live 2026-08-07
(scratchpad unctad_auth_findings.md):

  catalogue+schema (keyless):
    GET https://unctadstat-api.unctad.org/api/datacenter/en            (99 datasets)
    GET https://unctadstat-api.unctad.org/api/reportMetadata/{DS}/en   (dims+measures+version)
  observations (keyed — UNCTAD_CLIENT_ID / UNCTAD_API_KEY from .env, NEVER in code):
    POST https://unctadstat-user-api.unctad.org/{DS}/cur/Facts?culture=en
    headers ClientId / ClientSecret; multipart form $select/$filter/$format=csv

Series identity: non-time dimension codes in (rowAxe, pageAxe) order + the measure code,
joined with '.', e.g. '0000.02.M0100' = World / exports / US$-current. obs_date from the
isTime dimension (Year -> Dec-31, matching the family's annual convention elsewhere).
One Facts POST per measure group, base (magnitude=1) variant only — the magnitude
variants are the same number scaled, not new data.

Licence: CC BY 3.0 IGO — verbatim audit §UNCTAD (DATABASE_LICENSES_VERBATIM.md:2501-2507),
CLEARED, re-host OK with attribution.

Run: python jobs/ingest_unctad_ds.py US.TradeMerchTotal [--dry]
"""
from __future__ import annotations

import csv
import datetime as dt
import io
import os
import re
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

META = "https://unctadstat-api.unctad.org/api"
FACTS = "https://unctadstat-user-api.unctad.org"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

SCHEMA = pa.schema([
    ("series_key", pa.string()), ("obs_date", pa.date32()), ("value", pa.float64()),
])


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def creds():
    # Environment first (CI secrets), .env fallback (workstation). CI has no .env file,
    # so an env-less lookup there must fail LOUDLY, not FileNotFoundError obscurely.
    cid = os.environ.get("UNCTAD_CLIENT_ID")
    key = os.environ.get("UNCTAD_API_KEY")
    if cid and key:
        return cid, key
    env_path = os.path.join(ROOT, ".env")
    if os.path.exists(env_path):
        out = {}
        with open(env_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("UNCTAD_") and "=" in line:
                    k, v = line.split("=", 1)
                    out[k.strip()] = v.strip().strip('"').strip("'")
        cid = cid or out.get("UNCTAD_CLIENT_ID")
        key = key or out.get("UNCTAD_API_KEY")
    if not (cid and key):
        raise SystemExit("UNCTAD_CLIENT_ID / UNCTAD_API_KEY missing "
                         "(environment and .env both empty)")
    return cid, key


def report_metadata(ds_name: str) -> dict:
    r = requests.get(f"{META}/reportMetadata/{ds_name}/en", headers=UA, timeout=120)
    r.raise_for_status()
    return r.json()


# Mechanical slugs that would COLLIDE with a legacy DBnomics-era source id get an
# explicit override. R399: source_id_for("US.Cpi_A") produced "unctad_cpia" — the exact
# id of a legacy source with 637 live series — and the ingest silently overwrote its
# store (recovered exactly from the served CSVs). The guard in ingest() now refuses any
# id that already exists in the catalog's source table unless its homepage records THIS
# dataset, so a future collision fails loudly instead of clobbering.
SOURCE_ID_OVERRIDES = {
    "US.Cpi_A": "unctad_cpi_annual",
}


def source_id_for(ds_name: str) -> str:
    # US.TradeMerchTotal -> unctad_trademerchtotal (successor naming; legacy slugs retired)
    if ds_name in SOURCE_ID_OVERRIDES:
        return SOURCE_ID_OVERRIDES[ds_name]
    return "unctad_" + ds_name.split(".", 1)[1].replace("_", "").lower()


def assert_no_source_collision(src: str, ds_name: str) -> None:
    """Refuse to write under a source id that belongs to something else (R399)."""
    import sqlite3
    cat = os.path.join(ROOT, "data", "catalog.db")
    if not os.path.exists(cat):
        return
    con = sqlite3.connect(f"file:{cat}?mode=ro", uri=True, timeout=60)
    row = con.execute("SELECT homepage FROM source WHERE source_id=?", (src,)).fetchone()
    con.close()
    if row and (not row[0] or f"/dataviewer/{ds_name}" not in row[0]):
        raise SystemExit(
            f"COLLISION: source id {src!r} already exists in the catalog and its homepage "
            f"({row[0]!r}) does not record dataset {ds_name!r}. Add an entry to "
            f"SOURCE_ID_OVERRIDES instead of overwriting a legacy source (R399).")


def parse_time(v: str, is_year: bool) -> dt.date | None:
    s = str(v).strip()
    try:
        if is_year or (len(s) == 4 and s.isdigit()):
            return dt.date(int(s), 12, 31)
        if len(s) == 10:
            return dt.date.fromisoformat(s)
        if len(s) == 7 and s[4] == "-":
            return dt.date(int(s[:4]), int(s[5:7]), 1)
        # Quarterly '2005Q01' (US.MerchVolumeQuarterly's Quarter axis, isTime=true).
        # Period-start like Q conventions elsewhere in the repo: Q1->Jan .. Q4->Oct.
        if len(s) == 7 and s[4] in "Qq" and s[:4].isdigit() and s[5:].isdigit():
            q = int(s[5:])
            if 1 <= q <= 4:
                return dt.date(int(s[:4]), (q - 1) * 3 + 1, 1)
        # Monthly '1995M01' (US.UCPI_M / CommodityPrice_M family's Period axis).
        if len(s) == 7 and s[4] in "Mm" and s[:4].isdigit() and s[5:].isdigit():
            m = int(s[5:])
            if 1 <= m <= 12:
                return dt.date(int(s[:4]), m, 1)
        # Semiannual '2018S01' (US.PortCallsArrivals_S). S1->Jan, S2->Jul (period-start).
        if len(s) == 7 and s[4] in "Ss" and s[:4].isdigit() and s[5:].isdigit():
            h = int(s[5:])
            if 1 <= h <= 2:
                return dt.date(int(s[:4]), (h - 1) * 6 + 1, 1)
    except ValueError:
        pass
    return None


def parse_period_code(v: str) -> tuple[dt.date, str | None] | None:
    """Period-coded time axes (isTime FALSE, field 'Year', string codetype).

    MEASURED on US.TradeMerchGR (52 codes): TWO families share the axis —
      annual YoY   '19801981' (consecutive years, label '1981')  -> obs at end-year
      multi-year   '19921995', '19952000' (span averages)        -> obs at end-year
    and their END-YEARS COLLIDE (7 duplicates: an annual '19941995' AND the span
    '19921995' both end 1995). So the span family carries a series-key suffix by span
    LENGTH — '|SPAN=5Y' — which makes the six 5-year averages ONE proper time series
    (obs at each span's end-year) instead of a flood of single-observation series,
    while the usda-style suffix still separates a period-average from the annual
    figure under one identity. Within a span length the end-year is unique by
    construction (same length + same end = same code), so no collisions.
    Returns (obs_date, suffix_or_None); None for an unparseable code.
    """
    s = str(v).strip()
    if len(s) == 8 and s.isdigit():
        y0, y1 = int(s[:4]), int(s[4:])
        if 1500 < y0 <= y1 < 2200:
            if y1 - y0 == 1:
                return dt.date(y1, 12, 31), None
            return dt.date(y1, 12, 31), f"|SPAN={y1 - y0}Y"
    if len(s) == 4 and s.isdigit():
        return dt.date(int(s), 12, 31), None
    return None


class FactsSizeCap(RuntimeError):
    """The API refuses requests estimated over ~62,500 cells with HTTP 400:
    'The request exceed the maximal size. Maximal size : 62500. Estimated size : N.'
    (measured on US.IntraTrade, estimate 6,988,144). Carries both numbers so the
    caller can compute how many time-chunks are needed."""

    def __init__(self, cap: int, estimated: int):
        super().__init__(f"Facts size cap {cap} < estimated {estimated}")
        self.cap, self.estimated = cap, estimated


def facts_csv(ds_name: str, select: str, cid: str, key: str, flt: str | None = None) -> str:
    form = {"$select": select, "$format": "csv"}
    if flt:
        form["$filter"] = flt
    files = {k: (None, v) for k, v in form.items()}
    for attempt in range(4):
        try:
            r = requests.post(f"{FACTS}/{ds_name}/cur/Facts?culture=en",
                              headers={"ClientId": cid, "ClientSecret": key, **UA},
                              files=files, timeout=600)
            if r.status_code == 200:
                return r.text
            if r.status_code in (429, 502, 503, 504):
                time.sleep(20 * (attempt + 1)); continue
            if r.status_code == 400 and "maximal size" in r.text.lower():
                m = re.search(r"Maximal size\s*:\s*(\d+).*?Estimated size\s*:\s*(\d+)",
                              r.text, re.S)
                if m:
                    raise FactsSizeCap(int(m.group(1)), int(m.group(2)))
            raise RuntimeError(f"Facts HTTP {r.status_code}: {r.text[:300]}")
        except requests.RequestException as e:
            log(f"  transient {type(e).__name__}; retry {attempt + 1}")
            time.sleep(15 * (attempt + 1))
    raise RuntimeError(f"Facts unreachable for {ds_name} after 4 tries")


def dim_codes(ds_name: str, version, dim_table: str) -> list[str]:
    """Keyless dimension table -> ordered code list (for chunked Facts pulls)."""
    r = requests.get(f"{META.replace('/api', '')}/datamart-api/{ds_name}/{version}/"
                     f"{dim_table}?$orderby=Order&culture=en", headers=UA, timeout=120)
    r.raise_for_status()
    body = r.json()
    vals = body.get("value", body) if isinstance(body, dict) else body
    return [x["Code"] for x in vals if x.get("Code") is not None]


def facts_csv_chunked(ds_name: str, select: str, cid: str, key: str, meta: dict,
                      tdim: dict, progress=None) -> list[str]:
    """Full-dataset pull that respects the size cap: try one POST; on FactsSizeCap,
    partition the TIME dimension's codes into groups sized from the error's own
    numbers (cap/estimated, 15% headroom) and pull per group; a group that still
    caps is split in half recursively (down to single codes)."""
    try:
        return [facts_csv(ds_name, select, cid, key)]
    except FactsSizeCap as e:
        codes = dim_codes(ds_name, meta.get("version"), tdim["name"])
        if not codes:
            raise
        frac = max(1e-6, e.cap / e.estimated * 0.85)
        per = max(1, int(len(codes) * frac))
        if progress:
            progress(f"  size cap {e.cap:,} < est {e.estimated:,}: chunking "
                     f"{len(codes)} {tdim['name']} codes, ~{per}/chunk")
        numeric = (tdim.get("codetype") == "number")
        tfield = tdim["field"]

        def flt_for(group):
            if numeric:
                return f"{tfield} in ({','.join(str(c) for c in group)})"
            return f"{tfield}/Code in ({','.join(repr(str(c)) for c in group)})"

        # Second-level split: on datasets with partner/product dimensions ONE time code
        # alone can exceed the cap (measured: US.IntraTrade, 225,424 cells in a single
        # year vs the 62,500 cap). Split such a group further by the FIRST key dim's
        # codes, halving recursively. Work items are (time_group, dim_codes_or_None).
        kdims = [d for axe in ("rowAxe", "colAxe", "pageAxe")
                 for d in (meta["defaults"].get(axe) or [])
                 if not (bool(d.get("isTime")) or d.get("field", "").lower() == "year")]
        split_dim = kdims[0] if kdims else None
        split_codes = None

        def flt_for_pair(tgroup, dgroup):
            f = flt_for(tgroup)
            if dgroup is not None:
                f += (f" and {split_dim['field']}/Code in "
                      f"({','.join(repr(str(c)) for c in dgroup)})")
            return f

        out: list[str] = []
        stack = [(codes[i:i + per], None) for i in range(0, len(codes), per)]
        while stack:
            tgroup, dgroup = stack.pop(0)
            try:
                out.append(facts_csv(ds_name, select, cid, key,
                                     flt=flt_for_pair(tgroup, dgroup)))
            except FactsSizeCap:
                if len(tgroup) > 1:
                    mid = len(tgroup) // 2
                    stack[:0] = [(tgroup[:mid], dgroup), (tgroup[mid:], dgroup)]
                    continue
                if split_dim is None:
                    raise
                if dgroup is None:
                    if split_codes is None:
                        split_codes = dim_codes(ds_name, meta.get("version"),
                                                split_dim["name"])
                        if progress:
                            progress(f"  single-{tdim['name']}-code still caps: also "
                                     f"splitting by {split_dim['name']} "
                                     f"({len(split_codes)} codes)")
                    mid = len(split_codes) // 2
                    stack[:0] = [(tgroup, split_codes[:mid]), (tgroup, split_codes[mid:])]
                    continue
                if len(dgroup) == 1:
                    raise   # one time code x one dim code still too big — new layout class
                mid = len(dgroup) // 2
                stack[:0] = [(tgroup, dgroup[:mid]), (tgroup, dgroup[mid:])]
        return out


class UnsupportedLayout(RuntimeError):
    """A dataset shape this generic machinery has not been taught. Callers decide
    whether that is fatal (ingest) or a structural unit-failure (fetcher)."""


def dataset_layout(meta: dict):
    """(kfields, tfield, is_year, period_axis, measures) from reportMetadata.

    Non-time dims in (rowAxe, colAxe, pageAxe) order form the series key; the single
    time axis is the observation axis. isTime is authoritative when SET — but
    PERIOD-CODED axes (growth-rate datasets) report isTime FALSE while still being
    the time axis (measured: US.TradeMerchGR's colAxe is name=Periods, field=Year,
    isTime=false, codes like '19921995'); the field name 'Year' is the API's own
    time marker in that layout. Measures: the magnitude-1 base per observation group
    (the variants are the same number scaled).
    """
    defaults = meta["defaults"]
    dims = [d for axe in ("rowAxe", "colAxe", "pageAxe") for d in defaults.get(axe) or []]

    def _is_time(d):
        return bool(d.get("isTime")) or d.get("field", "").lower() == "year"

    time_dims = [d for d in dims if _is_time(d)]
    key_dims = [d for d in dims if not _is_time(d)]
    if len(time_dims) != 1:
        raise UnsupportedLayout(f"{meta.get('name')}: {len(time_dims)} time dims")
    tdim = time_dims[0]
    measures = []
    for grp in defaults.get("observations") or []:
        base = next((m for m in grp.get("measures", []) if m.get("magnitude") == 1), None)
        if base:
            measures.append(base["code"])
    if not measures:
        raise UnsupportedLayout(f"{meta.get('name')}: no magnitude-1 measures")
    return ([d["field"] for d in key_dims], tdim["field"],
            bool(tdim.get("isTime")) and tdim["field"].lower() == "year",
            not tdim.get("isTime"), measures)


def dataset_time_dim(meta: dict) -> dict:
    """The raw time-axis dict (name/field/codetype) — needed for chunked pulls."""
    defaults = meta["defaults"]
    dims = [d for axe in ("rowAxe", "colAxe", "pageAxe") for d in defaults.get(axe) or []]
    for d in dims:
        if bool(d.get("isTime")) or d.get("field", "").lower() == "year":
            return d
    raise UnsupportedLayout(f"{meta.get('name')}: no time dim")


def pull_rows(ds_name: str, cid: str, key: str, meta: dict, progress=None):
    """Fetch + parse ALL observations for one dataset. THE single row-building path —
    the ingest below and every fetcher call this, so the parse rules cannot drift
    between the two (the insee_bdm/eurostat parity lesson, R-ledger 2026-08-08).
    Returns (rows_k, rows_d, rows_v)."""
    kfields, tfield, is_year, period_axis, measures = dataset_layout(meta)
    tdim = dataset_time_dim(meta)
    rows_k, rows_d, rows_v = [], [], []
    for mcode in measures:
        select = ", ".join(f"{f}/Code" for f in kfields) + f", {tfield}, M{mcode}/Value"
        texts = facts_csv_chunked(ds_name, select, cid, key, meta, tdim, progress=progress)
        n0 = len(rows_k)
        for rec in (rec for text in texts for rec in csv.DictReader(io.StringIO(text))):
            vals = [rec.get(f"{f}_Code", "") for f in kfields]
            tv = rec.get(tfield) or rec.get(f"{tfield}_Code", "")
            vv = rec.get(f"M{mcode}_Value", "")
            suffix = None
            if period_axis:
                parsed = parse_period_code(tv)
                if parsed is None:
                    continue
                d, suffix = parsed
            else:
                d = parse_time(tv, is_year)
            if d is None or vv in ("", None):
                continue
            try:
                v = float(vv)
            except ValueError:
                continue
            rows_k.append(".".join(vals + [f"M{mcode}"]) + (suffix or ""))
            rows_d.append(d)
            rows_v.append(v)
        if progress:
            progress(f"  M{mcode}: {len(rows_k) - n0:,} obs")
    return rows_k, rows_d, rows_v


def ingest(ds_name: str, dry: bool) -> int:
    cid, key = creds()
    meta = report_metadata(ds_name)
    src = source_id_for(ds_name)
    assert_no_source_collision(src, ds_name)
    out_dir = os.path.join(ROOT, "data", "clean_full", src)

    try:
        kfields, tfield, _, _, measures = dataset_layout(meta)
    except UnsupportedLayout as e:
        raise SystemExit(f"{e} — teach the job this layout")
    log(f"{ds_name} -> {src}: key dims {kfields}, time {tfield}, "
        f"measures {['M' + c for c in measures]}, version {meta.get('version')}")

    rows_k, rows_d, rows_v = pull_rows(ds_name, cid, key, meta, progress=log)

    if not rows_k:
        log(f"  {ds_name}: 0 obs — refusing to write an empty store")
        return 0
    n_series = len(set(rows_k))
    if dry:
        log(f"  DRY: {len(rows_k):,} obs / {n_series:,} series")
        return len(rows_k)
    os.makedirs(out_dir, exist_ok=True)
    tbl = pa.table({"series_key": pa.array(rows_k, pa.string()),
                    "obs_date": pa.array(rows_d, pa.date32()),
                    "value": pa.array(rows_v, pa.float64())}, schema=SCHEMA)
    path = os.path.join(out_dir, f"{src}.parquet")
    pq.write_table(tbl, path, compression="zstd")
    n = pq.read_metadata(path).num_rows
    log(f"  WROTE {path}: {n:,} obs / {n_series:,} series")
    return n


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if not args:
        raise SystemExit("usage: ingest_unctad_ds.py <US.DatasetName> [--dry]")
    ingest(args[0], "--dry" in sys.argv)
