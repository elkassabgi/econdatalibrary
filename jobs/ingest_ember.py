#!/usr/bin/env python3
"""FULL-coverage ingest of Ember's published electricity/energy data (CC BY 4.0).

Ember publishes every dataset shown on ember-energy.org/data as flat CSVs on a public
Google Cloud Storage bucket (emb-prod-bkt-publicdata). The bucket is openly LISTABLE
via the GCS JSON API, so we enumerate the ENTIRE catalog rather than hand-curating a
slice. We take every *current* CSV under `public-downloads/` and EXCLUDE the
`*-archive/` point-in-time snapshots (those are historical copies of the current files,
not new data).

GROUPED STORAGE (anti-bloat): ONE Parquet per source CSV ->
  data/clean_full/ember/<dataset_id>.parquet
Each Parquet is a faithful long table carrying a `series_key` column (the comma-joined
identifying dimensions), plus obs_date, value, unit, and the descriptive dimension
columns. 86 source CSVs -> at most ~86 Parquet files for the whole source.

Schema families handled:
  A. global/europe/india/us "*_full_release_long_format" -> Category/Subcategory/
     Variable/Unit/Value with Year or Date (+ optional State for us/india).
  B. capacity monthly wind/solar (Month,Year,...,Installed Capacity,...).
  C. customs / solar-export long tables (Area,Date,...,Amount (USD)/(kg)/(items)).
  D. european wholesale price (Country,ISO3,Date,Price).
  E. methane / NECP / RES-tracker / ISET / methane_imeo (assorted dim+value+year).
  F. wide "graphic"/chart helper + Turkiye/price-chart files (columns are geos/fuels)
     -> melted to long so every cell is one observation.
For any file we cannot map to a known shape, we fall back to a generic melter that
keeps non-numeric columns as the key and melts numeric columns to long.

Usage:
  python jobs/ingest_ember.py --discover        # enumerate catalog only, no download
  python jobs/ingest_ember.py --dry             # download+parse, print, NO parquet
  python jobs/ingest_ember.py                    # full run (download + write parquet)
  python jobs/ingest_ember.py --only yearly_full_release_long_format   # one dataset
"""
from __future__ import annotations

import concurrent.futures as cf
import datetime as dt
import io
import json
import os
import re
import sys
import time
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
sys.path.insert(0, ROOT)

# Windows console is cp1252; force UTF-8 so Turkish/Euro chars in logs don't crash.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:  # noqa: BLE001
    pass

UA = "Econ-Fin Data Library admin@hfdatalibrary.com"
GCS_LIST = "https://storage.googleapis.com/storage/v1/b/emb-prod-bkt-publicdata/o"
GCS_OBJ = "https://storage.googleapis.com/emb-prod-bkt-publicdata/"
PREFIX = "public-downloads/"

RAW = os.path.join(ROOT, "data", "raw", "ember")
OUT = os.path.join(ROOT, "data", "clean_full", "ember")
MANIFEST = os.path.join(RAW, "ember_manifest_full.json")

DATE_FMTS = ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d", "%d-%m-%Y")


# --------------------------------------------------------------------------- #
# 1. CATALOG ENUMERATION                                                       #
# --------------------------------------------------------------------------- #
def enumerate_catalog(session: requests.Session) -> list[dict]:
    """Page the GCS JSON listing and return every object under PREFIX."""
    items, token = [], None
    while True:
        params = {"prefix": PREFIX, "maxResults": 1000}
        if token:
            params["pageToken"] = token
        j = _get_json(session, GCS_LIST, params)
        items.extend(j.get("items", []))
        token = j.get("nextPageToken")
        if not token:
            break
    return items


def _get_json(session, url, params, tries=5):
    last = None
    for a in range(tries):
        try:
            r = session.get(url, params=params, timeout=120)
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(min(2 ** a, 30))
    raise RuntimeError(f"GCS list failed: {last}")


def current_csvs(items: list[dict]) -> list[dict]:
    """Current (non-archive) CSV objects, sorted, as {name,size,updated}."""
    out = []
    for it in items:
        name = it["name"]
        if not name.lower().endswith(".csv"):
            continue
        if "-archive/" in name:
            continue
        out.append({"name": name, "size": int(it.get("size", 0)),
                    "updated": it.get("updated")})
    out.sort(key=lambda x: x["name"])
    return out


def dataset_id(name: str) -> str:
    """Stable, filesystem-safe id from the GCS key (one per source CSV)."""
    rel = name[len(PREFIX):] if name.startswith(PREFIX) else name
    rel = rel[:-4] if rel.lower().endswith(".csv") else rel
    rel = rel.replace("/outputs/", "/").replace("/charts/", "/chart_")
    rel = re.sub(r"[^A-Za-z0-9._-]+", "_", rel).strip("_")
    return rel


# --------------------------------------------------------------------------- #
# 2. DOWNLOAD                                                                   #
# --------------------------------------------------------------------------- #
def download_bytes(session: requests.Session, key: str, tries=5) -> bytes:
    url = GCS_OBJ + key
    last = None
    for a in range(tries):
        try:
            r = session.get(url, timeout=300)
            r.raise_for_status()
            return r.content
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(min(2 ** a, 30))
    raise RuntimeError(f"download failed {key}: {last}")


def read_csv_bytes(raw: bytes) -> pd.DataFrame:
    """Decode with utf-8 then cp1252 fallback (price files use € in cp1252)."""
    for enc in ("utf-8", "utf-8-sig", "cp1252", "latin-1"):
        try:
            return pd.read_csv(io.BytesIO(raw), encoding=enc, low_memory=False)
        except (UnicodeDecodeError, pd.errors.ParserError):
            continue
    # last resort: replace undecodable bytes
    return pd.read_csv(io.StringIO(raw.decode("utf-8", "replace")), low_memory=False)


# --------------------------------------------------------------------------- #
# 3. DATE PARSING                                                               #
# --------------------------------------------------------------------------- #
def parse_year(v) -> Optional[dt.date]:
    try:
        y = int(float(v))
        if 1800 <= y <= 2100:
            return dt.date(y, 12, 31)
    except (ValueError, TypeError):
        pass
    return None


def parse_date(v) -> Optional[dt.date]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    if not s:
        return None
    for fmt in DATE_FMTS:
        try:
            return dt.datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    # year-only fallback
    if re.fullmatch(r"\d{4}", s):
        return dt.date(int(s), 12, 31)
    # mm/yy like "01/16" (Turkiye capacity) -> first of month, 20yy
    m = re.fullmatch(r"(\d{1,2})/(\d{2})", s)
    if m:
        mm, yy = int(m.group(1)), int(m.group(2))
        if 1 <= mm <= 12:
            return dt.date(2000 + yy, mm, 1)
    try:
        ts = pd.to_datetime(s, errors="raise", dayfirst=False)
        return dt.date(ts.year, ts.month, ts.day)
    except Exception:  # noqa: BLE001
        return None


def to_float(v) -> Optional[float]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        f = float(v)
        return f if f == f else None  # drop NaN
    except (ValueError, TypeError):
        return None


# --------------------------------------------------------------------------- #
# 4. PARSERS  (each returns a long DataFrame: series_key,obs_date,value,*dims)  #
# --------------------------------------------------------------------------- #
def _s(v) -> str:
    """Null-safe cell -> string: NaN/None/'nan' -> '' (so labels never read 'nan')."""
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    s = str(v).strip()
    return "" if s.lower() in ("nan", "none", "<na>") else s


def _mk(series_key, obs_date, value, **dims) -> dict:
    row = {"series_key": series_key, "obs_date": obs_date, "value": value}
    row.update(dims)
    return row


def parse_long_release(df: pd.DataFrame, geo_cols, datecol, year: bool) -> list[dict]:
    """Schema A/E-ish: identifying geo cols + Category/Subcategory/Variable/Unit/Value."""
    rows = []
    has_state = "State" in df.columns
    cols = list(df.columns)
    for r in df.itertuples(index=False, name=None):  # plain tuple: preserves col names
        d = dict(zip(cols, r))
        val = to_float(d.get("Value"))
        if val is None:
            continue
        od = (parse_year(d.get(datecol)) if year else parse_date(d.get(datecol)))
        if od is None:
            continue
        geo = _s(d.get(geo_cols[0]))
        iso = _s(d.get(geo_cols[1])) if len(geo_cols) > 1 else ""
        st = _s(d.get("State")) if has_state else ""
        cat = _s(d.get("Category"))
        sub = _s(d.get("Subcategory"))
        var = _s(d.get("Variable"))
        unit = _s(d.get("Unit"))
        geography = iso or geo  # ISO when present; else the (aggregate-region) Area name
        keyparts = [p for p in (geography, st, cat, sub, var, unit) if p]
        sk = "|".join(keyparts)
        rows.append(_mk(sk, od, val, geography=geography, area=geo, state=st,
                        category=cat, subcategory=sub, variable=var, unit=unit))
    return rows


def parse_generic_long(df: pd.DataFrame, datecol: str, value_cols: list[str],
                       key_cols: list[str], year_is_sep_col=None) -> list[dict]:
    """Generic: one or more numeric value columns; key from key_cols (+ value-col name).

    If `year_is_sep_col` given, combine that year col with a month col for the date.
    """
    rows = []
    multi = len(value_cols) > 1
    cols = list(df.columns)
    for r in df.itertuples(index=False, name=None):
        d = dict(zip(cols, r))
        od = None
        if datecol is not None:
            od = parse_date(d.get(datecol))
            if od is None:
                od = parse_year(d.get(datecol))
        if od is None and year_is_sep_col:
            ycol, mcol = year_is_sep_col
            y = parse_year(d.get(ycol))
            if y is not None:
                mm = d.get(mcol)
                try:
                    mmi = int(float(mm))
                    od = dt.date(y.year, max(1, min(12, mmi)), 1)
                except (ValueError, TypeError):
                    od = y
        if od is None:
            continue
        base = [_s(d.get(k)) for k in key_cols]
        for vc in value_cols:
            val = to_float(d.get(vc))
            if val is None:
                continue
            parts = list(base)
            if multi:
                parts.append(vc)
            sk = "|".join([p for p in parts if p]) or vc
            rows.append(_mk(sk, od, val, dimension=vc if multi else (base[0] if base else "")))
    return rows


# Column-name fragments that denote IDENTIFIERS / coordinates / codes, never
# measurements. Used to keep the generic melters honest (a KPLER port id or a
# latitude is not an observation).
_NON_MEASURE = re.compile(
    r"(^id$|_id$|_kpler|kpler_|latitude|longitude|^lat$|^lon$|^lng$|"
    r"_code$|^code$|country.?code|iso|generation_id|mine_id|source_id|"
    r"port_kpler|installation_kpler|^year$|^month$|^unnamed)", re.I)


def _melt_value_cols(df: pd.DataFrame, exclude: set) -> list[str]:
    """Numeric columns eligible as measurements (drop ids/coords/codes)."""
    cols = []
    for c in df.columns:
        if c in exclude:
            continue
        if _NON_MEASURE.search(str(c)):
            continue
        cols.append(c)
    return cols


def parse_wide_melt(df: pd.DataFrame, datecol: str, idvars: list[str]) -> list[dict]:
    """Schema F: melt non-id, non-date columns (geos/fuels) to long, honestly.

    Excludes identifier/coordinate/code columns so we don't manufacture
    observations from things that are not measurements.
    """
    rows = []
    idset = set(idvars) | {datecol}
    value_cols = _melt_value_cols(df, idset)
    cols = list(df.columns)
    for r in df.itertuples(index=False, name=None):
        d = dict(zip(cols, r))
        od = parse_date(d.get(datecol)) or parse_year(d.get(datecol))
        if od is None:
            continue
        prefix = [_s(d.get(k)) for k in idvars if k != datecol]
        for c in value_cols:
            val = to_float(d.get(c))
            if val is None:
                continue
            sk = "|".join([p for p in prefix if p] + [str(c)])
            rows.append(_mk(sk, od, val, series=str(c)))
    return rows


# --------------------------------------------------------------------------- #
# 5. ROUTER -- pick a parser per dataset                                        #
# --------------------------------------------------------------------------- #
def route(ds_id: str, df: pd.DataFrame) -> tuple[str, list[dict]]:
    cols = set(df.columns)
    name = ds_id.lower()

    # -------- per-DATASET overrides (scoped fixes; see test_ember_route_overrides) ----
    # Each corrects ONE file whose branch-assigned keying under-keyed it, sealing
    # duplicate (series_key, obs_date) pairs into the store so every later snapshot
    # merge collapsed below the 97% never-shrink floor and transient-failed forever
    # (run 32816867502: 4/53). The fix is per dataset id, NEVER per branch — widening a
    # branch's keys re-grains every other dataset it routes (R333).
    if ds_id == "methane_chart_satellite_emissions":
        # Event-grain plume detections. Without LATITUDE/LONGITUDE the key carries no
        # event identity: 9,325 melted rows held only 3,013 distinct keys (69 detections
        # under ONE key on 2025-06-22 alone). Lat/lon ARE the identity of a detection.
        return "satellite_events", parse_generic_long(
            df, "DATE", ["EMISSION_RATE__KGH", "UNCERTAINTY__KGH"],
            ["CODE", "SENSOR", "PLATFORM", "REGIONNAME", "LATITUDE", "LONGITUDE"])
    if ds_id == "necp_Ember_NECP_data_2024":
        # Branch E1 matched this file incidentally (YEAR + VALUE) and its keys[:6] cap
        # dropped COUNTRY_NAME and WEM_WAM — a 27-country x scenario panel collapsed
        # from 4,813 value rows to 471 keys. The key list is PINNED, not derived from
        # dtypes (adversarial review 2026-08-26): a dynamic `dtype==object` list
        # silently re-grains on any upstream column add/drop/dtype flip (WEM_WAM is
        # 13,852 nulls away from typing float64 in an emptier snapshot), and a
        # snapshot-source re-grain is invisible to every existing guard because old
        # and new keys never collide (R333). A missing pinned column RAISES, which
        # the orchestrator books as a visible permanent transient — this codebase's
        # honest state for schema drift. The redundant columns (FUEL_LOWER,
        # SHORT_COUNTRY_CODE) are harmless for uniqueness and preserve today's keys.
        _NECP_KEYS = ["CATEGORY", "KPI", "UNIT", "SECTOR", "FUEL_GROUP", "FUEL_CODE",
                      "FUEL_LOWER", "COUNTRY_NAME", "SHORT_COUNTRY_CODE", "WEM_WAM"]
        missing = [c for c in _NECP_KEYS if c not in df.columns]
        if missing:
            raise ValueError(
                f"necp_Ember_NECP_data_2024: pinned key column(s) {missing} absent — "
                f"upstream schema changed; re-grain decision required, refusing to parse")
        return "year_value", parse_generic_long(df, "YEAR", ["VALUE"], _NECP_KEYS)
    if ds_id == "turkiye_data_tool_tur_data_tool_srmc_chart":
        # Branch F2 melted with idvars=[DATETIME] only, so every series_key was the
        # literal string "VALUE" (195 rows -> 65 keys). MEASURE_ENG is the identity
        # (3 fuels); MEASURE_TUR is its translation, not a second dimension.
        return "tidy_measures", parse_generic_long(
            df, "DATETIME", ["VALUE"], ["MEASURE_ENG"])
    # ----------------------------------------------------------------------------------

    # A. global / europe long-format release (Area, ISO 3 code, Year/Date, Category...)
    if {"Category", "Subcategory", "Variable", "Unit", "Value"} <= cols and \
       "ISO 3 code" in cols and ("Year" in cols or "Date" in cols):
        year = "Year" in cols
        return "long_release", parse_long_release(
            df, ["Area", "ISO 3 code"], "Year" if year else "Date", year)

    # A'. us / india subnational long-format (Country, Country code, State, ..., Category)
    if {"Category", "Subcategory", "Variable", "Unit", "Value"} <= cols and \
       "State" in cols and ("Year" in cols or "Date" in cols):
        year = "Year" in cols
        return "long_release_state", parse_long_release(
            df, ["Country", "Country code"], "Year" if year else "Date", year)

    # B. capacity monthly wind/solar
    if {"Month", "Year", "Installed Capacity"} <= cols:
        return "capacity", parse_generic_long(
            df, None, ["Installed Capacity", "Capacity additions (month-on-month)",
                       "Capacity additions (year-to-date)"],
            ["ISO 3 Code", "Area", "Source", "Unit"], year_is_sep_col=("Year", "Month"))

    # C. customs / solar-export long (Area, Date, ... Amount columns)
    amount_cols = [c for c in df.columns if c.startswith("Amount") or c in
                   ("Capacity (MW)", "Cumulative capacity (MW)", "Commodity price")]
    if "Date" in cols and "Area" in cols and amount_cols and \
       ("Commodity category" in cols or "Commodity type" in cols or
        "Commodity code" in cols):
        keys = [c for c in ("Area", "Region", "Commodity category", "Commodity type",
                            "Commodity code") if c in cols]
        return "customs", parse_generic_long(df, "Date", amount_cols, keys)

    # D. european wholesale price
    if {"Country", "Date"} <= cols and any(c.startswith("Price") for c in cols):
        pcol = next(c for c in df.columns if c.startswith("Price"))
        keys = [c for c in ("Country", "ISO3 Code") if c in cols]
        return "price_wholesale", parse_generic_long(df, "Date", [pcol], keys)

    # E1. methane chart_coal_emissions / mine-by-mine (YEAR + value col)
    if "YEAR" in cols and any(c in cols for c in
                              ("EMISSIONS_CH4_KT", "METHANE_EMISSIONS_TONNES",
                               "VALUE", "EMISSIONS_CH4_KT".lower())):
        vcols = [c for c in ("EMISSIONS_CH4_KT", "METHANE_EMISSIONS_TONNES", "VALUE",
                             "SHARE_OF_GENERATION_PCT", "CAPACITY_GW",
                             "CAPACITY_ADDITIONS_GW") if c in cols]
        keys = [c for c in df.columns if c != "YEAR" and c not in vcols
                and df[c].dtype == object][:6]
        return "year_value", parse_generic_long(df, "YEAR", vcols or
                                                 [c for c in df.columns
                                                  if c not in keys and c != "YEAR"], keys)

    # E2. NECP full data (CATEGORY...YEAR,VALUE)
    if "YEAR" in cols and "VALUE" in cols:
        keys = [c for c in df.columns if c not in ("YEAR", "VALUE")
                and df[c].dtype == object][:8]
        return "year_value", parse_generic_long(df, "YEAR", ["VALUE"], keys)

    # E3. ISET (STATE,FINANCIAL_YEAR,METRIC_ID,VALUE)
    if "FINANCIAL_YEAR" in cols and "VALUE" in cols:
        keys = [c for c in df.columns if c not in ("FINANCIAL_YEAR", "VALUE")]
        return "year_value", parse_generic_long(df, "FINANCIAL_YEAR", ["VALUE"], keys)

    # E4. necp_*_chart (Year,Country,Fuel, numeric cols)
    if "Year" in cols and "Country" in cols and "Value" not in cols:
        vcols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])
                 and c != "Year"]
        keys = [c for c in df.columns if c not in vcols and c != "Year"]
        return "year_value", parse_generic_long(df, "Year", vcols, keys)

    # F1. price charts with explicit date col (price_date / delivery_date / COST_DATETIME)
    for dc in ("price_date", "delivery_date", "COST_DATETIME", "Date", "date"):
        if dc in cols:
            num = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
            obj = [c for c in df.columns if c not in num and c != dc
                   and not c.lower().endswith("_string") and "date_string" not in c.lower()]
            if num:
                # wide (geos as columns) vs tidy (one value col)
                if len(num) == 1:
                    return "tidy_date", parse_generic_long(df, dc, num, obj)
                return "wide_date", parse_wide_melt(
                    df, dc, [dc] + obj + [c for c in df.columns
                                          if c.lower().endswith("_string")])

    # F2. Turkiye / china_solar wide (DATETIME or Date with geo/fuel columns)
    for dc in ("DATETIME", "Date"):
        if dc in cols:
            id_extra = [c for c in ("POPUP_DATE", "view", "Unnamed: 0") if c in cols]
            id_extra += [c for c in df.columns if c.startswith("Unnamed")]
            return "wide_melt", parse_wide_melt(df, dc, [dc] + id_extra)

    # Fallback: generic melter on first datey column else first column.
    datey = None
    for c in df.columns:
        if re.search(r"date|year|month|datetime|time", str(c), re.I):
            datey = c
            break
    if datey is not None:
        num = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]) and c != datey]
        obj = [c for c in df.columns if c not in num and c != datey]
        if num:
            return "fallback_melt", parse_wide_melt(df, datey, [datey] + obj)
    return "unparsed", []


# --------------------------------------------------------------------------- #
# 6. WRITE                                                                      #
# --------------------------------------------------------------------------- #
def write_parquet(ds_id: str, rows: list[dict]) -> tuple[int, int, str, str]:
    if not rows:
        return 0, 0, "", ""
    cols = {}
    for k in ("series_key", "obs_date", "value", "geography", "area", "state",
              "category", "subcategory", "variable", "unit", "dimension", "series"):
        present = any(k in r for r in rows)
        if present:
            cols[k] = [r.get(k) for r in rows]
    tbl = pa.table({
        "series_key": pa.array([str(x) for x in cols["series_key"]], pa.string()),
        "obs_date": pa.array(cols["obs_date"], pa.date32()),
        "value": pa.array([float(x) for x in cols["value"]], pa.float64()),
        **{k: pa.array([None if v is None else str(v) for v in cols[k]], pa.string())
           for k in cols if k not in ("series_key", "obs_date", "value")},
    })
    path = os.path.join(OUT, ds_id.replace("/", "__") + ".parquet")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pq.write_table(tbl, path, compression="zstd")
    dates = [d for d in cols["obs_date"] if d is not None]
    nkeys = len(set(map(str, cols["series_key"])))
    return len(rows), nkeys, str(min(dates)), str(max(dates))


# --------------------------------------------------------------------------- #
# 7. MAIN                                                                       #
# --------------------------------------------------------------------------- #
def main():
    os.makedirs(RAW, exist_ok=True)
    os.makedirs(OUT, exist_ok=True)
    args = sys.argv[1:]
    discover_only = "--discover" in args
    dry = "--dry" in args
    only = None
    if "--only" in args:
        only = args[args.index("--only") + 1]

    session = requests.Session()
    session.headers["User-Agent"] = UA

    # 1. enumerate
    print("enumerating Ember GCS catalog ...", flush=True)
    items = enumerate_catalog(session)
    with open(MANIFEST, "w", encoding="utf-8") as f:
        json.dump([{"name": it["name"], "size": int(it.get("size", 0)),
                    "updated": it.get("updated")} for it in items], f)
    csvs = current_csvs(items)
    archive = sum(1 for it in items if it["name"].lower().endswith(".csv")
                  and "-archive/" in it["name"])
    print(f"  total objects: {len(items):,}", flush=True)
    print(f"  current CSV datasets: {len(csvs)}  (excluded {archive:,} archive snapshots)",
          flush=True)
    print(f"  total download size: {sum(c['size'] for c in csvs)/1e6:.1f} MB", flush=True)
    if only:
        csvs = [c for c in csvs if only in c["name"]]
        print(f"  --only filter -> {len(csvs)} file(s)", flush=True)
    if discover_only:
        for c in csvs:
            print(f"    {dataset_id(c['name']):60s} {c['size']/1e6:8.2f} MB", flush=True)
        return

    # 2+3. download + parse + write (bounded concurrency on download; parse in main)
    summary = {"datasets": [], "n_datasets": 0, "n_observations": 0,
               "source_total_csv_files": len(csvs)}
    results = {}

    def fetch(c):
        return c, download_bytes(session, c["name"])

    n_obs_total = 0
    n_ds_done = 0
    with cf.ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(fetch, c): c for c in csvs}
        for fut in cf.as_completed(futs):
            c = futs[fut]
            ds = dataset_id(c["name"])
            try:
                _, raw = fut.result()
                df = read_csv_bytes(raw)
            except Exception as e:  # noqa: BLE001
                print(f"  [DL/READ FAIL] {ds}: {e}", flush=True)
                summary["datasets"].append({"dataset_id": ds, "key": c["name"],
                                            "error": str(e), "n_obs": 0})
                continue
            try:
                kind, rows = route(ds, df)
            except Exception as e:  # noqa: BLE001
                print(f"  [PARSE FAIL] {ds}: {e}", flush=True)
                summary["datasets"].append({"dataset_id": ds, "key": c["name"],
                                            "error": f"parse: {e}", "n_obs": 0,
                                            "raw_rows": int(len(df))})
                continue
            if dry:
                samp = rows[0] if rows else None
                print(f"  {ds:55s} kind={kind:16s} raw={len(df):>8,} obs={len(rows):>9,}"
                      + (f"  e.g. sk={samp['series_key'][:40]!r} {samp['obs_date']} ={samp['value']}"
                         if samp else "  (no obs)"), flush=True)
                n_obs_total += len(rows)
                n_ds_done += 1
                continue
            n_obs, nkeys, dmin, dmax = write_parquet(ds, rows)
            n_obs_total += n_obs
            n_ds_done += 1
            if n_obs > 0:                 # write_parquet only writes a file when n_obs>0
                results[ds] = n_obs
            summary["datasets"].append({
                "dataset_id": ds, "key": c["name"], "kind": kind,
                "raw_rows": int(len(df)), "n_obs": n_obs, "n_series_keys": nkeys,
                "date_min": dmin, "date_max": dmax, "size_mb": round(c["size"]/1e6, 2)})
            print(f"  [{n_ds_done}/{len(csvs)}] {ds:50s} kind={kind:15s} "
                  f"obs={n_obs:>9,} keys={nkeys:>6,} {dmin}..{dmax}", flush=True)

    summary["n_datasets"] = n_ds_done
    summary["n_observations"] = n_obs_total
    summary["written_parquet"] = len(results)
    if not dry:
        with open(os.path.join(OUT, "_summary.json"), "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
    print(f"\n{'DRY' if dry else 'DONE'}: {n_ds_done} datasets / {n_obs_total:,} observations",
          flush=True)
    if not dry:
        print(f"  parquet files written: {len(results)} -> {OUT}", flush=True)
        print(f"  summary -> {os.path.join(OUT, '_summary.json')}", flush=True)


if __name__ == "__main__":
    main()
