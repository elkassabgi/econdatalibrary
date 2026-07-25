#!/usr/bin/env python3
"""Full-coverage ingest of the FHFA House Price Index (HPI) datasets.

Source page: https://www.fhfa.gov/data/hpi/datasets   License: us-public-domain.

GROUPED storage (anti-bloat): a handful of Parquet cubes under
data/clean_full/fhfa/, ONE file per logical cube, each holding MANY series
keyed by a `series_key` column. No one-file-per-series.

Cubes written
-------------
  hpi_master                FHFA's own consolidated master.csv: USA/CensusDiv,
                            State, MSA levels x {traditional, distress-free,
                            expanded-data, manufactured, non-metro,
                            developmental} x {purchase-only, all-transactions} x
                            {monthly, quarterly}.  Columns carry both
                            index_nsa and index_sa.
  hpi_at_3zip_quarterly     Developmental quarterly All-Transactions index for
                            three-digit ZIP codes (NSA only; not in master).
  annual_national           \
  annual_state              |  Annual All-Transactions "experimental" indexes
  annual_cbsa               |  (cumulative nominal appreciation). Three index
  annual_county             |  bases per obs: native (hpi), 1990 base, 2000
  annual_zip3               |  base, plus annual_change %.  These geographies
  annual_zip5               |  (county, zip3, zip5, tract) are NOT in master.
  annual_tract              /

Each cube => <cube>.parquet (obs) + <cube>__series.parquet (series meta) +
<cube>.meta.json (verification stats). A top-level fhfa.meta.json aggregates.

obs_date convention: monthly -> first of month; quarterly -> quarter END;
annual -> Dec 31.

Usage:
  python jobs/ingest_fhfa.py --download   # (re)download raw files
  python jobs/ingest_fhfa.py              # parse raw -> grouped parquet + verify
  python jobs/ingest_fhfa.py --download --build   # do both
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
import time
import warnings

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

warnings.filterwarnings("ignore")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
RAW = os.path.join(ROOT, "data", "raw", "fhfa")
OUT = os.path.join(ROOT, "data", "clean_full", "fhfa")
UA = "Econ-Fin Data Library admin@hfdatalibrary.com"
LICENSE_ID = "us-public-domain"
BASE = "https://www.fhfa.gov"

# raw file name -> remote path
RAW_FILES = {
    "hpi_master.csv": "/hpi/download/monthly/hpi_master.csv",
    "hpi_at_3zip.xlsx": "/hpi/download/quarterly_datasets/hpi_at_3zip.xlsx",
    "annual_hpi_at_national.xlsx": "/hpi/download/annual/hpi_at_national.xlsx",
    "annual_hpi_at_state.xlsx": "/hpi/download/annual/hpi_at_state.xlsx",
    "annual_hpi_at_cbsa.xlsx": "/hpi/download/annual/hpi_at_cbsa.xlsx",
    "annual_hpi_at_county.xlsx": "/hpi/download/annual/hpi_at_county.xlsx",
    "annual_hpi_at_zip3.xlsx": "/hpi/download/annual/hpi_at_zip3.xlsx",
    "annual_hpi_at_zip5.xlsx": "/hpi/download/annual/hpi_at_zip5.xlsx",
    "annual_hpi_at_tract.csv": "/hpi/download/annual/hpi_at_tract.csv",
}


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _qtr_end(year: int, q: int) -> dt.date:
    m = q * 3
    if m == 3:
        return dt.date(year, 3, 31)
    if m == 6:
        return dt.date(year, 6, 30)
    if m == 9:
        return dt.date(year, 9, 30)
    return dt.date(year, 12, 31)


def _to_float(x):
    if x is None:
        return None
    s = str(x).strip()
    if s in ("", ".", "nan", "NaN", "None", "-"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _to_int(x):
    f = _to_float(x)
    return None if f is None else int(f)


def _safe(s: str) -> str:
    return "".join(c if (c.isalnum() or c in "._-") else "_" for c in str(s))


def _write_cube(cube: str, obs_df: pd.DataFrame, series_df: pd.DataFrame, value_cols):
    """Write <cube>.parquet + <cube>__series.parquet and return verification stats.

    obs_df must have: series_key, obs_date (date), <value_cols...>.
    series_df is one row per series with metadata.
    """
    os.makedirs(OUT, exist_ok=True)
    # ---- obs table ----
    cols = {
        "dataset": pa.array([cube] * len(obs_df), type=pa.string()),
        "series_key": pa.array(obs_df["series_key"].astype(str).tolist(), type=pa.string()),
        "obs_date": pa.array(obs_df["obs_date"].tolist(), type=pa.date32()),
    }
    for vc in value_cols:
        cols[vc] = pa.array(obs_df[vc].tolist(), type=pa.float64())
    # pass through any extra string columns (e.g. index_type, state_abbr)
    for extra in obs_df.columns:
        if extra in ("series_key", "obs_date") or extra in value_cols or extra in cols:
            continue
        cols[extra] = pa.array(obs_df[extra].astype("string").tolist(), type=pa.string())
    tbl = pa.table(cols)
    pq.write_table(tbl, os.path.join(OUT, f"{cube}.parquet"), compression="zstd")

    # ---- series table ----
    s_cols = {c: pa.array(series_df[c].astype("string").tolist(), type=pa.string())
              for c in series_df.columns if c != "n_obs"}
    if "n_obs" in series_df.columns:
        s_cols["n_obs"] = pa.array(series_df["n_obs"].tolist(), type=pa.int64())
    s_tbl = pa.table(s_cols)
    pq.write_table(s_tbl, os.path.join(OUT, f"{cube}__series.parquet"), compression="zstd")

    # ---- verify by re-reading ----
    back = pq.read_table(os.path.join(OUT, f"{cube}.parquet"))
    n_obs = back.num_rows
    n_series = back.column("series_key").to_pandas().nunique()
    dates = back.column("obs_date").to_pandas().dropna()
    meta = {
        "cube": cube,
        "n_obs": int(n_obs),
        "n_series": int(n_series),
        "value_cols": list(value_cols),
        "start": str(dates.min()) if len(dates) else None,
        "end": str(dates.max()) if len(dates) else None,
        "n_series_meta_rows": int(s_tbl.num_rows),
        "verified_rows": int(n_obs),
        "verify_ok": bool(n_obs == len(obs_df) and s_tbl.num_rows == len(series_df)),
    }
    with open(os.path.join(OUT, f"{cube}.meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
    return meta


# --------------------------------------------------------------------------- #
# download
# --------------------------------------------------------------------------- #
def download():
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    os.makedirs(RAW, exist_ok=True)
    sess = requests.Session()
    retry = Retry(total=5, backoff_factor=1.5,
                  status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET"])
    sess.mount("https://", HTTPAdapter(max_retries=retry))
    sess.headers.update({"User-Agent": UA})
    print(f"DOWNLOAD: {len(RAW_FILES)} files -> {RAW}", flush=True)
    for name, path in RAW_FILES.items():
        out = os.path.join(RAW, name)
        for attempt in range(3):
            try:
                r = sess.get(BASE + path, timeout=300)
                r.raise_for_status()
                with open(out, "wb") as f:
                    f.write(r.content)
                print(f"  {name:30} {r.status_code} {len(r.content):>12,} bytes", flush=True)
                break
            except Exception as e:  # noqa: BLE001
                print(f"  {name:30} attempt {attempt+1} failed: {e}", flush=True)
                time.sleep(2 * (attempt + 1))
        time.sleep(0.5)  # polite


# --------------------------------------------------------------------------- #
# parse: master
# --------------------------------------------------------------------------- #
def build_master():
    path = os.path.join(RAW, "hpi_master.csv")
    df = pd.read_csv(path, dtype=str)
    df.columns = [c.strip() for c in df.columns]
    # series_key = type|flavor|freq|place_id
    df["series_key"] = (df["hpi_type"].str.strip() + "|" + df["hpi_flavor"].str.strip()
                        + "|" + df["frequency"].str.strip() + "|" + df["place_id"].str.strip())

    def mk_date(row):
        y = _to_int(row["yr"])
        p = _to_int(row["period"])
        if y is None or p is None:
            return None
        if row["frequency"].strip() == "monthly":
            if 1 <= p <= 12:
                return dt.date(y, p, 1)
            return None
        # quarterly
        if 1 <= p <= 4:
            return _qtr_end(y, p)
        return None

    df["obs_date"] = df.apply(mk_date, axis=1)
    df["index_nsa"] = df["index_nsa"].map(_to_float)
    df["index_sa"] = df["index_sa"].map(_to_float)
    df["rstderr"] = df["rstderr"].map(_to_float)
    before = len(df)
    df = df[df["obs_date"].notna()].copy()
    # keep rows that have at least one index value
    df = df[df["index_nsa"].notna() | df["index_sa"].notna()].copy()
    obs = df[["series_key", "obs_date", "index_nsa", "index_sa", "rstderr"]].copy()

    # series meta
    meta_rows = []
    for sk, g in df.groupby("series_key"):
        r0 = g.iloc[0]
        meta_rows.append({
            "dataset": "hpi_master",
            "series_key": sk,
            "hpi_type": r0["hpi_type"].strip(),
            "hpi_flavor": r0["hpi_flavor"].strip(),
            "frequency": r0["frequency"].strip(),
            "level": r0["level"].strip(),
            "place_name": r0["place_name"].strip(),
            "place_id": r0["place_id"].strip(),
            "has_sa": str(bool(g["index_sa"].notna().any())),
            "n_obs": int(len(g)),
            "start": str(g["obs_date"].min()),
            "end": str(g["obs_date"].max()),
        })
    series_df = pd.DataFrame(meta_rows)
    m = _write_cube("hpi_master", obs, series_df,
                    value_cols=["index_nsa", "index_sa", "rstderr"])
    m["dropped_no_date_or_value"] = int(before - len(df))
    print(f"  hpi_master: series={m['n_series']:,} obs={m['n_obs']:,} "
          f"({m['start']}..{m['end']}) verify={m['verify_ok']}", flush=True)
    return m


# --------------------------------------------------------------------------- #
# parse: quarterly 3-digit ZIP (developmental)
# --------------------------------------------------------------------------- #
def build_3zip_quarterly():
    path = os.path.join(RAW, "hpi_at_3zip.xlsx")
    # header at row index 4
    df = pd.read_excel(path, sheet_name=0, header=4, dtype=str)
    df.columns = [str(c).strip() for c in df.columns]
    # expected: Three-Digit ZIP Code | Year | Quarter | Index (NSA) | Index Type
    colmap = {}
    for c in df.columns:
        cl = c.lower()
        if "zip" in cl:
            colmap[c] = "zip3"
        elif cl == "year":
            colmap[c] = "year"
        elif cl == "quarter":
            colmap[c] = "quarter"
        elif "index" in cl and "type" not in cl:
            colmap[c] = "index_nsa"
        elif "type" in cl:
            colmap[c] = "index_type"
    df = df.rename(columns=colmap)
    df = df[df["zip3"].notna()].copy()
    df["zip3"] = df["zip3"].astype(str).str.strip().str.zfill(3)
    df["year_i"] = df["year"].map(_to_int)
    df["q_i"] = df["quarter"].map(_to_int)
    df["obs_date"] = [
        _qtr_end(y, q) if (y is not None and q in (1, 2, 3, 4)) else None
        for y, q in zip(df["year_i"], df["q_i"])
    ]
    df["index_nsa"] = df["index_nsa"].map(_to_float)
    df["series_key"] = df["zip3"]
    before = len(df)
    df = df[df["obs_date"].notna() & df["index_nsa"].notna()].copy()
    obs = df[["series_key", "obs_date", "index_nsa", "index_type"]].copy()

    meta_rows = []
    for sk, g in df.groupby("series_key"):
        meta_rows.append({
            "dataset": "hpi_at_3zip_quarterly",
            "series_key": sk,
            "zip3": sk,
            "hpi_type": "developmental",
            "hpi_flavor": "all-transactions",
            "frequency": "quarterly",
            "level": "3-digit ZIP",
            "index_type_last": str(g.iloc[-1]["index_type"]),
            "n_obs": int(len(g)),
            "start": str(g["obs_date"].min()),
            "end": str(g["obs_date"].max()),
        })
    series_df = pd.DataFrame(meta_rows)
    m = _write_cube("hpi_at_3zip_quarterly", obs, series_df, value_cols=["index_nsa"])
    m["dropped_no_date_or_value"] = int(before - len(df))
    print(f"  hpi_at_3zip_quarterly: series={m['n_series']:,} obs={m['n_obs']:,} "
          f"({m['start']}..{m['end']}) verify={m['verify_ok']}", flush=True)
    return m


# --------------------------------------------------------------------------- #
# parse: annual all-transactions files (county/zip/tract/etc)
# --------------------------------------------------------------------------- #
def _build_annual_xlsx(cube, fname, key_cols, extra_meta_cols=None):
    """Generic builder for the annual XLSX files (header at row index 5).

    key_cols: ordered list of (df_colname_after_rename, role) where role in
              {"place_id","place_name","state","fips","abbr"} used to form the key.
    """
    extra_meta_cols = extra_meta_cols or []
    path = os.path.join(RAW, fname)
    df = pd.read_excel(path, sheet_name=0, header=5, dtype=str)
    df.columns = [str(c).strip() for c in df.columns]
    # normalize columns by position-independent matching
    ren = {}
    for c in df.columns:
        cl = c.lower()
        if cl == "year":
            ren[c] = "year"
        elif "annual change" in cl:
            ren[c] = "annual_change"
        elif cl == "hpi" or cl.startswith("hpi") and "base" not in cl and "1990" not in cl and "2000" not in cl:
            ren[c] = "hpi"
        elif "1990" in cl:
            ren[c] = "hpi_1990"
        elif "2000" in cl:
            ren[c] = "hpi_2000"
        elif "five-digit" in cl or "5-digit" in cl:
            ren[c] = "zip"
        elif "three-digit" in cl or "3-digit" in cl:
            ren[c] = "zip"
        elif cl == "cbsa":
            ren[c] = "cbsa"
        elif cl == "fips" or "fips" in cl:
            ren[c] = "fips"
        elif cl == "abbreviation":
            ren[c] = "abbr"
        elif cl == "state":
            ren[c] = "state"
        elif cl == "county":
            ren[c] = "county"
        elif cl == "name":
            ren[c] = "name"
    df = df.rename(columns=ren)

    # form series_key + place_name from key_cols
    def build_key(row):
        parts = []
        for col, _role in key_cols:
            v = row.get(col)
            parts.append("" if pd.isna(v) else str(v).strip())
        return "|".join(parts)

    df["series_key"] = df.apply(build_key, axis=1)
    df = df[df["series_key"].str.replace("|", "", regex=False).str.strip() != ""].copy()
    df["year_i"] = df["year"].map(_to_int)
    df = df[df["year_i"].notna()].copy()
    df["obs_date"] = df["year_i"].map(lambda y: dt.date(int(y), 12, 31))
    for vc in ("annual_change", "hpi", "hpi_1990", "hpi_2000"):
        if vc in df.columns:
            df[vc] = df[vc].map(_to_float)
        else:
            df[vc] = None
    before = len(df)
    df = df[df["hpi"].notna()].copy()
    obs = df[["series_key", "obs_date", "hpi", "hpi_1990", "hpi_2000", "annual_change"]].copy()

    # series meta
    keep_meta = ["zip", "cbsa", "fips", "abbr", "state", "county", "name"]
    keep_meta = [c for c in keep_meta if c in df.columns] + extra_meta_cols
    meta_rows = []
    for sk, g in df.groupby("series_key"):
        r0 = g.iloc[0]
        row = {"dataset": cube, "series_key": sk,
               "hpi_type": "developmental", "hpi_flavor": "all-transactions",
               "frequency": "annual"}
        for mc in keep_meta:
            if mc in g.columns:
                row[mc] = "" if pd.isna(r0[mc]) else str(r0[mc]).strip()
        row["n_obs"] = int(len(g))
        row["start"] = str(g["obs_date"].min())
        row["end"] = str(g["obs_date"].max())
        meta_rows.append(row)
    series_df = pd.DataFrame(meta_rows)
    m = _write_cube(cube, obs, series_df,
                    value_cols=["hpi", "hpi_1990", "hpi_2000", "annual_change"])
    m["dropped_no_hpi"] = int(before - len(df))
    print(f"  {cube}: series={m['n_series']:,} obs={m['n_obs']:,} "
          f"({m['start']}..{m['end']}) verify={m['verify_ok']}", flush=True)
    return m


def build_annual_national():
    # national: Year | Annual Change | HPI | HPI 1990 | HPI 2000  (single series)
    path = os.path.join(RAW, "annual_hpi_at_national.xlsx")
    df = pd.read_excel(path, sheet_name=0, header=5, dtype=str)
    df.columns = [str(c).strip() for c in df.columns]
    ren = {}
    for c in df.columns:
        cl = c.lower()
        if cl == "year":
            ren[c] = "year"
        elif "annual change" in cl:
            ren[c] = "annual_change"
        elif "1990" in cl:
            ren[c] = "hpi_1990"
        elif "2000" in cl:
            ren[c] = "hpi_2000"
        elif cl == "hpi":
            ren[c] = "hpi"
    df = df.rename(columns=ren)
    df["series_key"] = "USA"
    df["year_i"] = df["year"].map(_to_int)
    df = df[df["year_i"].notna()].copy()
    df["obs_date"] = df["year_i"].map(lambda y: dt.date(int(y), 12, 31))
    for vc in ("annual_change", "hpi", "hpi_1990", "hpi_2000"):
        df[vc] = df[vc].map(_to_float) if vc in df.columns else None
    before = len(df)
    df = df[df["hpi"].notna()].copy()
    obs = df[["series_key", "obs_date", "hpi", "hpi_1990", "hpi_2000", "annual_change"]].copy()
    series_df = pd.DataFrame([{
        "dataset": "annual_national", "series_key": "USA",
        "hpi_type": "developmental", "hpi_flavor": "all-transactions",
        "frequency": "annual", "place_name": "United States",
        "n_obs": int(len(df)), "start": str(df["obs_date"].min()),
        "end": str(df["obs_date"].max()),
    }])
    m = _write_cube("annual_national", obs, series_df,
                    value_cols=["hpi", "hpi_1990", "hpi_2000", "annual_change"])
    m["dropped_no_hpi"] = int(before - len(df))
    print(f"  annual_national: series={m['n_series']:,} obs={m['n_obs']:,} "
          f"({m['start']}..{m['end']}) verify={m['verify_ok']}", flush=True)
    return m


# --------------------------------------------------------------------------- #
# parse: census tract CSV
# --------------------------------------------------------------------------- #
def build_annual_tract():
    path = os.path.join(RAW, "annual_hpi_at_tract.csv")
    # columns: tract, state_abbr, year, annual_change, hpi, hpi1990, hpi2000
    df = pd.read_csv(path, dtype=str)
    df.columns = [c.strip() for c in df.columns]
    df = df[df["tract"].notna()].copy()
    df["series_key"] = df["tract"].astype(str).str.strip()
    df["year_i"] = df["year"].map(_to_int)
    df = df[df["year_i"].notna()].copy()
    df["obs_date"] = df["year_i"].map(lambda y: dt.date(int(y), 12, 31))
    df["hpi"] = df["hpi"].map(_to_float)
    df["hpi_1990"] = df["hpi1990"].map(_to_float)
    df["hpi_2000"] = df["hpi2000"].map(_to_float)
    df["annual_change"] = df["annual_change"].map(_to_float)
    before = len(df)
    df = df[df["hpi"].notna()].copy()
    obs = df[["series_key", "obs_date", "hpi", "hpi_1990", "hpi_2000", "annual_change"]].copy()

    # series meta -- vectorized (2.1M rows -> ~70k tracts)
    grp = df.groupby("series_key")
    meta = grp.agg(state_abbr=("state_abbr", "first"),
                   n_obs=("obs_date", "size"),
                   start=("obs_date", "min"),
                   end=("obs_date", "max")).reset_index()
    meta["dataset"] = "annual_tract"
    meta["hpi_type"] = "developmental"
    meta["hpi_flavor"] = "all-transactions"
    meta["frequency"] = "annual"
    meta["level"] = "census-tract"
    meta["start"] = meta["start"].astype(str)
    meta["end"] = meta["end"].astype(str)
    series_df = meta[["dataset", "series_key", "state_abbr", "hpi_type", "hpi_flavor",
                      "frequency", "level", "n_obs", "start", "end"]]
    m = _write_cube("annual_tract", obs, series_df,
                    value_cols=["hpi", "hpi_1990", "hpi_2000", "annual_change"])
    m["dropped_no_hpi"] = int(before - len(df))
    print(f"  annual_tract: series={m['n_series']:,} obs={m['n_obs']:,} "
          f"({m['start']}..{m['end']}) verify={m['verify_ok']}", flush=True)
    return m


# --------------------------------------------------------------------------- #
def build():
    os.makedirs(OUT, exist_ok=True)
    metas = []
    print("BUILD: parsing FHFA raw -> grouped parquet", flush=True)
    metas.append(build_master())
    metas.append(build_3zip_quarterly())
    metas.append(build_annual_national())
    metas.append(_build_annual_xlsx(
        "annual_state", "annual_hpi_at_state.xlsx",
        key_cols=[("fips", "fips")]))
    metas.append(_build_annual_xlsx(
        "annual_cbsa", "annual_hpi_at_cbsa.xlsx",
        key_cols=[("cbsa", "place_id")]))
    metas.append(_build_annual_xlsx(
        "annual_county", "annual_hpi_at_county.xlsx",
        key_cols=[("fips", "fips")]))
    metas.append(_build_annual_xlsx(
        "annual_zip3", "annual_hpi_at_zip3.xlsx",
        key_cols=[("zip", "place_id")]))
    metas.append(_build_annual_xlsx(
        "annual_zip5", "annual_hpi_at_zip5.xlsx",
        key_cols=[("zip", "place_id")]))
    metas.append(build_annual_tract())

    total_obs = sum(m["n_obs"] for m in metas)
    total_series = sum(m["n_series"] for m in metas)
    agg = {
        "source": "fhfa",
        "license": LICENSE_ID,
        "attribution": "Source: U.S. FHFA (public domain)",
        "homepage": "https://www.fhfa.gov/data/hpi/datasets",
        "n_cubes": len(metas),
        "n_files_parquet": len(metas) * 2,  # obs + series each
        "total_obs": int(total_obs),
        "total_series": int(total_series),
        "cubes": metas,
        "all_verify_ok": all(m["verify_ok"] for m in metas),
        "built_at": dt.datetime.now().isoformat(timespec="seconds"),
    }
    with open(os.path.join(OUT, "fhfa.meta.json"), "w", encoding="utf-8") as f:
        json.dump(agg, f, indent=2)
    print(f"\nDONE: {len(metas)} cubes  total_obs={total_obs:,}  "
          f"total_series={total_series:,}  all_verify_ok={agg['all_verify_ok']}", flush=True)
    return agg


def main():
    do_dl = "--download" in sys.argv
    do_build = "--build" in sys.argv or not do_dl  # default to build if no flag
    if do_dl:
        download()
    if do_build:
        build()


if __name__ == "__main__":
    main()
