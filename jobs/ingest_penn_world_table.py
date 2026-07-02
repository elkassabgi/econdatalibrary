#!/usr/bin/env python3
"""FULL-COVERAGE grouped ingest of the ENTIRE Penn World Table 11.0 panel.

Source: Penn World Table 11.0 (CC BY 4.0).  license_id = cc-by-4.0.
Cite: Feenstra, Inklaar & Timmer (2015), "The Next Generation of the Penn World
Table", American Economic Review 105(10), 3150-3182.  DOI 10.34894/FABVLR.

CATALOG / ENUMERATION
---------------------
PWT ships as ONE Excel workbook (pwt110.xlsx) on the GGDC Dataverse. There is no
incremental API and no per-series endpoint: a release is a single full annual
panel. The "catalog" is therefore the cross product of every economy x every
variable in the workbook's `Data` sheet. PWT 11.0 = 185 economies x 1950-2023 x
42 numeric variables. We download the workbook (polite UA, retry/backoff), then
parse EVERY (countrycode, variable) pair -- no sampling, no curated subset.

VARIABLES
---------
The `Data` sheet has 51 columns. We publish the 42 NUMERIC variables (real GDP /
employment / population levels; current-price GDP, capital & TFP; national-
accounts variables; exchange rates & GDP price levels; expenditure shares in
CGDPo; price levels of expenditure categories & capital; plus cor_exp). The 5
`i_*` "Data information variables" (i_cig, i_xm, i_xr, i_outlier, i_irr) are
CATEGORICAL TEXT labels (Benchmark / Extrapolated / Interpolated / Outlier ...),
not numeric series, so they are not emitted as float observations; their meaning
is recorded in metadata instead. currency_unit is an identifier, not data.

GROUPED STORAGE (anti-bloat)
----------------------------
ONE Parquet per VARIABLE ->  data/clean_full/penn_world_table/<variable>.parquet
with columns
    series_key (string), obs_date (date32), value (float64)
where series_key = "<variable>|<ISO3>".  All 185 economies of a variable live
inside that ONE file. 42 variables => 42 files for the whole source -- NOT one-
file-per-series (which would be ~7,770 tiny files). Annual values are dated
Dec-31 of the year, matching the eurostat / owid / faostat annual convention.

A per-source JSON summary (_ingest_summary.json) records coverage. catalog.db is
NOT touched here; data/clean/ is NOT touched here.

Usage:
  python jobs/ingest_penn_world_table.py --dry      # parse + report, no writes
  python jobs/ingest_penn_world_table.py            # FULL run (download if needed, write all)
  python jobs/ingest_penn_world_table.py --force-download   # re-download the workbook
"""
from __future__ import annotations

import datetime as dt
import io
import json
import os
import sys
import time

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
sys.path.insert(0, ROOT)

SOURCE_ID = "penn_world_table"
LICENSE_ID = "cc-by-4.0"
PWT_VERSION = "11.0"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

# GGDC Dataverse datafile id for pwt110.xlsx (302-redirects to the object store,
# serves with Content-Disposition filename pwt110.xlsx). DOI 10.34894/FABVLR.
DATA_URL = "https://dataverse.nl/api/access/datafile/554105"

RAW = os.path.join(ROOT, "data", "raw", SOURCE_ID)
OUT = os.path.join(ROOT, "data", "clean_full", SOURCE_ID)
XLSX = os.path.join(RAW, "pwt110.xlsx")
os.makedirs(RAW, exist_ok=True)
os.makedirs(OUT, exist_ok=True)

# Identifier columns and the categorical "Data information" columns -> NOT data.
ID_COLS = {"countrycode", "country", "currency_unit", "year"}
INFO_COLS = {"i_cig", "i_xm", "i_xr", "i_outlier", "i_irr"}

# Authoritative variable definitions (verbatim from the workbook's Legend sheet)
# and a compact unit string. Order follows the Legend's thematic grouping.
VAR_DEFS = {
    "rgdpe":   ("Expenditure-side real GDP at chained PPPs (in mil. 2021US$)", "mil. 2021US$"),
    "rgdpo":   ("Output-side real GDP at chained PPPs (in mil. 2021US$)", "mil. 2021US$"),
    "pop":     ("Population (in millions)", "millions of persons"),
    "emp":     ("Number of persons engaged (in millions)", "millions of persons"),
    "avh":     ("Average annual hours worked by persons engaged", "hours/year"),
    "hc":      ("Human capital index, based on years of schooling and returns to education", "index"),
    "ccon":    ("Real consumption of households and government, at current PPPs (in mil. 2021US$)", "mil. 2021US$"),
    "cda":     ("Real domestic absorption (real consumption plus investment), at current PPPs (in mil. 2021US$)", "mil. 2021US$"),
    "cgdpe":   ("Expenditure-side real GDP at current PPPs (in mil. 2021US$)", "mil. 2021US$"),
    "cgdpo":   ("Output-side real GDP at current PPPs (in mil. 2021US$)", "mil. 2021US$"),
    "cn":      ("Capital stock at current PPPs (in mil. 2021US$)", "mil. 2021US$"),
    "ck":      ("Capital services levels at current PPPs (USA=1)", "index, USA=1"),
    "ctfp":    ("TFP level at current PPPs (USA=1)", "index, USA=1"),
    "cwtfp":   ("Welfare-relevant TFP levels at current PPPs (USA=1)", "index, USA=1"),
    "rgdpna":  ("Real GDP at constant 2021 national prices (in mil. 2021US$)", "mil. 2021US$"),
    "rconna":  ("Real consumption at constant 2021 national prices (in mil. 2021US$)", "mil. 2021US$"),
    "rdana":   ("Real domestic absorption at constant 2021 national prices (in mil. 2021US$)", "mil. 2021US$"),
    "rnna":    ("Capital stock at constant 2021 national prices (in mil. 2021US$)", "mil. 2021US$"),
    "rkna":    ("Capital services at constant 2021 national prices (2021=1)", "index, 2021=1"),
    "rtfpna":  ("TFP at constant national prices (2021=1)", "index, 2021=1"),
    "rwtfpna": ("Welfare-relevant TFP at constant national prices (2021=1)", "index, 2021=1"),
    "labsh":   ("Share of labour compensation in GDP at current national prices", "share"),
    "irr":     ("Real internal rate of return", "rate"),
    "delta":   ("Average depreciation rate of the capital stock", "rate"),
    "xr":      ("Exchange rate, national currency/USD (market+estimated)", "NC/USD"),
    "pl_con":  ("Price level of CCON (PPP/XR), price level of USA GDPo in 2021=1", "index, USA 2021=1"),
    "pl_da":   ("Price level of CDA (PPP/XR), price level of USA GDPo in 2021=1", "index, USA 2021=1"),
    "pl_gdpo": ("Price level of CGDPo (PPP/XR), price level of USA GDPo in 2021=1", "index, USA 2021=1"),
    "cor_exp": ("Correlation between expenditure shares of the country and the US (benchmark observations only)", "correlation"),
    "csh_c":   ("Share of household consumption at current PPPs", "share"),
    "csh_i":   ("Share of gross capital formation at current PPPs", "share"),
    "csh_g":   ("Share of government consumption at current PPPs", "share"),
    "csh_x":   ("Share of merchandise exports at current PPPs", "share"),
    "csh_m":   ("Share of merchandise imports at current PPPs", "share"),
    "csh_r":   ("Share of residual trade and GDP statistical discrepancy at current PPPs", "share"),
    "pl_c":    ("Price level of household consumption, price level of USA GDPo in 2021=1", "index, USA 2021=1"),
    "pl_i":    ("Price level of capital formation, price level of USA GDPo in 2021=1", "index, USA 2021=1"),
    "pl_g":    ("Price level of government consumption, price level of USA GDPo in 2021=1", "index, USA 2021=1"),
    "pl_x":    ("Price level of exports, price level of USA GDPo in 2021=1", "index, USA 2021=1"),
    "pl_m":    ("Price level of imports, price level of USA GDPo in 2021=1", "index, USA 2021=1"),
    "pl_n":    ("Price level of the capital stock, price level of USA in 2021=1", "index, USA 2021=1"),
    "pl_k":    ("Price level of the capital services, price level of USA=1", "index, USA=1"),
}

SCHEMA = pa.schema([
    ("series_key", pa.string()),
    ("obs_date", pa.date32()),
    ("value", pa.float64()),
])


def log(msg):
    print(msg, flush=True)


def download_workbook(force=False):
    """Fetch pwt110.xlsx with polite UA + exponential backoff; cache to disk."""
    if os.path.exists(XLSX) and os.path.getsize(XLSX) > 100_000 and not force:
        log(f"workbook cached: {XLSX} ({os.path.getsize(XLSX):,} bytes)")
        return
    last = None
    for attempt in range(5):
        try:
            r = requests.get(DATA_URL, headers=UA, timeout=300, allow_redirects=True)
            r.raise_for_status()
            with open(XLSX, "wb") as f:
                f.write(r.content)
            log(f"downloaded pwt110.xlsx: {len(r.content):,} bytes")
            return
        except Exception as e:  # noqa: BLE001 -- network/transient
            last = e
            log(f"  download attempt {attempt} failed: {e!r}"[:200])
            time.sleep(2 ** attempt)  # 1,2,4,8,16s
    raise RuntimeError(f"PWT download failed after retries: {last}")


def atomic_write_parquet(tbl, out_path, *, retries=8):
    """Write Parquet to a temp file then rename into place (Windows-safe)."""
    tmp = f"{out_path}.{os.getpid()}.part"
    pq.write_table(tbl, tmp, compression="zstd")
    last = None
    for attempt in range(retries):
        try:
            os.replace(tmp, out_path)
            return
        except PermissionError as e:  # WinError 32 (file briefly locked)
            last = e
            time.sleep(0.25 * (attempt + 1))
    try:
        with open(tmp, "rb") as fsrc, open(out_path, "wb") as fdst:
            fdst.write(fsrc.read())
        os.remove(tmp)
        return
    except Exception:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass
        raise last


def main():
    args = sys.argv[1:]
    dry = "--dry" in args
    force_dl = "--force-download" in args

    download_workbook(force=force_dl)

    df = pd.read_excel(XLSX, sheet_name="Data", engine="openpyxl")
    log(f"Data sheet: {df.shape[0]:,} rows x {df.shape[1]} cols")

    # Variables to publish = numeric columns, neither identifier nor i_* info code.
    data_cols = [c for c in df.columns if c not in ID_COLS and c not in INFO_COLS]
    # sanity: every published var must be in VAR_DEFS (legend) and numeric
    missing_def = [c for c in data_cols if c not in VAR_DEFS]
    if missing_def:
        raise RuntimeError(f"variables without a legend definition: {missing_def}")
    economies = (
        df[["countrycode", "country", "currency_unit"]]
        .drop_duplicates("countrycode")
        .set_index("countrycode")
    )
    year_min, year_max = int(df["year"].min()), int(df["year"].max())
    log(f"economies: {len(economies)} | variables: {len(data_cols)} | "
        f"years: {year_min}-{year_max}")

    # Pre-coerce year to date32 (Dec-31 annual convention).
    df = df.copy()
    df["_obs_date"] = df["year"].astype(int).map(lambda y: dt.date(y, 12, 31))

    results = []
    grand_obs = 0
    files_written = 0
    for var in data_cols:
        definition, unit = VAR_DEFS[var]
        # one column -> drop rows with no value for this variable, keep all economies
        sub = df.loc[df[var].notna(), ["countrycode", "_obs_date", var]]
        if sub.empty:
            results.append({"variable": var, "status": "empty", "n_obs": 0,
                            "n_series": 0, "definition": definition})
            continue
        keys = (var + "|" + sub["countrycode"].astype(str)).tolist()
        dates = sub["_obs_date"].tolist()
        vals = pd.to_numeric(sub[var], errors="coerce").astype(float).tolist()
        n_series = sub["countrycode"].nunique()
        n_obs = len(keys)
        d_min = min(dates)
        d_max = max(dates)

        if not dry:
            tbl = pa.table({
                "series_key": pa.array(keys, type=pa.string()),
                "obs_date": pa.array(dates, type=pa.date32()),
                "value": pa.array(vals, type=pa.float64()),
            }, schema=SCHEMA)
            atomic_write_parquet(tbl, os.path.join(OUT, var + ".parquet"))
            files_written += 1

        grand_obs += n_obs
        results.append({
            "variable": var, "status": "ok", "n_obs": n_obs, "n_series": n_series,
            "n_economies": n_series, "start": str(d_min), "end": str(d_max),
            "unit": unit, "definition": definition,
        })
        log(f"  {var:9} economies={n_series:>4} obs={n_obs:>7,} "
            f"[{d_min.year}-{d_max.year}]"
            + ("" if not dry else "  (dry)"))

    total_series = sum(r["n_series"] for r in results)
    summary = {
        "source_id": SOURCE_ID,
        "license_id": LICENSE_ID,
        "pwt_version": PWT_VERSION,
        "doi": "10.34894/FABVLR",
        "homepage": "https://www.rug.nl/ggdc/productivity/pwt",
        "data_url": DATA_URL,
        "attribution": ("Source: Feenstra, Robert C., Robert Inklaar and Marcel P. "
                        "Timmer (2015), 'The Next Generation of the Penn World "
                        "Table', American Economic Review, 105(10), 3150-3182 -- "
                        "Penn World Table 11.0 (CC BY 4.0)"),
        "grouping": "one parquet per variable; series_key=<variable>|<ISO3>",
        "info_columns_excluded": sorted(INFO_COLS),
        "info_columns_note": ("i_* are categorical data-quality labels "
                              "(Benchmark/Extrapolated/Interpolated/Outlier/etc.), "
                              "not numeric series"),
        "economies_total": len(economies),
        "variables_total": len(data_cols),
        "year_min": year_min,
        "year_max": year_max,
        "series_written_total": total_series,
        "observations_written": grand_obs,
        "parquet_files_written_this_run": files_written,
        "parquet_files_on_disk_total": len([f for f in os.listdir(OUT) if f.endswith(".parquet")]),
        "generated": dt.datetime.now().isoformat(timespec="seconds"),
        "results": sorted(results, key=lambda r: r["variable"]),
    }
    if not dry:
        with open(os.path.join(OUT, "_ingest_summary.json"), "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)

    log("")
    log(f"{'DRY' if dry else 'DONE'}: {SOURCE_ID} PWT {PWT_VERSION}")
    log(f"  economies: {len(economies)} | variables: {len(data_cols)} | "
        f"years: {year_min}-{year_max}")
    log(f"  series (country x variable, where present): {total_series:,}")
    log(f"  observations: {grand_obs:,}")
    log(f"  parquet files on disk: {summary['parquet_files_on_disk_total']}")


if __name__ == "__main__":
    main()
