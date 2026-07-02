#!/usr/bin/env python3
"""Turn the World Bank WDI bulk download into Connector-shaped series + observations.

Reads WDICSV.csv directly out of data/raw/worldbank/WDI_CSV.zip (no unzip to disk).
That CSV is one row per (country, indicator) with a wide block of year columns. We melt
it into the library's long form:

  - one SeriesMeta per (Indicator Code, Country Code), id = "worldbank:<IndicatorCode>:<CountryCode>"
  - annual Observations, obs_date = Dec 31 of the year, blank/NaN cells skipped
  - license_id "cc-by-4.0" (World Bank WDI is CC BY 4.0)

Note: "Country Code" here follows the WDI file, which mixes ISO3 country codes with
aggregate/region codes (WLD, EUU, AFE, high-income groupings, ...). We keep them verbatim
so every published series round-trips back to a row in the source bulk.

This is a PROCESSING smoke-test: it builds the series objects in memory and prints
summary counts + one known data point. It deliberately does NOT touch data/catalog.db
or data/clean/ -- persistence is run_connector.py's job.

Run:  python jobs/process_worldbank_wdi.py
"""
from __future__ import annotations

import os
import sys
import zipfile
from datetime import date
from typing import Iterator

import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from connectors.base import SeriesMeta, Observation  # noqa: E402

SOURCE_ID = "worldbank_wdi"
LICENSE_ID = "cc-by-4.0"
ATTRIBUTION = "Source: World Bank, World Development Indicators (CC BY 4.0)"

ZIP_PATH = os.path.join(ROOT, "data", "raw", "worldbank", "WDI_CSV.zip")
MEMBER = "WDICSV.csv"

# The four leading metadata columns; everything else in the header is a year.
META_COLS = ["Country Name", "Country Code", "Indicator Name", "Indicator Code"]


def _read_wdi(zip_path: str = ZIP_PATH, member: str = MEMBER) -> pd.DataFrame:
    """Read WDICSV.csv straight from the zip. utf-8-sig strips the BOM on the
    first header cell; low_memory=False keeps mixed dtypes from triggering warnings."""
    if not os.path.exists(zip_path):
        raise FileNotFoundError(f"WDI bulk not found: {zip_path}")
    with zipfile.ZipFile(zip_path) as z:
        with z.open(member) as fh:
            df = pd.read_csv(fh, encoding="utf-8-sig", dtype=str, low_memory=False)
    missing = [c for c in META_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"{member} missing expected columns: {missing}; got {list(df.columns)[:6]}")
    return df


def _year_columns(df: pd.DataFrame) -> list[str]:
    """Year columns are exactly the 4-digit-numeric headers (1960.. through whatever
    the vintage ships). Detected, not hardcoded, so a newer/older bulk just works."""
    years = [c for c in df.columns if c not in META_COLS and str(c).strip().isdigit() and len(str(c).strip()) == 4]
    return sorted(years, key=lambda c: int(c))


def iter_series(df: pd.DataFrame) -> Iterator[tuple[SeriesMeta, list[Observation]]]:
    """Yield (SeriesMeta, [Observation, ...]) for each (indicator, country) row.

    Each CSV row already IS one series (country x indicator), so we walk rows and
    emit the non-blank year cells as annual observations dated Dec 31."""
    year_cols = _year_columns(df)
    # itertuples is the fast row walk; map header names to tuple positions once.
    col_idx = {name: i + 1 for i, name in enumerate(df.columns)}  # +1: index 0 is the row Index
    i_cname = col_idx["Country Name"]
    i_ccode = col_idx["Country Code"]
    i_iname = col_idx["Indicator Name"]
    i_icode = col_idx["Indicator Code"]
    year_pos = [(int(y), col_idx[y]) for y in year_cols]

    for row in df.itertuples(index=True, name=None):
        ind_code = row[i_icode]
        country_code = row[i_ccode]
        if not ind_code or not country_code:
            continue
        sid = f"worldbank:{ind_code}:{country_code}"

        obs: list[Observation] = []
        for yr, pos in year_pos:
            raw = row[pos]
            # skip blank cells: empty string, None, or NaN
            if raw is None:
                continue
            s = str(raw).strip()
            if s == "" or s.lower() == "nan":
                continue
            try:
                val = float(s)
            except ValueError:
                continue
            if val != val:  # NaN guard
                continue
            obs.append(Observation(sid, date(yr, 12, 31), val, version="clean"))

        if not obs:
            continue

        meta = SeriesMeta(
            series_id=sid,
            title=f"{row[i_iname]} - {country_code}",
            frequency="A",
            unit=None,
            geography=country_code,
            category="macro",
            license_id=LICENSE_ID,
            metadata={
                "indicator": ind_code,
                "indicator_name": row[i_iname],
                "country_name": row[i_cname],
                "country_code": country_code,
                "source": "World Bank WDI bulk (WDICSV.csv)",
            },
        )
        yield meta, obs


def main() -> int:
    df = _read_wdi()
    year_cols = _year_columns(df)
    n_indicators = df["Indicator Code"].nunique(dropna=True)
    n_countries = df["Country Code"].nunique(dropna=True)

    print(f"[worldbank_wdi] read {MEMBER} from {ZIP_PATH}")
    print(f"[worldbank_wdi] year columns: {year_cols[0]}..{year_cols[-1]} ({len(year_cols)} years)")
    print(f"[worldbank_wdi] indicators : {n_indicators:,}")
    print(f"[worldbank_wdi] countries  : {n_countries:,}")

    total_series = 0
    total_obs = 0
    sample_last_date = None
    sample_last_value = None
    for meta, obs in iter_series(df):
        total_series += 1
        total_obs += len(obs)
        if meta.series_id == "worldbank:NY.GDP.MKTP.CD:USA":
            last = max(obs, key=lambda o: o.obs_date)
            sample_last_date = last.obs_date
            sample_last_value = last.value

    print(f"[worldbank_wdi] total series      : {total_series:,}")
    print(f"[worldbank_wdi] total observations: {total_obs:,}")

    print("\n--- SAMPLE: worldbank:NY.GDP.MKTP.CD:USA (USA GDP, current US$) ---")
    if sample_last_value is not None:
        print(f"  last observation: {sample_last_date}  value={sample_last_value:,.0f}")
    else:
        print("  NOT FOUND (no USA / NY.GDP.MKTP.CD row with data)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
