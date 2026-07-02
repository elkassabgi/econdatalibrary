"""Zillow Research connector (housing: home values + rents; re-serveable).

Zillow publishes its research indices as public, key-free wide CSVs on
files.zillowstatic.com -- one row per geography (RegionID/RegionName/...) and one
column per month. We pull two flagship monthly series families:

  * ZHVI -- Zillow Home Value Index (smoothed, seasonally adjusted; the typical
    home value, 35th-65th percentile, all single-family + condo/co-op homes).
  * ZORI -- Zillow Observed Rent Index (smoothed; the typical asking rent across
    all home types).

Starter set: the US national figure plus the largest metros by Zillow's SizeRank,
for each index. Quality over completeness -- the long tail of ~900 metros / ZIPs /
counties can be turned on later by widening TOP_N or adding geographies.

Series id format: zillow:<index>:<RegionID>     e.g. zillow:zhvi:102001 (US),
                                                     zillow:zori:394913 (New York, NY).

License: zillow-research (re-serveable). Attribution MUST read exactly
"Data Provided by Zillow Group" per Zillow's terms of use.
"""
from __future__ import annotations

import datetime as dt
import io
import os
import sys
import time
from typing import Optional

import pandas as pd
import requests

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
from connectors.base import Connector, SeriesMeta, Observation  # noqa: E402

UA = "Econ-Fin Data Library admin@hfdatalibrary.com"
BASE = "https://files.zillowstatic.com/research/public_csvs"

# Identifier (non-date) columns Zillow puts at the left of every research CSV.
ID_COLS = ["RegionID", "SizeRank", "RegionName", "RegionType", "StateName"]

# How many metros (by SizeRank) to carry per index, on top of the US national row.
TOP_N = 25

# Index family -> (download URL, human title, unit, frequency).
# ZHVI = smoothed, seasonally adjusted typical home value (uc_sfrcondo, 33-67 tier).
# ZORI = smoothed typical observed rent (uc_sfrcondomfr).
INDICES = {
    "zhvi": {
        "url": f"{BASE}/zhvi/Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv",
        "title": "Zillow Home Value Index (ZHVI), all homes, smoothed & seasonally adjusted",
        "short": "ZHVI",
        "unit": "USD",
        "category": "housing",
    },
    "zori": {
        "url": f"{BASE}/zori/Metro_zori_uc_sfrcondomfr_sm_month.csv",
        "title": "Zillow Observed Rent Index (ZORI), all homes, smoothed",
        "short": "ZORI",
        "unit": "USD/month",
        "category": "housing",
    },
}


class ZillowConnector(Connector):
    source_id = "zillow"
    name = "Zillow Research"
    license_id = "zillow-research"
    schedule = "0 7 * * 3"          # weekly; Zillow refreshes research data ~monthly (mid-month)
    attribution = "Data Provided by Zillow Group"
    homepage = "https://www.zillow.com/research/data/"

    # ---- networking ------------------------------------------------------
    def _get_csv(self, url: str) -> pd.DataFrame:
        """Download a Zillow wide CSV with a polite UA, retries, and backoff."""
        last_err: Optional[Exception] = None
        for attempt in range(4):
            try:
                r = requests.get(url, headers={"User-Agent": UA}, timeout=120)
                r.raise_for_status()
                return pd.read_csv(io.StringIO(r.text))
            except Exception as e:          # noqa: BLE001 -- retry any transient failure
                last_err = e
                time.sleep(2 * (attempt + 1))
        raise RuntimeError(f"Zillow download failed after retries: {url} ({last_err})")

    # ---- helpers ---------------------------------------------------------
    @staticmethod
    def _geography(region_type: str, state: Optional[str]) -> Optional[str]:
        if region_type == "country":
            return "US"
        if isinstance(state, str) and state.strip():
            return f"US-{state.strip()}"
        return "US"

    @staticmethod
    def _series_id(index: str, region_id) -> str:
        return f"zillow:{index}:{int(region_id)}"

    def _selected_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """US national row + the largest TOP_N metros, by SizeRank."""
        df = df.sort_values("SizeRank")
        national = df[df["RegionType"] == "country"]
        metros = df[df["RegionType"] == "msa"].head(TOP_N)
        return pd.concat([national, metros])

    def _meta_for(self, index: str, row) -> SeriesMeta:
        spec = INDICES[index]
        region = str(row["RegionName"])
        sid = self._series_id(index, row["RegionID"])
        title = f"{spec['short']} - {region}"
        geo = self._geography(str(row["RegionType"]), row.get("StateName"))
        return SeriesMeta(
            series_id=sid,
            title=title,
            frequency="M",
            unit=spec["unit"],
            geography=geo,
            category=spec["category"],
            license_id=self.license_id,
            metadata={
                "index": spec["short"],
                "region_id": int(row["RegionID"]),
                "region_name": region,
                "region_type": str(row["RegionType"]),
                "state": (str(row["StateName"]) if isinstance(row.get("StateName"), str) else None),
                "size_rank": int(row["SizeRank"]),
                "source_url": spec["url"],
            },
        )

    # ---- contract --------------------------------------------------------
    def discover(self) -> list[SeriesMeta]:
        out: list[SeriesMeta] = []
        for index, spec in INDICES.items():
            df = self._get_csv(spec["url"])
            for _, row in self._selected_rows(df).iterrows():
                out.append(self._meta_for(index, row))
        return out

    def fetch(self, since: Optional[dt.date] = None):
        for index in INDICES:
            df = self._get_csv(INDICES[index]["url"])
            date_cols = [c for c in df.columns if c not in ID_COLS]
            # Parse the wide date headers once (YYYY-MM-DD month-end labels).
            parsed = []
            for c in date_cols:
                try:
                    parsed.append((c, dt.date.fromisoformat(str(c))))
                except ValueError:
                    continue  # ignore any non-date column defensively
            for _, row in self._selected_rows(df).iterrows():
                meta = self._meta_for(index, row)
                obs: list[Observation] = []
                for col, d in parsed:
                    if since is not None and d < since:
                        continue
                    val = row[col]
                    if pd.isna(val):
                        continue
                    try:
                        fval = float(val)
                    except (TypeError, ValueError):
                        continue
                    obs.append(Observation(meta.series_id, d, fval, version="clean"))
                if obs:
                    yield meta, obs
