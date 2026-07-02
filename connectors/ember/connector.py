"""Ember -- global electricity data (generation, capacity, emissions, demand; CC BY 4.0).

Ember publishes its global electricity dataset as two long-format CSVs hosted on a
public Google Cloud Storage bucket (the same bucket that serves the methodology PDF
linked from ember-energy.org/data). There is no key required and no incremental API
for the bulk files, so we download the relevant CSV in full and filter locally:

  yearly : https://storage.googleapis.com/emb-prod-bkt-publicdata/public-downloads/
           yearly_full_release_long_format.csv   (~49 MB, 228 areas, 2000-present)
  monthly: https://storage.googleapis.com/emb-prod-bkt-publicdata/public-downloads/
           monthly_full_release_long_format.csv  (~69 MB, 98 areas, 1999-present)

(The ember-energy.org HTML pages sit behind a bot wall that 403s automated clients;
the GCS objects themselves are openly served, which is why we read them directly.)

Both files share one schema -- columns:
  Area, ISO 3 code, Year|Date, Area type, Continent, Ember region, EU, OECD, G20,
  G7, ASEAN, Category, Subcategory, Variable, Unit, Value, YoY absolute change,
  YoY % change

A single physical "series" is therefore the slice fixing
(Area, Category, Subcategory, Variable, Unit) and varying the time column. We pin a
curated set of high-value (metric x geography) slices. Countries are matched by their
ISO 3 code (robust to label edits); Ember's aggregate regions (World, EU) carry a
blank ISO 3 code and are matched by Area name and assigned a synthetic geography code.

Series id format:
  ember:<Y|M>:<metric_id>:<GEO>      e.g.  ember:Y:gen_total_twh:WORLD
                                            ember:M:gen_solar_twh:USA

Starter set: 6 yearly metrics-geographies wide (42 series) + 18 monthly = 60 series.
The matcher is generic, so extending either list is just adding tuples.
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
BUCKET = "https://storage.googleapis.com/emb-prod-bkt-publicdata/public-downloads"
YEARLY_URL = f"{BUCKET}/yearly_full_release_long_format.csv"
MONTHLY_URL = f"{BUCKET}/monthly_full_release_long_format.csv"

# --- curated metrics -------------------------------------------------------
# metric_id -> (Category, Subcategory, Variable, Unit, human title fragment)
# The 4-tuple after metric_id is the exact match key into the long CSV.
YEARLY_METRICS = {
    "gen_total_twh":          ("Electricity generation", "Total", "Total Generation", "TWh",
                               "Total electricity generation"),
    "gen_share_clean_pct":    ("Electricity generation", "Aggregate fuel", "Clean", "%",
                               "Clean generation share"),
    "gen_share_fossil_pct":   ("Electricity generation", "Aggregate fuel", "Fossil", "%",
                               "Fossil generation share"),
    "gen_solar_twh":          ("Electricity generation", "Fuel", "Solar", "TWh",
                               "Solar generation"),
    "gen_wind_twh":           ("Electricity generation", "Fuel", "Wind", "TWh",
                               "Wind generation"),
    "emissions_total_mtco2":  ("Power sector emissions", "Total", "Total emissions", "mtCO2",
                               "Power-sector CO2 emissions"),
    "co2_intensity_gco2_kwh": ("Power sector emissions", "CO2 intensity", "CO2 intensity",
                               "gCO2/kWh", "Power-sector CO2 intensity"),
}

MONTHLY_METRICS = {
    "gen_total_twh":          ("Electricity generation", "Total", "Total Generation", "TWh",
                               "Total electricity generation"),
    "gen_share_clean_pct":    ("Electricity generation", "Aggregate fuel", "Clean", "%",
                               "Clean generation share"),
    "gen_solar_twh":          ("Electricity generation", "Fuel", "Solar", "TWh",
                               "Solar generation"),
    "gen_wind_twh":           ("Electricity generation", "Fuel", "Wind", "TWh",
                               "Wind generation"),
    "demand_twh":             ("Electricity demand", "Demand", "Demand", "TWh",
                               "Electricity demand"),
    "co2_intensity_gco2_kwh": ("Power sector emissions", "CO2 intensity", "CO2 intensity",
                               "gCO2/kWh", "Power-sector CO2 intensity"),
}

# --- curated geographies ---------------------------------------------------
# geo_code -> (kind, match_value, display_name)
#   kind "iso"    -> match rows where ISO 3 code == match_value
#   kind "region" -> match rows where Area == match_value AND ISO 3 code is blank
#                    (Ember's aggregate regions have no ISO 3 code)
YEARLY_GEO = {
    "WORLD": ("region", "World", "World"),
    "EU":    ("region", "EU", "European Union"),
    "USA":   ("iso", "USA", "United States"),
    "CHN":   ("iso", "CHN", "China"),
    "IND":   ("iso", "IND", "India"),
    "DEU":   ("iso", "DEU", "Germany"),
}

MONTHLY_GEO = {
    "WORLD": ("region", "World", "World"),
    "EU":    ("region", "EU", "European Union"),
    "USA":   ("iso", "USA", "United States"),
}


class EmberConnector(Connector):
    source_id = "ember"
    name = "Ember (electricity)"
    license_id = "cc-by-4.0"
    # Ember refreshes twice a month (first and third week); check weekly on Mondays.
    schedule = "0 7 * * 1"
    attribution = "Source: Ember -- Yearly/Monthly Electricity Data (CC BY 4.0)"
    homepage = "https://ember-energy.org"

    # ---- helpers ----------------------------------------------------------

    def _series_id(self, freq: str, metric_id: str, geo: str) -> str:
        return f"{self.source_id}:{freq}:{metric_id}:{geo}"

    def _meta(self, freq: str, metric_id: str, geo: str,
              metrics: dict, geos: dict) -> SeriesMeta:
        cat, sub, var, unit, title_frag = metrics[metric_id]
        _, match_value, display = geos[geo]
        geography = geo if geos[geo][0] == "iso" else display
        cadence = "Annual" if freq == "A" else "Monthly"
        return SeriesMeta(
            series_id=self._series_id(freq, metric_id, geo),
            title=f"{title_frag} - {display} ({cadence}, {unit})",
            frequency=freq,
            unit=unit,
            geography=geography,
            category="energy",
            license_id=self.license_id,
            metadata={
                "provider": "Ember",
                "dataset": "yearly_full_release" if freq == "A" else "monthly_full_release",
                "area": display,
                "ember_category": cat,
                "ember_subcategory": sub,
                "ember_variable": var,
            },
        )

    def discover(self) -> list[SeriesMeta]:
        """List the curated series (offline; no download)."""
        out: list[SeriesMeta] = []
        for metric_id in YEARLY_METRICS:
            for geo in YEARLY_GEO:
                out.append(self._meta("A", metric_id, geo, YEARLY_METRICS, YEARLY_GEO))
        for metric_id in MONTHLY_METRICS:
            for geo in MONTHLY_GEO:
                out.append(self._meta("M", metric_id, geo, MONTHLY_METRICS, MONTHLY_GEO))
        return out

    # ---- networking -------------------------------------------------------

    def _download_csv(self, url: str) -> pd.DataFrame:
        """Fetch a long-format CSV with retries and return it as a DataFrame."""
        last: Optional[Exception] = None
        for attempt in range(4):
            try:
                r = requests.get(url, headers={"User-Agent": UA}, timeout=180)
                r.raise_for_status()
                return pd.read_csv(io.BytesIO(r.content))
            except Exception as exc:  # network / parse / transient 5xx
                last = exc
                if attempt < 3:
                    time.sleep(2 ** attempt)  # 1s, 2s, 4s backoff
        raise RuntimeError(f"Ember download failed after retries: {url}: {last}")

    # ---- parsing ----------------------------------------------------------

    @staticmethod
    def _select(df: pd.DataFrame, kind: str, match_value: str,
                cat: str, sub: str, var: str, unit: str) -> pd.DataFrame:
        if kind == "iso":
            sel = df[df["ISO 3 code"] == match_value]
        else:  # region: blank ISO 3 code, matched by Area name
            sel = df[(df["Area"] == match_value) & (df["ISO 3 code"].isna())]
        return sel[(sel["Category"] == cat) & (sel["Subcategory"] == sub)
                   & (sel["Variable"] == var) & (sel["Unit"] == unit)]

    def _emit(self, df: pd.DataFrame, freq: str, datecol: str,
              metrics: dict, geos: dict, since: Optional[dt.date]):
        for metric_id, (cat, sub, var, unit, _frag) in metrics.items():
            for geo, (kind, match_value, _display) in geos.items():
                sub_df = self._select(df, kind, match_value, cat, sub, var, unit)
                if sub_df.empty:
                    continue
                sid = self._series_id(freq, metric_id, geo)
                obs: list[Observation] = []
                for raw_date, raw_val in zip(sub_df[datecol], sub_df["Value"]):
                    if pd.isna(raw_val) or pd.isna(raw_date):
                        continue  # skip missing values
                    try:
                        val = float(raw_val)
                    except (ValueError, TypeError):
                        continue  # skip non-numeric values
                    d = self._parse_date(raw_date, freq)
                    if d is None:
                        continue
                    if since is not None and d < since:
                        continue
                    obs.append(Observation(sid, d, val, version="clean"))
                if obs:
                    obs.sort(key=lambda o: o.obs_date)
                    yield self._meta(freq, metric_id, geo, metrics, geos), obs

    @staticmethod
    def _parse_date(raw, freq: str) -> Optional[dt.date]:
        """Yearly -> Dec 31 of the year; Monthly -> first day of the month."""
        if freq == "A":
            try:
                return dt.date(int(raw), 12, 31)
            except (ValueError, TypeError):
                return None
        # monthly: values look like '2018-01-01'
        try:
            ts = pd.to_datetime(raw)
            return dt.date(ts.year, ts.month, 1)
        except (ValueError, TypeError):
            return None

    def fetch(self, since: Optional[dt.date] = None):
        yearly = self._download_csv(YEARLY_URL)
        yield from self._emit(yearly, "A", "Year", YEARLY_METRICS, YEARLY_GEO, since)

        monthly = self._download_csv(MONTHLY_URL)
        yield from self._emit(monthly, "M", "Date", MONTHLY_METRICS, MONTHLY_GEO, since)
