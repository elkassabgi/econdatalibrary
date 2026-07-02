"""ILOSTAT connector (international labour statistics; CC BY 4.0).

ILOSTAT is the ILO's central statistics database. We pull from its public query
API (the same backend the Rilostat R package and the SDMX/bulk facilities sit on):

    https://rplumber.ilo.org/data/indicator/?id=<INDICATOR_A>&ref_area=<ISO3+...>
        &sex=<SEX>&classif1=<CLASSIF>&format=.csv

The `_A` suffix on an indicator code selects the ANNUAL collection. Each indicator
carries dimensions (sex, age-band, etc.); we fix sex=Total and one canonical age band
per indicator so every (indicator, breakdown, country) pair is a clean univariate
annual series with exactly one observation per year (verified: no duplicate-source rows
once sex+classif1 are pinned). No API key required.

Series id format: ilostat:<INDICATOR>:<CLASSIF1>:<ISO3>
  e.g. ilostat:UNE_DEAP_SEX_AGE_RT:AGE_YTHADULT_YGE15:USA  (unemployment rate, 15+, USA)

Starter set = 5 high-value labour indicators x ~13 major economies (G7 + BRICS + a few
more). Countries that don't report a given series to the ILO simply produce no series,
so the connector degrades gracefully rather than crashing.
"""
from __future__ import annotations

import csv
import datetime as dt
import io
import os
import sys
import time
from typing import Optional

import requests

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
from connectors.base import Connector, SeriesMeta, Observation  # noqa: E402

API = "https://rplumber.ilo.org/data/indicator/"
HEADERS = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

# Canonical breakdown pinned for every series: sex = Total.
SEX_TOTAL = "SEX_T"

# Starter indicators. Each entry pins one age band (classif1) so the result is a single
# univariate series per country. (indicator_code, classif1, title, unit, frequency).
# NB: indicator_code here is WITHOUT the "_A" suffix (added when querying the annual set);
#     it is what we store in the series id and metadata.
INDICATORS = [
    ("UNE_DEAP_SEX_AGE_RT", "AGE_YTHADULT_YGE15",
     "Unemployment rate, aged 15+ (Total)", "%", "A"),
    ("UNE_DEAP_SEX_AGE_RT", "AGE_YTHADULT_Y15-24",
     "Youth unemployment rate, aged 15-24 (Total)", "%", "A"),
    ("EMP_DWAP_SEX_AGE_RT", "AGE_YTHADULT_YGE15",
     "Employment-to-population ratio, aged 15+ (Total)", "%", "A"),
    ("EAP_DWAP_SEX_AGE_RT", "AGE_YTHADULT_YGE15",
     "Labour force participation rate, aged 15+ (Total)", "%", "A"),
    ("EMP_TEMP_SEX_AGE_NB", "AGE_YTHADULT_YGE15",
     "Employment, aged 15+ (Total, thousands)", "thousands", "A"),
]

# Major economies (ISO3). The API takes "+"-joined codes in one request per indicator.
# Countries with no data for a given indicator are silently absent from the response.
COUNTRIES = [
    "USA", "CAN", "MEX", "BRA", "GBR", "FRA", "DEU", "ITA",
    "RUS", "CHN", "IND", "JPN", "KOR", "IDN", "ZAF", "AUS",
]

# Stable short geography names for titles / metadata (avoids an extra metadata call).
COUNTRY_NAME = {
    "USA": "United States", "CAN": "Canada", "MEX": "Mexico", "BRA": "Brazil",
    "GBR": "United Kingdom", "FRA": "France", "DEU": "Germany", "ITA": "Italy",
    "RUS": "Russian Federation", "CHN": "China", "IND": "India", "JPN": "Japan",
    "KOR": "Korea, Rep.", "IDN": "Indonesia", "ZAF": "South Africa", "AUS": "Australia",
}


def _sid(indicator: str, classif1: str, iso3: str) -> str:
    return f"ilostat:{indicator}:{classif1}:{iso3}"


class ILOStatConnector(Connector):
    source_id = "ilostat"
    name = "ILOSTAT"
    license_id = "cc-by-4.0"
    schedule = "0 6 * * 1"  # weekly; ILOSTAT refreshes annually/irregularly
    attribution = "Source: International Labour Organization, ILOSTAT (CC BY 4.0)"
    homepage = "https://ilostat.ilo.org"

    def _meta(self, indicator, classif1, title, unit, freq, iso3) -> SeriesMeta:
        return SeriesMeta(
            _sid(indicator, classif1, iso3),
            f"{title} - {COUNTRY_NAME.get(iso3, iso3)}",
            freq, unit, iso3, "labour", self.license_id,
            {"indicator": indicator, "sex": SEX_TOTAL, "classif1": classif1,
             "provider": "ILOSTAT"},
        )

    def discover(self) -> list[SeriesMeta]:
        out: list[SeriesMeta] = []
        for indicator, classif1, title, unit, freq in INDICATORS:
            for iso3 in COUNTRIES:
                out.append(self._meta(indicator, classif1, title, unit, freq, iso3))
        return out

    def _get(self, params: dict) -> str:
        """GET the query API as CSV text, with polite retries."""
        last_exc: Optional[Exception] = None
        for attempt in range(4):
            try:
                r = requests.get(API, params=params, headers=HEADERS, timeout=120)
                if r.status_code == 200:
                    return r.text
                # 429 / 5xx -> back off and retry; other 4xx -> give up on this indicator
                if r.status_code not in (429, 500, 502, 503, 504):
                    r.raise_for_status()
                last_exc = RuntimeError(f"HTTP {r.status_code}")
            except requests.RequestException as e:
                last_exc = e
            time.sleep(2 * (attempt + 1))
        raise RuntimeError(f"ILOSTAT request failed for {params.get('id')}: {last_exc}")

    @staticmethod
    def _rows(csv_text: str):
        # API returns UTF-8 with a BOM; utf-8-sig strips it so the first header is clean.
        text = csv_text.encode("utf-8").decode("utf-8-sig") if csv_text else ""
        return csv.DictReader(io.StringIO(text))

    def fetch(self, since: Optional[dt.date] = None):
        timefrom = str(since.year) if since else None
        for indicator, classif1, title, unit, freq in INDICATORS:
            params = {
                "id": f"{indicator}_A",          # annual collection
                "ref_area": "+".join(COUNTRIES),
                "sex": SEX_TOTAL,
                "classif1": classif1,
                "format": ".csv",
            }
            if timefrom:
                params["timefrom"] = timefrom

            csv_text = self._get(params)

            # Bucket rows into one observation list per country.
            by_iso: dict[str, list[Observation]] = {}
            for row in self._rows(csv_text):
                iso3 = (row.get("ref_area") or "").strip()
                yr = (row.get("time") or "").strip()
                raw = (row.get("obs_value") or "").strip()
                if not iso3 or not yr.isdigit() or raw == "":
                    continue
                try:
                    val = float(raw)
                except (ValueError, TypeError):
                    continue  # skip non-numeric / suppressed values
                sid = _sid(indicator, classif1, iso3)
                by_iso.setdefault(iso3, []).append(
                    Observation(sid, dt.date(int(yr), 12, 31), val, version="clean"))

            for iso3, obs in by_iso.items():
                if not obs:
                    continue
                obs.sort(key=lambda o: o.obs_date)
                yield self._meta(indicator, classif1, title, unit, freq, iso3), obs

            time.sleep(0.5)  # be polite between indicator pulls
