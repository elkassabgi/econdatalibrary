"""Bank for International Settlements (BIS) connector -- central bank policy rates.

Source: BIS SDMX RESTful API v1 (https://stats.bis.org/api/v1/). No API key.
Dataflow WS_CBPOL ("Central bank policy rates") -- the interest rate that best
captures each monetary authority's policy intentions. DSD BIS:BIS_CBPOL(1.0) has
two dimensions, FREQ.REF_AREA (positions 1 & 2).

We pull the MONTHLY (end-of-period) frequency for all 49 reporting areas in a
single bulk CSV request (key "M." wildcards REF_AREA) -- one polite HTTP call for
the whole panel. Monthly end-of-period is the canonical clean series for an
academic library; the daily frequency captures the same rate decisions at a much
larger size, so we leave it out of the starter set. SDMX startPeriod gives cheap
incremental refreshes.

Licence: BIS terms allow non-commercial redistribution with attribution
(license_id "bis-attrib-nc", which is in core.licenses.RESERVABLE and flagged
non-commercial). Series id format: bis:WS_CBPOL:<REF_AREA>  (e.g. bis:WS_CBPOL:US).
"""
from __future__ import annotations

import csv
import datetime as dt
import io
import os
import sys
import time
from typing import Iterable, Optional

import requests

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
from connectors.base import Connector, SeriesMeta, Observation  # noqa: E402

# BIS SDMX v1: data/{agency,dataflow,version}/{key}. CSV via the SDMX-CSV accept header.
DATA_URL = "https://stats.bis.org/api/v1/data/BIS,WS_CBPOL,1.0/{key}"
ACCEPT_CSV = "application/vnd.sdmx.data+csv"
USER_AGENT = "Econ-Fin Data Library admin@hfdatalibrary.com"
UNIT = "Per cent per year"  # CL_BIS_UNIT code 368 (UNIT_MEASURE on every CBPOL obs)

# The 49 monthly reporting areas (REF_AREA -> name), resolved from CL_BIS_GL_REF_AREA.
# Kept here so discover() is stable offline; fetch() still prefers the live CSV title.
AREAS = {
    "AR": "Argentina", "AT": "Austria", "AU": "Australia", "BE": "Belgium",
    "BR": "Brazil", "CA": "Canada", "CH": "Switzerland", "CL": "Chile",
    "CN": "China", "CO": "Colombia", "CZ": "Czechia", "DE": "Germany",
    "DK": "Denmark", "ES": "Spain", "FR": "France", "GB": "United Kingdom",
    "GR": "Greece", "HK": "Hong Kong SAR", "HR": "Croatia", "HU": "Hungary",
    "ID": "Indonesia", "IL": "Israel", "IN": "India", "IS": "Iceland",
    "IT": "Italy", "JP": "Japan", "KR": "Korea", "KW": "Kuwait",
    "MA": "Morocco", "MK": "North Macedonia", "MX": "Mexico", "MY": "Malaysia",
    "NL": "Netherlands", "NO": "Norway", "NZ": "New Zealand", "PE": "Peru",
    "PH": "Philippines", "PL": "Poland", "PT": "Portugal", "RO": "Romania",
    "RS": "Serbia", "RU": "Russia", "SA": "Saudi Arabia", "SE": "Sweden",
    "TH": "Thailand", "TR": "Türkiye", "US": "United States",
    "XM": "Euro area", "ZA": "South Africa",
}


class BISConnector(Connector):
    source_id = "bis"
    name = "Bank for International Settlements"
    license_id = "bis-attrib-nc"
    schedule = "0 7 * * *"  # daily morning refresh; CBPOL updates around rate decisions
    attribution = "Source: Bank for International Settlements (BIS). Non-commercial use, attribution required."
    homepage = "https://www.bis.org"

    def _series_id(self, area: str) -> str:
        return f"bis:WS_CBPOL:{area}"

    def _meta(self, area: str, title: Optional[str] = None) -> SeriesMeta:
        nice = AREAS.get(area, area)
        return SeriesMeta(
            series_id=self._series_id(area),
            title=title or f"Central bank policy rate - {nice} (monthly, end of period)",
            frequency="M",
            unit=UNIT,
            geography=area,
            category="rates",
            license_id=self.license_id,
            metadata={"dataflow": "WS_CBPOL", "ref_area": area, "freq": "M",
                      "non_commercial": True},
        )

    def discover(self) -> list[SeriesMeta]:
        return [self._meta(area) for area in AREAS]

    def _get(self, key: str, since: Optional[dt.date]) -> str:
        params = {"detail": "full"}
        if since is not None:
            params["startPeriod"] = f"{since.year:04d}-{since.month:02d}"
        url = DATA_URL.format(key=key)
        headers = {"User-Agent": USER_AGENT, "Accept": ACCEPT_CSV}
        last_exc: Optional[Exception] = None
        for attempt in range(5):
            try:
                r = requests.get(url, params=params, headers=headers, timeout=120)
                if r.status_code == 404:
                    return ""  # SDMX "No Results Found" for an empty/too-recent window
                if r.status_code in (429, 500, 502, 503, 504):
                    raise requests.HTTPError(f"{r.status_code} {r.reason}", response=r)
                r.raise_for_status()
                return r.text
            except requests.RequestException as exc:
                last_exc = exc
                if attempt == 4:
                    break
                time.sleep(2 ** attempt)  # 1s, 2s, 4s, 8s backoff
        raise RuntimeError(f"BIS request failed for key {key!r}: {last_exc}")

    def fetch(self, since: Optional[dt.date] = None
              ) -> Iterable[tuple[SeriesMeta, list[Observation]]]:
        # One bulk request for the whole monthly panel (REF_AREA wildcarded).
        text = self._get("M.", since)
        if not text.strip():
            return

        by_area: dict[str, list[Observation]] = {}
        titles: dict[str, str] = {}
        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            if (row.get("FREQ") or "").strip() != "M":
                continue
            area = (row.get("REF_AREA") or "").strip()
            period = (row.get("TIME_PERIOD") or "").strip()  # "YYYY-MM"
            raw = (row.get("OBS_VALUE") or "").strip()
            if not area or not period or not raw:
                continue
            try:
                year_s, month_s = period.split("-")[:2]
                obs_date = dt.date(int(year_s), int(month_s), 1)
            except (ValueError, IndexError):
                continue  # skip rows whose period isn't a clean YYYY-MM
            try:
                value = float(raw)
            except (ValueError, TypeError):
                continue  # skip None / non-numeric
            sid = self._series_id(area)
            by_area.setdefault(area, []).append(
                Observation(sid, obs_date, value, version="clean"))
            t = (row.get("TITLE") or "").strip()
            if t and area not in titles:
                titles[area] = t

        for area, obs in by_area.items():
            obs.sort(key=lambda o: o.obs_date)
            yield self._meta(area, titles.get(area)), obs
