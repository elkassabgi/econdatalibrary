"""Australian Bureau of Statistics (ABS) connector -- macro indicators; CC BY 4.0.

ABS SDMX Data API (https://data.api.abs.gov.au/rest). No API key, freely accessible.
We request one fully-specified series per call (a dotted SDMX data key) and ask for
JSON with `dimensionAtObservation=AllDimensions`, which returns a compact
`dataSets[0].observations` map keyed by colon-separated positional indices into each
dimension's `values` list (TIME_PERIOD included). The first element of each value
array is the numeric observation; the rest are attribute indices we ignore.

Series id format: abs:<FLOW>:<DATAKEY> (e.g. "abs:CPI:1.10001.10.50.Q").

Starter set = the headline national indicators an economist reaches for first:
CPI (quarterly index + quarterly change, plus the monthly CPI indicator and its annual
rate), Labour Force (unemployment / participation / employment, seasonally adjusted),
National Accounts key aggregates (real GDP level + growth, GDP per capita, household
saving ratio), the Wage Price Index, Average Weekly Earnings, and Retail Trade turnover.
All are Australia-wide. The full state/industry breakdowns can be added later by
expanding SERIES -- the fetch/parse path is generic over any ABS dataflow.

ABS dataflows publish only certain (measure x adjustment) combinations, so some keys
404 (e.g. there is no stored "annual % change" for headline quarterly CPI -- we get the
annual rate from the monthly CPI indicator instead). fetch() treats a 404 / empty body
as "this series has no data right now" and skips it rather than aborting the whole run.
"""
from __future__ import annotations

import datetime as dt
import os
import sys
import time
from calendar import monthrange
from typing import Optional

import requests

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
from connectors.base import Connector, SeriesMeta, Observation  # noqa: E402

UA = "Econ-Fin Data Library admin@hfdatalibrary.com"

# Per-series definitions. Key = series_id suffix "<FLOW>:<DATAKEY>".
# value = (title, frequency, unit, geography, category)
# frequency uses the contract's vocabulary ('M','Q','A','irregular'); ABS half-yearly
# (AWE) has no exact code there, so it is recorded as 'irregular' (semi-annual).
SERIES: dict[str, tuple[str, str, Optional[str], str, str]] = {
    # ---- Consumer Price Index (quarterly, all groups, Australia, original) ----
    "CPI:1.10001.10.50.Q": (
        "CPI All groups, index numbers (Australia, quarterly)", "Q", "Index", "AU", "prices"),
    "CPI:2.10001.10.50.Q": (
        "CPI All groups, % change from previous quarter (Australia)", "Q", "Percent", "AU", "prices"),
    # ---- Monthly CPI indicator (weighted average of eight capital cities) ----
    "CPI_M:1.10001.10.50.M": (
        "Monthly CPI indicator, All groups, index numbers (Australia)", "M", "Index", "AU", "prices"),
    "CPI_M:3.10001.10.50.M": (
        "Monthly CPI indicator, All groups, annual % change (Australia)", "M", "Percent", "AU", "prices"),
    # ---- Labour Force (monthly, persons, all ages, seasonally adjusted, Australia) ----
    "LF:M13.3.1599.20.AUS.M": (
        "Unemployment rate (persons, SA, Australia)", "M", "Percent", "AU", "labour"),
    "LF:M12.3.1599.20.AUS.M": (
        "Participation rate (persons, SA, Australia)", "M", "Percent", "AU", "labour"),
    "LF:M16.3.1599.20.AUS.M": (
        "Employment to population ratio (persons, SA, Australia)", "M", "Percent", "AU", "labour"),
    "LF:M3.3.1599.20.AUS.M": (
        "Employed persons (persons, SA, Australia)", "M", "Thousands", "AU", "labour"),
    "LF:M1.3.1599.20.AUS.M": (
        "Employed full-time (persons, SA, Australia)", "M", "Thousands", "AU", "labour"),
    "LF:M9.3.1599.20.AUS.M": (
        "Labour force (persons, SA, Australia)", "M", "Thousands", "AU", "labour"),
    # ---- National Accounts key aggregates (quarterly, Australia) ----
    "ANA_AGG:M1.GPM.20.AUS.Q": (
        "GDP, chain volume measures, seasonally adjusted (Australia)", "Q", "AUD millions", "AU", "national-accounts"),
    "ANA_AGG:M2.GPM.20.AUS.Q": (
        "GDP, chain volume measures, % change from previous quarter (Australia)", "Q", "Percent", "AU", "national-accounts"),
    "ANA_AGG:M1.GPM_PCA.20.AUS.Q": (
        "GDP per capita, chain volume measures, SA (Australia)", "Q", "AUD", "AU", "national-accounts"),
    "ANA_AGG:M7.HSR.20.AUS.Q": (
        "Household saving ratio, seasonally adjusted (Australia)", "Q", "Percent", "AU", "national-accounts"),
    # ---- Wage Price Index (quarterly, total hourly incl bonuses, all industries, original) ----
    "WPI:1.THRPIB.7.TOT.10.AUS.Q": (
        "Wage Price Index, total hourly rates incl bonuses, index (Australia)", "Q", "Index", "AU", "labour"),
    "WPI:3.THRPIB.7.TOT.10.AUS.Q": (
        "Wage Price Index, total hourly rates incl bonuses, annual % change (Australia)", "Q", "Percent", "AU", "labour"),
    # ---- Average Weekly Earnings (half-yearly, all employees total, persons, all industries) ----
    "AWE:1.1.3.7.TOT.10.AUS.S": (
        "Average weekly total earnings, all employees (persons, Australia)", "irregular", "AUD/week", "AU", "labour"),
    # ---- Retail Trade (monthly turnover, total, current prices, SA, Australia) ----
    "RT:M1.20.20.AUS.M": (
        "Retail turnover, total, current prices, SA (Australia)", "M", "AUD millions", "AU", "trade"),
}

BASE = "https://data.api.abs.gov.au/rest/data"


def _period_to_date(period: str) -> Optional[dt.date]:
    """Map an SDMX TIME_PERIOD id to a period-END calendar date.

    Handles 'YYYY' (annual), 'YYYY-MM' (monthly), 'YYYY-Qn' (quarterly) and
    'YYYY-Sn' (half-year / semester). Returns None if unrecognised.
    """
    period = period.strip()
    try:
        if "-Q" in period:
            y, q = period.split("-Q")
            q = int(q)
            month = q * 3                      # 1->3, 2->6, 3->9, 4->12
            return dt.date(int(y), month, monthrange(int(y), month)[1])
        if "-S" in period:
            y, s = period.split("-S")
            month = 6 if int(s) == 1 else 12   # semester end: Jun / Dec
            return dt.date(int(y), month, monthrange(int(y), month)[1])
        if "-" in period:
            y, m = period.split("-")[:2]
            y, m = int(y), int(m)
            return dt.date(y, m, monthrange(y, m)[1])
        if len(period) == 4 and period.isdigit():
            return dt.date(int(period), 12, 31)
    except (ValueError, IndexError):
        return None
    return None


def _since_to_startperiod(since: dt.date, freq: str) -> str:
    """Render `since` as an ABS startPeriod string appropriate to the frequency."""
    if freq == "Q":
        return f"{since.year}-Q{(since.month - 1) // 3 + 1}"
    if freq == "M":
        return f"{since.year}-{since.month:02d}"
    if freq == "irregular":  # half-yearly
        return f"{since.year}-S{1 if since.month <= 6 else 2}"
    return str(since.year)


class ABSConnector(Connector):
    source_id = "abs"
    name = "Australian Bureau of Statistics"
    license_id = "cc-by-4.0"
    schedule = "0 7 * * *"          # daily; ABS releases land on weekday mornings
    attribution = "Source: Australian Bureau of Statistics (ABS), CC BY 4.0"
    homepage = "https://www.abs.gov.au"

    def __init__(self) -> None:
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": UA})

    def discover(self) -> list[SeriesMeta]:
        out = []
        for suffix, (title, freq, unit, geo, cat) in SERIES.items():
            flow, key = suffix.split(":", 1)
            out.append(SeriesMeta(
                f"abs:{suffix}", title, freq, unit, geo, cat, self.license_id,
                {"dataflow": flow, "datakey": key, "agency": "ABS"},
            ))
        return out

    def _get(self, url: str, params: dict) -> Optional[dict]:
        """GET with retries. Returns parsed JSON, or None for 'no data' (404)."""
        last_exc: Optional[Exception] = None
        for attempt in range(4):
            try:
                r = self._session.get(url, params=params, timeout=90)
            except requests.RequestException as exc:           # transient network
                last_exc = exc
                time.sleep(2 ** attempt)
                continue
            if r.status_code == 404:
                return None                                    # series/combination absent
            if r.status_code in (429, 500, 502, 503, 504):     # throttle / server hiccup
                last_exc = RuntimeError(f"HTTP {r.status_code}")
                time.sleep(2 ** attempt + 1)
                continue
            r.raise_for_status()
            if not r.content:                                  # empty body == no data
                return None
            try:
                return r.json()
            except ValueError:
                return None
        if last_exc:
            raise last_exc
        return None

    @staticmethod
    def _parse(payload: dict, sid: str) -> list[Observation]:
        """Decode AllDimensions jsondata into clean Observations for one series."""
        data = payload.get("data", {})
        structures = data.get("structures") or []
        datasets = data.get("dataSets") or []
        if not structures or not datasets:
            return []
        dims = structures[0]["dimensions"]["observation"]
        # positional index of the time dimension + its value list
        time_pos = next((i for i, d in enumerate(dims) if d["id"] == "TIME_PERIOD"), None)
        if time_pos is None:
            return []
        time_vals = dims[time_pos]["values"]
        obs_map = datasets[0].get("observations", {})
        out: list[Observation] = []
        for key, arr in obs_map.items():
            if not arr or arr[0] is None:
                continue
            try:
                value = float(arr[0])
            except (TypeError, ValueError):
                continue                                       # skip non-numeric
            parts = key.split(":")
            try:
                t_idx = int(parts[time_pos])
                period = time_vals[t_idx]["id"]
            except (IndexError, ValueError, KeyError):
                continue
            obs_date = _period_to_date(period)
            if obs_date is None:
                continue
            out.append(Observation(sid, obs_date, value, version="clean"))
        out.sort(key=lambda o: o.obs_date)
        return out

    def fetch(self, since: Optional[dt.date] = None):
        for suffix, (title, freq, unit, geo, cat) in SERIES.items():
            flow, key = suffix.split(":", 1)
            sid = f"abs:{suffix}"
            params = {"format": "jsondata", "dimensionAtObservation": "AllDimensions"}
            if since is not None:
                params["startPeriod"] = _since_to_startperiod(since, freq)
            payload = self._get(f"{BASE}/{flow}/{key}", params)
            if payload is None:
                continue                                       # 404 / empty -> skip series
            obs = self._parse(payload, sid)
            if not obs:
                continue
            meta = SeriesMeta(
                sid, title, freq, unit, geo, cat, self.license_id,
                {"dataflow": flow, "datakey": key, "agency": "ABS"},
            )
            yield meta, obs
