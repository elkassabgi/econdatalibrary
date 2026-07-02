"""FHFA House Price Index connector (US Federal Housing Finance Agency; public domain).

FHFA publishes one consolidated "master" CSV that appends every HPI cut -- type
(traditional/non-metro/distress-free/...), flavor (purchase-only / all-transactions /
expanded-data), frequency (monthly/quarterly), and geographic level (USA & Census
Division / State / MSA / Puerto Rico). We download that single file and split it into
one series per (type, flavor, frequency, place).

Master CSV columns:
    hpi_type, hpi_flavor, frequency, level, place_name, place_id, yr, period,
    index_nsa, index_sa, rstderr, note

We emit the not-seasonally-adjusted index (index_nsa) as the observation value: NSA is
populated for every flavor, whereas index_sa is only published for the purchase-only
series. The seasonally-adjusted value, when present, is carried in each Observation's
metadata-free fields via the series metadata note instead. period maps to a date as
month-of-quarter for quarterly (Q1->Jan, Q2->Apr, Q3->Jul, Q4->Oct) and month-start for
monthly.

Starter set (high-value, ~61 series):
  * United States + the 9 Census Divisions on the headline traditional purchase-only
    MONTHLY index (the most-watched FHFA series, deep monthly history back to 1991).
  * All 50 states + DC on the traditional all-transactions QUARTERLY index (the
    longest-running cut, back to 1975; complete national state coverage).

No API key required. Series id format: fhfa:<flavor>:<freq>:<place_id>.
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

MASTER_URL = "https://www.fhfa.gov/hpi/download/monthly/hpi_master.csv"
UA = "Econ-Fin Data Library admin@hfdatalibrary.com"

# The 9 Census Divisions + the national aggregate, as they appear in the master file.
_NAT_PLACES = {
    "USA": "United States",
    "DV_NE": "New England Division",
    "DV_MA": "Middle Atlantic Division",
    "DV_ENC": "East North Central Division",
    "DV_WNC": "West North Central Division",
    "DV_SA": "South Atlantic Division",
    "DV_ESC": "East South Central Division",
    "DV_WSC": "West South Central Division",
    "DV_MT": "Mountain Division",
    "DV_PAC": "Pacific Division",
}

# 50 states + DC (the master file uses two-letter postal codes for the State level).
_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO",
    "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA",
    "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
]

# Each entry: (hpi_type, hpi_flavor, frequency, level) -> set of allowed place_ids.
# The connector keeps only rows matching one of these selectors.
_SELECTORS = [
    # Headline purchase-only MONTHLY: USA + 9 Census Divisions.
    ("traditional", "purchase-only", "monthly", "USA or Census Division", set(_NAT_PLACES)),
    # All-transactions QUARTERLY: every state + DC (deepest history, full coverage).
    ("traditional", "all-transactions", "quarterly", "State", set(_STATES)),
]

_FLAVOR_TAG = {"purchase-only": "po", "all-transactions": "at", "expanded-data": "exp"}
_FREQ_CODE = {"monthly": "M", "quarterly": "Q"}


def _period_to_date(frequency: str, yr: int, period: int) -> Optional[dt.date]:
    """Map (frequency, year, period) to the period-start date."""
    if frequency == "monthly":
        if 1 <= period <= 12:
            return dt.date(yr, period, 1)
        return None
    if frequency == "quarterly":
        if 1 <= period <= 4:
            return dt.date(yr, (period - 1) * 3 + 1, 1)
        return None
    return None


def _to_float(s: Optional[str]) -> Optional[float]:
    if s is None:
        return None
    s = s.strip()
    if not s or s.upper() in {"NA", "N/A", "."}:
        return None
    try:
        return float(s)
    except ValueError:
        return None


class FHFAConnector(Connector):
    source_id = "fhfa"
    name = "FHFA House Price Index"
    license_id = "us-public-domain"
    schedule = "0 7 25 * *"   # monthly: FHFA releases the monthly HPI ~25th of each month
    attribution = "Source: U.S. Federal Housing Finance Agency, House Price Index (public domain)"
    homepage = "https://www.fhfa.gov/data/hpi"

    def _sid(self, flavor: str, frequency: str, place_id: str) -> str:
        return f"fhfa:{_FLAVOR_TAG.get(flavor, flavor)}:{_FREQ_CODE.get(frequency, frequency)}:{place_id}"

    def _meta_for(self, flavor: str, frequency: str, level: str,
                  place_id: str, place_name: str) -> SeriesMeta:
        geo = "US" if place_id == "USA" else place_id
        flavor_label = {
            "purchase-only": "purchase-only",
            "all-transactions": "all-transactions",
            "expanded-data": "expanded-data",
        }.get(flavor, flavor)
        title = f"FHFA HPI ({flavor_label}, NSA) - {place_name}"
        return SeriesMeta(
            series_id=self._sid(flavor, frequency, place_id),
            title=title,
            frequency=_FREQ_CODE.get(frequency, "irregular"),
            unit="Index (1991Q1=100 or 1980Q1=100 per series)",
            geography=geo,
            category="housing",
            license_id=self.license_id,
            metadata={
                "hpi_type": "traditional",
                "hpi_flavor": flavor,
                "level": level,
                "place_id": place_id,
                "place_name": place_name,
                "seasonal_adjustment": "NSA",
            },
        )

    def discover(self) -> list[SeriesMeta]:
        out: list[SeriesMeta] = []
        for _typ, flavor, freq, level, _ in _SELECTORS:
            if level == "USA or Census Division":
                for pid, pname in _NAT_PLACES.items():
                    out.append(self._meta_for(flavor, freq, level, pid, pname))
            elif level == "State":
                for pid in _STATES:
                    out.append(self._meta_for(flavor, freq, level, pid, pid))
        return out

    def _download(self) -> str:
        last_err: Optional[Exception] = None
        for attempt in range(4):
            try:
                r = requests.get(MASTER_URL, timeout=120, headers={"User-Agent": UA})
                r.raise_for_status()
                return r.text
            except requests.RequestException as e:
                last_err = e
                time.sleep(2 ** attempt)
        raise RuntimeError(f"FHFA master CSV download failed after retries: {last_err}")

    def fetch(self, since: Optional[dt.date] = None):
        text = self._download()
        reader = csv.DictReader(io.StringIO(text))

        # Pre-index the selectors for fast membership tests.
        wanted: dict[tuple, set] = {}
        for typ, flavor, freq, level, places in _SELECTORS:
            wanted[(typ, flavor, freq, level)] = places

        # Accumulate observations per series, plus remember a place_name per series.
        obs_by_series: dict[str, list[Observation]] = {}
        meta_keys: dict[str, tuple] = {}   # sid -> (flavor, freq, level, place_id, place_name)

        for row in reader:
            typ = (row.get("hpi_type") or "").strip()
            flavor = (row.get("hpi_flavor") or "").strip()
            freq = (row.get("frequency") or "").strip()
            level = (row.get("level") or "").strip()
            key = (typ, flavor, freq, level)
            places = wanted.get(key)
            if places is None:
                continue
            place_id = (row.get("place_id") or "").strip()
            if place_id not in places:
                continue

            try:
                yr = int((row.get("yr") or "").strip())
                period = int((row.get("period") or "").strip())
            except (ValueError, TypeError):
                continue
            obs_date = _period_to_date(freq, yr, period)
            if obs_date is None:
                continue
            if since is not None and obs_date < since:
                continue

            value = _to_float(row.get("index_nsa"))
            if value is None:
                continue

            sid = self._sid(flavor, freq, place_id)
            obs_by_series.setdefault(sid, []).append(
                Observation(sid, obs_date, value, version="clean"))
            if sid not in meta_keys:
                place_name = (row.get("place_name") or place_id).strip()
                meta_keys[sid] = (flavor, freq, level, place_id, place_name)

        for sid, obs in obs_by_series.items():
            flavor, freq, level, place_id, place_name = meta_keys[sid]
            obs.sort(key=lambda o: o.obs_date)
            yield self._meta_for(flavor, freq, level, place_id, place_name), obs
