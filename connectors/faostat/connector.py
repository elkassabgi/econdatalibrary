"""FAOSTAT connector -- Agricultural Producer Prices (PP domain; CC BY 4.0).

FAOSTAT's REST API (faostatservices.fao.org/api/v1) is now gated behind an
authorization header / developer-portal key (every endpoint returns
"Missing Authorization Header" 401 as of 2026). The bulk-download service is
still fully open, so we pull the normalized PP bulk ZIP and stream-parse it.

Domain PP = "Producer Prices": prices received by farmers at the farm gate /
first point of sale. The bulk file carries four elements:
    5530 Producer Price (LCU/tonne)   -- local currency
    5531 Producer Price (SLC/tonne)   -- standard local currency
    5532 Producer Price (USD/tonne)   -- US dollars  <-- we use this
    5539 Producer Price Index (2014-2016 = 100)
We take element 5532 (USD/tonne) because it is cross-country comparable, and
Months Code 7021 ("Annual value") for a clean annual series. Coverage runs
1991-2024 for the major producers in our curated starter set.

Series id: faostat:PP:<area_code>:<item_code>:5532
  e.g. faostat:PP:231:15:5532  (USA, Wheat, USD/tonne)

The bulk CSV is ~214 MB uncompressed; we stream it from the ZIP in memory,
keep only our curated (area, item) keys, and never spill to disk.
"""
from __future__ import annotations

import csv
import datetime as dt
import io
import os
import sys
import time
import zipfile
from collections import defaultdict
from typing import Iterable, Optional

import requests

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
from connectors.base import Connector, SeriesMeta, Observation  # noqa: E402

UA = "Econ-Fin Data Library admin@hfdatalibrary.com"
BULK_URL = "https://bulks-faostat.fao.org/production/Prices_E_All_Data_(Normalized).zip"
CSV_NAME = "Prices_E_All_Data_(Normalized).csv"

ELEMENT_USD = "5532"          # Producer Price (USD/tonne)
MONTHS_ANNUAL = "7021"        # "Annual value" -> annual frequency

# Curated countries: FAOSTAT internal Area Code -> (display name, ISO3).
# Keyed on the numeric Area Code (stable) rather than the display name, which
# differs between the data CSV ("China, mainland") and the lookup file
# ("China; mainland").
COUNTRIES = {
    "231": ("United States of America", "USA"),
    "41":  ("China, mainland", "CHN"),
    "100": ("India", "IND"),
    "21":  ("Brazil", "BRA"),
    "9":   ("Argentina", "ARG"),
    "33":  ("Canada", "CAN"),
    "10":  ("Australia", "AUS"),
    "185": ("Russian Federation", "RUS"),
    "68":  ("France", "FRA"),
    "79":  ("Germany", "DEU"),
    "106": ("Italy", "ITA"),
    "203": ("Spain", "ESP"),
    "229": ("United Kingdom", "GBR"),
    "223": ("Turkiye", "TUR"),
    "216": ("Thailand", "THA"),
}

# Curated commodities: FAOSTAT Item Code -> display name (globally traded staples).
ITEMS = {
    "15":  "Wheat",
    "56":  "Maize (corn)",
    "27":  "Rice",
    "236": "Soya beans",
    "44":  "Barley",
    "83":  "Sorghum",
    "156": "Sugar cane",
    "656": "Coffee, green",
    "767": "Cotton lint, ginned",
    "267": "Sunflower seed",
    "116": "Potatoes",
    "388": "Tomatoes",
    "515": "Apples",
    "486": "Bananas",
    "490": "Oranges",
    "560": "Grapes",
}

# Curated (area_code, item_code) starter set. Each pair has ~30+ annual USD
# observations through 2024 (verified against the bulk file). Chosen so each
# major producer is represented by the crops it is actually significant in,
# rather than a dense country x item grid full of gaps.
PAIRS = [
    # United States
    ("231", "15"), ("231", "56"), ("231", "27"), ("231", "236"),
    ("231", "83"), ("231", "767"), ("231", "490"), ("231", "560"),
    # China (mainland)
    ("41", "56"), ("41", "236"), ("41", "515"),
    # India
    ("100", "15"), ("100", "27"), ("100", "156"),
    # Brazil
    ("21", "56"), ("21", "236"), ("21", "656"), ("21", "156"),
    # Argentina
    ("9", "15"), ("9", "236"), ("9", "267"),
    # Canada
    ("33", "15"), ("33", "44"), ("33", "236"), ("33", "267"),
    # Australia
    ("10", "15"), ("10", "44"),
    # Russia
    ("185", "15"), ("185", "44"), ("185", "267"),
    # France
    ("68", "15"), ("68", "56"), ("68", "44"), ("68", "560"),
    # Germany
    ("79", "15"), ("79", "56"), ("79", "116"),
    # Italy
    ("106", "388"), ("106", "560"),
    # Spain
    ("203", "490"), ("203", "560"),
    # United Kingdom
    ("229", "15"), ("229", "44"),
    # Turkiye
    ("223", "388"), ("223", "515"),
    # Thailand
    ("216", "27"), ("216", "156"),
]


def _meta(area_code: str, item_code: str) -> SeriesMeta:
    country_name, iso3 = COUNTRIES[area_code]
    item_name = ITEMS[item_code]
    sid = f"faostat:PP:{area_code}:{item_code}:{ELEMENT_USD}"
    return SeriesMeta(
        series_id=sid,
        title=f"Producer price, {item_name} - {country_name} (USD/tonne)",
        frequency="A",
        unit="USD/tonne",
        geography=iso3,
        category="agriculture",
        license_id="cc-by-4.0",
        metadata={
            "domain": "PP",
            "area_code": area_code,
            "item_code": item_code,
            "element_code": ELEMENT_USD,
            "country": country_name,
            "item": item_name,
        },
    )


class FAOSTATConnector(Connector):
    source_id = "faostat"
    name = "FAOSTAT"
    license_id = "cc-by-4.0"
    schedule = "0 7 8 * *"     # monthly; FAO refreshes PP a few times a year
    attribution = ("Source: Food and Agriculture Organization of the United "
                   "Nations (FAO), FAOSTAT Producer Prices (CC BY 4.0)")
    homepage = "https://www.fao.org/faostat"

    # Keys we keep while streaming the bulk CSV.
    _WANTED = set(PAIRS)

    def discover(self) -> list[SeriesMeta]:
        return [_meta(a, i) for (a, i) in PAIRS]

    def _download_zip(self) -> bytes:
        last = None
        for attempt in range(4):
            try:
                r = requests.get(BULK_URL, timeout=300, headers={"User-Agent": UA})
                r.raise_for_status()
                return r.content
            except requests.RequestException as e:
                last = e
                time.sleep(2 ** attempt)
        raise RuntimeError(f"FAOSTAT bulk download failed after retries: {last}")

    def fetch(self, since: Optional[dt.date] = None
              ) -> Iterable[tuple[SeriesMeta, list[Observation]]]:
        since_year = since.year if since else None
        blob = self._download_zip()

        by_series: dict[tuple[str, str], list[Observation]] = defaultdict(list)
        with zipfile.ZipFile(io.BytesIO(blob)) as z:
            # Stream the (large) normalized CSV line by line; FAOSTAT bulk files
            # are latin-1 encoded.
            with z.open(CSV_NAME) as fh:
                text = io.TextIOWrapper(fh, encoding="latin-1", newline="")
                reader = csv.DictReader(text)
                for row in reader:
                    if row.get("Element Code") != ELEMENT_USD:
                        continue
                    if row.get("Months Code") != MONTHS_ANNUAL:
                        continue
                    key = (row.get("Area Code"), row.get("Item Code"))
                    if key not in self._WANTED:
                        continue
                    raw = row.get("Value")
                    if raw is None or raw == "":
                        continue
                    try:
                        val = float(raw)
                    except (TypeError, ValueError):
                        continue
                    try:
                        year = int(row["Year"])
                    except (TypeError, ValueError, KeyError):
                        continue
                    if since_year is not None and year < since_year:
                        continue
                    sid = f"faostat:PP:{key[0]}:{key[1]}:{ELEMENT_USD}"
                    flag = (row.get("Flag") or "").strip()
                    by_series[key].append(
                        Observation(
                            series_id=sid,
                            obs_date=dt.date(year, 12, 31),
                            value=val,
                            version="clean",
                            flags=(flag,) if flag else (),
                        )
                    )

        for key in PAIRS:
            obs = by_series.get(key)
            if not obs:
                continue
            obs.sort(key=lambda o: o.obs_date)
            yield _meta(key[0], key[1]), obs
