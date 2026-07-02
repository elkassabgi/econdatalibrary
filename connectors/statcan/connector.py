"""Statistics Canada connector (Canadian macro; Statistics Canada Open Licence).

Source: the Web Data Service (WDS) REST API -- https://www150.statcan.gc.ca/t1/wds/rest/ .
No API key. We pull a curated starter set of headline Canada-wide series addressed by
StatCan *vector* IDs (a vector is a stable, unique time-series identifier of the form
"V" + up to 10 digits).

Two endpoints are used:
  * getSeriesInfoFromVector              -> authoritative title + frequencyCode per vector
  * getDataFromVectorsAndLatestNPeriods  -> the actual observations (latest N periods)
Both accept a JSON array of {"vectorId": <int>, ...} (POST) and cap at 300 vectors/request,
so we chunk. WDS values are published in the units named by `scalarFactorCode` (0=units,
3=thousands, 6=millions, ...); we store the value exactly as published and record the scalar
factor + a descriptive unit in metadata rather than rescaling (no precision artifacts, and it
matches how StatCan presents the table). Suppressed/NA points carry statusCode in {1,8,9} and
are skipped along with any non-numeric value.

Series id format: statcan:V<vectorId>.
"""
from __future__ import annotations
import datetime as dt
import os
import sys
import time
from typing import Optional

import requests

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
from connectors.base import Connector, SeriesMeta, Observation  # noqa: E402

BASE = "https://www150.statcan.gc.ca/t1/wds/rest"
HEADERS = {
    "User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
    "Content-Type": "application/json",
}

# WDS frequencyCode -> contract frequency letter.
FREQ_MAP = {
    1: "D", 21: "D",          # daily
    2: "W",                   # weekly
    4: "irregular", 7: "irregular",  # biweekly / bimonthly
    6: "M", 20: "M",          # monthly
    9: "Q", 19: "Q",          # quarterly
    11: "irregular",          # semi-annual
    12: "A",                  # annual
    13: "A", 14: "A", 15: "A", 16: "A", 17: "A", 18: "irregular",  # multi-year / occasional
}

# scalarFactorCode -> unit-of-measure phrase (power-of-ten label StatCan publishes against).
SCALAR_LABEL = {
    0: "units", 1: "tens", 2: "hundreds", 3: "thousands", 4: "tens of thousands",
    5: "hundreds of thousands", 6: "millions", 7: "tens of millions",
    8: "hundreds of millions", 9: "billions",
}

# statusCode values that mean "no usable value" (skip the point).
BAD_STATUS = {1, 8, 9}  # 1=not available, 8=too unreliable, 9=not applicable

# How many reference periods of history to pull per vector.
# 480 months = 40y covers every monthly series in full; annual/quarterly are well within it.
LATEST_N = 480
CHUNK = 250  # WDS caps these endpoints at 300 vectors/request; stay under it.

# ---------------------------------------------------------------------------
# Curated starter set: headline, Canada-wide indicators. Every vector below was
# verified live against getSeriesInfoFromVector (productId + title checked) before
# being added. `unit` is a human label; the precise scalar factor comes from the data.
# (vector_id, fallback_title, category, unit_hint, geography)
# ---------------------------------------------------------------------------
SERIES = [
    # --- Prices: CPI (18-10-0004 monthly index 2002=100; 18-10-0256 BoC core, SA) ---
    (41690973,  "CPI, all-items, Canada (NSA, 2002=100)",                 "prices", "index, 2002=100", "CA"),
    (41691233,  "CPI, all-items excluding food and energy, Canada (NSA)", "prices", "index, 2002=100", "CA"),
    (41691046,  "CPI, food purchased from restaurants, Canada (NSA)",     "prices", "index, 2002=100", "CA"),
    (112593705, "CPI-trim (excl. 8 most volatile + indirect taxes), Canada (SA)", "prices", "index", "CA"),
    (112593706, "CPI-common style: all-items excl. 8 most volatile, Canada (SA)", "prices", "index", "CA"),
    (112593707, "CPI, all-items excluding indirect taxes, Canada (SA)",   "prices", "index", "CA"),
    (111955442, "New Housing Price Index, total (house and land), Canada","prices", "index, 201612=100", "CA"),

    # --- Output: monthly real GDP by industry (36-10-0434, chained 2017$, SAAR) ---
    (65201210,  "Monthly real GDP, all industries, Canada (chained 2017$, SAAR)",      "output", "millions of chained 2017 dollars", "CA"),
    (65201211,  "Monthly real GDP, goods-producing industries, Canada (SAAR)",         "output", "millions of chained 2017 dollars", "CA"),
    (65201212,  "Monthly real GDP, services-producing industries, Canada (SAAR)",      "output", "millions of chained 2017 dollars", "CA"),
    (65201221,  "Monthly real GDP, durable manufacturing industries, Canada (SAAR)",   "output", "millions of chained 2017 dollars", "CA"),

    # --- Labour market: Labour Force Survey (14-10-0287, monthly, SA, 15+) ---
    (2062815,   "Unemployment rate, 15+, Canada (SA, %)",        "labour", "percent", "CA"),
    (2062811,   "Employment, 15+, Canada (SA)",                  "labour", "thousands of persons", "CA"),
    (2062814,   "Unemployment, 15+, Canada (SA)",               "labour", "thousands of persons", "CA"),
    (2062810,   "Labour force, 15+, Canada (SA)",               "labour", "thousands of persons", "CA"),
    (2062809,   "Population, 15+, Canada (SA, LFS)",            "labour", "thousands of persons", "CA"),
    (2132579,   "Average hourly wage rate, all industries, 15+, Canada", "labour", "current dollars", "CA"),

    # --- Money & rates (10-10-0122, monthly) ---
    (122530,    "Bank rate, Canada (monthly, %)",               "rates", "percent", "CA"),

    # --- Demography (17-10-0009 quarterly; 17-10-0005 annual) ---
    (1,         "Population, Canada (quarterly estimate)",      "demography", "persons", "CA"),
    (466668,    "Population, all ages, Canada (annual, July 1)","demography", "persons", "CA"),
]


class StatCanConnector(Connector):
    source_id = "statcan"
    name = "Statistics Canada"
    license_id = "statcan-open"
    schedule = "0 7 * * *"  # WDS releases each business day at 08:30 ET; pull daily.
    attribution = ("Source: Statistics Canada, Web Data Service. "
                   "Reproduced and distributed under the Statistics Canada Open Licence.")
    homepage = "https://www.statcan.gc.ca"

    # -- HTTP helper with polite retry/backoff -------------------------------
    def _post(self, endpoint: str, payload: list) -> list:
        url = f"{BASE}/{endpoint}"
        last_exc = None
        for attempt in range(4):
            try:
                r = requests.post(url, json=payload, headers=HEADERS, timeout=90)
                if r.status_code == 429 or r.status_code >= 500:
                    raise requests.HTTPError(f"HTTP {r.status_code}")
                r.raise_for_status()
                data = r.json()
                # WDS returns a list (one element per requested vector).
                if isinstance(data, dict):
                    data = [data]
                return data
            except (requests.RequestException, ValueError) as e:
                last_exc = e
                time.sleep(2 * (attempt + 1))  # 2s, 4s, 6s
        raise RuntimeError(f"StatCan WDS request to {endpoint} failed: {last_exc}")

    def _chunks(self, vectors: list[int]):
        for i in range(0, len(vectors), CHUNK):
            yield vectors[i:i + CHUNK]

    # -- Pull authoritative title + frequency for each vector ----------------
    def _series_info(self) -> dict[int, dict]:
        info: dict[int, dict] = {}
        vectors = [v for v, *_ in SERIES]
        for chunk in self._chunks(vectors):
            data = self._post("getSeriesInfoFromVector", [{"vectorId": v} for v in chunk])
            for item in data:
                if item.get("status") != "SUCCESS":
                    continue
                o = item.get("object") or {}
                vid = o.get("vectorId")
                if vid is None:
                    continue
                info[int(vid)] = {
                    "title": (o.get("SeriesTitleEn") or "").strip(),
                    "freq": FREQ_MAP.get(o.get("frequencyCode"), "irregular"),
                    "product_id": o.get("productId"),
                }
        return info

    def _meta_for(self, vector: int, fallback_title: str, category: str,
                  unit: Optional[str], geo: Optional[str], info: dict) -> SeriesMeta:
        i = info.get(vector, {})
        sid = f"statcan:V{vector}"
        title = fallback_title  # curated label is the most readable; keep StatCan's raw title in metadata
        return SeriesMeta(
            sid, title, i.get("freq", "M"), unit, geo, category, self.license_id,
            {
                "vector": f"V{vector}",
                "vector_id": vector,
                "product_id": i.get("product_id"),
                "statcan_title": i.get("title"),
            },
        )

    # -- Contract: discover --------------------------------------------------
    def discover(self) -> list[SeriesMeta]:
        info = self._series_info()
        return [self._meta_for(v, title, cat, unit, geo, info)
                for (v, title, cat, unit, geo) in SERIES]

    # -- Contract: fetch -----------------------------------------------------
    def fetch(self, since: Optional[dt.date] = None):
        info = self._series_info()
        spec = {v: (title, cat, unit, geo) for (v, title, cat, unit, geo) in SERIES}
        vectors = list(spec)

        for chunk in self._chunks(vectors):
            payload = [{"vectorId": v, "latestN": LATEST_N} for v in chunk]
            data = self._post("getDataFromVectorsAndLatestNPeriods", payload)
            for item in data:
                if item.get("status") != "SUCCESS":
                    continue
                o = item.get("object") or {}
                vid = o.get("vectorId")
                if vid is None:
                    continue
                vid = int(vid)
                title, cat, unit, geo = spec.get(vid, (f"StatCan V{vid}", "macro", None, "CA"))
                meta = self._meta_for(vid, title, cat, unit, geo, info)

                obs: list[Observation] = []
                for dp in o.get("vectorDataPoint", []) or []:
                    if dp.get("statusCode") in BAD_STATUS:
                        continue
                    raw = dp.get("value")
                    if raw is None:
                        continue
                    try:
                        val = float(raw)
                    except (TypeError, ValueError):
                        continue
                    d = self._parse_date(dp.get("refPer") or dp.get("refPerRaw"))
                    if d is None:
                        continue
                    if since is not None and d < since:
                        continue
                    scalar = dp.get("scalarFactorCode", 0)
                    flags = ()
                    if dp.get("symbolCode") == 1:
                        flags = ("preliminary",)
                    elif dp.get("symbolCode") == 3:
                        flags = ("revised",)
                    obs.append(Observation(
                        meta.series_id, d, val, version="clean", flags=flags))
                    # record scalar/unit context once on the series metadata
                    if "scalar_factor_code" not in meta.metadata:
                        meta.metadata["scalar_factor_code"] = scalar
                        meta.metadata["scalar_unit"] = SCALAR_LABEL.get(scalar, str(scalar))

                if obs:
                    obs.sort(key=lambda x: x.obs_date)
                    yield meta, obs

    @staticmethod
    def _parse_date(s: Optional[str]) -> Optional[dt.date]:
        if not s:
            return None
        s = s.strip()
        try:
            if len(s) == 10:            # YYYY-MM-DD
                return dt.date.fromisoformat(s)
            if len(s) == 7:             # YYYY-MM
                y, m = s.split("-")
                return dt.date(int(y), int(m), 1)
            if len(s) == 4:             # YYYY
                return dt.date(int(s), 12, 31)
        except ValueError:
            return None
        return None
