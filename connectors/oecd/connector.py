"""OECD Data Explorer connector (SDMX-JSON v2; macro; CC BY 4.0).

The OECD exposes its databases as SDMX 2.1 dataflows at
https://sdmx.oecd.org/public/rest/data/<agency>,<dataflow>/<key>?format=jsondata .
The JSON ("SDMX-JSON v2") packs every series under data.dataSets[0].series keyed by a
colon-joined tuple of *positional indices* into data.structures[0].dimensions.series;
observations are keyed by positional indices into dimensions.observation (TIME_PERIOD).

Strategy: curate a handful of headline macro indicators. Each indicator pins every
dimension except REF_AREA to a single value, then requests a fixed list of major
economies in one call (REF_AREA accepts a '+'-joined OR filter). That yields exactly
one clean series per country per indicator -- ~4 indicators x ~13 areas = ~50 series.
We parse REF_AREA back out of the series key, so if the API drops an area for a given
measure (e.g. EA20 has no headline QNA growth) we simply don't emit it. No API key.

Series id format: oecd:<indicator_code>:<REF_AREA>  (e.g. oecd:GDP_GROWTH_QOQ:USA).
"""
from __future__ import annotations

import datetime as dt
import os
import sys
import time
from dataclasses import dataclass
from typing import Optional

import requests

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
from connectors.base import Connector, SeriesMeta, Observation  # noqa: E402

BASE = "https://sdmx.oecd.org/public/rest/data"
HEADERS = {
    "User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
    # SDMX-JSON v2 media type; format=jsondata in the query is the documented fallback.
    "Accept": "application/vnd.sdmx.data+json;version=2.0",
}

# Major economies + the OECD/euro-area aggregates. The API silently returns whichever
# of these it actually has for a given measure, so over-listing is safe.
COUNTRIES = [
    "USA", "JPN", "DEU", "GBR", "FRA", "ITA", "CAN",   # G7
    "AUS", "KOR", "ESP", "MEX", "NLD", "CHE", "SWE",   # other large OECD
    "OECD", "EA20",                                     # aggregates
]


@dataclass(frozen=True)
class Indicator:
    code: str          # our short id, used in series_id
    title: str         # human title (per-country title appends " - <area>")
    flow: str          # "<agency>,<dataflow>[,<version>]"
    key: str           # SDMX key with "{c}" placeholder for the REF_AREA slot
    freq: str          # 'M' | 'Q' (our frequency letter)
    unit: Optional[str]
    category: str
    start: str         # default startPeriod when no `since` is given


# Curated headline set. Keys were verified live against the dataflows' DSDs.
INDICATORS = [
    Indicator(
        code="GDP_GROWTH_QOQ",
        title="GDP, real, growth rate, quarter-on-quarter (%, s.a.)",
        flow="OECD.SDD.NAD,DSD_NAMAIN1@DF_QNA_EXPENDITURE_GROWTH_OECD,1.1",
        key="Q.Y.{c}.S1..B1GQ......G1.",
        freq="Q", unit="Percent change", category="macro", start="1995-Q1",
    ),
    Indicator(
        code="GDP_GROWTH_YOY",
        title="GDP, real, growth rate, year-on-year (%, s.a.)",
        flow="OECD.SDD.NAD,DSD_NAMAIN1@DF_QNA_EXPENDITURE_GROWTH_OECD,1.1",
        key="Q.Y.{c}.S1..B1GQ......GY.",
        freq="Q", unit="Percent change", category="macro", start="1995-Q1",
    ),
    Indicator(
        code="CPI_YOY",
        title="CPI, all items, inflation rate, year-on-year (%)",
        flow="OECD.SDD.TPS,DSD_PRICES@DF_PRICES_ALL",
        key="{c}.M.N.CPI.PA._T.N.GY",
        freq="M", unit="Percent change", category="macro", start="1995-01",
    ),
    Indicator(
        code="UNEMP_RATE",
        title="Harmonised unemployment rate, persons 15+ (% of labour force, s.a.)",
        flow="OECD.SDD.TPS,DSD_LFS@DF_IALFS_UNE_M",
        key="{c}.UNE_LF_M...Y._T.Y_GE15..M",
        freq="M", unit="Percent of labour force", category="macro", start="1995-01",
    ),
]


def _parse_time(token: str) -> Optional[dt.date]:
    """SDMX TIME_PERIOD -> first day of the period. Handles 'YYYY-Qn', 'YYYY-MM', 'YYYY'."""
    try:
        if "-Q" in token:
            y, q = token.split("-Q")
            return dt.date(int(y), (int(q) - 1) * 3 + 1, 1)
        if len(token) == 7 and token[4] == "-":          # YYYY-MM
            y, m = token.split("-")
            return dt.date(int(y), int(m), 1)
        if len(token) == 4 and token.isdigit():           # YYYY
            return dt.date(int(token), 1, 1)
        if len(token) == 10 and token[4] == "-":          # YYYY-MM-DD
            return dt.date.fromisoformat(token)
    except (ValueError, TypeError):
        return None
    return None


class OECDConnector(Connector):
    source_id = "oecd"
    name = "OECD Data Explorer"
    license_id = "cc-by-4.0"
    schedule = "0 7 * * 1"      # weekly (OECD macro refreshes are not daily)
    attribution = "Source: OECD Data Explorer (https://data-explorer.oecd.org), CC BY 4.0"
    homepage = "https://data-explorer.oecd.org"

    # ---- discovery -------------------------------------------------------
    def discover(self) -> list[SeriesMeta]:
        out: list[SeriesMeta] = []
        for ind in INDICATORS:
            for area in COUNTRIES:
                out.append(self._meta(ind, area))
        return out

    def _meta(self, ind: Indicator, area: str) -> SeriesMeta:
        return SeriesMeta(
            series_id=f"oecd:{ind.code}:{area}",
            title=f"{ind.title} - {area}",
            frequency=ind.freq,
            unit=ind.unit,
            geography=area,
            category=ind.category,
            license_id=self.license_id,
            metadata={"indicator": ind.code, "dataflow": ind.flow, "ref_area": area},
        )

    # ---- fetch -----------------------------------------------------------
    def fetch(self, since: Optional[dt.date] = None):
        # NB: the OECD public endpoint caps the *total* number of cells per
        # response and, when a multi-country request exceeds it, silently
        # truncates some series to a handful of points (verified live: a single
        # country returns full history, three countries get clipped unevenly).
        # So we query one REF_AREA at a time -- full history, deterministic.
        for ind in INDICATORS:
            for area in COUNTRIES:
                try:
                    result = self._fetch_one(ind, area, since)
                except requests.RequestException as e:
                    print(f"[oecd] WARN: {ind.code}:{area} failed: {e}", file=sys.stderr)
                    continue
                if result is not None:
                    yield result
                time.sleep(0.3)   # be polite between calls

    def _fetch_one(self, ind: Indicator, area: str, since: Optional[dt.date]):
        """Return (SeriesMeta, [Observation]) for one indicator+area, or None if empty."""
        agency, flow = ind.flow.split(",", 1)
        key = ind.key.format(c=area)
        start = self._start_period(ind, since)
        url = f"{BASE}/{agency},{flow}/{key}"
        params = {"format": "jsondata", "dimensionAtObservation": "TIME_PERIOD"}
        if start:
            params["startPeriod"] = start

        payload = self._get_json(url, params)
        data = payload.get("data") or {}
        datasets = data.get("dataSets") or []
        structures = data.get("structures") or []
        if not datasets or not structures:
            return None
        series_map = datasets[0].get("series") or {}
        if not series_map:
            return None

        # Ordered TIME_PERIOD tokens; an observation key "n" indexes into this list.
        struct = structures[0]
        time_tokens = [v["id"] for v in struct["dimensions"]["observation"][0]["values"]]

        sid = f"oecd:{ind.code}:{area}"
        obs: list[Observation] = []
        # One area pinned -> exactly one series in the map (take it without decoding the key).
        sobj = next(iter(series_map.values()))
        for okey, ovals in (sobj.get("observations") or {}).items():
            try:
                token = time_tokens[int(okey)]
            except (IndexError, ValueError):
                continue
            d = _parse_time(token)
            if d is None or not ovals or ovals[0] is None:
                continue
            try:
                val = float(ovals[0])
            except (TypeError, ValueError):
                continue
            obs.append(Observation(sid, d, val, version="clean"))
        if not obs:
            return None
        obs.sort(key=lambda o: o.obs_date)
        return self._meta(ind, area), obs

    # ---- helpers ---------------------------------------------------------
    @staticmethod
    def _start_period(ind: Indicator, since: Optional[dt.date]) -> Optional[str]:
        """Translate an incremental `since` date into an SDMX startPeriod for this freq."""
        if since is None:
            return ind.start
        if ind.freq == "Q":
            return f"{since.year}-Q{(since.month - 1) // 3 + 1}"
        if ind.freq == "M":
            return f"{since.year}-{since.month:02d}"
        return str(since.year)

    @staticmethod
    def _get_json(url: str, params: dict, *, retries: int = 4) -> dict:
        """GET with polite backoff. OECD returns 429/503 under load; retry those."""
        last = None
        for attempt in range(retries):
            try:
                r = requests.get(url, params=params, headers=HEADERS, timeout=90)
            except requests.RequestException as e:
                last = e
                time.sleep(2 * (attempt + 1))
                continue
            if r.status_code == 200:
                return r.json()
            if r.status_code == 404:
                # No data for this selection -> empty, not an error.
                return {}
            if r.status_code in (429, 500, 502, 503, 504):
                wait = r.headers.get("Retry-After")
                time.sleep(float(wait) if (wait and wait.isdigit()) else 3 * (attempt + 1))
                last = requests.HTTPError(f"{r.status_code} for {r.url}")
                continue
            r.raise_for_status()
        if last:
            raise last
        raise requests.HTTPError(f"exhausted retries for {url}")
