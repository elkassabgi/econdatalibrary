"""IMF Data connector -- macro statistics via the IMF SDMX 2.1 REST API.

The legacy JSON service (dataservices.imf.org/REST/SDMX_JSON.svc) was retired in
the 2024-25 migration to data.imf.org, and the monolithic IFS dataflow was split
into thematic dataflows. We target the new API at api.imf.org and pull two of the
highest-value, densely-populated dataflows:

  * CPI    -- Consumer Price Index (headline index level + YoY inflation rate)
  * MFS_IR -- Monetary & Financial Statistics, interest rates (deposit, lending,
              money-market, treasury-bill, government-bond yield)

Transport note: the new API's JSON serializer throws a server-side
JsonGenerationException on multi-series data queries, and SDMX-ML returns 500, but
SDMX-CSV (Accept: application/vnd.sdmx.data+csv) is reliable -- so we request CSV.
We issue ONE broad cross-country query per (indicator, transformation) and split
the result into per-country series, mirroring the World Bank connector's shape.

No API key (IMF Data is free of charge). Country codes are ISO3. License class
is `imf-terms` (re-serveable, but we must disclose the data is available free).

Series id format:
  imf:CPI:<INDEX>:<ISO3>          headline index level (e.g. imf:CPI:IX:USA)
  imf:CPI:INFL_YOY:<ISO3>         year-over-year inflation, percent
  imf:MFS_IR:<RATE>:<ISO3>        an interest rate (e.g. imf:MFS_IR:DEPOSIT:USA)
"""
from __future__ import annotations

import csv
import datetime as dt
import io
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Optional

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
from connectors.base import Connector, SeriesMeta, Observation  # noqa: E402

BASE = "https://api.imf.org/external/sdmx/2.1/data"
UA = "Econ-Fin Data Library admin@hfdatalibrary.com"
CSV_ACCEPT = "application/vnd.sdmx.data+csv"

# Curated country set: G20 + a few additional major / well-covered economies.
# ISO3 codes (the COUNTRY dimension in the new IMF API is ISO3, not the legacy
# 3-digit IFS area codes). Keeps the starter set focused while covering the
# highest-value economies; the full country list can be expanded later.
COUNTRIES = {
    "USA": "United States", "GBR": "United Kingdom", "DEU": "Germany",
    "FRA": "France", "ITA": "Italy", "ESP": "Spain", "JPN": "Japan",
    "CAN": "Canada", "AUS": "Australia", "KOR": "South Korea",
    "MEX": "Mexico", "BRA": "Brazil", "IND": "India", "CHN": "China",
    "RUS": "Russia", "ZAF": "South Africa", "TUR": "Turkiye",
    "IDN": "Indonesia", "SAU": "Saudi Arabia", "ARG": "Argentina",
    "CHE": "Switzerland", "SWE": "Sweden", "NOR": "Norway",
    "POL": "Poland", "CZE": "Czechia", "HUN": "Hungary",
}

# --- CPI dataflow -------------------------------------------------------------
# Key: COUNTRY.INDEX_TYPE.COICOP_1999.TYPE_OF_TRANSFORMATION.FREQUENCY
# We use INDEX_TYPE=CPI, COICOP_1999=_T (all items), FREQUENCY=M.
# Two transformations: IX (index level) and YOY_PCH_PA_PT (YoY % change).
CPI_SPECS = [
    # (rate_code, transformation_value, title_suffix, unit, category)
    ("IX",       "IX",            "CPI, all items (index)",      "Index",   "prices"),
    ("INFL_YOY", "YOY_PCH_PA_PT", "Inflation, all items (YoY %)", "Percent", "prices"),
]

# --- MFS_IR dataflow ----------------------------------------------------------
# Key: COUNTRY.INDICATOR.FREQUENCY  (FREQUENCY=M)
# Indicator codes are opaque in the API and not exposed via an enumerated
# codelist; these mappings come from the IFS/MFS indicator catalogue.
MFS_IR_SPECS = [
    # (rate_code, indicator_value, title, category)
    ("DEPOSIT",  "MFS135_RT_PT_A_PT",  "Deposit rate",            "rates"),
    ("LENDING",  "MFS162_RT_PT_A_PT",  "Lending rate",            "rates"),
    ("MONEYMKT", "MMRT_RT_PT_A_PT",    "Money market rate",       "rates"),
    ("TBILL",    "GSTBILY_RT_PT_A_PT", "Treasury bill rate",      "rates"),
    ("GOVBOND",  "S13BOND_RT_PT_A_PT", "Government bond yield",   "rates"),
]


def _parse_period(tp: str) -> Optional[dt.date]:
    """Map an SDMX TIME_PERIOD to a date (period-end for sub-annual).

    Handles 'YYYY-Mmm' (monthly), 'YYYY-Qq' (quarterly), 'YYYY' (annual).
    Returns None for anything unrecognised so callers can skip it.
    """
    tp = (tp or "").strip()
    try:
        if "-M" in tp:
            y, m = tp.split("-M")
            return dt.date(int(y), int(m), 1)
        if "-Q" in tp:
            y, q = tp.split("-Q")
            return dt.date(int(y), 3 * int(q), 1)   # quarter-start month
        if len(tp) == 4 and tp.isdigit():
            return dt.date(int(tp), 12, 31)         # annual -> year-end
    except (ValueError, TypeError):
        return None
    return None


def _freq_from_period(tp: str) -> str:
    if "-M" in tp:
        return "M"
    if "-Q" in tp:
        return "Q"
    return "A"


class IMFConnector(Connector):
    source_id = "imf"
    name = "IMF Data"
    license_id = "imf-terms"
    schedule = "0 7 * * 1"   # weekly (IMF macro series update monthly/quarterly)
    attribution = "Source: IMF Data (data available free of charge); IMF SDMX API"
    homepage = "https://data.imf.org"

    # ---- HTTP helper ---------------------------------------------------------
    def _get_csv(self, dataflow: str, key: str, since: Optional[dt.date]) -> list[dict]:
        """Fetch one SDMX-CSV data query, flattened one row per observation.

        Retries on transient errors. Returns a list of dict rows (DictReader).
        """
        start = "1960"
        if since is not None:
            # incremental: pull from the start of the `since` year (cheap, safe overlap)
            start = str(since.year)
        url = (f"{BASE}/IMF.STA,{dataflow}/{key}"
               f"?startPeriod={start}&dimensionAtObservation=AllDimensions")
        last_err: Optional[Exception] = None
        for attempt in range(4):
            try:
                req = urllib.request.Request(
                    url, headers={"User-Agent": UA, "Accept": CSV_ACCEPT})
                with urllib.request.urlopen(req, timeout=120) as resp:
                    raw = resp.read().decode("utf-8-sig")
                return list(csv.DictReader(io.StringIO(raw)))
            except urllib.error.HTTPError as e:
                # 404 / 400 => no data for this key; treat as empty, do not retry.
                if e.code in (400, 404):
                    return []
                last_err = e
            except (urllib.error.URLError, TimeoutError) as e:
                last_err = e
            time.sleep(2 * (attempt + 1))   # polite backoff
        if last_err:
            raise last_err
        return []

    def _key_country_field(self, country: str) -> str:
        return country

    # ---- discover ------------------------------------------------------------
    def discover(self) -> list[SeriesMeta]:
        metas: list[SeriesMeta] = []
        for code, _trans, title, unit, cat in CPI_SPECS:
            for iso, cname in COUNTRIES.items():
                sid = f"imf:CPI:{code}:{iso}"
                metas.append(SeriesMeta(
                    sid, f"{title} - {cname}", "M", unit, iso, cat,
                    self.license_id,
                    {"dataflow": "CPI", "rate_code": code, "iso3": iso}))
        for code, _ind, title, cat in MFS_IR_SPECS:
            for iso, cname in COUNTRIES.items():
                sid = f"imf:MFS_IR:{code}:{iso}"
                metas.append(SeriesMeta(
                    sid, f"{title} - {cname}", "M", "Percent per annum", iso, cat,
                    self.license_id,
                    {"dataflow": "MFS_IR", "rate_code": code, "iso3": iso}))
        return metas

    # ---- fetch ---------------------------------------------------------------
    def fetch(self, since: Optional[dt.date] = None):
        wanted = set(COUNTRIES)

        # ---- CPI: one cross-country query per transformation ----
        for code, trans, title, unit, cat in CPI_SPECS:
            key = f".CPI._T.{trans}.M"          # all countries, all-items, monthly
            rows = self._get_csv("CPI", key, since)
            by_iso: dict[str, list[Observation]] = {}
            for r in rows:
                iso = (r.get("COUNTRY") or "").strip()
                if iso not in wanted:
                    continue
                val = self._to_float(r.get("OBS_VALUE"))
                if val is None:
                    continue
                d = _parse_period(r.get("TIME_PERIOD", ""))
                if d is None:
                    continue
                if since is not None and d < since:
                    continue
                sid = f"imf:CPI:{code}:{iso}"
                by_iso.setdefault(iso, []).append(
                    Observation(sid, d, val, version="clean"))
            for iso, obs in by_iso.items():
                obs.sort(key=lambda o: o.obs_date)
                yield (SeriesMeta(
                    f"imf:CPI:{code}:{iso}", f"{title} - {COUNTRIES[iso]}", "M",
                    unit, iso, cat, self.license_id,
                    {"dataflow": "CPI", "rate_code": code, "iso3": iso}), obs)

        # ---- MFS_IR: one cross-country query per indicator ----
        for code, ind, title, cat in MFS_IR_SPECS:
            key = f".{ind}.M"                   # all countries, monthly
            rows = self._get_csv("MFS_IR", key, since)
            by_iso = {}
            for r in rows:
                iso = (r.get("COUNTRY") or "").strip()
                if iso not in wanted:
                    continue
                val = self._to_float(r.get("OBS_VALUE"))
                if val is None:
                    continue
                d = _parse_period(r.get("TIME_PERIOD", ""))
                if d is None:
                    continue
                if since is not None and d < since:
                    continue
                sid = f"imf:MFS_IR:{code}:{iso}"
                by_iso.setdefault(iso, []).append(
                    Observation(sid, d, val, version="clean"))
            for iso, obs in by_iso.items():
                obs.sort(key=lambda o: o.obs_date)
                yield (SeriesMeta(
                    f"imf:MFS_IR:{code}:{iso}", f"{title} - {COUNTRIES[iso]}", "M",
                    "Percent per annum", iso, cat, self.license_id,
                    {"dataflow": "MFS_IR", "rate_code": code, "iso3": iso}), obs)

    @staticmethod
    def _to_float(v) -> Optional[float]:
        if v is None:
            return None
        s = str(v).strip()
        if not s:
            return None
        try:
            return float(s)
        except (ValueError, TypeError):
            return None
