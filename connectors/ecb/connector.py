"""European Central Bank (ECB) Data Portal connector.

Pulls from the ECB SDMX 2.1 RESTful web service (no API key required):
    https://data-api.ecb.europa.eu/service/data/<flowRef>/<key>?format=jsondata

License: ecb-attrib-nomodify -- the ECB permits redistribution with attribution but
the values must NOT be modified, so every Observation is emitted exactly as received
(version="clean" here just denotes the served tier; no transformation is applied).

Starter set of high-value euro-area series across three dataflows:
  - EXR : euro reference exchange rates (daily, foreign currency per 1 EUR)
  - FM  : financial-market rates -- ECB key policy rates (daily) + EURIBOR (monthly)
  - YC  : euro-area AAA government bond spot yield curve (daily, by maturity)

SDMX-JSON layout (single fully-specified series per request):
  dataSets[0].series[<dimkey>].observations  -> {"<i>": [value, ...attrs], ...}
  structure.dimensions.observation[0].values -> [{"id": "<TIME_PERIOD>"}, ...]
The observation dict key "i" indexes positionally into that time-values list; missing
periods (weekends/holidays) are simply absent, so we never assume contiguous dates.

Series id format: ecb:<FLOW>:<key>  e.g. ecb:EXR:D.USD.EUR.SP00.A
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

BASE = "https://data-api.ecb.europa.eu/service/data"
HEADERS = {
    "User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
    "Accept": "application/json",
}

# ---- Starter set -------------------------------------------------------------
# Each entry: (flow, key) -> dict(title, freq, unit, category)
# Frequencies: 'D' daily, 'M' monthly. geography is the euro area ("EA"/"U2").

_EXR = {  # Euro reference rates: units of the named currency per 1 EUR (daily avg)
    "USD": "US dollar", "GBP": "Pound sterling", "JPY": "Japanese yen",
    "CHF": "Swiss franc", "CNY": "Chinese yuan renminbi", "CAD": "Canadian dollar",
    "AUD": "Australian dollar", "SEK": "Swedish krona", "NOK": "Norwegian krone",
    "DKK": "Danish krone", "PLN": "Polish zloty", "HKD": "Hong Kong dollar",
    "INR": "Indian rupee", "BRL": "Brazilian real", "MXN": "Mexican peso",
    "KRW": "South Korean won", "TRY": "Turkish lira", "ZAR": "South African rand",
}

_FM_POLICY = {  # ECB key interest rates (daily levels, % p.a.)
    "MRR_FR": "ECB main refinancing operations rate (fixed rate, level)",
    "DFR":    "ECB deposit facility rate (level)",
    "MLFR":   "ECB marginal lending facility rate (level)",
}

_FM_EURIBOR = {  # EURIBOR fixings (monthly, % p.a.)
    "EURIBOR1MD_": "EURIBOR 1-month",
    "EURIBOR3MD_": "EURIBOR 3-month",
    "EURIBOR6MD_": "EURIBOR 6-month",
    "EURIBOR1YD_": "EURIBOR 12-month",
}

_YC = {  # Euro-area AAA gov't bond spot yields (daily, % p.a.), by maturity
    "SR_3M": "3-month", "SR_6M": "6-month", "SR_1Y": "1-year", "SR_2Y": "2-year",
    "SR_3Y": "3-year", "SR_5Y": "5-year", "SR_7Y": "7-year", "SR_10Y": "10-year",
    "SR_20Y": "20-year", "SR_30Y": "30-year",
}


def _build_series() -> dict[tuple[str, str], dict]:
    """Assemble the {(flow, key): meta-dict} starter catalog."""
    out: dict[tuple[str, str], dict] = {}

    for ccy, name in _EXR.items():
        out[("EXR", f"D.{ccy}.EUR.SP00.A")] = dict(
            title=f"Euro reference exchange rate: {name} per EUR (daily)",
            freq="D", unit=f"{ccy}/EUR", category="fx")

    for code, title in _FM_POLICY.items():
        out[("FM", f"D.U2.EUR.4F.KR.{code}.LEV")] = dict(
            title=title, freq="D", unit="% per annum", category="rates")

    for code, title in _FM_EURIBOR.items():
        out[("FM", f"M.U2.EUR.RT.MM.{code}.HSTA")] = dict(
            title=f"{title} EURIBOR (monthly avg)", freq="M",
            unit="% per annum", category="rates")

    for code, label in _YC.items():
        out[("YC", f"B.U2.EUR.4F.G_N_A.SV_C_YM.{code}")] = dict(
            title=f"Euro area AAA gov't bond spot yield, {label} maturity",
            freq="D", unit="% per annum", category="rates")

    return out


SERIES = _build_series()


class ECBConnector(Connector):
    source_id = "ecb"
    name = "ECB Data Portal"
    license_id = "ecb-attrib-nomodify"
    schedule = "30 6 * * *"   # daily; ECB euro ref rates publish ~16:00 CET on TARGET days
    attribution = "Source: European Central Bank (ECB) Data Portal. Values reproduced unmodified."
    homepage = "https://data.ecb.europa.eu"

    def _meta(self, flow: str, key: str, info: dict) -> SeriesMeta:
        sid = f"ecb:{flow}:{key}"
        return SeriesMeta(
            series_id=sid,
            title=info["title"],
            frequency=info["freq"],
            unit=info["unit"],
            geography="EA",            # euro area
            category=info["category"],
            license_id=self.license_id,
            metadata={"flow": flow, "key": key, "provider": "ECB"},
        )

    def discover(self) -> list[SeriesMeta]:
        return [self._meta(flow, key, info) for (flow, key), info in SERIES.items()]

    # -- HTTP with polite retry/backoff ---------------------------------------
    def _get(self, flow: str, key: str, params: dict) -> Optional[dict]:
        url = f"{BASE}/{flow}/{key}"
        last_exc: Optional[Exception] = None
        for attempt in range(4):
            try:
                r = requests.get(url, headers=HEADERS, params=params, timeout=90)
            except requests.RequestException as exc:
                last_exc = exc
                time.sleep(2 ** attempt)
                continue
            if r.status_code == 200:
                # 200 with empty body can occur when a window has no data
                if not r.content:
                    return None
                return r.json()
            if r.status_code == 404:
                return None                      # no data for this key/window
            if r.status_code in (429, 500, 502, 503, 504):
                # rate-limited / transient: honor Retry-After then back off
                wait = int(r.headers.get("Retry-After", 0)) or (2 ** attempt)
                time.sleep(wait)
                last_exc = RuntimeError(f"HTTP {r.status_code} for {flow}/{key}")
                continue
            r.raise_for_status()
        if last_exc:
            raise last_exc
        return None

    @staticmethod
    def _parse_date(token: str) -> Optional[dt.date]:
        """ECB TIME_PERIOD: 'YYYY-MM-DD' (D), 'YYYY-MM' (M), 'YYYY' (A), 'YYYY-Qn'."""
        try:
            parts = token.split("-")
            if len(parts) == 3:
                return dt.date(int(parts[0]), int(parts[1]), int(parts[2]))
            if len(parts) == 2:
                p1 = parts[1]
                if p1[0] in ("Q", "q"):                 # quarter -> first month
                    return dt.date(int(parts[0]), (int(p1[1:]) - 1) * 3 + 1, 1)
                return dt.date(int(parts[0]), int(p1), 1)  # monthly -> first of month
            if len(parts) == 1:
                return dt.date(int(parts[0]), 1, 1)        # annual -> Jan 1
        except (ValueError, IndexError):
            return None
        return None

    def _observations(self, sid: str, payload: dict) -> list[Observation]:
        datasets = payload.get("dataSets") or []
        if not datasets:
            return []
        series_map = datasets[0].get("series") or {}
        if not series_map:
            return []
        # Fully-specified key => exactly one series in the response.
        series = next(iter(series_map.values()))
        obs_map = series.get("observations") or {}

        time_dim = None
        for d in payload.get("structure", {}).get("dimensions", {}).get("observation", []):
            if d.get("id") == "TIME_PERIOD" or d.get("role") == "time":
                time_dim = d.get("values") or []
                break
        if time_dim is None:
            return []

        out: list[Observation] = []
        for idx_str, arr in obs_map.items():
            try:
                idx = int(idx_str)
            except ValueError:
                continue
            if not (0 <= idx < len(time_dim)):
                continue
            obs_date = self._parse_date(time_dim[idx].get("id", ""))
            if obs_date is None:
                continue
            raw = arr[0] if arr else None          # value is first element of obs array
            if raw is None:
                continue
            try:
                val = float(raw)                    # reproduced unmodified
            except (TypeError, ValueError):
                continue
            out.append(Observation(sid, obs_date, val, version="clean"))
        out.sort(key=lambda o: o.obs_date)
        return out

    def fetch(self, since: Optional[dt.date] = None):
        for (flow, key), info in SERIES.items():
            sid = f"ecb:{flow}:{key}"
            params = {"format": "jsondata"}
            if since is not None:
                params["startPeriod"] = since.isoformat()
            payload = self._get(flow, key, params)
            if not payload:
                continue
            obs = self._observations(sid, payload)
            if obs:
                yield self._meta(flow, key, info), obs
            time.sleep(0.2)   # be polite between requests
