"""World Bank Sovereign ESG Data connector (CC BY 4.0).

Same World Bank Open Data API as the `worldbank` connector, but scoped to the
Environment, Social and Governance (ESG) database (source=75) instead of the WDI.
The ESG codes do NOT resolve on the default WDI endpoint -- you must pass
`&source=75` on the data request, otherwise the API returns
"The indicator was not found." The response JSON is otherwise identical in shape
to WDI (payload[1] is a list of row dicts with countryiso3code/date/value), so the
parsing mirrors the WDI connector.

No API key. Annual frequency. Series id format: worldbank_esg:<indicator>:<ISO3>.

Curated starter set of 24 high-value indicators balanced across the three ESG
pillars (Environmental / Social / Governance). The Governance pillar maps to the
six Worldwide Governance Indicators (the *.EST estimate series, which can be
negative). Country-level rows only -- aggregate regions (Arab World, World, etc.)
carry an empty ISO3 code and are skipped.
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

UA = "Econ-Fin Data Library admin@hfdatalibrary.com"

# Curated starter set: code -> (title, pillar). Titles are the official World Bank
# indicator names (source=75). Pillar feeds the catalog category.
INDICATORS = {
    # --- Environmental ---
    "EN.ATM.CO2E.PC":     ("CO2 emissions (metric tons per capita)", "Environmental"),
    "EN.ATM.METH.PC":     ("Methane emissions (kt of CO2 equivalent per capita)", "Environmental"),
    "EN.ATM.PM25.MC.M3":  ("PM2.5 air pollution, mean annual exposure (micrograms per cubic meter)", "Environmental"),
    "EG.FEC.RNEW.ZS":     ("Renewable energy consumption (% of total final energy consumption)", "Environmental"),
    "EG.ELC.RNEW.ZS":     ("Renewable electricity output (% of total electricity output)", "Environmental"),
    "EG.USE.PCAP.KG.OE":  ("Energy use (kg of oil equivalent per capita)", "Environmental"),
    "AG.LND.FRST.ZS":     ("Forest area (% of land area)", "Environmental"),
    "ER.H2O.FWST.ZS":     ("Level of water stress: freshwater withdrawal as a proportion of available freshwater resources", "Environmental"),
    "EN.CLC.GHGR.MT.CE":  ("GHG net emissions/removals by LUCF (Mt of CO2 equivalent)", "Environmental"),
    # --- Social ---
    "SP.DYN.LE00.IN":     ("Life expectancy at birth, total (years)", "Social"),
    "SH.DYN.MORT":        ("Mortality rate, under-5 (per 1,000 live births)", "Social"),
    "SE.PRM.ENRR":        ("School enrollment, primary (% gross)", "Social"),
    "SI.POV.GINI":        ("Gini index", "Social"),
    "SL.UEM.TOTL.ZS":     ("Unemployment, total (% of total labor force) (modeled ILO estimate)", "Social"),
    "IT.NET.USER.ZS":     ("Individuals using the Internet (% of population)", "Social"),
    "SG.GEN.PARL.ZS":     ("Proportion of seats held by women in national parliaments (%)", "Social"),
    "SH.H2O.SMDW.ZS":     ("People using safely managed drinking water services (% of population)", "Social"),
    "EG.ELC.ACCS.ZS":     ("Access to electricity (% of population)", "Social"),
    # --- Governance (Worldwide Governance Indicators; estimates can be negative) ---
    "CC.EST":             ("Control of Corruption: Estimate", "Governance"),
    "GE.EST":             ("Government Effectiveness: Estimate", "Governance"),
    "PV.EST":             ("Political Stability and Absence of Violence/Terrorism: Estimate", "Governance"),
    "RL.EST":             ("Rule of Law: Estimate", "Governance"),
    "RQ.EST":             ("Regulatory Quality: Estimate", "Governance"),
    "VA.EST":             ("Voice and Accountability: Estimate", "Governance"),
}


class WorldBankESGConnector(Connector):
    source_id = "worldbank_esg"
    name = "World Bank Sovereign ESG"
    license_id = "cc-by-4.0"
    schedule = "0 6 * * 1"          # weekly (ESG data refreshes infrequently)
    attribution = "Source: World Bank, Sovereign ESG Data (CC BY 4.0)"
    homepage = "https://datatopics.worldbank.org/esg"
    BASE = "https://api.worldbank.org/v2"
    SOURCE = 75                     # ESG database id

    def discover(self) -> list[SeriesMeta]:
        return [
            SeriesMeta(f"worldbank_esg:{ind}", title, "A", None, None, pillar,
                       self.license_id, {"indicator": ind, "source": self.SOURCE,
                                         "pillar": pillar})
            for ind, (title, pillar) in INDICATORS.items()
        ]

    def _country_codes(self) -> set[str]:
        """ISO3 codes of real sovereign entities (cached).

        The /country endpoint mixes ~217 countries with ~79 aggregate regions
        (World, Arab World, "East Asia & Pacific (IDA & IBRD)", ...). Aggregates
        carry region.id == 'NA'; real countries carry a real region. We keep only
        the latter so this *Sovereign* ESG feed never publishes a region as a
        country. Falls back to None (= keep any 3-letter code) if the lookup fails.
        """
        cached = getattr(self, "_codes_cache", "unset")
        if cached != "unset":
            return cached
        codes = None
        try:
            payload = self._get(f"{self.BASE}/country",
                                {"format": "json", "per_page": 400})
            if isinstance(payload, list) and len(payload) > 1 and payload[1]:
                codes = {
                    c["id"] for c in payload[1]
                    if (c.get("region") or {}).get("id") != "NA" and c.get("id")
                }
        except Exception:
            codes = None
        self._codes_cache = codes
        return codes

    def _get(self, url: str, params: dict) -> dict:
        """GET with polite retries/backoff; returns parsed JSON."""
        last = None
        for attempt in range(4):
            try:
                r = requests.get(url, params=params, timeout=60,
                                 headers={"User-Agent": UA})
                if r.status_code in (429, 500, 502, 503, 504):
                    last = RuntimeError(f"HTTP {r.status_code}")
                    time.sleep(2 ** attempt)
                    continue
                r.raise_for_status()
                return r.json()
            except (requests.RequestException, ValueError) as e:
                last = e
                time.sleep(2 ** attempt)
        raise RuntimeError(f"World Bank ESG request failed: {url} ({last})")

    def fetch(self, since: Optional[dt.date] = None):
        # Incremental support: when `since` is given, only request years >= since.year.
        # The WB API caps `date` at the latest available year automatically.
        date_param = None
        if since is not None:
            date_param = f"{since.year}:{dt.date.today().year}"

        countries = self._country_codes()   # None => fall back to any ISO3-shaped code

        for ind, (title, pillar) in INDICATORS.items():
            url = f"{self.BASE}/country/all/indicator/{ind}"
            params = {"format": "json", "per_page": 20000, "source": self.SOURCE}
            if date_param:
                params["date"] = date_param

            payload = self._get(url, params)
            # Error responses come back as a single-element list with a "message" key.
            if not isinstance(payload, list) or len(payload) < 2 or not payload[1]:
                continue
            rows = payload[1]

            by_iso: dict[str, list] = {}
            for row in rows:
                val = row.get("value")
                if val is None:
                    continue
                try:
                    fval = float(val)
                except (TypeError, ValueError):
                    continue
                iso = row.get("countryiso3code") or ""
                # Country-level only: real ISO3 codes are 3 uppercase letters.
                # Aggregate regions (World, Arab World, ...) have an empty code,
                # and some IDA/IBRD aggregates (TEA, TEC, ...) have a 3-letter code
                # but are excluded via the sovereign-country whitelist.
                if len(iso) != 3 or not iso.isalpha():
                    continue
                if countries is not None and iso not in countries:
                    continue
                yr = row.get("date")
                if not (isinstance(yr, str) and yr.isdigit()):
                    continue
                sid = f"worldbank_esg:{ind}:{iso}"
                by_iso.setdefault(iso, []).append(
                    Observation(sid, dt.date(int(yr), 12, 31), fval, version="clean"))

            for iso, obs in by_iso.items():
                sid = f"worldbank_esg:{ind}:{iso}"
                meta = SeriesMeta(sid, f"{title} - {iso}", "A", None, iso, pillar,
                                  self.license_id,
                                  {"indicator": ind, "source": self.SOURCE,
                                   "pillar": pillar})
                yield meta, obs
