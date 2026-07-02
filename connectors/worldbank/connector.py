"""World Bank Open Data connector (macro; CC BY 4.0).

Representative of the SDMX-family / REST macro sources. No API key. Polite.
Series id format: worldbank:<indicator>:<ISO3>.
"""
from __future__ import annotations
from datetime import date
from typing import Optional

import requests

from connectors.base import Connector, SeriesMeta, Observation


class WorldBankConnector(Connector):
    source_id = "worldbank"
    name = "World Bank Open Data (WDI)"
    license_id = "cc-by-4.0"
    schedule = "0 6 * * 1"
    attribution = "Source: World Bank, World Development Indicators (CC BY 4.0)"
    homepage = "https://data.worldbank.org"
    BASE = "https://api.worldbank.org/v2"

    # Phase-0 starter set (the full list comes from configs/sources.yaml later).
    INDICATORS = {
        "NY.GDP.MKTP.CD": "GDP (current US$)",
        "FP.CPI.TOTL.ZG": "Inflation, consumer prices (annual %)",
        "SL.UEM.TOTL.ZS": "Unemployment, total (% of total labor force)",
    }

    def discover(self) -> list[SeriesMeta]:
        return [
            SeriesMeta(f"worldbank:{ind}", name, "A", None, None, "macro",
                       self.license_id, {"indicator": ind})
            for ind, name in self.INDICATORS.items()
        ]

    def fetch(self, since: Optional[date] = None):
        for ind, name in self.INDICATORS.items():
            url = f"{self.BASE}/country/all/indicator/{ind}?format=json&per_page=20000"
            r = requests.get(url, timeout=60, headers={"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"})
            r.raise_for_status()
            payload = r.json()
            rows = payload[1] if len(payload) > 1 and payload[1] else []
            by_series: dict[tuple, list] = {}
            for row in rows:
                if row.get("value") is None:
                    continue
                iso = row.get("countryiso3code") or (row.get("country") or {}).get("id")
                if not iso:
                    continue
                sid = f"worldbank:{ind}:{iso}"
                by_series.setdefault((sid, iso), []).append(
                    Observation(sid, date(int(row["date"]), 12, 31), float(row["value"]), version="clean"))
            for (sid, iso), obs in by_series.items():
                meta = SeriesMeta(sid, f"{name} - {iso}", "A", None, iso, "macro",
                                  self.license_id, {"indicator": ind})
                yield meta, obs
