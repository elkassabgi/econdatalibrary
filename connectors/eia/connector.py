"""U.S. Energy Information Administration connector (energy prices/production; public domain).

Uses the EIA v2 `/v2/seriesid/{ID}` compatibility route with curated v1-style series IDs
(verified live 2026-06-01). Paginates with length/offset for full history. Daily/weekly/
monthly periods all handled.

ToS (recorded in configs/sources.yaml): re-serving allowed; attribute to EIA; no EIA logo;
do NOT claim EIA as source for *modified* values -> derived transforms must be labelled
"derived from EIA data". Here we store unmodified values, so "Source: EIA" is correct.
"""
from __future__ import annotations
import datetime as dt
import os
import sys
from typing import Optional

import requests

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
from connectors.base import Connector, SeriesMeta, Observation  # noqa: E402
from core.config import require  # noqa: E402

UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

# series_id -> (title, frequency)
SERIES = {
    "PET.RWTC.D":                  ("WTI crude oil spot price, Cushing OK ($/bbl)", "D"),
    "PET.RBRTE.D":                 ("Brent crude oil spot price, Europe ($/bbl)", "D"),
    "NG.RNGWHHD.D":                ("Henry Hub natural gas spot price ($/MMBtu)", "D"),
    "PET.EMM_EPM0_PTE_NUS_DPG.W":  ("US regular gasoline retail price ($/gal)", "W"),
    "PET.EMD_EPD2D_PTE_NUS_DPG.W": ("US diesel retail price ($/gal)", "W"),
    "PET.WCRFPUS2.W":              ("US crude oil field production (thousand bbl/day)", "W"),
    "ELEC.PRICE.US-ALL.M":         ("US electricity retail price, all sectors (cents/kWh)", "M"),
}


def _parse_period(p) -> Optional[dt.date]:
    p = str(p)
    try:
        if "-Q" in p:
            y, q = p.split("-Q")
            return dt.date(int(y), {"1": 3, "2": 6, "3": 9, "4": 12}[q], 1)
        parts = p.split("-")
        if len(parts) == 3:
            return dt.date(int(parts[0]), int(parts[1]), int(parts[2]))
        if len(parts) == 2:
            return dt.date(int(parts[0]), int(parts[1]), 1)
        if len(parts) == 1 and parts[0].isdigit():
            return dt.date(int(parts[0]), 12, 31)
    except (ValueError, KeyError):
        return None
    return None


class EIAConnector(Connector):
    source_id = "eia"
    name = "U.S. Energy Information Administration"
    license_id = "us-public-domain"
    schedule = "0 6 * * *"
    attribution = "Source: U.S. Energy Information Administration (public domain)"
    homepage = "https://www.eia.gov"

    def discover(self):
        return [SeriesMeta(f"eia:{sid}", t, f, None, "US", "energy", self.license_id,
                           {"eia_id": sid}) for sid, (t, f) in SERIES.items()]

    def fetch(self, since: Optional[dt.date] = None):
        key = require("EIA_API_KEY")
        for sid, (title, freq) in SERIES.items():
            obs, offset = [], 0
            while True:
                r = requests.get(f"https://api.eia.gov/v2/seriesid/{sid}", headers=UA, timeout=60, params={
                    "api_key": key, "length": 5000, "offset": offset,
                    "sort[0][column]": "period", "sort[0][direction]": "asc"})
                if r.status_code != 200:
                    break
                data = r.json().get("response", {}).get("data", [])
                if not data:
                    break
                for d in data:
                    v = d.get("value", d.get("price"))
                    when = _parse_period(d.get("period"))
                    if v is None or when is None:
                        continue
                    try:
                        obs.append(Observation(f"eia:{sid}", when, float(v), version="clean"))
                    except (ValueError, TypeError):
                        continue
                if len(data) < 5000:
                    break
                offset += 5000
            if obs:
                yield SeriesMeta(f"eia:{sid}", title, freq, None, "US", "energy",
                                 self.license_id, {"eia_id": sid}), obs
