"""U.S. Bureau of Labor Statistics connector (CPI, employment, wages; public domain).

BLS API v2: POST up to 50 series IDs / request, <=20 years / request, 500 req/day with
a registration key. We pull here instead of via FRED (FRED's own feed isn't re-serveable).
Starter set of high-confidence series; the API reports per-series status so unknown IDs
surface as zero-coverage rather than crashing. Full history can be added by chunking years.
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

API = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

SERIES = {
    "CUUR0000SA0":    "CPI-U: All items, US city avg (NSA)",
    "CUUR0000SA0L1E": "CPI-U: All items less food & energy (NSA)",
    "LNS14000000":    "Unemployment rate, 16+ (SA, %)",
    "LNS11300000":    "Labor force participation rate (SA, %)",
    "LNS12000000":    "Civilian employment level (SA, thousands)",
    "LNS13000000":    "Unemployment level (SA, thousands)",
    "CES0000000001":  "Total nonfarm employment (SA, thousands)",
    "CES0500000003":  "Avg hourly earnings, total private (SA, $)",
    "CES0500000002":  "Avg weekly hours, total private (SA)",
}


class BLSConnector(Connector):
    source_id = "bls"
    name = "U.S. Bureau of Labor Statistics"
    license_id = "us-public-domain"
    schedule = "0 6 * * *"
    attribution = "Source: U.S. Bureau of Labor Statistics (public domain)"
    homepage = "https://www.bls.gov"

    def discover(self):
        return [SeriesMeta(f"bls:{sid}", title, "M", None, "US", "macro",
                           self.license_id, {"bls_id": sid}) for sid, title in SERIES.items()]

    def fetch(self, since: Optional[dt.date] = None):
        key = require("BLS_API_KEY")
        endyear = dt.date.today().year
        startyear = endyear - 19          # BLS v2 allows up to 20 years per request
        ids = list(SERIES)
        for i in range(0, len(ids), 50):  # API cap: 50 series per request
            chunk = ids[i:i + 50]
            r = requests.post(API, timeout=60, json={
                "seriesid": chunk, "startyear": str(startyear),
                "endyear": str(endyear), "registrationkey": key})
            r.raise_for_status()
            j = r.json()
            if j.get("status") != "REQUEST_SUCCEEDED":
                raise RuntimeError(f"BLS error: {j.get('status')} {j.get('message')}")
            for s in j.get("Results", {}).get("series", []):
                sid = s["seriesID"]
                obs = []
                for d in s.get("data", []):
                    per = d.get("period", "")
                    if not (per.startswith("M") and per[1:].isdigit() and 1 <= int(per[1:]) <= 12):
                        continue          # skip annual (M13)/quarterly/semiannual rows
                    try:
                        val = float(d["value"])
                    except (ValueError, TypeError):
                        continue
                    obs.append(Observation(f"bls:{sid}", dt.date(int(d["year"]), int(per[1:]), 1),
                                           val, version="clean"))
                if obs:
                    yield SeriesMeta(f"bls:{sid}", SERIES.get(sid, sid), "M", None, "US",
                                     "macro", self.license_id, {"bls_id": sid}), obs
