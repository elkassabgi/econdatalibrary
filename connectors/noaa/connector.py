"""NOAA NCEI Climate Data Online connector (monthly climate, major US stations; public domain).

CDO v2 API (header `token`). Pulls GSOM (Global Summary of the Month) average temperature
and precipitation for a handful of major US city stations -- a climate-economics starter set.
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
from core.config import require  # noqa: E402

BASE = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
STATIONS = {
    "GHCND:USW00094728": "New York (Central Park)",
    "GHCND:USW00023174": "Los Angeles Intl",
    "GHCND:USW00094846": "Chicago O'Hare",
    "GHCND:USW00012960": "Houston Bush",
    "GHCND:USW00013743": "Washington DC (Reagan)",
}
DATATYPES = {"TAVG": "avg temperature (degC)", "PRCP": "precipitation (mm)"}


class NOAAConnector(Connector):
    source_id = "noaa"
    name = "NOAA NCEI Climate Data Online"
    license_id = "us-public-domain"
    schedule = "0 6 * * 1"
    attribution = "Source: NOAA NCEI (public domain)"
    homepage = "https://www.ncei.noaa.gov"

    def _sid(self, station, dtid):
        return f"noaa:GSOM:{station.split(':')[1]}:{dtid}"

    def discover(self):
        return [SeriesMeta(self._sid(st, dtid), f"{stn} - {dtn}", "M", None, "US", "climate",
                           self.license_id, {"station": st, "datatype": dtid})
                for st, stn in STATIONS.items() for dtid, dtn in DATATYPES.items()]

    def fetch(self, since: Optional[dt.date] = None):
        headers = {"token": require("NOAA_API_KEY")}
        windows = [(1990, 1999), (2000, 2009), (2010, 2019), (2020, dt.date.today().year)]
        for st, stn in STATIONS.items():
            for dtid, dtn in DATATYPES.items():
                obs = []
                for y0, y1 in windows:
                    params = {"datasetid": "GSOM", "stationid": st, "datatypeid": dtid,
                              "startdate": f"{y0}-01-01", "enddate": f"{y1}-12-31",
                              "units": "metric", "limit": 1000, "offset": 1}
                    try:
                        r = requests.get(BASE, headers=headers, params=params, timeout=60)
                        if r.status_code != 200:
                            continue
                        for rec in r.json().get("results", []):
                            try:
                                od = dt.datetime.strptime(rec.get("date", "")[:10], "%Y-%m-%d").date()
                            except (ValueError, TypeError):
                                continue
                            v = rec.get("value")
                            if v is None:
                                continue
                            obs.append(Observation(self._sid(st, dtid), od, float(v), version="clean"))
                    except Exception:
                        pass
                    time.sleep(0.3)
                if obs:
                    yield SeriesMeta(self._sid(st, dtid), f"{stn} - {dtn}", "M", None, "US",
                                     "climate", self.license_id, {"station": st, "datatype": dtid}), obs
