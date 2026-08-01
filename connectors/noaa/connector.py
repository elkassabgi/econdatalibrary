"""NOAA NCEI Climate Data Online connector (monthly climate, major US stations; public domain).

CDO v2 API (header `token`). Pulls GSOM (Global Summary of the Month) average temperature
and precipitation for a handful of major US city stations -- a climate-economics starter set.

SUPERSEDED 2026-08-01, AND NOT WIRED INTO ANY RUN (jobs/ingest_all.py does not list it). The
source is now served whole from the NCEI bulk archive -- 3,135,873 series, every station on
earth -- via updater/strategies/fetchers/noaa.py. This connector's ten stations x two datatypes
are a subset of that, and the ten catalogue rows it produced were the ONLY noaa rows that
existed: all ten keyed `noaa:GSOM:...`, a format the store has never used, so all ten were
listed and would not download. They also understated coverage, claiming 1990 for a New York
series the store holds from 1869.

Kept because it documents the CDO API path, which the bulk archive does not offer. Its id
builder is brought to the current format below so that running it can no longer re-create the
broken rows -- but prefer the bulk fetcher for anything real.
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
        # lowercase `gsom`, matching the store's dataset-qualified series_key. Uppercase GSOM
        # is what made every row this connector ever wrote undownloadable.
        return f"noaa:gsom:{station.split(':')[1]}:{dtid}"

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
