"""Penn World Table 11.0 connector (cross-country macro panel; CC BY 4.0).

PWT ships as a single Excel workbook (pwt110.xlsx) hosted on the GGDC Dataverse
(DOI 10.34894/FABVLR). There is no incremental API: each release is a full annual
panel covering ~185 economies, 1950-2023. We download the workbook once, parse the
'Data' sheet with pandas, and emit one annual series per (economy x key variable).

No API key required. We download a single ~6 MB file per run, so `since` only gates
which observations we keep (the file itself is always fetched in full).

Series id format: penn_world_table:<variable>:<ISO3>   e.g. penn_world_table:rgdpe:USA
"""
from __future__ import annotations

import datetime as dt
import io
import os
import sys
import time
from typing import Optional

import pandas as pd
import requests

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
from connectors.base import Connector, SeriesMeta, Observation  # noqa: E402

# Canonical, stable download: the GGDC Dataverse datafile id for pwt110.xlsx.
# (The /api/access/datafile/<id> endpoint 302-redirects to the object store and
#  serves the workbook with Content-Disposition filename pwt110.xlsx.)
DATA_URL = "https://dataverse.nl/api/access/datafile/554105"

UA = "Econ-Fin Data Library admin@hfdatalibrary.com"

# Key variables to publish, with their PWT Legend definition and a compact unit string.
# 'ck' is an index (USA=1, capital *services*); the rest are levels or counts.
VARIABLES = {
    "rgdpe":  ("Expenditure-side real GDP at chained PPPs", "mil. 2021US$"),
    "rgdpo":  ("Output-side real GDP at chained PPPs", "mil. 2021US$"),
    "rgdpna": ("Real GDP at constant 2021 national prices", "mil. 2021US$"),
    "pop":    ("Population", "millions of persons"),
    "emp":    ("Number of persons engaged", "millions of persons"),
    "ck":     ("Capital services levels at current PPPs (USA=1)", "index, USA=1"),
}

# Curated starter set: high-value, heavily-studied economies (G20 core + others).
# 10 economies x 6 variables = 60 series -- the top of the recommended starter range.
ECONOMIES = {
    "USA": "United States",
    "CHN": "China",
    "JPN": "Japan",
    "DEU": "Germany",
    "GBR": "United Kingdom",
    "FRA": "France",
    "IND": "India",
    "BRA": "Brazil",
    "CAN": "Canada",
    "KOR": "Republic of Korea",
}


class PennWorldTableConnector(Connector):
    source_id = "penn_world_table"
    name = "Penn World Table 11.0"
    license_id = "cc-by-4.0"
    schedule = "0 7 1 * *"  # monthly check; PWT publishes a new version every ~1-2 years
    attribution = (
        "Source: Feenstra, Robert C., Robert Inklaar and Marcel P. Timmer (2015), "
        "'The Next Generation of the Penn World Table', American Economic Review, "
        "105(10), 3150-3182 -- Penn World Table 11.0 (CC BY 4.0)"
    )
    homepage = "https://www.rug.nl/ggdc/productivity/pwt"

    def _series_id(self, var: str, iso: str) -> str:
        return f"{self.source_id}:{var}:{iso}"

    def _meta(self, var: str, iso: str) -> SeriesMeta:
        definition, unit = VARIABLES[var]
        country = ECONOMIES[iso]
        return SeriesMeta(
            series_id=self._series_id(var, iso),
            title=f"{definition} - {country}",
            frequency="A",
            unit=unit,
            geography=iso,
            category="macro",
            license_id=self.license_id,
            metadata={"variable": var, "definition": definition,
                      "country": country, "pwt_version": "11.0"},
        )

    def discover(self) -> list[SeriesMeta]:
        return [self._meta(var, iso) for var in VARIABLES for iso in ECONOMIES]

    def _download(self) -> pd.DataFrame:
        """Fetch the workbook with retries and return the parsed 'Data' sheet."""
        last_exc: Optional[Exception] = None
        for attempt in range(4):
            try:
                r = requests.get(DATA_URL, timeout=180,
                                 headers={"User-Agent": UA}, allow_redirects=True)
                r.raise_for_status()
                return pd.read_excel(io.BytesIO(r.content),
                                     sheet_name="Data", engine="openpyxl")
            except Exception as exc:  # network / parse / transient 5xx
                last_exc = exc
                if attempt < 3:
                    time.sleep(2 ** attempt)  # 1s, 2s, 4s backoff
        raise RuntimeError(f"PWT download failed after retries: {last_exc}")

    def fetch(self, since: Optional[dt.date] = None):
        df = self._download()
        df = df[df["countrycode"].isin(ECONOMIES)]
        min_year = since.year if since else None

        for iso in ECONOMIES:
            sub = df[df["countrycode"] == iso]
            if sub.empty:
                continue
            for var in VARIABLES:
                if var not in sub.columns:
                    continue
                sid = self._series_id(var, iso)
                obs = []
                for year, raw in zip(sub["year"], sub[var]):
                    if pd.isna(raw) or pd.isna(year):
                        continue  # skip missing values
                    try:
                        val = float(raw)
                        yr = int(year)
                    except (ValueError, TypeError):
                        continue  # skip non-numeric
                    if min_year is not None and yr < min_year:
                        continue
                    obs.append(Observation(sid, dt.date(yr, 12, 31), val,
                                           version="clean"))
                if obs:
                    obs.sort(key=lambda o: o.obs_date)
                    yield self._meta(var, iso), obs
