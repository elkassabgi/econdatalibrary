"""U.S. Bureau of Economic Analysis connector (NIPA: GDP & income; public domain).

BEA API GetData on the NIPA dataset. Each table row (SeriesCode) becomes a series;
quarterly and annual frequencies are pulled separately. Values are stored as BEA
reports them (commas stripped); the BEA unit is kept in series metadata.
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
API = "https://apps.bea.gov/api/data/"

# NIPA table -> short description (starter set of the most-used national accounts)
TABLES = {
    "T10105": "GDP & components, current-dollar levels",
    "T10106": "Real GDP & components, chained-dollar levels",
    "T10101": "Real GDP, percent change from preceding period",
    "T20100": "Personal income & its disposition",
}
FREQS = ["Q", "A"]


def _parse_val(s) -> Optional[float]:
    if s is None:
        return None
    s = str(s).replace(",", "").strip()
    if s in ("", "(NA)", "(D)", "...", "---", "NA", "n.a."):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _parse_tp(tp) -> Optional[dt.date]:
    tp = str(tp)
    try:
        if "Q" in tp:
            y, q = tp.split("Q")
            return dt.date(int(y), {"1": 1, "2": 4, "3": 7, "4": 10}[q], 1)
        if "M" in tp:
            y, m = tp.split("M")
            return dt.date(int(y), int(m), 1)
        if tp.isdigit():
            return dt.date(int(tp), 12, 31)
    except (ValueError, KeyError):
        return None
    return None


class BEAConnector(Connector):
    source_id = "bea"
    name = "U.S. Bureau of Economic Analysis"
    license_id = "us-public-domain"
    schedule = "0 6 * * *"
    attribution = "Source: U.S. Bureau of Economic Analysis (public domain)"
    homepage = "https://www.bea.gov"

    def discover(self):
        return [SeriesMeta(f"bea:{t}", d, "Q", None, "US", "macro", self.license_id,
                           {"table": t}) for t, d in TABLES.items()]

    def fetch(self, since: Optional[dt.date] = None):
        key = require("BEA_API_KEY")
        for table in TABLES:
            for freq in FREQS:
                r = requests.get(API, headers=UA, timeout=90, params={
                    "UserID": key, "method": "GetData", "datasetname": "NIPA",
                    "TableName": table, "Frequency": freq, "Year": "ALL", "ResultFormat": "JSON"})
                if r.status_code != 200:
                    continue
                api = r.json().get("BEAAPI", {})
                res = api.get("Results", {})
                if isinstance(res, list):
                    res = res[0] if res else {}
                if "Error" in api or (isinstance(res, dict) and "Error" in res):
                    continue
                rows = res.get("Data", []) if isinstance(res, dict) else []
                by_series: dict[str, dict] = {}
                for row in rows:
                    code = row.get("SeriesCode")
                    when = _parse_tp(row.get("TimePeriod"))
                    val = _parse_val(row.get("DataValue"))
                    if not code or when is None or val is None:
                        continue
                    sid = f"bea:{code}:{freq}"
                    rec = by_series.setdefault(sid, {"desc": row.get("LineDescription", code),
                                                     "unit": row.get("CL_UNIT"), "obs": []})
                    rec["obs"].append(Observation(sid, when, val, version="clean"))
                for sid, rec in by_series.items():
                    yield SeriesMeta(sid, f"{rec['desc']} ({freq})", freq, rec.get("unit"),
                                     "US", "macro", self.license_id, {"table": table}), rec["obs"]
