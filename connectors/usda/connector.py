"""USDA NASS Quick Stats connector (US agriculture; public domain).

Quick Stats api_GET endpoint (key in query). Curated starter: corn/soybeans/wheat
production + prices received, national annual. Each NASS short_desc becomes a series.
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

API = "https://quickstats.nass.usda.gov/api/api_GET/"
QUERIES = [
    ("CORN", "PRODUCTION"), ("CORN", "PRICE RECEIVED"),
    ("SOYBEANS", "PRODUCTION"), ("SOYBEANS", "PRICE RECEIVED"),
    ("WHEAT", "PRODUCTION"), ("WHEAT", "PRICE RECEIVED"),
]
_SKIP = {"", "(D)", "(NA)", "(Z)", "(X)", "(S)"}


def slug(s):
    return (s.lower().replace(", ", "_").replace(" - ", "_").replace(" ", "_")
            .replace(",", "").replace("/", "_").replace("__", "_"))


class USDAConnector(Connector):
    source_id = "usda"
    name = "USDA NASS Quick Stats"
    license_id = "us-public-domain"
    schedule = "0 6 * * 1"
    attribution = "Source: USDA NASS (public domain)"
    homepage = "https://quickstats.nass.usda.gov"

    def discover(self):
        return [SeriesMeta(f"usda:{c}:{s}", f"{c} {s}", "A", None, "US", "agriculture",
                           self.license_id, {"commodity": c, "stat": s}) for c, s in QUERIES]

    def fetch(self, since: Optional[dt.date] = None):
        key = require("USDA_API_KEY")
        for commodity, stat in QUERIES:
            params = {"key": key, "source_desc": "SURVEY", "commodity_desc": commodity,
                      "statisticcat_desc": stat, "agg_level_desc": "NATIONAL", "format": "JSON"}
            try:
                r = requests.get(API, params=params, timeout=60)
                if r.status_code != 200:
                    continue
                data = r.json().get("data", [])
            except Exception:
                continue
            series = {}  # sid -> (short_desc, {date: value})
            for rec in data:
                if rec.get("reference_period_desc") != "YEAR":
                    continue
                short = rec.get("short_desc", "")
                v = str(rec.get("Value", "")).replace(",", "").strip()
                if v in _SKIP:
                    continue
                try:
                    fv = float(v)
                    od = dt.date(int(rec["year"]), 12, 31)
                except (ValueError, TypeError, KeyError):
                    continue
                sid = f"usda:{slug(short)}"
                series.setdefault(sid, (short, {}))[1][od] = fv
            for sid, (short, dv) in series.items():
                obs = [Observation(sid, d, val, version="clean") for d, val in sorted(dv.items())]
                if obs:
                    yield SeriesMeta(sid, short, "A", None, "US", "agriculture",
                                     self.license_id, {"commodity": commodity, "stat": stat}), obs
