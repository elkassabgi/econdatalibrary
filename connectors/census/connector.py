"""U.S. Census Bureau connector -- Economic Indicators Time Series (EITS; public domain).

The Census API exposes dozens of timeseries datasets. The trade datasets
(timeseries/intltrade/...) are commodity-granular and need summing to produce a
top-line number; the cleaner, headline macro series live under
``timeseries/eits/`` (the Economic Indicators program -- the same numbers the
Bureau puts in its monthly press releases).

Each EITS row is one ``cell_value`` keyed by ``category_code`` (the indicator,
e.g. retail trade total) x ``data_type_code`` (the measure, e.g. sales level vs
month-over-month % change) x ``seasonally_adj`` x ``geo_level_code`` x ``time``.
The vocabularies differ per dataset and the same code can appear for several
census regions, both SA and NSA, and as a paired standard-error row, so we do
NOT bulk-ingest. Instead we curate a registry where every entry fixes exactly
one US national series, then filter the response down to it in Python (defensive
against the API returning extra rows). ``error_data=no`` drops the SE rows.

Series id format: ``census:<dataset>:<category_code>:<data_type_code>:<sa|nsa>``.

API notes used here:
  - ``time=from+YYYY-MM`` returns full history open-ended (perfect for `since`).
  - Some datasets demand a ``time_slot_id`` and/or ``for=`` predicate even when
    we only want the US total; we pass both harmlessly and filter to geo US.
  - 500 calls/day/key is plenty: one call per dataset (not per series).

No bulk download; ~22 curated series across 6 datasets. Public domain (17 USC 105).
"""
from __future__ import annotations

import datetime as dt
import os
import sys
import time as _time
from typing import Optional

import requests

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
from connectors.base import Connector, SeriesMeta, Observation  # noqa: E402
from core.config import require  # noqa: E402

BASE = "https://api.census.gov/data/timeseries/eits"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
START = "1992-01"          # open-ended history floor for the full pull


# --- Curated registry -------------------------------------------------------
# One dict per published headline indicator. `sel` is the exact dimension tuple
# that isolates the single US national series inside that dataset's response.
#   dataset        EITS endpoint slug
#   category_code  the indicator code
#   data_type_code the measure code
#   sa             "yes"/"no" (Census `seasonally_adj`)
#   geo            geo_level_code to keep ("US"); None if the dataset has no geo col
#   needs_geo_pred datasets that 400 without a `for=`/`time_slot_id` predicate
#   needs_tsid     datasets that 400 without `time_slot_id`
# Units/frequency are recorded for the catalog; values are passed through as-is.
SERIES: list[dict] = [
    # ---- Advance Monthly Retail Trade & Food Services (MARTS) ----
    dict(dataset="marts", category_code="44X72", data_type_code="SM", sa="yes",
         geo=None, needs_geo_pred=False, needs_tsid=False, freq="M",
         unit="Mil. $", title="Retail & food services sales, total (advance, SA)"),
    dict(dataset="marts", category_code="44W72", data_type_code="SM", sa="yes",
         geo=None, needs_geo_pred=False, needs_tsid=False, freq="M",
         unit="Mil. $", title="Retail & food services sales ex-motor vehicle & parts (advance, SA)"),
    dict(dataset="marts", category_code="44000", data_type_code="SM", sa="yes",
         geo=None, needs_geo_pred=False, needs_tsid=False, freq="M",
         unit="Mil. $", title="Retail trade sales, total (advance, SA)"),
    dict(dataset="marts", category_code="722", data_type_code="SM", sa="yes",
         geo=None, needs_geo_pred=False, needs_tsid=False, freq="M",
         unit="Mil. $", title="Food services & drinking places sales (advance, SA)"),
    dict(dataset="marts", category_code="441", data_type_code="SM", sa="yes",
         geo=None, needs_geo_pred=False, needs_tsid=False, freq="M",
         unit="Mil. $", title="Motor vehicle & parts dealers sales (advance, SA)"),

    # ---- Monthly Retail Trade (MRTS, final/benchmarked) ----
    dict(dataset="mrts", category_code="44X72", data_type_code="SM", sa="yes",
         geo=None, needs_geo_pred=False, needs_tsid=True, freq="M",
         unit="Mil. $", title="Retail & food services sales, total (final, SA)"),

    # ---- Construction Spending (Value in Place, VIP) ----
    dict(dataset="vip", category_code="00XX", data_type_code="T", sa="no",
         geo=None, needs_geo_pred=False, needs_tsid=True, freq="M",
         unit="Mil. $", title="Total construction spending, all (NSA monthly)"),
    dict(dataset="vip", category_code="01XX", data_type_code="T", sa="no",
         geo=None, needs_geo_pred=False, needs_tsid=True, freq="M",
         unit="Mil. $", title="Private construction spending (NSA monthly)"),
    dict(dataset="vip", category_code="02XX", data_type_code="T", sa="no",
         geo=None, needs_geo_pred=False, needs_tsid=True, freq="M",
         unit="Mil. $", title="Public construction spending (NSA monthly)"),
    dict(dataset="vip", category_code="00XX", data_type_code="V", sa="no",
         geo=None, needs_geo_pred=False, needs_tsid=True, freq="M",
         unit="Mil. $", title="Total construction spending, all (NSA, value)"),

    # ---- New Residential Construction (RESCONST): starts / permits / completions ----
    dict(dataset="resconst", category_code="STARTS", data_type_code="TOTAL", sa="no",
         geo="US", needs_geo_pred=False, needs_tsid=True, freq="M",
         unit="Thous. units", title="Housing starts, total (US, NSA)"),
    dict(dataset="resconst", category_code="STARTS", data_type_code="SINGLE", sa="no",
         geo="US", needs_geo_pred=False, needs_tsid=True, freq="M",
         unit="Thous. units", title="Housing starts, single-family (US, NSA)"),
    dict(dataset="resconst", category_code="PERMITS", data_type_code="TOTAL", sa="no",
         geo="US", needs_geo_pred=False, needs_tsid=True, freq="M",
         unit="Thous. units", title="Building permits, total (US, NSA)"),
    dict(dataset="resconst", category_code="COMPLETIONS", data_type_code="TOTAL", sa="no",
         geo="US", needs_geo_pred=False, needs_tsid=True, freq="M",
         unit="Thous. units", title="Housing completions, total (US, NSA)"),

    # ---- New Residential Sales (RESSALES) ----
    dict(dataset="ressales", category_code="ASOLD", data_type_code="TOTAL", sa="yes",
         geo="US", needs_geo_pred=False, needs_tsid=True, freq="M",
         unit="Thous. units (SAAR)", title="New single-family houses sold (US, SAAR)"),
    dict(dataset="ressales", category_code="FORSALE", data_type_code="TOTAL", sa="no",
         geo="US", needs_geo_pred=False, needs_tsid=True, freq="M",
         unit="Thous. units", title="New single-family houses for sale (US, NSA)"),
    dict(dataset="ressales", category_code="SOLD", data_type_code="MEDIAN", sa="no",
         geo="US", needs_geo_pred=False, needs_tsid=True, freq="M",
         unit="$", title="Median sales price, new houses sold (US)"),

    # ---- Advance Durable Goods (ADVM3): new orders / shipments, total & ex-defense ----
    dict(dataset="advm3", category_code="DXD", data_type_code="NO", sa="yes",
         geo="US", needs_geo_pred=True, needs_tsid=True, freq="M",
         unit="Mil. $", title="Durable goods new orders, total (advance, SA)"),
    dict(dataset="advm3", category_code="DXD", data_type_code="VS", sa="yes",
         geo="US", needs_geo_pred=True, needs_tsid=True, freq="M",
         unit="Mil. $", title="Durable goods shipments, total (advance, SA)"),

    # ---- Manufacturers' Shipments, Inventories & Orders (M3, full) ----
    dict(dataset="m3", category_code="TCG", data_type_code="VS", sa="yes",
         geo="US", needs_geo_pred=True, needs_tsid=True, freq="M",
         unit="Mil. $", title="Manufacturers' shipments, all manufacturing (SA)"),
    dict(dataset="m3", category_code="TCG", data_type_code="NO", sa="yes",
         geo="US", needs_geo_pred=True, needs_tsid=True, freq="M",
         unit="Mil. $", title="Manufacturers' new orders, all manufacturing (SA)"),
    dict(dataset="m3", category_code="TCG", data_type_code="TI", sa="yes",
         geo="US", needs_geo_pred=True, needs_tsid=True, freq="M",
         unit="Mil. $", title="Manufacturers' total inventories, all manufacturing (SA)"),
]


def _sid(s: dict) -> str:
    sa = "sa" if s["sa"] == "yes" else "nsa"
    return f"census:{s['dataset']}:{s['category_code']}:{s['data_type_code']}:{sa}"


def _meta(s: dict) -> SeriesMeta:
    return SeriesMeta(
        series_id=_sid(s),
        title=s["title"],
        frequency=s["freq"],
        unit=s["unit"],
        geography="US",
        category="macro",
        license_id="us-public-domain",
        metadata={
            "dataset": s["dataset"],
            "category_code": s["category_code"],
            "data_type_code": s["data_type_code"],
            "seasonally_adj": s["sa"],
            "program": "Census Economic Indicators Time Series (EITS)",
        },
    )


def _to_date(t: str) -> Optional[dt.date]:
    """Census `time` is 'YYYY-MM' (occasionally 'YYYY-Qn' or 'YYYY'). Map to a date."""
    t = t.strip()
    try:
        if "-Q" in t:                       # quarterly -> first month of quarter
            y, q = t.split("-Q")
            return dt.date(int(y), (int(q) - 1) * 3 + 1, 1)
        if "-" in t:                        # YYYY-MM
            y, m = t.split("-")[:2]
            return dt.date(int(y), int(m), 1)
        return dt.date(int(t), 1, 1)        # annual
    except (ValueError, IndexError):
        return None


class CensusConnector(Connector):
    source_id = "census"
    name = "U.S. Census Bureau"
    license_id = "us-public-domain"
    schedule = "0 7 * * *"            # daily; EITS releases land on business mornings
    attribution = "Source: U.S. Census Bureau, Economic Indicators Time Series (public domain)"
    homepage = "https://www.census.gov"

    def discover(self) -> list[SeriesMeta]:
        return [_meta(s) for s in SERIES]

    # --- HTTP with simple retry/backoff ---
    def _get(self, dataset: str, params: dict) -> list[list]:
        url = f"{BASE}/{dataset}"
        last = None
        for attempt in range(4):
            try:
                r = requests.get(url, params=params, headers=UA, timeout=90)
            except requests.RequestException as e:
                last = e
                _time.sleep(2 * (attempt + 1))
                continue
            if r.status_code == 200:
                return r.json()
            # 204 = no rows for this query window; treat as empty, don't retry
            if r.status_code == 204:
                return []
            if r.status_code in (429, 500, 502, 503, 504):
                last = RuntimeError(f"{r.status_code} {r.text[:200]}")
                _time.sleep(2 * (attempt + 1))
                continue
            # other 4xx: not transient
            raise RuntimeError(f"Census {dataset} HTTP {r.status_code}: {r.text[:300]}")
        raise RuntimeError(f"Census {dataset} failed after retries: {last}")

    def fetch(self, since: Optional[dt.date] = None):
        key = require("CENSUS_API_KEY")
        start = f"{since.year:04d}-{since.month:02d}" if since else START

        # Group curated series by dataset so we make one API call per dataset.
        by_ds: dict[str, list[dict]] = {}
        for s in SERIES:
            by_ds.setdefault(s["dataset"], []).append(s)

        for dataset, specs in by_ds.items():
            params = {
                "get": "cell_value,category_code,data_type_code,seasonally_adj,geo_level_code",
                "time": f"from {start}",
                "error_data": "no",
                "key": key,
            }
            # Datasets that require a disambiguating predicate even for the US total.
            if any(sp["needs_tsid"] for sp in specs):
                params["time_slot_id"] = "0"
            if any(sp["needs_geo_pred"] for sp in specs):
                params["for"] = "us:*"

            try:
                rows = self._get(dataset, params)
            except RuntimeError as e:
                # One bad dataset shouldn't sink the rest; emit nothing for it.
                sys.stderr.write(f"[census] {dataset} skipped: {e}\n")
                continue
            if not rows or len(rows) < 2:
                continue

            header = rows[0]
            idx = {name: i for i, name in enumerate(header)}
            # Required columns; if Census reshapes the payload, skip the dataset.
            need = ("cell_value", "category_code", "data_type_code", "seasonally_adj")
            if not all(c in idx for c in need):
                sys.stderr.write(f"[census] {dataset} unexpected columns: {header}\n")
                continue
            has_geo = "geo_level_code" in idx
            i_time = idx.get("time")

            for s in specs:
                meta = _meta(s)
                seen: dict[dt.date, float] = {}
                for row in rows[1:]:
                    if row[idx["category_code"]] != s["category_code"]:
                        continue
                    if row[idx["data_type_code"]] != s["data_type_code"]:
                        continue
                    if row[idx["seasonally_adj"]] != s["sa"]:
                        continue
                    if s["geo"] is not None:
                        if not has_geo or row[idx["geo_level_code"]] != s["geo"]:
                            continue
                    # locate the time column (Census appends it; usually present)
                    tval = row[i_time] if i_time is not None and i_time < len(row) else None
                    if tval is None:
                        # fall back: find a YYYY-MM-looking token in the row
                        tval = next((c for c in row if isinstance(c, str)
                                     and len(c) >= 6 and c[:4].isdigit() and "-" in c), None)
                    d = _to_date(tval) if tval else None
                    if d is None:
                        continue
                    raw = row[idx["cell_value"]]
                    if raw in (None, "", "NA", "(NA)", "(S)", "(D)", "(Z)", "N", "-"):
                        continue
                    try:
                        val = float(str(raw).replace(",", ""))
                    except (ValueError, TypeError):
                        continue
                    seen[d] = val          # last write wins (defensive vs dup rows)

                obs = [Observation(meta.series_id, d, v, version="clean")
                       for d, v in sorted(seen.items())]
                if obs:
                    yield meta, obs
