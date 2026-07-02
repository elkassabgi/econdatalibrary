"""Our World in Data (OWID) connector -- climate / energy / economic indicators.

OWID publishes every chart through a stable "Grapher" data API. For a chart with
slug <slug> the full long-format data is one CSV:

    https://ourworldindata.org/grapher/<slug>.csv?csvType=full&useColumnShortNames=true

and the matching column/citation metadata is one JSON:

    https://ourworldindata.org/grapher/<slug>.metadata.json?csvType=full&useColumnShortNames=true

The CSV is tidy: columns are  entity, code, year, <one-or-more value columns>
(plus the occasional non-data helper column such as `owid_region`, which we ignore).
`code` is ISO alpha-3 for countries and an OWID code for aggregates (World = OWID_WRL,
EU-27 = OWID_EU27). There is no incremental endpoint, so each run downloads the full
CSV per chart and `since` only gates which observations we keep.

LICENSE NOTE -- OWID's own work (their text + the database) is CC BY 4.0, which is why
this source is re-serveable. But almost every chart is built on *upstream third-party*
data (Global Carbon Budget, Ember, the Energy Institute, the World Bank, ...). The repo
treats OWID as an "upstream 3rd-party" carve-out, so we capture each chart's upstream
citation in the series metadata and reproduce OWID's full credit line in attribution.

Series id format:
    owid:<slug>:<code>                     (charts with a single value column)
    owid:<slug>:<value_column>:<code>      (charts with multiple value columns)

Starter set: 8 high-value climate/energy/economic charts x a curated set of 8 core
economies & aggregates (World, US, China, India, EU-27, Germany, Japan, UK).
Quality over completeness: ~64 series, all annual, no API key required.
"""
from __future__ import annotations

import csv
import datetime as dt
import io
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
BASE = "https://ourworldindata.org/grapher"

# Columns that appear in some CSVs but are not the chart's subject data.
_NON_DATA_COLS = {"entity", "code", "year", "day", "date", "owid_region"}

# Curated entities (OWID `code`) -> display name. ISO3 for countries; OWID codes for
# the aggregates. Chosen for analytical value and broad cross-chart coverage.
ENTITIES: dict[str, str] = {
    "OWID_WRL": "World",
    "USA": "United States",
    "CHN": "China",
    "IND": "India",
    "OWID_EU27": "European Union (27)",
    "DEU": "Germany",
    "JPN": "Japan",
    "GBR": "United Kingdom",
}

# Curated charts. For each slug we pin the exact value column(s) we publish so that
# helper/companion columns OWID sometimes ships (e.g. population_historical alongside a
# poverty rate) never leak into the catalog. `cols=None` means "auto-detect the single
# value column". `category` + a fallback unit are recorded for the catalog; richer
# per-column title/unit/citation come from the chart's metadata.json at fetch time.
CHARTS: list[dict] = [
    {"slug": "annual-co2-emissions-per-country",
     "cols": ["emissions_total"], "category": "climate", "unit": "tonnes"},
    {"slug": "co2-emissions-per-capita",
     "cols": ["emissions_total_per_capita"], "category": "climate", "unit": "tonnes per person"},
    {"slug": "cumulative-co2-emissions",
     "cols": ["cumulative_emissions_total"], "category": "climate", "unit": "tonnes"},
    {"slug": "co2-intensity",
     "cols": ["emissions_total_per_gdp"], "category": "climate", "unit": "kg per $ of GDP"},
    {"slug": "per-capita-energy-use",
     "cols": ["primary_energy_consumption_per_capita__kwh"], "category": "energy", "unit": "kWh per person"},
    {"slug": "low-carbon-share-energy",
     "cols": ["low_carbon_energy__pct_equivalent_primary_energy"], "category": "energy", "unit": "% of primary energy"},
    {"slug": "renewable-share-energy",
     "cols": ["renewables__pct_equivalent_primary_energy"], "category": "energy", "unit": "% of primary energy"},
    {"slug": "national-gdp-constant-usd-wb",
     "cols": ["ny_gdp_mktp_kd"], "category": "macro", "unit": "constant 2015 US$"},
]


class OWIDConnector(Connector):
    source_id = "owid"
    name = "Our World in Data"
    license_id = "cc-by-4.0"
    schedule = "0 7 * * 1"  # weekly; OWID refreshes datasets on irregular cadences
    attribution = (
        "Source: Our World in Data (CC BY 4.0). Each series is built on upstream "
        "third-party data -- see the per-series citation in metadata for the original "
        "provider (e.g. Global Carbon Budget, Energy Institute, World Bank)."
    )
    homepage = "https://ourworldindata.org"

    # ----- id / metadata helpers ------------------------------------------------
    def _series_id(self, slug: str, col: str, code: str, multi: bool) -> str:
        return f"{self.source_id}:{slug}:{col}:{code}" if multi else f"{self.source_id}:{slug}:{code}"

    def _meta(self, chart: dict, col: str, code: str, multi: bool,
              colmeta: Optional[dict] = None) -> SeriesMeta:
        colmeta = colmeta or {}
        entity = ENTITIES.get(code, code)
        title = colmeta.get("titleShort") or colmeta.get("titleLong") or chart["slug"]
        unit = colmeta.get("unit") or chart.get("unit")
        meta = {
            "slug": chart["slug"],
            "value_column": col,
            "entity": entity,
            "chart_url": f"https://ourworldindata.org/grapher/{chart['slug']}",
        }
        if colmeta.get("citationShort"):
            meta["citation"] = colmeta["citationShort"]            # upstream credit
        if colmeta.get("descriptionShort"):
            meta["description"] = colmeta["descriptionShort"]
        if colmeta.get("timespan"):
            meta["timespan"] = colmeta["timespan"]
        if colmeta.get("shortUnit"):
            meta["short_unit"] = colmeta["shortUnit"]
        return SeriesMeta(
            series_id=self._series_id(chart["slug"], col, code, multi),
            title=f"{title} - {entity}",
            frequency="A",
            unit=unit,
            geography=code,
            category=chart["category"],
            license_id=self.license_id,
            metadata=meta,
        )

    def discover(self) -> list[SeriesMeta]:
        out: list[SeriesMeta] = []
        for chart in CHARTS:
            cols = chart["cols"]
            multi = len(cols) > 1
            for col in cols:
                for code in ENTITIES:
                    out.append(self._meta(chart, col, code, multi))
        return out

    # ----- network --------------------------------------------------------------
    def _get(self, url: str, *, parse_json: bool):
        """GET with polite UA + exponential-backoff retries (1s, 2s, 4s)."""
        last_exc: Optional[Exception] = None
        for attempt in range(4):
            try:
                r = requests.get(url, headers={"User-Agent": UA}, timeout=120)
                r.raise_for_status()
                return r.json() if parse_json else r.text
            except Exception as exc:  # network / transient 5xx / bad json
                last_exc = exc
                if attempt < 3:
                    time.sleep(2 ** attempt)
        raise RuntimeError(f"OWID request failed after retries [{url}]: {last_exc}")

    def _chart_csv_url(self, slug: str) -> str:
        return f"{BASE}/{slug}.csv?csvType=full&useColumnShortNames=true"

    def _chart_meta_url(self, slug: str) -> str:
        return f"{BASE}/{slug}.metadata.json?csvType=full&useColumnShortNames=true"

    def _columns_meta(self, slug: str) -> dict:
        """Per-column metadata (title/unit/citation) keyed by short column name."""
        try:
            j = self._get(self._chart_meta_url(slug), parse_json=True)
            return j.get("columns", {}) or {}
        except Exception:
            return {}  # metadata is a nicety; never block data on it

    # ----- fetch ----------------------------------------------------------------
    def fetch(self, since: Optional[dt.date] = None):
        min_year = since.year if since else None

        for chart in CHARTS:
            slug = chart["slug"]
            text = self._get(self._chart_csv_url(slug), parse_json=False)
            colmeta = self._columns_meta(slug)

            reader = csv.reader(io.StringIO(text))
            try:
                header = next(reader)
            except StopIteration:
                continue
            idx = {name: i for i, name in enumerate(header)}
            if "code" not in idx or "year" not in idx:
                continue

            # Resolve the value columns we publish, intersected with what's present.
            available = [c for c in header if c not in _NON_DATA_COLS]
            wanted = chart["cols"] if chart.get("cols") else available
            value_cols = [c for c in wanted if c in idx]
            if not value_cols:
                continue
            multi = len(value_cols) > 1

            # series_id -> (col, list[Observation])
            buckets: dict[str, tuple[str, list]] = {}
            for row in reader:
                if len(row) <= idx["year"]:
                    continue
                code = row[idx["code"]].strip()
                if code not in ENTITIES:        # curated entity filter
                    continue
                yr_raw = row[idx["year"]].strip()
                try:
                    yr = int(yr_raw)
                except (ValueError, TypeError):
                    continue                     # daily/date charts not in starter set
                if min_year is not None and yr < min_year:
                    continue
                for col in value_cols:
                    cell = row[idx[col]].strip() if idx[col] < len(row) else ""
                    if cell == "":
                        continue                 # skip None / empty
                    try:
                        val = float(cell)
                    except (ValueError, TypeError):
                        continue                 # skip non-numeric
                    sid = self._series_id(slug, col, code, multi)
                    buckets.setdefault(sid, (col, []))[1].append(
                        Observation(sid, dt.date(yr, 12, 31), val, version="clean"))

            for code in ENTITIES:                # deterministic, curated-order output
                for col in value_cols:
                    sid = self._series_id(slug, col, code, multi)
                    bucket = buckets.get(sid)
                    if not bucket or not bucket[1]:
                        continue
                    obs = bucket[1]
                    obs.sort(key=lambda o: o.obs_date)
                    yield self._meta(chart, col, code, multi, colmeta.get(col)), obs
