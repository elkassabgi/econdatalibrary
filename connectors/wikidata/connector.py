"""Wikidata econ/finance subset connector (REFERENCE/ENTITY data; CC0).

NOTE (2026-06): The FULL-coverage pull lives in jobs/ingest_wikidata.py, which
enumerates and writes the ENTIRE econ/finance entity universe as GROUPED Parquet
under data/clean_full/wikidata/ (companies.parquet 19,119 rows, stock_exchanges
1,118, currencies 2,356 -- one row per entity, series_key = QID). Use that for
the complete dataset. This Connector subclass remains only as the catalog/serve
adapter onto the time-series contract; it emits a CAPPED slice (TARGET_COMPANIES)
for the demo serve-path and is NOT the full-coverage path.

Wikidata is a knowledge graph, NOT a time-series source. This connector emits a
small, curated CATALOG OF LISTED COMPANIES queried via the Wikidata Query Service
(SPARQL). Each company is mapped onto the time-series contract as a one-row
"series": the rich entity attributes (ticker symbol(s), stock exchange(s), ISIN,
country, industry, founding date, Wikidata QID/URL) live in SeriesMeta.metadata,
and a single synthetic Observation carries the company's founding year (P571) on
its founding date -- a real, interpretable number where Wikidata has one. When no
founding date is recorded, a presence flag (value 1.0 on a fixed snapshot date) is
emitted so the entity is still ingested (the orchestrator skips empty obs lists).

Data model note (why the SPARQL looks the way it does): in Wikidata a ticker
symbol (P249) is stored as a QUALIFIER on the "stock exchange" statement (P414),
not as a truthy/direct claim -- only ~30 items use wdt:P249 directly, while ~15k
carry a ticker as a pq:P249 qualifier. We therefore anchor on p:P414 / pq:P249 and
require an ISIN (wdt:P946) so the slice is genuine, unambiguous listed securities.

Series id format: wikidata:<QID>   (e.g. wikidata:Q489921 = Mastercard)
License: CC0 (Wikidata data is dedicated to the public domain).
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

ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT = "Econ-Fin Data Library admin@hfdatalibrary.com"

# Size of the curated slice. Wikidata holds ~15k companies with a ticker-on-exchange
# statement; we keep a small, high-value catalog (hint: LIMIT a few hundred). The
# raw query fans out one row per (company x exchange x industry), so we ask for
# ~3x the rows we want distinct companies, then dedupe in Python.
TARGET_COMPANIES = 250
RAW_ROW_LIMIT = 800

# Date used for the synthetic presence observation when a company has no founding
# date (P571). Picked as a stable, clearly-synthetic snapshot anchor.
SNAPSHOT_DATE = dt.date(2000, 1, 1)

# Anchor on the ticker-qualifier pattern and require an ISIN. No global ORDER BY
# (a sitelinks sort makes the query ~25x slower and prone to WDQS timeouts).
SPARQL = f"""
SELECT ?company ?companyLabel ?ticker ?exchangeLabel ?isin ?inception ?countryLabel ?industryLabel WHERE {{
  ?company p:P414 ?exchStmt .
  ?exchStmt ps:P414 ?exchange .
  ?exchStmt pq:P249 ?ticker .
  ?company wdt:P946 ?isin .
  OPTIONAL {{ ?company wdt:P571 ?inception . }}
  OPTIONAL {{ ?company wdt:P17  ?country . }}
  OPTIONAL {{ ?company wdt:P452 ?industry . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}}
LIMIT {RAW_ROW_LIMIT}
"""


def _is_real_label(label: Optional[str]) -> bool:
    """The label service echoes the bare QID (e.g. 'Q112165834') when no English
    label exists -- treat those as missing rather than storing a useless id."""
    if not label:
        return False
    s = label.strip()
    if not s:
        return False
    if s[0] in "QPL" and s[1:].isdigit():
        return False
    return True


def _parse_year(inception_iso: Optional[str]) -> Optional[dt.date]:
    """Parse a Wikidata time literal (e.g. '1976-04-01T00:00:00Z'). Wikidata uses
    month/day 00 for year/month-only precision, and supports years far in the past;
    normalise those to a valid date (Jan/1) and reject out-of-range/negative years."""
    if not inception_iso:
        return None
    s = inception_iso.lstrip("+")
    head = s.split("T", 1)[0]
    parts = head.split("-")
    try:
        # Leading '-' (BCE) -> empty first part; not meaningful for companies.
        if parts[0] == "" or len(parts) < 1:
            return None
        year = int(parts[0])
        month = int(parts[1]) if len(parts) > 1 and parts[1] != "00" else 1
        day = int(parts[2]) if len(parts) > 2 and parts[2] != "00" else 1
        if not (1 <= year <= 9999):
            return None
        if not (1 <= month <= 12):
            month = 1
        if not (1 <= day <= 28):
            day = 1  # avoid month-length edge cases for a synthetic date
        return dt.date(year, month, day)
    except (ValueError, TypeError, IndexError):
        return None


class WikidataConnector(Connector):
    source_id = "wikidata"
    name = "Wikidata (econ/finance subset)"
    license_id = "cc0"
    schedule = "0 5 * * 1"  # weekly; reference data changes slowly
    attribution = "Source: Wikidata (CC0 1.0 public domain dedication)"
    homepage = "https://www.wikidata.org"

    def __init__(self):
        self._cache: Optional[dict] = None

    # ---- internal: run the SPARQL query and collapse to one record per company ----
    def _query(self) -> dict:
        if self._cache is not None:
            return self._cache
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "application/sparql-results+json",
        }
        params = {"query": SPARQL, "format": "json"}
        last_err = None
        for attempt in range(4):
            try:
                r = requests.get(ENDPOINT, params=params, headers=headers, timeout=180)
                if r.status_code in (429, 500, 502, 503, 504):
                    wait = min(60, 5 * (attempt + 1))
                    retry_after = r.headers.get("Retry-After")
                    if retry_after and retry_after.isdigit():
                        wait = min(120, int(retry_after))
                    time.sleep(wait)
                    last_err = RuntimeError(f"WDQS HTTP {r.status_code}")
                    continue
                r.raise_for_status()
                rows = r.json().get("results", {}).get("bindings", [])
                self._cache = self._collapse(rows)
                return self._cache
            except (requests.RequestException, ValueError) as e:
                last_err = e
                time.sleep(min(60, 5 * (attempt + 1)))
        raise RuntimeError(f"Wikidata SPARQL query failed after retries: {last_err}")

    @staticmethod
    def _collapse(rows: list[dict]) -> dict:
        """Dedupe the (company x exchange x industry) fan-out into one record per
        company, aggregating the multi-valued fields and keeping the earliest
        founding date. Returns an ordered dict keyed by QID."""
        out: dict[str, dict] = {}
        for row in rows:
            d = {k: v.get("value") for k, v in row.items()}
            uri = d.get("company")
            if not uri:
                continue
            qid = uri.rsplit("/", 1)[-1]
            rec = out.get(qid)
            if rec is None:
                if len(out) >= TARGET_COMPANIES:
                    continue  # cap distinct companies; ignore further new ones
                label = d.get("companyLabel")
                rec = {
                    "qid": qid,
                    "url": uri,
                    "name": label if _is_real_label(label) else qid,
                    "tickers": set(),
                    "exchanges": set(),
                    "industries": set(),
                    "isin": d.get("isin"),
                    "country": d.get("countryLabel") if _is_real_label(d.get("countryLabel")) else None,
                    "inception": None,
                }
                out[qid] = rec
            if d.get("ticker"):
                rec["tickers"].add(d["ticker"])
            if _is_real_label(d.get("exchangeLabel")):
                rec["exchanges"].add(d["exchangeLabel"])
            if _is_real_label(d.get("industryLabel")):
                rec["industries"].add(d["industryLabel"])
            if not rec.get("isin") and d.get("isin"):
                rec["isin"] = d["isin"]
            inc = _parse_year(d.get("inception"))
            if inc and (rec["inception"] is None or inc < rec["inception"]):
                rec["inception"] = inc
        return out

    @staticmethod
    def _meta_for(rec: dict) -> SeriesMeta:
        tickers = sorted(rec["tickers"])
        exchanges = sorted(rec["exchanges"])
        industries = sorted(rec["industries"])
        primary_ticker = tickers[0] if tickers else None
        title_bits = rec["name"]
        if primary_ticker:
            title_bits = f"{rec['name']} ({primary_ticker})"
        meta = {
            "data_kind": "reference-entity",
            "note": "Reference/entity record, not a time series. The single "
                    "observation encodes the company's founding year (or 1.0 as a "
                    "presence flag if unknown); all attributes are in this metadata.",
            "wikidata_qid": rec["qid"],
            "wikidata_url": rec["url"],
            "company_name": rec["name"],
            "tickers": tickers,
            "primary_ticker": primary_ticker,
            "stock_exchanges": exchanges,
            "isin": rec.get("isin"),
            "industries": industries,
            "country": rec.get("country"),
            "inception_date": rec["inception"].isoformat() if rec.get("inception") else None,
            "ticker_property": "P249",
            "isin_property": "P946",
        }
        return SeriesMeta(
            series_id=f"wikidata:{rec['qid']}",
            title=title_bits,
            frequency="irregular",
            unit="year (founding)",
            geography=rec.get("country"),
            category="reference",
            license_id="cc0",
            metadata=meta,
        )

    def discover(self) -> list[SeriesMeta]:
        recs = self._query()
        return [self._meta_for(rec) for rec in recs.values()]

    def fetch(self, since: Optional[dt.date] = None):
        # `since` is not meaningful for a static reference catalog; we always emit
        # the current curated slice (the orchestrator upserts by series_id).
        recs = self._query()
        for rec in recs.values():
            meta = self._meta_for(rec)
            inc = rec.get("inception")
            if inc is not None:
                obs = [Observation(meta.series_id, inc, float(inc.year), version="clean")]
            else:
                # Presence flag so the entity is still ingested (empty obs are skipped upstream).
                obs = [Observation(meta.series_id, SNAPSHOT_DATE, 1.0, version="clean")]
            yield meta, obs
