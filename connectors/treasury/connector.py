"""U.S. Treasury FiscalData connector (federal debt & interest rates; public domain).

FiscalData REST API (https://fiscaldata.treasury.gov), no key required. Paginated
JSON via page[number]/page[size]; meta.total-pages bounds the loop and server-side
filter=record_date:gte:YYYY-MM-DD drives incremental pulls. Raw amount values are
plain decimal strings (no $ / commas), so float() is applied directly.

Starter set (high-value):
  * debt_to_penny  -> daily total public debt outstanding + its two components.
  * avg_interest_rates -> monthly average interest rate per security class
    (one series per security_type_desc / security_desc pair).

Series id formats:
  treasury:debt_to_penny:<field>
  treasury:avg_interest_rates:<security_type_slug>:<security_slug>
"""
from __future__ import annotations
import datetime as dt
import os
import re
import sys
import time
from typing import Optional

import requests

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
from connectors.base import Connector, SeriesMeta, Observation  # noqa: E402

API = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
UA = "Econ-Fin Data Library admin@hfdatalibrary.com"
PAGE_SIZE = 10000  # API hard cap is 10000 rows/page

# --- debt_to_penny: which CURRENCY columns to expose as series -------------------
DEBT_FIELDS = {
    "tot_pub_debt_out_amt": "Total public debt outstanding",
    "debt_held_public_amt": "Debt held by the public",
    "intragov_hold_amt":    "Intragovernmental holdings",
}

# --- avg_interest_rates: curated high-value (security_type_desc, security_desc) ---
# Headline marketable instruments, the rolled-up totals, and a couple of widely
# watched non-marketable lines. The fetch reports per-pair coverage, so a renamed
# label simply yields zero rows instead of crashing.
RATE_SERIES = [
    ("Marketable", "Treasury Bills"),
    ("Marketable", "Treasury Notes"),
    ("Marketable", "Treasury Bonds"),
    ("Marketable", "Treasury Inflation-Protected Securities (TIPS)"),
    ("Marketable", "Treasury Floating Rate Notes (FRN)"),
    ("Marketable", "Total Marketable"),
    ("Non-marketable", "United States Savings Securities"),
    ("Non-marketable", "State and Local Government Series"),
    ("Non-marketable", "Government Account Series"),
    ("Non-marketable", "Total Non-marketable"),
    ("Interest-bearing Debt", "Total Interest-bearing Debt"),
]


def _slug(s: str) -> str:
    """Stable, id-safe token from a Treasury label."""
    s = s.lower().replace("&", "and")
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def _to_float(v) -> Optional[float]:
    """FiscalData amounts arrive as decimal strings; tolerate stray $ , and 'null'."""
    if v is None:
        return None
    s = str(v).strip().replace("$", "").replace(",", "")
    if s == "" or s.lower() in ("null", "none", "na", "n/a", "*"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _parse_date(s: str) -> Optional[dt.date]:
    try:
        return dt.datetime.strptime(s, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


class TreasuryConnector(Connector):
    source_id = "treasury"
    name = "U.S. Treasury FiscalData"
    license_id = "us-public-domain"
    schedule = "0 7 * * *"            # daily; debt_to_penny updates each business day
    attribution = "Source: U.S. Department of the Treasury, Bureau of the Fiscal Service (public domain)"
    homepage = "https://fiscaldata.treasury.gov"

    # ------------------------------------------------------------------ helpers
    def _session(self) -> requests.Session:
        s = requests.Session()
        s.headers.update({"User-Agent": UA})
        return s

    def _get_pages(self, sess, endpoint: str, *, fields: list[str], since: Optional[dt.date]):
        """Yield data rows across all pages, oldest filter applied server-side."""
        params = {
            "fields": ",".join(fields),
            "sort": "record_date",
            "page[size]": str(PAGE_SIZE),
        }
        if since is not None:
            params["filter"] = f"record_date:gte:{since.isoformat()}"
        page = 1
        total_pages = None
        while True:
            params["page[number]"] = str(page)
            payload = self._request(sess, f"{API}/{endpoint}", params)
            rows = payload.get("data", []) or []
            for row in rows:
                yield row
            if total_pages is None:
                total_pages = (payload.get("meta", {}) or {}).get("total-pages", 1) or 1
            if page >= total_pages or not rows:
                break
            page += 1
            time.sleep(0.2)  # be polite between pages

    def _request(self, sess, url: str, params: dict) -> dict:
        last_exc = None
        for attempt in range(4):
            try:
                r = sess.get(url, params=params, timeout=60)
                if r.status_code in (429, 500, 502, 503, 504):
                    raise requests.HTTPError(f"{r.status_code} {r.reason}", response=r)
                r.raise_for_status()
                return r.json()
            except (requests.RequestException, ValueError) as e:
                last_exc = e
                time.sleep(min(2 ** attempt, 8))
        raise RuntimeError(f"FiscalData request failed after retries: {url} -- {last_exc}")

    # ------------------------------------------------------------------ contract
    def discover(self) -> list[SeriesMeta]:
        metas: list[SeriesMeta] = []
        for field, title in DEBT_FIELDS.items():
            sid = f"treasury:debt_to_penny:{field}"
            metas.append(SeriesMeta(
                sid, f"U.S. {title} (to the penny)", "D", "USD", "US", "fiscal",
                self.license_id,
                {"dataset": "debt_to_penny", "field": field}))
        for stype, sdesc in RATE_SERIES:
            sid = f"treasury:avg_interest_rates:{_slug(stype)}:{_slug(sdesc)}"
            metas.append(SeriesMeta(
                sid, f"Average interest rate -- {sdesc} ({stype})", "M", "percent", "US",
                "fiscal", self.license_id,
                {"dataset": "avg_interest_rates",
                 "security_type_desc": stype, "security_desc": sdesc}))
        return metas

    def fetch(self, since: Optional[dt.date] = None):
        sess = self._session()
        yield from self._fetch_debt(sess, since)
        yield from self._fetch_rates(sess, since)

    # ------------------------------------------------------------------ datasets
    def _fetch_debt(self, sess, since: Optional[dt.date]):
        fields = ["record_date", *DEBT_FIELDS.keys()]
        by_field: dict[str, list[Observation]] = {f: [] for f in DEBT_FIELDS}
        for row in self._get_pages(sess, "v2/accounting/od/debt_to_penny",
                                   fields=fields, since=since):
            d = _parse_date(row.get("record_date"))
            if d is None:
                continue
            for field in DEBT_FIELDS:
                val = _to_float(row.get(field))
                if val is None:
                    continue
                by_field[field].append(
                    Observation(f"treasury:debt_to_penny:{field}", d, val, version="clean"))
        for field, title in DEBT_FIELDS.items():
            obs = by_field[field]
            if not obs:
                continue
            sid = f"treasury:debt_to_penny:{field}"
            meta = SeriesMeta(sid, f"U.S. {title} (to the penny)", "D", "USD", "US",
                              "fiscal", self.license_id,
                              {"dataset": "debt_to_penny", "field": field})
            yield meta, obs

    def _fetch_rates(self, sess, since: Optional[dt.date]):
        wanted = {(st, sd) for st, sd in RATE_SERIES}
        fields = ["record_date", "security_type_desc", "security_desc", "avg_interest_rate_amt"]
        buckets: dict[tuple[str, str], list[Observation]] = {k: [] for k in wanted}
        for row in self._get_pages(sess, "v2/accounting/od/avg_interest_rates",
                                   fields=fields, since=since):
            key = (row.get("security_type_desc"), row.get("security_desc"))
            if key not in wanted:
                continue
            d = _parse_date(row.get("record_date"))
            val = _to_float(row.get("avg_interest_rate_amt"))
            if d is None or val is None:
                continue
            stype, sdesc = key
            sid = f"treasury:avg_interest_rates:{_slug(stype)}:{_slug(sdesc)}"
            buckets[key].append(Observation(sid, d, val, version="clean"))
        for (stype, sdesc), obs in buckets.items():
            if not obs:
                continue
            sid = f"treasury:avg_interest_rates:{_slug(stype)}:{_slug(sdesc)}"
            meta = SeriesMeta(sid, f"Average interest rate -- {sdesc} ({stype})", "M",
                              "percent", "US", "fiscal", self.license_id,
                              {"dataset": "avg_interest_rates",
                               "security_type_desc": stype, "security_desc": sdesc})
            yield meta, obs
