"""Bank of England Interactive Database (IADB) connector (UK macro & markets; OGL-UK-3.0).

Source: the BoE statistical database's CSV export endpoint, no API key:
    https://www.bankofengland.co.uk/boeapps/database/_iadb-fromshowcolumns.asp

How it works
------------
The endpoint returns a "wide" CSV -- one DATE column plus one column per requested
series code -- when called with csv.x=yes and UsingCodes=Y (UsingCodes is required;
omitting it makes the server return an HTML error page). Example:

    DATE,IUDBEDR,IUDSOIA
    02 Jan 2024,5.25,5.1863
    ...

So we can pull many series in a single request. We batch series that share a
frequency (mixing daily + monthly in one CSV would interleave their date rows),
and if a batch comes back as an HTML error page (e.g. one bad code poisons the
whole request) we transparently fall back to fetching that batch's codes one at a
time, skipping the offender rather than crashing the run.

Date format in the CSV is "%d %b %Y" (e.g. "02 Jan 2024"). Values are plain
decimals; blanks / non-numeric cells are skipped. `since` is pushed to the server
via Datefrom and also enforced client-side.

Series id format: boe:<IADB code>   (e.g. boe:IUDBEDR)

Curated starter set (high-value UK indicators)
----------------------------------------------
Policy & money-market rates, the gilt yield curve (nominal + real), the main
sterling spot exchange rates and the effective exchange-rate index, headline
household borrowing/deposit rates, and broad money (M4). All series codes and
their titles were verified against the live IADB before being hard-coded here.
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
CSV_URL = "https://www.bankofengland.co.uk/boeapps/database/_iadb-fromshowcolumns.asp"

# Earliest history to request when no `since` is given. BoE accepts a generous
# from-date and simply returns whatever the series covers.
DEFAULT_FROM = dt.date(1975, 1, 1)

# Curated series. Each: (IADB code, title, frequency, unit, category).
# frequency drives request batching (same-frequency codes share one CSV call).
SERIES: list[tuple[str, str, str, str, str]] = [
    # --- Policy & money-market reference rates (daily) ---
    ("IUDBEDR", "Official Bank Rate",                                          "D", "percent",          "policy_rate"),
    ("IUDSOIA", "Sterling Overnight Index Average (SONIA)",                    "D", "percent",          "reference_rate"),
    # --- Gilt yield curve, nominal par yields (daily) ---
    ("IUDSNPY", "British Government Securities, 5-year nominal par yield",     "D", "percent",          "gilt_yield"),
    ("IUDMNPY", "British Government Securities, 10-year nominal par yield",    "D", "percent",          "gilt_yield"),
    ("IUDLNPY", "British Government Securities, 20-year nominal par yield",    "D", "percent",          "gilt_yield"),
    # --- Gilt yield curve, real (inflation zero-coupon) yields (daily) ---
    ("IUDSIZC", "British Government Securities, 5-year real (zero-coupon) yield",  "D", "percent",      "gilt_yield_real"),
    ("IUDMIZC", "British Government Securities, 10-year real (zero-coupon) yield", "D", "percent",      "gilt_yield_real"),
    ("IUDLIZC", "British Government Securities, 20-year real (zero-coupon) yield", "D", "percent",      "gilt_yield_real"),
    # --- Sterling spot exchange rates (daily; foreign currency into GBP) ---
    ("XUDLUSS", "Spot exchange rate, US dollar into sterling",                 "D", "USD per GBP",      "exchange_rate"),
    ("XUDLERS", "Spot exchange rate, euro into sterling",                      "D", "EUR per GBP",      "exchange_rate"),
    ("XUDLJYS", "Spot exchange rate, Japanese yen into sterling",             "D", "JPY per GBP",      "exchange_rate"),
    ("XUDLCDS", "Spot exchange rate, Canadian dollar into sterling",          "D", "CAD per GBP",      "exchange_rate"),
    ("XUDLSFS", "Spot exchange rate, Swiss franc into sterling",              "D", "CHF per GBP",      "exchange_rate"),
    ("XUDLADS", "Spot exchange rate, Australian dollar into sterling",        "D", "AUD per GBP",      "exchange_rate"),
    ("XUDLBK67", "Sterling effective exchange rate index (Jan 2005 = 100)",   "D", "index",            "exchange_rate"),
    # --- Headline household borrowing & deposit rates (monthly) ---
    ("IUMABEDR", "Monthly average of the official Bank Rate",                  "M", "percent",          "policy_rate"),
    ("IUMBV34",  "Household 2-year (75% LTV) fixed-rate mortgage rate",        "M", "percent",          "lending_rate"),
    ("IUMTLMV",  "Household revert-to-rate (SVR) mortgage rate",               "M", "percent",          "lending_rate"),
    ("IUMWTFA",  "Household 1-year fixed-rate bond deposit rate (incl. bonus)", "M", "percent",         "deposit_rate"),
    ("IUMB6VJ",  "Household instant-access deposit rate (incl. bonus)",        "M", "percent",          "deposit_rate"),
    # --- Broad money (monthly) ---
    ("LPMAUYN",  "M4 broad money, amounts outstanding (SA)",                   "M", "GBP millions",     "monetary_aggregate"),
]

_FREQ_GEO = "UK"


def _to_float(v: str) -> Optional[float]:
    """IADB cells are plain decimals; tolerate blanks, 'n/a', stray spaces/commas."""
    if v is None:
        return None
    s = v.strip().replace(",", "")
    if s == "" or s.lower() in ("na", "n/a", "none", "null", "*", "nd"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _parse_date(s: str) -> Optional[dt.date]:
    """IADB date format is e.g. '02 Jan 2024'."""
    s = s.strip()
    try:
        return dt.datetime.strptime(s, "%d %b %Y").date()
    except (ValueError, TypeError):
        return None


class BankOfEnglandConnector(Connector):
    source_id = "boe"
    name = "Bank of England"
    license_id = "ogl-uk-3.0"
    schedule = "0 7 * * 1-5"  # business-day release of rates/FX; refresh each weekday
    attribution = ("Source: Bank of England Interactive Database. "
                   "Contains public sector information licensed under the Open Government "
                   "Licence v3.0 (OGL-UK-3.0).")
    homepage = "https://www.bankofengland.co.uk"

    # ------------------------------------------------------------------ http
    def _session(self) -> requests.Session:
        s = requests.Session()
        s.headers.update({"User-Agent": UA, "Accept": "text/csv,*/*"})
        return s

    def _params(self, codes: list[str], since: Optional[dt.date]) -> dict:
        frm = since or DEFAULT_FROM
        return {
            "csv.x": "yes",
            "Datefrom": frm.strftime("%d/%b/%Y"),   # e.g. 01/Jan/2020
            "Dateto": "now",
            "SeriesCodes": ",".join(codes),
            "CSVF": "TN",          # tabular, names-not-applicable -> bare DATE,<code> header
            "UsingCodes": "Y",     # REQUIRED: column headers are the series codes
            "VPD": "Y",            # provisional data included
            "VFD": "N",            # no value-friendly description column
        }

    def _get_csv(self, sess: requests.Session, codes: list[str],
                 since: Optional[dt.date], retries: int = 4) -> Optional[str]:
        """GET the wide CSV for `codes` with backoff. Returns CSV text, or None if
        the server returned an HTML error page (bad code / no data) after retries."""
        last = None
        for attempt in range(retries):
            try:
                r = sess.get(CSV_URL, params=self._params(codes, since), timeout=90)
                if r.status_code == 200:
                    text = r.text.lstrip("﻿")  # strip any BOM
                    if text[:5].upper() == "DATE,":
                        return text
                    # HTML error page (e.g. invalid code) -- not retryable per se.
                    return None
                if r.status_code not in (429, 500, 502, 503, 504):
                    r.raise_for_status()
                last = requests.HTTPError(f"HTTP {r.status_code} for {r.url}")
            except requests.RequestException as e:  # noqa: PERF203
                last = e
            time.sleep(1.5 * (2 ** attempt))
        if last:
            raise last
        return None

    def _parse_csv(self, text: str, since: Optional[dt.date]
                   ) -> dict[str, list[Observation]]:
        """Parse a wide IADB CSV into {code: [Observation, ...]}.

        Header row is 'DATE,<code1>,<code2>,...'. Each subsequent row is a date
        plus one cell per code (cells may be blank where a series has no value).
        """
        reader = csv.reader(io.StringIO(text))
        try:
            header = next(reader)
        except StopIteration:
            return {}
        codes = [h.strip() for h in header[1:]]
        out: dict[str, list[Observation]] = {c: [] for c in codes}
        for row in reader:
            if not row or len(row) < 2:
                continue
            d = _parse_date(row[0])
            if d is None:
                continue
            if since is not None and d < since:
                continue
            for i, code in enumerate(codes, start=1):
                if i >= len(row):
                    continue
                val = _to_float(row[i])
                if val is None:
                    continue
                out[code].append(Observation(f"{self.source_id}:{code}", d, val, version="clean"))
        return out

    def _fetch_codes(self, sess: requests.Session, codes: list[str],
                     since: Optional[dt.date]) -> dict[str, list[Observation]]:
        """Fetch a same-frequency batch; on an HTML-error response, retry each code
        singly so one bad/empty code never sinks its whole batch."""
        text = self._get_csv(sess, codes, since)
        if text is not None:
            return self._parse_csv(text, since)
        if len(codes) == 1:
            return {}  # the single code itself is bad/empty -- skip it
        merged: dict[str, list[Observation]] = {}
        for code in codes:
            time.sleep(0.4)  # be polite between fallback calls
            single = self._get_csv(sess, [code], since)
            if single is not None:
                merged.update(self._parse_csv(single, since))
        return merged

    # ------------------------------------------------------------------ contract
    def discover(self) -> list[SeriesMeta]:
        out: list[SeriesMeta] = []
        for code, title, freq, unit, category in SERIES:
            out.append(SeriesMeta(
                f"{self.source_id}:{code}", title, freq, unit, _FREQ_GEO, category,
                self.license_id, {"iadb_code": code},
            ))
        return out

    def fetch(self, since: Optional[dt.date] = None):
        sess = self._session()
        meta_by_code = {code: (title, freq, unit, category)
                        for code, title, freq, unit, category in SERIES}

        # Group codes by frequency so each CSV request has aligned date rows.
        by_freq: dict[str, list[str]] = {}
        for code, _title, freq, _unit, _category in SERIES:
            by_freq.setdefault(freq, []).append(code)

        for freq, codes in by_freq.items():
            obs_by_code = self._fetch_codes(sess, codes, since)
            for code in codes:
                obs = obs_by_code.get(code) or []
                if not obs:
                    continue
                title, _freq, unit, category = meta_by_code[code]
                meta = SeriesMeta(
                    f"{self.source_id}:{code}", title, freq, unit, _FREQ_GEO, category,
                    self.license_id, {"iadb_code": code},
                )
                yield meta, obs
            time.sleep(0.4)  # polite between frequency batches
