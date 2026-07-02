"""Frankfurter connector -- ECB euro foreign-exchange reference rates.

Frankfurter (https://frankfurter.dev) is a free, key-less wrapper over the
European Central Bank's daily euro foreign-exchange *reference* rates, published
each TARGET working day around 16:00 CET, with history back to 1999-01-04.

We use the classic ECB-only endpoint (`/v1`), not the newer multi-provider `/v2`
aggregate: the `ecb-attrib-nomodify` license here covers the ECB reference rates,
which must be cached RAW and *not modified* (we re-publish the published values
verbatim, so observations are tagged version="clean" but are byte-faithful).

One series per currency, always quoted against the euro:
  series_id = "frankfurter:EUR:<CUR>"   e.g. frankfurter:EUR:USD
  value     = units of <CUR> per 1 EUR  (exactly as the ECB publishes).

The timeseries endpoint returns every traded currency for a date range in one
request, keyed {date: {CUR: rate}}; we pivot that into per-currency series.
`since` is honoured by moving the range start (open-ended `{start}..`), so daily
incremental runs pull only new days. No API key, no documented call cap; polite
User-Agent + retries with backoff.
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

# Classic ECB-only API. The /v2 host blends 84 central banks, which falls outside
# the ECB-specific license/attribution -- so we deliberately stay on /v1.
BASE = "https://api.frankfurter.dev/v1"
HEADERS = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

# ECB reference rates begin on the euro's first publication day.
HISTORY_START = dt.date(1999, 1, 4)

# Base currency for every series. The ECB quotes everything against the euro.
QUOTE = "EUR"


def _get(url: str, *, retries: int = 4, timeout: int = 90) -> Optional[dict]:
    """GET JSON with polite backoff. Returns None on a 404 (no data for range)."""
    last_exc: Optional[Exception] = None
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            if r.status_code == 404:
                # Open-ended range starting after the last published day -> no rows.
                return None
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"{r.status_code} for {url}")
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError) as exc:
            last_exc = exc
            if attempt < retries - 1:
                time.sleep(2 ** attempt)  # 1s, 2s, 4s
    raise RuntimeError(f"Frankfurter request failed after {retries} tries: {url}") from last_exc


class FrankfurterConnector(Connector):
    source_id = "frankfurter"
    name = "Frankfurter (ECB euro reference rates)"
    license_id = "ecb-attrib-nomodify"
    schedule = "30 15 * * 1-5"  # ~16:00 CET on TARGET working days (UTC 15:30, Mon-Fri)
    attribution = (
        "Source: European Central Bank euro foreign-exchange reference rates, "
        "via Frankfurter (frankfurter.dev). Rates redistributed unmodified."
    )
    homepage = "https://frankfurter.dev"

    def _currencies(self) -> dict[str, str]:
        """Live ECB-traded currency list: {ISO code -> currency name} (incl. EUR)."""
        data = _get(f"{BASE}/currencies")
        return data or {}

    def discover(self) -> list[SeriesMeta]:
        out: list[SeriesMeta] = []
        for code, cur_name in sorted(self._currencies().items()):
            if code == QUOTE:
                continue  # EUR/EUR is trivially 1.0
            sid = f"frankfurter:{QUOTE}:{code}"
            out.append(SeriesMeta(
                series_id=sid,
                title=f"{QUOTE}/{code} -- {cur_name} per euro (ECB reference rate)",
                frequency="D",
                unit=f"{code} per {QUOTE}",
                geography=None,
                category="fx",
                license_id=self.license_id,
                metadata={"base": QUOTE, "quote": code, "currency_name": cur_name,
                          "provider": "ECB"},
            ))
        return out

    def fetch(self, since: Optional[dt.date] = None):
        start = since or HISTORY_START
        if start < HISTORY_START:
            start = HISTORY_START
        if start > dt.date.today():
            return  # nothing newer than today could exist

        names = self._currencies()

        # One request returns every currency for the whole range, keyed by date.
        payload = _get(f"{BASE}/{start.isoformat()}..")
        if not payload:
            return  # 404 -> no published rates in [start, today]
        rates = payload.get("rates") or {}

        # Pivot {date: {CUR: rate}} -> {CUR: [Observation, ...]}.
        by_currency: dict[str, list[Observation]] = {}
        for date_str, row in rates.items():
            try:
                d = dt.date.fromisoformat(date_str)
            except (ValueError, TypeError):
                continue
            for code, raw in (row or {}).items():
                if code == QUOTE:
                    continue
                try:
                    val = float(raw)
                except (TypeError, ValueError):
                    continue  # skip None / non-numeric
                sid = f"frankfurter:{QUOTE}:{code}"
                by_currency.setdefault(code, []).append(
                    Observation(sid, d, val, version="clean"))

        for code, obs in sorted(by_currency.items()):
            if not obs:
                continue
            obs.sort(key=lambda o: o.obs_date)
            sid = f"frankfurter:{QUOTE}:{code}"
            cur_name = names.get(code, code)
            meta = SeriesMeta(
                series_id=sid,
                title=f"{QUOTE}/{code} -- {cur_name} per euro (ECB reference rate)",
                frequency="D",
                unit=f"{code} per {QUOTE}",
                geography=None,
                category="fx",
                license_id=self.license_id,
                metadata={"base": QUOTE, "quote": code, "currency_name": cur_name,
                          "provider": "ECB"},
            )
            yield meta, obs
