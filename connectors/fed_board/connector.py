"""U.S. Federal Reserve Board H.15 "Selected Interest Rates" connector (public domain).

Source: the Fed's Data Download Program (DDP). No API key.

How it works (robust, no hard-coded session hashes)
----------------------------------------------------
Each H.15 series has a stable text "stub" at
    https://www.federalreserve.gov/releases/h15/data/Business_day/H15_<file>.txt
which no longer carries data itself -- it now points at the real CSV endpoint and,
crucially, names the canonical series id, e.g.:

    This series, H15/H15/RIFLGFCY10_N.B, is available at:
    http://www.federalreserve.gov/datadownload/Output.aspx?rel=H15&series=<hash>&...

So for each curated series we (1) read the stub to resolve its *current* DDP hash,
then (2) GET that Output.aspx CSV. Resolving the hash at fetch time means we keep
working even if the DDP rotates its opaque `series=` tokens.

CSV layout (seriescolumn, label=include): 6 metadata header rows, then `date,value`
rows. Missing observations are encoded as "ND" and are skipped.

Series id format: fed_board:<RIF code>  (e.g. fed_board:RIFLGFCY10_N.B)
"""
from __future__ import annotations

import csv
import datetime as dt
import io
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

UA = "Econ-Fin Data Library admin@hfdatalibrary.com"
STUB = "https://www.federalreserve.gov/releases/h15/data/Business_day/H15_{file}.txt"
OUTPUT = "https://www.federalreserve.gov/datadownload/Output.aspx"

# Curated high-value starter set for the U.S. yield curve + key policy/reference rates.
# Each tuple: (stub filename, canonical RIF code, human title, maturity tag, category).
# Daily ("Business_day") frequency; values are percent per year.
SERIES: list[tuple[str, str, str, Optional[str], str]] = [
    # --- Policy / money-market reference rates ---
    ("FF_O",      "RIFSPFF_N.B",   "Federal funds effective rate",                       "overnight", "policy_rate"),
    ("DWPC_NA",   "RIFSRP_F02_N.B", "Discount window primary credit rate",               None,        "policy_rate"),
    ("PRIME_NA",  "RIFSPBLP_N.B",  "Bank prime loan rate",                               None,        "reference_rate"),
    # --- Treasury bills, secondary market (discount basis) ---
    ("TB_WK4",    "RIFSGFSW04_N.B", "Treasury bill secondary market rate, 4-week",       "4W",  "treasury_bill"),
    ("TB_M3",     "RIFSGFSM03_N.B", "Treasury bill secondary market rate, 3-month",      "3M",  "treasury_bill"),
    ("TB_M6",     "RIFSGFSM06_N.B", "Treasury bill secondary market rate, 6-month",      "6M",  "treasury_bill"),
    ("TB_Y1",     "RIFSGFSY01_N.B", "Treasury bill secondary market rate, 1-year",       "1Y",  "treasury_bill"),
    # --- Treasury constant maturities, nominal (the yield curve) ---
    ("TCMNOM_M1", "RIFLGFCM01_N.B", "Treasury constant maturity yield, 1-month",         "1M",  "treasury_cmt"),
    ("TCMNOM_M3", "RIFLGFCM03_N.B", "Treasury constant maturity yield, 3-month",         "3M",  "treasury_cmt"),
    ("TCMNOM_M6", "RIFLGFCM06_N.B", "Treasury constant maturity yield, 6-month",         "6M",  "treasury_cmt"),
    ("TCMNOM_Y1", "RIFLGFCY01_N.B", "Treasury constant maturity yield, 1-year",          "1Y",  "treasury_cmt"),
    ("TCMNOM_Y2", "RIFLGFCY02_N.B", "Treasury constant maturity yield, 2-year",          "2Y",  "treasury_cmt"),
    ("TCMNOM_Y3", "RIFLGFCY03_N.B", "Treasury constant maturity yield, 3-year",          "3Y",  "treasury_cmt"),
    ("TCMNOM_Y5", "RIFLGFCY05_N.B", "Treasury constant maturity yield, 5-year",          "5Y",  "treasury_cmt"),
    ("TCMNOM_Y7", "RIFLGFCY07_N.B", "Treasury constant maturity yield, 7-year",          "7Y",  "treasury_cmt"),
    ("TCMNOM_Y10", "RIFLGFCY10_N.B", "Treasury constant maturity yield, 10-year",        "10Y", "treasury_cmt"),
    ("TCMNOM_Y20", "RIFLGFCY20_N.B", "Treasury constant maturity yield, 20-year",        "20Y", "treasury_cmt"),
    ("TCMNOM_Y30", "RIFLGFCY30_N.B", "Treasury constant maturity yield, 30-year",        "30Y", "treasury_cmt"),
    # --- Treasury inflation-indexed (TIPS) constant maturities ---
    ("TCMII_Y5",  "RIFLGFCY05_XII_N.B", "Treasury inflation-indexed yield, 5-year",      "5Y",  "treasury_tips"),
    ("TCMII_Y10", "RIFLGFCY10_XII_N.B", "Treasury inflation-indexed yield, 10-year",     "10Y", "treasury_tips"),
    ("TCMII_Y30", "RIFLGFCY30_XII_N.B", "Treasury inflation-indexed yield, 30-year",     "30Y", "treasury_tips"),
]

_HASH_RE = re.compile(r"series=([0-9a-fA-F]{8,})")


class FedBoardConnector(Connector):
    source_id = "fed_board"
    name = "U.S. Federal Reserve Board (H.15)"
    license_id = "us-public-domain"
    schedule = "0 7 * * 1-5"  # business-day release, early-morning refresh
    attribution = ("Source: Board of Governors of the Federal Reserve System (US), "
                   "H.15 Selected Interest Rates (public domain)")
    homepage = "https://www.federalreserve.gov"

    def _session(self) -> requests.Session:
        s = requests.Session()
        s.headers.update({"User-Agent": UA})
        return s

    def _get(self, sess: requests.Session, url: str, *, params: Optional[dict] = None,
             retries: int = 4) -> requests.Response:
        """GET with simple exponential backoff; raises on final failure."""
        last = None
        for attempt in range(retries):
            try:
                r = sess.get(url, params=params, timeout=90)
                if r.status_code == 200:
                    return r
                # 429/5xx -> back off and retry; other 4xx -> stop early
                if r.status_code not in (429, 500, 502, 503, 504):
                    r.raise_for_status()
                last = requests.HTTPError(f"HTTP {r.status_code} for {r.url}")
            except requests.RequestException as e:  # noqa: PERF203
                last = e
            time.sleep(1.5 * (2 ** attempt))
        raise last if last else RuntimeError(f"failed to GET {url}")

    def _resolve_hash(self, sess: requests.Session, file: str, rif: str) -> Optional[str]:
        """Read the stable .txt stub and pull out the current DDP `series=` hash.

        Confirms the stub really is the series we expect (its canonical id matches
        `rif`) so a renamed/retired file can't silently feed us the wrong data.
        """
        r = self._get(sess, STUB.format(file=file))
        text = r.text
        if rif not in text:
            return None
        m = _HASH_RE.search(text)
        return m.group(1) if m else None

    def _parse_csv(self, text: str, rif: str, since: Optional[dt.date]) -> tuple[str, list[Observation]]:
        """Parse a seriescolumn CSV: 6 header rows then date,value. Returns (description, obs)."""
        description = ""
        obs: list[Observation] = []
        sid = f"{self.source_id}:{rif}"
        reader = csv.reader(io.StringIO(text))
        for row in reader:
            if not row:
                continue
            tag = row[0].strip()
            if tag == "Series Description":
                description = row[1].strip() if len(row) > 1 else ""
                continue
            # Skip the remaining metadata header rows and the column-label row.
            if tag in ("Unit:", "Multiplier:", "Currency:", "Time Period") or tag.startswith("Unique Identifier"):
                continue
            if len(row) < 2:
                continue
            d_raw, v_raw = tag, row[1].strip()
            try:
                d = dt.datetime.strptime(d_raw, "%Y-%m-%d").date()
            except ValueError:
                continue  # not a data row
            if since is not None and d < since:
                continue
            if not v_raw or v_raw.upper() == "ND":  # "ND" = no data published that day
                continue
            try:
                val = float(v_raw)
            except ValueError:
                continue
            obs.append(Observation(sid, d, val, version="clean"))
        return description, obs

    def _clean_title(self, fallback: str, description: str) -> str:
        """Prefer the Fed's own series description, collapsing its doubled spaces."""
        desc = re.sub(r"\s+", " ", description).strip()
        return desc or fallback

    def discover(self) -> list[SeriesMeta]:
        out = []
        for _file, rif, title, maturity, category in SERIES:
            out.append(SeriesMeta(
                f"{self.source_id}:{rif}", title, "D", "Percent per year", "US",
                category, self.license_id,
                {"h15_id": rif, "maturity": maturity, "release": "H.15"},
            ))
        return out

    def fetch(self, since: Optional[dt.date] = None):
        sess = self._session()
        for file, rif, title, maturity, category in SERIES:
            sid = f"{self.source_id}:{rif}"
            try:
                hsh = self._resolve_hash(sess, file, rif)
                if not hsh:
                    continue  # stub missing/renamed -- skip rather than crash the run
                r = self._get(sess, OUTPUT, params={
                    "rel": "H15", "series": hsh, "lastObs": "", "from": "", "to": "",
                    "filetype": "csv", "label": "include", "layout": "seriescolumn",
                })
                ctype = r.headers.get("Content-Type", "")
                if "csv" not in ctype.lower() and "Time Period" not in r.text:
                    continue  # got the HTML interface back instead of data -- skip
                description, obs = self._parse_csv(r.text, rif, since)
            except requests.RequestException:
                continue  # network trouble on one series shouldn't sink the rest
            if not obs:
                continue
            meta = SeriesMeta(
                sid, self._clean_title(title, description), "D", "Percent per year",
                "US", category, self.license_id,
                {"h15_id": rif, "maturity": maturity, "release": "H.15"},
            )
            yield meta, obs
            time.sleep(0.4)  # be polite between series
