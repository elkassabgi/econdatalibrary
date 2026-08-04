"""DBnomics connector -- aggregator with per-series license passthrough.

DBnomics (https://db.nomics.world - BANNED, R251) re-publishes time series from ~90 statistical
providers behind one uniform REST API. There is no single license: each series
inherits the terms of its *underlying provider*. So our source-level license_id is
"dbnomics-passthrough" (declared RESERVABLE in core/licenses.py only because each
series carries its own provider terms), and for every series we record the upstream
provider code + that provider's terms_of_use URL in SeriesMeta.metadata so the
ingest/publish layer can apply the right per-provider rule.

API (v22, no key):
  GET /v22/series/<PROVIDER>/<DATASET>/<SERIES_CODE>?observations=1
Each response carries a `provider` block (code, name, terms_of_use, website),
a `dataset` block, and series.docs[0] with parallel arrays:
  period_start_day[]  -> ISO observation date (e.g. "1980-01-01")
  value[]             -> float, or the string "NA" for a gap (we skip those)
  @frequency          -> "daily" | "monthly" | "quarterly" | "annual" | ...

We curate a high-value starter set spanning several providers (IMF WEO, World Bank
WDI, Eurostat, OECD, BIS, ECB, AMECO, Federal Reserve H.15) to exercise the
passthrough design. The set is deliberately small/curated (quality over breadth);
the full list would later move to configs/sources.yaml. Per-series 404s are logged
and skipped rather than aborting the whole run (mirrors the BLS connector's
tolerant per-series behaviour).

Series id format: dbnomics:<PROVIDER>/<DATASET>/<SERIES_CODE>
(we keep the native DBnomics path as the identifier -- it is globally unique and
round-trips back to the API).
"""
# RETIRED 2026-08-03 — DBnomics is BANNED (CLAUDE.md §0, ledger R251): no fetching, no probing
# api.db.nomics.world, no relays or mirrors; every source comes from its own publisher.
#
# This file is a COMPLETE, WORKING client for the banned host — the v22 base URL, paging, the lot
# — and it was still listed in jobs/ingest_all.py's connector run list, which shells out to
# run_connector.py for EVERY entry. Anyone running that script would have pulled from the relay.
# ingest_all is not scheduled, and that is precisely what made it dangerous rather than harmless:
# a dormant run-all that violates the ban the first time someone runs it, reading as one name
# among twenty-three. Same shape as the watchdog entry that resurrected the banned puller every
# five minutes until it was found.
#
# Failing at IMPORT, loudly, naming the ban — the same treatment as
# updater/strategies/fetchers/_dbnomics.py. Better than deletion, which would remove the
# explanation along with the hazard, and better than a comment, which stops nobody.
raise ImportError(
    "connectors.dbnomics.connector is RETIRED: DBnomics is banned (CLAUDE.md §0, ledger R251) — "
    "no fetching, no probing api.db.nomics.world, no relays or mirrors. Fetch from the "
    "publisher's own API instead; see who_hwf/who_rs/who_sdg for the migrated pattern."
)

import datetime as dt
import math
import os
import sys
import time
from typing import Optional

import requests

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
from connectors.base import Connector, SeriesMeta, Observation  # noqa: E402

API = "https://api.db.nomics.world/v22"  # dead code after the ImportError above - BANNED (R251)
UA = "Econ-Fin Data Library admin@hfdatalibrary.com"

# DBnomics frequency label -> contract single-letter frequency code.
FREQ_MAP = {
    "daily": "D",
    "business": "D",
    "weekly": "W",
    "bi-weekly": "W",
    "monthly": "M",
    "quarterly": "Q",
    "semi-annual": "Q",
    "annual": "A",
    "yearly": "A",
}

# Best-effort upstream-license hint per provider, recorded in metadata for the
# publish-time gate. This is a *passthrough note*, not the enforced source license
# (the source license_id stays "dbnomics-passthrough"); the live terms_of_use URL
# returned by the API is also captured per series.
PROVIDER_LICENSE_HINT = {
    "IMF":      "imf-terms",
    "WB":       "cc-by-4.0",
    "Eurostat": "cc-by-4.0",        # EU content; non-EU/some trade carved out upstream
    "OECD":     "cc-by-4.0",
    "BIS":      "bis-attrib-nc",    # non-commercial redistribution only
    "ECB":      "ecb-attrib-nomodify",
    "AMECO":    "cc-by-4.0",        # European Commission / DG ECFIN
    "FED":      "us-public-domain",
}

# Curated starter set: every entry verified live against the v22 API.
# (series_path, human_title, geography_iso_or_region, category)
SERIES = [
    # --- IMF World Economic Outlook (':latest' resolves to the current vintage) ---
    ("IMF/WEO:latest/USA.NGDP_RPCH", "Real GDP growth (annual %)", "USA", "macro"),
    ("IMF/WEO:latest/CHN.NGDP_RPCH", "Real GDP growth (annual %)", "CHN", "macro"),
    ("IMF/WEO:latest/DEU.NGDP_RPCH", "Real GDP growth (annual %)", "DEU", "macro"),
    ("IMF/WEO:latest/JPN.NGDP_RPCH", "Real GDP growth (annual %)", "JPN", "macro"),
    ("IMF/WEO:latest/IND.NGDP_RPCH", "Real GDP growth (annual %)", "IND", "macro"),
    ("IMF/WEO:latest/USA.PCPIPCH", "Inflation, avg consumer prices (annual %)", "USA", "macro"),
    ("IMF/WEO:latest/USA.LUR", "Unemployment rate (% of labour force)", "USA", "macro"),
    ("IMF/WEO:latest/USA.GGXWDG_NGDP", "General government gross debt (% of GDP)", "USA", "macro"),

    # --- World Bank World Development Indicators (via DBnomics) ---
    ("WB/WDI/A-NY.GDP.MKTP.KD.ZG-USA", "GDP growth (annual %)", "USA", "macro"),
    ("WB/WDI/A-NY.GDP.MKTP.KD.ZG-CHN", "GDP growth (annual %)", "CHN", "macro"),
    ("WB/WDI/A-FP.CPI.TOTL.ZG-USA", "Inflation, consumer prices (annual %)", "USA", "macro"),

    # --- Eurostat ---
    ("Eurostat/prc_hicp_manr/M.RCH_A.CP00.EA", "HICP all-items, annual rate of change", "EA", "prices"),
    ("Eurostat/une_rt_m/M.SA.TOTAL.PC_ACT.T.EA20", "Unemployment rate (SA, % active pop.)", "EA20", "labour"),
    ("Eurostat/namq_10_gdp/Q.CLV_PCH_PRE.SCA.B1GQ.EA20", "Real GDP, q-o-q % change (SCA)", "EA20", "macro"),

    # --- OECD Main Economic Indicators / Key Economic Indicators ---
    ("OECD/MEI/USA.LRHUTTTT.STSA.M", "Harmonised unemployment rate (SA)", "USA", "labour"),
    ("OECD/KEI/LOLITOAA.USA.ST.M", "Composite leading indicator (amplitude adj.)", "USA", "leading-indicators"),

    # --- Bank for International Settlements (NON-COMMERCIAL passthrough) ---
    ("BIS/WS_SPP/Q.US.N.628", "Residential property prices, nominal index", "USA", "real-estate"),

    # --- European Central Bank ---
    ("ECB/FM/B.U2.EUR.4F.KR.MRR_FR.LEV", "ECB main refinancing operations rate (%)", "EA", "rates"),

    # --- AMECO (European Commission macro database) ---
    ("AMECO/ZUTN/USA.1.0.0.0.ZUTN", "Unemployment rate (% of active population)", "USA", "labour"),

    # --- U.S. Federal Reserve H.15 selected interest rates ---
    ("FED/H15/RIFLGFCY10_N.B", "10-Year Treasury constant maturity yield (%)", "USA", "rates"),
    ("FED/H15/RIFLGFCY02_N.B", "2-Year Treasury constant maturity yield (%)", "USA", "rates"),
]


class DBnomicsConnector(Connector):
    source_id = "dbnomics"
    name = "DBnomics"
    license_id = "dbnomics-passthrough"   # per-series; real terms come from each provider
    schedule = "0 7 * * 1"                # weekly (Mon 07:00) -- aggregator refresh cadence
    attribution = ("Source: DBnomics (https://db.nomics.world), re-publishing data from the "  # dead code - BANNED (R251)
                   "underlying provider named per series; each series under its provider's terms.")
    homepage = "https://db.nomics.world"  # dead code after the ImportError above - BANNED (R251)

    # ---- helpers ----------------------------------------------------------
    def _provider_of(self, path: str) -> str:
        return path.split("/", 1)[0]

    def _freq_code(self, label: Optional[str]) -> str:
        if not label:
            return "irregular"
        return FREQ_MAP.get(label.strip().lower(), "irregular")

    def _get(self, path: str) -> Optional[dict]:
        """GET one series with observations; retry on transient errors.

        Returns the parsed JSON, or None if the series is unavailable (404 /
        permanently empty) so the caller can skip it without aborting the run.
        """
        url = f"{API}/series/{path}?observations=1"
        last_exc: Optional[Exception] = None
        for attempt in range(4):
            try:
                r = requests.get(url, headers={"User-Agent": UA}, timeout=60)
            except requests.RequestException as e:
                last_exc = e
                time.sleep(1.5 * (attempt + 1))
                continue
            if r.status_code == 404:
                return None
            if r.status_code == 429 or 500 <= r.status_code < 600:
                # rate-limited / server error -> back off and retry
                time.sleep(2.0 * (attempt + 1))
                last_exc = RuntimeError(f"HTTP {r.status_code} for {path}")
                continue
            r.raise_for_status()
            return r.json()
        if last_exc:
            raise last_exc
        return None

    def _meta_for(self, path: str, title: str, geo: Optional[str], category: str,
                  payload: Optional[dict]) -> SeriesMeta:
        provider = self._provider_of(path)
        sid = f"dbnomics:{path}"
        md = {
            "dbnomics_path": path,
            "provider_code": provider,
            "provider_license_hint": PROVIDER_LICENSE_HINT.get(provider),
            "aggregator": "dbnomics",
        }
        freq = "irregular"
        unit = None
        if payload:
            prov = payload.get("provider") or {}
            ds = payload.get("dataset") or {}
            md["provider_name"] = prov.get("name")
            md["provider_terms_of_use"] = prov.get("terms_of_use")
            md["provider_website"] = prov.get("website")
            md["dataset_code"] = ds.get("code") or prov.get("code")
            md["dataset_name"] = ds.get("name")
            docs = (payload.get("series") or {}).get("docs") or []
            if docs:
                doc = docs[0]
                freq = self._freq_code(doc.get("@frequency"))
                unit = (doc.get("dimensions") or {}).get("unit")
                if doc.get("series_name"):
                    md["dbnomics_series_name"] = doc["series_name"]
        return SeriesMeta(sid, f"{title} - {geo}" if geo else title,
                          freq, unit, geo, category, self.license_id, md)

    # ---- contract ---------------------------------------------------------
    def discover(self) -> list[SeriesMeta]:
        # Lightweight: build metadata from the curated table without fetching
        # observations (avoids hammering the API just to list series).
        return [self._meta_for(path, title, geo, cat, None)
                for (path, title, geo, cat) in SERIES]

    def fetch(self, since: Optional[dt.date] = None):
        for (path, title, geo, cat) in SERIES:
            payload = self._get(path)
            if not payload:
                # series unavailable upstream; skip (logged implicitly by absence)
                continue
            docs = (payload.get("series") or {}).get("docs") or []
            if not docs:
                continue
            doc = docs[0]
            sid = f"dbnomics:{path}"
            dates = doc.get("period_start_day") or []
            values = doc.get("value") or []
            obs: list[Observation] = []
            for d_str, val in zip(dates, values):
                # skip gaps: DBnomics encodes missing as the string "NA"; also
                # guard against NaN/None and any non-numeric.
                if val is None or isinstance(val, str):
                    continue
                if isinstance(val, bool):  # bool is an int subclass; never a real value
                    continue
                try:
                    fval = float(val)
                except (TypeError, ValueError):
                    continue
                if math.isnan(fval) or math.isinf(fval):
                    continue
                try:
                    obs_date = dt.date.fromisoformat(d_str[:10])
                except (TypeError, ValueError):
                    continue
                if since is not None and obs_date < since:
                    continue
                obs.append(Observation(sid, obs_date, fval, version="clean"))
            if obs:
                meta = self._meta_for(path, title, geo, cat, payload)
                yield meta, obs
            # be polite between upstream calls
            time.sleep(0.3)
