"""DeFiLlama connector (DeFi TVL; open data, attribution required).

DeFiLlama publishes a free, key-less REST API of Total Value Locked (TVL) across
the DeFi ecosystem. We build daily USD time series for:
  - total DeFi TVL                       defillama:tvl:total
  - per-chain TVL (major chains)         defillama:chain_tvl:<Chain>
  - blue-chip protocol TVL               defillama:protocol_tvl:<slug>
  - total stablecoin market cap (USD)    defillama:stablecoins:total_usd

API surface used (no key, polite headers, retries, no pagination -- each call
returns the full history as one JSON array):
  GET https://api.llama.fi/v2/historicalChainTvl           -> [{date:int(s), tvl:float}]
  GET https://api.llama.fi/v2/historicalChainTvl/{chain}   -> [{date:int(s), tvl:float}]
  GET https://api.llama.fi/protocol/{slug}                 -> {... "tvl":[{date:int(s), totalLiquidityUSD:float}] ...}
  GET https://stablecoins.llama.fi/stablecoincharts/all    -> [{date:str(s), totalCirculatingUSD:{peggedUSD:float}}]

`date` is a Unix timestamp in seconds (UTC midnight snapshots, daily). Values are
USD. We emit Observation(version="clean") and skip null / non-numeric / negative
points. `since` filters incrementally by obs_date.
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

UA = "Econ-Fin Data Library admin@hfdatalibrary.com"

API = "https://api.llama.fi"
STABLE_API = "https://stablecoins.llama.fi"

# --- Starter set -------------------------------------------------------------
# Major chains. Keys are the DeFiLlama chain names exactly as the API expects
# them in /v2/historicalChainTvl/{chain} (spaces are URL-encoded at call time);
# values are human-readable titles for the catalog.
CHAINS = {
    "Ethereum":       "Ethereum",
    "BSC":            "BNB Smart Chain (BSC)",
    "Solana":         "Solana",
    "Tron":           "Tron",
    "Bitcoin":        "Bitcoin",
    "Base":           "Base",
    "Arbitrum":       "Arbitrum",
    "Polygon":        "Polygon",
    "Avalanche":      "Avalanche",
    "OP Mainnet":     "Optimism (OP Mainnet)",
    "Sui":            "Sui",
    "Aptos":          "Aptos",
    "Near":           "NEAR",
    "Starknet":       "Starknet",
}

# Blue-chip protocols. Keys are DeFiLlama protocol slugs (path of /protocol/{slug});
# values are human-readable titles.
PROTOCOLS = {
    "aave":              "Aave",
    "lido":              "Lido",
    "makerdao":          "MakerDAO (Sky)",
    "uniswap":           "Uniswap",
    "curve-dex":         "Curve",
    "compound-finance":  "Compound",
    "pancakeswap":       "PancakeSwap",
    "eigenlayer":        "EigenLayer",
}


class DeFiLlamaConnector(Connector):
    source_id = "defillama"
    name = "DeFiLlama"
    license_id = "defillama-open"
    schedule = "0 7 * * *"          # daily; TVL snapshots update once per day
    attribution = "Source: DeFiLlama (https://defillama.com)"
    homepage = "https://defillama.com"

    # -- HTTP helper: polite UA, retries with backoff ------------------------
    def _get(self, url: str, *, tries: int = 4, timeout: int = 90):
        last = None
        for attempt in range(tries):
            try:
                r = requests.get(url, timeout=timeout, headers={"User-Agent": UA})
                if r.status_code == 429 or r.status_code >= 500:
                    raise requests.HTTPError(f"{r.status_code} for {url}")
                r.raise_for_status()
                return r.json()
            except (requests.RequestException, ValueError) as e:
                last = e
                if attempt < tries - 1:
                    time.sleep(2 * (attempt + 1))  # 2s, 4s, 6s
        raise RuntimeError(f"DeFiLlama request failed after {tries} tries: {url} ({last})")

    # -- value/date coercion -------------------------------------------------
    @staticmethod
    def _to_date(ts) -> Optional[dt.date]:
        try:
            return dt.datetime.fromtimestamp(int(ts), dt.timezone.utc).date()
        except (TypeError, ValueError, OverflowError, OSError):
            return None

    @staticmethod
    def _to_value(v) -> Optional[float]:
        if v is None:
            return None
        try:
            f = float(v)
        except (TypeError, ValueError):
            return None
        if f != f or f < 0:            # NaN or negative -> drop (TVL is >= 0)
            return None
        return f

    def _series_meta(self, sid: str, title: str, category: str, geo, meta: dict) -> SeriesMeta:
        return SeriesMeta(sid, title, "D", "USD", geo, category, self.license_id, meta)

    # -- discover ------------------------------------------------------------
    def discover(self) -> list[SeriesMeta]:
        out = [
            self._series_meta(
                "defillama:tvl:total", "Total DeFi TVL (USD)",
                "crypto", None, {"endpoint": "v2/historicalChainTvl"}),
            self._series_meta(
                "defillama:stablecoins:total_usd", "Total stablecoin market cap (USD)",
                "crypto", None, {"endpoint": "stablecoincharts/all"}),
        ]
        for chain, title in CHAINS.items():
            out.append(self._series_meta(
                f"defillama:chain_tvl:{chain}", f"DeFi TVL on {title} (USD)",
                "crypto", None, {"chain": chain}))
        for slug, title in PROTOCOLS.items():
            out.append(self._series_meta(
                f"defillama:protocol_tvl:{slug}", f"{title} protocol TVL (USD)",
                "crypto", None, {"protocol": slug}))
        return out

    # -- fetch ---------------------------------------------------------------
    def fetch(self, since: Optional[dt.date] = None):
        def keep(d: dt.date) -> bool:
            return since is None or d >= since

        # 1) Total DeFi TVL
        meta = self._series_meta(
            "defillama:tvl:total", "Total DeFi TVL (USD)",
            "crypto", None, {"endpoint": "v2/historicalChainTvl"})
        obs = self._points_tvl(f"{API}/v2/historicalChainTvl", "tvl", meta.series_id, keep)
        if obs:
            yield meta, obs

        # 2) Per-chain TVL
        for chain, title in CHAINS.items():
            sid = f"defillama:chain_tvl:{chain}"
            m = self._series_meta(sid, f"DeFi TVL on {title} (USD)", "crypto", None, {"chain": chain})
            try:
                rows = self._points_tvl(
                    f"{API}/v2/historicalChainTvl/{requests.utils.quote(chain)}",
                    "tvl", sid, keep)
            except RuntimeError:
                continue           # one bad chain shouldn't sink the whole run
            if rows:
                yield m, rows

        # 3) Blue-chip protocol TVL
        for slug, title in PROTOCOLS.items():
            sid = f"defillama:protocol_tvl:{slug}"
            m = self._series_meta(sid, f"{title} protocol TVL (USD)", "crypto", None, {"protocol": slug})
            try:
                payload = self._get(f"{API}/protocol/{slug}")
            except RuntimeError:
                continue
            rows = self._points_from(payload.get("tvl") or [], "totalLiquidityUSD", sid, keep)
            if rows:
                yield m, rows

        # 4) Total stablecoin market cap (USD)
        sid = "defillama:stablecoins:total_usd"
        m = self._series_meta(sid, "Total stablecoin market cap (USD)", "crypto", None,
                              {"endpoint": "stablecoincharts/all"})
        try:
            payload = self._get(f"{STABLE_API}/stablecoincharts/all")
        except RuntimeError:
            payload = []
        rows = []
        for p in payload or []:
            d = self._to_date(p.get("date"))
            if d is None or not keep(d):
                continue
            usd = (p.get("totalCirculatingUSD") or {}).get("peggedUSD")
            v = self._to_value(usd)
            if v is None:
                continue
            rows.append(Observation(sid, d, v, version="clean"))
        if rows:
            yield m, rows

    # -- point extractors ----------------------------------------------------
    def _points_tvl(self, url: str, value_key: str, sid: str, keep) -> list[Observation]:
        payload = self._get(url)
        return self._points_from(payload or [], value_key, sid, keep)

    def _points_from(self, payload, value_key: str, sid: str, keep) -> list[Observation]:
        rows: list[Observation] = []
        for p in payload:
            d = self._to_date(p.get("date"))
            if d is None or not keep(d):
                continue
            v = self._to_value(p.get(value_key))
            if v is None:
                continue
            rows.append(Observation(sid, d, v, version="clean"))
        return rows
