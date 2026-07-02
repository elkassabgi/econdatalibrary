"""S1 fetcher — DeFiLlama (TVL / fees / volumes / stablecoins / yields).

Open data (no key). DeFiLlama re-publishes the FULL history every day across many
bulk + per-entity endpoints; there is no since/startPeriod param and no manifest,
so the only honest refresh is "re-pull the whole table and overwrite via merge"
(registry.adapter.vintage_signal: poll-only, full-history overwrite). merge_and_write
dedups on the natural key, lets a revised value win, and never shrinks, so a plain
full re-pull is an idempotent daily delta.

This fetcher OWNS DeFiLlama's parquet files and reuses jobs/ingest_defillama.py's
EXACT URLs + parse logic. It refreshes the families whose whole history is reachable
through a HANDFUL of cheap bulk GETs:

  catalog     - _catalog_protocols/chains/stablecoins/yield_pools.parquet  (metadata
                snapshots; natural-key merge so a delisted entity's row is kept)
  overview    - fees_*/dexs_*/options_*/aggregators_* .parquet  (one bulk
                /overview/<type>?dataType=<dt>&...Breakdown=false call per file;
                ALL protocols' full daily history in one response)
  chains      - chains_tvl.parquet  (one /v2/historicalChainTvl aggregate +
                — left to the heavy ingester for the 450 per-chain series)
  stablecoins - stablecoins_total.parquet (one bulk /stablecoincharts/all)

The per-ENTITY families are NOT refreshed here and are deliberately left untouched
(their rows are still counted in the returned obs so the never-shrink view holds):
  tvl_protocol_shard00-29  (one /protocol/<slug> call per ~7.7k protocols)
  yields_pool_shard00-59   (one /chart/<pool> call per ~16k pools; the yields
                            endpoint is throttled to ~0.5 req/s and the JSONL stage
                            resumes by pool, not by date — registry open_question).
  stablecoins_circulating  (one /stablecoin/<id> call per ~380 assets).
These are slow per-entity loops (tens of minutes to hours) unsuited to a fast/daily
S1 tick; run jobs/ingest_defillama.py {tvl,stablecoins,yields} for those. Each is
fully idempotent-overwrite there, so skipping them here costs nothing but freshness
on the long tail, never data loss.

Schema (grouped time-series files): series_key(str) | obs_date(date32) | value(float64)
                                    [+ tvl_usd/apy/apy_base/apy_reward on yields].
Dedup key for every grouped file is (series_key, obs_date) — verified unique on disk.
Catalog snapshots dedup on their natural primary key. A 200 that parses 0 rows from a
real body -> structural; timeout/5xx/429 -> transient (status='partial', retried).
"""
from __future__ import annotations
import datetime as dt
import os

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ... import config, blob, merge
from ..base import Result
from ._common import Tally, finalize
from ._vintage import UA as UA_HDR

SOURCE = "defillama"
UA = "Econ-Fin Data Library admin@hfdatalibrary.com"

# Cheap vintage probe: the protocols bulk file exposes ETag + Last-Modified and is
# regenerated on every daily refresh of the whole source.
VINTAGE_URL = "https://api.llama.fi/protocols"

DEDUP_TS = ("series_key", "obs_date")          # grouped time-series files
FLOATCOLS = {"value", "tvl_usd", "apy", "apy_base", "apy_reward",
             "circulating_usd", "circulating", "tvl", "mcap", "price"}

# Overview bulk jobs — copied verbatim from jobs/ingest_defillama.OVERVIEW_JOBS so the
# (type,dataType)->filename mapping and the produced files stay identical.
OVERVIEW_JOBS = [
    ("fees", "dailyFees", "fees_dailyFees"),
    ("fees", "dailyRevenue", "fees_dailyRevenue"),
    ("fees", "dailyHoldersRevenue", "fees_dailyHoldersRevenue"),
    ("fees", "dailySupplySideRevenue", "fees_dailySupplySideRevenue"),
    ("fees", "dailyProtocolRevenue", "fees_dailyProtocolRevenue"),
    ("fees", "dailyUserFees", "fees_dailyUserFees"),
    ("fees", "dailyBribesRevenue", "fees_dailyBribesRevenue"),
    ("fees", "dailyTokenTaxes", "fees_dailyTokenTaxes"),
    ("dexs", "dailyVolume", "dexs_dailyVolume"),
    ("options", "dailyNotionalVolume", "options_dailyNotionalVolume"),
    ("options", "dailyPremiumVolume", "options_dailyPremiumVolume"),
    ("aggregators", "dailyVolume", "aggregators_dailyVolume"),
    ("aggregator-derivatives", "dailyVolume", "aggregator_derivatives_dailyVolume"),
    ("bridge-aggregators", "dailyVolume", "bridge_aggregators_dailyVolume"),
]


# --------------------------------------------------------------------------- #
# vintage
# --------------------------------------------------------------------------- #
def current_vintage(unit):
    """ETag / Last-Modified of the protocols bulk file (HEAD). None if undeterminable
    (the strategy then fetches anyway, which is safe under merge's never-shrink)."""
    from ._vintage import http_vintage
    return http_vintage(VINTAGE_URL)


# --------------------------------------------------------------------------- #
# http (mirrors jobs/ingest_defillama.get(): retry/backoff, 402/404 -> sentinel)
# --------------------------------------------------------------------------- #
def _session():
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    retry = Retry(total=5, backoff_factor=1.5,
                  status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET"], respect_retry_after_header=True)
    ad = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=4)
    s.mount("https://", ad)
    return s


def _get(sess, url, timeout=120):
    """200 -> parsed json; 402/404 -> {'__http__': code}; transient/other -> {'__err__': msg}.
    Network/timeout never raises here — the caller maps the sentinel to tally.transient_unit()."""
    try:
        r = sess.get(url, timeout=timeout)
    except (requests.Timeout, requests.ConnectionError) as e:
        return {"__err__": repr(e)}
    except requests.RequestException as e:
        return {"__err__": repr(e)}
    if r.status_code == 200:
        try:
            return r.json()
        except ValueError as e:
            return {"__err__": f"bad json: {e}"}
    if r.status_code in (402, 404):
        return {"__http__": r.status_code}
    if r.status_code in (429, 500, 502, 503, 504):
        return {"__err__": f"HTTP{r.status_code}"}
    return {"__err__": f"HTTP{r.status_code}"}


def _is_err(d):
    return isinstance(d, dict) and "__err__" in d


def _is_http(d):
    return isinstance(d, dict) and "__http__" in d


# --------------------------------------------------------------------------- #
# parse helpers (mirror jobs/ingest_defillama)
# --------------------------------------------------------------------------- #
def _to_date(ts):
    if ts is None:
        return None
    if isinstance(ts, str):
        ts = ts.strip()
        if "T" in ts or ("-" in ts and ":" in ts):
            try:
                return dt.datetime.fromisoformat(ts.replace("Z", "+00:00")).date()
            except ValueError:
                return None
        try:
            ts = float(ts)
        except ValueError:
            return None
    try:
        ts = float(ts)
        if ts > 1e12:
            ts /= 1000.0
        return dt.datetime.utcfromtimestamp(ts).date()
    except (ValueError, OverflowError, OSError):
        return None


def _f(x):
    return float(x) if isinstance(x, (int, float)) else None


def _table_from_cols(cols):
    """cols = dict name->list. Type per ingester: obs_date->date32, FLOATCOLS->float64,
    else string ('' for None). Returns a pyarrow.Table (may be 0 rows)."""
    arrays = {}
    for k, v in cols.items():
        if k == "obs_date":
            arrays[k] = pa.array(v, type=pa.date32())
        elif k in FLOATCOLS:
            arrays[k] = pa.array([float(x) if isinstance(x, (int, float)) else None
                                  for x in v], type=pa.float64())
        else:
            arrays[k] = pa.array(["" if x is None else str(x) for x in v], type=pa.string())
    return pa.table(arrays)


def _ts_maxes(keys, dates):
    """{series_key: 'YYYY-MM-DD'} of the max obs_date per series in this batch."""
    out = {}
    for k, d in zip(keys, dates):
        if d is None:
            continue
        if k not in out or d > out[k]:
            out[k] = d
    return {k: v.isoformat() for k, v in out.items()}


# --------------------------------------------------------------------------- #
# per-file refreshers — each returns (tbl, dedup_keys, err) ; tbl=None on transient
# --------------------------------------------------------------------------- #
def _catalog_protocols(sess):
    d = _get(sess, "https://api.llama.fi/protocols")
    if _is_err(d):
        return None, None, d["__err__"]
    if not isinstance(d, list):
        return _table_from_cols({"protocol_id": []}), ("protocol_id",), None
    cols = {k: [] for k in ["protocol_id", "name", "slug", "category", "chain",
                            "chains", "gecko_id", "cmc_id", "symbol", "url",
                            "twitter", "parent", "tvl", "mcap", "listed_at"]}
    for p in d:
        cols["protocol_id"].append(str(p.get("id", "")))
        cols["name"].append(p.get("name") or "")
        cols["slug"].append(p.get("slug") or "")
        cols["category"].append(p.get("category") or "")
        cols["chain"].append(p.get("chain") or "")
        cols["chains"].append(",".join(p.get("chains") or []))
        cols["gecko_id"].append(p.get("gecko_id") or "")
        cols["cmc_id"].append(str(p.get("cmcId") or ""))
        cols["symbol"].append(p.get("symbol") or "")
        cols["url"].append(p.get("url") or "")
        cols["twitter"].append(p.get("twitter") or "")
        cols["parent"].append(str(p.get("parentProtocol") or ""))
        cols["tvl"].append(_f(p.get("tvl")))
        cols["mcap"].append(_f(p.get("mcap")))
        cols["listed_at"].append(str(p.get("listedAt") or ""))
    return _table_from_cols(cols), ("protocol_id",), None


def _catalog_chains(sess):
    d = _get(sess, "https://api.llama.fi/v2/chains")
    if _is_err(d):
        return None, None, d["__err__"]
    if not isinstance(d, list):
        return _table_from_cols({"name": []}), ("name",), None
    cols = {k: [] for k in ["name", "chain_id", "gecko_id", "token_symbol", "cmc_id", "tvl"]}
    for c in d:
        cols["name"].append(c.get("name") or "")
        cols["chain_id"].append(str(c.get("chainId") or ""))
        cols["gecko_id"].append(c.get("gecko_id") or "")
        cols["token_symbol"].append(c.get("tokenSymbol") or "")
        cols["cmc_id"].append(str(c.get("cmcId") or ""))
        cols["tvl"].append(_f(c.get("tvl")))
    return _table_from_cols(cols), ("name",), None


def _catalog_stablecoins(sess):
    d = _get(sess, "https://stablecoins.llama.fi/stablecoins?includePrices=true")
    if _is_err(d):
        return None, None, d["__err__"]
    if not isinstance(d, dict):
        return _table_from_cols({"stablecoin_id": []}), ("stablecoin_id",), None
    assets = d.get("peggedAssets", []) or []
    cols = {k: [] for k in ["stablecoin_id", "name", "symbol", "gecko_id",
                            "peg_type", "peg_mechanism", "price", "chains"]}
    for a in assets:
        cols["stablecoin_id"].append(str(a.get("id", "")))
        cols["name"].append(a.get("name") or "")
        cols["symbol"].append(a.get("symbol") or "")
        cols["gecko_id"].append(a.get("gecko_id") or "")
        cols["peg_type"].append(a.get("pegType") or "")
        cols["peg_mechanism"].append(a.get("pegMechanism") or "")
        cols["price"].append(_f(a.get("price")))
        cols["chains"].append(",".join(a.get("chains") or []))
    return _table_from_cols(cols), ("stablecoin_id",), None


def _catalog_yield_pools(sess):
    d = _get(sess, "https://yields.llama.fi/pools")
    if _is_err(d):
        return None, None, d["__err__"]
    if not isinstance(d, dict):
        return _table_from_cols({"pool_id": []}), ("pool_id",), None
    pools = d.get("data", []) or []
    cols = {k: [] for k in ["pool_id", "chain", "project", "symbol", "tvl_usd",
                            "apy", "apy_base", "apy_reward", "stablecoin", "il_risk",
                            "exposure", "pool_meta"]}
    for p in pools:
        cols["pool_id"].append(p.get("pool") or "")
        cols["chain"].append(p.get("chain") or "")
        cols["project"].append(p.get("project") or "")
        cols["symbol"].append(p.get("symbol") or "")
        cols["tvl_usd"].append(_f(p.get("tvlUsd")))
        cols["apy"].append(_f(p.get("apy")))
        cols["apy_base"].append(_f(p.get("apyBase")))
        cols["apy_reward"].append(_f(p.get("apyReward")))
        cols["stablecoin"].append(str(p.get("stablecoin")))
        cols["il_risk"].append(p.get("ilRisk") or "")
        cols["exposure"].append(p.get("exposure") or "")
        cols["pool_meta"].append(str(p.get("poolMeta") or ""))
    return _table_from_cols(cols), ("pool_id",), None


def _overview(sess, typ, dtype):
    url = (f"https://api.llama.fi/overview/{typ}"
           f"?dataType={dtype}&excludeTotalDataChart=true"
           f"&excludeTotalDataChartBreakdown=false")
    d = _get(sess, url, timeout=180)
    if _is_err(d):
        return None, None, None, None, d["__err__"]
    if _is_http(d) or not isinstance(d, dict):
        # 402/404 or unexpected shape -> empty (structural caught by 0 rows + before>0)
        return _table_from_cols({"series_key": [], "obs_date": [], "value": []}), \
            DEDUP_TS, [], [], None
    tdcb = d.get("totalDataChartBreakdown") or []
    keys, dates, vals = [], [], []
    for day in tdcb:
        if not isinstance(day, list) or len(day) < 2:
            continue
        od = _to_date(day[0])
        if od is None:
            continue
        entry = day[1]
        if not isinstance(entry, dict):
            continue
        for proto, v in entry.items():
            if isinstance(v, dict):
                vv = sum(x for x in v.values() if isinstance(x, (int, float)))
            elif isinstance(v, (int, float)):
                vv = float(v)
            else:
                continue
            keys.append(str(proto)); dates.append(od); vals.append(float(vv))
    tbl = _table_from_cols({"series_key": keys, "obs_date": dates, "value": vals})
    return tbl, DEDUP_TS, keys, dates, None


def _chains_tvl_aggregate(sess):
    """Refresh ONLY the __ALL__ aggregate series of chains_tvl.parquet via the single
    bulk /v2/historicalChainTvl call. The 450 per-chain series stay as-is (one GET each
    -> heavy ingester). merge keeps every existing chain series untouched and never
    shrinks, so refreshing just the aggregate is a safe partial advance of this file."""
    d = _get(sess, "https://api.llama.fi/v2/historicalChainTvl")
    if _is_err(d):
        return None, None, None, None, d["__err__"]
    if not isinstance(d, list):
        return _table_from_cols({"series_key": [], "obs_date": [], "value": []}), \
            DEDUP_TS, [], [], None
    keys, dates, vals = [], [], []
    for pt in d:
        od = _to_date(pt.get("date"))
        if od is not None and isinstance(pt.get("tvl"), (int, float)):
            keys.append("__ALL__"); dates.append(od); vals.append(float(pt["tvl"]))
    tbl = _table_from_cols({"series_key": keys, "obs_date": dates, "value": vals})
    return tbl, DEDUP_TS, keys, dates, None


def _stablecoins_total(sess):
    """Refresh stablecoins_total.parquet (__ALL__ circulating-USD) via one bulk call."""
    d = _get(sess, "https://stablecoins.llama.fi/stablecoincharts/all")
    if _is_err(d):
        return None, None, None, None, d["__err__"]
    if not isinstance(d, list):
        return _table_from_cols({"series_key": [], "obs_date": [], "value": []}), \
            DEDUP_TS, [], [], None
    keys, dates, vals = [], [], []
    for pt in d:
        od = _to_date(pt.get("date"))
        cu = pt.get("totalCirculatingUSD")
        if isinstance(cu, dict):
            cu = sum(x for x in cu.values() if isinstance(x, (int, float)))
        if od is not None and isinstance(cu, (int, float)):
            keys.append("__ALL__"); dates.append(od); vals.append(float(cu))
    tbl = _table_from_cols({"series_key": keys, "obs_date": dates, "value": vals})
    return tbl, DEDUP_TS, keys, dates, None


# --------------------------------------------------------------------------- #
# orchestration helpers
# --------------------------------------------------------------------------- #
def _merge_file(path, tbl, dedup_keys, tally, cursors=None, keys=None, dates=None):
    """Publish one refreshed table under merge's never-shrink invariant and update the
    tally honestly. before>0 + 0 parsed rows -> structural; else add/empty."""
    before = blob.row_count(path)
    if tbl is None:                          # transient sub-failure: keep old data
        tally.transient_unit()
        return before, None
    if tbl.num_rows == 0:
        if before > 0:
            tally.structural_unit()          # 200 but parsed nothing from a real cube
        else:
            tally.empty_unit()
        return before, None
    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=dedup_keys)
    tally.added_unit(max(0, n - before))
    if cursors is not None and keys is not None and dates is not None:
        for sk, d in _ts_maxes(keys, dates).items():
            prev = cursors.get(sk)
            if prev is None or d > prev:
                cursors[sk] = d
    return n, md


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    sess = _session()
    tally = Tally()
    cursors: dict[str, str] = {}
    total = 0
    maxd = None

    def acc(path, md):
        nonlocal maxd
        if md and (maxd is None or md > maxd):
            maxd = md

    # ----- catalog metadata snapshots (natural-key merge; never-shrink keeps delisted) -----
    for fn, fetch in [("_catalog_protocols.parquet", _catalog_protocols),
                      ("_catalog_chains.parquet", _catalog_chains),
                      ("_catalog_stablecoins.parquet", _catalog_stablecoins),
                      ("_catalog_yield_pools.parquet", _catalog_yield_pools)]:
        path = os.path.join(out_dir, fn)
        tbl, dk, err = fetch(sess)
        n, md = _merge_file(path, tbl, dk, tally)
        total += n

    # ----- overview: one bulk breakdown call per (type,dataType) file -----
    for typ, dtype, fname in OVERVIEW_JOBS:
        path = os.path.join(out_dir, f"{fname}.parquet")
        tbl, dk, keys, dates, err = _overview(sess, typ, dtype)
        n, md = _merge_file(path, tbl, dk, tally, cursors, keys, dates)
        total += n
        acc(path, md)

    # ----- chains_tvl.parquet: bulk __ALL__ aggregate only (per-chain via heavy ingester) -----
    path = os.path.join(out_dir, "chains_tvl.parquet")
    tbl, dk, keys, dates, err = _chains_tvl_aggregate(sess)
    n, md = _merge_file(path, tbl, dk, tally, cursors, keys, dates)
    total += n
    acc(path, md)

    # ----- stablecoins_total.parquet: bulk __ALL__ aggregate -----
    path = os.path.join(out_dir, "stablecoins_total.parquet")
    tbl, dk, keys, dates, err = _stablecoins_total(sess)
    n, md = _merge_file(path, tbl, dk, tally, cursors, keys, dates)
    total += n
    acc(path, md)

    # ----- per-ENTITY families left untouched here (slow per-entity loops; see docstring).
    # Count their existing rows so the returned obs reflects the whole source.
    for fn in os.listdir(out_dir):
        if not fn.endswith(".parquet"):
            continue
        if (fn.startswith("tvl_protocol_shard")
                or fn.startswith("yields_pool_shard")
                or fn == "stablecoins_circulating.parquet"
                or fn == "bridge_aggregators_snapshot.parquet"):
            total += blob.row_count(os.path.join(out_dir, fn))

    # The big "all-empty window => structural" floor would false-positive on a quiet
    # day (every overview can legitimately have no new breakdown row), so raise it above
    # the attempted-unit count; real breaks are caught per-file via structural_unit().
    return finalize(tally, total, maxd, source=SOURCE, series_cursors=cursors,
                    empty_window_floor=tally.attempted + 1)
