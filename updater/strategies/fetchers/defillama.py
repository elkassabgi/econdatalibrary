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
  chains      - chains_tvl.parquet  (one bulk /v2/historicalChainTvl for the __ALL__
                aggregate, PLUS one /v2/historicalChainTvl/<name> for each of the 14
                CATALOGUED chains — 973,352 B total, measured 2026-09-06T02:57:06Z.
                The other ~437 per-chain series stay with the heavy ingester: they are
                not catalogued, so nobody can download them, and all 451 would be ~31 MB
                a run. Before this, __ALL__ reached 2026-09-05 while every downloadable
                chain sat at 2026-06-04 — the file looked fresh and the served data was
                93 days stale.)
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
Dedup key for every grouped file is (series_key, obs_date). THAT KEY IS NOT UNIQUE ON DISK
TODAY, and this docstring used to claim it was (R773): swept 2026-09-06T03:02:33Z over all
112 R2 objects — 107 examined, 38,806,211 rows, 5 skipped and named — 33 files carry 21,759
duplicate pairs. Every file THIS fetcher writes is clean, because merge_and_write dedups; the
duplicates live exactly in the files no merge touches, written by jobs/ingest_defillama.py's
raw pq.write_parquet. Their cause is upstream: /protocol/<slug> appends an intraday "now"
point repeating the current day, and the ingester stored it beside the settled daily close, so
each file duplicates one date per key — the day it last ran. Seven of eight served protocol
CSVs hand a user that date twice with two different values. Repair: tools/repair_defillama_parents.py.
Catalog snapshots dedup on their natural primary key. A 200 that parses 0 rows from a
real body -> structural; timeout/5xx/429 -> transient (status='partial', retried).
"""
from __future__ import annotations
import datetime as dt
import os
from urllib.parse import quote as _urlquote      # 'OP Mainnet' has a space; safe="" encodes it

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ... import config, blob, merge
from ..base import Result
from ._common import (CURSOR_CAP, Deadline, Tally, finalize, load_rotation, merge_cursor_map,
                      rotate_after, save_rotation)
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
    ("fees", "dailyBribesRevenue", "fees_dailyBribesRevenue"),   # RETIRED upstream, see below
    ("fees", "dailyTokenTaxes", "fees_dailyTokenTaxes"),
    ("dexs", "dailyVolume", "dexs_dailyVolume"),
    ("options", "dailyNotionalVolume", "options_dailyNotionalVolume"),
    ("options", "dailyPremiumVolume", "options_dailyPremiumVolume"),
    ("aggregators", "dailyVolume", "aggregators_dailyVolume"),
    ("aggregator-derivatives", "dailyVolume", "aggregator_derivatives_dailyVolume"),
    # dataType RENAMED upstream: bridge-aggregators stopped accepting "dailyVolume" and now
    # wants "dailyBridgeVolume". The old name does not 400 — DefiLlama answers 500 Internal
    # server error, which we (correctly) classify as transient, so this sub-unit retried
    # forever and silently never produced a file. Every other type still takes "dailyVolume",
    # which is why the break looks arbitrary. Filename kept: it still describes the metric,
    # and renaming it would move the series keys downstream for no gain.
    ("bridge-aggregators", "dailyBridgeVolume", "bridge_aggregators_dailyVolume"),
]

# Metrics DefiLlama has stopped publishing. The endpoint still answers 200 with a
# well-formed envelope -- every expected key present, allChains populated -- but
# `protocols: []`, an empty breakdown, and `totalAllTime: 0`. That last field is the
# tell: a merely quiet day still reports a non-zero all-time total, so a zero there
# means the series is gone upstream, not idle.
#
# We keep what we already collected (merge is never-shrink: 17,030 rows, 2021-05-19
# to 2026-06-24, which is exactly where it stopped) and go on probing it, but a zero
# parse here counts as EMPTY rather than STRUCTURAL. Removing the job outright would
# have been the easy fix and the wrong one: it would also remove the probe, so if
# DefiLlama resumes the metric we would never notice. This way recovery is automatic
# -- the day a real body comes back, added_unit() picks it up with no code change --
# while a permanently-retired metric stops holding the whole source at `partial`.
#   Confirmed retired 2026-07-27; last real observation 2026-06-24.
#
# fees_dailyTokenTaxes joined the same class: probed 2026-08-27 with the same
# tell — totalAllTime: 0, protocols: [], empty totalDataChart. Our store keeps
# 1,690 rows, 2023-06-14 to 2026-06-15 (where it stopped). Same recovery
# property: the probe continues, and a real body resumes ingestion untouched.
RETIRED_UPSTREAM = {"fees_dailyBribesRevenue.parquet",
                    "fees_dailyTokenTaxes.parquet"}


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


# set by _merge_file when the cursor cap bites; cleared at the top of update()
_CURSORS_CAPPED: list = []


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


# The FOURTEEN chains whose per-chain series are CATALOGUED - the only ones a user can download.
# This tuple is the FALLBACK; _served_chains() prefers the catalogue at runtime (R778 #6).
#
# COST, at the right grain (R778 #8 corrected all of these; my first numbers were each a little
# flattering). The fourteen per-chain GETs are 973,352 B, measured 2026-09-06T02:57:06Z (7.4 s,
# 14/14 HTTP 200, every one carrying data through that day). The bulk __ALL__ call is a FURTHER
# 121,264 B, so the family costs 1,094,616 B a run, plus one /v2/chains listing. Refreshing all 451
# chain series would be ~31 MB, but that figure is extrapolated from the fourteen LARGEST and is an
# upper bound, not a measurement. R769 rule 3: count BYTES, not requests.
#
# WHAT WAS FROZEN: 449 of the 450 per-chain series sat at 2026-06-04 (one at 2022-07-25) while
# __ALL__ reached 2026-09-05. The other 436 stay with the ingester - they are not catalogued, so
# nobody can download them.
#
# A RE-PULL REWRITES HISTORY, AND THAT IS BY DESIGN (R778 #4, measured, previously undisclosed):
# against the current store, 0 stored dates are absent from the API (so this is NOT a re-grain -
# the risk never-shrink cannot see), 1,322 rows are new, and 8,118 of 26,402 overlapping values
# (30.7%) DIFFER, going back to 2019 - Avalanche 2021-02-03 moves 124 -> 3,612,846, Base 2024-06-01
# 1.623 B -> 1.521 B. DeFiLlama restates its own history as adapters are corrected; the fetcher's
# declared strategy is overwrite_if_changed, so taking the publisher's current numbers is right.
# Saying so is the part that was missing.
#
# THE WORK LIST COMES FROM THE CATALOGUE, NEVER FROM THE PUBLISHER'S LISTING (R61, R769 rule 7).
# Six of defillama's eight catalogued PROTOCOLS are absent from /protocols while live at their own
# endpoint, so a listing-driven enumeration reproduces exactly the hole this change exists to close.
# tests/test_defillama_served_chains.py pins this tuple against catalog.db.
SERVED_CHAINS = ("Aptos", "Arbitrum", "Avalanche", "BSC", "Base", "Bitcoin", "Ethereum",
                 "Near", "OP Mainnet", "Polygon", "Solana", "Starknet", "Sui", "Tron")


CHAINS_BUDGET_MIN = 12
CHAINS_ROTATION = "_chains_rotation.json"
# /v2/chains returned 473 names at 02:37Z and 466 at 04:46Z. A listing far below that is not a
# smaller world, it is a broken read, and treating it as truth marks every chain retired.
CHAIN_LISTING_FLOOR = 50


def _live_chain_names(sess, work):
    """The publisher's chain listing, or None meaning UNKNOWN - classify nothing as retired.

    R780 #2: this was guarded only by `isinstance(lc, list)`, so an empty list gave set(), a
    renamed `name` field gave {None}, and a list of strings gave set() - each non-None and each
    making EVERY chain read as 'absent from the listing', i.e. retired. An absence check is void
    without a known-PRESENT control in the same call (R338/R346), and my own two-sided control
    used `[]` as its stand-in for 'retired' - the same bytes a broken listing produces - so it
    pinned the misclassification as the expected result.

    Two controls, both cheap: the listing must be plausibly large, and it must contain at least
    one chain we are actually asking about. Either failing makes the oracle UNKNOWN rather than
    empty, and an unknown oracle downgrades every failure to an honest transient."""
    lc = _get(sess, "https://api.llama.fi/v2/chains")
    if not isinstance(lc, list):
        print("  [defillama] chain listing unavailable; no chain will be classed as retired",
              flush=True)
        return None
    names = {c.get("name") for c in lc if isinstance(c, dict) and isinstance(c.get("name"), str)}
    # THE FLOOR IS THE CONTROL, and it is the one that discriminates: every broken shape the
    # reviewer named - `[]`, a renamed `name` field, a list of bare strings - yields ZERO usable
    # names, so a plausible size is exactly the evidence that the read worked.
    if len(names) < CHAIN_LISTING_FLOOR:
        print(f"  [defillama] chain listing failed its size control ({len(names)} usable names, "
              f"floor {CHAIN_LISTING_FLOOR}) - NOT trusting it, so no chain will be classed as "
              f"retired", flush=True)
        return None
    # MASS ABSENCE IS AN INSTRUMENT FAILURE, NOT FOURTEEN SIMULTANEOUS RETIREMENTS. My first
    # version required at least one of OUR chains to be present, which my own positive control
    # then proved made the retired path UNREACHABLE - the very condition that defines retirement
    # (this chain is absent) also tripped the trust check, so 150 green cases were vacuous. The
    # honest split is by proportion: one chain gone is a retirement, most of them gone at once is
    # the listing having changed shape under us.
    present = len(names & set(work))
    if work and present < max(1, len(work) // 2):
        print(f"  [defillama] chain listing holds only {present} of our {len(work)} chains - that "
              f"is a listing change, not {len(work) - present} simultaneous retirements; NOT "
              f"trusting it for retirement classification", flush=True)
        return None
    return names


def _served_chains(say=True):
    """The work list, read from the CATALOGUE at runtime where it exists, falling back to the
    pinned tuple only when it genuinely is not there.

    R778 #6: a constant checked only by a test that SKIPS wherever catalog.db is absent is not a
    pin — and tests.yml pulls no catalogue, so it never ran in CI. `updater-daily.yml` DOES pull
    catalog.db and exports ECONDL_CATALOG (:166, :193-207), which is the run that matters, so the
    list is taken from the catalogue THERE and the constant is only a fallback. Path resolution is
    copied from orchestrate.py:1067 rather than re-typed (R66). Prints what RESOLVED, never what
    was configured (R335)."""
    cat = os.environ.get("ECONDL_CATALOG") or os.path.join(config.ROOT, "data", "catalog.db")
    if os.path.exists(cat):
        try:
            import sqlite3
            con = sqlite3.connect(f"file:{cat}?mode=ro", uri=True)
            try:
                rows = con.execute(
                    "SELECT series_id FROM series WHERE source_id='defillama' AND series_id LIKE ?",
                    ("defillama:chain_tvl:%",)).fetchall()
            finally:
                con.close()
            names = tuple(sorted(r[0].split("chain_tvl:", 1)[1] for r in rows if r[0]))
            if names:
                if say:
                    # R780 #6: DIFF against the pin and say so. The catalogue this reads can be
                    # stale - ECONDL_CATALOG points at the R2 coherence copy, measured ten days
                    # old (LastModified 2026-08-27T15:30:12Z) and refreshed only by a tool that is
                    # classifier-blocked (R250) - so silently preferring it would hide a drift in
                    # either direction.
                    gone = sorted(set(SERVED_CHAINS) - set(names))
                    new = sorted(set(names) - set(SERVED_CHAINS))
                    print(f"  [defillama] chain work list: {len(names)} from the CATALOGUE {cat}"
                          + (f"; not in the pinned list: {new}" if new else "")
                          + (f"; pinned but no longer catalogued: {gone}" if gone else "")
                          + ("; identical to the pinned list" if not new and not gone else ""),
                          flush=True)
                return names
            if say:
                # R780 #6: this used to print "no catalogue at <path>" for a catalogue that EXISTS
                # and simply holds no chain rows - R335's own rule (print what RESOLVED, not what
                # was configured) broken inside a docstring citing R335.
                print(f"  [defillama] catalogue {cat} EXISTS but holds no defillama chain_tvl "
                      f"rows; falling back to the pinned list of {len(SERVED_CHAINS)}", flush=True)
            return SERVED_CHAINS
        except Exception as ex:                      # a broken read must not stop the fetch
            if say:
                print(f"  [defillama] catalogue {cat} unreadable ({type(ex).__name__}: {ex}); "
                      f"falling back to the pinned list of {len(SERVED_CHAINS)}", flush=True)
            return SERVED_CHAINS
    if say:
        print(f"  [defillama] chain work list: {len(SERVED_CHAINS)} from the PINNED constant "
              f"(no catalogue file at {cat})", flush=True)
    return SERVED_CHAINS


def _chains_tvl_aggregate(sess, tally=None):
    """Refresh the __ALL__ aggregate AND the fourteen SERVED per-chain series of chains_tvl.parquet.

    WHY THEY WERE FROZEN. The bulk /v2/historicalChainTvl call carries only the __ALL__ total, so
    the 14 catalogued per-chain series were left to jobs/ingest_defillama.py - which nothing
    schedules. Measured 2026-09-06T02:26Z: __ALL__ reached 2026-09-05 while all 450 per-chain series
    stopped at 2026-06-04, i.e. the file looked fresh and every downloadable chain was 93 days stale.

    ONE TABLE, DELIBERATELY (R769 finding 3 / R44). _merge_file books structural_unit whenever a
    parsed table is empty while the file had rows, and finalize() raises that as a whole-source
    DefinitiveError. It does NOT "publish nothing" - finalize runs after every merge, so the earlier
    families' writes stand and what is lost is the status, the vintage and last_success (R778 #8
    corrected me on that). If each chain were merged into its own file, one renamed chain would take
    the source down that way. Accumulating them into the single table this file already uses means
    a 404 costs that chain's rows and nothing else.

    BUT THE VETO IS NOT "UNREACHABLE", WHICH IS WHAT I CLAIMED AND MEASURED WRONG. My control made
    every CHAIN bogus and never the BULK call, and the bulk failure used to return before the loop
    ran - so a bulk 404 emptied the table and vetoed the source with all fourteen chains healthy.
    Fixed below; the honest statement now is that no SINGLE entity's failure can empty the table,
    and a total outage returns None (transient, keep old data) rather than an empty table.

    NO DEDUP HERE, AND THAT IS MEASURED. /protocol/<slug> appends an intraday "now" point repeating
    the current day, and storing it beside the settled close is what minted 21,759 duplicate pairs
    across this store (R773). This endpoint does NOT: 0 duplicate dates across all fourteen at
    2026-09-06T03:42:09Z. So a first-wins rule would be cargo-culted from the other endpoint. A
    duplicate here would mean the shape CHANGED, which must be loud rather than quietly deduped -
    so it is a named per-chain refusal, and the chain is skipped rather than guessed at.
    """
    keys, dates, vals = [], [], []

    # ---- the bulk __ALL__ call -------------------------------------------------------------
    # R778 #2: this used to RETURN on an error or a non-list, before the per-chain loop below ever
    # ran - so a bulk 404 produced an EMPTY table while the file had rows, which _merge_file books
    # as structural_unit and finalize() raises as a whole-source DefinitiveError. My claim that the
    # veto was unreachable was measured on the wrong axis: I made every CHAIN bogus and never the
    # bulk call (a one-sided test over a two-sided space, R322). The bulk failure is now recorded
    # and the chains still run.
    #
    # R778 #1: __ALL__ was also the one entity of fifteen with NO failure class. `structural_unit`
    # is FILE-grained, so putting fifteen entities in one table turned it into "at least one of
    # fifteen parsed something" - a bulk call returning `[]`, or a renamed field, gave __ALL__ zero
    # rows with status ok and the vintage ADVANCED, freezing `defillama:tvl:total` (the headline
    # series) behind a file that looked fresh. That is precisely the defect this change exists to
    # fix, inverted onto the most-used series. This publisher has already served a well-formed 200
    # carrying zero rows twice (see RETIRED_UPSTREAM), so it is not hypothetical.
    d = _get(sess, "https://api.llama.fi/v2/historicalChainTvl")
    if _is_err(d):
        if tally is not None:
            tally.transient_unit(f"chains_tvl.parquet:__ALL__ ({d['__err__']})")
    elif not isinstance(d, list):
        if tally is not None:
            code = d.get("__http__") if _is_http(d) else type(d).__name__
            tally.transient_unit(f"chains_tvl.parquet:__ALL__ (HTTP/shape {code})")
    else:
        for pt in d:
            od = _to_date(pt.get("date"))
            if od is not None and isinstance(pt.get("tvl"), (int, float)):
                keys.append("__ALL__"); dates.append(od); vals.append(float(pt["tvl"]))
        if not keys and tally is not None:
            tally.transient_unit("chains_tvl.parquet:__ALL__ (200 but parsed 0 points - the bulk "
                                 "shape changed; tvl:total would freeze behind a fresh file)")

    # ---- the served per-chain series -------------------------------------------------------
    # R778 #3 / R299: a 404 is NOT transient. `transient_unit` makes finalize() return partial, and
    # a partial never sets last_success_utc (R231), so one permanently renamed chain would pin the
    # whole source at partial for ever - broken AND unmonitorable. The two causes are told apart by
    # a signal the run already has: the publisher's own chain listing. Absent from the listing AND
    # 404 = retired upstream, which is a human's job (re-catalogue or drop it) and is booked
    # non-demoting; present in the listing but failing = a genuine transient. R61 is respected -
    # listing absence alone never delists anything, it only classifies a failure that already
    # happened. The real detector for "a served series stopped moving" is the health gate's
    # RED-DATA, which watches the observation frontier, not the run status.
    work = list(_served_chains())
    listed = _live_chain_names(sess, work)

    # R780 #8: fifteen NEW GETs with no budget at all, on a fetcher whose observed runs are
    # 88.7-165.3 s. Worst case per GET is the retry stack - 766.5 s - so fifteen of them is 3.19 h
    # against orchestrate's 45-minute SIGALRM. A budget alone would be a TRUNCATION though, not a
    # bound (R190, and Deadline's own docstring): fourteen names in a FIXED order re-walk the same
    # prefix for ever. So the budget comes WITH a rotation bookmark, saved after every sub-unit and
    # after a complete pass, so the next run always starts somewhere new.
    dl = Deadline(minutes=CHAINS_BUDGET_MIN)
    out_dir = config.source_dir(SOURCE)
    work = rotate_after(work, load_rotation(out_dir, CHAINS_ROTATION))

    for name in work:
        if dl.spent():
            if tally is not None:
                tally.deferred_unit(f"chains_tvl.parquet:{name} (budget {CHAINS_BUDGET_MIN} min "
                                    f"spent; the rotation bookmark starts the next run here)")
            continue
        url = "https://api.llama.fi/v2/historicalChainTvl/" + _urlquote(name, safe="")
        c = _get(sess, url)
        label = f"chains_tvl.parquet:{name}"
        if _is_err(c):
            if tally is not None:
                tally.transient_unit(f"{label} ({c['__err__']})")
            continue                       # retry may help; the other chains still publish
        if _is_http(c) or not isinstance(c, list):
            code = c.get("__http__") if _is_http(c) else "shape"
            # R780 #7: `_get` maps BOTH 402 and 404 to __http__, so a Pro-gate paywall read as
            # "the publisher no longer has this chain". Only a 404 is even a candidate.
            retired = (code == 404 and listed is not None and name not in listed)
            if tally is not None:
                if retired:
                    # DEMOTING, and I reverted to that on measurement. I had booked this
                    # non-demoting on the argument that the health gate's RED-DATA would catch a
                    # served chain that stopped moving. R780 #3 measured the opposite with the real
                    # function: `health._recency_signal` is max(observed) across the whole unit and
                    # defillama is ONE unit, so 14 frozen chains and 1 frozen chain both report
                    # obs_age 0.2d and RED-DATA NO - it had ALREADY missed this very freeze while
                    # all fourteen sat 93 days stale. With no detector behind it, "non-demoting"
                    # just means silent, so coverage being incomplete is reported as what it is.
                    # R299 is satisfied by the MESSAGE naming the permanent cause and the action,
                    # which is what that rule actually asks for - not by the class.
                    tally.transient_unit(
                        f"{label} (HTTP 404 AND absent from /v2/chains - RETIRED upstream. This "
                        f"will not clear by retrying: re-catalogue it or delist it.)")
                    print(f"  [defillama] RETIRED UPSTREAM: {name} is catalogued but the publisher "
                          f"no longer serves or lists it - a human must re-catalogue or delist it",
                          flush=True)
                else:
                    tally.transient_unit(f"{label} (HTTP {code}"
                                         + ("; still in /v2/chains, so retry may help"
                                            if listed is not None else
                                            "; the chain listing could not be trusted, so this is "
                                            "NOT classified as retired")
                                         + ")")
            continue
        ck, cd, cv = [], [], []
        for pt in c:
            od = _to_date(pt.get("date"))
            if od is not None and isinstance(pt.get("tvl"), (int, float)):
                ck.append(name); cd.append(od); cv.append(float(pt["tvl"]))
        # R780 #1 - THE BRANCH THAT WAS NOT THERE, and a regression against the parent commit.
        # A 200 carrying a list that parses to ZERO points fell through everything: not _is_err,
        # not _is_http, it IS a list, `len(set([])) != len([])` is `0 != 0`, and `extend([])` is a
        # no-op - so a renamed `tvl` field, a string `tvl`, a renamed date or an all-null series
        # produced status ok, "+N new rows" and NO tally call anywhere. Before this change that
        # body gave a zero-row table and _merge_file booked structural_unit. It is R778 #1
        # verbatim, which I fixed for __ALL__ and did not carry to the fourteen - and it is silent
        # on exactly the shape RETIRED_UPSTREAM exists to document.
        if not ck:
            if tally is not None:
                tally.transient_unit(f"{label} (200 but parsed 0 points - the per-chain shape "
                                     f"changed; this series would freeze behind a fresh file)")
            continue
        if len(set(cd)) != len(cd):
            if tally is not None:
                tally.transient_unit(f"{label} (publisher returned a repeated obs_date - shape "
                                     f"changed; refusing to guess which value is the daily close)")
            continue
        keys.extend(ck); dates.extend(cd); vals.extend(cv)
        save_rotation(out_dir, name, CHAINS_ROTATION)

    if not keys:
        # NOTHING parsed - bulk and every chain failed. Returning an empty TABLE here would be
        # booked structural and raise a whole-source DefinitiveError; returning None is _merge_file's
        # "transient sub-failure: keep old data" path, which is the honest reading of a total
        # outage (retry helps) and leaves the other families' merges alone (R778 #2).
        return None, None, None, None, "chains: bulk and every served chain failed"
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
def _merge_file(path, tbl, dedup_keys, tally, cursors=None, keys=None, dates=None,
                qualify=None):
    """Publish one refreshed table under merge's never-shrink invariant and update the
    tally honestly. before>0 + 0 parsed rows -> structural; else add/empty.

    `qualify` maps an in-file series_key to the CATALOG suffix before the cursor is
    reported (2026-08-05): this store spells identity as FILE + bare key ('Bitcoin' in
    chains_tvl.parquet) while the catalog id is family-qualified
    ('defillama:chain_tvl:Bitcoin'), so unqualified cursors could never map under
    §5.7's exact rule and every run demoted to partial with all 24 served CSVs frozen
    ("the catalog this run read has 24 rows for it but none matched")."""
    before = blob.row_count(path)
    label = os.path.basename(path)           # names the offender in the failure message
    if tbl is None:                          # transient sub-failure: keep old data
        tally.transient_unit(label)
        return before, None
    if tbl.num_rows == 0:
        if before > 0 and label not in RETIRED_UPSTREAM:
            tally.structural_unit(label)     # 200 but parsed nothing from a real cube
        else:
            tally.empty_unit(label)          # retired upstream, or genuinely nothing yet
        return before, None
    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=dedup_keys)
    tally.added_unit(max(0, n - before))
    if cursors is not None and keys is not None and dates is not None:
        # BOUNDED (2026-07-30) — found by tools/audit_cursor_blowup.py. 38,466,591 store
        # rows folded one cursor per series with no cap. abs's version of this exact shape
        # (376M series, ~94 GB) destroyed the CI runner and took the whole daily updater
        # down with it; every cursor is also a state.db row and a _catalog_ids_for query.
        m = _ts_maxes(keys, dates)
        if qualify:
            m = {qualify(k): v for k, v in m.items()}
        if merge_cursor_map(cursors, m):
            _CURSORS_CAPPED.append(1)
    return n, md


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    sess = _session()
    tally = Tally()
    _CURSORS_CAPPED.clear()
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
    # Cursor keys are family-qualified to the CATALOG suffix (see _merge_file docstring):
    # '__ALL__' is served as defillama:tvl:total, a per-chain row as defillama:chain_tvl:<C>.
    path = os.path.join(out_dir, "chains_tvl.parquet")
    tbl, dk, keys, dates, err = _chains_tvl_aggregate(sess, tally)
    n, md = _merge_file(path, tbl, dk, tally, cursors, keys, dates,
                        qualify=lambda k: "tvl:total" if k == "__ALL__"
                        else f"chain_tvl:{k}")
    total += n
    acc(path, md)

    # ----- stablecoins_total.parquet: bulk __ALL__ aggregate -----
    path = os.path.join(out_dir, "stablecoins_total.parquet")
    tbl, dk, keys, dates, err = _stablecoins_total(sess)
    n, md = _merge_file(path, tbl, dk, tally, cursors, keys, dates,
                        qualify=lambda k: "stablecoins:total_usd" if k == "__ALL__"
                        else f"stablecoins:{k}")
    total += n
    acc(path, md)

    # ----- per-ENTITY families left untouched here (slow per-entity loops; see docstring).
    # Count their existing rows so the returned obs reflects the whole source.
    #
    # R36: blob.row_count below was routed, os.listdir was not. On a runner
    # (AQUEDUCT_BACKEND=r2) the local directory is absent, so this loop added NOTHING and the
    # run under-reported its own obs by every row in these families — silently, because a
    # smaller total is not an error.
    for fn in blob.list_parquets(out_dir):
        if (fn.startswith("tvl_protocol_shard")
                or fn.startswith("yields_pool_shard")
                or fn == "stablecoins_circulating.parquet"
                or fn == "bridge_aggregators_snapshot.parquet"):
            total += blob.row_count(os.path.join(out_dir, fn))

    # The big "all-empty window => structural" floor would false-positive on a quiet
    # day (every overview can legitimately have no new breakdown row), so raise it above
    # the attempted-unit count; real breaks are caught per-file via structural_unit().
    if _CURSORS_CAPPED:
        print(f"[defillama] cursor set hit the {CURSOR_CAP:,} cap — further changed series "
              f"are not individually reported", flush=True)
    return finalize(tally, total, maxd, source=SOURCE, series_cursors=cursors,
                    empty_window_floor=tally.attempted + 1)
