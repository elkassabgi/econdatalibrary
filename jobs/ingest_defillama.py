#!/usr/bin/env python3
"""Full-coverage grouped ingest of DeFiLlama.

Writes GROUPED Parquet to data/clean_full/defillama/ -- one file per cube,
each holding MANY series via a series-key column. License: defillama-open.

Families (phases):
  catalog    - protocols/chains/pools/stablecoins master lists (metadata)
  overview   - fees/revenue/dexs/options/aggregators/etc. -> ONE file per
               (type,dataType) holding ALL protocols' full daily history
  chains     - per-chain historical TVL -> ONE file (chain-tvl.parquet)
  stablecoins- per-asset circulating history -> grouped files
  tvl        - per-protocol TVL + chain-split history -> sharded grouped files
  yields     - per-pool APY/TVL history -> sharded grouped files

Usage:
  python jobs/ingest_defillama.py <phase> [--limit N] [--shard k/n]
  phases: catalog overview chains stablecoins tvl yields  (or 'all' lists order)

Anti-bloat: at most a few hundred Parquet files for the whole source.
Concurrency <= 6, polite UA, retry/backoff.
"""
import concurrent.futures as cf
import datetime as dt
import json
import os
import sys
import threading
import time

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT = os.path.join(ROOT, "data", "clean_full", "defillama")
UA = "Econ-Fin Data Library admin@hfdatalibrary.com"
LICENSE = "defillama-open"
MAXW = 6

os.makedirs(OUT, exist_ok=True)
_local = threading.local()


def sess():
    s = getattr(_local, "s", None)
    if s is None:
        s = requests.Session()
        s.headers.update({"User-Agent": UA})
        retry = Retry(total=5, backoff_factor=1.5,
                      status_forcelist=[429, 500, 502, 503, 504],
                      allowed_methods=["GET"], respect_retry_after_header=True)
        ad = HTTPAdapter(max_retries=retry, pool_connections=MAXW, pool_maxsize=MAXW)
        s.mount("https://", ad)
        _local.s = s
    return s


def get(url, timeout=120, tries=4):
    last = None
    for i in range(tries):
        try:
            r = sess().get(url, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (402, 404):
                return {"__http__": r.status_code}
            if r.status_code == 429:
                # honor Retry-After if present, else escalating backoff
                ra = r.headers.get("Retry-After")
                time.sleep(float(ra) if ra and ra.isdigit() else 2.0 * (i + 1))
                last = "HTTP429"
                continue
            last = f"HTTP{r.status_code}"
        except Exception as e:  # noqa: BLE001
            last = repr(e)
        time.sleep(1.5 * (i + 1))
    return {"__err__": last}


def to_date(ts):
    """Unix seconds (int or numeric str) -> date32; tolerate ms and ISO."""
    if ts is None:
        return None
    if isinstance(ts, str):
        ts = ts.strip()
        if "T" in ts or "-" in ts and ":" in ts:
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
        if ts > 1e12:  # milliseconds
            ts /= 1000.0
        return dt.datetime.utcfromtimestamp(ts).date()
    except (ValueError, OverflowError, OSError):
        return None


def _dedup_first(cols):
    """Drop later rows repeating a (series_key, obs_date) pair. Returns (cols, n_dropped).

    WHY, AND WHY *FIRST* (R773). `/protocol/<slug>` ends its `tvl` array with an intraday "now"
    point whose timestamp falls on the current day, and the settled 00:00 UTC close for that
    same day is already in the array. Both map to the same obs_date, this ingester wrote both,
    and the served CSV then hands a user one date twice with two different values: 33 of 107
    store objects carry 21,759 such pairs, and seven of the eight served protocol CSVs are
    affected.

    KEEPING THE FIRST OCCURRENCE IS MEASURED, NOT ASSUMED. Fetched 2026-09-06:

        /protocol/aave   2,302 points, exactly ONE duplicated date (today)
                         index 2300  ts=1788652800  00:00:00Z  18309869039   <- settled close
                         index 2301  ts=1788665243  03:27:23Z  18397673946   <- intraday "now"
        /protocol/lido   2,088 points, same shape (00:00:00Z then 01:53:59Z)

    So the settled close comes first and the intraday point is appended after it.

    AND THIS IS WHY THE OBVIOUS RULE WOULD HAVE BEEN WRONG. "Drop points that are not at
    midnight UTC" looks equivalent and is not: lido carries 146 non-midnight points out of
    2,088, only ONE of which is part of a duplicated pair. That rule would have deleted 145
    legitimate observations. Deduplicating on the pair keeps a lone non-midnight point as the
    only observation for its date and drops only the genuine repeat.

    One pass, one set (R85): rebuilding the seen-set per row is how a 90-minute repair happened.
    """
    keys, dates = cols["series_key"], cols["obs_date"]
    seen, keep = set(), []
    for i, pair in enumerate(zip(keys, dates)):
        if pair not in seen:
            seen.add(pair)
            keep.append(i)
    if len(keep) == len(keys):
        return cols, 0
    return {k: [v[i] for i in keep] for k, v in cols.items()}, len(keys) - len(keep)


def write_parquet(path, cols):
    """cols = dict of name->list (parallel). Writes if any rows."""
    n = len(next(iter(cols.values()))) if cols else 0
    if n == 0:
        return 0
    # Applied HERE rather than in phase_tvl because every grouped file this job writes is keyed
    # on (series_key, obs_date) and none of them goes through merge_and_write, which is the only
    # thing that dedups elsewhere. The catalogue phases have no obs_date column and are skipped.
    if "series_key" in cols and "obs_date" in cols:
        cols, dropped = _dedup_first(cols)
        if dropped:
            n = len(cols["series_key"])
            print(f"    dedup: dropped {dropped:,} row(s) repeating a (series_key, obs_date) "
                  f"pair in {os.path.basename(path)} - the intraday 'now' point (R773)",
                  flush=True)
    FLOATCOLS = {"value", "tvl_usd", "apy", "apy_base", "apy_reward",
                 "circulating_usd", "circulating", "tvl", "mcap", "price"}
    arrays = {}
    for k, v in cols.items():
        if k == "obs_date":
            arrays[k] = pa.array(v, type=pa.date32())
        elif k in FLOATCOLS:
            arrays[k] = pa.array([float(x) if isinstance(x, (int, float)) else None
                                  for x in v], type=pa.float64())
        else:
            arrays[k] = pa.array(["" if x is None else str(x) for x in v],
                                 type=pa.string())
    pq.write_table(pa.table(arrays), path, compression="zstd")
    return n


def count_parquet(path):
    try:
        return pq.read_metadata(path).num_rows
    except Exception:  # noqa: BLE001
        return 0


# ---------------------------------------------------------------------------
# PHASE: catalog (master metadata lists)
# ---------------------------------------------------------------------------
def phase_catalog():
    tot = 0
    # protocols
    d = get("https://api.llama.fi/protocols")
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
        cols["tvl"].append(float(p["tvl"]) if isinstance(p.get("tvl"), (int, float)) else None)
        cols["mcap"].append(float(p["mcap"]) if isinstance(p.get("mcap"), (int, float)) else None)
        cols["listed_at"].append(str(p.get("listedAt") or ""))
    tot += write_parquet(os.path.join(OUT, "_catalog_protocols.parquet"), cols)
    print(f"  protocols: {len(d)}", flush=True)

    # chains
    d = get("https://api.llama.fi/v2/chains")
    cols = {k: [] for k in ["name", "chain_id", "gecko_id", "token_symbol", "cmc_id", "tvl"]}
    for c in d:
        cols["name"].append(c.get("name") or "")
        cols["chain_id"].append(str(c.get("chainId") or ""))
        cols["gecko_id"].append(c.get("gecko_id") or "")
        cols["token_symbol"].append(c.get("tokenSymbol") or "")
        cols["cmc_id"].append(str(c.get("cmcId") or ""))
        cols["tvl"].append(float(c["tvl"]) if isinstance(c.get("tvl"), (int, float)) else None)
    tot += write_parquet(os.path.join(OUT, "_catalog_chains.parquet"), cols)
    print(f"  chains: {len(d)}", flush=True)

    # stablecoins
    d = get("https://stablecoins.llama.fi/stablecoins?includePrices=true")
    pa_ = d.get("peggedAssets", [])
    cols = {k: [] for k in ["stablecoin_id", "name", "symbol", "gecko_id",
                            "peg_type", "peg_mechanism", "price", "chains"]}
    for a in pa_:
        cols["stablecoin_id"].append(str(a.get("id", "")))
        cols["name"].append(a.get("name") or "")
        cols["symbol"].append(a.get("symbol") or "")
        cols["gecko_id"].append(a.get("gecko_id") or "")
        cols["peg_type"].append(a.get("pegType") or "")
        cols["peg_mechanism"].append(a.get("pegMechanism") or "")
        cols["price"].append(float(a["price"]) if isinstance(a.get("price"), (int, float)) else None)
        cols["chains"].append(",".join(a.get("chains") or []))
    tot += write_parquet(os.path.join(OUT, "_catalog_stablecoins.parquet"), cols)
    print(f"  stablecoins: {len(pa_)}", flush=True)

    # yield pools (snapshot)
    d = get("https://yields.llama.fi/pools")
    pools = d.get("data", [])
    cols = {k: [] for k in ["pool_id", "chain", "project", "symbol", "tvl_usd",
                            "apy", "apy_base", "apy_reward", "stablecoin", "il_risk",
                            "exposure", "pool_meta"]}
    for p in pools:
        cols["pool_id"].append(p.get("pool") or "")
        cols["chain"].append(p.get("chain") or "")
        cols["project"].append(p.get("project") or "")
        cols["symbol"].append(p.get("symbol") or "")
        cols["tvl_usd"].append(float(p["tvlUsd"]) if isinstance(p.get("tvlUsd"), (int, float)) else None)
        cols["apy"].append(float(p["apy"]) if isinstance(p.get("apy"), (int, float)) else None)
        cols["apy_base"].append(float(p["apyBase"]) if isinstance(p.get("apyBase"), (int, float)) else None)
        cols["apy_reward"].append(float(p["apyReward"]) if isinstance(p.get("apyReward"), (int, float)) else None)
        cols["stablecoin"].append(str(p.get("stablecoin")))
        cols["il_risk"].append(p.get("ilRisk") or "")
        cols["exposure"].append(p.get("exposure") or "")
        cols["pool_meta"].append(str(p.get("poolMeta") or ""))
    tot += write_parquet(os.path.join(OUT, "_catalog_yield_pools.parquet"), cols)
    print(f"  yield_pools: {len(pools)}", flush=True)
    print(f"PHASE catalog: {tot} catalog rows written", flush=True)
    return tot


# ---------------------------------------------------------------------------
# PHASE: overview (fees/revenue/volumes) -- bulk breakdown, ALL protocols
# ---------------------------------------------------------------------------
OVERVIEW_JOBS = [
    # (adapter_type, dataType or None, out_filename)
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
    # Renamed upstream (the old "dailyVolume" now answers 500, not 400); kept in sync with
    # updater/strategies/fetchers/defillama.OVERVIEW_JOBS.
    ("bridge-aggregators", "dailyBridgeVolume", "bridge_aggregators_dailyVolume"),
]


def phase_overview():
    tot = 0
    for typ, dtype, fname in OVERVIEW_JOBS:
        url = (f"https://api.llama.fi/overview/{typ}"
               f"?dataType={dtype}&excludeTotalDataChart=true"
               f"&excludeTotalDataChartBreakdown=false")
        d = get(url, timeout=180)
        if not isinstance(d, dict) or "__" in str(list(d.keys())[:1]):
            print(f"  {fname}: SKIP ({d})", flush=True)
            continue
        tdcb = d.get("totalDataChartBreakdown") or []
        keys, dates, vals = [], [], []
        for day in tdcb:
            if not isinstance(day, list) or len(day) < 2:
                continue
            od = to_date(day[0])
            if od is None:
                continue
            entry = day[1]
            if not isinstance(entry, dict):
                continue
            for proto, v in entry.items():
                # v may be number or dict-of-chains; sum dict values
                if isinstance(v, dict):
                    vv = sum(x for x in v.values() if isinstance(x, (int, float)))
                elif isinstance(v, (int, float)):
                    vv = float(v)
                else:
                    continue
                keys.append(str(proto)); dates.append(od); vals.append(float(vv))
        n = write_parquet(os.path.join(OUT, f"{fname}.parquet"),
                          {"series_key": keys, "obs_date": dates, "value": vals})
        nser = len(set(keys))
        tot += n
        print(f"  {fname:38} series={nser:>5} obs={n:>9,} days={len(tdcb)}", flush=True)
    print(f"PHASE overview: {tot:,} observations", flush=True)
    return tot


# ---------------------------------------------------------------------------
# PHASE: chains -- per-chain historical TVL, grouped into ONE file
# ---------------------------------------------------------------------------
def phase_chains(limit=None):
    chains = get("https://api.llama.fi/v2/chains")
    names = [c["name"] for c in chains if c.get("name")]
    if limit:
        names = names[:limit]
    keys, dates, vals = [], [], []
    # all-chains aggregate first
    agg = get("https://api.llama.fi/v2/historicalChainTvl")
    if isinstance(agg, list):
        for pt in agg:
            od = to_date(pt.get("date"))
            if od is not None and isinstance(pt.get("tvl"), (int, float)):
                keys.append("__ALL__"); dates.append(od); vals.append(float(pt["tvl"]))

    lock = threading.Lock()
    done = [0]

    def fetch(name):
        import urllib.parse
        d = get("https://api.llama.fi/v2/historicalChainTvl/" + urllib.parse.quote(name))
        out = []
        if isinstance(d, list):
            for pt in d:
                od = to_date(pt.get("date"))
                if od is not None and isinstance(pt.get("tvl"), (int, float)):
                    out.append((name, od, float(pt["tvl"])))
        with lock:
            done[0] += 1
            if done[0] % 100 == 0:
                print(f"    chains {done[0]}/{len(names)}", flush=True)
        return out

    with cf.ThreadPoolExecutor(max_workers=MAXW) as ex:
        for res in ex.map(fetch, names):
            for k, od, v in res:
                keys.append(k); dates.append(od); vals.append(v)

    n = write_parquet(os.path.join(OUT, "chains_tvl.parquet"),
                      {"series_key": keys, "obs_date": dates, "value": vals})
    print(f"PHASE chains: {len(set(keys))} chains, {n:,} obs", flush=True)
    return n


# ---------------------------------------------------------------------------
# PHASE: stablecoins -- per-asset circulating-USD history, grouped
# ---------------------------------------------------------------------------
def phase_stablecoins(limit=None):
    lst = get("https://stablecoins.llama.fi/stablecoins?includePrices=true")
    assets = lst.get("peggedAssets", [])
    ids = [str(a["id"]) for a in assets if a.get("id") is not None]
    if limit:
        ids = ids[:limit]

    # 1) aggregate all-stablecoins circulating history
    agg = get("https://stablecoins.llama.fi/stablecoincharts/all")
    keys, dates, vals = [], [], []
    if isinstance(agg, list):
        for pt in agg:
            od = to_date(pt.get("date"))
            cu = pt.get("totalCirculatingUSD")
            if isinstance(cu, dict):
                cu = sum(x for x in cu.values() if isinstance(x, (int, float)))
            if od is not None and isinstance(cu, (int, float)):
                keys.append("__ALL__"); dates.append(od); vals.append(float(cu))
    write_parquet(os.path.join(OUT, "stablecoins_total.parquet"),
                  {"series_key": keys, "obs_date": dates, "value": vals})

    # 2) per-asset total circulating-USD history (sum across chains per date)
    lock = threading.Lock()
    done = [0]

    def fetch(sid):
        d = get("https://stablecoins.llama.fi/stablecoin/" + sid)
        out = []
        if isinstance(d, dict) and "chainBalances" in d:
            # aggregate circulating across chains by date
            bydate = {}
            for ch, blk in d.get("chainBalances", {}).items():
                for pt in blk.get("tokens", []):
                    od = to_date(pt.get("date"))
                    if od is None:
                        continue
                    cu = pt.get("circulating")
                    if isinstance(cu, dict):
                        cu = sum(x for x in cu.values() if isinstance(x, (int, float)))
                    if isinstance(cu, (int, float)):
                        bydate[od] = bydate.get(od, 0.0) + float(cu)
            for od, v in bydate.items():
                out.append((sid, od, v))
        with lock:
            done[0] += 1
            if done[0] % 50 == 0:
                print(f"    stablecoins {done[0]}/{len(ids)}", flush=True)
        return out

    k2, d2, v2 = [], [], []
    with cf.ThreadPoolExecutor(max_workers=MAXW) as ex:
        for res in ex.map(fetch, ids):
            for sid, od, v in res:
                k2.append(sid); d2.append(od); v2.append(v)
    n = write_parquet(os.path.join(OUT, "stablecoins_circulating.parquet"),
                      {"series_key": k2, "obs_date": d2, "value": v2})
    print(f"PHASE stablecoins: {len(set(k2))} assets, {n:,} obs (+{len(keys)} agg)", flush=True)
    return n + len(keys)


# ---------------------------------------------------------------------------
# PHASE: tvl -- per-protocol TVL history (total + chain-split), sharded grouped
# ---------------------------------------------------------------------------
def phase_tvl(limit=None, shard=None):
    plist = get("https://api.llama.fi/protocols")
    slugs = [p["slug"] for p in plist if p.get("slug")]
    if shard:
        k, nsh = shard
        slugs = [s for i, s in enumerate(slugs) if i % nsh == k]
    if limit:
        slugs = slugs[:limit]

    # Bucket protocols into ~30 shards for grouped output to stay file-bounded.
    NSHARDS = 30
    buckets = {i: {"keys": [], "dates": [], "vals": []} for i in range(NSHARDS)}
    lock = threading.Lock()
    done = [0]
    errs = [0]

    def fetch(slug):
        import urllib.parse
        d = get("https://api.llama.fi/protocol/" + urllib.parse.quote(slug))
        rows = []
        if isinstance(d, dict) and ("tvl" in d or "chainTvls" in d):
            for pt in d.get("tvl", []) or []:
                od = to_date(pt.get("date"))
                v = pt.get("totalLiquidityUSD")
                if od is not None and isinstance(v, (int, float)):
                    rows.append((f"{slug}|__total__", od, float(v)))
            # chain splits: chainTvls[chain].tvl = [{date,totalLiquidityUSD}]
            for ch, blk in (d.get("chainTvls") or {}).items():
                for pt in blk.get("tvl", []) or []:
                    od = to_date(pt.get("date"))
                    v = pt.get("totalLiquidityUSD")
                    if od is not None and isinstance(v, (int, float)):
                        rows.append((f"{slug}|{ch}", od, float(v)))
        else:
            with lock:
                errs[0] += 1
        b = hash(slug) % NSHARDS
        with lock:
            for key, od, v in rows:
                buckets[b]["keys"].append(key)
                buckets[b]["dates"].append(od)
                buckets[b]["vals"].append(v)
            done[0] += 1
            if done[0] % 200 == 0:
                tot = sum(len(buckets[i]["keys"]) for i in range(NSHARDS))
                print(f"    tvl {done[0]}/{len(slugs)} rows={tot:,} errs={errs[0]}", flush=True)

    with cf.ThreadPoolExecutor(max_workers=MAXW) as ex:
        list(ex.map(fetch, slugs))

    tot = 0
    for i in range(NSHARDS):
        b = buckets[i]
        if not b["keys"]:
            continue
        n = write_parquet(os.path.join(OUT, f"tvl_protocol_shard{i:02d}.parquet"),
                          {"series_key": b["keys"], "obs_date": b["dates"], "value": b["vals"]})
        tot += n
    nseries = sum(len(set(buckets[i]["keys"])) for i in range(NSHARDS))
    print(f"PHASE tvl: {len(slugs)} protocols, {nseries} series, {tot:,} obs, errs={errs[0]}", flush=True)
    return tot


# ---------------------------------------------------------------------------
# PHASE: yields -- per-pool APY/TVL history, sharded grouped
# ---------------------------------------------------------------------------
def phase_yields(limit=None, shard=None):
    """Rate-limited per-pool history. Durable + resumable.

    yields.llama.fi/chart is heavily rate-limited (~0.5 success/s even single-
    threaded). We stage each completed pool as one JSONL line, so a restart skips
    pools already staged. Final sharded Parquet is (re)built from the stage file.
    Use phase_yields_build() to (re)materialize Parquet from the stage at any time.
    """
    import json as _json
    d = get("https://yields.llama.fi/pools", timeout=180)
    pools = [p["pool"] for p in d.get("data", []) if p.get("pool")]
    if shard:
        k, nsh = shard
        pools = [p for i, p in enumerate(pools) if i % nsh == k]
    if limit:
        pools = pools[:limit]

    stage = os.path.join(OUT, "_yields_stage.jsonl")
    done_pools = set()
    if os.path.exists(stage):
        with open(stage, encoding="utf-8") as f:
            for line in f:
                pid = line.split("\t", 1)[0]
                if pid:
                    done_pools.add(pid)
    todo = [p for p in pools if p not in done_pools]
    print(f"yields: total={len(pools)} already_staged={len(done_pools)} todo={len(todo)}",
          flush=True)

    YW = int(os.environ.get("DL_YIELDS_WORKERS", "1"))
    lock = threading.Lock()
    fh = open(stage, "a", encoding="utf-8")
    done = [0]; errs = [0]; obs = [0]

    def fetch(pid):
        r = get("https://yields.llama.fi/chart/" + pid, timeout=30, tries=10)
        pts = []
        ok = isinstance(r, dict) and r.get("status") == "success"
        if ok:
            for pt in r.get("data", []) or []:
                ts = pt.get("timestamp")
                pts.append([ts, pt.get("tvlUsd"), pt.get("apy"),
                            pt.get("apyBase"), pt.get("apyReward")])
        line = pid + "\t" + _json.dumps(pts) + "\n"
        with lock:
            fh.write(line)
            done[0] += 1
            obs[0] += len(pts)
            if not ok:
                errs[0] += 1
            if done[0] % 500 == 0:
                fh.flush()
                print(f"    yields {done[0]}/{len(todo)} obs={obs[0]:,} errs={errs[0]}",
                      flush=True)

    if YW <= 1:
        for p in todo:
            fetch(p)
    else:
        with cf.ThreadPoolExecutor(max_workers=YW) as ex:
            list(ex.map(fetch, todo))
    fh.flush(); fh.close()
    print(f"yields fetch done: staged {done[0]} new pools, {obs[0]:,} new obs, errs={errs[0]}",
          flush=True)
    return phase_yields_build()


def phase_yields_build():
    """(Re)build sharded Parquet from the JSONL stage file. Idempotent."""
    import json as _json
    stage = os.path.join(OUT, "_yields_stage.jsonl")
    if not os.path.exists(stage):
        print("yields build: no stage file", flush=True)
        return 0
    NSHARDS = 60
    buckets = {i: {"k": [], "d": [], "tvl": [], "apy": [], "apyb": [], "apyr": []}
               for i in range(NSHARDS)}
    npools = 0
    with open(stage, encoding="utf-8") as f:
        for line in f:
            if "\t" not in line:
                continue
            pid, js = line.split("\t", 1)
            try:
                pts = _json.loads(js)
            except Exception:  # noqa: BLE001
                continue
            npools += 1
            b = hash(pid) % NSHARDS
            bk = buckets[b]
            for ts, tvl, apy, apyb, apyr in pts:
                od = to_date(ts)
                if od is None:
                    continue
                bk["k"].append(pid); bk["d"].append(od)
                bk["tvl"].append(float(tvl) if isinstance(tvl, (int, float)) else None)
                bk["apy"].append(float(apy) if isinstance(apy, (int, float)) else None)
                bk["apyb"].append(float(apyb) if isinstance(apyb, (int, float)) else None)
                bk["apyr"].append(float(apyr) if isinstance(apyr, (int, float)) else None)
    tot = 0
    for i in range(NSHARDS):
        b = buckets[i]
        if not b["k"]:
            continue
        n = write_parquet(os.path.join(OUT, f"yields_pool_shard{i:02d}.parquet"),
                          {"series_key": b["k"], "obs_date": b["d"],
                           "tvl_usd": b["tvl"], "apy": b["apy"],
                           "apy_base": b["apyb"], "apy_reward": b["apyr"]})
        tot += n
    nseries = sum(len(set(buckets[i]["k"])) for i in range(NSHARDS))
    print(f"PHASE yields: {npools} pools, {nseries} series, {tot:,} obs", flush=True)
    return tot


PHASES = {
    "catalog": phase_catalog,
    "overview": phase_overview,
    "chains": phase_chains,
    "stablecoins": phase_stablecoins,
    "tvl": phase_tvl,
    "yields": phase_yields,
    "yields_build": phase_yields_build,
}


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in PHASES:
        print("phases:", " ".join(PHASES)); return
    phase = sys.argv[1]
    kw = {}
    if "--limit" in sys.argv:
        kw["limit"] = int(sys.argv[sys.argv.index("--limit") + 1])
    if "--shard" in sys.argv:
        sp = sys.argv[sys.argv.index("--shard") + 1]
        k, n = sp.split("/"); kw["shard"] = (int(k), int(n))
    t0 = time.time()
    fn = PHASES[phase]
    # filter kwargs the fn accepts
    import inspect
    ok = set(inspect.signature(fn).parameters)
    kw = {k: v for k, v in kw.items() if k in ok}
    fn(**kw)
    print(f"[{phase}] done in {time.time()-t0:.0f}s", flush=True)


if __name__ == "__main__":
    main()
