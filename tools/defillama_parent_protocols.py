"""Fetch the DefiLlama PARENT protocols our per-protocol crawl structurally cannot reach.

The crawl iterates the /protocols listing, which contains CHILD protocols
(aave-v1, aave-v3, uniswap-v2, ...). Six catalogued series name PARENT entities
(aave, makerdao, uniswap, compound-finance, pancakeswap, eigenlayer) that are
served only at /protocol/<slug> and never appear in that listing -- so no amount
of crawling would ever produce them, and their catalog rows resolved to nothing.
lido and curve-dex are both parents AND listed children, which is exactly why
those two alone had data.

Because the crawl can never emit these slugs, a dedicated file cannot collide
with a future ingester run. Rows are built with the ingester's own construction
(jobs/ingest_defillama.py:413-426): '<slug>|__total__' from tvl[], plus
'<slug>|<chain>' from chainTvls, so the shape is identical to the 30 shards.
"""
import datetime as dt
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# IMPORTED, NOT COPIED — this file's docstring above says its rows are built "with the ingester's
# own construction", and copying that construction is exactly how it missed the fix. R773's
# duplicate-date defect was fixed in jobs/ingest_defillama.py, and this second producer kept
# writing 147 duplicate pairs (141 of them contradictory, all at its own max date 2026-07-27)
# into six of the seven corrupted served CSVs: aave 52, uniswap 49, compound-finance 20,
# pancakeswap 18, eigenlayer 4, makerdao 4. A rule that lives in two places is a rule that gets
# fixed in one.
from jobs.ingest_defillama import _dedup_first                # noqa: E402

OUT = r"E:\research\econfindatalibrary\data\clean_full\defillama\tvl_protocol_shard_parents.parquet"
SLUGS = ["aave", "makerdao", "uniswap", "compound-finance", "pancakeswap", "eigenlayer"]


def to_date(ts):
    if ts is None:
        return None
    try:
        ts = float(ts)
    except (TypeError, ValueError):
        return None
    if ts > 1e11:          # milliseconds
        ts /= 1000.0
    try:
        return dt.datetime.fromtimestamp(ts, dt.timezone.utc).date()
    except (OverflowError, OSError, ValueError):
        return None


def main():
    sess = requests.Session()
    sess.headers["User-Agent"] = "econdatalibrary/1.0"
    keys, dates, vals = [], [], []
    for slug in SLUGS:
        r = sess.get(f"https://api.llama.fi/protocol/{slug}", timeout=120)
        r.raise_for_status()
        d = r.json()
        n0 = len(keys)
        for pt in d.get("tvl", []) or []:
            od, v = to_date(pt.get("date")), pt.get("totalLiquidityUSD")
            if od is not None and isinstance(v, (int, float)):
                keys.append(f"{slug}|__total__"); dates.append(od); vals.append(float(v))
        tot = len(keys) - n0
        for ch, blk in (d.get("chainTvls") or {}).items():
            for pt in (blk.get("tvl", []) or []):
                od, v = to_date(pt.get("date")), pt.get("totalLiquidityUSD")
                if od is not None and isinstance(v, (int, float)):
                    keys.append(f"{slug}|{ch}"); dates.append(od); vals.append(float(v))
        print(f"  {slug:20} total_rows={tot:<6} +chain_rows={len(keys)-n0-tot}", flush=True)

    if not keys:
        print("NO ROWS FETCHED -- refusing to write an empty file", file=sys.stderr)
        return 1
    cols, dropped = _dedup_first({"series_key": keys, "obs_date": dates, "value": vals})
    if dropped:
        print(f"  dedup: dropped {dropped:,} row(s) repeating a (series_key, obs_date) pair — "
              f"the intraday 'now' point (R773)", flush=True)
    keys, dates, vals = cols["series_key"], cols["obs_date"], cols["value"]

    tbl = pa.table({"series_key": pa.array(keys, pa.string()),
                    "obs_date": pa.array(dates, pa.date32()),
                    "value": pa.array(vals, pa.float64())})
    tmp = OUT + ".tmp"
    pq.write_table(tbl, tmp, compression="zstd")
    os.replace(tmp, OUT)
    print(f"wrote {tbl.num_rows:,} rows -> {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
