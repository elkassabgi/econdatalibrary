"""S2 fetcher — Sveriges Riksbank SWEA (SEK FX rates, policy/repo rate, bond yields, ~117 series).

Riksbank open data, no key. Layout: one parquet clean_full/riksbank/riksbank.parquet holding every
series, schema (series_key, obs_date, value), series_key = "RIKSBANK:<seriesId>".

Date-tail with a freshness pre-filter: GET /swea/v1/Series (one request) returns every series' current
observationMaxDate; we compare it to each series' stored max obs_date and only pull Observations for
series that actually advanced — so a normal daily tick makes ~1 + (series-that-moved) requests, not 117.
Each pulled window starts a revision-lookback behind the stored frontier (same-day revisions captured);
merge.merge_and_write dedups the overlap and never shrinks. Store I/O goes through blob (CI-safe, R36).

HONEST-STATUS: the catalog request failing after retries -> TransientError (whole run partial, retried;
nothing silently reported no_change). A per-series 429/5xx/timeout after retries -> that series is a
transient_unit (kept, retried next tick), the rest of the run proceeds. A 4xx on one series -> empty.
"""
from __future__ import annotations
import datetime as dt
import os
import re
import time

import pyarrow as pa
import pyarrow.compute as pc
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Tally, _max_by_key, finalize, revision_since

SOURCE = "riksbank"
BASE = "https://api.riksbank.se/swea/v1"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com", "Accept": "application/json"}
PARQUET = "riksbank.parquet"
DEDUP = ("series_key", "obs_date")
RATE = 3.0          # Riksbank rate-limits aggressively; keep the ingester's proven spacing
EARLIEST = "1990-01-01"


def _get_json(sess, url, tries=6):
    """GET → parsed JSON. 4xx(400/404) → None (no data). Exhausted retries → TransientError."""
    for a in range(tries):
        try:
            r = sess.get(url, headers=UA, timeout=60)
        except (requests.Timeout, requests.ConnectionError) as e:
            if a == tries - 1:
                raise TransientError(f"riksbank: {e}")
            time.sleep(min(5 * (a + 1), 30)); continue
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError:
                if a == tries - 1:
                    raise TransientError("riksbank: 200 body not JSON")
                time.sleep(min(5 * (a + 1), 30)); continue
        if r.status_code in (400, 404):
            return None
        if r.status_code == 429:
            wait = 65
            try:
                m = re.search(r"(\d+)\s+second", (r.json() or {}).get("message", ""))
                if m:
                    wait = int(m.group(1)) + 5
            except Exception:
                pass
            if a == tries - 1:
                raise TransientError("riksbank: 429 retry budget exhausted")
            time.sleep(min(wait, 90)); continue
        if r.status_code in (500, 502, 503, 504):
            if a == tries - 1:
                raise TransientError(f"riksbank HTTP {r.status_code}")
            time.sleep(min(5 * (a + 1), 30)); continue
        # other 4xx: treat as no data for this url
        return None
    raise TransientError("riksbank: retry budget exhausted")


def _pdate(s):
    if not s:
        return None
    try:
        return dt.date.fromisoformat(str(s)[:10])
    except (ValueError, TypeError):
        return None


def _stored_max(path) -> dict[str, dt.date]:
    """Per-series max obs_date from the single parquet (empty dict if absent)."""
    if not blob.exists(path):
        return {}
    t = blob.read_table(path, columns=["series_key", "obs_date"])
    if t.num_rows == 0:
        return {}
        # _max_by_key, NOT group_by. Arrow indexes string data with int32 offsets; past 2 GiB in one
    # column group_by dereferences past the overflowed offsets and KILLS THE PROCESS
    # (0xC0000005 / SIGABRT) - it does not raise, so no try/except catches it. ons_uk died that
    # way on 2026-08-01 after 8h56m. merge.py documented it; the fetchers never got the memo.
    # _max_by_key returns ISO STRINGS, so the previous `isinstance(v, dt.date)` filter could
    # never be true and this returned an EMPTY map on EVERY run — silently. No crash, no log
    # line; just no frontier, so every series re-fetched from EARLIEST and no cursors reached
    # the §5.7 coherence check, which demotes the run to `partial` — and a partial never sets
    # last_success_utc (R231). That is why riksbank has no recorded success.
    #
    # PARSE BACK TO dt.date, do not pass the strings through: update() compares
    # `cat_max <= smax` against a dt.date from _pdate, and hands smax to revision_since().
    # Returning strings would swap a silent empty for a TypeError — the annotation above is
    # the contract this function owes its caller, and it is dates.
    agg_map = _max_by_key(t)
    out: dict[str, dt.date] = {}
    for k, v in agg_map.items():
        d = _pdate(v)
        if k and d is not None:
            out[k] = d
    return out


def _fetch_obs(sess, sid, frm, to):
    """Observations for one series → list[(date, float)]. Rate-limited; transient errors propagate."""
    data = _get_json(sess, f"{BASE}/Observations/{sid}/{frm}/{to}")
    time.sleep(RATE)
    if not data:
        return []
    if isinstance(data, dict):
        obs = data.get("observations", data.get("value", []))
    elif isinstance(data, list):
        obs = data
    else:
        return []
    out = []
    for o in obs:
        d = _pdate(o.get("date") or o.get("Date") or "")
        raw = o.get("value")
        if raw is None:
            raw = o.get("Value", o.get("average"))
        if d is None or raw is None:
            continue
        try:
            v = float(str(raw).replace(",", "."))
        except (ValueError, TypeError):
            continue
        if v != v:  # NaN
            continue
        out.append((d, v))
    return out


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    path = os.path.join(out_dir, PARQUET)
    sess = requests.Session()

    stored = _stored_max(path)

    catalog = _get_json(sess, f"{BASE}/Series")   # 1 request; TransientError if it fails
    if not isinstance(catalog, list) or not catalog:
        raise TransientError("riksbank: empty/invalid series catalog")

    tally = Tally()
    cursors: dict[str, str] = {}
    keys, dates, vals = [], [], []
    maxd: dt.date | None = None

    for s in catalog:
        sid = s.get("seriesId")
        if not sid:
            continue
        sk = f"RIKSBANK:{sid}"
        smax = stored.get(sk)
        cat_max = _pdate(s.get("observationMaxDate"))
        # freshness pre-filter: catalog says no new data past our frontier -> skip the request
        if smax is not None and cat_max is not None and cat_max <= smax:
            tally.empty_unit()
            continue
        frm = revision_since(smax, unit).isoformat() if smax else (s.get("observationMinDate") or EARLIEST)
        to = s.get("observationMaxDate") or dt.date.today().isoformat()
        try:
            rows = _fetch_obs(sess, sid, frm, to)
        except TransientError:
            tally.transient_unit()
            continue
        if not rows:
            tally.empty_unit()
            continue
        smd = None
        for d, v in rows:
            keys.append(sk); dates.append(d); vals.append(v)
            if smd is None or d > smd:
                smd = d
        tally.added_unit(len(rows))
        if smd:
            cursors[sk] = smd.isoformat()
            if maxd is None or smd > maxd:
                maxd = smd

    if keys:
        tbl = pa.table({
            "series_key": pa.array(keys, pa.string()),
            "obs_date": pa.array(dates, pa.date32()),
            "value": pa.array(vals, pa.float64()),
        })
        n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
        total = n
    else:
        total = blob.row_count(path) if blob.exists(path) else 0

    last_obs = maxd.isoformat() if maxd else (since or None)
    return finalize(tally, total, last_obs, source=SOURCE, series_cursors=cursors,
                    empty_window_floor=len(catalog) + 1)
