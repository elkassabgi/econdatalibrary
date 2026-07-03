"""S2 fetcher — Frankfurter (ECB euro reference rates). No key, CC-style reuse.

Layout: single parquet clean_full/frankfurter/frankfurter_fx_eur.parquet,
schema (series_key, obs_date, value); series_key = "EUR<CCY>" (e.g. EURUSD) —
identical to the legacy jobs/ingest_frankfurter.py so the merge EXTENDS the
existing 263k-row history instead of forking keys.

Date-tail: one range call /{start}..{end} returns EVERY currency for the
window, so the unit-level max obs_date is the natural cursor. The stored
boundary day is re-fetched (captures same-day revisions; dedup keep-last).
First run (empty store) backfills from 1999-01-04 in year chunks.

HONEST-STATUS: timeouts/5xx/429 -> TransientError (run partial, retried next
tick). A 200 whose JSON lacks the documented {rates: {...}} envelope on a
non-trivial body -> structural (DefinitiveError via finalize). A quiet window
(no new dates) -> earned no_change.
"""
from __future__ import annotations
import datetime as dt
import os
import time

import pyarrow as pa
import requests

from ... import config, blob, merge
from ...errors import TransientError
from ..base import Result
from ._common import Tally, finalize

SOURCE = "frankfurter"
FILE = "frankfurter_fx_eur.parquet"
API = "https://api.frankfurter.app"
BASE = "EUR"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
DEDUP = ("series_key", "obs_date")
EARLIEST = dt.date(1999, 1, 4)          # ECB reference rates begin here
CHUNK_DAYS = 366                        # year-sized range calls


def _get_json(session, url, tries=5):
    for a in range(tries):
        try:
            r = session.get(url, headers=UA, timeout=180)
        except (requests.Timeout, requests.ConnectionError) as e:
            if a == tries - 1:
                raise TransientError(f"frankfurter: {e}")
            time.sleep(min(2 ** a, 30)); continue
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError:
                if a == tries - 1:
                    raise TransientError("frankfurter: 200 body not JSON")
                time.sleep(min(2 ** a, 30)); continue
        if r.status_code in (429, 500, 502, 503, 504):
            if a == tries - 1:
                raise TransientError(f"frankfurter HTTP {r.status_code}")
            time.sleep(min(2 ** a, 30)); continue
        raise TransientError(f"frankfurter HTTP {r.status_code}")
    raise TransientError("frankfurter: retry budget exhausted")


def _unit_last(path) -> dt.date | None:
    if not blob.exists(path):
        return None
    t = blob.read_table(path)
    if t.num_rows == 0 or "obs_date" not in t.column_names:
        return None
    import pyarrow.compute as pc
    d = pc.max(t.column("obs_date")).as_py()
    return d if isinstance(d, dt.date) else None


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, FILE)

    last = _unit_last(path)
    if last is not None:
        start = last                      # boundary re-fetch (same-day revisions)
    else:
        try:
            start = dt.date.fromisoformat(str(since)[:10]) if since else EARLIEST
        except ValueError:
            start = EARLIEST
    end = dt.date.today()

    tally = Tally()
    keys, dates, vals = [], [], []
    session = requests.Session()
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + dt.timedelta(days=CHUNK_DAYS - 1), end)
        url = f"{API}/{cursor.isoformat()}..{chunk_end.isoformat()}"
        try:
            payload = _get_json(session, url)
        except TransientError:
            tally.transient_unit()
            cursor = chunk_end + dt.timedelta(days=1)
            time.sleep(0.5)
            continue
        rates = (payload or {}).get("rates")
        if not isinstance(rates, dict):
            # 200 JSON without the documented envelope on a real body = schema break
            tally.structural_unit()
            cursor = chunk_end + dt.timedelta(days=1)
            continue
        added = 0
        for day, per_ccy in rates.items():
            try:
                d = dt.date.fromisoformat(day[:10])
            except (ValueError, TypeError):
                continue
            if not isinstance(per_ccy, dict):
                continue
            for ccy, val in per_ccy.items():
                try:
                    v = float(val)
                except (TypeError, ValueError):
                    continue
                keys.append(f"{BASE}{ccy}")
                dates.append(d)
                vals.append(v)
                added += 1
        genuinely_new = (last is None and added > 0) or any(
            d > last for d in dates[-added:] if added) if added else False
        if genuinely_new:
            tally.added_unit(added)
        else:
            tally.empty_unit()
        cursor = chunk_end + dt.timedelta(days=1)
        time.sleep(0.3)

    last_db = last.isoformat() if last else None
    if not keys:
        return finalize(tally, blob.row_count(path), last_db, source=SOURCE)

    new_table = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date": pa.array(dates, pa.date32()),
        "value": pa.array(vals, pa.float64()),
    })
    n, maxd = merge.merge_and_write(path, new_table, mode="merge", dedup_keys=DEDUP)

    # per-series cursors: every currency shares the unit window; report each
    # series' true max from this fetch so a delisted currency can't hide.
    cur: dict[str, str] = {}
    for k, d in zip(keys, dates):
        iso = d.isoformat()
        if k not in cur or iso > cur[k]:
            cur[k] = iso
    return finalize(tally, n, maxd or last_db, source=SOURCE, series_cursors=cur)
