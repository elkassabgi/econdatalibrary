"""S2 fetcher — UNHCR refugee population statistics (refugees, asylum seekers, IDPs, stateless…).

CC BY 4.0, no key. Single parquet clean_full/unhcr/unhcr.parquet, schema (series_key, obs_date, value),
series_key = "{endpoint}:{field}:{coo_iso}:{coa_iso}" (e.g. population:refugees:AFG:PAK); obs_date is the
Dec-31 annual stamp. Three endpoints: population, idmc, solutions; nine numeric fields per bilateral row.

Date-tail: UNHCR publishes annually and revises recent years, so instead of re-pulling 1951-present we
read the stored max year and refetch only [max_year - LOOKBACK .. this_year] for the three endpoints
(a not-yet-published trailing year just returns empty). merge.merge_and_write dedups the overlap and
never shrinks. Store I/O via blob (CI-safe, R36).

HONEST-STATUS: a page failing after retries (timeout/5xx/429) -> that endpoint+year is a transient_unit
(kept, retried next tick); a real-but-empty year -> empty_unit; parsed rows -> added_unit. Nothing is
ever silently reported no_change on a transport failure.
"""
from __future__ import annotations
import datetime as dt
import os
import time

import pyarrow as pa
import pyarrow.compute as pc
import requests

from ... import config, blob, merge
from ...errors import TransientError
from ..base import Result
from ._common import (Deadline, Tally, finalize, load_rotation, rotate_after,
                      save_rotation)

SOURCE = "unhcr"
BASE = "https://api.unhcr.org/population/v1"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com", "Accept": "application/json"}
PARQUET = "unhcr.parquet"
DEDUP = ("series_key", "obs_date")
ENDPOINTS = ("population", "idmc", "solutions")
NUMERIC_FIELDS = ["refugees", "asylum_seekers", "returned_refugees",
                  "idps", "returned_idps", "stateless", "ooc", "oip", "hst"]
PAGE = 100
RATE = 0.3
LOOKBACK_YEARS = 2
FIRST_YEAR = 1951
DEFAULT_WINDOW_YEARS = 4


def _get_json(sess, url, tries=5):
    """200 → json; 400/404 → None (empty/gone); exhausted retries on a retryable fault → TransientError."""
    for a in range(tries):
        try:
            r = sess.get(url, headers=UA, timeout=60)
        except (requests.Timeout, requests.ConnectionError) as e:
            if a == tries - 1:
                raise TransientError(f"unhcr: {e}")
            time.sleep(min(5 * (a + 1), 30)); continue
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError:
                if a == tries - 1:
                    raise TransientError("unhcr: 200 body not JSON")
                time.sleep(min(5 * (a + 1), 30)); continue
        if r.status_code in (400, 404):
            return None
        if r.status_code in (429, 500, 502, 503, 504):
            if a == tries - 1:
                raise TransientError(f"unhcr HTTP {r.status_code}")
            time.sleep(30 if r.status_code == 429 else min(5 * (a + 1), 30)); continue
        return None
    raise TransientError("unhcr: retry budget exhausted")


def _stored_max_year(path) -> int | None:
    if not blob.exists(path):
        return None
    t = blob.read_table(path, columns=["obs_date"])
    if t.num_rows == 0:
        return None
    md = pc.max(t.column("obs_date")).as_py()
    if not isinstance(md, dt.date):
        return None
    # guard a corrupt far-future stamp so the window doesn't shoot past today
    return min(md.year, dt.date.today().year)


def _fetch_endpoint_year(sess, endpoint, year, keys, dates, vals) -> int:
    """Paginate one endpoint+year; append rows; return count. Transient errors propagate."""
    added = 0
    page = 1
    while True:
        url = (f"{BASE}/{endpoint}/?limit={PAGE}&page={page}"
               f"&coo_all=true&coa_all=true&year={year}")
        data = _get_json(sess, url)
        if not data or not data.get("items"):
            break
        for it in data["items"]:
            try:
                yr = int(it.get("year", year))
            except (TypeError, ValueError):
                yr = year
            coo = (it.get("coo_iso") or it.get("coo") or "WLD")[:10]
            coa = (it.get("coa_iso") or it.get("coa") or "WLD")[:10]
            d = dt.date(yr, 12, 31)
            for field in NUMERIC_FIELDS:
                raw = it.get(field)
                if raw is None or raw in ("", "-", "0", 0):
                    continue
                try:
                    v = float(str(raw).replace(",", ""))
                except (ValueError, TypeError):
                    continue
                if v == 0:
                    continue
                keys.append(f"{endpoint}:{field}:{coo}:{coa}")
                dates.append(d)
                vals.append(v)
                added += 1
        try:
            max_pages = int(data.get("maxPages", page))
        except (TypeError, ValueError):
            max_pages = page
        if page >= max_pages:
            break
        page += 1
        time.sleep(RATE)
    return added


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, PARQUET)
    sess = requests.Session()

    this_year = dt.date.today().year
    smy = _stored_max_year(path)
    if smy is not None:
        start_year = smy - LOOKBACK_YEARS
    elif since:
        try:
            start_year = dt.date.fromisoformat(str(since)[:10]).year - LOOKBACK_YEARS
        except Exception:
            start_year = this_year - DEFAULT_WINDOW_YEARS
    else:
        start_year = this_year - DEFAULT_WINDOW_YEARS
    start_year = max(FIRST_YEAR, start_year)
    years = list(range(start_year, this_year + 1))

    tally = Tally()
    keys, dates, vals = [], [], []

    # BOUND BELOW THE ORCHESTRATOR'S 45-MINUTE CAP, AND ROTATE.
    # unhcr's measured cloud runs are 62.5 and 72.3 minutes — over the cap every time. The
    # cap landed 2026-08-01 (36130d02), after unhcr's last run, and there is exactly ONE
    # merge_and_write here, after both loops. So the next run gets killed mid-sweep and
    # stores NOTHING: a discard, not a truncation (R243).
    #
    # The sweep is ENDPOINTS x years, both fixed orders, so a bound alone would re-walk the
    # same head forever and never reach the later endpoints (R190). Flattening the nested
    # loop into one ordered task list makes the rotation cover the whole grid rather than
    # just the inner axis — rotating years within endpoint 1 would still starve endpoint N.
    budget_min = float(os.environ.get("UNHCR_BUDGET_MIN", "30"))
    dl = Deadline(minutes=budget_min)
    grid = [(e, y) for e in ENDPOINTS for y in years]
    order = rotate_after(grid, load_rotation(out_dir), key=lambda t: f"{t[0]}:{t[1]}")
    stopped_early = False
    last_task = ""
    done_tasks = 0

    for endpoint, yr in order:
        if dl.spent():
            stopped_early = True
            print(f"[{SOURCE}] budget of {budget_min:.0f} min spent after "
                  f"{dl.elapsed_min():.1f} min — {done_tasks}/{len(order)} endpoint-years "
                  f"done, {len(order) - done_tasks} deferred to the next tick "
                  f"(resuming after {last_task!r})", flush=True)
            break
        done_tasks += 1
        last_task = f"{endpoint}:{yr}"
        try:
            n = _fetch_endpoint_year(sess, endpoint, yr, keys, dates, vals)
        except TransientError:
            tally.transient_unit()
            time.sleep(RATE)
            continue
        if n > 0:
            tally.added_unit(n)
        else:
            tally.empty_unit()
        time.sleep(RATE)

    if last_task:
        save_rotation(out_dir, last_task)

    if keys:
        tbl = pa.table({
            "series_key": pa.array(keys, pa.string()),
            "obs_date": pa.array(dates, pa.date32()),
            "value": pa.array(vals, pa.float64()),
        })
        n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
        total = n
        cursors = _series_maxes(tbl)
        last_obs = md
    else:
        total = blob.row_count(path) if blob.exists(path) else 0
        cursors = {}
        last_obs = since or None

    # The floor must be measured against the endpoint-years this tick ATTEMPTED; against the
    # whole grid a bounded pass would read as a wholesale outage every time.
    floor = (done_tasks + 1) if stopped_early else (len(ENDPOINTS) * max(1, len(years)) + 1)
    return finalize(tally, total, last_obs, source=SOURCE, series_cursors=cursors,
                    empty_window_floor=max(floor, 1))


def _series_maxes(tbl):
    out = {}
    for k, d in zip(tbl.column("series_key").to_pylist(), tbl.column("obs_date").to_pylist()):
        if d is None:
            continue
        if k not in out or d > out[k]:
            out[k] = d
    return {k: v.isoformat() for k, v in out.items()}
