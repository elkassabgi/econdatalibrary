"""S2 fetcher — TCMB (Central Bank of Turkey) daily FX rates. Public, no key.

Layout: single parquet under clean_full/tcmb/tcmb.parquet, schema
(series_key, obs_date, value). series_key is {CURRENCYCODE}_{fb|fs|nb|ns}
(forex/banknote buying/selling); value is rate / Unit.

Incremental: read the existing parquet's max obs_date, enumerate weekdays from
the day after it (or from `since`) through today, and GET the per-date XML
tcmb.gov.tr/kurlar/{YYYYMM}/{DDMMYYYY}.xml. Non-trading days return 404 and are
skipped. New rows are MERGEd (dedup on series_key+obs_date, new wins on revision,
never-shrink) so existing history is preserved.

HONEST STATUS (per the contract — see fetchers/_common.py):
Each fetched weekday is a sub-unit recorded on a Tally:
  - 404                                  -> empty_unit()   (non-trading day / holiday)
  - timeout / 5xx / 429 / network        -> transient_unit() (keep going -> status='partial')
  - 200 with well-formed XML but 0 parsed currency rows, OR XML whose expected
    <Currency> structure is gone (schema drift)  -> structural_unit() -> DefinitiveError
  - 200 with rows                        -> added_unit(n)
A multi-week window where EVERY weekday returns 404/empty is itself treated as a
structural break by finalize()'s empty-window floor (the per-date URL scheme
likely moved / host reorg), not laundered into no_change.
"""
from __future__ import annotations
import datetime as dt
import os
import time
import xml.etree.ElementTree as ET

import pyarrow as pa
import pyarrow.compute as pc
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Tally, _max_by_key, finalize

UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
BASE = "https://tcmb.gov.tr/kurlar"
DEDUP = ("series_key", "obs_date")
RATE = 0.05                      # ~20 req/s self-throttle (no documented hard limit)
START_FLOOR = dt.date(1996, 1, 2)  # first available; used only if parquet is missing

RATE_FIELDS = ["ForexBuying", "ForexSelling", "BanknoteBuying", "BanknoteSelling"]
SUFFIX_MAP = {"ForexBuying": "fb", "ForexSelling": "fs",
              "BanknoteBuying": "nb", "BanknoteSelling": "ns"}

# Sentinels distinguishing the three non-row outcomes of a single date fetch.
EMPTY = object()       # 404 — legitimately non-trading day / holiday
STRUCTURAL = object()  # 200 with a non-trivial XML body that parsed 0 currency rows


def _fetch_day(sess: requests.Session, d: dt.date, tries: int = 5):
    """GET + parse one date's XML.

    Returns:
      - dict {series_key: value}  on a 200 trading day with currency rows
      - EMPTY                     on a 404 (non-trading day / holiday)
      - STRUCTURAL                on a 200 whose body parsed to 0 currency rows
                                  from a non-empty body, or whose expected
                                  <Currency>/CurrencyCode structure is gone
    Raises TransientError on timeout/5xx/429/network; DefinitiveError on hard 4xx.
    """
    url = f"{BASE}/{d:%Y%m}/{d:%d%m%Y}.xml"
    for a in range(tries):
        try:
            r = sess.get(url, headers=UA, timeout=30)
        except (requests.Timeout, requests.ConnectionError) as e:
            if a == tries - 1:
                raise TransientError(f"TCMB {d.isoformat()}: {e}")
            time.sleep(min(2 ** a, 30)); continue
        if r.status_code == 404:
            return EMPTY  # non-trading day (weekend handled by caller; holidays land here)
        if r.status_code in (429, 500, 502, 503, 504):
            if a == tries - 1:
                raise TransientError(f"TCMB {d.isoformat()} HTTP {r.status_code}")
            time.sleep(min(2 ** a, 30)); continue
        if r.status_code != 200:
            raise DefinitiveError(f"TCMB {d.isoformat()} HTTP {r.status_code}")
        body = r.content or b""
        # A 200 with a genuinely empty/whitespace body is not a structural break;
        # treat it like a non-trading day (no content served for this date).
        if not body.strip():
            return EMPTY
        try:
            root = ET.fromstring(body)
        except ET.ParseError as e:
            # malformed body for a 200 is structural -> definitive
            raise DefinitiveError(f"TCMB {d.isoformat()} XML parse error: {e}")
        out: dict[str, float] = {}
        currencies = root.findall("Currency")
        for cur in currencies:
            code = cur.get("CurrencyCode", cur.get("Kod", ""))
            if not code:
                continue
            unit = 1.0
            unit_el = cur.find("Unit")
            if unit_el is not None and unit_el.text:
                try:
                    unit = float(unit_el.text)
                except ValueError:
                    unit = 1.0
            if unit == 0:
                unit = 1.0
            for field in RATE_FIELDS:
                el = cur.find(field)
                if el is not None and el.text and el.text.strip():
                    try:
                        val = float(el.text.replace(",", ".")) / unit
                    except (ValueError, TypeError):
                        continue
                    out[f"{code}_{SUFFIX_MAP[field]}"] = val
        if out:
            return out
        # 200 with a non-trivial body but 0 usable currency rows: either the
        # <Currency> elements are gone (renamed root/children) or every element
        # lost its CurrencyCode/Kod attribute. Both are schema/structural breaks,
        # NOT a quiet trading day. Surface as structural.
        return STRUCTURAL
    return EMPTY


def _existing_max_date(path: str):
    if not blob.exists(path):
        return None
    t = blob.read_table(path)
    if t.num_rows == 0 or "obs_date" not in t.column_names:
        return None
    m = pc.max(t.column("obs_date")).as_py()
    return m if isinstance(m, dt.date) else None


def _per_series_cursors(path: str) -> dict[str, str]:
    """Map series_key -> max obs_date (ISO) from the existing parquet (empty if none)."""
    if not blob.exists(path):
        return {}
    t = blob.read_table(path)
    if t.num_rows == 0 or "series_key" not in t.column_names:
        return {}
        # _max_by_key, NOT group_by. Arrow indexes string data with int32 offsets; past 2 GiB in one
    # column group_by dereferences past the overflowed offsets and KILLS THE PROCESS
    # (0xC0000005 / SIGABRT) - it does not raise, so no try/except catches it. ons_uk died that
    # way on 2026-08-01 after 8h56m. merge.py documented it; the fetchers never got the memo.
    grp_map = _max_by_key(t)
    out: dict[str, str] = {}
    for k, d in zip(list(grp_map.keys()),
                    list(grp_map.values())):
        if isinstance(d, dt.datetime):
            d = d.date()
        if d is not None:
            out[k] = d.isoformat()
    return out


def update(unit, since) -> Result:
    out_dir = config.source_dir("tcmb")
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "tcmb.parquet")
    before = blob.row_count(path)

    # Determine the start of the gap to fetch.
    start: dt.date | None = None
    if since:
        try:
            start = dt.date.fromisoformat(since)
        except ValueError:
            start = None
    if start is None:
        last = _existing_max_date(path)
        # Re-fetch the boundary day too (captures same-day revisions); merge dedups it.
        start = last if last else START_FLOOR

    today = dt.date.today()

    # Enumerate weekdays in [start, today]; weekends are guaranteed non-trading.
    days: list[dt.date] = []
    d = start
    while d <= today:
        if d.weekday() < 5:
            days.append(d)
        d += dt.timedelta(days=1)

    sess = requests.Session()
    tally = Tally()
    keys, dates, vals = [], [], []
    # per-series max obs_date seen in THIS fetch (merged into the stored cursors below)
    fetched_cursor: dict[str, dt.date] = {}

    for day in days:
        try:
            rates = _fetch_day(sess, day)
        except TransientError:
            # A timeout/5xx/429/network failure for this day: record and keep going.
            # Do NOT abort the whole run — other days still publish, status -> partial.
            tally.transient_unit()
            time.sleep(RATE)
            continue
        if rates is EMPTY:
            tally.empty_unit()
        elif rates is STRUCTURAL:
            tally.structural_unit()
        elif rates:
            tally.added_unit(len(rates))
            for k, v in rates.items():
                keys.append(k); dates.append(day); vals.append(v)
                cur = fetched_cursor.get(k)
                if cur is None or day > cur:
                    fetched_cursor[k] = day
        else:
            # defensive: empty dict shouldn't occur (returned as EMPTY/STRUCTURAL)
            tally.empty_unit()
        time.sleep(RATE)

    # Stored per-series cursors (frozen series can't hide behind a unit-level max).
    cursors = _per_series_cursors(path)

    if not keys:
        # No new rows fetched. finalize() decides honest status from the tally:
        #   - any structural day            -> DefinitiveError
        #   - whole large window 404/empty  -> DefinitiveError (empty-window floor)
        #   - any transient day             -> 'partial'
        #   - otherwise (a few holidays)    -> 'no_change'
        last = _existing_max_date(path)
        last_obs = last.isoformat() if last else (since or None)
        return finalize(tally, before, last_obs, source="tcmb", series_cursors=cursors)

    new_table = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date": pa.array(dates, pa.date32()),
        "value": pa.array(vals, pa.float64()),
    })

    total, maxd = merge.merge_and_write(path, new_table, mode="merge", dedup_keys=DEDUP)
    added = max(0, total - before)

    # Reconcile the tally's added counter with the actual merge delta so the
    # ok/no_change decision reflects genuinely-new rows (all-revision days = 0 new).
    # The empty/transient/structural counts already recorded drive the honest status.
    tally.added = added

    # Merge this run's per-series maxima into the stored cursors.
    for k, day in fetched_cursor.items():
        iso = day.isoformat()
        if k not in cursors or iso > cursors[k]:
            cursors[k] = iso

    return finalize(tally, total, maxd, source="tcmb", series_cursors=cursors)
