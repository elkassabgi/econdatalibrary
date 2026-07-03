"""S2 fetcher — CNB (Czech National Bank daily FX fixing). No key.

Layout: single parquet clean_full/cnb/cnb.parquet, schema (series_key,
obs_date, value); series_key = "CNB_FX:<CCY>_CZK", values normalized to
per-1-unit (year.txt quotes per `amount` units — e.g. JPY per 100) — identical
to the legacy jobs/ingest_cnb.py so the merge EXTENDS the existing 264k-row
history.

Date-tail: year.txt?year=YYYY files; fetch every year from the stored max
obs_date's year through the current year (boundary year re-fetched; dedup
keep-last). Jan-1 boundary is safe by construction: the stored-max year is
always included, so late-December rows published after New Year are caught.
First run backfills from 1991 (CNB's published history).

HONEST-STATUS: timeouts/5xx/429 -> TransientError per year (run partial). A
200 whose body yields zero parseable rows for a NON-current year that should
have data -> structural. Current-year-only quiet window -> earned no_change.
"""
from __future__ import annotations
import datetime as dt
import os
import re
import time

import pyarrow as pa
import requests

from ... import config, blob, merge
from ...errors import TransientError
from ..base import Result
from ._common import Tally, finalize

SOURCE = "cnb"
FILE = "cnb.parquet"
BASE = "https://www.cnb.cz/en/financial_markets/foreign_exchange_market/exchange_rate_fixing/year.txt"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
DEDUP = ("series_key", "obs_date")
EARLIEST_YEAR = 1991


def _fetch_year(session, year, tries=5) -> str:
    url = f"{BASE}?year={year}"
    for a in range(tries):
        try:
            r = session.get(url, headers=UA, timeout=180)
        except (requests.Timeout, requests.ConnectionError) as e:
            if a == tries - 1:
                raise TransientError(f"cnb {year}: {e}")
            time.sleep(min(2 ** a, 30)); continue
        if r.status_code == 200:
            return r.text
        if r.status_code in (429, 500, 502, 503, 504):
            if a == tries - 1:
                raise TransientError(f"cnb {year} HTTP {r.status_code}")
            time.sleep(min(2 ** a, 30)); continue
        raise TransientError(f"cnb {year} HTTP {r.status_code}")
    raise TransientError(f"cnb {year}: retry budget exhausted")


def _parse_year(text: str) -> list[tuple[str, dt.date, float]]:
    """Port of jobs/ingest_cnb.py's parser: pipe-delimited, header row like
    'Date|amount CODE|…', data rows 'DD.MM.YYYY|rate|…'; values normalized
    per-1-unit (v_raw / amount)."""
    rows: list[tuple[str, dt.date, float]] = []
    col_map: dict[int, tuple[str, float]] = {}
    for line in (text or "").splitlines():
        parts = [p.strip() for p in line.split("|")]
        if len(parts) < 2:
            continue
        if parts[0].lower().startswith("date"):
            col_map = {}
            for j, cell in enumerate(parts[1:], start=1):
                m = re.match(r"([\d.,]+)\s+([A-Z]{3})$", cell.strip())
                if m:
                    try:
                        amount = float(m.group(1).replace(",", "."))
                    except ValueError:
                        continue
                    if amount > 0:
                        col_map[j] = (m.group(2), amount)
            continue
        if not col_map:
            continue
        try:
            d_parts = parts[0].split(".")
            if len(d_parts) != 3:
                continue
            obs_date = dt.date(int(d_parts[2]), int(d_parts[1]), int(d_parts[0]))
        except (ValueError, TypeError):
            continue
        for j, (ccy, amount) in col_map.items():
            if j >= len(parts):
                continue
            v_str = parts[j].strip().replace(",", ".")
            if not v_str or v_str in ("N/A", "-"):
                continue
            try:
                v = float(v_str) / amount
            except (ValueError, TypeError):
                continue
            if v == v:  # NaN guard
                rows.append((f"CNB_FX:{ccy}_CZK", obs_date, v))
    return rows


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
    today = dt.date.today()
    if last is not None:
        years = range(last.year, today.year + 1)   # boundary year included
    else:
        try:
            start_year = dt.date.fromisoformat(str(since)[:10]).year if since else EARLIEST_YEAR
        except ValueError:
            start_year = EARLIEST_YEAR
        years = range(start_year, today.year + 1)

    tally = Tally()
    keys, dates, vals = [], [], []
    session = requests.Session()
    for year in years:
        try:
            text = _fetch_year(session, year)
        except TransientError:
            tally.transient_unit()
            time.sleep(0.5)
            continue
        rows = _parse_year(text)
        if not rows:
            # A past year with a real body but zero parseable rows = the format
            # broke (structural); an empty current-year early January is possible
            # but CNB publishes from Jan 2, so treat current-year-empty as empty.
            if year < today.year and len((text or "").strip()) > 200:
                tally.structural_unit()
            else:
                tally.empty_unit()
            time.sleep(0.3)
            continue
        added = 0
        for k, d, v in rows:
            if last is not None and d < last:
                continue                       # only the tail (>= boundary day)
            keys.append(k); dates.append(d); vals.append(v)
            added += 1
        genuinely_new = any(d > last for _, d, _ in rows) if last is not None else added > 0
        if genuinely_new:
            tally.added_unit(added)
        else:
            tally.empty_unit()
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

    cur: dict[str, str] = {}
    for k, d in zip(keys, dates):
        iso = d.isoformat()
        if k not in cur or iso > cur[k]:
            cur[k] = iso
    return finalize(tally, n, maxd or last_db, source=SOURCE, series_cursors=cur)
