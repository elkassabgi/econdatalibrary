"""S1 fetcher - KOF Globalisation Index (ETH Zurich), annual, 1970-present.

Re-hosted under KOF's written NC academic permission (DATABASE_LICENSES_VERBATIM.md). Single grouped parquet
clean_full/kof_globalization/kof_globalization.parquet, schema (series_key, obs_date, value). KOF re-estimates
and re-publishes the WHOLE panel each yearly edition (history is revised).

WHICH EDITION (fixed 2026-11-25). KOF publishes an edition at a year-stamped path,
.../Globalization/<Y>/KOFGI_<Y>_public.xlsx. Until this fix update() walked a fixed list whose first entry was
the 2025 file, so no run, due or forced, could fetch a newer edition: each due run booked no_change and read
green (R159). The probe could not help: finalize() stamps new_vintage "date-tail" and this module has not opted in
to the probe stamp, so the probe token is never stored and gates nothing (strategies/base.py, R1154). Now probe and
fetch share ONE year selector, edition_years(): the UTC run year + 1 down to 2024, newest first. No hand edit
is needed when a new edition ships.
  * + 1: KOF ships an edition late in its stamp year (the 2025 file carried Last-Modified 19 Dec 2025,
    tools/audit_vintage_stability.py), so a next-year stamp released in December is in reach. Each such year
    costs one 404 until it exists.
  * floor 2024: the oldest edition this fetcher ever carried. The walk stops at the first edition that
    answers, so the floor costs requests only when every newer edition is gone.

HOW LOUD (each rule is pinned in tests/test_kof_globalization_newest_edition.py):
  * an edition is a 200 whose body starts PK\\x03\\x04 (an xlsx is a zip) and is over 5,000 bytes. Any other
    answer for a year (403/404, or a 200 HTML page - a soft-404) means that year is not published;
  * a transient answer (timeout, connection error, 429, 5xx) for ANY year STOPS the walk with nothing merged.
    An older edition must never stand in for one we could not judge: the merge lets incoming rows win, so it
    would overwrite the newer edition's revisions;
  * no year answers -> structural (DefinitiveError, existing data kept), so a human resolves the URL;
  * the chosen edition fails to parse, or parses 0 rows -> structural, with the error named. Never a fallback
    to an older year;
  * the chosen edition's newest year is older than the store's newest year or than `since`, or it carries
    under 95% of the stored keys -> structural, nothing merged: an older file or an id fork;
  * the merged edition's year is below (UTC run year - 1) -> PARTIAL, naming the gap (R1300): the newer
    edition is missing, or it is at a URL this module does not know.

MERGE, NOT OVERWRITE. The whole panel is re-estimated, so an overwrite looks natural, but it REMOVES stored
rows that KOF no longer ships, and removing stored rows is the owner's decision. The merge keeps them, lets the
edition win every (key, date) it carries, and asks the merge which keys changed their value
(report_changed_keys): a pass that revises values without adding a row books a revision, so it reads ok and
the CSV phase re-derives (R549) instead of booking no_change. Known cost: a (key, date) that a new edition
drops keeps the older edition's value. The 95% key floor stops an id fork from merging beside the old keys.

The version-stable time-series API fallback (datenservice.kof.ethz.ch/api/v1 ... globidx_v2020) is REMOVED:
its content was never measured against the editions, and the KOF page links a different host
(tsdb-api.kof.ethz.ch/v2). A download to measure it needs the owner's approval.

Parse logic is reused from jobs/ingest_kof_globalization.py (first sheet yielding obs wins,
KOF:{SAFE_COL}:{ISO} keys, Dec-31 obs) so keys dedup-match the published file.
"""
from __future__ import annotations
import datetime as dt
import io
import os

import pyarrow as pa
import pyarrow.compute as pc
import requests

from ... import config, blob, merge
from ._common import Tally, finalize
from ._vintage import UA

SOURCE = "kof_globalization"
DEDUP = ("series_key", "obs_date")

# The ONE year selector (edition_years) runs from the UTC run year + 1 down to this floor. Fixed: see the
# module docstring. It never needs a hand edit when KOF ships a new edition.
OLDEST_EDITION = 2024
_DAM_TMPL = ("https://ethz.ch/content/dam/ethz/special-interest/dual/kof-dam/"
             "documents/Globalization/{y}/KOFGI_{y}_public.xlsx")
_MIN_BYTES = 5000
_XLSX_MAGIC = b"PK\x03\x04"
_TRANSIENT_STATUS = (429, 500, 502, 503, 504)
_MIN_KEY_SHARE = 0.95


def _utc_now(clock=dt.datetime):
    """The run's clock, in UTC. The only place this module reads a clock; tests replace it."""
    return clock.now(dt.timezone.utc)


def _run_year(now) -> int:
    if now.tzinfo is None:
        raise ValueError("kof_globalization: the run clock must be timezone-aware (UTC)")
    return now.astimezone(dt.timezone.utc).year


def edition_years(now=None) -> tuple:
    """Edition years to try, newest first: the UTC run year + 1 down to OLDEST_EDITION. Shared by
    current_vintage(), update() and jobs/ingest_kof_globalization.py, so probe and fetch walk the same years."""
    now = _utc_now() if now is None else now
    return tuple(range(_run_year(now) + 1, OLDEST_EDITION - 1, -1))


def edition_url(year: int) -> str:
    return _DAM_TMPL.format(y=year)


def _head_kind(r) -> str:
    """'ok' (an edition), 'transient', or 'gone' for one HEAD answer. A HEAD has no body, so a soft-404 is
    told apart by its text/* Content-Type and a Content-Length of 5,000 bytes or less."""
    if r.status_code in _TRANSIENT_STATUS:
        return "transient"
    if r.status_code != 200:
        return "gone"
    ctype = (r.headers.get("Content-Type") or "").strip().lower()
    if ctype.startswith("text/"):
        return "gone"
    n = r.headers.get("Content-Length")
    if n is not None:
        try:
            if int(n) <= _MIN_BYTES:
                return "gone"
        except ValueError:
            return "gone"
    return "ok"


def current_vintage(unit):
    """Cheap probe: HEAD the year-stamped URLs from edition_years(), newest first; the first answer that is
    an edition gives "<year>:<ETag or Last-Modified or Content-Length>". A transient answer for any year
    returns None (undeterminable): it cannot rule out that year, so it must not vouch for an older one.

    The token is NOT stored (finalize stamps "date-tail"; no probe-stamp opt-in), so it gates nothing:
    it is the runs row's "probe note" only."""
    for y in edition_years():
        try:
            r = requests.head(edition_url(y), headers=UA, timeout=60, allow_redirects=True)
        except (requests.Timeout, requests.ConnectionError):
            return None
        kind = _head_kind(r)
        if kind == "transient":
            return None
        if kind == "ok":
            tag = r.headers.get("ETag") or r.headers.get("Last-Modified") \
                or r.headers.get("Content-Length") or "200"
            return f"{y}:{tag}"
    return None


def _fetch(url: str):
    """(kind, payload). kind 'ok': payload is the xlsx body. kind 'transient' or 'gone': payload is the
    reason. Only a 200 whose body starts PK\\x03\\x04 and is over 5,000 bytes is an edition."""
    try:
        r = requests.get(url, headers=UA, timeout=120, allow_redirects=True)
    except (requests.Timeout, requests.ConnectionError) as e:
        return "transient", type(e).__name__
    if r.status_code in _TRANSIENT_STATUS:
        return "transient", f"HTTP {r.status_code}"
    if r.status_code != 200:
        return "gone", f"HTTP {r.status_code}"
    body = r.content or b""
    if not body.startswith(_XLSX_MAGIC):
        return "gone", (f"HTTP 200 but not an xlsx ({len(body):,} bytes, "
                        f"Content-Type {r.headers.get('Content-Type')!r})")
    if len(body) <= _MIN_BYTES:
        return "gone", f"HTTP 200 but an xlsx of only {len(body):,} bytes"
    return "ok", body


def _parse_kof_xlsx(data: bytes):
    """Parse KOF XLSX as jobs/ingest_kof_globalization.py does (first sheet yielding obs wins;
    KOF:{SAFE_COL}:{ISO} keys; year -> Dec-31 obs). A row shorter than its id or year column is skipped."""
    import openpyxl
    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    keys, dates, vals = [], [], []

    for sheet_name in wb.sheetnames:
        if any(s in sheet_name.lower() for s in ["readme", "note", "info", "about", "source"]):
            continue
        ws = wb[sheet_name]
        rows = list(ws.iter_rows(values_only=True))
        if len(rows) < 5:
            continue

        header_idx = None
        for ri, row in enumerate(rows[:10]):
            row_lower = [str(c).strip().lower() if c else "" for c in row]
            if any(v in row_lower for v in ["code", "country", "iso", "year"]):
                header_idx = ri
                break
        if header_idx is None:
            continue

        header = [str(c).strip().lower() if c else "" for c in rows[header_idx]]
        iso_ci = next((i for i, h in enumerate(header) if h in ("code", "iso3", "iso", "iso3c", "country_code")), None)
        ctry_ci = next((i for i, h in enumerate(header) if h in ("country", "name", "country_name")), None)
        year_ci = next((i for i, h in enumerate(header) if h in ("year",)), None)

        id_ci = iso_ci if iso_ci is not None else ctry_ci
        if id_ci is None or year_ci is None:
            continue

        skip_ci = {iso_ci, ctry_ci, year_ci, None}
        val_col_names = []
        for i, h in enumerate(header):
            if i in skip_ci or not h:
                continue
            val_col_names.append((i, h))
        if not val_col_names:
            continue

        need = max(id_ci, year_ci)
        n_before = len(vals)
        for row in rows[header_idx + 1:]:
            if not row or len(row) <= need or row[id_ci] is None or row[year_ci] is None:
                continue
            try:
                yr = int(float(str(row[year_ci]).strip()))
                obs_d = dt.date(yr, 12, 31)
            except (TypeError, ValueError):
                continue

            id_val = str(row[id_ci]).strip()
            if not id_val or id_val.lower() in ("nan", "none", ""):
                continue

            for col_i, col_name in val_col_names:
                if col_i >= len(row) or row[col_i] is None:
                    continue
                try:
                    v = float(row[col_i])
                    if v != v:
                        continue
                    safe_col = col_name.replace(" ", "_")[:20].upper()
                    keys.append(f"KOF:{safe_col}:{id_val.upper()}")
                    dates.append(obs_d)
                    vals.append(v)
                except (TypeError, ValueError):
                    pass

        if len(vals) - n_before > 0:
            break  # use first successful sheet (matches ingester)

    return keys, dates, vals


def _series_maxes(tbl):
    out = {}
    if tbl.num_rows == 0:
        return out
    for k, d in zip(tbl.column("series_key").to_pylist(), tbl.column("obs_date").to_pylist()):
        if d is None:
            continue
        if k not in out or d > out[k]:
            out[k] = d
    return {k: v.isoformat() for k, v in out.items()}


def _stored(path):
    """(newest stored obs_date, set of stored series keys); (None, empty set) when there is no store."""
    if not blob.exists(path):
        return None, set()
    t = blob.read_table(path, columns=["series_key", "obs_date"])
    if t.num_rows == 0:
        return None, set()
    return pc.max(t.column("obs_date")).as_py(), set(t.column("series_key").to_pylist())


def _year_of(since):
    if since is None:
        return None
    try:
        return int(str(since)[:4])
    except ValueError:
        return None


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "kof_globalization.parquet")
    before = blob.row_count(path)
    tally = Tally()
    now = _utc_now()
    run_year = _run_year(now)
    years = edition_years(now)

    # 1. The newest edition that answers. A transient for ANY year stops here, with nothing merged.
    year = data = None
    misses = []
    for y in years:
        kind, got = _fetch(edition_url(y))
        if kind == "transient":
            tally.transient_unit(f"{y} edition: {got}; walk stopped, nothing merged")
            return finalize(tally, before, None, source=SOURCE)
        if kind == "ok":
            year, data = y, got
            break
        misses.append(f"{y}: {got}")
    if data is None:
        span = f"{years[0]}..{years[-1]}" if years else f"none (UTC run year {run_year})"
        tally.structural_unit(f"no year-stamped edition answers for {span} ({'; '.join(misses)})")
        return finalize(tally, before, None, source=SOURCE)   # raises DefinitiveError

    # 2. Parse it. Any parse error, or 0 rows, is structural; never a fallback to an older year.
    label = f"{year} edition"
    try:
        keys, dates, vals = _parse_kof_xlsx(data)
    except Exception as e:  # noqa: BLE001 - a real xlsx body that will not parse is a break, named
        tally.structural_unit(f"{label}: parse error {type(e).__name__}: {e}")
        return finalize(tally, before, None, source=SOURCE)
    tbl = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date": pa.array(dates, pa.date32()),
        "value": pa.array(vals, pa.float64()),
    })
    if tbl.num_rows == 0:
        tally.structural_unit(f"{label}: the xlsx parsed 0 rows (sheet layout changed?); no fallback")
        return finalize(tally, before, None, source=SOURCE)

    # 3. Refuse an edition older than what we hold, and an id fork.
    newest = pc.max(tbl.column("obs_date")).as_py()
    stored_newest, stored_keys = _stored(path)
    refuse = []
    if stored_newest is not None and newest.year < stored_newest.year:
        refuse.append(f"its newest year {newest.year} is older than the store's {stored_newest.year}")
    since_year = _year_of(since)
    if since_year is not None and newest.year < since_year:
        refuse.append(f"its newest year {newest.year} is older than since {since}")
    if stored_keys:
        share = len(stored_keys & set(keys)) / len(stored_keys)
        if share < _MIN_KEY_SHARE:
            refuse.append(f"it carries {share:.1%} of the {len(stored_keys):,} stored keys "
                          f"(floor {_MIN_KEY_SHARE:.0%}; an id fork)")
    if refuse:
        tally.structural_unit(f"{label} refused, nothing merged: " + "; ".join(refuse))
        return finalize(tally, before, None, source=SOURCE)

    # 4. Merge, and count a revision that adds no row as a revision (R549).
    n, md, changed = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP, report_changed_keys=True)
    added = max(0, n - before)
    if added:
        tally.added_unit(added, label)
    elif changed:
        tally.revised_unit(label)
    else:
        tally.empty_unit(label)
    res = finalize(tally, n, md, source=SOURCE, series_cursors=_series_maxes(tbl))

    # 5. A newest edition below (UTC run year - 1) is a gap, recorded as partial (R1300).
    fname = f"KOFGI_{year}_public.xlsx"
    if year < run_year - 1:
        res.status = "partial"
        res.error = (f"newest edition gap: the newest KOF edition that answers is {year} ({fname}) but the UTC "
                     f"run year is {run_year}, so the {run_year - 1} edition is missing or at a URL this fetcher "
                     f"does not know; merged {year}; {res.error}")
    else:
        res.error = f"{res.error}; edition {year} ({fname})"
    return res
