"""S1 fetcher — Global Peace Index (GPI), annual, 163 countries, 23 indicators.

Institute for Economics and Peace (IEP). Single grouped parquet
clean_full/gpi/gpi.parquet, schema (series_key, obs_date, value),
series_key = "GPI:{indicator}:{iso3-or-country}". IEP re-publishes the whole
annual table (and revises prior years) each June, so this is a whole-table
overwrite_if_changed source: re-fetch the workbook/CSV and MERGE (dedup
series_key+obs_date, new wins on revision, never-shrink). Reuses the URL list +
parse logic from jobs/ingest_gpi.py.

BLOCKER (verified live 2026-06): every hardcoded GPI URL 404s. IEP moved its
structured data behind a licensing wall (economicsandpeace.org/consulting/
data-licensing/); the wp-content path now serves only PDFs. OWID no longer hosts
a global-peace-index grapher slug; the GitHub `datasets/global-peace-index`
mirror is gone; the Mendeley CC-BY mirror (DOI 10.17632/yjxnfkcv4h, 2008-2023)
is reachable in a browser but its public-API/file endpoints return 403 to server
clients (Cloudflare WAF). With no free programmatic full-table source, this
fetcher does NOT fake success: current_vintage returns None (no usable signal)
and update surfaces the wholesale-404 honestly via the Tally/finalize contract
(every URL 404 -> structural sub-units -> DefinitiveError; timeouts/5xx -> the
unit goes 'partial' and retries). If/when a working URL is restored, add it to
the front of GPI_URLS and the fetcher resumes with no other change.
"""
from __future__ import annotations
import datetime as dt
import io
import os
import re

import pyarrow as pa
import requests

from ... import config, blob, merge
from ..base import Result
from ._common import Tally, finalize
from ._vintage import UA

SOURCE = "gpi"
DEDUP = ("series_key", "obs_date")  # matches jobs/ingest_gpi.py output schema

# URL list copied from jobs/ingest_gpi.py (IEP report files change name each year,
# so several candidates are tried in order; first one that parses >0 rows wins).
GPI_URLS = [
    # 2024 edition
    "https://www.visionofhumanity.org/wp-content/uploads/2024/06/GPI-2024-web.xlsx",
    "https://www.visionofhumanity.org/wp-content/uploads/2024/07/GPI-2024-full-report-data.xlsx",
    "https://www.visionofhumanity.org/wp-content/uploads/2024/06/GPI-2024-download.xlsx",
    # 2023 edition
    "https://www.visionofhumanity.org/wp-content/uploads/2023/06/GPI-2023-web.xlsx",
    "https://www.visionofhumanity.org/wp-content/uploads/2023/06/GPI-2023-Results-Overall-Scores-and-Domains.xlsx",
    # 2022
    "https://www.visionofhumanity.org/wp-content/uploads/2022/06/GPI-2022-web.xlsx",
    # GitHub / OWID mirrors (CSV)
    "https://raw.githubusercontent.com/datasets/global-peace-index/master/data/global-peace-index.csv",
    "https://ourworldindata.org/grapher/global-peace-index.csv",
]

_TRANSIENT_HTTP = (429, 500, 502, 503, 504)


def current_vintage(unit):
    """Cheap probe: ETag/Last-Modified of the first URL that returns 200.

    A bare http_vintage() also returns a 404 page's Content-Length, which would be
    a junk "vintage" that never moves; so we require a 200 first (a real file) and
    only then read its validator headers. Every GPI_URL currently 404s, so this
    returns None — correct: there is genuinely no cheap signal. The strategy then
    fetches anyway (cadence-gated), and update() surfaces the dead source honestly.
    """
    for url in GPI_URLS:
        try:
            r = requests.head(url, headers=UA, timeout=60, allow_redirects=True)
        except (requests.Timeout, requests.ConnectionError):
            continue
        if r.status_code != 200:
            continue
        h = r.headers
        v = h.get("ETag") or h.get("Last-Modified") or h.get("Content-Length")
        if v:
            return v
    return None


def _parse_owid_csv(data: bytes):
    """OWID/GitHub CSV: columns Entity, Code, Year, <value cols...>."""
    import csv
    text = data.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    headers = [h.strip() for h in (reader.fieldnames or [])]

    entity_col = next((h for h in headers if h.lower() in ("entity", "country", "name")), None)
    code_col = next((h for h in headers if h.lower() in ("code", "iso3", "iso")), None)
    year_col = next((h for h in headers if h.lower() in ("year",)), None)
    id_col = code_col or entity_col
    if not id_col or not year_col:
        return [], [], []

    val_cols = [h for h in headers if h not in (entity_col, code_col, year_col) and h]
    keys, dates, vals = [], [], []
    for rec in reader:
        cid = (rec.get(id_col) or "").strip()
        if not cid:
            continue
        try:
            yr = int(float((rec.get(year_col) or "").strip()))
            obs_d = dt.date(yr, 12, 31)
        except (TypeError, ValueError):
            continue
        for col in val_cols:
            raw = rec.get(col, "")
            if not raw or str(raw).strip() in ("", "NA"):
                continue
            try:
                v = float(str(raw).strip())
            except (TypeError, ValueError):
                continue
            if v != v:  # NaN
                continue
            safe = re.sub(r"[^a-zA-Z0-9_]", "_", col)[:30]
            keys.append(f"GPI:{safe}:{cid}")
            dates.append(obs_d)
            vals.append(v)
    return keys, dates, vals


def _parse_gpi_xlsx(data: bytes):
    """IEP GPI Excel workbook (one sheet of country x indicator scores)."""
    import openpyxl
    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    keys, dates, vals = [], [], []

    for sheet_name in wb.sheetnames:
        if any(s in sheet_name.lower() for s in ["cover", "about", "note", "method", "source"]):
            continue
        ws = wb[sheet_name]
        rows = list(ws.iter_rows(values_only=True))
        if len(rows) < 5:
            continue

        header_idx = None
        for ri, row in enumerate(rows[:10]):
            row_lower = [str(c).strip().lower() if c else "" for c in row]
            if "country" in row_lower or "iso" in row_lower or "code" in row_lower:
                header_idx = ri
                break
        if header_idx is None:
            continue

        header = [str(c).strip() if c else "" for c in rows[header_idx]]
        ctry_ci = next((i for i, h in enumerate(header) if h.lower() in ("country", "nation", "name")), None)
        iso_ci = next((i for i, h in enumerate(header) if h.lower() in ("iso", "iso3", "code", "iso_code")), None)
        year_ci = next((i for i, h in enumerate(header) if h.lower() in ("year",)), None)
        id_ci = iso_ci if iso_ci is not None else ctry_ci
        if id_ci is None:
            continue

        skip_ci = {id_ci, ctry_ci, iso_ci, year_ci, None}
        val_cols = [(i, h) for i, h in enumerate(header) if i not in skip_ci and h]
        if not val_cols:
            continue

        sheet_yr = None
        if year_ci is None:
            m = re.search(r"\b(20\d{2})\b", sheet_name)
            if m:
                sheet_yr = int(m.group(0))

        n_before = len(vals)
        for row in rows[header_idx + 1:]:
            if not row or row[id_ci] is None:
                continue
            cid = str(row[id_ci]).strip()
            if not cid or cid.lower() in ("nan", "none"):
                continue

            if year_ci is not None and row[year_ci] is not None:
                try:
                    yr = int(float(str(row[year_ci]).strip()))
                    obs_d = dt.date(yr, 12, 31)
                except (TypeError, ValueError):
                    continue
            elif sheet_yr:
                obs_d = dt.date(sheet_yr, 12, 31)
            else:
                continue

            for col_i, col_name in val_cols:
                if col_i >= len(row) or row[col_i] is None:
                    continue
                try:
                    v = float(row[col_i])
                except (TypeError, ValueError):
                    continue
                if v != v:  # NaN
                    continue
                safe = re.sub(r"[^a-zA-Z0-9_]", "_", col_name)[:30]
                keys.append(f"GPI:{safe}:{cid}")
                dates.append(obs_d)
                vals.append(v)

        if len(vals) > n_before:
            break  # first sheet that yields data is the results sheet

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


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "gpi.parquet")
    before = blob.row_count(path)
    tally = Tally()

    keys, dates, vals = [], [], []
    got_data = False
    for url in GPI_URLS:
        try:
            r = requests.get(url, headers=UA, timeout=120, allow_redirects=True)
        except (requests.Timeout, requests.ConnectionError):
            tally.transient_unit()  # network/timeout — retry next run, never "no data"
            continue

        if r.status_code in _TRANSIENT_HTTP:
            tally.transient_unit()
            continue
        if r.status_code != 200 or len(r.content) < 1000:
            # 404 / hard-4xx / empty body for this candidate URL -> structural for this sub-unit.
            tally.structural_unit()
            continue

        try:
            if url.endswith(".csv"):
                k, d, v = _parse_owid_csv(r.content)
            else:
                k, d, v = _parse_gpi_xlsx(r.content)
        except Exception:
            tally.structural_unit()  # 200 with a body we couldn't parse -> schema break
            continue

        if v:
            keys, dates, vals = k, d, v
            got_data = True
            break
        tally.structural_unit()  # 200, real body, parsed 0 rows -> structural

    if not got_data:
        # No URL yielded data. finalize() is honest: any structural sub-unit raises
        # DefinitiveError (the source is broken, not quiet); pure transients -> 'partial'.
        return finalize(tally, before, None, source=SOURCE)

    tbl = pa.table({"series_key": pa.array(keys, pa.string()),
                    "obs_date": pa.array(dates, pa.date32()),
                    "value": pa.array(vals, pa.float64())})

    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    tally.added_unit(max(0, n - before))  # the one successful workbook sub-unit
    return finalize(tally, n, md, source=SOURCE, series_cursors=_series_maxes(tbl))
