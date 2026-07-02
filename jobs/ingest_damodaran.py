#!/usr/bin/env python3
"""Damodaran Online Financial Data ingest (NYU Stern — Aswath Damodaran).

Source: http://pages.stern.nyu.edu/~adamodar/
License: Free for academic use (publicly posted data)
Coverage: Country risk premiums, industry statistics, equity risk premiums,
          betas, cost of capital, etc. Annual snapshots updated each January.

series_key: DAMODARAN:{dataset}:{col_label}:{entity}

Output: data/clean_full/damodaran/damodaran.parquet
Run: python jobs/ingest_damodaran.py
"""
from __future__ import annotations
import datetime as dt, io, os, re, time
import requests, openpyxl, xlrd
import pyarrow as pa, pyarrow.parquet as pq

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "damodaran")
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

BASE = "https://pages.stern.nyu.edu/~adamodar/pc/datasets"

# Metadata-row keywords to skip when searching for the real header
METADATA_PREFIXES = (
    "date updated", "created by", "what is this data", "home page",
    "data website", "companies in each", "variable definitio",
    "customized", "what is your", "enter your", "do you want",
    "if marginal", "bond used", "fred graph", "home price data",
    "end game", "source:", "last updated",
    # wacc.xls interactive parameter rows
    "to update", "long term", "risk premium", "global default",
    "if yes", "these costs", "expected inflation", "note:", "notes:",
    "updated", "as of", "using",
)

# Sheet names to always skip regardless of content
SKIP_SHEET_KEYWORDS = (
    "explanation", "faq", "notes", "readme", "about",
    "instructions", "cover", "lookup", "summary of",
    "data update sequence",
)

# Datasets: (key, url, entity_type, [optional] specific_sheets)
# specific_sheets = list of sheet names to parse (in order); None = auto-detect
ANNUAL_DATASETS = [
    # Country risk premiums — both current and stable version
    ("ctryprem",    f"{BASE}/ctrypremApr26.xlsx",  "country",
     ["Regional breakdown", "Country Tax Rates",
      "Sovereign Ratings (Moody’s,S&P)", "10-year CDS Spreads",
      "PRS Worksheet", "Default Spreads for Ratings",
      "Regional Weighted Averages"]),
    ("ctryprem_old", f"{BASE}/ctryprem.xlsx",       "country",
     ["Regional breakdown", "Country Tax Rates",
      "Sovereign Ratings (Moody’s,S&P)", "10-year CDS Spreads",
      "PRS Worksheet", "Default Spreads for Ratings"]),
    # Industry betas (US + global)
    ("betas",        f"{BASE}/betas.xls",           "industry", None),
    ("betaEurope",   f"{BASE}/betaEurope.xls",      "industry", None),
    ("betaJapan",    f"{BASE}/betaJapan.xls",       "industry", None),
    ("betaemerg",    f"{BASE}/betaemerg.xls",       "industry", None),
    ("betaGlobal",   f"{BASE}/betaGlobal.xls",      "industry", None),
    # Margins
    ("margins",      f"{BASE}/margin.xls",          "industry", None),
    # EV/EBITDA multiples
    ("evmultiples",  f"{BASE}/vebitda.xls",         "industry", None),
    # Dividend/payout
    ("divfcfe",      f"{BASE}/divfcfe.xls",         "industry", None),
    # Historical S&P 500 returns — clean time-series sheets
    ("histretSP",    f"{BASE}/histretSP.xls",       "USA",
     ["Nominal vs Real Data", "Small Cap", "T. Bond yield & return",
      "Gold Prices", "Home Prices"]),
    # PE ratios
    ("pedata",       f"{BASE}/pedata.xls",          "country",  None),
    # P/BV ratios
    ("pbdata",       f"{BASE}/pbvdata.xls",         "country",  None),
    # Total betas (US)
    ("totalbeta",    f"{BASE}/totalbeta.xls",       "industry", None),
    # Cost of equity / WACC
    ("costequity",   f"{BASE}/wacc.xls",            "industry", None),
    # Sales to capital (capex)
    ("salescapital", f"{BASE}/capex.xls",           "industry", None),
    # Leverage ratios
    ("debt",         f"{BASE}/dbtfund.xls",         "industry", None),
    # Tax rates
    ("taxrate",      f"{BASE}/taxrate.xls",         "industry", None),
    # Price/Sales
    ("psdata",       f"{BASE}/psdata.xls",          "country",  None),
    # Working capital
    ("wcdata",       f"{BASE}/wcdata.xls",          "industry", None),
    # EV/Sales
    ("evsales",      f"{BASE}/evsales.xls",         "industry", None),
    # Profitability
    ("profitability", f"{BASE}/profit.xls",         "industry", None),
]


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def fetch(url: str) -> bytes | None:
    for attempt in range(3):
        try:
            r = requests.get(url, headers=UA, timeout=60, allow_redirects=True)
            if r.status_code == 200 and len(r.content) > 500:
                return r.content
            if r.status_code in (403, 404):
                return None
            log(f"  HTTP {r.status_code}: {url[-60:]}")
        except Exception as e:
            log(f"  ERR: {e}")
        time.sleep(3 * (attempt + 1))
    return None


def _is_metadata_row(row) -> bool:
    """Return True if this row looks like a Damodaran metadata header row."""
    if not row:
        return True
    first = None
    for c in row:
        if c is not None:
            first = str(c).strip().lower()
            break
    if not first:
        return True
    return any(first.startswith(p) for p in METADATA_PREFIXES)


def _is_skip_sheet(name: str) -> bool:
    nl = name.lower()
    return any(kw in nl for kw in SKIP_SHEET_KEYWORDS)


def _float_cell(val) -> float | None:
    """Convert a cell value to float, handling %, commas, etc."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        f = float(val)
        return None if f != f else f  # NaN check
    s = str(val).replace("%", "").replace(",", "").strip()
    if not s or s.lower() in ("na", "n/a", "#n/a", "#ref!", "#div/0!", "#value!"):
        return None
    try:
        f = float(s)
        return None if f != f else f
    except ValueError:
        return None


def _strip_xlquotes(s: str) -> str:
    """Strip surrounding single-quotes that Excel/xlrd adds as text-format artifacts."""
    s = s.strip()
    if len(s) >= 2 and s[0] == "'" and s[-1] == "'":
        return s[1:-1].strip()
    return s


def _entity_str(val) -> str:
    """Normalize an entity value to string key."""
    if val is None:
        return ""
    if isinstance(val, float) and val == int(val):
        return str(int(val))
    return _strip_xlquotes(str(val).strip())


def _obs_date_from_year(yr_val, snapshot_date: dt.date) -> dt.date:
    """Parse a year value to a Dec 31 date."""
    try:
        yr_f = float(yr_val) if not isinstance(yr_val, (int, float)) else yr_val
        yr = int(yr_f)
        if 1900 <= yr <= 2030:
            return dt.date(yr, 12, 31)
    except (TypeError, ValueError):
        pass
    return snapshot_date


# ─────────────────────────────────────────────────────────────────
# Core row parser (shared between xlsx and xls)
# ─────────────────────────────────="─────────────────────────────
def _parse_rows(rows, dataset: str, sheet_name: str) -> tuple[list, list, list]:
    """Parse a list-of-tuples (from either xlsx or xls) into (keys, dates, vals).

    Handles Damodaran's two main layouts:
    1. Industry cross-section: entity=col0 (industry name), cols 1..N are metrics.
       No year column → use snapshot_date.
    2. Time series: col0 = Year (or period string), remaining cols are metrics.
    """
    if len(rows) < 2:
        return [], [], []

    snapshot_date = dt.date(dt.date.today().year, 1, 1)

    # ── Find actual header row ──────────────────────────────────────
    # Skip metadata rows (Date updated:, Created by:, etc.)
    header_idx = -1
    for ri, row in enumerate(rows[:35]):
        if _is_metadata_row(row):
            continue
        non_null = [c for c in (row or []) if c is not None]
        # col 0 must be a non-empty string (real header labels are text, not numbers)
        if not non_null or not isinstance(non_null[0], str) or not non_null[0].strip():
            continue
        # Must have at least 3 non-None cells (wacc.xls real header is at row 18)
        if len(non_null) >= 3:
            header_idx = ri
            break

    if header_idx < 0:
        return [], [], []

    # Build header labels (strip Excel text-format quote pairs)
    raw_header = rows[header_idx]
    header = []
    for c in raw_header:
        if c is None:
            header.append("")
        elif isinstance(c, float) and c == int(c) and abs(c) < 1e6:
            header.append(str(int(c)))
        else:
            header.append(_strip_xlquotes(str(c).strip()))

    if not any(header):
        return [], [], []

    # ── Identify key columns ────────────────────────────────────────
    # Year/period column (for time-series sheets)
    year_ci = None
    for i, h in enumerate(header):
        if h.lower() in ("year", "date", "yr", "period"):
            year_ci = i
            break

    # Entity column: first column that has non-empty label
    entity_ci = 0
    for i, h in enumerate(header):
        if h and not h.replace(".", "").replace("-", "").replace(" ", "").isdigit():
            entity_ci = i
            break

    # Value columns: all non-empty columns except entity and year
    skip_ci = {entity_ci, year_ci}
    val_cols = []
    for i, h in enumerate(header):
        if i in skip_ci or not h:
            continue
        # Skip columns that are clearly non-numeric labels
        label = re.sub(r"[^a-zA-Z0-9_]", "_", h)[:25].strip("_")
        if label:
            val_cols.append((i, label))

    if not val_cols:
        return [], [], []

    # ── Parse data rows ─────────────────────────────────────────────
    keys, dates, vals = [], [], []
    n_entity_errors = 0

    for row in rows[header_idx + 1:]:
        if not row or all(c is None for c in row):
            continue

        entity_raw = row[entity_ci] if entity_ci < len(row) else None
        entity_str = _entity_str(entity_raw)
        if not entity_str or entity_str.lower() in ("nan", "none", "total", "n/a", "aggregate"):
            continue

        # Stop if we hit another metadata-like row
        if entity_str.lower().startswith(METADATA_PREFIXES):
            break

        entity_key = re.sub(r"[^a-zA-Z0-9_]", "_", entity_str)[:30].strip("_")
        if not entity_key:
            n_entity_errors += 1
            if n_entity_errors > 5:
                break
            continue
        n_entity_errors = 0

        # Determine observation date
        if year_ci is not None and year_ci < len(row) and row[year_ci] is not None:
            obs_d = _obs_date_from_year(row[year_ci], snapshot_date)
        else:
            # Try entity as year (histretSP 'Nominal vs Real Data': Year is entity col)
            try:
                yr_s = entity_str.replace(".0", "")
                if yr_s.isdigit() and 1900 <= int(yr_s) <= 2030:
                    obs_d = dt.date(int(yr_s), 12, 31)
                    entity_key = yr_s
                else:
                    obs_d = snapshot_date
            except (ValueError, TypeError):
                obs_d = snapshot_date

        for col_i, col_label in val_cols:
            if col_i >= len(row):
                continue
            v = _float_cell(row[col_i])
            if v is None:
                continue
            keys.append(f"DAMODARAN:{dataset}:{col_label}:{entity_key}")
            dates.append(obs_d)
            vals.append(v)

    n_new = len(keys)
    if n_new > 0:
        log(f"    Sheet '{sheet_name}': {n_new:,} obs")

    return keys, dates, vals


# ─────────────────────────────────────────────────────────────────
# File-type dispatchers
# ─────────────────────────────────────────────────────────────────
def _get_rows_xlsx(data: bytes, sheet_name: str):
    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    if sheet_name not in wb.sheetnames:
        return None
    ws = wb[sheet_name]
    return list(ws.iter_rows(values_only=True))


def _get_rows_xls(data: bytes, sheet_name: str):
    wb = xlrd.open_workbook(file_contents=data)
    if sheet_name not in wb.sheet_names():
        return None
    ws = wb.sheet_by_name(sheet_name)
    result = []
    for ri in range(ws.nrows):
        row = []
        for ci in range(ws.ncols):
            ctype = ws.cell_type(ri, ci)
            val   = ws.cell_value(ri, ci)
            if ctype in (xlrd.XL_CELL_EMPTY, xlrd.XL_CELL_BLANK):
                row.append(None)
            elif ctype == xlrd.XL_CELL_DATE:
                try:
                    row.append(xlrd.xldate_as_datetime(val, wb.datemode))
                except Exception:
                    row.append(val)
            elif ctype == xlrd.XL_CELL_ERROR:
                row.append(None)
            elif ctype == xlrd.XL_CELL_TEXT:
                # Strip surrounding single-quote pairs (Damodaran .xls format artifact)
                s = str(val).strip()
                if len(s) >= 2 and s[0] == "'" and s[-1] == "'":
                    s = s[1:-1].strip()
                row.append(s if s else None)
            else:
                row.append(val)
        result.append(tuple(row))
    return result


def _all_sheet_names(data: bytes, is_xlsx: bool) -> list[str]:
    if is_xlsx:
        wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
        return wb.sheetnames
    else:
        wb = xlrd.open_workbook(file_contents=data)
        return wb.sheet_names()


def _get_rows(data: bytes, sheet_name: str, is_xlsx: bool):
    if is_xlsx:
        return _get_rows_xlsx(data, sheet_name)
    else:
        return _get_rows_xls(data, sheet_name)


# ─────────────────────────────────────────────────────────────────
# Main dataset parser
# ─────────────────────────────────────────────────────────────────
def parse_dataset(data: bytes, dataset: str, url: str,
                  specific_sheets) -> tuple[list, list, list]:
    is_xlsx = url.lower().endswith(".xlsx")

    all_keys, all_dates, all_vals = [], [], []

    if specific_sheets:
        # Use explicitly named sheets (ctryprem, histretSP, etc.)
        all_names = _all_sheet_names(data, is_xlsx)

        def _norm(s: str) -> str:
            """Normalize for comparison: lowercase + ASCII apostrophes."""
            return s.lower().replace('’', "'").replace('‘', "'").replace('“', '"').replace('”', '"')

        norm_to_real = {_norm(n): n for n in all_names}

        for sn in specific_sheets:
            real_sn = sn if sn in all_names else norm_to_real.get(_norm(sn))
            if real_sn is None:
                log(f"    Sheet '{sn}' not found, skipping")
                continue
            sn = real_sn
            rows = _get_rows(data, sn, is_xlsx)
            if rows is None:
                continue
            k, d, v = _parse_rows(rows, dataset, sn)
            all_keys.extend(k); all_dates.extend(d); all_vals.extend(v)
    else:
        # Auto-mode: try all sheets, use first successful one
        for sn in _all_sheet_names(data, is_xlsx):
            if _is_skip_sheet(sn):
                continue
            rows = _get_rows(data, sn, is_xlsx)
            if rows is None:
                continue
            k, d, v = _parse_rows(rows, dataset, sn)
            all_keys.extend(k); all_dates.extend(d); all_vals.extend(v)
            if all_vals:
                break  # first successful sheet

    return all_keys, all_dates, all_vals


def main():
    os.makedirs(OUT, exist_ok=True)
    out = os.path.join(OUT, "damodaran.parquet")

    if os.path.exists(out):
        n = pq.read_metadata(out).num_rows
        log(f"Damodaran: already {n:,} rows"); return

    log("=== Damodaran Financial Data Ingest ===")
    all_keys, all_dates, all_vals = [], [], []

    for entry in ANNUAL_DATASETS:
        if len(entry) == 4:
            dataset, url, entity_type, specific_sheets = entry
        else:
            dataset, url, entity_type = entry
            specific_sheets = None

        log(f"  {dataset}  ({url.split('/')[-1]})")
        data = fetch(url)
        if not data:
            log(f"    -> not found")
            continue

        k, d, v = parse_dataset(data, dataset, url, specific_sheets)
        if v:
            log(f"    -> {len(v):,} obs total")
            all_keys.extend(k); all_dates.extend(d); all_vals.extend(v)
        else:
            log(f"    -> 0 obs (parse failed)")
        time.sleep(0.5)

    if not all_vals:
        log("0 observations parsed"); return

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out, compression="zstd")
    n = pq.read_metadata(out).num_rows
    log(f"=== Damodaran DONE: {n:,} obs ===")


if __name__ == "__main__":
    main()
