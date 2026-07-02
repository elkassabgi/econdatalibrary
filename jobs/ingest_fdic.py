#!/usr/bin/env python3
"""FDIC bank data ingest — full history from banks.data.fdic.gov.

Public Domain (US government data). No API key required.

Endpoints covered:
  financials  — quarterly call-report data per institution (the main dataset)
  institutions — all FDIC-insured bank attributes (snapshot)
  history      — institution name changes / mergers
  failures     — failed bank list (1934–present)
  summary      — industry-level aggregate stats by quarter

Output: data/clean_full/fdic/<endpoint>.parquet
  financials stored in long format: series_key = "CERT={cert}:{field}", obs_date, value
  Others stored wide (one row per record with all available fields).

Run: python jobs/ingest_fdic.py
     python jobs/ingest_fdic.py --only financials
"""
from __future__ import annotations
import datetime as dt, json, os, sys, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "fdic")
BASE = "https://banks.data.fdic.gov/api"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
PAGE = 10000

# Key financial metrics from FDIC call reports (most analytically useful).
# Full list has 200+ fields; we take the core balance sheet + income + ratios.
FINANCIAL_FIELDS = [
    "REPDTE","CERT","ASSET","NETINC","EQ","DEP","LNLSNET","INTINC","EINTEXP",
    "LNLSCI","LNLSRE","LNLSCONS","SC","TRADING","INTEXP","NONII","NONIX",
    "ROAS","ROES","NETINM","LNATRES","NCLNLS","CHRTOFF","ORE",
    "REPDTE",
]
# deduplicate
FINANCIAL_FIELDS = list(dict.fromkeys(FINANCIAL_FIELDS))

INST_FIELDS = [
    "CERT","INSTNAME","CITY","STALP","ZIP","REPDTE","ASSET","DEP","NETINC",
    "EQ","ESTYMD","ENDEFYMD","ACTIVE","SPECGRP","CHRTAGNT","INSDATE",
    "NAMEHCR","HCTMULT","TRUST","INSURED","RSSDHCR","STCHRTR",
]

HIST_FIELDS = ["CERT","INSTNAME","CITY","STALP","PCITY","EFFDATE","CLASS",
               "RESTYPE","RESTYPE1","CHANGECODE","PCITY"]
HIST_FIELDS = list(dict.fromkeys(HIST_FIELDS))

FAIL_FIELDS = ["CERT","INSTNAME","CITY","STALP","SAVR","RESTYPE","RESDATE",
               "FAILDATE","COST","QBFASSET","QBFDATE","CLASS"]

SUMM_FIELDS = ["REPDTE","ASSET","NETINC","EQ","DEP","LNLSNET","INTINC",
               "NONII","ROAS","ROES","INTEXP"]


def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def api_get(endpoint: str, params: dict, retries: int = 4) -> dict | None:
    url = f"{BASE}/{endpoint}"
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=UA, timeout=120)
            if r.status_code == 200:
                return r.json()
            log(f"  HTTP {r.status_code} attempt {attempt+1}: {endpoint}")
        except Exception as e:
            log(f"  ERR attempt {attempt+1}: {e}")
        time.sleep(5 * (attempt + 1))
    return None


def fetch_all_pages(endpoint: str, fields: list[str], filters: str = "",
                    sort_by: str = "", sort_order: str = "ASC") -> list[dict]:
    """Paginate through all records for an endpoint."""
    rows = []
    offset = 0
    params = {
        "fields": ",".join(fields),
        "limit": PAGE,
        "offset": 0,
        "format": "json",
        "output": "json",
    }
    if filters: params["filters"] = filters
    if sort_by: params["sort_by"] = sort_by; params["sort_order"] = sort_order

    while True:
        params["offset"] = offset
        result = api_get(endpoint, params)
        if not result:
            break
        data = result.get("data", [])
        if not data:
            break
        rows.extend(data)
        meta  = result.get("meta", {})
        total = int(meta.get("total", 0))
        offset += len(data)
        log(f"    {endpoint}: {offset:,}/{total:,} rows")
        if offset >= total:
            break
        time.sleep(0.3)
    return rows


def parse_date(s: str) -> dt.date | None:
    """Parse FDIC date strings: YYYYMMDD or YYYY-MM-DD."""
    if not s:
        return None
    s = s.strip()
    try:
        if len(s) == 8 and s.isdigit():
            return dt.date(int(s[:4]), int(s[4:6]), int(s[6:8]))
        if len(s) == 10 and s[4] == "-":
            return dt.date.fromisoformat(s[:10])
    except Exception:
        pass
    return None


def ingest_financials():
    """Download FDIC quarterly call-report data in long format.
    Strategy: paginate by REPDTE year-chunks to avoid timeouts.
    """
    out_path = os.path.join(OUT, "financials.parquet")
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"financials: already {n:,} rows"); return n

    log("financials: fetching all quarterly records...")
    # Chunk by year to avoid single giant request
    all_keys, all_dates, all_vals = [], [], []
    start_year = 1986
    end_year   = dt.date.today().year

    for yr in range(start_year, end_year + 1):
        date_lo = f"{yr}0101"; date_hi = f"{yr}1231"
        filt = f"REPDTE:[{date_lo} TO {date_hi}]"
        rows = fetch_all_pages("financials", FINANCIAL_FIELDS,
                               filters=filt, sort_by="CERT")
        log(f"  {yr}: {len(rows):,} institution-quarters")
        for rec in rows:
            rec = rec.get("data", rec)  # API wraps in {"data": {...}}
            cert = str(rec.get("CERT", ""))
            repdte_raw = str(rec.get("REPDTE", ""))
            d = parse_date(repdte_raw)
            if not d or not cert:
                continue
            for field in FINANCIAL_FIELDS:
                if field in ("REPDTE", "CERT"):
                    continue
                raw_v = rec.get(field, None)
                if raw_v is None or raw_v == "":
                    continue
                try:
                    v = float(raw_v)
                except (TypeError, ValueError):
                    continue
                all_keys.append(f"CERT={cert}:{field}")
                all_dates.append(d)
                all_vals.append(v)
        if len(all_vals) > 5_000_000:
            log(f"  flushing {len(all_vals):,} rows to parquet...")
            _append_parquet(out_path, all_keys, all_dates, all_vals)
            all_keys, all_dates, all_vals = [], [], []

    if all_vals:
        _append_parquet(out_path, all_keys, all_dates, all_vals)

    n = pq.read_metadata(out_path).num_rows if os.path.exists(out_path) else 0
    log(f"financials: DONE {n:,} obs")
    return n


def _append_parquet(path: str, keys: list, dates: list, vals: list):
    """Append rows to a Parquet file (creates if not exists)."""
    tbl = pa.table({
        "series_key": pa.array(keys,  pa.string()),
        "obs_date":   pa.array(dates, pa.date32()),
        "value":      pa.array(vals,  pa.float64()),
    })
    if os.path.exists(path):
        existing = pq.read_table(path)
        tbl = pa.concat_tables([existing, tbl])
    pq.write_table(tbl, path, compression="zstd")


def ingest_wide(endpoint: str, fields: list[str],
                key_fields: list[str], date_field: str | None,
                filters: str = "") -> int:
    """Download a FDIC table and save in wide format (all fields as columns).
    For endpoints where the number of records is small enough.
    """
    out_path = os.path.join(OUT, f"{endpoint}.parquet")
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"{endpoint}: already {n:,} rows"); return n

    rows_raw = fetch_all_pages(endpoint, fields, filters=filters)
    if not rows_raw:
        log(f"{endpoint}: 0 rows"); return 0

    # Build columns dynamically
    col_data: dict[str, list] = {f: [] for f in fields}
    for rec in rows_raw:
        rec = rec.get("data", rec)
        for f in fields:
            col_data[f].append(rec.get(f, None))

    # Infer arrow types
    pa_cols = {}
    for f, vals in col_data.items():
        if f == date_field:
            pa_cols[f] = pa.array([parse_date(str(v)) if v else None for v in vals],
                                  type=pa.date32())
        else:
            # try float, fallback to string
            try:
                pa_cols[f] = pa.array(
                    [float(v) if v not in (None, "", "N/A") else None for v in vals],
                    type=pa.float64())
            except (TypeError, ValueError):
                pa_cols[f] = pa.array([str(v) if v is not None else "" for v in vals],
                                      type=pa.string())

    tbl = pa.table(pa_cols)
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"{endpoint}: DONE {n:,} rows")
    return n


def main():
    os.makedirs(OUT, exist_ok=True)
    only = set()
    for a in sys.argv[1:]:
        if a.startswith("--only"):
            only = set(a.split("=",1)[-1].split(",")) if "=" in a else set()
        elif not a.startswith("--"):
            only.add(a)

    total = 0

    if not only or "financials" in only:
        log("=== FDIC Financials (call reports) ===")
        total += ingest_financials()

    if not only or "institutions" in only:
        log("=== FDIC Institutions ===")
        total += ingest_wide("institutions", INST_FIELDS,
                             key_fields=["CERT"], date_field="REPDTE")

    if not only or "history" in only:
        log("=== FDIC History ===")
        total += ingest_wide("history", HIST_FIELDS,
                             key_fields=["CERT"], date_field="EFFDATE")

    if not only or "failures" in only:
        log("=== FDIC Failures ===")
        total += ingest_wide("failures", FAIL_FIELDS,
                             key_fields=["CERT"], date_field="FAILDATE")

    if not only or "summary" in only:
        log("=== FDIC Summary (industry aggregates) ===")
        total += ingest_wide("summary", SUMM_FIELDS,
                             key_fields=[], date_field="REPDTE")

    log(f"GRAND TOTAL: {total:,} FDIC observations")


if __name__ == "__main__":
    main()
