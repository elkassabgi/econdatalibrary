#!/usr/bin/env python3
"""CBOE Volatility Indexes — VIX, SKEW, VVIX, VXN, RVX, and more.

License: Free for informational use (CBOE terms)
Source: https://cdn.cboe.com/api/global/us_indices/daily_prices/
No API key required.

Coverage (daily, various start dates):
  * VIX  — S&P 500 Volatility Index, 1990–present
  * SKEW — S&P 500 Skew Index (tail risk), 1990–present
  * VVIX — Volatility of VIX, 2007–present
  * VXN  — Nasdaq 100 Volatility, 2001–present
  * RVX  — Russell 2000 Volatility, 2004–present
  * VXD  — DJIA Volatility, 1997–present
  * VIX9D  — 9-day VIX, 2011–present
  * VIX3M  — 3-month VIX, 2011–present
  * VIX6M  — 6-month VIX, 2011–present
  * VIX1Y  — 1-year VIX, 2011–present
  * GVZ  — Gold Volatility Index, 2008–present
  * OVX  — Crude Oil Volatility, 2007–present
  * EUVIX — Euro FX Volatility, 2012–present

Run: python jobs/ingest_cboe.py
"""
from __future__ import annotations
import csv, datetime as dt, io, os, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "cboe")
BASE = "https://cdn.cboe.com/api/global/us_indices/daily_prices"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

INDICES = [
    ("VIX_History.csv",    "VIX",    "CBOE VIX S&P 500 Volatility Index"),
    ("SKEW_History.csv",   "SKEW",   "CBOE SKEW Index (tail risk)"),
    ("VVIX_History.csv",   "VVIX",   "CBOE VVIX Volatility of VIX"),
    ("VXN_History.csv",    "VXN",    "CBOE Nasdaq 100 Volatility Index"),
    ("RVX_History.csv",    "RVX",    "CBOE Russell 2000 Volatility Index"),
    ("VXD_History.csv",    "VXD",    "CBOE DJIA Volatility Index"),
    ("VIX9D_History.csv",  "VIX9D",  "CBOE 9-Day VIX"),
    ("VIX3M_History.csv",  "VIX3M",  "CBOE 3-Month VIX"),
    ("VIX6M_History.csv",  "VIX6M",  "CBOE 6-Month VIX"),
    ("VIX1Y_History.csv",  "VIX1Y",  "CBOE 1-Year VIX"),
    ("GVZ_History.csv",    "GVZ",    "CBOE Gold Volatility Index"),
    ("OVX_History.csv",    "OVX",    "CBOE Crude Oil Volatility Index"),
    ("EUVIX_History.csv",  "EUVIX",  "CBOE Euro FX Volatility Index"),
    ("JYVIX_History.csv",  "JYVIX",  "CBOE JPY Volatility Index"),
    ("BPVIX_History.csv",  "BPVIX",  "CBOE British Pound Volatility Index"),
    ("VXAPL_History.csv",  "VXAPL",  "CBOE Apple Volatility"),
    ("VXGOG_History.csv",  "VXGOG",  "CBOE Google Volatility"),
    ("VXGS_History.csv",   "VXGS",   "CBOE Goldman Sachs Volatility"),
    ("VXIBM_History.csv",  "VXIBM",  "CBOE IBM Volatility"),
    ("VXAZN_History.csv",  "VXAZN",  "CBOE Amazon Volatility"),
]


def log(m):
    try:
        print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
    except UnicodeEncodeError:
        print(f"[{time.strftime('%H:%M:%S')}] {str(m).encode('ascii','replace').decode()}", flush=True)


def parse_cboe_csv(content: bytes, series_name: str) -> list[tuple[dt.date, str, float]]:
    """Parse CBOE index history CSV. Returns [(date, column_name, value)]."""
    results = []
    try:
        text = content.decode("utf-8-sig", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        headers = reader.fieldnames or []
        # Identify date column: "DATE" or "Trade Date"
        date_col = next((h for h in headers if h.strip().upper() in ("DATE", "TRADE DATE")), None)
        if date_col is None:
            log(f"  No date column in {series_name}, headers: {headers[:5]}")
            return results
        # Value columns: OPEN, HIGH, LOW, CLOSE (or just one for some indices)
        val_cols = [h for h in headers if h != date_col and h.strip()]

        for row in reader:
            date_str = row.get(date_col, "").strip()
            if not date_str:
                continue
            try:
                # Try MM/DD/YYYY, YYYY-MM-DD
                if "/" in date_str:
                    parts = date_str.split("/")
                    if len(parts) == 3:
                        m, d, y = int(parts[0]), int(parts[1]), int(parts[2])
                        obs_date = dt.date(y, m, d)
                    else:
                        continue
                else:
                    obs_date = dt.date.fromisoformat(date_str[:10])
            except (ValueError, TypeError):
                continue

            for col in val_cols:
                v_raw = row.get(col, "").strip()
                if not v_raw or v_raw.upper() in ("", "N/A", "NA"):
                    continue
                try:
                    v = float(v_raw.replace(",", ""))
                    if v != v:
                        continue
                except (ValueError, TypeError):
                    continue
                col_norm = col.strip().upper().replace(" ", "_")
                key = f"{series_name}_{col_norm}" if len(val_cols) > 1 else series_name
                results.append((obs_date, key, v))
    except Exception as e:
        log(f"  Parse error ({series_name}): {e}")
    return results


def main():
    os.makedirs(OUT, exist_ok=True)
    out_path = os.path.join(OUT, "cboe.parquet")

    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"Already done: {n:,} rows"); return

    all_keys, all_dates, all_vals = [], [], []
    seen: set[tuple] = set()

    for filename, series_name, desc in INDICES:
        url = f"{BASE}/{filename}"
        try:
            r = requests.get(url, headers=UA, timeout=60)
            if r.status_code != 200:
                log(f"  HTTP {r.status_code}: {series_name}")
                continue
            if len(r.content) < 100:
                continue
            results = parse_cboe_csv(r.content, series_name)
            n = 0
            for obs_date, key, v in results:
                tok = (key, obs_date)
                if tok not in seen:
                    seen.add(tok)
                    all_keys.append(key)
                    all_dates.append(obs_date)
                    all_vals.append(v)
                    n += 1
            log(f"  {series_name}: {n:,} obs")
        except Exception as e:
            log(f"  ERR {series_name}: {e}")
        time.sleep(0.3)

    if not all_vals:
        log("0 observations"); return

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"DONE: {n:,} CBOE volatility index observations ({len(set(all_keys))} series)")


if __name__ == "__main__":
    main()
