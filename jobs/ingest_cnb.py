#!/usr/bin/env python3
"""Czech National Bank (CNB) — Exchange rate fixing, 1991–present.

License: Open data (CNB public data policy)
Source: https://www.cnb.cz/en/financial_markets/foreign_exchange_market/
No API key required.

Coverage:
  * ~30 currencies vs CZK, daily business day rates, 1991–present
  * AUD, BGN, BRL, CAD, CHF, CNY, DKK, EUR, GBP, HKD, HUF, IDR, ILS,
    INR, ISK, JPY, KRW, MXN, MYR, NOK, NZD, PHP, PLN, RON, SEK, SGD,
    THB, TRY, USD, XDR, ZAR
  * Year files: pipe-delimited, daily data per calendar year

Run: python jobs/ingest_cnb.py
"""
from __future__ import annotations
import datetime as dt, io, os, re, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "cnb")
BASE = "https://www.cnb.cz/en/financial_markets/foreign_exchange_market/exchange_rate_fixing/year.txt"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
START_YEAR = 1991  # CNB fixing data from 1991


def log(m):
    try:
        print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
    except UnicodeEncodeError:
        print(f"[{time.strftime('%H:%M:%S')}] {str(m).encode('ascii','replace').decode()}", flush=True)


def parse_cnb_year(text: str, year: int) -> list[tuple[str, dt.date, float]]:
    """Parse CNB year text file.

    Format: Date|1 AUD|1 BGN|...|1 ZAR
    Date format: DD.MM.YYYY
    Values: decimal with '.' separator
    """
    results = []
    try:
        lines = text.strip().split("\n")
        if not lines:
            return results

        # Parse header: "Date|1 AUD|1 BGN|100 HUF|..."
        header = lines[0].strip().split("|")
        if not header or header[0].lower() not in ("date", "datum"):
            return results

        # Build column mapping: col_idx → (currency_code, amount_multiplier)
        col_map: dict[int, tuple[str, float]] = {}
        for j, h in enumerate(header[1:], start=1):
            h = h.strip()
            m = re.match(r"^(\d+)\s+([A-Z]+)$", h)
            if m:
                amount = float(m.group(1))
                code = m.group(2)
                col_map[j] = (code, amount)

        if not col_map:
            return results

        for line in lines[1:]:
            line = line.strip()
            if not line:
                continue
            parts = line.split("|")
            if len(parts) < 2:
                continue

            # Parse date: DD.MM.YYYY
            date_str = parts[0].strip()
            try:
                if "." in date_str:
                    d_parts = date_str.split(".")
                    if len(d_parts) == 3:
                        day, mon, yr = int(d_parts[0]), int(d_parts[1]), int(d_parts[2])
                        obs_date = dt.date(yr, mon, day)
                    else:
                        continue
                else:
                    obs_date = dt.date.fromisoformat(date_str[:10])
            except (ValueError, TypeError):
                continue

            for col_idx, (currency, amount) in col_map.items():
                if col_idx >= len(parts):
                    continue
                v_str = parts[col_idx].strip().replace(",", ".")
                if not v_str or v_str in ("", "N/A", "-"):
                    continue
                try:
                    v_raw = float(v_str)
                    if v_raw != v_raw:
                        continue
                    # Normalize: rate is per `amount` units → convert to per-1-unit
                    v = v_raw / amount
                    results.append((f"CNB_FX:{currency}_CZK", obs_date, v))
                except (ValueError, TypeError):
                    continue

    except Exception as e:
        log(f"  Parse error year {year}: {e}")
    return results


def main():
    os.makedirs(OUT, exist_ok=True)
    out_path = os.path.join(OUT, "cnb.parquet")
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"Already done: {n:,} rows"); return

    today = dt.date.today()
    all_keys, all_dates, all_vals = [], [], []
    seen: set[tuple] = set()
    n_years = 0

    for year in range(START_YEAR, today.year + 1):
        url = f"{BASE}?year={year}"
        try:
            r = requests.get(url, headers=UA, timeout=60)
            if r.status_code == 200 and len(r.content) > 100:
                text = r.content.decode("utf-8", errors="replace")
                rows = parse_cnb_year(text, year)
                n = 0
                for key, d, v in rows:
                    tok = (key, d)
                    if tok not in seen:
                        seen.add(tok)
                        all_keys.append(key)
                        all_dates.append(d)
                        all_vals.append(v)
                        n += 1
                log(f"  {year}: {n:,} obs")
                n_years += 1
            else:
                log(f"  {year}: HTTP {r.status_code}")
        except Exception as e:
            log(f"  {year}: ERR {e}")
        time.sleep(0.2)

    if not all_vals:
        log("0 observations"); return

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"DONE: {n:,} CNB Czech exchange rate observations ({n_years} years, {len(set(all_keys))} currencies)")


if __name__ == "__main__":
    main()
