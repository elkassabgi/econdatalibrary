#!/usr/bin/env python3
"""BCRP (Banco Central de Reserva del Perú) — Peruvian exchange rate data.

License: Open data (public domain)
Source: https://estadisticas.bcrp.gob.pe/
No API key required.

Coverage:
  * Daily exchange rates: USD/PEN, EUR/PEN, GBP/PEN, JPY/PEN (buying, selling, spot)

NOTE: Only daily PD* series work via the public JSON API.
Monthly/quarterly/annual PN* series return empty responses.

API: https://estadisticas.bcrp.gob.pe/estadisticas/series/api/{code}/json/{start}/{end}
  * Single series only — batched requests fail
  * Daily period format: YYYY-M-D
  * Response period names: "02.Ene.97" (DD.MonthSP.YY)

Run: python jobs/ingest_bcrp.py
"""
from __future__ import annotations
import datetime as dt, os, re, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "bcrp")
BASE = "https://estadisticas.bcrp.gob.pe/estadisticas/series/api"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
        "Accept": "application/json"}
RATE = 0.5

# Daily exchange rate series — confirmed working via API
SERIES = [
    ("PD04638PD", "USDPEN_mid"),    # USD/PEN interbank mid
    ("PD04639PD", "USDPEN_buy"),    # USD/PEN buying
    ("PD04640PD", "USDPEN_sell"),   # USD/PEN selling
    ("PD04628PD", "EURPEN"),        # EUR/PEN
    ("PD04635PD", "GBPPEN"),        # GBP/PEN
    ("PD04629PD", "JPYPEN"),        # JPY/PEN
]

LOG_FILE = os.path.join(OUT, "_bcrp_log.txt")

MONTH_ES = {
    "ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
    "jul": 7, "ago": 8, "sep": 9, "oct": 10, "nov": 11, "dic": 12,
}


def log(m):
    msg = f"[{time.strftime('%H:%M:%S')}] {m}"
    try:
        print(msg, flush=True)
    except UnicodeEncodeError:
        print(msg.encode("ascii", "replace").decode(), flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(msg + "\n")
    except Exception:
        pass


def parse_bcrp_period(s: str) -> dt.date | None:
    """Parse BCRP daily period strings like '02.Ene.97' or '15.Mar.2023'."""
    s = (s or "").strip()
    try:
        # Daily: "02.Ene.97" or "02.Ene.2020" (DD.MonthSP.YY or DD.MonthSP.YYYY)
        m = re.match(r"(\d{1,2})\.([A-Za-z]{3})\.(\d{2,4})$", s)
        if m:
            day = int(m.group(1))
            mon = MONTH_ES.get(m.group(2).lower())
            yr_raw = int(m.group(3))
            yr = yr_raw + (1900 if yr_raw >= 50 else 2000) if yr_raw < 100 else yr_raw
            if mon:
                return dt.date(yr, mon, day)
        # Fallback: ISO date
        if re.match(r"\d{4}-\d{2}-\d{2}", s):
            return dt.date.fromisoformat(s[:10])
    except (ValueError, TypeError):
        pass
    return None


def fetch_series(code: str, label: str) -> list[tuple[str, dt.date, float]]:
    """Fetch a single daily series. Returns list of (series_key, date, value)."""
    start = "1996-1-2"
    end = dt.date.today().strftime("%Y-%m-%d")
    url = f"{BASE}/{code}/json/{start}/{end}"

    for attempt in range(3):
        try:
            r = requests.get(url, headers=UA, timeout=120)
            if r.status_code != 200:
                log(f"  HTTP {r.status_code} for {code}")
                return []
            d = r.json()
            series_meta = d.get("config", {}).get("series", [])
            periods = d.get("periods", [])
            if not periods:
                log(f"  {code}: empty periods")
                return []

            results = []
            for period in periods:
                p_name = period.get("name", "")
                values = period.get("values", [])
                if not values:
                    continue
                v_raw = values[0]
                if v_raw is None or str(v_raw).strip() in ("", "n.d.", "null", "None"):
                    continue
                try:
                    v = float(str(v_raw).replace(",", "."))
                except (ValueError, TypeError):
                    continue
                obs_date = parse_bcrp_period(p_name)
                if obs_date:
                    results.append((f"BCRP:{label}", obs_date, v))
            return results
        except Exception as e:
            log(f"  ERR {code} attempt {attempt+1}: {e}")
            time.sleep(5 * (attempt + 1))
    return []


def main():
    os.makedirs(OUT, exist_ok=True)
    out_path = os.path.join(OUT, "bcrp.parquet")

    done: set[str] = set()
    all_keys: list[str] = []
    all_dates: list[dt.date] = []
    all_vals: list[float] = []

    if os.path.exists(out_path):
        tbl = pq.read_table(out_path)
        done = set(tbl.column("series_key").to_pylist())
        all_keys  = tbl.column("series_key").to_pylist()
        all_dates = [d.as_py() for d in tbl.column("obs_date")]
        all_vals  = tbl.column("value").to_pylist()
        log(f"Resuming: {len(done)} series done, {len(all_vals):,} obs")

    todo = [(c, l) for c, l in SERIES if f"BCRP:{l}" not in done]
    log(f"BCRP: {len(todo)} series to fetch")

    for code, label in todo:
        rows = fetch_series(code, label)
        if rows:
            for k, d, v in rows:
                all_keys.append(k)
                all_dates.append(d)
                all_vals.append(v)
            log(f"  {label} ({code}): {len(rows):,} obs")
        else:
            log(f"  {label} ({code}): 0 obs")
        time.sleep(RATE)

    if not all_vals:
        log("0 observations"); return

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"DONE: {n:,} BCRP Peru daily observations")


if __name__ == "__main__":
    main()
