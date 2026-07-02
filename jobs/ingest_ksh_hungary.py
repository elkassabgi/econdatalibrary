#!/usr/bin/env python3
"""KSH Hungary (Központi Statisztikai Hivatal) — STADAT CSV tables.

License: Open data (free to use)
Source: https://www.ksh.hu/stadat
Format: CSV files at https://www.ksh.hu/stadat_files/{theme}/en/{theme}{NNNN}.csv

Coverage: ~1,600+ annual statistical tables across 27 themes including:
  * nep — Population
  * mun — Labour Market
  * gdp — National Accounts / GDP
  * ene — Energy
  * mez — Agriculture
  * jov — Household income
  * tur — Tourism
  * ... 27 themes total

Run: python jobs/ingest_ksh_hungary.py
"""
from __future__ import annotations
import datetime as dt, os, re, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "ksh")
BASE = "https://www.ksh.hu/stadat_files"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
RATE = 0.3

THEMES = [
    "ara", "bel", "ber", "ege", "ele", "ene", "epi", "fol", "gdp", "gsz",
    "ido", "iga", "ikt", "ipa", "jov", "kkr", "kor", "ksp", "lak", "mez",
    "mun", "nep", "okt", "sza", "szo", "tte", "tur",
]

LOG_FILE = os.path.join(OUT, "_ksh_log.txt")


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


def get_csv(url: str, retries: int = 3) -> str | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=30)
            if r.status_code == 200:
                return r.text
            if r.status_code == 404:
                return None
            if r.status_code == 429:
                time.sleep(30); continue
        except Exception as e:
            pass
        time.sleep(3 * (attempt + 1))
    return None


def parse_number(s: str) -> float | None:
    """Parse Hungarian-style numbers: '4 560 875' or '31,0' or '1.5'."""
    s = s.strip()
    if not s or s in ("..", ":", "N/A", "n/a", "", "-", "–"):
        return None
    # Remove space thousands separator
    s = s.replace(" ", "").replace("\xa0", "")
    # Hungarian decimal: comma → dot
    s = s.replace(",", ".")
    # Remove trailing dots used as "unavailable"
    if s == ".":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def parse_ksh_csv(csv_text: str, theme: str, table_num: int) -> list[tuple[str, dt.date, float]]:
    """Parse a KSH STADAT CSV into (series_key, obs_date, value) triples."""
    results = []
    lines = csv_text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    if len(lines) < 3:
        return results

    # Line 0: table title (may span multiple cols with ; separators)
    title = lines[0].split(";")[0].strip()

    # Line 1: header row — first col is "Denomination", rest are years/periods
    header_parts = lines[1].split(";")
    time_labels = [h.strip() for h in header_parts[1:]]

    # Parse years from time labels (annual only: "2022", "2022*", etc.)
    year_dates: list[dt.date | None] = []
    for lbl in time_labels:
        clean = lbl.strip().rstrip("*").strip()
        if re.match(r"^\d{4}$", clean):
            year_dates.append(dt.date(int(clean), 12, 31))
        else:
            year_dates.append(None)

    # Rows 2+: data rows
    # Section headers have no values (all empty after first col)
    # Data rows have the series name and then values
    current_section = ""
    prefix = f"KSH:{theme}{table_num:04d}"

    for line in lines[2:]:
        if not line.strip():
            continue
        parts = line.split(";")
        row_label = parts[0].strip()
        if not row_label:
            continue

        values_raw = parts[1:] if len(parts) > 1 else []

        # Check if this is a section header (all value cols empty)
        has_values = any(p.strip() and p.strip() not in ("..", ":") for p in values_raw)

        if not has_values and values_raw:
            # This is a section header
            current_section = row_label
            continue

        # Build series key
        if current_section:
            key = f"{prefix}:{current_section}:{row_label}"
        else:
            key = f"{prefix}:{row_label}"

        # Extract values aligned with year_dates
        for i, (raw_v, obs_date) in enumerate(zip(values_raw, year_dates)):
            if obs_date is None:
                continue
            v = parse_number(raw_v)
            if v is None:
                continue
            results.append((key, obs_date, v))

    return results


def main():
    os.makedirs(OUT, exist_ok=True)

    total_obs = 0
    for theme in THEMES:
        out_path = os.path.join(OUT, f"{theme}.parquet")
        if os.path.exists(out_path):
            n = pq.read_metadata(out_path).num_rows
            log(f"Skip {theme}: {n:,} rows already saved")
            total_obs += n
            continue

        log(f"Theme: {theme}")
        all_keys, all_dates, all_vals = [], [], []
        seen: set[tuple] = set()
        n_tables = 0

        for table_num in range(1, 500):
            url = f"{BASE}/{theme}/en/{theme}{table_num:04d}.csv"
            csv_text = get_csv(url)
            if csv_text is None:
                # 404 = no more tables for this theme
                break

            rows = parse_ksh_csv(csv_text, theme, table_num)
            n = 0
            for key, d, v in rows:
                tok = (key, d)
                if tok not in seen:
                    seen.add(tok)
                    all_keys.append(key)
                    all_dates.append(d)
                    all_vals.append(v)
                    n += 1

            if n > 0:
                log(f"  [{table_num}] {theme}{table_num:04d}: {n:,} obs")
            n_tables += 1
            time.sleep(RATE)

        if all_vals:
            tbl = pa.table({
                "series_key": pa.array(all_keys,  pa.string()),
                "obs_date":   pa.array(all_dates, pa.date32()),
                "value":      pa.array(all_vals,  pa.float64()),
            })
            pq.write_table(tbl, out_path, compression="zstd")
            n_saved = pq.read_metadata(out_path).num_rows
            log(f"  {theme}: {n_tables} tables → {n_saved:,} obs saved")
            total_obs += n_saved
        else:
            log(f"  {theme}: {n_tables} tables → 0 obs")

    log(f"DONE: {total_obs:,} total KSH Hungary observations")


if __name__ == "__main__":
    main()
