#!/usr/bin/env python3
"""Reserve Bank of Australia (RBA) statistical tables ingest.

License: RBA copyright — free for non-commercial research use with attribution
Source: https://www.rba.gov.au/statistics/tables/

Strategy:
  * Scrape the statistics index page for all CSV download links (216 files)
  * Parse each CSV: metadata rows at top, Series ID row, then date+value rows
  * One Parquet for the full dataset; fully resumable

RBA CSV format:
  Row 0:  Table title
  Row 1:  Column labels (Title, ...)
  Row 2:  Description
  Row 3:  Frequency
  Row 4:  Type
  Row 5:  Units
  Row 6:  empty
  Row 7:  Source
  Row 8:  Publication date
  Row 9:  Series ID (ARBALNOIW, ARBALESBW, ...)
  Row 10+: Date (DD-Mon-YYYY), value1, value2, ...

Run: python jobs/ingest_rba.py
"""
from __future__ import annotations
import csv, datetime as dt, io, os, re, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "rba")
BASE = "https://www.rba.gov.au"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
        "Accept": "text/html,text/csv,application/csv,*/*"}
RATE = 0.5
import sys as _sys
_enc = _sys.stdout.encoding or "utf-8"


def log(m):
    try:
        print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
    except UnicodeEncodeError:
        print(f"[{time.strftime('%H:%M:%S')}] {m}".encode(_enc, errors='replace').decode(_enc), flush=True)


def get_bytes(url: str, retries: int = 4) -> bytes | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=120)
            if r.status_code == 200:
                return r.content
            if r.status_code in (400, 404):
                return None
            if r.status_code == 429:
                time.sleep(60); continue
            log(f"  HTTP {r.status_code} attempt {attempt+1}: {url[-80:]}")
        except Exception as e:
            log(f"  ERR attempt {attempt+1}: {e}")
        time.sleep(5 * (attempt + 1))
    return None


def parse_rba_date(s: str) -> dt.date | None:
    """Parse RBA date formats: DD-Mon-YYYY, DD/MM/YYYY, YYYY-MM-DD, YYYY."""
    s = (s or "").strip()
    try:
        if len(s) == 11 and s[2] == "-":  # DD-Mon-YYYY
            return dt.datetime.strptime(s, "%d-%b-%Y").date()
        if len(s) == 10 and s[2] == "/" and s[5] == "/":  # DD/MM/YYYY
            return dt.datetime.strptime(s, "%d/%m/%Y").date()
        if len(s) == 10 and s[4] == "-":  # YYYY-MM-DD
            return dt.date.fromisoformat(s)
        if len(s) == 4 and s.isdigit():   # YYYY
            return dt.date(int(s), 12, 31)
        if len(s) == 7 and s[4] in ("-", "/"):  # YYYY-MM or YYYY/MM
            yr, mo = int(s[:4]), int(s[5:7])
            return dt.date(yr, mo, 1)
    except Exception:
        pass
    return None


def get_csv_links() -> list[str]:
    """Scrape RBA statistics tables page for all CSV links."""
    content = get_bytes(f"{BASE}/statistics/tables/")
    if not content:
        return []
    html = content.decode("utf-8", errors="replace")
    links = re.findall(r'href="(/statistics/tables/csv/[^"]+\.csv)"', html)
    return sorted(set(links))


def parse_rba_csv(content: bytes, csv_path: str) -> tuple[list, list, list]:
    """Parse RBA CSV file into (series_keys, dates, values) arrays."""
    all_keys, all_dates, all_vals = [], [], []
    try:
        text = content.decode("utf-8-sig", errors="replace")
        lines = text.splitlines()
        if len(lines) < 11:
            return [], [], []

        # Find the "Series ID" row (usually row 9 but can vary)
        series_id_row = None
        data_start = None
        for idx, line in enumerate(lines):
            if line.startswith("Series ID,") or line.lower().startswith("series id,"):
                series_id_row = idx
                data_start = idx + 1
                break

        if series_id_row is None:
            # Fallback: try to detect by checking if first column looks like a date
            for idx in range(5, min(20, len(lines))):
                parts = lines[idx].split(",")
                if parts and parse_rba_date(parts[0].strip()):
                    # The row before is likely headers/series IDs
                    series_id_row = idx - 1
                    data_start = idx
                    break

        if series_id_row is None or data_start is None:
            return [], [], []

        # Parse series IDs (skip first col which is "Series ID" label)
        reader = csv.reader(io.StringIO("\n".join(lines)))
        rows = list(reader)
        series_ids = rows[series_id_row][1:]  # skip first col

        # Derive a file prefix for unnamed series
        file_prefix = os.path.basename(csv_path).replace(".csv", "")

        # Parse data rows
        for row in rows[data_start:]:
            if not row:
                continue
            date_str = row[0].strip()
            if not date_str:
                continue
            d = parse_rba_date(date_str)
            if d is None:
                continue
            for col_idx, val_str in enumerate(row[1:]):
                val_str = val_str.strip()
                if not val_str or val_str in ("", "..", "N/A", "n/a", "na", "—"):
                    continue
                try:
                    v = float(val_str.replace(",", ""))
                except ValueError:
                    continue
                sid = (series_ids[col_idx].strip()
                       if col_idx < len(series_ids) and series_ids[col_idx].strip()
                       else f"{file_prefix}:col{col_idx+1}")
                all_keys.append(sid)
                all_dates.append(d)
                all_vals.append(v)

    except Exception as e:
        log(f"  parse error ({csv_path}): {e}")
    return all_keys, all_dates, all_vals


def main():
    os.makedirs(OUT, exist_ok=True)
    out_path = os.path.join(OUT, "rba.parquet")

    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"Already done: {n:,} rows in {out_path}"); return

    log("Fetching RBA statistics page for CSV links...")
    links = get_csv_links()
    log(f"Found {len(links)} CSV files")

    all_keys, all_dates, all_vals = [], [], []
    for i, path in enumerate(links, 1):
        url = BASE + path
        log(f"[{i}/{len(links)}] {path}")
        content = get_bytes(url)
        if not content:
            continue
        k, d, v = parse_rba_csv(content, path)
        all_keys.extend(k)
        all_dates.extend(d)
        all_vals.extend(v)
        log(f"  → {len(v):,} obs (total {len(all_vals):,})")
        time.sleep(RATE)

    if not all_vals:
        log("0 observations collected"); return

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"DONE: {n:,} total RBA observations → {out_path}")


if __name__ == "__main__":
    main()
