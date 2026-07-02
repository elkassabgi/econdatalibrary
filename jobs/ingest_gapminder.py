#!/usr/bin/env python3
"""Gapminder open data — systema globalis (615 indicators × 200+ countries).

License: CC BY 4.0
Source: https://github.com/open-numbers/ddf--gapminder--systema_globalis
No API key required (public GitHub).

Strategy:
  * List all datapoint CSV files in the repo via GitHub API
  * Download each: geo, time, {indicator} columns
  * Long-format Parquet: series_key = {indicator}:{geo_code}

Run: python jobs/ingest_gapminder.py
"""
from __future__ import annotations
import csv, datetime as dt, io, os, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT  = r"D:/research/econfindatalibrary"
OUT   = os.path.join(ROOT, "data", "clean_full", "gapminder")
REPO  = "open-numbers/ddf--gapminder--systema_globalis"
RAWBASE = f"https://raw.githubusercontent.com/{REPO}/master"
UA    = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
         "Accept": "application/json"}
RATE  = 0.2   # GitHub has generous rate limits (60 req/min for raw)


def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def get_json(url: str, retries: int = 4) -> dict | list | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=60)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 403:
                log(f"  GitHub 403 (rate limited?), sleeping 60s")
                time.sleep(60); continue
            if r.status_code in (400, 404):
                return None
            log(f"  HTTP {r.status_code} attempt {attempt+1}")
        except Exception as e:
            log(f"  ERR attempt {attempt+1}: {e}")
        time.sleep(5 * (attempt + 1))
    return None


def get_bytes(url: str, retries: int = 4) -> bytes | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers={**UA, "Accept": "text/plain"}, timeout=120)
            if r.status_code == 200:
                return r.content
            if r.status_code == 403:
                time.sleep(60); continue
            if r.status_code in (400, 404):
                return None
        except Exception as e:
            log(f"  ERR attempt {attempt+1}: {e}")
        time.sleep(5 * (attempt + 1))
    return None


def get_file_list() -> list[str]:
    """Get all datapoint CSV paths from the GitHub tree API."""
    url = f"https://api.github.com/repos/{REPO}/git/trees/master?recursive=0"
    data = get_json(url)
    if not data or "tree" not in data:
        return []
    paths = [x["path"] for x in data["tree"]
             if x["path"].startswith("countries-etc-datapoints/") and x["path"].endswith(".csv")]
    return sorted(paths)


def parse_ddf_csv(content: bytes, path: str) -> tuple[list, list, list]:
    """Parse DDF datapoints CSV: geo, time, indicator_value columns."""
    all_keys, all_dates, all_vals = [], [], []
    try:
        # Extract indicator name from filename
        # ddf--datapoints--{indicator}--by--geo--time.csv
        filename = os.path.basename(path)
        parts = filename.replace(".csv", "").split("--")
        # Find the indicator part (between 'datapoints' and 'by')
        try:
            dp_idx = parts.index("datapoints")
            by_idx = parts.index("by")
            indicator = "_".join(parts[dp_idx+1:by_idx])
        except (ValueError, IndexError):
            indicator = filename.replace(".csv", "")

        text = content.decode("utf-8-sig", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        if not reader.fieldnames:
            return [], [], []

        cols = [c.lower() for c in reader.fieldnames]
        # Find geo and time columns
        geo_col  = next((reader.fieldnames[i] for i, c in enumerate(cols)
                         if c in ("geo", "country", "iso3", "iso")), None)
        time_col = next((reader.fieldnames[i] for i, c in enumerate(cols)
                         if c in ("time", "year", "date")), None)
        # Value column: whatever is left
        val_col  = next((reader.fieldnames[i] for i, c in enumerate(cols)
                         if c not in (
                             (geo_col or "").lower(), (time_col or "").lower()
                         ) and c not in ("", "nan")), None)

        if not time_col or not val_col:
            return [], [], []

        for row in reader:
            t_raw = row.get(time_col, "").strip()
            v_raw = row.get(val_col, "").strip()
            if not t_raw or not v_raw or v_raw.lower() in ("", "nan", "na", "null"):
                continue
            try:
                yr = int(t_raw[:4])
                d  = dt.date(yr, 12, 31)
                v  = float(v_raw)
            except (ValueError, TypeError):
                continue
            geo = row.get(geo_col, "").strip() if geo_col else ""
            key = f"{indicator}:{geo}" if geo else indicator
            all_keys.append(key)
            all_dates.append(d)
            all_vals.append(v)

    except Exception as e:
        log(f"  parse error ({path}): {e}")
    return all_keys, all_dates, all_vals


def main():
    os.makedirs(OUT, exist_ok=True)
    out_path = os.path.join(OUT, "gapminder.parquet")
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"Already done: {n:,} rows"); return

    log("Fetching Gapminder file list from GitHub...")
    files = get_file_list()
    log(f"Found {len(files)} datapoint CSV files")

    all_keys, all_dates, all_vals = [], [], []
    FLUSH_EVERY = 100_000  # flush to parquet incrementally

    for i, path in enumerate(files, 1):
        url = f"{RAWBASE}/{path}"
        content = get_bytes(url)
        if not content:
            time.sleep(RATE); continue

        k, d, v = parse_ddf_csv(content, path)
        all_keys.extend(k)
        all_dates.extend(d)
        all_vals.extend(v)

        if i % 50 == 0:
            log(f"[{i}/{len(files)}] {os.path.basename(path)}: {len(v):,} obs, total {len(all_vals):,}")

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
    log(f"DONE: {n:,} Gapminder observations → {out_path}")


if __name__ == "__main__":
    main()
