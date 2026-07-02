#!/usr/bin/env python3
"""World Bank Global Findex Database (financial inclusion) — full ingest.

The 300th source. WB Open Data source id 28: account ownership, savings,
borrowing, digital payments, etc., by country and demographic breakdown,
2011/2014/2017/2021/2024. License: CC BY 4.0 (World Bank Open Data).

Long format {series_key, obs_date, value}:
  series_key = FINDEX:<indicator_code>:<ISO3>
  obs_date   = <year>-12-31 (annual)
Full coverage: ALL indicators in source 28 (no sampling). Resumable.

Run: python jobs/ingest_global_findex.py
"""
from __future__ import annotations
import datetime as dt, json, os, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
OUT = os.path.join(ROOT, "data", "clean_full", "global_findex")
SRC = 28
API = "https://api.worldbank.org/v2"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
RATE = 0.05


def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def get(url, retries=5):
    for a in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=60)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 404):
                return None
        except Exception as e:
            log(f"  ERR {e} (try {a+1})")
        time.sleep(3 * (a + 1))
    return None


def list_indicators() -> list[str]:
    ids, page, pages = [], 1, 1
    while page <= pages:
        j = get(f"{API}/indicator?source={SRC}&format=json&per_page=1000&page={page}")
        if not isinstance(j, list) or len(j) < 2:
            break
        pages = j[0].get("pages", 1)
        ids.extend(i["id"] for i in (j[1] or []))
        page += 1
    return ids


def fetch_indicator(code: str) -> list[tuple]:
    rows = []
    j = get(f"{API}/country/all/indicator/{code}?source={SRC}&format=json&per_page=20000")
    if not isinstance(j, list) or len(j) < 2 or not j[1]:
        return rows
    for d in j[1]:
        v = d.get("value")
        if v is None:
            continue
        iso = d.get("countryiso3code") or (d.get("country") or {}).get("id")
        yr = d.get("date")
        if not iso or not yr:
            continue
        try:
            y = int(yr)
        except (TypeError, ValueError):
            continue
        rows.append((f"FINDEX:{code}:{iso}", dt.date(y, 12, 31), float(v)))
    return rows


def main():
    os.makedirs(OUT, exist_ok=True)
    final = os.path.join(OUT, "global_findex.parquet")
    if os.path.exists(final):
        n = pq.read_metadata(final).num_rows
        log(f"already built: {n:,} rows — delete to rebuild"); return

    inds = list_indicators()
    log(f"Global Findex (source {SRC}): {len(inds)} indicators")
    keys, dates, vals = [], [], []
    seen = set()
    for i, code in enumerate(inds):
        rows = fetch_indicator(code)
        n = 0
        for k, d, v in rows:
            tok = (k, d)
            if tok in seen:
                continue
            seen.add(tok); keys.append(k); dates.append(d); vals.append(v); n += 1
        if (i + 1) % 200 == 0 or n > 5000:
            log(f"  [{i+1}/{len(inds)}] {code}: +{n} (total {len(keys):,})")
        time.sleep(RATE)

    if not keys:
        log("FATAL: 0 rows fetched — not writing"); return
    tbl = pa.table({"series_key": pa.array(keys, pa.string()),
                    "obs_date": pa.array(dates, pa.date32()),
                    "value": pa.array(vals, pa.float64())})
    tmp = final + ".tmp"
    pq.write_table(tbl, tmp, compression="zstd"); os.replace(tmp, final)
    log(f"DONE: {tbl.num_rows:,} obs from {len(inds)} indicators -> {final}")


if __name__ == "__main__":
    main()
