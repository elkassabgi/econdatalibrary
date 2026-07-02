#!/usr/bin/env python3
"""Recover the 7 OWID wide-format charts the main ingest left 'empty'.

These charts encode a matrix, not a tidy series: the `year` column actually holds
an ORDINAL period (month 1-12, or ISO week 1-53) and the VALUE columns are named
`_YYYY` (one column per calendar year). The tidy parser saw the `year` values
(1..12 / 1..53) fall outside the valid-year range and skipped every row.

We pivot them into the standard grouped schema (series_key, obs_date, value):
  * monthly-* charts:  obs_date = date(YYYY_from_column, month_from_year_col, 1)
  * *-by-week charts:  obs_date = ISO week date(YYYY_from_column, week_from_year_col, 1)
series_key = "<slug>|<entity_or_code>|<value_col_suffix>" so the calendar year and
the cumulative/area qualifier stay in the column name -> key.

Writes/overwrites data/clean_full/owid/<slug>.parquet (same dir, same schema as
the main ingest) and prints obs counts so coverage can be updated.
"""
import datetime as dt
import io
import os
import re
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
sys.path.insert(0, ROOT)
OUT = os.path.join(ROOT, "data", "clean_full", "owid")
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
SCHEMA = pa.schema([("series_key", pa.string()),
                    ("obs_date", pa.date32()),
                    ("value", pa.float64())])

YEAR_COL_RE = re.compile(r"^_(\d{4})(.*)$")

# slug -> ordinal kind ('month' or 'week')
TARGETS = {
    "monthly-average-surface-temperatures-by-year": "month",
    "monthly-average-surface-temperatures-by-decade": "month",
    "monthly-surface-temperature-anomalies-by-year": "month",
    "monthly-surface-temperature-anomalies-by-decade": "month",
    "cumulative-area-burnt-by-wildfires-by-week": "week",
    "cumulative-co-emissions-released-by-wildfires-by-week": "week",
    "weekly-cumulative-share-of-the-area-burnt-by-wildfires-each-year": "week",
}


def fetch(slug):
    url = f"https://ourworldindata.org/grapher/{slug}.csv?csvType=full&useColumnShortNames=true"
    for attempt in range(5):
        r = requests.get(url, headers=UA, timeout=120)
        if r.status_code == 200:
            return r.text
        if r.status_code in (403, 404):
            return None
        import time
        time.sleep(2 * (attempt + 1))
    return None


def ordinal_to_date(kind, year, ordinal):
    try:
        if kind == "month":
            if 1 <= ordinal <= 12:
                return dt.date(year, ordinal, 1)
        elif kind == "week":
            if 1 <= ordinal <= 53:
                try:
                    return dt.date.fromisocalendar(year, ordinal, 1)
                except ValueError:
                    return None
    except ValueError:
        return None
    return None


def recover(slug, kind):
    import csv
    txt = fetch(slug)
    if txt is None:
        print(f"  [{slug}] FETCH FAILED")
        return 0
    rdr = csv.reader(io.StringIO(txt))
    header = next(rdr)
    idx = {h: i for i, h in enumerate(header)}
    ei = idx.get("entity")
    ci = idx.get("code")
    yi = idx.get("year")  # ordinal month/week
    # value columns: those matching _YYYY...
    ycols = []
    for h in header:
        m = YEAR_COL_RE.match(h)
        if m:
            ycols.append((h, int(m.group(1)), m.group(2)))  # (colname, year, suffix)
    if yi is None or not ycols:
        print(f"  [{slug}] no ordinal/_YYYY structure")
        return 0

    keys, dates, vals = [], [], []
    for row in rdr:
        if len(row) < len(header):
            row = row + [""] * (len(header) - len(row))
        try:
            ordv = int(row[yi])
        except (ValueError, TypeError):
            continue
        code = (row[ci].strip() if ci is not None else "") or (row[ei].strip() if ei is not None else "")
        if not code:
            continue
        for colname, yr, suffix in ycols:
            cell = row[idx[colname]].strip()
            if not cell:
                continue
            try:
                v = float(cell)
            except ValueError:
                continue
            od = ordinal_to_date(kind, yr, ordv)
            if od is None:
                continue
            # key keeps the column suffix (e.g. cumulative/area qualifier)
            sk = f"{slug}|{code}{('|' + suffix) if suffix else ''}"
            keys.append(sk)
            dates.append(od)
            vals.append(v)

    if not keys:
        print(f"  [{slug}] still empty after pivot")
        return 0
    tbl = pa.table({"series_key": pa.array(keys, type=pa.string()),
                    "obs_date": pa.array(dates, type=pa.date32()),
                    "value": pa.array(vals, type=pa.float64())}, schema=SCHEMA)
    out_path = os.path.join(OUT, slug + ".parquet")
    tmp = out_path + f".{os.getpid()}.part"
    pq.write_table(tbl, tmp, compression="zstd")
    os.replace(tmp, out_path)
    n_series = len(set(keys))
    print(f"  [{slug:55}] obs={len(keys):>9,} series={n_series:>6,} "
          f"dates={min(dates)}..{max(dates)}")
    return len(keys)


def main():
    print(f"Recovering {len(TARGETS)} wide-format charts...")
    total = 0
    for slug, kind in TARGETS.items():
        total += recover(slug, kind)
    print(f"\nDONE: recovered {total:,} observations across {len(TARGETS)} charts")


if __name__ == "__main__":
    main()
