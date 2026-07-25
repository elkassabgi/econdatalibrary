#!/usr/bin/env python3
"""Grouped ingest of SEC EDGAR companyfacts.

ONE Parquet per company (all its XBRL metrics inside: metric = taxonomy:tag:unit,
obs_date, value, vintage_date=the SEC 'filed' date for point-in-time) + ONE coarse
catalog row per company. us-public-domain. Filing-document POINTERS stay on sec.gov.

Run: python jobs/ingest_sec_edgar.py
"""
import datetime as dt
import json
import os
import sys
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
sys.path.insert(0, ROOT)
from core import catalog  # noqa: E402
from connectors.base import SeriesMeta  # noqa: E402

RAW = os.path.join(ROOT, "data", "raw", "sec_edgar")
OUT = os.path.join(ROOT, "data", "clean_grouped", "sec_edgar")
os.makedirs(OUT, exist_ok=True)


def d(s):
    try:
        return dt.datetime.strptime(s, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


ct = json.load(open(os.path.join(RAW, "company_tickers.json")))
cik2tick = {}
for v in ct.values():
    cik2tick.setdefault(int(v["cik_str"]), v["ticker"])

db = catalog.connect()
fts = catalog.init(db)
catalog.upsert_source(db, "sec_edgar", "U.S. SEC EDGAR (company financials)", "us-public-domain",
                      "Source: U.S. SEC EDGAR (public domain)", "https://www.sec.gov/edgar")

z = zipfile.ZipFile(os.path.join(RAW, "companyfacts.zip"))
n_co = n_obs = 0
for name in z.namelist():
    if not name.endswith(".json"):
        continue
    try:
        data = json.loads(z.read(name))
    except Exception:
        continue
    ent = data.get("entityName") or name
    try:
        cikn = int(data.get("cik"))
    except (TypeError, ValueError):
        digits = name[3:-5]
        cikn = int(digits) if digits.isdigit() else None
    tick = cik2tick.get(cikn)
    ident = tick or (f"CIK{cikn:010d}" if cikn else name[:-5])
    metric, odate, vals, vint = [], [], [], []
    for tax, tags in data.get("facts", {}).items():
        for tag, body in tags.items():
            for unit, points in body.get("units", {}).items():
                sk = f"{tax}:{tag}:{unit}"
                for p in points:
                    end = d(p.get("end", ""))
                    val = p.get("val")
                    if end is None or val is None:
                        continue
                    try:
                        fv = float(val)
                    except (ValueError, TypeError):
                        continue
                    metric.append(sk)
                    odate.append(end)
                    vals.append(fv)
                    vint.append(d(p.get("filed", "")))
    if not metric:
        continue
    tbl = pa.table({
        "metric": metric,
        "obs_date": pa.array(odate, type=pa.date32()),
        "value": vals,
        "vintage_date": pa.array(vint, type=pa.date32()),
    })
    pq.write_table(tbl, os.path.join(OUT, ident.replace("/", "_").replace(":", "_") + ".parquet"))
    meta = SeriesMeta(f"sec_edgar:{ident}", ent, "Q", None, "US", "fundamentals", "us-public-domain",
                      {"cik": cikn, "ticker": tick, "n_metrics": len(set(metric)), "n_obs": len(metric)})
    catalog.upsert_series(db, meta, start=str(min(odate)), end=str(max(odate)))
    n_co += 1
    n_obs += len(metric)
    if n_co % 1000 == 0:
        db.commit()
        print(f"  {n_co} companies, {n_obs:,} obs", flush=True)

db.commit()
catalog.rebuild_fts(db, fts)
print(f"DONE: {n_co:,} companies (grouped Parquet) / {n_obs:,} observations")
