#!/usr/bin/env python3
"""Verify integrity, completeness, and accuracy of downloaded data.

Checks:
  1. Exact byte-size match  -> proves the archives are not truncated
  2. Archive integrity      -> central directory intact + CRC-check a random sample
  3. Coverage               -> member counts vs the ticker universe
  4. Content + recency       -> parse several well-known companies, show latest filing dates
  5. World Bank accuracy     -> stored Parquet value vs the LIVE World Bank API

Run: python jobs/verify_data.py
"""
import json
import os
import random
import sqlite3
import sys
import zipfile

HERE = os.path.dirname(__file__)
RAW = os.path.abspath(os.path.join(HERE, "..", "data", "raw", "sec_edgar"))
CATALOG = os.path.abspath(os.path.join(HERE, "..", "data", "catalog.db"))
CLEAN = os.path.abspath(os.path.join(HERE, "..", "data", "clean"))

EXPECTED_SIZE = {"companyfacts.zip": 1385082359, "submissions.zip": 1542907394}

ok = True


def hr(t):
    print("\n" + "=" * 66 + f"\n{t}\n" + "=" * 66)


hr("1. SEC EDGAR -- exact byte sizes (match = not truncated)")
for name, exp in EXPECTED_SIZE.items():
    sz = os.path.getsize(os.path.join(RAW, name))
    match = sz == exp
    ok &= match
    print(f"  {name}: {sz:,} bytes (expected {exp:,}) -> {'OK' if match else 'MISMATCH'}")

hr("2. SEC EDGAR -- archive integrity (central dir intact + CRC sample)")
zips = {}
for name in EXPECTED_SIZE:
    z = zipfile.ZipFile(os.path.join(RAW, name))
    zips[name] = z
    members = z.namelist()
    n = 800 if name == "companyfacts.zip" else 400
    sample = random.sample(members, min(n, len(members)))
    bad = 0
    for m in sample:
        try:
            with z.open(m) as f:
                f.read()  # decompress -> verifies CRC
        except Exception:
            bad += 1
    ok &= bad == 0
    print(f"  {name}: {len(members):,} members | CRC-checked {len(sample):,} random -> {bad} corrupt")

hr("3. SEC EDGAR -- coverage")
ct = json.load(open(os.path.join(RAW, "company_tickers.json")))
n_tickers = len({v["cik_str"] for v in ct.values()})
print(f"  tickered companies (company_tickers.json):    {n_tickers:,}")
print(f"  companies w/ XBRL facts (companyfacts.zip):   {len(zips['companyfacts.zip'].namelist()):,}")
print(f"  total filers (submissions.zip):               {len(zips['submissions.zip'].namelist()):,}")
print("  (facts >= tickered is expected: includes delisted / foreign / non-tickered filers)")

hr("4. SEC EDGAR -- content & recency (multi-company spot check)")
t2c = {}
for v in ct.values():
    t2c.setdefault(v["ticker"], v["cik_str"])
zf = zips["companyfacts.zip"]


def inspect(cik):
    d = json.loads(zf.read(f"CIK{cik:010d}.json"))
    gaap = d.get("facts", {}).get("us-gaap", {})
    maxfiled = None
    for body in gaap.values():
        for points in body.get("units", {}).values():
            for p in points:
                fdt = p.get("filed")
                if fdt and (maxfiled is None or fdt > maxfiled):
                    maxfiled = fdt
    rev = gaap.get("RevenueFromContractWithCustomerExcludingAssessedTax") or gaap.get("Revenues")
    rv = list(rev["units"].values())[0][-1].get("val") if rev else None
    return d.get("entityName"), len(gaap), maxfiled, rv


filed_dates = []
for tk in ["AAPL", "MSFT", "AMZN", "TSLA", "JPM", "WMT", "NVDA", "KO"]:
    cik = t2c.get(tk)
    if not cik:
        print(f"  {tk}: ticker not found")
        continue
    try:
        name, ntags, mf, rv = inspect(cik)
        filed_dates.append(mf)
        rvs = f"${rv:,}" if rv else "n/a"
        print(f"  {tk:5} CIK{cik:<7} {(name or '')[:32]:32} tags={ntags:3} filed<={mf} rev={rvs}")
    except Exception as e:
        print(f"  {tk}: ERROR {e}")
        ok = False
if filed_dates:
    print(f"  most recent filing in sample: {max(d for d in filed_dates if d)}")

hr("5. World Bank -- stored Parquet vs LIVE API (accuracy)")
con = sqlite3.connect(CATALOG)
nwb = con.execute("select count(*) from series where source_id='worldbank'").fetchone()[0]
print(f"  catalog WB series: {nwb:,}")
import pyarrow.parquet as pq  # noqa: E402
rows = pq.read_table(os.path.join(CLEAN, "worldbank", "worldbank__NY.GDP.MKTP.CD__USA.parquet")).to_pylist()
stored = rows[-1]
print(f"  stored USA GDP {stored['obs_date']} = {stored['value']:,.0f}")
import requests  # noqa: E402
live = requests.get(
    "https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD?format=json&date=2024",
    headers={"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}, timeout=30).json()
liveval = live[1][0]["value"]
match = abs(stored["value"] - liveval) < 1
ok &= match
print(f"  live  USA GDP 2024 = {liveval:,.0f} -> {'MATCH' if match else 'MISMATCH'}")

hr("VERDICT")
print("  ALL CHECKS PASSED" if ok else "  *** SOME CHECKS FAILED ***")
sys.exit(0 if ok else 1)
