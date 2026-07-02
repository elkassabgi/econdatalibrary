#!/usr/bin/env python3
"""Validate the free government API keys with a minimal live call each.

Run: python jobs/validate_keys.py
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from core.config import load_env  # noqa: E402

import requests  # noqa: E402

load_env()
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}


def check_bls():
    key = os.environ["BLS_API_KEY"]
    r = requests.post("https://api.bls.gov/publicAPI/v2/timeseries/data/",
                      json={"seriesid": ["CUUR0000SA0"], "startyear": "2024",
                            "endyear": "2025", "registrationkey": key}, timeout=30)
    j = r.json()
    series = j.get("Results", {}).get("series", [])
    n = len(series[0]["data"]) if series else 0
    return j.get("status") == "REQUEST_SUCCEEDED", f"status={j.get('status')}, CPI-U points={n}"


def check_bea():
    key = os.environ["BEA_API_KEY"]
    r = requests.get("https://apps.bea.gov/api/data/", headers=UA, timeout=30, params={
        "UserID": key, "method": "GetData", "datasetname": "NIPA", "TableName": "T10101",
        "Frequency": "Q", "Year": "2024", "ResultFormat": "JSON"})
    api = r.json().get("BEAAPI", {})
    if "Error" in api or "Error" in api.get("Results", {}):
        return False, str(api.get("Error") or api.get("Results", {}).get("Error"))[:90]
    data = api.get("Results", {}).get("Data", [])
    return bool(data), f"NIPA T10101 rows={len(data)}"


def check_census():
    key = os.environ["CENSUS_API_KEY"]
    r = requests.get("https://api.census.gov/data/2021/acs/acs1", headers=UA, timeout=30,
                     params={"get": "NAME,B01001_001E", "for": "us:1", "key": key})
    ok = r.status_code == 200
    return ok, (f"status={r.status_code}, US pop row={r.json()[1]}" if ok else f"status={r.status_code} {r.text[:80]}")


def check_eia():
    key = os.environ["EIA_API_KEY"]
    r = requests.get("https://api.eia.gov/v2/petroleum/pri/spt/data/", headers=UA, timeout=30, params={
        "api_key": key, "frequency": "weekly", "data[0]": "value", "facets[series][]": "RWTC",
        "sort[0][column]": "period", "sort[0][direction]": "desc", "length": "2"})
    if r.status_code != 200:
        return False, f"status={r.status_code} {r.text[:80]}"
    data = r.json().get("response", {}).get("data", [])
    last = data[0] if data else {}
    return bool(data), f"WTI spot last {last.get('period')} = {last.get('value')}"


results = {}
for name, fn in [("BLS", check_bls), ("BEA", check_bea), ("Census", check_census), ("EIA", check_eia)]:
    try:
        ok, msg = fn()
        results[name] = ok
        print(f"  {'OK  ' if ok else 'FAIL'} {name:7} {msg}")
    except Exception as e:
        results[name] = False
        print(f"  FAIL {name:7} {type(e).__name__}: {e}")

print(f"\nall keys valid: {all(results.values())}")
