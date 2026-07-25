#!/usr/bin/env python3
"""Enumerate the ENTIRE BEA API catalog and write a manifest JSON.

Caches GetDataSetList, GetParameterList, GetParameterValues, and the
GetParameterValuesFiltered LineCodes for Regional tables, so the crawler
does not have to re-enumerate. Prints the TOTAL counts that exist.
"""
from __future__ import annotations
import json
import os
import sys
import time

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
sys.path.insert(0, ROOT)
from core.config import require  # noqa: E402

UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
API = "https://apps.bea.gov/api/data/"
OUT = os.path.join(ROOT, "data", "raw", "bea")
KEY = require("BEA_API_KEY")

S = requests.Session()
S.headers.update(UA)


def call(method, **params):
    p = {"UserID": KEY, "method": method, "ResultFormat": "JSON"}
    p.update(params)
    for attempt in range(6):
        try:
            r = S.get(API, params=p, timeout=120)
            if r.status_code == 429:
                time.sleep(8 * (attempt + 1))
                continue
            d = r.json()
            return d.get("BEAAPI", {})
        except Exception as e:  # noqa: BLE001
            if attempt == 5:
                raise
            time.sleep(3 * (attempt + 1))
    return {}


def results_list(api, key="ParamValue"):
    res = api.get("Results", {})
    if isinstance(res, list):
        res = res[0] if res else {}
    v = res.get(key, res) if isinstance(res, dict) else []
    if isinstance(v, dict):
        v = [v]
    return v if isinstance(v, list) else []


def gpv(dsn, pn):
    api = call("GetParameterValues", datasetname=dsn, ParameterName=pn)
    return results_list(api, "ParamValue")


def main():
    manifest = {}

    # 1) dataset list
    api = call("GetDataSetList")
    res = api.get("Results", {})
    if isinstance(res, list):
        res = res[0]
    datasets = res.get("Dataset", [])
    manifest["datasets"] = datasets
    real = [d["DatasetName"] for d in datasets if d["DatasetName"] != "APIDatasetMetaData"]
    print(f"TOTAL DATASETS: {len(datasets)} ({len(real)} data + APIDatasetMetaData)", flush=True)

    # 2) parameter list + values for each real dataset
    manifest["parameters"] = {}
    manifest["param_values"] = {}
    for dsn in real:
        api = call("GetParameterList", datasetname=dsn)
        params = results_list(api, "Parameter")
        manifest["parameters"][dsn] = params
        manifest["param_values"].setdefault(dsn, {})
        time.sleep(0.25)

    # enumerate the driving parameter values we need
    need = {
        "NIPA": ["TableName", "Frequency"],
        "NIUnderlyingDetail": ["TableName", "Frequency"],
        "FixedAssets": ["TableName"],
        "GDPbyIndustry": ["TableID", "Industry", "Frequency"],
        "UnderlyingGDPbyIndustry": ["TableID", "Industry", "Frequency"],
        "InputOutput": ["TableID"],
        "ITA": ["Indicator", "AreaOrCountry", "Frequency"],
        "IIP": ["TypeOfInvestment", "Component", "Frequency"],
        "IntlServTrade": ["TypeOfService", "AreaOrCountry"],
        "IntlServSTA": ["Industry", "AreaOrCountry"],
        "Regional": ["TableName", "GeoFips"],
        "MNE": ["DirectionOfInvestment", "Classification", "SeriesID", "Year"],
    }
    for dsn, pns in need.items():
        for pn in pns:
            vals = gpv(dsn, pn)
            manifest["param_values"][dsn][pn] = vals
            print(f"  {dsn}.{pn}: {len(vals)}", flush=True)
            time.sleep(0.25)

    # 3) Regional LineCodes per table (GetParameterValuesFiltered)
    manifest["regional_linecodes"] = {}
    rtables = [x["Key"] for x in manifest["param_values"]["Regional"]["TableName"]]
    print(f"Regional tables: {len(rtables)} -- fetching LineCodes per table...", flush=True)
    for i, t in enumerate(rtables):
        api = call("GetParameterValuesFiltered", datasetname="Regional",
                   TargetParameter="LineCode", TableName=t)
        lcs = results_list(api, "ParamValue")
        manifest["regional_linecodes"][t] = [str(x.get("Key")) for x in lcs]
        if (i + 1) % 20 == 0:
            print(f"    {i+1}/{len(rtables)} tables", flush=True)
        time.sleep(0.2)

    # totals
    pv = manifest["param_values"]
    total_linecodes = sum(len(v) for v in manifest["regional_linecodes"].values())
    print("\n===== CATALOG TOTALS =====", flush=True)
    print(f"NIPA tables: {len(pv['NIPA']['TableName'])}", flush=True)
    print(f"NIUnderlyingDetail tables: {len(pv['NIUnderlyingDetail']['TableName'])}", flush=True)
    print(f"FixedAssets tables: {len(pv['FixedAssets']['TableName'])}", flush=True)
    print(f"GDPbyIndustry tables: {len(pv['GDPbyIndustry']['TableID'])} industries: {len(pv['GDPbyIndustry']['Industry'])}", flush=True)
    print(f"UnderlyingGDPbyIndustry tables: {len(pv['UnderlyingGDPbyIndustry']['TableID'])}", flush=True)
    print(f"InputOutput tables: {len(pv['InputOutput']['TableID'])}", flush=True)
    print(f"ITA indicators: {len(pv['ITA']['Indicator'])} countries: {len(pv['ITA']['AreaOrCountry'])}", flush=True)
    print(f"IIP types: {len(pv['IIP']['TypeOfInvestment'])}", flush=True)
    print(f"IntlServTrade services: {len(pv['IntlServTrade']['TypeOfService'])} countries: {len(pv['IntlServTrade']['AreaOrCountry'])}", flush=True)
    print(f"IntlServSTA industries: {len(pv['IntlServSTA']['Industry'])} countries: {len(pv['IntlServSTA']['AreaOrCountry'])}", flush=True)
    print(f"Regional tables: {len(pv['Regional']['TableName'])} total (table x linecode) combos: {total_linecodes}", flush=True)
    print(f"MNE directions: {len(pv['MNE']['DirectionOfInvestment'])} classifications: {len(pv['MNE']['Classification'])}", flush=True)

    os.makedirs(OUT, exist_ok=True)
    with open(os.path.join(OUT, "catalog_manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f)
    print(f"\nWROTE {os.path.join(OUT, 'catalog_manifest.json')}", flush=True)


if __name__ == "__main__":
    main()
